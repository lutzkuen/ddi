//! Where each partition starts reading (docs/kafka-design.md §4.4).
//!
//! Two moments decide it:
//!
//! - **At open**, before the member joins its group, once for the whole topic
//!   ([`plan_open`]): which era the app_id's offsets are in (fresh, kafka-delta-ingest's,
//!   ddi's), the typo guard, the takeover check, the `kdi_compat` refusal, and the txn-only
//!   commit that records where a fresh app_id starts or that ends kafka-delta-ingest's era.
//! - **At every assign, reset or blocked recheck**, per partition ([`resolve`]): its offset
//!   in the target against what the topic holds now.
//!
//! Nothing here reads Kafka: the watermarks are handed in, so every rule is tested without a
//! broker.

use std::collections::{BTreeMap, BTreeSet};

use deltalake::DeltaTable;
use tracing::warn;

use crate::config::kafka::{NamedStart, StartPolicy};
use crate::error::{Error, Result};
use crate::fence::{self, Era, NoTxnStart};
use crate::kafka::partitions::{Block, Reread};
use crate::sink::MessageMark;

/// How many recent commits the typo guard reads for the appIds a target holds (§4.4 step 7).
pub(crate) const TYPO_GUARD_COMMITS: u64 = 100;

/// Where one partition starts: an offset, re-read first when it is the stored one, or a
/// block an operator has to settle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Start {
    Ready { offset: i64, reread: Option<Reread> },
    Blocked(Block),
}

/// §4.4's table: where a partition whose offset in the target is `stored` starts, given the
/// topic's `low` and `high` watermarks and the partition's explicit `starting_offsets`
/// entry, if any (the `None` rows only; an entry for a partition with an offset is ignored).
///
/// | stored | condition | start |
/// |---|---|---|
/// | `b` | `low <= b < high` | `b`, re-read and checked against `mark` (Unverifiable without one) |
/// | `b` | `b + 1 == low` | `b + 1`, nothing to verify |
/// | `b` | `b + 1 < low` | blocked: unavailable |
/// | `b` | `b >= high` | blocked: regressed |
/// | none | | [`fence::start_without_offset`] |
pub(crate) fn resolve(
    stored: Option<i64>,
    mark: Option<MessageMark>,
    low: i64,
    high: i64,
    explicit: Option<i64>,
) -> Start {
    match stored {
        Some(b) if b + 1 < low => Start::Blocked(Block::Unavailable { wanted: b + 1, low }),
        Some(b) if b + 1 == low => Start::Ready {
            offset: b + 1,
            reread: None,
        },
        Some(b) if b >= high => Start::Blocked(Block::Regressed { stored: b, high }),
        Some(b) => Start::Ready {
            offset: b,
            reread: Some(
                mark.filter(|m| m.offset == b)
                    .map_or(Reread::Unverifiable(b), Reread::Expect),
            ),
        },
        None => match fence::start_without_offset(low, high, explicit) {
            NoTxnStart::Explicit(offset) => Start::Ready {
                offset,
                reread: None,
            },
            NoTxnStart::Zero => Start::Ready {
                offset: 0,
                reread: None,
            },
            NoTxnStart::Unavailable { wanted, low } => {
                Start::Blocked(Block::Unavailable { wanted, low })
            }
            NoTxnStart::Regressed { wanted, high } => Start::Blocked(Block::Regressed {
                stored: wanted - 1,
                high,
            }),
        },
    }
}

/// Where every partition of a fresh app_id starts (§4.4 step 8): an explicit map (complete,
/// unless it has `"*"`), `earliest` (the low watermark) or `latest` (the high one, which
/// under `read_committed` is the last stable offset).
pub(crate) fn fresh_starts(
    topic: &str,
    policy: &StartPolicy,
    watermarks: &BTreeMap<i32, (i64, i64)>,
) -> Result<BTreeMap<i32, i64>> {
    let named = |n: NamedStart, (low, high): (i64, i64)| match n {
        NamedStart::Earliest => low,
        NamedStart::Latest => high,
    };
    match policy {
        StartPolicy::Earliest => Ok(watermarks
            .iter()
            .map(|(p, w)| (*p, named(NamedStart::Earliest, *w)))
            .collect()),
        StartPolicy::Latest => Ok(watermarks
            .iter()
            .map(|(p, w)| (*p, named(NamedStart::Latest, *w)))
            .collect()),
        StartPolicy::Explicit { partitions, rest } => {
            let missing: Vec<i32> = watermarks
                .keys()
                .filter(|p| !partitions.contains_key(p))
                .copied()
                .collect();
            if rest.is_none() && !missing.is_empty() {
                return Err(Error::KafkaStartingOffsetsIncomplete {
                    topic: topic.to_string(),
                    missing,
                });
            }
            Ok(watermarks
                .iter()
                .map(|(p, w)| match partitions.get(p) {
                    Some(s) => (*p, *s),
                    None => (*p, named(rest.expect("checked above"), *w)),
                })
                .collect())
        }
    }
}

/// The baseline a fresh start commits: every partition starting above 0 at its start − 1. A
/// partition starting at 0 gets no offset: −1 would be read by kafka-delta-ingest as `End` on
/// a rollback, and a partition without an offset starts at 0 anyway. `None` when none starts
/// above 0.
pub(crate) fn fresh_baseline(starts: &BTreeMap<i32, i64>) -> Option<BTreeMap<i32, i64>> {
    let assert: BTreeMap<i32, i64> = starts
        .iter()
        .filter(|(_, s)| **s > 0)
        .map(|(p, s)| (*p, s - 1))
        .collect();
    (!assert.is_empty()).then_some(assert)
}

/// The typo guard (§4.4 step 7): a fresh app_id on a target that has data, without an
/// explicit `starting_offsets` of the pipeline's own, is refused unless every `txn` the target
/// holds belongs to another configured Kafka pipeline on it (EO-m15). A target with data and
/// no `txn` at all is refused too.
pub(crate) fn typo_guard(
    app_id: &str,
    target_uri: &str,
    data_files: usize,
    explicit: bool,
    app_ids: &BTreeSet<String>,
    neighbours: &[String],
) -> Result<()> {
    if data_files == 0 || explicit {
        return Ok(());
    }
    let owner = |txn: &String| {
        neighbours.iter().find(|a| {
            fence::partition_of(a, txn).is_some() || *txn == fence::takeover_marker_app_id(a)
        })
    };
    if !app_ids.is_empty() && app_ids.iter().all(|t| owner(t).is_some()) {
        return Ok(());
    }
    // What each txn says its app_id is: `<a>-<p>` and `<a>.ddi` are `a`'s.
    let mut seen: Vec<String> = app_ids
        .iter()
        .map(|t| {
            if let Some(a) = t.strip_suffix(".ddi") {
                return a.to_string();
            }
            match t.rsplit_once('-') {
                Some((a, p)) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => {
                    a.to_string()
                }
                _ => t.clone(),
            }
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    seen.sort_by_key(|a| (edit_distance(a, app_id), a.clone()));
    Err(Error::KafkaAppIdUnknown {
        app_id: app_id.to_string(),
        target_uri: target_uri.to_string(),
        seen,
    })
}

/// Levenshtein distance, for the typo guard's suggestion.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut prev = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cur = row[j + 1];
            row[j + 1] = (prev + usize::from(ca != *cb)).min(row[j] + 1).min(cur + 1);
            prev = cur;
        }
    }
    row[b.len()]
}

/// What the target looks like to the open: its data files, and whether it holds rows that may
/// be this topic's (§4.4 step 2): it has data files, unless the file statistics of
/// `<prefix>topic`, where it has that column, show that none of them holds this topic.
pub(crate) fn target_rows(target: &DeltaTable, prefix: &str, topic: &str) -> Result<(usize, bool)> {
    use deltalake::delta_datafusion::DataFusionMixins;
    let snapshot = target.snapshot().map_err(Error::Delta)?;
    let files = snapshot.log_data().num_files();
    if files == 0 {
        return Ok((0, false));
    }
    let column = format!("{prefix}topic");
    if snapshot
        .snapshot()
        .read_schema()
        .field_with_name(&column)
        .is_err()
    {
        return Ok((files, true));
    }
    for file in snapshot.log_data().iter() {
        let Some(stats) = file.stats() else {
            return Ok((files, true));
        };
        let Ok(stats) = serde_json::from_str::<serde_json::Value>(&stats) else {
            return Ok((files, true));
        };
        let bound = |which: &str| {
            stats
                .get(which)
                .and_then(|v| v.get(&column))
                .and_then(|v| v.as_str())
        };
        let (Some(min), Some(max)) = (bound("minValues"), bound("maxValues")) else {
            return Ok((files, true));
        };
        // Writers truncate string statistics (delta-rs and Spark at 32 characters): a bound
        // that may be cut off proves nothing.
        if min.chars().count() >= 32 || max.chars().count() >= 32 {
            return Ok((files, true));
        }
        if min <= topic && topic <= max {
            return Ok((files, true));
        }
    }
    Ok((files, false))
}

/// What the open has found out, for [`plan_open`].
pub(crate) struct OpenFacts<'a> {
    pub app_id: &'a str,
    pub topic: &'a str,
    pub target_uri: &'a str,
    /// Every partition of the topic, with its offset in the target.
    pub stored: &'a BTreeMap<i32, Option<i64>>,
    /// Every partition of the topic, with its low and high watermarks.
    pub watermarks: &'a BTreeMap<i32, (i64, i64)>,
    pub era: Era,
    pub data_files: usize,
    pub holds_rows: bool,
    pub policy: &'a StartPolicy,
    /// The pipeline sets `starting_offsets` itself.
    pub explicit: bool,
    pub kdi_compat: Option<bool>,
    /// Other configured Kafka pipelines' app_ids on this target.
    pub neighbours: &'a [String],
    /// The appIds of the target's recent `txn`s and its checkpoint's: the typo guard's
    /// evidence, read only for a fresh app_id on a target with data.
    pub app_ids: &'a BTreeSet<String>,
}

/// What the open decided.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OpenPlan {
    /// The txn-only commit to make before joining the group: a fresh app_id's baseline, or the
    /// takeover baseline that ends kafka-delta-ingest's era. Both carry the takeover marker.
    pub baseline: Option<BTreeMap<i32, i64>>,
    /// `kdi_compat` as it applies: the setting, or false where it may be left unset.
    pub kdi_compat: bool,
}

/// §4.4 steps 3, 6, 7 and 8, in the order that gives the most useful refusal: in a fresh era
/// the typo guard first (a mistyped app_id gets its suggestion), then the takeover check over
/// every partition, then the `kdi_compat` refusal, then the baseline. The caller has read the
/// facts (steps 1 and 2) and made the refusals of steps 4 and 5; the baseline it commits is
/// the last thing before it subscribes or assigns, so a refused open leaves no marker.
pub(crate) fn plan_open(f: &OpenFacts<'_>) -> Result<OpenPlan> {
    let explicit_entries: BTreeMap<i32, i64> = f.policy.partitions().cloned().unwrap_or_default();
    if f.era == Era::Fresh {
        typo_guard(
            f.app_id,
            f.target_uri,
            f.data_files,
            f.explicit,
            f.app_ids,
            f.neighbours,
        )?;
    }
    let partitions: BTreeMap<i32, (Option<i64>, i64)> = f
        .stored
        .iter()
        .map(|(p, s)| (*p, (*s, f.watermarks.get(p).map_or(0, |w| w.1))))
        .collect();
    let acknowledged: BTreeSet<i32> = explicit_entries.keys().copied().collect();
    fence::takeover_check(f.app_id, f.era, f.holds_rows, &partitions, &acknowledged)?;

    let may_be_kdis = f.era == Era::Kdi || (f.era == Era::Fresh && f.holds_rows);
    let kdi_compat = match f.kdi_compat {
        Some(set) => set,
        None if may_be_kdis => {
            return Err(Error::KafkaCompatUnset {
                app_id: f.app_id.to_string(),
                target_uri: f.target_uri.to_string(),
                fresh: f.era == Era::Fresh,
            })
        }
        None => false,
    };

    if f.era == Era::Ddi {
        let ignored: Vec<i32> = explicit_entries
            .keys()
            .filter(|p| f.stored.get(p).is_some_and(|s| s.is_some()))
            .copied()
            .collect();
        if !ignored.is_empty() {
            warn!(
                app_id = f.app_id,
                partitions = ?ignored,
                "starting_offsets entries for partitions that have an offset in the target are \
                 ignored; they can be removed"
            );
        }
    }
    let baseline = match f.era {
        Era::Fresh => fresh_baseline(&fresh_starts(f.topic, f.policy, f.watermarks)?),
        Era::Kdi => fence::takeover_baseline(f.era, f.stored, &explicit_entries),
        Era::Ddi => None,
    };
    Ok(OpenPlan {
        baseline,
        kdi_compat,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mark(offset: i64) -> MessageMark {
        MessageMark {
            offset,
            timestamp_ms: 7,
            value_len: 3,
        }
    }

    /// Every row of §4.4's table.
    #[test]
    fn every_row_of_the_start_table() {
        // Some(b), low <= b < high: assigned at b, re-read and checked.
        assert_eq!(
            resolve(Some(41), Some(mark(41)), 0, 100, None),
            Start::Ready {
                offset: 41,
                reread: Some(Reread::Expect(mark(41)))
            }
        );
        // ... Unverifiable without a mark, or with one for another offset.
        for m in [None, Some(mark(40))] {
            assert_eq!(
                resolve(Some(41), m, 0, 100, None),
                Start::Ready {
                    offset: 41,
                    reread: Some(Reread::Unverifiable(41))
                }
            );
        }
        // Some(b), b + 1 == low: b itself trimmed, nothing to verify.
        assert_eq!(
            resolve(Some(41), Some(mark(41)), 42, 100, None),
            Start::Ready {
                offset: 42,
                reread: None
            }
        );
        // Some(b), b + 1 < low: a retention gap.
        assert_eq!(
            resolve(Some(41), None, 50, 100, None),
            Start::Blocked(Block::Unavailable {
                wanted: 42,
                low: 50
            })
        );
        // Some(b), b >= high: recreated, or another cluster.
        assert_eq!(
            resolve(Some(41), None, 0, 41, None),
            Start::Blocked(Block::Regressed {
                stored: 41,
                high: 41
            })
        );
        // None with an explicit entry s, low <= s <= high.
        assert_eq!(
            resolve(None, None, 5, 100, Some(7)),
            Start::Ready {
                offset: 7,
                reread: None
            }
        );
        // ... below the low watermark, or past the high one.
        assert_eq!(
            resolve(None, None, 5, 100, Some(3)),
            Start::Blocked(Block::Unavailable { wanted: 3, low: 5 })
        );
        assert_eq!(
            resolve(None, None, 5, 100, Some(101)),
            Start::Blocked(Block::Regressed {
                stored: 100,
                high: 100
            })
        );
        // None, low == 0: exactly offset 0.
        assert_eq!(
            resolve(None, None, 0, 100, None),
            Start::Ready {
                offset: 0,
                reread: None
            }
        );
        // None, low > 0: messages never read were trimmed.
        assert_eq!(
            resolve(None, None, 5, 100, None),
            Start::Blocked(Block::Unavailable { wanted: 0, low: 5 })
        );
    }

    fn watermarks() -> BTreeMap<i32, (i64, i64)> {
        BTreeMap::from([(0, (0, 10)), (1, (5, 20)), (2, (0, 0))])
    }

    fn explicit(entries: &[(i32, i64)], rest: Option<NamedStart>) -> StartPolicy {
        StartPolicy::Explicit {
            partitions: entries.iter().copied().collect(),
            rest,
        }
    }

    #[test]
    fn fresh_starts_by_policy() {
        let w = watermarks();
        assert_eq!(
            fresh_starts("t", &StartPolicy::Earliest, &w).unwrap(),
            BTreeMap::from([(0, 0), (1, 5), (2, 0)])
        );
        assert_eq!(
            fresh_starts("t", &StartPolicy::Latest, &w).unwrap(),
            BTreeMap::from([(0, 10), (1, 20), (2, 0)])
        );
        assert_eq!(
            fresh_starts("t", &explicit(&[(1, 7)], Some(NamedStart::Latest)), &w).unwrap(),
            BTreeMap::from([(0, 10), (1, 7), (2, 0)])
        );
        let e = fresh_starts("t", &explicit(&[(1, 7)], None), &w).unwrap_err();
        assert!(
            matches!(&e, Error::KafkaStartingOffsetsIncomplete { missing, .. } if missing == &vec![0, 2]),
            "{e:?}"
        );
    }

    /// A baseline whenever any start is above 0, asserting start − 1; a partition starting at
    /// 0 gets no offset at all.
    #[test]
    fn baseline_when_any_start_positive() {
        assert_eq!(
            fresh_baseline(&BTreeMap::from([(0, 0), (1, 5), (2, 1)])),
            Some(BTreeMap::from([(1, 4), (2, 0)]))
        );
        assert_eq!(fresh_baseline(&BTreeMap::from([(0, 0), (1, 0)])), None);
    }

    fn facts<'a>(
        stored: &'a BTreeMap<i32, Option<i64>>,
        watermarks: &'a BTreeMap<i32, (i64, i64)>,
        policy: &'a StartPolicy,
        app_ids: &'a BTreeSet<String>,
        neighbours: &'a [String],
    ) -> OpenFacts<'a> {
        OpenFacts {
            app_id: "kafka_delta_ingest",
            topic: "orders",
            target_uri: "/lake/orders",
            stored,
            watermarks,
            era: Era::Fresh,
            data_files: 0,
            holds_rows: false,
            policy,
            explicit: false,
            kdi_compat: None,
            neighbours,
            app_ids,
        }
    }

    /// A fresh app_id on a target with data, and no explicit start: refused, with the app_id
    /// the target's offsets hold as the suggestion; let through with an explicit start, or on
    /// an empty target.
    #[test]
    fn app_id_typo_refused() {
        let ids: BTreeSet<String> = ["kafka_delta_ingest-0", "kafka_delta_ingest-1", "other-3"]
            .into_iter()
            .map(String::from)
            .collect();
        let e = typo_guard("kafka_delta_ingset", "/lake/orders", 3, false, &ids, &[]).unwrap_err();
        let Error::KafkaAppIdUnknown { seen, .. } = &e else {
            panic!("{e:?}")
        };
        assert_eq!(seen[0], "kafka_delta_ingest", "{e}");
        assert!(
            e.to_string()
                .contains("did you mean \"kafka_delta_ingest\""),
            "{e}"
        );

        assert!(typo_guard("kafka_delta_ingset", "/t", 3, true, &ids, &[]).is_ok());
        assert!(typo_guard("kafka_delta_ingset", "/t", 0, false, &ids, &[]).is_ok());
        // Data and no txn at all: refused too.
        let none = BTreeSet::new();
        let e = typo_guard("a", "/t", 3, false, &none, &[]).unwrap_err();
        assert!(e.to_string().contains("no offsets of any app_id"), "{e}");
    }

    /// A second pipeline on a shared table, whose txns are all a configured neighbour's, is let
    /// through the typo guard; the exemption does not reach the takeover check, which a target
    /// with rows that may be the topic's still runs.
    #[test]
    fn shared_table_second_pipeline_allowed() {
        let ids: BTreeSet<String> = ["events_a-0", "events_a-1", "events_a.ddi"]
            .into_iter()
            .map(String::from)
            .collect();
        let neighbours = vec!["events_a".to_string()];
        assert!(typo_guard("events_b", "/t", 4, false, &ids, &neighbours).is_ok());

        // The twin: the same table with rows that may be this topic's.
        let stored = BTreeMap::from([(0, None), (1, None)]);
        let w = BTreeMap::from([(0, (0, 5)), (1, (0, 0))]);
        let policy = StartPolicy::Earliest;
        let mut f = facts(&stored, &w, &policy, &ids, &neighbours);
        f.app_id = "events_b";
        f.data_files = 4;
        f.holds_rows = true;
        f.kdi_compat = Some(false);
        let e = plan_open(&f).unwrap_err();
        assert!(
            matches!(&e, Error::KafkaOffsetMissing { partitions, fresh: true, .. } if partitions == &vec![0]),
            "{e:?}"
        );
        // With rows of other topics only (file statistics), it starts where it is told.
        f.holds_rows = false;
        assert_eq!(plan_open(&f).unwrap().baseline, None);
    }

    #[test]
    fn kdi_compat_unset_on_kdi_table_refused() {
        let ids = BTreeSet::from(["kafka_delta_ingest-0".to_string()]);
        let policy = StartPolicy::Explicit {
            partitions: BTreeMap::from([(1, 0)]),
            rest: None,
        };
        let stored = BTreeMap::from([(0, Some(9)), (1, None)]);
        let w = BTreeMap::from([(0, (0, 20)), (1, (0, 4))]);
        let mut f = facts(&stored, &w, &policy, &ids, &[]);
        // kafka-delta-ingest's era.
        f.era = Era::Kdi;
        f.data_files = 2;
        f.holds_rows = true;
        let e = plan_open(&f).unwrap_err();
        assert!(
            matches!(e, Error::KafkaCompatUnset { fresh: false, .. }),
            "{e:?}"
        );
        // Fresh with rows.
        let stored_fresh = BTreeMap::from([(0, None), (1, None)]);
        let policy_all = StartPolicy::Explicit {
            partitions: BTreeMap::from([(0, 0), (1, 0)]),
            rest: None,
        };
        let mut f = facts(&stored_fresh, &w, &policy_all, &ids, &[]);
        f.explicit = true;
        f.data_files = 2;
        f.holds_rows = true;
        let e = plan_open(&f).unwrap_err();
        assert!(
            matches!(e, Error::KafkaCompatUnset { fresh: true, .. }),
            "{e:?}"
        );
        // Set either way, or unset on a table that cannot be kdi's: no refusal.
        f.kdi_compat = Some(true);
        assert!(plan_open(&f).unwrap().kdi_compat);
        f.kdi_compat = None;
        f.holds_rows = false;
        f.data_files = 0;
        assert!(!plan_open(&f).unwrap().kdi_compat);
    }

    /// The takeover check names every partition of the topic without an offset whose topic
    /// holds messages, not only the ones this member will own.
    #[test]
    fn takeover_check_runs_over_every_partition_at_open() {
        let stored: BTreeMap<i32, Option<i64>> = (0..6)
            .map(|p| (p, if p == 0 { Some(3) } else { None }))
            .collect();
        let w: BTreeMap<i32, (i64, i64)> = (0..6)
            .map(|p| (p, (0, if p == 4 { 0 } else { 10 })))
            .collect();
        let policy = StartPolicy::Explicit {
            partitions: BTreeMap::from([(2, 5)]),
            rest: None,
        };
        let ids = BTreeSet::new();
        let mut f = facts(&stored, &w, &policy, &ids, &[]);
        f.era = Era::Kdi;
        f.kdi_compat = Some(true);
        let e = plan_open(&f).unwrap_err();
        assert!(
            matches!(&e, Error::KafkaOffsetMissing { partitions, fresh: false, .. } if partitions == &vec![1, 3, 5]),
            "{e:?}"
        );
    }

    /// A fresh start above 0 commits a baseline, and kafka-delta-ingest's era a takeover
    /// baseline with every acknowledgment as an offset; ddi's era none.
    #[test]
    fn baseline_writes_the_takeover_marker() {
        let stored = BTreeMap::from([(0, None), (1, None)]);
        let w = BTreeMap::from([(0, (0, 10)), (1, (0, 7))]);
        let ids = BTreeSet::new();
        let policy = StartPolicy::Latest;
        let f = facts(&stored, &w, &policy, &ids, &[]);
        assert_eq!(
            plan_open(&f).unwrap().baseline,
            Some(BTreeMap::from([(0, 9), (1, 6)]))
        );

        let stored = BTreeMap::from([(0, Some(4)), (1, None)]);
        let policy = StartPolicy::Explicit {
            partitions: BTreeMap::from([(1, 3)]),
            rest: None,
        };
        let mut f = facts(&stored, &w, &policy, &ids, &[]);
        f.era = Era::Kdi;
        f.kdi_compat = Some(true);
        assert_eq!(
            plan_open(&f).unwrap().baseline,
            Some(BTreeMap::from([(0, 4), (1, 2)]))
        );

        f.era = Era::Ddi;
        f.kdi_compat = None;
        assert_eq!(plan_open(&f).unwrap().baseline, None);
    }

    /// A refused open plans no baseline: the refusals come before it, so nothing it would have
    /// committed (the marker above all) is left behind.
    #[test]
    fn takeover_baseline_committed_last_at_open() {
        let stored = BTreeMap::from([(0, Some(4)), (1, None)]);
        let w = BTreeMap::from([(0, (0, 10)), (1, (0, 7))]);
        let ids = BTreeSet::new();
        let policy = StartPolicy::Earliest;
        let mut f = facts(&stored, &w, &policy, &ids, &[]);
        f.era = Era::Kdi;
        f.kdi_compat = Some(true);
        // Partition 1 has messages and no acknowledgment: refused, no plan at all.
        assert!(matches!(
            plan_open(&f),
            Err(Error::KafkaOffsetMissing { .. })
        ));
    }

    #[test]
    fn edit_distance_orders_suggestions() {
        assert_eq!(edit_distance("kafka_delta_ingest", "kafka_delta_ingest"), 0);
        assert_eq!(edit_distance("kafka_delta_ingset", "kafka_delta_ingest"), 2);
        assert_eq!(edit_distance("", "abc"), 3);
    }
}
