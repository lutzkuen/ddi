//! `ddi kafka set-offsets`: move a Kafka pipeline's offsets by hand, with the guards that keep
//! it from losing or doubling data by accident (docs/kafka-design.md §4.8).
//!
//! User-facing offsets are always the **next offset to read**; only the `txn` stores the last
//! one written. A move is one txn-only commit, stamped as every Kafka txn is
//! ([`fence::KAFKA_TXN_LAST_UPDATED`]), and it never writes the takeover marker: in kdi's era
//! it acknowledges only the partitions it names, and the takeover check still runs for the rest
//! at the next open.

use std::collections::BTreeMap;

use deltalake::arrow::array::RecordBatch;
use deltalake::delta_datafusion::DataFusionMixins as _;

use crate::config::ResolvedPipeline;
use crate::error::{Error, Result};
use crate::fence::{self, Era};
use crate::sink::{KafkaOffsets, Sink};

use super::tools::{self, PartitionStatus};

/// A commit another writer beat this one to is read again and tried again this many times.
const ATTEMPTS: usize = 5;

/// What the operator asked for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Request {
    /// Partition → the next offset to read.
    pub moves: BTreeMap<i32, i64>,
    /// Write every partition's offset again at its own stored value, with ddi's stamp
    /// (`--reassert`): neither forward nor back, and it needs no guard.
    pub reassert: bool,
    /// `--acknowledge-skip "<reason>"`: the reason an operator gives for skipping offsets that
    /// are still in the topic, recorded as `ddi.kafka.offsetOverride`.
    pub acknowledge_skip: Option<String>,
    /// `--allow-replay`: a move backwards, which reads offsets again.
    pub allow_replay: bool,
}

/// What a request comes to, once it has passed every guard.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    /// The `txn` to write for each partition: the last offset accounted for.
    pub assert: BTreeMap<i32, i64>,
    pub override_reason: Option<String>,
    /// Whether some partition moves backwards, which needs a checkpoint of ddi's own right
    /// after (§4.8, "A move backwards outlives Trino's checkpoints").
    pub backward: bool,
}

/// Apply every guard of §4.8 to `request`, given where each partition is. Pure.
pub fn decide(partitions: &[PartitionStatus], era: Era, request: &Request) -> Result<Decision> {
    let refuse = |why: String| Err(Error::Other(format!("set-offsets: {why}")));
    if era == Era::Fresh {
        return refuse(
            "this app_id has no offsets in the target (a fresh one): start it with \
             `starting_offsets` in the config instead. A partition's txn without the takeover \
             marker would make the era read as kafka-delta-ingest's"
                .into(),
        );
    }
    let by_partition: BTreeMap<i32, &PartitionStatus> =
        partitions.iter().map(|p| (p.partition, p)).collect();

    if request.reassert {
        if !request.moves.is_empty() {
            return refuse("--reassert takes no --partition/--next/--all-partitions".into());
        }
        let assert: BTreeMap<i32, i64> = partitions
            .iter()
            .filter_map(|p| p.stored.map(|s| (p.partition, s)))
            .collect();
        if assert.is_empty() {
            return refuse("no partition has an offset to reassert".into());
        }
        return Ok(Decision {
            assert,
            override_reason: None,
            backward: false,
        });
    }

    if request.moves.is_empty() {
        return refuse(
            "name a partition with --partition P --next O, or use --all-partitions".into(),
        );
    }
    let mut assert = BTreeMap::new();
    let mut skips = false;
    let mut backward = false;
    for (&p, &next) in &request.moves {
        let Some(s) = by_partition.get(&p) else {
            return refuse(format!("the topic has no partition {p}"));
        };
        if next < 0 {
            return refuse(format!("partition {p}: --next {next} is negative"));
        }
        if next == 0 {
            if s.stored.is_some() {
                return refuse(format!(
                    "partition {p} has an offset ({:?}); --next 0 would have to remove it, which \
                     a txn cannot. Use a new app_id with `starting_offsets` to read from 0",
                    s.stored
                ));
            }
            continue;
        }
        if next > s.high {
            return refuse(format!(
                "partition {p}: --next {next} is past the end of the topic ({})",
                s.high
            ));
        }
        let current_next = s.stored.map_or(0, |v| v + 1);
        match next.cmp(&current_next) {
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => {
                // Skipping what the topic has trimmed loses nothing that can still be read;
                // skipping what it holds is a decision somebody has to own.
                if current_next.max(s.low) < next {
                    skips = true;
                    if request.acknowledge_skip.is_none() {
                        return refuse(format!(
                            "partition {p}: --next {next} skips offsets {}..{} that the topic \
                             still holds. Say why with --acknowledge-skip \"<reason>\"",
                            current_next.max(s.low),
                            next
                        ));
                    }
                } else if request.acknowledge_skip.is_some() {
                    skips = true;
                }
            }
            std::cmp::Ordering::Less => {
                if !request.allow_replay {
                    return refuse(format!(
                        "partition {p}: --next {next} is behind its next offset ({current_next}) \
                         and would read {next}..{current_next} again. Pass --allow-replay if \
                         that is what you want (the rows are written twice)"
                    ));
                }
                if next < s.low {
                    return refuse(format!(
                        "partition {p}: --next {next} is below the topic's first offset ({}): \
                         those messages are gone",
                        s.low
                    ));
                }
                backward = true;
            }
        }
        assert.insert(p, next - 1);
    }
    if assert.is_empty() {
        return refuse("nothing to write: every named partition is at --next 0 already".into());
    }
    Ok(Decision {
        assert,
        override_reason: if skips {
            request.acknowledge_skip.clone()
        } else {
            None
        },
        backward,
    })
}

/// What was done.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    pub version: i64,
    /// Partition → (stored before, stored after).
    pub moved: BTreeMap<i32, (Option<i64>, i64)>,
    /// Whether ddi wrote a checkpoint at the commit (a move backwards).
    pub checkpointed: bool,
}

/// Apply `request` to `cfg`'s target. Reads the topic and the target afresh on each attempt,
/// so a concurrent owner's commit makes it decide again rather than write a stale value.
pub async fn set_offsets(cfg: &ResolvedPipeline, request: &Request) -> Result<Outcome> {
    let k = cfg.kafka.as_ref().ok_or_else(|| {
        Error::Other("internal: set-offsets on a pipeline that is not Kafka".into())
    })?;
    let mut last = None;
    for attempt in 0..ATTEMPTS {
        let view = tools::view(cfg).await?;
        let decision = decide(&view.partitions, view.era, request)?;
        let schema = view
            .target
            .snapshot()
            .map_err(Error::Delta)?
            .snapshot()
            .read_schema();
        let offsets = KafkaOffsets {
            topic: k.topic.clone(),
            group_id: "set-offsets".into(),
            ranges: BTreeMap::new(),
            assert: decision.assert.clone(),
            marks: BTreeMap::new(),
            cleanup_logs: false,
            baseline: false,
            override_reason: decision.override_reason.clone(),
            takeover_marker: false,
        };
        let sink = Sink::new(&cfg.app_id, cfg.target_file_size);
        match sink
            .commit_kafka(view.target, vec![RecordBatch::new_empty(schema)], &offsets)
            .await
        {
            Ok(table) => {
                let version = table.version().map_or(-1, |v| v as i64);
                let mut checkpointed = false;
                if decision.backward {
                    deltalake::checkpoints::create_checkpoint(&table, None)
                        .await
                        .map_err(Error::Delta)?;
                    checkpointed = true;
                    verify_after_checkpoint(cfg, &decision.assert).await?;
                }
                let moved = decision
                    .assert
                    .iter()
                    .map(|(p, after)| {
                        let before = view
                            .partitions
                            .iter()
                            .find(|x| x.partition == *p)
                            .and_then(|x| x.stored);
                        (*p, (before, *after))
                    })
                    .collect();
                return Ok(Outcome {
                    version,
                    moved,
                    checkpointed,
                });
            }
            Err(e) if is_conflict(&e) && attempt + 1 < ATTEMPTS => {
                last = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| Error::Other("set-offsets: no attempt was made".into())))
}

fn is_conflict(e: &Error) -> bool {
    use deltalake::kernel::transaction::TransactionError;
    matches!(
        e,
        Error::Delta(deltalake::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(_) | TransactionError::VersionAlreadyExists(_),
        })
    )
}

/// After a move backwards and the checkpoint at its commit: read each moved partition at the
/// new head, and refuse, naming them, if any answers another value. A Trino checkpoint written
/// from a base below the commit while ours was being written keeps the higher, older offset
/// (§4.8); the operator runs the command again.
async fn verify_after_checkpoint(
    cfg: &ResolvedPipeline,
    wanted: &BTreeMap<i32, i64>,
) -> Result<()> {
    let table = cfg.storage.open(&cfg.target_uri).await?;
    let ids: Vec<String> = wanted
        .keys()
        .map(|p| fence::txn_app_id(&cfg.app_id, *p))
        .collect();
    let read = fence::read_txns(&table, &ids, &fence::LookupCounters::default()).await?;
    let wrong: Vec<String> = wanted
        .iter()
        .filter_map(|(p, v)| {
            let got = read
                .get(&fence::txn_app_id(&cfg.app_id, *p))
                .and_then(|t| t.version);
            (got != Some(*v)).then(|| format!("partition {p} reads {got:?}, not {v}"))
        })
        .collect();
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(Error::Other(format!(
            "set-offsets: the move did not hold after its checkpoint ({}); another writer's \
             checkpoint raced it. Stop every writer of the table (Trino included) and run it again",
            wrong.join("; ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(partition: i32, stored: Option<i64>, low: i64, high: i64) -> PartitionStatus {
        PartitionStatus {
            partition,
            stored,
            low,
            high,
        }
    }

    fn mv(entries: &[(i32, i64)]) -> Request {
        Request {
            moves: entries.iter().copied().collect(),
            ..Request::default()
        }
    }

    fn why(r: Result<Decision>) -> String {
        r.unwrap_err().to_string()
    }

    #[test]
    fn a_fresh_app_id_is_refused() {
        let e = why(decide(&[p(0, None, 0, 10)], Era::Fresh, &mv(&[(0, 5)])));
        assert!(e.contains("starting_offsets"), "{e}");
    }

    #[test]
    fn forward_over_retained_offsets_needs_an_acknowledgment() {
        let parts = [p(0, Some(9), 0, 100)];
        let e = why(decide(&parts, Era::Ddi, &mv(&[(0, 50)])));
        assert!(
            e.contains("--acknowledge-skip") && e.contains("10..50"),
            "{e}"
        );

        let mut r = mv(&[(0, 50)]);
        r.acknowledge_skip = Some("poison pill".into());
        let d = decide(&parts, Era::Ddi, &r).unwrap();
        assert_eq!(d.assert, BTreeMap::from([(0, 49)]));
        assert_eq!(d.override_reason.as_deref(), Some("poison pill"));
        assert!(!d.backward);
    }

    #[test]
    fn forward_over_trimmed_offsets_only_is_free() {
        // Stored 9, the topic starts at 30: moving to 30 skips only what is gone.
        let parts = [p(0, Some(9), 30, 100)];
        let d = decide(&parts, Era::Ddi, &mv(&[(0, 30)])).unwrap();
        assert_eq!(d.assert, BTreeMap::from([(0, 29)]));
        assert_eq!(d.override_reason, None);
    }

    #[test]
    fn backward_needs_allow_replay_and_the_offsets_to_exist() {
        let parts = [p(0, Some(99), 20, 200)];
        let e = why(decide(&parts, Era::Ddi, &mv(&[(0, 50)])));
        assert!(e.contains("--allow-replay"), "{e}");

        let mut r = mv(&[(0, 50)]);
        r.allow_replay = true;
        let d = decide(&parts, Era::Ddi, &r).unwrap();
        assert_eq!(d.assert, BTreeMap::from([(0, 49)]));
        assert!(d.backward);

        let mut gone = mv(&[(0, 10)]);
        gone.allow_replay = true;
        assert!(why(decide(&parts, Era::Ddi, &gone)).contains("gone"));
    }

    #[test]
    fn at_the_next_offset_it_is_a_reassert_and_needs_no_guard() {
        let parts = [p(0, Some(9), 0, 100)];
        let d = decide(&parts, Era::Kdi, &mv(&[(0, 10)])).unwrap();
        assert_eq!(d.assert, BTreeMap::from([(0, 9)]));
        assert!(!d.backward && d.override_reason.is_none());
    }

    #[test]
    fn the_reassert_writes_each_partition_at_its_own_offset() {
        let parts = [
            p(0, Some(9), 0, 100),
            p(1, Some(41), 0, 100),
            p(2, None, 0, 5),
        ];
        let r = Request {
            reassert: true,
            ..Request::default()
        };
        let d = decide(&parts, Era::Kdi, &r).unwrap();
        assert_eq!(d.assert, BTreeMap::from([(0, 9), (1, 41)]));
        assert!(!d.backward);
        // It takes no move beside it.
        let mut both = mv(&[(0, 3)]);
        both.reassert = true;
        assert!(why(decide(&parts, Era::Kdi, &both)).contains("--reassert"));
    }

    #[test]
    fn zero_past_the_end_and_unknown_partitions_are_refused() {
        let parts = [p(0, Some(9), 0, 100), p(1, None, 0, 100)];
        assert!(why(decide(&parts, Era::Ddi, &mv(&[(0, 0)]))).contains("new app_id"));
        assert!(why(decide(&parts, Era::Ddi, &mv(&[(0, 101)]))).contains("past the end"));
        assert!(why(decide(&parts, Era::Ddi, &mv(&[(7, 1)]))).contains("no partition 7"));
        // Nothing to remove on a partition with no offset: --next 0 alone writes nothing.
        assert!(why(decide(&parts, Era::Ddi, &mv(&[(1, 0)]))).contains("nothing to write"));
    }

    #[test]
    fn a_partition_with_no_offset_moves_forward_from_zero() {
        let parts = [p(0, Some(9), 0, 100), p(1, None, 0, 100)];
        let mut r = mv(&[(1, 40)]);
        r.acknowledge_skip = Some("start here".into());
        let d = decide(&parts, Era::Kdi, &r).unwrap();
        assert_eq!(d.assert, BTreeMap::from([(1, 39)]));
    }
}
