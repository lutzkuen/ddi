//! Skipping rows a rebuild already wrote, without the rebuilder's cooperation.
//!
//! # Why a timestamp
//!
//! A batch rebuild reads the source at some snapshot and commits its output later. Rows
//! that arrive in between are in neither: not in the rebuild's snapshot, and — once the
//! rebuild overwrites the target — no longer in the target either, even though `ddi` may
//! have streamed them. That window is the whole problem.
//!
//! A timestamp that increases with arrival order closes it, because the question "did the
//! rebuild already cover this row?" becomes a property of the row rather than of the
//! schedule:
//!
//! ```text
//! 00:00  batch reads source@100, whose newest row is _timestamp = T
//! 00:03  rows T+1, T+2 arrive; ddi streams them
//! 00:05  batch OVERWRITES the target with everything up to T
//! 00:06  ddi reads max(_timestamp) = T from the target, and emits T+1, T+2
//! ```
//!
//! No handshake, no hooks, no shared state. The answer was already in the data.
//!
//! # Why a key as well
//!
//! `> T` alone is wrong at the boundary. Rows sharing exactly `T` may be split between
//! "the rebuild saw it" and "it arrived a moment later" — `>` drops the latter, `>=`
//! duplicates the former. Neither is acceptable, so rows *at* `T` are resolved
//! individually against the keys the target already holds at `T`. That set is small by
//! construction: only the rows sharing the maximum timestamp.
//!
//! With no key configured, ties fall back to `>`, which can drop a row that arrived in
//! the same instant as the rebuild's newest. Fine for a strictly increasing sequence,
//! not for a second-granularity clock under load.
//!
//! # When it applies
//!
//! Only while the pipeline has to infer from the target's data what the target already
//! holds — a *coverage window*. One opens after a rebuild by another writer, on a first start
//! against a target that already has rows, after the source was dropped and recreated or
//! relocated, and on a reopen part-way through any of those. An ordinary restart opens none:
//! the `txn` offset commits atomically with the rows it describes, so it is exact on its own,
//! and a timestamp filter on top of it can only drop rows. Applying one on every open is what
//! used to lose a lagging Kafka partition's late rows at every restart.
//!
//! A window closes after the first batch that carries a row newer than the watermark.
//! Whatever filled the target read a prefix of this source through the same model, so every
//! row it read is at or below the watermark and a newer one lies past its read point: from
//! there an older timestamp is a late row, not a covered one. The window of a rebuild or a
//! first start also closes once the source head it opened against has been read, because a
//! rebuild that read no further cannot have covered anything committed after it. That head is
//! loaded before the target, so a rebuild that commits between the two loads can have read a
//! little past it, and what it read past the head is written again: the head errs towards
//! duplicates, never towards a gap. A replaced source's window does not close at a head —
//! its new table is usually re-seeded *after* the pipeline reopens on it — and neither does
//! one on the apply half of a staged upsert, whose stage is filled at its own pace. While a window stays open each commit
//! records it (see [`RecordedCutoff`]), so a reopen carries on where the last commit left off.
//!
//! Inside a window the ordering requirement still holds, and a source that breaks it is
//! inexact there. A table written from a multi-partition Kafka topic is append-only and still
//! breaks it, because Kafka orders timestamps only within a partition: a late row at or below
//! the watermark is dropped with the covered ones (and counted), and [`bounded_rescan_start`]
//! can start past it. Outside a window the order does not matter at all. `watermark_uri` does
//! not help such a source: a pipeline with a timestamp never reads it, so a rebuild's handover
//! is always this rescan. A watermark per value of a partition column — the newest timestamp or
//! offset per Kafka partition — would be exact there; it is a possible future option, not
//! something `ddi` does today.

use std::collections::{HashMap, HashSet};
use std::fmt;

use deltalake::arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use deltalake::arrow::compute::kernels::boolean::{and, or};
use deltalake::arrow::compute::kernels::cmp::{eq, gt};
use deltalake::arrow::compute::{cast, filter_record_batch};
use deltalake::arrow::datatypes::{DataType, Schema};
use deltalake::DeltaTable;
use futures::TryStreamExt;
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::source::Version;
use crate::stats::{bound_of_scalar, bound_of_stat, Bound, Slack};

/// The default timestamp column, matching the convention this tool assumes tables follow.
pub const DEFAULT_TIMESTAMP_COLUMN: &str = "_timestamp";

/// What the target already contains, expressed as a cut-off.
#[derive(Debug, Clone, Default)]
pub struct Dedup {
    timestamp_column: String,
    key_column: Option<String>,
    /// `max(timestamp)` in the target, as a one-element array. `None` when the target is
    /// empty, which means nothing has been covered and everything passes.
    watermark: Option<ArrayRef>,
    /// Keys already present *at* the watermark instant, stringified for comparability.
    boundary_keys: HashSet<String>,
}

impl Dedup {
    /// Read the cut-off out of the target table.
    ///
    /// One streaming pass over two of the target's columns, once per open that has to infer
    /// coverage — not per batch, not on an ordinary restart, and never the whole table.
    ///
    /// # What this is careful about, and why
    ///
    /// The question is small — the highest timestamp, and the keys tied at it — but the
    /// table it is asked of is not. An earlier version answered it by collecting the whole
    /// target into a `MemTable` and running `SELECT max(...)` over that, which read *every*
    /// column of every row into memory. On a silver table whose rows carry JSON blobs
    /// (product descriptions, line-item arrays, images) that is gigabytes to compute one
    /// scalar, it was paid again on every restart, and it grows with the table — so a
    /// pipeline that had been fine gets slower until it cannot start at all, and the
    /// crash-loop makes it worse rather than better.
    ///
    /// Two things fix that, and both matter:
    ///
    /// - **Only the two columns involved are read.** The projection is pushed into the Delta
    ///   scan, so the parquet reader never decodes the rest.
    /// - **The pass is streaming.** The running answer is a single timestamp and the keys
    ///   tied with it, so memory is bounded by the size of *that* — small by construction —
    ///   rather than by the size of the table. A batch that cannot beat the running maximum
    ///   is dropped as soon as it has been looked at.
    pub async fn read(
        target: &DeltaTable,
        timestamp_column: &str,
        key_column: Option<&str>,
    ) -> Result<Self> {
        use deltalake::arrow::row::{OwnedRow, RowConverter, SortField};
        use std::cmp::Ordering;

        let mut out = Self {
            timestamp_column: timestamp_column.to_string(),
            key_column: key_column.map(str::to_string),
            ..Default::default()
        };

        use deltalake::delta_datafusion::DataFusionMixins;
        let schema = target
            .snapshot()
            .map_err(Error::Delta)?
            .snapshot()
            .read_schema();
        require_columns(&schema, timestamp_column, key_column)?;

        let (_t, mut stream) = target
            .clone()
            .scan_table()
            .with_columns(projection(timestamp_column, key_column))
            // Bounded like everything else, so a target whose files are enormous throttles
            // rather than growing. The pass itself is already streaming; this covers what
            // the scan holds underneath it.
            .with_session_state(std::sync::Arc::new(crate::budget::session(target)?))
            .await
            .map_err(Error::Delta)?;

        // The running answer: the highest timestamp seen, the row encoding used to compare
        // against it, and the keys that share it.
        let mut best: Option<OwnedRow> = None;
        let mut converter: Option<RowConverter> = None;
        let mut rows_scanned = 0usize;

        while let Some(batch) = stream.try_next().await.map_err(|e| {
            crate::spill::classify(e, "dedup: cannot scan the target to read its watermark")
        })? {
            if batch.num_rows() == 0 {
                continue;
            }
            rows_scanned += batch.num_rows();

            // As the table declares its columns, not as whichever engine wrote each file
            // happened to type them. delta-rs's scan already projects onto the table schema,
            // so today this changes nothing and costs nothing — it is here to make that a
            // property of this function rather than of a pinned dependency's internals. What
            // it guards is severe: a watermark read at millisecond precision from a
            // Trino-compacted file would sit a thousandfold below every microsecond row, so
            // nothing would ever be suppressed. See [`crate::schema::read_as_declared`].
            let batch = crate::schema::read_as_declared(batch, &schema)?;
            let ts = column(&batch, timestamp_column)?;
            // A byte-comparable encoding, so any Delta type orders correctly without a
            // match arm per type — the same device `upsert::collapse` uses.
            let conv = match &converter {
                Some(c) => c,
                None => {
                    converter = Some(
                        RowConverter::new(vec![SortField::new(ts.data_type().clone())]).map_err(
                            |e| {
                                Error::Schema(format!(
                                    "dedup timestamp {timestamp_column:?} cannot be ordered: {e}"
                                ))
                            },
                        )?,
                    );
                    converter.as_ref().expect("just set")
                }
            };
            let encoded = conv
                .convert_columns(std::slice::from_ref(&ts))
                .map_err(|e| Error::Other(format!("dedup: cannot order the target: {e}")))?;

            let keys = match key_column {
                Some(k) => {
                    let col = column(&batch, k)?;
                    Some(cast(&col, &DataType::Utf8).map_err(|e| {
                        Error::Config(format!("dedup key column {k:?} is not comparable: {e}"))
                    })?)
                }
                None => None,
            };

            for i in 0..batch.num_rows() {
                // `max` ignores nulls, as the SQL it replaces did.
                if ts.is_null(i) {
                    continue;
                }
                let row = encoded.row(i);
                let cmp = match &best {
                    Some(b) => row.cmp(&b.row()),
                    None => Ordering::Greater,
                };
                match cmp {
                    Ordering::Greater => {
                        best = Some(row.owned());
                        out.watermark = Some(ts.slice(i, 1));
                        // A new maximum makes every key gathered so far irrelevant.
                        out.boundary_keys.clear();
                    }
                    Ordering::Equal => {}
                    Ordering::Less => continue,
                }
                if let Some(keys) = &keys {
                    let keys = keys.as_any().downcast_ref::<StringArray>().expect("utf8");
                    if !keys.is_null(i) {
                        out.boundary_keys.insert(keys.value(i).to_string());
                    }
                }
            }
        }

        debug!(
            timestamp_column,
            key_column,
            rows_scanned,
            boundary_keys = out.boundary_keys.len(),
            watermark_known = out.watermark.is_some(),
            "read the target's watermark"
        );
        Ok(out)
    }

    /// True when there is nothing to suppress, so `apply` can be skipped entirely.
    pub fn is_inert(&self) -> bool {
        self.watermark.is_none()
    }

    pub fn watermark_is_known(&self) -> bool {
        self.watermark.is_some()
    }

    /// The cut-off itself, for bounding how far back a rescan must reach.
    pub fn watermark(&self) -> Option<&ArrayRef> {
        self.watermark.as_ref()
    }

    pub fn boundary_key_count(&self) -> usize {
        self.boundary_keys.len()
    }

    /// Drop the rows the target already holds.
    pub fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.filter(batch).map(|f| f.kept)
    }

    /// Drop the rows the target already holds, and say how many went and how many of those
    /// kept are past the watermark altogether.
    pub fn filter(&self, batch: RecordBatch) -> Result<Filtered> {
        let Some(mark) = &self.watermark else {
            // Nothing is covered by a target with no rows, so everything is past it.
            let beyond = batch.num_rows();
            return Ok(Filtered {
                kept: batch,
                covered: 0,
                beyond,
            });
        };

        let ts = require_timestamps(&batch, &self.timestamp_column)?;
        let scalar = deltalake::arrow::array::Scalar::new(mark.clone());

        let after = gt(&ts, &scalar).map_err(|e| {
            Error::Schema(format!(
                "dedup timestamp {:?}: cannot compare against the target's newest value; \
                 source and target must agree on its type ({e})",
                self.timestamp_column
            ))
        })?;
        // Rows at the watermark are not counted, even with a key that is new: they share the
        // instant of the newest row the target holds, so they say nothing about whether
        // whatever filled it read past them.
        let beyond = after.true_count();

        let keep = match &self.key_column {
            // Rows exactly at the watermark are decided one by one.
            Some(key) => {
                let at = eq(&ts, &scalar).map_err(|e| Error::Schema(format!("dedup: {e}")))?;
                let unseen = self.keys_not_yet_present(&batch, key)?;
                or(
                    &after,
                    &and(&at, &unseen).map_err(|e| Error::Other(e.to_string()))?,
                )
                .map_err(|e| Error::Other(e.to_string()))?
            }
            None => after,
        };

        let kept = filter_record_batch(&batch, &keep)
            .map_err(|e| Error::Other(format!("dedup: filter failed: {e}")))?;
        let covered = batch.num_rows() - kept.num_rows();
        Ok(Filtered {
            kept,
            covered,
            beyond,
        })
    }

    fn keys_not_yet_present(&self, batch: &RecordBatch, key: &str) -> Result<BooleanArray> {
        let col = column(batch, key)?;
        let text = cast(&col, &DataType::Utf8)
            .map_err(|e| Error::Schema(format!("dedup key {key:?} is not comparable: {e}")))?;
        let text = text.as_any().downcast_ref::<StringArray>().expect("utf8");
        Ok((0..text.len())
            .map(|i| Some(text.is_null(i) || !self.boundary_keys.contains(text.value(i))))
            .collect())
    }
}

/// What [`Dedup::filter`] made of one batch.
#[derive(Debug)]
pub struct Filtered {
    /// The rows the target does not hold yet.
    pub kept: RecordBatch,
    /// Rows dropped because the target already holds them.
    pub covered: usize,
    /// Of `kept`, the rows strictly newer than the watermark: past anything whatever filled
    /// the target can have read. The first of these is what ends a coverage window.
    pub beyond: usize,
}

/// Fail unless the target has the columns the cut-off is read from.
///
/// Checked on every open that names a `dedup_timestamp`, not only on one that reads the
/// cut-off: a column that is not there is a configuration mistake whichever state the target
/// is in, and an ordinary restart is where it would otherwise go unnoticed until the next
/// rebuild needed it. Here rather than left to the scan, so the message still names what the
/// table does have.
pub fn require_columns(
    schema: &Schema,
    timestamp_column: &str,
    key_column: Option<&str>,
) -> Result<()> {
    for wanted in [Some(timestamp_column), key_column].into_iter().flatten() {
        if schema.index_of(wanted).is_err() {
            return Err(Error::Config(format!(
                "dedup column {wanted:?} is not in the target table. Columns: [{}]",
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }
    Ok(())
}

/// The batch's `timestamp_column`, provided every row has one.
///
/// A row with no timestamp can be neither kept nor skipped safely by the cut-off, and outside
/// a coverage window it would be written with nothing to recognise it by the next time the
/// target's coverage has to be inferred — so an appending pipeline refuses it on every batch,
/// window or not. An upserting one leaves it to `upsert::collapse`, whose message is about
/// the merge it cannot order.
pub fn require_timestamps(batch: &RecordBatch, timestamp_column: &str) -> Result<ArrayRef> {
    let ts = column(batch, timestamp_column)?;
    if ts.null_count() > 0 {
        return Err(Error::Schema(format!(
            "dedup timestamp {:?} contains {} null(s); a row with no timestamp can be \
             neither kept nor skipped safely",
            timestamp_column,
            ts.null_count()
        )));
    }
    Ok(ts)
}

// ---------------------------------------------------------------- coverage windows

/// Why a pipeline had to infer from the target's data what the target already holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageReason {
    /// Another writer rewrote the target since this pipeline's last commit.
    Rebuilt,
    /// The pipeline has never committed, and the target already has rows.
    Bootstrap,
    /// The source is not the table this pipeline last read: dropped and recreated, or moved.
    SourceReplaced,
}

impl CoverageReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rebuilt => "rebuilt",
            Self::Bootstrap => "bootstrap",
            Self::SourceReplaced => "source_replaced",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "rebuilt" => Some(Self::Rebuilt),
            "bootstrap" => Some(Self::Bootstrap),
            "source_replaced" => Some(Self::SourceReplaced),
            _ => None,
        }
    }
}

impl fmt::Display for CoverageReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `commitInfo` key naming why a coverage window is open.
pub const CUTOFF_REASON_KEY: &str = "ddi.cutoff.reason";
/// `commitInfo` key naming the source version the window closes after, when it has one.
pub const CUTOFF_SOURCE_THROUGH_KEY: &str = "ddi.cutoff.sourceThrough";

/// A coverage window, as a commit made inside it records it.
///
/// Recorded because a reopen cannot work it out again. Once this pipeline has committed, the
/// target's log says the newest commit is its own, which reads exactly like an ordinary
/// restart — so without this a restart part-way through a rebuild's rescan would stop
/// filtering and re-emit the rest of what the rebuild wrote. Only the reason and the head are
/// kept: the watermark itself is read from the target again, and is the same one, because a
/// commit made inside an open window holds only rows exactly at the watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedCutoff {
    pub reason: CoverageReason,
    /// The source version the window closes after once read. `None` when only a newer row
    /// closes it.
    pub source_through: Option<Version>,
}

impl RecordedCutoff {
    pub fn to_commit_metadata(&self) -> Vec<(String, serde_json::Value)> {
        let mut out = vec![(
            CUTOFF_REASON_KEY.to_string(),
            serde_json::Value::from(self.reason.as_str()),
        )];
        if let Some(through) = self.source_through {
            out.push((
                CUTOFF_SOURCE_THROUGH_KEY.to_string(),
                serde_json::Value::from(through),
            ));
        }
        out
    }

    /// The window a commit recorded, if it recorded one.
    ///
    /// One this build cannot read is taken as no window, with a warning. That errs towards
    /// re-emitting rows the target holds rather than dropping rows it does not.
    pub fn from_commit_info(info: &HashMap<String, serde_json::Value>) -> Option<Self> {
        let raw = info.get(CUTOFF_REASON_KEY)?;
        let Some(reason) = raw.as_str().and_then(CoverageReason::parse) else {
            warn!(
                key = CUTOFF_REASON_KEY,
                value = %raw,
                "a commit of ours records a coverage window this build does not know; \
                 resuming as though it had closed"
            );
            return None;
        };
        let source_through = match info.get(CUTOFF_SOURCE_THROUGH_KEY) {
            None => None,
            Some(v) => match v.as_u64() {
                Some(through) => Some(through),
                None => {
                    warn!(
                        key = CUTOFF_SOURCE_THROUGH_KEY,
                        value = %v,
                        "a commit of ours records a coverage window whose source version is \
                         not a version; resuming as though it had closed"
                    );
                    return None;
                }
            },
        };
        Some(Self {
            reason,
            source_through,
        })
    }
}

/// The only columns this needs to read.
///
/// Its own function so the one property that matters can be asserted directly: everything
/// else in the target — the JSON payloads, the descriptions, the image blobs — is never
/// decoded. Reading them to compute one `max()` is what used to make a restart cost
/// gigabytes and grow with the table.
///
/// Deduplicated, because the two are allowed to be the same column — a monotonic id is both
/// a sequence and an identity — and a projection naming it twice is rejected by the scan.
fn projection<'a>(timestamp_column: &'a str, key_column: Option<&'a str>) -> Vec<&'a str> {
    let mut wanted = vec![timestamp_column];
    if let Some(k) = key_column.filter(|k| *k != timestamp_column) {
        wanted.push(k);
    }
    wanted
}

fn column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        Error::Schema(format!(
            "dedup column {name:?} is not in the transformed batch. Columns: [{}]",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;
    Ok(batch.column(idx).clone())
}

// ---------------------------------------------------------------- bounded rescan

/// The earliest source version that can still hold rows beyond `watermark`.
///
/// After a rebuild, `ddi` has to re-read the source far enough back to recover whatever
/// the rebuild wiped — but it does not know how far back that is. Reading from
/// `starting_version` is always correct and always wasteful: on a large bronze table it
/// re-reads history every night.
///
/// Delta records per-file `maxValues`, so the log can answer it directly. Walking
/// backwards from the head, the first commit whose newest row is already covered by the
/// watermark is the boundary: everything before it is covered too, because the timestamp
/// increases with arrival — which is the same assumption the filter already rests on, and
/// fails the same way. On a source that breaks it, an earlier commit can still hold a row
/// newer than the watermark, and the rescan starts past it.
///
/// Anything unexpected — statistics missing, a type that will not line up, a log that
/// runs out — returns `fallback`. Being slow is a cost; being wrong is not an option.
pub async fn bounded_rescan_start(
    source: &DeltaTable,
    timestamp_column: &str,
    watermark: &ArrayRef,
    fallback: Version,
    max_scan: u64,
) -> Result<Version> {
    use deltalake::kernel::Action;
    use deltalake::logstore::get_actions;

    let Some(mark) = bound_of_scalar(watermark) else {
        return Ok(fallback);
    };
    // A commit counts as covered only when even the highest value its statistic could stand
    // for sits below the lowest value the watermark could. A DECIMAL is compared as a double,
    // and two decimals can share one, so `<=` on the doubles alone would take a newer commit
    // for one already written. A timestamp maximum may have been truncated to the
    // millisecond, as Spark writes it, and stand for a row just past the watermark. Re-reading
    // the boundary commit costs nothing: the dedup filter drops what the target already
    // holds. See `crate::stats`.
    let slack = Slack::of(watermark.data_type());
    let covered_to = slack.map_or_else(|| mark.clone(), |s| s.below(&mark));
    let Some(head) = source.version() else {
        return Ok(fallback);
    };
    let log = source.log_store();

    let mut v = head;
    let mut scanned = 0u64;
    loop {
        if scanned >= max_scan || v < fallback {
            return Ok(fallback);
        }
        let Some(raw) = log.read_commit_entry(v).await? else {
            return Ok(fallback); // log truncated under us
        };
        let actions = get_actions(v, &raw)?;

        // Highest value this commit added. A commit that added nothing (compaction, a
        // txn marker) says nothing about coverage, so keep walking.
        let mut commit_max: Option<Bound> = None;
        let mut saw_add = false;
        for a in &actions {
            let Action::Add(add) = a else { continue };
            if !add.data_change {
                continue;
            }
            saw_add = true;
            let Some(stats) = add.stats.as_deref() else {
                return Ok(fallback); // no statistics: cannot reason, so re-read it all
            };
            let parsed: serde_json::Value = match serde_json::from_str(stats) {
                Ok(p) => p,
                Err(_) => return Ok(fallback),
            };
            let Some(stat) = parsed
                .get("maxValues")
                .and_then(|m| m.get(timestamp_column))
            else {
                return Ok(fallback);
            };
            let Some(b) = bound_of_stat(stat, &mark) else {
                return Ok(fallback);
            };
            let b = match slack {
                Some(s) => s.above(&b),
                None => b,
            };
            commit_max = Some(match commit_max {
                Some(cur) if cur > b => cur,
                _ => b,
            });
        }

        if saw_add {
            if let Some(cmax) = commit_max {
                // Everything this commit added is already in the target, so everything
                // before it is too.
                if cmax <= covered_to {
                    return Ok(v.saturating_add(1));
                }
            }
        }

        if v == 0 {
            return Ok(fallback);
        }
        v -= 1;
        scanned += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int64Array, TimestampMicrosecondArray};
    use deltalake::arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]))
    }

    fn batch(rows: &[(i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn mark(ts: i64) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(vec![ts])) as ArrayRef
    }

    fn ids(b: &RecordBatch) -> Vec<i64> {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        (0..a.len()).map(|i| a.value(i)).collect()
    }

    #[test]
    fn only_the_timestamp_and_the_key_are_ever_read() {
        // The regression this guards is a performance one, which is why it is asserted on
        // the projection rather than on a timing: a target whose rows carry JSON blobs cost
        // gigabytes per restart when every column was collected to compute one max().
        assert_eq!(
            projection("_timestamp", Some("order_id")),
            ["_timestamp", "order_id"]
        );
        assert_eq!(projection("_timestamp", None), ["_timestamp"]);
    }

    #[test]
    fn a_column_that_is_both_sequence_and_key_is_named_once() {
        // A monotonic id is a legitimate choice for both, and a projection naming it twice
        // is rejected by the scan — which is how this first showed up.
        assert_eq!(projection("id", Some("id")), ["id"]);
    }

    #[test]
    fn an_unknown_watermark_passes_everything() {
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            ..Default::default()
        };
        assert!(d.is_inert());
        assert_eq!(
            ids(&d.apply(batch(&[(1, 10), (2, 20)])).unwrap()),
            vec![1, 2]
        );
    }

    #[test]
    fn rows_older_than_the_watermark_are_dropped() {
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            watermark: Some(mark(20)),
            ..Default::default()
        };
        let out = d
            .apply(batch(&[(1, 10), (2, 20), (3, 30), (4, 40)]))
            .unwrap();
        assert_eq!(
            ids(&out),
            vec![3, 4],
            "20 is at the watermark, so it is covered"
        );
    }

    #[test]
    fn a_row_at_the_watermark_with_an_unseen_key_is_kept() {
        // The boundary case a bare `>` would silently drop: it arrived in the same
        // instant as the rebuild's newest row, but the rebuild never saw it.
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            key_column: Some("order_id".into()),
            watermark: Some(mark(20)),
            boundary_keys: ["2".to_string()].into_iter().collect(),
        };
        let out = d
            .apply(batch(&[(1, 10), (2, 20), (9, 20), (3, 30)]))
            .unwrap();
        assert_eq!(
            ids(&out),
            vec![9, 3],
            "2 is already in the target at t=20; 9 shares the instant but is new"
        );
    }

    #[test]
    fn filter_counts_covered_and_beyond_separately() {
        // `beyond` is what closes a coverage window, so a tie with an unseen key must be kept
        // without counting: it shares the rebuild's newest instant and says nothing about
        // whether the rebuild read past it.
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            key_column: Some("order_id".into()),
            watermark: Some(mark(20)),
            boundary_keys: ["2".to_string()].into_iter().collect(),
        };
        let f = d
            .filter(batch(&[(1, 10), (2, 20), (9, 20), (3, 30)]))
            .unwrap();
        assert_eq!(ids(&f.kept), vec![9, 3]);
        assert_eq!(
            f.covered, 2,
            "1 is below the watermark and 2 is already there"
        );
        assert_eq!(f.beyond, 1, "only 3 is past the watermark");
    }

    #[test]
    fn a_recorded_cutoff_round_trips_through_commit_info() {
        let as_info = |c: &RecordedCutoff| -> HashMap<String, serde_json::Value> {
            c.to_commit_metadata().into_iter().collect()
        };
        for c in [
            RecordedCutoff {
                reason: CoverageReason::Rebuilt,
                source_through: Some(7),
            },
            RecordedCutoff {
                reason: CoverageReason::Bootstrap,
                source_through: Some(0),
            },
            RecordedCutoff {
                reason: CoverageReason::SourceReplaced,
                source_through: None,
            },
        ] {
            assert_eq!(RecordedCutoff::from_commit_info(&as_info(&c)), Some(c));
        }
        assert_eq!(
            RecordedCutoff::from_commit_info(&HashMap::new()),
            None,
            "a commit made outside a window records nothing"
        );

        // Anything unreadable is taken as no window, which errs towards duplicates.
        let mut unknown = HashMap::new();
        unknown.insert(CUTOFF_REASON_KEY.to_string(), serde_json::json!("backfill"));
        assert_eq!(RecordedCutoff::from_commit_info(&unknown), None);

        let mut not_a_version = as_info(&RecordedCutoff {
            reason: CoverageReason::Rebuilt,
            source_through: Some(7),
        });
        not_a_version.insert(
            CUTOFF_SOURCE_THROUGH_KEY.to_string(),
            serde_json::json!("seven"),
        );
        assert_eq!(RecordedCutoff::from_commit_info(&not_a_version), None);
        not_a_version.insert(CUTOFF_SOURCE_THROUGH_KEY.to_string(), serde_json::json!(-1));
        assert_eq!(RecordedCutoff::from_commit_info(&not_a_version), None);
    }

    #[test]
    fn without_a_key_ties_fall_back_to_strictly_greater() {
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            watermark: Some(mark(20)),
            ..Default::default()
        };
        let out = d.apply(batch(&[(2, 20), (9, 20), (3, 30)])).unwrap();
        assert_eq!(
            ids(&out),
            vec![3],
            "both rows at the boundary are assumed covered"
        );
    }

    #[test]
    fn a_null_timestamp_is_an_error_rather_than_a_guess() {
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            watermark: Some(mark(20)),
            ..Default::default()
        };
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![None as Option<i64>])) as ArrayRef,
            ],
        )
        .unwrap();
        assert!(d.apply(b).is_err());
    }

    #[test]
    fn a_missing_timestamp_column_names_what_is_available() {
        let d = Dedup {
            timestamp_column: "nope".into(),
            watermark: Some(mark(20)),
            ..Default::default()
        };
        let e = d.apply(batch(&[(1, 10)])).unwrap_err().to_string();
        assert!(e.contains("nope"), "got: {e}");
        assert!(e.contains("order_id"), "should list the real columns: {e}");
    }
}
