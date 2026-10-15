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
//! A window closes after the first batch that carries a row newer than the watermark — with a
//! cut-off per partition, newer than its own partition's, or in a partition the target holds
//! no row of. Whatever filled the target read a prefix of this source through the same model,
//! so every row it read is at or below the watermark and a newer one lies past its read point:
//! from there an older timestamp is a late row, not a covered one. The window of a rebuild or a
//! first start also closes once the source head it opened against has been read, because a
//! rebuild that read no further cannot have covered anything committed after it. That head is
//! loaded before the target, so a rebuild that commits between the two loads can have read a
//! little past it, and what it read past the head is written again: the head errs towards
//! duplicates, never towards a gap. A replaced source's window does not close at a head —
//! its new table is usually re-seeded *after* the pipeline reopens on it — and neither does
//! one on the apply half of a staged upsert, whose stage is filled at its own pace. While a
//! window stays open each commit records it (see [`RecordedCutoff`]), so a reopen carries on
//! where the last commit left off.
//!
//! Inside a window the ordering requirement still holds, and a source that breaks it is
//! inexact there. A table written from a multi-partition Kafka topic is append-only and still
//! breaks it, because Kafka orders timestamps only within a partition: a late row at or below
//! the watermark is dropped with the covered ones (and counted), and [`bounded_rescan_start`]
//! can start past it. Outside a window the order does not matter at all. `watermark_uri` does
//! not help such a source: a pipeline with a timestamp never reads it, so a rebuild's handover
//! is always this rescan.
//!
//! # Per partition
//!
//! What does help it is `dedup_partition`: the cut-off kept per value of the column the
//! timestamp is ordered within — `kafka_partition`, or `kafka_topic` and `kafka_partition`
//! together for a table fed from several topics. Each partition's mark is its newest value in
//! the target and the keys tied at it, a row is covered only by its own partition's mark, and
//! a partition the target holds no row of has nothing covered. The ordering requirement
//! relaxes to match: non-decreasing in arrival order within each partition, which
//! `kafka_offset` always is and a `LogAppendTime` timestamp is too; a `CreateTime` one is only
//! as monotonic as its producers' clocks. So `kafka_offset` may serve as `dedup_timestamp`
//! there — but an offset is a position in one topic on one cluster, and a topic recreated or
//! mirrored elsewhere starts again near zero, below every old mark.
//!
//! It is still one window, closed by the first row past *its* partition's mark, not one
//! window per partition: what filled the target read a prefix of the source, so such a row is
//! past that prefix and so is everything read after it, in every partition. More marks do make
//! an early close a little likelier — a DELETE on the target, or a whole partition aged out,
//! lowers one of them — and an early close is duplicates, never a gap. The rescan bound has its
//! own form, [`bounded_rescan_start_partitioned`].

use std::collections::{HashMap, HashSet};
use std::fmt;

use deltalake::arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, UInt32Array,
};
use deltalake::arrow::compute::kernels::boolean::{and, not, or};
use deltalake::arrow::compute::kernels::cmp::{eq, gt};
use deltalake::arrow::compute::{cast, filter_record_batch, take};
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
    /// The columns the cut-off is kept per value of. Empty for the global form above; set,
    /// `watermark` stays `None`, `boundary_keys` empty, and the cut-off is `partitions`.
    partition_columns: Vec<String>,
    /// One cut-off per partition value, in the partitioned form.
    partitions: PartitionMarks,
}

/// The partitioned form's cut-offs: per value of the partition columns, the newest
/// timestamp the target holds there and the keys tied at it.
#[derive(Debug, Clone, Default)]
struct PartitionMarks {
    /// Partition value, as text (see [`partition_key`]) → position in `marks` and `keys`.
    index: HashMap<String, u32>,
    /// One element per partition, typed as the target declares the timestamp. Built from
    /// the row encoding once the scan is done, so its buffers are its own and hold on to
    /// none of the batches the scan read.
    marks: Option<ArrayRef>,
    /// Per partition, the keys already present at its mark.
    keys: Vec<HashSet<String>>,
}

/// More distinct partition values than this in the target is a misconfiguration, not a
/// Kafka topic: the column is not one the timestamp is ordered within.
pub const MAX_DEDUP_PARTITIONS: usize = 100_000;

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
        Self::read_partitioned(target, timestamp_column, key_column, &[]).await
    }

    /// [`Dedup::read`], with one cut-off per value of `partition_columns` when there are any.
    ///
    /// Still one streaming pass, now over up to three columns, and still bounded by the size
    /// of the answer rather than of the table: per partition value, one timestamp and the
    /// keys tied with it. A target row with no partition value is passed over, as one with no
    /// timestamp is — it can claim coverage for no partition, which errs towards duplicates.
    pub async fn read_partitioned(
        target: &DeltaTable,
        timestamp_column: &str,
        key_column: Option<&str>,
        partition_columns: &[String],
    ) -> Result<Self> {
        Self::read_bounded(
            target,
            timestamp_column,
            key_column,
            partition_columns,
            MAX_DEDUP_PARTITIONS,
        )
        .await
    }

    /// [`Dedup::read_partitioned`] with the cardinality guard as a parameter, so a test can
    /// reach it without a hundred thousand partitions.
    #[doc(hidden)]
    pub async fn read_bounded(
        target: &DeltaTable,
        timestamp_column: &str,
        key_column: Option<&str>,
        partition_columns: &[String],
        max_partitions: usize,
    ) -> Result<Self> {
        use deltalake::arrow::row::{OwnedRow, RowConverter, SortField};
        use std::cmp::Ordering;

        let mut out = Self {
            timestamp_column: timestamp_column.to_string(),
            key_column: key_column.map(str::to_string),
            partition_columns: partition_columns.to_vec(),
            ..Default::default()
        };
        let partitioned = !partition_columns.is_empty();

        use deltalake::delta_datafusion::DataFusionMixins;
        let schema = target
            .snapshot()
            .map_err(Error::Delta)?
            .snapshot()
            .read_schema();
        require_columns(&schema, timestamp_column, key_column, partition_columns)?;

        let (_t, mut stream) = target
            .clone()
            .scan_table()
            .with_columns(projection(timestamp_column, key_column, partition_columns))
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
        // The same, per partition value, in the partitioned form.
        let mut running: HashMap<String, (OwnedRow, HashSet<String>)> = HashMap::new();
        let mut null_partition_rows = 0usize;

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
            let keys = keys
                .as_ref()
                .map(|k| k.as_any().downcast_ref::<StringArray>().expect("utf8"));
            let key_at = |i: usize| keys.filter(|k| !k.is_null(i)).map(|k| k.value(i));

            if partitioned {
                let parts = PartitionText::of(&batch, partition_columns)?;
                for i in 0..batch.num_rows() {
                    if ts.is_null(i) {
                        continue;
                    }
                    if parts.is_null(i) {
                        null_partition_rows += 1;
                        continue;
                    }
                    let row = encoded.row(i);
                    // Looked up by the text in hand: a new `String` only for a partition not
                    // seen before, never per row. With several columns that text is built once
                    // per distinct tuple in the batch; see `PartitionText::with`.
                    parts.with(i, |p| -> Result<()> {
                        match running.get_mut(p) {
                            Some((mark, tied)) => match row.cmp(&mark.row()) {
                                Ordering::Greater => {
                                    *mark = row.owned();
                                    tied.clear();
                                    tied.extend(key_at(i).map(str::to_string));
                                }
                                Ordering::Equal => tied.extend(key_at(i).map(str::to_string)),
                                Ordering::Less => {}
                            },
                            None => {
                                if running.len() >= max_partitions {
                                    return Err(too_many_partitions(
                                        partition_columns,
                                        max_partitions,
                                    ));
                                }
                                running.insert(
                                    p.to_string(),
                                    (
                                        row.owned(),
                                        key_at(i).map(str::to_string).into_iter().collect(),
                                    ),
                                );
                            }
                        }
                        Ok(())
                    })?;
                }
                continue;
            }

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
                if let Some(k) = key_at(i) {
                    out.boundary_keys.insert(k.to_string());
                }
            }
        }

        if let (true, Some(conv)) = (partitioned, &converter) {
            if !running.is_empty() {
                let mut index = HashMap::with_capacity(running.len());
                let mut rows = Vec::with_capacity(running.len());
                let mut keys = Vec::with_capacity(running.len());
                for (i, (p, (mark, tied))) in running.into_iter().enumerate() {
                    index.insert(p, i as u32);
                    rows.push(mark);
                    keys.push(tied);
                }
                let marks = conv
                    .convert_rows(rows.iter().map(|r| r.row()))
                    .map_err(|e| Error::Other(format!("dedup: cannot decode the marks: {e}")))?
                    .pop()
                    .expect("one column");
                out.partitions = PartitionMarks {
                    index,
                    marks: Some(marks),
                    keys,
                };
            }
        }
        if null_partition_rows > 0 {
            warn!(
                partition_columns = ?partition_columns,
                null_partition_rows,
                "target rows with no dedup_partition value cover nothing; any of them the \
                 source delivers again inside this window will be written again"
            );
        }

        debug!(
            timestamp_column,
            key_column,
            partition_columns = ?partition_columns,
            rows_scanned,
            partitions = out.partition_count(),
            null_partition_rows,
            boundary_keys = out.boundary_key_count(),
            watermark_known = out.watermark_is_known(),
            "read the target's watermark"
        );
        Ok(out)
    }

    /// True when there is nothing to suppress, so `apply` can be skipped entirely.
    pub fn is_inert(&self) -> bool {
        match self.is_partitioned() {
            true => self.partitions.index.is_empty(),
            false => self.watermark.is_none(),
        }
    }

    pub fn watermark_is_known(&self) -> bool {
        !self.is_inert()
    }

    /// The cut-off itself, for bounding how far back a rescan must reach. Global form only:
    /// the partitioned form has no single value, and `None` here. See [`rescan_start`].
    pub fn watermark(&self) -> Option<&ArrayRef> {
        self.watermark.as_ref()
    }

    pub fn boundary_key_count(&self) -> usize {
        self.boundary_keys.len() + self.partitions.keys.iter().map(HashSet::len).sum::<usize>()
    }

    /// The columns the cut-off is kept per value of; empty for a single cut-off.
    pub fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    fn is_partitioned(&self) -> bool {
        !self.partition_columns.is_empty()
    }

    /// Partition values the target holds a mark for. Zero in the global form.
    pub fn partition_count(&self) -> usize {
        self.partitions.index.len()
    }

    /// Each partition value and its mark, as a one-element array, ordered by value.
    pub fn partition_marks(&self) -> Vec<(&str, ArrayRef)> {
        let Some(marks) = &self.partitions.marks else {
            return Vec::new();
        };
        let mut out: Vec<(&str, ArrayRef)> = self
            .partitions
            .index
            .iter()
            .map(|(p, &i)| (p.as_str(), marks.slice(i as usize, 1)))
            .collect();
        out.sort_by(|a, b| natural(a.0).cmp(&natural(b.0)));
        out
    }

    /// Drop the rows the target already holds.
    pub fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.filter(batch).map(|f| f.kept)
    }

    /// Drop the rows the target already holds, and say how many went and how many of those
    /// kept are past the watermark altogether.
    pub fn filter(&self, batch: RecordBatch) -> Result<Filtered> {
        if self.is_partitioned() {
            return self.filter_partitioned(batch);
        }
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

        let after = gt(&ts, &scalar).map_err(|e| self.incomparable(e))?;
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

    /// [`Dedup::filter`] for the partitioned form: each row against its own partition's mark.
    fn filter_partitioned(&self, batch: RecordBatch) -> Result<Filtered> {
        let ts = require_timestamps(&batch, &self.timestamp_column)?;
        let parts = require_partitions(&batch, &self.partition_columns)?;
        let keys = match &self.key_column {
            Some(k) => Some(key_text(&batch, k)?),
            None => None,
        };
        let (covered, beyond) = self.covered_rows(&ts, &parts, keys.as_ref(), true)?;
        let keep = not(&covered).map_err(|e| Error::Other(e.to_string()))?;
        let kept = filter_record_batch(&batch, &keep)
            .map_err(|e| Error::Other(format!("dedup: filter failed: {e}")))?;
        let covered = batch.num_rows() - kept.num_rows();
        Ok(Filtered {
            kept,
            covered,
            beyond,
        })
    }

    /// Per row, whether its own partition's mark covers it; and how many rows lie past their
    /// partition's mark, or in a partition the target holds no row of.
    ///
    /// A row is covered when it is below its partition's mark, or at it with a key the target
    /// already holds there. At it with no key configured, `ties_covered` decides: the filter
    /// takes such a row as covered, the same fallback the single cut-off has, while the rescan
    /// bound, which must never take a row it has not seen for one the target holds, does not.
    /// A null timestamp or partition is never covered and never past anything; the filter has
    /// refused both before it gets here.
    fn covered_rows(
        &self,
        ts: &ArrayRef,
        parts: &PartitionText,
        keys: Option<&StringArray>,
        ties_covered: bool,
    ) -> Result<(BooleanArray, usize)> {
        let n = ts.len();
        let Some(marks) = &self.partitions.marks else {
            // A target with no rows covers nothing, so everything is past it.
            return Ok((BooleanArray::from(vec![false; n]), n));
        };
        let idx: UInt32Array = (0..n)
            .map(|i| match parts.is_null(i) {
                true => None,
                false => parts.with(i, |p| self.partitions.index.get(p).copied()),
            })
            .collect();
        let mark = take(marks.as_ref(), &idx, None)
            .map_err(|e| Error::Other(format!("dedup: cannot line up the marks: {e}")))?;
        let after = gt(ts, &mark).map_err(|e| self.incomparable(e))?;
        let at = eq(ts, &mark).map_err(|e| Error::Schema(format!("dedup: {e}")))?;

        let mut beyond = 0usize;
        let covered = (0..n)
            .map(|i| {
                if ts.is_null(i) || parts.is_null(i) {
                    return Some(false);
                }
                // In a partition the target lacks: had whatever filled it read this row, the
                // partition would be there.
                if mark.is_null(i) || after.value(i) {
                    beyond += 1;
                    return Some(false);
                }
                if !at.value(i) {
                    return Some(true);
                }
                Some(match keys {
                    Some(k) => {
                        !k.is_null(i)
                            && self.partitions.keys[idx.value(i) as usize].contains(k.value(i))
                    }
                    None => ties_covered,
                })
            })
            .collect();
        Ok((covered, beyond))
    }

    fn incomparable(&self, e: impl fmt::Display) -> Error {
        Error::Schema(format!(
            "dedup timestamp {:?}: cannot compare against the target's newest value; \
             source and target must agree on its type ({e})",
            self.timestamp_column
        ))
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
    /// Of `kept`, the rows strictly newer than the watermark — in the partitioned form, than
    /// their own partition's, or in a partition the target holds no row of: past anything
    /// whatever filled the target can have read. The first of these is what ends a coverage
    /// window.
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
    partition_columns: &[String],
) -> Result<()> {
    let wanted = [Some(timestamp_column), key_column]
        .into_iter()
        .flatten()
        .chain(partition_columns.iter().map(String::as_str));
    for wanted in wanted {
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
    for c in partition_columns {
        let dtype = schema
            .field_with_name(c)
            .expect("checked above")
            .data_type();
        if !groups_exactly(dtype) {
            return Err(Error::Config(format!(
                "dedup_partition {c:?} is {dtype} in the target; it must be an integer or a \
                 string column, such as kafka_partition. Those are the only types whose text \
                 is the value itself, which is what one cut-off per value is kept by."
            )));
        }
    }
    Ok(())
}

/// Whether a partition column of this type can key a cut-off: its text is the value, and its
/// statistics line up with that text.
fn groups_exactly(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    )
}

/// The batch's partition columns, provided every row has a value in each.
///
/// A row with no partition can be measured against no partition's mark, and once written it is
/// one the next handover's read passes over — so it is refused on every batch, on every write
/// mode. Unlike a row with no timestamp, an upserting pipeline has no merge check to fall back
/// on: accepted outside a window, the same row would stop it inside the next one.
pub fn require_partitions(
    batch: &RecordBatch,
    partition_columns: &[String],
) -> Result<PartitionText> {
    for c in partition_columns {
        let col = column(batch, c)?;
        if col.null_count() > 0 {
            return Err(Error::Schema(format!(
                "dedup partition {c:?} contains {} null(s); a row with no partition can be \
                 neither kept nor skipped safely",
                col.null_count()
            )));
        }
    }
    PartitionText::of(batch, partition_columns)
}

/// The partition columns of some rows, as the text a partition's mark is looked up by.
pub struct PartitionText {
    columns: Vec<StringArray>,
    /// With several columns, each distinct tuple's text and each row's place in that list,
    /// built on the first lookup: a batch holds few tuples, and building the text per row is
    /// a `Vec`, a `String` and JSON escaping for every row of a target scan.
    tuples: std::sync::OnceLock<(Vec<String>, Vec<u32>)>,
}

impl PartitionText {
    fn of(batch: &RecordBatch, partition_columns: &[String]) -> Result<Self> {
        partition_columns
            .iter()
            .map(|c| column(batch, c))
            .collect::<Result<Vec<_>>>()
            .and_then(|cols| Self::from_columns(partition_columns, cols))
    }

    fn from_columns(names: &[String], columns: Vec<ArrayRef>) -> Result<Self> {
        let columns = names
            .iter()
            .zip(columns)
            .map(|(name, col)| {
                let text = cast(&col, &DataType::Utf8).map_err(|e| {
                    Error::Schema(format!("dedup partition {name:?} is not comparable: {e}"))
                })?;
                Ok(text
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8")
                    .clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            columns,
            tuples: Default::default(),
        })
    }

    fn is_null(&self, i: usize) -> bool {
        self.columns.iter().any(|c| c.is_null(i))
    }

    /// Call `f` with row `i`'s partition. Never allocates per row: one column is its own
    /// text, and several are looked up in the batch's distinct tuples.
    fn with<R>(&self, i: usize, f: impl FnOnce(&str) -> R) -> R {
        match self.columns.as_slice() {
            [one] => f(one.value(i)),
            many => {
                let (texts, at) = self.tuples.get_or_init(|| distinct_tuples(many));
                match at.get(i).and_then(|&t| texts.get(t as usize)) {
                    Some(text) => f(text),
                    // A row with a null in it, which no caller asks for.
                    None => f(&partition_key(
                        &many.iter().map(|c| c.value(i)).collect::<Vec<_>>(),
                    )),
                }
            }
        }
    }
}

/// Each distinct tuple of `columns`' values as its [`partition_key`], and per row the position
/// of its own; `u32::MAX` for a row with a null.
fn distinct_tuples(columns: &[StringArray]) -> (Vec<String>, Vec<u32>) {
    let n = columns.first().map_or(0, |c| c.len());
    let mut seen: HashMap<Vec<&str>, u32> = HashMap::new();
    let mut texts = Vec::new();
    let mut at = Vec::with_capacity(n);
    let mut tuple: Vec<&str> = Vec::with_capacity(columns.len());
    for i in 0..n {
        if columns.iter().any(|c| c.is_null(i)) {
            at.push(u32::MAX);
            continue;
        }
        tuple.clear();
        tuple.extend(columns.iter().map(|c| c.value(i)));
        let t = match seen.get(tuple.as_slice()) {
            Some(&t) => t,
            None => {
                let t = texts.len() as u32;
                texts.push(partition_key(&tuple));
                seen.insert(tuple.clone(), t);
                t
            }
        };
        at.push(t);
    }
    (texts, at)
}

/// The text one partition is known by: the value itself for one column, a JSON array of the
/// values for several, which no two different tuples share.
pub fn partition_key(values: &[&str]) -> String {
    match values {
        [one] => one.to_string(),
        many => serde_json::to_string(many).expect("strings serialise"),
    }
}

/// Integers in numeric order, ahead of anything else in text order. For the startup line.
fn natural(p: &str) -> (Option<i128>, &str) {
    (p.parse().ok(), p)
}

fn too_many_partitions(partition_columns: &[String], limit: usize) -> Error {
    Error::Config(format!(
        "dedup_partition {partition_columns:?} has more than {limit} distinct values in the \
         target; it must name a low-cardinality column such as kafka_partition, the column \
         dedup_timestamp is ordered within — not a row identity"
    ))
}

fn key_text(batch: &RecordBatch, key: &str) -> Result<StringArray> {
    let col = column(batch, key)?;
    let text = cast(&col, &DataType::Utf8)
        .map_err(|e| Error::Schema(format!("dedup key {key:?} is not comparable: {e}")))?;
    Ok(text
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8")
        .clone())
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
/// `commitInfo` key naming the columns the window's cut-off is kept per value of, when it is
/// partitioned. Absent from every commit of a single cut-off, and from every commit written
/// before the option existed, which read the same way.
pub const CUTOFF_PARTITION_COLUMNS_KEY: &str = "ddi.cutoff.partitionColumns";
/// Appended to the [`CUTOFF_REASON_KEY`] value of a partitioned window, as in
/// `rebuilt.partitioned`.
///
/// So that a build from before `dedup_partition` — which ignores
/// [`CUTOFF_PARTITION_COLUMNS_KEY`] and would resume the window with one cut-off, over-covering
/// every partition behind the newest — does not know the reason, and resumes as though the
/// window had closed: rows written again, none dropped.
pub const CUTOFF_PARTITIONED_REASON_SUFFIX: &str = ".partitioned";

/// A coverage window, as a commit made inside it records it.
///
/// Recorded because a reopen cannot work it out again. Once this pipeline has committed, the
/// target's log says the newest commit is its own, which reads exactly like an ordinary
/// restart — so without this a restart part-way through a rebuild's rescan would stop
/// filtering and re-emit the rest of what the rebuild wrote. Only the reason, the head and the
/// form are kept: the watermark itself is read from the target again, and is the same one,
/// because a commit made inside an open window holds only rows exactly at the watermark — in
/// the partitioned form, exactly at their own partition's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedCutoff {
    pub reason: CoverageReason,
    /// The source version the window closes after once read. `None` when only a newer row
    /// closes it.
    pub source_through: Option<Version>,
    /// The columns the cut-off was kept per value of. Empty for a single cut-off.
    ///
    /// Recorded because the form cannot change mid-window. A commit made inside a partitioned
    /// window can hold rows above other partitions' marks, so re-reading the target with one
    /// cut-off, or by another column, would take rows nobody has delivered for covered ones.
    pub partition_columns: Vec<String>,
}

impl RecordedCutoff {
    pub fn to_commit_metadata(&self) -> Vec<(String, serde_json::Value)> {
        let reason = match self.partition_columns.is_empty() {
            true => self.reason.as_str().to_string(),
            false => format!("{}{CUTOFF_PARTITIONED_REASON_SUFFIX}", self.reason.as_str()),
        };
        let mut out = vec![(
            CUTOFF_REASON_KEY.to_string(),
            serde_json::Value::from(reason),
        )];
        if let Some(through) = self.source_through {
            out.push((
                CUTOFF_SOURCE_THROUGH_KEY.to_string(),
                serde_json::Value::from(through),
            ));
        }
        if !self.partition_columns.is_empty() {
            out.push((
                CUTOFF_PARTITION_COLUMNS_KEY.to_string(),
                serde_json::Value::from(self.partition_columns.clone()),
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
        let (text, partitioned) = match raw.as_str() {
            Some(t) => match t.strip_suffix(CUTOFF_PARTITIONED_REASON_SUFFIX) {
                Some(base) => (Some(base), true),
                None => (Some(t), false),
            },
            None => (None, false),
        };
        let Some(reason) = text.and_then(CoverageReason::parse) else {
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
        let partition_columns = match info.get(CUTOFF_PARTITION_COLUMNS_KEY) {
            None => Vec::new(),
            Some(v) => match v
                .as_array()
                .and_then(|a| a.iter().map(|c| c.as_str().map(str::to_string)).collect())
            {
                Some(columns) => columns,
                None => {
                    warn!(
                        key = CUTOFF_PARTITION_COLUMNS_KEY,
                        value = %v,
                        "a commit of ours records a coverage window whose partition columns \
                         are not a list of names; resuming as though it had closed"
                    );
                    return None;
                }
            },
        };
        // The two are written together; one without the other is no window this build wrote.
        if partitioned == partition_columns.is_empty() {
            warn!(
                key = CUTOFF_REASON_KEY,
                value = %raw,
                partition_columns = ?partition_columns,
                "a commit of ours records a coverage window whose reason and partition columns \
                 disagree; resuming as though it had closed"
            );
            return None;
        }
        Some(Self {
            reason,
            source_through,
            partition_columns,
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
/// a sequence and an identity — and a projection naming it twice is rejected by the scan. The
/// partition columns, when there are any, are the third and last thing read.
fn projection<'a>(
    timestamp_column: &'a str,
    key_column: Option<&'a str>,
    partition_columns: &'a [String],
) -> Vec<&'a str> {
    let mut wanted = vec![timestamp_column];
    for c in key_column
        .into_iter()
        .chain(partition_columns.iter().map(String::as_str))
    {
        if !wanted.contains(&c) {
            wanted.push(c);
        }
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
/// newer than the watermark, and the rescan starts past it. A cut-off kept per partition
/// bounds its rescan with [`bounded_rescan_start_partitioned`]; [`rescan_start`] picks.
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
                // before it is too. A commit tied at the watermark proves nothing: rows
                // sharing the watermark may be in it and not in the target, and
                // Dedup::filter settles those ties by key. So it is re-read (the walk
                // goes on past it), like the partitioned bound's ties_covered=false.
                if cmax < covered_to {
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

/// Data files the partitioned bound may read, over all the commits it walks, before it gives
/// up and returns its fallback. Each read is of two or three columns only.
pub const MAX_PROBE_FILES: usize = 10_000;

/// The earliest source version that can still hold a row `dedup` does not cover, in whichever
/// form it is: [`bounded_rescan_start`] for a single cut-off, [`bounded_rescan_start_partitioned`]
/// for one per partition, and `fallback` for a target with no rows.
pub async fn rescan_start(
    source: &DeltaTable,
    dedup: &Dedup,
    fallback: Version,
    max_scan: u64,
) -> Result<Version> {
    if dedup.is_partitioned() {
        return bounded_rescan_start_partitioned(source, dedup, fallback, max_scan).await;
    }
    match dedup.watermark() {
        Some(w) => {
            bounded_rescan_start(source, &dedup.timestamp_column, w, fallback, max_scan).await
        }
        None => Ok(fallback),
    }
}

/// [`bounded_rescan_start`] for a cut-off kept per partition.
///
/// The single cut-off asks of each commit "is its newest row covered?", which with one mark
/// makes every row of it covered. Per partition there is no newest row to ask about — a file
/// written from a Kafka topic mixes every partition, and its statistics give the timestamp
/// range and the partition range separately — so this asks the question it stands for
/// directly: **is every row of this commit covered by its own partition's mark?** Walking
/// back from the head, the first commit that is ends the walk, for the reason the single
/// cut-off's does: under the ordering requirement, now per partition, a row past the read
/// point of whatever filled the target is past its partition's mark, so a commit with none
/// is at or before that read point, and so is everything before it.
///
/// One stray row cannot fake that. A redelivered old row, a straggler from a producer whose
/// clock runs behind, sits beside the new rows it was delivered with, and those are not
/// covered. A commit that is nothing but such rows is the failure the single cut-off has too.
///
/// Each file is answered from its statistics where they settle it — a file of one partition
/// whose newest timestamp is below that partition's mark is covered; one in a partition the
/// target lacks, or wholly above its mark, is not, and nor is one mixing partitions whose
/// lowest timestamp is above every mark — and otherwise by reading its timestamp,
/// partition and key columns and testing every row. That read is of two or three columns
/// of commits the rescan is about to read in full anyway, it stops at the first row that is
/// not covered, and [`MAX_PROBE_FILES`] caps it. It reads the **source's** columns under the
/// target's names, so it assumes what the timestamp bound already does: a source column of
/// that name holds the value the transform passes through. A source without the timestamp or
/// a partition column under those names returns `fallback` at once; one without the key column
/// — a model that derives its key — is still bounded, with a row exactly at its partition's
/// mark taken as not covered.
///
/// Only a plain append can prove anything. A DELETE, UPDATE or MERGE on the source rewrites
/// files full of old rows, and a RESTORE re-adds them: either would look covered while the
/// commits before it hold rows that are not. Those are walked past.
///
/// Anything that cannot be answered proves nothing and the walk goes on; running out of log,
/// of `max_scan` or of the probe budget returns `fallback`. Slow, never wrong. A row in a
/// partition the target lacks is never covered, so a source that keeps delivering one — a
/// model that filters a partition out — is never bounded; that fallback is warned about, with
/// the partition named.
pub async fn bounded_rescan_start_partitioned(
    source: &DeltaTable,
    dedup: &Dedup,
    fallback: Version,
    max_scan: u64,
) -> Result<Version> {
    use deltalake::logstore::get_actions;

    if !dedup.is_partitioned() || dedup.is_inert() {
        return Ok(fallback);
    }
    let Some(head) = source.version() else {
        return Ok(fallback);
    };
    let marks = MarkBounds::of(dedup);
    // A partition column the source is partitioned by has one exact value per file, in the
    // log; any other is in the data, and in the statistics at best.
    let snapshot = source.snapshot().map_err(Error::Delta)?;
    let delta_partitioned: Vec<String> = snapshot.metadata().partition_columns().to_vec();
    // Without the timestamp or a partition column no file can prove anything, so walking
    // would only spend the probe budget before falling back: fall back now. The key is not
    // needed — without it a row exactly at its mark is taken as not covered.
    let source_schema = snapshot.schema();
    let required = std::iter::once(&dedup.timestamp_column).chain(dedup.partition_columns.iter());
    if let Some(missing) = required
        .into_iter()
        .find(|c| source_schema.field(c.as_str()).is_none())
    {
        debug!(
            column = %missing,
            "the source has no column of this name, so the rescan cannot be bounded"
        );
        return Ok(fallback);
    }
    let log = source.log_store();
    let mut probe = Probe {
        store: log.object_store(None),
        dedup,
        delta_partitioned: &delta_partitioned,
        budget: MAX_PROBE_FILES,
    };

    let mut v = head;
    let mut scanned = 0u64;
    loop {
        if scanned >= max_scan || v < fallback {
            break;
        }
        let Some(raw) = log.read_commit_entry(v).await? else {
            break; // log truncated under us
        };
        let actions = get_actions(v, &raw)?;
        if may_prove_coverage(&actions) {
            match commit_coverage(&actions, &marks, &mut probe).await {
                Some(true) => {
                    debug!(
                        version = v,
                        rescan_from = v.saturating_add(1),
                        "every row of this source commit is covered by its partition's mark"
                    );
                    return Ok(v.saturating_add(1));
                }
                Some(false) => {}
                // Out of probe budget.
                None => break,
            }
        }
        if v == 0 {
            break;
        }
        v -= 1;
        scanned += 1;
    }
    // A partition the target holds no row of has nothing covered, so a source that keeps
    // delivering one — a model that filters out a topic or some partitions — leaves no commit
    // that proves anything, on every handover. Said, because nothing else would say it.
    if let Some(partition) = marks.absent.get() {
        warn!(
            partition = %partition,
            dedup_partition = ?dedup.partition_columns,
            rescan_from = fallback,
            "the rescan could not be bounded: the source holds rows of a partition the target \
             has none of, and no commit with such a row can be shown covered. A model that \
             filters out whole partitions or topics rescans from its starting version on every \
             handover; leave dedup_partition off for it, or give it a source of its own"
        );
    }
    Ok(fallback)
}

/// Whether a commit can stand for "everything before me was read": a plain append, and not a
/// RESTORE, which re-adds files without a `Remove` and so classifies as one.
fn may_prove_coverage(actions: &[deltalake::kernel::Action]) -> bool {
    use crate::source::{classify, CommitClass};
    use deltalake::kernel::Action;

    let restore = actions.iter().any(|a| {
        matches!(a, Action::CommitInfo(ci)
            if ci.operation.as_deref().is_some_and(|op| op.eq_ignore_ascii_case("RESTORE")))
    });
    matches!(classify(actions), CommitClass::Data { .. }) && !restore
}

/// Whether every row a commit added is covered, and it added at least one. `None` when the
/// probe budget ran out before that could be answered.
async fn commit_coverage(
    actions: &[deltalake::kernel::Action],
    marks: &MarkBounds<'_>,
    probe: &mut Probe<'_>,
) -> Option<bool> {
    use deltalake::kernel::Action;

    let adds: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Add(add) if add.data_change => Some(add),
            _ => None,
        })
        .collect();
    // Statistics first, for every file: they are free, and one file they show to be past its
    // mark settles the commit without reading anything.
    let mut verdicts = Vec::with_capacity(adds.len());
    for add in &adds {
        let stats = add
            .stats
            .as_deref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
        let v = file_coverage_from_stats(
            stats.as_ref(),
            &add.partition_values,
            &probe.dedup.timestamp_column,
            &probe.dedup.partition_columns,
            marks,
        );
        if v == FileCoverage::Uncovered {
            return Some(false);
        }
        verdicts.push(v);
    }
    for (add, v) in adds.iter().zip(verdicts.iter_mut()) {
        if *v == FileCoverage::Unknown {
            *v = probe.file(add, marks).await?;
            if !matches!(v, FileCoverage::Covered { .. }) {
                // Uncovered, or a file that could not be read: either way it proves nothing,
                // and the rest of the commit need not be read.
                return Some(false);
            }
        }
    }
    Some(commit_is_covered(&verdicts))
}

/// What one file says about whether its rows are covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileCoverage {
    /// Every one of its `rows` is covered by its own partition's mark.
    Covered { rows: u64 },
    /// At least one row is not.
    Uncovered,
    /// Cannot be told from what was looked at.
    Unknown,
}

/// A commit is covered when every file in it is, and they hold at least one row between them:
/// a commit of empty files says nothing about where anything was read to.
fn commit_is_covered(files: &[FileCoverage]) -> bool {
    let mut rows = 0u64;
    for f in files {
        match f {
            FileCoverage::Covered { rows: n } => rows += n,
            FileCoverage::Uncovered | FileCoverage::Unknown => return false,
        }
    }
    rows > 0
}

/// The marks, as bounds a statistic can be compared with.
struct MarkBounds<'a> {
    index: &'a HashMap<String, u32>,
    bounds: Vec<Option<Bound>>,
    /// The highest of `bounds`, when every mark has one: a lowest timestamp past it is past
    /// every row's own mark, whichever partitions a file mixes.
    highest: Option<Bound>,
    slack: Option<Slack>,
    /// The first partition the walk met rows of that the target holds none of, for saying why
    /// it could not be bounded.
    absent: std::sync::OnceLock<String>,
}

impl<'a> MarkBounds<'a> {
    fn of(dedup: &'a Dedup) -> Self {
        use std::cmp::Ordering;

        let marks = dedup.partitions.marks.as_ref();
        let bounds: Vec<Option<Bound>> = (0..dedup.partitions.keys.len())
            .map(|i| marks.and_then(|m| bound_of_scalar(&m.slice(i, 1))))
            .collect();
        let highest = bounds.iter().try_fold(None::<Bound>, |high, b| {
            let b = b.as_ref()?;
            Some(Some(match high {
                None => b.clone(),
                Some(h) => match h.partial_cmp(b)? {
                    Ordering::Less => b.clone(),
                    _ => h,
                },
            }))
        });
        Self {
            index: &dedup.partitions.index,
            bounds,
            highest: highest.flatten(),
            slack: marks.and_then(|m| Slack::of(m.data_type())),
            absent: Default::default(),
        }
    }
}

/// What a file's statistics alone say about its rows.
///
/// Covered only when the file holds one partition, known exactly — a value of a column the
/// source is partitioned by, or an integer whose recorded minimum and maximum agree — that
/// partition has a mark, and the highest timestamp the file's maximum could stand for is
/// strictly below it. Strictly, because a row at the mark is covered only if its key is one
/// the target holds there, which statistics cannot say. A string partition read from
/// statistics is never taken as known: a writer may truncate both ends to the same prefix.
///
/// Uncovered when a row provably lies outside every mark: a null partition or timestamp, a
/// partition the target lacks, or a lowest timestamp above the mark — for a file mixing
/// partitions, or one whose partition is not known exactly, above the highest mark of all.
/// Anything else is Unknown.
fn file_coverage_from_stats(
    stats: Option<&serde_json::Value>,
    partition_values: &HashMap<String, Option<String>>,
    timestamp_column: &str,
    partition_columns: &[String],
    marks: &MarkBounds,
) -> FileCoverage {
    use FileCoverage::*;

    let Some(stats) = stats else {
        return Unknown;
    };
    let Some(rows) = stats.get("numRecords").and_then(serde_json::Value::as_u64) else {
        return Unknown;
    };
    if rows == 0 {
        return Covered { rows: 0 };
    }
    let stat = |which: &str, column: &str| stats.get(which).and_then(|m| m.get(column));
    let nulls = |column: &str| stat("nullCount", column).and_then(serde_json::Value::as_u64);

    // `None` once a partition column is not known exactly: several partitions, or no telling.
    let mut parts = Some(Vec::with_capacity(partition_columns.len()));
    for c in partition_columns {
        if let Some(value) = partition_values.get(c) {
            match value {
                Some(v) => parts.iter_mut().for_each(|p| p.push(v.clone())),
                None => return Uncovered,
            }
            continue;
        }
        match nulls(c) {
            Some(0) => {}
            Some(_) => return Uncovered,
            None => {
                parts = None;
                continue;
            }
        }
        match (
            stat("minValues", c).and_then(integer_text),
            stat("maxValues", c).and_then(integer_text),
        ) {
            (Some(lo), Some(hi)) if lo == hi => parts.iter_mut().for_each(|p| p.push(lo.clone())),
            _ => parts = None,
        }
    }
    let Some(parts) = parts else {
        // No one mark to measure against, but a lowest timestamp past the highest of them is
        // past every row's own: a file a kafka-delta-ingest commit after the rebuild wrote
        // is settled here, without being read.
        let Some(highest) = &marks.highest else {
            return Unknown;
        };
        let min = stat("minValues", timestamp_column).and_then(|s| bound_of_stat(s, highest));
        return match min {
            Some(min)
                if highest.provably_below(&marks.slack.map_or(min.clone(), |s| s.below(&min))) =>
            {
                Uncovered
            }
            _ => Unknown,
        };
    };
    let key = partition_key(&parts.iter().map(String::as_str).collect::<Vec<_>>());
    let Some(&i) = marks.index.get(&key) else {
        let _ = marks.absent.set(key);
        return Uncovered;
    };
    let Some(mark) = &marks.bounds[i as usize] else {
        return Unknown;
    };

    match nulls(timestamp_column) {
        Some(0) => {}
        Some(_) => return Uncovered,
        None => return Unknown,
    }
    let read = |which: &str| stat(which, timestamp_column).and_then(|s| bound_of_stat(s, mark));
    if let Some(max) = read("maxValues") {
        let max = marks.slack.map_or_else(|| max.clone(), |s| s.above(&max));
        if max.provably_below(mark) {
            return Covered { rows };
        }
    }
    if let Some(min) = read("minValues") {
        let min = marks.slack.map_or_else(|| min.clone(), |s| s.below(&min));
        if mark.provably_below(&min) {
            return Uncovered;
        }
    }
    Unknown
}

/// An integer statistic as the text its column casts to. Read as either sign of 64 bits, so a
/// UInt64 past `i64::MAX` is not lost; anything that is not an integer is not one.
fn integer_text(stat: &serde_json::Value) -> Option<String> {
    let n = stat.as_number()?;
    n.as_i64()
        .map(|v| v.to_string())
        .or_else(|| n.as_u64().map(|v| v.to_string()))
}

/// Reads the few columns of a source file that say whether its rows are covered.
struct Probe<'a> {
    store: std::sync::Arc<dyn deltalake::logstore::object_store::ObjectStore>,
    dedup: &'a Dedup,
    delta_partitioned: &'a [String],
    /// Files still allowed to be read.
    budget: usize,
}

impl Probe<'_> {
    /// Every row of `add` tested against its partition's mark. `None` once the budget is
    /// spent; an unreadable file, or one without the columns, is Unknown.
    async fn file(
        &mut self,
        add: &deltalake::kernel::Add,
        marks: &MarkBounds<'_>,
    ) -> Option<FileCoverage> {
        if self.budget == 0 {
            return None;
        }
        self.budget -= 1;
        Some(match self.read(add, marks).await {
            Ok(v) => v,
            Err(e) => {
                debug!(path = %add.path, error = %e, "cannot read this file for the rescan bound");
                FileCoverage::Unknown
            }
        })
    }

    #[allow(deprecated)] // parquet 59.2 deprecates ParquetObjectReader; replacing it is its own change
    async fn read(
        &self,
        add: &deltalake::kernel::Add,
        bounds: &MarkBounds<'_>,
    ) -> Result<FileCoverage> {
        use deltalake::arrow::array::new_null_array;
        use deltalake::parquet::arrow::async_reader::{
            ParquetObjectReader, ParquetRecordBatchStreamBuilder,
        };
        use deltalake::parquet::arrow::ProjectionMask;
        use deltalake::Path as StorePath;

        let d = self.dedup;
        let Some(marks) = &d.partitions.marks else {
            return Ok(FileCoverage::Unknown);
        };
        let path = StorePath::parse(&add.path)
            .map_err(|e| Error::Other(format!("bad file path {:?}: {e}", add.path)))?;
        // The size from the Add, as `scan_source` does: the probe for it is a suffix range
        // request, which Azure Blob Storage does not implement.
        let reader = ParquetObjectReader::new(self.store.clone(), path)
            .with_file_size(add.size.max(0) as u64);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| Error::Other(e.to_string()))?;

        // In the file: the timestamp, the key, and the partition columns the source is not
        // partitioned by. A timestamp or partition column it does not have — renamed, mapped
        // to a physical name, or added later — leaves the question open. A key it does not
        // have only means a row exactly at its mark cannot be shown covered: the model may
        // derive its key, and the rows below the marks still settle the file.
        let file_schema = builder.schema().clone();
        let key_column = d
            .key_column
            .as_deref()
            .filter(|k| file_schema.index_of(k).is_ok());
        let in_data: Vec<&str> = [Some(d.timestamp_column.as_str()), key_column]
            .into_iter()
            .flatten()
            .chain(
                d.partition_columns
                    .iter()
                    .filter(|c| !self.delta_partitioned.contains(c))
                    .map(String::as_str),
            )
            .collect();
        let mut roots = Vec::with_capacity(in_data.len());
        for c in &in_data {
            match file_schema.index_of(c) {
                Ok(i) => roots.push(i),
                Err(_) => return Ok(FileCoverage::Unknown),
            }
        }
        roots.sort_unstable();
        roots.dedup();
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
        let mut stream = builder
            .with_projection(mask)
            .build()
            .map_err(|e| Error::Other(e.to_string()))?;

        let mut rows = 0u64;
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|e| Error::Other(e.to_string()))?
        {
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            let get = |c: &str| batch.column_by_name(c).cloned();
            // As the target types it: what the filter compares is the coerced row. A value
            // that does not fit becomes null, and a null is never covered.
            let Some(ts) = get(&d.timestamp_column) else {
                return Ok(FileCoverage::Unknown);
            };
            let Ok(ts) = cast(&ts, marks.data_type()) else {
                return Ok(FileCoverage::Unknown);
            };
            let mut cols = Vec::with_capacity(d.partition_columns.len());
            for c in &d.partition_columns {
                cols.push(match self.delta_partitioned.contains(c) {
                    true => match add.partition_values.get(c).cloned().flatten() {
                        Some(v) => std::sync::Arc::new(StringArray::from(vec![v; n])) as ArrayRef,
                        None => new_null_array(&DataType::Utf8, n),
                    },
                    false => match get(c) {
                        Some(col) => col,
                        None => return Ok(FileCoverage::Unknown),
                    },
                });
            }
            let Ok(parts) = PartitionText::from_columns(&d.partition_columns, cols) else {
                return Ok(FileCoverage::Unknown);
            };
            let keys = match key_column {
                Some(k) => match get(k).and_then(|c| cast(&c, &DataType::Utf8).ok()) {
                    Some(text) => Some(
                        text.as_any()
                            .downcast_ref::<StringArray>()
                            .expect("utf8")
                            .clone(),
                    ),
                    None => return Ok(FileCoverage::Unknown),
                },
                None => None,
            };
            let Ok((covered, _)) = d.covered_rows(&ts, &parts, keys.as_ref(), false) else {
                return Ok(FileCoverage::Unknown);
            };
            if covered.true_count() < n {
                if bounds.absent.get().is_none() {
                    let absent = (0..n).filter(|&i| !parts.is_null(i)).find_map(|i| {
                        parts.with(i, |p| {
                            (!d.partitions.index.contains_key(p)).then(|| p.to_string())
                        })
                    });
                    if let Some(p) = absent {
                        let _ = bounds.absent.set(p);
                    }
                }
                return Ok(FileCoverage::Uncovered);
            }
            rows += n as u64;
        }
        Ok(FileCoverage::Covered { rows })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int32Array, Int64Array, TimestampMicrosecondArray};
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
            projection("_timestamp", Some("order_id"), &[]),
            ["_timestamp", "order_id"]
        );
        assert_eq!(projection("_timestamp", None, &[]), ["_timestamp"]);
    }

    #[test]
    fn a_column_that_is_both_sequence_and_key_is_named_once() {
        // A monotonic id is a legitimate choice for both, and a projection naming it twice
        // is rejected by the scan — which is how this first showed up.
        assert_eq!(projection("id", Some("id"), &[]), ["id"]);
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
            ..Default::default()
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
            ..Default::default()
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
                partition_columns: vec![],
            },
            RecordedCutoff {
                reason: CoverageReason::Bootstrap,
                source_through: Some(0),
                partition_columns: vec![],
            },
            RecordedCutoff {
                reason: CoverageReason::SourceReplaced,
                source_through: None,
                partition_columns: vec![],
            },
            RecordedCutoff {
                reason: CoverageReason::Rebuilt,
                source_through: Some(7),
                partition_columns: vec!["kafka_partition".into()],
            },
            RecordedCutoff {
                reason: CoverageReason::SourceReplaced,
                source_through: None,
                partition_columns: vec!["kafka_topic".into(), "kafka_partition".into()],
            },
        ] {
            assert_eq!(
                RecordedCutoff::from_commit_info(&as_info(&c)),
                Some(c.clone())
            );
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
            partition_columns: vec![],
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

    // ------------------------------------------------------------ per partition

    /// A cut-off kept per value of `part`: each entry is a partition, its mark, and the keys
    /// already present at it.
    fn partitioned(marks: &[(&str, i64, &[&str])], key: bool) -> Dedup {
        Dedup {
            timestamp_column: "_timestamp".into(),
            key_column: key.then(|| "order_id".into()),
            partition_columns: vec!["part".into()],
            partitions: PartitionMarks {
                index: marks
                    .iter()
                    .enumerate()
                    .map(|(i, m)| (m.0.to_string(), i as u32))
                    .collect(),
                marks: Some(Arc::new(TimestampMicrosecondArray::from(
                    marks.iter().map(|m| m.1).collect::<Vec<_>>(),
                ))),
                keys: marks
                    .iter()
                    .map(|m| m.2.iter().map(|k| k.to_string()).collect())
                    .collect(),
            },
            ..Default::default()
        }
    }

    /// `(order_id, part, _timestamp)` rows.
    fn parted(rows: &[(i64, Option<&str>, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("part", DataType::Utf8, true),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn the_partition_column_is_projected_once() {
        let part = ["kafka_partition".to_string()];
        assert_eq!(
            projection("_timestamp", Some("id"), &part),
            ["_timestamp", "id", "kafka_partition"]
        );
        let repeated = ["id".to_string(), "kafka_partition".to_string()];
        assert_eq!(
            projection("_timestamp", Some("id"), &repeated),
            ["_timestamp", "id", "kafka_partition"],
            "a projection naming a column twice is rejected by the scan"
        );
    }

    #[test]
    fn each_row_is_measured_against_its_own_partition() {
        // The loss the option exists for: B lags, so B6 is older than A's newest and a single
        // cut-off at 20 would drop it.
        let d = partitioned(&[("A", 20, &[]), ("B", 5, &[])], false);
        let f = d
            .filter(parted(&[
                (1, Some("A"), 10),
                (2, Some("A"), 30),
                (3, Some("B"), 3),
                (4, Some("B"), 5),
                (5, Some("B"), 6),
            ]))
            .unwrap();
        assert_eq!(ids(&f.kept), vec![2, 5]);
        assert_eq!(f.covered, 3);
        assert_eq!(f.beyond, 2, "A30 is past A's mark and B6 past B's");
    }

    #[test]
    fn a_partition_the_target_lacks_has_nothing_covered() {
        let d = partitioned(&[("A", 20, &[])], false);
        let f = d.filter(parted(&[(1, Some("C"), 1)])).unwrap();
        assert_eq!(ids(&f.kept), vec![1]);
        assert_eq!(
            f.beyond, 1,
            "had whatever filled the target read it, C would be there"
        );
    }

    #[test]
    fn ties_are_resolved_against_their_own_partitions_keys() {
        let d = partitioned(&[("A", 20, &["2"]), ("B", 20, &["9"])], true);
        let f = d
            .filter(parted(&[
                (9, Some("A"), 20),
                (2, Some("A"), 20),
                (9, Some("B"), 20),
            ]))
            .unwrap();
        assert_eq!(
            ids(&f.kept),
            vec![9],
            "A9 is new at A's mark; A2 and B9 are held"
        );
        assert_eq!(f.covered, 2);
        assert_eq!(f.beyond, 0, "a tie says nothing about the read point");
    }

    #[test]
    fn partitioned_without_a_key_ties_fall_back_to_strictly_greater() {
        let d = partitioned(&[("A", 20, &[])], false);
        let f = d
            .filter(parted(&[
                (2, Some("A"), 20),
                (9, Some("A"), 20),
                (3, Some("A"), 21),
            ]))
            .unwrap();
        assert_eq!(ids(&f.kept), vec![3]);
    }

    #[test]
    fn a_null_partition_value_is_an_error_rather_than_a_guess() {
        let d = partitioned(&[("A", 20, &[])], false);
        let e = d.filter(parted(&[(1, None, 30)])).unwrap_err();
        assert!(matches!(e, Error::Schema(_)), "got: {e}");
        assert!(e.to_string().contains("\"part\""), "names the column: {e}");
    }

    #[test]
    fn an_integer_partition_column_matches_by_value() {
        // kafka-delta-ingest writes kafka_partition as an Int32.
        let mut d = partitioned(&[("0", 20, &[]), ("1", 5, &[])], false);
        d.partition_columns = vec!["kafka_partition".into()];
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("kafka_partition", DataType::Int32, false),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![10, 10, 10])) as ArrayRef,
            ],
        )
        .unwrap();
        let f = d.filter(b).unwrap();
        assert_eq!(
            ids(&f.kept),
            vec![2, 3],
            "1 is below 0's mark; 1 and 2 are not"
        );
    }

    #[test]
    fn several_partition_columns_key_one_cut_off_per_tuple() {
        // Partition 0 of two topics is two partitions, not one.
        let key = |t: &str, p: &str| partition_key(&[t, p]);
        let mut d = partitioned(
            &[(&key("x", "0"), 20, &[]), (&key("y", "0"), 5, &[])],
            false,
        );
        d.partition_columns = vec!["topic".into(), "part".into()];
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("topic", DataType::Utf8, false),
                Field::new("part", DataType::Int32, false),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![0, 0])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![10, 10])) as ArrayRef,
            ],
        )
        .unwrap();
        assert_eq!(ids(&d.filter(b).unwrap().kept), vec![2]);
        assert_ne!(
            key("a\",\"b", "c"),
            key("a", "b\",\"c"),
            "no two tuples share a key"
        );
    }

    #[test]
    fn a_partitioned_type_mismatch_still_names_the_timestamp() {
        let d = partitioned(&[("A", 20, &[])], false);
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("part", DataType::Utf8, false),
                Field::new("_timestamp", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ],
        )
        .unwrap();
        let e = d.filter(b).unwrap_err().to_string();
        assert!(e.contains("_timestamp"), "got: {e}");
        assert!(e.contains("must agree on its type"), "got: {e}");
    }

    #[test]
    fn the_rescan_bound_takes_no_tie_without_a_key_for_covered() {
        // The filter assumes a keyless tie is covered, as the single cut-off does. The bound
        // must not: a commit past the read point can open with a row at a partition's mark.
        let d = partitioned(&[("A", 20, &[])], false);
        let b = parted(&[(1, Some("A"), 20)]);
        let parts = PartitionText::of(&b, &d.partition_columns).unwrap();
        let ts = b.column(2).clone();
        let (filter, _) = d.covered_rows(&ts, &parts, None, true).unwrap();
        let (bound, _) = d.covered_rows(&ts, &parts, None, false).unwrap();
        assert!(filter.value(0));
        assert!(!bound.value(0));
    }

    #[test]
    fn an_unreadable_partition_record_resumes_as_closed() {
        let mut info: HashMap<String, serde_json::Value> = RecordedCutoff {
            reason: CoverageReason::Rebuilt,
            source_through: Some(7),
            partition_columns: vec!["kafka_partition".into()],
        }
        .to_commit_metadata()
        .into_iter()
        .collect();
        for bad in [
            serde_json::json!(3),
            serde_json::json!(["kafka_partition", 1]),
        ] {
            info.insert(CUTOFF_PARTITION_COLUMNS_KEY.to_string(), bad);
            assert_eq!(RecordedCutoff::from_commit_info(&info), None);
        }

        // The reason and the columns are written together; one without the other is no
        // window this build wrote.
        info.insert(
            CUTOFF_PARTITION_COLUMNS_KEY.to_string(),
            serde_json::json!(["kafka_partition"]),
        );
        info.insert(CUTOFF_REASON_KEY.to_string(), serde_json::json!("rebuilt"));
        assert_eq!(RecordedCutoff::from_commit_info(&info), None);
        info.remove(CUTOFF_PARTITION_COLUMNS_KEY);
        info.insert(
            CUTOFF_REASON_KEY.to_string(),
            serde_json::json!("rebuilt.partitioned"),
        );
        assert_eq!(RecordedCutoff::from_commit_info(&info), None);
    }

    #[test]
    fn a_partitioned_window_is_unknown_to_a_build_without_dedup_partition() {
        // Such a build ignores the columns and parses the reason with this same table: were it
        // to know the reason, it would resume the window with one cut-off, which covers every
        // partition behind the newest. Not knowing it, it resumes as though it had closed.
        for reason in [
            CoverageReason::Rebuilt,
            CoverageReason::Bootstrap,
            CoverageReason::SourceReplaced,
        ] {
            let info: HashMap<String, serde_json::Value> = RecordedCutoff {
                reason,
                source_through: Some(7),
                partition_columns: vec!["kafka_partition".into()],
            }
            .to_commit_metadata()
            .into_iter()
            .collect();
            let written = info[CUTOFF_REASON_KEY].as_str().unwrap();
            assert_eq!(CoverageReason::parse(written), None, "{written}");

            let single: HashMap<String, serde_json::Value> = RecordedCutoff {
                reason,
                source_through: Some(7),
                partition_columns: vec![],
            }
            .to_commit_metadata()
            .into_iter()
            .collect();
            let written = single[CUTOFF_REASON_KEY].as_str().unwrap();
            assert_eq!(
                CoverageReason::parse(written),
                Some(reason),
                "a single cut-off is still written as before: {written}"
            );
        }
    }

    #[test]
    fn only_integer_and_string_columns_can_key_a_cut_off() {
        let schema = Schema::new(vec![
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("p32", DataType::Int32, false),
            Field::new("p64", DataType::Int64, false),
            Field::new("pu64", DataType::UInt64, false),
            Field::new("ptext", DataType::Utf8, false),
            Field::new("pf", DataType::Float64, false),
            Field::new("pd", DataType::Decimal128(10, 2), false),
        ]);
        for ok in ["p32", "p64", "pu64", "ptext"] {
            require_columns(&schema, "_timestamp", None, &[ok.to_string()])
                .unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        for bad in ["_timestamp", "pf", "pd"] {
            let e = require_columns(&schema, "_timestamp", None, &[bad.to_string()]).unwrap_err();
            assert!(matches!(e, Error::Config(_)), "{bad}: {e}");
            assert!(e.to_string().contains(bad), "{bad}: {e}");
        }
        let e = require_columns(&schema, "_timestamp", None, &["nope".to_string()]).unwrap_err();
        assert!(e.to_string().contains("p32"), "lists the real columns: {e}");
    }

    // ------------------------------------------------------------ the partitioned bound

    fn bounds(dedup: &Dedup) -> MarkBounds<'_> {
        MarkBounds::of(dedup)
    }

    fn stats(json: serde_json::Value) -> Option<serde_json::Value> {
        Some(json)
    }

    /// A file of partition `p` (from statistics) with timestamps `[lo, hi]`.
    fn one_partition(p: i64, lo: &str, hi: &str) -> Option<serde_json::Value> {
        stats(serde_json::json!({
            "numRecords": 3,
            "minValues": {"part": p, "_timestamp": lo},
            "maxValues": {"part": p, "_timestamp": hi},
            "nullCount": {"part": 0, "_timestamp": 0},
        }))
    }

    #[test]
    fn a_file_of_one_partition_below_its_mark_is_covered_by_its_statistics() {
        // Marks at 2026-01-01T00:00:00.000500 for partition 0 and .000100 for 1.
        let d = partitioned(
            &[
                ("0", 1_767_225_600_000_500, &[]),
                ("1", 1_767_225_600_000_100, &[]),
            ],
            false,
        );
        let m = bounds(&d);
        let none = HashMap::new();
        let ts = "_timestamp";
        let part = ["part".to_string()];
        let at = |us: u32| format!("2026-01-01T00:00:00.{us:06}Z");

        assert_eq!(
            file_coverage_from_stats(
                one_partition(0, &at(1), &at(400)).as_ref(),
                &none,
                ts,
                &part,
                &m
            ),
            FileCoverage::Covered { rows: 3 }
        );
        assert_eq!(
            file_coverage_from_stats(
                one_partition(0, &at(1), &at(500)).as_ref(),
                &none,
                ts,
                &part,
                &m
            ),
            FileCoverage::Unknown,
            "at the mark is not provably covered: its key may be new"
        );
        assert_eq!(
            file_coverage_from_stats(
                one_partition(1, &at(1), &at(400)).as_ref(),
                &none,
                ts,
                &part,
                &m
            ),
            FileCoverage::Unknown,
            "the same file is not below partition 1's mark"
        );
        assert_eq!(
            file_coverage_from_stats(
                one_partition(1, &at(101), &at(400)).as_ref(),
                &none,
                ts,
                &part,
                &m
            ),
            FileCoverage::Uncovered,
            "wholly past partition 1's mark"
        );
        assert_eq!(
            file_coverage_from_stats(
                one_partition(7, &at(1), &at(2)).as_ref(),
                &none,
                ts,
                &part,
                &m
            ),
            FileCoverage::Uncovered,
            "a partition the target lacks"
        );
    }

    #[test]
    fn a_millisecond_maximum_is_read_as_the_highest_value_it_could_stand_for() {
        // A maximum of exactly .000 may have been truncated from up to .000999, past the mark.
        let d = partitioned(&[("0", 1_767_225_600_000_500, &[])], false);
        let m = bounds(&d);
        let f = one_partition(0, "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z");
        assert_eq!(
            file_coverage_from_stats(
                f.as_ref(),
                &HashMap::new(),
                "_timestamp",
                &["part".into()],
                &m
            ),
            FileCoverage::Unknown
        );
    }

    #[test]
    fn a_file_mixing_partitions_or_without_statistics_is_left_to_the_probe() {
        let d = partitioned(&[("0", 100, &[]), ("1", 100, &[])], false);
        let m = bounds(&d);
        let part = ["part".to_string()];
        let mixed = stats(serde_json::json!({
            "numRecords": 3,
            "minValues": {"part": 0, "_timestamp": "1970-01-01T00:00:00.000001Z"},
            "maxValues": {"part": 1, "_timestamp": "1970-01-01T00:00:00.000002Z"},
            "nullCount": {"part": 0, "_timestamp": 0},
        }));
        let no_partition_stats = stats(serde_json::json!({
            "numRecords": 3,
            "minValues": {"_timestamp": "1970-01-01T00:00:00.000001Z"},
            "maxValues": {"_timestamp": "1970-01-01T00:00:00.000002Z"},
            "nullCount": {"_timestamp": 0},
        }));
        for (what, f) in [
            ("mixed", mixed),
            ("no stats", None),
            ("no partition stats", no_partition_stats),
        ] {
            assert_eq!(
                file_coverage_from_stats(f.as_ref(), &HashMap::new(), "_timestamp", &part, &m),
                FileCoverage::Unknown,
                "{what}"
            );
        }
        let with_nulls = stats(serde_json::json!({
            "numRecords": 3,
            "minValues": {"part": 0, "_timestamp": "1970-01-01T00:00:00.000001Z"},
            "maxValues": {"part": 0, "_timestamp": "1970-01-01T00:00:00.000002Z"},
            "nullCount": {"part": 1, "_timestamp": 0},
        }));
        assert_eq!(
            file_coverage_from_stats(
                with_nulls.as_ref(),
                &HashMap::new(),
                "_timestamp",
                &part,
                &m
            ),
            FileCoverage::Uncovered,
            "a row with no partition is covered by nothing"
        );
    }

    #[test]
    fn a_file_mixing_partitions_past_the_highest_mark_is_settled_by_its_statistics() {
        // Marks at 100 µs and 5 µs: a mixed file whose lowest row is past 100 has every row
        // past its own partition's mark, or in a partition the target lacks.
        let d = partitioned(&[("0", 100, &[]), ("1", 5, &[])], false);
        let m = bounds(&d);
        let part = ["part".to_string()];
        let mixed = |lo: &str| {
            stats(serde_json::json!({
                "numRecords": 3,
                "minValues": {"part": 0, "_timestamp": lo},
                "maxValues": {"part": 1, "_timestamp": "1970-01-01T00:00:00.000500Z"},
                "nullCount": {"part": 0, "_timestamp": 0},
            }))
        };
        let of = |f: Option<serde_json::Value>| {
            file_coverage_from_stats(f.as_ref(), &HashMap::new(), "_timestamp", &part, &m)
        };
        assert_eq!(
            of(mixed("1970-01-01T00:00:00.000101Z")),
            FileCoverage::Uncovered
        );
        assert_eq!(
            of(mixed("1970-01-01T00:00:00.000100Z")),
            FileCoverage::Unknown,
            "at the highest mark, a row of that partition may be covered"
        );
        assert_eq!(
            of(mixed("1970-01-01T00:00:00.000050Z")),
            FileCoverage::Unknown,
            "past partition 1's mark but not partition 0's"
        );
        // No partition statistics at all: the same answer from the timestamp alone.
        let unknown_partition = stats(serde_json::json!({
            "numRecords": 3,
            "minValues": {"_timestamp": "1970-01-01T00:00:00.000101Z"},
            "maxValues": {"_timestamp": "1970-01-01T00:00:00.000500Z"},
            "nullCount": {"_timestamp": 0},
        }));
        assert_eq!(of(unknown_partition), FileCoverage::Uncovered);

        // A mark with no bound leaves nothing to compare against.
        let mut no_bound = bounds(&d);
        no_bound.bounds[0] = None;
        no_bound.highest = None;
        assert_eq!(
            file_coverage_from_stats(
                mixed("1970-01-01T00:00:00.000101Z").as_ref(),
                &HashMap::new(),
                "_timestamp",
                &part,
                &no_bound
            ),
            FileCoverage::Unknown
        );
        assert_eq!(m.highest, Some(Bound::Int(100)));
    }

    #[test]
    fn a_partition_the_target_lacks_is_remembered_for_the_warning() {
        let d = partitioned(&[("0", 100, &[])], false);
        let m = bounds(&d);
        assert_eq!(
            file_coverage_from_stats(
                one_partition(
                    7,
                    "1970-01-01T00:00:00.000001Z",
                    "1970-01-01T00:00:00.000002Z"
                )
                .as_ref(),
                &HashMap::new(),
                "_timestamp",
                &["part".to_string()],
                &m
            ),
            FileCoverage::Uncovered
        );
        assert_eq!(m.absent.get().map(String::as_str), Some("7"));
    }

    #[test]
    fn several_partition_columns_build_each_tuples_text_once_per_batch() {
        let cols = vec![
            Arc::new(StringArray::from(vec![
                Some("t"),
                Some("t"),
                None,
                Some("u"),
                Some("t"),
            ])) as ArrayRef,
            Arc::new(Int32Array::from(vec![
                Some(0),
                Some(1),
                Some(0),
                Some(0),
                Some(0),
            ])) as ArrayRef,
        ];
        let names = ["topic".to_string(), "partition".to_string()];
        let parts = PartitionText::from_columns(&names, cols).unwrap();
        let text: Vec<Option<String>> = (0..5)
            .map(|i| (!parts.is_null(i)).then(|| parts.with(i, str::to_string)))
            .collect();
        assert_eq!(
            text,
            [
                Some(partition_key(&["t", "0"])),
                Some(partition_key(&["t", "1"])),
                None,
                Some(partition_key(&["u", "0"])),
                Some(partition_key(&["t", "0"])),
            ]
        );
        let (texts, at) = parts.tuples.get().expect("built on the first lookup");
        assert_eq!(texts.len(), 3, "one text per distinct tuple, not per row");
        assert_eq!(at[0], at[4]);
        assert_eq!(at[2], u32::MAX);
    }

    #[test]
    fn a_delta_partition_value_is_exact() {
        let d = partitioned(&[("0", 100, &[]), ("1", 5, &[])], false);
        let m = bounds(&d);
        let f = stats(serde_json::json!({
            "numRecords": 2,
            "minValues": {"_timestamp": "1970-01-01T00:00:00.000010Z"},
            "maxValues": {"_timestamp": "1970-01-01T00:00:00.000050Z"},
            "nullCount": {"_timestamp": 0},
        }));
        let in_part = |p: Option<&str>| -> HashMap<String, Option<String>> {
            [("part".to_string(), p.map(str::to_string))]
                .into_iter()
                .collect()
        };
        let part = ["part".to_string()];
        assert_eq!(
            file_coverage_from_stats(f.as_ref(), &in_part(Some("0")), "_timestamp", &part, &m),
            FileCoverage::Covered { rows: 2 }
        );
        assert_eq!(
            file_coverage_from_stats(f.as_ref(), &in_part(Some("1")), "_timestamp", &part, &m),
            FileCoverage::Uncovered,
            "its lowest row is past partition 1's mark"
        );
        assert_eq!(
            file_coverage_from_stats(f.as_ref(), &in_part(None), "_timestamp", &part, &m),
            FileCoverage::Uncovered
        );
    }

    #[test]
    fn an_unsigned_partition_past_i64_still_lines_up() {
        assert_eq!(
            integer_text(&serde_json::json!(u64::MAX)).as_deref(),
            Some("18446744073709551615")
        );
        assert_eq!(integer_text(&serde_json::json!(-3)).as_deref(), Some("-3"));
        assert_eq!(integer_text(&serde_json::json!(1.5)), None);
        assert_eq!(integer_text(&serde_json::json!("0")), None);
    }

    #[test]
    fn a_commit_is_covered_only_when_every_file_is_and_it_holds_a_row() {
        use FileCoverage::*;
        assert!(commit_is_covered(&[
            Covered { rows: 2 },
            Covered { rows: 0 }
        ]));
        assert!(!commit_is_covered(&[Covered { rows: 2 }, Uncovered]));
        assert!(!commit_is_covered(&[Covered { rows: 2 }, Unknown]));
        assert!(
            !commit_is_covered(&[Covered { rows: 0 }]),
            "an empty file reads nothing"
        );
        assert!(!commit_is_covered(&[]));
    }

    #[test]
    fn only_a_plain_append_may_prove_coverage() {
        use deltalake::kernel::{Action, Add, CommitInfo, Remove};
        let add = Action::Add(Add {
            path: "a.parquet".into(),
            data_change: true,
            ..Default::default()
        });
        let remove = Action::Remove(Remove {
            path: "b.parquet".into(),
            data_change: true,
            ..Default::default()
        });
        let op = |o: &str| {
            Action::CommitInfo(CommitInfo {
                operation: Some(o.into()),
                ..Default::default()
            })
        };
        assert!(may_prove_coverage(&[op("WRITE"), add.clone()]));
        assert!(!may_prove_coverage(&[op("DELETE"), add.clone(), remove]));
        assert!(!may_prove_coverage(&[op("RESTORE"), add]));
        assert!(!may_prove_coverage(&[op("WRITE")]));
    }
}
