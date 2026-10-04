//! Delta sink: write parquet and commit the offset in the **same** Delta transaction.
//!
//! The atomicity of that single commit is the entire exactly-once argument. If the process
//! dies at any point before the commit lands, nothing was written and the offset did not
//! move. If it dies after, both moved. There is no window in which data exists without its
//! offset, or vice versa.
//!
//! That argument holds for a MERGE exactly as it does for an append —
//! `CommitProperties::with_application_transaction` puts the `txn` action in the merge's own
//! commit — with one wrinkle delta-rs introduces and [`Sink::upsert`] has to close: a merge
//! that changes nothing writes **no commit at all**, and would take the offset down with it.
//!
//! It holds for a Kafka batch too, whose offset is one number per partition rather than one
//! in all: [`Sink::commit_kafka`] writes a `txn` action for each, in the batch's own commit.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use deltalake::arrow::array::RecordBatch;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::Transaction;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use tracing::debug;

use crate::dbt::watermark::SOURCE_TABLE_ID_VERSION_KEY;
use crate::dedup::RecordedCutoff;
use crate::error::{Error, Result};
use crate::lookup::LookupSnapshot;
use crate::source::Version;
use crate::upsert::MergePlan;

pub struct Sink {
    app_id: String,
    target_file_size: Option<NonZeroU64>,
    /// Which source table this pipeline is reading, and a source version that had that id, so
    /// a later run can tell whether it is still the same one. A dropped-and-recreated source
    /// keeps its path but gets a new id, and nothing else in the log records that. A table
    /// replaced in place gets a new id as well, but keeps its log, which still gives that
    /// version the id it had there: the version is what tells the two apart.
    source_identity: Option<(Version, String)>,
    /// The exact lookup snapshots that enriched the source batch currently being committed.
    lookup_snapshots: Vec<LookupCommit>,
    /// The coverage window the batch being committed was filtered in, while that window stays
    /// open past it. Recorded so that a reopen part-way through a window carries on filtering
    /// where this commit left off, rather than re-emitting what the target already holds.
    /// See [`crate::dedup`].
    cutoff: Option<RecordedCutoff>,
}

#[derive(Clone, Debug)]
struct LookupCommit {
    name: String,
    version: i64,
    table_id: Option<String>,
    used_pre_history: bool,
    used_current: bool,
}

impl Sink {
    pub fn new(app_id: impl Into<String>, target_file_size: u64) -> Self {
        Self {
            app_id: app_id.into(),
            target_file_size: NonZeroU64::new(target_file_size),
            source_identity: None,
            lookup_snapshots: Vec::new(),
            cutoff: None,
        }
    }

    pub fn with_source_identity(mut self, identity: Option<(Version, String)>) -> Self {
        self.source_identity = identity;
        self
    }

    /// Replace the source identity recorded with the next target commit: the id the batch was
    /// read under, which differs from the one at open once the stream has read across a table
    /// replaced in place.
    pub fn set_source_identity(&mut self, identity: Option<(Version, String)>) {
        self.source_identity = identity;
    }

    /// Replace the provenance recorded with the next target commit.
    ///
    /// This is set immediately after snapshots are selected and before any work that could
    /// write the target. A retry repeats selection from the same source commit timestamp, so
    /// the metadata also tells an operator exactly which FX version produced a row.
    pub fn set_lookup_snapshots(&mut self, snapshots: &[LookupSnapshot]) {
        self.lookup_snapshots = snapshots
            .iter()
            .map(|snapshot| LookupCommit {
                name: snapshot.name.clone(),
                version: snapshot.version as i64,
                table_id: snapshot.table_id.clone(),
                used_pre_history: snapshot.used_pre_history,
                used_current: snapshot.used_current,
            })
            .collect();
    }

    /// Replace the coverage window recorded with the next target commit. `None` records none,
    /// which is what the commit that closes a window, and every commit outside one, carries.
    pub fn set_cutoff(&mut self, cutoff: Option<RecordedCutoff>) {
        self.cutoff = cutoff;
    }

    /// The commit properties every write of ours carries, whatever its shape.
    fn properties(&self, source_version: i64) -> CommitProperties {
        let mut metadata = vec![
            (
                "ddi.sourceVersion".to_string(),
                serde_json::Value::from(source_version),
            ),
            (
                "ddi.appId".to_string(),
                serde_json::Value::from(self.app_id.clone()),
            ),
        ];
        if let Some((version, id)) = &self.source_identity {
            metadata.push((
                "ddi.sourceTableId".to_string(),
                serde_json::Value::from(id.clone()),
            ));
            metadata.push((
                SOURCE_TABLE_ID_VERSION_KEY.to_string(),
                serde_json::Value::from(*version),
            ));
        }
        for lookup in &self.lookup_snapshots {
            let prefix = format!("ddi.lookup.{}", lookup.name);
            metadata.push((
                format!("{prefix}.version"),
                serde_json::Value::from(lookup.version),
            ));
            if let Some(id) = &lookup.table_id {
                metadata.push((
                    format!("{prefix}.tableId"),
                    serde_json::Value::from(id.clone()),
                ));
            }
            if lookup.used_pre_history {
                metadata.push((
                    format!("{prefix}.preHistory"),
                    serde_json::Value::from(true),
                ));
            }
            if lookup.used_current {
                metadata.push((format!("{prefix}.current"), serde_json::Value::from(true)));
            }
        }
        // On every shape of commit, the offset-only ones included: an empty append after a
        // no-op merge moves the offset exactly as far as a merge would have, so a reopen after
        // it must find the window still open just the same.
        if let Some(cutoff) = &self.cutoff {
            metadata.extend(cutoff.to_commit_metadata());
        }

        CommitProperties::default()
            .with_application_transaction(Transaction::new(&self.app_id, source_version))
            // Recorded for operators and for v2's mid-commit cursor work. Purely
            // informational today: the txn action above is the authority.
            .with_metadata(metadata)
    }

    /// The commit properties of a Kafka batch: a `txn` per asserted partition, and the
    /// evidence an operator reads beside them.
    ///
    /// No `ddi.sourceVersion`, which nothing reads back and a Kafka batch has no single value
    /// for, and none of the Delta source's provenance: a Kafka pipeline has no source table, no
    /// lookups and no coverage window.
    fn kafka_properties(&self, offsets: &KafkaOffsets) -> CommitProperties {
        // Every one stamped with a time no engine expires, unlike a Delta pipeline's `txn`,
        // which keeps none (crate::fence::KAFKA_TXN_LAST_UPDATED says why).
        let txn = |app_id: String, version: i64| {
            Transaction::new_with_last_update(
                app_id,
                version,
                Some(crate::fence::KAFKA_TXN_LAST_UPDATED),
            )
        };
        let mut txns: Vec<Transaction> = offsets
            .assert
            .iter()
            .map(|(p, last)| txn(crate::fence::txn_app_id(&self.app_id, *p), *last))
            .collect();
        // Once, in the first Kafka commit whose snapshot lacks it, and never again: in every
        // commit, a shared appId would make every two members' concurrent commits conflict.
        if offsets.takeover_marker {
            txns.push(txn(
                crate::fence::takeover_marker_app_id(&self.app_id),
                crate::fence::TAKEOVER_MARKER_VERSION,
            ));
        }
        let ranges: serde_json::Map<String, serde_json::Value> = offsets
            .ranges
            .iter()
            .map(|(p, (first, last))| (p.to_string(), serde_json::json!([first, last])))
            .collect();
        let marks: serde_json::Map<String, serde_json::Value> = offsets
            .marks
            .iter()
            .map(|(p, m)| {
                let mark = serde_json::json!([m.offset, m.timestamp_ms, m.value_len]);
                (p.to_string(), mark)
            })
            .collect();

        let mut metadata = vec![
            (
                "ddi.appId".to_string(),
                serde_json::Value::from(self.app_id.clone()),
            ),
            (
                "ddi.kafka.topic".to_string(),
                serde_json::Value::from(offsets.topic.clone()),
            ),
            (
                "ddi.kafka.groupId".to_string(),
                serde_json::Value::from(offsets.group_id.clone()),
            ),
            (
                "ddi.kafka.offsets".to_string(),
                serde_json::Value::Object(ranges),
            ),
            (
                "ddi.kafka.marks".to_string(),
                serde_json::Value::Object(marks),
            ),
        ];
        if offsets.baseline {
            metadata.push((
                "ddi.kafka.baseline".to_string(),
                serde_json::Value::from(true),
            ));
        }
        if let Some(reason) = &offsets.override_reason {
            metadata.push((
                "ddi.kafka.offsetOverride".to_string(),
                serde_json::Value::from(reason.clone()),
            ));
        }

        CommitProperties::default()
            // The plural replaces the list where the singular adds to it. The txns are the
            // authority; the metadata is evidence.
            .with_application_transactions(txns)
            .with_metadata(metadata)
            // Some(false) as well as Some(true): left as None, delta-rs follows
            // delta.enableExpiredLogCleanup, true by default, and lists the whole log after
            // every commit (docs/kafka-design.md §7.2).
            .with_cleanup_expired_logs(Some(offsets.cleanup_logs))
    }

    /// Append `batches` and record `source_version` in a `txn` action, atomically.
    ///
    /// Returns the table at its new version.
    ///
    /// `SaveMode::Append` is not a choice here. Overwrite would destroy the very history
    /// the offset refers to; replacing rows in place is [`Self::upsert`], and it is a
    /// different method rather than a save mode because it needs a key, a window, and a
    /// rule for which row wins.
    pub async fn commit(
        &self,
        table: DeltaTable,
        batches: Vec<RecordBatch>,
        source_version: i64,
    ) -> Result<DeltaTable> {
        let props = self.properties(source_version);
        let table = self.write(table, batches, props).await?;
        debug!(
            app_id = %self.app_id,
            source_version,
            target_version = ?table.version(),
            "committed data + txn atomically"
        );
        Ok(table)
    }

    /// Append `batches` and record a Kafka batch's offsets, one `txn` per partition,
    /// atomically.
    ///
    /// Returns the table at its new version. The rows are written exactly as
    /// [`Self::commit`] writes them; only what the commit records differs.
    ///
    /// Every partition in `offsets.assert` gets a `txn` action, not only those this batch
    /// read. delta-rs refuses a commit when a commit that landed after its snapshot wrote a
    /// `txn` with one of the same appIds (`ConcurrentTransaction`), so re-asserting every
    /// partition this member owns fences all of them against a second writer, and a commit
    /// whose PUT is retried after it already landed fences itself rather than landing twice.
    pub async fn commit_kafka(
        &self,
        table: DeltaTable,
        batches: Vec<RecordBatch>,
        offsets: &KafkaOffsets,
    ) -> Result<DeltaTable> {
        let props = self.kafka_properties(offsets);
        let table = self.write(table, batches, props).await?;
        debug!(
            app_id = %self.app_id,
            topic = %offsets.topic,
            partitions = offsets.ranges.len(),
            asserted = offsets.assert.len(),
            target_version = ?table.version(),
            "committed data + txns atomically"
        );
        Ok(table)
    }

    /// Append `batches` in one commit that carries `props`.
    ///
    /// Every append of ours goes through here, whatever its commit records, so that how the
    /// rows are written cannot differ between one kind of commit and another.
    async fn write(
        &self,
        table: DeltaTable,
        batches: Vec<RecordBatch>,
        props: CommitProperties,
    ) -> Result<DeltaTable> {
        // The same runtime every other session in this process gets. A plain append has little
        // to spill today — an in-memory input, and no partition columns anywhere in this
        // tool's config — but delta-rs builds its own SessionState when it is not given one,
        // with an unbounded memory pool and a DiskManager this process has never seen. A
        // runtime nobody configured is precisely the hole `crate::spill` exists to close, and
        // leaving one open because it happens to be quiet is how it gets loud later. Taken
        // before `write` moves the table, and `delta_session` rather than `session` because a
        // write is planned through delta-rs's own node — see there.
        let session = std::sync::Arc::new(crate::budget::delta_session(&table)?);

        let mut write = table
            .write(batches)
            .with_session_state(session)
            .with_save_mode(SaveMode::Append)
            // NOTE the polarity: `safe = true` makes a failed cast produce NULL, which is
            // precisely the silent data-quality failure this tool exists to avoid. `false`
            // makes it an error. No schema_mode is set, so the target schema is never
            // evolved — a mismatch fails instead (plan §2.7).
            .with_cast_safety(false)
            .with_commit_properties(props);

        if let Some(size) = self.target_file_size {
            write = write.with_target_file_size(Some(size));
        }

        write
            .await
            .map_err(|e| crate::spill::classify_delta(e, "append: writing the batch"))
    }

    /// Merge `batch` onto the keys it already has, and record `source_version`, atomically.
    ///
    /// `batch` must already hold at most one row per key — see [`crate::upsert::collapse`],
    /// which is not optional: delta-rs aborts a merge whose source matches one target row
    /// twice.
    ///
    /// # Why the version is checked afterwards
    ///
    /// delta-rs returns early without committing when a merge produced no file actions
    /// (`operations/merge/mod.rs`: `if actions.is_empty() { return Ok(...) }`), and it does
    /// that *after* the `txn` action has been attached — so the offset silently fails to
    /// advance. This is not an edge case here, it is a guaranteed one: the whole point of
    /// `s.<seq> > t.<seq>` is that re-delivered old rows change nothing, and a batch of
    /// them produces exactly zero actions. Left alone, the pipeline would re-read the same
    /// source commits forever.
    ///
    /// So when the table did not move, an empty append carries the `txn` action instead.
    /// A crash between the two is safe in the only direction that matters: the offset stays
    /// put and the merge runs again, which by key is a no-op.
    pub async fn upsert(
        &self,
        table: DeltaTable,
        batch: RecordBatch,
        source_version: i64,
        plan: &MergePlan,
    ) -> Result<(DeltaTable, UpsertStats)> {
        use deltalake::datafusion::prelude::SessionContext;

        let before = table.version();
        let rows = batch.num_rows();
        let target_schema = batch.schema();

        let session = std::sync::Arc::new(crate::budget::session(&table)?);
        let ctx = SessionContext::new_with_state((*session).clone());
        let source = ctx
            .read_batch(batch)
            .map_err(|e| Error::Other(format!("upsert: cannot register the batch: {e}")))?;

        let mut merge = table
            .merge(source, plan.predicate.clone())
            .with_source_alias(crate::upsert::SOURCE_ALIAS)
            .with_target_alias(crate::upsert::TARGET_ALIAS)
            // The append path pins both of these; the merge path needs them pinned
            // separately, because it casts inside its own UPDATE/INSERT projections rather
            // than through SchemaCoercer.
            //
            // NOTE the polarity of safe_cast, as in `commit` above: `true` would turn a
            // failed cast into a NULL, which is the silent data-quality failure this tool
            // exists to avoid. `false` makes it an error.
            .with_safe_cast(false)
            // The target schema is the contract and is never evolved. This is the merge's
            // equivalent of `commit`'s "no schema_mode is set".
            .with_merge_schema(false)
            // Leave the source stats pass on: it is what lets delta-rs turn
            // `t.key = s.key` into a static key range and skip target files by it. The
            // batch is an in-memory table, so re-planning it is cheap.
            .with_streaming(false)
            // The merge reads the target and joins it against the batch, and both of those
            // are DataFusion's to hold. Giving it the pipeline's share is what makes a
            // merge window that turned out wider than expected spill instead of OOM.
            .with_session_state(session)
            .with_commit_properties(self.properties(source_version));

        merge = merge
            .when_matched_update(|update| {
                let mut u = update.predicate(plan.newer_than_stored.clone());
                for c in &plan.update_columns {
                    u = u.update(quoted(c), source_col(c));
                }
                u
            })
            .map_err(Error::Delta)?
            .when_not_matched_insert(|insert| {
                let mut i = insert;
                for c in &plan.insert_columns {
                    i = i.set(quoted(c), source_col(c));
                }
                i
            })
            .map_err(Error::Delta)?;

        // Classified rather than wrapped: a merge joins a slice of the target against the
        // batch, so it is the largest spiller here, and a full spill directory arriving as a
        // plain Delta error is indistinguishable from a wrong answer to the retry loop.
        let (table, metrics) = merge
            .await
            .map_err(|e| crate::spill::classify_delta(e, "upsert: merging into the target"))?;

        let stats = UpsertStats {
            rows_in: rows,
            updated: metrics.num_target_rows_updated,
            inserted: metrics.num_target_rows_inserted,
            files_scanned: metrics.num_target_files_scanned,
            committed: table.version() != before,
            window_bounded: plan.window.is_bounded(),
            window_clamped: plan.window.clamped,
            // The caller owns both: the wait happened before this sink was entered, and the
            // retry loop that may run this method more than once is out there too.
            queue_millis: 0,
            merge_millis: 0,
        };

        if stats.committed {
            debug!(
                app_id = %self.app_id,
                source_version,
                target_version = ?table.version(),
                updated = stats.updated,
                inserted = stats.inserted,
                files_scanned = stats.files_scanned,
                "merged data + txn atomically"
            );
            return Ok((table, stats));
        }

        // Nothing changed, so delta-rs wrote nothing — including our txn action. Advance
        // the offset on its own, or these source commits are re-read forever.
        debug!(
            app_id = %self.app_id,
            source_version,
            rows,
            "merge changed nothing; committing the offset on its own"
        );
        let empty = RecordBatch::new_empty(target_schema);
        let table = self.commit(table, vec![empty], source_version).await?;
        Ok((table, stats))
    }
}

/// What one merge did, for logs and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpsertStats {
    /// Rows handed to the merge, after collapsing duplicate keys.
    pub rows_in: usize,
    /// Existing rows replaced by a newer one.
    pub updated: usize,
    /// Keys the target did not have.
    pub inserted: usize,
    /// Target files the merge had to open. The cost the window exists to keep down.
    pub files_scanned: usize,
    /// False when the merge changed nothing and the offset had to be committed separately.
    pub committed: bool,
    /// False when the target's statistics could not bound the window at all, so the whole
    /// table was read.
    pub window_bounded: bool,
    /// True when `upsert_lookback` held the window above what completeness asked for, so a
    /// key may have been inserted alongside an older row instead of replacing it.
    pub window_clamped: bool,
    /// Milliseconds spent waiting for a merge permit, before any work began.
    ///
    /// Filled in by the caller rather than here: the wait happens outside this sink, and a
    /// sink that timed its own queue would be reporting a wait it never did.
    pub queue_millis: u64,
    /// Milliseconds spent merging, permit in hand, including any replanned attempts. Those
    /// attempts really were spent, so counting only the last one would understate what the
    /// target cost.
    pub merge_millis: u64,
}

impl UpsertStats {
    /// Rows that actually landed. Deliberately excludes delta-rs's "copied" count, which
    /// counts untouched rows rewritten as a side effect of rewriting their file.
    pub fn rows_written(&self) -> usize {
        self.updated + self.inserted
    }
}

/// The offset a commit records: how far it has read its source.
#[derive(Clone, Debug)]
pub enum Offsets {
    /// The last source version a Delta batch read, as one `txn` under the pipeline's app_id.
    Version(i64),
    /// A Kafka batch's offsets, as one `txn` per partition.
    Kafka(KafkaOffsets),
}

/// The offsets one Kafka batch commits, and the evidence recorded beside them.
///
/// Holds no rdkafka type, so it is compiled into every build and its commits can be tested
/// without a broker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaOffsets {
    pub topic: String,
    pub group_id: String,
    /// The partitions this batch read: the first and the last offset it accounted for in
    /// each.
    pub ranges: BTreeMap<i32, (i64, i64)>,
    /// Every `txn` this commit writes, by partition: the last offset for each partition in
    /// `ranges`, and the unchanged base of every other owned partition whose base is known
    /// (docs/kafka-design.md §4.3). Always holds every key of `ranges`.
    pub assert: BTreeMap<i32, i64>,
    /// For each asserted partition where it is known, the message at the asserted offset, so
    /// that a resume can tell the topic it read from one recreated under the same name (§4.4).
    pub marks: BTreeMap<i32, MessageMark>,
    /// Whether this commit cleans up expired log files afterwards (§7.2).
    pub cleanup_logs: bool,
    /// Whether this is the txn-only commit that records where a fresh app_id starts (§4.4).
    pub baseline: bool,
    /// Why an operator moved these offsets by hand, when one did (§4.8).
    pub override_reason: Option<String>,
    /// Whether this commit writes the takeover marker, `<app_id>.ddi`: it is the first Kafka
    /// commit of this app_id whose read snapshot lacks it ([`crate::fence::Fence::takeover_marker_due`],
    /// docs/kafka-design.md §4.3).
    pub takeover_marker: bool,
}

/// A message, recorded well enough to tell whether reading its offset again returns it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageMark {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub value_len: u32,
}

/// `s."col"`, quoted so a column called `order` survives the planner.
fn source_col(name: &str) -> deltalake::datafusion::prelude::Expr {
    deltalake::datafusion::prelude::col(format!("{}.{}", crate::upsert::SOURCE_ALIAS, quoted(name)))
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use deltalake::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use deltalake::arrow::datatypes::SchemaRef;
    use deltalake::delta_datafusion::DataFusionMixins;
    use deltalake::kernel::transaction::{CommitConflictError, TransactionError};
    use deltalake::kernel::{Action, DataType, PrimitiveType, StructField};
    use deltalake::logstore::object_store::{
        self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use deltalake::protocol::SaveMode;
    use deltalake::{ensure_table_uri, DeltaTable, DeltaTableError, Path};
    use futures::stream::BoxStream;
    use futures::TryStreamExt;

    use super::{KafkaOffsets, MessageMark, Sink};
    use crate::error::Error;

    async fn create(path: &str, partitioned: bool) -> DeltaTable {
        let mut columns = vec![
            StructField::new("id", DataType::Primitive(PrimitiveType::Long), false),
            StructField::new("name", DataType::Primitive(PrimitiveType::String), true),
        ];
        let mut partition_columns = Vec::new();
        if partitioned {
            columns.push(StructField::new(
                "region",
                DataType::Primitive(PrimitiveType::String),
                true,
            ));
            partition_columns.push("region");
        }
        DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
            .await
            .unwrap()
            .create()
            .with_columns(columns)
            .with_partition_columns(partition_columns)
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap()
    }

    async fn open(path: &str) -> DeltaTable {
        deltalake::open_table(ensure_table_uri(path).unwrap())
            .await
            .unwrap()
    }

    /// The table's schema as the pipeline reads it, which is what its batches are built in.
    fn schema_of(table: &DeltaTable) -> SchemaRef {
        table.snapshot().unwrap().snapshot().read_schema()
    }

    /// One row per id, in every column the table has.
    fn rows(table: &DeltaTable, ids: &[i64]) -> RecordBatch {
        let schema = schema_of(table);
        let columns = schema
            .fields()
            .iter()
            .map(|f| -> ArrayRef {
                match f.name().as_str() {
                    "id" => Arc::new(Int64Array::from(ids.to_vec())),
                    "name" => Arc::new(StringArray::from_iter_values(
                        ids.iter().map(|i| format!("row-{i}")),
                    )),
                    "region" => Arc::new(StringArray::from(vec!["eu"; ids.len()])),
                    other => panic!("no test values for column {other:?}"),
                }
            })
            .collect();
        RecordBatch::try_new(schema, columns).unwrap()
    }

    /// A batch that read `ranges` and asserts each at its last offset, with no other owned
    /// partitions and no evidence beyond the ranges.
    fn read(ranges: &[(i32, (i64, i64))]) -> KafkaOffsets {
        KafkaOffsets {
            topic: "orders".into(),
            group_id: "ddi-orders".into(),
            ranges: ranges.iter().copied().collect(),
            assert: ranges.iter().map(|(p, (_, last))| (*p, *last)).collect(),
            marks: BTreeMap::new(),
            cleanup_logs: false,
            baseline: false,
            override_reason: None,
            takeover_marker: false,
        }
    }

    async fn txn(table: &DeltaTable, app_id: &str) -> Option<i64> {
        table
            .snapshot()
            .unwrap()
            .transaction_version(table.log_store().as_ref(), app_id)
            .await
            .unwrap()
    }

    async fn actions_at(table: &DeltaTable, version: u64) -> Vec<Action> {
        let raw = table
            .log_store()
            .read_commit_entry(version)
            .await
            .unwrap()
            .expect("the commit exists");
        deltalake::logstore::get_actions(version, &raw).unwrap()
    }

    fn commit_info(actions: &[Action]) -> &std::collections::HashMap<String, serde_json::Value> {
        actions
            .iter()
            .find_map(|a| match a {
                Action::CommitInfo(ci) => Some(&ci.info),
                _ => None,
            })
            .expect("every commit of ours has a commitInfo")
    }

    async fn ids_in(table: &DeltaTable) -> Vec<i64> {
        let (_, stream) = table.scan_table().await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let id = b.column_by_name("id").unwrap().as_any();
                id.downcast_ref::<Int64Array>().unwrap().values().to_vec()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    fn is_concurrent_transaction(e: &Error) -> bool {
        matches!(
            e,
            Error::Delta(DeltaTableError::Transaction {
                source: TransactionError::CommitConflict(
                    CommitConflictError::ConcurrentTransaction
                ),
            })
        )
    }

    #[tokio::test]
    async fn kafka_commit_writes_n_txns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create(path, false).await;

        let mut offsets = read(&[(0, (5, 9)), (1, (3, 4))]);
        // Partition 2 is owned but was not in this batch: it is re-asserted at its base.
        offsets.assert.insert(2, 17);
        offsets.marks.insert(
            0,
            MessageMark {
                offset: 9,
                timestamp_ms: 1_700_000_000_000,
                value_len: 42,
            },
        );
        let batch = rows(&table, &[1, 2, 3]);
        let table = Sink::new("app", 0)
            .commit_kafka(table, vec![batch], &offsets)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1));

        let table = open(path).await;
        assert_eq!(txn(&table, "app-0").await, Some(9));
        assert_eq!(txn(&table, "app-1").await, Some(4));
        assert_eq!(txn(&table, "app-2").await, Some(17));
        assert_eq!(
            txn(&table, "app").await,
            None,
            "nothing under the bare app_id, which is a Delta pipeline's offset"
        );
        assert_eq!(ids_in(&table).await, vec![1, 2, 3]);

        let actions = actions_at(&table, 1).await;
        let txns: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Txn(t) => Some(t),
                _ => None,
            })
            .collect();
        assert_eq!(txns.len(), 3, "one txn per asserted partition: {txns:?}");
        assert!(
            txns.iter()
                .all(|t| t.last_updated == Some(crate::fence::KAFKA_TXN_LAST_UPDATED)),
            "stamped with a time no engine expires: {txns:?}"
        );

        let info = commit_info(&actions);
        assert_eq!(info["ddi.appId"], serde_json::json!("app"));
        assert_eq!(info["ddi.kafka.topic"], serde_json::json!("orders"));
        assert_eq!(info["ddi.kafka.groupId"], serde_json::json!("ddi-orders"));
        assert_eq!(
            info["ddi.kafka.offsets"],
            serde_json::json!({"0": [5, 9], "1": [3, 4]})
        );
        assert_eq!(
            info["ddi.kafka.marks"],
            serde_json::json!({"0": [9, 1_700_000_000_000_i64, 42]})
        );
        for absent in [
            "ddi.sourceVersion",
            "ddi.kafka.baseline",
            "ddi.kafka.offsetOverride",
        ] {
            assert!(!info.contains_key(absent), "{absent} in {info:?}");
        }
    }

    /// A batch with nothing to write still moves its partitions' offsets: a zero-row batch in
    /// the target's schema commits the txns and no file.
    async fn empty_commit_carries_txns_only(partitioned: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create(path, partitioned).await;

        let mut offsets = read(&[(0, (10, 12)), (3, (7, 7))]);
        offsets.override_reason = Some("acknowledged skip".into());
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new("app", 0)
            .commit_kafka(table, vec![empty], &offsets)
            .await
            .unwrap();

        let table = open(path).await;
        assert_eq!(table.version(), Some(1));
        assert_eq!(txn(&table, "app-0").await, Some(12));
        assert_eq!(txn(&table, "app-3").await, Some(7));
        let actions = actions_at(&table, 1).await;
        assert!(
            actions
                .iter()
                .all(|a| matches!(a, Action::Txn(_) | Action::CommitInfo(_))),
            "only txns and commitInfo: {actions:?}"
        );
        assert_eq!(
            actions
                .iter()
                .filter(|a| matches!(a, Action::Txn(_)))
                .count(),
            2
        );
        let info = commit_info(&actions);
        assert_eq!(
            info["ddi.kafka.offsetOverride"],
            serde_json::json!("acknowledged skip")
        );
        assert!(ids_in(&table).await.is_empty());
    }

    #[tokio::test]
    async fn kafka_empty_commit_carries_txns_only() {
        empty_commit_carries_txns_only(false).await;
    }

    #[tokio::test]
    async fn kafka_empty_commit_carries_txns_only_on_a_partitioned_target() {
        empty_commit_carries_txns_only(true).await;
    }

    /// How many files one commit of 6000 rows adds, written by a sink with
    /// `target_file_size`, as a Delta batch or as a Kafka one.
    async fn files_added_with(target_file_size: u64, kafka: bool) -> usize {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create(path, false).await;
        let batches: Vec<RecordBatch> = (0..3)
            .map(|i| rows(&table, &(i * 2000..(i + 1) * 2000).collect::<Vec<i64>>()))
            .collect();
        let sink = Sink::new("app", target_file_size);
        let table = match kafka {
            false => sink.commit(table, batches, 7).await.unwrap(),
            true => sink
                .commit_kafka(table, batches, &read(&[(0, (0, 5999))]))
                .await
                .unwrap(),
        };
        assert_eq!(table.version(), Some(1));
        actions_at(&table, 1)
            .await
            .iter()
            .filter(|a| matches!(a, Action::Add(_)))
            .count()
    }

    /// A pipeline's `target_file_size` reaches the writer, on both kinds of commit. Every
    /// pipeline the suite runs sets 128 MiB, which a test's few rows never come near, so
    /// nothing else would notice the setting going missing: delta-rs would then split files
    /// at its own default instead, and every commit would look the same.
    #[tokio::test]
    async fn target_file_size_splits_a_commits_files() {
        for kafka in [false, true] {
            let small = files_added_with(1, kafka).await;
            assert!(
                small > 1,
                "a 1-byte target splits the write: {small} file(s)"
            );
            // 0 sets nothing, which is what a dropped setting looks like.
            assert_eq!(files_added_with(0, kafka).await, 1, "kafka: {kafka}");
        }
    }

    /// A commit asked to write the takeover marker writes it beside the partitions' txns, as
    /// `<app_id>.ddi` at version 1, and one not asked writes none.
    #[tokio::test]
    async fn kafka_commit_writes_the_takeover_marker_when_asked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create(path, false).await;

        let mut offsets = read(&[(0, (0, 4))]);
        offsets.takeover_marker = true;
        let batch = rows(&table, &[1]);
        let sink = Sink::new("app", 0);
        let table = sink
            .commit_kafka(table, vec![batch], &offsets)
            .await
            .unwrap();
        let batch = rows(&table, &[2]);
        let table = sink
            .commit_kafka(table, vec![batch], &read(&[(0, (5, 6))]))
            .await
            .unwrap();

        let txns = |actions: Vec<Action>| -> Vec<(String, i64)> {
            actions
                .into_iter()
                .filter_map(|a| match a {
                    Action::Txn(t) => Some((t.app_id, t.version)),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(
            txns(actions_at(&table, 1).await),
            vec![("app-0".to_string(), 4), ("app.ddi".to_string(), 1)]
        );
        assert_eq!(
            txns(actions_at(&table, 2).await),
            vec![("app-0".to_string(), 6)]
        );
        assert_eq!(txn(&table, "app.ddi").await, Some(1));
    }

    #[tokio::test]
    async fn kafka_baseline_commit_is_marked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create(path, true).await;

        // A fresh app_id starting at offset 100 of partition 0 and 0 of partition 1: only
        // the first has an offset before its start to record.
        let mut offsets = read(&[]);
        offsets.assert.insert(0, 99);
        offsets.baseline = true;
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new("app", 0)
            .commit_kafka(table, vec![empty], &offsets)
            .await
            .unwrap();

        let table = open(path).await;
        assert_eq!(txn(&table, "app-0").await, Some(99));
        assert_eq!(txn(&table, "app-1").await, None);
        let actions = actions_at(&table, 1).await;
        let info = commit_info(&actions);
        assert_eq!(info["ddi.kafka.baseline"], serde_json::json!(true));
        assert_eq!(info["ddi.kafka.offsets"], serde_json::json!({}));
    }

    /// Pins the behaviour of delta-rs the Kafka fence rests on (docs/kafka-design.md §4.3): a
    /// commit carrying a txn that another commit wrote after its snapshot is refused. A
    /// delta-rs upgrade that stopped refusing it would let a stale member write offsets the
    /// owner has already written past, and this is the test that would say so.
    #[tokio::test]
    async fn fence_concurrent_txn_rejected_by_delta_rs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path, false).await;
        // Three handles on version 0.
        let (first, second, third) = (open(path).await, open(path).await, open(path).await);
        let sink = Sink::new("a", 0);

        let batch = rows(&first, &[1]);
        sink.commit_kafka(first, vec![batch], &read(&[(0, (0, 4))]))
            .await
            .unwrap();

        let batch = rows(&second, &[2]);
        let e = sink
            .commit_kafka(second, vec![batch], &read(&[(0, (0, 6))]))
            .await
            .unwrap_err();
        assert!(is_concurrent_transaction(&e), "got: {e}");

        // The control: on the same old snapshot, a commit that asserts only another partition
        // is rebased over the first and lands. What refused the second was the txn, not the
        // append.
        let batch = rows(&third, &[3]);
        sink.commit_kafka(third, vec![batch], &read(&[(1, (0, 2))]))
            .await
            .unwrap();

        let table = open(path).await;
        assert_eq!(table.version(), Some(2));
        assert_eq!(txn(&table, "a-0").await, Some(4));
        assert_eq!(txn(&table, "a-1").await, Some(2));
        assert_eq!(ids_in(&table).await, vec![1, 3]);
    }

    /// An object store whose first conditional PUT under `_delta_log/` lands and then reports
    /// that the object already exists, which is what a PUT retried after its response was lost
    /// gets back. Everything else passes straight through.
    #[derive(Debug)]
    struct LandsThenSaysItExists {
        inner: Arc<dyn ObjectStore>,
        tripped: AtomicBool,
    }

    impl std::fmt::Display for LandsThenSaysItExists {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "LandsThenSaysItExists({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for LandsThenSaysItExists {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let conditional = matches!(opts.mode, PutMode::Create);
            let put = self.inner.put_opts(location, payload, opts).await?;
            if conditional
                && location.as_ref().starts_with("_delta_log/")
                && !self.tripped.swap(true, Ordering::SeqCst)
            {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "the response to the first attempt was lost".into(),
                });
            }
            Ok(put)
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A commit that landed, but whose writer was told the version already exists, goes round
    /// delta-rs's conflict loop again and meets itself there: its own txns are in the winning
    /// commit, so it is refused rather than written a second time (docs/kafka-design.md §4.7
    /// row 3).
    #[tokio::test]
    async fn retried_put_of_own_commit_is_concurrent_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path, false).await;

        let plain = open(path).await;
        let store = plain.log_store();
        let flaky = deltalake::logstore::default_logstore(
            Arc::new(LandsThenSaysItExists {
                inner: store.object_store(None),
                tripped: AtomicBool::new(false),
            }),
            store.root_object_store(None),
            store.config().location(),
            store.config().options(),
        );
        let mut table = DeltaTable::new(flaky, plain.config.clone());
        table.load().await.unwrap();

        let batch = rows(&table, &[1, 2]);
        let e = Sink::new("app", 0)
            .commit_kafka(table, vec![batch], &read(&[(0, (0, 4)), (1, (0, 2))]))
            .await
            .unwrap_err();
        assert!(is_concurrent_transaction(&e), "got: {e}");

        // It landed, and only once.
        let table = open(path).await;
        assert_eq!(table.version(), Some(1));
        assert_eq!(txn(&table, "app-0").await, Some(4));
        assert_eq!(txn(&table, "app-1").await, Some(2));
        assert_eq!(ids_in(&table).await, vec![1, 2]);
    }
}
