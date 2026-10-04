//! The data-quality table: where rows that will not fit the target go instead of stopping
//! the world.
//!
//! # Why this exists
//!
//! A cast failure used to be a pipeline failure, on the argument that input is typed
//! Parquet rather than arbitrary JSON, so a value that will not convert means something is
//! genuinely wrong. That argument holds for one pipeline watched by a person. It does not
//! survive three hundred of them: the chance that *some* stream meets a bad row approaches
//! one, and "stop and wait for a human" turns a single malformed value into an outage.
//!
//! So the rejects go somewhere durable and the rest of the batch commits. What is *not*
//! given up is the part that matters — no value is ever nulled to make it fit, and no row is
//! silently dropped. Every rejected row is in a table you can query, with the reason
//! attached. See [`crate::schema::SchemaCoercer::coerce_quarantining`].
//!
//! A row the transform itself cannot evaluate — a cast that does not parse, a date past what
//! the engine represents — comes here too, found by
//! [`crate::transform::Transform::apply_isolating`]. It has no output row to show, so its
//! payload is the source row and its `column_name` is NULL.
//!
//! # Where it lives
//!
//! `<target_uri>__ddi_dq`, unless `dq_uri` says otherwise. Deriving it means a fleet of
//! pipelines needs no per-pipeline configuration, and the table sits next to the data it
//! failed to become.
//!
//! Like every other table here, this one is never created — create it with external tooling
//! (see [`SCHEMA`]). A pipeline whose table does not exist keeps the old behaviour: bad rows
//! stop it, and it retries. That is deliberate rather than a fallback, because quietly
//! discarding rejects because a table was missing is the one outcome worse than stopping.
//!
//! # Exactly-once, across two tables
//!
//! The rejects cannot ride in the target's commit — a Delta commit covers one table. So the
//! order is chosen instead: **the DQ table is written first, then the target and its
//! offset.** A crash in between replays the batch, which can duplicate a reject but can
//! never lose one. That is the same asymmetry [`crate::dbt::watermark`] argues for: a
//! duplicate is visible and repairable, a gap is neither.
//!
//! Even that duplicate is usually avoided. The DQ commit carries a `txn` action of its own,
//! under `<app_id>.dq`, holding the source version it covers. Before writing, that version
//! is read back; if the rejects for this batch are already there, the write is skipped. It
//! is the same trick the pipeline uses for its own offset, pointed at a second table.
//!
//! That skip is keyed on the version alone, so "never lose one" holds only while a replay
//! rejects the same rows the first attempt did — which it does for a deterministic
//! transform over pinned lookups. A model that reads `now()` or `random()` into a value that
//! can fail, or a lookup running on `use_current` whose head moved in between, can reject a
//! row on the replay that the first attempt did not, and that row then reaches neither
//! table.
//!
//! # Kafka: a mark per partition
//!
//! A Kafka batch has no single version to key that skip on, and the members of a consumer
//! group write one DQ table side by side, so one shared `txn` would make every pair of them
//! conflict. [`DataQuality::write_kafka`] keeps one mark per partition instead, under
//! `<app_id>.dq-<p>` ([`kafka_app_id_for`]): the last offset of that partition whose rejects
//! are recorded. Members own disjoint partitions, so their marks never collide.
//!
//! The skip is per row, and `<=` rather than the equality above, because the argument for
//! equality does not carry over: a source version can mean different data after a rewind,
//! but an offset names one message for the life of a topic, a resume checks it is still the
//! same topic, and moving offsets by hand moves these marks with them
//! (docs/kafka-design.md §5.7).
//!
//! Only a message that could not be decoded is skipped, when its offset is at or below its
//! partition's mark: whether it decodes depends on its bytes and its writer schema alone,
//! and both are fixed for good. Every other reject was judged against things a replay can
//! find changed — the target's schema after an ALTER TABLE, the union of writer schemas,
//! the SQL, a value the SQL computes from `now()` — so a retry can reject a row at an offset
//! below the mark that the first attempt took, and skipping it would leave it in neither
//! table. Those, a message whose writer schema the registry did not have, and a reject that
//! names no offset are always written: a duplicate row is visible, a lost one is not.

use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::transaction::{CommitProperties, TransactionError};
use deltalake::kernel::{DataType as DeltaType, PrimitiveType, StructField, Transaction};
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, DeltaTableError};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::fence::{self, LookupCounters};
use crate::schema::Rejected;
use crate::sink::KafkaOffsets;

/// The suffix appended to a target's URI to find its data-quality table.
pub const SUFFIX: &str = "__ddi_dq";

/// Where a pipeline's rejects go, unless it says otherwise.
pub fn uri_for(target_uri: &str) -> String {
    format!("{}{SUFFIX}", target_uri.trim_end_matches('/'))
}

/// The `txn` app id the DQ table's own offset is stored under.
///
/// Distinct from the pipeline's, because the two tables advance independently: a batch with
/// no rejects writes nothing here, so this version legitimately lags the target's.
pub fn app_id_for(app_id: &str) -> String {
    format!("{app_id}.dq")
}

/// The `txn` appId a Kafka pipeline's mark for one partition is stored under in its DQ
/// table: the last offset of `partition` whose rejects are recorded there.
///
/// One per partition rather than one per pipeline, because the members of a consumer group
/// each write their own partitions' rejects, and would otherwise all conflict on one appId.
pub fn kafka_app_id_for(app_id: &str, partition: i32) -> String {
    format!("{app_id}.dq-{partition}")
}

/// How the reason a Kafka message is rejected with begins when the message could not be
/// decoded at all (docs/kafka-design.md §5.7). No other reject's reason begins so.
///
/// The only rejects [`DataQuality::write_kafka`] skips on a replay, when they are at or
/// below their partition's mark: whether a message decodes depends on nothing but its bytes
/// and its writer schema, which no retry changes.
pub const KAFKA_UNDECODABLE: &str = "avro:";

/// How the reason a Kafka message is rejected with begins when its writer schema was not in
/// the registry.
///
/// The one undecodable never skipped on a replay ([`DataQuality::write_kafka`]): the schema
/// can be registered between two attempts, and the same message then decodes.
pub const KAFKA_SCHEMA_NOT_FOUND: &str = "avro: schema not found";

/// The prefix of the columns a Kafka reject carries its coordinates in, until the pipeline
/// says otherwise (`metadata_prefix`).
pub const KAFKA_METADATA_PREFIX: &str = "kafka_";

/// How often a Kafka DQ write that conflicted is tried again in place, before it is left to
/// the pipeline's own retry.
const KAFKA_CONFLICT_RETRIES: u32 = 3;

/// The table's columns, for whoever creates it.
///
/// Deliberately flat and writer-agnostic — no nested types, no engine-specific spellings —
/// so any tool can create it and any engine can read it:
///
/// ```sql
/// CREATE TABLE silver.orders__ddi_dq (
///   app_id          VARCHAR,
///   pipeline        VARCHAR,
///   source_version  BIGINT,   -- the batch's last source version, not the row's:
///                             -- a batch may span several commits
///   column_name     VARCHAR,  -- NULL when the transform could not evaluate the row
///   reason          VARCHAR,
///   payload         VARCHAR,  -- the row as JSON: the transform's output, or the source
///                             -- row when the transform could not evaluate it
///   _timestamp      TIMESTAMP(6)
/// ) WITH (location = '.../silver/orders__ddi_dq')
/// ```
pub fn columns() -> Vec<StructField> {
    vec![
        StructField::new("app_id", DeltaType::Primitive(PrimitiveType::String), false),
        StructField::new(
            "pipeline",
            DeltaType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "source_version",
            DeltaType::Primitive(PrimitiveType::Long),
            false,
        ),
        StructField::new(
            "column_name",
            DeltaType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new("reason", DeltaType::Primitive(PrimitiveType::String), false),
        StructField::new("payload", DeltaType::Primitive(PrimitiveType::String), true),
        StructField::new(
            "_timestamp",
            DeltaType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
    ]
}

/// The same shape, in Arrow, for building the batch to write.
pub fn arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("app_id", DataType::Utf8, false),
        Field::new("pipeline", DataType::Utf8, true),
        Field::new("source_version", DataType::Int64, false),
        Field::new("column_name", DataType::Utf8, true),
        Field::new("reason", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, true),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// Writes rejected rows to a pipeline's data-quality table.
pub struct DataQuality {
    app_id: String,
    dq_app_id: String,
    pipeline: String,
    uri: String,
    storage: crate::storage::Storage,
    table: DeltaTable,
    /// `<prefix>partition` and `<prefix>offset` are where a Kafka reject's coordinates are.
    kafka_prefix: String,
    kafka: Arc<KafkaCounters>,
}

/// What recording a Kafka pipeline's rejects has cost and saved, for
/// `ddi_kafka_dq_rows_skipped_total`, `ddi_kafka_dq_conflict_retries_total` and the offset
/// lookups its marks take.
#[derive(Debug, Default)]
pub struct KafkaCounters {
    rows_skipped: AtomicU64,
    conflict_retries: AtomicU64,
    lookups: LookupCounters,
}

impl KafkaCounters {
    /// Rejects not written because their partition's mark says they already were.
    pub fn rows_skipped(&self) -> u64 {
        self.rows_skipped.load(Ordering::Relaxed)
    }

    /// Writes tried again after a conflict on a mark.
    pub fn conflict_retries(&self) -> u64 {
        self.conflict_retries.load(Ordering::Relaxed)
    }

    /// The reads of the marks.
    pub fn lookups(&self) -> &LookupCounters {
        &self.lookups
    }
}

impl DataQuality {
    /// Open the table, or report that there is none.
    ///
    /// `Ok(None)` means no table exists at `uri`, which is not an error: the pipeline simply
    /// keeps failing on bad rows. Anything else — a table that is there but unreadable — is,
    /// because silently treating it as absent would discard the rejects.
    pub async fn open(
        storage: &crate::storage::Storage,
        uri: &str,
        app_id: &str,
        pipeline: &str,
    ) -> Result<Option<Self>> {
        match storage.open(uri).await {
            Ok(table) => Ok(Some(Self {
                app_id: app_id.to_string(),
                dq_app_id: app_id_for(app_id),
                pipeline: pipeline.to_string(),
                uri: uri.to_string(),
                storage: storage.clone(),
                table,
                kafka_prefix: KAFKA_METADATA_PREFIX.to_string(),
                kafka: Arc::new(KafkaCounters::default()),
            })),
            Err(_) => Ok(None),
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Read a Kafka reject's coordinates from `<prefix>partition` and `<prefix>offset`.
    pub fn set_kafka_metadata_prefix(&mut self, prefix: impl Into<String>) {
        self.kafka_prefix = prefix.into();
    }

    /// The counters [`Self::write_kafka`] adds to.
    pub fn kafka_counters(&self) -> &Arc<KafkaCounters> {
        &self.kafka
    }

    /// Write every reject in this batch for `source_version`, unless they are already there.
    ///
    /// Returns how many rows were written — zero when a replay found them already recorded.
    ///
    /// Takes the whole slice rather than one at a time on purpose. The idempotence check
    /// below is keyed on `source_version`, so a second call for the same version would find
    /// the first call's `txn` action and skip — silently losing every reject after the
    /// first. One version, one commit.
    pub async fn write(
        &mut self,
        rejected: &[Rejected],
        source_version: i64,
        now_micros: i64,
    ) -> Result<usize> {
        let rows: usize = rejected.iter().map(Rejected::len).sum();
        if rows == 0 {
            return Ok(0);
        }
        if self.already_recorded(source_version).await? {
            debug!(
                app_id = %self.app_id,
                source_version,
                rows,
                "rejects for this batch are already in the data-quality table; not writing \
                 them twice"
            );
            return Ok(0);
        }

        let batch = self.batch(rejected, source_version, now_micros)?;

        let table = std::mem::replace(&mut self.table, DeltaTable::new_in_memory());
        let props = CommitProperties::default()
            .with_application_transaction(Transaction::new(&self.dq_app_id, source_version))
            .with_metadata([
                (
                    "ddi.sourceVersion".to_string(),
                    serde_json::Value::from(source_version),
                ),
                (
                    "ddi.appId".to_string(),
                    serde_json::Value::from(self.app_id.clone()),
                ),
            ]);

        // As in `Sink::commit`: without this delta-rs builds a runtime with an unbounded
        // memory pool and its own private spill budget, outside everything `crate::budget` and
        // `crate::spill` account for. A quarantine write is small, and "small" is not a bound.
        let session = std::sync::Arc::new(crate::budget::delta_session(&table)?);

        match table
            .write(vec![batch])
            .with_session_state(session)
            .with_save_mode(SaveMode::Append)
            .with_cast_safety(false)
            .with_commit_properties(props)
            .await
        {
            Ok(t) => {
                self.table = t;
                Ok(rows)
            }
            Err(e) => {
                // Reopen from storage rather than leaving an in-memory stand-in behind: a
                // scratch table would accept the next write and lose it, which is the one
                // outcome this module exists to prevent. If the reopen fails too, the error
                // below still stops the batch.
                self.table = self
                    .storage
                    .open(&self.uri)
                    .await
                    .unwrap_or_else(|_| DeltaTable::new_in_memory());
                Err(Error::Other(format!(
                    "cannot write {rows} rejected row(s) to the data-quality table {:?}: {e}. \
                     The batch was not committed, so nothing was lost — but this pipeline \
                     cannot make progress until the table is writable.",
                    self.uri
                )))
            }
        }
    }

    /// Has *this* batch's rejects already landed? The DQ table's own `txn` action says so.
    ///
    /// # Why the version must match exactly
    ///
    /// The obvious test is `stored >= source_version` — "we are already past this" — and it
    /// is wrong, because a source version number only means something within one incarnation
    /// of one table. A source that is dropped and recreated restarts its log at zero, and
    /// [`crate::pipeline`] rightly rewinds to replay it; a dbt rebuild rewinds too. Under
    /// `>=`, a stored version of 500 would then swallow the rejects of every replayed batch
    /// below it — silently, and precisely when a table is being rebuilt and rejects matter
    /// most.
    ///
    /// Equality skips exactly one thing: the batch that was just written when the process
    /// died before the target could commit. That is the whole window this check exists for.
    /// Every other repeat writes again, which costs a duplicate row — visible, and
    /// deduplicable by `(app_id, source_version, payload)`.
    async fn already_recorded(&self, source_version: i64) -> Result<bool> {
        let Ok(snapshot) = self.table.snapshot() else {
            return Ok(false); // never written to
        };
        let stored = snapshot
            .transaction_version(self.table.log_store().as_ref(), &self.dq_app_id)
            .await
            .map_err(Error::Delta)?;
        Ok(stored == Some(source_version))
    }

    fn batch(
        &self,
        rejected: &[Rejected],
        source_version: i64,
        now_micros: i64,
    ) -> Result<RecordBatch> {
        let mut columns = Vec::new();
        let mut reasons = Vec::new();
        let mut payload = Vec::new();
        for r in rejected {
            columns.extend(r.columns.iter().cloned());
            reasons.extend(r.reasons.iter().cloned());
            payload.extend(payloads(&r.rows)?);
        }
        let n = reasons.len();
        self.assemble(
            vec![source_version; n],
            columns,
            reasons,
            payload,
            now_micros,
        )
    }

    /// The data-quality batch for these rows, one `source_version` each.
    fn assemble(
        &self,
        source_versions: Vec<i64>,
        columns: Vec<Option<String>>,
        reasons: Vec<String>,
        payload: Vec<Option<String>>,
        now_micros: i64,
    ) -> Result<RecordBatch> {
        let n = reasons.len();
        RecordBatch::try_new(
            arrow_schema(),
            vec![
                Arc::new(StringArray::from(vec![self.app_id.as_str(); n])) as ArrayRef,
                Arc::new(StringArray::from(vec![self.pipeline.as_str(); n])) as ArrayRef,
                Arc::new(Int64Array::from(source_versions)) as ArrayRef,
                Arc::new(StringArray::from(columns)) as ArrayRef,
                Arc::new(StringArray::from(reasons)) as ArrayRef,
                Arc::new(StringArray::from(payload)) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![now_micros; n])) as ArrayRef,
            ],
        )
        .map_err(|e| Error::Other(format!("cannot assemble the data-quality batch: {e}")))
    }

    /// Write the rejects of a Kafka batch that read `offsets.ranges`, except those already
    /// recorded, and move each of its partitions' marks to the last offset it read.
    ///
    /// Returns how many rows were written. In steps (docs/kafka-design.md §5.7):
    /// 1. Read the marks `<app_id>.dq-<p>` of the batch's partitions from this table's handle
    ///    (`fence::read_own_txns`: a mark never expires, so
    ///    `delta.setTransactionRetentionDuration` is not refused here, as it is on a target).
    /// 2. Skip a reject recorded at or below its partition's mark: one with coordinates — its
    ///    `<prefix>partition` and `<prefix>offset` columns, naming an offset the batch read —
    ///    that could not be decoded ([`KAFKA_UNDECODABLE`]), unless its writer schema was not
    ///    found ([`KAFKA_SCHEMA_NOT_FOUND`]). Only those are rejected again on a replay,
    ///    whatever has changed in between; any other reject is written each time.
    /// 3. Commit the rest, each row's `source_version` its offset (−1 without one), with
    ///    `<app_id>.dq-<p>` = max(mark, last offset read) for every partition of the batch.
    /// 4. A commit that conflicts on a mark reopens the table. When the commit that won is
    ///    this write's own — it landed, and the acknowledgement of its PUT was lost — the
    ///    write is done ([`Self::landed`]). When it is another writer's — a zombie, or a
    ///    handover — the write goes back to step 1, up to [`KAFKA_CONFLICT_RETRIES`] times.
    ///    After that it is [`Error::DataQualityConflict`], for the pipeline to retry, never an
    ///    error that replans the target.
    ///
    /// Every other failure is [`Error::DataQualityWrite`], with what failed beneath it as it
    /// was raised — delta-rs's error, `Capacity`, or the `OffsetLogTruncated` of a handle left
    /// behind by log cleanup — so that the pipeline can tell a fault worth retrying from a
    /// table it cannot write, and neither from a failure of the target (§3.6).
    ///
    /// [`Self::write`] and its equality rule are untouched by any of this.
    pub async fn write_kafka(
        &mut self,
        rejected: &[Rejected],
        offsets: &KafkaOffsets,
        now_micros: i64,
    ) -> Result<usize> {
        let rows: usize = rejected.iter().map(Rejected::len).sum();
        if rows == 0 {
            return Ok(0);
        }
        let coordinates: Vec<Vec<Option<(i32, i64)>>> = rejected
            .iter()
            .map(|r| self.coordinates(r, &offsets.ranges))
            .collect();
        // Recorded with the commit, so that a conflict can be told from this write's own
        // commit having landed already (`Self::landed`).
        let write_id = write_id();

        let mut retries = 0;
        loop {
            let read_version = self.table.version();
            match self
                .try_write_kafka(rejected, &coordinates, offsets, &write_id, now_micros)
                .await
            {
                Ok(written) => return Ok(written),
                Err(Attempt::Conflict {
                    error: e,
                    written,
                    skipped,
                }) => {
                    self.table = match self.storage.open(&self.uri).await {
                        Ok(table) => table,
                        Err(cause) => return Err(self.unwritable(written, cause)),
                    };
                    let landed = match self.landed(read_version, &write_id).await {
                        Ok(landed) => landed,
                        Err(cause) => return Err(self.unwritable(written, cause)),
                    };
                    if landed {
                        debug!(
                            app_id = %self.app_id,
                            dq_uri = %self.uri,
                            "the commit reported as a conflict ({e}) is this write's own, which \
                             landed before its acknowledgement was lost; not writing it twice"
                        );
                        self.kafka
                            .rows_skipped
                            .fetch_add(skipped, Ordering::Relaxed);
                        return Ok(written);
                    }
                    if retries < KAFKA_CONFLICT_RETRIES {
                        retries += 1;
                        self.kafka.conflict_retries.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            app_id = %self.app_id,
                            dq_uri = %self.uri,
                            attempt = retries,
                            "another writer recorded rejects for one of this batch's \
                             partitions first ({e}); re-reading the marks and trying again"
                        );
                        continue;
                    }
                    return Err(Error::DataQualityConflict {
                        dq_uri: self.uri.clone(),
                        rows: written,
                        attempts: KAFKA_CONFLICT_RETRIES + 1,
                        detail: e.to_string(),
                    });
                }
                Err(Attempt::Failed { rows, cause }) => {
                    // As in `write`: the next attempt starts from the table as it is now, so
                    // a handle that has fallen behind log cleanup does not fail it again.
                    if let Ok(table) = self.storage.open(&self.uri).await {
                        self.table = table;
                    }
                    return Err(self.unwritable(rows, cause));
                }
            }
        }
    }

    /// One pass of steps 1-3 of [`Self::write_kafka`].
    async fn try_write_kafka(
        &mut self,
        rejected: &[Rejected],
        coordinates: &[Vec<Option<(i32, i64)>>],
        offsets: &KafkaOffsets,
        write_id: &str,
        now_micros: i64,
    ) -> std::result::Result<usize, Attempt> {
        let all: usize = rejected.iter().map(Rejected::len).sum();
        // Every reject's coordinates are in these partitions, or it has none.
        let partitions: Vec<i32> = offsets.ranges.keys().copied().collect();
        let ids: Vec<String> = partitions
            .iter()
            .map(|p| kafka_app_id_for(&self.app_id, *p))
            .collect();
        let txns = fence::read_own_txns(&self.table, &ids, &self.kafka.lookups)
            .await
            .map_err(|cause| Attempt::Failed { rows: all, cause })?;
        let marks: BTreeMap<i32, i64> = partitions
            .iter()
            .zip(&ids)
            .filter_map(|(p, id)| Some((*p, txns.get(id)?.version?)))
            .collect();

        let mut source_versions = Vec::new();
        let mut columns = Vec::new();
        let mut reasons = Vec::new();
        let mut payload = Vec::new();
        let mut skipped = 0u64;
        for (r, coordinates) in rejected.iter().zip(coordinates) {
            let keep: Vec<bool> = coordinates
                .iter()
                .zip(&r.reasons)
                .map(|(at, reason)| {
                    let recorded = at.is_some_and(|(p, offset)| {
                        rejected_again_on_a_replay(reason)
                            && marks.get(&p).is_some_and(|mark| offset <= *mark)
                    });
                    !recorded
                })
                .collect();
            skipped += keep.iter().filter(|k| !**k).count() as u64;
            if !keep.iter().any(|k| *k) {
                continue;
            }
            let rows = deltalake::arrow::compute::filter_record_batch(
                &r.rows,
                &BooleanArray::from(keep.clone()),
            )
            .map_err(|e| Attempt::Failed {
                rows: all,
                cause: Error::Other(format!(
                    "cannot set aside the rejects already recorded: {e}"
                )),
            })?;
            payload.extend(payloads(&rows).map_err(|cause| Attempt::Failed { rows: all, cause })?);
            for (i, kept) in keep.iter().enumerate() {
                if *kept {
                    source_versions.push(coordinates[i].map_or(-1, |(_, offset)| offset));
                    columns.push(r.columns[i].clone());
                    reasons.push(r.reasons[i].clone());
                }
            }
        }

        if reasons.is_empty() {
            debug!(
                app_id = %self.app_id,
                skipped,
                "every reject of this batch is already in the data-quality table; not writing \
                 them twice"
            );
            self.kafka
                .rows_skipped
                .fetch_add(skipped, Ordering::Relaxed);
            return Ok(0);
        }
        let written = reasons.len();
        let batch = self
            .assemble(source_versions, columns, reasons, payload, now_micros)
            .map_err(|cause| Attempt::Failed {
                rows: written,
                cause,
            })?;

        let txns = offsets.ranges.iter().map(|(p, (_, last))| {
            let mark = marks.get(p).map_or(*last, |m| (*m).max(*last));
            Transaction::new(kafka_app_id_for(&self.app_id, *p), mark)
        });
        let ranges: serde_json::Map<String, serde_json::Value> = offsets
            .ranges
            .iter()
            .map(|(p, (first, last))| (p.to_string(), serde_json::json!([first, last])))
            .collect();
        let props = CommitProperties::default()
            .with_application_transactions(txns.collect())
            .with_metadata([
                (
                    "ddi.appId".to_string(),
                    serde_json::Value::from(self.app_id.clone()),
                ),
                (
                    "ddi.kafka.offsets".to_string(),
                    serde_json::Value::Object(ranges),
                ),
                (
                    KAFKA_WRITE_ID.to_string(),
                    serde_json::Value::from(write_id),
                ),
            ]);

        // As in `write`, and for the same reason: no runtime nobody configured.
        let session =
            crate::budget::delta_session(&self.table).map_err(|cause| Attempt::Failed {
                rows: written,
                cause,
            })?;
        let session = Arc::new(session);
        // A clone rather than the handle itself, so that a failed write leaves a usable
        // handle behind even when the table cannot be reopened.
        let written_table = self
            .table
            .clone()
            .write(vec![batch])
            .with_session_state(session)
            .with_save_mode(SaveMode::Append)
            .with_cast_safety(false)
            .with_commit_properties(props)
            .await;
        match written_table {
            Ok(t) => {
                self.table = t;
                self.kafka
                    .rows_skipped
                    .fetch_add(skipped, Ordering::Relaxed);
                Ok(written)
            }
            Err(e) if is_conflict(&e) => Err(Attempt::Conflict {
                error: e,
                written,
                skipped,
            }),
            // Typed, as the target's own write is: a full spill directory is out of capacity,
            // and anything else is delta-rs's error as it came.
            Err(e) => Err(Attempt::Failed {
                rows: written,
                cause: crate::spill::classify_delta(e, "data-quality: writing rejects"),
            }),
        }
    }

    /// `rows` of a batch's rejects were not written to this table, because of `cause`.
    fn unwritable(&self, rows: usize, cause: Error) -> Error {
        Error::DataQualityWrite {
            dq_uri: self.uri.clone(),
            rows,
            source: Box::new(cause),
        }
    }

    /// Whether this write's own commit, the one recording `write_id`, is among those that
    /// landed after `read_version` on the table as just reopened.
    ///
    /// A conflict is not always another writer's. When the response to a commit's PUT is
    /// lost and the PUT is retried, it finds the version taken by that very commit, and
    /// delta-rs then reports the commit's own marks as a conflict (docs/kafka-design.md §4.7
    /// row 3). Tried again, the write would record every reject that is never skipped a second
    /// time. Read newest first, which is where that commit is. A commit no longer in the log
    /// ends the search: one that cannot be read is not known to be this write's, and writing
    /// again costs a duplicate, never a lost reject.
    async fn landed(&self, read_version: Option<u64>, write_id: &str) -> Result<bool> {
        let Some(head) = self.table.version() else {
            return Ok(false);
        };
        let after = read_version.map_or(0, |v| v + 1);
        let log_store = self.table.log_store();
        for version in (after..=head).rev() {
            let Some(commit) = log_store.read_commit_entry(version).await? else {
                return Ok(false);
            };
            if write_id_of(&commit).as_deref() == Some(write_id) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Each row's partition and offset, where its `<prefix>partition` and `<prefix>offset`
    /// columns are integers, both are set, and they name an offset of `ranges`: one the batch
    /// read.
    ///
    /// A row without them — a transform's output that dropped the columns — has no
    /// coordinates, and is always written. So has one whose columns name a message the batch
    /// did not read: a message's own coordinates always lie inside the batch, so those are
    /// something else under the same names — values the SQL computed, or a payload's own
    /// fields read under the default prefix where the pipeline set another. Taken at their
    /// word, they would be checked against another partition's mark, or below the batch's
    /// range, and the reject skipped.
    fn coordinates(
        &self,
        rejected: &Rejected,
        ranges: &BTreeMap<i32, (i64, i64)>,
    ) -> Vec<Option<(i32, i64)>> {
        let column = |name: String| {
            let c = rejected.rows.column_by_name(&name)?;
            if !c.data_type().is_integer() {
                return None;
            }
            // An integer too wide for i64 becomes null, and so has no coordinates.
            deltalake::arrow::compute::cast(c, &DataType::Int64).ok()
        };
        let partitions = column(format!("{}partition", self.kafka_prefix));
        let offsets = column(format!("{}offset", self.kafka_prefix));
        let (Some(partitions), Some(offsets)) = (partitions, offsets) else {
            return vec![None; rejected.len()];
        };
        let (partitions, offsets) = (
            partitions.as_primitive::<Int64Type>(),
            offsets.as_primitive::<Int64Type>(),
        );
        (0..rejected.len())
            .map(|i| {
                if partitions.is_null(i) || offsets.is_null(i) {
                    return None;
                }
                let (partition, offset) =
                    (i32::try_from(partitions.value(i)).ok()?, offsets.value(i));
                let (first, last) = ranges.get(&partition)?;
                (*first..=*last)
                    .contains(&offset)
                    .then_some((partition, offset))
            })
            .collect()
    }
}

/// Whether a Kafka reject with this reason is one every replay of its message rejects
/// again: a message that could not be decoded, for a reason other than a writer schema the
/// registry did not have yet.
fn rejected_again_on_a_replay(reason: &str) -> bool {
    reason.starts_with(KAFKA_UNDECODABLE) && !reason.starts_with(KAFKA_SCHEMA_NOT_FOUND)
}

/// How one attempt of [`DataQuality::write_kafka`] ended, when it did not write.
enum Attempt {
    /// A commit wrote one of the same marks first: another writer's, worth trying again in
    /// place, or this attempt's own, landed already. `written` and `skipped` are the rows the
    /// attempt wrote and left out, which are what it did if the commit is its own.
    Conflict {
        error: DeltaTableError,
        written: usize,
        skipped: u64,
    },
    /// Anything else, about `rows` of the batch's rejects.
    Failed { rows: usize, cause: Error },
}

/// The commitInfo key a Kafka data-quality commit records its write's id under.
const KAFKA_WRITE_ID: &str = "ddi.kafka.dqWriteId";

/// An id for one [`DataQuality::write_kafka`] call, which its commit records: no commit of
/// another call, in this process or any other, records the same one.
fn write_id() -> String {
    use std::hash::BuildHasher;
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let n = WRITES.fetch_add(1, Ordering::Relaxed);
    // Keyed at random in every process, so two processes with one pid and one clock reading
    // still differ.
    let random =
        std::collections::hash_map::RandomState::new().hash_one((std::process::id(), nanos, n));
    format!("{random:016x}-{nanos:x}-{n}")
}

/// The write id a data-quality commit records, if it records one.
fn write_id_of(commit: &[u8]) -> Option<String> {
    serde_json::Deserializer::from_slice(commit)
        .into_iter::<serde_json::Value>()
        .map_while(std::result::Result::ok)
        .find_map(|line| {
            Some(
                line.get("commitInfo")?
                    .get(KAFKA_WRITE_ID)?
                    .as_str()?
                    .to_string(),
            )
        })
}

/// Did a commit fail because another one landed first on something it asserted?
fn is_conflict(e: &DeltaTableError) -> bool {
    matches!(
        e,
        DeltaTableError::VersionAlreadyExists(_)
            | DeltaTableError::Transaction {
                source: TransactionError::CommitConflict(_)
                    | TransactionError::VersionAlreadyExists(_)
                    | TransactionError::MaxCommitAttempts(_),
            }
    )
}

/// Each row of `batch`, as its own JSON object.
///
/// The reject is kept as text rather than in its own columns because there is nothing to put
/// them in: every pipeline's rows have a different shape, and the point of the table is that
/// one shape serves all of them. JSON keeps the row readable and queryable
/// (`json_extract_scalar(payload, '$.order_id')`) without pinning the DQ table's schema to
/// any pipeline's.
///
/// A value with no JSON spelling — and there are few, since the transform output is already
/// a flat-ish Arrow batch — costs the payload, not the row: the reject is still recorded,
/// with the reason and the offending column, and only `payload` is null.
fn payloads(batch: &RecordBatch) -> Result<Vec<Option<String>>> {
    use deltalake::arrow::json::writer::{LineDelimited, WriterBuilder};

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let row = batch.slice(i, 1);
        let mut buf = Vec::new();
        // `with_explicit_nulls` matters more here than anywhere else: the default drops null
        // fields from the object entirely, so a row rejected *because* a column was null
        // would name a column its own payload does not mention.
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut buf);
        out.push(match writer.write(&row).and_then(|()| writer.finish()) {
            Ok(()) => Some(String::from_utf8_lossy(&buf).trim().to_string()),
            Err(_) => None,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_table_sits_next_to_the_target() {
        assert_eq!(
            uri_for("/lake/silver/orders"),
            "/lake/silver/orders__ddi_dq"
        );
        assert_eq!(
            uri_for("abfss://lake@acct.dfs.core.windows.net/silver/orders"),
            "abfss://lake@acct.dfs.core.windows.net/silver/orders__ddi_dq"
        );
    }

    #[test]
    fn a_trailing_slash_does_not_produce_a_stray_segment() {
        assert_eq!(
            uri_for("/lake/silver/orders/"),
            "/lake/silver/orders__ddi_dq"
        );
    }

    #[test]
    fn the_dq_offset_is_a_separate_app_id() {
        // Sharing the pipeline's app_id would make the two tables' offsets overwrite each
        // other's meaning: a batch with no rejects writes nothing here, so this one
        // legitimately lags behind.
        assert_eq!(app_id_for("ddi.orders"), "ddi.orders.dq");
        assert_ne!(app_id_for("ddi.orders"), "ddi.orders");
    }

    #[test]
    fn a_row_is_serialised_as_one_json_object() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("n/a"), None])) as ArrayRef,
            ],
        )
        .unwrap();

        let got = payloads(&batch).unwrap();
        assert_eq!(got.len(), 2);
        let first = got[0].as_deref().unwrap();
        assert!(first.contains("\"order_id\":1"), "got: {first}");
        assert!(
            first.contains("\"amount\":\"n/a\""),
            "the value that could not be cast must survive verbatim: {first}"
        );
        assert!(
            !first.contains('\n'),
            "one line per row, so a payload is one value: {first}"
        );
    }

    #[test]
    fn the_arrow_and_delta_shapes_agree() {
        let arrow = arrow_schema();
        let delta = columns();
        assert_eq!(arrow.fields().len(), delta.len());
        for (a, d) in arrow.fields().iter().zip(delta.iter()) {
            assert_eq!(a.name(), d.name(), "column order must match");
            assert_eq!(
                a.is_nullable(),
                d.is_nullable(),
                "nullability must match for {:?}",
                a.name()
            );
        }
    }

    // ---------------------------------------------------------------- Kafka rejects

    use deltalake::arrow::array::Int32Array;
    use deltalake::kernel::Action;
    use futures::TryStreamExt;

    const NOW: i64 = 1_700_000_000_000_000;
    const MALFORMED: &str = "avro: malformed body: unexpected end of input";

    async fn create_dq(path: &str) {
        DeltaTable::try_from_url(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap()
            .create()
            .with_columns(columns())
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
    }

    async fn open_dq(path: &str) -> DataQuality {
        DataQuality::open(&crate::storage::Storage::default(), path, "app", "orders")
            .await
            .unwrap()
            .expect("the table was just created")
    }

    /// Messages of `partition` at `offsets` the decoder set aside, as it sets them aside.
    fn undecodable(partition: i32, offsets: &[i64], reason: &str) -> Rejected {
        let n = offsets.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("kafka_topic", DataType::Utf8, false),
            Field::new("kafka_partition", DataType::Int32, false),
            Field::new("kafka_offset", DataType::Int64, false),
            Field::new("kafka_value_b64", DataType::Utf8, true),
        ]));
        let rows = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["orders"; n])) as ArrayRef,
                Arc::new(Int32Array::from(vec![partition; n])) as ArrayRef,
                Arc::new(Int64Array::from(offsets.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("AAAAKg=="); n])) as ArrayRef,
            ],
        )
        .unwrap();
        Rejected {
            rows,
            reasons: vec![reason.to_string(); n],
            columns: vec![None; n],
        }
    }

    /// A transform's output row that would not coerce, without the coordinate columns.
    fn uncoordinated(order_ids: &[i64]) -> Rejected {
        let n = order_ids.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Utf8, true),
        ]));
        let rows = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(order_ids.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("n/a"); n])) as ArrayRef,
            ],
        )
        .unwrap();
        Rejected {
            rows,
            reasons: vec!["cannot cast 'n/a' to DECIMAL(10,2)".to_string(); n],
            columns: vec![Some("amount".to_string()); n],
        }
    }

    /// One rejected row with integer `<prefix>partition` and `<prefix>offset` columns for each
    /// `(prefix, partition, offset)`, as SQL that computed them would leave it.
    fn rejected_with(coordinates: &[(&str, i32, i64)], reason: &str) -> Rejected {
        let mut fields = vec![Field::new("order_id", DataType::Int64, false)];
        let mut values: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![42]))];
        for (prefix, partition, offset) in coordinates {
            fields.push(Field::new(
                format!("{prefix}partition"),
                DataType::Int32,
                false,
            ));
            fields.push(Field::new(
                format!("{prefix}offset"),
                DataType::Int64,
                false,
            ));
            values.push(Arc::new(Int32Array::from(vec![*partition])));
            values.push(Arc::new(Int64Array::from(vec![*offset])));
        }
        Rejected {
            rows: RecordBatch::try_new(Arc::new(Schema::new(fields)), values).unwrap(),
            reasons: vec![reason.to_string()],
            columns: vec![None],
        }
    }

    /// A Kafka batch that read `ranges`.
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
        }
    }

    /// Every row of the table: (source_version, reason, payload), in source_version order.
    async fn recorded(path: &str) -> Vec<(i64, String, Option<String>)> {
        let table = deltalake::open_table(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap();
        let (_, stream) = table.scan_table().await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut rows = Vec::new();
        // The scan may hand text back as any of Arrow's string types.
        let text = |b: &RecordBatch, name: &str| {
            deltalake::arrow::compute::cast(b.column_by_name(name).unwrap(), &DataType::Utf8)
                .unwrap()
        };
        for b in &batches {
            let version = b.column_by_name("source_version").unwrap();
            let version = version.as_primitive::<Int64Type>();
            let (reason, payload) = (text(b, "reason"), text(b, "payload"));
            let (reason, payload) = (reason.as_string::<i32>(), payload.as_string::<i32>());
            for i in 0..b.num_rows() {
                rows.push((
                    version.value(i),
                    reason.value(i).to_string(),
                    payload.is_valid(i).then(|| payload.value(i).to_string()),
                ));
            }
        }
        rows.sort();
        rows
    }

    async fn txn(path: &str, app_id: &str) -> Option<i64> {
        let table = deltalake::open_table(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap();
        table
            .snapshot()
            .unwrap()
            .transaction_version(table.log_store().as_ref(), app_id)
            .await
            .unwrap()
    }

    async fn head_commit(path: &str) -> Vec<Action> {
        let table = deltalake::open_table(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap();
        let version = table.version().unwrap();
        let raw = table
            .log_store()
            .read_commit_entry(version)
            .await
            .unwrap()
            .unwrap();
        deltalake::logstore::get_actions(version, &raw).unwrap()
    }

    fn txns_of(actions: &[Action]) -> Vec<(String, i64)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Txn(t) => Some((t.app_id.clone(), t.version)),
                _ => None,
            })
            .collect()
    }

    fn info_of(actions: &[Action]) -> std::collections::HashMap<String, serde_json::Value> {
        actions
            .iter()
            .find_map(|a| match a {
                Action::CommitInfo(ci) => Some(ci.info.clone()),
                _ => None,
            })
            .unwrap()
    }

    #[test]
    fn a_kafka_mark_is_one_app_id_per_partition() {
        assert_eq!(kafka_app_id_for("orders", 3), "orders.dq-3");
        assert_ne!(kafka_app_id_for("orders", 3), app_id_for("orders"));
        // Never one the target's offsets are kept under (`<app_id>-<p>`).
        assert_ne!(
            kafka_app_id_for("orders", 3),
            crate::fence::txn_app_id("orders", 3)
        );
    }

    #[tokio::test]
    async fn dq_marks_skip_recorded_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;

        let batch = [undecodable(0, &[3, 7], MALFORMED)];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            2
        );
        assert_eq!(txn(path, "app.dq-0").await, Some(9));
        let actions = head_commit(path).await;
        assert_eq!(txns_of(&actions), vec![("app.dq-0".to_string(), 9)]);
        let info = info_of(&actions);
        assert_eq!(info["ddi.appId"], serde_json::json!("app"));
        assert_eq!(info["ddi.kafka.offsets"], serde_json::json!({"0": [0, 9]}));
        assert!(!info.contains_key("ddi.sourceVersion"), "{info:?}");

        // The target commit failed, and the batch is read again: by this process, or by the
        // next owner of the partition, which has only the table to go on.
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            0
        );
        let mut next_owner = open_dq(path).await;
        assert_eq!(
            next_owner
                .write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            0
        );
        assert_eq!(dq.kafka_counters().rows_skipped(), 2);

        // A batch that overlaps the recorded one: offset 7 is there already, 11 is not.
        let wider = [undecodable(0, &[7, 11], MALFORMED)];
        assert_eq!(
            next_owner
                .write_kafka(&wider, &read(&[(0, (5, 12))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(txn(path, "app.dq-0").await, Some(12));

        let rows = recorded(path).await;
        assert_eq!(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![3, 7, 11],
            "source_version holds each message's offset"
        );
        let payload = rows[2].2.as_deref().unwrap();
        assert!(payload.contains("\"kafka_partition\":0"), "{payload}");
        assert!(payload.contains("\"kafka_offset\":11"), "{payload}");
    }

    #[tokio::test]
    async fn dq_schema_not_found_never_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;

        let not_found = format!("{KAFKA_SCHEMA_NOT_FOUND}: id 42 (404, error code 40403)");
        let batch = [
            undecodable(0, &[3], &not_found),
            undecodable(0, &[4], MALFORMED),
        ];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 4))]), NOW)
                .await
                .unwrap(),
            2
        );
        // On the replay the registry may have the schema, and the message decode: its reject
        // is recorded again rather than taken for the one already there.
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 4))]), NOW)
                .await
                .unwrap(),
            1
        );

        let rows = recorded(path).await;
        let versions: Vec<i64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(versions, vec![3, 3, 4]);
        assert!(rows[..2].iter().all(|r| r.1 == not_found));
    }

    #[tokio::test]
    async fn dq_without_coordinates_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;

        // A coerce reject from SQL that dropped the kafka_ columns, and an undecodable whose
        // offset is null: neither names a message, so neither can be found again.
        let mut no_offset = undecodable(1, &[0], MALFORMED);
        let schema = no_offset.rows.schema();
        let nullable = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        ));
        let mut columns = no_offset.rows.columns().to_vec();
        columns[2] = Arc::new(Int64Array::from(vec![None::<i64>]));
        no_offset.rows = RecordBatch::try_new(nullable, columns).unwrap();
        let batch = [uncoordinated(&[41, 42]), no_offset];

        let offsets = read(&[(0, (0, 8)), (1, (0, 3))]);
        assert_eq!(dq.write_kafka(&batch, &offsets, NOW).await.unwrap(), 3);
        assert_eq!(dq.write_kafka(&batch, &offsets, NOW).await.unwrap(), 3);
        assert_eq!(dq.kafka_counters().rows_skipped(), 0);

        let rows = recorded(path).await;
        assert_eq!(rows.len(), 6);
        assert!(
            rows.iter().all(|r| r.0 == -1),
            "no offset to record: {rows:?}"
        );
        // The marks move all the same: a batch's partitions are covered whatever they held.
        assert_eq!(txn(path, "app.dq-0").await, Some(8));
        assert_eq!(txn(path, "app.dq-1").await, Some(3));

        // Under another metadata prefix, kafka_partition and kafka_offset are just columns.
        let mut prefixed = open_dq(path).await;
        prefixed.set_kafka_metadata_prefix("meta_");
        let batch = [undecodable(0, &[5], MALFORMED)];
        assert_eq!(
            prefixed
                .write_kafka(&batch, &read(&[(0, (0, 8))]), NOW)
                .await
                .unwrap(),
            1,
            "below the mark, but not known to be"
        );
    }

    /// Only an undecodable is skipped by its partition's mark. A row the transform, the
    /// conform or the coercer set aside was judged against the target's schema, the SQL and
    /// the union of writer schemas, any of which can change between two attempts at one
    /// batch — an ALTER TABLE on the target in between (§4.7 row 20) is enough. A retry can
    /// then reject a row the first attempt did not, at an offset already under the mark, and
    /// it has to be written: a duplicate is visible, a row in neither table is not.
    #[tokio::test]
    async fn dq_only_an_undecodable_is_skipped_by_the_mark() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;

        // The first attempt: offset 3 does not decode, and every other row fits the target.
        let first = [undecodable(0, &[3], MALFORMED)];
        let batch = read(&[(0, (1, 10))]);
        assert_eq!(dq.write_kafka(&first, &batch, NOW).await.unwrap(), 1);
        assert_eq!(txn(path, "app.dq-0").await, Some(10));

        // The target commit failed on a new column, and the retry's coercer rejects offset 7.
        let coerced = rejected_with(
            &[("kafka_", 0, 7)],
            "value does not fit \"note\" (Utf8 -> Int32)",
        );
        let retry = [undecodable(0, &[3], MALFORMED), coerced];
        assert_eq!(
            dq.write_kafka(&retry, &batch, NOW).await.unwrap(),
            1,
            "offset 3 is recorded already; offset 7 is new"
        );
        let versions: Vec<i64> = recorded(path).await.iter().map(|r| r.0).collect();
        assert_eq!(versions, vec![3, 7]);
        assert_eq!(dq.kafka_counters().rows_skipped(), 1);
    }

    /// A batch that read below a partition's mark leaves the mark where it is. A zombie that
    /// recorded up to 8, then the new owner's batch up to 5: moved back to 5, the mark would
    /// let a later replay up to 8 record the rejects above 5 again. The lower batch has to
    /// commit for the mark to move at all, which only a reject that is never skipped makes it
    /// do.
    #[tokio::test]
    async fn dq_mark_never_moves_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;

        let upper = [undecodable(0, &[7], MALFORMED)];
        assert_eq!(
            dq.write_kafka(&upper, &read(&[(0, (0, 8))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(txn(path, "app.dq-0").await, Some(8));

        let lower = [uncoordinated(&[1])];
        assert_eq!(
            dq.write_kafka(&lower, &read(&[(0, (0, 5))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(txn(path, "app.dq-0").await, Some(8), "max(8, 5)");
        assert_eq!(
            dq.write_kafka(&upper, &read(&[(0, (6, 8))]), NOW)
                .await
                .unwrap(),
            0,
            "the replay of offset 7 is still covered"
        );
    }

    #[test]
    fn only_an_undecodable_is_rejected_again_on_every_replay() {
        assert!(rejected_again_on_a_replay(MALFORMED));
        assert!(rejected_again_on_a_replay("avro: no magic byte"));
        assert!(KAFKA_SCHEMA_NOT_FOUND.starts_with(KAFKA_UNDECODABLE));
        for reason in [
            format!("{KAFKA_SCHEMA_NOT_FOUND}: id 42 (404, error code 40403)"),
            "value does not fit \"note\" (Utf8 -> Int32)".to_string(),
            "cannot cast 'n/a' to DECIMAL(10,2)".to_string(),
            "writer schema 7 field \"amount\" cannot be read as DECIMAL(10,2)".to_string(),
        ] {
            assert!(!rejected_again_on_a_replay(&reason), "{reason}");
        }
    }

    /// A message's coordinates always lie inside what its batch read. A reject whose
    /// `<prefix>partition` and `<prefix>offset` lie outside — computed by the SQL under those
    /// names, or a payload's own fields read under the default prefix where the pipeline set
    /// another — names no message of the batch, and is written rather than checked against
    /// another partition's mark, or an offset the batch never read.
    #[tokio::test]
    async fn dq_coordinates_outside_the_batch_are_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        let first = [undecodable(0, &[50], MALFORMED)];
        assert_eq!(
            dq.write_kafka(&first, &read(&[(0, (0, 100))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(txn(path, "app.dq-0").await, Some(100));

        // A batch that read partition 1 only, with a reject that says partition 0.
        let other_partition = [rejected_with(&[("kafka_", 0, 5)], MALFORMED)];
        assert_eq!(
            dq.write_kafka(&other_partition, &read(&[(1, (0, 10))]), NOW)
                .await
                .unwrap(),
            1
        );
        // A batch that read partition 0 from 101, with a reject that says offset 7.
        let below = [rejected_with(&[("kafka_", 0, 7)], MALFORMED)];
        assert_eq!(
            dq.write_kafka(&below, &read(&[(0, (101, 200))]), NOW)
                .await
                .unwrap(),
            1
        );
        // A pipeline whose metadata prefix is meta_, read as if it were kafka_: the payload's
        // own kafka_partition and kafka_offset are not this message's.
        let payload_fields = [rejected_with(
            &[("meta_", 3, 4), ("kafka_", 0, 9)],
            MALFORMED,
        )];
        assert_eq!(
            dq.write_kafka(&payload_fields, &read(&[(3, (0, 10))]), NOW)
                .await
                .unwrap(),
            1
        );

        let versions: Vec<i64> = recorded(path).await.iter().map(|r| r.0).collect();
        assert_eq!(
            versions,
            vec![-1, -1, -1, 50],
            "no offset to record for those"
        );
        assert_eq!(dq.kafka_counters().rows_skipped(), 0);
    }

    #[tokio::test]
    async fn dq_two_writers_disjoint_partitions_no_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        // Two members of one group, both with a handle on version 0.
        let (mut a, mut b) = (open_dq(path).await, open_dq(path).await);

        let from_a = [undecodable(0, &[1], MALFORMED)];
        let from_b = [undecodable(1, &[2], MALFORMED)];
        assert_eq!(
            a.write_kafka(&from_a, &read(&[(0, (0, 5))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            b.write_kafka(&from_b, &read(&[(1, (0, 6))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(a.kafka_counters().conflict_retries(), 0);
        assert_eq!(
            b.kafka_counters().conflict_retries(),
            0,
            "rebased by delta-rs, never retried here"
        );

        assert_eq!(txn(path, "app.dq-0").await, Some(5));
        assert_eq!(txn(path, "app.dq-1").await, Some(6));
        assert_eq!(recorded(path).await.len(), 2);
    }

    #[tokio::test]
    async fn dq_same_partition_conflict_retried_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        // A zombie and the partition's new owner, both with a handle on version 0. The owner
        // records the batch first; the zombie then records the same batch and one more.
        let (mut owner, mut zombie) = (open_dq(path).await, open_dq(path).await);
        let first = [undecodable(0, &[2, 4], MALFORMED)];
        assert_eq!(
            owner
                .write_kafka(&first, &read(&[(0, (0, 5))]), NOW)
                .await
                .unwrap(),
            2
        );

        let again = [undecodable(0, &[2, 4, 7], MALFORMED)];
        assert_eq!(
            zombie
                .write_kafka(&again, &read(&[(0, (0, 8))]), NOW)
                .await
                .unwrap(),
            1,
            "re-read the mark after the conflict, and wrote only what it did not cover"
        );
        assert_eq!(zombie.kafka_counters().conflict_retries(), 1);
        assert_eq!(zombie.kafka_counters().rows_skipped(), 2);

        let rows = recorded(path).await;
        assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), vec![2, 4, 7]);
        assert_eq!(txn(path, "app.dq-0").await, Some(8));
    }

    /// `delta.setTransactionRetentionDuration` is refused on a target because a `txn` written
    /// with a `lastUpdated`, as kafka-delta-ingest wrote them, can expire under it. A
    /// data-quality table holds only ddi's marks, which have none and never expire, so the
    /// property is no reason to stop recording rejects there.
    #[tokio::test]
    async fn dq_txn_retention_on_the_data_quality_table_is_not_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        DeltaTable::try_from_url(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap()
            .create()
            .with_columns(columns())
            .with_configuration([(
                "delta.setTransactionRetentionDuration",
                Some("interval 1 days"),
            )])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        let mut dq = open_dq(path).await;

        let batch = [undecodable(0, &[3], MALFORMED)];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(txn(path, "app.dq-0").await, Some(9));
        // And the mark is read back as on any other table: the replay is skipped.
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            0
        );
    }

    /// `write` keeps the equality rule and the commit it always made: one `txn` under
    /// `<app_id>.dq` holding the batch's source version, and `ddi.sourceVersion` beside it.
    #[tokio::test]
    async fn dq_delta_write_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        let rejects = [uncoordinated(&[1, 2])];

        assert_eq!(dq.write(&rejects, 5, NOW).await.unwrap(), 2);
        let actions = head_commit(path).await;
        assert_eq!(txns_of(&actions), vec![("app.dq".to_string(), 5)]);
        let info = info_of(&actions);
        assert_eq!(info["ddi.sourceVersion"], serde_json::json!(5));
        assert_eq!(info["ddi.appId"], serde_json::json!("app"));
        assert!(!info.contains_key("ddi.kafka.offsets"), "{info:?}");

        // The same version again is the replay the skip exists for.
        assert_eq!(dq.write(&rejects, 5, NOW).await.unwrap(), 0);
        // A lower one is a rewind, and is written: `>=` would swallow it.
        assert_eq!(dq.write(&rejects, 3, NOW).await.unwrap(), 2);
        assert_eq!(txn(path, "app.dq").await, Some(3));

        let rows = recorded(path).await;
        assert_eq!(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![3, 3, 5, 5]
        );
        assert!(rows
            .iter()
            .all(|r| r.2.as_deref().unwrap().contains("\"order_id\"")));
    }

    // ---------------------------------------------------------------- commits that go wrong

    use deltalake::logstore::object_store::{
        self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use deltalake::Path;
    use futures::stream::BoxStream;
    use std::sync::atomic::AtomicU32;

    /// What [`FlakyCommits`] does to a conditional PUT of a commit file.
    #[derive(Clone, Copy, Debug)]
    enum Fault {
        /// Write it, then say it already exists, the first time only: what the PUT retried
        /// after its response was lost gets back.
        LandsThenSaysItExists,
        /// Every time, write another writer's commit there first, which asserts mark 0 of
        /// partition 0: a rival that always wins the race.
        RivalFirst,
    }

    /// An object store that gets one thing wrong about commits, and passes everything else
    /// through.
    #[derive(Debug)]
    struct FlakyCommits {
        inner: Arc<dyn ObjectStore>,
        fault: Fault,
        /// How often the fault has struck.
        struck: Arc<AtomicU32>,
    }

    fn is_commit(location: &Path) -> bool {
        location
            .as_ref()
            .rsplit_once("_delta_log/")
            .and_then(|(_, name)| name.strip_suffix(".json"))
            .is_some_and(|v| v.len() == 20 && v.bytes().all(|b| b.is_ascii_digit()))
    }

    impl std::fmt::Display for FlakyCommits {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FlakyCommits({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FlakyCommits {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let commit = matches!(opts.mode, PutMode::Create) && is_commit(location);
            match self.fault {
                Fault::LandsThenSaysItExists => {
                    let put = self.inner.put_opts(location, payload, opts).await?;
                    if commit && self.struck.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(object_store::Error::AlreadyExists {
                            path: location.to_string(),
                            source: "the response to the first attempt was lost".into(),
                        });
                    }
                    Ok(put)
                }
                Fault::RivalFirst => {
                    if commit {
                        self.struck.fetch_add(1, Ordering::SeqCst);
                        let rival = "{\"commitInfo\":{\"timestamp\":1700000000000,\
                                     \"operation\":\"WRITE\",\"operationParameters\":{\"mode\":\"Append\"}}}\n\
                                     {\"txn\":{\"appId\":\"app.dq-0\",\"version\":0}}\n";
                        self.inner
                            .put_opts(location, rival.into(), PutOptions::default())
                            .await?;
                    }
                    self.inner.put_opts(location, payload, opts).await
                }
            }
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

        // Sorted, as a cloud store lists: the kernel relies on it for every scheme but file://,
        // and the local file system lists in no order.
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            sorted(self.inner.list(prefix))
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            sorted(self.inner.list_with_offset(prefix, offset))
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

    fn sorted(
        listing: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        use futures::{StreamExt, TryStreamExt};
        futures::stream::once(async move {
            let mut all: Vec<ObjectMeta> = listing.try_collect().await?;
            all.sort_by(|a, b| a.location.cmp(&b.location));
            Ok::<_, object_store::Error>(futures::stream::iter(all.into_iter().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }

    /// `dq`'s table, from here on reached through a store with `fault`, until it is reopened.
    async fn with_fault(dq: &mut DataQuality, fault: Fault) -> Arc<AtomicU32> {
        let plain = deltalake::open_table(deltalake::ensure_table_uri(&dq.uri).unwrap())
            .await
            .unwrap();
        let store = plain.log_store();
        let struck = Arc::new(AtomicU32::new(0));
        let flaky = deltalake::logstore::default_logstore(
            Arc::new(FlakyCommits {
                inner: store.object_store(None),
                fault,
                struck: struck.clone(),
            }),
            store.root_object_store(None),
            store.config().location(),
            store.config().options(),
        );
        let mut table = DeltaTable::new(flaky, plain.config.clone());
        table.load().await.unwrap();
        dq.table = table;
        struck
    }

    /// The data-quality commit landed, but the response to its PUT was lost, and the retried
    /// PUT found the version taken — by itself. delta-rs then meets its own marks in the
    /// winning commit and reports a conflict (§4.7 row 3). That commit is this write's, so the
    /// write is done: tried again, it would record every reject that is never skipped a
    /// second time, in a process that never crashed.
    #[tokio::test]
    async fn dq_commit_that_landed_unacknowledged_is_not_written_twice() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        let struck = with_fault(&mut dq, Fault::LandsThenSaysItExists).await;

        let not_found = format!("{KAFKA_SCHEMA_NOT_FOUND}: id 42 (404, error code 40403)");
        let batch = [
            undecodable(0, &[3, 7], MALFORMED),
            undecodable(0, &[5], &not_found),
            uncoordinated(&[1]),
        ];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            4
        );
        assert_eq!(struck.load(Ordering::SeqCst), 1, "the fault struck");

        let rows = recorded(path).await;
        assert_eq!(rows.len(), 4, "each reject once: {rows:?}");
        let table = deltalake::open_table(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(1), "one commit");
        assert_eq!(txn(path, "app.dq-0").await, Some(9));
        assert_eq!(
            dq.kafka_counters().conflict_retries(),
            0,
            "nobody else was there to conflict with"
        );
    }

    /// The scheme every table reached through a [`Fault::RivalFirst`] store is opened by, so
    /// that the data-quality table reopened after a conflict still has its rival. Registered
    /// once for the process; no other test opens a table under it.
    const RIVAL_SCHEME: &str = "ddi-dq-rival";

    struct RivalFactory {
        struck: Arc<AtomicU32>,
    }

    impl deltalake::logstore::ObjectStoreFactory for RivalFactory {
        fn parse_url_opts(
            &self,
            url: &url::Url,
            _: &deltalake::logstore::StorageConfig,
        ) -> deltalake::DeltaResult<(deltalake::logstore::ObjectStoreRef, Path)> {
            let store = FlakyCommits {
                inner: Arc::new(object_store::local::LocalFileSystem::new()),
                fault: Fault::RivalFirst,
                struck: self.struck.clone(),
            };
            let path = Path::from_url_path(url.path())
                .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
            Ok((Arc::new(store), path))
        }
    }

    impl deltalake::logstore::LogStoreFactory for RivalFactory {
        fn with_options(
            &self,
            prefixed_store: deltalake::logstore::ObjectStoreRef,
            root_store: deltalake::logstore::ObjectStoreRef,
            location: &url::Url,
            options: &deltalake::logstore::StorageConfig,
        ) -> deltalake::DeltaResult<Arc<dyn deltalake::logstore::LogStore>> {
            Ok(deltalake::logstore::default_logstore(
                prefixed_store,
                root_store,
                location,
                options,
            ))
        }
    }

    /// How often the rival under [`RIVAL_SCHEME`] has committed first.
    fn rival_scheme() -> Arc<AtomicU32> {
        static STRUCK: std::sync::OnceLock<Arc<AtomicU32>> = std::sync::OnceLock::new();
        STRUCK
            .get_or_init(|| {
                let struck = Arc::new(AtomicU32::new(0));
                let factory = Arc::new(RivalFactory {
                    struck: struck.clone(),
                });
                let scheme = url::Url::parse(&format!("{RIVAL_SCHEME}://")).unwrap();
                deltalake::logstore::object_store_factories().insert(
                    scheme.clone(),
                    factory.clone() as Arc<dyn deltalake::logstore::ObjectStoreFactory>,
                );
                deltalake::logstore::logstore_factories().insert(
                    scheme,
                    factory as Arc<dyn deltalake::logstore::LogStoreFactory>,
                );
                struck
            })
            .clone()
    }

    /// What a [`Error::DataQualityWrite`] says failed beneath it.
    fn cause(e: &Error) -> &Error {
        match e {
            Error::DataQualityWrite { source, .. } => source,
            other => panic!("expected a failed data-quality write, got {other:?}"),
        }
    }

    /// Every way `write_kafka` fails is an error a retry loop can sort by type rather than by
    /// its text (docs/kafka-design.md §3.6): a failed write with what failed typed beneath
    /// it, or a conflict that outlasted the retries. Never a bare cause, which would read as
    /// the target's: a log cleaned up under a stale data-quality handle is
    /// `OffsetLogTruncated` beneath, which on its own would replan the target.
    #[tokio::test]
    async fn dq_write_failures_are_typed() {
        let dir = tempfile::tempdir().unwrap();

        // A table that is not a data-quality table.
        let wrong = dir.path().join("wrong");
        let wrong = wrong.to_str().unwrap();
        DeltaTable::try_from_url(deltalake::ensure_table_uri(wrong).unwrap())
            .await
            .unwrap()
            .create()
            .with_columns(vec![StructField::new(
                "x",
                DeltaType::Primitive(PrimitiveType::Long),
                true,
            )])
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
        let e = open_dq(wrong)
            .await
            .write_kafka(
                &[undecodable(0, &[3], MALFORMED)],
                &read(&[(0, (0, 9))]),
                NOW,
            )
            .await
            .unwrap_err();
        assert!(matches!(cause(&e), Error::Delta(_)), "{e:?}");

        // A data-quality handle that log cleanup left behind.
        let path = dir.path().join("dq");
        let path = path.to_str().unwrap();
        create_dq(path).await;
        let mut stale = open_dq(path).await;
        let first = [undecodable(0, &[1], MALFORMED)];
        assert_eq!(
            stale
                .write_kafka(&first, &read(&[(0, (0, 1))]), NOW)
                .await
                .unwrap(),
            1
        ); // 1
        let mut other = open_dq(path).await;
        for offset in 10..13 {
            let theirs = [undecodable(1, &[offset], MALFORMED)];
            other
                .write_kafka(&theirs, &read(&[(1, (offset, offset))]), NOW)
                .await
                .unwrap();
        } // 2..=4
        let head = deltalake::open_table(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap();
        deltalake::checkpoints::create_checkpoint(&head, None)
            .await
            .unwrap();
        for v in 0..=3u64 {
            std::fs::remove_file(format!("{path}/_delta_log/{v:020}.json")).unwrap();
        }
        let next = [undecodable(0, &[5], MALFORMED)];
        let e = stale
            .write_kafka(&next, &read(&[(0, (2, 5))]), NOW)
            .await
            .unwrap_err();
        assert!(
            matches!(cause(&e), Error::OffsetLogTruncated { version: 1, .. }),
            "{e:?}"
        );
        // The failure reopened the handle, so the batch's next attempt goes through.
        assert_eq!(
            stale
                .write_kafka(&next, &read(&[(0, (2, 5))]), NOW)
                .await
                .unwrap(),
            1
        );
    }

    /// An object store that refuses the commit: the store's own error, typed beneath.
    #[cfg(unix)]
    #[tokio::test]
    async fn dq_store_fault_is_typed() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        let log = dir.path().join("_delta_log");
        let writable = std::fs::metadata(&log).unwrap().permissions();
        std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o555)).unwrap();
        let read_only = std::fs::File::create(log.join("probe")).is_err();
        let outcome = dq
            .write_kafka(
                &[undecodable(0, &[3], MALFORMED)],
                &read(&[(0, (0, 9))]),
                NOW,
            )
            .await;
        std::fs::set_permissions(&log, writable).unwrap();
        if !read_only {
            return; // running as root, where the mode bits do not bite. Nothing to pin.
        }
        let e = outcome.unwrap_err();
        assert!(
            matches!(e, Error::DataQualityWrite { rows: 1, .. }),
            "{e:?}"
        );
        assert!(matches!(cause(&e), Error::Delta(_)), "{e:?}");
    }

    /// A rival that commits the same partition's mark first, every time: the retries in place
    /// run out, and the error says so by its type.
    #[tokio::test]
    async fn dq_conflict_past_the_retries_in_place_is_typed() {
        let struck = rival_scheme();
        let dir = tempfile::tempdir().unwrap();
        let path = std::fs::canonicalize(dir.path()).unwrap();
        create_dq(path.to_str().unwrap()).await;
        let uri = format!("{RIVAL_SCHEME}://{}", path.to_str().unwrap());
        let mut dq = DataQuality::open(&crate::storage::Storage::default(), &uri, "app", "orders")
            .await
            .unwrap()
            .expect("the table was just created");

        let e = dq
            .write_kafka(
                &[undecodable(0, &[10, 11], MALFORMED)],
                &read(&[(0, (10, 12))]),
                NOW,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                e,
                Error::DataQualityConflict {
                    rows: 2,
                    attempts: 4,
                    ..
                }
            ),
            "{e:?}"
        );
        assert_eq!(dq.kafka_counters().conflict_retries(), 3);
        assert_eq!(struck.load(Ordering::SeqCst), 4, "one rival per attempt");
    }
}
