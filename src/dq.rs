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
//! # Kafka: no skip at all
//!
//! A Kafka batch's rejects are a plain append ([`DataQuality::write_kafka`]), each row with
//! its message's offset in `source_version` where the row says which message it came from, and
//! nothing that would let a replay skip them. A replay after the rejects landed and the target
//! commit did not writes them again: a visible duplicate, which a reader removes by the whole
//! row but `_timestamp` — never by partition and offset alone, which one message that fans out
//! into several rows shares among genuine rejects (docs/kafka-design.md §5.7, I9).
//!
//! Every skip tried for Kafka lost a reject some way: a reject is judged against things a
//! replay can find changed (the target's schema after an ALTER TABLE, the union of writer
//! schemas, the SQL, the decoder build), and a mark per partition says nothing about which
//! offsets below it a replay would reject. Losing one is worse than recording one twice. And
//! with no `txn` in it, a commit here never contends with another member's, which the
//! members of a consumer group, all writing one DQ table, would otherwise do on every write.

use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
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

/// What recording a Kafka pipeline's rejects has cost, for
/// `ddi_kafka_dq_conflict_retries_total`.
#[derive(Debug, Default)]
pub struct KafkaCounters {
    conflict_retries: AtomicU64,
}

impl KafkaCounters {
    /// Writes tried again in place after a conflict that was not their own commit.
    pub fn conflict_retries(&self) -> u64 {
        self.conflict_retries.load(Ordering::Relaxed)
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

    /// Add to `counters` from now on: a pipeline's metrics, which outlive this table's handle
    /// when it is opened again.
    pub fn set_kafka_counters(&mut self, counters: Arc<KafkaCounters>) {
        self.kafka = counters;
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
        let mut props = CommitProperties::default()
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
        // As a target's commit (`Sink::properties`): the default cleanup, except on a table
        // that protects its checkpoints.
        if crate::fence::protects_checkpoints(&table) {
            props = props.with_cleanup_expired_logs(Some(false));
        }

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

    /// Write the rejects of a Kafka batch that read `offsets.ranges`, as a plain append.
    ///
    /// Returns how many rows were written: every reject, every time (docs/kafka-design.md
    /// §5.7). In steps:
    /// 1. Each row's `source_version` is its message's offset, where its `<prefix>partition`
    ///    and `<prefix>offset` columns name an offset the batch read ([`Self::coordinates`]),
    ///    and −1 otherwise. Its partition is in its payload, beside the offset.
    /// 2. One commit appends them, with no `txn`, and records `ddi.appId`,
    ///    `ddi.kafka.offsets` and `ddi.kafka.dqWriteId`, an id unique to this call. First, a
    ///    table whose protocol or column mapping delta-rs cannot write is refused, as a
    ///    target is, before anything is written ([`crate::fence::refuse_unwritable_protocol`]).
    /// 3. A commit that ends in a conflict (another engine changed the table's metadata or
    ///    protocol, a DELETE, UPDATE or MERGE removed files from it, or delta-rs ran out of
    ///    attempts) reopens the table. When the commit that landed is this write's own — it
    ///    did land, and the acknowledgement of its PUT was lost — the write is done
    ///    ([`Self::landed`]); otherwise it goes back to step 2, up to
    ///    [`KAFKA_CONFLICT_RETRIES`] times. After that it is [`Error::DataQualityConflict`],
    ///    for the pipeline to retry, never an error that replans the target. Another member's
    ///    append is no conflict: delta-rs rebases a plain append over another.
    /// 4. A write that fails otherwise may still have landed: the checkpoint or the log
    ///    cleanup after its commit failed, or the response to its PUT was lost and the retries
    ///    ended in a store error. It too reopens the table and looks for its own commit, and is
    ///    done when it finds it. Only when that reopen or that read fails is it left to the
    ///    pipeline's retry, which may then record the rejects twice.
    ///
    /// A replay of the batch, after these rejects landed and the target commit did not, writes
    /// them again: a visible duplicate. A reader removes it by the whole row but `_timestamp`
    /// — partition, offset, `column_name`, `reason` and `payload` — and never by partition and
    /// offset alone, under which a message that fans out into several rows (a `CROSS JOIN
    /// UNNEST`) has several genuine rejects. Nothing is skipped, because every rule tried for
    /// knowing that a reject is already recorded lost one some way, and a lost reject is worse
    /// than a duplicate (I9).
    ///
    /// Every other failure is [`Error::DataQualityWrite`], with what failed beneath it as it
    /// was raised — delta-rs's error, `Capacity`, the `Config` of a failed reopen, or the
    /// `TargetProtocolUnsupported` a table with a protocol or column mapping delta-rs cannot
    /// write is refused with before anything is written — so that the pipeline can tell a
    /// fault worth retrying from a table it cannot write, and neither from a failure of the
    /// target ([`Error::kafka_class`], §3.6).
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
        let batch = self
            .kafka_batch(rejected, &offsets.ranges, now_micros)
            .map_err(|cause| self.unwritable(rows, cause))?;
        // Recorded with the commit, so that a conflict can be told from this write's own
        // commit having landed already (`Self::landed`).
        let write_id = write_id();

        let mut retries = 0;
        loop {
            let read_version = self.table.version();
            let e = match self.try_write_kafka(&batch, offsets, &write_id).await {
                Ok(()) => return Ok(rows),
                Err(Attempt::Conflict(e)) => e,
                Err(Attempt::Failed(cause)) => {
                    // As in `write`: the next attempt starts from the table as it is now, so
                    // a handle that has fallen behind log cleanup does not fail it again.
                    if let Ok(table) = self.storage.open(&self.uri).await {
                        self.table = table;
                    }
                    return Err(self.unwritable(rows, cause));
                }
                Err(Attempt::Ambiguous(cause)) => {
                    // The commit may have landed: delta-rs reports an error after it did when
                    // the checkpoint or the log cleanup after it fails, or when the response to
                    // its PUT was lost and the retries ended in a store error. Retried with the
                    // batch, it would record every reject again, in a process that never
                    // crashed. Only when the table cannot be reopened or its log read is it left
                    // to that retry, which may then write them twice (I9).
                    let Ok(table) = self.storage.open(&self.uri).await else {
                        return Err(self.unwritable(rows, cause));
                    };
                    self.table = table;
                    if let Ok(true) = self.landed(read_version, &write_id).await {
                        debug!(
                            app_id = %self.app_id,
                            dq_uri = %self.uri,
                            "the data-quality commit landed before this failure ({cause}); not \
                             writing it twice"
                        );
                        return Ok(rows);
                    }
                    return Err(self.unwritable(rows, cause));
                }
            };
            self.table = match self.storage.open(&self.uri).await {
                Ok(table) => table,
                Err(cause) => return Err(self.unwritable(rows, cause)),
            };
            let landed = match self.landed(read_version, &write_id).await {
                Ok(landed) => landed,
                Err(cause) => return Err(self.unwritable(rows, cause)),
            };
            if landed {
                debug!(
                    app_id = %self.app_id,
                    dq_uri = %self.uri,
                    "the commit reported as a conflict ({e}) is preceded by this write's own, \
                     which landed before its acknowledgement was lost; not writing it twice"
                );
                return Ok(rows);
            }
            if retries < KAFKA_CONFLICT_RETRIES {
                retries += 1;
                self.kafka.conflict_retries.fetch_add(1, Ordering::Relaxed);
                warn!(
                    app_id = %self.app_id,
                    dq_uri = %self.uri,
                    attempt = retries,
                    "recording this batch's rejects conflicted with another commit ({e}); \
                     trying again on the table as it is now"
                );
                continue;
            }
            return Err(Error::DataQualityConflict {
                dq_uri: self.uri.clone(),
                rows,
                attempts: KAFKA_CONFLICT_RETRIES + 1,
                detail: e.to_string(),
            });
        }
    }

    /// Step 1 of [`Self::write_kafka`]: every reject of the batch, each with its message's
    /// offset as its `source_version`, or −1.
    fn kafka_batch(
        &self,
        rejected: &[Rejected],
        ranges: &BTreeMap<i32, (i64, i64)>,
        now_micros: i64,
    ) -> Result<RecordBatch> {
        let mut source_versions = Vec::new();
        let mut columns = Vec::new();
        let mut reasons = Vec::new();
        let mut payload = Vec::new();
        for r in rejected {
            let coordinates = self.coordinates(r, ranges);
            source_versions.extend(coordinates.iter().map(|c| c.map_or(-1, |(_, o)| o)));
            columns.extend(r.columns.iter().cloned());
            reasons.extend(r.reasons.iter().cloned());
            payload.extend(payloads(&r.rows)?);
        }
        self.assemble(source_versions, columns, reasons, payload, now_micros)
    }

    /// Step 2 of [`Self::write_kafka`], once.
    async fn try_write_kafka(
        &mut self,
        batch: &RecordBatch,
        offsets: &KafkaOffsets,
        write_id: &str,
    ) -> std::result::Result<(), Attempt> {
        // Asked first, as of a target: delta-rs reads a column-mapped table and refuses to
        // write one only once it is writing, with an error of no type of its own, which would
        // be retried as a passing fault for as long as the table stays as it is.
        crate::fence::refuse_unwritable_protocol(&self.table).map_err(Attempt::Failed)?;
        let ranges: serde_json::Map<String, serde_json::Value> = offsets
            .ranges
            .iter()
            .map(|(p, (first, last))| (p.to_string(), serde_json::json!([first, last])))
            .collect();
        // The log is cleaned up, where the table allows it, only by a commit that also
        // checkpoints, as the target's is (docs/kafka-design.md §7.2): otherwise every commit
        // would list the whole log after it lands, and fail there wherever it may not delete.
        // A commit rebased past the version expected cleans up once more or once less.
        use deltalake::table::config::TablePropertiesExt;
        let properties = self
            .table
            .snapshot()
            .map_err(|e| Attempt::Failed(Error::Delta(e)))?
            .table_config();
        let next = self.table.version().map_or(0, |v| v + 1);
        let cleanup = properties.enable_expired_log_cleanup()
            && (next + 1).is_multiple_of(properties.checkpoint_interval().get())
            && !crate::fence::protects_checkpoints(&self.table);
        // No `txn`: a plain append, which delta-rs rebases over any other.
        let props = CommitProperties::default()
            .with_cleanup_expired_logs(Some(cleanup))
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
        let session = crate::budget::delta_session(&self.table).map_err(Attempt::Failed)?;
        let session = Arc::new(session);
        // A clone rather than the handle itself, so that a failed write leaves a usable
        // handle behind even when the table cannot be reopened.
        let written = self
            .table
            .clone()
            .write(vec![batch.clone()])
            .with_session_state(session)
            .with_save_mode(SaveMode::Append)
            .with_cast_safety(false)
            .with_commit_properties(props)
            .await;
        match written {
            Ok(t) => {
                self.table = t;
                Ok(())
            }
            Err(e) if is_conflict(&e) => Err(Attempt::Conflict(e)),
            // Typed, as the target's own write is: a full spill directory is out of capacity,
            // and anything else is delta-rs's error as it came. Raised by the write itself, it
            // may have been raised after the commit landed.
            Err(e) => Err(Attempt::Ambiguous(crate::spill::classify_delta(
                e,
                "data-quality: writing rejects",
            ))),
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
    /// A conflict does not mean the write did not land. When the response to a commit's PUT is
    /// lost and the PUT is retried, it finds the version taken by that very commit; delta-rs
    /// then checks the commits since its snapshot, its own among them, and gives up when a
    /// later one conflicts (another engine's metadata change, or its attempts run out), though
    /// its own is in (docs/kafka-design.md §4.7 row 10). Tried again, the write would record
    /// every reject a second time, in a process that never crashed. Read newest first. A
    /// commit no longer in the log ends the search: one that cannot be read is not known to be
    /// this write's, and writing again costs a duplicate, never a lost reject. One that is
    /// there but does not parse is not this write's, which wrote valid JSON.
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
    /// coordinates. Nor has one whose columns name a message the batch did not read: a
    /// message's own coordinates always lie inside the batch, so those are something else
    /// under the same names — values the SQL computed, or a payload's own fields read under
    /// the default prefix where the pipeline set another — and recorded as its offset they
    /// would name another message.
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

/// How one attempt of [`DataQuality::write_kafka`] ended, when it did not write.
enum Attempt {
    /// delta-rs gave up on a conflict: another writer's commit, worth trying again in place,
    /// unless this attempt's own landed before it.
    Conflict(DeltaTableError),
    /// The write failed otherwise, possibly after its commit landed: a failed checkpoint or
    /// log cleanup after it, or a PUT whose response was lost and whose retries failed.
    Ambiguous(Error),
    /// Refused before anything was written.
    Failed(Error),
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

/// Did a commit fail because another one landed first and changed what it was planned on?
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
            takeover_marker: false,
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

    /// A Kafka batch's rejects are a plain append: no `txn`, so that no two writers of the
    /// table ever contend on one, and each row with its message's offset where it is known.
    #[tokio::test]
    async fn dq_kafka_commit_is_a_plain_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;

        let batch = [undecodable(0, &[3, 7], MALFORMED), uncoordinated(&[41])];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            3
        );
        let actions = head_commit(path).await;
        assert_eq!(txns_of(&actions), vec![], "no txn: {actions:?}");
        let info = info_of(&actions);
        assert_eq!(info["ddi.appId"], serde_json::json!("app"));
        assert_eq!(info["ddi.kafka.offsets"], serde_json::json!({"0": [0, 9]}));
        assert!(info["ddi.kafka.dqWriteId"].is_string(), "{info:?}");
        for absent in ["ddi.sourceVersion", "ddi.kafka.decoder"] {
            assert!(!info.contains_key(absent), "{absent} in {info:?}");
        }

        let rows = recorded(path).await;
        assert_eq!(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![-1, 3, 7],
            "source_version holds each message's offset, -1 where it has none"
        );
        let payload = rows[2].2.as_deref().unwrap();
        assert!(payload.contains("\"kafka_partition\":0"), "{payload}");
        assert!(payload.contains("\"kafka_offset\":7"), "{payload}");
    }

    /// The target commit failed after the rejects were recorded, and the batch is read again:
    /// by this process, or by the next owner of the partition. Its rejects are written again,
    /// a visible duplicate a reader removes by the whole row but `_timestamp`; a skip that
    /// judged them recorded could lose one, which is worse.
    #[tokio::test]
    async fn dq_a_replay_writes_its_rejects_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;

        let batch = [undecodable(0, &[3], MALFORMED)];
        let offsets = read(&[(0, (0, 9))]);
        assert_eq!(dq.write_kafka(&batch, &offsets, NOW).await.unwrap(), 1);
        assert_eq!(dq.write_kafka(&batch, &offsets, NOW).await.unwrap(), 1);
        let rows = recorded(path).await;
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0], rows[1], "the same reject, twice");

        let mut next_owner = open_dq(path).await;
        assert_eq!(
            next_owner.write_kafka(&batch, &offsets, NOW).await.unwrap(),
            1
        );
        assert_eq!(recorded(path).await.len(), 3);
    }

    /// Two members of a group write rejects side by side, for the same partition even, as a
    /// zombie and the partition's new owner do: plain appends, which delta-rs rebases over each
    /// other, so neither conflicts nor retries.
    #[tokio::test]
    async fn dq_two_members_append_without_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        // Both with a handle on version 0.
        let (mut a, mut b) = (open_dq(path).await, open_dq(path).await);

        let from_a = [undecodable(0, &[2, 4], MALFORMED)];
        let from_b = [undecodable(0, &[2, 4, 7], MALFORMED)];
        assert_eq!(
            a.write_kafka(&from_a, &read(&[(0, (0, 5))]), NOW)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            b.write_kafka(&from_b, &read(&[(0, (0, 8))]), NOW)
                .await
                .unwrap(),
            3
        );
        assert_eq!(a.kafka_counters().conflict_retries(), 0);
        assert_eq!(
            b.kafka_counters().conflict_retries(),
            0,
            "rebased by delta-rs, never retried here"
        );
        let rows = recorded(path).await;
        assert_eq!(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![2, 2, 4, 4, 7]
        );
    }

    #[tokio::test]
    async fn dq_without_coordinates_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;

        // A coerce reject from SQL that dropped the kafka_ columns, and an undecodable whose
        // offset is null: neither names a message, so neither has an offset to record.
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

        let rows = recorded(path).await;
        assert_eq!(rows.len(), 3);
        assert!(
            rows.iter().all(|r| r.0 == -1),
            "no offset to record: {rows:?}"
        );

        // Under another metadata prefix, kafka_partition and kafka_offset are just columns.
        let mut prefixed = open_dq(path).await;
        prefixed.set_kafka_metadata_prefix("meta_");
        let batch = [undecodable(0, &[5], MALFORMED)];
        assert_eq!(
            prefixed
                .write_kafka(&batch, &read(&[(0, (0, 8))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(recorded(path).await.iter().filter(|r| r.0 == 5).count(), 0);
    }

    /// A message's coordinates always lie inside what its batch read. A reject whose
    /// `<prefix>partition` and `<prefix>offset` lie outside — computed by the SQL under those
    /// names, or a payload's own fields read under the default prefix where the pipeline set
    /// another — names no message of the batch, and its row records no offset rather than one
    /// that names another message.
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

    /// Neither data-quality write cleans up the log of a table that protects its checkpoints,
    /// and both still checkpoint it.
    #[tokio::test]
    async fn dq_never_cleans_up_a_log_that_protects_its_checkpoints() {
        // Four writes to a table that checkpoints every 2 versions and keeps no log history;
        // `protected` decides whether another engine committed `checkpointProtection` first
        // (version 1).
        async fn log_after(protected: bool, kafka: bool) -> Vec<String> {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap();
            DeltaTable::try_from_url(deltalake::ensure_table_uri(path).unwrap())
                .await
                .unwrap()
                .create()
                .with_columns(columns())
                .with_configuration([
                    ("delta.checkpointInterval", Some("2")),
                    ("delta.logRetentionDuration", Some("interval 0 seconds")),
                ])
                .with_save_mode(SaveMode::ErrorIfExists)
                .await
                .unwrap();
            if protected {
                crate::fence::protect_checkpoints_for_tests(dir.path(), 0, 1);
            }
            let mut dq = open_dq(path).await;
            for version in 1..=4 {
                // Apart in time, so that every commit is older than the cleanup after it.
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let rejects = [uncoordinated(&[version])];
                let written = match kafka {
                    true => {
                        dq.write_kafka(&rejects, &read(&[(0, (version, version))]), NOW)
                            .await
                    }
                    false => dq.write(&rejects, version, NOW).await,
                };
                assert_eq!(written.unwrap(), 1);
            }
            let mut log: Vec<String> = std::fs::read_dir(dir.path().join("_delta_log"))
                .unwrap()
                .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
                .filter(|name| !name.starts_with('.'))
                .collect();
            log.sort();
            log
        }

        for kafka in [false, true] {
            // Unprotected, the write at version 3 checkpoints and cleans up below it.
            assert_eq!(
                log_after(false, kafka).await,
                [
                    "00000000000000000003.checkpoint.parquet",
                    "00000000000000000003.json",
                    "00000000000000000004.json",
                    "_last_checkpoint",
                ],
                "kafka: {kafka}"
            );
            assert_eq!(
                log_after(true, kafka).await,
                [
                    "00000000000000000000.json",
                    "00000000000000000001.json",
                    "00000000000000000002.json",
                    "00000000000000000003.checkpoint.parquet",
                    "00000000000000000003.json",
                    "00000000000000000004.json",
                    "00000000000000000005.checkpoint.parquet",
                    "00000000000000000005.json",
                    "_last_checkpoint",
                ],
                "kafka: {kafka}"
            );
        }
    }

    // ---------------------------------------------------------------- commits that go wrong

    use crate::error::KafkaClass;
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
        /// Every time, write another engine's change of the table's metadata there first: a
        /// rival that always wins the race, with a commit a plain append conflicts with.
        RivalFirst,
        /// Write it, have another writer change the table's metadata in the next version,
        /// then say it already exists, the first time only: delta-rs then meets that change
        /// in its conflict check, after its own commit, and reports a conflict.
        LandsThenAlteredThenSaysItExists,
        /// Write it, then fail as a store does whose retries ran out, the first time only: the
        /// response to the PUT was lost, and so were the retries'.
        LandsThenFails,
        /// Fail every PUT of a checkpoint, after the commit it follows has landed.
        CheckpointFails,
        /// Refuse every delete, as an immutable container does; count each.
        DeleteDenied,
        /// The first time only, write another engine's DELETE there first: a commit that
        /// removes, with `dataChange`, every file the previous commit added.
        RivalDeletesFirst,
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
                Fault::LandsThenAlteredThenSaysItExists => {
                    let put = self.inner.put_opts(location, payload, opts).await?;
                    if commit && self.struck.fetch_add(1, Ordering::SeqCst) == 0 {
                        let next = next_commit(location);
                        let altered = metadata_change(self.inner.as_ref(), location).await;
                        self.inner
                            .put_opts(&next, altered.into(), PutOptions::default())
                            .await?;
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
                        let rival = metadata_change(self.inner.as_ref(), location).await;
                        self.inner
                            .put_opts(location, rival.into(), PutOptions::default())
                            .await?;
                    }
                    self.inner.put_opts(location, payload, opts).await
                }
                Fault::LandsThenFails => {
                    let put = self.inner.put_opts(location, payload, opts).await?;
                    if commit && self.struck.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(object_store::Error::Generic {
                            store: "Azure",
                            source: "the response was lost, and the retries ended in 503".into(),
                        });
                    }
                    Ok(put)
                }
                Fault::CheckpointFails if is_checkpoint(location) => {
                    self.struck.fetch_add(1, Ordering::SeqCst);
                    Err(checkpoint_refused())
                }
                Fault::RivalDeletesFirst => {
                    if commit && self.struck.fetch_add(1, Ordering::SeqCst) == 0 {
                        let rival = delete_of_previous(self.inner.as_ref(), location).await;
                        self.inner
                            .put_opts(location, rival.into(), PutOptions::default())
                            .await?;
                    }
                    self.inner.put_opts(location, payload, opts).await
                }
                Fault::CheckpointFails | Fault::DeleteDenied => {
                    self.inner.put_opts(location, payload, opts).await
                }
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            if matches!(self.fault, Fault::CheckpointFails) && is_checkpoint(location) {
                self.struck.fetch_add(1, Ordering::SeqCst);
                return Err(checkpoint_refused());
            }
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
            if matches!(self.fault, Fault::DeleteDenied) {
                use futures::StreamExt;
                let struck = self.struck.clone();
                return locations
                    .map(move |location| {
                        struck.fetch_add(1, Ordering::SeqCst);
                        Err(object_store::Error::PermissionDenied {
                            path: location?.to_string(),
                            source: "403 This operation is not permitted on an immutable \
                                     container"
                                .into(),
                        })
                    })
                    .boxed();
            }
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

    fn is_checkpoint(location: &Path) -> bool {
        location.as_ref().contains(".checkpoint.")
    }

    fn checkpoint_refused() -> object_store::Error {
        object_store::Error::Generic {
            store: "Azure",
            source: "503 Server Busy, writing the checkpoint".into(),
        }
    }

    /// Another engine's DELETE, as the commit at `location`: it removes, with `dataChange`,
    /// every file the commit before it added.
    async fn delete_of_previous(store: &dyn ObjectStore, location: &Path) -> String {
        use deltalake::logstore::object_store::ObjectStoreExt;
        let (dir, name) = location.as_ref().rsplit_once('/').unwrap();
        let version: u64 = name.strip_suffix(".json").unwrap().parse().unwrap();
        let previous = store
            .get(&Path::from(format!("{dir}/{:020}.json", version - 1)))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut commit = serde_json::json!({"commitInfo": {
            "timestamp": 1_700_000_000_000i64, "operation": "DELETE"
        }})
        .to_string();
        for line in std::str::from_utf8(&previous).unwrap().lines() {
            let line: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(add) = line.get("add") {
                let remove = serde_json::json!({"remove": {
                    "path": add["path"], "dataChange": true,
                    "deletionTimestamp": 1_700_000_000_000i64
                }});
                commit.push('\n');
                commit.push_str(&remove.to_string());
            }
        }
        commit.push('\n');
        commit
    }

    /// The commit file after the one at `location`.
    fn next_commit(location: &Path) -> Path {
        let (dir, name) = location.as_ref().rsplit_once('/').unwrap();
        let version: u64 = name.strip_suffix(".json").unwrap().parse().unwrap();
        Path::from(format!("{dir}/{:020}.json", version + 1))
    }

    /// A commit that sets a table property, as another engine's `ALTER TABLE` writes it: the
    /// `metaData` of the table's commit 0 at `<location's directory>`, with one more key.
    async fn metadata_change(store: &dyn ObjectStore, location: &Path) -> String {
        use deltalake::logstore::object_store::ObjectStoreExt;
        let (dir, _) = location.as_ref().rsplit_once('/').unwrap();
        let zero = store
            .get(&Path::from(format!("{dir}/{:020}.json", 0)))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let mut metadata = std::str::from_utf8(&zero)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .find(|l| l.get("metaData").is_some())
            .unwrap();
        metadata["metaData"]["configuration"]["comment"] = serde_json::json!("altered");
        let info = serde_json::json!({"commitInfo": {
            "timestamp": 1_700_000_000_000i64, "operation": "SET TBLPROPERTIES"
        }});
        format!("{info}\n{metadata}\n")
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
        let flaky = |inner| -> Arc<dyn ObjectStore> {
            Arc::new(FlakyCommits {
                inner,
                fault,
                struck: struck.clone(),
            })
        };
        let flaky = deltalake::logstore::default_logstore(
            flaky(store.object_store(None)),
            flaky(store.root_object_store(None)),
            store.config().location(),
            store.config().options(),
        );
        let mut table = DeltaTable::new(flaky);
        table.load().await.unwrap();
        dq.table = table;
        struck
    }

    /// The data-quality commit landed, but the response to its PUT was lost, and the retried
    /// PUT found the version taken — by itself. delta-rs then checks its own commit for a
    /// conflict, finds none in a plain append, and lands the very same actions at the next
    /// version: the same files, under the same `ddi.kafka.dqWriteId`, which the table counts
    /// once. Each reject is recorded once, in a process that never crashed.
    #[tokio::test]
    async fn dq_commit_that_landed_unacknowledged_is_not_written_twice() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        let struck = with_fault(&mut dq, Fault::LandsThenSaysItExists).await;

        let batch = [undecodable(0, &[3, 7], MALFORMED), uncoordinated(&[1])];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            struck.load(Ordering::SeqCst),
            2,
            "the fault struck the first PUT, and delta-rs put the same commit again"
        );

        let rows = recorded(path).await;
        assert_eq!(rows.len(), 3, "each reject once: {rows:?}");
        let table = deltalake::open_table(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap();
        let at = |version: u64| {
            let table = &table;
            async move {
                let raw = table
                    .log_store()
                    .read_commit_entry(version)
                    .await
                    .unwrap()
                    .unwrap();
                let actions = deltalake::logstore::get_actions(version, &raw).unwrap();
                let files: Vec<String> = actions
                    .iter()
                    .filter_map(|a| match a {
                        Action::Add(add) => Some(add.path.clone()),
                        _ => None,
                    })
                    .collect();
                (files, info_of(&actions)[KAFKA_WRITE_ID].clone())
            }
        };
        let (first, second) = (at(1).await, at(2).await);
        assert_eq!(first, second, "the same files, under the same write");
        assert_eq!(table.get_file_uris().unwrap().count(), first.0.len());
        assert_eq!(
            dq.kafka_counters().conflict_retries(),
            0,
            "nobody else was there to conflict with"
        );
    }

    /// The data-quality commit landed, the response to its PUT was lost, and before the retry
    /// another engine changed the table's metadata. delta-rs's conflict check then meets that
    /// change and gives up with a conflict, though its own commit is in. The write finds its
    /// commit by `ddi.kafka.dqWriteId` and is done: tried again in place, it would record every
    /// reject a second time, in a process that never crashed.
    #[tokio::test]
    async fn dq_landed_commit_is_found_when_a_conflict_follows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        let struck = with_fault(&mut dq, Fault::LandsThenAlteredThenSaysItExists).await;

        let batch = [undecodable(0, &[3, 7], MALFORMED), uncoordinated(&[1])];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            3
        );
        assert_eq!(struck.load(Ordering::SeqCst), 1, "the fault struck");
        let rows = recorded(path).await;
        assert_eq!(rows.len(), 3, "each reject once: {rows:?}");
        let table = deltalake::open_table(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(2), "ours, then the other engine's");
        assert_eq!(dq.kafka_counters().conflict_retries(), 0);
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
    /// the target's. A table the rejects cannot be written to is the protocol refusal, or
    /// delta-rs's protocol or Arrow error, beneath: Blocking. A log cleaned up under a stale
    /// data-quality handle is delta-rs's `InvalidVersion` beneath: Transient, which the reopen
    /// after it cures.
    #[tokio::test]
    async fn dq_write_failures_are_typed() {
        let dir = tempfile::tempdir().unwrap();
        let create_with = |path: &str, columns: Vec<StructField>| {
            let url = deltalake::ensure_table_uri(path).unwrap();
            async move {
                DeltaTable::try_from_url(url)
                    .await
                    .unwrap()
                    .create()
                    .with_columns(columns)
                    .with_save_mode(SaveMode::ErrorIfExists)
                    .await
                    .unwrap();
            }
        };
        let fail = |path: &str| {
            let path = path.to_string();
            async move {
                open_dq(&path)
                    .await
                    .write_kafka(
                        &[undecodable(0, &[3], MALFORMED)],
                        &read(&[(0, (0, 9))]),
                        NOW,
                    )
                    .await
                    .unwrap_err()
            }
        };
        let transaction = |e: &Error| match cause(e) {
            Error::Delta(DeltaTableError::Transaction { source }) => Some(source.to_string()),
            _ => None,
        };

        // A table that is not a data-quality table: it lacks even the feature the rejects'
        // timestamp column needs.
        let wrong = dir.path().join("wrong");
        let wrong = wrong.to_str().unwrap();
        create_with(
            wrong,
            vec![StructField::new(
                "x",
                DeltaType::Primitive(PrimitiveType::Long),
                true,
            )],
        )
        .await;
        let e = fail(wrong).await;
        assert_eq!(e.kafka_class(), KafkaClass::BlockingPipeline, "{e:?}");
        assert!(
            matches!(
                cause(&e),
                Error::Delta(DeltaTableError::Transaction {
                    source: TransactionError::TableFeaturesRequired(_)
                })
            ),
            "{e:?} {:?}",
            transaction(&e)
        );

        // A data-quality table with a column the rejects do not have.
        let extra = dir.path().join("extra");
        let extra = extra.to_str().unwrap();
        let mut with_extra = columns();
        with_extra.push(StructField::new(
            "extra",
            DeltaType::Primitive(PrimitiveType::Long),
            false,
        ));
        create_with(extra, with_extra).await;
        let e = fail(extra).await;
        assert_eq!(e.kafka_class(), KafkaClass::BlockingPipeline, "{e:?}");
        assert!(
            matches!(cause(&e), Error::Delta(DeltaTableError::Arrow { .. })),
            "{e:?}"
        );

        // A data-quality table another engine upgraded to a writer feature delta-rs does not
        // write.
        let upgraded = dir.path().join("upgraded");
        let upgraded = upgraded.to_str().unwrap();
        create_dq(upgraded).await;
        std::fs::write(
            format!("{upgraded}/_delta_log/00000000000000000001.json"),
            "{\"commitInfo\":{\"timestamp\":1700000000000,\"operation\":\"UPGRADE PROTOCOL\"}}\n\
             {\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\
             \"readerFeatures\":[\"timestampNtz\"],\
             \"writerFeatures\":[\"timestampNtz\",\"identityColumns\"]}}\n",
        )
        .unwrap();
        // Refused before anything is written, as a target with that protocol is.
        let e = fail(upgraded).await;
        assert_eq!(e.kafka_class(), KafkaClass::BlockingPipeline, "{e:?}");
        assert!(
            matches!(cause(&e), Error::TargetProtocolUnsupported { detail, .. }
                if detail.contains("IdentityColumns")),
            "{e:?}"
        );

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
            matches!(cause(&e), Error::Delta(DeltaTableError::InvalidVersion(1))),
            "{e:?}"
        );
        assert_eq!(e.kafka_class(), KafkaClass::Transient, "{e:?}");
        // The failure reopened the handle, so the batch's next attempt goes through.
        assert_eq!(
            stale
                .write_kafka(&next, &read(&[(0, (2, 5))]), NOW)
                .await
                .unwrap(),
            1
        );
    }

    /// Commit `version` of the data-quality table at `path` by hand, as another engine that
    /// turns column mapping on writes it: the protocol gains the `columnMapping` feature, and
    /// every column a field id and a physical name.
    fn turn_on_column_mapping(path: &str, version: u64) {
        let zero = std::fs::read_to_string(format!("{path}/_delta_log/{:020}.json", 0)).unwrap();
        let lines: Vec<serde_json::Value> = zero
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let mut metadata = lines
            .iter()
            .find(|l| l.get("metaData").is_some())
            .unwrap()
            .clone();
        let mut schema: serde_json::Value =
            serde_json::from_str(metadata["metaData"]["schemaString"].as_str().unwrap()).unwrap();
        let fields = schema["fields"].as_array_mut().unwrap();
        for (i, field) in fields.iter_mut().enumerate() {
            field["metadata"] = serde_json::json!({
                "delta.columnMapping.id": i + 1,
                "delta.columnMapping.physicalName": format!("col-{i}"),
            });
        }
        let max = fields.len();
        metadata["metaData"]["schemaString"] = serde_json::json!(schema.to_string());
        metadata["metaData"]["configuration"]["delta.columnMapping.mode"] =
            serde_json::json!("name");
        metadata["metaData"]["configuration"]["delta.columnMapping.maxColumnId"] =
            serde_json::json!(max.to_string());
        let protocol = serde_json::json!({"protocol": {
            "minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["timestampNtz", "columnMapping"],
            "writerFeatures": ["timestampNtz", "columnMapping"],
        }});
        let info = serde_json::json!({"commitInfo": {
            "timestamp": 1_700_000_000_000i64, "operation": "SET TBLPROPERTIES"
        }});
        std::fs::write(
            format!("{path}/_delta_log/{version:020}.json"),
            format!("{info}\n{protocol}\n{metadata}\n"),
        )
        .unwrap();
    }

    /// The data files under the table at `path`.
    fn data_files(path: &str) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".parquet")
            })
            .count()
    }

    /// ddi refuses to write a column-mapped table (delta-rs 1.x could, but ddi's writers are
    /// not proven against it). The write asks first, so the cause is the typed refusal a
    /// target gets, which §3.6 classifies Blocking, pipeline: retrying cannot help until
    /// someone points the pipeline at another table. And nothing is written first.
    #[tokio::test]
    async fn dq_column_mapped_table_is_refused_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        turn_on_column_mapping(path, 1);
        let mut dq = open_dq(path).await;

        let e = dq
            .write_kafka(
                &[undecodable(0, &[3], MALFORMED)],
                &read(&[(0, (0, 9))]),
                NOW,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(cause(&e), Error::TargetProtocolUnsupported { detail, .. }
                if detail.contains("column mapping")),
            "{e:?}"
        );
        assert_eq!(e.kafka_class(), KafkaClass::BlockingPipeline);
        assert!(!std::path::Path::new(&format!("{path}/_delta_log/{:020}.json", 2)).exists());
        assert_eq!(data_files(path), 0, "refused before anything was written");
    }

    /// A commit of the data-quality table that cannot be read stops the write with a cause
    /// §3.6 classifies Blocking, pipeline: retrying cannot help until someone repairs the log,
    /// and it is no internal fault of the consumer's, which would make it leave its group.
    #[tokio::test]
    async fn dq_unreadable_commit_is_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        // Another writer's commit, after this handle's version, that is not JSON.
        std::fs::write(
            format!("{path}/_delta_log/{:020}.json", 1),
            "{\"commitInfo\": not json at all",
        )
        .unwrap();

        let e = dq
            .write_kafka(
                &[undecodable(0, &[3], MALFORMED)],
                &read(&[(0, (0, 9))]),
                NOW,
            )
            .await
            .unwrap_err();
        assert!(matches!(e, Error::DataQualityWrite { .. }), "{e:?}");
        assert_eq!(e.kafka_class(), KafkaClass::BlockingPipeline, "{e:?}");
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
        // The variant §3.6 classifies as Transient.
        assert!(
            matches!(cause(&e), Error::Delta(DeltaTableError::ObjectStore { .. })),
            "{e:?}"
        );
        assert_eq!(e.kafka_class(), KafkaClass::Transient);
    }

    /// A rival that changes the table's metadata first, every time: the retries in place run
    /// out, and the error says so by its type.
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
        assert_eq!(e.kafka_class(), KafkaClass::Transient);
        assert_eq!(struck.load(Ordering::SeqCst), 4, "one rival per attempt");
    }

    async fn create_dq_with(path: &str, properties: &[(&str, &str)]) {
        DeltaTable::try_from_url(deltalake::ensure_table_uri(path).unwrap())
            .await
            .unwrap()
            .create()
            .with_columns(columns())
            .with_configuration(properties.iter().map(|(k, v)| (*k, Some(*v))))
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
    }

    /// The data-quality commit landed, then the checkpoint delta-rs writes after it failed, so
    /// the write reported an error for a commit that is in the table. The write finds its own
    /// commit by `ddi.kafka.dqWriteId` and is done: left to the pipeline's retry of the batch,
    /// it would record every reject a second time, in a process that never crashed.
    #[tokio::test]
    async fn dq_commit_that_landed_before_its_checkpoint_failed_is_not_written_twice() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq_with(path, &[("delta.checkpointInterval", "2")]).await;
        let mut dq = open_dq(path).await;
        let struck = with_fault(&mut dq, Fault::CheckpointFails).await;

        let batch = [undecodable(0, &[3, 7], MALFORMED), uncoordinated(&[1])];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            3
        );
        assert!(
            struck.load(Ordering::SeqCst) >= 1,
            "the checkpoint after commit 1 failed"
        );
        assert!(!dir
            .path()
            .join("_delta_log")
            .join(format!("{:020}.checkpoint.parquet", 1))
            .exists());
        assert_eq!(recorded(path).await.len(), 3, "each reject once");

        // The next batch is written on the table as it is now.
        let next = [undecodable(0, &[12], MALFORMED)];
        assert_eq!(
            dq.write_kafka(&next, &read(&[(0, (10, 12))]), NOW)
                .await
                .unwrap(),
            1
        );
        assert_eq!(recorded(path).await.len(), 4);
    }

    /// The commit's PUT landed, its response was lost, and the store's retries ended in an
    /// error rather than in finding the commit there: the write reports a store fault for a
    /// commit that is in the table, and finds it.
    #[tokio::test]
    async fn dq_commit_that_landed_before_a_store_error_is_not_written_twice() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        let struck = with_fault(&mut dq, Fault::LandsThenFails).await;

        let batch = [undecodable(0, &[3, 7], MALFORMED), uncoordinated(&[1])];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (0, 9))]), NOW)
                .await
                .unwrap(),
            3
        );
        assert_eq!(struck.load(Ordering::SeqCst), 1);
        assert_eq!(recorded(path).await.len(), 3, "each reject once");
        assert_eq!(dq.kafka_counters().conflict_retries(), 0);
    }

    /// A Kafka data-quality commit cleans up the log only when it also checkpoints, as the
    /// target's commits do (docs/kafka-design.md §7.2). Every other commit would list the
    /// whole log after it lands, and where it may not delete (an immutable container) fail
    /// after landing, every time. The one that checkpoints still does, and is found to have
    /// landed.
    #[tokio::test]
    async fn dq_log_is_cleaned_up_only_by_a_commit_that_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq_with(
            path,
            &[
                ("delta.checkpointInterval", "3"),
                ("delta.logRetentionDuration", "interval 0 seconds"),
            ],
        )
        .await;
        let mut dq = open_dq(path).await;
        let mut deletes = Vec::new();
        for offset in [10, 20, 30, 40] {
            // Every file before this commit is past the retention of 0 seconds.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let struck = with_fault(&mut dq, Fault::DeleteDenied).await;
            let batch = [undecodable(0, &[offset], MALFORMED)];
            assert_eq!(
                dq.write_kafka(&batch, &read(&[(0, (offset, offset))]), NOW)
                    .await
                    .unwrap(),
                1,
                "offset {offset}"
            );
            deletes.push(struck.load(Ordering::SeqCst));
        }
        // Commit 2 checkpoints, and alone tries to delete what precedes it.
        assert_eq!(deletes[0], 0, "{deletes:?}");
        assert!(deletes[1] > 0, "{deletes:?}");
        assert_eq!(&deletes[2..], &[0, 0], "{deletes:?}");
        assert_eq!(
            recorded(path).await.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![10, 20, 30, 40],
            "each reject once"
        );
    }

    /// A DELETE on the data-quality table conflicts with a concurrent Kafka append: delta-rs
    /// counts a plain append as having read every file of its snapshot, so a winning remove
    /// with `dataChange` of any of them is a conflict (docs/kafka-design.md §5.7 step 4). The
    /// write tries again in place, on the table as it is, and records each reject once.
    #[tokio::test]
    async fn dq_a_delete_on_the_table_conflicts_with_an_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create_dq(path).await;
        let mut dq = open_dq(path).await;
        dq.write_kafka(
            &[undecodable(0, &[3], MALFORMED)],
            &read(&[(0, (3, 3))]),
            NOW,
        )
        .await
        .unwrap(); // 1
        let struck = with_fault(&mut dq, Fault::RivalDeletesFirst).await;

        let batch = [undecodable(0, &[7, 8], MALFORMED)];
        assert_eq!(
            dq.write_kafka(&batch, &read(&[(0, (7, 8))]), NOW)
                .await
                .unwrap(),
            2
        );
        assert_eq!(struck.load(Ordering::SeqCst), 1, "one DELETE won the race");
        assert_eq!(
            dq.kafka_counters().conflict_retries(),
            1,
            "it conflicted, once"
        );
        assert_eq!(
            recorded(path).await.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![7, 8],
            "the deleted row gone, the new ones once"
        );
    }
}
