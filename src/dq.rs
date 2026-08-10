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

use deltalake::arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{DataType as DeltaType, PrimitiveType, StructField, Transaction};
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use std::sync::Arc;
use tracing::debug;

use crate::error::{Error, Result};
use crate::schema::Rejected;

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
///   column_name     VARCHAR,
///   reason          VARCHAR,
///   payload         VARCHAR,
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
            })),
            Err(_) => Ok(None),
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
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

        match table
            .write(vec![batch])
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

        RecordBatch::try_new(
            arrow_schema(),
            vec![
                Arc::new(StringArray::from(vec![self.app_id.as_str(); n])) as ArrayRef,
                Arc::new(StringArray::from(vec![self.pipeline.as_str(); n])) as ArrayRef,
                Arc::new(deltalake::arrow::array::Int64Array::from(vec![
                    source_version;
                    n
                ])) as ArrayRef,
                Arc::new(StringArray::from(columns)) as ArrayRef,
                Arc::new(StringArray::from(reasons)) as ArrayRef,
                Arc::new(StringArray::from(payload)) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![now_micros; n])) as ArrayRef,
            ],
        )
        .map_err(|e| Error::Other(format!("cannot assemble the data-quality batch: {e}")))
    }
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
    use deltalake::arrow::array::Int64Array;

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
}
