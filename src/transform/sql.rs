//! DataFusion SQL transform.
//!
//! DataFusion is already in the dependency tree via delta-rs, so this costs nothing.
//! The batch is registered as the table `source`; the configured SELECT runs against it
//! and nothing else — there is no catalog to reach into, which is half of why the
//! stateless guarantee holds.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::datafusion::datasource::MemTable;
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::delta_datafusion::DataFusionMixins;

use crate::error::{Error, Result};
use crate::lookup::LookupSnapshot;
use crate::transform::udf::register_udfs;
use crate::transform::Transform;

/// The name the source batch is registered under. Referenced by every transform_sql.
pub const SOURCE_TABLE: &str = "source";

pub struct SqlTransform {
    sql: String,
}

impl SqlTransform {
    /// Build a transform, normalising any dialect spelling this engine does not run.
    ///
    /// Normalising here rather than only in `Config::resolve` is what makes the two
    /// impossible to disagree: whoever builds a transform — the resolver, a test, a library
    /// consumer — gets the same query, so one cannot accept what the other refuses. The
    /// rewrite is idempotent, so passing already-normalised SQL through it changes nothing.
    ///
    /// SQL that will not parse is kept verbatim rather than rejected: this constructor
    /// cannot fail, and [`crate::transform::validate::validate_sql`] has already refused it
    /// in every path that reaches a running pipeline. Keeping it means the real planning
    /// error surfaces instead of a rewriting one.
    pub fn new(sql: impl Into<String>) -> Self {
        Self::new_with_lookups(sql, &BTreeSet::new())
    }

    /// Build a transform whose SQL may reference the supplied, already-declared lookup names.
    ///
    /// The config resolver remains the gate that reports invalid SQL. This repeat normalisation
    /// protects library callers too, while keeping a lookup join from being mistaken for an
    /// undeclared second source on the way to execution.
    pub fn new_with_lookups(sql: impl Into<String>, lookups: &BTreeSet<String>) -> Self {
        let sql = sql.into();
        let sql =
            crate::transform::validate::normalise_sql_with_lookups(&sql, lookups).unwrap_or(sql);
        Self { sql }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Run the SQL over `batches` interpreted with `schema`.
    ///
    /// A fresh `SessionContext` per call is deliberate: it guarantees no state survives
    /// between batches, which is the property the whole design rests on. The cost is
    /// negligible next to reading the parquet.
    pub async fn run(
        &self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        self.run_with_lookups(schema, batches, &[]).await
    }

    /// Run the SQL against one source batch and the immutable lookup snapshots selected for it.
    pub async fn run_with_lookups(
        &self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        lookups: &[LookupSnapshot],
    ) -> Result<Vec<RecordBatch>> {
        let config = SessionConfig::new()
            // Grain-preserving transforms need no repartitioning, and keeping it off makes
            // output row order track input order, which makes tests deterministic.
            .with_target_partitions(1);
        // Built against the process's memory budget, so a transform that sorts or groups
        // spills rather than growing. The batch it is handed is already bounded by
        // `max_bytes_per_batch`; this bounds what the transform makes of it.
        let ctx = SessionContext::new_with_state(
            deltalake::datafusion::execution::session_state::SessionStateBuilder::new()
                .with_config(config)
                .with_runtime_env(crate::budget::runtime()?)
                .with_default_features()
                .build(),
        );
        register_udfs(&ctx);

        let provider = MemTable::try_new(schema, vec![batches])
            .map_err(|e| Error::Transform(format!("could not register source batch: {e}")))?;
        ctx.register_table(SOURCE_TABLE, Arc::new(provider))
            .map_err(|e| Error::Transform(format!("could not register {SOURCE_TABLE:?}: {e}")))?;

        for lookup in lookups {
            let provider = lookup.table.table_provider().await.map_err(|e| {
                Error::Transform(format!(
                    "could not register lookup {:?} at Delta version {}: {e}",
                    lookup.name, lookup.version
                ))
            })?;
            ctx.register_table(lookup.name.as_str(), provider)
                .map_err(|e| {
                    Error::Transform(format!("could not register lookup {:?}: {e}", lookup.name))
                })?;
        }

        let df = ctx
            .sql(&self.sql)
            .await
            .map_err(|e| Error::Transform(format!("transform_sql failed to plan: {e}")))?;
        let out = df
            .collect()
            .await
            .map_err(|e| Error::Transform(format!("transform_sql failed to execute: {e}")))?;
        Ok(out)
    }
}

#[async_trait]
impl Transform for SqlTransform {
    async fn apply(&self, input: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let Some(first) = input.first() else {
            return Ok(vec![]);
        };
        let schema = first.schema();
        self.run(schema, input).await
    }

    async fn apply_with_lookups(
        &self,
        input: Vec<RecordBatch>,
        lookups: &[LookupSnapshot],
    ) -> Result<Vec<RecordBatch>> {
        let Some(first) = input.first() else {
            return Ok(vec![]);
        };
        let schema = first.schema();
        self.run_with_lookups(schema, input, lookups).await
    }

    fn describe(&self) -> String {
        format!("sql: {}", self.sql.replace('\n', " ").trim())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{ArrayRef, Date32Array, Int32Array, Int64Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};

    fn simple_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("total", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["OPEN", "DRAFT", "OPEN"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100, 200, 300])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    async fn run(sql: &str) -> Vec<RecordBatch> {
        SqlTransform::new(sql)
            .apply(vec![simple_batch()])
            .await
            .unwrap()
    }

    fn total_rows(b: &[RecordBatch]) -> usize {
        b.iter().map(|x| x.num_rows()).sum()
    }

    #[tokio::test]
    async fn projection_and_rename() {
        let out = run("SELECT order_id AS id, total FROM source").await;
        assert_eq!(total_rows(&out), 3);
        assert_eq!(out[0].schema().field(0).name(), "id");
        assert_eq!(out[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn filter_drops_rows() {
        let out = run("SELECT order_id FROM source WHERE status <> 'DRAFT'").await;
        assert_eq!(total_rows(&out), 2);
    }

    #[tokio::test]
    async fn cast_changes_type() {
        let out = run("SELECT CAST(total AS DECIMAL(18,4)) AS total FROM source").await;
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &DataType::Decimal128(18, 4)
        );
    }

    #[tokio::test]
    async fn trino_from_unixtime_uses_amsterdam_calendar_date_across_dst() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "epoch_seconds",
            DataType::Int64,
            false,
        )]));
        // 2024-03-31 22:30:00 UTC is 2024-04-01 00:30:00 in Amsterdam (CEST),
        // so a date conversion must produce April 1 rather than March 31.
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_711_924_200])) as ArrayRef],
        )
        .unwrap();

        let out = SqlTransform::new(
            "SELECT CAST(from_unixtime(epoch_seconds, 'Europe/Amsterdam') AS DATE) \
             AS local_date FROM source",
        )
        .apply(vec![batch])
        .await
        .unwrap();
        let dates = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("CAST(... AS DATE) returns Arrow Date32");
        assert_eq!(dates.value(0), 19_814, "2024-04-01 in Date32 days");
    }

    #[tokio::test]
    async fn empty_input_yields_empty_output() {
        let out = SqlTransform::new("SELECT 1 AS x FROM source")
            .apply(vec![])
            .await
            .unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn transform_is_stateless_across_calls() {
        // The same transform applied twice must produce identical output — no memory.
        let t = SqlTransform::new("SELECT order_id FROM source");
        let a = t.apply(vec![simple_batch()]).await.unwrap();
        let b = t.apply(vec![simple_batch()]).await.unwrap();
        assert_eq!(total_rows(&a), total_rows(&b));
    }

    #[tokio::test]
    async fn planning_error_is_reported_as_a_transform_error() {
        let err = SqlTransform::new("SELECT no_such_column FROM source")
            .apply(vec![simple_batch()])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("transform"), "got: {err}");
    }
}
