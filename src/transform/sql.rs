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
use deltalake::datafusion::prelude::SessionContext;

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

    /// Build the transform behind a `ddi_publish` model.
    ///
    /// Differs from [`SqlTransform::new`] only in which validator normalised the text: a
    /// publication may aggregate, because its rows describe one committed batch and are sent
    /// rather than stored. See [`crate::transform::validate::Grain`]. Execution is identical —
    /// the batch is registered as `source` and nothing else exists to read, which is what
    /// makes running this over the *committed* rows the same operation as running the
    /// model over the target table.
    pub fn new_per_batch(sql: impl Into<String>) -> Self {
        let sql = sql.into();
        let sql = crate::transform::validate::normalise_publish_sql(&sql).unwrap_or(sql);
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
        let config = crate::budget::session_config()
            // Grain-preserving transforms need no repartitioning, and keeping it off makes
            // output row order track input order, which makes tests deterministic.
            .with_target_partitions(1);
        // Built against the process's memory budget, so a transform that sorts or groups
        // spills rather than growing. The batch it is handed is already bounded by
        // `max_bytes_per_batch`; this bounds what the transform makes of it. Through
        // `session_config` rather than a bare one so it agrees with every other session in
        // the process about how spill is written — this is the highest-frequency session
        // here, and it was the only one not covered.
        let ctx = SessionContext::new_with_state(
            deltalake::datafusion::execution::session_state::SessionStateBuilder::new()
                .with_config(config)
                .with_runtime_env(crate::budget::runtime()?)
                .with_default_features()
                // After DataFusion's own coercion, so the casts it inserts are covered too:
                // a DECIMAL becomes the nearest DOUBLE or REAL, as in Trino, rather than
                // Arrow's double-rounded division. See `crate::transform::decimal`.
                .with_analyzer_rule(Arc::new(
                    crate::transform::decimal::CorrectlyRoundedDecimalCasts,
                ))
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
        // Through `classify` rather than straight to `Transform`, because a transform that
        // sorts or groups spills, and a full spill directory is a fact about the machine that
        // wants the opposite handling from a fact about the data. See `crate::spill`.
        let out = df
            .collect()
            .await
            .map_err(|e| crate::spill::classify(e, "transform_sql failed to execute"))?;
        // The JSON marker is how expressions inside this query know text from JSON. Outside
        // it the rows are text like any other, and nothing downstream should have to know.
        Ok(crate::transform::json::strip_json_marker(out))
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
    use deltalake::arrow::array::{
        Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampMicrosecondArray,
    };
    use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

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

    /// A batch of epoch seconds in a BIGINT column `a`, one row per value.
    fn epochs(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    async fn run_over(sql: &str, batch: RecordBatch) -> Vec<RecordBatch> {
        SqlTransform::new(sql)
            .apply(vec![batch])
            .await
            .unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    async fn dates(sql: &str, batch: RecordBatch) -> Vec<i32> {
        let out = run_over(sql, batch).await;
        let d = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("CAST(... AS DATE) returns Arrow Date32");
        d.values().to_vec()
    }

    fn micros(out: &[RecordBatch]) -> &TimestampMicrosecondArray {
        out[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("from_unixtime is a microsecond timestamp")
    }

    /// 2376-07-12 00:00:00 UTC: past the last instant an i64 of nanoseconds can hold.
    const IN_2376: i64 = 12_828_758_400;

    #[tokio::test]
    async fn from_unixtime_past_2262_gives_trinos_24th_century_date() {
        // The value in the issue. Trino returns a date for it; this failed the batch with
        // "Overflow happened on: 12828758400 * 1000000000".
        for sql in [
            "SELECT CAST(from_unixtime(a, 'Europe/Amsterdam') AS DATE) AS d FROM source",
            "SELECT CAST(from_unixtime(a) AS DATE) AS d FROM source",
        ] {
            assert_eq!(dates(sql, epochs(&[IN_2376])).await, vec![148_481], "{sql}");
        }
        assert_eq!(
            dates(
                "SELECT CAST(from_unixtime(a, -5, -30) AS DATE) AS d FROM source",
                epochs(&[IN_2376])
            )
            .await,
            vec![148_480],
            "midnight UTC is still the day before at -05:30"
        );
    }

    #[tokio::test]
    async fn from_unixtime_is_a_microsecond_timestamp_with_its_zone() {
        let out = run_over(
            "SELECT from_unixtime(a, 'Europe/Amsterdam') AS t FROM source",
            epochs(&[1_711_924_200]),
        )
        .await;
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Amsterdam".into()))
        );
        assert_eq!(micros(&out).value(0), 1_711_924_200_000_000);
    }

    #[tokio::test]
    async fn from_unixtime_rounds_fractional_seconds_as_trino_does() {
        // Java's Math.round: half toward positive infinity, at the millisecond.
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![1.5, -0.0005, 0.0015])) as ArrayRef],
        )
        .unwrap();
        let out = run_over("SELECT from_unixtime(f, 'UTC') AS t FROM source", batch).await;
        assert_eq!(
            out[0].schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(micros(&out).values().to_vec(), vec![1_500_000, 0, 2_000]);
    }

    #[tokio::test]
    async fn from_unixtime_compares_with_a_delta_timestamp_at_microsecond_precision() {
        // DataFusion compares mixed units at the coarser one. A millisecond value would
        // drop the column's last 500 µs and call the two equal; Trino does not.
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("a", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_711_924_200_000_500])
                        .with_timezone("UTC"),
                ) as ArrayRef,
                Arc::new(Int64Array::from(vec![1_711_924_200])) as ArrayRef,
            ],
        )
        .unwrap();
        let out = run_over(
            "SELECT t > from_unixtime(a, 'UTC') AS gt, t = from_unixtime(a, 'UTC') AS eq \
             FROM source",
            batch,
        )
        .await;
        let flag = |i: usize| {
            out[0]
                .column(i)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        };
        assert!(flag(0), "t is later by 500 µs");
        assert!(!flag(1), "so it is not equal");
    }

    #[tokio::test]
    async fn a_fixed_offset_moves_the_calendar_date() {
        // 2024-03-31 22:30 UTC: April 1 in Amsterdam, still March 31 at -05:30.
        assert_eq!(
            dates(
                "SELECT CAST(from_unixtime(a, -5, -30) AS DATE) AS d FROM source",
                epochs(&[1_711_924_200])
            )
            .await,
            vec![19_813]
        );
        assert_eq!(
            dates(
                "SELECT CAST(from_unixtime(a, 'Europe/Amsterdam') AS DATE) AS d FROM source",
                epochs(&[1_711_924_200])
            )
            .await,
            vec![19_814]
        );
    }

    #[tokio::test]
    async fn a_from_unixtime_value_is_exactly_a_delta_timestamp() {
        // A Delta `timestamp` column is microseconds in UTC, so landing the value there only
        // relabels its zone — in the 24th century too.
        let out = run_over(
            "SELECT from_unixtime(a, 'Europe/Amsterdam') AS t FROM source",
            epochs(&[IN_2376]),
        )
        .await;
        let target = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));
        let landed = crate::schema::SchemaCoercer::new(target)
            .coerce(&out[0])
            .unwrap();
        let t = landed
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(t.value(0), IN_2376 * 1_000_000);
        assert!(!t.is_null(0));
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
