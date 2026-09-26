//! DataFusion SQL transform.
//!
//! DataFusion is already in the dependency tree via delta-rs, so this costs nothing.
//! The batch is registered as the table `source`; the configured SELECT runs against it
//! and nothing else — there is no catalog to reach into, which is half of why the
//! stateless guarantee holds.
//!
//! # A row the transform cannot evaluate
//!
//! One value can fail a whole batch: a string that will not cast, a division by zero, a date
//! past what the engine represents. Before, that stopped the pipeline, and retrying the same
//! source version could only fail the same way. [`Transform::apply_isolating`] finds that row
//! instead, so it can be set aside and the rest of the batch committed.
//!
//! It costs nothing while the batch evaluates, which is nearly always. When it does not, the
//! error is classified while DataFusion's error is still typed: a failure while planning, or
//! one no value can cause — capacity, I/O, an internal error — stops the batch as before, and
//! only an error about a value starts a search. The search halves the batch until the rows
//! that fail on their own are found, about `2·log2(n)` runs per bad row, and is correct only
//! because the transform is row-local: the answer for the batch is the answers for its rows
//! put together. The SQL is checked for that once, when it is built — see
//! [`crate::transform::validate::cross_row_construct`].

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::arrow::error::ArrowError;
use deltalake::datafusion::datasource::{MemTable, TableProvider};
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::physical_plan;
use deltalake::datafusion::prelude::SessionContext;
use futures::future::BoxFuture;
use tracing::warn;

use crate::error::{Error, Result};
use crate::lookup::LookupSnapshot;
use crate::schema::Rejected;
use crate::transform::udf::register_udfs;
use crate::transform::{Isolated, Transform};

/// The name the source batch is registered under. Referenced by every transform_sql.
pub const SOURCE_TABLE: &str = "source";

/// How a failed run is reported, and today's text for it.
const EXECUTE: &str = "transform_sql failed to execute";

pub struct SqlTransform {
    sql: String,
    /// Why this SQL cannot be evaluated in parts, or `None` when it can.
    cross_row: Option<String>,
}

/// A lookup snapshot, ready to register: built once per batch and shared by every run over it.
type Lookup = (String, Arc<dyn TableProvider>);

/// Why one run of the SQL failed.
enum Failure {
    /// DataFusion could not evaluate a value. See [`attributable`].
    Evaluation(DataFusionError),
    /// Anything else: planning, registration, capacity, I/O. Never blamed on a row.
    Other(Error),
}

impl Failure {
    fn into_error(self) -> Error {
        match self {
            Failure::Evaluation(e) => Error::Evaluation(format!("{EXECUTE}: {e}")),
            Failure::Other(e) => e,
        }
    }
}

/// Whether `e` can be the fault of a value, rather than of the query or the machine.
///
/// Decided on the variant at the root of the chain, because DataFusion wraps what an
/// operator raised. The Arrow kinds listed are the ones a kernel raises about its input; an
/// `Execution` error is what DataFusion and the functions registered here raise about theirs,
/// with structural problems raised as `Plan` instead. What a machine can cause —
/// `ResourcesExhausted`, a spill file that cannot be created (also an `Execution` error, which
/// is why it is asked first), object-store and I/O errors, a panic — and what only a bug can,
/// `Internal`, are never evaluated in parts. Integer overflow is not here because DataFusion
/// does not raise it.
fn attributable(e: &DataFusionError) -> bool {
    if crate::spill::is_capacity(e) {
        return false;
    }
    match e.find_root() {
        DataFusionError::ArrowError(a, _) => matches!(
            a.as_ref(),
            ArrowError::CastError(_)
                | ArrowError::ParseError(_)
                | ArrowError::ComputeError(_)
                | ArrowError::DivideByZero
                | ArrowError::ArithmeticOverflow(_)
                | ArrowError::InvalidArgumentError(_)
                | ArrowError::JsonError(_)
        ),
        DataFusionError::Execution(_) => true,
        _ => false,
    }
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
    ///
    /// Whether the SQL is row-local is read off the normalised text only. Text kept verbatim
    /// because it did not validate may be anything, `GROUP BY` included, and evaluating that
    /// in parts would commit a partial sum per part.
    pub fn new_with_lookups(sql: impl Into<String>, lookups: &BTreeSet<String>) -> Self {
        let sql = sql.into();
        match crate::transform::validate::normalise_sql_with_lookups(&sql, lookups) {
            Ok(sql) => Self {
                cross_row: crate::transform::validate::cross_row_construct(&sql),
                sql,
            },
            Err(_) => Self {
                sql,
                cross_row: Some("SQL that did not pass validation".into()),
            },
        }
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
        Self {
            sql,
            cross_row: Some("a per-batch publication".into()),
        }
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
        let lookups = lookup_providers(lookups).await?;
        self.execute(schema, batches, &lookups)
            .await
            .map_err(Failure::into_error)
    }

    /// One run over `batches`, telling a value it cannot evaluate from any other failure.
    async fn execute(
        &self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        lookups: &[Lookup],
    ) -> std::result::Result<Vec<RecordBatch>, Failure> {
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
                .with_runtime_env(crate::budget::runtime().map_err(Failure::Other)?)
                .with_default_features()
                // Around DataFusion's own coercion: an integer literal beside a DECIMAL is
                // typed as Trino types it before coercion widens it, and after coercion,
                // so that the casts it inserts are covered too, a DECIMAL becomes a DOUBLE or
                // REAL as Trino converts it rather than by Arrow's division. See
                // `crate::transform::decimal`.
                .with_analyzer_rules(crate::transform::decimal::analyzer_rules())
                .build(),
        );
        register_udfs(&ctx);

        let provider = MemTable::try_new(schema, vec![batches]).map_err(|e| {
            Failure::Other(Error::Transform(format!(
                "could not register source batch: {e}"
            )))
        })?;
        ctx.register_table(SOURCE_TABLE, Arc::new(provider))
            .map_err(|e| {
                Failure::Other(Error::Transform(format!(
                    "could not register {SOURCE_TABLE:?}: {e}"
                )))
            })?;

        for (name, provider) in lookups {
            ctx.register_table(name.as_str(), provider.clone())
                .map_err(|e| {
                    Failure::Other(Error::Transform(format!(
                        "could not register lookup {name:?}: {e}"
                    )))
                })?;
        }

        // `DataFrame::collect`, in its two halves. Constant folding happens in the first, so
        // a literal that cannot be cast fails there — a fault of the query, which evaluating
        // the batch in parts would only reproduce, and would otherwise be blamed on a row.
        let planning =
            |e| Failure::Other(crate::spill::classify(e, "transform_sql failed to plan"));
        let df = ctx.sql(&self.sql).await.map_err(planning)?;
        let task = Arc::new(df.task_ctx());
        let plan = df.create_physical_plan().await.map_err(planning)?;
        // Through `classify` rather than straight to `Transform`, because a transform that
        // sorts or groups spills, and a full spill directory is a fact about the machine that
        // wants the opposite handling from a fact about the data. See `crate::spill`.
        let out = physical_plan::collect(plan, task).await.map_err(|e| {
            if attributable(&e) {
                Failure::Evaluation(e)
            } else {
                Failure::Other(crate::spill::classify(e, EXECUTE))
            }
        })?;
        // The JSON marker is how expressions inside this query know text from JSON. Outside
        // it the rows are text like any other, and nothing downstream should have to know.
        Ok(crate::transform::json::strip_json_marker(out))
    }
}

/// Build each lookup's table provider, once for however many runs a batch takes.
async fn lookup_providers(lookups: &[LookupSnapshot]) -> Result<Vec<Lookup>> {
    let mut out = Vec::with_capacity(lookups.len());
    for lookup in lookups {
        let provider = lookup.table.table_provider().await.map_err(|e| {
            Error::Transform(format!(
                "could not register lookup {:?} at Delta version {}: {e}",
                lookup.name, lookup.version
            ))
        })?;
        out.push((lookup.name.clone(), provider));
    }
    Ok(out)
}

/// Rows `lo..hi` of `input`, counted across its batches, without copying them.
fn slice_rows(input: &[RecordBatch], lo: usize, hi: usize) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut start = 0;
    for b in input {
        let end = start + b.num_rows();
        let (from, to) = (lo.max(start), hi.min(end));
        if from < to {
            out.push(b.slice(from - start, to - from));
        }
        if end >= hi {
            break;
        }
        start = end;
    }
    out
}

/// What bisecting a batch that failed as a whole found.
struct Bisected {
    /// The output of every part that evaluated, in input order.
    output: Vec<RecordBatch>,
    /// Each row that failed on its own, in ascending order, with its error.
    failed: Vec<(usize, DataFusionError)>,
    /// How many runs it took.
    runs: usize,
}

/// Find the rows of `0..n` that fail on their own, given that all of them together fail.
///
/// `run(lo, hi)` evaluates rows `lo..hi`. A range that fails is halved, the left half first
/// so output stays in input order. When the left half evaluates, the right one must hold the
/// failure and is split without being run, which saves a run per level — and when that
/// inference is wrong, because the failure depended on the batch rather than on a row, the
/// halves simply evaluate, so a wrong inference costs runs and never a row: a row is only
/// ever recorded after failing alone. For `k` bad rows that is at most
/// `2·k·(ceil(log2 n) + 1)` runs.
///
/// Stops — with [`Error::Evaluation`] on the row after `max_rejects`, and with the failure
/// itself on one that is not about a value — because either way nothing of this batch may be
/// committed.
async fn bisect<F, Fut>(n: usize, max_rejects: usize, first: &str, run: F) -> Result<Bisected>
where
    F: FnMut(usize, usize) -> Fut + Send,
    Fut: Future<Output = std::result::Result<Vec<RecordBatch>, Failure>> + Send,
{
    let mut b = Bisection {
        run,
        max_rejects,
        first,
        found: Bisected {
            output: Vec::new(),
            failed: Vec::new(),
            runs: 0,
        },
    };
    b.walk(0, n, true).await?;
    Ok(b.found)
}

struct Bisection<'a, F> {
    run: F,
    max_rejects: usize,
    first: &'a str,
    found: Bisected,
}

impl<'a, F, Fut> Bisection<'a, F>
where
    F: FnMut(usize, usize) -> Fut + Send,
    Fut: Future<Output = std::result::Result<Vec<RecordBatch>, Failure>> + Send + 'a,
{
    /// Evaluate rows `lo..hi`, splitting what fails, and say whether they evaluated whole.
    fn walk(&mut self, lo: usize, hi: usize, known_to_fail: bool) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            if !known_to_fail || hi - lo == 1 {
                self.found.runs += 1;
                match (self.run)(lo, hi).await {
                    Ok(out) => {
                        self.found.output.extend(out);
                        return Ok(true);
                    }
                    Err(Failure::Other(e)) => return Err(e),
                    Err(Failure::Evaluation(e)) if hi - lo == 1 => {
                        self.found.failed.push((lo, e));
                        if self.found.failed.len() > self.max_rejects {
                            return Err(Error::Evaluation(format!(
                                "{}. More than {} rows of this batch cannot be evaluated; that \
                                 many is more likely a model or upstream-schema problem than \
                                 bad rows. Nothing was committed. Raise \
                                 max_evaluation_rejects_per_batch to set them aside anyway.",
                                self.first, self.max_rejects
                            )));
                        }
                        return Ok(false);
                    }
                    Err(Failure::Evaluation(_)) => {}
                }
            }
            let mid = lo + (hi - lo) / 2;
            let left = self.walk(lo, mid, false).await?;
            self.walk(mid, hi, left).await?;
            Ok(false)
        })
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

    fn cross_row(&self) -> Option<String> {
        self.cross_row.clone()
    }

    async fn apply_isolating(
        &self,
        input: Vec<RecordBatch>,
        lookups: &[LookupSnapshot],
        max_rejects: usize,
    ) -> Result<Isolated> {
        let Some(schema) = input.first().map(RecordBatch::schema) else {
            return Ok(Isolated {
                output: vec![],
                unevaluable: None,
                reevaluations: 0,
            });
        };
        // Every run below scans the same pinned snapshots, inside the same budgeted session a
        // single run gets; only the providers are shared.
        let lookups = lookup_providers(lookups).await?;

        let first = match self.execute(schema.clone(), input.clone(), &lookups).await {
            Ok(output) => {
                return Ok(Isolated {
                    output,
                    unevaluable: None,
                    reevaluations: 0,
                })
            }
            Err(Failure::Other(e)) => return Err(e),
            Err(Failure::Evaluation(e)) => format!("{EXECUTE}: {e}"),
        };
        if max_rejects == 0 {
            return Err(Error::Evaluation(first));
        }
        if let Some(why) = &self.cross_row {
            return Err(Error::Evaluation(format!(
                "{first}. A row it cannot evaluate is set aside only when every output row \
                 comes from one input row, and this transform is not row-local ({why}), so it \
                 cannot be evaluated in parts."
            )));
        }

        // A failure no row causes — `1/0` in the projection, a granularity date_trunc does not
        // know — fails on no rows at all, and bisecting it would blame every row in turn. Only
        // an operator that runs on an empty batch can show it: behind a WHERE, the filter
        // passes nothing on and nothing is evaluated, so such a failure is attributed to every
        // row that reaches it, and the cap below is what stops that.
        let empty = vec![RecordBatch::new_empty(schema.clone())];
        match self.execute(schema.clone(), empty, &lookups).await {
            Ok(_) => {}
            Err(Failure::Evaluation(_)) => {
                return Err(Error::Transform(format!(
                    "{first}. It fails on an empty batch too, so no row is to blame and \
                     nothing was set aside."
                )))
            }
            Err(Failure::Other(e)) => return Err(e),
        }

        let n = input.iter().map(RecordBatch::num_rows).sum();
        let run = |lo, hi| self.execute(schema.clone(), slice_rows(&input, lo, hi), &lookups);
        let found = bisect(n, max_rejects, &first, run).await?;
        // The empty batch, and every part.
        let reevaluations = 1 + found.runs;

        if found.failed.is_empty() {
            // Every row evaluated in some part that passed, so under row-locality their
            // outputs together are the batch's answer.
            warn!(
                reevaluations,
                "the batch failed as a whole ({first}) but every part of it evaluated; the \
                 failure depended on the batch, not on a row, so the output of its parts is \
                 used instead"
            );
            return Ok(Isolated {
                output: found.output,
                unevaluable: None,
                reevaluations,
            });
        }

        let rows: Vec<RecordBatch> = found
            .failed
            .iter()
            .flat_map(|(i, _)| slice_rows(&input, *i, i + 1))
            .collect();
        let rows = deltalake::arrow::compute::concat_batches(&schema, &rows).map_err(|e| {
            Error::Transform(format!(
                "could not set aside the rows the transform cannot evaluate: {e}"
            ))
        })?;
        Ok(Isolated {
            output: found.output,
            unevaluable: Some(Rejected {
                rows,
                reasons: found
                    .failed
                    .iter()
                    .map(|(_, e)| format!("transform_sql could not evaluate this row: {e}"))
                    .collect(),
                columns: vec![None; found.failed.len()],
            }),
            reevaluations,
        })
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

    /// `date_trunc(g, from_unixtime(a, 'Europe/Amsterdam'))` over `a`, as epoch microseconds.
    async fn truncated(granularity: &str, a: &[i64]) -> Result<Vec<i64>> {
        let out = SqlTransform::new(format!(
            "SELECT date_trunc('{granularity}', from_unixtime(a, 'Europe/Amsterdam')) AS t \
             FROM source"
        ))
        .apply(vec![epochs(a)])
        .await?;
        Ok(micros(&out).values().to_vec())
    }

    #[tokio::test]
    async fn date_trunc_past_2262_is_refused_rather_than_wrapped() {
        // DataFusion truncates these in nanoseconds, unchecked: the 2376 value would come
        // back as a date in the 18th century, or panic a debug build.
        for granularity in ["day", "hour", "month"] {
            let e = truncated(granularity, &[1_711_924_200, IN_2376])
                .await
                .expect_err("must not wrap")
                .to_string();
            assert!(e.contains("date_trunc('"), "names the function: {e}");
            assert!(e.contains("2262"), "and the limit: {e}");
        }
        // Seconds are truncated without the conversion, so the value is fine there.
        assert_eq!(
            truncated("second", &[IN_2376]).await.unwrap(),
            vec![IN_2376 * 1_000_000]
        );
        // A Delta timestamp column holding the same instant took the same wrong path.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![IN_2376 * 1_000_000]).with_timezone("UTC"),
            ) as ArrayRef],
        )
        .unwrap();
        let e = SqlTransform::new("SELECT date_trunc('day', t) AS d FROM source")
            .apply(vec![batch])
            .await
            .expect_err("must not wrap")
            .to_string();
        assert!(e.contains("2262"), "{e}");
    }

    #[tokio::test]
    async fn date_trunc_in_range_is_unchanged_by_the_guard() {
        let a = [1_711_924_200];
        // 2024-04-01 00:30 in Amsterdam: both truncate to local midnight, 22:00 UTC.
        assert_eq!(
            truncated("day", &a).await.unwrap(),
            vec![1_711_922_400_000_000]
        );
        assert_eq!(
            truncated("hour", &a).await.unwrap(),
            vec![1_711_922_400_000_000]
        );

        // Everything else is DataFusion's own function: the same answer as its built-in,
        // run in a session that does not have the override.
        for granularity in ["week", "month", "quarter", "year", "minute"] {
            for name in ["date_trunc", "datetrunc"] {
                let sql = crate::transform::validate::normalise_sql(&format!(
                    "SELECT {name}('{granularity}', from_unixtime(a, 'Europe/Amsterdam')) AS t \
                     FROM source"
                ))
                .unwrap();
                let checked = SqlTransform::new(sql.as_str())
                    .apply(vec![epochs(&a)])
                    .await
                    .unwrap();
                let builtin = SessionContext::new();
                let batch = epochs(&a);
                builtin
                    .register_table(
                        SOURCE_TABLE,
                        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
                    )
                    .unwrap();
                let builtin = builtin.sql(&sql).await.unwrap().collect().await.unwrap();
                assert_eq!(
                    micros(&checked).values(),
                    micros(&builtin).values(),
                    "{name}('{granularity}')"
                );
            }
        }
    }

    // ------------------------------------------------ a row the transform cannot evaluate

    /// Rows `ids`, each with its id spelled out as `name` — except those in `bad`, whose name
    /// is "n/a" and will not cast to a number.
    fn named(ids: std::ops::Range<i64>, bad: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let names: Vec<String> = ids
            .clone()
            .map(|i| match bad.contains(&i) {
                true => "n/a".to_string(),
                false => i.to_string(),
            })
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.collect::<Vec<_>>())) as ArrayRef,
                Arc::new(StringArray::from(names)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    /// Every `id` in `batches`, in order.
    fn ids(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(b.schema().index_of("id").unwrap())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    const CAST_NAME: &str = "SELECT id, CAST(name AS BIGINT) AS n FROM source";

    async fn isolate(sql: &str, input: Vec<RecordBatch>, max: usize) -> Result<Isolated> {
        SqlTransform::new(sql)
            .apply_isolating(input, &[], max)
            .await
    }

    #[tokio::test]
    async fn a_row_the_transform_cannot_evaluate_is_isolated_and_the_rest_keep_their_order() {
        let bad = [0, 517, 999];
        // Across two input batches, so the parts do too.
        let input = vec![named(0..600, &bad), named(600..1000, &bad)];
        let got = isolate(CAST_NAME, input, 10).await.unwrap();

        let want: Vec<i64> = (0..1000).filter(|i| !bad.contains(i)).collect();
        assert_eq!(ids(&got.output), want, "every other row, in input order");

        let rejected = got.unevaluable.expect("three rows could not be evaluated");
        assert_eq!(ids(std::slice::from_ref(&rejected.rows)), bad.to_vec());
        assert_eq!(
            rejected.rows.schema(),
            named(0..1, &[]).schema(),
            "the source row, since there is no output row to show"
        );
        for reason in &rejected.reasons {
            assert!(reason.contains("could not evaluate"), "{reason}");
            assert!(reason.contains("Cannot cast string 'n/a'"), "{reason}");
        }
        assert_eq!(rejected.columns, vec![None; 3], "no one column is to blame");
        // Two runs for the whole batch and the empty one, and 2·(ceil(log2 n) + 1) at most
        // for each bad row.
        assert!(got.reevaluations < 2 + 2 * 3 * 11, "{}", got.reevaluations);
    }

    #[tokio::test]
    async fn a_2376_row_under_date_trunc_is_isolated() {
        // The issue's row. from_unixtime reads it now; truncating it still needs a
        // nanosecond value DataFusion cannot hold, so that one row is what is set aside.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("a", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    1_711_924_200,
                    IN_2376,
                    1_711_924_201,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let got = isolate(
            "SELECT id, date_trunc('day', from_unixtime(a, 'Europe/Amsterdam')) AS d \
             FROM source",
            vec![batch],
            10,
        )
        .await
        .unwrap();
        assert_eq!(ids(&got.output), vec![1, 3]);
        let rejected = got.unevaluable.unwrap();
        assert_eq!(ids(&[rejected.rows]), vec![2]);
        assert!(
            rejected.reasons[0].contains("date_trunc"),
            "{:?}",
            rejected.reasons
        );
    }

    #[tokio::test]
    async fn a_clean_batch_costs_one_run() {
        let got = isolate(CAST_NAME, vec![named(0..100, &[])], 10)
            .await
            .unwrap();
        assert_eq!(ids(&got.output).len(), 100);
        assert!(got.unevaluable.is_none());
        assert_eq!(got.reevaluations, 0);
    }

    /// A batch of `id` 1..=10 with a microsecond timestamp `ts`.
    fn timestamped() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((1..=10).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_711_924_200_000_000; 10])
                        .with_timezone("UTC"),
                ) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_failure_that_needs_no_row_is_not_blamed_on_one() {
        // Neither of these is about a value: they fail on an empty batch too, and bisecting
        // them would set every row aside in turn.
        for sql in [
            "SELECT id, 1/0 AS boom FROM source",
            "SELECT id, date_trunc('fortnight', ts) AS d FROM source",
        ] {
            let e = isolate(sql, vec![timestamped()], 10)
                .await
                .expect_err("fails whatever the rows");
            assert!(matches!(e, Error::Transform(_)), "{sql}: {e:?}");
            assert!(e.to_string().contains("empty batch"), "{sql}: {e}");
        }
    }

    #[tokio::test]
    async fn behind_a_filter_a_failure_that_needs_no_row_reaches_the_cap() {
        // The documented limit: a filter passes nothing on from an empty batch, so the probe
        // cannot see a failure behind one, and every row that reaches it takes the blame.
        let sql = "SELECT date_trunc('fortnight', ts) AS d FROM source WHERE id > 0";
        let e = isolate(sql, vec![timestamped()], 5)
            .await
            .expect_err("more rows than the cap");
        assert!(matches!(e, Error::Evaluation(_)), "{e:?}");
        assert!(
            e.to_string().contains("max_evaluation_rejects_per_batch"),
            "{e}"
        );

        let got = isolate(sql, vec![timestamped()], 100).await.unwrap();
        assert!(got.output.iter().all(|b| b.num_rows() == 0));
        assert_eq!(got.unevaluable.unwrap().len(), 10, "the whole batch");
    }

    #[tokio::test]
    async fn a_constant_that_cannot_be_folded_fails_planning_not_rows() {
        let e = isolate(
            "SELECT id, CAST('x' AS BIGINT) AS n FROM source",
            vec![named(0..10, &[])],
            10,
        )
        .await
        .expect_err("the optimiser cannot fold it");
        assert!(!matches!(e, Error::Evaluation(_)), "{e:?}");
        assert!(e.to_string().contains("failed to plan"), "{e}");
    }

    #[tokio::test]
    async fn too_many_unevaluable_rows_fail_the_batch() {
        let all: Vec<i64> = (0..20).collect();
        let e = isolate(CAST_NAME, vec![named(0..20, &all)], 5)
            .await
            .expect_err("twenty bad rows against a cap of five");
        assert!(matches!(e, Error::Evaluation(_)), "{e:?}");
        let m = e.to_string();
        assert!(m.contains("More than 5 rows"), "{m}");
        assert!(m.contains("max_evaluation_rejects_per_batch"), "{m}");
    }

    #[tokio::test]
    async fn a_cross_row_transform_is_not_bisected() {
        // Its answer for a batch is not the answers for its parts put together.
        let e = isolate(
            &format!("{CAST_NAME} LIMIT 1000"),
            vec![named(0..10, &[3])],
            10,
        )
        .await
        .expect_err("not evaluated in parts");
        assert!(matches!(e, Error::Evaluation(_)), "{e:?}");
        assert!(e.to_string().contains("LIMIT"), "{e}");
    }

    #[test]
    fn a_transform_that_did_not_pass_validation_is_never_bisected() {
        // Kept verbatim because it did not validate, so nothing is known about its rows.
        let t = SqlTransform::new("SELECT k, count(*) FROM source GROUP BY k");
        assert!(t.cross_row().is_some());
        assert!(
            SqlTransform::new_per_batch("SELECT count(*) AS n FROM source")
                .cross_row()
                .is_some()
        );
        assert_eq!(SqlTransform::new(CAST_NAME).cross_row(), None);
    }

    #[tokio::test]
    async fn with_isolation_off_the_error_is_todays() {
        let e = isolate(CAST_NAME, vec![named(0..10, &[3])], 0)
            .await
            .expect_err("nothing may be set aside");
        assert!(matches!(e, Error::Evaluation(_)), "{e:?}");
        assert!(
            e.to_string().starts_with(
                "transform error: transform_sql failed to execute: Arrow error: Cast error"
            ),
            "{e}"
        );
    }

    /// Stands in for the transform: rows `lo..hi` fail if any of them is in `bad`, and
    /// otherwise come back as one batch of their own indices.
    fn fake<'a>(
        bad: &'static [usize],
        calls: &'a std::sync::atomic::AtomicUsize,
    ) -> impl FnMut(usize, usize) -> std::future::Ready<std::result::Result<Vec<RecordBatch>, Failure>>
           + Send
           + 'a {
        move |lo, hi| {
            calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::future::ready(if (lo..hi).any(|i| bad.contains(&i)) {
                Err(Failure::Evaluation(DataFusionError::Execution(format!(
                    "bad row in {lo}..{hi}"
                ))))
            } else {
                Ok(vec![indices(lo, hi)])
            })
        }
    }

    fn indices(lo: usize, hi: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from((lo as i64..hi as i64).collect::<Vec<_>>())) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn bisection_with_a_fake_evaluator_finds_exactly_the_failing_rows() {
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let found = bisect(1000, 10, "first", fake(&[0, 517, 999], &calls))
            .await
            .unwrap();
        let failed: Vec<usize> = found.failed.iter().map(|(i, _)| *i).collect();
        assert_eq!(failed, vec![0, 517, 999]);
        let want: Vec<i64> = (0..1000).filter(|i| ![0, 517, 999].contains(i)).collect();
        assert_eq!(ids(&found.output), want, "every other row once, in order");
        assert_eq!(found.runs, calls.into_inner());
        assert!(found.runs <= 2 * 3 * 11, "{}", found.runs);
    }

    #[tokio::test]
    async fn a_batch_that_fails_only_as_a_whole_is_committed_in_parts() {
        // Nothing fails once split, so no row is to blame: every part is evaluated exactly
        // once, and the wrong inference that the right half must fail costs a run per level.
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let found = bisect(1000, 10, "first", fake(&[], &calls)).await.unwrap();
        assert!(found.failed.is_empty());
        assert_eq!(ids(&found.output), (0..1000).collect::<Vec<i64>>());
        assert!(found.runs <= 10 + 1, "ceil(log2 1000) + 1: {}", found.runs);
    }

    #[tokio::test]
    async fn a_machine_failure_mid_bisection_aborts_the_step() {
        let mut calls = 0;
        let run = |lo, hi| {
            calls += 1;
            std::future::ready(match calls {
                3 => Err(Failure::Other(Error::Capacity(
                    "the spill directory is full".into(),
                ))),
                _ if lo == 0 => Err(Failure::Evaluation(DataFusionError::Execution(
                    "bad".into(),
                ))),
                _ => Ok(vec![indices(lo, hi)]),
            })
        };
        let e = bisect(100, 10, "first", run)
            .await
            .err()
            .expect("nothing of this batch may be committed");
        assert!(matches!(e, Error::Capacity(_)), "{e:?}");
    }

    #[tokio::test]
    async fn only_value_driven_failures_are_attributed_to_rows() {
        let arrow = |e: ArrowError| DataFusionError::ArrowError(Box::new(e), None);
        for e in [
            arrow(ArrowError::CastError("Cannot cast string 'n/a'".into())),
            arrow(ArrowError::DivideByZero),
            arrow(ArrowError::ArithmeticOverflow(
                "12828758400 * 1000000000".into(),
            )),
            arrow(ArrowError::InvalidArgumentError("bad value".into())),
            DataFusionError::Execution("date_trunc('day') cannot truncate".into()),
            // Read at the root, however it was wrapped on the way out.
            DataFusionError::Context(
                "projection".into(),
                Box::new(arrow(ArrowError::CastError("x".into()))),
            ),
        ] {
            assert!(attributable(&e), "{e}");
        }

        let panicked = tokio::spawn(async { panic!("a panic in an operator") })
            .await
            .unwrap_err();
        for e in [
            DataFusionError::ResourcesExhausted("Failed to allocate".into()),
            DataFusionError::IoError(std::io::Error::other("disk")),
            DataFusionError::ObjectStore(Box::new(deltalake::ObjectStoreError::Generic {
                store: "abfss",
                source: "timed out".into(),
            })),
            DataFusionError::Plan("no such column".into()),
            DataFusionError::Internal("invariant".into()),
            arrow(ArrowError::IoError(
                "read".into(),
                std::io::Error::other("disk"),
            )),
            DataFusionError::ExecutionJoin(Box::new(panicked)),
            DataFusionError::Execution(
                "Failed to create partition file at \"/spill\": Too many open files".into(),
            ),
        ] {
            assert!(!attributable(&e), "{e}");
        }
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
