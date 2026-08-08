//! `orders_raw -> orders_stg` under everything that happens to a shared table.
//!
//! Bronze carries a JSON payload and a `_timestamp` that increases with arrival. Silver
//! is the parsed, cast, typed version. A batch job rebuilds silver from scratch on its own
//! schedule while `ddi` streams into it continuously, and neither is aware of the other.
//!
//! Every test asserts the same two things, because they are the only ones that matter:
//! **no key missing, no key twice.**

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use common::pipeline_cfg;
use delta_delta_ingest::config::ResolvedPipeline;
use delta_delta_ingest::dedup::DEFAULT_TIMESTAMP_COLUMN;
use delta_delta_ingest::pipeline::Pipeline;
use delta_delta_ingest::source::ChangePolicy;
use deltalake::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};
use futures::TryStreamExt;

/// The model, as a dbt project would compile it. Runs unchanged in a warehouse that has
/// `json_extract_scalar` — Trino, Starburst — and here.
const STG_SQL: &str = "\
SELECT order_id,
       CAST(json_extract_scalar(data, '$.customer_id') AS BIGINT) AS customer_id,
       CAST(json_extract_scalar(data, '$.amount') AS BIGINT)      AS amount,
       json_extract_scalar(data, '$.status')                      AS status,
       _timestamp
FROM source";

// ---------------------------------------------------------------- shapes

fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
        Field::new(
            DEFAULT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn stg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
        Field::new("status", DataType::Utf8, true),
        Field::new(
            DEFAULT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// One raw order. `ts` is microseconds; it increases with arrival.
#[derive(Clone, Copy)]
struct Order {
    id: i64,
    ts: i64,
}

fn orders(range: std::ops::RangeInclusive<i64>) -> Vec<Order> {
    range
        .map(|i| Order {
            id: i,
            ts: i * 1_000,
        })
        .collect()
}

fn raw_batch(os: &[Order]) -> RecordBatch {
    RecordBatch::try_new(
        raw_schema(),
        vec![
            Arc::new(Int64Array::from(
                os.iter().map(|o| o.id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                os.iter()
                    .map(|o| {
                        format!(
                            r#"{{"customer_id":{},"amount":{},"status":"paid"}}"#,
                            o.id * 7,
                            o.id * 100
                        )
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                os.iter().map(|o| o.ts).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

/// What the batch job writes: the same transformation, done its own way.
fn stg_batch(os: &[Order]) -> RecordBatch {
    RecordBatch::try_new(
        stg_schema(),
        vec![
            Arc::new(Int64Array::from(
                os.iter().map(|o| o.id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                os.iter().map(|o| o.id * 7).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                os.iter().map(|o| o.id * 100).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(vec!["paid"; os.len()])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                os.iter().map(|o| o.ts).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------- lakehouse

struct Lake {
    _dir: tempfile::TempDir,
    raw: String,
    stg: String,
}

async fn create(path: &str, schema: SchemaRef) {
    let delta: StructType = schema.as_ref().try_into_kernel().unwrap();
    let url = ensure_table_uri(path).unwrap();
    DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();
}

impl Lake {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let raw = root.join("orders_raw").to_str().unwrap().to_string();
        let stg = root.join("orders_stg").to_str().unwrap().to_string();
        create(&raw, raw_schema()).await;
        create(&stg, stg_schema()).await;
        Self {
            _dir: dir,
            raw,
            stg,
        }
    }

    /// New orders arrive in bronze — one Delta commit.
    async fn arrive(&self, os: &[Order]) {
        let url = ensure_table_uri(&self.raw).unwrap();
        open_table(url)
            .await
            .unwrap()
            .write(vec![raw_batch(os)])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    /// The batch job: full refresh of silver from the orders it saw.
    async fn rebuild(&self, saw: &[Order]) {
        let url = ensure_table_uri(&self.stg).unwrap();
        open_table(url)
            .await
            .unwrap()
            .write(vec![stg_batch(saw)])
            .with_save_mode(SaveMode::Overwrite)
            .await
            .unwrap();
    }

    fn cfg(&self) -> ResolvedPipeline {
        let mut c = pipeline_cfg("orders_stg", &self.raw, &self.stg);
        c.transform_sql = Some(STG_SQL.to_string());
        c.dedup_timestamp = Some(DEFAULT_TIMESTAMP_COLUMN.to_string());
        c.dedup_key = Some("order_id".to_string());
        // Deletes and updates upstream are not ours to propagate; skip those commits.
        c.change_policy = ChangePolicy::SkipChangeCommits;
        c
    }

    async fn stream(&self) -> usize {
        Pipeline::open(self.cfg())
            .await
            .expect("pipeline should open")
            .run_until_caught_up()
            .await
            .expect("streaming should succeed")
    }

    async fn silver(&self) -> Vec<(i64, i64, i64, String)> {
        let url = ensure_table_uri(&self.stg).unwrap();
        let (_t, stream) = open_table(url).await.unwrap().scan_table().await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out = Vec::new();
        for b in batches {
            if b.num_rows() == 0 {
                continue;
            }
            let id = col_i64(&b, "order_id");
            let cust = col_i64(&b, "customer_id");
            let amt = col_i64(&b, "amount");
            let st = deltalake::arrow::compute::cast(
                b.column(b.schema().index_of("status").unwrap()),
                &DataType::Utf8,
            )
            .unwrap();
            let st = st.as_any().downcast_ref::<StringArray>().unwrap().clone();
            for i in 0..b.num_rows() {
                out.push((id[i], cust[i], amt[i], st.value(i).to_string()));
            }
        }
        out.sort();
        out
    }

    /// The whole point, asserted the same way every time.
    async fn assert_exactly(&self, expected: &[Order]) {
        let rows = self.silver().await;
        let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let unique: HashSet<i64> = ids.iter().copied().collect();

        assert_eq!(
            ids.len(),
            unique.len(),
            "duplicated keys in silver: {ids:?}"
        );
        let want: Vec<i64> = {
            let mut v: Vec<i64> = expected.iter().map(|o| o.id).collect();
            v.sort();
            v
        };
        assert_eq!(ids, want, "silver holds the wrong set of keys");

        // And the transformation actually ran: JSON parsed and cast, not passed through.
        for (id, cust, amt, status) in &rows {
            assert_eq!(*cust, id * 7, "customer_id for {id}");
            assert_eq!(*amt, id * 100, "amount for {id}");
            assert_eq!(status, "paid");
        }
    }
}

fn col_i64(b: &RecordBatch, name: &str) -> Vec<i64> {
    let a = b.column(b.schema().index_of(name).unwrap());
    let a = a.as_any().downcast_ref::<Int64Array>().expect("int64");
    (0..a.len()).map(|i| a.value(i)).collect()
}

// ================================================================ scenarios

#[tokio::test]
async fn orders_arrive_and_are_parsed_cast_and_streamed() {
    let lake = Lake::new().await;
    lake.arrive(&orders(1..=5)).await;
    lake.arrive(&orders(6..=9)).await;

    lake.stream().await;
    lake.assert_exactly(&orders(1..=9)).await;
}

#[tokio::test]
async fn a_full_refresh_from_the_batch_side_neither_skips_nor_doubles() {
    let lake = Lake::new().await;
    lake.arrive(&orders(1..=5)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=5)).await;

    // The batch job rebuilds silver from everything it can see.
    lake.rebuild(&orders(1..=5)).await;
    lake.assert_exactly(&orders(1..=5)).await;

    // ddi carries on across the rebuild.
    lake.stream().await;
    lake.assert_exactly(&orders(1..=5)).await;

    lake.arrive(&orders(6..=8)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=8)).await;
}

#[tokio::test]
async fn orders_arriving_while_the_batch_job_runs_are_still_streamed() {
    // The window that makes this hard. The batch reads bronze at one instant and commits
    // its output later; rows landing in between are in neither its snapshot nor, after
    // the overwrite, the target. Only the timestamp can tell them apart afterwards.
    let lake = Lake::new().await;
    lake.arrive(&orders(1..=5)).await;
    lake.stream().await;

    // 1. the batch reads bronze — it sees 1..=5
    let batch_snapshot = orders(1..=5);

    // 2. while it works, more orders arrive, and ddi streams them
    lake.arrive(&orders(6..=7)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=7)).await;

    // 3. the batch commits what it read back in step 1, wiping 6 and 7
    lake.rebuild(&batch_snapshot).await;
    lake.assert_exactly(&orders(1..=5)).await;

    // 4. ddi must put them back, and only them
    lake.stream().await;
    lake.assert_exactly(&orders(1..=7)).await;

    // 5. and keep going
    lake.arrive(&orders(8..=10)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=10)).await;
}

#[tokio::test]
async fn compaction_of_bronze_changes_nothing() {
    let lake = Lake::new().await;
    for i in 1..=6 {
        lake.arrive(&orders(i..=i)).await;
    }
    lake.stream().await;
    lake.assert_exactly(&orders(1..=6)).await;

    let (_t, stats) = open_table(ensure_table_uri(&lake.raw).unwrap())
        .await
        .unwrap()
        .optimize()
        .await
        .unwrap();
    assert!(
        stats.num_files_added > 0 || stats.num_files_removed > 0,
        "optimize was a no-op, so this proves nothing"
    );

    lake.stream().await;
    lake.assert_exactly(&orders(1..=6)).await;

    lake.arrive(&orders(7..=8)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=8)).await;
}

#[tokio::test]
async fn compaction_of_silver_changes_nothing() {
    let lake = Lake::new().await;
    let mut cfg = lake.cfg();
    cfg.max_files_per_batch = 1; // several silver files, so OPTIMIZE has work to do
    for i in 1..=6 {
        lake.arrive(&orders(i..=i)).await;
    }
    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    let (_t, stats) = open_table(ensure_table_uri(&lake.stg).unwrap())
        .await
        .unwrap()
        .optimize()
        .await
        .unwrap();
    assert!(
        stats.num_files_added > 0 || stats.num_files_removed > 0,
        "optimize was a no-op, so this proves nothing"
    );

    // A compaction is not a rebuild: it must not reset anything.
    lake.stream().await;
    lake.assert_exactly(&orders(1..=6)).await;

    lake.arrive(&orders(7..=9)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=9)).await;
}

#[tokio::test]
async fn deletes_and_updates_in_bronze_are_skipped_not_propagated() {
    let lake = Lake::new().await;
    lake.arrive(&orders(1..=5)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=5)).await;

    // Someone deletes from bronze. This tool is append-only; under
    // skip_change_commits the commit is consumed and ignored.
    let t = open_table(ensure_table_uri(&lake.raw).unwrap())
        .await
        .unwrap();
    let (_t, m) = t.delete().with_predicate("order_id = 2").await.unwrap();
    assert!(m.num_deleted_rows.unwrap_or(0) > 0, "nothing was deleted");

    lake.stream().await;
    lake.assert_exactly(&orders(1..=5)).await;

    // And the stream continues past it.
    lake.arrive(&orders(6..=7)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=7)).await;
}

#[tokio::test]
async fn deleting_old_rows_from_silver_does_not_bring_them_back() {
    // A delete on the target looks like a rewrite, so ddi rereads its watermark. Removing
    // rows *behind* the newest leaves the watermark where it was, so nothing is re-emitted
    // — the deletion stands.
    let lake = Lake::new().await;
    lake.arrive(&orders(1..=6)).await;
    lake.stream().await;

    let t = open_table(ensure_table_uri(&lake.stg).unwrap())
        .await
        .unwrap();
    let (_t, m) = t.delete().with_predicate("order_id = 2").await.unwrap();
    assert!(m.num_deleted_rows.unwrap_or(0) > 0, "nothing was deleted");

    lake.stream().await;
    let ids: Vec<i64> = lake.silver().await.iter().map(|r| r.0).collect();
    assert_eq!(
        ids,
        vec![1, 3, 4, 5, 6],
        "a deleted row behind the watermark stays deleted"
    );

    lake.arrive(&orders(7..=8)).await;
    lake.stream().await;
    let ids: Vec<i64> = lake.silver().await.iter().map(|r| r.0).collect();
    assert_eq!(ids, vec![1, 3, 4, 5, 6, 7, 8], "and the stream carries on");
}

#[tokio::test]
async fn silver_dropped_and_recreated_is_refilled_from_scratch() {
    let lake = Lake::new().await;
    lake.arrive(&orders(1..=5)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=5)).await;

    // Drop silver entirely and recreate it empty. Its log is gone, so our offset is gone
    // with it, and there is nothing to be beyond.
    std::fs::remove_dir_all(&lake.stg).unwrap();
    create(&lake.stg, stg_schema()).await;

    lake.stream().await;
    lake.assert_exactly(&orders(1..=5)).await;

    lake.arrive(&orders(6..=7)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=7)).await;
}

#[tokio::test]
async fn bronze_dropped_and_recreated_is_a_loud_error_not_a_silent_stall() {
    // The nastiest of the five. A recreated source restarts its log at zero, so a cursor
    // that has already passed that point would sit "caught up" forever against commits
    // that will never arrive: a pipeline that looks healthy and streams nothing.
    let lake = Lake::new().await;
    lake.arrive(&orders(1..=5)).await;
    lake.arrive(&orders(6..=9)).await;
    lake.stream().await;
    lake.assert_exactly(&orders(1..=9)).await;

    std::fs::remove_dir_all(&lake.raw).unwrap();
    create(&lake.raw, raw_schema()).await;
    lake.arrive(&orders(100..=101)).await;

    let Err(e) = Pipeline::open(lake.cfg()).await else {
        panic!("a source whose log went backwards must not open quietly");
    };
    let msg = e.to_string();
    assert!(msg.contains("gone backwards"), "got: {msg}");
    assert!(msg.contains("dropped and recreated"), "got: {msg}");
    assert!(msg.contains("app_id"), "it must name the way out: {msg}");
}

#[tokio::test]
async fn a_fresh_pipeline_against_a_populated_silver_adds_only_what_is_missing() {
    // Starting ddi for the first time against a table the batch job has been building for
    // months: it must fill in from the batch's high-water mark, not replay history.
    let lake = Lake::new().await;
    lake.arrive(&orders(1..=6)).await;
    lake.rebuild(&orders(1..=4)).await; // the batch got as far as 4

    lake.stream().await;
    lake.assert_exactly(&orders(1..=6)).await;
}

#[tokio::test]
async fn ties_at_the_watermark_are_resolved_by_key() {
    // Two orders share an instant; the batch saw one of them. Without key resolution one
    // is either dropped or duplicated.
    let lake = Lake::new().await;
    let same_ts = vec![Order { id: 1, ts: 500 }, Order { id: 2, ts: 500 }];
    lake.arrive(&same_ts).await;

    lake.rebuild(&same_ts[..1]).await; // batch saw only order 1, at t=500
    lake.stream().await;

    let ids: Vec<i64> = lake.silver().await.iter().map(|r| r.0).collect();
    assert_eq!(
        ids,
        vec![1, 2],
        "order 2 shares the watermark instant but was never written"
    );
}

#[tokio::test]
async fn the_rescan_after_a_rebuild_is_bounded_by_file_statistics() {
    // Correctness is covered above; this is about cost. A rebuild must not send ddi back
    // through the whole of bronze, because on a real table that is the entire history,
    // every night. Delta records maxValues per file, so the log itself says how far back
    // the rescan has to reach.
    use delta_delta_ingest::dedup::{bounded_rescan_start, Dedup};

    let lake = Lake::new().await;
    for i in 1..=20 {
        lake.arrive(&orders(i..=i)).await; // 20 separate commits
    }
    lake.stream().await;
    lake.assert_exactly(&orders(1..=20)).await;

    // The batch rebuilds from everything it saw except the last two commits.
    lake.rebuild(&orders(1..=18)).await;

    let target = open_table(ensure_table_uri(&lake.stg).unwrap())
        .await
        .unwrap();
    let source = open_table(ensure_table_uri(&lake.raw).unwrap())
        .await
        .unwrap();
    let dedup = Dedup::read(&target, DEFAULT_TIMESTAMP_COLUMN, Some("order_id"))
        .await
        .unwrap();

    let start = bounded_rescan_start(
        &source,
        DEFAULT_TIMESTAMP_COLUMN,
        dedup.watermark().expect("the rebuild left a watermark"),
        0,
        10_000,
    )
    .await
    .unwrap();

    // Orders 1..=18 landed in commits 1..=18, so everything needed is at 19 and beyond.
    assert_eq!(
        start, 19,
        "the rescan should start just past the last commit the rebuild covered, not at 0"
    );

    // And the pipeline built on it still gets the right answer.
    lake.stream().await;
    lake.assert_exactly(&orders(1..=20)).await;
}

#[tokio::test]
async fn an_unbounded_rescan_is_still_correct_when_statistics_are_missing() {
    // The fallback path: anything it cannot reason about must send it back to the start
    // rather than guess. Asking for a column the source does not carry stands in for any
    // of the ways statistics can be unusable.
    use delta_delta_ingest::dedup::{bounded_rescan_start, Dedup};

    let lake = Lake::new().await;
    lake.arrive(&orders(1..=5)).await;
    lake.stream().await;
    lake.rebuild(&orders(1..=3)).await;

    let target = open_table(ensure_table_uri(&lake.stg).unwrap())
        .await
        .unwrap();
    let source = open_table(ensure_table_uri(&lake.raw).unwrap())
        .await
        .unwrap();
    let dedup = Dedup::read(&target, DEFAULT_TIMESTAMP_COLUMN, Some("order_id"))
        .await
        .unwrap();

    let start = bounded_rescan_start(
        &source,
        "a_column_the_source_does_not_have",
        dedup.watermark().unwrap(),
        0,
        10_000,
    )
    .await
    .unwrap();
    assert_eq!(start, 0, "no usable statistics must mean a full rescan");
}
