//! `orders_raw -> orders_stg` where silver holds **one row per order**, not one per event.
//!
//! Bronze is a log: an order is placed, then paid, then shipped, and each of those is a new
//! row carrying the same `order_id` and a later `_timestamp`. Append-only silver would hold
//! all three. Under `write_mode = "upsert"` it holds the last one.
//!
//! Every test asserts the same two things, because they are the only ones that matter:
//! **one row per key, and it is the newest one delivered.**

mod common;

use std::sync::Arc;

use common::pipeline_cfg;
use delta_delta_ingest::config::{ResolvedPipeline, WriteMode};
use delta_delta_ingest::dedup::DEFAULT_TIMESTAMP_COLUMN;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use delta_delta_ingest::upsert::Lookback;
use deltalake::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};
use futures::TryStreamExt;

// ---------------------------------------------------------------- shapes

fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new(
            DEFAULT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// Silver carries one column bronze knows nothing about. Something else owns `region` —
/// an enrichment job, a dbt post-hook — and an update must not blank it.
fn stg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new(
            DEFAULT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("region", DataType::Utf8, true),
    ]))
}

/// One delivery of one order: its id, what it now says, and when it arrived.
#[derive(Clone, Copy, Debug)]
struct Event {
    id: i64,
    status: &'static str,
    ts: i64,
}

fn ev(id: i64, status: &'static str, ts: i64) -> Event {
    Event { id, status, ts }
}

fn raw_batch(es: &[Event]) -> RecordBatch {
    RecordBatch::try_new(
        raw_schema(),
        vec![
            Arc::new(Int64Array::from(
                es.iter().map(|e| e.id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                es.iter().map(|e| e.status).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                es.iter().map(|e| e.ts).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Rows written straight into silver, bypassing the pipeline — a rebuild, or a backfill.
fn stg_batch(rows: &[(i64, &str, i64, Option<&str>)]) -> RecordBatch {
    RecordBatch::try_new(
        stg_schema(),
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
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
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

    /// Events arrive in bronze — one Delta commit per call.
    async fn arrive(&self, es: &[Event]) {
        open_table(ensure_table_uri(&self.raw).unwrap())
            .await
            .unwrap()
            .write(vec![raw_batch(es)])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    /// Someone else writes silver directly. One commit per call, so each becomes its own
    /// file and the merge window has something to prune.
    async fn seed_silver(&self, rows: &[(i64, &str, i64, Option<&str>)], mode: SaveMode) {
        open_table(ensure_table_uri(&self.stg).unwrap())
            .await
            .unwrap()
            .write(vec![stg_batch(rows)])
            .with_save_mode(mode)
            .await
            .unwrap();
    }

    fn cfg(&self) -> ResolvedPipeline {
        let mut c = pipeline_cfg("orders_stg", &self.raw, &self.stg);
        c.transform_sql = Some("SELECT order_id, status, _timestamp FROM source".to_string());
        c.dedup_timestamp = Some(DEFAULT_TIMESTAMP_COLUMN.to_string());
        c.dedup_key = Some("order_id".to_string());
        c.write_mode = WriteMode::Upsert;
        c.upsert_key = Some("order_id".to_string());
        c
    }

    async fn stream(&self) -> usize {
        self.stream_with(self.cfg()).await
    }

    async fn stream_with(&self, cfg: ResolvedPipeline) -> usize {
        Pipeline::open(cfg)
            .await
            .expect("pipeline should open")
            .run_until_caught_up()
            .await
            .expect("streaming should succeed")
    }

    /// Silver, sorted by order — `(id, status, ts, region)`.
    async fn silver(&self) -> Vec<(i64, String, i64, Option<String>)> {
        let (_t, stream) = open_table(ensure_table_uri(&self.stg).unwrap())
            .await
            .unwrap()
            .scan_table()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut out = Vec::new();
        for b in batches {
            let get = |n: &str| b.column(b.schema().index_of(n).unwrap()).clone();
            // A scan may hand text back as Utf8, LargeUtf8 or Utf8View depending on the
            // reader, so normalise rather than assuming.
            let text = |n: &str| deltalake::arrow::compute::cast(&get(n), &DataType::Utf8).unwrap();
            let ids = get("order_id");
            let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
            let st = text("status");
            let st = st.as_any().downcast_ref::<StringArray>().unwrap();
            let ts = get(DEFAULT_TIMESTAMP_COLUMN);
            let ts = ts
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let rg = text("region");
            let rg = rg.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                out.push((
                    ids.value(i),
                    st.value(i).to_string(),
                    ts.value(i),
                    (!rg.is_null(i)).then(|| rg.value(i).to_string()),
                ));
            }
        }
        // By key, then by sequence: append mode legitimately holds a key more than once,
        // and the assertion should not depend on which file the scan reached first.
        out.sort_by_key(|r| (r.0, r.2));
        out
    }

    async fn statuses(&self) -> Vec<(i64, String)> {
        self.silver()
            .await
            .into_iter()
            .map(|r| (r.0, r.1))
            .collect()
    }

    async fn target_version(&self) -> u64 {
        open_table(ensure_table_uri(&self.stg).unwrap())
            .await
            .unwrap()
            .version()
            .unwrap()
    }
}

// ---------------------------------------------------------------- the point

#[tokio::test]
async fn a_key_delivered_again_replaces_the_row_it_already_had() {
    // The whole reason this mode exists. Append-only silver would hold three rows for
    // order 1; here it holds the last one.
    let lake = Lake::new().await;
    lake.arrive(&[ev(1, "placed", 10), ev(2, "placed", 11)])
        .await;
    lake.stream().await;

    lake.arrive(&[ev(1, "paid", 20)]).await;
    lake.arrive(&[ev(1, "shipped", 30)]).await;
    lake.stream().await;

    assert_eq!(
        lake.statuses().await,
        vec![(1, "shipped".to_string()), (2, "placed".to_string())],
        "one row per order, carrying the newest status"
    );
}

#[tokio::test]
async fn a_redelivery_of_older_data_does_not_roll_the_target_back() {
    // Replay is not a rollback. A re-read of an old commit — after a rebuild, or a source
    // that was dropped and recreated — must not undo what came after it.
    let lake = Lake::new().await;
    lake.arrive(&[ev(1, "shipped", 30)]).await;
    lake.stream().await;

    // The same key arrives again bearing an older timestamp.
    lake.arrive(&[ev(1, "placed", 10)]).await;
    lake.stream().await;

    assert_eq!(
        lake.statuses().await,
        vec![(1, "shipped".to_string())],
        "the stored row is newer, so it stands"
    );
}

#[tokio::test]
async fn two_versions_of_a_key_in_one_batch_collapse_to_the_newest() {
    // Both in a single source commit, so both reach one merge. Without the collapse
    // delta-rs aborts this — "multiple source rows that satisfy duplicate relevant WHEN
    // MATCHED clauses" — rather than applying them in order.
    let lake = Lake::new().await;
    lake.seed_silver(&[(1, "placed", 5, None)], SaveMode::Append)
        .await;
    lake.arrive(&[ev(1, "paid", 20), ev(1, "shipped", 30)])
        .await;
    lake.stream().await;

    assert_eq!(lake.statuses().await, vec![(1, "shipped".to_string())]);
}

#[tokio::test]
async fn a_merge_that_changes_nothing_still_advances_the_offset() {
    // delta-rs declines to commit a merge that produced no file actions, and the `txn`
    // action goes down with it — so without the empty-append fallback in `Sink::upsert` the
    // pipeline re-reads the same source commits forever.
    //
    // Getting there takes a little care, because `Dedup` catches the easy version: it is
    // read once when the pipeline opens, so a row below *that* watermark never reaches the
    // merge at all. The row here is above the open-time watermark and still older than what
    // the first step stored, which is precisely the gap between the two mechanisms.
    let lake = Lake::new().await;
    lake.seed_silver(&[(1, "placed", 10, None)], SaveMode::Append)
        .await;

    lake.arrive(&[ev(1, "shipped", 100)]).await;
    lake.arrive(&[ev(1, "paid", 50)]).await;

    // One commit per batch, so the two deliveries reach two separate merges. Batched
    // together they would collapse into one and never exercise the no-op path.
    let mut cfg = lake.cfg();
    cfg.max_files_per_batch = 1;
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();

    let first = p.step().await.unwrap();
    let StepOutcome::Progressed {
        upsert: Some(applied),
        ..
    } = first
    else {
        panic!("expected the update to apply, got {first:?}");
    };
    assert_eq!(applied.updated, 1);

    let second = p.step().await.unwrap();
    let StepOutcome::Progressed {
        upsert: Some(noop), ..
    } = second
    else {
        panic!("expected a second upsert step, got {second:?}");
    };
    assert_eq!(
        (noop.updated, noop.inserted),
        (0, 0),
        "50 is older than the stored 100, so the merge is a no-op"
    );
    assert!(
        !noop.committed,
        "and delta-rs therefore wrote no commit of its own"
    );

    assert_eq!(
        p.step().await.unwrap(),
        StepOutcome::CaughtUp,
        "the offset must still have moved"
    );

    // The part that would otherwise loop forever: a fresh pipeline must agree there is
    // nothing left to read.
    let mut restarted = Pipeline::open(cfg).await.unwrap();
    assert_eq!(restarted.step().await.unwrap(), StepOutcome::CaughtUp);
    assert_eq!(lake.statuses().await, vec![(1, "shipped".to_string())]);
}

#[tokio::test]
async fn a_column_the_transform_never_produces_survives_an_update() {
    // `region` is owned by something else. The coercer fills it with NULL because the
    // transform says nothing about it, and an UPDATE SET * would write that NULL over the
    // real value on every re-delivery — invisibly, and with no new row left behind for the
    // other writer to notice.
    let lake = Lake::new().await;
    lake.seed_silver(&[(1, "placed", 10, Some("emea"))], SaveMode::Append)
        .await;

    lake.arrive(&[ev(1, "shipped", 30)]).await;
    lake.stream().await;

    assert_eq!(
        lake.silver().await,
        vec![(1, "shipped".to_string(), 30, Some("emea".to_string()))],
        "status updated, region left alone"
    );
}

#[tokio::test]
async fn a_new_key_gets_the_columns_the_transform_does_not_produce_as_null() {
    // The other half of the previous test: for an *inserted* row there is no history to
    // preserve, so NULL is the honest value rather than a destroyed one.
    let lake = Lake::new().await;
    lake.arrive(&[ev(7, "placed", 10)]).await;
    lake.stream().await;

    assert_eq!(
        lake.silver().await,
        vec![(7, "placed".to_string(), 10, None)]
    );
}

// ---------------------------------------------------------------- the window

#[tokio::test]
async fn the_merge_opens_only_the_files_that_could_hold_these_keys() {
    // Four commits, four files, disjoint key ranges. A batch touching one key must not read
    // the other three — that is the entire point of bounding the window.
    let lake = Lake::new().await;
    for block in 0..4i64 {
        let base = block * 100;
        lake.seed_silver(
            &[
                (base + 1, "placed", base + 1, None),
                (base + 2, "placed", base + 2, None),
            ],
            SaveMode::Append,
        )
        .await;
    }

    lake.arrive(&[ev(301, "shipped", 4_000)]).await;
    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    let outcome = p.step().await.unwrap();

    let StepOutcome::Progressed {
        upsert: Some(stats),
        ..
    } = outcome
    else {
        panic!("expected an upsert step, got {outcome:?}");
    };
    assert!(stats.window_bounded, "the statistics should bound this");
    assert!(
        stats.files_scanned <= 1,
        "only the file holding key 301 need be opened, but {} were",
        stats.files_scanned
    );
    assert_eq!(
        lake.statuses().await.len(),
        8,
        "and nothing else moved: still one row per key"
    );
}

#[tokio::test]
async fn a_key_whose_stored_row_is_far_older_than_the_batch_is_still_updated() {
    // The widening. Order 1 was stored long ago; the batch that corrects it is timestamped
    // now. A window drawn from the batch alone would start above the stored row, miss it,
    // and insert a second copy. The target's own file statistics are what stop that.
    let lake = Lake::new().await;
    lake.seed_silver(&[(1, "placed", 10, None)], SaveMode::Append)
        .await;
    // Plenty of newer, unrelated traffic in between.
    lake.seed_silver(
        &[
            (500, "placed", 900_000, None),
            (501, "placed", 900_001, None),
        ],
        SaveMode::Append,
    )
    .await;

    lake.arrive(&[ev(1, "shipped", 1_000_000)]).await;
    lake.stream().await;

    assert_eq!(
        lake.statuses().await,
        vec![
            (1, "shipped".to_string()),
            (500, "placed".to_string()),
            (501, "placed".to_string())
        ],
        "order 1 was replaced, not duplicated"
    );
}

#[tokio::test]
async fn a_lookback_that_is_too_short_says_so_rather_than_quietly_duplicating() {
    // The bargain `upsert_lookback` buys. The stored row for order 1 sits below the floor,
    // so it cannot be matched and the correction is inserted alongside it. That is a real
    // cost, so it must be reported — a silent duplicate here would be the worst outcome.
    let lake = Lake::new().await;
    lake.seed_silver(&[(1, "placed", 10, None)], SaveMode::Append)
        .await;

    let mut cfg = lake.cfg();
    cfg.upsert_lookback = Some(Lookback::Duration(1_000));
    lake.arrive(&[ev(1, "shipped", 1_000_000)]).await;

    let mut p = Pipeline::open(cfg).await.unwrap();
    let outcome = p.step().await.unwrap();
    let StepOutcome::Progressed {
        upsert: Some(stats),
        ..
    } = outcome
    else {
        panic!("expected an upsert step, got {outcome:?}");
    };
    assert!(
        stats.window_clamped,
        "the floor beat the statistics, and that has to be visible"
    );
}

#[tokio::test]
async fn a_generous_lookback_leaves_the_statistics_in_charge() {
    let lake = Lake::new().await;
    lake.seed_silver(&[(1, "placed", 10, None)], SaveMode::Append)
        .await;

    let mut cfg = lake.cfg();
    cfg.upsert_lookback = Some(Lookback::Duration(10_000_000));
    lake.arrive(&[ev(1, "shipped", 1_000_000)]).await;
    lake.stream_with(cfg).await;

    assert_eq!(
        lake.statuses().await,
        vec![(1, "shipped".to_string())],
        "the floor was below the stored row, so nothing was clamped away"
    );
}

// ---------------------------------------------------------------- living with others

#[tokio::test]
async fn an_upsert_commit_of_ours_is_not_mistaken_for_a_foreign_rebuild() {
    // A merge writes dataChange Removes, exactly like a dbt overwrite. `target_state` tells
    // them apart only because it checks for our own txn action *before* it checks for a
    // Remove, and both live in the same commit. Swap those two tests and every upsert
    // pipeline sharing a target with dbt rescans on every restart.
    use delta_delta_ingest::dbt::watermark::{target_state, TargetState, DEFAULT_MAX_SCAN};

    let lake = Lake::new().await;
    lake.arrive(&[ev(1, "placed", 10)]).await;
    lake.stream().await;
    lake.arrive(&[ev(1, "shipped", 20)]).await;
    lake.stream().await;

    let target = open_table(ensure_table_uri(&lake.stg).unwrap())
        .await
        .unwrap();
    let state = target_state(&target, &lake.cfg().app_id, DEFAULT_MAX_SCAN)
        .await
        .unwrap();
    assert_eq!(
        state,
        TargetState::OursOrUntouched,
        "our own merge must not read as somebody else's rebuild"
    );
}

#[tokio::test]
async fn a_rebuild_of_the_target_neither_loses_a_key_nor_doubles_one() {
    // The dbt handover, under upsert. The batch job overwrites silver from what it saw;
    // rows that arrived while it was reading are re-emitted, and merge onto their keys.
    let lake = Lake::new().await;
    lake.arrive(&[ev(1, "placed", 10), ev(2, "placed", 11)])
        .await;
    lake.stream().await;

    // Rows land while the batch job is reading.
    lake.arrive(&[ev(3, "placed", 12)]).await;
    lake.stream().await;

    // ...and the batch job then overwrites silver with only what it saw.
    lake.seed_silver(
        &[(1, "placed", 10, None), (2, "placed", 11, None)],
        SaveMode::Overwrite,
    )
    .await;

    lake.stream().await;
    assert_eq!(
        lake.statuses().await,
        vec![
            (1, "placed".to_string()),
            (2, "placed".to_string()),
            (3, "placed".to_string())
        ],
        "the row the rebuild wiped came back, and the ones it kept were not doubled"
    );
}

#[tokio::test]
async fn a_target_that_already_holds_a_key_twice_is_refused_at_startup() {
    // What an append-only target looks like after a key was restated. A merge would update
    // both copies and keep them forever, so switching modes has to start from a target that
    // already holds the grain it claims.
    let lake = Lake::new().await;
    lake.seed_silver(
        &[(1, "placed", 10, None), (1, "shipped", 20, None)],
        SaveMode::Append,
    )
    .await;

    let e = match Pipeline::open(lake.cfg()).await {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a duplicated key must stop the pipeline"),
    };
    assert!(e.contains("order_id"), "should name the key: {e}");
    assert!(
        e.contains("more than once"),
        "should say what is wrong: {e}"
    );
    assert!(e.contains("row_number"), "should say how to fix it: {e}");
}

#[tokio::test]
async fn restarting_mid_stream_resumes_without_replaying_or_skipping() {
    let lake = Lake::new().await;
    lake.arrive(&[ev(1, "placed", 10)]).await;
    lake.stream().await;
    let after_first = lake.target_version().await;

    // A fresh pipeline with nothing new to read must not write at all.
    lake.stream().await;
    assert_eq!(
        lake.target_version().await,
        after_first,
        "an idle restart must not commit"
    );

    lake.arrive(&[ev(1, "paid", 20), ev(2, "placed", 21)]).await;
    lake.stream().await;
    assert_eq!(
        lake.statuses().await,
        vec![(1, "paid".to_string()), (2, "placed".to_string())]
    );
}

#[tokio::test]
async fn an_append_pipeline_is_untouched_by_any_of_this() {
    // The default has to stay exactly what it was: every delivery is a new row.
    let lake = Lake::new().await;
    let mut cfg = lake.cfg();
    cfg.write_mode = WriteMode::Append;
    cfg.upsert_key = None;

    lake.arrive(&[ev(1, "placed", 10)]).await;
    lake.stream_with(cfg.clone()).await;
    lake.arrive(&[ev(1, "shipped", 20)]).await;
    lake.stream_with(cfg).await;

    assert_eq!(
        lake.statuses().await,
        vec![(1, "placed".to_string()), (1, "shipped".to_string())],
        "append-only keeps both, which is what the README has always promised"
    );
}
