//! What happens to a row the target will not take.
//!
//! Bronze carries `amount` as text, because bronze always does. Silver declares it a
//! `BIGINT`. Most rows convert; the one that says `"n/a"` does not. The question this file
//! answers is what that one row costs — historically the whole pipeline, and now a row in a
//! table next to the target.
//!
//! Every test asserts the same two things: **the good rows landed, and the bad ones are
//! somewhere you can find them.**

mod common;

use std::sync::Arc;

use common::pipeline_cfg;
use delta_delta_ingest::config::ResolvedPipeline;
use delta_delta_ingest::dedup::DEFAULT_TIMESTAMP_COLUMN;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
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

/// Bronze: `amount` is text, as it arrives.
fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // Nullable in bronze, NOT NULL in silver. Bronze takes what it is given; silver is
        // where the contract is enforced.
        Field::new("order_id", DataType::Int64, true),
        Field::new("amount", DataType::Utf8, true),
        Field::new(
            DEFAULT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// Silver: `amount` is a number. The coercer is what has to bridge the two.
fn stg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
        Field::new(
            DEFAULT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn raw_batch(rows: &[(i64, Option<&str>, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        raw_schema(),
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

// ---------------------------------------------------------------- lakehouse

struct Lake {
    _dir: tempfile::TempDir,
    raw: String,
    stg: String,
    dq: String,
}

async fn create(path: &str, schema: SchemaRef) {
    let delta: StructType = schema.as_ref().try_into_kernel().unwrap();
    DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();
}

impl Lake {
    /// A lake with no data-quality table yet. Call [`Self::create_dq`] to add one.
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let raw = root.join("orders_raw").to_str().unwrap().to_string();
        let stg = root.join("orders_stg").to_str().unwrap().to_string();
        create(&raw, raw_schema()).await;
        create(&stg, stg_schema()).await;
        let dq = delta_delta_ingest::dq::uri_for(&stg);
        Self {
            _dir: dir,
            raw,
            stg,
            dq,
        }
    }

    /// Create the table at the derived location, exactly as an operator would.
    async fn create_dq(&self) {
        DeltaTable::try_from_url(ensure_table_uri(&self.dq).unwrap())
            .await
            .unwrap()
            .create()
            .with_columns(delta_delta_ingest::dq::columns())
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
    }

    async fn arrive(&self, rows: &[(i64, Option<&str>, i64)]) {
        open_table(ensure_table_uri(&self.raw).unwrap())
            .await
            .unwrap()
            .write(vec![raw_batch(rows)])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    fn cfg(&self) -> ResolvedPipeline {
        // No transform: the coercer is what casts text to number, which is the code under
        // test. A CAST in transform_sql would fail in DataFusion instead.
        pipeline_cfg("orders_stg", &self.raw, &self.stg)
    }

    async fn stream(&self) -> delta_delta_ingest::Result<usize> {
        Pipeline::open(self.cfg())
            .await?
            .run_until_caught_up()
            .await
    }

    async fn silver(&self) -> Vec<(i64, Option<i64>)> {
        let (_t, stream) = open_table(ensure_table_uri(&self.stg).unwrap())
            .await
            .unwrap()
            .scan_table()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out = Vec::new();
        for b in batches {
            let ids = b.column(b.schema().index_of("order_id").unwrap()).clone();
            let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
            let amt = b.column(b.schema().index_of("amount").unwrap()).clone();
            let amt = amt.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((ids.value(i), (!amt.is_null(i)).then(|| amt.value(i))));
            }
        }
        out.sort();
        out
    }

    /// Rejected rows as `(source_version, column_name, reason, payload)`.
    async fn rejects(&self) -> Vec<(i64, String, String, String)> {
        let (_t, stream) = open_table(ensure_table_uri(&self.dq).unwrap())
            .await
            .unwrap()
            .scan_table()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out = Vec::new();
        for b in batches {
            let text = |n: &str| {
                deltalake::arrow::compute::cast(
                    b.column(b.schema().index_of(n).unwrap()),
                    &DataType::Utf8,
                )
                .unwrap()
            };
            let version = b
                .column(b.schema().index_of("source_version").unwrap())
                .clone();
            let version = version.as_any().downcast_ref::<Int64Array>().unwrap();
            let column = text("column_name");
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            let reason = text("reason");
            let reason = reason.as_any().downcast_ref::<StringArray>().unwrap();
            let payload = text("payload");
            let payload = payload.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                out.push((
                    version.value(i),
                    column.value(i).to_string(),
                    reason.value(i).to_string(),
                    payload.value(i).to_string(),
                ));
            }
        }
        out.sort();
        out
    }
}

// ---------------------------------------------------------------- the point

#[tokio::test]
async fn a_row_that_will_not_cast_is_set_aside_and_the_rest_commits() {
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[
        (1, Some("100"), 10),
        (2, Some("n/a"), 11),
        (3, Some("300"), 12),
    ])
    .await;

    lake.stream().await.expect("one bad row must not stop this");

    assert_eq!(
        lake.silver().await,
        vec![(1, Some(100)), (3, Some(300))],
        "the rows that convert land, and nothing was nulled to make row 2 fit"
    );

    let rejects = lake.rejects().await;
    assert_eq!(rejects.len(), 1, "exactly the one bad row: {rejects:?}");
    assert_eq!(rejects[0].1, "amount", "names the column that did it");
    assert!(
        rejects[0].2.contains("amount"),
        "reason names it too: {}",
        rejects[0].2
    );
    assert!(
        rejects[0].3.contains("\"n/a\""),
        "the payload keeps the value verbatim: {}",
        rejects[0].3
    );
    assert!(
        rejects[0].3.contains("\"order_id\":2"),
        "and enough of the row to find it again: {}",
        rejects[0].3
    );
}

#[tokio::test]
async fn without_a_data_quality_table_a_bad_row_still_stops_the_pipeline() {
    // The old behaviour, kept deliberately. Discarding rejects because nobody created a
    // table would be worse than stopping, so the absence of one is not a licence to drop.
    let lake = Lake::new().await;
    lake.arrive(&[(1, Some("100"), 10), (2, Some("n/a"), 11)])
        .await;

    let e = lake
        .stream()
        .await
        .expect_err("no table to set the row aside in, so it must stop")
        .to_string();
    assert!(e.contains("amount"), "names the column: {e}");
    assert!(
        lake.silver().await.is_empty(),
        "and the batch is atomic, so nothing landed"
    );
}

#[tokio::test]
async fn the_offset_advances_past_a_batch_whose_rows_were_all_rejected() {
    // The upstream-schema-change shape. Every row fails, so the target learns nothing —
    // but the offset must still move, or the pipeline re-reads the same commit forever.
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[(1, Some("n/a"), 10), (2, Some("also bad"), 11)])
        .await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    // Nothing reached the target, so the step reports Skipped — but it still carries the
    // reject count, which is what makes a fully-rejected batch visible.
    let first = p.step().await.unwrap();
    let StepOutcome::Skipped { rejected, .. } = first else {
        panic!("expected a skipped step that consumed the batch, got {first:?}");
    };
    assert_eq!(rejected, 2);

    assert_eq!(
        p.step().await.unwrap(),
        StepOutcome::CaughtUp,
        "the offset must have moved"
    );
    let mut restarted = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(
        restarted.step().await.unwrap(),
        StepOutcome::CaughtUp,
        "and a fresh pipeline must agree, or this loops forever"
    );
    assert_eq!(lake.rejects().await.len(), 2);
}

#[tokio::test]
async fn replaying_a_batch_does_not_record_its_rejects_twice() {
    // The data-quality table cannot share the target's commit, so a crash between the two
    // replays the batch. The DQ table's own txn action is what stops that becoming a
    // duplicate.
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[(1, Some("100"), 10), (2, Some("n/a"), 11)])
        .await;

    lake.stream().await.unwrap();
    assert_eq!(lake.rejects().await.len(), 1);

    // A second pipeline with the same app_id replays nothing, but one that starts over —
    // as it would after a rebuild — re-reads the same source commit.
    let mut cfg = lake.cfg();
    cfg.starting_version = 0;
    cfg.dedup_timestamp = Some(DEFAULT_TIMESTAMP_COLUMN.to_string());
    let mut p = Pipeline::open(cfg).await.unwrap();
    while !matches!(p.step().await.unwrap(), StepOutcome::CaughtUp) {}

    assert_eq!(
        lake.rejects().await.len(),
        1,
        "the same reject must not be recorded a second time"
    );
}

#[tokio::test]
async fn a_null_in_a_not_null_target_column_is_rejected_per_row() {
    // Not a cast failure — the value is simply absent where the target insists on one. Same
    // treatment: that row is set aside, its neighbours are not.
    let lake = Lake::new().await;
    lake.create_dq().await;

    // order_id is NOT NULL in silver, so a row without one cannot be stored.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, true),
            Field::new("amount", DataType::Utf8, true),
            Field::new(
                DEFAULT_TIMESTAMP_COLUMN,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("100"),
                Some("200"),
                Some("300"),
            ])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![10, 11, 12])) as ArrayRef,
        ],
    )
    .unwrap();
    open_table(ensure_table_uri(&lake.raw).unwrap())
        .await
        .unwrap()
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    lake.stream().await.unwrap();

    assert_eq!(lake.silver().await, vec![(1, Some(100)), (3, Some(300))]);
    let rejects = lake.rejects().await;
    assert_eq!(rejects.len(), 1);
    assert_eq!(rejects[0].1, "order_id");
    assert!(
        rejects[0].2.contains("NOT NULL"),
        "the reason should say so: {}",
        rejects[0].2
    );
}

#[tokio::test]
async fn a_clean_batch_writes_nothing_to_the_data_quality_table() {
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[(1, Some("100"), 10), (2, Some("200"), 11)])
        .await;

    lake.stream().await.unwrap();

    assert_eq!(lake.silver().await, vec![(1, Some(100)), (2, Some(200))]);
    assert!(
        lake.rejects().await.is_empty(),
        "the common case must stay free"
    );
}

#[tokio::test]
async fn a_missing_target_column_is_still_a_hard_error() {
    // A column the transform never produces is not bad data: it is the same on every batch
    // and belongs to no row. Quarantining it would leave a target that silently never
    // grows, so it stops the pipeline (which then retries) instead.
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[(1, Some("100"), 10)]).await;

    let mut cfg = lake.cfg();
    // `order_id` is NOT NULL in silver, and this transform never produces it.
    cfg.transform_sql = Some("SELECT amount, _timestamp FROM source".to_string());
    let e = match Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
    {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a missing NOT NULL column must not be quarantined away"),
    };
    assert!(
        e.contains("order_id") || e.contains("_timestamp"),
        "got: {e}"
    );
}

#[tokio::test]
async fn the_data_quality_table_is_found_next_to_the_target_without_configuring_it() {
    // The reason it is derived: three hundred pipelines should need no per-pipeline setting.
    let lake = Lake::new().await;
    assert!(
        lake.dq.ends_with("orders_stg__ddi_dq"),
        "derived from the target: {}",
        lake.dq
    );
    assert_eq!(lake.cfg().dq_uri(), lake.dq);
}
