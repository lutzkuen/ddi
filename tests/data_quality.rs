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
use delta_delta_ingest::config::{ResolvedPipeline, WriteMode};
use delta_delta_ingest::dedup::DEFAULT_TIMESTAMP_COLUMN;
use delta_delta_ingest::dq::DataQuality;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use delta_delta_ingest::schema::Rejected;
use delta_delta_ingest::storage::Storage;
use delta_delta_ingest::Error;
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

    /// What a batch job that filled silver before this pipeline ever started leaves there.
    async fn fill_silver(&self, rows: &[(i64, i64, i64)]) {
        let batch = RecordBatch::try_new(
            stg_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap();
        open_table(ensure_table_uri(&self.stg).unwrap())
            .await
            .unwrap()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    fn cfg(&self) -> ResolvedPipeline {
        // No transform: the coercer is what casts text to number, which is the code under
        // test. A CAST in transform_sql would fail in DataFusion instead.
        pipeline_cfg("orders_stg", &self.raw, &self.stg)
    }

    /// A pipeline whose transform does the cast, so a value that will not convert fails in
    /// DataFusion rather than in the coercer.
    fn casting_cfg(&self) -> ResolvedPipeline {
        let mut cfg = self.cfg();
        cfg.transform_sql = Some(
            "SELECT order_id, CAST(amount AS BIGINT) AS amount, _timestamp FROM source".into(),
        );
        cfg
    }

    /// Record raw row 2 as a reject of the first batch (txn version 1), as the data-quality
    /// write does just before the target's commit — which is the crash window this leaves
    /// open: the pipeline has not committed, so it will read the batch again.
    async fn record_before_a_crash(&self, column: Option<&str>) {
        let cfg = self.cfg();
        let mut dq = DataQuality::open(&Storage::default(), &self.dq, &cfg.app_id, &cfg.name)
            .await
            .unwrap()
            .expect("created");
        let written = dq
            .write(
                &[Rejected {
                    rows: raw_batch(&[(2, Some("n/a"), 11)]),
                    reasons: vec!["seeded".into()],
                    columns: vec![column.map(str::to_string)],
                }],
                1,
                0,
            )
            .await
            .unwrap();
        assert_eq!(written, 1);
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
    async fn rejects(&self) -> Vec<(i64, Option<String>, String, String)> {
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
                    (!column.is_null(i)).then(|| column.value(i).to_string()),
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
    assert_eq!(
        rejects[0].1.as_deref(),
        Some("amount"),
        "names the column that did it"
    );
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
    assert!(first.fully_rejected(), "the shape an alert is raised on");

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
async fn a_bad_row_among_covered_ones_is_not_a_fully_rejected_batch() {
    // Inside a coverage window every good row of a batch can be one the target already holds.
    // The bad row left over is one bad row, not the upstream type change a batch whose every
    // row failed is taken for — and a rescan would otherwise raise that alarm on every batch
    // that re-reads it.
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[
        (1, Some("100"), 10),
        (2, Some("n/a"), 11),
        (3, Some("300"), 12),
    ])
    .await;
    // Filled by the batch job before this pipeline first started.
    lake.fill_silver(&[(1, 100, 10), (3, 300, 12)]).await;

    let mut cfg = lake.cfg();
    cfg.dedup_timestamp = Some(DEFAULT_TIMESTAMP_COLUMN.into());
    cfg.dedup_key = Some("order_id".into());
    let mut p = Pipeline::open(cfg).await.unwrap();
    assert!(
        p.coverage().is_some(),
        "a first start against a populated target"
    );

    let step = p.step().await.unwrap();
    let StepOutcome::Skipped {
        rejected, covered, ..
    } = step.clone()
    else {
        panic!("expected a skipped step, got {step:?}");
    };
    assert_eq!((rejected, covered), (1, 2));
    assert!(
        !step.fully_rejected(),
        "one bad row, with the rest already in the target"
    );
    assert_eq!(lake.rejects().await.len(), 1);
}

#[tokio::test]
async fn replaying_a_batch_does_not_record_its_rejects_twice() {
    // The data-quality table cannot share the target's commit, so a crash between the two
    // replays the batch. The DQ table's own txn action is what stops that becoming a
    // duplicate. The crash is simulated by recording the batch's reject before the pipeline
    // ever runs: the DQ write committed, the target's did not.
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[(1, Some("100"), 10), (2, Some("n/a"), 11)])
        .await;
    lake.record_before_a_crash(Some("amount")).await;

    lake.stream().await.unwrap();

    assert_eq!(lake.silver().await, vec![(1, Some(100))]);
    let rejects = lake.rejects().await;
    assert_eq!(
        rejects.len(),
        1,
        "the same reject must not be recorded a second time: {rejects:?}"
    );
    assert_eq!(rejects[0].2, "seeded", "the one written before the crash");
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
    assert_eq!(rejects[0].1.as_deref(), Some("order_id"));
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

// ---------------------------------------------------------------- the transform itself

#[tokio::test]
async fn a_row_the_transform_cannot_evaluate_is_set_aside_and_the_rest_commits() {
    // The cast happens in the transform now, so "n/a" fails the batch in DataFusion, before
    // the coercer could set it aside. Before #11 that stopped the pipeline on every retry.
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[
        (1, Some("100"), 10),
        (2, Some("n/a"), 11),
        (3, Some("300"), 12),
    ])
    .await;

    let mut p = Pipeline::open(lake.casting_cfg()).await.unwrap();
    let outcome = p.step().await.expect("one bad value must not stop this");
    let StepOutcome::Progressed {
        rows,
        rejected,
        unevaluable,
        reevaluations,
        ..
    } = outcome
    else {
        panic!("expected a commit, got {outcome:?}");
    };
    assert_eq!((rows, rejected, unevaluable), (2, 1, 1));
    assert!(reevaluations > 0, "finding the row took runs");

    assert_eq!(lake.silver().await, vec![(1, Some(100)), (3, Some(300))]);
    let rejects = lake.rejects().await;
    assert_eq!(rejects.len(), 1, "{rejects:?}");
    let (version, column, reason, payload) = &rejects[0];
    assert_eq!(
        *version, 1,
        "the batch's own txn version; the CREATE is version 0"
    );
    assert_eq!(*column, None, "no one column is to blame");
    assert!(reason.contains("could not evaluate"), "{reason}");
    assert!(reason.contains("n/a"), "{reason}");
    assert!(
        payload.contains("\"order_id\":2"),
        "the source row: {payload}"
    );
    assert!(payload.contains("\"amount\":\"n/a\""), "{payload}");
}

#[tokio::test]
async fn without_a_data_quality_table_a_row_the_transform_cannot_evaluate_still_stops_the_pipeline()
{
    let lake = Lake::new().await;
    lake.arrive(&[(1, Some("100"), 10), (2, Some("n/a"), 11)])
        .await;

    let e = Pipeline::open(lake.casting_cfg())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .expect_err("nowhere to set the row aside");
    assert!(matches!(e, Error::Evaluation(_)), "{e:?}");
    let m = e.to_string();
    assert!(m.contains("transform_sql failed to execute"), "{m}");
    assert!(
        m.contains(&lake.dq),
        "says which table would set it aside: {m}"
    );
    assert!(lake.silver().await.is_empty());

    // With isolation turned off, creating the table would change nothing, so the error must
    // not say it would.
    let mut cfg = lake.casting_cfg();
    cfg.max_evaluation_rejects_per_batch = 0;
    let m = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .expect_err("isolation is off")
        .to_string();
    assert!(m.contains("transform_sql failed to execute"), "{m}");
    assert!(!m.contains(&lake.dq), "promises nothing: {m}");
}

#[tokio::test]
async fn a_batch_the_transform_cannot_evaluate_at_all_is_skipped_and_the_offset_advances() {
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[(1, Some("n/a"), 10), (2, Some("also bad"), 11)])
        .await;

    let mut p = Pipeline::open(lake.casting_cfg()).await.unwrap();
    let first = p.step().await.unwrap();
    let StepOutcome::Skipped {
        rejected,
        unevaluable,
        ..
    } = first
    else {
        panic!("expected a skipped step that consumed the batch, got {first:?}");
    };
    assert_eq!((rejected, unevaluable), (2, 2));
    assert_eq!(p.step().await.unwrap(), StepOutcome::CaughtUp);
    let mut restarted = Pipeline::open(lake.casting_cfg()).await.unwrap();
    assert_eq!(restarted.step().await.unwrap(), StepOutcome::CaughtUp);
    assert_eq!(lake.rejects().await.len(), 2);
}

#[tokio::test]
async fn an_unevaluable_reject_already_recorded_before_a_crash_is_not_written_twice() {
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[
        (1, Some("100"), 10),
        (2, Some("n/a"), 11),
        (3, Some("300"), 12),
    ])
    .await;
    lake.record_before_a_crash(None).await;

    let mut p = Pipeline::open(lake.casting_cfg()).await.unwrap();
    p.step().await.unwrap();
    assert_eq!(p.step().await.unwrap(), StepOutcome::CaughtUp);

    assert_eq!(lake.silver().await, vec![(1, Some(100)), (3, Some(300))]);
    let rejects = lake.rejects().await;
    assert_eq!(rejects.len(), 1, "{rejects:?}");
    assert_eq!(rejects[0].2, "seeded");
}

#[tokio::test]
async fn a_failure_no_row_causes_is_not_quarantined() {
    // Division by a constant zero fails on an empty batch too, so it is the query that is
    // wrong. Setting every row aside for it would empty the target into the DQ table.
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[(1, Some("100"), 10), (2, Some("200"), 11)])
        .await;

    let mut cfg = lake.cfg();
    cfg.transform_sql = Some("SELECT order_id, 1/0 AS amount, _timestamp FROM source".into());
    let e = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .expect_err("no row is to blame");
    assert!(matches!(e, Error::Transform(_)), "{e:?}");
    assert!(lake.silver().await.is_empty());
    assert!(lake.rejects().await.is_empty());
}

#[tokio::test]
async fn more_unevaluable_rows_than_the_limit_stop_the_batch() {
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[
        (1, Some("100"), 10),
        (2, Some("n/a"), 11),
        (3, Some("also bad"), 12),
    ])
    .await;

    let mut cfg = lake.casting_cfg();
    cfg.max_evaluation_rejects_per_batch = 1;
    let e = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .expect_err("two bad rows against a limit of one");
    assert!(
        e.to_string().contains("max_evaluation_rejects_per_batch"),
        "names the knob: {e}"
    );
    assert!(lake.silver().await.is_empty());
    assert!(lake.rejects().await.is_empty());
}

#[tokio::test]
async fn an_upsert_pipeline_sets_aside_the_row_it_cannot_evaluate() {
    let lake = Lake::new().await;
    lake.create_dq().await;
    lake.arrive(&[
        (1, Some("100"), 10),
        (2, Some("n/a"), 11),
        (3, Some("300"), 12),
    ])
    .await;
    lake.arrive(&[(1, Some("oops"), 13), (3, Some("301"), 14)])
        .await;

    let mut cfg = lake.casting_cfg();
    cfg.write_mode = WriteMode::Upsert;
    cfg.upsert_key = Some("order_id".into());
    cfg.dedup_key = Some("order_id".into());
    cfg.dedup_timestamp = Some(DEFAULT_TIMESTAMP_COLUMN.into());
    cfg.max_files_per_batch = 1;
    let mut p = Pipeline::open(cfg).await.unwrap();

    let first = p.step().await.unwrap();
    let StepOutcome::Progressed {
        upsert, rejected, ..
    } = first
    else {
        panic!("expected a merge, got {first:?}");
    };
    assert!(upsert.is_some());
    assert_eq!(rejected, 1);

    let second = p.step().await.unwrap();
    let StepOutcome::Progressed {
        upsert: Some(upsert),
        unevaluable,
        ..
    } = second
    else {
        panic!("expected a merge, got {second:?}");
    };
    assert_eq!((upsert.updated, unevaluable), (1, 1));

    // Key 1's newer delivery could not be evaluated, so the stored row stands — as it does
    // when a newer delivery will not coerce.
    assert_eq!(lake.silver().await, vec![(1, Some(100)), (3, Some(301))]);
    let versions: Vec<i64> = lake.rejects().await.iter().map(|r| r.0).collect();
    assert_eq!(versions, vec![1, 2], "one per arrive commit");
}
