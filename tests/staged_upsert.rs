//! `style_raw -> style` by way of a stage, and the properties that make it a real upsert.
//!
//! The mode exists because a direct upsert on a high-cardinality current-state stream pays
//! for the whole target on every batch: random keys touch every file, so 5,000 rows rewrite
//! the entire state. Staging does not make the merge cheaper — it makes there be fewer of
//! them.
//!
//! So the tests here are about the seam, not about merging, which `upsert.rs` already covers:
//! that the two halves keep separate offsets, that many staged commits become one merge, that
//! either half can be killed and resumed, and that after all of it the target holds exactly
//! what a direct upsert would have left.

mod common;

use std::sync::Arc;

use delta_delta_ingest::config::{Config, ResolvedPipeline};
use delta_delta_ingest::pipeline::Pipeline;
use deltalake::arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};
use futures::TryStreamExt;

// ---------------------------------------------------------------- shapes

/// Bronze and silver agree on every column, which under `staged_upsert` is not a
/// convenience but the rule: see the full-row test at the bottom.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("style_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn batch(rows: &[(i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
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

async fn create(path: &str, s: SchemaRef) {
    let delta: StructType = s.as_ref().try_into_kernel().unwrap();
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

// ---------------------------------------------------------------- lakehouse

struct Lake {
    _dir: tempfile::TempDir,
    raw: String,
    target: String,
    stage: String,
    /// A second silver table fed by an ordinary upsert from the same bronze, so "the same
    /// answer" can be asserted against something rather than against a literal.
    direct: String,
}

impl Lake {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let p = |n: &str| root.join(n).to_str().unwrap().to_string();
        let (raw, target, direct) = (p("style_raw"), p("style"), p("style_direct"));
        create(&raw, schema()).await;
        create(&target, schema()).await;
        create(&direct, schema()).await;
        Self {
            _dir: dir,
            stage: delta_delta_ingest::stage::uri_for(&target),
            raw,
            target,
            direct,
        }
    }

    /// One Delta commit per call.
    async fn arrive(&self, rows: &[(i64, &str, i64)]) {
        open_table(ensure_table_uri(&self.raw).unwrap())
            .await
            .unwrap()
            .write(vec![batch(rows)])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    fn config(&self, extra: &str) -> Config {
        Config::from_toml_str(&format!(
            r#"
[[pipeline]]
name = "style"
app_id = "ddi.style"
source_uri = "{}"
target_uri = "{}"
write_mode = "staged_upsert"
dedup_timestamp = "_timestamp"
upsert_key = "style_id"
transform_sql = "SELECT style_id, status, _timestamp FROM source"
{extra}
"#,
            self.raw, self.target
        ))
        .unwrap()
    }

    /// The two halves, in the order they have to run: nothing can be applied before it has
    /// been staged.
    fn halves(&self) -> (ResolvedPipeline, ResolvedPipeline) {
        let r = self.config("").resolve().unwrap();
        assert_eq!(r.len(), 2);
        let mut it = r.into_iter();
        (it.next().unwrap(), it.next().unwrap())
    }

    fn direct_cfg(&self) -> ResolvedPipeline {
        let mut c = common::pipeline_cfg("direct", &self.raw, &self.direct);
        c.transform_sql = Some("SELECT style_id, status, _timestamp FROM source".into());
        c.dedup_timestamp = Some("_timestamp".into());
        c.write_mode = delta_delta_ingest::config::WriteMode::Upsert;
        c.upsert_key = Some("style_id".into());
        c
    }

    async fn rows(&self, uri: &str) -> Vec<(i64, String, i64)> {
        let (_t, stream) = open_table(ensure_table_uri(uri).unwrap())
            .await
            .unwrap()
            .scan_table()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out = Vec::new();
        for b in batches {
            let get = |n: &str| b.column(b.schema().index_of(n).unwrap()).clone();
            let ids = get("style_id");
            let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
            let st = deltalake::arrow::compute::cast(&get("status"), &DataType::Utf8).unwrap();
            let st = st.as_any().downcast_ref::<StringArray>().unwrap();
            let ts = get("_timestamp");
            let ts = ts
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push((ids.value(i), st.value(i).to_string(), ts.value(i)));
            }
        }
        out.sort();
        out
    }

    async fn version(&self, uri: &str) -> i64 {
        open_table(ensure_table_uri(uri).unwrap())
            .await
            .unwrap()
            .version()
            .map(|v| v as i64)
            .unwrap_or(-1)
    }
}

async fn run(cfg: ResolvedPipeline) -> usize {
    Pipeline::open(cfg)
        .await
        .expect("pipeline should open")
        .run_until_caught_up()
        .await
        .expect("streaming should succeed")
}

// ---------------------------------------------------------------- tests

#[tokio::test]
async fn the_stage_is_created_from_the_target_it_feeds() {
    let lake = Lake::new().await;
    let (ingest, _apply) = lake.halves();

    // Nothing has created it: the stage is the one table this tool provisions, because it
    // is the one table that is its own.
    assert!(open_table(ensure_table_uri(&lake.stage).unwrap())
        .await
        .is_err());

    lake.arrive(&[(1, "new", 10)]).await;
    run(ingest).await;

    let staged = open_table(ensure_table_uri(&lake.stage).unwrap())
        .await
        .expect("the ingest half created its own stage");
    let names: Vec<String> = staged
        .snapshot()
        .unwrap()
        .schema()
        .fields()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(names, vec!["style_id", "status", "_timestamp"]);
}

#[tokio::test]
async fn many_staged_commits_become_one_merge() {
    // The entire point of the mode. Five source commits would be five merges under a direct
    // upsert, each rewriting the whole target.
    let lake = Lake::new().await;
    let (ingest, apply) = lake.halves();

    // Staged one at a time, which is what the ingest worker does when it is keeping up:
    // it commits what has arrived rather than waiting for more. (Handed all five at once it
    // would batch them, and there would be nothing here for the apply half to coalesce.)
    for i in 0..5 {
        lake.arrive(&[(i, "new", 10 + i)]).await;
        run(ingest.clone()).await;
    }
    assert_eq!(
        lake.version(&lake.stage).await,
        5,
        "the create is commit 0, then one commit per staged batch"
    );

    let before = lake.version(&lake.target).await;
    run(apply).await;
    let after = lake.version(&lake.target).await;

    assert_eq!(
        after - before,
        1,
        "five staged commits, one commit to the target"
    );
    assert_eq!(lake.rows(&lake.target).await.len(), 5);
}

#[tokio::test]
async fn the_final_state_is_the_one_a_direct_upsert_would_have_left() {
    // Staging changes when the merge happens, not what it decides.
    let lake = Lake::new().await;
    let (ingest, apply) = lake.halves();

    // A key placed, corrected, and corrected again — across three commits, so the apply
    // half has to collapse them before merging.
    lake.arrive(&[(1, "new", 10), (2, "new", 11)]).await;
    lake.arrive(&[(1, "held", 20)]).await;
    lake.arrive(&[(1, "shipped", 30), (3, "new", 31)]).await;

    run(ingest).await;
    run(apply).await;
    run(lake.direct_cfg()).await;

    let staged = lake.rows(&lake.target).await;
    assert_eq!(
        staged,
        lake.rows(&lake.direct).await,
        "the same rows, whichever way they got there"
    );
    assert_eq!(
        staged,
        vec![
            (1, "shipped".to_string(), 30),
            (2, "new".to_string(), 11),
            (3, "new".to_string(), 31),
        ]
    );
}

#[tokio::test]
async fn each_half_resumes_from_its_own_offset() {
    // The two halves advance independently, and neither may be resumed from the other's
    // position. Running one repeatedly must not move the other.
    let lake = Lake::new().await;
    let (ingest, apply) = lake.halves();

    lake.arrive(&[(1, "new", 10)]).await;
    run(ingest.clone()).await;

    // Ingest again with nothing new: it is caught up, and the stage must not grow.
    let stage_version = lake.version(&lake.stage).await;
    assert_eq!(run(ingest.clone()).await, 0, "nothing left to stage");
    assert_eq!(lake.version(&lake.stage).await, stage_version);

    run(apply.clone()).await;
    let target_version = lake.version(&lake.target).await;

    // And the apply half is now caught up on the stage independently.
    assert_eq!(run(apply.clone()).await, 0, "nothing left to apply");
    assert_eq!(lake.version(&lake.target).await, target_version);

    // New data moves only the half that has work.
    lake.arrive(&[(2, "new", 20)]).await;
    run(ingest).await;
    assert!(lake.version(&lake.stage).await > stage_version);
    assert_eq!(
        lake.version(&lake.target).await,
        target_version,
        "staging alone does not touch the target"
    );

    run(apply).await;
    assert_eq!(lake.rows(&lake.target).await.len(), 2);
}

#[tokio::test]
async fn replaying_either_half_is_idempotent() {
    // The property the two `txn` offsets exist for: re-running a half that has already
    // committed must change nothing, at either boundary.
    let lake = Lake::new().await;
    let (ingest, apply) = lake.halves();

    lake.arrive(&[(1, "new", 10), (2, "new", 11)]).await;
    lake.arrive(&[(1, "shipped", 20)]).await;

    for _ in 0..3 {
        run(ingest.clone()).await;
    }
    let staged_rows = lake.rows(&lake.stage).await.len();

    for _ in 0..3 {
        run(apply.clone()).await;
    }

    assert_eq!(
        lake.rows(&lake.stage).await.len(),
        staged_rows,
        "re-staging the same source commits appends nothing"
    );
    assert_eq!(
        lake.rows(&lake.target).await,
        vec![(1, "shipped".to_string(), 20), (2, "new".to_string(), 11)],
    );
}

#[tokio::test]
async fn an_interrupted_apply_picks_up_the_rows_it_had_not_reached() {
    // Kill the apply half between accumulations. What was staged but not merged is still
    // staged, and the next run finishes the job.
    let lake = Lake::new().await;
    let (ingest, apply) = lake.halves();

    lake.arrive(&[(1, "new", 10)]).await;
    run(ingest.clone()).await;
    run(apply.clone()).await;
    assert_eq!(lake.rows(&lake.target).await.len(), 1);

    // More arrives and is staged, but the apply half never runs — the process died.
    lake.arrive(&[(2, "new", 20)]).await;
    lake.arrive(&[(1, "shipped", 30)]).await;
    run(ingest).await;
    assert_eq!(
        lake.rows(&lake.target).await.len(),
        1,
        "the target is behind, which is the mode's whole bargain"
    );

    // A fresh apply worker, opened from nothing but the durable offset.
    run(apply).await;
    assert_eq!(
        lake.rows(&lake.target).await,
        vec![(1, "shipped".to_string(), 30), (2, "new".to_string(), 20)],
    );
}

#[tokio::test]
async fn a_transform_that_leaves_a_column_out_is_refused() {
    // Option A, enforced. A staged row is merged long after it was written, by which point
    // "the transform said nothing about this column" and "this column is null" are the same
    // bytes — so the mode requires the transform to produce all of them.
    let lake = Lake::new().await;
    let cfg = Config::from_toml_str(&format!(
        r#"
[[pipeline]]
name = "style"
app_id = "ddi.style"
source_uri = "{}"
target_uri = "{}"
write_mode = "staged_upsert"
dedup_timestamp = "_timestamp"
upsert_key = "style_id"
transform_sql = "SELECT style_id, _timestamp FROM source"
"#,
        lake.raw, lake.target
    ))
    .unwrap();
    let ingest = cfg.resolve().unwrap().into_iter().next().unwrap();

    lake.arrive(&[(1, "new", 10)]).await;

    let e = Pipeline::open(ingest)
        .await
        .expect("it opens; the transform has not run yet")
        .run_until_caught_up()
        .await
        .unwrap_err()
        .to_string();

    assert!(e.contains("status"), "names the missing column: {e}");
    assert!(e.contains("staged_upsert"), "and the rule: {e}");
    assert!(
        e.contains("upsert"),
        "and what to do instead, which is the mode that can carry the distinction: {e}"
    );
}

#[tokio::test]
async fn a_rewrite_of_the_stage_stops_the_apply_half_rather_than_losing_rows() {
    // A compaction of the stage is `dataChange: false` and skipped by the reader that skips
    // them anyway. A genuine rewrite is not, and it means somebody is editing rows that are
    // pending application — so the apply half must refuse rather than guess.
    let lake = Lake::new().await;
    let (ingest, apply) = lake.halves();

    lake.arrive(&[(1, "new", 10)]).await;
    run(ingest).await;
    run(apply.clone()).await;

    // Something rewrites the staging table wholesale.
    open_table(ensure_table_uri(&lake.stage).unwrap())
        .await
        .unwrap()
        .write(vec![batch(&[(9, "forged", 99)])])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();

    let e = Pipeline::open(apply)
        .await
        .expect("it opens")
        .run_until_caught_up()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        e.to_lowercase().contains("change"),
        "the rewrite is named rather than absorbed: {e}"
    );
}
