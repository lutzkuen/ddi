//! The dbt handover, end to end.
//!
//! `ddi` stores its offset as a `txn` action in the target's log, and `txn` actions
//! survive an overwrite. So when dbt rebuilds a shared target, `ddi` would otherwise wake
//! up believing it processed through version N, resume at N+1, and never re-emit the rows
//! it streamed while dbt was reading. The first test here is that failure, reproduced
//! against a real table; the rest are the fix.

mod common;

use std::sync::Arc;

use common::*;
use delta_delta_ingest::config::ResolvedPipeline;
use delta_delta_ingest::dbt::watermark::{target_state, TargetState, WatermarkStore};
use delta_delta_ingest::pipeline::Pipeline;
use deltalake::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};

// ------------------------------------------------------------------ watermark table

fn watermark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("app_id", DataType::Utf8, false),
        Field::new("source_version", DataType::Int64, false),
    ]))
}

async fn create_watermark_table(path: &str) {
    let delta: StructType = watermark_schema().as_ref().try_into_kernel().unwrap();
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

/// What a dbt post-hook does: `INSERT INTO ddi_watermark VALUES (app_id, version)`.
async fn record_watermark(path: &str, app_id: &str, version: i64) {
    let batch = RecordBatch::try_new(
        watermark_schema(),
        vec![
            Arc::new(StringArray::from(vec![app_id])) as ArrayRef,
            Arc::new(Int64Array::from(vec![version])) as ArrayRef,
        ],
    )
    .unwrap();
    let url = ensure_table_uri(path).unwrap();
    open_table(url)
        .await
        .unwrap()
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
}

/// What the nightly dbt run does: replace the target with its own full recompute.
async fn dbt_rebuild(target: &str, ids: &[i64]) {
    let url = ensure_table_uri(target).unwrap();
    open_table(url)
        .await
        .unwrap()
        .write(vec![batch(ids)])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();
}

struct Lake {
    f: Fixture,
    watermark: String,
}

async fn lake() -> Lake {
    let f = Fixture::new().await;
    let watermark = std::path::Path::new(&f.target)
        .parent()
        .unwrap()
        .join("ddi_watermark")
        .to_str()
        .unwrap()
        .to_string();
    create_watermark_table(&watermark).await;
    Lake { f, watermark }
}

fn cfg_with_watermark(lake: &Lake, name: &str) -> ResolvedPipeline {
    let mut c = lake.f.cfg(name);
    c.watermark_uri = Some(lake.watermark.clone());
    c
}

// ------------------------------------------------------------------ the hazard

#[tokio::test]
async fn without_a_watermark_a_dbt_rebuild_strands_streamed_rows() {
    // The bug, reproduced. Kept as a test so the fix cannot silently regress into it.
    let f = Fixture::new().await;
    for i in 1..=3 {
        append(&f.source, &[i]).await;
    }

    let cfg = f.cfg("copy"); // no watermark_uri — the unprotected configuration
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3]);

    // dbt read the source as of version 2 and rebuilds from that. Row 3, streamed while
    // dbt was reading, is not in its output.
    dbt_rebuild(&f.target, &[1, 2]).await;

    append(&f.source, &[4]).await;
    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    assert_eq!(
        read_ids(&f.target).await,
        vec![1, 2, 4],
        "row 3 is gone: streamed by ddi, wiped by dbt, never re-emitted"
    );
}

// ------------------------------------------------------------------ the fix

#[tokio::test]
async fn a_watermark_makes_ddi_re_stream_the_gap_dbt_wiped() {
    let lake = lake().await;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }

    let cfg = cfg_with_watermark(&lake, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3]);

    // dbt rebuilds from source version 2, and records that.
    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;

    append(&lake.f.source, &[4]).await;
    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 4],
        "row 3 must be re-streamed from dbt's watermark, and row 4 must follow"
    );
}

#[tokio::test]
async fn an_overwrite_with_no_watermark_recorded_is_a_loud_error() {
    // The refusal matters as much as the reset: continuing here would drop rows silently.
    let lake = lake().await;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }

    let cfg = cfg_with_watermark(&lake, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await; // ... and no watermark written

    let Err(e) = Pipeline::open(cfg).await else {
        panic!("a target rebuilt with no watermark recorded must not open cleanly");
    };
    let msg = e.to_string();
    assert!(msg.contains("rewritten"), "got: {msg}");
    assert!(msg.contains("watermark"), "got: {msg}");
    assert!(
        msg.contains("silently drop"),
        "the error must say what it is protecting against: {msg}"
    );
}

#[tokio::test]
async fn an_ordinary_restart_still_uses_our_own_offset() {
    // The watermark must only take over after an actual overwrite. If it applied on every
    // restart, a stale watermark would replay the whole stream and duplicate everything.
    let lake = lake().await;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    // A watermark from an older dbt run is sitting there.
    let cfg = cfg_with_watermark(&lake, "copy");
    record_watermark(&lake.watermark, &cfg.app_id, 0).await;

    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3]);

    // Restart with no overwrite in between.
    let n = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(n, 0, "a plain restart must be a no-op");
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3],
        "the stale watermark must not have replayed anything"
    );
}

#[tokio::test]
async fn compaction_of_the_target_is_not_mistaken_for_a_dbt_rebuild() {
    // OPTIMIZE on the target removes files with dataChange=false. Treating that as a
    // rebuild would reset the offset and duplicate the whole tail.
    let lake = lake().await;
    for i in 1..=4 {
        append(&lake.f.source, &[i]).await;
    }
    // One source commit per batch, so the target ends up with four small files for
    // OPTIMIZE to actually merge. Batched together they would be a single file and the
    // compaction would be a no-op, proving nothing.
    let mut cfg = cfg_with_watermark(&lake, "copy");
    cfg.max_files_per_batch = 1;
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    let t = open(&lake.f.target).await;
    let (_t, stats) = t.optimize().await.unwrap();
    assert!(
        stats.num_files_added > 0 || stats.num_files_removed > 0,
        "optimize was a no-op, so this test proves nothing"
    );

    let target = open(&lake.f.target).await;
    assert_eq!(
        target_state(&target, &cfg.app_id, 10_000).await.unwrap(),
        TargetState::OursOrUntouched,
        "a compaction is not a rebuild"
    );

    let n = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn the_watermark_store_reads_the_highest_version_for_its_own_app_id() {
    let lake = lake().await;
    record_watermark(&lake.watermark, "ddi.other", 999).await;
    record_watermark(&lake.watermark, "ddi.mine", 5).await;
    record_watermark(&lake.watermark, "ddi.mine", 11).await;

    let store = WatermarkStore::new(&lake.watermark);
    assert_eq!(store.last("ddi.mine").await.unwrap(), Some(11));
    assert_eq!(
        store.last("ddi.absent").await.unwrap(),
        None,
        "an unknown app_id has no watermark, rather than borrowing someone else's"
    );
}
