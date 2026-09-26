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
    let watermark = beside_the_target(&f, "ddi_watermark");
    create_watermark_table(&watermark).await;
    Lake { f, watermark }
}

fn beside_the_target(f: &Fixture, name: &str) -> String {
    std::path::Path::new(&f.target)
        .parent()
        .unwrap()
        .join(name)
        .to_str()
        .unwrap()
        .to_string()
}

fn cfg_with_watermark(lake: &Lake, name: &str) -> ResolvedPipeline {
    let mut c = lake.f.cfg(name);
    c.watermark_uri = Some(lake.watermark.clone());
    c
}

/// Both, as every dbt model has them once a watermark table is set: `ddi_timestamp` defaults
/// to `_timestamp`. `id` plays the timestamp here.
fn cfg_with_watermark_and_timestamp(lake: &Lake) -> ResolvedPipeline {
    let mut c = cfg_with_watermark(lake, "copy");
    c.dedup_timestamp = Some("id".into());
    c.dedup_key = Some("id".into());
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

#[tokio::test]
async fn a_recorded_watermark_wins_over_the_timestamp_rescan() {
    // Both set, as every dbt model has them: `ddi_timestamp` defaults to `_timestamp`. The
    // watermark is exact however rows arrive, and the rescan is not: here 3 is a lagging
    // Kafka partition's row, landing after 5, so under the target's max(_timestamp) of 5 it
    // reads as already covered, and its commit is not even re-read.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    // Running before dbt reads, as it would be: a watermark counts only when it is newer than
    // the source head this pipeline last handed over at, or first opened at.
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in [1, 5, 3] {
        append(&lake.f.source, &[i]).await; // versions 1, 2, 3
    }
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 3, 5]);

    // dbt rebuilds from source version 2, and records that. Row 3 is gone.
    dbt_rebuild(&lake.f.target, &[1, 5]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    append(&lake.f.source, &[4]).await; // older than 5 too

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(
        p.coverage(),
        None,
        "the rebuild said what the target holds, so nothing is inferred from its data"
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 3, 4, 5],
        "3 re-streamed from dbt's watermark and 4 after it, neither dropped for being older \
         than 5"
    );
}

#[tokio::test]
async fn a_rebuild_that_recorded_no_watermark_falls_back_to_the_timestamp() {
    // The same pair of settings, and a rebuild that wrote nothing to the watermark table on
    // its first night: an empty table. With a timestamp to fall back on, that is no reason to
    // refuse. A table holding only earlier rebuilds' rows is the test after this one.
    let lake = lake().await;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }

    let cfg = cfg_with_watermark_and_timestamp(&lake);
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await; // ... and no watermark written
    append(&lake.f.source, &[4]).await;

    Pipeline::open(cfg)
        .await
        .expect("the rescan needs no watermark")
        .run_until_caught_up()
        .await
        .unwrap();
    let got = read_ids(&lake.f.target).await;
    assert_eq!(got, vec![1, 2, 3, 4], "row 3 recovered by the rescan");
}

#[tokio::test]
async fn a_watermark_table_that_is_not_there_falls_back_to_the_timestamp() {
    // `[storage].watermark_uri` is read for every dbt model, and each has a timestamp. Before
    // it was read for them, a table nobody created, or one since moved, stopped nothing: the
    // rescan answered the rebuild. It still does, where it stopped the model at every rebuild.
    // Without a timestamp there is nothing to fall back on, and the open still refuses.
    let lake = lake().await;
    let mut cfg = cfg_with_watermark_and_timestamp(&lake);
    cfg.watermark_uri = Some(beside_the_target(&lake.f, "never_created"));
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    append(&lake.f.source, &[4]).await;

    let mut without_a_timestamp = cfg.clone();
    without_a_timestamp.dedup_timestamp = None;
    without_a_timestamp.dedup_key = None;
    let Err(e) = Pipeline::open(without_a_timestamp).await else {
        panic!("with nothing to fall back on, a missing watermark table must still refuse");
    };
    assert!(e.to_string().contains("must exist"), "got: {e}");

    Pipeline::open(cfg)
        .await
        .expect("the rescan needs no watermark table")
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 4],
        "row 3 recovered by the rescan"
    );
}

#[tokio::test]
async fn a_watermark_table_of_another_shape_falls_back_to_the_timestamp() {
    // What a warehouse writes when the table is declared with an INTEGER source_version: a
    // row the store refuses to read, at every retry.
    let lake = lake().await;
    let mut cfg = cfg_with_watermark_and_timestamp(&lake);
    let watermark = beside_the_target(&lake.f, "ddi_watermark_int");
    cfg.watermark_uri = Some(watermark.clone());
    let int_typed = Arc::new(Schema::new(vec![
        Field::new("app_id", DataType::Utf8, false),
        Field::new("source_version", DataType::Int32, false),
    ]));
    let row = RecordBatch::try_new(
        int_typed,
        vec![
            Arc::new(StringArray::from(vec![cfg.app_id.as_str()])) as ArrayRef,
            Arc::new(deltalake::arrow::array::Int32Array::from(vec![2])) as ArrayRef,
        ],
    )
    .unwrap();
    DeltaTable::try_from_url(ensure_table_uri(&watermark).unwrap())
        .await
        .unwrap()
        .write(vec![row])
        .await
        .unwrap();

    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    append(&lake.f.source, &[4]).await;

    Pipeline::open(cfg)
        .await
        .expect("an INTEGER source_version is refused, and the rescan needs none")
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 4],
        "row 3 recovered by the rescan"
    );
}

#[tokio::test]
async fn a_watermark_table_that_cannot_be_read_stops_the_open_rather_than_falling_back() {
    // A read that failed says nothing about what the table holds, and the rescan's cut-off
    // would drop for good the late rows the watermark kept. Here the file holding this
    // rebuild's row is gone, as a read racing a VACUUM finds it: the open fails, to be retried,
    // rather than taking the rescan.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    for file in std::fs::read_dir(&lake.watermark).unwrap() {
        let path = file.unwrap().path();
        if path.extension().is_some_and(|e| e == "parquet") {
            std::fs::remove_file(path).unwrap();
        }
    }

    let Err(e) = Pipeline::open(cfg).await else {
        panic!("a watermark table that could not be read must not fall back to the rescan");
    };
    assert!(
        e.to_string().contains("cannot read watermark table"),
        "got: {e}"
    );
}

#[tokio::test]
async fn a_watermark_an_earlier_rebuild_recorded_does_not_stand_for_this_one() {
    // The table only grows, so its newest row can be last night's: here the post-hook has not
    // run yet when ddi reopens. Taken for this rebuild's, it appended again every row since
    // last night on top of a rebuild that already held them, and ddi's next commit hid the
    // rebuild, so the row the post-hook then wrote was never read.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await; // versions 1..=3
    }
    p.run_until_caught_up().await.unwrap();

    // Night 1: dbt rebuilds from source version 2 and records it.
    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    append(&lake.f.source, &[4]).await; // version 4
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 5..=6 {
        append(&lake.f.source, &[i]).await; // versions 5, 6
    }
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 4, 5, 6]);

    // Night 2: dbt rebuilds from source version 6, and ddi reopens before the post-hook
    // records it. The newest row is night 1's.
    dbt_rebuild(&lake.f.target, &[1, 2, 3, 4, 5, 6]).await;
    append(&lake.f.source, &[7]).await; // version 7
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    let got = read_ids(&lake.f.target).await;
    assert_eq!(
        got,
        vec![1, 2, 3, 4, 5, 6, 7],
        "the rescan's cut-off, not night 1's watermark: 3 to 6 are not appended again"
    );

    // The post-hook lands late. Night 3 records its version before reading it, as a pre-hook
    // does, and ddi streams 9 while dbt runs: that row counts, and 9 is re-streamed.
    record_watermark(&lake.watermark, &cfg.app_id, 6).await;
    append(&lake.f.source, &[8]).await; // version 8
    p.run_until_caught_up().await.unwrap();
    record_watermark(&lake.watermark, &cfg.app_id, 8).await;
    append(&lake.f.source, &[9]).await; // version 9
    p.run_until_caught_up().await.unwrap();
    dbt_rebuild(&lake.f.target, &[1, 2, 3, 4, 5, 6, 7, 8]).await;

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None, "resumed from night 3's watermark");
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9]
    );
}

#[tokio::test]
async fn a_delete_after_a_handover_is_not_undone_by_the_watermark_before_it() {
    // Another writer's DELETE on the target reads like a rebuild, and records no watermark.
    // The newest row is then the last rebuild's, and resuming from it appended again every
    // row since, the deleted one included.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 4..=6 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 4, 5, 6]);

    // A GDPR delete, and a restart before ddi commits again.
    let (_t, m) = open(&lake.f.target)
        .await
        .delete()
        .with_predicate("id = 4")
        .await
        .unwrap();
    assert!(m.num_deleted_rows.unwrap_or(0) > 0, "nothing was deleted");
    let mut p = Pipeline::open(cfg).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 5, 6],
        "4 stays deleted, and nothing is written twice"
    );

    append(&lake.f.source, &[7]).await;
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 5, 6, 7]);
}

// ------------------------------------------------------- zero-cooperation dedup

/// The mode where the rebuilding writer knows nothing about `ddi`: no watermark table,
/// no hooks. `ddi` reads how far the target already reaches and emits only what lies
/// beyond it.
///
/// These fixtures have no timestamp column, so `id` plays that role — the mechanism only
/// needs a column that increases with arrival order, whatever its type.
fn cfg_with_dedup_key(f: &Fixture, name: &str) -> ResolvedPipeline {
    let mut c = f.cfg(name);
    c.dedup_timestamp = Some("id".into());
    c.dedup_key = Some("id".into());
    c
}

#[tokio::test]
async fn a_dedup_key_needs_no_cooperation_from_the_rebuilding_writer() {
    let f = Fixture::new().await;
    for i in 1..=3 {
        append(&f.source, &[i]).await;
    }

    let cfg = cfg_with_dedup_key(&f, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3]);

    // A vanilla rebuild: it overwrites the target and records nothing anywhere. It read
    // the source as of version 2, so row 3 -- which ddi had already streamed -- is gone.
    dbt_rebuild(&f.target, &[1, 2]).await;

    append(&f.source, &[4]).await;
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    let got = read_ids(&f.target).await;
    assert_eq!(
        got,
        vec![1, 2, 3, 4],
        "row 3 recovered from the rescan, row 4 streamed normally"
    );
    assert!(
        !has_duplicates(&got),
        "and 1 and 2 not written twice: {got:?}"
    );
}

#[tokio::test]
async fn a_rebuild_that_overtook_the_stream_does_not_duplicate_its_rows() {
    // The other direction: dbt is *ahead* of ddi. Everything ddi has yet to read is
    // already in the target, so a rescan must emit nothing at all.
    let f = Fixture::new().await;
    for i in 1..=2 {
        append(&f.source, &[i]).await;
    }

    let cfg = cfg_with_dedup_key(&f, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    // More source arrives, and dbt rebuilds covering all of it before ddi gets there.
    for i in 3..=5 {
        append(&f.source, &[i]).await;
    }
    dbt_rebuild(&f.target, &[1, 2, 3, 4, 5]).await;

    let n = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    let got = read_ids(&f.target).await;
    assert_eq!(got, vec![1, 2, 3, 4, 5], "no key reprocessed: {got:?}");
    assert!(!has_duplicates(&got));
    assert_eq!(
        n, 0,
        "the rescan is bounded by the source's file statistics, so there was nothing left \
         to read at all — not merely nothing left to emit"
    );
}

#[tokio::test]
async fn an_empty_target_after_a_rebuild_streams_everything() {
    // max(key) over an empty table is NULL, which must mean "nothing covered", not
    // "everything covered".
    let f = Fixture::new().await;
    for i in 1..=3 {
        append(&f.source, &[i]).await;
    }
    let cfg = cfg_with_dedup_key(&f, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&f.target, &[]).await; // a rebuild that produced no rows

    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3]);
}
