//! Plan Milestone 4 — restart correctness.
//!
//! "If restart-idempotency is not provable by a test that kills the process mid-batch,
//! the project has no reason to exist."

mod common;

use common::*;
use delta_delta_ingest::offset::OffsetStore;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};

#[tokio::test]
async fn a_full_run_copies_every_row_exactly_once() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2]).await;
    append(&f.source, &[3, 4]).await;
    append(&f.source, &[5]).await;

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    p.run_until_caught_up().await.unwrap();

    let got = read_ids(&f.target).await;
    assert_eq!(got, vec![1, 2, 3, 4, 5]);
    assert!(!has_duplicates(&got));
}

#[tokio::test]
async fn re_running_a_caught_up_pipeline_writes_nothing() {
    // The definition of idempotent: the second run must be a no-op.
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    let after_first = read_ids(&f.target).await;
    let version_after_first = open(&f.target).await.version();

    // A brand-new Pipeline, as if the process had restarted.
    let mut p2 = Pipeline::open(f.cfg("copy")).await.unwrap();
    let n = p2.run_until_caught_up().await.unwrap();

    assert_eq!(n, 0, "a caught-up restart must commit nothing");
    assert_eq!(read_ids(&f.target).await, after_first, "no rows changed");
    assert_eq!(
        open(&f.target).await.version(),
        version_after_first,
        "no new target commit"
    );
}

#[tokio::test]
async fn restarting_mid_stream_produces_no_duplicates_and_no_gaps() {
    let f = Fixture::new().await;
    for i in 1..=6 {
        append(&f.source, &[i]).await;
    }

    // Process 1: a single step, then "crash" (drop the pipeline).
    {
        let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
        let out = p.step().await.unwrap();
        assert!(matches!(out, StepOutcome::Progressed { .. }));
    }

    // Process 2: fresh Pipeline reads its resume point from the target's txn action.
    let mut p2 = Pipeline::open(f.cfg("copy")).await.unwrap();
    p2.run_until_caught_up().await.unwrap();

    let got = read_ids(&f.target).await;
    assert_eq!(got, vec![1, 2, 3, 4, 5, 6], "no gaps");
    assert!(!has_duplicates(&got), "no duplicates: {got:?}");
}

#[tokio::test]
async fn many_interleaved_restarts_still_yield_each_row_once() {
    let f = Fixture::new().await;
    for i in 1..=10 {
        append(&f.source, &[i]).await;
    }

    // Restart before every single step — the most hostile schedule that still makes
    // progress.
    loop {
        let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
        match p.step().await.unwrap() {
            StepOutcome::CaughtUp => break,
            _ => continue,
        }
    }

    let got = read_ids(&f.target).await;
    assert_eq!(got, (1..=10).collect::<Vec<i64>>());
    assert!(!has_duplicates(&got));
}

#[tokio::test]
async fn the_offset_lives_in_the_target_and_nowhere_else() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2]).await;

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    p.run_until_caught_up().await.unwrap();

    // Read the offset back the way any Delta engine would: from the txn action.
    let target = open(&f.target).await;
    let store = OffsetStore::new("ddi.test.copy", 0);
    let stored = store.last_committed_version(&target).await.unwrap();
    assert_eq!(
        stored,
        Some(open(&f.source).await.version().unwrap()),
        "the target must record the last consumed source version"
    );

    // And there must be no side-car state anywhere in the target directory.
    let stray: Vec<_> = std::fs::read_dir(&f.target)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.contains("checkpoint_state") || n.contains(".offset") || n == "_ddi")
        .collect();
    assert!(stray.is_empty(), "found side-car state: {stray:?}");
}

#[tokio::test]
async fn a_different_app_id_replays_from_the_start() {
    // app_id is the offset key; changing it is a full replay, by design.
    let f = Fixture::new().await;
    append(&f.source, &[1, 2]).await;

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1, 2]);

    let mut other = f.cfg("copy");
    other.app_id = "ddi.test.someone-else".into();
    let mut p2 = Pipeline::open(other).await.unwrap();
    p2.run_until_caught_up().await.unwrap();

    let got = read_ids(&f.target).await;
    assert_eq!(
        got,
        vec![1, 1, 2, 2],
        "a new app_id replays everything: {got:?}"
    );
}

#[tokio::test]
async fn two_pipelines_fan_out_from_one_source_and_resume_independently() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let t1 = root.join("t1").to_str().unwrap().to_string();
    let t2 = root.join("t2").to_str().unwrap().to_string();
    create_table(&source).await;
    create_table(&t1).await;
    create_table(&t2).await;

    append(&source, &[1, 2]).await;

    // Only the first target catches up.
    let mut a = Pipeline::open(pipeline_cfg("a", &source, &t1))
        .await
        .unwrap();
    a.run_until_caught_up().await.unwrap();
    append(&source, &[3]).await;
    a.run_until_caught_up().await.unwrap();

    // The second starts cold and must still get everything.
    let mut b = Pipeline::open(pipeline_cfg("b", &source, &t2))
        .await
        .unwrap();
    b.run_until_caught_up().await.unwrap();

    assert_eq!(read_ids(&t1).await, vec![1, 2, 3]);
    assert_eq!(read_ids(&t2).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn a_transform_that_filters_everything_still_advances_the_offset() {
    // Otherwise a fully-filtered commit would be re-read forever.
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;

    let mut cfg = f.cfg("filtered");
    cfg.transform_sql = Some("SELECT id, name FROM source WHERE id > 100".into());

    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();

    assert_eq!(
        read_ids(&f.target).await,
        Vec::<i64>::new(),
        "nothing matched"
    );

    let target = open(&f.target).await;
    let stored = OffsetStore::new(&cfg.app_id, 0)
        .last_committed_version(&target)
        .await
        .unwrap();
    assert_eq!(
        stored,
        Some(open(&f.source).await.version().unwrap()),
        "the offset must advance even when no rows survive the filter"
    );

    // And a restart must not reprocess.
    let mut p2 = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p2.run_until_caught_up().await.unwrap(), 0);
}

#[tokio::test]
async fn a_sql_transform_is_applied_and_still_exactly_once() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3, 4]).await;

    let mut cfg = f.cfg("evens");
    cfg.transform_sql = Some("SELECT id, name FROM source WHERE id % 2 = 0".into());

    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&f.target).await, vec![2, 4]);

    let mut p2 = Pipeline::open(cfg).await.unwrap();
    p2.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&f.target).await,
        vec![2, 4],
        "restart changed nothing"
    );
}

#[tokio::test]
async fn optimize_on_the_source_does_not_replay_rows_downstream() {
    // End-to-end version of the compaction rule: the bug this catches is duplicated
    // data in the target after a routine OPTIMIZE.
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;
    append(&f.source, &[2]).await;

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1, 2]);

    let t = open(&f.source).await;
    let (_t, m) = t.optimize().await.unwrap();
    assert!(
        m.num_files_added > 0 || m.num_files_removed > 0,
        "optimize was a no-op"
    );

    let mut p2 = Pipeline::open(f.cfg("copy")).await.unwrap();
    p2.run_until_caught_up().await.unwrap();

    let got = read_ids(&f.target).await;
    assert_eq!(got, vec![1, 2], "OPTIMIZE replayed the table: {got:?}");
}
