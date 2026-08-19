//! A source file that was vacuumed out from under the cursor.
//!
//! The incident: a pipeline is stopped — deliberately excluded, or just behind — while the
//! source commits, runs `OPTIMIZE`, and then `VACUUM`s the files the compaction retired.
//! The commit the pipeline still has to read survives in the log; the file it added does
//! not. There is no correct incremental batch left to build, because the replacement files
//! also hold rows from commits already consumed.
//!
//! What is pinned here is that this reads as *itself* — a typed error naming the relation,
//! the version and the file, and a gauge an operator can alert on — rather than as a
//! generic transform error retried until somebody reads the logs. And that nothing was
//! skipped on the way: the offset does not move and the target does not grow.

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use common::{append, open, pipeline_cfg, read_ids, Fixture};
use delta_delta_ingest::metrics::Metrics;
use delta_delta_ingest::offset::OffsetStore;
use delta_delta_ingest::pipeline::Pipeline;
use delta_delta_ingest::Error;

/// A clock far enough ahead that a tombstone written a moment ago is already past the
/// table's default seven-day `deletedFileRetentionDuration`.
///
/// Moving the clock rather than dropping the retention floor keeps `VACUUM`'s own safety
/// check switched on, so the test exercises the operation an operator actually runs.
#[derive(Debug)]
struct EightDaysLater;

impl deltalake::operations::vacuum::Clock for EightDaysLater {
    fn current_timestamp_millis(&self) -> i64 {
        chrono::Utc::now().timestamp_millis() + 8 * 86_400_000
    }
}

/// Every `add.path` recorded by commit `version`, read straight out of the commit JSON.
///
/// Read from the log rather than from a snapshot because the point is to name the file
/// *this commit* added, which is exactly what a later compaction takes out of the snapshot.
fn adds_of(table_path: &str, version: u64) -> Vec<String> {
    let commit = format!("{table_path}/_delta_log/{version:020}.json");
    std::fs::read_to_string(&commit)
        .unwrap()
        .lines()
        .filter_map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            v.get("add")
                .map(|a| a["path"].as_str().unwrap().to_string())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_vacuumed_source_file_is_named_rather_than_retried_as_a_transform_error() {
    let f = Fixture::new().await;

    // v1: consumed normally, so the pipeline has a durable offset to fall behind from.
    append(&f.source, &[1]).await;
    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1]);
    drop(p);

    // v2: committed while the pipeline is stopped. This is the batch it will resume on.
    append(&f.source, &[2]).await;
    let stranded = adds_of(&f.source, 2);
    assert_eq!(
        stranded.len(),
        1,
        "the fixture expects one Add at version 2"
    );

    // OPTIMIZE retires it with dataChange=false, so ddi classifies that commit as a
    // compaction and skips it — the pipeline still has to read the original file.
    let (_t, opt) = open(&f.source).await.optimize().await.unwrap();
    assert!(
        opt.num_files_removed > 0,
        "optimize retired nothing, so there is no tombstone to vacuum and this test proves \
         nothing"
    );

    // VACUUM then removes the object itself. Stricter than the suite's usual
    // `added > 0 || removed > 0` on purpose: the whole scenario is the physical deletion.
    let (_t, vac) = open(&f.source)
        .await
        .vacuum()
        .with_clock(Arc::new(EightDaysLater))
        .await
        .unwrap();
    assert!(
        vac.files_deleted.contains(&stranded[0]),
        "the file version 2 added must be physically gone: {:?}",
        vac.files_deleted
    );

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    assert_eq!(
        p.cursor().version,
        2,
        "precondition: the pipeline still resumes at the commit whose file was removed"
    );

    let e = p
        .run_until_caught_up()
        .await
        .expect_err("a source file the store no longer has must not read as success");

    match &e {
        Error::SourceFileVacuumed {
            source_uri,
            version,
            path,
        } => {
            assert_eq!(*version, 2, "the version that added the file, not the head");
            assert_eq!(path, &stranded[0], "the file that is actually missing");
            assert_eq!(source_uri, &f.source, "the relation an operator has to fix");
        }
        other => panic!("expected SourceFileVacuumed, got {other:?}"),
    }

    let msg = e.to_string();
    assert!(
        msg.contains("delta.deletedFileRetentionDuration"),
        "the message must name the setting that prevents a recurrence: {msg}"
    );
    assert!(
        msg.contains("Restore"),
        "and the recovery that does not require rebuilding anything: {msg}"
    );

    // Nothing was skipped past. The durable offset is the one that matters — the in-memory
    // cursor has already moved to the end of the batch that then failed to read.
    let offset = OffsetStore::new("ddi.test.copy", 0)
        .last_committed_version(&open(&f.target).await)
        .await
        .unwrap();
    assert_eq!(
        offset,
        Some(1),
        "the unreadable batch must not have advanced the offset"
    );
    assert_eq!(
        read_ids(&f.target).await,
        vec![1],
        "and must not have written anything"
    );

    // And the retry the supervisor performs — which reopens, deriving the cursor from that
    // offset again — lands on the same commit and says the same thing, rather than quietly
    // skipping it or degrading to a generic error. This is the "retries forever" loop from
    // the incident, now with a name attached to every pass through it.
    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    let again = p.run_until_caught_up().await.unwrap_err();
    assert!(
        matches!(again, Error::SourceFileVacuumed { version: 2, .. }),
        "a reopened pipeline must report the same thing, not step over the gap: {again}"
    );
    assert_eq!(
        read_ids(&f.target).await,
        vec![1],
        "and still must not have written anything"
    );
}

/// The gauge the supervisor drives, exercised through the same call the supervisor makes.
///
/// `drive()` lives in the binary, so the classification deliberately sits in
/// `PipelineMetrics::observe_error` where a test can reach it. Without this, the gauge
/// would be a field nothing ever writes, which reads as a healthy zero forever.
#[tokio::test]
async fn the_health_gauge_holds_until_a_step_succeeds() {
    let metrics = Metrics::new();
    let m = metrics.pipeline("copy");

    assert_eq!(
        m.source_file_vacuumed.load(Ordering::Relaxed),
        0,
        "a pipeline that has never failed is not blocked"
    );

    m.observe_error(&Error::SourceFileVacuumed {
        source_uri: "abfss://raw@acct.dfs.core.windows.net/orders".into(),
        version: 107,
        path: "ingest_date=2024-04-23/part-00000.snappy.parquet".into(),
    });
    assert_eq!(m.source_file_vacuumed.load(Ordering::Relaxed), 1);
    assert_eq!(m.up.load(Ordering::Relaxed), 0);
    assert_eq!(m.errors.load(Ordering::Relaxed), 1);

    let out = metrics.render();
    assert!(
        out.contains("# TYPE ddi_source_file_vacuumed gauge"),
        "{out}"
    );
    assert!(
        out.contains("ddi_source_file_vacuumed{pipeline=\"copy\"} 1"),
        "{out}"
    );

    // Most of an attempt runs before the read that finds the file missing — both tables are
    // reopened, the resume point resolved, the batch built — so a later attempt failing some
    // other way is not evidence the file came back. Dropping the gauge there would resolve
    // the alert, and reset any `for` clause on it, while the stream is still stuck.
    m.observe_error(&Error::Transform("the target timed out on reopen".into()));
    assert_eq!(
        m.source_file_vacuumed.load(Ordering::Relaxed),
        1,
        "an unrelated failure says nothing about the missing file, so the alert must hold"
    );

    m.mark_progress();
    assert_eq!(
        m.source_file_vacuumed.load(Ordering::Relaxed),
        0,
        "a step that succeeded read what it needed, so nothing is missing any more"
    );
}

/// A batch spanning several commits must blame the commit that added the file, not the
/// last one in the batch — an operator sent to the wrong version restores the wrong files.
#[tokio::test(flavor = "multi_thread")]
async fn the_error_names_the_commit_that_added_the_file_not_the_end_of_the_batch() {
    let f = Fixture::new().await;

    // Three commits, all unconsumed, all in one batch: the default limits are far above
    // three tiny files. The missing file is in the *middle* one, which is the only position
    // that discriminates — version 1 is also the batch's start and version 3 is also its
    // through_version, so stranding either would pass against an implementation that
    // reports one version per batch instead of one per file.
    append(&f.source, &[1]).await;
    append(&f.source, &[2]).await;
    append(&f.source, &[3]).await;
    let stranded = adds_of(&f.source, 2);
    assert_eq!(stranded.len(), 1);

    std::fs::remove_file(format!("{}/{}", f.source, stranded[0])).unwrap();

    let mut cfg = pipeline_cfg("copy", &f.source, &f.target);
    cfg.starting_version = 1;
    let mut p = Pipeline::open(cfg).await.unwrap();
    let e = p.run_until_caught_up().await.unwrap_err();

    assert_eq!(
        p.cursor().version,
        4,
        "precondition: all three commits were one batch, or this proves nothing about \
         which version a batch blames"
    );
    assert!(
        matches!(&e, Error::SourceFileVacuumed { version: 2, .. }),
        "the batch starts at version 1 and runs through version 3; version 2 is what added \
         the missing file: {e}"
    );
}
