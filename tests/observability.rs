//! Lag reporting, end to end.
//!
//! The unit tests in `metrics.rs` cover the arithmetic. These cover the *wiring*: a gauge
//! whose inputs are never written is worse than no gauge, because it reads as a healthy
//! zero forever. Each test drives a real `Pipeline` against real tables and feeds the
//! metrics exactly the way the daemon loop does.

mod common;

use std::sync::atomic::Ordering;

use common::*;
use delta_delta_ingest::metrics::Metrics;
use delta_delta_ingest::pipeline::Pipeline;

/// Mirror of what `drive()` in the binary records after every step.
fn record(m: &delta_delta_ingest::metrics::PipelineMetrics, p: &Pipeline) {
    if let Some(head) = p.source_head_version() {
        m.source_head_version.store(head as i64, Ordering::Relaxed);
    }
    m.cursor_version
        .store(p.cursor().version as i64, Ordering::Relaxed);
}

#[tokio::test]
async fn the_head_is_unknown_until_the_first_step() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let p = Pipeline::open(f.cfg("copy")).await.unwrap();
    assert_eq!(
        p.source_head_version(),
        None,
        "opening a pipeline must not claim to know the head; only a poll does"
    );
}

#[tokio::test]
async fn a_backlog_is_reported_as_lag() {
    let f = Fixture::new().await;
    for i in 1..=5 {
        append(&f.source, &[i]).await;
    }

    // One file per batch, so a single step consumes exactly one commit and leaves the rest.
    let mut cfg = f.cfg("copy");
    cfg.max_files_per_batch = 1;

    let metrics = Metrics::new();
    let m = metrics.pipeline("copy");

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(m.lag(), 0, "no lag is claimed before the first poll");

    p.step().await.unwrap();
    record(&m, &p);

    let head = p.source_head_version().expect("head known after a step");
    assert_eq!(head, 5, "five appends on top of the CREATE commit");
    assert_eq!(
        m.lag(),
        4,
        "consumed v1, so v2..=v5 are still outstanding (cursor={})",
        p.cursor()
    );
}

#[tokio::test]
async fn draining_the_source_takes_lag_to_zero() {
    let f = Fixture::new().await;
    for i in 1..=4 {
        append(&f.source, &[i]).await;
    }

    let metrics = Metrics::new();
    let m = metrics.pipeline("copy");

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    record(&m, &p);

    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3, 4]);
    assert_eq!(m.lag(), 0, "a drained source is zero lag");
}

#[tokio::test]
async fn compaction_on_the_source_does_not_read_as_backlog() {
    // The case that forced lag to be measured from the cursor rather than the durable
    // offset. An OPTIMIZE commit is consumed and skipped without writing a txn action, so
    // the offset legitimately stays behind the head. Measuring lag from the offset would
    // page an operator about a source that is in fact fully drained.
    use delta_delta_ingest::offset::OffsetStore;

    let f = Fixture::new().await;
    append(&f.source, &[1]).await;
    append(&f.source, &[2]).await;

    let cfg = f.cfg("copy");
    let metrics = Metrics::new();
    let m = metrics.pipeline("copy");

    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();

    // Compact the source. This adds commits that carry no new data.
    let t = open(&f.source).await;
    let (_t, stats) = t.optimize().await.unwrap();
    assert!(
        stats.num_files_added > 0 || stats.num_files_removed > 0,
        "optimize was a no-op, so this test proves nothing"
    );

    let mut p2 = Pipeline::open(cfg.clone()).await.unwrap();
    p2.run_until_caught_up().await.unwrap();
    record(&m, &p2);

    let head = p2.source_head_version().expect("head known after a step");
    let target = open(&f.target).await;
    let offset = OffsetStore::new(&cfg.app_id, 0)
        .last_committed_version(&target)
        .await
        .unwrap()
        .expect("data was committed, so an offset exists");

    assert!(
        offset < head,
        "precondition: the compaction commit must leave the durable offset \
         ({offset}) behind the head ({head})"
    );
    assert_eq!(
        m.lag(),
        0,
        "the source is fully consumed; compaction must not read as backlog"
    );
    assert_eq!(
        read_ids(&f.target).await,
        vec![1, 2],
        "and no rows replayed"
    );
}
