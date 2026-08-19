//! A batch sized by what it will cost, not by what it is called.
//!
//! Its own test binary on purpose: [`Budget::install`] is process-wide, once, because it is
//! read on the hot path and a per-pipeline handle threaded through everything would be a
//! lot of plumbing for a number that never changes. That makes it awkward to test alongside
//! anything that expects the default, so it does not have to be.

mod common;

use common::*;
use delta_delta_ingest::budget::Budget;
use delta_delta_ingest::pipeline::Pipeline;

/// Small enough that a single 4-row file's decoded Arrow overruns it, so the stream is
/// forced down to one commit per batch and the floor below that is what is on trial.
const TINY: u64 = 64 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn a_budget_splits_a_batch_that_would_otherwise_arrive_whole() {
    // `max_bytes_per_batch` counts the compressed bytes the log recorded, and six little
    // files fit inside the default with room to spare — so without a budget this is one
    // batch. That is the shape that kills the process at scale: a cold pipeline filling
    // 256 MB of *compressed* budget with files that decode to five or six times that.
    let f = Fixture::new().await;
    for i in 1..=6 {
        append(&f.source, &[i]).await;
    }

    Budget::resolve(Some(TINY), 1).install();

    let mut p = Pipeline::open(f.cfg("split")).await.unwrap();
    let batches = p.run_until_caught_up().await.unwrap();

    assert!(
        batches > 1,
        "a budget must stop a batch accumulating, but it took {batches}"
    );
    assert_eq!(
        read_ids(&f.target).await,
        (1..=6).collect::<Vec<i64>>(),
        "and every row must still arrive — splitting is backpressure, not loss"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_budget_smaller_than_one_commit_still_makes_progress() {
    // The floor, and the reason the ceiling is only applied once a batch is carrying
    // something. A commit that fits `max_bytes_per_batch` has always been delivered, and a
    // memory budget must not turn a pipeline that worked yesterday into one that errors
    // today — a stalled pipeline is a worse answer than a large batch.
    //
    // The budget above is already far below a single commit's decoded size, so this is that
    // case: it must still drain.
    let f = Fixture::new().await;
    for i in 1..=3 {
        append(&f.source, &[i]).await;
    }

    // Whichever test ran first installed it; `install` is idempotent by design.
    Budget::resolve(Some(TINY), 1).install();

    let mut p = Pipeline::open(f.cfg("floor")).await.unwrap();
    p.run_until_caught_up()
        .await
        .expect("a budget below one commit must throttle, never refuse");

    assert_eq!(read_ids(&f.target).await, (1..=3).collect::<Vec<i64>>());
}
