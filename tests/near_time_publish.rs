//! Near-time publication: reading the source directly, independent of the target commit.
//!
//! Complements `realtime_publish.rs`, which is entirely about the post-commit path. These
//! tests are about the trade this mode makes instead: it can publish before, or even
//! without, a target commit ever landing — and in exchange a restart has no durable state
//! of its own to resume from beyond the target's last *committed* offset, so it can resend
//! a window it had already published.

mod common;

use common::{append, FakeHub, Fixture, HubBehaviour};
use delta_delta_ingest::config::{PublishModel, PublisherConfig, PublisherKind, ResolvedPipeline};
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use delta_delta_ingest::publish::near_time::NearTimeReader;

/// A pipeline that publishes near-time to `hub`, from a plain `count`/`sum` over `id`.
fn near_time_cfg(f: &Fixture, name: &str, hub: &FakeHub) -> ResolvedPipeline {
    let mut cfg = f.cfg(name);
    cfg.publish = Some(PublishModel {
        model: format!("{name}_live"),
        kind: PublisherKind::Webpubsub,
        group: "sales".into(),
        publish_sql: "SELECT count(*) AS rows_delta, sum(id) AS id_delta FROM source".into(),
        near_time: true,
    });
    cfg.publish_to = Some(PublisherConfig {
        kind: PublisherKind::Webpubsub,
        connection_string: Some(format!("Endpoint={};AccessKey=test-key", hub.addr)),
        connection_string_env: None,
        hub: "ddi".into(),
        message_ttl_secs: 60,
        timeout_secs: 5,
        failure_threshold: 100, // effectively off, so a test drives one behaviour at a time
        breaker_cooldown_secs: 1,
        max_message_bytes: "900KB".into(),
    });
    cfg
}

#[tokio::test]
async fn near_time_disables_the_post_commit_publisher() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let mut p = Pipeline::open(near_time_cfg(&f, "copy", &hub))
        .await
        .unwrap();
    let outcome = p.step().await.unwrap();

    let StepOutcome::Progressed {
        published, rows, ..
    } = outcome
    else {
        panic!("expected a commit, got {outcome:?}");
    };
    assert_eq!(rows, 3, "the commit itself is unaffected by near_time");
    assert!(
        published.is_none(),
        "a near_time pipeline publishes from its own reader, not after this commit"
    );
    assert_eq!(
        hub.hits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "and nothing was sent from the commit path"
    );
}

#[tokio::test]
async fn near_time_publishes_from_the_source_without_a_target_commit() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let cfg = near_time_cfg(&f, "copy", &hub);

    // The main pipeline never runs in this test: the target stays empty and uncommitted
    // throughout, and the reader still publishes.
    let mut reader = NearTimeReader::open(&cfg)
        .await
        .unwrap()
        .expect("near_time is configured");
    let n = reader.run_until_caught_up().await.unwrap();
    assert!(n > 0, "at least one batch was polled");

    let envelopes = hub.delivered();
    assert_eq!(envelopes.len(), 1, "one source commit, one message");
    let e = &envelopes[0];
    assert!(
        e.target_version.is_none(),
        "there is no commit yet to attach a version to"
    );
    assert_eq!(e.row_count, 1, "one aggregate row");
    let row = &e.rows.as_array().unwrap()[0];
    assert_eq!(row["rows_delta"], 3);
    assert_eq!(row["id_delta"], 6);
}

#[tokio::test]
async fn a_restart_may_resend_a_window_already_published() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let cfg = near_time_cfg(&f, "copy", &hub);

    // First reader: publishes the delta for ids 1 and 2. The target is still empty, so the
    // durable offset a fresh reader would resume from has not moved past where this one
    // itself started.
    let mut first = NearTimeReader::open(&cfg).await.unwrap().unwrap();
    first.run_until_caught_up().await.unwrap();
    assert_eq!(hub.delivered().len(), 1);

    append(&f.source, &[3, 4]).await;

    // Second reader, opened fresh rather than continuing the first: it re-derives its
    // starting cursor from that same, still-unmoved target offset, so on its way to ids 3
    // and 4 it re-reads and republishes ids 1 and 2 as well.
    let mut second = NearTimeReader::open(&cfg).await.unwrap().unwrap();
    second.run_until_caught_up().await.unwrap();

    let delivered = hub.delivered();
    assert_eq!(
        delivered.len(),
        2,
        "the first reader's message plus the second reader's, which covers the same ground \
         again: {delivered:?}"
    );
    let total: i64 = delivered
        .iter()
        .map(|e| e.rows.as_array().unwrap()[0]["id_delta"].as_i64().unwrap())
        .sum();
    assert_eq!(
        total, 13,
        "1 + 2 counted twice (once per reader) plus 3 + 4 once: {delivered:?}"
    );
}
