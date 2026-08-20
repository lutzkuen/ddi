//! Realtime publication, end to end against a real HTTP server.
//!
//! The properties under test are all about *ordering and isolation* rather than about
//! transport: a payload is only ever sent after its Delta commit is durable, and no
//! behaviour of the far end — refusing, hanging, or never being reachable at all — can
//! change what the pipeline committed or where its offset ended up.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{append, create_table, open, pipeline_cfg, read_ids, Fixture};
use delta_delta_ingest::config::{PublishModel, PublisherConfig, PublisherKind, ResolvedPipeline};
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use delta_delta_ingest::publish::Envelope;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// What the far end should do with a request.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HubBehaviour {
    /// The documented success: 202 Accepted.
    Accept,
    /// A server-side failure.
    Fail,
    /// Accept the connection and never answer, so the client's timeout is what ends it.
    Hang,
}

/// One captured request: its start line, and its body.
type Captured = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// A stand-in for the Web PubSub data plane.
///
/// Reads the request to its `Content-Length` in a loop rather than in a single read: a body
/// can arrive split across packets, and the envelope assertions are the point of this file,
/// so a truncated read would make them silently weak instead of failing.
struct FakeHub {
    pub addr: String,
    pub requests: Captured,
    pub hits: Arc<AtomicU64>,
}

impl FakeHub {
    async fn start(behaviour: HubBehaviour) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let hits = Arc::new(AtomicU64::new(0));

        let sink = requests.clone();
        let counter = hits.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let sink = sink.clone();
                let counter = counter.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];

                    // Headers first, so Content-Length is known.
                    let header_end = loop {
                        match socket.read(&mut chunk).await {
                            Ok(0) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            Err(_) => return,
                        }
                        if let Some(i) = find(&buf, b"\r\n\r\n") {
                            break i + 4;
                        }
                    };

                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let len: usize = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().ok())?
                        })
                        .unwrap_or(0);

                    // Then exactly as many body bytes as were promised.
                    while buf.len() - header_end < len {
                        match socket.read(&mut chunk).await {
                            Ok(0) => break,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            Err(_) => break,
                        }
                    }

                    counter.fetch_add(1, Ordering::SeqCst);
                    let target = head.lines().next().unwrap_or_default().to_string();
                    sink.lock()
                        .unwrap()
                        .push((target, buf[header_end..].to_vec()));

                    match behaviour {
                        HubBehaviour::Accept => {
                            let _ = socket
                                .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
                                .await;
                        }
                        HubBehaviour::Fail => {
                            let _ = socket
                                .write_all(
                                    b"HTTP/1.1 500 Internal Server Error\r\n\
                                      Content-Length: 11\r\n\r\nhub is down",
                                )
                                .await;
                        }
                        // Never answers. The publisher's own timeout has to end this.
                        HubBehaviour::Hang => {
                            tokio::time::sleep(Duration::from_secs(120)).await;
                        }
                    }
                });
            }
        });

        Self {
            addr,
            requests,
            hits,
        }
    }

    fn envelopes(&self) -> Vec<Envelope> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(_, body)| {
                serde_json::from_slice(body).unwrap_or_else(|e| {
                    panic!(
                        "body was not a ddi envelope ({e}): {}",
                        String::from_utf8_lossy(body)
                    )
                })
            })
            .collect()
    }

    fn request_lines(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(line, _)| line.clone())
            .collect()
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// A pipeline that publishes one row per batch to `hub`.
fn publishing_cfg(f: &Fixture, name: &str, hub: &FakeHub, timeout_secs: u64) -> ResolvedPipeline {
    let mut cfg = f.cfg(name);
    cfg.publish = Some(PublishModel {
        model: format!("{name}_live"),
        kind: PublisherKind::Webpubsub,
        group: "sales".into(),
        publish_sql: "SELECT count(*) AS rows_delta, sum(id) AS id_delta FROM source".into(),
    });
    cfg.publish_to = Some(PublisherConfig {
        kind: PublisherKind::Webpubsub,
        connection_string: Some(format!("Endpoint={};AccessKey=test-key", hub.addr)),
        connection_string_env: None,
        hub: "ddi".into(),
        message_ttl_secs: 60,
        timeout_secs,
        failure_threshold: 100, // effectively off, so each test drives one behaviour
        breaker_cooldown_secs: 1,
        max_message_bytes: "900KB".into(),
    });
    cfg
}

// ---------------------------------------------------------------------------
// AC1 — an append-only pipeline can opt in.

#[tokio::test]
async fn an_append_pipeline_publishes_each_committed_batch() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let mut p = Pipeline::open(publishing_cfg(&f, "copy", &hub, 5))
        .await
        .unwrap();
    p.run_until_caught_up().await.unwrap();

    assert_eq!(
        read_ids(&f.target).await,
        vec![1, 2, 3],
        "and it still streams"
    );

    let envelopes = hub.envelopes();
    assert_eq!(envelopes.len(), 1, "one message per commit");
    let e = &envelopes[0];
    assert_eq!(e.ddi, 1);
    assert_eq!(e.pipeline, "copy");
    assert_eq!(e.group, "sales");
    assert!(e.complete);
    assert_eq!(e.row_count, 1, "the aggregate is one row: {:?}", e.rows);
    assert_eq!(e.rows[0]["rows_delta"], 3, "a delta for this batch alone");
    assert_eq!(e.rows[0]["id_delta"], 6);
}

// AC5 — the request is the one the data-plane spec describes.

#[tokio::test]
async fn the_request_targets_the_documented_send_endpoint() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let mut p = Pipeline::open(publishing_cfg(&f, "copy", &hub, 5))
        .await
        .unwrap();
    p.run_until_caught_up().await.unwrap();

    let line = &hub.request_lines()[0];
    assert!(line.starts_with("POST "), "got: {line}");
    assert!(
        line.contains("/api/hubs/ddi/groups/sales/:send"),
        "got: {line}"
    );
    assert!(line.contains("api-version=2024-12-01"), "got: {line}");
    assert!(line.contains("messageTtlSeconds=60"), "got: {line}");
}

// ---------------------------------------------------------------------------
// AC3 — publication happens only after the commit succeeds.

#[tokio::test]
async fn nothing_is_published_when_the_target_commit_fails() {
    // The target is deleted out from under the pipeline after it opens, so the commit
    // fails while everything before it — read, transform, coerce, and the render that
    // builds the payload — has already succeeded. If publication were not gated on the
    // commit, this is the case that would push a payload for rows that do not exist.
    let f = Fixture::new().await;
    append(&f.source, &[1, 2]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let mut p = Pipeline::open(publishing_cfg(&f, "copy", &hub, 5))
        .await
        .unwrap();

    std::fs::remove_dir_all(&f.target).expect("the target is a local directory");

    let result = p.step().await;
    assert!(result.is_err(), "the commit cannot succeed: {result:?}");
    assert_eq!(
        hub.hits.load(Ordering::SeqCst),
        0,
        "a payload was published for a commit that never landed"
    );
}

#[tokio::test]
async fn the_published_target_version_is_the_one_the_commit_produced() {
    // Ordering stated positively: the version in the message is the version the commit
    // created, which is only knowable after it has landed.
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let mut p = Pipeline::open(publishing_cfg(&f, "copy", &hub, 5))
        .await
        .unwrap();
    let outcome = p.step().await.unwrap();

    let StepOutcome::Progressed {
        target_version,
        published,
        ..
    } = outcome
    else {
        panic!("expected a commit, got {outcome:?}");
    };
    assert!(published.expect("this pipeline publishes").sent);
    assert_eq!(hub.envelopes()[0].target_version, target_version);
    assert!(target_version.is_some(), "the commit produced a version");
}

// ---------------------------------------------------------------------------
// AC4 — the payload carries identity and enough cursor to detect a gap.

#[tokio::test]
async fn a_source_optimize_between_batches_is_not_a_gap() {
    // The correction this design turns on. An OPTIMIZE on the *source* is consumed and
    // skipped without producing a batch, so the next batch's source version is not the
    // previous one plus one. A client testing consecutive source versions would re-baseline
    // every time anyone compacted a bronze table — so what it actually tests is
    // `prev_source_version`, which is a fact about the publication sequence.
    let f = Fixture::new().await;
    // Two commits, so there are two files for OPTIMIZE to actually merge.
    append(&f.source, &[1]).await;
    append(&f.source, &[2]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let cfg = publishing_cfg(&f, "copy", &hub, 5);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();

    // Compact the source: commits that carry no data.
    let t = open(&f.source).await;
    let (_t, stats) = t.optimize().await.unwrap();
    assert!(
        stats.num_files_added > 0 || stats.num_files_removed > 0,
        "optimize was a no-op, so this test proves nothing"
    );

    // Drain them on their own. This is the case that matters: every commit in the scan is
    // skipped, so the reader advances its cursor and returns no batch at all — nothing
    // commits and nothing publishes, and the version chain moves anyway.
    p.run_until_caught_up().await.unwrap();

    append(&f.source, &[3]).await;
    p.run_until_caught_up().await.unwrap();

    let envelopes = hub.envelopes();
    assert_eq!(envelopes.len(), 2, "two data batches, two messages");

    let (first, second) = (&envelopes[0], &envelopes[1]);
    assert_eq!(
        second.prev_source_version,
        Some(first.source_version),
        "the publication chain is unbroken across the compaction"
    );
    assert!(
        second.from_source_version > first.source_version + 1,
        "and this is why a source-version test would have been wrong: {} follows {}",
        second.from_source_version,
        first.source_version
    );
}

#[tokio::test]
async fn the_first_message_of_a_process_reports_no_predecessor() {
    // A restart is a gap: whatever was in flight when the process died was not sent, so the
    // client must re-baseline rather than assume it can carry on.
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let mut p = Pipeline::open(publishing_cfg(&f, "copy", &hub, 5))
        .await
        .unwrap();
    p.run_until_caught_up().await.unwrap();

    assert_eq!(hub.envelopes()[0].prev_source_version, None);
}

#[tokio::test]
async fn a_fully_filtered_batch_still_publishes_so_the_chain_has_no_hole() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let mut cfg = publishing_cfg(&f, "copy", &hub, 5);
    // Filters the second commit out entirely, so it commits the offset with no rows.
    cfg.transform_sql = Some("SELECT id, name FROM source WHERE id < 2".into());
    let mut p = Pipeline::open(cfg).await.unwrap();
    p.run_until_caught_up().await.unwrap();

    // Drained first, or both commits would arrive in one batch: a batch accumulates whole
    // source commits up to its byte and file limits.
    append(&f.source, &[2]).await;
    p.run_until_caught_up().await.unwrap();

    let envelopes = hub.envelopes();
    assert_eq!(envelopes.len(), 2, "both commits publish: {envelopes:?}");
    assert_eq!(
        envelopes[1].prev_source_version,
        Some(envelopes[0].source_version),
        "including the one that produced nothing"
    );
}

// ---------------------------------------------------------------------------
// AC6 — a publisher failure cannot invalidate or roll back a commit.

#[tokio::test]
async fn a_hub_returning_500_does_not_stop_the_pipeline() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2]).await;

    let hub = FakeHub::start(HubBehaviour::Fail).await;
    let mut p = Pipeline::open(publishing_cfg(&f, "copy", &hub, 5))
        .await
        .unwrap();
    p.run_until_caught_up().await.unwrap();

    // A second batch, so this covers a failure that repeats rather than a single one.
    append(&f.source, &[3]).await;
    p.run_until_caught_up().await.unwrap();

    assert_eq!(
        read_ids(&f.target).await,
        vec![1, 2, 3],
        "every row committed even though every publish failed"
    );
    assert_eq!(
        hub.hits.load(Ordering::SeqCst),
        2,
        "and both were attempted"
    );
}

#[tokio::test]
async fn a_hub_that_never_answers_does_not_stall_the_pipeline_past_its_timeout() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let hub = FakeHub::start(HubBehaviour::Hang).await;
    let mut p = Pipeline::open(publishing_cfg(&f, "copy", &hub, 1))
        .await
        .unwrap();

    let started = std::time::Instant::now();
    p.run_until_caught_up().await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(
        read_ids(&f.target).await,
        vec![1],
        "the commit is unaffected"
    );
    assert!(
        elapsed < Duration::from_secs(30),
        "the publisher's own timeout must bound this, not the server: {elapsed:?}"
    );
}

#[tokio::test]
async fn an_unreachable_hub_does_not_stop_the_pipeline() {
    // Nothing is listening on this port: the connection is refused rather than answered.
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);

    let mut cfg = f.cfg("copy");
    cfg.publish = Some(PublishModel {
        model: "copy_live".into(),
        kind: PublisherKind::Webpubsub,
        group: "sales".into(),
        publish_sql: "SELECT count(*) AS rows_delta FROM source".into(),
    });
    cfg.publish_to = Some(PublisherConfig {
        kind: PublisherKind::Webpubsub,
        connection_string: Some(format!("Endpoint={addr};AccessKey=k")),
        connection_string_env: None,
        hub: "ddi".into(),
        message_ttl_secs: 60,
        timeout_secs: 5,
        failure_threshold: 5,
        breaker_cooldown_secs: 1,
        max_message_bytes: "900KB".into(),
    });

    let mut p = Pipeline::open(cfg).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1]);
}

// ---------------------------------------------------------------------------
// AC7 — publication can be disabled without affecting normal behaviour.

#[tokio::test]
async fn a_pipeline_with_no_publisher_behaves_exactly_as_before() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2]).await;

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    let outcome = p.step().await.unwrap();

    assert_eq!(read_ids(&f.target).await, vec![1, 2]);
    let StepOutcome::Progressed { published, .. } = outcome else {
        panic!("expected a commit, got {outcome:?}");
    };
    assert!(
        published.is_none(),
        "nothing to report when nothing publishes"
    );
}

// ---------------------------------------------------------------------------
// AC9 — an upsert pipeline never publishes.

#[tokio::test]
async fn an_upsert_pipeline_does_not_publish_even_when_asked_directly() {
    // Belt and braces: config resolution and the dbt gate both refuse this combination, so
    // this constructs it by hand to prove the runtime refuses it too. A merge replaces the
    // row stored under a key, and the committed batch does not contain the value it
    // replaced — so the batch alone cannot say what the dashboard delta was.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();
    create_table(&source).await;
    create_table(&target).await;
    append(&source, &[1]).await;

    let hub = FakeHub::start(HubBehaviour::Accept).await;
    let mut cfg = pipeline_cfg("merge", &source, &target);
    cfg.write_mode = delta_delta_ingest::config::WriteMode::Upsert;
    cfg.upsert_key = Some("id".into());
    cfg.dedup_timestamp = Some("id".into());
    cfg.publish = Some(PublishModel {
        model: "merge_live".into(),
        kind: PublisherKind::Webpubsub,
        group: "sales".into(),
        publish_sql: "SELECT count(*) AS rows_delta FROM source".into(),
    });
    cfg.publish_to = Some(PublisherConfig {
        kind: PublisherKind::Webpubsub,
        connection_string: Some(format!("Endpoint={};AccessKey=k", hub.addr)),
        connection_string_env: None,
        hub: "ddi".into(),
        message_ttl_secs: 60,
        timeout_secs: 5,
        failure_threshold: 5,
        breaker_cooldown_secs: 1,
        max_message_bytes: "900KB".into(),
    });

    let mut p = Pipeline::open(cfg).await.unwrap();
    p.run_until_caught_up().await.unwrap();

    assert_eq!(read_ids(&target).await, vec![1], "it merges as normal");
    assert_eq!(
        hub.hits.load(Ordering::SeqCst),
        0,
        "but publishes nothing at all"
    );
}
