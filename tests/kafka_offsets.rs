//! Where a Kafka pipeline starts and resumes: retention gaps, recreated topics, partitions
//! without offsets, fresh baselines and once-mode's end (docs/kafka-design.md §9.2, tests
//! 11-14, 19 and 28).
//!
//! Ignored unless a broker is there: see tests/kafka_common/mod.rs for how to run them.

#![cfg(feature = "kafka")]

mod kafka_common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use delta_delta_ingest::sink::{KafkaOffsets, Sink};
use deltalake::arrow::array::RecordBatch;
use rdkafka::producer::Producer as _;

use kafka_common::*;

async fn produce_to(f: &Fixture, id: u32, p: i32, n: i64) -> BTreeSet<(i32, i64)> {
    let producer = Producer::new();
    let mut out = BTreeSet::new();
    for i in 0..n {
        let o = producer
            .send(
                &f.topic,
                p,
                None,
                Some(&framed(EVENT_V1, id, event(i, None))),
            )
            .await;
        out.insert((p, o));
    }
    out
}

async fn run_to_end(toml: &str) {
    let mut p = Pipeline::open_with(resolve(toml, true), None)
        .await
        .unwrap();
    for _ in 0..600 {
        if p.step().await.unwrap() == StepOutcome::CaughtUp {
            return;
        }
    }
    panic!("once-mode did not finish");
}

/// What `ddi kafka set-offsets --partition p --next next --acknowledge-skip` writes (M5): a
/// txn-only commit with partition `p` at `next - 1`, and the reason.
async fn set_offset(f: &Fixture, p: i32, next: i64, reason: &str) {
    let table = open(&f.target).await;
    let schema = {
        use deltalake::delta_datafusion::DataFusionMixins;
        table.snapshot().unwrap().snapshot().read_schema()
    };
    let offsets = KafkaOffsets {
        topic: f.topic.clone(),
        group_id: "set-offsets".into(),
        ranges: BTreeMap::new(),
        assert: BTreeMap::from([(p, next - 1)]),
        marks: BTreeMap::new(),
        cleanup_logs: false,
        baseline: false,
        override_reason: Some(reason.to_string()),
        takeover_marker: false,
    };
    Sink::new(&f.app_id, 128 * 1024 * 1024)
        .commit_kafka(table, vec![RecordBatch::new_empty(schema)], &offsets)
        .await
        .unwrap();
}

/// Every commit's `commitInfo` in the target's log.
fn commit_infos(path: &str) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    let log = std::path::Path::new(path).join("_delta_log");
    let mut files: Vec<_> = std::fs::read_dir(log)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    for f in files {
        for line in std::fs::read_to_string(f).unwrap().lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(info) = v.get("commitInfo") {
                out.push(info.clone());
            }
        }
    }
    out
}

/// 11. Retention trims a partition past its stored offset while ddi is down: that partition
/// is blocked (`ddi_kafka_offsets_unavailable`) and the others go on. An acknowledged skip
/// unblocks it within blocked_recheck_secs, and the commit records why.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn seek_past_retention_blocks_partition() {
    let f = Fixture::new(3).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = BTreeSet::new();
    for p in 0..3 {
        produced.extend(produce_to(&f, id, p, 20).await);
    }
    let toml = f.toml("gap", "", "");
    run_to_end(&toml).await;
    for p in 0..3 {
        produced.extend(produce_to(&f, id, p, 20).await);
    }
    trim(&f.topic, 1, 30).await;
    // Offsets 20..29 of partition 1 are gone, never read.
    produced.retain(|(p, o)| !(*p == 1 && (20..30).contains(o)));

    let config = f.write("run.toml", &toml);
    let mut m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    eventually(Duration::from_secs(60), "partition 1 to block", || async {
        m.metric("ddi_kafka_offsets_unavailable").await == 1.0
            && m.metric("ddi_kafka_blocked_partitions").await == 1.0
    })
    .await;
    eventually(
        Duration::from_secs(60),
        "the other partitions to go on",
        || async {
            let rows = coordinates(&f.target).await;
            rows.iter().filter(|(p, _)| *p != 1).count() == 80
        },
    )
    .await;
    assert_eq!(
        coordinates(&f.target)
            .await
            .iter()
            .filter(|(p, _)| *p == 1)
            .count(),
        20,
        "the blocked partition read on"
    );

    set_offset(&f, 1, 30, "retention removed 20..29").await;
    eventually(Duration::from_secs(60), "partition 1 to resume", || async {
        coordinates(&f.target).await.len() == produced.len()
    })
    .await;
    assert_exactly(&coordinates(&f.target).await, &produced);
    eventually(Duration::from_secs(10), "the gauge to clear", || async {
        m.metric("ddi_kafka_offsets_unavailable").await == 0.0
    })
    .await;
    assert!(commit_infos(&f.target)
        .iter()
        .any(|c| c["ddi.kafka.offsetOverride"] == "retention removed 20..29"));
    m.kill().await;
}

/// 12. Retention trims a partition under a running member, past what it has fetched: the
/// buffered messages are committed, and only that partition blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn mid_fetch_retention_commits_buffer_then_blocks() {
    let f = Fixture::new(3).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write(
        "run.toml",
        &f.toml("midfetch", "", "allowed_latency_secs = 15"),
    );
    let m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    eventually(
        Duration::from_secs(60),
        "the member to own every partition",
        || async { m.metric("ddi_kafka_assigned_partitions").await == 3.0 },
    )
    .await;
    let mut others = BTreeSet::new();
    produce_to(&f, id, 0, 50).await;
    others.extend(produce_to(&f, id, 1, 10).await);
    eventually(Duration::from_secs(30), "the member to buffer", || async {
        m.metric("ddi_kafka_oldest_buffered_seconds").await >= 1.0
    })
    .await;
    // Frozen, it fetches nothing while partition 0 grows and is trimmed past what it read.
    m.signal("STOP");
    produce_to(&f, id, 0, 300).await;
    trim(&f.topic, 0, 320).await;
    m.signal("CONT");
    others.extend(produce_to(&f, id, 2, 10).await);
    eventually(Duration::from_secs(60), "partition 0 to block", || async {
        m.metric("ddi_kafka_offsets_unavailable").await == 1.0
    })
    .await;
    eventually(
        Duration::from_secs(60),
        "the buffer and the others to commit",
        || async {
            let rows = coordinates(&f.target).await;
            rows.iter().filter(|(p, _)| *p != 0).count() == others.len()
                && rows.iter().filter(|(p, _)| *p == 0).count() >= 50
        },
    )
    .await;
    let rows = coordinates(&f.target).await;
    let p0: Vec<i64> = rows
        .iter()
        .filter(|(p, _)| *p == 0)
        .map(|(_, o)| *o)
        .collect();
    // A contiguous prefix: everything read before the trim, nothing at or past it.
    assert_eq!(p0, (0..p0.len() as i64).collect::<Vec<_>>());
    assert!(p0.len() < 320);
    let other_rows: BTreeSet<_> = rows.iter().copied().filter(|(p, _)| *p != 0).collect();
    assert_eq!(other_rows, others);
    let mut m = m;
    m.kill().await;
}

/// 13. The topic recreated with fewer messages than the stored offset: every partition is
/// regressed, and once-mode fails saying so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn topic_recreated_fails() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    for p in 0..2 {
        produce_to(&f, id, p, 20).await;
    }
    let toml = f.toml("recreated", "", "");
    run_to_end(&toml).await;
    delete_topic(&f.topic).await;
    create_topic(&f.topic, 2).await;
    for p in 0..2 {
        produce_to(&f, id, p, 5).await;
    }
    let config = f.write("once.toml", &toml);
    let (ok, out) = once(&config).await;
    assert!(!ok, "{out}");
    assert!(out.contains("the topic ends before it"), "{out}");
    assert_eq!(
        coordinates(&f.target).await.len(),
        40,
        "nothing more was written"
    );
}

/// 13. The topic recreated with fewer messages while a member reads it: the next offset it
/// fetches is past the new end, librdkafka stops fetching the partition (`auto.offset.reset =
/// error`), and the partition blocks as regressed rather than stalling unseen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn topic_recreated_smaller_while_running_blocks() {
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    produce_to(&f, id, 0, 40).await;
    // Little prefetch, so that the member is still reading the old topic when it goes.
    let settings = "max_messages_per_batch = 5\n[kafka.pipeline.settings]\n\
                    \"queued.min.messages\" = 1\n\"fetch.message.max.bytes\" = 100";
    let toml = f.toml("shrunk", "", settings);
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    while coordinates(&f.target).await.is_empty() {
        p.step().await.unwrap();
    }
    delete_topic(&f.topic).await;
    create_topic(&f.topic, 1).await;
    produce_to(&f, id, 0, 2).await;
    let mut outcome = None;
    for _ in 0..120 {
        match p.step().await {
            Ok(StepOutcome::CaughtUp) => {
                outcome = Some(Ok(()));
                break;
            }
            Ok(_) => {}
            Err(e) => {
                outcome = Some(Err(e));
                break;
            }
        }
    }
    match outcome {
        Some(Err(e)) => assert!(
            matches!(
                e,
                delta_delta_ingest::Error::KafkaOffsetsRegressed { partition: 0, .. }
            ),
            "{e}"
        ),
        other => panic!(
            "the run ended {other:?} with {} rows, the partition never blocked",
            coordinates(&f.target).await.len()
        ),
    }
}

/// 13. The topic recreated and refilled past the stored offset: the message there is not the
/// one ddi recorded (I19), and the partition blocks rather than reading on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn topic_recreated_with_more_data_detected() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    for p in 0..2 {
        produce_to(&f, id, p, 20).await;
    }
    let toml = f.toml("refilled", "", "");
    run_to_end(&toml).await;
    delete_topic(&f.topic).await;
    create_topic(&f.topic, 2).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    for p in 0..2 {
        produce_to(&f, id, p, 30).await;
    }
    let config = f.write("once.toml", &toml);
    let (ok, out) = once(&config).await;
    assert!(!ok, "{out}");
    assert!(
        out.contains("is not the one this pipeline read there"),
        "{out}"
    );
    assert_eq!(
        coordinates(&f.target).await.len(),
        40,
        "nothing more was written"
    );
}

/// 14. A partition added while running: the group rebalances and the new partition, which
/// has no offset in a table ddi has taken over, starts at exactly offset 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn new_partition_starts_at_zero() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = BTreeSet::new();
    for p in 0..2 {
        produced.extend(produce_to(&f, id, p, 10).await);
    }
    let config = f.write(
        "run.toml",
        &f.toml(
            "grows",
            "",
            "[kafka.pipeline.settings]\n\"topic.metadata.refresh.interval.ms\" = 1000",
        ),
    );
    let mut m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    eventually(Duration::from_secs(60), "the first rows", || async {
        coordinates(&f.target).await.len() == 20
    })
    .await;
    add_partitions(&f.topic, 3).await;
    produced.extend(produce_to(&f, id, 2, 15).await);
    eventually(
        Duration::from_secs(90),
        "the new partition's rows",
        || async { coordinates(&f.target).await.len() == produced.len() },
    )
    .await;
    assert_exactly(&coordinates(&f.target).await, &produced);
    m.kill().await;
}

/// 14. A partition without an offset, in a table ddi has taken over, whose messages were
/// trimmed: it cannot start at 0, and blocks rather than starting at the low watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn missing_txn_low_above_zero_blocks() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    produce_to(&f, id, 0, 10).await;
    let toml = f.toml("missing", "", "");
    run_to_end(&toml).await;
    assert_eq!(txns(&f.target, &f.app_id, 2).await[&1], None);
    produce_to(&f, id, 1, 10).await;
    trim(&f.topic, 1, 5).await;
    let config = f.write("once.toml", &toml);
    let (ok, out) = once(&config).await;
    assert!(!ok, "{out}");
    assert!(
        out.contains("the next offset to read is 0, and the topic now starts at 5"),
        "{out}"
    );
    assert!(coordinates(&f.target).await.iter().all(|(p, _)| *p == 0));
}

/// 19. A fresh app_id with `starting_offsets = "latest"` commits a baseline before it reads;
/// killed right after it, a restart resumes from the baseline, not from a new "latest".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn latest_baseline_survives_crash() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    for p in 0..2 {
        produce_to(&f, id, p, 10).await;
    }
    let config = f.write(
        "run.toml",
        &f.toml("latest", "", "starting_offsets = \"latest\""),
    );
    let mut m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    eventually(Duration::from_secs(60), "the baseline", || async {
        txns(&f.target, &f.app_id, 2)
            .await
            .values()
            .all(|t| *t == Some(9))
    })
    .await;
    m.kill().await;
    assert!(coordinates(&f.target).await.is_empty());
    let mut produced = BTreeSet::new();
    for p in 0..2 {
        produced.extend(produce_to(&f, id, p, 5).await);
    }
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
    assert!(commit_infos(&f.target)
        .iter()
        .any(|c| c["ddi.kafka.baseline"] == true));
}

/// 19. `starting_offsets = "earliest"` on a topic retention has trimmed: the trimmed
/// partition starts at its low watermark through a baseline; the untrimmed one at 0 with no
/// offset at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn earliest_on_trimmed_topic_baseline() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = BTreeSet::new();
    for p in 0..2 {
        produced.extend(produce_to(&f, id, p, 20).await);
    }
    trim(&f.topic, 0, 7).await;
    produced.retain(|(p, o)| !(*p == 0 && *o < 7));
    let toml = f.toml("earliest", "", "starting_offsets = \"earliest\"");
    // Only the baseline, then nothing more, while the pipeline has not read anything yet.
    let p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    drop(p);
    let stored = txns(&f.target, &f.app_id, 2).await;
    assert_eq!(stored, BTreeMap::from([(0, Some(6)), (1, None)]));
    run_to_end(&toml).await;
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 28. Once-mode ends with every kind of partition: one with messages, an empty one, one
/// already caught up, and one whose last offset is a transaction marker it never sees (the
/// idle rule).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn once_finishes_with_idle_empty_and_caught_up_partitions() {
    let f = Fixture::new(4).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = produce_to(&f, id, 2, 5).await;
    let toml = f.toml("ends", "", "");
    run_to_end(&toml).await; // partition 2 caught up from here on
    produced.extend(produce_to(&f, id, 0, 12).await);
    // Partition 3's last offset is a commit marker.
    let tx = Producer::with(&[("transactional.id", &unique("tx"))]);
    tx.inner()
        .init_transactions(Duration::from_secs(30))
        .unwrap();
    tx.inner().begin_transaction().unwrap();
    for i in 0..4 {
        let o = tx
            .send(
                &f.topic,
                3,
                None,
                Some(&framed(EVENT_V1, id, event(i, None))),
            )
            .await;
        produced.insert((3, o));
    }
    tx.inner()
        .commit_transaction(Duration::from_secs(30))
        .unwrap();
    let started = std::time::Instant::now();
    let config = f.write("once.toml", &toml);
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    assert!(started.elapsed() < Duration::from_secs(60), "{out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 28. A commit that takes longer than once-mode's idle time does not end the run: idle is
/// counted only while a partition is polled, not while the member commits, holds a batch or
/// waits for a seek.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn once_outlasts_a_commit_slower_than_its_idle_time() {
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let produced = produce_to(&f, id, 0, 30).await;
    let config = f.write(
        "once.toml",
        &f.toml("slow", "", "max_messages_per_batch = 10"),
    );
    let (ok, out) = once_with(&config, &[("DDI_TEST_FAULT", "sleep_before_commit:11")]).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 28. Under read_committed once-mode ends at the last stable offset: an open transaction's
/// messages are not read, and neither is anything after them, until it commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn once_with_open_transaction() {
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let before = produce_to(&f, id, 0, 5).await;
    let tx = Producer::with(&[("transactional.id", &unique("tx"))]);
    tx.inner()
        .init_transactions(Duration::from_secs(30))
        .unwrap();
    tx.inner().begin_transaction().unwrap();
    let mut open_tx = BTreeSet::new();
    for i in 0..3 {
        let o = tx
            .send(
                &f.topic,
                0,
                None,
                Some(&framed(EVENT_V1, id, event(100 + i, None))),
            )
            .await;
        open_tx.insert((0, o));
    }
    let config = f.write("once.toml", &f.toml("lso", "", ""));
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &before);

    tx.inner()
        .commit_transaction(Duration::from_secs(30))
        .unwrap();
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    let all: BTreeSet<_> = before.union(&open_tx).copied().collect();
    assert_exactly(&coordinates(&f.target).await, &all);
}

/// 28. `ddi once` beside a running member of the same group assigns manually and never joins
/// the group: the member is not rebalanced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn once_does_not_rebalance_running_group() {
    let f = Fixture::new(3).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write("run.toml", &f.toml("beside", "", ""));
    let mut m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    eventually(
        Duration::from_secs(60),
        "the member to own every partition",
        || async { m.metric("ddi_kafka_assigned_partitions").await == 3.0 },
    )
    .await;
    let mut produced = BTreeSet::new();
    for p in 0..3 {
        produced.extend(produce_to(&f, id, p, 10).await);
    }
    let rebalances = m.metric("ddi_kafka_rebalances_total").await;
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(m.metric("ddi_kafka_rebalances_total").await, rebalances);
    assert_eq!(m.metric("ddi_kafka_assigned_partitions").await, 3.0);
    eventually(Duration::from_secs(30), "every message", || async {
        coordinates(&f.target).await.len() >= produced.len()
    })
    .await;
    assert_exactly(&coordinates(&f.target).await, &produced);
    m.kill().await;
}
