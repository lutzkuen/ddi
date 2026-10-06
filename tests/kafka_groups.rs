//! Kafka pipelines in consumer groups: fencing between deployments, rebalances with their
//! revoke flush, churn, outages, a zombie, and the progress rule (docs/kafka-design.md §9.2,
//! tests 5-10 and 32).
//!
//! Ignored unless a broker is there: see tests/kafka_common/mod.rs for how to run them.

#![cfg(feature = "kafka")]

mod kafka_common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use delta_delta_ingest::metrics::Metrics;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use delta_delta_ingest::sink::{KafkaOffsets, Sink};
use deltalake::arrow::array::RecordBatch;

use kafka_common::*;

/// Produce one event to every partition in turn until `stop`, about `per_sec` a second; the
/// coordinates produced.
fn produce_until(
    f: &Arc<Fixture>,
    id: u32,
    per_sec: u64,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<BTreeSet<(i32, i64)>> {
    let f = f.clone();
    tokio::spawn(async move {
        let producer = Producer::new();
        let mut produced = BTreeSet::new();
        let mut n: i64 = 0;
        while !stop.load(Ordering::Relaxed) {
            let p = (n % i64::from(f.partitions)) as i32;
            let o = producer
                .send(
                    &f.topic,
                    p,
                    None,
                    Some(&framed(EVENT_V1, id, event(n, None))),
                )
                .await;
            produced.insert((p, o));
            n += 1;
            tokio::time::sleep(Duration::from_millis(1000 / per_sec.max(1))).await;
        }
        produced
    })
}

async fn produce_n(f: &Fixture, id: u32, n: i64) -> BTreeSet<(i32, i64)> {
    let producer = Producer::new();
    let mut produced = BTreeSet::new();
    for i in 0..n {
        let p = (i % i64::from(f.partitions)) as i32;
        let o = producer
            .send(
                &f.topic,
                p,
                None,
                Some(&framed(EVENT_V1, id, event(i, None))),
            )
            .await;
        produced.insert((p, o));
    }
    produced
}

/// Re-assert, as a second deployment of the same app_id would, every partition's offset at
/// the value the target holds: a txn-only commit that conflicts with any concurrent commit of
/// those partitions without moving them.
async fn reassert(f: &Fixture, partitions: &[i32]) -> bool {
    let current = txns(&f.target, &f.app_id, f.partitions).await;
    let assert: BTreeMap<i32, i64> = partitions
        .iter()
        .filter_map(|p| current[p].map(|o| (*p, o)))
        .collect();
    if assert.is_empty() {
        return false;
    }
    let table = open(&f.target).await;
    let schema = {
        use deltalake::delta_datafusion::DataFusionMixins;
        table.snapshot().unwrap().snapshot().read_schema()
    };
    let offsets = KafkaOffsets {
        topic: f.topic.clone(),
        group_id: "another-deployment".into(),
        ranges: BTreeMap::new(),
        assert,
        marks: BTreeMap::new(),
        cleanup_logs: false,
        baseline: false,
        override_reason: None,
        takeover_marker: false,
    };
    Sink::new(&f.app_id, 128 * 1024 * 1024)
        .commit_kafka(table, vec![RecordBatch::new_empty(schema)], &offsets)
        .await
        .is_ok()
}

/// 5. Two deployments with one app_id and different groups, stepped in turn over the same
/// partitions: every interleaved commit is fenced or lands, and no message lands twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn two_groups_same_app_id_never_duplicate() {
    let f = Fixture::new(3).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let produced = produce_n(&f, id, 300).await;
    let (ma, mb) = (
        Metrics::new().pipeline("a").kafka(),
        Metrics::new().pipeline("b").kafka(),
    );
    let toml = |group: &str| {
        f.toml("twins", "", "max_messages_per_batch = 25").replace(
            &format!("group_id = \"g-{}\"", f.app_id),
            &format!("group_id = \"{group}\""),
        )
    };
    let mut a = Pipeline::open_with(resolve(&toml("ga"), true), Some(ma.clone()))
        .await
        .unwrap();
    let mut b = Pipeline::open_with(resolve(&toml("gb"), true), Some(mb.clone()))
        .await
        .unwrap();
    let (mut a_done, mut b_done) = (false, false);
    for _ in 0..2000 {
        if !a_done {
            a_done = a.step().await.unwrap() == StepOutcome::CaughtUp;
        }
        if !b_done {
            b_done = b.step().await.unwrap() == StepOutcome::CaughtUp;
        }
        let rows = coordinates(&f.target).await;
        let distinct: BTreeSet<_> = rows.iter().copied().collect();
        assert_eq!(
            rows.len(),
            distinct.len(),
            "a duplicate between two deployments"
        );
        if a_done && b_done {
            break;
        }
    }
    assert!(a_done && b_done);
    assert_exactly(&coordinates(&f.target).await, &produced);
    let fenced = |m: &delta_delta_ingest::metrics::KafkaMetrics| {
        m.fenced_concurrent_txn.load(Ordering::Relaxed)
            + m.fenced_base_mismatch.load(Ordering::Relaxed)
            + m.fenced_other_conflict.load(Ordering::Relaxed)
    };
    assert!(
        fenced(&ma) + fenced(&mb) > 0,
        "two deployments never fenced each other"
    );
}

/// 5, variant. A foreign txn-only commit on one partition between our assign and our commit
/// makes the commit conflict; the reset finds that partition's offset unchanged, keeps the
/// batch and commits it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn a_foreign_commit_between_assign_and_commit_forces_a_reset() {
    let f = Fixture::new(4).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = produce_n(&f, id, 40).await;
    let toml = f.toml("foreign", "", "");
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    while p.step().await.unwrap() != StepOutcome::CaughtUp {}
    drop(p);
    produced.extend(produce_n(&f, id, 40).await);

    let metrics = Metrics::new().pipeline("foreign").kafka();
    let mut p = Pipeline::open_with(resolve(&toml, true), Some(metrics.clone()))
        .await
        .unwrap();
    assert!(reassert(&f, &[3]).await, "the foreign commit");
    while p.step().await.unwrap() != StepOutcome::CaughtUp {}
    assert!(metrics.fenced_concurrent_txn.load(Ordering::Relaxed) >= 1);
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 6. Two `ddi run` members in one group under continuous production: one stopped with
/// SIGTERM, the other SIGKILLed mid-batch, both restarted, the rest drained with `ddi once`:
/// every message exactly once, and the group rebalanced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn rebalance_two_consumers() {
    let f = Arc::new(Fixture::new(6).await);
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write(
        "run.toml",
        &f.toml("pair", "", "max_messages_per_batch = 50"),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let producing = produce_until(&f, id, 300, stop.clone());

    let log = |n: &str| f.dir.path().join(format!("{n}.log"));
    let mut a = Member::start(&config, &log("a1"), &[]);
    let mut b = Member::start(&config, &log("b1"), &[]);
    eventually(
        Duration::from_secs(60),
        "both members to own partitions",
        || async {
            a.metric("ddi_kafka_assigned_partitions").await > 0.0
                && b.metric("ddi_kafka_assigned_partitions").await > 0.0
        },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let rebalances =
        a.metric("ddi_kafka_rebalances_total").await + b.metric("ddi_kafka_rebalances_total").await;

    a.sigterm();
    assert!(
        a.exited(Duration::from_secs(60)).await,
        "SIGTERM did not stop a member:\n{}",
        a.log_text()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    b.kill().await;
    let mut a = Member::start(&config, &log("a2"), &[]);
    let mut b = Member::start(&config, &log("b2"), &[]);
    tokio::time::sleep(Duration::from_secs(15)).await;
    stop.store(true, Ordering::Relaxed);
    let produced = producing.await.unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    a.kill().await;
    b.kill().await;

    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
    assert!(rebalances > 0.0);
}

/// 7. A member holding a buffer (a long latency) when a second joins: the revoke commits the
/// buffer before the partitions are given up, and nothing is read twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn revoke_flushes_before_unassign() {
    let f = Fixture::new(4).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write(
        "run.toml",
        &f.toml("flush", "", "allowed_latency_secs = 120"),
    );
    let log = |n: &str| f.dir.path().join(format!("{n}.log"));
    let mut a = Member::start(&config, &log("a"), &[]);
    eventually(
        Duration::from_secs(60),
        "the member to own every partition",
        || async { a.metric("ddi_kafka_assigned_partitions").await == 4.0 },
    )
    .await;
    let produced = produce_n(&f, id, 200).await;
    eventually(Duration::from_secs(30), "the member to buffer", || async {
        a.metric("ddi_kafka_oldest_buffered_seconds").await >= 1.0
    })
    .await;
    assert!(
        coordinates(&f.target).await.is_empty(),
        "committed before the latency"
    );

    let mut b = Member::start(&config, &log("b"), &[]);
    eventually(
        Duration::from_secs(60),
        "the revoke to commit the buffer",
        || async {
            a.metric("ddi_kafka_revoke_flushes_total{pipeline=\"flush\",outcome=\"committed\"}")
                .await
                >= 1.0
        },
    )
    .await;
    assert_exactly(&coordinates(&f.target).await, &produced);
    a.kill().await;
    b.kill().await;
}

/// 7, variant. A revoke flush that outlasts revoke_flush_timeout_secs (a commit stalled by a
/// fault) is dropped; the next owner reads it from the target's offsets, and nothing is
/// lost or read twice.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn revoke_flush_timeout_drops() {
    let f = Fixture::new(4).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write(
        "run.toml",
        &f.toml(
            "stall",
            "",
            "allowed_latency_secs = 120\nrevoke_flush_timeout_secs = 5",
        ),
    );
    let log = |n: &str| f.dir.path().join(format!("{n}.log"));
    let mut a = Member::start(&config, &log("a"), &[("DDI_TEST_FAULT", "stall_commit:20")]);
    eventually(
        Duration::from_secs(60),
        "the member to own every partition",
        || async { a.metric("ddi_kafka_assigned_partitions").await == 4.0 },
    )
    .await;
    let produced = produce_n(&f, id, 100).await;
    eventually(Duration::from_secs(30), "the member to buffer", || async {
        a.metric("ddi_kafka_oldest_buffered_seconds").await >= 1.0
    })
    .await;
    let mut b = Member::start(&config, &log("b"), &[]);
    eventually(Duration::from_secs(60), "the flush to time out", || async {
        a.metric("ddi_kafka_revoke_flushes_total{pipeline=\"stall\",outcome=\"timeout\"}")
            .await
            >= 1.0
    })
    .await;
    a.kill().await;
    b.kill().await;
    let drain = f.write("drain.toml", &f.toml("stall", "", ""));
    let (ok, out) = once(&drain).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 8. Three members with a 30 s latency while a fourth restarts every 10 s for three minutes:
/// every one of the three keeps committing, at least once a minute (I20).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn liveness_under_churn() {
    let f = Arc::new(Fixture::new(6).await);
    let id = register(&f.subject(), EVENT_V1).await;
    // A restart is a graceful one, as a rolling deploy's: the member leaves its group. A
    // member killed outright would hold every rebalance until its session timed out.
    let config = f.write(
        "run.toml",
        &f.toml(
            "churn",
            "",
            "allowed_latency_secs = 30\n[kafka.pipeline.settings]\n\
             \"session.timeout.ms\" = 10000\n\"heartbeat.interval.ms\" = 1000",
        ),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let producing = produce_until(&f, id, 100, stop.clone());
    let log = |n: String| f.dir.path().join(format!("{n}.log"));
    let mut members: Vec<Member> = (0..3)
        .map(|i| Member::start(&config, &log(format!("m{i}")), &[]))
        .collect();
    eventually(
        Duration::from_secs(60),
        "every member to own partitions",
        || async {
            let mut all = true;
            for m in &members {
                all &= m.metric("ddi_kafka_assigned_partitions").await > 0.0;
            }
            all
        },
    )
    .await;

    let started = Instant::now();
    let mut churner: Option<Member> = None;
    let mut last: [(f64, Instant); 3] = [(0.0, Instant::now()); 3];
    let mut worst = Duration::ZERO;
    let mut round = 0;
    while started.elapsed() < Duration::from_secs(180) {
        if let Some(mut c) = churner.take() {
            c.sigterm();
            if !c.exited(Duration::from_secs(30)).await {
                c.kill().await;
            }
        }
        churner = Some(Member::start(&config, &log(format!("churn{round}")), &[]));
        round += 1;
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for (i, m) in members.iter().enumerate() {
                let committed = m.metric("ddi_batches_committed_total").await;
                if committed > last[i].0 {
                    last[i] = (committed, Instant::now());
                }
                worst = worst.max(last[i].1.elapsed());
            }
        }
    }
    if let Some(mut c) = churner.take() {
        c.kill().await;
    }
    stop.store(true, Ordering::Relaxed);
    let produced = producing.await.unwrap();
    for m in &mut members {
        m.kill().await;
    }
    assert!(
        worst <= Duration::from_secs(60),
        "a member went {worst:?} without committing under churn:\n{}",
        members
            .iter()
            .map(|m| m.log_text())
            .collect::<Vec<_>>()
            .join("\n----\n")
    );
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 9. The target read-only for 30 s: the member holds its batch and retries in place
/// (`ddi_kafka_retrying` = 1), never leaves its group, and commits once the target is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn transient_storage_outage_stays_in_group() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new(3).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write("run.toml", &f.toml("outage", "", ""));
    let mut a = Member::start(&config, &f.dir.path().join("a.log"), &[]);
    eventually(
        Duration::from_secs(60),
        "the member to own every partition",
        || async { a.metric("ddi_kafka_assigned_partitions").await == 3.0 },
    )
    .await;
    let mut produced = produce_n(&f, id, 30).await;
    eventually(Duration::from_secs(30), "a first commit", || async {
        a.metric("ddi_batches_committed_total").await >= 1.0
    })
    .await;
    let rebalances = a.metric("ddi_kafka_rebalances_total").await;
    let log = std::path::Path::new(&f.target).join("_delta_log");
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o555)).unwrap();
    produced.extend(produce_n(&f, id, 30).await);
    eventually(Duration::from_secs(30), "the member to retry", || async {
        a.metric("ddi_kafka_retrying").await == 1.0
    })
    .await;
    tokio::time::sleep(Duration::from_secs(25)).await;
    assert_eq!(
        a.metric("ddi_kafka_rebalances_total").await,
        rebalances,
        "it left its group"
    );
    assert_eq!(a.metric("ddi_kafka_assigned_partitions").await, 3.0);
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o755)).unwrap();
    eventually(
        Duration::from_secs(90),
        "the held batch to commit",
        || async { coordinates(&f.target).await.len() == produced.len() },
    )
    .await;
    assert_exactly(&coordinates(&f.target).await, &produced);
    assert_eq!(a.metric("ddi_kafka_retrying").await, 0.0);
    a.kill().await;
}

/// 10. A member that sleeps longer than max.poll.interval.ms before its commit is thrown out
/// of the group; its commit, landing after another member took over, is fenced, and nothing
/// is read twice.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn zombie_after_poll_interval() {
    let f = Fixture::new(4).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let settings = "[kafka.pipeline.settings]\n\"max.poll.interval.ms\" = 6000\n\
                    \"session.timeout.ms\" = 6000\n\"heartbeat.interval.ms\" = 1000";
    let config = f.write("run.toml", &f.toml("zombie", "", settings));
    let mut zombie = Member::start(
        &config,
        &f.dir.path().join("zombie.log"),
        &[("DDI_TEST_FAULT", "sleep_before_commit:12")],
    );
    eventually(
        Duration::from_secs(60),
        "the zombie to own every partition",
        || async { zombie.metric("ddi_kafka_assigned_partitions").await == 4.0 },
    )
    .await;
    let mut other = Member::start(&config, &f.dir.path().join("other.log"), &[]);
    let produced = produce_n(&f, id, 120).await;
    eventually(
        Duration::from_secs(120),
        "every message in the target",
        || async { coordinates(&f.target).await.len() >= produced.len() },
    )
    .await;
    // Let the zombie's late commits land, or be fenced.
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert_exactly(&coordinates(&f.target).await, &produced);
    zombie.kill().await;
    other.kill().await;
}

/// 32. A second deployment that fences every commit: `Waiting` steps are not progress, so
/// ddi_pipeline_seconds_since_progress keeps growing while ddi_kafka_fenced_total does (I22).
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn waiting_not_progress_when_fenced() {
    let f = Arc::new(Fixture::new(2).await);
    let id = register(&f.subject(), EVENT_V1).await;
    // Offsets for both partitions first, so the other deployment has something to re-assert.
    produce_n(&f, id, 10).await;
    let toml = f.toml("fenced", "", "");
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    while p.step().await.unwrap() != StepOutcome::CaughtUp {}
    drop(p);

    let config = f.write("run.toml", &toml);
    let mut a = Member::start(
        &config,
        &f.dir.path().join("a.log"),
        &[("DDI_TEST_FAULT", "sleep_before_commit:2")],
    );
    eventually(
        Duration::from_secs(60),
        "the member to own its partitions",
        || async { a.metric("ddi_kafka_assigned_partitions").await == 2.0 },
    )
    .await;
    // One commit of its own first, so the gauge has a last progress to count from.
    produce_n(&f, id, 4).await;
    eventually(Duration::from_secs(60), "a first commit", || async {
        a.metric("ddi_batches_committed_total").await >= 1.0
    })
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let fencing = {
        let (f, stop) = (f.clone(), stop.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                reassert(&f, &[0, 1]).await;
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        })
    };
    produce_n(&f, id, 10).await;
    tokio::time::sleep(Duration::from_secs(25)).await;
    let since = a.metric("ddi_pipeline_seconds_since_progress").await;
    let fenced = a.metric("ddi_kafka_fenced_total").await;
    stop.store(true, Ordering::Relaxed);
    fencing.await.unwrap();
    assert!(
        fenced >= 3.0,
        "only {fenced} fenced commits:\n{}",
        a.log_text()
    );
    assert!(
        since >= 15.0,
        "seconds_since_progress is {since} while every commit was fenced"
    );
    // Once the other deployment stops, the member commits and progresses again.
    eventually(
        Duration::from_secs(60),
        "the member to commit again",
        || async { a.metric("ddi_pipeline_seconds_since_progress").await < 10.0 },
    )
    .await;
    a.kill().await;
}
