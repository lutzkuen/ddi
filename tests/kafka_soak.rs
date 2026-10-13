//! The M3 soak (docs/kafka-design.md §13): three `ddi run` members on one topic under
//! continuous production, while a fourth is started and stopped over and over, for
//! `DDI_SOAK_SECS` (two hours by default). Afterwards the target must hold every produced
//! (partition, offset) exactly once, and every member must have committed regularly.
//!
//! Not part of any suite: it runs only when asked for by name, with a broker and a directory
//! for its tables and logs.
//!
//! ```bash
//! DDI_TEST_KAFKA=127.0.0.1:9092 DDI_SOAK_DIR=/tmp/soak DDI_SOAK_SECS=7200 \
//!   cargo test --features kafka --test kafka_soak -- --ignored --nocapture soak
//! ```

#![cfg(feature = "kafka")]

mod kafka_common;

use std::collections::BTreeSet;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_common::*;

const PARTITIONS: i32 = 12;
/// Messages a second, spread over every partition.
const RATE: u64 = 200;
/// How long the fourth member stays, and how long it stays away.
const CHURN_UP: Duration = Duration::from_secs(45);
const CHURN_DOWN: Duration = Duration::from_secs(15);

fn report(path: &std::path::Path, line: &str) {
    println!("{line}");
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(f, "{line}");
    }
}

/// A `ddi run` member from the soak's own copy of the binary, logging next to the tables.
fn start_member(
    bin: &std::path::Path,
    config: &std::path::Path,
    root: &std::path::Path,
    name: &str,
) -> Member {
    let port = free_port();
    let log = root.join(format!("{name}.log"));
    let out = std::fs::File::create(&log).unwrap();
    let child = tokio::process::Command::new(bin)
        .arg("run")
        .arg("--config")
        .arg(config)
        .arg("--metrics-addr")
        .arg(format!("127.0.0.1:{port}"))
        .env("RUST_LOG", "warn,delta_delta_ingest=info")
        .stdout(std::process::Stdio::from(out.try_clone().unwrap()))
        .stderr(std::process::Stdio::from(out))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    Member { child, port, log }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a two-hour soak; run by name with DDI_TEST_KAFKA and DDI_SOAK_DIR"]
async fn soak() {
    let secs: u64 = std::env::var("DDI_SOAK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7200);
    let root = PathBuf::from(std::env::var("DDI_SOAK_DIR").expect("set DDI_SOAK_DIR"));
    std::fs::create_dir_all(&root).unwrap();
    let log = root.join("report.txt");
    // A copy, so that a rebuild during the soak cannot change what the members run.
    let bin = root.join("ddi");
    std::fs::copy(env!("CARGO_BIN_EXE_ddi"), &bin).unwrap();

    let topic = unique("soak");
    create_topic(&topic, PARTITIONS).await;
    let subject = format!("{topic}-value");
    let id = register(&subject, EVENT_V1).await;
    let target = root.join("bronze").to_str().unwrap().to_string();
    let dq = format!("{target}__ddi_dq");
    create_target(&target).await;
    create_dq(&dq).await;
    let app_id = unique("soak-app");
    let config = root.join("pipelines.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[runtime]
allowed_latency_secs = 15

[kafka]
brokers = "{brokers}"
blocked_recheck_secs = 10

[kafka.schema_registry]
url = "{sr}"

[[kafka.pipeline]]
name = "soak"
app_id = "{app_id}"
topic = "{topic}"
target_uri = "{target}"
group_id = "g-{app_id}"

[kafka.pipeline.settings]
"session.timeout.ms" = 10000
"heartbeat.interval.ms" = 1000
"#,
            brokers = brokers(),
            sr = registry(),
        ),
    )
    .unwrap();
    report(
        &log,
        &format!(
            "soak: topic {topic}, {PARTITIONS} partitions, {RATE}/s, {secs} s, app_id {app_id}"
        ),
    );

    let start = {
        let (bin, config, root) = (bin.clone(), config.clone(), root.clone());
        move |name: &str| start_member(&bin, &config, &root, name)
    };
    let mut members: Vec<Member> = (0..3).map(|i| start(&format!("member{i}"))).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let produced = Arc::new(AtomicU64::new(0));

    let producing = {
        let (stop, produced, topic) = (stop.clone(), produced.clone(), topic.clone());
        tokio::spawn(async move {
            let producer = Producer::new();
            let mut n: i64 = 0;
            let tick = Duration::from_micros(1_000_000 / RATE);
            let mut next = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                let p = (n % i64::from(PARTITIONS)) as i32;
                producer
                    .send(&topic, p, None, Some(&framed(EVENT_V1, id, event(n, None))))
                    .await;
                produced.fetch_add(1, Ordering::Relaxed);
                n += 1;
                next += tick;
                if let Some(wait) = next.checked_duration_since(Instant::now()) {
                    tokio::time::sleep(wait).await;
                }
            }
        })
    };

    let churning = {
        let stop = stop.clone();
        let start = start.clone();
        let log = log.clone();
        tokio::spawn(async move {
            let mut restarts = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let mut churner = start(&format!("churner{}", restarts % 4));
                tokio::time::sleep(CHURN_UP).await;
                churner.sigterm();
                if !churner.exited(Duration::from_secs(60)).await {
                    report(&log, "churner did not stop within 60 s of SIGTERM; killed");
                    churner.kill().await;
                }
                restarts += 1;
                tokio::time::sleep(CHURN_DOWN).await;
            }
            restarts
        })
    };

    // Every 15 s, each member's committed batches: the longest stretch without a new one.
    let began = Instant::now();
    let mut last: [(f64, Instant); 3] = [(0.0, Instant::now()); 3];
    let mut worst = [Duration::ZERO; 3];
    let mut samples = 0u64;
    while began.elapsed() < Duration::from_secs(secs) {
        tokio::time::sleep(Duration::from_secs(15)).await;
        samples += 1;
        for (i, m) in members.iter().enumerate() {
            let text = m.metrics().await;
            if text.is_empty() {
                report(&log, &format!("member{i}: no metrics (down?)"));
                continue;
            }
            let committed = metric(&text, "ddi_batches_committed_total");
            if committed > last[i].0 {
                last[i] = (committed, Instant::now());
            }
            worst[i] = worst[i].max(last[i].1.elapsed());
        }
        if samples % 20 == 0 {
            let mut line = format!(
                "t={:>5}s produced={}",
                began.elapsed().as_secs(),
                produced.load(Ordering::Relaxed)
            );
            for (i, m) in members.iter().enumerate() {
                let text = m.metrics().await;
                line.push_str(&format!(
                    " | m{i} commits={} assigned={} rebalances={} fenced={} errors={} lag={}",
                    metric(&text, "ddi_batches_committed_total"),
                    metric(&text, "ddi_kafka_assigned_partitions"),
                    metric(&text, "ddi_kafka_rebalances_total"),
                    metric(&text, "ddi_kafka_fenced_total"),
                    metric(&text, "ddi_errors_total"),
                    metric(&text, "ddi_kafka_lag_messages{"),
                ));
            }
            report(&log, &line);
        }
    }

    stop.store(true, Ordering::Relaxed);
    producing.await.unwrap();
    let restarts = churning.await.unwrap();
    let total = produced.load(Ordering::Relaxed);
    report(
        &log,
        &format!("production stopped: {total} messages; churner restarts: {restarts}"),
    );
    let expected: BTreeSet<(i32, i64)> = (0..PARTITIONS)
        .flat_map(|p| {
            let (low, high) = watermarks(&topic, p);
            assert_eq!(low, 0);
            (0..high).map(move |o| (p, o))
        })
        .collect();
    assert_eq!(
        expected.len() as u64,
        total,
        "the topic holds what was produced"
    );
    let drained = Instant::now();
    while coordinates(&target).await.len() < expected.len()
        && drained.elapsed() < Duration::from_secs(300)
    {
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let rows = coordinates(&target).await;
    let mut summary = String::new();
    for (i, m) in members.iter().enumerate() {
        let text = m.metrics().await;
        summary.push_str(&format!(
            "member{i}: commits={} rebalances={} fenced={} revoke_flushes={} errors={} restarts={} longest_without_commit={:?}\n",
            metric(&text, "ddi_batches_committed_total"),
            metric(&text, "ddi_kafka_rebalances_total"),
            metric(&text, "ddi_kafka_fenced_total"),
            metric(&text, "ddi_kafka_revoke_flushes_total"),
            metric(&text, "ddi_errors_total"),
            metric(&text, "ddi_pipeline_restarts_total"),
            worst[i],
        ));
    }
    report(&log, &summary);
    for m in &mut members {
        m.kill().await;
    }
    let distinct: BTreeSet<(i32, i64)> = rows.iter().copied().collect();
    report(
        &log,
        &format!(
            "rows={} distinct={} expected={} duplicates={} missing={} extra={} duration={}s",
            rows.len(),
            distinct.len(),
            expected.len(),
            rows.len() - distinct.len(),
            expected.difference(&distinct).count(),
            distinct.difference(&expected).count(),
            began.elapsed().as_secs()
        ),
    );
    assert_exactly(&rows, &expected);
    for (i, w) in worst.iter().enumerate() {
        assert!(
            *w <= Duration::from_secs(120),
            "member{i} went {w:?} without a commit"
        );
    }
}
