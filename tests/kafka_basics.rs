//! A Kafka pipeline end to end against a real broker: once-mode, restarts, kills, a commit that
//! fails after its upload, compression, the raw format and accounting
//! (docs/kafka-design.md §9.2, tests 1-4, 20, 24, 25).
//!
//! Ignored unless a broker is there: see tests/kafka_common/mod.rs for how to run them.

#![cfg(feature = "kafka")]

mod kafka_common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use delta_delta_ingest::metrics::Metrics;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use deltalake::arrow::array::{Array, AsArray};
use deltalake::arrow::datatypes::{DataType, TimeUnit};

use kafka_common::*;

/// Produce `per_partition` events to each partition of `f`'s topic, every `tombstone_every`th
/// a tombstone; the coordinates of the events (tombstones excluded) and of the tombstones.
async fn produce_events(
    f: &Fixture,
    id: u32,
    per_partition: i64,
    tombstone_every: Option<i64>,
) -> (BTreeSet<(i32, i64)>, BTreeSet<(i32, i64)>) {
    let producer = Producer::new();
    let mut events = BTreeSet::new();
    let mut tombstones = BTreeSet::new();
    for p in 0..f.partitions {
        for i in 0..per_partition {
            let n = i64::from(p) * 1_000_000 + i;
            let tombstone = tombstone_every.is_some_and(|t| i % t == t - 1);
            let value =
                (!tombstone).then(|| framed(EVENT_V1, id, event(n, Some(&format!("e{n}")))));
            let o = producer
                .send(
                    &f.topic,
                    p,
                    Some(n.to_string().as_bytes()),
                    value.as_deref(),
                )
                .await;
            if tombstone {
                tombstones.insert((p, o));
            } else {
                events.insert((p, o));
            }
        }
    }
    (events, tombstones)
}

/// Step an in-process pipeline until once-mode says it is done.
async fn run_once(p: &mut Pipeline) -> usize {
    let mut commits = 0;
    for _ in 0..600 {
        match p.step().await.unwrap() {
            StepOutcome::CaughtUp => return commits,
            StepOutcome::Waiting => {}
            _ => commits += 1,
        }
    }
    panic!("once-mode did not finish in 600 steps");
}

/// 1. N Avro messages over 6 partitions, read in once-mode: the rows are exactly the
/// produced (partition, offset)s, each partition's txn is its last offset, tombstones are
/// counted and skipped, and the metadata columns are typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn basic_once() {
    let f = Fixture::new(6).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let (events, tombstones) = produce_events(&f, id, 50, Some(10)).await;

    let metrics = Metrics::new().pipeline("basic").kafka();
    let cfg = resolve(&f.toml("basic", "", ""), true);
    let mut p = Pipeline::open_with(cfg, Some(metrics.clone()))
        .await
        .unwrap();
    run_once(&mut p).await;

    assert_exactly(&coordinates(&f.target).await, &events);
    let txns = txns(&f.target, &f.app_id, f.partitions).await;
    for part in 0..f.partitions {
        assert_eq!(
            txns[&part],
            Some(49),
            "partition {part}: the last offset, a tombstone"
        );
    }
    assert_eq!(
        metrics.tombstones.load(Ordering::Relaxed) as usize,
        tombstones.len()
    );
    assert_eq!(
        metrics.messages.load(Ordering::Relaxed) as usize,
        events.len() + tombstones.len()
    );

    let batches = scan(&f.target).await;
    let b = &batches[0];
    let schema = b.schema();
    assert_eq!(
        schema
            .field_with_name("kafka_timestamp")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    let topic = b.column_by_name("kafka_topic").unwrap();
    assert_eq!(text(topic.as_ref(), 0).as_deref(), Some(f.topic.as_str()));
    let ts = b.column_by_name("kafka_timestamp").unwrap();
    assert!(ts.is_valid(0));
    let name = b.column_by_name("name").unwrap();
    assert!(text(name.as_ref(), 0).unwrap().starts_with('e'));
}

/// 2. Restarts at arbitrary points, the pipeline dropped between steps as a crash would drop
/// it (as tests/exactly_once.rs does for Delta): every message lands exactly once, and every
/// open re-reads each partition's stored offset once to verify it, never twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn restart_resumes_at_stored_plus_one() {
    let f = Fixture::new(4).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let (events, _) = produce_events(&f, id, 60, None).await;
    let toml = f.toml("restarts", "", "max_messages_per_batch = 13");
    let metrics = Metrics::new().pipeline("restarts").kafka();

    // A fixed but irregular schedule of how many steps each process gets.
    for steps in [1usize, 3, 2, 5, 1, 4, 2, 6, 3, 1, 2, 7] {
        let mut p = Pipeline::open_with(resolve(&toml, true), Some(metrics.clone()))
            .await
            .unwrap();
        for _ in 0..steps {
            if p.step().await.unwrap() == StepOutcome::CaughtUp {
                break;
            }
        }
        drop(p);
        let rows = coordinates(&f.target).await;
        let distinct: BTreeSet<_> = rows.iter().copied().collect();
        assert_eq!(rows.len(), distinct.len(), "a duplicate after a restart");
    }
    let before = metrics.dropped_reread.load(Ordering::Relaxed);
    let mut p = Pipeline::open_with(resolve(&toml, true), Some(metrics.clone()))
        .await
        .unwrap();
    run_once(&mut p).await;
    assert_exactly(&coordinates(&f.target).await, &events);

    // The last open re-read the stored offset of every partition that had one, once.
    let stored = txns(&f.target, &f.app_id, f.partitions).await;
    let reread = metrics.dropped_reread.load(Ordering::Relaxed) - before;
    assert!(stored.values().all(|s| s.is_some()));
    assert!(
        reread <= f.partitions as u64,
        "{reread} re-reads for {} partitions in one open",
        f.partitions
    );
    assert_eq!(metrics.dropped_duplicate.load(Ordering::Relaxed), 0);
}

/// 3. `ddi once` SIGKILLed thirty times under continuous production: after every kill no
/// duplicate, and after a last run every message exactly once. At least one kill landed
/// between a parquet upload and its commit, which leaves the file behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn kafka_kill_mid_batch() {
    let f = Arc::new(Fixture::new(3).await);
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write(
        "kill.toml",
        &f.toml("kill", "", "max_messages_per_batch = 15"),
    );

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let producing = {
        let (f, stop) = (f.clone(), stop.clone());
        tokio::spawn(async move {
            let producer = Producer::new();
            let mut produced = BTreeSet::new();
            let mut n: i64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let p = (n % 3) as i32;
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
                // About 200 a second: enough for many commits per run, little to drain.
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            produced
        })
    };

    let mut killed = 0;
    let mut i = 0u64;
    // Thirty kills, and on past them until one has landed between an upload and its commit,
    // which leaves the file behind: a narrow window on a local disk, and wider than most.
    let mut orphaned = 0;
    while killed < 30 || orphaned == 0 {
        i += 1;
        assert!(
            i < 200,
            "{killed} of {i} runs were still running when killed, and {orphaned} files left"
        );
        let mut child = ddi()
            .arg("once")
            .arg("--config")
            .arg(&config)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let after = Duration::from_millis(250 + (i * 397) % 1400);
        tokio::select! {
            status = child.wait() => assert!(status.unwrap().success(), "ddi once failed"),
            _ = tokio::time::sleep(after) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                killed += 1;
            }
        }
        let rows = coordinates(&f.target).await;
        let distinct: BTreeSet<_> = rows.iter().copied().collect();
        assert_eq!(rows.len(), distinct.len(), "a duplicate after kill {i}");
        orphaned = orphans(&f.target).await;
    }
    stop.store(true, Ordering::Relaxed);
    let produced = producing.await.unwrap();
    // The rest in batches of any size.
    let drain = f.write("drain.toml", &f.toml("kill", "", ""));
    let (ok, out) = once(&drain).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
    assert!(
        orphaned >= 1,
        "no kill landed between an upload and its commit"
    );
}

/// 4. A commit that fails after its parquet upload (the log made read-only after the first
/// commit): the file is left behind, the txn does not move, and the batch is held and
/// retried in place; once the log is writable again it commits, exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn kill_between_upload_and_commit() {
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let producer = Producer::new();
    let mut produced = BTreeSet::new();
    for n in 0..10 {
        let p = n as i32 % 2;
        let o = producer
            .send(
                &f.topic,
                p,
                None,
                Some(&framed(EVENT_V1, id, event(n, None))),
            )
            .await;
        produced.insert((p, o));
    }
    let metrics = Metrics::new().pipeline("upload").kafka();
    let mut p = Pipeline::open_with(
        resolve(&f.toml("upload", "", "max_messages_per_batch = 5"), true),
        Some(metrics.clone()),
    )
    .await
    .unwrap();
    // The first commit.
    loop {
        if matches!(p.step().await.unwrap(), StepOutcome::Progressed { .. }) {
            break;
        }
    }
    let after_first = txns(&f.target, &f.app_id, 2).await;
    let log = std::path::Path::new(&f.target).join("_delta_log");
    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o555)).unwrap();

    // The next batches fail on the commit and are held.
    let mut held = false;
    for _ in 0..20 {
        let o = p.step().await.unwrap();
        assert!(
            !matches!(o, StepOutcome::Progressed { .. }),
            "committed into a read-only log"
        );
        if metrics.retrying.load(Ordering::Relaxed) == 1 {
            held = true;
            break;
        }
    }
    assert!(held, "a failed commit was not held for a retry");
    assert_eq!(
        txns(&f.target, &f.app_id, 2).await,
        after_first,
        "the txn moved"
    );
    assert!(orphans(&f.target).await >= 1, "no upload was left behind");

    std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o755)).unwrap();
    run_once(&mut p).await;
    assert_exactly(&coordinates(&f.target).await, &produced);
    assert_eq!(metrics.retrying.load(Ordering::Relaxed), 0);
}

/// 20. A cooperative assignment strategy is refused when the config is loaded: ddi assigns
/// eagerly only (§2.5 rule 8, I7).
#[tokio::test]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn cooperative_rejected_at_load() {
    let f = Fixture::new(1).await;
    let config = f.write(
        "coop.toml",
        &f.toml(
            "coop",
            "",
            "[kafka.pipeline.settings]\n\"partition.assignment.strategy\" = \"cooperative-sticky\"",
        ),
    );
    let out = ddi()
        .arg("validate")
        .arg("--config")
        .arg(&config)
        .output()
        .await
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "{text}");
    assert!(text.contains("INVALID  coop"), "{text}");
    assert!(text.contains("cooperative"), "{text}");
}

/// 24. Every message read is a row, a reject or a tombstone: consumed = rows + rejects +
/// tombstones, from what the steps and the metrics report (I8).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn accounting_identity() {
    let f = Fixture::new(3).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let producer = Producer::new();
    for n in 0..300i64 {
        let p = (n % 3) as i32;
        let value = match n % 17 {
            0 => None,                                  // a tombstone
            5 => Some(b"{\"not\": \"avro\"}".to_vec()), // undecodable
            _ => Some(framed(EVENT_V1, id, event(n, None))),
        };
        producer.send(&f.topic, p, None, value.as_deref()).await;
    }
    let metrics = Metrics::new().pipeline("accounting").kafka();
    let mut p = Pipeline::open_with(
        resolve(
            &f.toml("accounting", "", "max_messages_per_batch = 40"),
            true,
        ),
        Some(metrics.clone()),
    )
    .await
    .unwrap();
    let (mut rows, mut rejected) = (0usize, 0usize);
    loop {
        match p.step().await.unwrap() {
            StepOutcome::CaughtUp => break,
            StepOutcome::Progressed {
                rows: r,
                rejected: x,
                ..
            } => {
                rows += r;
                rejected += x;
            }
            StepOutcome::Skipped { rejected: x, .. } => rejected += x,
            StepOutcome::Waiting => {}
        }
    }
    let consumed = metrics.messages.load(Ordering::Relaxed) as usize;
    let tombstones = metrics.tombstones.load(Ordering::Relaxed) as usize;
    let undecodable = metrics.undecodable.load(Ordering::Relaxed) as usize;
    assert_eq!(consumed, 300);
    assert_eq!(tombstones, 18);
    assert_eq!(undecodable, 18);
    assert_eq!(consumed, rows + rejected + tombstones);
    assert_eq!(rejected, undecodable);
    assert_eq!(rejects(&f.dq).await.len(), undecodable);
    assert_eq!(coordinates(&f.target).await.len(), rows);
}

/// 25. A topic its producer compresses with zstd reads like any other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn zstd_compressed_topic() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let producer = Producer::with(&[("compression.type", "zstd"), ("linger.ms", "50")]);
    let mut produced = BTreeSet::new();
    let sends: Vec<_> = (0..200i64)
        .map(|n| {
            let value = framed(EVENT_V1, id, event(n, Some("a fairly compressible name")));
            let topic = f.topic.clone();
            let producer = &producer;
            async move {
                (
                    (n % 2) as i32,
                    producer
                        .send(&topic, (n % 2) as i32, None, Some(&value))
                        .await,
                )
            }
        })
        .collect();
    for (p, o) in futures::future::join_all(sends).await {
        produced.insert((p, o));
    }
    let config = f.write("zstd.toml", &f.toml("zstd", "", ""));
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 25. `value_format = "raw"`: each value's bytes in `kafka_value`, no registry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn raw_format_once() {
    use deltalake::kernel::{DataType as Delta, PrimitiveType, StructField};
    let f = Fixture::bare(2).await;
    let mut columns = event_columns();
    columns.retain(|c| c.name().starts_with("kafka_"));
    columns.push(StructField::new(
        "kafka_value",
        Delta::Primitive(PrimitiveType::Binary),
        true,
    ));
    create_table_with(&f.target, columns, &[], &[]).await;
    let producer = Producer::new();
    let mut produced = BTreeMap::new();
    for n in 0..20i64 {
        let value = format!("{{\"n\": {n}}}").into_bytes();
        let p = (n % 2) as i32;
        let o = producer.send(&f.topic, p, None, Some(&value)).await;
        produced.insert((p, o), value);
    }
    let config = f.write("raw.toml", &f.toml("raw", "", "value_format = \"raw\""));
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    let mut seen = BTreeMap::new();
    for b in scan(&f.target).await {
        let p = b.column_by_name("kafka_partition").unwrap();
        let o = b.column_by_name("kafka_offset").unwrap();
        let v = b.column_by_name("kafka_value").unwrap();
        let v = deltalake::arrow::compute::cast(v, &DataType::Binary).unwrap();
        let v = v.as_binary::<i32>();
        let p = p.as_primitive::<deltalake::arrow::datatypes::Int32Type>();
        let o = o.as_primitive::<deltalake::arrow::datatypes::Int64Type>();
        for i in 0..b.num_rows() {
            seen.insert((p.value(i), o.value(i)), v.value(i).to_vec());
        }
    }
    assert_eq!(seen, produced);
}
