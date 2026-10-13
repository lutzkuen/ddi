//! What happens to a Kafka pipeline's target under it: a schema change, a replaced table, a
//! commit whose response is lost, a downstream Delta pipeline, partitioning, log cleanup, the
//! memory budget, and a protocol upgrade (docs/kafka-design.md §9.2, tests 21-23, 27 and
//! 29-33).
//!
//! Ignored unless a broker is there: see tests/kafka_common/mod.rs for how to run them.

#![cfg(feature = "kafka")]

mod common;
mod kafka_common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use deltalake::arrow::array::{Array, RecordBatch};
use deltalake::kernel::{DataType as Delta, PrimitiveType, StructField};
use deltalake::protocol::SaveMode;

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

async fn run_to_end(p: &mut Pipeline) {
    for _ in 0..1000 {
        if p.step().await.unwrap() == StepOutcome::CaughtUp {
            return;
        }
    }
    panic!("once-mode did not finish");
}

/// The next commit file's path in a table's log.
fn next_commit(path: &str) -> std::path::PathBuf {
    let log = std::path::Path::new(path).join("_delta_log");
    let newest = std::fs::read_dir(&log)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.strip_suffix(".json")?.parse::<u64>().ok()
        })
        .max()
        .unwrap();
    log.join(format!("{:020}.json", newest + 1))
}

/// The rows each data commit of the target's log added, by `ddi.kafka.offsets`.
fn batch_sizes(path: &str) -> Vec<i64> {
    let log = std::path::Path::new(path).join("_delta_log");
    let mut files: Vec<_> = std::fs::read_dir(log)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    let mut out = Vec::new();
    for f in files {
        for line in std::fs::read_to_string(f).unwrap().lines() {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(ranges) = v["commitInfo"]["ddi.kafka.offsets"].as_object() {
                let n: i64 = ranges
                    .values()
                    .map(|r| r[1].as_i64().unwrap() - r[0].as_i64().unwrap() + 1)
                    .sum();
                if n > 0 {
                    out.push(n);
                }
            }
        }
    }
    out
}

/// 21. A nullable column added to the target while a member runs: its commit conflicts with
/// the metadata change, the reset rebuilds its casts and keeps the batch (I15), and the rows
/// land with the new column null.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn alter_table_mid_stream() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write("run.toml", &f.toml("alter", "", "allowed_latency_secs = 8"));
    let mut m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    eventually(
        Duration::from_secs(60),
        "the member to own every partition",
        || async { m.metric("ddi_kafka_assigned_partitions").await == 2.0 },
    )
    .await;
    let mut produced = produce_to(&f, id, 0, 10).await;
    produced.extend(produce_to(&f, id, 1, 10).await);
    eventually(Duration::from_secs(30), "the member to buffer", || async {
        m.metric("ddi_kafka_oldest_buffered_seconds").await >= 1.0
    })
    .await;
    open(&f.target)
        .await
        .add_columns()
        .with_fields([StructField::new(
            "extra",
            Delta::Primitive(PrimitiveType::String),
            true,
        )])
        .await
        .unwrap();
    eventually(Duration::from_secs(60), "the rows", || async {
        coordinates(&f.target).await.len() == produced.len()
    })
    .await;
    assert_exactly(&coordinates(&f.target).await, &produced);
    assert!(
        m.metric("ddi_kafka_fenced_total{pipeline=\"alter\",kind=\"other_conflict\"}")
            .await
            >= 1.0,
        "{}",
        m.log_text()
    );
    let batches = scan(&f.target).await;
    let extra = batches[0].column_by_name("extra").expect("the new column");
    assert_eq!(extra.null_count(), extra.len());
    m.kill().await;
}

/// 22. The target dropped and recreated in place while a member runs: the member notices
/// before its next commit (I18), writes nothing into the new table, and holds in its group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn replaced_target_blocks() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write("run.toml", &f.toml("replaced", "", ""));
    let mut m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    eventually(
        Duration::from_secs(60),
        "the member to own every partition",
        || async { m.metric("ddi_kafka_assigned_partitions").await == 2.0 },
    )
    .await;
    let first = produce_to(&f, id, 0, 10).await;
    eventually(Duration::from_secs(30), "a first commit", || async {
        coordinates(&f.target).await.len() == first.len()
    })
    .await;
    // Three more commits, so the old table's version is past the new one's.
    for _ in 0..3 {
        let n = coordinates(&f.target).await.len();
        produce_to(&f, id, 1, 2).await;
        eventually(Duration::from_secs(30), "a commit", || async {
            coordinates(&f.target).await.len() > n
        })
        .await;
    }
    std::fs::remove_dir_all(&f.target).unwrap();
    create_target(&f.target).await;
    produce_to(&f, id, 0, 10).await;
    eventually(Duration::from_secs(60), "the member to notice", || async {
        m.log_text()
            .contains("is no longer the table this pipeline opened")
    })
    .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        coordinates(&f.target).await.is_empty(),
        "wrote into the new table"
    );
    assert_eq!(open(&f.target).await.version(), Some(0));
    assert_eq!(
        m.metric("ddi_kafka_assigned_partitions").await,
        2.0,
        "it left its group"
    );
    // Held in its group, it says why: a gauge with the reason, and the error counted.
    eventually(Duration::from_secs(10), "the blocked gauge", || async {
        m.metric("ddi_kafka_pipeline_blocked{pipeline=\"replaced\",reason=\"target_replaced\"}")
            .await
            == 1.0
    })
    .await;
    assert!(m.metric("ddi_errors_total").await >= 1.0);
    m.kill().await;
}

/// 23. A commit that lands but whose response is lost (fail_after_commit): the batch is held
/// and retried, the retry meets its own commit (ConcurrentTransaction), and the reset finds
/// the offsets moved and drops it. Nothing lands twice (I17).
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn ambiguous_commit_error_no_duplicate() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = produce_to(&f, id, 0, 30).await;
    produced.extend(produce_to(&f, id, 1, 30).await);
    let config = f.write(
        "once.toml",
        &f.toml("ambiguous", "", "max_messages_per_batch = 20"),
    );
    let (ok, out) = once_with(&config, &[("DDI_TEST_FAULT", "fail_after_commit")]).await;
    assert!(ok, "{out}");
    assert!(
        out.contains("fail_after_commit"),
        "the fault did not fire: {out}"
    );
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 23, a variant. The reset after that ambiguous commit seeks its partitions again, and the
/// seek times out (fail_seek), twice: a transient failure, tried again on the next pass with
/// the partitions paused, never an error that drops the consumer (in a group, a rebalance;
/// here, a failed run).
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn a_seek_that_times_out_is_tried_again() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = produce_to(&f, id, 0, 30).await;
    produced.extend(produce_to(&f, id, 1, 30).await);
    let config = f.write(
        "once.toml",
        &f.toml("seek", "", "max_messages_per_batch = 20"),
    );
    let (ok, out) = once_with(
        &config,
        &[("DDI_TEST_FAULT", "fail_after_commit,fail_seek:2")],
    )
    .await;
    assert!(ok, "{out}");
    assert!(out.contains("fail_seek"), "the fault did not fire: {out}");
    assert_exactly(&coordinates(&f.target).await, &produced);
}

/// 27. A Delta→Delta pipeline over the bronze a Kafka pipeline wrote, with the cut-off kept
/// per Kafka partition (`dedup_partition = "kafka_partition"`, `dedup_timestamp =
/// "kafka_offset"`): after a rebuild of silver, the rescan is exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn downstream_partition_watermark() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    // Partition 0 runs far ahead of partition 1 in every bronze commit.
    let producer = Producer::new();
    let toml = f.toml("bronze", "", "max_messages_per_batch = 12");
    let mut all = BTreeSet::new();
    for round in 0..4i64 {
        for i in 0..10 {
            let n = round * 100 + i;
            producer
                .send(
                    &f.topic,
                    0,
                    None,
                    Some(&framed(EVENT_V1, id, event(n, None))),
                )
                .await;
            all.insert(n);
        }
        let n = round * 100 + 50;
        producer
            .send(
                &f.topic,
                1,
                None,
                Some(&framed(EVENT_V1, id, event(n, None))),
            )
            .await;
        all.insert(n);
        let mut p = Pipeline::open_with(resolve(&toml, true), None)
            .await
            .unwrap();
        run_to_end(&mut p).await;
    }

    let silver = f.dir.path().join("silver").to_str().unwrap().to_string();
    create_target(&silver).await;
    let mut cfg = common::pipeline_cfg("silver", &f.target, &silver);
    cfg.dedup_timestamp = Some("kafka_offset".into());
    cfg.dedup_key = Some("id".into());
    cfg.dedup_partition = vec!["kafka_partition".into()];
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(
        common::read_ids(&silver).await,
        all.iter().copied().collect::<Vec<_>>()
    );

    // A batch job rebuilds silver from what bronze held after its second data commit.
    let mut early = open(&f.target).await;
    early.load_version(3).await.unwrap();
    let (_, stream) = early.scan_table().await.unwrap();
    let rows: Vec<RecordBatch> = futures::TryStreamExt::try_collect(stream).await.unwrap();
    open(&silver)
        .await
        .write(rows)
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();
    let mut p = Pipeline::open(cfg).await.unwrap();
    assert!(
        p.coverage().is_some(),
        "the rebuild opened no coverage window"
    );
    p.run_until_caught_up().await.unwrap();
    let ids = common::read_ids(&silver).await;
    assert!(!common::has_duplicates(&ids), "{ids:?}");
    assert_eq!(ids, all.iter().copied().collect::<Vec<_>>());
}

/// 29. A target partitioned by `ingest_date`, which the SQL derives from the payload: rows land
/// in their partitions, an all-tombstone batch commits its offsets with no file on the
/// partitioned table, and every partition value is its rows' own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn partitioned_target_ingest_date() {
    let f = Fixture::bare(2).await;
    let mut columns = event_columns();
    columns.push(StructField::new(
        "ingest_date",
        Delta::Primitive(PrimitiveType::String),
        true,
    ));
    create_table_with(&f.target, columns, &["ingest_date"], &[]).await;
    create_dq(&f.dq).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let producer = Producer::new();
    let mut produced = BTreeSet::new();
    for n in 0..20i64 {
        let name = format!("2026-10-{:02}T12:00:00Z", 1 + n % 3);
        let p = (n % 2) as i32;
        let o = producer
            .send(
                &f.topic,
                p,
                None,
                Some(&framed(EVENT_V1, id, event(n, Some(&name)))),
            )
            .await;
        produced.insert((p, o));
    }
    let toml = f.toml(
        "dated",
        "",
        "transform_sql = \"SELECT *, substr(name, 1, 10) AS ingest_date FROM source\"",
    );
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    run_to_end(&mut p).await;
    drop(p);
    assert_exactly(&coordinates(&f.target).await, &produced);
    for b in scan(&f.target).await {
        let name = b.column_by_name("name").unwrap();
        let date = b.column_by_name("ingest_date").unwrap();
        for i in 0..b.num_rows() {
            assert_eq!(
                text(date.as_ref(), i).unwrap(),
                text(name.as_ref(), i).unwrap()[..10]
            );
        }
    }

    // Only tombstones: a zero-row commit that moves the offsets.
    let before = open(&f.target).await;
    let files = before.snapshot().unwrap().log_data().num_files();
    let mut last = BTreeMap::new();
    for p in 0..2 {
        for _ in 0..3 {
            last.insert(p, producer.send(&f.topic, p, None, None).await);
        }
    }
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    run_to_end(&mut p).await;
    let after = open(&f.target).await;
    assert_eq!(after.snapshot().unwrap().log_data().num_files(), files);
    let stored = txns(&f.target, &f.app_id, 2).await;
    for (p, o) in last {
        assert_eq!(stored[&p], Some(o));
    }
}

/// 29. A partition column the SQL does not produce is refused when the pipeline opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn partition_column_unmapped_refused_at_open() {
    let f = Fixture::bare(1).await;
    let mut columns = event_columns();
    columns.push(StructField::new(
        "ingest_date",
        Delta::Primitive(PrimitiveType::String),
        true,
    ));
    create_table_with(&f.target, columns, &["ingest_date"], &[]).await;
    register(&f.subject(), EVENT_V1).await;
    let toml = f.toml("undated", "", "transform_sql = \"SELECT * FROM source\"");
    let e = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .err()
        .expect("the open is refused");
    assert!(
        matches!(&e, delta_delta_ingest::Error::KafkaPartitionColumnUnmapped { columns, .. } if columns == &vec!["ingest_date".to_string()]),
        "{e:?}"
    );
    assert!(e.kafka_waits_for_an_operator());
}

/// 30. An idle member's handle falls behind log cleanup: another writer commits, checkpoints
/// and the expired commits are removed. The member's next commit refreshes first (here after
/// a handle_max_age of a few seconds) and lands; without the refresh, the missing commit in its
/// conflict check is a Replan, and it lands all the same.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn handle_aged_past_log_retention() {
    use delta_delta_ingest::sink::Sink;
    use deltalake::delta_datafusion::DataFusionMixins;
    for aging in [true, false] {
        let f = Fixture::bare(1).await;
        create_table_with(
            &f.target,
            event_columns(),
            &[],
            &[
                ("delta.logRetentionDuration", "interval 1 seconds"),
                ("delta.checkpointInterval", "2"),
            ],
        )
        .await;
        create_dq(&f.dq).await;
        let id = register(&f.subject(), EVENT_V1).await;
        let config = f.write("run.toml", &f.toml("aged", "", ""));
        let env: &[(&str, &str)] = if aging {
            &[("DDI_TEST_FAULT", "handle_max_age:4")]
        } else {
            &[]
        };
        let mut m = Member::start(&config, &f.dir.path().join("m.log"), env);
        let mut produced = produce_to(&f, id, 0, 5).await;
        eventually(Duration::from_secs(60), "a first commit", || async {
            coordinates(&f.target).await.len() == 5
        })
        .await;
        // Another writer: txn-only commits of its own, a checkpoint, and the cleanup that
        // removes the commits the idle member's handle stands on.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let sink = Sink::new("another-writer", 128 * 1024 * 1024);
        for v in 0..6 {
            let t = open(&f.target).await;
            let schema = t.snapshot().unwrap().snapshot().read_schema();
            sink.commit(t, vec![RecordBatch::new_empty(schema)], v)
                .await
                .unwrap();
        }
        let t = open(&f.target).await;
        deltalake::checkpoints::create_checkpoint(&t, None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        deltalake::checkpoints::cleanup_metadata(&open(&f.target).await, None)
            .await
            .unwrap();
        assert!(
            !std::path::Path::new(&f.target)
                .join("_delta_log/00000000000000000001.json")
                .exists(),
            "the cleanup removed nothing"
        );
        tokio::time::sleep(Duration::from_secs(5)).await;
        produced.extend(produce_to(&f, id, 0, 5).await);
        eventually(Duration::from_secs(60), "the next commit", || async {
            coordinates(&f.target).await.len() == produced.len()
        })
        .await;
        assert_exactly(&coordinates(&f.target).await, &produced);
        m.kill().await;
    }
}

/// 31. With a memory budget installed, one tiny batch (a malformed message isolated) does not
/// shrink the next ones: Kafka batches close on decoded bytes measured against the budget's
/// batch share, never on a ratio a small batch raised (EO-M5).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn small_flush_does_not_shrink_batches() {
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let producer = Producer::new();
    producer
        .send(&f.topic, 0, None, Some(b"\x00\x00\x00\x00\x07\xff"))
        .await;
    producer
        .send(
            &f.topic,
            0,
            None,
            Some(&framed(EVENT_V1, id, event(1, None))),
        )
        .await;
    // 40 MB in all: 32 MB is librdkafka's, 8 MB the pipeline's, half of it a batch.
    let section = "max_memory = \"40MB\"";
    let toml =
        f.toml("budget", "", "")
            .replacen("[runtime]\n", &format!("[runtime]\n{section}\n"), 1);
    let config = f.write("once.toml", &toml);
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    let name = "x".repeat(200);
    let futures: Vec<_> = (0..30_000i64)
        .map(|n| {
            let value = framed(EVENT_V1, id, event(n, Some(&name)));
            let (producer, topic) = (&producer, f.topic.clone());
            async move { producer.send(&topic, 0, None, Some(&value)).await }
        })
        .collect();
    futures::future::join_all(futures).await;
    let (ok, out) = once(&config).await;
    assert!(ok, "{out}");
    let sizes = batch_sizes(&f.target);
    assert_eq!(sizes.iter().sum::<i64>(), 30_002, "{sizes:?}");
    assert!(sizes.len() >= 3, "the budget closed no batch: {sizes:?}");
    // Every batch of the backlog but the last closed at the cap, not at a shrunk size.
    let backlog = &sizes[1..sizes.len() - 1];
    let cap = *backlog.iter().max().unwrap();
    assert!(
        backlog.iter().all(|s| *s * 2 >= cap),
        "batches shrank: {sizes:?}"
    );
}

/// 33. Another engine upgrades the target's protocol to a writer feature delta-rs cannot
/// write: the member's commit conflicts, its reset refuses the table, and it holds in its
/// group without writing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn protocol_upgrade_blocks() {
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write("run.toml", &f.toml("upgraded", "", ""));
    let mut m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    let first = produce_to(&f, id, 0, 5).await;
    eventually(Duration::from_secs(60), "a first commit", || async {
        coordinates(&f.target).await.len() == first.len()
    })
    .await;
    std::fs::write(
        next_commit(&f.target),
        "{\"commitInfo\":{\"timestamp\":1700000000000,\"operation\":\"UPGRADE PROTOCOL\"}}\n\
         {\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":7,\
         \"writerFeatures\":[\"identityColumns\"]}}\n",
    )
    .unwrap();
    produce_to(&f, id, 0, 5).await;
    let held = tokio::time::timeout(Duration::from_secs(60), async {
        while !m.log_text().contains("needs a writer this build cannot be") {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(held.is_ok(), "the member did not hold:\n{}", m.log_text());
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(coordinates(&f.target).await.len(), first.len());
    assert_eq!(
        m.metric("ddi_kafka_assigned_partitions").await,
        1.0,
        "it left its group"
    );
    eventually(Duration::from_secs(10), "the blocked gauge", || async {
        m.metric(
            "ddi_kafka_pipeline_blocked{pipeline=\"upgraded\",reason=\"protocol_unsupported\"}",
        )
        .await
            == 1.0
    })
    .await;
    assert!(m.metric("ddi_errors_total").await >= 1.0);
    m.kill().await;
}
