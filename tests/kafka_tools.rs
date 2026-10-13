//! The operator tools: `ddi kafka plan`, `clone-table` and `set-offsets` (docs/kafka-design.md
//! §4.8, §9.2).
//!
//! Ignored unless a broker is there: see tests/kafka_common/mod.rs for how to run them.

#![cfg(feature = "kafka")]

mod common;
mod kafka_common;

use std::collections::{BTreeMap, BTreeSet};

use delta_delta_ingest::fence::Era;
use delta_delta_ingest::kafka::setoffsets::{self, Request};
use delta_delta_ingest::kafka::{clone, plan};
use delta_delta_ingest::sink::{KafkaOffsets, Sink};
use deltalake::arrow::array::RecordBatch;
use deltalake::delta_datafusion::DataFusionMixins as _;
use deltalake::kernel::{DataType as Delta, PrimitiveType, StructField};

use kafka_common::*;

/// What kafka-delta-ingest leaves in a table, as far as the offsets go: a `txn` for each
/// partition it wrote, and no takeover marker.
async fn seed(f: &Fixture, stored: &[(i32, i64)]) {
    let table = open(&f.target).await;
    let schema = table.snapshot().unwrap().snapshot().read_schema();
    let offsets = KafkaOffsets {
        topic: f.topic.clone(),
        group_id: "seed".into(),
        ranges: BTreeMap::new(),
        assert: stored.iter().copied().collect(),
        marks: BTreeMap::new(),
        cleanup_logs: false,
        baseline: false,
        override_reason: None,
        takeover_marker: false,
    };
    Sink::new(&f.app_id, 128 * 1024 * 1024)
        .commit_kafka(table, vec![RecordBatch::new_empty(schema)], &offsets)
        .await
        .unwrap();
}

async fn produce(f: &Fixture, id: u32, p: i32, n: i64) {
    let producer = Producer::new();
    for i in 0..n {
        producer
            .send(
                &f.topic,
                p,
                None,
                Some(&framed(EVENT_V1, id, event(i, None))),
            )
            .await;
    }
}

/// `ddi kafka <args>` against `config`; its status and output.
async fn run(config: &std::path::Path, args: &[&str]) -> (bool, String) {
    let out = ddi()
        .arg("--config")
        .arg(config)
        .arg("kafka")
        .args(args)
        .output()
        .await
        .unwrap();
    let mut text = String::from_utf8_lossy(&out.stdout).to_string();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn plan_names_the_partition_a_takeover_must_acknowledge() {
    let f = Fixture::new(3).await;
    let id = register(&f.subject(), EVENT_V1).await;
    for p in 0..3 {
        produce(&f, id, p, 10).await;
    }
    // kdi wrote partitions 0 and 1 only; partition 2 holds messages and has no offset.
    seed(&f, &[(0, 9), (1, 4)]).await;

    let toml = f.toml("tools", "kdi_compat = true", "");
    let cfg = resolve(&toml, false);
    let plan = plan::plan(&cfg).await.unwrap();
    assert_eq!(plan.era, Era::Kdi);
    assert!(plan.takeover.is_some(), "{plan:?}");
    assert!(plan.is_blocked(), "{:?}", plan.blocking);
    let p2 = plan
        .partitions
        .iter()
        .find(|p| p.status.partition == 2)
        .unwrap();
    assert!(p2.ack_needed && p2.status.stored.is_none());
    assert_eq!(
        plan.starting_offsets(),
        BTreeMap::from([(0, 10), (1, 5)]),
        "stored + 1 for every partition with an offset"
    );
    let text = plan.render();
    assert!(
        text.contains("ack needed") && text.contains("BLOCKING"),
        "{text}"
    );

    // The binary exits non-zero on it, and zero once partition 2 is acknowledged.
    let config = f.write("plan.toml", &toml);
    let (ok, out) = run(&config, &["plan"]).await;
    assert!(!ok, "{out}");
    let acknowledged = f.toml(
        "tools",
        "kdi_compat = true",
        "starting_offsets = { \"2\" = 0, \"*\" = \"earliest\" }",
    );
    let config = f.write("plan-ack.toml", &acknowledged);
    let (ok, out) = run(&config, &["plan"]).await;
    assert!(ok, "{out}");
    assert!(out.contains("acknowledged at 0"), "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn clone_table_keeps_the_case_and_the_partition_columns() {
    let f = Fixture::bare(1).await;
    let mut columns = vec![
        StructField::new("MixedCase", Delta::Primitive(PrimitiveType::String), true),
        StructField::new("n", Delta::Primitive(PrimitiveType::Long), true),
        StructField::new("ingest_date", Delta::Primitive(PrimitiveType::String), true),
    ];
    columns.extend(event_columns());
    create_table_with(
        &f.target,
        columns,
        &["ingest_date"],
        &[("delta.checkpointInterval", "10")],
    )
    .await;
    create_dq(&f.dq).await;
    register(&f.subject(), EVENT_V1).await;
    let toml = f.toml("tools", "", "");
    let cfg = resolve(&toml, false);
    let to = f.dir.path().join("shadow");
    let to = to.to_str().unwrap();
    let version = clone::clone_table(&cfg, to).await.unwrap();
    assert_eq!(version, 0);

    let clone = open(to).await;
    let snapshot = clone.snapshot().unwrap();
    let names: Vec<String> = snapshot
        .schema()
        .fields()
        .map(|f| f.name().to_string())
        .collect();
    assert!(names.contains(&"MixedCase".to_string()), "{names:?}");
    assert_eq!(
        snapshot.metadata().partition_columns(),
        &["ingest_date".to_string()]
    );
    assert_eq!(
        snapshot
            .metadata()
            .configuration()
            .get("delta.checkpointInterval")
            .map(String::as_str),
        Some("10")
    );
    assert!(
        !snapshot
            .metadata()
            .configuration()
            .keys()
            .any(|k| k.contains("columnMapping")),
        "no column mapping"
    );
    assert_ne!(
        snapshot.metadata().id(),
        open(&f.target).await.snapshot().unwrap().metadata().id(),
        "a table of its own"
    );
    assert_eq!(snapshot.log_data().num_files(), 0);
    // Cloning again refuses: the table exists.
    assert!(clone::clone_table(&cfg, to).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn create_dq_makes_the_table_once() {
    let f = Fixture::bare(1).await;
    create_target(&f.target).await;
    let toml = f.toml("tools", "", "");
    let config = f.write("dq.toml", &toml);
    let (ok, out) = run(&config, &["create-dq"]).await;
    assert!(ok && out.contains("__ddi_dq"), "{out}");
    let names: Vec<String> = open(&f.dq)
        .await
        .snapshot()
        .unwrap()
        .schema()
        .fields()
        .map(|c| c.name().to_string())
        .collect();
    assert!(names.contains(&"reason".to_string()), "{names:?}");
    let (ok, _) = run(&config, &["create-dq"]).await;
    assert!(!ok, "the table exists");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn set_offsets_moves_guards_and_reasserts() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    for p in 0..2 {
        produce(&f, id, p, 40).await;
    }
    seed(&f, &[(0, 9), (1, 19)]).await;
    let toml = f.toml("tools", "kdi_compat = true", "");
    let cfg = resolve(&toml, false);

    // Forward over offsets the topic still holds: refused without a reason, written with one.
    let req = |next, skip: Option<&str>, replay| Request {
        moves: BTreeMap::from([(0, next)]),
        acknowledge_skip: skip.map(str::to_string),
        allow_replay: replay,
        ..Request::default()
    };
    let e = setoffsets::set_offsets(&cfg, &req(30, None, false))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("--acknowledge-skip"), "{e}");
    assert_eq!(
        txns(&f.target, &f.app_id, 2).await[&0],
        Some(9),
        "nothing written"
    );

    let out = setoffsets::set_offsets(&cfg, &req(30, Some("poison pill"), false))
        .await
        .unwrap();
    assert_eq!(out.moved[&0], (Some(9), 29));
    assert!(!out.checkpointed);
    let stored = txns(&f.target, &f.app_id, 2).await;
    assert_eq!((stored[&0], stored[&1]), (Some(29), Some(19)));
    // No takeover marker: in kdi's era only the named partition is acknowledged.
    let table = open(&f.target).await;
    let marker = delta_delta_ingest::fence::takeover_marker_app_id(&f.app_id);
    let read = delta_delta_ingest::fence::read_txns(
        &table,
        &[marker.clone()],
        &delta_delta_ingest::fence::LookupCounters::default(),
    )
    .await
    .unwrap();
    assert_eq!(read[&marker].version, None);
    let history = table.history(Some(1)).await.unwrap();
    let info = history.into_iter().next().unwrap();
    assert_eq!(
        info.info
            .get("ddi.kafka.offsetOverride")
            .and_then(|v| v.as_str()),
        Some("poison pill")
    );

    // Backwards: refused, then allowed, and checkpointed so a Trino checkpoint cannot undo it.
    let e = setoffsets::set_offsets(&cfg, &req(5, None, false))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("--allow-replay"), "{e}");
    let out = setoffsets::set_offsets(&cfg, &req(5, None, true))
        .await
        .unwrap();
    assert!(out.checkpointed);
    assert_eq!(txns(&f.target, &f.app_id, 2).await[&0], Some(4));
    let log = std::path::Path::new(&f.target).join("_delta_log");
    let checkpoints = std::fs::read_dir(&log)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains("checkpoint"))
        .count();
    assert!(checkpoints > 0, "a checkpoint at the move");

    // Reassert: every partition again at its own offset, neither forward nor back.
    let before = txns(&f.target, &f.app_id, 2).await;
    let out = setoffsets::set_offsets(
        &cfg,
        &Request {
            reassert: true,
            ..Request::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(out.moved[&0], (Some(4), 4));
    assert_eq!(out.moved[&1], (Some(19), 19));
    assert_eq!(txns(&f.target, &f.app_id, 2).await, before);

    // The binary: a partition that does not exist, and the all-partitions form.
    let config = f.write("so.toml", &toml);
    let (ok, out) = run(&config, &["set-offsets", "--partition", "9", "--next", "1"]).await;
    assert!(!ok && out.contains("no partition 9"), "{out}");
    let (ok, out) = run(
        &config,
        &[
            "set-offsets",
            "--all-partitions",
            "--next",
            "40",
            "--acknowledge-skip",
            "test",
        ],
    )
    .await;
    assert!(ok, "{out}");
    let stored = txns(&f.target, &f.app_id, 2).await;
    assert_eq!((stored[&0], stored[&1]), (Some(39), Some(39)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn set_offsets_is_refused_on_a_fresh_app_id() {
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    produce(&f, id, 0, 5).await;
    let toml = f.toml("tools", "", "");
    let cfg = resolve(&toml, false);
    let e = setoffsets::set_offsets(
        &cfg,
        &Request {
            moves: BTreeMap::from([(0, 3)]),
            acknowledge_skip: Some("x".into()),
            ..Request::default()
        },
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("starting_offsets"), "{e}");
    assert_eq!(txns(&f.target, &f.app_id, 1).await[&0], None);
    let _: BTreeSet<i32> = BTreeSet::new();
}
