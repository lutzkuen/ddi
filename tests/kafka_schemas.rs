//! Writer schemas that change, messages that cannot be decoded, and the data-quality table
//! (docs/kafka-design.md §9.2, tests 15-18).
//!
//! Ignored unless a broker is there: see tests/kafka_common/mod.rs for how to run them.

#![cfg(feature = "kafka")]

mod kafka_common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use apache_avro::types::Value;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use deltalake::arrow::array::{Array, AsArray};
use deltalake::arrow::datatypes::Int64Type;
use deltalake::kernel::{DataType as Delta, PrimitiveType, StructField};

use kafka_common::*;

const V2: &str = r#"{"type":"record","name":"Event","namespace":"ddi.test","fields":[
    {"name":"id","type":"long"},
    {"name":"name","type":["null","string"],"default":null},
    {"name":"score","type":["null","int"],"default":null}
]}"#;

/// v3 makes `name` a number: older rows whose name is not one cannot be cast.
const V3: &str = r#"{"type":"record","name":"Event","namespace":"ddi.test","fields":[
    {"name":"id","type":"long"},
    {"name":"name","type":["null","long"],"default":null},
    {"name":"score","type":["null","int"],"default":null}
]}"#;

fn v2(id: i64, name: &str, score: i32) -> Value {
    Value::Record(vec![
        ("id".into(), Value::Long(id)),
        (
            "name".into(),
            Value::Union(1, Box::new(Value::String(name.into()))),
        ),
        ("score".into(), Value::Union(1, Box::new(Value::Int(score)))),
    ])
}

fn v3(id: i64, name: i64, score: i32) -> Value {
    Value::Record(vec![
        ("id".into(), Value::Long(id)),
        ("name".into(), Value::Union(1, Box::new(Value::Long(name)))),
        ("score".into(), Value::Union(1, Box::new(Value::Int(score)))),
    ])
}

/// `id` and `score` of every row of a table, by id.
async fn scores(path: &str) -> BTreeMap<i64, Option<i64>> {
    let mut out = BTreeMap::new();
    for b in scan(path).await {
        let id = b.column_by_name("id").unwrap();
        let score = deltalake::arrow::compute::cast(
            b.column_by_name("score").unwrap(),
            &deltalake::arrow::datatypes::DataType::Int64,
        )
        .unwrap();
        let id = id.as_primitive::<Int64Type>();
        let score = score.as_primitive::<Int64Type>();
        for i in 0..b.num_rows() {
            out.insert(id.value(i), score.is_valid(i).then(|| score.value(i)));
        }
    }
    out
}

/// 15. v1, then v2 adding a field, then v3 changing a field's type, interleaved on one topic.
/// The identity path writes every message; the SQL path, over the union of the writer
/// schemas, reads NULL for the v2 field in v1 rows and sets aside the older rows whose value
/// the newest type cannot hold. Nothing stalls.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn schema_evolution() {
    let f = Fixture::new(2).await;
    compatibility_none(&f.subject()).await;
    let id1 = register(&f.subject(), EVENT_V1).await;
    let id2 = register(&f.subject(), V2).await;
    let id3 = register(&f.subject(), V3).await;
    let producer = Producer::new();
    let mut produced = BTreeSet::new();
    let mut unconvertible = BTreeSet::new();
    let mut v1_ids = BTreeSet::new();
    for n in 0..90i64 {
        let p = (n % 2) as i32;
        // Every third version in turn; a v1 or v2 name is a number only for odd n.
        let name = if n % 2 == 1 {
            n.to_string()
        } else {
            format!("e{n}")
        };
        let value = match n % 3 {
            0 => {
                v1_ids.insert(n);
                framed(EVENT_V1, id1, event(n, Some(&name)))
            }
            1 => framed(V2, id2, v2(n, &name, n as i32)),
            _ => framed(V3, id3, v3(n, n, n as i32)),
        };
        let o = producer.send(&f.topic, p, None, Some(&value)).await;
        produced.insert((p, o));
        if n % 3 != 2 && n % 2 == 0 {
            unconvertible.insert((p as i64, o));
        }
    }

    // The identity path, into a target with a STRING name.
    let mut columns = event_columns();
    columns.push(StructField::new(
        "score",
        Delta::Primitive(PrimitiveType::Integer),
        true,
    ));
    let identity = f.dir.path().join("identity").to_str().unwrap().to_string();
    create_table_with(&identity, columns, &[], &[]).await;
    let toml = f
        .toml("identity", "", "")
        .replace(&f.target, &identity)
        .replace(&f.app_id, &format!("{}-identity", f.app_id));
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    while p.step().await.unwrap() != StepOutcome::CaughtUp {}
    assert_exactly(&coordinates(&identity).await, &produced);
    let s = scores(&identity).await;
    assert!(
        v1_ids.iter().all(|i| s[i].is_none()),
        "v1 rows have no score"
    );

    // The SQL path, into a target whose name is a number, as v3 has it.
    let sql_target = f.dir.path().join("sql").to_str().unwrap().to_string();
    let sql_dq = format!("{sql_target}__ddi_dq");
    create_table_with(
        &sql_target,
        vec![
            StructField::new("id", Delta::Primitive(PrimitiveType::Long), true),
            StructField::new("name", Delta::Primitive(PrimitiveType::Long), true),
            StructField::new("score", Delta::Primitive(PrimitiveType::Long), true),
            StructField::new(
                "kafka_partition",
                Delta::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new("kafka_offset", Delta::Primitive(PrimitiveType::Long), true),
        ],
        &[],
        &[],
    )
    .await;
    create_dq(&sql_dq).await;
    let toml = f
        .toml(
            "sql",
            "",
            "transform_sql = \"SELECT id, name, CAST(score AS BIGINT) AS score, kafka_partition, \
             kafka_offset FROM source\"",
        )
        .replace(&f.target, &sql_target)
        .replace(&f.app_id, &format!("{}-sql", f.app_id));
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    while p.step().await.unwrap() != StepOutcome::CaughtUp {}
    let rows: BTreeSet<_> = coordinates(&sql_target).await.into_iter().collect();
    let rejected: BTreeSet<(i64, i64)> = rejects(&sql_dq)
        .await
        .into_iter()
        .map(|(p, o, _)| (p, o))
        .collect();
    assert_eq!(rejected, unconvertible);
    let expected: BTreeSet<(i32, i64)> = produced
        .iter()
        .copied()
        .filter(|(p, o)| !unconvertible.contains(&(*p as i64, *o)))
        .collect();
    assert_eq!(rows, expected);
    let s = scores(&sql_target).await;
    assert!(
        v1_ids.iter().filter_map(|i| s.get(i)).all(|v| v.is_none()),
        "the v2 field reads NULL in v1 rows"
    );
    assert!(s.values().any(|v| v.is_some()));
}

/// 16. A message that cannot be decoded, with no data-quality table: the pipeline holds
/// (`ddi_kafka_undecodable_blocking`, and `ddi_pipeline_up` 0) and stays in its group; once the
/// table is created the recheck resumes it, and the message is a row there with its partition,
/// offset and bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn undecodable_goes_to_dq_or_blocks() {
    let f = Fixture::bare(2).await;
    create_target(&f.target).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write("run.toml", &f.toml("undecodable", "", ""));
    let mut m = Member::start(&config, &f.dir.path().join("m.log"), &[]);
    eventually(
        Duration::from_secs(60),
        "the member to own every partition",
        || async { m.metric("ddi_kafka_assigned_partitions").await == 2.0 },
    )
    .await;
    let producer = Producer::new();
    let mut good = BTreeSet::new();
    for n in 0..6i64 {
        let o = producer
            .send(
                &f.topic,
                0,
                None,
                Some(&framed(EVENT_V1, id, event(n, None))),
            )
            .await;
        good.insert((0, o));
    }
    let bad = producer
        .send(&f.topic, 1, None, Some(b"{\"json\": true}"))
        .await;
    eventually(Duration::from_secs(60), "the pipeline to hold", || async {
        m.metric("ddi_kafka_undecodable_blocking").await == 1.0
    })
    .await;
    eventually(
        Duration::from_secs(10),
        "the pipeline to read down",
        || async { m.metric("ddi_pipeline_up").await == 0.0 },
    )
    .await;
    let rebalances = m.metric("ddi_kafka_rebalances_total").await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(coordinates(&f.target).await.is_empty());
    assert_eq!(
        m.metric("ddi_kafka_assigned_partitions").await,
        2.0,
        "it left its group"
    );
    assert_eq!(m.metric("ddi_kafka_rebalances_total").await, rebalances);

    create_dq(&f.dq).await;
    eventually(Duration::from_secs(60), "the recheck to commit", || async {
        coordinates(&f.target).await.len() == good.len()
    })
    .await;
    assert_exactly(&coordinates(&f.target).await, &good);
    let rows = scan(&f.dq).await;
    let payload = rows[0].column_by_name("payload").unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(&text(payload.as_ref(), 0).unwrap()).unwrap();
    assert_eq!(payload["kafka_partition"], 1);
    assert_eq!(payload["kafka_offset"], bad);
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload["kafka_value_b64"].as_str().unwrap())
        .unwrap();
    assert_eq!(bytes, b"{\"json\": true}");
    eventually(Duration::from_secs(10), "the gauge to clear", || async {
        m.metric("ddi_kafka_undecodable_blocking").await == 0.0
    })
    .await;
    assert_eq!(m.metric("ddi_pipeline_up").await, 1.0);
    m.kill().await;
}

/// 16. A batch held for an operator whose retry, once the cause is fixed, loses its commit to
/// another writer that moved one of its partitions (here an operator's set-offsets, landing while
/// the retry's commit waits): the batch is dropped, the hold ends with it, and the pipeline reads
/// on from the moved offset. It used to stay paused and blocked, with no batch to retry and no
/// recheck due, until a restart.
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn a_held_batch_dropped_for_another_writer_ends_the_hold() {
    use delta_delta_ingest::kafka::setoffsets::{self, Request};
    let f = Fixture::bare(1).await;
    create_target(&f.target).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let toml = f.toml("dropped", "", "");
    let config = f.write("run.toml", &toml);
    // Every target commit waits 8 s: the other writer commits in that time.
    let mut m = Member::start(
        &config,
        &f.dir.path().join("m.log"),
        &[("DDI_TEST_FAULT", "sleep_before_commit:8")],
    );
    eventually(
        Duration::from_secs(60),
        "the member to own the partition",
        || async { m.metric("ddi_kafka_assigned_partitions").await == 1.0 },
    )
    .await;
    let producer = Producer::new();
    let event_at = |n| framed(EVENT_V1, id, event(n, None));

    // A first commit, so that the partition has an offset to move.
    let mut expected = BTreeSet::new();
    for n in 0..3 {
        expected.insert((
            0,
            producer.send(&f.topic, 0, None, Some(&event_at(n))).await,
        ));
    }
    eventually(Duration::from_secs(60), "the first commit", || async {
        coordinates(&f.target).await.len() == expected.len()
    })
    .await;

    // A batch with an undecodable message and no data-quality table: held for an operator.
    for n in 3..5 {
        producer.send(&f.topic, 0, None, Some(&event_at(n))).await;
    }
    let bad = producer
        .send(&f.topic, 0, None, Some(b"{\"json\": true}"))
        .await;
    eventually(Duration::from_secs(60), "the pipeline to hold", || async {
        m.metric("ddi_kafka_undecodable_blocking").await == 1.0
    })
    .await;

    // The operator creates the table: the recheck retries the batch, which writes its reject
    // and then waits before its target commit, while the operator also moves the partition
    // past the batch.
    create_dq(&f.dq).await;
    eventually(
        Duration::from_secs(30),
        "the retry to write its reject",
        || async { rejects(&f.dq).await.len() == 1 },
    )
    .await;
    let moves = Request {
        moves: BTreeMap::from([(0, bad + 1)]),
        acknowledge_skip: Some("the test moves past the held batch".into()),
        ..Request::default()
    };
    let first_last = expected.iter().map(|(_, o)| *o).max().unwrap();
    let out = setoffsets::set_offsets(&resolve(&toml, false), &moves)
        .await
        .unwrap();
    // Inside the retry's commit wait, the partition still had the first commit's offset.
    assert_eq!(
        out.moved[&0],
        (Some(first_last), bad),
        "set-offsets landed after the retry's commit: the 8 s window was missed"
    );

    // The retry's commit conflicts and the batch is dropped ...
    eventually(
        Duration::from_secs(30),
        "the retry to lose its commit and drop the batch",
        || async { m.log_text().contains("the batch was dropped") },
    )
    .await;
    // ... and the pipeline goes on from the moved offset.
    let after = producer.send(&f.topic, 0, None, Some(&event_at(5))).await;
    expected.insert((0, after));
    eventually(
        Duration::from_secs(90),
        "a message after the moved offset to be committed",
        || async { coordinates(&f.target).await.contains(&(0, after)) },
    )
    .await;
    assert_exactly(&coordinates(&f.target).await, &expected);
    assert_eq!(rejects(&f.dq).await.len(), 1);
    assert_eq!(m.metric("ddi_kafka_pipeline_blocked").await, 0.0);
    assert_eq!(m.metric("ddi_pipeline_up").await, 1.0);
    m.kill().await;
}

/// 16. More undecodable messages in one batch than max_evaluation_rejects_per_batch: a
/// format or registry misconfiguration, not bad data, so the pipeline holds (once-mode fails),
/// and says which message was the first, by partition, offset and writer schema id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn undecodable_above_the_cap_blocks() {
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let producer = Producer::new();
    let mut offsets = Vec::new();
    for n in 0..5 {
        // Framed with the registered id, and cut inside its name.
        let mut truncated = framed(EVENT_V1, id, event(n, Some("a name")));
        truncated.truncate(truncated.len() - 2);
        offsets.push(producer.send(&f.topic, 0, None, Some(&truncated)).await);
    }
    let toml = f.toml("cap", "", "max_evaluation_rejects_per_batch = 2");
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    let e = loop {
        match p.step().await {
            Ok(StepOutcome::CaughtUp) => panic!("finished past the cap"),
            Ok(_) => {}
            Err(e) => break e,
        }
    };
    assert!(
        matches!(
            &e,
            delta_delta_ingest::Error::KafkaTooManyUndecodable {
                count: 5,
                cap: 2,
                partition: 0,
                offset,
                schema_id: Some(s),
                ..
            } if *offset == offsets[0] && *s == id
        ),
        "{e:?}"
    );
    assert!(
        e.to_string().contains(&format!(
            "the first is partition 0 offset {} of topic {:?} (writer schema id {id}): \
             truncated",
            offsets[0], f.topic
        )),
        "{e}"
    );
    assert!(rejects(&f.dq).await.is_empty());
}

/// 17. A crash between the data-quality write and the target commit (abort_after_dq): the
/// replay, with other batch boundaries, writes those rejects again. Every reject is in the
/// table, the crashed batch's twice, and none is missing (I9).
#[cfg(feature = "fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn dq_crash_between_dq_and_target() {
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let producer = Producer::new();
    let (mut good, mut bad) = (BTreeSet::new(), BTreeSet::new());
    for n in 0..60i64 {
        let p = (n % 2) as i32;
        if n % 7 == 3 {
            let o = producer.send(&f.topic, p, None, Some(b"\x01garbage")).await;
            bad.insert((i64::from(p), o));
        } else {
            let o = producer
                .send(
                    &f.topic,
                    p,
                    None,
                    Some(&framed(EVENT_V1, id, event(n, None))),
                )
                .await;
            good.insert((p, o));
        }
    }
    let crash = f.write(
        "crash.toml",
        &f.toml("dqcrash", "", "max_messages_per_batch = 20"),
    );
    let (ok, out) = once_with(&crash, &[("DDI_TEST_FAULT", "abort_after_dq")]).await;
    assert!(!ok, "the fault did not abort: {out}");
    let crashed: BTreeSet<(i64, i64)> = rejects(&f.dq)
        .await
        .into_iter()
        .map(|(p, o, _)| (p, o))
        .collect();
    assert!(
        !crashed.is_empty(),
        "nothing reached the data-quality table before the abort"
    );
    assert!(
        coordinates(&f.target).await.is_empty(),
        "the target committed past the abort"
    );

    let replay = f.write(
        "replay.toml",
        &f.toml("dqcrash", "", "max_messages_per_batch = 13"),
    );
    let (ok, out) = once(&replay).await;
    assert!(ok, "{out}");
    assert_exactly(&coordinates(&f.target).await, &good);
    let mut counts: BTreeMap<(i64, i64), usize> = BTreeMap::new();
    for (p, o, _) in rejects(&f.dq).await {
        *counts.entry((p, o)).or_default() += 1;
    }
    let seen: BTreeSet<_> = counts.keys().copied().collect();
    assert_eq!(
        seen, bad,
        "a reject is missing, or one is not a message that failed"
    );
    for (k, n) in &counts {
        assert_eq!(*n, if crashed.contains(k) { 2 } else { 1 }, "{k:?}");
    }
}

/// 18. Two members of one group writing rejects for minutes: plain appends that never
/// contend (I21) — no step error, no conflict retried, no rebalance, every reject once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn two_members_dq_rejects_concurrently() {
    let f = Arc::new(Fixture::new(4).await);
    let id = register(&f.subject(), EVENT_V1).await;
    let config = f.write(
        "run.toml",
        &f.toml("dqpair", "", "allowed_latency_secs = 1"),
    );
    let log = |n: &str| f.dir.path().join(format!("{n}.log"));
    // Taken over first: the first commit under a fresh app_id writes the takeover marker, and
    // two members racing to write it is one ConcurrentTransaction and one replay (§4.3), which
    // writes that batch's rejects again. Here nothing should replay.
    let mut a = Member::start(&config, &log("a"), &[]);
    let first = Producer::new()
        .send(
            &f.topic,
            0,
            None,
            Some(&framed(EVENT_V1, id, event(-1, None))),
        )
        .await;
    eventually(Duration::from_secs(60), "the takeover", || async {
        coordinates(&f.target).await.len() == 1
    })
    .await;
    let mut b = Member::start(&config, &log("b"), &[]);
    eventually(
        Duration::from_secs(60),
        "both members to own partitions",
        || async {
            a.metric("ddi_kafka_assigned_partitions").await == 2.0
                && b.metric("ddi_kafka_assigned_partitions").await == 2.0
        },
    )
    .await;
    let rebalances =
        a.metric("ddi_kafka_rebalances_total").await + b.metric("ddi_kafka_rebalances_total").await;
    let stop = Arc::new(AtomicBool::new(false));
    let producing = {
        let (f, stop) = (f.clone(), stop.clone());
        tokio::spawn(async move {
            let producer = Producer::new();
            let (mut good, mut bad) = (BTreeSet::new(), BTreeSet::new());
            let mut n: i64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let p = (n % 4) as i32;
                if n % 5 == 0 {
                    let o = producer.send(&f.topic, p, None, Some(b"bad")).await;
                    bad.insert((i64::from(p), o));
                } else {
                    let o = producer
                        .send(
                            &f.topic,
                            p,
                            None,
                            Some(&framed(EVENT_V1, id, event(n, None))),
                        )
                        .await;
                    good.insert((p, o));
                }
                n += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            (good, bad)
        })
    };
    tokio::time::sleep(Duration::from_secs(120)).await;
    stop.store(true, Ordering::Relaxed);
    let (mut good, bad) = producing.await.unwrap();
    good.insert((0, first));
    eventually(Duration::from_secs(60), "every message", || async {
        coordinates(&f.target).await.len() == good.len()
    })
    .await;
    let rejected = rejects(&f.dq).await;
    let unique: BTreeSet<(i64, i64)> = rejected.iter().map(|(p, o, _)| (*p, *o)).collect();
    assert_eq!(
        unique.len(),
        rejected.len(),
        "a reject was written twice; fenced: {} and {}",
        a.metric("ddi_kafka_fenced_total").await,
        b.metric("ddi_kafka_fenced_total").await
    );
    assert_eq!(unique, bad);
    assert_exactly(&coordinates(&f.target).await, &good);
    for m in [&a, &b] {
        assert_eq!(
            m.metric("ddi_kafka_fenced_total").await,
            0.0,
            "{}",
            m.log_text()
        );
        assert_eq!(m.metric("ddi_kafka_dq_conflict_retries_total").await, 0.0);
        assert_eq!(m.metric("ddi_errors_total").await, 0.0, "{}", m.log_text());
    }
    assert_eq!(
        a.metric("ddi_kafka_rebalances_total").await + b.metric("ddi_kafka_rebalances_total").await,
        rebalances,
        "the members rebalanced"
    );
    a.kill().await;
    b.kill().await;
}
