//! Taking a table over from the real kafka-delta-ingest, and handing it back (docs/kafka-design.md
//! §9.3, §9.4, §10): one corpus, produced twice, ingested by kdi alone and by kdi then ddi, and the
//! two tables compared row by row and column by column, partition values included.
//!
//! Needs a Redpanda (see tests/kafka_common/mod.rs) and a kdi binary: `DDI_TEST_KDI=<path>`. The
//! kdi built from the fork on delta-rs 0.26 is what the older streams of the production fleet run
//! (the rest run 0.29.4), so it is the stricter reader of what ddi writes.

#![cfg(feature = "kafka")]

mod common;
mod kafka_common;

use std::collections::BTreeMap;

use delta_delta_ingest::fence::{self, Era};
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};

use kafka_common::bronze::*;
use kafka_common::*;

/// The `-t` list of the production streams: the six standard transforms, the aliases the
/// entrypoint adds for camelCase fields, and `data` as text.
const TRANSFORMS: [&str; 11] = [
    "messagetime: epoch_millis_to_micro(messageTime)",
    "ingest_date: substr(epoch_millis_to_iso8601(messageTime),`0`,`10`)",
    "kafka_topic: kafka.topic",
    "kafka_partition: kafka.partition",
    "kafka_offset: kafka.offset",
    "kafka_timestamp: kafka.timestamp",
    "messageid: messageId",
    "correlationid: correlationId",
    "producerversion: producerVersion",
    "producerhost: producerHost",
    "data: to_string(data)",
];

fn kdi() -> String {
    std::env::var("DDI_TEST_KDI")
        .expect("set DDI_TEST_KDI to a kafka-delta-ingest binary (see tests/kafka_kdi.rs)")
}

/// Run kdi until its table holds every message the topic has now, then stop it: its output, and
/// whether it got there. (`-e` would end it by itself, but waits for the last message of every
/// partition to be one it processed, and did not end here after its first batch.)
async fn run_kdi(topic: &str, table: &str, app_id: &str) -> (bool, String) {
    let mut c = tokio::process::Command::new(kdi());
    c.arg("ingest")
        .arg(topic)
        .arg(table)
        .args(["-k", &brokers()])
        .args(["-g", &format!("kdi-{app_id}")])
        .args(["-a", app_id])
        .args(["-o", "earliest"])
        .args(["-l", "2", "-m", "5", "-b", "1000000", "-c"])
        .args(["--avro", &registry()])
        .env("RUST_LOG", "info")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    for t in TRANSFORMS {
        c.args(["-t", t]);
    }
    let want: Vec<i64> = (0..3).map(|p| watermarks(topic, p).1 - 1).collect();
    let mut child = c.spawn().expect("start kdi");
    let mut reached = false;
    for _ in 0..240 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let stored = txns(table, app_id, 3).await;
        if (0..3).all(|p| stored[&p].is_some_and(|o| o >= want[p as usize])) {
            reached = true;
            break;
        }
        if child.try_wait().ok().flatten().is_some() {
            break;
        }
    }
    let _ = child.start_kill();
    let out = child.wait_with_output().await.expect("kdi's output");
    let mut text = String::from_utf8_lossy(&out.stdout).to_string();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (reached, text)
}

/// Every row of a bronze table by `(kafka_partition, kafka_offset)`, each as `column = text`.
async fn rows(path: &str) -> BTreeMap<(i32, i64), BTreeMap<String, Option<String>>> {
    let mut out = BTreeMap::new();
    for b in scan(path).await {
        let schema = b.schema();
        for i in 0..b.num_rows() {
            let mut row = BTreeMap::new();
            for (c, field) in schema.fields().iter().enumerate() {
                row.insert(field.name().clone(), text(b.column(c).as_ref(), i));
            }
            let p: i32 = row["kafka_partition"].clone().unwrap().parse().unwrap();
            let o: i64 = row["kafka_offset"].clone().unwrap().parse().unwrap();
            assert!(
                out.insert((p, o), row).is_none(),
                "({p}, {o}) twice in {path}"
            );
        }
    }
    out
}

/// Produce `n` messages per partition to `topic`, starting at ids from `from`, over 3
/// partitions; some with the second writer schema, one tombstone per call at partition 2.
async fn produce(topic: &str, ids: (u32, u32), from: i64, n: i64) {
    let producer = Producer::new();
    // A tombstone first: kdi skips it, ddi steps over it, and neither writes a row. Never last:
    // kdi's `-e` waits for the last message of each partition to be one it wrote.
    producer.send(topic, 2, None, None).await;
    // 2026-10-13T23:59:59.9Z onwards: a few messages land on the next UTC day.
    let base = 1_791_935_999_000i64;
    for i in from..from + n {
        for p in 0..3 {
            let millis = base + i * 400 + p as i64;
            let id = format!("m-{p}-{i}");
            let corr = (i % 3 != 0).then(|| format!("c-{i}"));
            let fee = (i % 2 == 0).then(|| format!("{i}.50"));
            let bytes = if i % 4 == 1 {
                let v = message_v2(&id, millis, corr.as_deref(), fee.as_deref(), Some("web"));
                framed(ENVELOPE_V2, ids.1, v)
            } else {
                let v = message(&id, millis, corr.as_deref(), fee.as_deref());
                framed(ENVELOPE, ids.0, v)
            };
            producer.send(topic, p, None, Some(&bytes)).await;
        }
    }
}

/// ddi to the end of the topic, in once-mode, on `target`.
async fn ddi_once(f: &Fixture, target: &str, app_id: &str) {
    let toml = format!(
        r#"
[runtime]
allowed_latency_secs = 2

[kafka]
brokers = "{brokers}"
blocked_recheck_secs = 2

[kafka.schema_registry]
url = "{sr}"

[[kafka.pipeline]]
name = "bronze"
app_id = "{app_id}"
topic = "{topic}"
target_uri = "{target}"
group_id = "ddi-{app_id}"
kdi_compat = true
#kdi_null_text: inherited from kdi_compat
transform_sql = """{BRONZE_SQL}"""
"#,
        brokers = brokers(),
        sr = registry(),
        topic = f.topic,
    );
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    for _ in 0..2000 {
        if p.step().await.unwrap() == StepOutcome::CaughtUp {
            return;
        }
    }
    panic!("once-mode did not finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda and DDI_TEST_KDI"]
async fn ddi_takes_a_kdi_table_over_and_hands_it_back() {
    let f = Fixture::bare(3).await;
    let root = f.dir.path().to_str().unwrap().to_string();
    let (partial, full) = (format!("{root}/partial"), format!("{root}/full"));
    let app = "dat_orders";
    create_table_with(&partial, bronze_columns(), &["ingest_date"], &[]).await;
    create_table_with(&full, bronze_columns(), &["ingest_date"], &[]).await;
    create_dq(&format!("{partial}__ddi_dq")).await;
    let ids = (
        register(&f.subject(), ENVELOPE).await,
        register(&f.subject(), ENVELOPE_V2).await,
    );

    // kdi alone takes the first 40 messages of each partition, and stops.
    produce(&f.topic, ids, 0, 40).await;
    let (ok, log) = run_kdi(&f.topic, &partial, app).await;
    assert!(ok, "{log}");
    let stopped = txns(&partial, app, 3).await;
    assert!(stopped.values().all(|v| v.is_some()), "{stopped:?}");

    // The rest arrives, and a second kdi reads all of it into a table of its own.
    produce(&f.topic, ids, 40, 40).await;
    let (ok, log) = run_kdi(&f.topic, &full, app).await;
    assert!(ok, "{log}");

    // ddi takes the partial table over from where kdi stopped.
    ddi_once(&f, &partial, app).await;
    let t = open(&partial).await;
    let read = fence::read_txns(
        &t,
        &[fence::takeover_marker_app_id(app)],
        &fence::LookupCounters::default(),
    )
    .await
    .unwrap();
    assert!(
        read.values().next().unwrap().version.is_some(),
        "the takeover marker"
    );

    let (theirs, ours) = (rows(&full).await, rows(&partial).await);
    assert_eq!(theirs.len(), 240, "3 partitions of 80, tombstone excluded");
    let missing: Vec<_> = theirs.keys().filter(|k| !ours.contains_key(k)).collect();
    let extra: Vec<_> = ours.keys().filter(|k| !theirs.contains_key(k)).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "missing {missing:?}, extra {extra:?}"
    );
    for (key, row) in &theirs {
        assert_eq!(
            &ours[key], row,
            "row {key:?} differs between kdi alone and kdi then ddi"
        );
    }
    // The partition values too: the same files hold the same dates.
    let days = |m: &BTreeMap<(i32, i64), BTreeMap<String, Option<String>>>| {
        m.values()
            .map(|r| r["ingest_date"].clone().unwrap())
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(days(&theirs), days(&ours));
    assert!(
        days(&theirs).len() >= 2,
        "the corpus straddles midnight UTC"
    );

    // And back: kdi reads ddi's offsets (delta-rs 0.26 on 0.32's txns and checkpoints) and
    // adds exactly what arrived since, once.
    produce(&f.topic, ids, 80, 2).await;
    let before = rows(&partial).await.len();
    let (ok, log) = run_kdi(&f.topic, &partial, app).await;
    assert!(ok, "{log}");
    let after = rows(&partial).await;
    assert_eq!(
        after.len(),
        before + 6,
        "kdi adds the 6 new messages and no duplicate"
    );

    // ... and ddi takes it again, now in its own era: no takeover check, nothing re-read.
    ddi_once(&f, &partial, app).await;
    assert_eq!(rows(&partial).await.len(), before + 6);
    let t = open(&partial).await;
    let mut wanted: Vec<String> = (0..3).map(|p| fence::txn_app_id(app, p)).collect();
    wanted.push(fence::takeover_marker_app_id(app));
    let all = fence::read_txns(&t, &wanted, &fence::LookupCounters::default())
        .await
        .unwrap();
    assert_eq!(fence::era(app, &all), Era::Ddi);
}
