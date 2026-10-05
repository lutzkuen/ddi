//! What the Kafka integration tests (tests/kafka_*.rs) share: a Redpanda with its built-in
//! Schema Registry, topics and producers, Avro framed as Confluent producers frame it, Delta
//! targets on the local filesystem, and the `ddi` binary run as `once` or as `run` members.
//!
//! Every test is `#[ignore]`d unless a broker is there to read. To run them:
//!
//! ```bash
//! docker run -d --rm --name ddi-redpanda --memory 1500m \
//!   -p 127.0.0.1:9092:9092 -p 127.0.0.1:8081:8081 redpandadata/redpanda:v26.2.3 \
//!   redpanda start --mode dev-container --smp 1 --memory 1G \
//!   --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr 127.0.0.1:9092 \
//!   --schema-registry-addr 0.0.0.0:8081
//! DDI_TEST_KAFKA=127.0.0.1:9092 DDI_TEST_SR=http://127.0.0.1:8081 \
//!   cargo test --features kafka,fault-injection --test 'kafka_*' -- --ignored --test-threads 1
//! ```
//!
//! Topics, app_ids and tables are named uniquely per test, so the tests share one broker.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use apache_avro::types::Value;
use deltalake::arrow::array::{Array, AsArray, RecordBatch};
use deltalake::arrow::datatypes::{Int32Type, Int64Type};
use deltalake::kernel::{DataType as Delta, PrimitiveType, StructField};
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, DeltaTable};
use futures::TryStreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::TopicPartitionList;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use delta_delta_ingest::config::{Config, ResolvedPipeline};

/// The broker every Kafka test reads, from `DDI_TEST_KAFKA`.
pub fn brokers() -> String {
    std::env::var("DDI_TEST_KAFKA")
        .expect("set DDI_TEST_KAFKA to a Kafka or Redpanda broker, e.g. 127.0.0.1:9092")
}

/// The Schema Registry, from `DDI_TEST_SR`; Redpanda's own on 8081 by default.
pub fn registry() -> String {
    std::env::var("DDI_TEST_SR").unwrap_or_else(|_| "http://127.0.0.1:8081".into())
}

/// A name no other test, and no earlier run, has used.
pub fn unique(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 1_000_000_000_000;
    format!(
        "{prefix}-{}-{nanos}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

// ------------------------------------------------------------------------------ topics

pub fn admin() -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .expect("an admin client")
}

fn options() -> AdminOptions {
    AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)))
}

pub async fn create_topic(topic: &str, partitions: i32) {
    create_topic_with(topic, partitions, &[]).await
}

pub async fn create_topic_with(topic: &str, partitions: i32, config: &[(&str, &str)]) {
    let mut t = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    for (k, v) in config {
        t = t.set(k, v);
    }
    let out = admin().create_topics([&t], &options()).await.unwrap();
    for r in out {
        r.unwrap_or_else(|(t, e)| panic!("create topic {t}: {e}"));
    }
    // Metadata takes a moment to reach every broker thread.
    wait_for_partitions(topic, partitions).await;
}

pub async fn delete_topic(topic: &str) {
    let out = admin().delete_topics(&[topic], &options()).await.unwrap();
    for r in out {
        r.unwrap_or_else(|(t, e)| panic!("delete topic {t}: {e}"));
    }
    // Gone from metadata before it is created again.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if partition_count(topic) == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("topic {topic} was not deleted");
}

pub async fn add_partitions(topic: &str, total: usize) {
    let out = admin()
        .create_partitions([&NewPartitions::new(topic, total)], &options())
        .await
        .unwrap();
    for r in out {
        r.unwrap_or_else(|(t, e)| panic!("add partitions to {t}: {e}"));
    }
    wait_for_partitions(topic, total as i32).await;
}

/// Delete partition `p`'s messages below `before`, as retention would (`rpk topic
/// trim-prefix`).
pub async fn trim(topic: &str, p: i32, before: i64) {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, p, rdkafka::Offset::Offset(before))
        .unwrap();
    let out = admin().delete_records(&tpl, &options()).await.unwrap();
    for e in out.elements() {
        e.error()
            .unwrap_or_else(|err| panic!("trim {topic}/{p} below {before}: {err}"));
    }
}

fn metadata_consumer() -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .unwrap()
}

pub fn partition_count(topic: &str) -> i32 {
    let c = metadata_consumer();
    match c.fetch_metadata(Some(topic), Duration::from_secs(10)) {
        Ok(m) => m
            .topics()
            .iter()
            .find(|t| t.name() == topic && t.error().is_none())
            .map_or(0, |t| t.partitions().len() as i32),
        Err(_) => 0,
    }
}

async fn wait_for_partitions(topic: &str, n: i32) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if partition_count(topic) == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("topic {topic} did not get {n} partitions");
}

/// `(low, high)` of one partition.
pub fn watermarks(topic: &str, p: i32) -> (i64, i64) {
    metadata_consumer()
        .fetch_watermarks(topic, p, Duration::from_secs(10))
        .unwrap()
}

// ------------------------------------------------------------------------------ producing

/// A producer that waits for every message to be acknowledged, so its offset is known.
pub struct Producer(FutureProducer);

impl Producer {
    pub fn new() -> Self {
        Self::with(&[])
    }

    pub fn with(settings: &[(&str, &str)]) -> Self {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", brokers())
            .set("acks", "all")
            .set("linger.ms", "0")
            .set("message.timeout.ms", "30000");
        for (k, v) in settings {
            c.set(*k, *v);
        }
        Self(c.create().expect("a producer"))
    }

    /// Send one message to partition `p`; its offset.
    pub async fn send(&self, topic: &str, p: i32, key: Option<&[u8]>, value: Option<&[u8]>) -> i64 {
        let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic).partition(p);
        if let Some(k) = key {
            record = record.key(k);
        }
        if let Some(v) = value {
            record = record.payload(v);
        }
        let (partition, offset) = self
            .0
            .send(record, Duration::from_secs(30))
            .await
            .map(|d| (d.partition, d.offset))
            .unwrap_or_else(|(e, _)| panic!("produce to {topic}/{p}: {e}"));
        assert_eq!(partition, p);
        offset
    }

    pub fn inner(&self) -> &FutureProducer {
        &self.0
    }
}

impl Default for Producer {
    fn default() -> Self {
        Self::new()
    }
}

// ------------------------------------------------------------------------------- Avro

/// The writer schema most tests produce with.
pub const EVENT_V1: &str = r#"{"type":"record","name":"Event","namespace":"ddi.test","fields":[
    {"name":"id","type":"long"},
    {"name":"name","type":["null","string"],"default":null}
]}"#;

/// Register `schema` under `subject` in the registry; its id.
pub async fn register(subject: &str, schema: &str) -> u32 {
    let body = serde_json::json!({ "schema": schema }).to_string();
    let url = format!("{}/subjects/{subject}/versions", registry());
    let response = reqwest::Client::new()
        .post(url)
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .body(body)
        .send()
        .await
        .expect("the Schema Registry");
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "register {subject}: {status} {text}");
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    v["id"].as_u64().expect("an id") as u32
}

/// Allow any change between versions of `subject`, for tests that evolve a schema the way a
/// registry with `NONE` compatibility lets producers.
pub async fn compatibility_none(subject: &str) {
    let url = format!("{}/config/{subject}", registry());
    let response = reqwest::Client::new()
        .put(url)
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .body(r#"{"compatibility":"NONE"}"#)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{:?}",
        response.text().await
    );
}

/// `value` written with `schema`, framed as a Confluent producer frames it: the magic byte,
/// the schema id, the Avro body.
pub fn framed(schema: &str, id: u32, value: Value) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(schema).unwrap();
    let body = apache_avro::to_avro_datum(&schema, value).unwrap();
    let mut out = vec![0u8];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend(body);
    out
}

/// An `EVENT_V1` record.
pub fn event(id: i64, name: Option<&str>) -> Value {
    Value::Record(vec![
        ("id".into(), Value::Long(id)),
        (
            "name".into(),
            match name {
                Some(n) => Value::Union(1, Box::new(Value::String(n.into()))),
                None => Value::Union(0, Box::new(Value::Null)),
            },
        ),
    ])
}

// ------------------------------------------------------------------------------- Delta

/// The columns of the target most tests write: an event's fields and its coordinates.
pub fn event_columns() -> Vec<StructField> {
    vec![
        StructField::new("id", Delta::Primitive(PrimitiveType::Long), true),
        StructField::new("name", Delta::Primitive(PrimitiveType::String), true),
        StructField::new("kafka_topic", Delta::Primitive(PrimitiveType::String), true),
        StructField::new(
            "kafka_partition",
            Delta::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new("kafka_offset", Delta::Primitive(PrimitiveType::Long), true),
        StructField::new(
            "kafka_timestamp",
            Delta::Primitive(PrimitiveType::Timestamp),
            true,
        ),
    ]
}

pub async fn create_table_with(
    path: &str,
    columns: Vec<StructField>,
    partitions: &[&str],
    configuration: &[(&str, &str)],
) -> DeltaTable {
    DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(columns)
        .with_partition_columns(partitions.iter().map(|p| p.to_string()))
        .with_configuration(configuration.iter().map(|(k, v)| (*k, Some(*v))))
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap()
}

pub async fn create_target(path: &str) -> DeltaTable {
    create_table_with(path, event_columns(), &[], &[]).await
}

pub async fn create_dq(path: &str) -> DeltaTable {
    create_table_with(path, delta_delta_ingest::dq::columns(), &[], &[]).await
}

pub async fn open(path: &str) -> DeltaTable {
    deltalake::open_table(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
}

pub async fn scan(path: &str) -> Vec<RecordBatch> {
    let t = open(path).await;
    let (_, stream) = t.scan_table().await.unwrap();
    stream.try_collect().await.unwrap()
}

/// Value `i` of `column` as text, whatever string type the scan gave it; `None` for null.
pub fn text(column: &dyn Array, i: usize) -> Option<String> {
    let a = deltalake::arrow::compute::cast(
        &column.slice(0, column.len()),
        &deltalake::arrow::datatypes::DataType::Utf8,
    )
    .unwrap();
    let a = a.as_string::<i32>();
    a.is_valid(i).then(|| a.value(i).to_string())
}

/// Every `(kafka_partition, kafka_offset)` in the table, sorted, duplicates kept.
pub async fn coordinates(path: &str) -> Vec<(i32, i64)> {
    let mut out = Vec::new();
    for b in scan(path).await {
        let p = b.column_by_name("kafka_partition").unwrap();
        let o = b.column_by_name("kafka_offset").unwrap();
        let p = p.as_primitive::<Int32Type>();
        let o = o.as_primitive::<Int64Type>();
        for i in 0..b.num_rows() {
            out.push((p.value(i), o.value(i)));
        }
    }
    out.sort_unstable();
    out
}

/// The rows of the data-quality table: `(partition, offset, reason)`, sorted, duplicates
/// kept. The partition is read from the payload's `kafka_partition`.
pub async fn rejects(path: &str) -> Vec<(i64, i64, String)> {
    let mut out = Vec::new();
    for b in scan(path).await {
        let version = b.column_by_name("source_version").unwrap();
        let version = version.as_primitive::<Int64Type>();
        let reason = b.column_by_name("reason").unwrap();
        let payload = b.column_by_name("payload").unwrap();
        for i in 0..b.num_rows() {
            let p = text(payload.as_ref(), i)
                .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                .and_then(|v| v.get("kafka_partition").and_then(|p| p.as_i64()))
                .unwrap_or(-1);
            out.push((
                p,
                version.value(i),
                text(reason.as_ref(), i).unwrap_or_default(),
            ));
        }
    }
    out.sort();
    out
}

/// Whether `rows` holds each of `expected` exactly once and nothing else.
pub fn assert_exactly(rows: &[(i32, i64)], expected: &BTreeSet<(i32, i64)>) {
    let mut seen = BTreeSet::new();
    let mut dups = Vec::new();
    for r in rows {
        if !seen.insert(*r) {
            dups.push(*r);
        }
    }
    let missing: Vec<_> = expected.difference(&seen).take(20).collect();
    let extra: Vec<_> = seen.difference(expected).take(20).collect();
    assert!(
        dups.is_empty() && missing.is_empty() && extra.is_empty(),
        "{} rows, {} expected: duplicates {:?}, missing {:?} (first 20), extra {:?}",
        rows.len(),
        expected.len(),
        &dups[..dups.len().min(20)],
        missing,
        extra
    );
}

/// The offsets `app_id`'s txns hold in the table, by partition.
pub async fn txns(path: &str, app_id: &str, partitions: i32) -> BTreeMap<i32, Option<i64>> {
    let t = open(path).await;
    let ids: Vec<String> = (0..partitions)
        .map(|p| delta_delta_ingest::fence::txn_app_id(app_id, p))
        .collect();
    let read = delta_delta_ingest::fence::read_txns(
        &t,
        &ids,
        &delta_delta_ingest::fence::LookupCounters::default(),
    )
    .await
    .unwrap();
    (0..partitions)
        .map(|p| {
            let id = delta_delta_ingest::fence::txn_app_id(app_id, p);
            (p, read.get(&id).and_then(|t| t.version))
        })
        .collect()
}

/// The parquet files in a table's directory that no commit of its log adds: what a commit
/// that failed after its upload leaves behind.
pub async fn orphans(path: &str) -> usize {
    let t = open(path).await;
    let live: BTreeSet<String> = t
        .get_file_uris()
        .unwrap()
        .map(|u| u.rsplit('/').next().unwrap().to_string())
        .collect();
    let mut n = 0;
    for entry in walk(Path::new(path)) {
        let name = entry.file_name().unwrap().to_string_lossy().to_string();
        if name.ends_with(".parquet")
            && !entry.to_string_lossy().contains("_delta_log")
            && !live.contains(&name)
        {
            n += 1;
        }
    }
    n
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}

// ------------------------------------------------------------------------------ config

/// One Kafka pipeline's world: a topic, a target with its data-quality table, and a config.
pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub topic: String,
    pub app_id: String,
    pub target: String,
    pub dq: String,
    pub partitions: i32,
}

impl Fixture {
    /// A fresh topic of `partitions`, a target with the event columns, and its DQ table.
    pub async fn new(partitions: i32) -> Self {
        let f = Self::bare(partitions).await;
        create_target(&f.target).await;
        create_dq(&f.dq).await;
        f
    }

    /// A topic and paths, with no table created.
    pub async fn bare(partitions: i32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let topic = unique("t");
        create_topic(&topic, partitions).await;
        Self {
            target: root.join("bronze").to_str().unwrap().to_string(),
            dq: root.join("bronze__ddi_dq").to_str().unwrap().to_string(),
            app_id: unique("app"),
            topic,
            partitions,
            dir,
        }
    }

    pub fn subject(&self) -> String {
        format!("{}-value", self.topic)
    }

    /// The config of one Kafka pipeline `name` reading this fixture's topic, with `extra`
    /// lines in its `[[kafka.pipeline]]`, `section` lines in `[kafka]`.
    pub fn toml(&self, name: &str, section: &str, extra: &str) -> String {
        format!(
            r#"
[runtime]
allowed_latency_secs = 2

[kafka]
brokers = "{brokers}"
blocked_recheck_secs = 2
{section}

[kafka.schema_registry]
url = "{sr}"

[[kafka.pipeline]]
name = "{name}"
app_id = "{app}"
topic = "{topic}"
target_uri = "{target}"
group_id = "g-{app}"
{extra}
"#,
            brokers = brokers(),
            sr = registry(),
            app = self.app_id,
            topic = self.topic,
            target = self.target,
        )
    }

    /// Write `toml` next to the tables; its path.
    pub fn write(&self, file: &str, toml: &str) -> PathBuf {
        let p = self.dir.path().join(file);
        std::fs::write(&p, toml).unwrap();
        p
    }
}

/// The one Kafka pipeline `toml` describes, resolved; in once-mode when `once`.
pub fn resolve(toml: &str, once: bool) -> ResolvedPipeline {
    let r = Config::from_toml_str(toml).unwrap().resolve_all().unwrap();
    assert!(r.rejected.is_empty(), "{:?}", r.rejected);
    let mut p = r
        .pipelines
        .into_iter()
        .find(|p| p.kafka.is_some())
        .expect("a Kafka pipeline");
    Arc::make_mut(p.kafka.as_mut().unwrap()).once = once;
    p
}

// ------------------------------------------------------------------------------ binary

pub fn ddi() -> tokio::process::Command {
    let mut c = tokio::process::Command::new(env!("CARGO_BIN_EXE_ddi"));
    c.env("RUST_LOG", "warn,delta_delta_ingest=info")
        .env_remove("DDI_TEST_FAULT")
        .kill_on_drop(true);
    c
}

/// `ddi once` to the end; its output, stdout and stderr together.
pub async fn once(config: &Path) -> (bool, String) {
    once_with(config, &[]).await
}

pub async fn once_with(config: &Path, env: &[(&str, &str)]) -> (bool, String) {
    let mut c = ddi();
    c.arg("once").arg("--config").arg(config);
    for (k, v) in env {
        c.env(k, v);
    }
    let out = tokio::time::timeout(Duration::from_secs(300), c.output())
        .await
        .expect("ddi once finished within 5 minutes")
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

/// A port nothing listens on.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A `ddi run` member, with its metrics endpoint.
pub struct Member {
    pub child: tokio::process::Child,
    pub port: u16,
    pub log: PathBuf,
}

impl Member {
    pub fn start(config: &Path, log: &Path, env: &[(&str, &str)]) -> Self {
        let port = free_port();
        let file = std::fs::File::create(log).unwrap();
        let mut c = ddi();
        c.arg("run")
            .arg("--config")
            .arg(config)
            .arg("--metrics-addr")
            .arg(format!("127.0.0.1:{port}"))
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file));
        for (k, v) in env {
            c.env(k, v);
        }
        Self {
            child: c.spawn().expect("spawn ddi run"),
            port,
            log: log.to_path_buf(),
        }
    }

    /// The `/metrics` text, or empty while the member is not listening.
    pub async fn metrics(&self) -> String {
        let Ok(mut s) = tokio::net::TcpStream::connect(("127.0.0.1", self.port)).await else {
            return String::new();
        };
        let _ = s.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").await;
        let mut out = String::new();
        let _ = s.read_to_string(&mut out).await;
        out
    }

    /// The sum of a series' values whose line starts with `prefix` (a name and, optionally,
    /// the start of its labels).
    pub async fn metric(&self, prefix: &str) -> f64 {
        metric(&self.metrics().await, prefix)
    }

    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    pub fn sigterm(&self) {
        if let Some(pid) = self.child.id() {
            let _ = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
        }
    }

    pub fn signal(&self, signal: &str) {
        if let Some(pid) = self.child.id() {
            let _ = std::process::Command::new("kill")
                .arg(format!("-{signal}"))
                .arg(pid.to_string())
                .status();
        }
    }

    pub async fn kill(&mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    /// Wait for the member to exit by itself, up to `within`; whether it did.
    pub async fn exited(&mut self, within: Duration) -> bool {
        tokio::time::timeout(within, self.child.wait())
            .await
            .is_ok()
    }
}

/// The sum of the values of `text`'s series whose line starts with `prefix`.
pub fn metric(text: &str, prefix: &str) -> f64 {
    text.lines()
        .filter(|l| l.starts_with(prefix))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Wait until `check` holds, polling every 200 ms, up to `within`.
pub async fn eventually<F, Fut>(within: Duration, what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + within;
    loop {
        if check().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
