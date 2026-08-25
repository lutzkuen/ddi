//! Shared helpers for integration tests against real local-filesystem Delta tables.

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use deltalake::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, DeltaTable};

use delta_delta_ingest::config::ResolvedPipeline;
use delta_delta_ingest::publish::Envelope;
use delta_delta_ingest::source::ChangePolicy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub fn arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

pub fn delta_columns() -> Vec<StructField> {
    vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), false),
        StructField::new(
            "name",
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ]
}

pub fn batch(ids: &[i64]) -> RecordBatch {
    let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Create an empty Delta table at `path`.
pub async fn create_table(path: &str) -> DeltaTable {
    let url = ensure_table_uri(path).unwrap();
    DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(delta_columns())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap()
}

/// Open an existing table fresh from storage.
pub async fn open(path: &str) -> DeltaTable {
    let url = ensure_table_uri(path).unwrap();
    deltalake::open_table(url).await.unwrap()
}

/// Append one commit containing `ids`.
pub async fn append(path: &str, ids: &[i64]) -> DeltaTable {
    let t = open(path).await;
    t.write(vec![batch(ids)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

/// Read every `id` currently in the table.
pub async fn read_ids(path: &str) -> Vec<i64> {
    let t = open(path).await;
    let (_t, stream) = t.scan_table().await.unwrap();
    use futures::TryStreamExt;
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of("id").unwrap();
        let col = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id must be int64");
        for i in 0..col.len() {
            out.push(col.value(i));
        }
    }
    out.sort_unstable();
    out
}

pub fn has_duplicates(v: &[i64]) -> bool {
    let mut seen = HashSet::new();
    v.iter().any(|x| !seen.insert(*x))
}

/// A pipeline config wired to two local paths.
pub fn pipeline_cfg(name: &str, source: &str, target: &str) -> ResolvedPipeline {
    ResolvedPipeline {
        name: name.into(),
        app_id: format!("ddi.test.{name}"),
        source_uri: source.into(),
        target_uri: target.into(),
        lookups: vec![],
        starting_version: 0,
        change_policy: ChangePolicy::Fail,
        transform_sql: None,
        allowed_latency_secs: 1,
        near_time_poll_interval_secs: 1,
        max_bytes_per_batch: 256 * 1024 * 1024,
        max_files_per_batch: 1_000,
        max_output_rows_per_batch: 5_000_000,
        target_file_size: 128 * 1024 * 1024,
        watermark_uri: None,
        dedup_timestamp: None,
        dedup_key: None,
        write_mode: Default::default(),
        upsert_key: None,
        upsert_lookback: None,
        upsert_tiebreak: Vec::new(),
        upsert_grain_check: Default::default(),
        stage_for: None,
        dq_uri: None,
        source_relation: None,
        target_relation: None,
        publish: None,
        publish_to: None,
        storage: delta_delta_ingest::storage::Storage::default(),
    }
}

/// A temp dir plus source/target paths inside it.
pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub source: String,
    pub target: String,
}

impl Fixture {
    pub async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let source = root.join("source").to_str().unwrap().to_string();
        let target = root.join("target").to_str().unwrap().to_string();
        create_table(&source).await;
        create_table(&target).await;
        Self {
            dir,
            source,
            target,
        }
    }

    pub fn cfg(&self, name: &str) -> ResolvedPipeline {
        pipeline_cfg(name, &self.source, &self.target)
    }
}

// ---------------------------------------------------------------------------
// A stand-in for the Web PubSub data plane, shared by every test that drives a real
// `Publisher` (or `NearTimeReader`) against a real HTTP server rather than mocking the sink
// trait — the properties under test are about ordering and isolation, not transport.

/// What the far end should do with a request.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HubBehaviour {
    /// The documented success: 202 Accepted.
    Accept,
    /// A server-side failure.
    Fail,
    /// Accept the connection and never answer, so the client's timeout is what ends it.
    Hang,
}

/// One captured request: its full head, its body, and the status we answered with.
///
/// The whole head is kept, not just the start line: the Authorization header is the one part
/// of this request that no other test can observe, and without it here the header could be
/// deleted outright with the suite still green.
type Captured = Arc<Mutex<Vec<(String, Vec<u8>, u16)>>>;

/// A stand-in for the Web PubSub data plane.
///
/// Reads the request to its `Content-Length` in a loop rather than in a single read: a body
/// can arrive split across packets, and the envelope assertions are the point of the tests
/// that use this, so a truncated read would make them silently weak instead of failing.
pub struct FakeHub {
    pub addr: String,
    pub requests: Captured,
    pub hits: Arc<AtomicU64>,
    behaviour: Arc<AtomicU64>,
}

impl HubBehaviour {
    fn code(self) -> u64 {
        match self {
            HubBehaviour::Accept => 0,
            HubBehaviour::Fail => 1,
            HubBehaviour::Hang => 2,
        }
    }
    fn from_code(c: u64) -> Self {
        match c {
            1 => HubBehaviour::Fail,
            2 => HubBehaviour::Hang,
            _ => HubBehaviour::Accept,
        }
    }
}

impl FakeHub {
    pub async fn start(behaviour: HubBehaviour) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let hits = Arc::new(AtomicU64::new(0));
        let behaviour = Arc::new(AtomicU64::new(behaviour.code()));

        let sink = requests.clone();
        let counter = hits.clone();
        let mode = behaviour.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let sink = sink.clone();
                let counter = counter.clone();
                let mode = mode.clone();
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
                    let behaviour = HubBehaviour::from_code(mode.load(Ordering::SeqCst));
                    let status = match behaviour {
                        HubBehaviour::Accept => 202,
                        HubBehaviour::Fail => 500,
                        HubBehaviour::Hang => 0,
                    };
                    // Recorded before the response is written, so a test inspecting the
                    // capture after the client returned always sees it.
                    sink.lock()
                        .unwrap()
                        .push((head.clone(), buf[header_end..].to_vec(), status));

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
            behaviour,
        }
    }

    /// Change what the far end does from here on, so one test can lose exactly one message.
    pub fn set(&self, behaviour: HubBehaviour) {
        self.behaviour.store(behaviour.code(), Ordering::SeqCst);
    }

    /// Every envelope that arrived, whether or not it was accepted.
    pub fn envelopes(&self) -> Vec<Envelope> {
        self.parse(|_| true)
    }

    /// Only the envelopes a client would actually have received.
    pub fn delivered(&self) -> Vec<Envelope> {
        self.parse(|status| status == 202)
    }

    fn parse(&self, keep: impl Fn(u16) -> bool) -> Vec<Envelope> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, status)| keep(*status))
            .map(|(_, body, _)| {
                serde_json::from_slice(body).unwrap_or_else(|e| {
                    panic!(
                        "body was not a ddi envelope ({e}): {}",
                        String::from_utf8_lossy(body)
                    )
                })
            })
            .collect()
    }

    pub fn heads(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|(head, _, _)| head.clone())
            .collect()
    }

    pub fn request_lines(&self) -> Vec<String> {
        self.heads()
            .iter()
            .map(|h| h.lines().next().unwrap_or_default().to_string())
            .collect()
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
