//! A hand-rolled Schema Registry for tests: a `tokio` HTTP/1.1 server on a local port that
//! answers each request as the test says, and records what it was asked (docs/kafka-design.md
//! §9.1, "M2, registry"). No registry, no broker.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The media type of the Schema Registry API, which the stub answers with.
const SR_MEDIA_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// One request as the stub saw it.
#[derive(Clone, Debug)]
pub(crate) struct Seen {
    /// `GET /schemas/ids/7?deleted=true HTTP/1.1`, or an absolute URL through a proxy.
    pub(crate) line: String,
    /// Header names lower-cased.
    pub(crate) headers: Vec<(String, String)>,
}

impl Seen {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
    pub(crate) fn target(&self) -> &str {
        self.line.split(' ').nth(1).unwrap_or_default()
    }
}

/// A response: status and JSON body.
pub(crate) type Reply = (u16, String);

/// A hand-rolled HTTP/1.1 server answering every request with `reply`, one request per
/// connection, recording what it saw.
pub(crate) struct Stub {
    pub(crate) url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Stub {
    pub(crate) async fn start(reply: impl Fn(&Seen) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let reply = Arc::new(reply);
        let log = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let log = log.clone();
                let reply = reply.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&buf).to_string();
                    let mut lines = head.split("\r\n");
                    let line = lines.next().unwrap_or_default().to_string();
                    let headers = lines
                        .filter_map(|l| l.split_once(':'))
                        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                        .collect();
                    let seen = Seen { line, headers };
                    let (status, body) = reply(&seen);
                    log.lock().unwrap().push(seen);
                    let response = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: {SR_MEDIA_TYPE}\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        Self { url, seen }
    }

    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}
