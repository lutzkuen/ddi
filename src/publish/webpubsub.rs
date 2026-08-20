//! Azure Web PubSub, spoken directly over its data-plane REST API.
//!
//! One request shape — send these bytes to that group — so this speaks the protocol rather
//! than taking a vendor SDK, the same trade [`crate::trino`] makes for its one query shape.
//!
//! Everything here is outbound. `ddi` never holds a browser's WebSocket, never mints a
//! client access token, and never serves a negotiate endpoint: those would make the daemon
//! authenticate dashboard users, a concept it does not have, and would turn a compromise of
//! it into a token-minting oracle. The dashboard's own web app owns that side.
//!
//! Protocol facts below come from the data-plane OpenAPI spec (stable, `2024-12-01`) and
//! Microsoft Learn's reference for it, not from this repository.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::publish::jwt;

/// The API version every request must carry. It is a required query parameter, and it is
/// also part of the URL the token's `aud` claim is compared against — so the two are built
/// from one string rather than two.
const API_VERSION: &str = "2024-12-01";

/// How long a minted token is valid. Short because it is per request anyway: `aud` pins it
/// to one URL, so nothing is cached and a longer life would buy nothing.
const TOKEN_TTL_SECS: i64 = 300;

/// An endpoint and the key that signs requests to it.
#[derive(Clone)]
pub struct Connection {
    pub endpoint: String,
    pub access_key: String,
}

/// Never renders the access key. This type is held by a publisher that is held by a
/// pipeline, and pipelines get logged.
impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("endpoint", &self.endpoint)
            .field("access_key", &"<redacted>")
            .finish()
    }
}

impl Connection {
    /// Parse the portal's connection string:
    /// `Endpoint=https://x.webpubsub.azure.com;AccessKey=<key>;Version=1.0;`
    ///
    /// Each entry is split on its **first** `=` only. A base64 access key ends in `=`
    /// padding, so splitting on every `=` truncates the key into something that yields a
    /// clean-looking 401 — a real bug, and the reason this is not a one-liner. Keys are
    /// matched case-insensitively, and `Version` is parsed and ignored exactly as every
    /// Azure SDK ignores it.
    pub fn parse(s: &str) -> Result<Self> {
        let mut endpoint = None;
        let mut access_key = None;

        for entry in s.split(';') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((key, value)) = entry.split_once('=') else {
                continue;
            };
            match key.trim().to_ascii_lowercase().as_str() {
                "endpoint" => endpoint = Some(value.trim().to_string()),
                "accesskey" => access_key = Some(value.trim().to_string()),
                // `Port` and `Version` are recognised spellings we have nothing to do with.
                _ => {}
            }
        }

        let endpoint = endpoint.ok_or_else(|| {
            Error::Config(
                "Web PubSub connection string has no Endpoint=; it should read \
                 Endpoint=https://<name>.webpubsub.azure.com;AccessKey=<key>;"
                    .into(),
            )
        })?;
        let access_key = access_key.ok_or_else(|| {
            Error::Config(
                "Web PubSub connection string has no AccessKey=; it should read \
                 Endpoint=https://<name>.webpubsub.azure.com;AccessKey=<key>;"
                    .into(),
            )
        })?;
        if access_key.is_empty() {
            return Err(Error::Config(
                "Web PubSub connection string has an empty AccessKey".into(),
            ));
        }

        // The SDKs prefix a scheme-less endpoint rather than refusing it. Keeping that
        // behaviour also means a test can point at `http://127.0.0.1:PORT`.
        let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint
        } else {
            format!("https://{endpoint}")
        };

        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            access_key,
        })
    }
}

/// Sends one message per committed batch to one group.
pub struct WebPubSubPublisher {
    conn: Connection,
    hub: String,
    message_ttl_secs: u32,
    http: reqwest::Client,
}

impl WebPubSubPublisher {
    pub fn new(
        conn: Connection,
        hub: &str,
        message_ttl_secs: u32,
        timeout: Duration,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| {
                Error::Config(format!("could not build the publisher's HTTP client: {e}"))
            })?;
        Ok(Self {
            conn,
            hub: hub.to_string(),
            message_ttl_secs,
            http,
        })
    }

    /// A short, safe description for logs and errors. Never includes the access key.
    pub fn describe(&self) -> String {
        format!("webpubsub {}/hubs/{}", self.conn.endpoint, self.hub)
    }

    /// The absolute URL for a send, which is also the token's audience.
    ///
    /// Built by hand rather than with a URL builder for one reason: the literal `:send`
    /// segment is Azure's action convention, and a percent-encoding pass would turn it into
    /// `%3Asend`. The hub and group are constrained where they are declared — the dbt gate
    /// restricts a group to `[A-Za-z0-9._:-]` — so nothing here needs escaping, and this
    /// asserts that rather than assuming it.
    fn send_url(&self, group: &str) -> Result<String> {
        fn safe(s: &str) -> bool {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':'))
        }
        if !safe(&self.hub) {
            return Err(Error::Config(format!(
                "[publish].hub {:?} has characters that would have to be escaped into the \
                 request path; use letters, digits, and . _ - :",
                self.hub
            )));
        }
        if !safe(group) {
            return Err(Error::Config(format!(
                "publish group {group:?} has characters that would have to be escaped into \
                 the request path; use letters, digits, and . _ - :"
            )));
        }
        let mut url = format!(
            "{}/api/hubs/{}/groups/{}/:send?api-version={API_VERSION}",
            self.conn.endpoint, self.hub, group
        );
        // 0 means "never expires" to the service, so it is only sent when it says something.
        if self.message_ttl_secs > 0 {
            url.push_str(&format!("&messageTtlSeconds={}", self.message_ttl_secs));
        }
        Ok(url)
    }

    /// POST one JSON payload to one group.
    ///
    /// Returns `Ok(())` on any 2xx. The documented success is **202 Accepted** and only
    /// that, but a status check rather than an equality check means a future 200 does not
    /// read as an outage; the status is logged either way so a change is visible.
    ///
    /// A 202 is **not** delivery. A group nobody has joined accepts the message and discards
    /// it, so nothing derived from this return value may be described as a delivery count.
    pub async fn send_json(&self, group: &str, body: Vec<u8>, now_unix: i64) -> Result<u16> {
        let url = self.send_url(group)?;
        let token = jwt::bearer(&self.conn.access_key, &url, now_unix, TOKEN_TTL_SECS);

        let response = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {token}"))
            .body(body)
            .send()
            .await
            .map_err(|e| {
                // `e` renders the URL but never a header, so the token cannot reach a log
                // through here.
                Error::Config(format!("publishing to {} failed: {e}", self.describe()))
            })?;

        let status = response.status();
        if status.is_success() {
            return Ok(status.as_u16());
        }

        // The error body carries {code,message,target,details,inner} plus an x-ms-error-code
        // header. Truncated because it is going into a log line on a path that may be
        // repeating once per batch.
        let code = response
            .headers()
            .get("x-ms-error-code")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let mut body = response.text().await.unwrap_or_default();
        body.truncate(300);
        Err(Error::Config(format!(
            "publishing to {} returned {status}{}{}",
            self.describe(),
            if code.is_empty() {
                String::new()
            } else {
                format!(" ({code})")
            },
            if body.is_empty() {
                String::new()
            } else {
                format!(": {body}")
            }
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publisher(endpoint: &str, ttl: u32) -> WebPubSubPublisher {
        WebPubSubPublisher::new(
            Connection {
                endpoint: endpoint.into(),
                access_key: "k".into(),
            },
            "ddi",
            ttl,
            Duration::from_secs(5),
        )
        .unwrap()
    }

    #[test]
    fn a_connection_string_splits_on_the_first_equals_only() {
        // The bug this is written against: a base64 access key ends in '=' padding, and
        // splitting on every '=' silently truncates it.
        let c = Connection::parse(
            "Endpoint=https://x.webpubsub.azure.com;AccessKey=c2VjcmV0LWtleQ==;Version=1.0;",
        )
        .unwrap();
        assert_eq!(c.endpoint, "https://x.webpubsub.azure.com");
        assert_eq!(c.access_key, "c2VjcmV0LWtleQ==");
    }

    #[test]
    fn connection_string_keys_are_case_insensitive() {
        let c = Connection::parse("endpoint=https://x.webpubsub.azure.com;accesskey=k").unwrap();
        assert_eq!(c.access_key, "k");
    }

    #[test]
    fn a_scheme_less_endpoint_is_assumed_https() {
        let c = Connection::parse("Endpoint=x.webpubsub.azure.com;AccessKey=k").unwrap();
        assert_eq!(c.endpoint, "https://x.webpubsub.azure.com");
    }

    #[test]
    fn a_local_http_endpoint_survives_parsing() {
        // Which is what lets an integration test point this at a fake hub.
        let c = Connection::parse("Endpoint=http://127.0.0.1:9099;AccessKey=k").unwrap();
        assert_eq!(c.endpoint, "http://127.0.0.1:9099");
    }

    #[test]
    fn a_connection_string_missing_a_part_names_the_form_it_wanted() {
        let e = Connection::parse("AccessKey=k").unwrap_err().to_string();
        assert!(e.contains("Endpoint="), "got: {e}");
        let e = Connection::parse("Endpoint=https://x")
            .unwrap_err()
            .to_string();
        assert!(e.contains("AccessKey="), "got: {e}");
    }

    #[test]
    fn the_access_key_never_appears_in_a_description_or_debug_rendering() {
        let conn = Connection::parse("Endpoint=https://x;AccessKey=hunter2").unwrap();
        assert!(
            !format!("{conn:?}").contains("hunter2"),
            "Debug leaks the key"
        );

        let p = WebPubSubPublisher::new(conn, "ddi", 60, Duration::from_secs(5)).unwrap();
        // `describe` goes into a startup log line and into every publish failure.
        assert!(!p.describe().contains("hunter2"), "got: {}", p.describe());
        assert!(
            p.describe().contains("https://x"),
            "but still says where: {}",
            p.describe()
        );
    }

    #[test]
    fn the_request_url_matches_the_data_plane_spec() {
        // POST {endpoint}/api/hubs/{hub}/groups/{group}/:send?api-version=...
        let url = publisher("https://x.webpubsub.azure.com", 60)
            .send_url("sales")
            .unwrap();
        assert_eq!(
            url,
            "https://x.webpubsub.azure.com/api/hubs/ddi/groups/sales/:send\
             ?api-version=2024-12-01&messageTtlSeconds=60"
        );
    }

    #[test]
    fn the_send_action_segment_is_not_percent_encoded() {
        let url = publisher("https://x.webpubsub.azure.com", 0)
            .send_url("sales")
            .unwrap();
        assert!(
            url.contains("/:send?"),
            "the colon is Azure's own convention: {url}"
        );
        assert!(!url.contains("%3A"), "got: {url}");
    }

    #[test]
    fn a_zero_ttl_is_omitted_because_the_service_reads_it_as_never_expires() {
        let url = publisher("https://x.webpubsub.azure.com", 0)
            .send_url("sales")
            .unwrap();
        assert!(!url.contains("messageTtlSeconds"), "got: {url}");
    }

    #[test]
    fn a_group_that_would_need_escaping_is_refused_rather_than_mangled() {
        let e = publisher("https://x.webpubsub.azure.com", 60)
            .send_url("sales/eu")
            .unwrap_err()
            .to_string();
        assert!(e.contains("escaped"), "got: {e}");
    }
}
