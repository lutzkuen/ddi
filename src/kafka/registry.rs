//! The Confluent Schema Registry client (docs/kafka-design.md §5.2).
//!
//! Two requests, and nothing else of the registry's API:
//! - `GET {url}/schemas/ids/{id}?deleted=true`, the writer schema a message names in its
//!   header. kafka-delta-ingest asked for soft-deleted schemas too, through
//!   schema_registry_converter 4.10.0 (docs/kafka-design.md §0.3 fact 3, to be checked against
//!   the deployed converter in M-1), and so does ddi: a schema deleted after its messages were
//!   written still decodes them.
//! - `GET {url}/subjects/{subject}/versions/latest`, the subject's newest version, which the SQL
//!   path's union `U` includes so that SQL naming a just-added field plans before the first
//!   message with it arrives (§5.5). Only [`SubjectSchemas`] asks for it.
//!
//! Not `schema_registry_converter` itself: it defaults to native-tls, pulls reqwest 0.13 and a
//! second apache-avro, and brings a JSON-schema validator, for two GETs.
//!
//! # What an answer means
//!
//! | Answer | Here |
//! |---|---|
//! | 2xx, `schemaType` absent or `AVRO` | [`Lookup::Avro`], cached for the life of the process |
//! | 2xx, `schemaType` another | [`Lookup::Undecodable`], deterministic |
//! | 2xx with `references` | [`Error::KafkaSchemaReferences`]: holds the pipeline |
//! | 404 with the registry's error body (40403) | [`Lookup::Undecodable`], not deterministic: the id may be registered later |
//! | 401, 403 | [`SchemaRegistryFailure::Unauthorized`]: consumer-fatal |
//! | 5xx, 429, 408, a timeout or no connection | retried after 100 ms, 500 ms and 2 s, then [`SchemaRegistryFailure::Unavailable`] |
//! | a 404 without the registry's error body, and anything else | [`SchemaRegistryFailure::Refused`]: holds the pipeline |
//!
//! # Credentials and network
//!
//! kdi's environment variables, read once when the client is built: basic auth from
//! `username_env`, with `password_env` optional as in kdi (serialization.rs:96-101); a proxy
//! for every request from `proxy_env` (kdi used `reqwest::Proxy::all` too), which otherwise
//! leaves reqwest's own `HTTP(S)_PROXY` handling as kdi had it; a private CA from
//! `ca_pem_env`, added to the webpki roots rustls trusts (kdi used the operating system's store
//! through native-tls). 30 s per request.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::OnceCell;
use tracing::{debug, warn};

use crate::config::kafka::ResolvedSchemaRegistry;
use crate::error::{Error, Result, SchemaRegistryFailure};

/// How long one request may take, connect and read together (as kdi's converter, 30 s).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// The waits before the retries of a request that met an outage: three retries after the first
/// try, then the outage is the caller's (§5.2).
const RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(500),
    Duration::from_secs(2),
];
/// How long a schema id the registry did not know is taken as still unknown without asking
/// again. A message naming one is undecodable either way; this only spares the registry one
/// request per such message, which a topic full of them would otherwise send.
const NOT_FOUND_TTL: Duration = Duration::from_secs(10);
/// The media type of the Schema Registry API.
const SR_MEDIA_TYPE: &str = "application/vnd.schemaregistry.v1+json";

// ------------------------------------------------------------------------------- answers

/// A schema as the registry holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredSchema {
    /// Its id, the one Confluent-framed messages carry in their header.
    pub id: u32,
    /// The schema itself, as JSON text.
    pub schema: String,
    /// `None` when the registry left `schemaType` out, which means AVRO.
    pub schema_type: Option<String>,
    /// Other subjects' schemas this one refers to.
    pub references: Vec<SchemaReference>,
    /// Set when the answer named them: a subject's latest version always does.
    pub subject: Option<String>,
    pub version: Option<i32>,
}

impl RegisteredSchema {
    /// Whether this is an Avro schema: `schemaType` absent (the registry's default) or `AVRO`.
    pub fn is_avro(&self) -> bool {
        self.schema_type.as_deref().is_none_or(|t| t == "AVRO")
    }
}

/// A reference to another subject's schema.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

/// What a writer schema id turned out to be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// An Avro schema without references: messages written with it can be decoded.
    Avro(Arc<RegisteredSchema>),
    /// Messages naming this id cannot be decoded, and go to the data-quality table.
    Undecodable {
        reason: String,
        /// Whether asking again could ever give another answer: false for an id the registry
        /// does not know (yet), true for a schema that is not Avro.
        deterministic: bool,
    },
}

/// Where a decoder gets the writer schema a message names: the registry, or in tests a map.
#[async_trait]
pub trait WriterSchemas: Send + Sync {
    /// The writer schema with this id. An error is the registry's, never the message's: the
    /// message is kept and decoded once the cause clears.
    async fn writer_schema(&self, id: u32) -> Result<Lookup>;
}

// --------------------------------------------------------------------------------- cache

/// Writer schemas by `(registry URL, id)`, shared by every pipeline of the process.
///
/// Ids are immutable, so an answer is kept for the life of the process, and only one request is
/// ever made per id: callers that ask for an id while it is being fetched wait for that fetch.
/// A failed fetch keeps nothing, so the next caller asks again. An id the registry did not know
/// is remembered for [`NOT_FOUND_TTL`] only.
#[derive(Debug, Default)]
pub struct SchemaCache {
    ids: Mutex<HashMap<CacheKey, Arc<OnceCell<Arc<RegisteredSchema>>>>>,
    not_found: Mutex<HashMap<CacheKey, (Instant, String)>>,
}

/// A registry's URL (its trailing `/` trimmed) and a schema id.
type CacheKey = (String, u32);

impl SchemaCache {
    /// The process-wide cache.
    pub fn global() -> Arc<SchemaCache> {
        static GLOBAL: OnceLock<Arc<SchemaCache>> = OnceLock::new();
        GLOBAL.get_or_init(Arc::default).clone()
    }

    fn cell(&self, key: &CacheKey) -> Arc<OnceCell<Arc<RegisteredSchema>>> {
        let mut ids = self.ids.lock().unwrap_or_else(|e| e.into_inner());
        ids.entry(key.clone()).or_default().clone()
    }

    fn not_found_since(&self, key: &CacheKey, ttl: Duration) -> Option<String> {
        let mut nf = self.not_found.lock().unwrap_or_else(|e| e.into_inner());
        match nf.get(key) {
            Some((at, detail)) if at.elapsed() < ttl => Some(detail.clone()),
            Some(_) => {
                nf.remove(key);
                None
            }
            None => None,
        }
    }

    fn remember_not_found(&self, key: CacheKey, detail: String) {
        let mut nf = self.not_found.lock().unwrap_or_else(|e| e.into_inner());
        nf.insert(key, (Instant::now(), detail));
    }

    /// Keeps a schema learnt another way (a subject's latest version), unless the id is
    /// already known.
    fn offer(&self, url: &str, schema: &Arc<RegisteredSchema>) {
        let _ = self.cell(&(url.to_string(), schema.id)).set(schema.clone());
    }
}

// -------------------------------------------------------------------------------- client

/// One Schema Registry, as a pipeline reaches it: its URL, credentials, proxy and trust.
pub struct SchemaRegistry {
    /// The URL with its trailing `/` trimmed (kdi serialization.rs:91-94): what errors print,
    /// and the cache's key.
    url: String,
    base: url::Url,
    client: reqwest::Client,
    /// Username and optional password.
    auth: Option<(String, Option<String>)>,
    retry_delays: Vec<Duration>,
    cache: Arc<SchemaCache>,
    /// Told the outcome of every request, for `ddi_kafka_schema_registry_requests_total`.
    count: Option<RequestCounter>,
}

/// Told how each request to the registry ended: `found`, `not_found`, `unauthorized`,
/// `refused` or `unavailable` (one per try, so a retried outage counts each).
pub type RequestCounter = Arc<dyn Fn(&'static str) + Send + Sync>;

impl std::fmt::Debug for SchemaRegistry {
    /// The URL and whether credentials are set, never the credentials or the proxy (whose URL
    /// may carry its own).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaRegistry")
            .field("url", &self.url)
            .field("basic_auth", &self.auth.is_some())
            .finish_non_exhaustive()
    }
}

impl SchemaRegistry {
    /// A client for `cfg`, reading its variables through `env` (the process environment in
    /// production) and caching in the process-wide [`SchemaCache`].
    ///
    /// A variable the config named explicitly and that is unset, an unusable proxy URL or a CA
    /// that is not PEM is a config error, raised when the pipeline opens.
    pub fn new(cfg: &ResolvedSchemaRegistry, env: &dyn Fn(&str) -> Option<String>) -> Result<Self> {
        Self::with_cache(cfg, env, SchemaCache::global())
    }

    /// As [`Self::new`], with a cache of the caller's.
    pub fn with_cache(
        cfg: &ResolvedSchemaRegistry,
        env: &dyn Fn(&str) -> Option<String>,
        cache: Arc<SchemaCache>,
    ) -> Result<Self> {
        let url = cfg.url.trim_end_matches('/').to_string();
        let base = url::Url::parse(&url)
            .map_err(|e| Error::Config(format!("schema_registry.url {url:?} is not a URL: {e}")))?;
        if base.cannot_be_a_base() {
            return Err(Error::Config(format!(
                "schema_registry.url {url:?} is not an http(s) URL"
            )));
        }

        // kdi: basic auth when the username is set, with the password if that is too.
        let username = cfg.username_env.read("schema_registry.username_env", env)?;
        let password = cfg.password_env.read("schema_registry.password_env", env)?;
        let auth = username.map(|u| (u, password));

        let mut builder = reqwest::Client::builder().timeout(REQUEST_TIMEOUT);
        if let Some(proxy) = cfg.proxy_env.read("schema_registry.proxy_env", env)? {
            let var = cfg.proxy_env.name().unwrap_or_default();
            // The variable's value is not printed: a proxy URL may carry credentials.
            let proxy = reqwest::Proxy::all(proxy.trim()).map_err(|_| {
                Error::Config(format!(
                    "the proxy URL in {var} (schema_registry.proxy_env) is not one reqwest can use"
                ))
            })?;
            builder = builder.proxy(proxy);
        }
        if let Some(pem) = cfg.ca_pem_env.read("schema_registry.ca_pem_env", env)? {
            let var = cfg.ca_pem_env.name().unwrap_or_default();
            let certs = reqwest::Certificate::from_pem_bundle(pem.as_bytes()).map_err(|e| {
                Error::Config(format!(
                    "{var} (schema_registry.ca_pem_env) does not hold PEM certificates: {e}"
                ))
            })?;
            if certs.is_empty() {
                return Err(Error::Config(format!(
                    "{var} (schema_registry.ca_pem_env) holds no PEM certificate"
                )));
            }
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }
        let client = builder.build().map_err(|e| {
            Error::Config(format!(
                "cannot build the Schema Registry client for {url}: {e}"
            ))
        })?;
        Ok(Self {
            url,
            base,
            client,
            auth,
            retry_delays: RETRY_DELAYS.to_vec(),
            cache,
            count: None,
        })
    }

    /// Tells `count` how every request ends, from now on.
    pub fn with_request_counter(mut self, count: RequestCounter) -> Self {
        self.count = Some(count);
        self
    }

    fn counted(&self, outcome: &'static str) {
        if let Some(count) = &self.count {
            count(outcome);
        }
    }

    /// The URL, as errors print it.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Shorter waits between retries, for tests.
    #[cfg(test)]
    fn with_retry_delays(mut self, delays: &[Duration]) -> Self {
        self.retry_delays = delays.to_vec();
        self
    }

    /// The writer schema with this id: from the cache, or from one request shared by every
    /// caller that asks while it runs.
    pub async fn lookup(&self, id: u32) -> Result<Lookup> {
        let key = (self.url.clone(), id);
        if let Some(detail) = self.cache.not_found_since(&key, NOT_FOUND_TTL) {
            return Ok(not_found(id, &detail));
        }
        let cell = self.cache.cell(&key);
        let fetched = cell
            .get_or_try_init(|| async {
                // A caller that waited on another's fetch of an id the registry did not know
                // takes that answer rather than asking again.
                if let Some(detail) = self.cache.not_found_since(&key, NOT_FOUND_TTL) {
                    return Err(Fetch::NotFound(detail));
                }
                match self.get(self.schema_url(id)).await? {
                    Answer::Found(body) => {
                        let raw = self.parse(&body, &format!("schema id {id}"))?;
                        Ok(Arc::new(raw.into_registered(id)))
                    }
                    Answer::NotFound(detail) => Err(Fetch::NotFound(detail)),
                }
            })
            .await;
        match fetched {
            Ok(schema) => self.classify(schema.clone()),
            Err(Fetch::NotFound(detail)) => {
                let lookup = not_found(id, &detail);
                if self.cache.not_found_since(&key, NOT_FOUND_TTL).is_none() {
                    self.cache.remember_not_found(key, detail);
                }
                Ok(lookup)
            }
            Err(Fetch::Failed(e)) => Err(e),
        }
    }

    /// The subject's latest registered version, or `None` where the registry has no such
    /// subject: another subject naming strategy than the one configured (§5.5).
    pub async fn latest(&self, subject: &str) -> Result<Option<Arc<RegisteredSchema>>> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .expect("checked in new")
            .pop_if_empty()
            .extend(["subjects", subject, "versions", "latest"]);
        match self.get(url).await? {
            Answer::NotFound(detail) => {
                warn!(
                    url = %self.url,
                    subject,
                    "the Schema Registry has no subject {subject:?} ({detail}); the union of \
                     writer schemas is built from the ids seen only. Set `subject` if the \
                     topic's schemas are registered under another name."
                );
                Ok(None)
            }
            Answer::Found(body) => {
                let raw = self.parse(&body, &format!("the latest version of {subject:?}"))?;
                let Some(id) = raw.id else {
                    return Err(self.refused(format!(
                        "the latest version of {subject:?} came without its id"
                    )));
                };
                let schema = Arc::new(raw.into_registered(id));
                self.cache.offer(&self.url, &schema);
                Ok(Some(schema))
            }
        }
    }

    fn schema_url(&self, id: u32) -> url::Url {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .expect("checked in new")
            .pop_if_empty()
            .extend(["schemas", "ids", &id.to_string()]);
        url.set_query(Some("deleted=true"));
        url
    }

    /// An answer the decoder can use, or the reason it cannot.
    fn classify(&self, schema: Arc<RegisteredSchema>) -> Result<Lookup> {
        if !schema.references.is_empty() {
            return Err(Error::KafkaSchemaReferences {
                url: self.url.clone(),
                id: schema.id,
                subjects: schema
                    .references
                    .iter()
                    .map(|r| format!("{} version {}", r.subject, r.version))
                    .collect(),
            });
        }
        if !schema.is_avro() {
            return Ok(Lookup::Undecodable {
                reason: format!(
                    "writer schema {} is {}, not AVRO",
                    schema.id,
                    schema.schema_type.as_deref().unwrap_or_default()
                ),
                deterministic: true,
            });
        }
        Ok(Lookup::Avro(schema))
    }

    /// One GET, retried through an outage.
    async fn get(&self, url: url::Url) -> std::result::Result<Answer, Error> {
        let mut delays = self.retry_delays.iter();
        let mut attempts = 1;
        loop {
            match self.try_get(url.clone()).await {
                Attempt::Done(answer) => return answer,
                Attempt::Retry(detail) => {
                    let Some(delay) = delays.next() else {
                        return Err(Error::SchemaRegistry {
                            url: self.url.clone(),
                            failure: SchemaRegistryFailure::Unavailable,
                            detail: format!(
                                "GET {} failed {attempts} times; the last time: {detail}",
                                url.path()
                            ),
                        });
                    };
                    debug!(url = %self.url, path = url.path(), "retrying in {delay:?}: {detail}");
                    tokio::time::sleep(*delay).await;
                    attempts += 1;
                }
            }
        }
    }

    async fn try_get(&self, url: url::Url) -> Attempt {
        let path = url.path().to_string();
        let mut request = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, SR_MEDIA_TYPE);
        if let Some((username, password)) = &self.auth {
            request = request.basic_auth(username, password.as_deref());
        }
        let response = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                self.counted("unavailable");
                return Attempt::Retry(describe(&e));
            }
        };
        let status = response.status();
        let body = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                self.counted("unavailable");
                return Attempt::Retry(describe(&e));
            }
        };
        self.counted(match status.as_u16() {
            200..=299 => "found",
            401 | 403 => "unauthorized",
            404 if registry_error_code(&body).is_some() => "not_found",
            408 | 429 | 500..=599 => "unavailable",
            _ => "refused",
        });
        let what = || format!("GET {path}: HTTP {status}{}", registry_error(&body));
        match status.as_u16() {
            200..=299 => Attempt::Done(Ok(Answer::Found(body.to_vec()))),
            401 | 403 => Attempt::Done(Err(Error::SchemaRegistry {
                url: self.url.clone(),
                failure: SchemaRegistryFailure::Unauthorized,
                detail: format!(
                    "{}. Check the credentials in schema_registry.username_env and \
                     password_env (kdi's SCHEMA_REGISTRY_USERNAME and _PASSWORD by default).",
                    what()
                ),
            })),
            // The registry's own "not found" (40403 for an id, 40401 for a subject) carries
            // its error body. A 404 without one is a reverse proxy's or another server's page:
            // the URL is wrong, which no message is to blame for, and taken as "not found" it
            // would send every message to the data-quality table.
            404 if registry_error_code(&body).is_some() => Attempt::Done(Ok(Answer::NotFound(
                format!("HTTP 404{}", registry_error(&body)),
            ))),
            404 => Attempt::Done(Err(self.refused(format!(
                "{}; not the Schema Registry API's answer: is schema_registry.url right?",
                what()
            )))),
            408 | 429 | 500..=599 => Attempt::Retry(what()),
            _ => Attempt::Done(Err(self.refused(what()))),
        }
    }

    fn parse(&self, body: &[u8], what: &str) -> std::result::Result<RawSchema, Error> {
        let raw: RawSchema = serde_json::from_slice(body).map_err(|e| {
            self.refused(format!(
                "the answer for {what} is not the Schema Registry API's ({e}): {}",
                excerpt(body)
            ))
        })?;
        if raw.schema.is_none() {
            return Err(self.refused(format!(
                "the answer for {what} has no schema: {}",
                excerpt(body)
            )));
        }
        Ok(raw)
    }

    fn refused(&self, detail: String) -> Error {
        Error::SchemaRegistry {
            url: self.url.clone(),
            failure: SchemaRegistryFailure::Refused,
            detail,
        }
    }
}

#[async_trait]
impl WriterSchemas for SchemaRegistry {
    async fn writer_schema(&self, id: u32) -> Result<Lookup> {
        self.lookup(id).await
    }
}

fn not_found(id: u32, detail: &str) -> Lookup {
    Lookup::Undecodable {
        reason: format!("schema id {id} is not in the Schema Registry ({detail})"),
        deterministic: false,
    }
}

/// How one request ended.
enum Attempt {
    Done(std::result::Result<Answer, Error>),
    /// An outage: worth another try.
    Retry(String),
}

enum Answer {
    Found(Vec<u8>),
    NotFound(String),
}

/// Why a fetch for the cache kept nothing.
enum Fetch {
    NotFound(String),
    Failed(Error),
}

impl From<Error> for Fetch {
    fn from(e: Error) -> Self {
        Fetch::Failed(e)
    }
}

/// The registry's answer as it comes: every field optional, so that a missing one is named.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSchema {
    schema: Option<String>,
    schema_type: Option<String>,
    #[serde(default)]
    references: Option<Vec<SchemaReference>>,
    id: Option<u32>,
    subject: Option<String>,
    version: Option<i32>,
}

impl RawSchema {
    fn into_registered(self, id: u32) -> RegisteredSchema {
        RegisteredSchema {
            id,
            schema: self.schema.unwrap_or_default(),
            schema_type: self.schema_type,
            references: self.references.unwrap_or_default(),
            subject: self.subject,
            version: self.version,
        }
    }
}

/// The Schema Registry API's error body: `{"error_code": 40403, "message": "..."}`.
#[derive(Deserialize)]
struct RawError {
    error_code: Option<i64>,
    message: Option<String>,
}

/// The registry's `error_code`, when the body is the registry's error JSON.
fn registry_error_code(body: &[u8]) -> Option<i64> {
    serde_json::from_slice::<RawError>(body)
        .ok()
        .and_then(|e| e.error_code)
}

/// `", error_code 40403: Schema not found"` from the registry's error body, or a short excerpt
/// of whatever else came; nothing for an empty body.
fn registry_error(body: &[u8]) -> String {
    if body.is_empty() {
        return String::new();
    }
    match serde_json::from_slice::<RawError>(body) {
        Ok(RawError {
            error_code: Some(code),
            message,
        }) => format!(
            ", error_code {code}{}",
            message.map(|m| format!(": {m}")).unwrap_or_default()
        ),
        _ => format!(": {}", excerpt(body)),
    }
}

/// At most 200 characters of a body, for an error message.
fn excerpt(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    match text.char_indices().nth(200) {
        Some((i, _)) => format!("{}...", &text[..i]),
        None => text.to_string(),
    }
}

/// A transport error and its causes, without the URL (the error names the registry already) and
/// so without anything a proxy URL could carry.
fn describe(e: &reqwest::Error) -> String {
    use std::error::Error as _;
    let kind = if e.is_timeout() {
        "timed out"
    } else if e.is_connect() {
        "cannot connect"
    } else {
        "request failed"
    };
    let mut out = kind.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        let text = s.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = s.source();
    }
    out
}

// ------------------------------------------------------------------------------ the union

/// The writer schemas the SQL path's union `U` is built from (docs/kafka-design.md §5.5): the
/// subject's latest registered version, and every writer schema the pipeline has decoded a
/// message with, including ids registered under other subjects.
///
/// Versions never seen are left out, so an old deleted or incompatible version cannot stop a
/// pipeline from starting. The members only grow: a subject's latest version, once met, stays,
/// also when a newer one replaces it or the subject goes away.
#[derive(Debug)]
pub struct SubjectSchemas {
    registry: Arc<SchemaRegistry>,
    subject: String,
    /// Every version that was the subject's latest when asked.
    latest: BTreeMap<u32, Arc<RegisteredSchema>>,
    /// The id the subject's latest version had when last asked; `None` while the registry has
    /// no such subject.
    latest_id: Option<u32>,
    seen: BTreeMap<u32, Arc<RegisteredSchema>>,
}

impl SubjectSchemas {
    /// Asks for the subject's latest version, as a pipeline opens.
    pub async fn open(registry: Arc<SchemaRegistry>, subject: impl Into<String>) -> Result<Self> {
        let mut this = Self {
            registry,
            subject: subject.into(),
            latest: BTreeMap::new(),
            latest_id: None,
            seen: BTreeMap::new(),
        };
        this.refresh_latest().await?;
        Ok(this)
    }

    /// Records a writer schema a message was decoded with. An id not in the union yet asks for
    /// the subject's latest version again (its message may be the first of a new version, and
    /// the newest type of a field is the one the union takes). Returns whether the union
    /// gained a member.
    ///
    /// On an error nothing is recorded, so the same call can be made again.
    pub async fn observe(&mut self, schema: &Arc<RegisteredSchema>) -> Result<bool> {
        if self.seen.contains_key(&schema.id) {
            return Ok(false);
        }
        if self.latest.contains_key(&schema.id) {
            self.seen.insert(schema.id, schema.clone());
            return Ok(false);
        }
        self.refresh_latest().await?;
        self.seen.insert(schema.id, schema.clone());
        Ok(true)
    }

    /// The union's members, newest (highest id) first, each once.
    pub fn members(&self) -> Vec<Arc<RegisteredSchema>> {
        let mut all: BTreeMap<u32, Arc<RegisteredSchema>> = self.latest.clone();
        all.extend(self.seen.iter().map(|(id, s)| (*id, s.clone())));
        all.into_values().rev().collect()
    }

    /// The id of the subject's latest version when last asked.
    pub fn latest_id(&self) -> Option<u32> {
        self.latest_id
    }

    async fn refresh_latest(&mut self) -> Result<()> {
        let Some(latest) = self.registry.latest(&self.subject).await? else {
            self.latest_id = None;
            return Ok(());
        };
        self.latest_id = Some(latest.id);
        // A version the decoder could not use is not one the SQL can be planned against: its
        // own messages will say why when they come.
        match self.registry.classify(latest.clone()) {
            Ok(Lookup::Avro(_)) => {
                self.latest.insert(latest.id, latest);
            }
            Ok(Lookup::Undecodable { reason, .. }) => warn!(
                subject = %self.subject,
                "the subject's latest version is left out of the union: {reason}"
            ),
            Err(e) => warn!(
                subject = %self.subject,
                "the subject's latest version is left out of the union: {e}"
            ),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::kafka::EnvRef;
    use crate::error::KafkaClass;
    use crate::kafka::sr_stub::Stub;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SCHEMA: &str = r#"{"type":"record","name":"E","fields":[{"name":"x","type":"long"}]}"#;

    fn schema_body(extra: &str) -> String {
        format!(
            "{{\"schema\":{}{extra}}}",
            serde_json::to_string(SCHEMA).unwrap()
        )
    }

    fn cfg(url: &str) -> ResolvedSchemaRegistry {
        ResolvedSchemaRegistry {
            url: url.to_string(),
            username_env: EnvRef::Default("SCHEMA_REGISTRY_USERNAME".into()),
            password_env: EnvRef::Default("SCHEMA_REGISTRY_PASSWORD".into()),
            proxy_env: EnvRef::Default("SCHEMA_REGISTRY_PROXY".into()),
            ca_pem_env: EnvRef::Disabled,
        }
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    /// A client with a cache of its own, so that no other test's answers are in it, and short
    /// retries.
    fn client(url: &str) -> SchemaRegistry {
        SchemaRegistry::with_cache(&cfg(url), &no_env, Arc::default())
            .unwrap()
            .with_retry_delays(&[Duration::from_millis(1); 3])
    }

    #[tokio::test]
    async fn basic_auth_from_kdi_env() {
        let stub = Stub::start(|_| (200, schema_body(""))).await;
        let env = |k: &str| match k {
            "SCHEMA_REGISTRY_USERNAME" => Some("svc".to_string()),
            "SCHEMA_REGISTRY_PASSWORD" => Some("hunter2".to_string()),
            _ => None,
        };
        let sr = SchemaRegistry::with_cache(&cfg(&stub.url), &env, Arc::default()).unwrap();
        sr.lookup(1).await.unwrap();
        // base64("svc:hunter2")
        assert_eq!(
            stub.seen()[0].header("authorization"),
            Some("Basic c3ZjOmh1bnRlcjI=")
        );
        // Never in Debug.
        let shown = format!("{sr:?}");
        assert!(
            !shown.contains("hunter2") && !shown.contains("svc"),
            "{shown}"
        );

        // The password is optional, as in kdi: the username alone is "svc:".
        let env = |k: &str| (k == "SCHEMA_REGISTRY_USERNAME").then(|| "svc".to_string());
        let sr = SchemaRegistry::with_cache(&cfg(&stub.url), &env, Arc::default()).unwrap();
        sr.lookup(1).await.unwrap();
        assert_eq!(
            stub.seen()[1].header("authorization"),
            Some("Basic c3ZjOg==")
        );

        // No username, no auth, whatever the password: kdi read the password only beside one.
        let env = |k: &str| (k == "SCHEMA_REGISTRY_PASSWORD").then(|| "hunter2".to_string());
        let sr = SchemaRegistry::with_cache(&cfg(&stub.url), &env, Arc::default()).unwrap();
        sr.lookup(1).await.unwrap();
        assert_eq!(stub.seen()[2].header("authorization"), None);

        // Names the config set itself are required: unset is an error at open.
        let mut explicit = cfg(&stub.url);
        explicit.username_env = EnvRef::Explicit("SR_USER".into());
        let e = SchemaRegistry::with_cache(&explicit, &no_env, Arc::default()).unwrap_err();
        assert!(
            matches!(e, Error::Config(ref m) if m.contains("SR_USER")),
            "{e}"
        );
    }

    #[tokio::test]
    async fn proxy_env_honoured() {
        // The stub is the proxy: a request through it names the registry's absolute URL.
        let proxy = Stub::start(|_| (200, schema_body(""))).await;
        let proxy_url = proxy.url.clone();
        let env = move |k: &str| (k == "SCHEMA_REGISTRY_PROXY").then(|| proxy_url.clone());
        let sr = SchemaRegistry::with_cache(&cfg("http://sr.invalid:8081/"), &env, Arc::default())
            .unwrap();
        assert!(matches!(sr.lookup(3).await.unwrap(), Lookup::Avro(_)));
        let seen = proxy.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(
            seen[0].target(),
            "http://sr.invalid:8081/schemas/ids/3?deleted=true"
        );
        assert_eq!(seen[0].header("host"), Some("sr.invalid:8081"));

        // A proxy URL reqwest cannot use is a config error that does not print it.
        let env = |k: &str| (k == "SCHEMA_REGISTRY_PROXY").then(|| "::not a url::".to_string());
        let e =
            SchemaRegistry::with_cache(&cfg("http://sr:8081"), &env, Arc::default()).unwrap_err();
        assert!(
            matches!(e, Error::Config(ref m) if m.contains("SCHEMA_REGISTRY_PROXY") && !m.contains("not a url")),
            "{e}"
        );
    }

    #[tokio::test]
    async fn deleted_true_query_sent() {
        let stub = Stub::start(|_| (200, schema_body(""))).await;
        // A trailing slash and a path prefix: trimmed and kept, as kdi did.
        let sr = client(&format!("{}/registry/", stub.url));
        sr.lookup(7).await.unwrap();
        let seen = stub.seen();
        assert_eq!(
            seen[0].line,
            "GET /registry/schemas/ids/7?deleted=true HTTP/1.1"
        );
        assert_eq!(seen[0].header("accept"), Some(SR_MEDIA_TYPE));
        assert_eq!(sr.url(), format!("{}/registry", stub.url));
    }

    #[tokio::test]
    async fn absent_schema_type_is_avro() {
        let stub = Stub::start(|s| match s.target() {
            "/schemas/ids/1?deleted=true" => (200, schema_body("")),
            "/schemas/ids/2?deleted=true" => (200, schema_body(",\"schemaType\":\"AVRO\"")),
            "/schemas/ids/3?deleted=true" => (200, schema_body(",\"schemaType\":\"PROTOBUF\"")),
            _ => (200, schema_body(",\"schemaType\":\"JSON\"")),
        })
        .await;
        let sr = client(&stub.url);
        for id in [1, 2] {
            let Lookup::Avro(s) = sr.lookup(id).await.unwrap() else {
                panic!("id {id} is Avro");
            };
            assert_eq!((s.id, s.schema.as_str()), (id, SCHEMA));
        }
        for (id, kind) in [(3, "PROTOBUF"), (4, "JSON")] {
            match sr.lookup(id).await.unwrap() {
                Lookup::Undecodable {
                    reason,
                    deterministic,
                } => {
                    assert!(deterministic, "a schema's type never changes");
                    assert!(
                        reason.contains(kind) && reason.contains("not AVRO"),
                        "{reason}"
                    );
                }
                other => panic!("{other:?}"),
            }
        }
    }

    /// 404 and 40403 make the message undecodable, never an error that would hold the pipeline,
    /// and not deterministic: the id may be registered later (a registry replica that has not
    /// caught up). Remembered for a moment, so a topic full of them is not one request each,
    /// also when many ask at once.
    ///
    /// Named `sr_…` because a Rust name cannot start with a digit (§9.1 lists it as
    /// `404_40403_is_undecodable_nondeterministic`).
    #[tokio::test]
    async fn sr_404_40403_is_undecodable_nondeterministic() {
        let stub = Stub::start(|s| match s.target() {
            "/schemas/ids/9?deleted=true" | "/schemas/ids/11?deleted=true" => (
                404,
                r#"{"error_code":40403,"message":"Schema not found"}"#.into(),
            ),
            // A 404 that is not the registry's own: a URL that points elsewhere.
            _ => (404, "<html>nothing here</html>".into()),
        })
        .await;
        let sr = client(&stub.url);
        for _ in 0..3 {
            match sr.lookup(9).await.unwrap() {
                Lookup::Undecodable {
                    reason,
                    deterministic,
                } => {
                    assert!(!deterministic);
                    assert!(
                        reason.contains("schema id 9") && reason.contains("40403"),
                        "{reason}"
                    );
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(stub.seen().len(), 1, "not asked again within the TTL");

        // Ten callers at once: one request.
        let lookups = (0..10).map(|_| sr.lookup(11));
        for l in futures::future::join_all(lookups).await {
            assert!(matches!(
                l.unwrap(),
                Lookup::Undecodable {
                    deterministic: false,
                    ..
                }
            ));
        }
        assert_eq!(stub.seen().len(), 2);

        // A 404 page that is not the registry's: the URL is wrong, not the message. It holds
        // the pipeline rather than sending every message to the data-quality table, and is
        // not retried.
        let e = sr.lookup(10).await.unwrap_err();
        assert!(
            matches!(
                e,
                Error::SchemaRegistry {
                    failure: SchemaRegistryFailure::Refused,
                    ..
                }
            ),
            "{e:?}"
        );
        assert_eq!(e.kafka_class(), KafkaClass::BlockingPipeline);
        let m = e.to_string();
        assert!(
            m.contains("nothing here") && m.contains("schema_registry.url"),
            "{m}"
        );
        assert_eq!(stub.seen().len(), 3, "a 404 is never retried");
    }

    /// Named `sr_…` because a Rust name cannot start with a digit (§9.1: `401_is_consumer_fatal`).
    #[tokio::test]
    async fn sr_401_is_consumer_fatal() {
        for status in [401, 403] {
            let stub = Stub::start(move |_| {
                (
                    status,
                    r#"{"error_code":40101,"message":"Unauthorized"}"#.into(),
                )
            })
            .await;
            let sr = client(&stub.url);
            let e = sr.lookup(1).await.unwrap_err();
            assert!(
                matches!(
                    e,
                    Error::SchemaRegistry {
                        failure: SchemaRegistryFailure::Unauthorized,
                        ..
                    }
                ),
                "{e:?}"
            );
            assert_eq!(e.kafka_class(), KafkaClass::ConsumerFatal);
            let m = e.to_string();
            assert!(
                m.contains(&status.to_string()) && m.contains("username_env"),
                "{m}"
            );
            assert_eq!(stub.seen().len(), 1, "credentials are not retried");
        }
    }

    #[tokio::test]
    async fn retry_on_5xx_then_transient() {
        for status in [500, 503, 429, 408] {
            let stub = Stub::start(move |_| (status, String::new())).await;
            let sr = client(&stub.url);
            let e = sr.lookup(1).await.unwrap_err();
            assert!(
                matches!(
                    e,
                    Error::SchemaRegistry {
                        failure: SchemaRegistryFailure::Unavailable,
                        ..
                    }
                ),
                "{e:?}"
            );
            assert_eq!(e.kafka_class(), KafkaClass::Transient);
            assert!(e.to_string().contains("4 times"), "{e}");
            assert_eq!(stub.seen().len(), 4, "the first try and three retries");
        }

        // An outage that ends within the retries is no error, and is cached once it ends.
        let calls = Arc::new(AtomicUsize::new(0));
        let n = calls.clone();
        let stub = Stub::start(move |_| match n.fetch_add(1, Ordering::SeqCst) {
            0 | 1 => (503, String::new()),
            _ => (200, schema_body("")),
        })
        .await;
        let sr = client(&stub.url);
        assert!(matches!(sr.lookup(1).await.unwrap(), Lookup::Avro(_)));
        assert!(matches!(sr.lookup(1).await.unwrap(), Lookup::Avro(_)));
        assert_eq!(stub.seen().len(), 3);

        // Nobody listening at all: the same, after the same retries.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let e = client(&url).lookup(1).await.unwrap_err();
        assert_eq!(e.kafka_class(), KafkaClass::Transient, "{e}");
        assert!(e.to_string().contains("connect"), "{e}");

        // Not every refusal is an outage: a 400, or an answer that is not the API's, holds the
        // pipeline until someone looks.
        for (status, body) in [
            (400, r#"{"error_code":400,"message":"bad"}"#.to_string()),
            (200, "<html>login</html>".to_string()),
            (200, r#"{"id":1}"#.to_string()),
        ] {
            let stub = Stub::start(move |_| (status, body.clone())).await;
            let e = client(&stub.url).lookup(1).await.unwrap_err();
            assert_eq!(e.kafka_class(), KafkaClass::BlockingPipeline, "{e}");
            assert_eq!(stub.seen().len(), 1);
        }
    }

    /// Ids are immutable, so one request per id for the whole process, however many pipelines
    /// and callers ask, and however many ask at once.
    #[tokio::test]
    async fn cache_one_request_per_id() {
        let stub = Stub::start(|_| (200, schema_body(""))).await;
        let cache = Arc::new(SchemaCache::default());
        let a = SchemaRegistry::with_cache(&cfg(&stub.url), &no_env, cache.clone()).unwrap();
        // Another pipeline on the same registry, its URL spelt with a trailing slash.
        let b =
            SchemaRegistry::with_cache(&cfg(&format!("{}/", stub.url)), &no_env, cache).unwrap();
        let lookups = (0..10).map(|i| if i % 2 == 0 { a.lookup(5) } else { b.lookup(5) });
        for l in futures::future::join_all(lookups).await {
            assert!(matches!(l.unwrap(), Lookup::Avro(s) if s.id == 5));
        }
        assert_eq!(stub.seen().len(), 1);
        a.lookup(6).await.unwrap();
        b.lookup(5).await.unwrap();
        assert_eq!(
            stub.seen().len(),
            2,
            "one more for the new id, none for the old"
        );

        // A registry with a cache of its own asks for itself.
        let c = client(&stub.url);
        c.lookup(5).await.unwrap();
        assert_eq!(stub.seen().len(), 3);
    }

    #[tokio::test]
    async fn references_block() {
        let stub = Stub::start(|_| {
            (
                200,
                schema_body(
                    r#","references":[{"name":"com.acme.Address","subject":"address-value","version":2}]"#,
                ),
            )
        })
        .await;
        let sr = client(&stub.url);
        let e = sr.lookup(4).await.unwrap_err();
        assert_eq!(e.kafka_class(), KafkaClass::BlockingPipeline);
        let m = e.to_string();
        assert!(
            m.contains("address-value version 2") && m.contains("not supported yet"),
            "{m}"
        );
        // An empty list is no reference.
        let stub = Stub::start(|_| (200, schema_body(r#","references":[]"#))).await;
        assert!(matches!(
            client(&stub.url).lookup(4).await.unwrap(),
            Lookup::Avro(_)
        ));
    }

    /// The union `U` asks for the subject's latest version when the pipeline opens, and again
    /// whenever a message names an id the union does not hold yet; never for one it holds.
    #[tokio::test]
    async fn latest_version_fetched_on_unseen_id() {
        let latest = Arc::new(AtomicUsize::new(1));
        let l = latest.clone();
        let stub = Stub::start(move |s| {
            if s.target() == "/subjects/orders-value/versions/latest" {
                let id = l.load(Ordering::SeqCst);
                return (
                    200,
                    format!(
                        "{{\"subject\":\"orders-value\",\"version\":{id},\"id\":{id},\"schema\":{}}}",
                        serde_json::to_string(SCHEMA).unwrap()
                    ),
                );
            }
            (200, schema_body(""))
        })
        .await;
        let latest_calls = |stub: &Stub| {
            stub.seen()
                .iter()
                .filter(|s| s.target().starts_with("/subjects/"))
                .count()
        };
        let sr = Arc::new(client(&stub.url));
        let mut u = SubjectSchemas::open(sr.clone(), "orders-value")
            .await
            .unwrap();
        assert_eq!(latest_calls(&stub), 1);
        assert_eq!(u.latest_id(), Some(1));
        let ids = |u: &SubjectSchemas| u.members().iter().map(|s| s.id).collect::<Vec<_>>();
        assert_eq!(ids(&u), [1]);

        // Id 1 is the latest already: neither asked for nor fetched by id (the latest's answer
        // filled the cache).
        let Lookup::Avro(one) = sr.lookup(1).await.unwrap() else {
            panic!()
        };
        assert!(!u.observe(&one).await.unwrap());
        assert_eq!(latest_calls(&stub), 1);
        assert_eq!(stub.seen().len(), 1);

        // Version 2 is registered, and its first message arrives.
        latest.store(2, Ordering::SeqCst);
        let Lookup::Avro(two) = sr.lookup(2).await.unwrap() else {
            panic!()
        };
        assert!(u.observe(&two).await.unwrap());
        assert_eq!(latest_calls(&stub), 2);
        assert_eq!(u.latest_id(), Some(2));
        assert_eq!(ids(&u), [2, 1]);

        // An id from another subject joins the union too, newest first; seen ids are not
        // asked about again.
        let Lookup::Avro(nine) = sr.lookup(9).await.unwrap() else {
            panic!()
        };
        assert!(u.observe(&nine).await.unwrap());
        assert!(!u.observe(&nine).await.unwrap());
        assert!(!u.observe(&one).await.unwrap());
        assert_eq!(latest_calls(&stub), 3);
        assert_eq!(ids(&u), [9, 2, 1]);
    }

    /// A subject the registry does not have (another subject naming strategy) leaves the union
    /// to the ids seen, with a warning, rather than refusing to start.
    #[tokio::test]
    async fn latest_version_of_an_unknown_subject_is_none() {
        let stub = Stub::start(|s| {
            if s.target().starts_with("/subjects/") {
                return (
                    404,
                    r#"{"error_code":40401,"message":"Subject not found"}"#.into(),
                );
            }
            (200, schema_body(""))
        })
        .await;
        let sr = Arc::new(client(&stub.url));
        // The subject is a path segment, escaped as one.
        let mut u = SubjectSchemas::open(sr.clone(), "com.acme/Order")
            .await
            .unwrap();
        assert_eq!(
            stub.seen()[0].target(),
            "/subjects/com.acme%2FOrder/versions/latest"
        );
        assert!(u.members().is_empty());
        let Lookup::Avro(s) = sr.lookup(3).await.unwrap() else {
            panic!()
        };
        assert!(u.observe(&s).await.unwrap());
        assert_eq!(u.members().len(), 1);
        assert_eq!(u.latest_id(), None);
    }

    #[test]
    fn a_private_ca_is_read_from_pem() {
        // A self-signed certificate, generated for this test.
        const PEM: &str = include_str!("testdata/sr-ca.pem");
        let env = |k: &str| (k == "SR_CA").then(|| PEM.to_string());
        let mut c = cfg("https://sr.internal:8081");
        c.ca_pem_env = EnvRef::Explicit("SR_CA".into());
        SchemaRegistry::with_cache(&c, &env, Arc::default()).unwrap();

        let env = |k: &str| (k == "SR_CA").then(|| "not a certificate".to_string());
        let e = SchemaRegistry::with_cache(&c, &env, Arc::default()).unwrap_err();
        assert!(
            matches!(e, Error::Config(ref m) if m.contains("SR_CA")),
            "{e}"
        );
        let e = SchemaRegistry::with_cache(&c, &no_env, Arc::default()).unwrap_err();
        assert!(
            matches!(e, Error::Config(ref m) if m.contains("SR_CA")),
            "{e}"
        );
    }
}
