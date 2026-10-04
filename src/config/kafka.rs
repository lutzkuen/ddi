//! `[kafka]` and `[[kafka.pipeline]]`: pipelines that read a Kafka topic instead of a Delta
//! table (docs/kafka-design.md §2).
//!
//! # Always compiled
//!
//! Parsing, resolution and validation do not depend on the `kafka` feature. A build without it
//! reads a config with Kafka pipelines, judges every entry exactly as a build with it would,
//! and then holds each one back by name while the Delta pipelines beside it run (I16). So one
//! file validates the same way on every build, and an image built without the feature says
//! what it cannot run instead of refusing the whole file.
//!
//! # Secrets
//!
//! The config holds the names of environment variables, never what they hold: those are read
//! when a pipeline opens, by [`ResolvedKafka::client_settings`]. The one secret a config can
//! carry is a value somebody wrote inline into `[kafka.settings]`. It is kept, because it is
//! the only copy, warned about at load, and redacted wherever it is printed
//! ([`KafkaSettings`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{parse_size, Config, PipelineConfig, ResolvedPipeline, WriteMode};
use crate::error::{Error, Result};
use crate::transform::validate::normalise_sql_with_lookups;

/// kdi's variable for a SCRAM identity, `{"username": .., "password": ..}` (kdi lib.rs:1310).
pub const DEFAULT_SCRAM_JSON_ENV: &str = "KAFKA_DELTA_INGEST_SCRAM_JSON";
/// kdi's variable for the client certificate, PEM.
pub const DEFAULT_SSL_CERT_ENV: &str = "KAFKA_DELTA_INGEST_CERT";
/// kdi's variable for the client key, PEM.
pub const DEFAULT_SSL_KEY_ENV: &str = "KAFKA_DELTA_INGEST_KEY";
/// kdi's variable for the CA, PEM.
pub const DEFAULT_SSL_CA_ENV: &str = "KAFKA_DELTA_INGEST_CA";
/// kdi's Schema Registry variables (kdi serialization.rs:96-107).
pub const DEFAULT_SR_USERNAME_ENV: &str = "SCHEMA_REGISTRY_USERNAME";
pub const DEFAULT_SR_PASSWORD_ENV: &str = "SCHEMA_REGISTRY_PASSWORD";
pub const DEFAULT_SR_PROXY_ENV: &str = "SCHEMA_REGISTRY_PROXY";

/// How long a revoked partition's buffer may take to commit before it is dropped (§3.4).
const DEFAULT_REVOKE_FLUSH_TIMEOUT_SECS: u64 = 120;
/// How often a paused pipeline or partition looks at its cause again (§3.6).
const DEFAULT_BLOCKED_RECHECK_SECS: u64 = 60;
/// The smallest `max_bytes_per_batch` a Kafka pipeline takes: below it a batch is a handful of
/// messages, and every one of them costs a Delta commit.
const MIN_MAX_BYTES_PER_BATCH: u64 = 1_000_000;

/// What a build without the `kafka` feature says about every Kafka pipeline (§2.5 rule 1).
pub const NOT_BUILT: &str = "this ddi was built without the `kafka` feature, so it cannot read \
     Kafka. The Delta pipelines in this config run; this one is held back. Build with \
     `--features kafka` (the image: `--build-arg DDI_FEATURES=kafka`).";

// ------------------------------------------------------------------------------- the file

/// `[kafka]`: the cluster, the Schema Registry, credentials and defaults shared by every
/// `[[kafka.pipeline]]`, whose own key wins.
///
/// Each shared key is an explicit `Option` here and again on [`KafkaPipelineConfig`] rather
/// than one struct flattened into both, because `#[serde(flatten)]` cannot be combined with
/// `deny_unknown_fields`, and a misspelt key must stay an error.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    /// `host:port,...` (kdi `-k`). Required, here or on the pipeline.
    pub brokers: Option<String>,
    /// How long a batch may wait before it commits (kdi `-l`). The timer starts at the first
    /// message after the last commit. Defaults to `[runtime] allowed_latency_secs`.
    pub allowed_latency_secs: Option<u64>,
    /// Kafka key+value bytes per commit. The decoded size is held to the memory budget as
    /// well. Defaults to `[runtime] max_bytes_per_batch`; at least 1MB.
    pub max_bytes_per_batch: Option<String>,
    /// An optional commit trigger on the message count. NOT kdi's `-m`, which sized an
    /// in-memory batch and committed nothing (§2.3).
    pub max_messages_per_batch: Option<usize>,
    /// The parquet file size the writer aims for (kdi `-b`).
    pub target_file_size: Option<String>,
    /// Where a fresh app_id starts: `"earliest"`, `"latest"`, or a map of partition to the
    /// NEXT offset to read, optionally with `"*" = "earliest" | "latest"` for the rest.
    pub starting_offsets: Option<StartingOffsets>,
    /// Render non-string values into STRING columns the way kdi did (§5.6). Unset refuses to
    /// start on a table kdi may have written (§4.4 step 6).
    pub kdi_compat: Option<bool>,
    /// Write the text `"null"` where kdi's JSON had the key with a null value. Defaults to
    /// `kdi_compat`.
    pub kdi_null_text: Option<bool>,
    /// The prefix of the Kafka metadata columns (§6.1). `kafka_` by default.
    pub metadata_prefix: Option<String>,
    /// Also commit consumed offsets to the group, for lag tooling only; never read back.
    pub commit_offsets_to_kafka: Option<bool>,
    /// How long a revoke may take to commit the buffer before it is dropped. 5..=300.
    pub revoke_flush_timeout_secs: Option<u64>,
    /// How often a paused pipeline or partition re-checks its cause.
    pub blocked_recheck_secs: Option<u64>,
    /// Rows of one batch the transform may fail to evaluate and still commit the rest.
    /// Defaults to `[runtime]`'s. Also caps undecodable messages per batch (§5.7).
    pub max_evaluation_rejects_per_batch: Option<usize>,
    /// Defaults to `[runtime]`'s.
    pub max_output_rows_per_batch: Option<usize>,
    /// kdi's credential variables. Unset takes kdi's name; `""` turns one off.
    pub scram_json_env: Option<String>,
    pub ssl_cert_env: Option<String>,
    pub ssl_key_env: Option<String>,
    pub ssl_ca_env: Option<String>,
    /// kdi `--avro <url>`.
    pub schema_registry: Option<SchemaRegistryConfig>,
    /// librdkafka settings passed through (kdi `-K k=v`).
    #[serde(default, skip_serializing_if = "KafkaSettings::is_empty")]
    pub settings: KafkaSettings,
    /// librdkafka key = the name of an environment variable holding its value, read at open.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub settings_env: BTreeMap<String, String>,
    #[serde(default, rename = "pipeline", skip_serializing_if = "Vec::is_empty")]
    pub pipelines: Vec<KafkaPipelineConfig>,
}

impl KafkaConfig {
    /// True for a config with no `[kafka]` table, which serialises as none.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// `[kafka.schema_registry]`, or the same table on one pipeline, which overrides it key by key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryConfig {
    /// `http(s)://host:port`. Required for `value_format = "avro_confluent"`.
    pub url: Option<String>,
    /// Basic auth. Unset takes kdi's names; `""` turns one off.
    pub username_env: Option<String>,
    pub password_env: Option<String>,
    /// An HTTP proxy for the registry. Unset takes kdi's name.
    pub proxy_env: Option<String>,
    /// A PEM added to the roots the registry's certificate is checked against. kdi used the
    /// OS store; unset means the bundled roots only.
    pub ca_pem_env: Option<String>,
}

/// One `[[kafka.pipeline]]`: one topic into one Delta table.
///
/// The keys a Delta pipeline has and this one does not — `lookups`, `write_mode`, `publish`,
/// `dedup_*`, `upsert_*` — are not fields, so `deny_unknown_fields` refuses them (§2.5 rule
/// 14): a Kafka pipeline appends, and has no Delta source to deduplicate against.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaPipelineConfig {
    /// Required. Empty or missing is held back, under its position in the file.
    #[serde(default)]
    pub name: String,
    /// The offset key: txns `<app_id>-<partition>` in the target. MUST equal kdi's `-a` to
    /// take over a table kdi wrote.
    #[serde(default)]
    pub app_id: String,
    /// Exactly one topic, never a pattern.
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub target_uri: String,
    /// The consumer group. Defaults to `ddi-<name>`; offsets live in the target, so a new
    /// group loses nothing, but it needs a Read ACL.
    pub group_id: Option<String>,
    /// `"avro_confluent"` (the default) or `"raw"`.
    pub value_format: Option<ValueFormat>,
    /// The registry subject the SQL path's union is read from (§5.5). `<topic>-value`.
    pub subject: Option<String>,
    /// A single SELECT over `source`. Omit for a straight copy.
    pub transform_sql: Option<String>,
    /// Defaults to `<target_uri>__ddi_dq`.
    pub dq_uri: Option<String>,

    // The keys below are shared with `[kafka]`; see there.
    pub brokers: Option<String>,
    pub allowed_latency_secs: Option<u64>,
    pub max_bytes_per_batch: Option<String>,
    pub max_messages_per_batch: Option<usize>,
    pub target_file_size: Option<String>,
    pub starting_offsets: Option<StartingOffsets>,
    pub kdi_compat: Option<bool>,
    pub kdi_null_text: Option<bool>,
    pub metadata_prefix: Option<String>,
    pub commit_offsets_to_kafka: Option<bool>,
    pub revoke_flush_timeout_secs: Option<u64>,
    pub blocked_recheck_secs: Option<u64>,
    pub max_evaluation_rejects_per_batch: Option<usize>,
    pub max_output_rows_per_batch: Option<usize>,
    pub scram_json_env: Option<String>,
    pub ssl_cert_env: Option<String>,
    pub ssl_key_env: Option<String>,
    pub ssl_ca_env: Option<String>,
    pub schema_registry: Option<SchemaRegistryConfig>,
    #[serde(default, skip_serializing_if = "KafkaSettings::is_empty")]
    pub settings: KafkaSettings,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub settings_env: BTreeMap<String, String>,
}

/// How a message value is read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueFormat {
    /// Confluent's framing: a magic byte, a schema id, an Avro body; schemas from the registry.
    #[default]
    AvroConfluent,
    /// The value's bytes as they are, in a BINARY column. For tests and diagnostics.
    Raw,
}

impl ValueFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AvroConfluent => "avro_confluent",
            Self::Raw => "raw",
        }
    }
}

/// `starting_offsets` as written.
///
/// Parsed loosely on purpose: any string, and any table of strings and integers, so that a
/// misspelt `"earliset"` or a negative offset is held back as this one pipeline's fault, by
/// [`resolve_start`], instead of failing the whole file the way a strict enum would.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StartingOffsets {
    Named(String),
    Map(BTreeMap<String, OffsetValue>),
}

/// One value of a `starting_offsets` table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OffsetValue {
    Offset(i64),
    Named(String),
}

/// librdkafka settings, printed with every secret value redacted.
///
/// The map behind `[kafka.settings]`, and what [`ResolvedKafka::client_settings`] assembles.
/// `Debug` is written by hand for the same reason as [`super::PublisherConfig`]'s: a
/// `{:?}` on a `Config`, a `ResolvedPipeline` or anything holding one must not print a
/// password somebody wrote inline.
#[derive(Clone, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct KafkaSettings(BTreeMap<String, String>);

/// A value of `[kafka.settings]`, as librdkafka takes every value: text. TOML's numbers and
/// booleans (`"fetch.min.bytes" = 1`, `"enable.ssl.certificate.verification" = false`) are
/// taken as the text they are written as, since refusing one would fail the whole file, Delta
/// pipelines included, over a value librdkafka accepts. Anything else is no librdkafka value.
impl<'de> Deserialize<'de> for KafkaSettings {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct Text(String);
        impl<'de> Deserialize<'de> for Text {
            fn deserialize<D: serde::Deserializer<'de>>(
                d: D,
            ) -> std::result::Result<Self, D::Error> {
                struct Visitor;
                impl serde::de::Visitor<'_> for Visitor {
                    type Value = Text;
                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str("a string, number or boolean")
                    }
                    fn visit_str<E>(self, v: &str) -> std::result::Result<Text, E> {
                        Ok(Text(v.to_string()))
                    }
                    fn visit_string<E>(self, v: String) -> std::result::Result<Text, E> {
                        Ok(Text(v))
                    }
                    fn visit_bool<E>(self, v: bool) -> std::result::Result<Text, E> {
                        Ok(Text(v.to_string()))
                    }
                    fn visit_i64<E>(self, v: i64) -> std::result::Result<Text, E> {
                        Ok(Text(v.to_string()))
                    }
                    fn visit_u64<E>(self, v: u64) -> std::result::Result<Text, E> {
                        Ok(Text(v.to_string()))
                    }
                    fn visit_f64<E>(self, v: f64) -> std::result::Result<Text, E> {
                        Ok(Text(v.to_string()))
                    }
                }
                d.deserialize_any(Visitor)
            }
        }
        let written = BTreeMap::<String, Text>::deserialize(d)?;
        Ok(Self(written.into_iter().map(|(k, v)| (k, v.0)).collect()))
    }
}

impl KafkaSettings {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// The value of librdkafka's setting `name`, under whichever of its names it is set.
    pub fn property(&self, name: &str) -> Option<&str> {
        let property = librdkafka_property(name);
        self.iter()
            .find(|(k, _)| librdkafka_property(k) == property)
            .map(|(_, v)| v)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0.insert(key.into(), value.into());
    }

    /// Sets `key`, replacing the setting under any other name librdkafka reads it by: a later
    /// layer wins, whichever name either layer wrote it under.
    fn set(&mut self, key: &str, value: impl Into<String>) {
        set_property(&mut self.0, key, value.into());
    }

    /// The value as it may be logged: itself, or `<redacted>` for a secret key.
    pub fn loggable<'a>(key: &str, value: &'a str) -> &'a str {
        if is_secret_key(key) {
            "<redacted>"
        } else {
            value
        }
    }
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for KafkaSettings {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl std::fmt::Debug for KafkaSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.iter().map(|(k, v)| (k, Self::loggable(k, v))))
            .finish()
    }
}

/// True for a librdkafka key whose value must not be logged.
///
/// The design's pattern (`password|secret|\.pem|key`, §2.1) plus the two keys librdkafka
/// itself marks sensitive that the pattern misses (rdkafka_conf.c, `_RK_SENSITIVE`). Generous
/// on purpose: `ssl.key.location` is only a path, and a path redacted costs nothing.
pub fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    ["password", "secret", ".pem", "key"]
        .iter()
        .any(|p| k.contains(p))
        || matches!(k.as_str(), "sasl.username" | "sasl.oauthbearer.config")
}

/// Keys whose value is a secret by itself, as opposed to a path to one: written inline into
/// `[kafka.settings]`, each draws a warning (§2.5 rule 11).
const INLINE_SECRET_KEYS: &[&str] = &[
    "sasl.password",
    "ssl.key.password",
    "ssl.key.pem",
    "ssl.keystore.password",
    "sasl.oauthbearer.client.secret",
    "sasl.oauthbearer.client.credentials.client.secret",
    "sasl.oauthbearer.config",
    "sasl.oauthbearer.assertion.private.key.passphrase",
    "sasl.oauthbearer.assertion.private.key.pem",
];

// ------------------------------------------------------------------------- resolved form

/// Where a fresh app_id starts (§4.4 step 8). Offsets are always the NEXT offset to read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartPolicy {
    /// Every partition from its low watermark.
    Earliest,
    /// Every partition from its high watermark, which under `read_committed` is the LSO.
    Latest,
    /// The listed partitions from the given offsets. The rest from `rest` (`"*"`), or, when
    /// that is `None`, nowhere: the map must then list every partition, which is checked at
    /// open against the topic's metadata (`KafkaStartingOffsetsIncomplete`).
    Explicit {
        partitions: BTreeMap<i32, i64>,
        rest: Option<NamedStart>,
    },
}

impl StartPolicy {
    /// The explicit per-partition entries, which are what the takeover check reads as
    /// acknowledgments (§4.4 step 3). Empty for `earliest` and `latest`.
    pub fn partitions(&self) -> Option<&BTreeMap<i32, i64>> {
        match self {
            Self::Explicit { partitions, .. } => Some(partitions),
            _ => None,
        }
    }
}

/// `"earliest"` or `"latest"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NamedStart {
    Earliest,
    Latest,
}

/// Which environment variable a credential is read from, and whether it must be set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvRef {
    /// Turned off with `""`, or a credential with no default name.
    Disabled,
    /// kdi's name, taken because nothing was configured. Optional: unset is a no-op.
    Default(String),
    /// A name the config set. Required: unset is an error at open (§2.5 rule 10).
    Explicit(String),
}

impl EnvRef {
    /// `pipeline`, else `section`, else `default`; an empty name turns it off.
    fn resolve(pipeline: &Option<String>, section: &Option<String>, default: Option<&str>) -> Self {
        match pipeline.as_ref().or(section.as_ref()) {
            Some(n) if n.trim().is_empty() => Self::Disabled,
            Some(n) => Self::Explicit(n.trim().to_string()),
            None => default.map_or(Self::Disabled, |d| Self::Default(d.to_string())),
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Disabled => None,
            Self::Default(n) | Self::Explicit(n) => Some(n),
        }
    }

    pub fn required(&self) -> bool {
        matches!(self, Self::Explicit(_))
    }

    /// The variable's value, `None` where an optional one is unset, and an error where a
    /// required one is.
    fn read(&self, what: &str, env: &dyn Fn(&str) -> Option<String>) -> Result<Option<String>> {
        let Some(name) = self.name() else {
            return Ok(None);
        };
        match env(name) {
            Some(v) => Ok(Some(v)),
            None if self.required() => Err(Error::Config(format!(
                "{what} names the environment variable {name:?}, which is not set"
            ))),
            None => Ok(None),
        }
    }
}

/// The Schema Registry a pipeline decodes with, as names; nothing is read until open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedSchemaRegistry {
    pub url: String,
    pub username_env: EnvRef,
    pub password_env: EnvRef,
    pub proxy_env: EnvRef,
    pub ca_pem_env: EnvRef,
}

/// Everything a Kafka pipeline needs beyond what [`ResolvedPipeline`] already says.
///
/// Holds no secret it read: credentials are variable names, read by [`Self::client_settings`]
/// at open. `settings` may hold a value written inline, which its `Debug` redacts.
#[derive(Clone, Debug)]
pub struct ResolvedKafka {
    pub topic: String,
    pub group_id: String,
    pub brokers: String,
    pub value_format: ValueFormat,
    /// The registry subject the SQL path's union is read from. `<topic>-value` by default.
    pub subject: String,
    /// `Some` whenever a URL was configured; always for `avro_confluent`.
    pub schema_registry: Option<ResolvedSchemaRegistry>,
    /// `[kafka.settings]`, then the pipeline's own, which wins key by key.
    pub settings: KafkaSettings,
    /// librdkafka key → environment variable, `[kafka.settings_env]` then the pipeline's.
    pub settings_env: BTreeMap<String, String>,
    pub scram_json_env: EnvRef,
    pub ssl_cert_env: EnvRef,
    pub ssl_key_env: EnvRef,
    pub ssl_ca_env: EnvRef,
    pub starting_offsets: StartPolicy,
    /// True when the pipeline itself sets `starting_offsets`. `[kafka]`'s value is a fleet
    /// default, which `pipelines.example.toml` sets, so it is not taken as a decision about
    /// this table: the typo guard runs unless the pipeline says where to start (§4.4 step 7).
    pub starting_offsets_explicit: bool,
    pub kdi_compat: Option<bool>,
    pub kdi_null_text: Option<bool>,
    pub metadata_prefix: String,
    pub max_messages_per_batch: Option<usize>,
    pub commit_offsets_to_kafka: bool,
    pub revoke_flush_timeout_secs: u64,
    pub blocked_recheck_secs: u64,
    /// Manual assignment, stopping at the end of the topic. Set by `ddi once`, never by the
    /// file (§3.8).
    pub once: bool,
}

/// librdkafka keys ddi sets itself, each with the config key that owns it, or why nobody may
/// set it (§2.4 step 5). An alias counts as its key.
const FORCED_KEYS: &[(&str, &str)] = &[
    ("bootstrap.servers", "ddi sets it from `brokers`"),
    (
        "metadata.broker.list",
        "it is librdkafka's name for bootstrap.servers, which ddi sets from `brokers`",
    ),
    ("group.id", "ddi sets it from the pipeline's `group_id`"),
    (
        "enable.auto.commit",
        "ddi keeps offsets in the target's Delta log, never in the group; \
         `commit_offsets_to_kafka = true` copies them there for lag tooling",
    ),
    (
        "auto.commit.enable",
        "it is the legacy name of enable.auto.commit, which ddi sets to false",
    ),
    (
        "enable.auto.offset.store",
        "ddi keeps offsets in the target's Delta log and sets it to false",
    ),
    (
        "auto.offset.reset",
        "ddi sets it to `error`, so a missing offset is a typed error and never a jump; \
         where a new app_id starts is `starting_offsets`",
    ),
    (
        "group.protocol",
        "ddi sets it to `classic`: KIP-848's `consumer` protocol is always incremental, and \
         ddi assigns eagerly only",
    ),
    ("enable.partition.eof", "ddi sets it to false"),
];

// ------------------------------------------------------------------- librdkafka's names
//
// One setting can be written under several names, and rdkafka's `ClientConfig` keeps them as
// separate keys of a `HashMap`, which it hands to librdkafka in iteration order (rdkafka 0.39
// config.rs:197, 296-300): two names for one setting are applied in an order that changes
// from one client to the next. So ddi compares names as the setting librdkafka 2.12.1 sets for
// them, from its property table (rdkafka-sys 4.10.0, librdkafka/src/rdkafka_conf.c), which
// `librdkafka_reads_names_as_ddi_does` checks against the library itself.

/// Global properties whose own name starts with `topic.`: librdkafka finds them among the
/// global ones before it would strip that prefix.
const GLOBAL_TOPIC_NAMES: &[&str] = &[
    "topic.blacklist",
    "topic.metadata.propagation.max.ms",
    "topic.metadata.refresh.fast.cnt",
    "topic.metadata.refresh.fast.interval.ms",
    "topic.metadata.refresh.interval.ms",
    "topic.metadata.refresh.sparse",
];

/// librdkafka's global aliases (`_RK_C_ALIAS`), each with the property it sets.
const GLOBAL_ALIASES: &[(&str, &str)] = &[
    ("bootstrap.servers", "metadata.broker.list"),
    ("compression.type", "compression.codec"),
    ("linger.ms", "queue.buffering.max.ms"),
    ("max.in.flight", "max.in.flight.requests.per.connection"),
    ("max.partition.fetch.bytes", "fetch.message.max.bytes"),
    ("retries", "message.send.max.retries"),
    ("sasl.mechanism", "sasl.mechanisms"),
    (
        "sasl.oauthbearer.client.credentials.client.id",
        "sasl.oauthbearer.client.id",
    ),
    (
        "sasl.oauthbearer.client.credentials.client.secret",
        "sasl.oauthbearer.client.secret",
    ),
];

/// Topic-level properties (and aliases) with no global property of the same name, so that
/// librdkafka sets them on the default topic config also when written without `topic.`.
const TOPIC_ONLY_NAMES: &[&str] = &[
    "acks",
    "auto.commit.enable",
    "auto.offset.reset",
    "compression.level",
    "consume.callback.max.messages",
    "delivery.timeout.ms",
    "message.timeout.ms",
    "msg_order_cmp",
    "offset.store.path",
    "offset.store.sync.interval.ms",
    "partitioner",
    "partitioner_cb",
    "produce.offset.report",
    "queuing.strategy",
    "request.required.acks",
    "request.timeout.ms",
];

/// librdkafka's topic-level aliases, each with the property it sets.
const TOPIC_ALIASES: &[(&str, &str)] = &[
    ("acks", "request.required.acks"),
    ("compression.type", "compression.codec"),
    ("delivery.timeout.ms", "message.timeout.ms"),
    ("enable.auto.commit", "auto.commit.enable"),
];

/// The setting librdkafka sets for `key`, one name per setting: a global property by its
/// name, a topic-level one as `topic.<name>`, an alias as the property it stands for.
///
/// `rd_kafka_conf_set` looks a name up among the global properties first, following an alias
/// to its property; a name it does not find there it sets on the default topic config, after
/// stripping one leading `topic.` (rdkafka_conf.c:1903-1907, 2544-2580). So
/// `topic.auto.offset.reset` is `auto.offset.reset`, `topic.enable.auto.commit` is the
/// topic-level `auto.commit.enable` (not the global `enable.auto.commit`), and
/// `sasl.mechanism` is `sasl.mechanisms`. A name librdkafka does not know is itself, and
/// fails when the client is created.
fn librdkafka_property(key: &str) -> std::borrow::Cow<'_, str> {
    let alias = |table: &[(&str, &'static str)], name: &str| {
        table.iter().find(|(a, _)| *a == name).map(|(_, p)| *p)
    };
    if let Some(property) = alias(GLOBAL_ALIASES, key) {
        return property.into();
    }
    let topic_level = match key.strip_prefix("topic.") {
        Some(name) if !GLOBAL_TOPIC_NAMES.contains(&key) => Some(name),
        Some(_) => None,
        None => TOPIC_ONLY_NAMES.contains(&key).then_some(key),
    };
    match topic_level {
        Some(name) => format!("topic.{}", alias(TOPIC_ALIASES, name).unwrap_or(name)).into(),
        None => key.into(),
    }
}

/// `map[key] = value`, after removing the setting under every other name librdkafka reads
/// it by, so that no two names for one setting reach the client.
fn set_property(map: &mut BTreeMap<String, String>, key: &str, value: String) {
    let property = librdkafka_property(key);
    map.retain(|k, _| librdkafka_property(k) != property);
    map.insert(key.to_string(), value);
}

/// Refuses one table that writes one librdkafka setting under two names: nothing says which
/// of the two it means.
fn refuse_two_names(keys: &[&str], place: &str) -> Result<()> {
    let mut seen: BTreeMap<std::borrow::Cow<'_, str>, &str> = BTreeMap::new();
    for key in keys {
        if let Some(first) = seen.insert(librdkafka_property(key), key) {
            return Err(Error::Config(format!(
                "{place}: {first:?} and {key:?} are one librdkafka setting under two names, \
                 and nothing says which value is meant; keep one"
            )));
        }
    }
    Ok(())
}

/// The values [`FORCED_KEYS`] get, in the order they are applied.
fn forced_values(brokers: &str, group_id: &str) -> [(&'static str, String); 7] {
    [
        ("bootstrap.servers", brokers.to_string()),
        ("group.id", group_id.to_string()),
        ("enable.auto.commit", "false".into()),
        ("enable.auto.offset.store", "false".into()),
        // RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET (-140) instead of a jump (CONFIGURATION.md:203).
        ("auto.offset.reset", "error".into()),
        // "Default is `classic`, but will change to `consumer`" (CONFIGURATION.md:133).
        ("group.protocol", "classic".into()),
        ("enable.partition.eof", "false".into()),
    ]
}

/// ddi's defaults, the first layer of §2.4, which any setting overrides.
fn default_settings(name: &str) -> [(&'static str, String); 4] {
    [
        ("client.id", format!("ddi-{name}")),
        // Lag and freshness come from the stats callback (§3.5).
        ("statistics.interval.ms", "15000".into()),
        ("max.poll.interval.ms", "600000".into()),
        // librdkafka prefetches 64 MB per consumer by default (CONFIGURATION.md:141), and
        // one process runs many; the budget reserves what this leaves (§3.7).
        ("queued.max.messages.kbytes", "16384".into()),
    ]
}

impl ResolvedKafka {
    /// librdkafka's settings for this pipeline, in the order of docs/kafka-design.md §2.4,
    /// later layers winning:
    ///
    /// 1. ddi's defaults (`client.id = ddi-<name>`, stats every 15 s, a 600 s poll interval,
    ///    16 MB of prefetch);
    /// 2. kdi's variables: the SCRAM JSON as `sasl.username`/`sasl.password`, the PEMs as
    ///    `ssl.certificate.pem`/`ssl.key.pem`/`ssl.ca.pem` (kdi lib.rs:1297-1341);
    /// 3. `[kafka.settings]`, then the pipeline's;
    /// 4. `settings_env`, the section's then the pipeline's;
    /// 5. the forced keys, which no layer above may have set.
    ///
    /// `env` reads a variable: [`std::env::var`] at open, a map in tests. This is the only
    /// place a secret is read, and the result redacts it in `Debug`. Errors name a variable,
    /// never its value. Turning the map into an rdkafka `ClientConfig` is the source's (M3).
    pub fn client_settings(
        &self,
        name: &str,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<KafkaSettings> {
        let mut out = KafkaSettings::default();
        for (k, v) in default_settings(name) {
            out.insert(k, v);
        }

        if let Some(json) = self.scram_json_env.read("scram_json_env", env)? {
            let var = self.scram_json_env.name().unwrap_or_default();
            let (username, password) = scram_identity(var, &json)?;
            out.insert("sasl.username", username);
            out.insert("sasl.password", password);
        }
        for (env_ref, what, key) in [
            (&self.ssl_cert_env, "ssl_cert_env", "ssl.certificate.pem"),
            (&self.ssl_key_env, "ssl_key_env", "ssl.key.pem"),
            (&self.ssl_ca_env, "ssl_ca_env", "ssl.ca.pem"),
        ] {
            if let Some(pem) = env_ref.read(what, env)? {
                out.insert(key, pem);
            }
        }

        // A later layer replaces an earlier one's setting under any of librdkafka's names for it.
        for (k, v) in self.settings.iter() {
            out.set(k, v);
        }
        for (key, var) in &self.settings_env {
            let value = env(var).ok_or_else(|| {
                Error::Config(format!(
                    "settings_env {key:?} names the environment variable {var:?}, which is \
                     not set"
                ))
            })?;
            out.set(key, value);
        }

        // Steps 1 and 2 never set a forced key, so any here came from the user, and a value
        // that came from the environment is judged now, at open, like one in the file was
        // at load.
        for key in out.keys() {
            refuse_forced_key(key, "settings")?;
        }
        check_values(&out, |key| out.property(key).is_some()).map_err(Error::Config)?;

        for (k, v) in forced_values(&self.brokers, &self.group_id) {
            out.insert(k, v);
        }
        Ok(out)
    }
}

/// kdi's SCRAM JSON, `{"username": "..", "password": ".."}`. kdi panicked on a bad one
/// (lib.rs:1310); here it is a config error that names the variable and not its content.
fn scram_identity(var: &str, json: &str) -> Result<(String, String)> {
    let bad = || {
        Error::Config(format!(
            "the environment variable {var:?} must hold a JSON object with string \"username\" \
             and \"password\" (kdi's KAFKA_DELTA_INGEST_SCRAM_JSON), and it does not"
        ))
    };
    let value: serde_json::Value = serde_json::from_str(json).map_err(|_| bad())?;
    let field = |k: &str| value.get(k).and_then(|v| v.as_str()).map(str::to_string);
    match (field("username"), field("password")) {
        (Some(u), Some(p)) => Ok((u, p)),
        _ => Err(bad()),
    }
}

// --------------------------------------------------------------------------- validation

/// A forced key, refused wherever the user set it, under any name librdkafka reads as it
/// (§2.5 rule 7): `topic.auto.offset.reset = "earliest"` beside ddi's `auto.offset.reset =
/// "error"` would otherwise win in some clients and not in others, and jump past a retention
/// gap without the -140 that blocks it.
fn refuse_forced_key(key: &str, place: &str) -> Result<()> {
    let property = librdkafka_property(key);
    let forced = FORCED_KEYS.iter().find(|(k, _)| *k == key).or_else(|| {
        FORCED_KEYS
            .iter()
            .find(|(k, _)| librdkafka_property(k) == property)
    });
    match forced {
        Some((k, why)) if *k == key => Err(Error::Config(format!(
            "{place}: {k:?} is ddi's to set: {why}"
        ))),
        Some((k, why)) => Err(Error::Config(format!(
            "{place}: {key:?} is librdkafka's {k:?} under another name, which is ddi's to \
             set: {why}"
        ))),
        None => Ok(()),
    }
}

/// What a librdkafka value may not say, and what it may say with a warning.
///
/// `has_key` answers whether some layer will set a key, including ones whose value is not
/// known yet (a `settings_env` entry at load), so a mechanism supplied from the environment
/// is not taken for a missing one.
fn check_values(
    settings: &KafkaSettings,
    has_key: impl Fn(&str) -> bool,
) -> std::result::Result<Vec<String>, String> {
    let mut warnings = Vec::new();

    // Rule 8. kdi ran with librdkafka's default `range,roundrobin` (CONFIGURATION.md:129).
    // `cooperative-sticky` is accepted when the consumer is created (probe, §0.3.21), and
    // incremental rebalancing is exactly what deferred assignment cannot follow.
    if let Some(s) = settings.property("partition.assignment.strategy") {
        let strategies: Vec<String> = s
            .split(',')
            .map(|x| x.trim().to_ascii_lowercase())
            .collect();
        if strategies.iter().any(|x| x != "range" && x != "roundrobin") {
            return Err(format!(
                "partition.assignment.strategy = {s:?}: ddi assigns eagerly only, so it takes \
                 \"range\", \"roundrobin\" or both, never \"cooperative-sticky\". Remove it to \
                 keep librdkafka's default, \"range,roundrobin\", which kdi ran with."
            ));
        }
    }

    // Rule 9. The build has no libsasl2; librdkafka's own default mechanism is GSSAPI, so a
    // SASL protocol without a mechanism asks for it too. Each layer has replaced the one
    // before under either name, so the one value here is the one librdkafka gets.
    if let Some(m) = settings.property("sasl.mechanisms") {
        if m.trim().eq_ignore_ascii_case("GSSAPI") {
            return Err(
                "sasl.mechanism = \"GSSAPI\" (Kerberos) is not supported: this build \
                 has no libsasl2. PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 and OAUTHBEARER \
                 are."
                    .into(),
            );
        }
    }
    let sasl = settings
        .property("security.protocol")
        .is_some_and(|p| p.trim().to_ascii_lowercase().starts_with("sasl_"));
    if sasl && !has_key("sasl.mechanisms") {
        return Err(
            "security.protocol is SASL but sasl.mechanism is not set, and librdkafka's \
             default mechanism is GSSAPI (Kerberos), which this build does not have. \
             Set sasl.mechanism, e.g. \"SCRAM-SHA-512\"."
                .into(),
        );
    }

    // Rule 15.
    if let Some(level) = settings.property("isolation.level") {
        if level.trim().eq_ignore_ascii_case("read_uncommitted") {
            warnings.push(
                "isolation.level = \"read_uncommitted\": rows of transactions that are later \
                 aborted will be written to the target, and `ddi once` ends at the high \
                 watermark rather than the last stable offset"
                    .into(),
            );
        }
    }
    Ok(warnings)
}

/// `starting_offsets` as a policy, or why it is not one (§2.5 rule 12).
fn resolve_start(s: &StartingOffsets) -> std::result::Result<StartPolicy, String> {
    let named = |v: &str| match v.trim().to_ascii_lowercase().as_str() {
        "earliest" => Some(NamedStart::Earliest),
        "latest" => Some(NamedStart::Latest),
        _ => None,
    };
    match s {
        StartingOffsets::Named(v) => match named(v) {
            Some(NamedStart::Earliest) => Ok(StartPolicy::Earliest),
            Some(NamedStart::Latest) => Ok(StartPolicy::Latest),
            None => Err(format!(
                "starting_offsets = {v:?} is none of \"earliest\", \"latest\", or a table of \
                 partition = next offset"
            )),
        },
        StartingOffsets::Map(m) => {
            if m.is_empty() {
                return Err(
                    "starting_offsets = {} names no partition. Use \"earliest\" or \
                     \"latest\", or list partitions (with \"*\" for the rest)"
                        .into(),
                );
            }
            let mut partitions = BTreeMap::new();
            let mut rest = None;
            for (key, value) in m {
                if key.trim() == "*" {
                    rest = Some(match value {
                        OffsetValue::Named(v) => named(v).ok_or_else(|| {
                            format!(
                                "starting_offsets \"*\" = {v:?}: the rest of the partitions \
                                 start at \"earliest\" or \"latest\""
                            )
                        })?,
                        OffsetValue::Offset(o) => {
                            return Err(format!(
                                "starting_offsets \"*\" = {o}: one offset cannot mean the same \
                                 message in every partition. Use \"earliest\" or \"latest\" for \
                                 the rest, and list the partitions that need an offset"
                            ))
                        }
                    });
                    continue;
                }
                let p: i32 = key.trim().parse().ok().filter(|p| *p >= 0).ok_or_else(|| {
                    format!(
                        "starting_offsets key {key:?} is neither a partition number (0 or \
                         more) nor \"*\""
                    )
                })?;
                match value {
                    OffsetValue::Offset(o) if *o >= 0 => {
                        partitions.insert(p, *o);
                    }
                    OffsetValue::Offset(o) => {
                        return Err(format!(
                            "starting_offsets \"{p}\" = {o}: an offset is 0 or more (the next \
                             offset to read)"
                        ))
                    }
                    OffsetValue::Named(v) => {
                        return Err(format!(
                            "starting_offsets \"{p}\" = {v:?}: a partition takes the next \
                             offset to read, a number. \"earliest\" and \"latest\" go under \
                             \"*\", for the partitions not listed"
                        ))
                    }
                }
            }
            Ok(StartPolicy::Explicit { partitions, rest })
        }
    }
}

/// `[a-z_][a-z0-9_]*` (§2.5 rule 12): a column name no engine needs quoted.
fn valid_metadata_prefix(p: &str) -> bool {
    let mut chars = p.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// A topic name Kafka would create: 1-249 of `[A-Za-z0-9._-]`, and not `.` or `..`. This also
/// keeps out librdkafka's `^regex` subscriptions: a pipeline reads exactly one topic.
fn valid_topic(t: &str) -> bool {
    (1..=249).contains(&t.len())
        && t != "."
        && t != ".."
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// A URI compared as the same table however it was written at its end.
fn same_table(uri: &str) -> &str {
    uri.trim().trim_end_matches('/')
}

/// The name a Kafka entry is reported under: its own, or its place in the file when it has
/// none.
pub(super) fn display_name(index: usize, p: &KafkaPipelineConfig) -> String {
    if p.name.trim().is_empty() {
        format!("kafka.pipeline[{index}]")
    } else {
        p.name.clone()
    }
}

/// The group a Kafka entry joins: its own, or `ddi-<name>`.
fn group_of(p: &KafkaPipelineConfig) -> String {
    p.group_id
        .clone()
        .unwrap_or_else(|| format!("ddi-{}", p.name))
}

/// True when an entry has the keys the rules between entries compare. One without them is
/// held back by rule 2, and comparing its empty values would only add a second, misleading
/// reason.
fn comparable(p: &KafkaPipelineConfig) -> bool {
    [&p.name, &p.app_id, &p.topic, &p.target_uri]
        .iter()
        .all(|v| !v.trim().is_empty())
}

fn dq_of(target_uri: &str, dq_uri: &Option<String>) -> String {
    dq_uri
        .clone()
        .unwrap_or_else(|| crate::dq::uri_for(target_uri))
}

/// The cross-pipeline rules of §2.5 (3-6) that involve a Kafka entry, and the Delta
/// pipelines they condemn. Delta pipelines among themselves keep `resolve_all`'s own checks.
pub(super) struct Conflicts {
    /// Per Kafka entry, by position: why it is held back.
    pub kafka: Vec<Option<String>>,
    /// Delta pipelines, by position in the list `resolve_all` checks: why they are.
    pub delta: Vec<(usize, String)>,
}

pub(super) fn conflicts(kafka: &[KafkaPipelineConfig], delta: &[PipelineConfig]) -> Conflicts {
    let mut out = Conflicts {
        kafka: vec![None; kafka.len()],
        delta: Vec::new(),
    };
    if kafka.is_empty() {
        return out;
    }
    let mut condemn = |i: usize, reason: String| {
        out.kafka[i].get_or_insert(reason);
    };
    let names = |ix: &[usize]| {
        ix.iter()
            .map(|&i| format!("{:?}", display_name(i, &kafka[i])))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // Rule 3: one namespace for both kinds.
    let mut by_name: BTreeMap<&str, (Vec<usize>, Vec<usize>)> = BTreeMap::new();
    for (i, p) in kafka.iter().enumerate() {
        if !p.name.trim().is_empty() {
            by_name.entry(p.name.as_str()).or_default().0.push(i);
        }
    }
    for (j, p) in delta.iter().enumerate() {
        if let Some(e) = by_name.get_mut(p.name.as_str()) {
            e.1.push(j);
        }
    }
    for (name, (ks, ds)) in &by_name {
        let n = ks.len() + ds.len();
        if n < 2 {
            continue;
        }
        let reason = format!(
            "duplicate pipeline name {name:?}: {n} entries share it across [[pipeline]] and \
             [[kafka.pipeline]], so none of them can be addressed unambiguously"
        );
        for &i in ks {
            condemn(i, reason.clone());
        }
        for &j in ds {
            out.delta.push((j, reason.clone()));
        }
    }

    // Rule 4: an app_id is an offset key per table, so it may repeat across tables (kdi's
    // default `kafka_delta_ingest` usually does) but never within one, nor within one DQ table,
    // whose rows are told apart by it.
    let mut by_target_app: BTreeMap<(&str, &str), Vec<usize>> = BTreeMap::new();
    let mut by_dq_app: BTreeMap<(String, &str), Vec<usize>> = BTreeMap::new();
    let mut by_target_topic: BTreeMap<(&str, &str), Vec<usize>> = BTreeMap::new();
    let mut by_group: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (i, p) in kafka.iter().enumerate().filter(|(_, p)| comparable(p)) {
        let target = same_table(&p.target_uri);
        let app = p.app_id.trim();
        by_target_app.entry((target, app)).or_default().push(i);
        by_dq_app
            .entry((
                same_table(&dq_of(&p.target_uri, &p.dq_uri)).to_string(),
                app,
            ))
            .or_default()
            .push(i);
        by_target_topic
            .entry((target, p.topic.trim()))
            .or_default()
            .push(i);
        by_group.entry(group_of(p)).or_default().push(i);
    }
    for ((target, app), ix) in &by_target_app {
        if ix.len() > 1 {
            for &i in ix {
                condemn(
                    i,
                    format!(
                        "Kafka pipelines {} all write {target:?} under app_id {app:?}. Their \
                         offsets are txns `{app}-<partition>` in that table's log, so they \
                         would overwrite each other's; all of them are held back. Give each \
                         its own app_id.",
                        names(ix)
                    ),
                );
            }
        }
    }
    for ((dq, app), ix) in &by_dq_app {
        if ix.len() > 1 {
            for &i in ix {
                condemn(
                    i,
                    format!(
                        "Kafka pipelines {} all record rejects in {dq:?} under app_id {app:?}, \
                         where nothing would tell their rows apart; all of them are held back. \
                         Give each its own app_id or dq_uri.",
                        names(ix)
                    ),
                );
            }
        }
    }

    // Rule 5: one topic read twice into one table is every row twice.
    for ((target, topic), ix) in &by_target_topic {
        if ix.len() > 1 {
            for &i in ix {
                condemn(
                    i,
                    format!(
                        "Kafka pipelines {} all read topic {topic:?} into {target:?}, which \
                         would write every message once per pipeline; all of them are held \
                         back.",
                        names(ix)
                    ),
                );
            }
        }
    }

    // Rule 5, against Delta pipelines, and rule 4's last clause: a Delta pipeline's txn
    // `x-0` would be Kafka app_id `x`'s partition 0 in a shared target, and in a shared DQ
    // table Delta's equality rule would take a Kafka reject for one of its own.
    for (i, p) in kafka.iter().enumerate().filter(|(_, p)| comparable(p)) {
        let target = same_table(&p.target_uri);
        let dq = dq_of(&p.target_uri, &p.dq_uri);
        for (j, d) in delta.iter().enumerate() {
            if same_table(&d.target_uri) == target {
                let reason = format!(
                    "Kafka pipeline {:?} and pipeline {:?} both write {target:?}. A Delta \
                     pipeline's offset txn `<app_id>` and a Kafka pipeline's \
                     `<app_id>-<partition>` share one namespace in that table's log, so both \
                     are held back. Give each its own target.",
                    display_name(i, p),
                    d.name
                );
                condemn(i, reason.clone());
                out.delta.push((j, reason));
            } else if d.app_id.trim() == p.app_id.trim()
                && same_table(&dq_of(&d.target_uri, &d.dq_uri)) == same_table(&dq)
            {
                let reason = format!(
                    "Kafka pipeline {:?} and pipeline {:?} both record rejects in {:?} under \
                     app_id {:?}. The Delta pipeline's retry skips a reject it finds already \
                     recorded under its app_id, and could find the Kafka pipeline's; both are \
                     held back. Give each its own app_id or dq_uri.",
                    display_name(i, p),
                    d.name,
                    same_table(&dq),
                    p.app_id.trim()
                );
                condemn(i, reason.clone());
                out.delta.push((j, reason));
            }
        }
        if crate::stage::is_stage_uri(target) {
            condemn(
                i,
                format!(
                    "target_uri {target:?} is the staging table of a staged_upsert pipeline, \
                     which is private to the pair that owns it: rows written there are merged \
                     into a target this pipeline never named."
                ),
            );
        }
    }

    // Rule 6: two pipelines in one group would take partitions from each other.
    for (group, ix) in &by_group {
        if ix.len() > 1 {
            for &i in ix {
                condemn(
                    i,
                    format!(
                        "Kafka pipelines {} all join consumer group {group:?}, so each would \
                         rebalance the others' partitions away; all of them are held back. \
                         Leave group_id unset (`ddi-<name>`), or give each its own.",
                        names(ix)
                    ),
                );
            }
        }
    }
    out
}

/// `pipeline`'s key, else the section's.
fn pick<T: Clone>(pipeline: &Option<T>, section: &Option<T>) -> Option<T> {
    pipeline.clone().or_else(|| section.clone())
}

impl Config {
    /// One `[[kafka.pipeline]]`, merged with `[kafka]` and `[runtime]` and checked against
    /// every rule of §2.5 that one entry can break on its own. The rules between entries are
    /// [`conflicts`]; the ones that need the environment or the broker are at open.
    ///
    /// Returns the pipeline and the warnings it drew, which the caller logs.
    pub(super) fn resolve_kafka(
        &self,
        p: &KafkaPipelineConfig,
    ) -> Result<(ResolvedPipeline, Vec<String>)> {
        let s = &self.kafka;
        let d = &self.runtime;
        let fail = |m: String| Err(Error::Config(m));
        let mut warnings = Vec::new();

        // Rule 2.
        for (key, value) in [
            ("name", &p.name),
            ("app_id", &p.app_id),
            ("topic", &p.topic),
            ("target_uri", &p.target_uri),
        ] {
            if value.trim().is_empty() {
                return fail(format!("{key} is required and must not be empty"));
            }
        }
        let topic = p.topic.trim().to_string();
        if !valid_topic(&topic) {
            return fail(format!(
                "topic {topic:?} is not a Kafka topic name (1-249 of A-Z a-z 0-9 . _ -). A \
                 pipeline reads exactly one topic, never a pattern."
            ));
        }
        let brokers = pick(&p.brokers, &s.brokers).unwrap_or_default();
        if brokers.trim().is_empty() {
            return fail(
                "brokers is required: set it in [kafka], or on the pipeline (kdi's -k)".into(),
            );
        }
        let group_id = group_of(p);
        if group_id.trim().is_empty() {
            return fail("group_id = \"\" names no group. Leave it unset for `ddi-<name>`.".into());
        }
        let subject = p
            .subject
            .clone()
            .unwrap_or_else(|| format!("{topic}-value"));
        if subject.trim().is_empty() {
            return fail(
                "subject = \"\" names no subject. Leave it unset for \
                 `<topic>-value`."
                    .into(),
            );
        }
        if p.dq_uri.as_deref().is_some_and(|u| u.trim().is_empty()) {
            return fail(
                "dq_uri = \"\" names no table. Leave it unset for \
                 `<target_uri>__ddi_dq`."
                    .into(),
            );
        }

        let value_format = p.value_format.unwrap_or_default();
        let sr = merged_registry(s.schema_registry.as_ref(), p.schema_registry.as_ref());
        let schema_registry = match &sr.url {
            Some(url) => {
                let parsed = url::Url::parse(url.trim()).ok();
                let http = parsed.as_ref().is_some_and(|u| {
                    matches!(u.scheme(), "http" | "https") && u.host_str().is_some()
                });
                if !http {
                    return fail(format!("schema_registry.url {url:?} is not an http(s) URL"));
                }
                Some(ResolvedSchemaRegistry {
                    url: url.trim().to_string(),
                    username_env: EnvRef::resolve(
                        &sr.username_env,
                        &None,
                        Some(DEFAULT_SR_USERNAME_ENV),
                    ),
                    password_env: EnvRef::resolve(
                        &sr.password_env,
                        &None,
                        Some(DEFAULT_SR_PASSWORD_ENV),
                    ),
                    proxy_env: EnvRef::resolve(&sr.proxy_env, &None, Some(DEFAULT_SR_PROXY_ENV)),
                    ca_pem_env: EnvRef::resolve(&sr.ca_pem_env, &None, None),
                })
            }
            None if value_format == ValueFormat::AvroConfluent => {
                return fail(
                    "value_format = \"avro_confluent\" needs schema_registry.url (kdi's \
                     --avro), in [kafka.schema_registry] or on the pipeline"
                        .into(),
                )
            }
            None => None,
        };

        // Rules 7, 8, 9, 11, 15.
        for (place, keys) in [
            ("[kafka.settings]", s.settings.keys().collect::<Vec<_>>()),
            ("[kafka.pipeline.settings]", p.settings.keys().collect()),
            (
                "[kafka.settings_env]",
                s.settings_env.keys().map(String::as_str).collect(),
            ),
            (
                "[kafka.pipeline.settings_env]",
                p.settings_env.keys().map(String::as_str).collect(),
            ),
        ] {
            for key in &keys {
                refuse_forced_key(key, place)?;
            }
            refuse_two_names(&keys, place)?;
        }
        // A static member id written in the file is every member's, since every member reads
        // one file, and members of one group with one id fence each other out in turn (§3.4).
        for (place, written) in [
            ("[kafka.settings]", &s.settings),
            ("[kafka.pipeline.settings]", &p.settings),
        ] {
            if let Some(key) = written
                .keys()
                .find(|k| librdkafka_property(k) == "group.instance.id")
            {
                return fail(format!(
                    "{place}: {key:?} written in the file is the same on every member that \
                     reads it, and members of one group with one static id fence each other \
                     out in turn (FENCED_INSTANCE_ID, fatal to the consumer). Set it in \
                     settings_env, from a variable each member sets to its own value (a \
                     StatefulSet's pod name), or leave it unset."
                ));
            }
        }
        // The pipeline's key wins, under any of librdkafka's names for the setting.
        let mut settings = KafkaSettings::default();
        for (k, v) in s.settings.iter().chain(p.settings.iter()) {
            settings.set(k, v);
        }
        let mut settings_env = BTreeMap::new();
        for (k, v) in s.settings_env.iter().chain(&p.settings_env) {
            set_property(&mut settings_env, k, v.clone());
        }
        warnings.extend(
            check_values(&settings, |key| {
                let property = librdkafka_property(key);
                settings.property(key).is_some()
                    || settings_env
                        .keys()
                        .any(|k| librdkafka_property(k) == property)
            })
            .map_err(Error::Config)?,
        );
        for key in INLINE_SECRET_KEYS {
            if settings.contains_key(key) {
                warnings.push(format!(
                    "{key} is written inline in the settings. It works, but the config file \
                     now holds a secret: prefer settings_env (or kdi's \
                     KAFKA_DELTA_INGEST_SCRAM_JSON for a SCRAM identity)."
                ));
            }
        }

        // Rule 12.
        if let Some(StartingOffsets::Map(m)) = &s.starting_offsets {
            if m.keys().any(|k| k.trim() != "*") {
                return fail(
                    "[kafka] starting_offsets lists partitions, which belong to one topic, and \
                     [kafka] applies to every pipeline. Set the map on the pipeline."
                        .into(),
                );
            }
        }
        let starting_offsets = match pick(&p.starting_offsets, &s.starting_offsets) {
            Some(written) => resolve_start(&written).map_err(Error::Config)?,
            None => StartPolicy::Earliest,
        };
        let allowed_latency_secs = pick(&p.allowed_latency_secs, &s.allowed_latency_secs)
            .unwrap_or(d.allowed_latency_secs);
        if allowed_latency_secs < 1 {
            return fail("allowed_latency_secs must be at least 1".into());
        }
        let max_messages_per_batch = pick(&p.max_messages_per_batch, &s.max_messages_per_batch);
        if max_messages_per_batch == Some(0) {
            return fail(
                "max_messages_per_batch = 0 would close every batch before its first \
                 message; use 1 or more, or leave it unset"
                    .into(),
            );
        }
        let max_bytes_written = pick(&p.max_bytes_per_batch, &s.max_bytes_per_batch)
            .unwrap_or_else(|| d.max_bytes_per_batch.clone());
        let max_bytes_per_batch = parse_size(&max_bytes_written, "max_bytes_per_batch")?;
        if max_bytes_per_batch < MIN_MAX_BYTES_PER_BATCH {
            return fail(format!(
                "max_bytes_per_batch = {max_bytes_written:?} is under 1MB; every batch that \
                 small is a Delta commit for a handful of messages"
            ));
        }
        let target_file_size = parse_size(
            &pick(&p.target_file_size, &s.target_file_size)
                .unwrap_or_else(|| d.target_file_size.clone()),
            "target_file_size",
        )?;
        let revoke_flush_timeout_secs =
            pick(&p.revoke_flush_timeout_secs, &s.revoke_flush_timeout_secs)
                .unwrap_or(DEFAULT_REVOKE_FLUSH_TIMEOUT_SECS);
        if !(5..=300).contains(&revoke_flush_timeout_secs) {
            return fail(format!(
                "revoke_flush_timeout_secs = {revoke_flush_timeout_secs} is outside 5..=300. A \
                 revoke holds the whole group until it returns."
            ));
        }
        let blocked_recheck_secs = pick(&p.blocked_recheck_secs, &s.blocked_recheck_secs)
            .unwrap_or(DEFAULT_BLOCKED_RECHECK_SECS);
        if blocked_recheck_secs < 1 {
            return fail(
                "blocked_recheck_secs = 0 would re-check a paused pipeline's cause on every \
                 step; use 1 or more"
                    .into(),
            );
        }
        let metadata_prefix = pick(&p.metadata_prefix, &s.metadata_prefix)
            .unwrap_or_else(|| crate::dq::KAFKA_METADATA_PREFIX.to_string());
        if !valid_metadata_prefix(&metadata_prefix) {
            return fail(format!(
                "metadata_prefix {metadata_prefix:?} must match [a-z_][a-z0-9_]*, so the \
                 metadata columns need no quoting in any engine"
            ));
        }

        // Rule 13: the validator `[[pipeline]]` uses, with no lookups (§2.2).
        let transform_sql = match &p.transform_sql {
            Some(sql) => Some(normalise_sql_with_lookups(sql, &BTreeSet::new()).map_err(
                |e| match e {
                    Error::Config(m) => Error::Config(m),
                    other => Error::Config(other.to_string()),
                },
            )?),
            None => None,
        };

        let kdi_compat = pick(&p.kdi_compat, &s.kdi_compat);
        let kafka = ResolvedKafka {
            topic: topic.clone(),
            group_id,
            brokers: brokers.trim().to_string(),
            value_format,
            subject,
            schema_registry,
            settings,
            settings_env,
            scram_json_env: EnvRef::resolve(
                &p.scram_json_env,
                &s.scram_json_env,
                Some(DEFAULT_SCRAM_JSON_ENV),
            ),
            ssl_cert_env: EnvRef::resolve(
                &p.ssl_cert_env,
                &s.ssl_cert_env,
                Some(DEFAULT_SSL_CERT_ENV),
            ),
            ssl_key_env: EnvRef::resolve(&p.ssl_key_env, &s.ssl_key_env, Some(DEFAULT_SSL_KEY_ENV)),
            ssl_ca_env: EnvRef::resolve(&p.ssl_ca_env, &s.ssl_ca_env, Some(DEFAULT_SSL_CA_ENV)),
            starting_offsets,
            starting_offsets_explicit: p.starting_offsets.is_some(),
            kdi_compat,
            kdi_null_text: pick(&p.kdi_null_text, &s.kdi_null_text).or(kdi_compat),
            metadata_prefix,
            max_messages_per_batch,
            commit_offsets_to_kafka: pick(&p.commit_offsets_to_kafka, &s.commit_offsets_to_kafka)
                .unwrap_or(false),
            revoke_flush_timeout_secs,
            blocked_recheck_secs,
            once: false,
        };

        let resolved = ResolvedPipeline {
            name: p.name.clone(),
            app_id: p.app_id.trim().to_string(),
            // Logs only: nothing opens it, and `ddi validate` skips its storage check.
            source_uri: format!("kafka://{topic}"),
            target_uri: p.target_uri.clone(),
            lookups: Vec::new(),
            starting_version: 0,
            change_policy: Default::default(),
            transform_sql,
            allowed_latency_secs,
            near_time_poll_interval_secs: d.near_time_poll_interval_secs,
            max_bytes_per_batch,
            max_files_per_batch: d.max_files_per_batch,
            max_output_rows_per_batch: pick(
                &p.max_output_rows_per_batch,
                &s.max_output_rows_per_batch,
            )
            .unwrap_or(d.max_output_rows_per_batch),
            target_file_size,
            watermark_uri: None,
            dedup_timestamp: None,
            dedup_key: None,
            dedup_partition: Vec::new(),
            write_mode: WriteMode::Append,
            upsert_key: None,
            upsert_lookback: None,
            upsert_tiebreak: Vec::new(),
            upsert_grain_check: Default::default(),
            stage_for: None,
            dq_uri: p.dq_uri.clone(),
            max_evaluation_rejects_per_batch: pick(
                &p.max_evaluation_rejects_per_batch,
                &s.max_evaluation_rejects_per_batch,
            )
            .unwrap_or(d.max_evaluation_rejects_per_batch),
            storage: crate::storage::Storage::new(self.storage.options.clone()),
            source_relation: None,
            target_relation: None,
            publish: None,
            publish_to: None,
            kafka: Some(Arc::new(kafka)),
        };
        Ok((resolved, warnings))
    }
}

/// `[kafka.schema_registry]` with the pipeline's own table over it, key by key.
fn merged_registry(
    section: Option<&SchemaRegistryConfig>,
    pipeline: Option<&SchemaRegistryConfig>,
) -> SchemaRegistryConfig {
    let s = section.cloned().unwrap_or_default();
    let p = pipeline.cloned().unwrap_or_default();
    SchemaRegistryConfig {
        url: pick(&p.url, &s.url),
        username_env: pick(&p.username_env, &s.username_env),
        password_env: pick(&p.password_env, &s.password_env),
        proxy_env: pick(&p.proxy_env, &s.proxy_env),
        ca_pem_env: pick(&p.ca_pem_env, &s.ca_pem_env),
    }
}

/// Every Kafka pipeline resolved, and every one held back with its reason, in file order.
///
/// `kafka_built` is whether this build can run them; without it, every entry that passes the
/// rules is held back with [`NOT_BUILT`], and every one that fails them says so as well.
pub(super) fn resolve_entries(
    cfg: &Config,
    conflicts: &Conflicts,
    kafka_built: bool,
) -> (Vec<ResolvedPipeline>, Vec<super::Rejection>) {
    let mut ok = Vec::new();
    let mut rejected = Vec::new();
    for (i, p) in cfg.kafka.pipelines.iter().enumerate() {
        let name = display_name(i, p);
        let outcome = match &conflicts.kafka[i] {
            Some(reason) => Err(reason.clone()),
            None => cfg.resolve_kafka(p).map_err(|e| match e {
                Error::Config(m) => m,
                other => other.to_string(),
            }),
        };
        match outcome {
            Ok((r, warnings)) => {
                for w in warnings {
                    tracing::warn!(pipeline = %name, "{w}");
                }
                if kafka_built {
                    ok.push(r);
                } else {
                    rejected.push(super::Rejection {
                        name,
                        reason: NOT_BUILT.into(),
                    });
                }
            }
            Err(reason) => rejected.push(super::Rejection {
                name,
                reason: if kafka_built {
                    reason
                } else {
                    format!("{}. Also, {NOT_BUILT}", reason.trim_end_matches('.'))
                },
            }),
        }
    }
    (ok, rejected)
}

/// How many of `running` read Kafka.
pub fn count(running: &[ResolvedPipeline]) -> usize {
    running.iter().filter(|p| p.kafka.is_some()).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Resolved;

    /// A Delta pipeline, to show what a Kafka entry does and does not do to one.
    const DELTA: &str = r#"
[[pipeline]]
name = "orders"
app_id = "ddi.orders"
source_uri = "/tmp/bronze/orders"
target_uri = "/tmp/silver/orders"
"#;

    /// The smallest `[kafka]` an Avro pipeline runs with.
    const SECTION: &str = r#"
[kafka]
brokers = "b-1:9096,b-2:9096"

[kafka.schema_registry]
url = "https://sr.internal:8081"
"#;

    /// One `[[kafka.pipeline]]`, with `extra` lines after its required keys.
    fn entry(name: &str, extra: &str) -> String {
        format!(
            "\n[[kafka.pipeline]]\nname = {name:?}\napp_id = \"kafka_delta_ingest\"\n\
             topic = \"topic-{name}\"\ntarget_uri = \"/tmp/bronze/{name}\"\n{extra}\n"
        )
    }

    fn config(toml: &str) -> Config {
        Config::from_toml_str(toml).unwrap_or_else(|e| panic!("{e}\n---\n{toml}"))
    }

    /// As a build with the `kafka` feature resolves it, whichever this build is.
    fn resolve(toml: &str) -> Resolved {
        config(toml).resolve_all_with(true).unwrap()
    }

    fn running(r: &Resolved) -> Vec<&str> {
        r.pipelines.iter().map(|p| p.name.as_str()).collect()
    }

    fn held<'a>(r: &'a Resolved, name: &str) -> &'a str {
        r.rejected
            .iter()
            .find(|x| x.name == name)
            .map(|x| x.reason.as_str())
            .unwrap_or_else(|| panic!("{name:?} was not held back: {r:?}"))
    }

    /// The reason the only Kafka entry in `extra`'s config is held back.
    fn refused(section_extra: &str, pipeline_extra: &str) -> String {
        let r = resolve(&format!(
            "{SECTION}{section_extra}{}",
            entry("events", pipeline_extra)
        ));
        assert!(r.pipelines.is_empty(), "expected a rejection: {r:?}");
        held(&r, "events").to_string()
    }

    fn kafka<'a>(r: &'a Resolved, name: &str) -> &'a ResolvedKafka {
        r.pipelines
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| p.kafka.as_deref())
            .unwrap_or_else(|| panic!("{name:?} is not a running Kafka pipeline: {r:?}"))
    }

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    fn settings_of(r: &Resolved, name: &str, vars: &[(&str, &str)]) -> Result<KafkaSettings> {
        kafka(r, name).client_settings(name, &env(vars))
    }

    // ----------------------------------------------------------------- I16 and the manifest

    #[test]
    fn kafka_config_without_feature_is_held_back() {
        let cfg = config(&format!("{DELTA}{SECTION}{}", entry("events", "")));

        let r = cfg.resolve_all_with(false).unwrap();
        assert_eq!(running(&r), ["orders"], "the Delta pipeline runs");
        assert_eq!(r.rejected.len(), 1, "{r:?}");
        assert_eq!(held(&r, "events"), NOT_BUILT);

        // An entry that is wrong as well says what is wrong, and that it would be held back
        // anyway, so fixing it does not lead to a second surprise.
        let bad = config(&format!(
            "{DELTA}{SECTION}{}",
            entry("events", "brokers = \"\"")
        ))
        .resolve_all_with(false)
        .unwrap();
        assert_eq!(running(&bad), ["orders"]);
        let reason = held(&bad, "events");
        assert!(reason.contains("brokers"), "{reason}");
        assert!(
            reason.contains("built without the `kafka` feature"),
            "{reason}"
        );

        // And the build this test runs in.
        let real = cfg.resolve_all().unwrap();
        if cfg!(feature = "kafka") {
            assert_eq!(running(&real), ["orders", "events"]);
            assert!(real.rejected.is_empty(), "{real:?}");
        } else {
            assert_eq!(running(&real), ["orders"]);
            assert_eq!(held(&real, "events"), NOT_BUILT);
        }
    }

    #[test]
    fn kafka_runs_with_manifest_set() {
        let dir = tempfile::tempdir().unwrap();
        let write_manifest = |models: serde_json::Value| {
            let path = dir.path().join("manifest.json");
            let manifest = serde_json::json!({
                "nodes": models,
                "sources": {
                    "source.p.bronze.orders_raw": {
                        "name": "orders_raw", "resource_type": "source", "schema": "bronze"
                    }
                }
            });
            std::fs::write(&path, manifest.to_string()).unwrap();
            format!(
                "manifest = {:?}\n\n[storage]\nuri_template = \"file:///tmp/lake/{{schema}}/{{name}}\"\n",
                path.display().to_string()
            )
        };
        let model = serde_json::json!({
            "model.p.orders": {
                "name": "orders",
                "resource_type": "model",
                "schema": "silver",
                "compiled_code": "SELECT order_id, _timestamp FROM bronze.orders_raw",
                "depends_on": {"nodes": ["source.p.bronze.orders_raw"]},
                "config": {"materialized": "table"}
            }
        });

        // `[[pipeline]]` is ignored when `manifest` is set; `[[kafka.pipeline]]` is not.
        let head = write_manifest(model);
        let ignored = "\n[[pipeline]]\nname = \"ignored\"\napp_id = \"x\"\n\
                       source_uri = \"/tmp/a\"\ntarget_uri = \"/tmp/b\"\n";
        let r = resolve(&format!("{head}{ignored}{SECTION}{}", entry("events", "")));
        assert_eq!(running(&r), ["orders", "events"]);
        assert!(r.rejected.is_empty(), "{r:?}");

        // A project with nothing to stream yet does not keep the Kafka leg off the air...
        let head = write_manifest(serde_json::json!({}));
        let r = resolve(&format!("{head}{SECTION}{}", entry("events", "")));
        assert_eq!(running(&r), ["events"]);

        // ...and without one, it is the error it always was.
        let e = config(&head)
            .resolve_all_with(true)
            .unwrap_err()
            .to_string();
        assert!(e.contains("no streamable models"), "{e}");
    }

    #[test]
    fn nothing_to_run_counts_kafka_pipelines() {
        let r = resolve(&format!("{SECTION}{}", entry("events", "")));
        assert_eq!(running(&r), ["events"]);
        let e = config(SECTION)
            .resolve_all_with(true)
            .unwrap_err()
            .to_string();
        assert!(e.contains("nothing to run"), "{e}");
    }

    #[test]
    fn a_config_without_kafka_is_what_it_was() {
        let cfg = config(DELTA);
        assert!(cfg.kafka.is_empty());
        let r = cfg.resolve_all_with(true).unwrap();
        assert_eq!(running(&r), ["orders"]);
        assert!(r.pipelines[0].kafka.is_none());
        let rendered = toml::to_string(&cfg).unwrap();
        assert!(!rendered.contains("kafka"), "{rendered}");
    }

    // ------------------------------------------------------------------- merge and defaults

    #[test]
    fn section_defaults_overridden_by_pipeline() {
        let section = r#"
[kafka]
brokers = "section:9092"
allowed_latency_secs = 300
max_bytes_per_batch = "128MB"
target_file_size = "64MB"
metadata_prefix = "k_"
kdi_compat = true
revoke_flush_timeout_secs = 60
blocked_recheck_secs = 30
commit_offsets_to_kafka = true
max_messages_per_batch = 5000
scram_json_env = "SECTION_SCRAM"
ssl_ca_env = ""

[kafka.schema_registry]
url = "http://section-sr:8081"
username_env = "SECTION_SR_USER"

[kafka.settings]
"client.id" = "section-client"
"fetch.min.bytes" = "1"

[kafka.settings_env]
"sasl.password" = "SECTION_PASSWORD"
"#;
        let mine = entry(
            "events",
            r#"allowed_latency_secs = 10
metadata_prefix = "m_"
kdi_null_text = false
scram_json_env = "MY_SCRAM"
group_id = "kdi-group"
subject = "events-subject"
dq_uri = "/tmp/dq/events"
[kafka.pipeline.schema_registry]
url = "http://my-sr:8081"
[kafka.pipeline.settings]
"client.id" = "my-client"
[kafka.pipeline.settings_env]
"sasl.password" = "MY_PASSWORD""#,
        );
        let theirs = entry("other", "");
        let r = resolve(&format!("{section}{mine}{theirs}"));
        assert!(r.rejected.is_empty(), "{r:?}");

        let p = &r.pipelines[0];
        let k = kafka(&r, "events");
        assert_eq!(p.allowed_latency_secs, 10, "the pipeline's own key wins");
        assert_eq!(
            p.max_bytes_per_batch, 128_000_000,
            "the section fills the rest"
        );
        assert_eq!(p.target_file_size, 64_000_000);
        assert_eq!(k.metadata_prefix, "m_");
        assert_eq!(k.kdi_compat, Some(true));
        assert_eq!(k.kdi_null_text, Some(false));
        assert_eq!(k.revoke_flush_timeout_secs, 60);
        assert_eq!(k.blocked_recheck_secs, 30);
        assert!(k.commit_offsets_to_kafka);
        assert_eq!(k.max_messages_per_batch, Some(5000));
        assert_eq!(k.brokers, "section:9092");
        assert_eq!(k.group_id, "kdi-group");
        assert_eq!(k.subject, "events-subject");
        assert_eq!(p.dq_uri(), "/tmp/dq/events");
        assert_eq!(k.scram_json_env, EnvRef::Explicit("MY_SCRAM".into()));
        assert_eq!(
            k.ssl_ca_env,
            EnvRef::Disabled,
            "\"\" turns kdi's variable off"
        );
        assert_eq!(
            k.ssl_cert_env,
            EnvRef::Default(DEFAULT_SSL_CERT_ENV.into()),
            "unset takes kdi's name"
        );
        let sr = k.schema_registry.as_ref().unwrap();
        assert_eq!(
            sr.url, "http://my-sr:8081",
            "the registry merges key by key"
        );
        assert_eq!(sr.username_env, EnvRef::Explicit("SECTION_SR_USER".into()));
        assert_eq!(
            sr.password_env,
            EnvRef::Default(DEFAULT_SR_PASSWORD_ENV.into())
        );
        assert_eq!(sr.ca_pem_env, EnvRef::Disabled);
        assert_eq!(k.settings.get("client.id"), Some("my-client"));
        assert_eq!(k.settings.get("fetch.min.bytes"), Some("1"));
        assert_eq!(k.settings_env["sasl.password"], "MY_PASSWORD");

        // The other pipeline takes the section, then `[runtime]`, then ddi's own defaults.
        let q = &r.pipelines[1];
        let o = kafka(&r, "other");
        assert_eq!(q.allowed_latency_secs, 300);
        assert_eq!(o.metadata_prefix, "k_");
        assert_eq!(
            o.kdi_null_text,
            Some(true),
            "kdi_null_text defaults to kdi_compat"
        );
        assert_eq!(o.group_id, "ddi-other");
        assert_eq!(o.subject, "topic-other-value");
        assert_eq!(o.settings.get("client.id"), Some("section-client"));
        assert_eq!(o.settings_env["sasl.password"], "SECTION_PASSWORD");
        assert_eq!(
            o.schema_registry.as_ref().unwrap().url,
            "http://section-sr:8081"
        );
        assert_eq!(q.dq_uri(), "/tmp/bronze/other__ddi_dq");
        assert_eq!(q.max_evaluation_rejects_per_batch, 100);
        assert_eq!(q.max_output_rows_per_batch, 5_000_000);

        // What every Kafka pipeline is, whatever it says.
        assert_eq!(q.source_uri, "kafka://topic-other");
        assert_eq!(q.write_mode, WriteMode::Append);
        assert!(q.lookups.is_empty() && q.publish.is_none() && q.watermark_uri.is_none());
        assert!(q.dedup_timestamp.is_none() && q.upsert_key.is_none());
        assert!(!o.once);

        // A pipeline that sets nothing beyond what it must.
        let bare = resolve(&format!("{SECTION}{}", entry("bare", "")));
        let b = kafka(&bare, "bare");
        assert_eq!(
            bare.pipelines[0].allowed_latency_secs, 30,
            "[runtime]'s default"
        );
        assert_eq!(bare.pipelines[0].max_bytes_per_batch, 256_000_000);
        assert_eq!(b.metadata_prefix, "kafka_");
        assert_eq!(b.kdi_compat, None, "unset stays unset (§4.4 step 6)");
        assert_eq!(b.kdi_null_text, None);
        assert_eq!(b.revoke_flush_timeout_secs, 120);
        assert_eq!(b.blocked_recheck_secs, 60);
        assert_eq!(b.value_format, ValueFormat::AvroConfluent);
        assert_eq!(b.starting_offsets, StartPolicy::Earliest);
        assert!(!b.starting_offsets_explicit);
    }

    #[test]
    fn librdkafka_settings_merge_in_the_designed_order() {
        let r = resolve(&format!(
            "{SECTION}{}",
            entry(
                "events",
                r#"[kafka.pipeline.settings]
"sasl.username" = "from-settings"
"queued.max.messages.kbytes" = "8192"
"security.protocol" = "SASL_SSL"
"sasl.mechanism" = "SCRAM-SHA-512"
[kafka.pipeline.settings_env]
"ssl.ca.pem" = "MY_CA""#
            )
        ));
        let vars = [
            (
                DEFAULT_SCRAM_JSON_ENV,
                r#"{"username": "scram-user", "password": "scram-pass"}"#,
            ),
            (DEFAULT_SSL_CA_ENV, "kdi-ca"),
            ("MY_CA", "settings-env-ca"),
        ];
        let s = settings_of(&r, "events", &vars).unwrap();
        // 1. ddi's defaults, one of them overridden by a setting.
        assert_eq!(s.get("client.id"), Some("ddi-events"));
        assert_eq!(s.get("statistics.interval.ms"), Some("15000"));
        assert_eq!(s.get("max.poll.interval.ms"), Some("600000"));
        assert_eq!(s.get("queued.max.messages.kbytes"), Some("8192"));
        // 2 < 3: a setting overrides kdi's SCRAM username, and kdi's password stands.
        assert_eq!(s.get("sasl.username"), Some("from-settings"));
        assert_eq!(s.get("sasl.password"), Some("scram-pass"));
        // 2 < 4: settings_env overrides kdi's CA variable.
        assert_eq!(s.get("ssl.ca.pem"), Some("settings-env-ca"));
        // 5: the forced keys.
        assert_eq!(s.get("bootstrap.servers"), Some("b-1:9096,b-2:9096"));
        assert_eq!(s.get("group.id"), Some("ddi-events"));
        assert_eq!(s.get("enable.auto.commit"), Some("false"));
        assert_eq!(s.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(s.get("auto.offset.reset"), Some("error"));
        assert_eq!(s.get("group.protocol"), Some("classic"));
        assert_eq!(s.get("enable.partition.eof"), Some("false"));
    }

    #[test]
    fn starting_offsets_star_and_shapes() {
        let start = |section: &str, pipeline: &str| {
            let r = resolve(&format!("{SECTION}{section}{}", entry("events", pipeline)));
            let k = kafka(&r, "events");
            (k.starting_offsets.clone(), k.starting_offsets_explicit)
        };
        assert_eq!(start("", ""), (StartPolicy::Earliest, false));
        assert_eq!(
            start("", "starting_offsets = \"latest\""),
            (StartPolicy::Latest, true)
        );
        assert_eq!(
            start(
                "",
                "starting_offsets = { \"0\" = 1234, \"*\" = \"latest\" }"
            ),
            (
                StartPolicy::Explicit {
                    partitions: BTreeMap::from([(0, 1234)]),
                    rest: Some(NamedStart::Latest),
                },
                true
            )
        );
        assert_eq!(
            start("", "starting_offsets = { \"0\" = 0, \"2\" = 7 }"),
            (
                StartPolicy::Explicit {
                    partitions: BTreeMap::from([(0, 0), (2, 7)]),
                    rest: None,
                },
                true
            )
        );
        // `[kafka]`'s value is a fleet default, not a decision about this table (§4.4 step 7).
        let section_latest = "starting_offsets = \"latest\"\n";
        let with_section = SECTION.replace("[kafka]\n", &format!("[kafka]\n{section_latest}"));
        let r = resolve(&format!("{with_section}{}", entry("events", "")));
        let k = kafka(&r, "events");
        assert_eq!(k.starting_offsets, StartPolicy::Latest);
        assert!(!k.starting_offsets_explicit);
        let star = SECTION.replace(
            "[kafka]\n",
            "[kafka]\nstarting_offsets = { \"*\" = \"latest\" }\n",
        );
        let r = resolve(&format!("{star}{}", entry("events", "")));
        assert_eq!(
            kafka(&r, "events").starting_offsets,
            StartPolicy::Explicit {
                partitions: BTreeMap::new(),
                rest: Some(NamedStart::Latest)
            }
        );

        // Every shape that is not one is this pipeline's fault, not the file's.
        for (written, says) in [
            ("\"earliset\"", "none of"),
            ("{}", "names no partition"),
            ("{ \"-1\" = 5 }", "neither a partition"),
            ("{ \"zero\" = 5 }", "neither a partition"),
            ("{ \"0\" = -1 }", "0 or more"),
            ("{ \"0\" = \"latest\" }", "go under \\\"*\\\""),
            ("{ \"*\" = 5 }", "one offset cannot mean"),
            ("{ \"*\" = \"middle\" }", "the rest of the partitions"),
        ] {
            let reason = refused("", &format!("starting_offsets = {written}"));
            assert!(reason.contains("starting_offsets"), "{written}: {reason}");
            assert!(
                reason.contains(&says.replace("\\\"", "\"")),
                "{written}: {reason}"
            );
        }
        let section_map =
            SECTION.replace("[kafka]\n", "[kafka]\nstarting_offsets = { \"0\" = 5 }\n");
        let r = resolve(&format!("{section_map}{}", entry("events", "")));
        assert!(held(&r, "events").contains("belong to one topic"), "{r:?}");
    }

    // ------------------------------------------------------------------ rules between entries

    #[test]
    fn same_app_id_on_two_kafka_targets_allowed() {
        // kdi's default app_id, on two tables: txns live in each table's own log.
        let r = resolve(&format!("{SECTION}{}{}", entry("a", ""), entry("b", "")));
        assert_eq!(running(&r), ["a", "b"]);
        assert!(r.rejected.is_empty(), "{r:?}");
    }

    #[test]
    fn same_target_and_app_id_rejected() {
        let r = resolve(&format!(
            "{SECTION}{}{}",
            entry("a", ""),
            entry("b", "").replace("/tmp/bronze/b", "/tmp/bronze/a/")
        ));
        assert!(r.pipelines.is_empty(), "{r:?}");
        for name in ["a", "b"] {
            let reason = held(&r, name);
            assert!(reason.contains("app_id \"kafka_delta_ingest\""), "{reason}");
            assert!(reason.contains("\"a\", \"b\""), "{reason}");
        }
    }

    #[test]
    fn same_dq_table_and_app_id_rejected() {
        let dq = "dq_uri = \"/tmp/dq/shared\"";
        let r = resolve(&format!("{SECTION}{}{}", entry("a", dq), entry("b", dq)));
        assert!(r.pipelines.is_empty(), "{r:?}");
        assert!(held(&r, "a").contains("record rejects in"), "{r:?}");
        // With distinct app_ids, the rows say whose they are.
        let r = resolve(&format!(
            "{SECTION}{}{}",
            entry("a", dq),
            entry("b", dq).replace("kafka_delta_ingest", "other_app")
        ));
        assert_eq!(running(&r), ["a", "b"], "{r:?}");
    }

    #[test]
    fn shared_group_id_rejected() {
        let g = "group_id = \"kafka_delta_ingest\"";
        let r = resolve(&format!(
            "{SECTION}{}{}{}",
            entry("a", g),
            entry("b", g),
            entry("c", "")
        ));
        assert_eq!(running(&r), ["c"], "the default group is per pipeline");
        for name in ["a", "b"] {
            assert!(held(&r, name).contains("consumer group"), "{r:?}");
        }
    }

    #[test]
    fn names_are_unique_across_both_kinds() {
        let r = resolve(&format!("{DELTA}{SECTION}{}", entry("orders", "")));
        assert!(r.pipelines.is_empty(), "{r:?}");
        assert!(
            held(&r, "orders").contains("duplicate pipeline name"),
            "{r:?}"
        );
        assert_eq!(r.rejected.len(), 2, "both are held back: {r:?}");

        let r = resolve(&format!(
            "{SECTION}{}{}",
            entry("a", ""),
            entry("a", "").replace("/tmp/bronze/a", "/tmp/bronze/a2")
        ));
        assert!(r.pipelines.is_empty(), "{r:?}");
        assert_eq!(r.rejected.len(), 2);
    }

    #[test]
    fn a_kafka_target_shared_with_a_delta_pipeline_rejects_both() {
        let r = resolve(&format!(
            "{DELTA}{SECTION}{}",
            entry("events", "").replace("/tmp/bronze/events", "/tmp/silver/orders")
        ));
        assert!(r.pipelines.is_empty(), "{r:?}");
        for name in ["orders", "events"] {
            assert!(held(&r, name).contains("both write"), "{r:?}");
        }
        // Reading a Kafka target is the normal bronze → silver path.
        let r = resolve(&format!(
            "{DELTA}{SECTION}{}",
            entry("events", "").replace("/tmp/bronze/events", "/tmp/bronze/orders")
        ));
        assert_eq!(running(&r), ["orders", "events"], "{r:?}");
    }

    #[test]
    fn a_kafka_app_id_equal_to_a_delta_app_id_only_elsewhere() {
        let same_app =
            |extra: &str| entry("events", extra).replace("kafka_delta_ingest", "ddi.orders");
        let r = resolve(&format!("{DELTA}{SECTION}{}", same_app("")));
        assert_eq!(running(&r), ["orders", "events"], "different tables: {r:?}");

        // One DQ table, one app_id: Delta's equality rule could take a Kafka reject for its own.
        let r = resolve(&format!(
            "{DELTA}{SECTION}{}",
            same_app("dq_uri = \"/tmp/silver/orders__ddi_dq\"")
        ));
        assert!(r.pipelines.is_empty(), "{r:?}");
        assert!(held(&r, "orders").contains("record rejects"), "{r:?}");
        assert!(held(&r, "events").contains("record rejects"), "{r:?}");
    }

    #[test]
    fn one_topic_twice_into_one_table_rejected() {
        let r = resolve(&format!(
            "{SECTION}{}{}",
            entry("a", ""),
            entry("b", "")
                .replace("topic-b", "topic-a")
                .replace("/tmp/bronze/b", "/tmp/bronze/a")
                .replace("kafka_delta_ingest", "other_app")
        ));
        assert!(r.pipelines.is_empty(), "{r:?}");
        assert!(
            held(&r, "b").contains("every message once per pipeline"),
            "{r:?}"
        );
    }

    #[test]
    fn distinct_topics_and_app_ids_may_share_a_table() {
        let r = resolve(&format!(
            "{SECTION}{}{}",
            entry("a", ""),
            entry("b", "")
                .replace("/tmp/bronze/b", "/tmp/bronze/a")
                .replace("kafka_delta_ingest", "other_app")
        ));
        assert_eq!(running(&r), ["a", "b"], "{r:?}");
    }

    #[test]
    fn a_stage_table_is_no_kafka_target() {
        let r = resolve(&format!(
            "{SECTION}{}",
            entry("events", "").replace("/tmp/bronze/events", "/tmp/silver/x__ddi_stage")
        ));
        assert!(held(&r, "events").contains("staging table"), "{r:?}");
    }

    // ---------------------------------------------------------------- rules on one entry

    #[test]
    fn required_values() {
        for (key, line) in [
            ("brokers", "brokers = \"  \""),
            ("group_id", "group_id = \"\""),
            ("subject", "subject = \"\""),
            ("dq_uri", "dq_uri = \"\""),
        ] {
            let reason = refused("", line);
            assert!(reason.contains(key), "{key}: {reason}");
        }
        for (key, written) in [
            (
                "app_id",
                "name = \"events\"\napp_id = \"\"\ntopic = \"t\"\ntarget_uri = \"/tmp/t\"",
            ),
            (
                "topic",
                "name = \"events\"\napp_id = \"a\"\ntarget_uri = \"/tmp/t\"",
            ),
            (
                "target_uri",
                "name = \"events\"\napp_id = \"a\"\ntopic = \"t\"\ntarget_uri = \" \"",
            ),
        ] {
            let r = resolve(&format!("{SECTION}\n[[kafka.pipeline]]\n{written}\n"));
            assert!(
                held(&r, "events").contains(&format!("{key} is required")),
                "{key}: {r:?}"
            );
        }
        // Two incomplete entries are each held back for what they lack, not for each other.
        let r = resolve(&format!(
            "{SECTION}\n[[kafka.pipeline]]\nname = \"x\"\napp_id = \"a\"\ntopic = \"t\"\n\
             \n[[kafka.pipeline]]\nname = \"y\"\napp_id = \"a\"\ntopic = \"t\"\n"
        ));
        for name in ["x", "y"] {
            assert!(
                held(&r, name).contains("target_uri is required"),
                "{name}: {r:?}"
            );
        }
        // Without a name, the entry is held back under its place in the file.
        let r = resolve(&format!(
            "{SECTION}{}\n[[kafka.pipeline]]\napp_id = \"a\"\ntopic = \"t\"\ntarget_uri = \"/tmp/t\"\n",
            entry("events", "")
        ));
        assert_eq!(running(&r), ["events"]);
        assert!(held(&r, "kafka.pipeline[1]").contains("name"), "{r:?}");

        // Brokers from the pipeline alone are enough.
        let r = resolve(&format!(
            "[kafka.schema_registry]\nurl = \"http://sr:8081\"\n{}",
            entry("events", "brokers = \"b:9092\"")
        ));
        assert_eq!(kafka(&r, "events").brokers, "b:9092");
        let r = resolve(&format!(
            "[kafka.schema_registry]\nurl = \"http://sr:8081\"\n{}",
            entry("events", "")
        ));
        assert!(held(&r, "events").contains("brokers is required"), "{r:?}");
    }

    #[test]
    fn one_topic_never_a_pattern() {
        for topic in ["^events-.*", "a,b", "with space", ".", ".."] {
            let r = resolve(&format!(
                "{SECTION}{}",
                entry("events", "").replace("topic-events", topic)
            ));
            assert!(
                held(&r, "events").contains("not a Kafka topic name"),
                "{topic}: {r:?}"
            );
        }
    }

    #[test]
    fn avro_confluent_needs_an_http_schema_registry() {
        let no_sr = "[kafka]\nbrokers = \"b:9092\"\n";
        let r = resolve(&format!("{no_sr}{}", entry("events", "")));
        assert!(held(&r, "events").contains("schema_registry.url"), "{r:?}");
        let r = resolve(&format!(
            "{no_sr}{}",
            entry("events", "value_format = \"raw\"")
        ));
        let k = kafka(&r, "events");
        assert_eq!(k.value_format, ValueFormat::Raw);
        assert!(k.schema_registry.is_none());
        for url in ["ftp://sr:21", "sr.internal:8081", "http://"] {
            let r = resolve(&format!(
                "{no_sr}{}",
                entry(
                    "events",
                    &format!("[kafka.pipeline.schema_registry]\nurl = {url:?}")
                )
            ));
            assert!(
                held(&r, "events").contains("not an http(s) URL"),
                "{url}: {r:?}"
            );
        }
    }

    #[test]
    fn forced_key_rejected() {
        for (key, _) in FORCED_KEYS {
            for (section, pipeline) in [
                (
                    format!("[kafka.settings]\n{key:?} = \"x\"\n"),
                    String::new(),
                ),
                (
                    String::new(),
                    format!("[kafka.pipeline.settings]\n{key:?} = \"x\""),
                ),
                (
                    format!("[kafka.settings_env]\n{key:?} = \"X\"\n"),
                    String::new(),
                ),
                (
                    String::new(),
                    format!("[kafka.pipeline.settings_env]\n{key:?} = \"X\""),
                ),
            ] {
                let reason = refused(&section, &pipeline);
                assert!(
                    reason.contains(&format!("{key:?} is ddi's to set")),
                    "{key}: {reason}"
                );
            }
        }
        let reason = refused("[kafka.settings]\n\"group.id\" = \"kdi\"\n", "");
        assert!(
            reason.contains("group_id"),
            "points at the key that owns it: {reason}"
        );
        let reason = refused("[kafka.settings]\n\"bootstrap.servers\" = \"b\"\n", "");
        assert!(reason.contains("brokers"), "{reason}");
    }

    #[test]
    fn cooperative_rejected_at_load() {
        for strategy in [
            "cooperative-sticky",
            "range,cooperative-sticky",
            "sticky",
            "",
        ] {
            let reason = refused(
                "",
                &format!(
                    "[kafka.pipeline.settings]\n\"partition.assignment.strategy\" = {strategy:?}"
                ),
            );
            assert!(reason.contains("eagerly"), "{strategy}: {reason}");
        }
        for strategy in [
            "range",
            "roundrobin",
            "range, roundrobin",
            "roundrobin,range",
        ] {
            let r = resolve(&format!(
                "{SECTION}{}",
                entry(
                    "events",
                    &format!("[kafka.pipeline.settings]\n\"partition.assignment.strategy\" = {strategy:?}")
                )
            ));
            assert_eq!(running(&r), ["events"], "{strategy}: {r:?}");
        }
        // KIP-848's protocol is incremental too, and the protocol is ddi's to set.
        let reason = refused("[kafka.settings]\n\"group.protocol\" = \"consumer\"\n", "");
        assert!(reason.contains("classic"), "{reason}");

        // A value that only arrives at open is judged then.
        let r = resolve(&format!(
            "{SECTION}{}",
            entry(
                "events",
                "[kafka.pipeline.settings_env]\n\"partition.assignment.strategy\" = \"STRATEGY\""
            )
        ));
        let e = settings_of(&r, "events", &[("STRATEGY", "cooperative-sticky")])
            .unwrap_err()
            .to_string();
        assert!(e.contains("eagerly"), "{e}");
    }

    #[test]
    fn gssapi_rejected() {
        for line in [
            "\"sasl.mechanism\" = \"GSSAPI\"",
            "\"sasl.mechanisms\" = \"gssapi\"",
            "\"security.protocol\" = \"SASL_SSL\"",
            "\"security.protocol\" = \"sasl_plaintext\"",
        ] {
            let reason = refused("", &format!("[kafka.pipeline.settings]\n{line}"));
            assert!(reason.contains("GSSAPI"), "{line}: {reason}");
        }
        // A mechanism from the environment is a mechanism; it is judged when it is read.
        let r = resolve(&format!(
            "{SECTION}{}",
            entry(
                "events",
                "[kafka.pipeline.settings]\n\"security.protocol\" = \"SASL_SSL\"\n\
                 [kafka.pipeline.settings_env]\n\"sasl.mechanism\" = \"MECH\""
            )
        ));
        assert!(settings_of(&r, "events", &[("MECH", "SCRAM-SHA-512")]).is_ok());
        let e = settings_of(&r, "events", &[("MECH", "GSSAPI")])
            .unwrap_err()
            .to_string();
        assert!(e.contains("GSSAPI"), "{e}");
    }

    #[test]
    fn environment_variables_are_read_at_open_only() {
        // Nothing is read at load, so a config validates where its secrets are not set.
        let r = resolve(&format!(
            "{SECTION}{}",
            entry(
                "events",
                "scram_json_env = \"MY_SCRAM\"\n\
                 [kafka.pipeline.settings_env]\n\"sasl.password\" = \"MY_PASSWORD\""
            )
        ));
        assert_eq!(running(&r), ["events"]);
        let e = settings_of(
            &r,
            "events",
            &[("MY_SCRAM", r#"{"username":"u","password":"p"}"#)],
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("\"MY_PASSWORD\", which is not set"), "{e}");
        let e = settings_of(&r, "events", &[("MY_PASSWORD", "p")])
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("\"MY_SCRAM\", which is not set"),
            "an explicit name is required: {e}"
        );

        // kdi's names are optional: unset, they do nothing.
        let r = resolve(&format!("{SECTION}{}", entry("events", "")));
        let s = settings_of(&r, "events", &[]).unwrap();
        assert!(s.get("sasl.username").is_none() && s.get("ssl.ca.pem").is_none());
    }

    #[test]
    fn scram_json_env_compat() {
        let r = resolve(&format!("{SECTION}{}", entry("events", "")));
        let s = settings_of(
            &r,
            "events",
            &[
                (
                    DEFAULT_SCRAM_JSON_ENV,
                    r#"{"username": "svc-ingest", "password": "pa$$"}"#,
                ),
                (DEFAULT_SSL_CERT_ENV, "-----BEGIN CERTIFICATE-----"),
                (DEFAULT_SSL_KEY_ENV, "-----BEGIN PRIVATE KEY-----"),
                (DEFAULT_SSL_CA_ENV, "-----BEGIN CERTIFICATE----- ca"),
            ],
        )
        .unwrap();
        assert_eq!(s.get("sasl.username"), Some("svc-ingest"));
        assert_eq!(s.get("sasl.password"), Some("pa$$"));
        assert_eq!(
            s.get("ssl.certificate.pem"),
            Some("-----BEGIN CERTIFICATE-----")
        );
        assert_eq!(s.get("ssl.key.pem"), Some("-----BEGIN PRIVATE KEY-----"));
        assert_eq!(s.get("ssl.ca.pem"), Some("-----BEGIN CERTIFICATE----- ca"));

        // kdi panicked here (lib.rs:1310); this is an error that names the variable, never
        // what it holds.
        for bad in [
            "not json",
            r#"{"username": "u"}"#,
            r#"{"username": "u", "password": 7}"#,
            r#"["u", "p"]"#,
        ] {
            let e = settings_of(&r, "events", &[(DEFAULT_SCRAM_JSON_ENV, bad)])
                .unwrap_err()
                .to_string();
            assert!(e.contains(DEFAULT_SCRAM_JSON_ENV), "{e}");
            assert!(!e.contains(bad), "the value must not reach the error: {e}");
        }

        // `""` turns one off, and a renamed one is read instead.
        let r = resolve(&format!(
            "{SECTION}{}",
            entry("events", "scram_json_env = \"\"\nssl_ca_env = \"MY_CA\"")
        ));
        let s = settings_of(
            &r,
            "events",
            &[
                (DEFAULT_SCRAM_JSON_ENV, r#"{"username":"u","password":"p"}"#),
                (DEFAULT_SSL_CA_ENV, "kdi-ca"),
                ("MY_CA", "my-ca"),
            ],
        )
        .unwrap();
        assert!(s.get("sasl.username").is_none(), "{s:?}");
        assert_eq!(s.get("ssl.ca.pem"), Some("my-ca"));
    }

    #[test]
    fn secret_redaction_in_debug() {
        let toml = format!(
            "{SECTION}[kafka.settings]\n\"sasl.password\" = \"hunter2-inline\"\n\
             \"ssl.key.pem\" = \"-----BEGIN PRIVATE KEY----- inline\"\n\
             \"fetch.min.bytes\" = \"4096\"\n{}",
            entry("events", "")
        );
        let cfg = config(&toml);
        let (_, warnings) = cfg.resolve_kafka(&cfg.kafka.pipelines[0]).unwrap();
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("sasl.password is written inline")),
            "{warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("ssl.key.pem")),
            "{warnings:?}"
        );

        let r = cfg.resolve_all_with(true).unwrap();
        let s = settings_of(
            &r,
            "events",
            &[(
                DEFAULT_SCRAM_JSON_ENV,
                r#"{"username": "svc", "password": "s3cret-from-env"}"#,
            )],
        )
        .unwrap();
        for printed in [
            format!("{cfg:?}"),
            format!("{:?}", r.pipelines[0]),
            format!("{r:?}"),
            format!("{s:?}"),
        ] {
            for secret in [
                "hunter2-inline",
                "BEGIN PRIVATE KEY",
                "s3cret-from-env",
                "svc",
            ] {
                assert!(!printed.contains(secret), "{secret} leaked: {printed}");
            }
            assert!(printed.contains("<redacted>"), "{printed}");
        }
        assert!(
            format!("{cfg:?}").contains("4096"),
            "plain values stay readable"
        );

        for key in [
            "sasl.password",
            "ssl.key.password",
            "ssl.key.pem",
            "ssl.ca.pem",
            "ssl.key.location",
            "sasl.oauthbearer.client.secret",
            "sasl.oauthbearer.config",
            "sasl.username",
        ] {
            assert!(is_secret_key(key), "{key}");
        }
        for key in [
            "fetch.min.bytes",
            "security.protocol",
            "client.id",
            "group.id",
        ] {
            assert!(!is_secret_key(key), "{key}");
        }
    }

    #[test]
    fn numbers_and_shapes_are_bounded() {
        for (line, says) in [
            ("allowed_latency_secs = 0", "allowed_latency_secs"),
            ("max_messages_per_batch = 0", "max_messages_per_batch"),
            ("max_bytes_per_batch = \"512KB\"", "under 1MB"),
            ("max_bytes_per_batch = \"lots\"", "max_bytes_per_batch"),
            ("revoke_flush_timeout_secs = 4", "5..=300"),
            ("revoke_flush_timeout_secs = 301", "5..=300"),
            ("blocked_recheck_secs = 0", "blocked_recheck_secs"),
            ("metadata_prefix = \"Kafka_\"", "[a-z_][a-z0-9_]*"),
            ("metadata_prefix = \"1x\"", "[a-z_][a-z0-9_]*"),
            ("metadata_prefix = \"\"", "[a-z_][a-z0-9_]*"),
            ("metadata_prefix = \"k-\"", "[a-z_][a-z0-9_]*"),
        ] {
            let reason = refused("", line);
            assert!(reason.contains(says), "{line}: {reason}");
        }
        for line in [
            "revoke_flush_timeout_secs = 5",
            "revoke_flush_timeout_secs = 300",
            "max_bytes_per_batch = \"1MB\"",
            "metadata_prefix = \"_k0_\"",
        ] {
            let r = resolve(&format!("{SECTION}{}", entry("events", line)));
            assert_eq!(running(&r), ["events"], "{line}: {r:?}");
        }
        // A section's value is judged on every pipeline that takes it.
        let r = resolve(&format!(
            "{}{}",
            SECTION.replace("[kafka]\n", "[kafka]\nallowed_latency_secs = 0\n"),
            entry("events", "")
        ));
        assert!(held(&r, "events").contains("allowed_latency_secs"), "{r:?}");
    }

    #[test]
    fn transform_sql_is_validated_and_normalised() {
        let reason = refused("", "transform_sql = \"DELETE FROM source\"");
        assert!(!reason.is_empty());
        let r = resolve(&format!(
            "{SECTION}{}",
            entry(
                "events",
                "transform_sql = \"select *, 1 as one from source\""
            )
        ));
        let sql = r.pipelines[0].transform_sql.as_deref().unwrap();
        assert!(
            sql.contains("FROM source"),
            "normalised like [[pipeline]]'s: {sql}"
        );
        // No lookups: a Kafka pipeline has no Delta snapshot to pin one to.
        let reason = refused(
            "",
            "transform_sql = \"SELECT s.* FROM source s JOIN rates r ON s.c = r.c\"",
        );
        assert!(!reason.is_empty());
    }

    #[test]
    fn delta_only_keys_are_unknown_fields() {
        for line in [
            "write_mode = \"upsert\"",
            "dedup_timestamp = \"_timestamp\"",
            "upsert_key = \"id\"",
            "source_uri = \"/tmp/x\"",
            "starting_version = 3",
            "lookups = []",
            "change_policy = \"fail\"",
        ] {
            let key = line.split(' ').next().unwrap();
            let e = Config::from_toml_str(&format!("{SECTION}{}", entry("events", line)))
                .unwrap_err()
                .to_string();
            assert!(e.contains(key), "{line}: {e}");
        }
        let e = Config::from_toml_str("[kafka]\nbroker = \"b\"\n")
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("broker"),
            "a misspelt section key is an error too: {e}"
        );
    }

    #[test]
    fn the_budget_reserves_for_each_running_kafka_pipeline() {
        use crate::budget::{Budget, KAFKA_RESERVE_BYTES};
        const GIB: u64 = 1024 * 1024 * 1024;
        let cfg = config(&format!(
            "[runtime]\nmax_memory = \"1GiB\"\n{DELTA}{SECTION}{}{}",
            entry("a", ""),
            entry("b", "")
        ));
        let r = cfg.resolve_all_with(true).unwrap();
        assert_eq!(running(&r), ["orders", "a", "b"]);
        assert_eq!(
            cfg.budget(&r.pipelines).unwrap().per_pipeline(),
            Budget::resolve_reserving(Some(GIB), 3, 2 * KAFKA_RESERVE_BYTES)
                .unwrap()
                .per_pipeline()
        );
        // Delta pipelines alone get the budget they always did.
        assert_eq!(
            cfg.budget(&r.pipelines[..1]).unwrap().per_pipeline(),
            Budget::resolve(Some(GIB), 1).per_pipeline()
        );
    }

    #[test]
    fn read_uncommitted_warns() {
        let cfg = config(&format!(
            "{SECTION}{}",
            entry(
                "events",
                "[kafka.pipeline.settings]\n\"isolation.level\" = \"read_uncommitted\""
            )
        ));
        let (_, warnings) = cfg.resolve_kafka(&cfg.kafka.pipelines[0]).unwrap();
        assert!(
            warnings.iter().any(|w| w.contains("read_uncommitted")),
            "{warnings:?}"
        );
        assert_eq!(running(&cfg.resolve_all_with(true).unwrap()), ["events"]);
    }

    // ------------------------------------------------- one setting under librdkafka's names

    #[test]
    fn a_forced_key_under_its_topic_name_is_refused() {
        // librdkafka sets a name it does not know globally on the default topic config, with
        // one leading `topic.` stripped (rdkafka_conf.c:2544-2580): these three are
        // auto.offset.reset and the topic-level auto.commit.enable, both ddi's.
        for key in [
            "topic.auto.offset.reset",
            "topic.enable.auto.commit",
            "topic.auto.commit.enable",
        ] {
            for (section, pipeline) in [
                (
                    format!("[kafka.settings]\n{key:?} = \"earliest\"\n"),
                    String::new(),
                ),
                (
                    String::new(),
                    format!("[kafka.pipeline.settings]\n{key:?} = \"earliest\""),
                ),
                (
                    format!("[kafka.settings_env]\n{key:?} = \"X\"\n"),
                    String::new(),
                ),
                (
                    String::new(),
                    format!("[kafka.pipeline.settings_env]\n{key:?} = \"X\""),
                ),
            ] {
                let reason = refused(&section, &pipeline);
                assert!(
                    reason.contains(&format!("{key:?}")) && reason.contains("ddi's to set"),
                    "{key}: {reason}"
                );
            }
        }
        let reason = refused(
            "[kafka.settings]\n\"topic.auto.offset.reset\" = \"earliest\"\n",
            "",
        );
        assert!(
            reason.contains("\"auto.offset.reset\""),
            "names the setting it is: {reason}"
        );
        // A global setting whose own name starts with `topic.` is found before any stripping.
        let r = resolve(&format!(
            "{SECTION}{}",
            entry(
                "events",
                "[kafka.pipeline.settings]\n\"topic.metadata.refresh.interval.ms\" = \"60000\""
            )
        ));
        assert_eq!(running(&r), ["events"], "{r:?}");
    }

    #[test]
    fn a_later_layer_replaces_an_earlier_layers_spelling() {
        let sasl = "\"security.protocol\" = \"SASL_SSL\"\n";
        let layered = |section: &str, pipeline: &str| {
            resolve(&format!(
                "{SECTION}[kafka.settings]\n{sasl}{section}\n{}",
                entry("events", &format!("[kafka.pipeline.settings]\n{pipeline}"))
            ))
        };
        // `sasl.mechanism` is librdkafka's alias of `sasl.mechanisms` (rdkafka_conf.c:992): the
        // pipeline's own key wins under whichever name either layer used.
        let r = layered(
            "\"sasl.mechanisms\" = \"SCRAM-SHA-256\"",
            "\"sasl.mechanism\" = \"SCRAM-SHA-512\"",
        );
        let k = kafka(&r, "events");
        assert_eq!(k.settings.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(k.settings.get("sasl.mechanisms"), None, "{:?}", k.settings);
        let s = settings_of(&r, "events", &[]).unwrap();
        assert_eq!(
            (s.get("sasl.mechanism"), s.get("sasl.mechanisms")),
            (Some("SCRAM-SHA-512"), None)
        );

        // So GSSAPI is judged where it wins, under either name.
        let r = layered(
            "\"sasl.mechanism\" = \"GSSAPI\"",
            "\"sasl.mechanisms\" = \"SCRAM-SHA-512\"",
        );
        let s = settings_of(&r, "events", &[]).unwrap();
        assert_eq!(
            (s.get("sasl.mechanism"), s.get("sasl.mechanisms")),
            (None, Some("SCRAM-SHA-512"))
        );
        let r = layered(
            "\"sasl.mechanisms\" = \"SCRAM-SHA-512\"",
            "\"sasl.mechanism\" = \"GSSAPI\"",
        );
        assert!(held(&r, "events").contains("GSSAPI"), "{r:?}");

        // settings_env, the later layer, replaces a setting's other spelling at open.
        let r = resolve(&format!(
            "{SECTION}{}",
            entry(
                "events",
                &format!(
                    "[kafka.pipeline.settings]\n{sasl}\"sasl.mechanism\" = \"SCRAM-SHA-512\"\n\
                     [kafka.pipeline.settings_env]\n\"sasl.mechanisms\" = \"MECH\""
                )
            )
        ));
        let s = settings_of(&r, "events", &[("MECH", "SCRAM-SHA-256")]).unwrap();
        assert_eq!(
            (s.get("sasl.mechanism"), s.get("sasl.mechanisms")),
            (None, Some("SCRAM-SHA-256"))
        );

        // Two names for one setting in one table: nothing says which one is meant.
        let reason = refused(
            "",
            &format!(
                "[kafka.pipeline.settings]\n{sasl}\"sasl.mechanism\" = \"SCRAM-SHA-512\"\n\
                 \"sasl.mechanisms\" = \"SCRAM-SHA-256\""
            ),
        );
        assert!(
            reason.contains("\"sasl.mechanism\"") && reason.contains("\"sasl.mechanisms\""),
            "{reason}"
        );
    }

    #[test]
    fn an_inline_group_instance_id_is_refused() {
        // Every member reads one file, so a static id written in it is every member's, and
        // members of one group with one id fence each other out in turn.
        for (section, pipeline) in [
            ("[kafka.settings]\n\"group.instance.id\" = \"ddi-1\"\n", ""),
            (
                "",
                "[kafka.pipeline.settings]\n\"group.instance.id\" = \"ddi-1\"",
            ),
        ] {
            let reason = refused(section, pipeline);
            assert!(
                reason.contains("group.instance.id") && reason.contains("settings_env"),
                "{reason}"
            );
        }
        // From the environment, each member sets its own.
        let r = resolve(&format!(
            "{SECTION}{}",
            entry(
                "events",
                "[kafka.pipeline.settings_env]\n\"group.instance.id\" = \"POD_NAME\""
            )
        ));
        let a = settings_of(&r, "events", &[("POD_NAME", "ddi-0")]).unwrap();
        let b = settings_of(&r, "events", &[("POD_NAME", "ddi-1")]).unwrap();
        assert_eq!(
            (a.get("group.instance.id"), b.get("group.instance.id")),
            (Some("ddi-0"), Some("ddi-1"))
        );
    }

    #[test]
    fn settings_take_numbers_and_booleans() {
        // librdkafka takes every value as text; TOML's numbers and booleans are written as it.
        let r = resolve(&format!(
            "{SECTION}[kafka.settings]\n\"fetch.min.bytes\" = 1\n\
             \"enable.ssl.certificate.verification\" = false\n{}",
            entry(
                "events",
                "[kafka.pipeline.settings]\n\"statistics.interval.ms\" = 0"
            )
        ));
        let s = settings_of(&r, "events", &[]).unwrap();
        assert_eq!(s.get("fetch.min.bytes"), Some("1"));
        assert_eq!(s.get("enable.ssl.certificate.verification"), Some("false"));
        assert_eq!(s.get("statistics.interval.ms"), Some("0"));
        // A forced key is ddi's, however its value is written.
        for line in [
            "\"enable.partition.eof\" = false",
            "\"enable.auto.commit\" = false",
        ] {
            let reason = refused("", &format!("[kafka.pipeline.settings]\n{line}"));
            assert!(reason.contains("is ddi's to set"), "{line}: {reason}");
        }
        // Anything else is no librdkafka value, and fails the file with its line.
        let e = Config::from_toml_str(&format!(
            "{SECTION}[kafka.settings]\n\"fetch.min.bytes\" = [1]\n"
        ))
        .unwrap_err()
        .to_string();
        assert!(e.contains("a string, number or boolean"), "{e}");
    }

    /// [`librdkafka_property`] against librdkafka itself: a value set under one name is what
    /// librdkafka reads under the other, every name in the tables is one it knows, and every
    /// forced key spelt with `topic.` is refused here exactly when librdkafka would take it.
    #[cfg(feature = "kafka")]
    #[test]
    fn librdkafka_reads_names_as_ddi_does() {
        use rdkafka::config::{ClientConfig, NativeClientConfig};
        let native = |pairs: &[(&str, &str)]| -> rdkafka::error::KafkaResult<NativeClientConfig> {
            let mut c = ClientConfig::new();
            for (k, v) in pairs {
                c.set(*k, *v);
            }
            c.create_native_config()
        };
        // (written, read back under, value, what librdkafka reports)
        for (written, read, value, reads) in [
            (
                "topic.auto.offset.reset",
                "auto.offset.reset",
                "earliest",
                "smallest",
            ),
            (
                "auto.offset.reset",
                "auto.offset.reset",
                "latest",
                "largest",
            ),
            (
                "topic.enable.auto.commit",
                "auto.commit.enable",
                "false",
                "false",
            ),
            (
                "topic.auto.commit.enable",
                "auto.commit.enable",
                "false",
                "false",
            ),
            (
                "sasl.mechanism",
                "sasl.mechanisms",
                "SCRAM-SHA-512",
                "SCRAM-SHA-512",
            ),
            (
                "bootstrap.servers",
                "metadata.broker.list",
                "h:9092",
                "h:9092",
            ),
            (
                "max.partition.fetch.bytes",
                "fetch.message.max.bytes",
                "4242",
                "4242",
            ),
            (
                "topic.metadata.refresh.interval.ms",
                "topic.metadata.refresh.interval.ms",
                "60000",
                "60000",
            ),
        ] {
            assert_eq!(
                librdkafka_property(written),
                librdkafka_property(read),
                "{written}"
            );
            let got = native(&[(written, value)]).unwrap().get(read).unwrap();
            assert_eq!(got, reads, "{written} = {value:?}, read as {read}");
        }

        // The global enable.auto.commit is not the topic-level auto.commit.enable.
        assert_ne!(
            librdkafka_property("enable.auto.commit"),
            librdkafka_property("auto.commit.enable")
        );
        let n = native(&[
            ("enable.auto.commit", "false"),
            ("auto.offset.reset", "error"),
        ]);
        assert_eq!(n.unwrap().get("auto.commit.enable").unwrap(), "true");

        let unknown = |name: &str| match native(&[(name, "x")]) {
            Err(e) => e.to_string().contains("No such configuration property"),
            Ok(_) => false,
        };
        let topic_level = TOPIC_ONLY_NAMES
            .iter()
            .chain(TOPIC_ALIASES.iter().map(|(a, _)| a))
            .map(|n| format!("topic.{n}"));
        for name in GLOBAL_TOPIC_NAMES
            .iter()
            .chain(TOPIC_ONLY_NAMES)
            .chain(GLOBAL_ALIASES.iter().map(|(a, _)| a))
            .chain(TOPIC_ALIASES.iter().map(|(a, _)| a))
            .map(|n| n.to_string())
            .chain(topic_level)
        {
            assert!(!unknown(&name), "librdkafka does not know {name:?}");
        }
        for (key, _) in FORCED_KEYS {
            let spelt = format!("topic.{key}");
            assert_eq!(
                refuse_forced_key(&spelt, "settings").is_err(),
                !unknown(&spelt),
                "{spelt}"
            );
        }
    }
}
