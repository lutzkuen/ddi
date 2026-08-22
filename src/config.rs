//! Configuration: N pipelines in one process.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use serde::{Deserialize, Serialize};

use tracing::warn;

use crate::error::{Error, Result};
use crate::lookup::{LookupConfig, ResolvedLookup};
use crate::source::{ChangePolicy, Version};
use crate::transform::validate::{normalise_publish_sql, normalise_sql_with_lookups};

fn default_allowed_latency() -> u64 {
    30
}
fn default_max_bytes() -> String {
    "256MB".into()
}
fn default_target_file_size() -> String {
    "128MB".into()
}
fn default_max_files() -> usize {
    1_000
}
fn default_max_output_rows() -> usize {
    // Unnest amplification: a row with a 10k-element array becomes 10k rows, so batches
    // are bounded on estimated *output* rows, not just input bytes. Plan §3.
    5_000_000
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Defaults {
    #[serde(default = "default_allowed_latency")]
    pub allowed_latency_secs: u64,
    #[serde(default = "default_max_bytes")]
    pub max_bytes_per_batch: String,
    #[serde(default = "default_target_file_size")]
    pub target_file_size: String,
    #[serde(default = "default_max_files")]
    pub max_files_per_batch: usize,
    #[serde(default = "default_max_output_rows")]
    pub max_output_rows_per_batch: usize,

    /// How much memory the whole process may use, divided across the pipelines in it.
    ///
    /// Unset means "whatever the container says", and if the container says nothing either
    /// then nothing is bounded — which is the behaviour there has always been, and the
    /// right one for a local run. Set it to take a tighter bound than the cgroup's, or to
    /// get one at all outside a container.
    ///
    /// It is a *process* number rather than a per-pipeline one on purpose: pipelines all
    /// start at once, and it is that simultaneity which turns a survivable allocation into
    /// an OOM. See [`crate::budget`].
    #[serde(default)]
    pub max_memory: Option<String>,

    /// How many merges may run at once across the whole process.
    ///
    /// A merge reads back a slice of the target it writes, so its cost is set by the target
    /// rather than by the batch — which means `max_bytes_per_batch` and `max_memory` do not
    /// bound it, and neither does dividing them further. Hundreds of pipelines starting
    /// together scan hundreds of targets together, and that simultaneity is what turns a
    /// survivable merge into an OOM. See [`crate::gate`].
    ///
    /// Unset means unbounded, which is the behaviour there has always been and the right one
    /// for a local run. Watch `ddi_merge_queue_seconds_total` after setting it: a rate that
    /// climbs towards 1 per merge slot means the limit, not the storage, is the throughput.
    #[serde(default)]
    pub max_concurrent_upsert_merges: Option<usize>,

    /// How many startup uniqueness checks may run at once.
    ///
    /// Separate from the merge limit because the two overlap in time but not in kind: every
    /// upsert pipeline preflights once, at startup, all at the same moment, and each one
    /// reads its whole target. One limit covering both would have to be set for that burst
    /// and would then throttle steady state for the rest of the run.
    #[serde(default)]
    pub max_concurrent_upsert_preflights: Option<usize>,

    /// Where DataFusion writes what it cannot hold in memory.
    ///
    /// Unset means the OS temporary directory — `$TMPDIR`, else `/tmp`. Inside a container
    /// that is the writable layer, and Kubernetes charges every byte of it to the pod's
    /// `ephemeral-storage`. So the way a spill kills this process is not an error: it is an
    /// eviction, with no log line, taking every other pipeline with it. That is the one
    /// failure this tool cannot contain from the inside. Point this at a volume and the
    /// failure becomes a query that stops.
    ///
    /// It is a *process* setting, like [`Self::max_memory`], and more strictly so: every
    /// session this tool builds shares one disk manager, so one directory and one budget
    /// cover the fleet. The directory is created and written to once at startup, so a volume
    /// that was not mounted is a startup failure rather than a first-sort failure an hour
    /// later. See [`crate::spill`].
    #[serde(default)]
    pub temp_directory: Option<String>,

    /// How many bytes of spill this whole process may hold on disk at once.
    ///
    /// A *process* number, and for a stronger reason than `max_memory`'s: DataFusion counts
    /// spill per `DiskManager`, and this process builds a `RuntimeEnv` — and so, without
    /// help, a `DiskManager` — per operation rather than per pipeline. Divided N ways it
    /// would bound nothing, because nobody can say what N is. Shared, it is the one number
    /// that can be compared against the volume behind [`Self::temp_directory`].
    ///
    /// Unset means DataFusion's own 100GB default — which is larger than most pods'
    /// ephemeral-storage limit, so unset is not "unbounded", it is "bounded above the point
    /// at which the pod is killed". Set it *below* the volume's real size and leave headroom:
    /// the budget is checked after each write, not before, so it can be overshot by about one
    /// buffer per open spill file.
    ///
    /// Watch `ddi_spill_bytes` against `ddi_spill_limit_bytes`; a ratio sitting near one means
    /// the next merge fails. `ddi_capacity_exhausted` is which pipeline it failed for.
    #[serde(default)]
    pub max_temp_directory_size: Option<String>,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            allowed_latency_secs: default_allowed_latency(),
            max_bytes_per_batch: default_max_bytes(),
            target_file_size: default_target_file_size(),
            max_files_per_batch: default_max_files(),
            max_output_rows_per_batch: default_max_output_rows(),
            max_memory: None,
            max_concurrent_upsert_merges: None,
            max_concurrent_upsert_preflights: None,
            temp_directory: None,
            max_temp_directory_size: None,
        }
    }
}

/// How a batch reaches the target.
///
/// Mirrors [`ChangePolicy`]'s shape: a small snake_case enum with the safe option as the
/// default, so a typo cannot silently pick the riskier one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Every row is a new row. The original mode, and still the default: the target keeps
    /// its whole history and this tool never reads it back.
    #[default]
    Append,
    /// A row replaces the one already stored under its key, when it is newer. The target
    /// holds one row per key, at the cost of reading part of it on every batch. See
    /// [`crate::upsert`].
    Upsert,
    /// The same end state as [`Self::Upsert`], reached by merging many source commits at
    /// once instead of each one on its own.
    ///
    /// One configured pipeline in this mode becomes two running ones — see [`crate::stage`]
    /// — so this variant never survives [`Config::resolve_all`]. Anything asking a
    /// *resolved* pipeline for its mode is therefore asking about one of the two halves,
    /// and gets `Append` or `Upsert`, which is what keeps the runtime unaware that staging
    /// exists at all.
    StagedUpsert,
}

impl WriteMode {
    /// True when this pipeline merges into its target rather than appending.
    ///
    /// Deliberately false for [`Self::StagedUpsert`]: a staged pipeline writes nothing
    /// itself, and the half of it that merges says so on its own.
    pub fn is_upsert(self) -> bool {
        matches!(self, WriteMode::Upsert)
    }

    pub fn is_staged(self) -> bool {
        matches!(self, WriteMode::StagedUpsert)
    }

    /// True when the target ends up holding one row per key, however it gets there. The
    /// question config validation asks, because both modes need a key and a timestamp.
    pub fn keeps_one_row_per_key(self) -> bool {
        matches!(self, WriteMode::Upsert | WriteMode::StagedUpsert)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub name: String,

    /// The offset key. MUST be unique and stable across restarts — it is what the target
    /// table's `txn` action is looked up by. Changing it replays from `starting_version`.
    pub app_id: String,

    pub source_uri: String,
    pub target_uri: String,

    /// Small Delta tables joined as pinned snapshots while processing a source batch.
    #[serde(default)]
    pub lookups: Vec<LookupConfig>,

    #[serde(default)]
    pub starting_version: Version,

    #[serde(default)]
    pub change_policy: ChangePolicy,

    /// A single SELECT over `source`. Omit for a straight copy.
    #[serde(default)]
    pub transform_sql: Option<String>,

    #[serde(default)]
    pub allowed_latency_secs: Option<u64>,
    #[serde(default)]
    pub max_bytes_per_batch: Option<String>,
    #[serde(default)]
    pub max_files_per_batch: Option<usize>,
    #[serde(default)]
    pub max_output_rows_per_batch: Option<usize>,
    #[serde(default)]
    pub target_file_size: Option<String>,

    /// Delta table where dbt records the source version it last rebuilt this target from.
    ///
    /// Set this whenever dbt also writes `target_uri`. Without it, a dbt overwrite
    /// silently strands every row this pipeline streamed after dbt began its read — see
    /// [`crate::dbt::watermark`]. Defaults to `[dbt].watermark_uri`.
    #[serde(default)]
    pub watermark_uri: Option<String>,

    /// Timestamp column used to skip rows a rebuild already covered.
    ///
    /// The zero-cooperation alternative to `watermark_uri`: instead of the batch telling
    /// us where it got to, we read `max(dedup_timestamp)` out of the target and emit only
    /// rows beyond it. The batch needs to know nothing about this tool.
    ///
    /// Must be non-decreasing in the order rows arrive in the source. See
    /// [`crate::dedup`].
    #[serde(default)]
    pub dedup_timestamp: Option<String>,

    /// Row identity, used to resolve rows sharing exactly the watermark instant.
    ///
    /// Optional but strongly recommended: without it, a row that arrived in the same
    /// instant as the rebuild's newest is assumed covered and dropped.
    #[serde(default)]
    pub dedup_key: Option<String>,

    /// Append every row, or merge it onto the key it already has. See [`crate::upsert`].
    #[serde(default)]
    pub write_mode: WriteMode,

    /// Row identity for `write_mode = "upsert"`. Defaults to `dedup_key`.
    #[serde(default)]
    pub upsert_key: Option<String>,

    /// Where rows that will not fit the target go, instead of stopping the pipeline.
    ///
    /// Defaults to `<target_uri>__ddi_dq`, so a fleet of pipelines needs no per-pipeline
    /// setting. The table is never created here; where it does not exist, bad rows keep
    /// failing the pipeline (which now retries rather than giving up). See [`crate::dq`].
    #[serde(default)]
    pub dq_uri: Option<String>,

    /// The furthest back the merge window is allowed to reach — `"48h"`, `"90m"`, or a
    /// bare number for a numeric sequence column.
    ///
    /// A cost ceiling, not a correctness knob. Left unset, the window opens exactly as far
    /// as the target's own file statistics say it must, which is always right and
    /// occasionally means reading the whole table. Set, it stops there and says so.
    #[serde(default)]
    pub upsert_lookback: Option<String>,

    /// Columns that decide which row wins when two share a `dedup_timestamp`.
    ///
    /// Compared left to right after the timestamp, so `["kafka_partition", "kafka_offset"]`
    /// reads as "later offset within the same partition wins". Every column must be in the
    /// target, because the comparison is made against the row already stored as well as
    /// between rows in hand.
    ///
    /// Without this, a tie is broken by position in the batch — which is only stable while
    /// batch boundaries are, and they are not: a retry regroups commits, and
    /// `write_mode = "staged_upsert"` regroups them by design. See [`crate::upsert`].
    #[serde(default)]
    pub upsert_tiebreak: Vec<String>,

    /// Where `write_mode = "staged_upsert"` parks rows between its two halves.
    ///
    /// Defaults to `<target_uri>__ddi_stage`, which is what almost every pipeline should
    /// use: it puts the stage beside the table it feeds, so a target that relocates takes
    /// its stage along. Set it only when the stage has to live somewhere else.
    #[serde(default)]
    pub stage_uri: Option<String>,

    /// How much staged data the apply half accumulates before merging.
    ///
    /// This is the knob the whole mode exists for: one merge per this many bytes instead of
    /// one merge per source commit, paid for in how stale the target may be. Defaults to
    /// `max_bytes_per_batch` — the conservative reading, "accumulate no more per merge than
    /// a direct upsert would have handled", and almost certainly too small to be worth
    /// staging for.
    #[serde(default)]
    pub apply_max_bytes: Option<String>,

    /// How long the apply half waits before merging what it has, however little that is.
    ///
    /// The ceiling on how stale the target may be, and therefore the number to publish to
    /// whoever reads it. Defaults to `allowed_latency_secs`.
    #[serde(default)]
    pub apply_max_latency_secs: Option<u64>,

    /// The real target this pipeline's stage stands in for, set only on the ingest half of a
    /// split `staged_upsert` and never written by hand.
    ///
    /// It carries two things the ingest half cannot otherwise know. The stage's schema is
    /// this table's schema, so it says what to create; and the full-row rule is stated
    /// against this table, so it says what the transform must produce. Both follow from the
    /// same fact — that the stage is a stand-in for a table it never writes to.
    #[serde(skip)]
    pub stage_for: Option<String>,

    /// Fully qualified catalog names, so a location can be re-resolved while running.
    /// Filled in from the dbt manifest.
    #[serde(default)]
    pub source_relation: Option<String>,
    #[serde(default)]
    pub target_relation: Option<String>,

    /// The live payload this pipeline's commits carry, when a dbt model declared one.
    ///
    /// Last by preference rather than by constraint. `toml 0.8` reorders a table's scalars
    /// ahead of its sub-tables when serialising, so field order here is free — `lookups` is a
    /// `Vec<LookupConfig>` sitting at field four with two dozen scalars after it, and round
    /// trips. It reads best last, and `a_pinned_publication_round_trips_through_toml` is what
    /// actually holds the convert path to working.
    #[serde(default)]
    pub publish: Option<PublishModel>,
}

/// Where the lake is and how to reach it. Deployment, not semantics.
///
/// Nothing here changes what a pipeline computes — that is the dbt project's job. These
/// are the things a dbt project cannot know: credentials, and where the tables live when
/// dbt has not said so itself.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Fallback for turning `schema.table` into a URI, e.g.
    /// `"abfss://lake@acct.dfs.core.windows.net/{schema}/{name}"`.
    ///
    /// Only consulted when the dbt project does not declare a location itself
    /// (`location_root`, `delta_path`, a source's `delta_table_path`, `meta.ddi_location`).
    #[serde(default)]
    pub uri_template: Option<String>,

    /// Optional table where a cooperating batch job records the source version it
    /// consumed. See [`crate::dbt::watermark`]; `meta.ddi_timestamp` needs no such thing.
    #[serde(default)]
    pub watermark_uri: Option<String>,

    /// Object-store credentials, passed through to delta-rs verbatim.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// A pipeline with defaults folded in and every value parsed.
#[derive(Clone, Debug)]
pub struct ResolvedPipeline {
    pub name: String,
    pub app_id: String,
    pub source_uri: String,
    pub target_uri: String,
    /// Pinned Delta lookup relations available to `transform_sql`.
    pub lookups: Vec<ResolvedLookup>,
    pub starting_version: Version,
    pub change_policy: ChangePolicy,
    pub transform_sql: Option<String>,
    pub allowed_latency_secs: u64,
    pub max_bytes_per_batch: u64,
    pub max_files_per_batch: usize,
    pub max_output_rows_per_batch: usize,
    pub target_file_size: u64,
    /// Where dbt records its rebuild watermark for this target, if dbt shares it.
    pub watermark_uri: Option<String>,
    /// Timestamp column used to skip rows a rebuild already covered.
    pub dedup_timestamp: Option<String>,
    /// Row identity, for resolving ties at the watermark instant.
    pub dedup_key: Option<String>,
    /// Append, or merge onto the key already stored.
    pub write_mode: WriteMode,
    /// Row identity for the merge. Always `Some` once `write_mode` is `Upsert` — resolve
    /// rejects the combination otherwise.
    pub upsert_key: Option<String>,
    /// How far back the merge window may reach. `None` means "as far as the target's
    /// statistics require".
    pub upsert_lookback: Option<crate::upsert::Lookback>,
    /// Columns compared after `dedup_timestamp` to settle a tie, left to right. Empty means
    /// position in the batch decides, which is stable only while batches are.
    pub upsert_tiebreak: Vec<String>,
    /// Set only on the ingest half of a staged upsert: the real target its stage feeds.
    ///
    /// Two things follow from it, and both are why the field exists rather than a pair of
    /// booleans. The stage is created with this table's schema if it is not there; and the
    /// transform is held to producing *every* column of it, because a staged row is written
    /// once and merged later, by which time "the transform said nothing about this column"
    /// is indistinguishable from "this column is null". See [`crate::stage`].
    pub stage_for: Option<String>,
    /// An explicit data-quality table, when the derived one will not do. Kept unresolved
    /// so that a target which moves takes its rejects with it — see [`Self::dq_uri`].
    pub dq_uri: Option<String>,
    /// How to reach object storage. The one thing a dbt project cannot tell us.
    pub storage: crate::storage::Storage,
    /// Fully qualified catalog name of the source, when there is a catalog to ask.
    pub source_relation: Option<String>,
    /// Fully qualified catalog name of the target.
    pub target_relation: Option<String>,
    /// What a dbt model asked to publish after each commit of this pipeline.
    ///
    /// `Some` only when a model asked, this pipeline is append-only, and the deployment
    /// configured somewhere to send it. Any of those missing leaves it `None` and the
    /// pipeline streams exactly as it would have.
    pub publish: Option<PublishModel>,
    /// Where that payload goes. Paired with `publish`: both or neither.
    pub publish_to: Option<PublisherConfig>,
}

impl ResolvedPipeline {
    /// Where this pipeline's rejected rows go.
    ///
    /// Derived from the target unless it was set explicitly, so a fleet needs no
    /// per-pipeline configuration and a table that relocates takes its rejects along.
    pub fn dq_uri(&self) -> String {
        self.dq_uri
            .clone()
            .unwrap_or_else(|| crate::dq::uri_for(&self.target_uri))
    }
}

/// `ddi`'s own configuration, which is deliberately not where the work is described.
///
/// What each pipeline *does* — which tables, which SQL, which timestamp — comes from the
/// dbt project, because that is where it is already written down and where it is kept
/// correct. Two copies of that would only ever disagree. What is left here is the part
/// dbt has no opinion about: where the manifest is, how hard to run, and how to
/// authenticate.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Path to the dbt project's `target/manifest.json`. This is the source of truth.
    #[serde(default)]
    pub manifest: Option<String>,

    /// How eagerly to run, and how large a batch may get.
    #[serde(default, alias = "defaults")]
    pub runtime: Defaults,

    #[serde(default)]
    pub storage: StorageConfig,

    /// Hand-written pipelines, for running without a dbt project at all. When `manifest`
    /// is set these are ignored — the manifest wins, so there is one place to look.
    #[serde(default, rename = "pipeline")]
    pub pipelines: Vec<PipelineConfig>,

    /// Where realtime payloads go. Omit it and nothing publishes, whatever dbt asks for.
    #[serde(default)]
    pub publish: Option<PublisherConfig>,

    /// Set by `--no-publish`, not by the file. Disables publication for the whole process
    /// without touching the dbt project or the deployment's own config.
    #[serde(skip)]
    pub publish_disabled: bool,
}

/// Which realtime backend a payload goes to.
///
/// One variant today. The trait behind it takes any implementation, so a second backend is
/// one impl and one arm here — but designing the abstraction against a sample of one would
/// be speculation, so it stays an enum rather than a plugin surface.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublisherKind {
    /// Azure Web PubSub, addressed by its data-plane REST API.
    #[default]
    Webpubsub,
}

impl PublisherKind {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "webpubsub" => Some(PublisherKind::Webpubsub),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            PublisherKind::Webpubsub => "webpubsub",
        }
    }

    /// Every spelling this build accepts, for an error that names the alternatives.
    pub fn known() -> &'static str {
        "webpubsub"
    }
}

/// Where realtime payloads go, when a dbt model asks for one.
///
/// Deployment-level, beside [`StorageConfig`], for two reasons. It carries a credential, and
/// `ddi dbt convert` deliberately omits `[storage]` from what it writes because that file is
/// meant to be readable in a merge request — a secret under `[[pipeline]]` would be printed
/// into it. It is also the only placement that works at all when `manifest` is set, since
/// hand-written `[[pipeline]]` entries are ignored then.
///
/// Absent means nothing publishes, whatever the dbt models ask for. That is a supported
/// state, not a misconfiguration: a dev deployment with no hub must still stream.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublisherConfig {
    #[serde(rename = "type", default)]
    pub kind: PublisherKind,

    /// The connection string itself. Prefer `connection_string_env`; this exists because a
    /// test needs to point at a local address without mutating process-global environment.
    #[serde(default)]
    pub connection_string: Option<String>,

    /// Name of the environment variable holding the connection string.
    #[serde(default)]
    pub connection_string_env: Option<String>,

    pub hub: String,

    /// 0..=300, per the service. A live delta that is a minute stale is worthless, so let
    /// the service drop it rather than replay it at a browser that reconnects later.
    #[serde(default = "default_message_ttl_secs")]
    pub message_ttl_secs: u32,

    /// Shorter than the Trino client's, deliberately: a publish taking five seconds is
    /// already useless to a dashboard and is holding up the next batch.
    #[serde(default = "default_publish_timeout_secs")]
    pub timeout_secs: u64,

    /// Consecutive failures before the breaker opens and stops paying to build payloads
    /// nobody is receiving.
    #[serde(default = "default_publish_failure_threshold")]
    pub failure_threshold: u32,

    /// How long the breaker stays open before letting one request through again.
    #[serde(default = "default_publish_breaker_cooldown_secs")]
    pub breaker_cooldown_secs: u64,

    /// Below the service's 1 MB frame limit, with room for the envelope around the rows.
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: String,
}

fn default_message_ttl_secs() -> u32 {
    60
}
fn default_publish_timeout_secs() -> u64 {
    5
}
fn default_publish_failure_threshold() -> u32 {
    5
}
fn default_publish_breaker_cooldown_secs() -> u64 {
    30
}
fn default_max_message_bytes() -> String {
    "900KB".into()
}

/// Hand-written so a connection string cannot reach a log through `{:?}` on a `Config`, a
/// `ResolvedPipeline`, or anything that contains one.
impl std::fmt::Debug for PublisherConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublisherConfig")
            .field("kind", &self.kind)
            .field("hub", &self.hub)
            .field(
                "connection_string",
                &self.connection_string.as_ref().map(|_| "<redacted>"),
            )
            .field("connection_string_env", &self.connection_string_env)
            .field("message_ttl_secs", &self.message_ttl_secs)
            .field("timeout_secs", &self.timeout_secs)
            .field("failure_threshold", &self.failure_threshold)
            .field("breaker_cooldown_secs", &self.breaker_cooldown_secs)
            .field("max_message_bytes", &self.max_message_bytes)
            .finish()
    }
}

impl PublisherConfig {
    /// Everything that can be judged without reaching the network.
    ///
    /// Never called in a way that can fail a pipeline: a publisher is a leaf, and a
    /// deployment whose hub is misspelled must still stream to Delta. The caller warns.
    pub fn check(&self) -> std::result::Result<(), String> {
        match (&self.connection_string, &self.connection_string_env) {
            (None, None) => {
                return Err(
                    "[publish] needs a credential: set connection_string_env to the \
                            name of an environment variable holding the Web PubSub \
                            connection string"
                        .into(),
                )
            }
            (Some(_), Some(_)) => {
                return Err("[publish] sets both connection_string and \
                            connection_string_env; use one, and prefer the env form"
                    .into())
            }
            _ => {}
        }
        if !crate::dbt::analyze::valid_group(&self.hub) {
            return Err(format!(
                "[publish].hub is {:?}. It goes into a request path, so it must be 1-128 \
                 characters of A-Z a-z 0-9 . _ : - starting with a letter or digit.",
                self.hub
            ));
        }
        if self.timeout_secs == 0 || self.timeout_secs > 300 {
            return Err(format!(
                "[publish].timeout_secs is {}; a publish that slow is already useless to a \
                 dashboard and is holding up the next batch. Use 1..=300.",
                self.timeout_secs
            ));
        }
        if self.breaker_cooldown_secs > 86_400 {
            return Err(format!(
                "[publish].breaker_cooldown_secs is {}, which is longer than a day",
                self.breaker_cooldown_secs
            ));
        }
        // The service's own bound. Sending outside it is a 400 at the far end, which is a
        // worse way to find out than a line at startup.
        if self.message_ttl_secs > 300 {
            return Err(format!(
                "[publish].message_ttl_secs is {}, and the service accepts 0..=300",
                self.message_ttl_secs
            ));
        }
        let bytes = parse_size(&self.max_message_bytes, "[publish].max_message_bytes")
            .map_err(|e| e.to_string())?;
        if bytes == 0 || bytes > 1_000_000 {
            return Err(format!(
                "[publish].max_message_bytes is {}, and the service's frame limit is 1 MB; \
                 leave room for the envelope around the rows",
                self.max_message_bytes
            ));
        }
        Ok(())
    }

    /// The connection string, from wherever it was configured.
    pub fn connection_string(&self) -> std::result::Result<String, String> {
        if let Some(s) = &self.connection_string {
            return Ok(s.clone());
        }
        let name = self
            .connection_string_env
            .as_deref()
            .ok_or_else(|| "no connection string configured".to_string())?;
        std::env::var(name).map_err(|_| {
            format!("[publish].connection_string_env names {name:?}, which is not set")
        })
    }

    pub fn max_message_bytes(&self) -> u64 {
        parse_size(&self.max_message_bytes, "max_message_bytes")
            .unwrap_or(900_000)
            .min(1_000_000)
    }
}

/// What a dbt model asked to publish, pinned onto the pipeline whose commits carry it.
///
/// Not a pipeline of its own: no Delta target, no `txn` action, no offset, no part in the
/// nightly handover. It rides on one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishModel {
    /// The dbt model the SQL came from, so a dashboard can name what it renders.
    pub model: String,
    pub kind: PublisherKind,
    /// The channel browsers subscribe to.
    pub group: String,
    /// A single SELECT over `source`, which at run time is the committed batch.
    pub publish_sql: String,
}

/// Why this pipeline must not publish, if it must not.
///
/// Reached from `resolve_publish`, which runs on the *expanded* list — so by the time this
/// sees a staged upsert it has already become an appending half and a merging half, and the
/// appending half would answer "no problem" about rows going into a private staging table.
///
/// That case is therefore not this function's to catch, and is not left to it: `expand_staged`
/// clears `publish` on both halves unconditionally as it builds them. **Do not delete those
/// two lines on the strength of this check** — it cannot see what they prevent.
fn publish_problem(p: &PipelineConfig) -> Option<String> {
    p.publish.as_ref()?;
    if p.write_mode.keeps_one_row_per_key() {
        return Some(format!(
            "pipeline {:?} is write_mode = {:?} and cannot publish. A merge replaces the row \
             already stored under a key, so the committed batch does not say what the \
             dashboard delta is — the value it replaced is not in it, and adding the new row \
             would double-count. Publish from an append-only model and aggregate current \
             state downstream.",
            p.name,
            match p.write_mode {
                WriteMode::Upsert => "upsert",
                WriteMode::StagedUpsert => "staged_upsert",
                WriteMode::Append => "append",
            }
        ));
    }
    None
}

/// A pipeline that will not run, and why.
#[derive(Clone, Debug)]
pub struct Rejection {
    pub name: String,
    pub reason: String,
}

/// What a config amounts to: the pipelines that will run, and the ones that will not.
#[derive(Clone, Debug, Default)]
pub struct Resolved {
    pub pipelines: Vec<ResolvedPipeline>,
    /// Empty in the ordinary case. Anything here is a pipeline held back, named so an
    /// operator can find it — and exported as `ddi_pipeline_config_valid 0` so a fleet does
    /// not rely on somebody reading the startup log.
    pub rejected: Vec<Rejection>,
}

impl Resolved {
    pub fn is_empty(&self) -> bool {
        self.pipelines.is_empty()
    }
}

/// Refuse to feed one pipeline's upserted target to another pipeline that cannot read it.
///
/// An upsert commit carries `Remove` actions with `dataChange: true`, so
/// [`crate::source::classify`] calls it a `Change` commit — the same class as a `DELETE` or
/// `MERGE` upstream. None of the three change policies is a safe default downstream:
///
/// - `fail` stops the pipeline on the first upsert commit.
/// - `skip_change_commits` drops the whole commit **including its `Add`s**, so keys inserted
///   by that merge never reach the downstream target. Silent, permanent loss.
/// - `ignore_changes` re-emits the rewritten rows, which is right only if the downstream is
///   itself upserting on the same key — otherwise it duplicates them.
///
/// Only the last combination works, so it is the only one allowed to pass quietly. The other
/// two are a configuration mistake that no amount of runtime care can repair, which puts the
/// check here.
/// Why this staged pipeline cannot be split, if it cannot.
///
/// Checked before expansion so the complaint names the pipeline the operator wrote rather
/// than a half this code invented. A staged pipeline that fails here is left unexpanded and
/// rejected by the resolver under its own name.
fn staged_problem(p: &PipelineConfig) -> Option<String> {
    if p.upsert_key.is_none() && p.dedup_key.is_none() {
        return Some(
            "write_mode = \"staged_upsert\" needs upsert_key (or dedup_key, which it falls \
             back to) — the apply half merges on it. Without it staging would only be a \
             slower append."
                .into(),
        );
    }
    if p.dedup_timestamp.is_none() {
        return Some(
            "write_mode = \"staged_upsert\" needs dedup_timestamp — it decides which of the \
             rows accumulated for one key is the one to keep. In dbt: \
             `meta: {ddi_timestamp: _timestamp}`."
                .into(),
        );
    }
    // A stage that is its own source or target is a loop, and the derived name cannot
    // collide, so reaching this means somebody set `stage_uri` by hand.
    let stage = p
        .stage_uri
        .clone()
        .unwrap_or_else(|| crate::stage::uri_for(&p.target_uri));
    if stage == p.target_uri || stage == p.source_uri {
        return Some(format!(
            "stage_uri {stage:?} is the same table as this pipeline's own source or target, \
             which would feed it its own output."
        ));
    }
    if !crate::stage::is_stage_uri(&stage) {
        return Some(format!(
            "stage_uri {stage:?} does not end in {:?}. The suffix is what tells every other \
             pipeline that the table is private to this one — without it, nothing stops a \
             second pipeline reading rows that are about to be consumed.",
            crate::stage::SUFFIX
        ));
    }
    None
}

/// Turn every `staged_upsert` entry into the two ordinary pipelines that implement it.
///
/// Runs before validation and before [`cascade_conflicts`], so everything downstream — the
/// resolver, the runtime, the metrics, the cascade check itself — sees only `Append` and
/// `Upsert` and needs to know nothing about staging. See [`crate::stage`] for why the
/// feature is shaped this way.
///
/// The split is not a copy. Each half keeps only what it is responsible for, and getting
/// that wrong is how a staged pipeline would quietly do a thing twice:
///
/// - The **transform, lookups and data-quality handling** belong to the ingest half alone.
///   A pinned lookup in particular *must* resolve there, against the raw source commit, or
///   the FX rate applied to a row would depend on when the apply half happened to run.
/// - The **merge key, timestamp and tie-breaker** belong to the apply half, which is the
///   only one that merges.
/// - The **rebuild watermark** belongs to the ingest half. Rows a dbt rebuild already covers
///   are dropped before they are staged, so applying it again downstream would be asking a
///   question already answered.
fn expand_staged(pipelines: &[PipelineConfig]) -> Vec<PipelineConfig> {
    let mut out = Vec::with_capacity(pipelines.len());
    for p in pipelines {
        // Left whole when it could not be correct, so the resolver reports the reason
        // against the operator's own name. See `staged_problem`.
        if !p.write_mode.is_staged() || staged_problem(p).is_some() {
            out.push(p.clone());
            continue;
        }
        let stage_uri = p
            .stage_uri
            .clone()
            .unwrap_or_else(|| crate::stage::uri_for(&p.target_uri));

        let mut ingest = p.clone();
        ingest.name = crate::stage::ingest_name(&p.name);
        ingest.app_id = crate::stage::ingest_app_id(&p.app_id);
        ingest.target_uri = stage_uri.clone();
        // Neither half may publish, and the ingest half is why this is stated here rather
        // than left to the resolver. Two lines below it becomes `write_mode = Append`, so by
        // the time anything downstream asks "is this append-only?" the answer is yes — while
        // the rows it commits are going into a private staging table that gets merged away.
        // `publish_problem` refuses the combination against the operator's own entry before
        // this split happens; this makes it true of the halves as well.
        ingest.publish = None;
        // The stage is not in anybody's catalog, and claiming it is would send the locator
        // looking for a table nothing has declared.
        ingest.target_relation = None;
        ingest.write_mode = WriteMode::Append;
        // Appending is all this half does; the merge settings are the other half's.
        ingest.upsert_key = None;
        ingest.upsert_lookback = None;
        ingest.upsert_tiebreak = Vec::new();
        // Rejects belong beside the real target, not beside the stage. Pinned explicitly
        // because the default derives from `target_uri`, which for this half is the stage.
        ingest.dq_uri = Some(
            p.dq_uri
                .clone()
                .unwrap_or_else(|| crate::dq::uri_for(&p.target_uri)),
        );
        ingest.stage_uri = None;
        ingest.apply_max_bytes = None;
        ingest.apply_max_latency_secs = None;
        ingest.stage_for = Some(p.target_uri.clone());

        let mut apply = p.clone();
        apply.name = crate::stage::apply_name(&p.name);
        apply.app_id = crate::stage::apply_app_id(&p.app_id);
        apply.source_uri = stage_uri;
        apply.source_relation = None;
        apply.write_mode = WriteMode::Upsert;
        // This half merges, so `publish_problem` would refuse it anyway. Stated for the
        // same reason as on the ingest half: neither is a thing the operator wrote.
        apply.publish = None;
        // Already applied on the way in. Running the transform twice would apply it to its
        // own output, and re-resolving a lookup here would pin it to the wrong instant.
        apply.transform_sql = None;
        apply.lookups = Vec::new();
        apply.watermark_uri = None;
        // The stage begins empty and is read from its first commit; the raw source's
        // starting version has nothing to say about it.
        apply.starting_version = 0;
        // The stage is ddi's own append-only table. A `dataChange: false` compaction of it
        // is skipped by the reader that skips them anyway, so what `fail` actually catches
        // is a genuine rewrite of staged rows — which no correct process performs, and
        // which would silently drop pending state if it were tolerated.
        apply.change_policy = ChangePolicy::Fail;
        apply.stage_uri = None;
        apply.apply_max_bytes = None;
        apply.apply_max_latency_secs = None;
        apply.stage_for = None;
        // The whole point of the mode: accumulate, then pay for the target once.
        if let Some(b) = &p.apply_max_bytes {
            apply.max_bytes_per_batch = Some(b.clone());
        }
        if let Some(l) = p.apply_max_latency_secs {
            apply.allowed_latency_secs = Some(l);
        }

        out.push(ingest);
        out.push(apply);
    }
    out
}

/// Pipelines that would read or write a stage table they do not own.
///
/// A stage is an implementation detail of the pipeline that owns it, and it is append-only
/// on the strength of that: the apply half assumes nothing else writes there, and drops its
/// rows once merged. Another pipeline writing to one would have its rows applied to a target
/// it never named; another pipeline reading one would be reading rows that are about to be
/// consumed, and would see each of them once or twice depending on a race.
///
/// Checked against the *expanded* list, so the two halves' own use of the stage is already
/// accounted for and only a third party trips it.
fn stage_conflicts(pipelines: &[PipelineConfig]) -> Vec<(String, String)> {
    use std::collections::BTreeMap;

    // Which pipeline legitimately owns each stage, by the naming rule that created it.
    let mut owners: BTreeMap<&str, &str> = BTreeMap::new();
    for p in pipelines {
        if crate::stage::is_stage_uri(&p.target_uri) {
            owners.insert(p.target_uri.as_str(), p.name.as_str());
        }
    }

    let mut out = Vec::new();
    for p in pipelines {
        for (uri, which) in [(&p.source_uri, "reads"), (&p.target_uri, "writes to")] {
            if !crate::stage::is_stage_uri(uri) {
                continue;
            }
            // Its own stage, or the stage of the pipeline it was split from.
            let owner = owners.get(uri.as_str()).copied().unwrap_or("");
            let mine = p.name == owner
                || crate::stage::apply_name(owner.trim_end_matches("__ingest")) == p.name;
            if mine {
                continue;
            }
            out.push((
                p.name.clone(),
                format!(
                    "{which} {uri:?}, which is the staging table of a staged_upsert pipeline. \
                     A stage is private to the pair that owns it: its rows are appended by one \
                     half and consumed by the other, so a third pipeline reading it would see \
                     each row once or twice depending on a race, and one writing to it would \
                     have its rows merged into a target it never named. Point this pipeline at \
                     the real table instead."
                ),
            ));
        }
    }
    out
}

fn cascade_conflicts(pipelines: &[PipelineConfig]) -> Vec<(String, String)> {
    use crate::source::ChangePolicy;

    let mut out = Vec::new();
    for up in pipelines.iter().filter(|p| p.write_mode.is_upsert()) {
        for down in pipelines.iter().filter(|d| d.source_uri == up.target_uri) {
            let ok =
                down.change_policy == ChangePolicy::IgnoreChanges && down.write_mode.is_upsert();
            if ok {
                continue;
            }
            // The *reader* is held back, not the writer. The upserting pipeline is correct
            // on its own; it is this one that cannot make sense of what it produces.
            out.push((
                down.name.clone(),
                format!(
                    "reads {:?} as its source, which pipeline {:?} upserts into. An upsert \
                     rewrites files, so every one of its commits carries a dataChange Remove \
                     and reads here as a change commit. With change_policy = {:?} this \
                     pipeline would {}. The only combination that survives is \
                     change_policy = \"ignore_changes\" together with write_mode = \"upsert\", \
                     keyed the same way, so re-emitted rows merge instead of accumulating.",
                    up.target_uri,
                    up.name,
                    down.change_policy,
                    match down.change_policy {
                        ChangePolicy::Fail => "stop on the first upsert commit",
                        ChangePolicy::SkipChangeCommits =>
                            "silently drop those commits whole — including keys they insert, \
                             which would never arrive",
                        ChangePolicy::IgnoreChanges =>
                            "re-emit rewritten rows, duplicating them because it appends",
                    },
                ),
            ));
        }
    }
    out
}

fn parse_size(s: &str, field: &str) -> Result<u64> {
    s.parse::<bytesize::ByteSize>()
        .map(|b| b.as_u64())
        .map_err(|e| Error::Config(format!("{field}: cannot parse size {s:?}: {e}")))
}

impl Config {
    /// The process's memory budget, resolved against the config and the container.
    ///
    /// Called once at startup, with the pipelines that are actually going to run — not the
    /// ones written down, since a rejected pipeline allocates nothing and dividing by it
    /// would make everyone else's share too small.
    pub fn budget(&self, running: usize) -> Result<crate::budget::Budget> {
        let configured = self
            .runtime
            .max_memory
            .as_deref()
            .map(|s| parse_size(s, "runtime.max_memory"))
            .transpose()?;
        Ok(crate::budget::Budget::resolve(configured, running))
    }

    /// The process's spill budget, resolved and probed against the filesystem.
    ///
    /// Fatal rather than held back, like [`Self::budget`] and unlike a bad pipeline: a spill
    /// directory belongs to the deployment, not to one stream, so there is no single entry to
    /// name in a rejection. It joins the faults `resolve_all` already aborts on. It has one
    /// side effect — it creates the directory it was given, and writes a probe file into it —
    /// which is deliberate, and means `ddi validate` also validates the pod rather than only
    /// the file.
    pub fn spill(&self) -> Result<crate::spill::Spill> {
        let cap = self
            .runtime
            .max_temp_directory_size
            .as_deref()
            .map(|s| {
                let n = parse_size(s, "runtime.max_temp_directory_size")?;
                // `crate::gate::Gate::new` reads a configured 0 as 1 rather than deadlocking,
                // and this deliberately does the opposite. The difference is what 0 *means*:
                // a zero semaphore is a pipeline that never runs again, which nobody typed on
                // purpose, so the nearest sensible reading wins. A zero disk budget is a
                // coherent policy — "never spill" — implemented by DataFusion as an error on
                // the first byte written by every sort, grouped aggregate and merge join in
                // the process. The two plausible intentions ("unbounded" and "no disk at all")
                // point in opposite directions, and guessing between them is how a fleet goes
                // quiet.
                if n == 0 {
                    return Err(Error::Config(format!(
                        "runtime.max_temp_directory_size is {s:?}, which is zero bytes — and a \
                         zero spill budget is not \"do not spill\", it is every sort, grouped \
                         aggregate and merge join in this process failing the moment it writes \
                         its first byte. Give it a real size below the volume behind \
                         runtime.temp_directory (\"8GB\"), or remove the key to keep \
                         DataFusion's own 100GB default."
                    )));
                }
                if n < crate::spill::MIN_TEMP_DIRECTORY_SIZE {
                    return Err(Error::Config(format!(
                        "runtime.max_temp_directory_size is {s:?}, which is under the 1MB \
                         floor. DataFusion writes a spill file in batches and checks the total \
                         after each one, so a budget this small is exceeded by the first write \
                         and every query that spills would fail rather than run slowly. Give it \
                         at least \"1MB\", and in a container something like \"8GB\"."
                    )));
                }
                Ok(n)
            })
            .transpose()?;
        crate::spill::Spill::resolve(self.runtime.temp_directory.as_deref(), cap)
    }

    pub fn from_toml_str(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| Error::Config(format!("invalid config: {e}")))
    }

    pub fn from_path(p: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(p)
            .map_err(|e| Error::Config(format!("cannot read {}: {e}", p.display())))?;
        Self::from_toml_str(&text)
    }

    /// Every pipeline that can run, and every one that cannot with the reason.
    ///
    /// A pipeline that cannot be correct must be caught here, at load, not on its first
    /// batch — but "caught" is not "fatal to everything else". One typo in one of three
    /// hundred entries used to abort the process, which makes the config file a single point
    /// of failure for the whole fleet and puts an editing mistake and an outage one
    /// keystroke apart. So a bad pipeline is set aside, named, and left out of the run.
    ///
    /// Only what cannot be attributed to a single pipeline is still fatal: an unreadable
    /// manifest, or nothing to run at all.
    pub fn resolve_all(&self) -> Result<Resolved> {
        // The dbt project is the source of truth whenever there is one.
        let derived;
        let pipelines: &[PipelineConfig] = match &self.manifest {
            Some(path) => {
                let manifest = crate::dbt::Manifest::from_path(std::path::Path::new(path))?;
                derived = crate::dbt::convert::pipelines(&manifest, &self.storage)?;
                if derived.is_empty() {
                    return Err(Error::Config(format!(
                        "no streamable models in {path:?}. Run `ddi dbt check` to see why                          each model was rejected."
                    )));
                }
                &derived
            }
            None => &self.pipelines,
        };

        if pipelines.is_empty() {
            return Err(Error::Config(
                "nothing to run: set `manifest` to a dbt target/manifest.json, or declare                  [[pipeline]] entries."
                    .into(),
            ));
        }

        // A `staged_upsert` entry is two pipelines wearing one name. Expanding first means
        // every check below — duplicate app_ids, cascades, the resolver itself — sees the
        // two halves as the ordinary pipelines they are, and none of them has to know that
        // staging exists. See `crate::stage`.
        let expanded = expand_staged(pipelines);
        let pipelines: &[PipelineConfig] = &expanded;

        let mut rejected: Vec<Rejection> = Vec::new();
        let mut reject = |name: &str, reason: String| {
            rejected.push(Rejection {
                name: name.to_string(),
                reason,
            })
        };

        // Cross-pipeline checks first, because they condemn pipelines that are individually
        // fine. Both sides of a collision are set aside: a shared app_id corrupts *both*
        // pipelines' offsets, so there is no innocent party to keep running.
        let mut by_app_id: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut by_name: HashMap<&str, usize> = HashMap::new();
        for p in pipelines {
            by_app_id.entry(p.app_id.trim()).or_default().push(&p.name);
            *by_name.entry(&p.name).or_default() += 1;
        }
        let mut condemned: HashMap<&str, String> = HashMap::new();
        for (app_id, users) in &by_app_id {
            if users.len() > 1 {
                for name in users {
                    condemned.entry(name).or_insert_with(|| {
                        format!(
                            "duplicate app_id {app_id:?}, also used by {}. app_id is the \
                             offset key; sharing one silently corrupts every sharer's resume \
                             point, so all of them are held back.",
                            users
                                .iter()
                                .filter(|n| n != &name)
                                .map(|n| format!("{n:?}"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    });
                }
            }
        }
        for (name, count) in &by_name {
            if *count > 1 {
                condemned.entry(name).or_insert_with(|| {
                    format!(
                        "duplicate pipeline name {name:?}: {count} entries share it, so \
                             none of them can be addressed unambiguously"
                    )
                });
            }
        }
        let cascades = cascade_conflicts(pipelines);
        for (name, reason) in &cascades {
            condemned
                .entry(name.as_str())
                .or_insert_with(|| reason.clone());
        }
        let stages = stage_conflicts(pipelines);
        for (name, reason) in &stages {
            condemned
                .entry(name.as_str())
                .or_insert_with(|| reason.clone());
        }

        // Said once here rather than once per pipeline, and never fatal: a deployment whose
        // hub is misspelled must still stream to Delta. Every pipeline that would have
        // published says so individually in `resolve_publish`.
        if let Some(sink) = &self.publish {
            if let Err(reason) = sink.check() {
                tracing::warn!("realtime publishing is disabled: {reason}");
            }
        }

        // Two pipelines pushing to one group interleave their messages on a socket a browser
        // cannot demultiplex by anything but the envelope. A client that filters correctly
        // merely ignores the foreign ones; one that does not re-baselines forever. Either way
        // it is a mistake, and unlike a duplicate app_id it costs nothing to keep streaming
        // through — so the *publication* is dropped from every sharer and the pipelines run.
        // Only pipelines that could actually publish contest a group. An upsert entry, or one
        // whose publication is refused for any other reason, will never send anything — so
        // letting it collide would take a healthy neighbour's dashboard down for company.
        let mut group_users: HashMap<&str, Vec<&str>> = HashMap::new();
        if !self.publish_disabled && self.publish.is_some() {
            for p in pipelines {
                if let Some(m) = &p.publish {
                    if publish_problem(p).is_none() {
                        group_users
                            .entry(m.group.as_str())
                            .or_default()
                            .push(&p.name);
                    }
                }
            }
        }
        let contested_groups: BTreeSet<&str> = group_users
            .iter()
            .filter(|(_, users)| users.len() > 1)
            .flat_map(|(group, users)| {
                warn!(
                    "pipelines {} all publish to group {group:?}. One group is one stream to a \
                     browser, so their messages would interleave with nothing to tell them \
                     apart. None of them will publish; all of them keep streaming to Delta.",
                    users
                        .iter()
                        .map(|n| format!("{n:?}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                users.iter().copied()
            })
            .collect();

        let d = &self.runtime;
        let mut ok = Vec::new();
        for p in pipelines {
            if let Some(reason) = condemned.get(p.name.as_str()) {
                reject(&p.name, reason.clone());
                continue;
            }
            match self.resolve_one(p, d) {
                Ok(mut r) => {
                    if contested_groups.contains(p.name.as_str()) {
                        r.publish = None;
                        r.publish_to = None;
                    }
                    ok.push(r)
                }
                Err(e) => reject(
                    &p.name,
                    match e {
                        Error::Config(m) => m,
                        other => other.to_string(),
                    },
                ),
            }
        }

        Ok(Resolved {
            pipelines: ok,
            rejected,
        })
    }

    /// Every pipeline, or an error naming the ones that cannot run.
    ///
    /// The strict form, for callers that want a config to be all-or-nothing —
    /// `ddi validate` and the tests. `ddi run` uses [`Self::resolve_all`].
    pub fn resolve(&self) -> Result<Vec<ResolvedPipeline>> {
        let r = self.resolve_all()?;
        if let Some(first) = r.rejected.first() {
            return Err(Error::Config(match r.rejected.len() {
                1 => format!("pipeline {:?}: {}", first.name, first.reason),
                n => format!(
                    "{n} pipelines cannot run:\n{}",
                    r.rejected
                        .iter()
                        .map(|x| format!("  {:?}: {}", x.name, x.reason))
                        .collect::<Vec<_>>()
                        .join("\n")
                ),
            }));
        }
        Ok(r.pipelines)
    }

    /// One pipeline, with defaults folded in and every value parsed.
    ///
    /// Messages here name the fault, not the pipeline: the caller already knows which one it
    /// is asking about, and repeats it when reporting.
    /// Pair what a dbt model asked to publish with somewhere to send it.
    ///
    /// Returns `(None, None)` for every way this can be wrong, having said which one it was.
    /// That is the whole contract: a dashboard setting must never be able to stop ingestion.
    /// The two files involved have different owners — an analyst edits the dbt project, an
    /// operator edits the deployment — so the combination "a model asks, this environment
    /// has no hub" is a normal state of a dev deployment rather than a mistake, and even the
    /// genuine mistakes are about a payload nobody is receiving yet.
    ///
    /// This deliberately departs from how contradictory *pipeline* settings are treated a
    /// few lines up, where an upsert key on an append pipeline is rejected rather than
    /// ignored. Those two keys live in one file owned by one person, and getting them wrong
    /// means the rows are wrong. Neither is true here.
    fn resolve_publish(
        &self,
        p: &PipelineConfig,
    ) -> (Option<PublishModel>, Option<PublisherConfig>) {
        let Some(model) = &p.publish else {
            return (None, None);
        };

        if self.publish_disabled {
            tracing::info!(
                pipeline = %p.name,
                model = %model.model,
                "--no-publish: not publishing, streaming as normal"
            );
            return (None, None);
        }

        if let Some(reason) = publish_problem(p) {
            tracing::warn!(pipeline = %p.name, "{reason}");
            return (None, None);
        }

        let Some(sink) = &self.publish else {
            tracing::info!(
                pipeline = %p.name,
                model = %model.model,
                "this model declares ddi_publish, but this deployment has no [publish] \
                 section to send it to. The Delta stream is unaffected."
            );
            return (None, None);
        };

        if let Err(reason) = sink.check() {
            tracing::warn!(
                pipeline = %p.name,
                "not publishing: {reason}. The Delta stream is unaffected."
            );
            return (None, None);
        }

        if sink.kind != model.kind {
            tracing::warn!(
                pipeline = %p.name,
                "model {:?} asks to publish to {:?}, but [publish] is configured for {:?}. \
                 Not publishing; the Delta stream is unaffected.",
                model.model,
                model.kind.as_str(),
                sink.kind.as_str()
            );
            return (None, None);
        }

        // Judged here rather than left to the first request. The dbt gate already applies
        // this, but a pinned config is meant to be read and edited by hand, and a group that
        // would have to be escaped would otherwise pass load, log "each committed batch will
        // also be published", and then fail on every batch until the breaker opened.
        if !crate::dbt::analyze::valid_group(&model.group) {
            tracing::warn!(
                pipeline = %p.name,
                "not publishing: group {:?} goes into a request path and into a browser's \
                 subscription, so it must be 1-128 characters of A-Z a-z 0-9 . _ : - \
                 starting with a letter or digit. The Delta stream is unaffected.",
                model.group
            );
            return (None, None);
        }

        // Normalised here for the same reason `transform_sql` is: what runs must be what the
        // validator approved, and a dialect spelling is rewritten once rather than on every
        // batch. dbt's own gate already validated this, so a failure here is either a
        // hand-written config or a manifest that went around it.
        let publish_sql = match normalise_publish_sql(&model.publish_sql) {
            Ok(sql) => sql,
            Err(e) => {
                tracing::warn!(
                    pipeline = %p.name,
                    "not publishing: publish SQL for model {:?} is not valid: {e}. The Delta \
                     stream is unaffected.",
                    model.model
                );
                return (None, None);
            }
        };

        (
            Some(PublishModel {
                publish_sql,
                ..model.clone()
            }),
            Some(sink.clone()),
        )
    }

    fn resolve_one(&self, p: &PipelineConfig, d: &Defaults) -> Result<ResolvedPipeline> {
        if p.app_id.trim().is_empty() {
            return Err(Error::Config(
                "app_id must not be empty — it is the offset key".into(),
            ));
        }

        // Validated *and normalised*: a dialect spelling this engine does not run — Trino's
        // `CROSS JOIN UNNEST` — is rewritten here, once, so the pipeline executes the same
        // query the validator approved. The model's own text stays in `PipelineConfig`, so
        // `ddi dbt convert` still pins what dbt wrote.
        if p.source_uri == p.target_uri {
            return Err(Error::Config(
                "source_uri and target_uri are the same table; that would feed the pipeline \
                 its own output"
                    .into(),
            ));
        }

        let mut lookup_names = BTreeSet::new();
        let mut lookups = Vec::with_capacity(p.lookups.len());
        for lookup in &p.lookups {
            let lookup = crate::lookup::resolve(lookup)?;
            if !lookup_names.insert(lookup.name.clone()) {
                return Err(Error::Config(format!(
                    "lookup {:?} is declared more than once",
                    lookup.name
                )));
            }
            if lookup.uri == p.source_uri || lookup.uri == p.target_uri {
                return Err(Error::Config(format!(
                    "lookup {:?} points at a streaming source or target; a lookup must be a \
                     separate, read-only Delta table",
                    lookup.name
                )));
            }
            lookups.push(lookup);
        }

        // Declared lookups are the only additional relations a transform may join. The
        // normaliser validates their narrow join shape and rewrites dialect spellings such as
        // Trino's CROSS JOIN UNNEST once, so the pipeline executes exactly what it approved.
        let transform_sql = match &p.transform_sql {
            Some(sql) => Some(normalise_sql_with_lookups(sql, &lookup_names).map_err(
                |e| match e {
                    Error::Config(m) => Error::Config(m),
                    other => Error::Config(other.to_string()),
                },
            )?),
            None => None,
        };

        if !lookups.is_empty() && transform_sql.is_none() {
            return Err(Error::Config(
                "lookups are declared but transform_sql is empty; a lookup must be joined by \
                 a SELECT transformation"
                    .into(),
            ));
        }

        // Only reachable when `expand_staged` declined to split this pipeline, which it does
        // exactly when it could not be correct. Reported here so the reason lands against
        // the name in the config file rather than against a generated half.
        if p.write_mode.is_staged() {
            return Err(Error::Config(staged_problem(p).unwrap_or_else(|| {
                "internal: a staged pipeline reached the resolver unsplit".into()
            })));
        }

        // The merge needs a key to merge on and a value to decide "newer" by. Neither can be
        // inferred, and a pipeline missing one cannot be correct, so it is caught here
        // rather than on its first batch.
        let upsert_key = p.upsert_key.clone().or_else(|| p.dedup_key.clone());
        if p.write_mode.is_upsert() {
            if upsert_key.is_none() {
                return Err(Error::Config(
                    "write_mode = \"upsert\" needs upsert_key (or dedup_key, which it falls \
                     back to) — it is the column a row is matched on. Without it every \
                     delivery would append another copy, which is what append mode already \
                     does."
                        .into(),
                ));
            }
            if p.dedup_timestamp.is_none() {
                return Err(Error::Config(
                    "write_mode = \"upsert\" needs dedup_timestamp — it is the column that \
                     decides whether an arriving row is newer than the one already stored, \
                     and it bounds how much of the target the merge has to read. In dbt: \
                     `meta: {ddi_timestamp: _timestamp}`."
                        .into(),
                ));
            }
        } else if p.upsert_key.is_some()
            || p.upsert_lookback.is_some()
            || !p.upsert_tiebreak.is_empty()
        {
            return Err(Error::Config(
                "upsert_key/upsert_lookback/upsert_tiebreak are set but write_mode is \
                 \"append\", so they do nothing. Set write_mode = \"upsert\", or remove them."
                    .into(),
            ));
        }

        // A tie-breaker that repeats a column, or names the timestamp it is meant to break a
        // tie *on*, cannot order anything the timestamp did not already order. Silently
        // ignoring it would leave the pipeline believing it had a stable order.
        {
            let mut seen = std::collections::BTreeSet::new();
            for c in &p.upsert_tiebreak {
                if c.trim().is_empty() {
                    return Err(Error::Config(
                        "upsert_tiebreak contains an empty column name".into(),
                    ));
                }
                if Some(c) == p.dedup_timestamp.as_ref() {
                    return Err(Error::Config(format!(
                        "upsert_tiebreak lists {c:?}, which is already dedup_timestamp. A \
                         column cannot break a tie against itself — the tie is that the two \
                         rows agree on it."
                    )));
                }
                if !seen.insert(c) {
                    return Err(Error::Config(format!(
                        "upsert_tiebreak lists {c:?} twice; the second comparison can never \
                         decide anything the first did not."
                    )));
                }
            }
        }
        let upsert_lookback = p
            .upsert_lookback
            .as_deref()
            .map(crate::upsert::Lookback::parse)
            .transpose()
            .map_err(|e| match e {
                Error::Config(m) => Error::Config(m),
                other => Error::Config(other.to_string()),
            })?;

        let max_bytes = parse_size(
            p.max_bytes_per_batch
                .as_ref()
                .unwrap_or(&d.max_bytes_per_batch),
            "max_bytes_per_batch",
        )?;
        let target_file_size = parse_size(
            p.target_file_size.as_ref().unwrap_or(&d.target_file_size),
            "target_file_size",
        )?;

        // A publication is a leaf, so nothing here may return `Err`: every way this can be
        // wrong leaves the pipeline streaming to Delta exactly as it would have, and says so.
        // The dbt path rejects the bad combinations far earlier and far more loudly — this is
        // the last gate, for hand-written config and for anything dbt let through.
        let (publish, publish_to) = self.resolve_publish(p);

        Ok(ResolvedPipeline {
            name: p.name.clone(),
            app_id: p.app_id.clone(),
            source_uri: p.source_uri.clone(),
            target_uri: p.target_uri.clone(),
            lookups,
            starting_version: p.starting_version,
            change_policy: p.change_policy,
            transform_sql,
            allowed_latency_secs: p.allowed_latency_secs.unwrap_or(d.allowed_latency_secs),
            max_bytes_per_batch: max_bytes,
            max_files_per_batch: p.max_files_per_batch.unwrap_or(d.max_files_per_batch),
            max_output_rows_per_batch: p
                .max_output_rows_per_batch
                .unwrap_or(d.max_output_rows_per_batch),
            target_file_size,
            watermark_uri: p
                .watermark_uri
                .clone()
                .or_else(|| self.storage.watermark_uri.clone()),
            dedup_timestamp: p.dedup_timestamp.clone(),
            dedup_key: p.dedup_key.clone(),
            write_mode: p.write_mode,
            upsert_key,
            upsert_lookback,
            upsert_tiebreak: p.upsert_tiebreak.clone(),
            stage_for: p.stage_for.clone(),
            dq_uri: p.dq_uri.clone(),
            storage: crate::storage::Storage::new(self.storage.options.clone()),
            source_relation: p.source_relation.clone(),
            target_relation: p.target_relation.clone(),
            publish,
            publish_to,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
[[pipeline]]
name = "orders_header"
app_id = "ddi.orders_header"
source_uri = "/tmp/bronze/orders"
target_uri = "/tmp/silver/orders"
transform_sql = "SELECT order_id FROM source"
"#;

    /// A `[runtime]` table plus the one pipeline `BASE` has, so a spill key can be tested
    /// against a config that is otherwise valid.
    fn with_runtime(keys: &str) -> Config {
        Config::from_toml_str(&format!("[runtime]\n{keys}\n{BASE}")).expect("valid TOML")
    }

    #[test]
    fn a_spill_directory_and_a_cap_are_read_from_the_runtime_table() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = with_runtime(&format!(
            "temp_directory = {:?}\nmax_temp_directory_size = \"8GB\"",
            dir.path().to_str().unwrap()
        ));
        let spill = cfg.spill().expect("a fresh tempdir is usable");
        // bytesize is decimal, as `max_bytes_per_batch` already is.
        assert_eq!(spill.limit_bytes(), 8_000_000_000);
        assert_eq!(spill.directory(), Some(dir.path()));
        assert!(spill.cap_was_configured());
    }

    #[test]
    fn no_spill_keys_means_the_os_temp_directory_and_datafusions_own_cap() {
        let spill = Config::from_toml_str(BASE).unwrap().spill().unwrap();
        assert_eq!(spill.directory(), None);
        assert!(!spill.cap_was_configured());
        assert_eq!(spill.limit_bytes(), 100 * 1024 * 1024 * 1024);
    }

    #[test]
    fn a_spill_cap_of_zero_is_refused_because_it_is_not_the_unbounded_default() {
        // All three parse to Ok(0), so none of them is caught by `parse_size`.
        for written in ["0", "0GB", "0B"] {
            let e = with_runtime(&format!("max_temp_directory_size = {written:?}"))
                .spill()
                .unwrap_err()
                .to_string();
            assert!(e.contains("runtime.max_temp_directory_size"), "{e}");
            assert!(e.contains("zero bytes"), "{e}");
            assert!(e.contains("8GB"), "{e}");
        }
    }

    #[test]
    fn a_spill_cap_below_one_megabyte_names_the_floor_and_the_size_to_write() {
        let e = with_runtime("max_temp_directory_size = \"64KB\"")
            .spill()
            .unwrap_err()
            .to_string();
        assert!(e.contains("1MB"), "{e}");
        assert!(e.contains("after each one"), "{e}");
    }

    #[test]
    fn a_spill_cap_that_is_not_a_size_names_the_key_and_the_value() {
        let e = with_runtime("max_temp_directory_size = \"8 gigs\"")
            .spill()
            .unwrap_err()
            .to_string();
        assert!(e.contains("runtime.max_temp_directory_size"), "{e}");
        assert!(e.contains("8 gigs"), "{e}");
        assert!(e.contains("cannot parse size"), "{e}");
    }

    #[test]
    fn a_spill_directory_that_cannot_be_used_is_refused_at_load_not_at_the_first_sort() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("mounted-nowhere");
        std::fs::write(&file, b"x").unwrap();
        let e = with_runtime(&format!("temp_directory = {:?}", file.to_str().unwrap()))
            .spill()
            .unwrap_err()
            .to_string();
        assert!(e.contains("runtime.temp_directory"), "{e}");
        assert!(e.contains("is not usable"), "{e}");
    }

    #[test]
    fn a_misspelled_spill_key_is_rejected_rather_than_ignored() {
        // `deny_unknown_fields` already does this; pinned now that there are neighbours close
        // enough in spelling to typo into.
        assert!(
            Config::from_toml_str(&format!("[runtime]\ntemp_directroy = \"/tmp\"\n{BASE}"))
                .is_err()
        );
    }

    #[test]
    fn minimal_config_resolves() {
        let c = Config::from_toml_str(BASE).unwrap();
        let r = c.resolve().unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].app_id, "ddi.orders_header");
        assert_eq!(
            r[0].change_policy,
            ChangePolicy::Fail,
            "Fail is the default"
        );
        assert_eq!(r[0].max_bytes_per_batch, 256 * 1000 * 1000);
    }

    #[test]
    fn duplicate_app_id_is_rejected_with_the_reason() {
        let toml = format!(
            "{BASE}\n[[pipeline]]\nname = \"other\"\napp_id = \"ddi.orders_header\"\n\
             source_uri = \"/tmp/a\"\ntarget_uri = \"/tmp/b\"\n"
        );
        let e = Config::from_toml_str(&toml).unwrap().resolve().unwrap_err();
        assert!(e.to_string().contains("duplicate app_id"), "got: {e}");
        assert!(e.to_string().contains("offset key"), "got: {e}");
    }

    #[test]
    fn empty_app_id_is_rejected() {
        let toml = r#"
[[pipeline]]
name = "x"
app_id = ""
source_uri = "/tmp/a"
target_uri = "/tmp/b"
"#;
        assert!(Config::from_toml_str(toml).unwrap().resolve().is_err());
    }

    #[test]
    fn stateful_sql_is_rejected_at_config_load_not_at_runtime() {
        let toml = r#"
[[pipeline]]
name = "x"
app_id = "ddi.x"
source_uri = "/tmp/a"
target_uri = "/tmp/b"
transform_sql = "SELECT customer_id, sum(total) FROM source GROUP BY customer_id"
"#;
        let e = Config::from_toml_str(toml).unwrap().resolve().unwrap_err();
        assert!(e.to_string().contains("GROUP BY"), "got: {e}");
    }

    #[test]
    fn source_equal_to_target_is_rejected() {
        let toml = r#"
[[pipeline]]
name = "x"
app_id = "ddi.x"
source_uri = "/tmp/same"
target_uri = "/tmp/same"
"#;
        let e = Config::from_toml_str(toml).unwrap().resolve().unwrap_err();
        assert!(e.to_string().contains("same table"), "got: {e}");
    }

    #[test]
    fn fan_out_two_targets_from_one_source_is_allowed() {
        // The common shape for order data: header + line items.
        let toml = r#"
[[pipeline]]
name = "orders_header"
app_id = "ddi.orders_header"
source_uri = "/tmp/bronze/orders"
target_uri = "/tmp/silver/orders"

[[pipeline]]
name = "orders_lines"
app_id = "ddi.orders_lines"
source_uri = "/tmp/bronze/orders"
target_uri = "/tmp/silver/order_lines"
"#;
        let r = Config::from_toml_str(toml).unwrap().resolve().unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].source_uri, r[1].source_uri);
        assert_ne!(
            r[0].app_id, r[1].app_id,
            "each target resumes independently"
        );
    }

    #[test]
    fn per_pipeline_overrides_beat_defaults() {
        let toml = r#"
[defaults]
max_bytes_per_batch = "64MB"

[[pipeline]]
name = "a"
app_id = "ddi.a"
source_uri = "/tmp/a"
target_uri = "/tmp/b"
max_bytes_per_batch = "8MB"

[[pipeline]]
name = "b"
app_id = "ddi.b"
source_uri = "/tmp/c"
target_uri = "/tmp/d"
"#;
        let r = Config::from_toml_str(toml).unwrap().resolve().unwrap();
        assert_eq!(r[0].max_bytes_per_batch, 8 * 1000 * 1000);
        assert_eq!(
            r[1].max_bytes_per_batch,
            64 * 1000 * 1000,
            "falls back to defaults"
        );
    }

    #[test]
    fn unknown_field_is_rejected_rather_than_ignored() {
        // A typo in a correctness-relevant key must not silently take the default.
        let toml = r#"
[[pipeline]]
name = "a"
app_id = "ddi.a"
source_uri = "/tmp/a"
target_uri = "/tmp/b"
change_polcy = "ignore_changes"
"#;
        assert!(Config::from_toml_str(toml).is_err());
    }

    #[test]
    fn change_policy_parses_from_snake_case() {
        let toml = BASE.to_string().replace(
            "transform_sql = \"SELECT order_id FROM source\"",
            "change_policy = \"skip_change_commits\"",
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve().unwrap();
        assert_eq!(r[0].change_policy, ChangePolicy::SkipChangeCommits);
    }

    #[test]
    fn lookup_table_id_change_policy_parses_and_defaults_to_strict() {
        let lookup = r#"
[[pipeline.lookups]]
name = "fx_rates"
uri = "/tmp/fx_rates"
"#;
        let pipeline = BASE.replace(
            "transform_sql = \"SELECT order_id FROM source\"",
            "transform_sql = \"SELECT o.order_id FROM source AS o LEFT JOIN fx_rates AS fx ON fx.currency = o.currency\"",
        );
        let strict = Config::from_toml_str(&(pipeline.clone() + lookup))
            .unwrap()
            .resolve()
            .unwrap();
        assert_eq!(
            strict[0].lookups[0].table_id_change_policy,
            crate::lookup::LookupTableIdChangePolicy::Strict
        );

        let mut current_toml = pipeline;
        current_toml.push_str(lookup);
        current_toml.push_str("table_id_change_policy = \"use_current\"\n");
        let current = Config::from_toml_str(&current_toml)
            .unwrap()
            .resolve()
            .unwrap();
        assert_eq!(
            current[0].lookups[0].table_id_change_policy,
            crate::lookup::LookupTableIdChangePolicy::UseCurrent
        );
    }

    #[test]
    fn empty_config_is_rejected() {
        assert!(Config::from_toml_str("").unwrap().resolve().is_err());
    }

    const UPSERT: &str = r#"
[[pipeline]]
name = "orders"
app_id = "ddi.orders"
source_uri = "/tmp/bronze/orders"
target_uri = "/tmp/silver/orders"
write_mode = "upsert"
dedup_timestamp = "_timestamp"
dedup_key = "order_id"
"#;

    const STAGED: &str = r#"
[[pipeline]]
name = "style"
app_id = "ddi.style"
source_uri = "/tmp/bronze/style"
target_uri = "/tmp/silver/style"
write_mode = "staged_upsert"
dedup_timestamp = "_timestamp"
upsert_key = "style_id"
transform_sql = "SELECT style_id, _timestamp FROM source"
apply_max_bytes = "512MB"
apply_max_latency_secs = 900
"#;

    fn staged() -> Vec<ResolvedPipeline> {
        Config::from_toml_str(STAGED).unwrap().resolve().unwrap()
    }

    #[test]
    fn a_staged_pipeline_becomes_an_ingest_and_an_apply() {
        let r = staged();
        assert_eq!(r.len(), 2, "one written down, two running");

        let ingest = &r[0];
        let apply = &r[1];
        assert_eq!(ingest.name, "style__ingest");
        assert_eq!(apply.name, "style__apply");

        // The stage is between them, and is the only table they share.
        assert_eq!(ingest.target_uri, "/tmp/silver/style__ddi_stage");
        assert_eq!(apply.source_uri, "/tmp/silver/style__ddi_stage");
        assert_eq!(ingest.source_uri, "/tmp/bronze/style");
        assert_eq!(apply.target_uri, "/tmp/silver/style");

        // Append in, merge out. Nothing downstream needs to know staging happened.
        assert_eq!(ingest.write_mode, WriteMode::Append);
        assert_eq!(apply.write_mode, WriteMode::Upsert);
        assert!(!r.iter().any(|p| p.write_mode.is_staged()));
    }

    #[test]
    fn the_two_halves_never_share_an_offset() {
        // The exactly-once story rests on this: one `txn` key advanced by both would let
        // either resume from a version the other had reached.
        let r = staged();
        assert_ne!(r[0].app_id, r[1].app_id);
        assert_eq!(r[0].app_id, "ddi.style.ingest");
        assert_eq!(r[1].app_id, "ddi.style.apply");
    }

    #[test]
    fn the_transform_and_its_lookups_belong_to_the_ingest_half_alone() {
        // Running the transform again downstream would apply it to its own output, and
        // re-resolving a pinned lookup there would pin it to the instant the apply worker
        // happened to run rather than to the source commit.
        let r = staged();
        assert!(r[0].transform_sql.is_some());
        assert!(r[1].transform_sql.is_none());
        assert!(r[1].lookups.is_empty());
    }

    #[test]
    fn the_accumulation_limits_land_on_the_half_that_merges() {
        let r = staged();
        assert_eq!(r[1].max_bytes_per_batch, 512 * 1000 * 1000);
        assert_eq!(r[1].allowed_latency_secs, 900);
        // The ingest half keeps the ordinary defaults: its whole purpose is to stay cheap
        // and current, and accumulating there would only delay the stage.
        assert_ne!(r[0].max_bytes_per_batch, r[1].max_bytes_per_batch);
    }

    #[test]
    fn rejects_go_beside_the_real_target_not_beside_the_stage() {
        // The default derives from `target_uri`, which for the ingest half is the stage —
        // so without pinning it, a fleet's rejects would scatter into staging tables.
        let r = staged();
        assert_eq!(r[0].dq_uri(), "/tmp/silver/style__ddi_dq");
    }

    #[test]
    fn only_the_ingest_half_is_held_to_producing_every_column() {
        let r = staged();
        assert_eq!(r[0].stage_for.as_deref(), Some("/tmp/silver/style"));
        assert!(r[1].stage_for.is_none());
    }

    #[test]
    fn a_staged_pipeline_without_a_key_is_named_as_the_operator_wrote_it() {
        // Not as "style__apply", which is a name they never chose and cannot search for.
        let toml = STAGED.replace("upsert_key = \"style_id\"\n", "");
        let e = Config::from_toml_str(&toml)
            .unwrap()
            .resolve()
            .unwrap_err()
            .to_string();
        assert!(e.contains("\"style\""), "names the written pipeline: {e}");
        assert!(!e.contains("__apply"), "not a generated half: {e}");
        assert!(e.contains("upsert_key"), "and what is missing: {e}");
    }

    #[test]
    fn a_third_pipeline_may_not_touch_somebody_elses_stage() {
        // The stage is append-only on the strength of nobody else writing it, and its rows
        // are consumed once. A reader would race the apply half for them.
        let toml = format!(
            "{STAGED}\n\
             [[pipeline]]\n\
             name = \"nosy\"\n\
             app_id = \"ddi.nosy\"\n\
             source_uri = \"/tmp/silver/style__ddi_stage\"\n\
             target_uri = \"/tmp/silver/copy\"\n"
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve_all().unwrap();
        let held = r
            .rejected
            .iter()
            .find(|x| x.name == "nosy")
            .expect("the interloper is held back");
        assert!(held.reason.contains("staging table"), "{}", held.reason);

        // And the pair that owns it still runs.
        assert_eq!(r.pipelines.len(), 2);
    }

    #[test]
    fn the_two_halves_are_not_mistaken_for_a_cascade() {
        // `cascade_conflicts` holds back a pipeline that reads a table another *upserts*
        // into. The ingest half only appends, which is exactly why the stage is append-only
        // — so the apply half reading it must pass cleanly.
        let r = Config::from_toml_str(STAGED)
            .unwrap()
            .resolve_all()
            .unwrap();
        assert!(r.rejected.is_empty(), "{:?}", r.rejected);
    }

    #[test]
    fn a_hand_written_stage_uri_must_still_look_like_a_stage() {
        // The suffix is what every other pipeline uses to recognise one; without it the
        // privacy guard above has nothing to go on.
        let toml = STAGED.replace(
            "apply_max_bytes = \"512MB\"",
            "stage_uri = \"/tmp/silver/somewhere_else\"",
        );
        let e = Config::from_toml_str(&toml)
            .unwrap()
            .resolve()
            .unwrap_err()
            .to_string();
        assert!(e.contains("__ddi_stage"), "names the rule: {e}");
    }

    #[test]
    fn direct_upsert_is_untouched_by_any_of_this() {
        let r = Config::from_toml_str(UPSERT).unwrap().resolve().unwrap();
        assert_eq!(r.len(), 1, "an upsert pipeline is still one pipeline");
        assert_eq!(r[0].name, "orders");
        assert_eq!(r[0].app_id, "ddi.orders");
        assert_eq!(r[0].write_mode, WriteMode::Upsert);
        assert!(r[0].stage_for.is_none());
    }

    /// A config where the second of three pipelines cannot possibly work.
    fn one_bad_of_three() -> Config {
        let toml = r#"
[[pipeline]]
name = "good_a"
app_id = "ddi.good_a"
source_uri = "/tmp/bronze/a"
target_uri = "/tmp/silver/a"

[[pipeline]]
name = "typo"
app_id = "ddi.typo"
source_uri = "/tmp/bronze/b"
target_uri = "/tmp/silver/b"
transform_sql = "SELECT customer_id, sum(total) FROM source GROUP BY customer_id"

[[pipeline]]
name = "good_b"
app_id = "ddi.good_b"
source_uri = "/tmp/bronze/c"
target_uri = "/tmp/silver/c"
"#;
        Config::from_toml_str(toml).unwrap()
    }

    #[test]
    fn one_unusable_pipeline_does_not_keep_the_others_off_the_air() {
        // The whole point. A typo in one entry of a large config used to abort the process,
        // which puts an editing mistake and a fleet outage one keystroke apart.
        let r = one_bad_of_three().resolve_all().unwrap();

        assert_eq!(
            r.pipelines
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            vec!["good_a", "good_b"],
            "the pipelines that can run, run"
        );
        assert_eq!(r.rejected.len(), 1);
        assert_eq!(r.rejected[0].name, "typo");
        assert!(
            r.rejected[0].reason.contains("GROUP BY"),
            "and the one that cannot is named with the reason: {}",
            r.rejected[0].reason
        );
    }

    #[test]
    fn the_strict_form_still_refuses_the_whole_config() {
        // `ddi validate` is a gate, so it must still fail — and now it names every fault
        // rather than only the first.
        let e = one_bad_of_three().resolve().unwrap_err().to_string();
        assert!(e.contains("typo"), "names the pipeline: {e}");
        assert!(e.contains("GROUP BY"), "and the reason: {e}");
    }

    #[test]
    fn the_strict_form_lists_every_fault_not_just_the_first() {
        let toml = r#"
[[pipeline]]
name = "a"
app_id = "ddi.a"
source_uri = "/tmp/same"
target_uri = "/tmp/same"

[[pipeline]]
name = "b"
app_id = "ddi.b"
source_uri = "/tmp/x"
target_uri = "/tmp/y"
upsert_lookback = "whenever"
"#;
        let e = Config::from_toml_str(toml)
            .unwrap()
            .resolve()
            .unwrap_err()
            .to_string();
        assert!(e.contains("\"a\""), "first fault: {e}");
        assert!(e.contains("\"b\""), "second fault too: {e}");
    }

    #[test]
    fn both_sides_of_a_duplicate_app_id_are_held_back() {
        // Neither is innocent: sharing an offset key corrupts every sharer's resume point,
        // so keeping one of them running would be the dangerous half of the choice.
        let toml = r#"
[[pipeline]]
name = "a"
app_id = "ddi.shared"
source_uri = "/tmp/bronze/a"
target_uri = "/tmp/silver/a"

[[pipeline]]
name = "b"
app_id = "ddi.shared"
source_uri = "/tmp/bronze/b"
target_uri = "/tmp/silver/b"

[[pipeline]]
name = "c"
app_id = "ddi.c"
source_uri = "/tmp/bronze/c"
target_uri = "/tmp/silver/c"
"#;
        let r = Config::from_toml_str(toml).unwrap().resolve_all().unwrap();
        assert_eq!(
            r.pipelines
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c"]
        );
        assert_eq!(r.rejected.len(), 2, "both sharers: {:?}", r.rejected);
        assert!(r.rejected.iter().all(|x| x.reason.contains("app_id")));
    }

    #[test]
    fn a_broken_cascade_holds_back_the_reader_not_the_writer() {
        // The upserting pipeline is correct on its own; it is the one that cannot read what
        // it produces that has to stand down.
        let toml = format!(
            "{UPSERT}\n[[pipeline]]\nname = \"gold\"\napp_id = \"ddi.gold\"\n\
             source_uri = \"/tmp/silver/orders\"\ntarget_uri = \"/tmp/gold/orders\"\n\
             change_policy = \"skip_change_commits\"\n"
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve_all().unwrap();
        assert_eq!(
            r.pipelines
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>(),
            vec!["orders"],
            "the upserting pipeline keeps running"
        );
        assert_eq!(r.rejected.len(), 1);
        assert_eq!(r.rejected[0].name, "gold");
        assert!(r.rejected[0].reason.contains("silently drop"));
    }

    #[test]
    fn a_config_where_nothing_can_run_still_resolves_and_says_so() {
        // Not an error at this level: the caller decides what "nothing to run" means.
        // `ddi run` treats it as fatal, `ddi validate` reports it.
        let toml = r#"
[[pipeline]]
name = "a"
app_id = "ddi.a"
source_uri = "/tmp/same"
target_uri = "/tmp/same"
"#;
        let r = Config::from_toml_str(toml).unwrap().resolve_all().unwrap();
        assert!(r.is_empty());
        assert_eq!(r.rejected.len(), 1);
    }

    #[test]
    fn append_is_the_default_write_mode() {
        let r = Config::from_toml_str(BASE).unwrap().resolve().unwrap();
        assert_eq!(r[0].write_mode, WriteMode::Append);
        assert!(!r[0].write_mode.is_upsert());
    }

    #[test]
    fn upsert_falls_back_to_dedup_key_for_its_identity() {
        let r = Config::from_toml_str(UPSERT).unwrap().resolve().unwrap();
        assert_eq!(r[0].write_mode, WriteMode::Upsert);
        assert_eq!(r[0].upsert_key.as_deref(), Some("order_id"));
    }

    #[test]
    fn upsert_without_a_key_is_rejected_at_load() {
        let toml = UPSERT.replace("dedup_key = \"order_id\"\n", "");
        let e = Config::from_toml_str(&toml).unwrap().resolve().unwrap_err();
        assert!(e.to_string().contains("upsert_key"), "got: {e}");
    }

    #[test]
    fn upsert_without_a_sequence_column_is_rejected_at_load() {
        let toml = UPSERT.replace("dedup_timestamp = \"_timestamp\"\n", "");
        let e = Config::from_toml_str(&toml).unwrap().resolve().unwrap_err();
        assert!(e.to_string().contains("dedup_timestamp"), "got: {e}");
    }

    #[test]
    fn upsert_settings_on_an_append_pipeline_are_rejected_rather_than_ignored() {
        // A knob that silently does nothing is how someone ends up believing their target
        // is deduplicated when it is not.
        let toml = format!("{BASE}upsert_lookback = \"48h\"\n");
        let e = Config::from_toml_str(&toml).unwrap().resolve().unwrap_err();
        assert!(e.to_string().contains("write_mode"), "got: {e}");
    }

    #[test]
    fn a_lookback_that_is_not_a_duration_or_a_number_is_rejected_at_load() {
        let toml = format!("{UPSERT}upsert_lookback = \"whenever\"\n");
        let e = Config::from_toml_str(&toml).unwrap().resolve().unwrap_err();
        assert!(e.to_string().contains("orders"), "names the pipeline: {e}");
        assert!(e.to_string().contains("48h"), "shows the spelling: {e}");
    }

    #[test]
    fn feeding_an_upsert_target_to_an_appending_pipeline_is_rejected() {
        // The cascade trap. An upsert commit reads downstream as a change commit, and
        // `skip_change_commits` — the setting that looks safest — drops the whole commit
        // including the keys it inserted.
        let toml = format!(
            "{UPSERT}\n[[pipeline]]\nname = \"gold\"\napp_id = \"ddi.gold\"\n\
             source_uri = \"/tmp/silver/orders\"\ntarget_uri = \"/tmp/gold/orders\"\n\
             change_policy = \"skip_change_commits\"\n"
        );
        let e = Config::from_toml_str(&toml).unwrap().resolve().unwrap_err();
        let e = e.to_string();
        assert!(
            e.contains("orders") && e.contains("gold"),
            "names both: {e}"
        );
        assert!(e.contains("silently drop"), "says what goes wrong: {e}");
        assert!(e.contains("ignore_changes"), "says the fix: {e}");
    }

    #[test]
    fn cascading_upsert_into_upsert_with_ignore_changes_is_allowed() {
        // The one combination that survives: the downstream re-reads rewritten rows and
        // merges them onto the same key, so re-emission is a no-op rather than a duplicate.
        let toml = format!(
            "{UPSERT}\n[[pipeline]]\nname = \"gold\"\napp_id = \"ddi.gold\"\n\
             source_uri = \"/tmp/silver/orders\"\ntarget_uri = \"/tmp/gold/orders\"\n\
             change_policy = \"ignore_changes\"\nwrite_mode = \"upsert\"\n\
             dedup_timestamp = \"_timestamp\"\ndedup_key = \"order_id\"\n"
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve().unwrap();
        assert_eq!(r.len(), 2);
    }

    // ---- Realtime publication ----
    //
    // The property every one of these is about: a publisher is a leaf. Nothing to do with a
    // dashboard may hold back a pipeline that would otherwise stream to Delta.

    const PUBLISH_SINK: &str = r#"
[publish]
type = "webpubsub"
connection_string = "Endpoint=https://x.webpubsub.azure.com;AccessKey=k;Version=1.0;"
hub = "ddi"
"#;

    /// An appending pipeline whose dbt model asked to publish.
    fn publishing_toml(extra: &str) -> String {
        format!(
            "{extra}\n[[pipeline]]\nname = \"orders\"\napp_id = \"ddi.orders\"\n\
             source_uri = \"/tmp/bronze/orders\"\ntarget_uri = \"/tmp/silver/orders\"\n\
             [pipeline.publish]\nmodel = \"orders_live\"\nkind = \"webpubsub\"\n\
             group = \"sales\"\npublish_sql = \"SELECT country, sum(amount) AS d FROM \
             source GROUP BY country\"\n"
        )
    }

    fn only(r: &Resolved) -> &ResolvedPipeline {
        assert_eq!(r.pipelines.len(), 1, "expected exactly one pipeline: {r:?}");
        &r.pipelines[0]
    }

    #[test]
    fn a_publish_model_plus_a_sink_resolves_to_a_publication() {
        let r = Config::from_toml_str(&publishing_toml(PUBLISH_SINK))
            .unwrap()
            .resolve_all()
            .unwrap();
        let p = only(&r);
        let publish = p.publish.as_ref().expect("should publish");
        assert_eq!(publish.group, "sales");
        assert_eq!(publish.model, "orders_live");
        assert!(p.publish_to.is_some(), "and knows where to send it");
        // Normalised on the way through, exactly like transform_sql.
        assert!(publish.publish_sql.contains("GROUP BY"), "{publish:?}");
    }

    #[test]
    fn no_publish_table_means_no_publisher_and_the_pipeline_still_runs() {
        let r = Config::from_toml_str(&publishing_toml(""))
            .unwrap()
            .resolve_all()
            .unwrap();
        let p = only(&r);
        assert!(p.publish.is_none(), "nothing to send it to");
        assert!(p.publish_to.is_none());
        assert!(
            r.rejected.is_empty(),
            "and nothing was held back: {:?}",
            r.rejected
        );
    }

    #[test]
    fn the_no_publish_flag_overrides_a_configured_sink() {
        let mut cfg = Config::from_toml_str(&publishing_toml(PUBLISH_SINK)).unwrap();
        cfg.publish_disabled = true;
        let r = cfg.resolve_all().unwrap();
        assert!(only(&r).publish.is_none());
        assert!(r.rejected.is_empty());
    }

    #[test]
    fn a_malformed_publish_table_is_not_fatal_to_the_fleet() {
        // The failure mode this is written against: one bad line in the deployment config
        // taking down ingestion for every pipeline in the process.
        let sink = "[publish]\ntype = \"webpubsub\"\nhub = \"ddi\"\n";
        let r = Config::from_toml_str(&publishing_toml(sink))
            .unwrap()
            .resolve_all()
            .unwrap();
        let p = only(&r);
        assert!(p.publish.is_none(), "no credential, so nothing publishes");
        assert!(
            r.rejected.is_empty(),
            "but the pipeline runs: {:?}",
            r.rejected
        );
    }

    #[test]
    fn a_publish_sink_needs_exactly_one_credential_form() {
        let both = PublisherConfig {
            kind: PublisherKind::Webpubsub,
            connection_string: Some("Endpoint=https://x;AccessKey=k;".into()),
            connection_string_env: Some("DDI_WPS".into()),
            hub: "ddi".into(),
            message_ttl_secs: 60,
            timeout_secs: 5,
            failure_threshold: 5,
            breaker_cooldown_secs: 30,
            max_message_bytes: "900KB".into(),
        };
        let e = both.check().unwrap_err();
        assert!(e.contains("both"), "got: {e}");
    }

    #[test]
    fn a_message_ttl_outside_the_services_range_is_refused_at_load() {
        let sink = format!("{PUBLISH_SINK}message_ttl_secs = 900\n");
        let r = Config::from_toml_str(&publishing_toml(&sink))
            .unwrap()
            .resolve_all()
            .unwrap();
        assert!(
            only(&r).publish.is_none(),
            "0..=300 is the service's own bound"
        );
        assert!(r.rejected.is_empty());
    }

    #[test]
    fn a_message_cap_above_the_frame_limit_is_refused() {
        let sink = format!("{PUBLISH_SINK}max_message_bytes = \"4MB\"\n");
        let r = Config::from_toml_str(&publishing_toml(&sink))
            .unwrap()
            .resolve_all()
            .unwrap();
        assert!(only(&r).publish.is_none());
    }

    #[test]
    fn an_upsert_pipeline_cannot_publish() {
        let toml = format!(
            "{PUBLISH_SINK}{UPSERT}[pipeline.publish]\nmodel = \"orders_live\"\n\
             kind = \"webpubsub\"\ngroup = \"sales\"\n\
             publish_sql = \"SELECT count(*) AS c FROM source\"\n"
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve_all().unwrap();
        let p = only(&r);
        assert!(
            p.publish.is_none(),
            "a merge does not say what the delta was"
        );
        assert!(
            r.rejected.is_empty(),
            "but it still streams: {:?}",
            r.rejected
        );
    }

    #[test]
    fn publish_problem_refuses_a_staged_upsert_before_it_is_split() {
        // The half of this that `publish_problem` actually owns. Asked of the operator's own
        // entry, which is the only form in which a staged upsert still says it merges — after
        // the split the ingest half claims write_mode = Append. The test below covers the
        // other half of the defence, and would pass even if this check were deleted.
        let mut p: PipelineConfig = toml::from_str(
            "name = \"style\"\napp_id = \"ddi.style\"\nsource_uri = \"/a\"\n\
             target_uri = \"/b\"\nwrite_mode = \"staged_upsert\"\n",
        )
        .unwrap();
        p.publish = Some(PublishModel {
            model: "style_live".into(),
            kind: PublisherKind::Webpubsub,
            group: "style".into(),
            publish_sql: "SELECT count(*) AS c FROM source".into(),
        });
        let reason = publish_problem(&p).expect("a staged upsert must not publish");
        assert!(reason.contains("staged_upsert"), "got: {reason}");
        assert!(reason.contains("double-count"), "says why: {reason}");
    }

    #[test]
    fn neither_half_of_a_staged_upsert_can_publish() {
        // The laundering hazard: `expand_staged` rewrites the ingest half to
        // `write_mode = Append`, so anything asking after the split gets "yes, append-only"
        // about rows going into a private staging table.
        let toml = format!(
            "{PUBLISH_SINK}{STAGED}[pipeline.publish]\nmodel = \"style_live\"\n\
             kind = \"webpubsub\"\ngroup = \"style\"\n\
             publish_sql = \"SELECT count(*) AS c FROM source\"\n"
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve_all().unwrap();
        assert!(!r.pipelines.is_empty(), "the pipeline still runs");
        for p in &r.pipelines {
            assert!(
                p.publish.is_none(),
                "half {:?} must not publish from a staged upsert",
                p.name
            );
        }
    }

    #[test]
    fn a_publish_model_whose_sql_is_invalid_does_not_hold_back_the_pipeline() {
        let toml = format!(
            "{PUBLISH_SINK}\n[[pipeline]]\nname = \"orders\"\napp_id = \"ddi.orders\"\n\
             source_uri = \"/tmp/bronze/orders\"\ntarget_uri = \"/tmp/silver/orders\"\n\
             [pipeline.publish]\nmodel = \"orders_live\"\nkind = \"webpubsub\"\n\
             group = \"sales\"\npublish_sql = \"SELECT avg(amount) AS a FROM source\"\n"
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve_all().unwrap();
        assert!(only(&r).publish.is_none(), "avg is not a delta");
        assert!(
            r.rejected.is_empty(),
            "and the pipeline runs: {:?}",
            r.rejected
        );
    }

    #[test]
    fn a_connection_string_never_appears_in_a_debug_rendering() {
        let cfg = Config::from_toml_str(&publishing_toml(PUBLISH_SINK)).unwrap();
        let rendered = format!("{cfg:?}");
        assert!(
            !rendered.contains("AccessKey=k"),
            "a Config is logged at startup: {rendered}"
        );
        assert!(rendered.contains("redacted"), "got: {rendered}");
    }

    #[test]
    fn two_pipelines_publishing_to_one_group_both_stop_publishing_and_keep_streaming() {
        // One group is one stream to a browser. Two producers on it interleave with nothing
        // in the envelope to tell a client which is which — a correct client ignores the
        // foreign ones, an incorrect one re-baselines forever. Neither is worth shipping, and
        // neither is worth stopping ingestion over.
        let toml = format!(
            "{PUBLISH_SINK}\n[[pipeline]]\nname = \"a\"\napp_id = \"ddi.a\"\n\
             source_uri = \"/tmp/bronze/a\"\ntarget_uri = \"/tmp/silver/a\"\n\
             [pipeline.publish]\nmodel = \"a_live\"\nkind = \"webpubsub\"\ngroup = \"sales\"\n\
             publish_sql = \"SELECT count(*) AS c FROM source\"\n\
             \n[[pipeline]]\nname = \"b\"\napp_id = \"ddi.b\"\n\
             source_uri = \"/tmp/bronze/b\"\ntarget_uri = \"/tmp/silver/b\"\n\
             [pipeline.publish]\nmodel = \"b_live\"\nkind = \"webpubsub\"\ngroup = \"sales\"\n\
             publish_sql = \"SELECT count(*) AS c FROM source\"\n"
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve_all().unwrap();
        assert_eq!(r.pipelines.len(), 2, "both still run: {:?}", r.rejected);
        assert!(
            r.rejected.is_empty(),
            "and neither is held back: {:?}",
            r.rejected
        );
        for p in &r.pipelines {
            assert!(p.publish.is_none(), "{:?} must not publish", p.name);
            assert!(p.publish_to.is_none());
        }
    }

    #[test]
    fn distinct_groups_are_left_alone() {
        let toml = format!(
            "{PUBLISH_SINK}\n[[pipeline]]\nname = \"a\"\napp_id = \"ddi.a\"\n\
             source_uri = \"/tmp/bronze/a\"\ntarget_uri = \"/tmp/silver/a\"\n\
             [pipeline.publish]\nmodel = \"a_live\"\nkind = \"webpubsub\"\ngroup = \"sales\"\n\
             publish_sql = \"SELECT count(*) AS c FROM source\"\n\
             \n[[pipeline]]\nname = \"b\"\napp_id = \"ddi.b\"\n\
             source_uri = \"/tmp/bronze/b\"\ntarget_uri = \"/tmp/silver/b\"\n\
             [pipeline.publish]\nmodel = \"b_live\"\nkind = \"webpubsub\"\ngroup = \"orders\"\n\
             publish_sql = \"SELECT count(*) AS c FROM source\"\n"
        );
        let r = Config::from_toml_str(&toml).unwrap().resolve_all().unwrap();
        assert_eq!(r.pipelines.len(), 2);
        for p in &r.pipelines {
            assert!(p.publish.is_some(), "{:?} should publish", p.name);
        }
    }
}
