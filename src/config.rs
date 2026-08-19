//! Configuration: N pipelines in one process.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::lookup::{LookupConfig, ResolvedLookup};
use crate::source::{ChangePolicy, Version};
use crate::transform::validate::normalise_sql_with_lookups;

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

        let d = &self.runtime;
        let mut ok = Vec::new();
        for p in pipelines {
            if let Some(reason) = condemned.get(p.name.as_str()) {
                reject(&p.name, reason.clone());
                continue;
            }
            match self.resolve_one(p, d) {
                Ok(r) => ok.push(r),
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
}
