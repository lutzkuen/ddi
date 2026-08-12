//! Configuration: N pipelines in one process.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::source::{ChangePolicy, Version};
use crate::transform::validate::normalise_sql;

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
}

impl WriteMode {
    pub fn is_upsert(self) -> bool {
        matches!(self, WriteMode::Upsert)
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
        let transform_sql = match &p.transform_sql {
            Some(sql) => Some(normalise_sql(sql).map_err(|e| match e {
                Error::Config(m) => Error::Config(m),
                other => Error::Config(other.to_string()),
            })?),
            None => None,
        };

        if p.source_uri == p.target_uri {
            return Err(Error::Config(
                "source_uri and target_uri are the same table; that would feed the pipeline \
                 its own output"
                    .into(),
            ));
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
        } else if p.upsert_key.is_some() || p.upsert_lookback.is_some() {
            return Err(Error::Config(
                "upsert_key/upsert_lookback are set but write_mode is \"append\", so they do \
                 nothing. Set write_mode = \"upsert\", or remove them."
                    .into(),
            ));
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
