//! Configuration: N pipelines in one process.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::source::{ChangePolicy, Version};
use crate::transform::validate::validate_sql;

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
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            allowed_latency_secs: default_allowed_latency(),
            max_bytes_per_batch: default_max_bytes(),
            target_file_size: default_target_file_size(),
            max_files_per_batch: default_max_files(),
            max_output_rows_per_batch: default_max_output_rows(),
        }
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
    /// How to reach object storage. The one thing a dbt project cannot tell us.
    pub storage: crate::storage::Storage,
    /// Fully qualified catalog name of the source, when there is a catalog to ask.
    pub source_relation: Option<String>,
    /// Fully qualified catalog name of the target.
    pub target_relation: Option<String>,
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

fn parse_size(s: &str, field: &str) -> Result<u64> {
    s.parse::<bytesize::ByteSize>()
        .map(|b| b.as_u64())
        .map_err(|e| Error::Config(format!("{field}: cannot parse size {s:?}: {e}")))
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Self> {
        toml::from_str(s).map_err(|e| Error::Config(format!("invalid config: {e}")))
    }

    pub fn from_path(p: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(p)
            .map_err(|e| Error::Config(format!("cannot read {}: {e}", p.display())))?;
        Self::from_toml_str(&text)
    }

    /// Fold in defaults, parse sizes, and validate every invariant that can be checked
    /// without touching storage.
    ///
    /// A pipeline that cannot be correct must fail here, at load, not on its first batch.
    pub fn resolve(&self) -> Result<Vec<ResolvedPipeline>> {
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

        // app_id uniqueness: duplicates silently corrupt offsets, because two pipelines
        // would read and overwrite the same txn key on (possibly) different targets.
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for p in pipelines {
            if p.app_id.trim().is_empty() {
                return Err(Error::Config(format!(
                    "pipeline {:?}: app_id must not be empty — it is the offset key",
                    p.name
                )));
            }
            if let Some(prev) = seen.insert(&p.app_id, &p.name) {
                return Err(Error::Config(format!(
                    "duplicate app_id {:?} used by pipelines {:?} and {:?}. app_id is the \
                     offset key; sharing one silently corrupts both pipelines' resume points.",
                    p.app_id, prev, p.name
                )));
            }
        }

        let mut names = HashMap::new();
        for p in pipelines {
            if names.insert(&p.name, ()).is_some() {
                return Err(Error::Config(format!(
                    "duplicate pipeline name {:?}",
                    p.name
                )));
            }
        }

        let d = &self.runtime;
        pipelines
            .iter()
            .map(|p| {
                if let Some(sql) = &p.transform_sql {
                    // Unwrap the inner Config message so the prefix is not doubled.
                    validate_sql(sql).map_err(|e| {
                        let detail = match e {
                            Error::Config(m) => m,
                            other => other.to_string(),
                        };
                        Error::Config(format!("pipeline {:?}: {detail}", p.name))
                    })?;
                }

                if p.source_uri == p.target_uri {
                    return Err(Error::Config(format!(
                        "pipeline {:?}: source_uri and target_uri are the same table; that \
                         would feed the pipeline its own output",
                        p.name
                    )));
                }

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
                    transform_sql: p.transform_sql.clone(),
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
                    storage: crate::storage::Storage::new(self.storage.options.clone()),
                    source_relation: p.source_relation.clone(),
                    target_relation: p.target_relation.clone(),
                })
            })
            .collect()
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
}
