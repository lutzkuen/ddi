//! dbt integration: decide which models can be streamed, and convert them.
//!
//! The premise is a two-speed lakehouse. dbt rebuilds a model nightly and owns
//! correctness; `ddi` streams the same transformation continuously in between and owns
//! latency. Because dbt periodically overwrites the target, the two have to agree on a
//! handover point — see [`watermark`].
//!
//! Everything here reads `target/manifest.json` and nothing else. No warehouse
//! connection, no adapter-specific code: the manifest is the same shape for dbt-trino,
//! dbt-databricks, dbt-spark and the rest, and storage locations come from a URI template
//! in `ddi`'s own config. That keeps this agnostic, which is the point — the analysis is
//! about the *shape of the SQL*, not about who executes it.

pub mod analyze;
pub mod convert;
pub mod profiles;
pub mod watermark;

use std::collections::HashMap;

use serde::Deserialize;

use crate::error::{Error, Result};

/// The subset of `manifest.json` this tool needs.
///
/// Deliberately tolerant: dbt adds fields every minor release, and a manifest that
/// carries something we do not model should still parse.
#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub nodes: HashMap<String, Node>,
    #[serde(default)]
    pub sources: HashMap<String, Node>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Node {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub resource_type: String,
    #[serde(default)]
    pub schema: String,
    #[serde(default)]
    pub database: Option<String>,
    /// Models may be aliased; sources carry `identifier`. Either overrides `name`.
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub identifier: Option<String>,
    /// Present only after `dbt compile` / `dbt run`. Its absence is a useful diagnostic.
    #[serde(default)]
    pub compiled_code: Option<String>,
    #[serde(default)]
    pub depends_on: DependsOn,
    #[serde(default)]
    pub config: NodeConfig,
    /// dbt surfaces model `meta:` both at the top level and under `config`, depending on
    /// where it was declared. Both are consulted.
    #[serde(default)]
    pub meta: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DependsOn {
    #[serde(default)]
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct NodeConfig {
    #[serde(default)]
    pub materialized: Option<String>,
    /// dbt-spark / dbt-trino external-table root, when the project sets one.
    #[serde(default)]
    pub location_root: Option<String>,
    /// Explicit path, as adapters that write straight to storage record it.
    #[serde(default)]
    pub delta_path: Option<String>,
    #[serde(default)]
    pub meta: HashMap<String, serde_json::Value>,
}

impl Node {
    /// The physical table name: `alias` for models, `identifier` for sources, else `name`.
    pub fn relation(&self) -> &str {
        self.alias
            .as_deref()
            .or(self.identifier.as_deref())
            .unwrap_or(&self.name)
    }

    /// `schema.table`, which is how the compiled SQL will refer to it.
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.relation())
    }

    /// `catalog.schema.table`, the name a catalog query needs. `None` when the manifest
    /// records no database, in which case there is nothing to ask a catalog about.
    pub fn fully_qualified(&self) -> Option<String> {
        self.database
            .as_deref()
            .filter(|d| !d.is_empty())
            .map(|d| format!("{d}.{}.{}", self.schema, self.relation()))
    }

    pub fn materialized(&self) -> &str {
        self.config.materialized.as_deref().unwrap_or("view")
    }

    /// A `meta:` value, from wherever dbt put it.
    ///
    /// This is how a project says which column carries its timestamp without ddi having
    /// to be configured separately for every model:
    ///
    /// ```yaml
    /// models:
    ///   - name: orders_stg
    ///     meta:
    ///       ddi_timestamp: event_ts
    ///       ddi_key: order_id
    /// ```
    pub fn meta_value(&self, key: &str) -> Option<&serde_json::Value> {
        self.meta.get(key).or_else(|| self.config.meta.get(key))
    }

    pub fn meta_str(&self, key: &str) -> Option<&str> {
        self.meta_value(key).and_then(|v| v.as_str())
    }
}

impl Manifest {
    pub fn from_path(p: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(p).map_err(|e| {
            Error::Config(format!(
                "cannot read dbt manifest {}: {e}. Run `dbt compile` to produce it.",
                p.display()
            ))
        })?;
        Self::from_json(&text)
    }

    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s)
            .map_err(|e| Error::Config(format!("could not parse dbt manifest: {e}")))
    }

    /// Any node, model or source, by unique id.
    pub fn node(&self, id: &str) -> Option<&Node> {
        self.nodes.get(id).or_else(|| self.sources.get(id))
    }

    /// Model unique ids, sorted so output is stable across runs.
    pub fn model_ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.resource_type == "model")
            .map(|(id, _)| id.clone())
            .collect();
        v.sort();
        v
    }

    /// Look up a model by its short name (what the user types for `--select`).
    pub fn model_by_name(&self, name: &str) -> Option<(&str, &Node)> {
        self.nodes
            .iter()
            .find(|(_, n)| n.resource_type == "model" && n.name == name)
            .map(|(id, n)| (id.as_str(), n))
    }
}

/// Expands `{database}` / `{schema}` / `{name}` into a storage URI.
///
/// The manifest names a relation, not a location, and resolving one to the other is the
/// single adapter-specific step in this whole module. A template keeps it declarative and
/// offline; `location_root` on the model wins when the project sets one, because that is
/// dbt's own answer to the same question.
#[derive(Debug, Clone)]
pub struct UriTemplate(String);

impl UriTemplate {
    pub fn new(t: impl Into<String>) -> Self {
        Self(t.into())
    }

    pub fn render(&self, node: &Node) -> String {
        let uri = self
            .0
            .replace("{database}", node.database.as_deref().unwrap_or(""))
            .replace("{schema}", &node.schema)
            .replace("{name}", node.relation());

        match &node.config.location_root {
            Some(root) => format!("{}/{}", root.trim_end_matches('/'), node.relation()),
            None => uri,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relation_prefers_alias_then_identifier_then_name() {
        let mut n = Node {
            name: "orders".into(),
            ..Default::default()
        };
        assert_eq!(n.relation(), "orders");
        n.identifier = Some("orders_raw".into());
        assert_eq!(n.relation(), "orders_raw", "sources carry identifier");
        n.alias = Some("orders_v2".into());
        assert_eq!(n.relation(), "orders_v2", "an alias wins");
    }

    #[test]
    fn template_expands_schema_and_name() {
        let t = UriTemplate::new("abfss://lake@acct/{schema}/{name}");
        let n = Node {
            name: "orders".into(),
            schema: "silver".into(),
            ..Default::default()
        };
        assert_eq!(t.render(&n), "abfss://lake@acct/silver/orders");
    }

    #[test]
    fn location_root_on_the_model_beats_the_template() {
        // dbt's own answer to "where does this table live" is more authoritative than
        // ours, so it wins when the project sets it.
        let t = UriTemplate::new("abfss://lake@acct/{schema}/{name}");
        let n = Node {
            name: "orders".into(),
            schema: "silver".into(),
            config: NodeConfig {
                location_root: Some("s3://other/silver/".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(t.render(&n), "s3://other/silver/orders");
    }

    #[test]
    fn an_unknown_manifest_field_does_not_break_parsing() {
        // dbt adds fields every minor release.
        let m = Manifest::from_json(
            r#"{"nodes":{"model.p.a":{"name":"a","resource_type":"model",
                "schema":"s","brand_new_field":42}},"sources":{},"extra":true}"#,
        )
        .unwrap();
        assert_eq!(m.model_ids(), vec!["model.p.a"]);
    }
}
