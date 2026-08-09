//! Manifest → pipelines.
//!
//! This is the only place a pipeline is described, and it derives everything from the dbt
//! project: which tables, which SQL, which timestamp, which key. `ddi run` calls it on
//! every start, so there is no generated file to keep in sync and nothing to drift.
//!
//! `ddi dbt convert` renders the same result as TOML, for reading, or for pinning it.

use crate::config::{Config, PipelineConfig, StorageConfig};
use crate::dbt::analyze::{analyze_all, Verdict};
use crate::dbt::{Manifest, Node, UriTemplate};
use crate::dedup::DEFAULT_TIMESTAMP_COLUMN;
use crate::error::{Error, Result};

/// Prefix for generated `app_id`s. The offset key must be stable forever, so it is
/// derived from the model name and nothing that moves (not the schema, not the path).
const APP_ID_PREFIX: &str = "ddi";

pub fn app_id_for(model: &str) -> String {
    format!("{APP_ID_PREFIX}.{model}")
}

/// Where a node's data actually lives.
///
/// dbt usually knows, and when it does it is more authoritative than anything configured
/// here: `location_root` is dbt's own answer, and adapters that write to a path record it
/// on the node. The template is a fallback for warehouses that name relations without
/// locating them.
fn location(node: &Node, template: Option<&UriTemplate>) -> Option<String> {
    node.meta_str("ddi_location")
        .or(node.meta_str("delta_table_path"))
        .or(node.config.delta_path.as_deref())
        .map(str::to_string)
        .or_else(|| {
            node.config
                .location_root
                .as_deref()
                .map(|r| format!("{}/{}", r.trim_end_matches('/'), node.relation()))
        })
        .or_else(|| template.map(|t| t.render(node)))
}

/// Every streamable model in the manifest, as pipelines.
pub fn pipelines(manifest: &Manifest, storage: &StorageConfig) -> Result<Vec<PipelineConfig>> {
    let template = storage.uri_template.as_deref().map(UriTemplate::new);
    let mut out = Vec::new();

    for v in analyze_all(manifest) {
        let Verdict::Streamable(s) = v else { continue };

        let target_node = manifest.node(&s.unique_id);
        let source_node = target_node
            .and_then(|n| n.depends_on.nodes.first())
            .and_then(|id| manifest.node(id));
        let (Some(src), Some(tgt)) = (source_node, target_node) else {
            return Err(Error::Config(format!(
                "model {:?}: could not resolve its relations from the manifest",
                s.name
            )));
        };

        let (Some(source_uri), Some(target_uri)) = (
            location(src, template.as_ref()),
            location(tgt, template.as_ref()),
        ) else {
            return Err(Error::Config(format!(
                "model {:?}: the manifest does not say where {} or {} live, and no \
                 [storage].uri_template is set. Either give the model a location_root in \
                 dbt, or add a template.",
                s.name,
                src.qualified(),
                tgt.qualified()
            )));
        };

        out.push(PipelineConfig {
            name: s.name.clone(),
            app_id: app_id_for(&s.name),
            source_uri,
            target_uri,
            starting_version: 0,
            change_policy: Default::default(),
            transform_sql: s.transform_sql.clone(),
            allowed_latency_secs: None,
            max_bytes_per_batch: None,
            max_files_per_batch: None,
            max_output_rows_per_batch: None,
            target_file_size: None,
            watermark_uri: storage.watermark_uri.clone(),
            // The handover, declared in dbt next to the model.
            dedup_timestamp: Some(
                tgt.meta_str("ddi_timestamp")
                    .unwrap_or(DEFAULT_TIMESTAMP_COLUMN)
                    .to_string(),
            ),
            dedup_key: tgt.meta_str("ddi_key").map(str::to_string),
            // Carried so a running pipeline can re-ask the catalog where these live.
            source_relation: src.fully_qualified(),
            target_relation: tgt.fully_qualified(),
        });
    }
    Ok(out)
}

/// The same derivation, rendered as TOML for a human to read.
pub fn to_toml(manifest: &Manifest, cfg: &Config) -> Result<String> {
    let derived = pipelines(manifest, &cfg.storage)?;

    let mut out = String::new();
    out.push_str(
        "# Derived from the dbt manifest by `ddi dbt convert`.\n\
         #\n\
         # `ddi run` derives exactly this on every start, so nothing here needs keeping\n\
         # in sync. It is written out only for reading, or for pinning.\n\n",
    );

    for p in &derived {
        out.push_str("[[pipeline]]\n");
        out.push_str(&format!("name       = {:?}\n", p.name));
        out.push_str(&format!("app_id     = {:?}\n", p.app_id));
        out.push_str(&format!("source_uri = {:?}\n", p.source_uri));
        out.push_str(&format!("target_uri = {:?}\n", p.target_uri));
        if let Some(ts) = &p.dedup_timestamp {
            out.push_str(&format!("dedup_timestamp = {ts:?}\n"));
        }
        if let Some(k) = &p.dedup_key {
            out.push_str(&format!("dedup_key       = {k:?}\n"));
        }
        if let Some(w) = &p.watermark_uri {
            out.push_str(&format!("watermark_uri   = {w:?}\n"));
        }
        if let Some(sql) = &p.transform_sql {
            out.push_str(&format!("transform_sql = \"\"\"\n{sql}\n\"\"\"\n"));
        }
        out.push('\n');
    }

    if derived.is_empty() {
        out.push_str("# No streamable models found.\n\n");
    }

    let rejected: Vec<Verdict> = analyze_all(manifest)
        .into_iter()
        .filter(|v| !v.is_streamable())
        .collect();
    if !rejected.is_empty() {
        out.push_str("# ---------------------------------------------------------------\n");
        out.push_str("# Not streamable:\n");
        for v in rejected {
            if let Verdict::Rejected { name, reason } = v {
                out.push_str(&format!("#   {name}: {reason}\n"));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbt::{DependsOn, NodeConfig};
    use std::collections::HashMap;

    fn fixture() -> Manifest {
        let mut nodes = HashMap::new();
        nodes.insert(
            "model.p.orders_stg".to_string(),
            Node {
                name: "orders_stg".into(),
                resource_type: "model".into(),
                schema: "silver".into(),
                compiled_code: Some(
                    "SELECT order_id, json_extract_scalar(data, '$.status') AS status, \
                     _timestamp FROM bronze.orders_raw"
                        .into(),
                ),
                depends_on: DependsOn {
                    nodes: vec!["source.p.bronze.orders_raw".into()],
                },
                config: NodeConfig {
                    materialized: Some("table".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        nodes.insert(
            "model.p.daily_totals".to_string(),
            Node {
                name: "daily_totals".into(),
                resource_type: "model".into(),
                schema: "gold".into(),
                compiled_code: Some(
                    "SELECT customer_id, sum(amount) AS t FROM bronze.orders_raw \
                     GROUP BY customer_id"
                        .into(),
                ),
                depends_on: DependsOn {
                    nodes: vec!["source.p.bronze.orders_raw".into()],
                },
                config: NodeConfig {
                    materialized: Some("table".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let mut sources = HashMap::new();
        sources.insert(
            "source.p.bronze.orders_raw".to_string(),
            Node {
                name: "orders_raw".into(),
                resource_type: "source".into(),
                schema: "bronze".into(),
                ..Default::default()
            },
        );
        Manifest { nodes, sources }
    }

    fn storage() -> StorageConfig {
        StorageConfig {
            uri_template: Some("s3://lake/{schema}/{name}".into()),
            ..Default::default()
        }
    }

    #[test]
    fn a_streamable_model_becomes_a_complete_pipeline() {
        let ps = pipelines(&fixture(), &storage()).unwrap();
        assert_eq!(ps.len(), 1, "only the streamable model");
        let p = &ps[0];
        assert_eq!(p.name, "orders_stg");
        assert_eq!(p.app_id, "ddi.orders_stg");
        assert_eq!(p.source_uri, "s3://lake/bronze/orders_raw");
        assert_eq!(p.target_uri, "s3://lake/silver/orders_stg");
        assert_eq!(
            p.dedup_timestamp.as_deref(),
            Some("_timestamp"),
            "the convention applies without anyone configuring it"
        );
    }

    #[test]
    fn dbt_declared_locations_beat_the_template() {
        // dbt's own answer to "where does this live" is more authoritative than ours.
        let mut m = fixture();
        m.nodes
            .get_mut("model.p.orders_stg")
            .unwrap()
            .config
            .delta_path = Some("abfss://real/silver/orders_stg".into());
        m.sources
            .get_mut("source.p.bronze.orders_raw")
            .unwrap()
            .meta
            .insert(
                "delta_table_path".into(),
                serde_json::Value::String("abfss://real/bronze/orders_raw".into()),
            );

        let p = &pipelines(&m, &storage()).unwrap()[0];
        assert_eq!(p.source_uri, "abfss://real/bronze/orders_raw");
        assert_eq!(p.target_uri, "abfss://real/silver/orders_stg");
    }

    #[test]
    fn meta_overrides_the_timestamp_and_supplies_the_key() {
        let mut m = fixture();
        let n = m.nodes.get_mut("model.p.orders_stg").unwrap();
        n.meta.insert(
            "ddi_timestamp".into(),
            serde_json::Value::String("event_ts".into()),
        );
        n.meta.insert(
            "ddi_key".into(),
            serde_json::Value::String("order_id".into()),
        );
        let p = &pipelines(&m, &storage()).unwrap()[0];
        assert_eq!(p.dedup_timestamp.as_deref(), Some("event_ts"));
        assert_eq!(p.dedup_key.as_deref(), Some("order_id"));
    }

    #[test]
    fn nowhere_to_put_the_table_says_so() {
        let e = pipelines(&fixture(), &StorageConfig::default()).unwrap_err();
        assert!(e.to_string().contains("uri_template"), "got: {e}");
        assert!(e.to_string().contains("location_root"), "got: {e}");
    }

    #[test]
    fn the_rendered_toml_round_trips() {
        let cfg = Config {
            storage: storage(),
            ..Default::default()
        };
        let toml = to_toml(&fixture(), &cfg).unwrap();
        let r = Config::from_toml_str(&toml)
            .unwrap_or_else(|e| panic!("does not parse: {e}\n\n{toml}"))
            .resolve()
            .unwrap_or_else(|e| panic!("does not resolve: {e}\n\n{toml}"));
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].dedup_timestamp.as_deref(), Some("_timestamp"));
        assert!(
            toml.contains("#   daily_totals:"),
            "rejections listed: {toml}"
        );
        assert!(toml.contains("GROUP BY"), "with the reason: {toml}");
    }
}
