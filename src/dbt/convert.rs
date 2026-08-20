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
use crate::lookup::LookupConfig;

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
        let source_node = manifest.node(&s.source_unique_id);
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

        let mut lookups = Vec::with_capacity(s.lookups.len());
        for lookup in &s.lookups {
            let Some(node) = manifest.node(&lookup.unique_id) else {
                return Err(Error::Config(format!(
                    "model {:?}: lookup {:?} disappeared from the manifest",
                    s.name, lookup.name
                )));
            };
            let Some(uri) = location(node, template.as_ref()) else {
                return Err(Error::Config(format!(
                    "model {:?}: the manifest does not say where lookup {} lives, and no \
                     [storage].uri_template is set. Give the source a Delta location or add \
                     a template.",
                    s.name,
                    node.qualified()
                )));
            };
            lookups.push(LookupConfig {
                name: lookup.name.clone(),
                uri,
                relation: node.fully_qualified(),
                pre_history_version: lookup.pre_history_version,
                table_id_change_policy: lookup.table_id_change_policy,
            });
        }

        out.push(PipelineConfig {
            name: s.name.clone(),
            app_id: app_id_for(&s.name),
            source_uri,
            target_uri,
            lookups,
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
            // Upserting is declared next to the model too, for the same reason the
            // timestamp is: the grain of a table is a property of the model, not of how
            // this daemon happens to be deployed.
            write_mode: match tgt.meta_str("ddi_write_mode") {
                Some("upsert") => crate::config::WriteMode::Upsert,
                Some("staged_upsert") => crate::config::WriteMode::StagedUpsert,
                _ => crate::config::WriteMode::Append,
            },
            // Derived from the target, and deliberately not declarable in dbt: the stage is
            // this tool's own working table, not part of the model the warehouse shares.
            stage_uri: None,
            // Set by the expansion, never by the manifest.
            stage_for: None,
            apply_max_bytes: tgt.meta_str("ddi_apply_max_bytes").map(str::to_string),
            apply_max_latency_secs: tgt
                .meta_value("ddi_apply_max_latency_secs")
                .and_then(|v| v.as_u64()),
            upsert_key: tgt.meta_str("ddi_upsert_key").map(str::to_string),
            // `meta: {ddi_tiebreak: [kafka_partition, kafka_offset]}`. A single string is
            // accepted too, because one tie-breaker is the common case and writing it as a
            // list of one is the kind of thing nobody remembers to do.
            upsert_tiebreak: match tgt.meta_value("ddi_tiebreak") {
                Some(serde_json::Value::Array(a)) => a
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
                Some(serde_json::Value::String(one)) => vec![one.clone()],
                _ => Vec::new(),
            },
            // Defaults to <target>__ddi_dq; declared only when it lives somewhere else.
            dq_uri: tgt.meta_str("ddi_dq").map(str::to_string),
            upsert_lookback: tgt.meta_str("ddi_upsert_lookback").map(str::to_string),
            // Carried so a running pipeline can re-ask the catalog where these live.
            source_relation: src.fully_qualified(),
            target_relation: tgt.fully_qualified(),
            // Filled in from the `ddi_publish` models in a second pass over the same
            // verdicts; nothing publishes until that lands.
            publish: None,
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

    // Serialised through serde rather than written field by field, and that is the whole
    // point: a hand-written emitter silently drops every field added after it, and this one
    // had already fallen behind `write_mode`, `upsert_key`, `upsert_lookback`, `dq_uri`,
    // `change_policy` and both `*_relation`. Pinning a config through this command would
    // then quietly turn an upserting pipeline back into an appending one — and the target it
    // had been keeping to one row per key would start collecting duplicates, which is only
    // discovered later when switching back to upsert fails the grain check.
    //
    // Deriving the output from the struct makes that class of bug impossible: a new field
    // appears here the day it is added. `pinned_is_exactly_what_run_would_derive` holds it
    // to that.
    if !derived.is_empty() {
        #[derive(serde::Serialize)]
        struct Pinned<'a> {
            #[serde(rename = "pipeline")]
            pipeline: &'a [PipelineConfig],
        }
        // Only the pipelines. `[storage]` is deliberately left out — it holds credentials,
        // and this file is meant to be readable in a merge request.
        let rendered = toml::to_string(&Pinned { pipeline: &derived })
            .map_err(|e| Error::Config(format!("cannot render the derived pipelines: {e}")))?;
        out.push_str(&rendered);
        out.push('\n');
    }

    if derived.is_empty() {
        out.push_str("# No streamable models found.\n\n");
    }

    let all = analyze_all(manifest);
    let rejected: Vec<&Verdict> = all
        .iter()
        .filter(|v| matches!(v, Verdict::Rejected { .. }))
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
    let unknown: Vec<&Verdict> = all.iter().filter(|v| v.is_unknown()).collect();
    if !unknown.is_empty() {
        out.push_str("# ---------------------------------------------------------------\n");
        out.push_str("# Could not be judged (this parser did not understand the SQL):\n");
        for v in unknown {
            if let Verdict::Unknown { name, reason } = v {
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

    fn fixture_with_current_lookup_policy() -> Manifest {
        let mut manifest = fixture();
        let model = manifest.nodes.get_mut("model.p.orders_stg").unwrap();
        model.compiled_code = Some(
            "SELECT o.order_id, fx.exchange_rate, o._timestamp \
             FROM bronze.orders_raw AS o \
             LEFT JOIN bronze.exchange_rates AS fx ON fx.currency = o.currency"
                .into(),
        );
        model
            .depends_on
            .nodes
            .push("source.p.bronze.exchange_rates".into());
        manifest.sources.insert(
            "source.p.bronze.exchange_rates".into(),
            Node {
                name: "exchange_rates".into(),
                resource_type: "source".into(),
                schema: "bronze".into(),
                meta: HashMap::from([
                    (
                        "ddi_lookup".into(),
                        serde_json::Value::String("fx_rates".into()),
                    ),
                    (
                        "ddi_lookup_table_id_change_policy".into(),
                        serde_json::Value::String("use_current".into()),
                    ),
                ]),
                ..Default::default()
            },
        );
        manifest
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
    fn dbt_lookup_policy_is_emitted_into_pinned_toml() {
        let manifest = fixture_with_current_lookup_policy();
        let derived = pipelines(&manifest, &storage()).unwrap();
        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].lookups.len(), 1);
        assert_eq!(
            derived[0].lookups[0].table_id_change_policy,
            crate::lookup::LookupTableIdChangePolicy::UseCurrent
        );

        let cfg = Config {
            storage: storage(),
            ..Default::default()
        };
        let toml = to_toml(&manifest, &cfg).unwrap();
        assert!(
            toml.contains("table_id_change_policy = \"use_current\""),
            "the dbt metadata must survive conversion: {toml}"
        );
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

    #[test]
    fn pinned_is_exactly_what_run_would_derive() {
        // The guarantee `ddi dbt convert` has to make: pinning a config and running the
        // manifest directly do the same thing. A hand-written emitter cannot make it — it
        // drops every field added after it was written, and this one had already lost
        // write_mode, upsert_key, upsert_lookback, dq_uri, change_policy and both
        // *_relation. The failure is silent and delayed: an upserting pipeline pinned to
        // TOML comes back as an appending one, and the target quietly starts collecting
        // duplicates.
        //
        // Comparing the whole struct through serde rather than field by field is what keeps
        // this test honest as fields are added.
        let mut manifest = fixture();
        // A database on both ends, so source_relation/target_relation are populated and
        // genuinely take part in the round trip rather than being None either way.
        manifest
            .sources
            .get_mut("source.p.bronze.orders_raw")
            .unwrap()
            .database = Some("hive".into());
        let n = manifest.nodes.get_mut("model.p.orders_stg").unwrap();
        n.database = Some("hive".into());
        for (k, v) in [
            ("ddi_timestamp", "_timestamp"),
            ("ddi_key", "order_id"),
            ("ddi_write_mode", "upsert"),
            ("ddi_upsert_key", "order_id"),
            ("ddi_upsert_lookback", "48h"),
            ("ddi_dq", "s3://lake/quarantine/orders"),
        ] {
            n.meta.insert(k.into(), serde_json::Value::String(v.into()));
        }

        let cfg = Config {
            storage: storage(),
            ..Default::default()
        };
        let derived = pipelines(&manifest, &cfg.storage).unwrap();
        let toml = to_toml(&manifest, &cfg).unwrap();
        let reparsed = Config::from_toml_str(&toml)
            .unwrap_or_else(|e| panic!("does not parse: {e}\n\n{toml}"))
            .pipelines;

        assert_eq!(
            serde_json::to_value(&derived).unwrap(),
            serde_json::to_value(&reparsed).unwrap(),
            "the pinned file must describe the same pipelines `ddi run` would derive.\n\n{toml}"
        );

        // And spelled out, because these are the ones that were being dropped.
        assert!(toml.contains("write_mode = \"upsert\""), "{toml}");
        assert!(toml.contains("upsert_key = \"order_id\""), "{toml}");
        assert!(toml.contains("upsert_lookback = \"48h\""), "{toml}");
        assert!(
            toml.contains("dq_uri = \"s3://lake/quarantine/orders\""),
            "{toml}"
        );
        assert!(toml.contains("change_policy"), "{toml}");
        assert!(toml.contains("target_relation"), "{toml}");
    }

    #[test]
    fn a_pinned_config_resolves_to_the_same_thing_as_the_manifest() {
        // One level up from the last test: not just the same TOML, but the same running
        // behaviour. `write_mode` surviving is what stops a deduplicated target silently
        // reverting to append-only.
        let mut manifest = fixture();
        let n = manifest.nodes.get_mut("model.p.orders_stg").unwrap();
        n.meta.insert(
            "ddi_write_mode".into(),
            serde_json::Value::String("upsert".into()),
        );
        n.meta.insert(
            "ddi_key".into(),
            serde_json::Value::String("order_id".into()),
        );

        let cfg = Config {
            storage: storage(),
            ..Default::default()
        };
        let toml = to_toml(&manifest, &cfg).unwrap();
        let pinned = Config::from_toml_str(&toml).unwrap().resolve().unwrap();

        assert_eq!(pinned.len(), 1);
        assert!(
            pinned[0].write_mode.is_upsert(),
            "a pinned upsert pipeline must still upsert"
        );
        assert_eq!(pinned[0].upsert_key.as_deref(), Some("order_id"));
    }
}
