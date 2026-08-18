//! Is this dbt model streamable?
//!
//! The verdict is derived from the compiled SQL, not from the model's name, tags or
//! config. A model is streamable when its transformation is the same kind of thing `ddi`
//! runs anyway: one source relation in, rows out, no memory of other rows.
//!
//! Rejections carry the reason, because "no" is only useful if it says what to change.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use deltalake::datafusion::sql::parser::Statement;
use deltalake::datafusion::sql::sqlparser::ast::{
    Ident, ObjectName, Query, Statement as SqlStatement, TableFactor, VisitMut, VisitorMut,
};

use crate::dbt::{Manifest, Node};
use crate::transform::sql::SOURCE_TABLE;
use crate::transform::validate::validate_sql_with_lookups;

/// A model that can be streamed, with everything needed to build a pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Streamable {
    pub unique_id: String,
    pub name: String,
    /// Manifest id of the one relation whose Delta log advances this pipeline.
    pub source_unique_id: String,
    /// `schema.table` of the single upstream relation.
    pub source_relation: String,
    /// Small, pinned Delta relations available only as LEFT JOIN lookups.
    pub lookups: Vec<StreamableLookup>,
    /// `schema.table` of the model itself.
    pub target_relation: String,
    /// The compiled SQL rewritten to read `source`. `None` for a straight copy.
    pub transform_sql: Option<String>,
}

/// One dbt-declared lookup relation used by a streamable model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamableLookup {
    pub unique_id: String,
    /// The validated SQL alias declared as `meta.ddi_lookup` on the source.
    pub name: String,
    pub relation: String,
    /// Optional explicit lookup snapshot for source commits older than the lookup table.
    pub pre_history_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Streamable(Box<Streamable>),
    /// The model was understood, and it cannot be streamed.
    Rejected {
        name: String,
        reason: String,
    },
    /// The model could not be understood, so nothing is claimed about it either way.
    ///
    /// Almost always SQL this parser does not accept — a warehouse dialect reaches well
    /// beyond what DataFusion's parser covers. Counting these as rejections would
    /// overstate what is known: they are not "cannot stream", they are "cannot tell", and
    /// a project's real streamable count lies somewhere between the two.
    Unknown {
        name: String,
        reason: String,
    },
}

impl Verdict {
    pub fn name(&self) -> &str {
        match self {
            Verdict::Streamable(s) => &s.name,
            Verdict::Rejected { name, .. } | Verdict::Unknown { name, .. } => name,
        }
    }

    pub fn is_streamable(&self) -> bool {
        matches!(self, Verdict::Streamable(_))
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Verdict::Unknown { .. })
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Verdict::Streamable(_) => None,
            Verdict::Rejected { reason, .. } | Verdict::Unknown { reason, .. } => Some(reason),
        }
    }
}

fn unknown(name: &str, reason: impl Into<String>) -> Verdict {
    Verdict::Unknown {
        name: name.to_string(),
        reason: reason.into(),
    }
}

fn reject(name: &str, reason: impl Into<String>) -> Verdict {
    Verdict::Rejected {
        name: name.to_string(),
        reason: reason.into(),
    }
}

/// Materializations that produce a real table `ddi` could append to.
///
/// A view has no storage to stream into, and `ephemeral` is inlined into its consumers
/// and never exists on its own. `external` is included because that is how several
/// adapters (dbt-duckdb, dbt-spark external tables) say "write this to storage" — which
/// is precisely a table `ddi` can append to.
const STREAMABLE_MATERIALIZATIONS: &[&str] = &["table", "incremental", "external"];

/// Decide whether `unique_id` can be streamed.
pub fn analyze(manifest: &Manifest, unique_id: &str) -> Verdict {
    let Some(node) = manifest.node(unique_id) else {
        return reject(unique_id, "no such node in the manifest");
    };
    let name = node.name.clone();

    if node.resource_type != "model" {
        return reject(
            &name,
            format!("not a model (it is a {})", node.resource_type),
        );
    }

    let mat = node.materialized();
    if !STREAMABLE_MATERIALIZATIONS.contains(&mat) {
        return reject(
            &name,
            format!(
                "materialized as {mat:?}; ddi appends to a real table, so the model must be \
                 materialized as one of: {}",
                STREAMABLE_MATERIALIZATIONS.join(", ")
            ),
        );
    }

    let Some(sql) = node.compiled_code.as_deref() else {
        return reject(
            &name,
            "no compiled_code in the manifest — run `dbt compile` (or `dbt run`) so the \
             manifest carries the resolved SQL",
        );
    };

    // Exactly one dependency owns the streaming cursor. A source annotated with
    // `meta: {ddi_lookup: fx_rates}` is different: it is a small Delta table joined as a
    // snapshot and never advances output on its own.
    let mut sources: Vec<(&str, &Node)> = Vec::new();
    let mut lookups: Vec<(&str, &Node, String)> = Vec::new();
    for id in &node.depends_on.nodes {
        let Some(upstream) = manifest.node(id) else {
            return reject(
                &name,
                format!("depends on {id:?}, which is absent from the manifest"),
            );
        };
        match upstream.meta_str("ddi_lookup") {
            Some(lookup_name) => lookups.push((id, upstream, lookup_name.to_string())),
            None => sources.push((id, upstream)),
        }
    }
    if sources.len() != 1 {
        return reject(
            &name,
            format!(
                "depends on {} streaming relations; ddi streams from exactly one. Mark a \
                 small, static Delta source with meta.ddi_lookup when it is a pinned lookup, \
                 or split the model.",
                sources.len()
            ),
        );
    }
    let (source_unique_id, source) = sources[0];

    let mut lookup_names = BTreeSet::new();
    let mut streamable_lookups = Vec::with_capacity(lookups.len());
    for (unique_id, lookup, lookup_name) in lookups {
        if !crate::lookup::valid_name(&lookup_name) {
            return reject(
                &name,
                format!(
                    "lookup source {} declares ddi_lookup={lookup_name:?}; it must be a \
                     lowercase SQL identifier and must not be \"source\"",
                    lookup.qualified()
                ),
            );
        }
        if !lookup_names.insert(lookup_name.clone()) {
            return reject(
                &name,
                format!("declares the lookup name {lookup_name:?} more than once"),
            );
        }
        let pre_history_version = match lookup.meta_value("ddi_lookup_pre_history_version") {
            None => None,
            Some(value) => match value
                .as_u64()
                .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
            {
                Some(version) => Some(version),
                None => {
                    return reject(
                        &name,
                        format!(
                            "lookup source {} declares ddi_lookup_pre_history_version={value}; it must be a non-negative Delta version",
                            lookup.qualified()
                        ),
                    );
                }
            },
        };
        streamable_lookups.push(StreamableLookup {
            unique_id: unique_id.to_string(),
            name: lookup_name,
            relation: lookup.qualified(),
            pre_history_version,
        });
    }

    let mut replacements = BTreeMap::new();
    add_relation_replacements(&mut replacements, source, SOURCE_TABLE);
    for lookup in &streamable_lookups {
        let lookup_node = manifest
            .node(&lookup.unique_id)
            .expect("lookups came from this manifest");
        add_relation_replacements(&mut replacements, lookup_node, &lookup.name);
    }

    // Rewrite the dbt relations into the names the DataFusion session actually registers.
    let rewrite = match rewrite_relations(sql, &replacements) {
        Ok(v) => v,
        // Not a rejection: this parser simply did not understand the SQL, which says
        // nothing about whether the transformation is streamable.
        Err(e) => return unknown(&name, e),
    };

    if !rewrite.unknown.is_empty() {
        return reject(
            &name,
            format!(
                "the compiled SQL reads relation(s) that are not declared as its streaming \
                 source or a ddi_lookup: {}",
                rewrite.unknown.into_iter().collect::<Vec<_>>().join(", ")
            ),
        );
    }

    // The real gate: the same validator the daemon applies to any transform_sql.
    if let Err(e) = validate_sql_with_lookups(&rewrite.sql, &lookup_names) {
        let detail = match e {
            crate::Error::Config(m) => m,
            other => other.to_string(),
        };
        return reject(&name, detail);
    }

    Verdict::Streamable(Box::new(Streamable {
        unique_id: unique_id.to_string(),
        name,
        source_unique_id: source_unique_id.to_string(),
        source_relation: source.qualified(),
        lookups: streamable_lookups,
        target_relation: node.qualified(),
        transform_sql: Some(rewrite.sql),
    }))
}

/// Every model in the manifest, in stable order.
pub fn analyze_all(manifest: &Manifest) -> Vec<Verdict> {
    manifest
        .model_ids()
        .iter()
        .map(|id| analyze(manifest, id))
        .collect()
}

/// A compiled model after physical dbt relation names became session-local aliases.
struct RewrittenSql {
    sql: String,
    unknown: BTreeSet<String>,
}

/// Add both forms dbt may put in compiled SQL: `schema.table` and
/// `catalog.schema.table`.
fn add_relation_replacements(
    replacements: &mut BTreeMap<String, String>,
    node: &Node,
    alias: &str,
) {
    replacements.insert(relation_key(&node.qualified()), alias.to_string());
    if let Some(fully_qualified) = node.fully_qualified() {
        replacements.insert(relation_key(&fully_qualified), alias.to_string());
    }
}

/// Replaces every declared dbt relation with its DataFusion session alias. A relation that
/// did not come from the model's one source or a declared lookup is deliberately retained
/// and reported rather than silently rewritten to `source`.
fn rewrite_relations(
    sql: &str,
    replacements: &BTreeMap<String, String>,
) -> std::result::Result<RewrittenSql, String> {
    // Use the same permissive front-end as the runtime validator. In particular, dbt models
    // use Trino's `ARRAY(JSON)` spelling before the unnest normaliser turns it into the
    // DataFusion form. Treating that model as "unknown" here would make `ddi dbt check`
    // disagree with the daemon that actually runs it.
    let mut statements = crate::transform::validate::parse_permissively(sql)
        .map_err(|e| format!("could not parse the compiled SQL: {e}"))?;

    if statements.len() > 1 {
        return Err(format!(
            "the compiled SQL contains {} statements; a transform is a single SELECT",
            statements.len()
        ));
    }
    let statement = statements
        .pop_front()
        .ok_or_else(|| "the compiled SQL is empty".to_string())?;

    let Statement::Statement(inner) = statement else {
        return Err("the compiled SQL is not a plain SELECT".to_string());
    };
    let mut inner: SqlStatement = *inner;

    let mut v = RelationRewriter {
        replacements: replacements.clone(),
        ..Default::default()
    };
    let _ = inner.visit(&mut v);
    Ok(RewrittenSql {
        sql: inner.to_string(),
        unknown: v.unknown,
    })
}

#[derive(Default)]
struct RelationRewriter {
    replacements: BTreeMap<String, String>,
    unknown: BTreeSet<String>,
    ctes: BTreeSet<String>,
}

impl VisitorMut for RelationRewriter {
    type Break = ();

    /// A query's CTE names are in scope for everything inside it. dbt's own staging
    /// models are written as `with source as (...), renamed as (...) select * from
    /// renamed`, so without this every model would look like it reads three tables.
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.ctes
            .extend(crate::transform::validate::cte_names(query));
        ControlFlow::Continue(())
    }

    /// Resolve names at the table factor rather than at every relation-shaped `ObjectName`,
    /// because only a plain named factor is a dbt relation at all. The permissive front-end
    /// reads Trino's `CROSS JOIN UNNEST(...)` as a table-valued call whose "name" is `UNNEST`;
    /// that is a fan-out of one row's own array, so there is nothing to resolve and nothing
    /// undeclared about it. Every other non-plain factor is left alone for
    /// `validate_sql_with_lookups`, which already rejects it in terms of what it is.
    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        if !crate::transform::validate::is_plain_table_relation(factor) {
            return ControlFlow::Continue(());
        }
        let TableFactor::Table { name, .. } = factor else {
            return ControlFlow::Continue(());
        };
        if crate::transform::validate::is_cte(name, &self.ctes) {
            return ControlFlow::Continue(());
        }
        let key = relation_key(&name.to_string());
        match self.replacements.get(&key) {
            Some(alias) => *name = ObjectName::from(vec![Ident::new(alias)]),
            None => {
                self.unknown.insert(name.to_string());
            }
        }
        ControlFlow::Continue(())
    }
}

fn relation_key(relation: &str) -> String {
    relation
        .split('.')
        .map(|part| part.trim_matches('"').to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbt::{DependsOn, NodeConfig};
    use std::collections::HashMap;

    fn manifest(model_sql: &str, extra_deps: &[&str]) -> Manifest {
        let mut nodes = HashMap::new();
        let mut deps = vec!["source.p.bronze.orders".to_string()];
        deps.extend(extra_deps.iter().map(|s| s.to_string()));

        nodes.insert(
            "model.p.orders_header".to_string(),
            Node {
                name: "orders_header".into(),
                resource_type: "model".into(),
                schema: "silver".into(),
                compiled_code: Some(model_sql.into()),
                depends_on: DependsOn { nodes: deps },
                config: NodeConfig {
                    materialized: Some("table".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        nodes.insert(
            "model.p.other".to_string(),
            Node {
                name: "other".into(),
                resource_type: "model".into(),
                schema: "silver".into(),
                ..Default::default()
            },
        );

        let mut sources = HashMap::new();
        sources.insert(
            "source.p.bronze.orders".to_string(),
            Node {
                name: "orders".into(),
                resource_type: "source".into(),
                schema: "bronze".into(),
                ..Default::default()
            },
        );
        Manifest { nodes, sources }
    }

    fn verdict(sql: &str) -> Verdict {
        analyze(&manifest(sql, &[]), "model.p.orders_header")
    }

    #[test]
    fn a_row_wise_projection_is_streamable() {
        let v = verdict(
            "SELECT order_id, CAST(created_at AS TIMESTAMP) AS created_at FROM bronze.orders",
        );
        let Verdict::Streamable(s) = v else {
            panic!("expected streamable, got {v:?}");
        };
        assert_eq!(s.source_relation, "bronze.orders");
        assert_eq!(s.target_relation, "silver.orders_header");
        assert!(
            s.transform_sql.as_ref().unwrap().contains("source"),
            "the upstream relation must be rewritten to `source`: {:?}",
            s.transform_sql
        );
        assert!(
            !s.transform_sql.as_ref().unwrap().contains("bronze"),
            "no trace of the warehouse name should survive: {:?}",
            s.transform_sql
        );
    }

    #[test]
    fn a_group_by_model_is_rejected_with_the_validators_reason() {
        let v =
            verdict("SELECT customer_id, sum(total) AS t FROM bronze.orders GROUP BY customer_id");
        let Verdict::Rejected { reason, .. } = v else {
            panic!("GROUP BY must not be streamable");
        };
        assert!(reason.contains("GROUP BY"), "got: {reason}");
    }

    #[test]
    fn a_join_to_a_table_the_manifest_never_declared_is_caught_by_the_sql() {
        // A hard-coded relation rather than a `ref()`, so dbt's dependency graph shows one
        // upstream and only the SQL reveals the second.
        let m = manifest(
            "SELECT o.order_id, p.name FROM bronze.orders o JOIN bronze.products p ON o.sku = p.sku",
            &[],
        );
        let Verdict::Rejected { reason, .. } = analyze(&m, "model.p.orders_header") else {
            panic!("a join must not be streamable");
        };
        assert!(reason.contains("not declared"), "got: {reason}");
        assert!(reason.contains("bronze.products"), "name it: {reason}");
    }

    #[test]
    fn a_model_with_two_declared_upstreams_is_rejected_on_the_dependency_graph() {
        let mut m = manifest(
            "SELECT o.order_id, p.name FROM bronze.orders o JOIN bronze.products p ON o.sku = p.sku",
            &["source.p.bronze.products"],
        );
        m.sources.insert(
            "source.p.bronze.products".to_string(),
            Node {
                name: "products".into(),
                resource_type: "source".into(),
                schema: "bronze".into(),
                ..Default::default()
            },
        );
        let Verdict::Rejected { reason, .. } = analyze(&m, "model.p.orders_header") else {
            panic!("two upstreams must not be streamable");
        };
        assert!(reason.contains("exactly one"), "got: {reason}");
    }

    #[test]
    fn a_self_join_is_rejected_even_though_it_has_one_upstream() {
        // Only one dependency, so the dependency count does not catch it — the SQL
        // validator has to.
        let v = verdict(
            "SELECT a.order_id FROM bronze.orders a JOIN bronze.orders b ON a.id = b.parent_id",
        );
        let Verdict::Rejected { reason, .. } = v else {
            panic!("a self-join must not be streamable");
        };
        assert!(reason.to_lowercase().contains("join"), "got: {reason}");
    }

    #[test]
    fn a_window_function_is_rejected() {
        let v = verdict(
            "SELECT order_id, row_number() OVER (PARTITION BY customer_id ORDER BY ts) AS rn \
             FROM bronze.orders",
        );
        assert!(!v.is_streamable());
    }

    #[test]
    fn an_external_model_is_streamable() {
        // How dbt-duckdb and dbt-spark spell "materialize this onto storage".
        let mut m = manifest("SELECT order_id FROM bronze.orders", &[]);
        m.nodes
            .get_mut("model.p.orders_header")
            .unwrap()
            .config
            .materialized = Some("external".into());
        assert!(analyze(&m, "model.p.orders_header").is_streamable());
    }

    #[test]
    fn a_view_is_rejected_because_there_is_nothing_to_append_to() {
        let mut m = manifest("SELECT order_id FROM bronze.orders", &[]);
        m.nodes
            .get_mut("model.p.orders_header")
            .unwrap()
            .config
            .materialized = Some("view".into());
        let Verdict::Rejected { reason, .. } = analyze(&m, "model.p.orders_header") else {
            panic!("a view must not be streamable");
        };
        assert!(reason.contains("view"), "got: {reason}");
    }

    #[test]
    fn an_uncompiled_manifest_says_to_run_dbt_compile() {
        let mut m = manifest("SELECT 1", &[]);
        m.nodes
            .get_mut("model.p.orders_header")
            .unwrap()
            .compiled_code = None;
        let Verdict::Rejected { reason, .. } = analyze(&m, "model.p.orders_header") else {
            panic!("expected rejection");
        };
        assert!(reason.contains("dbt compile"), "got: {reason}");
    }

    #[test]
    fn unnest_and_array_udfs_survive_the_rewrite() {
        // The interesting case: ddi's own SQL features must still validate after the
        // relation has been renamed.
        let v = verdict(
            "SELECT order_id, array_sum(line_items, 'price * qty') AS total FROM bronze.orders",
        );
        assert!(v.is_streamable(), "got {v:?}");
    }

    #[test]
    fn a_fully_qualified_three_part_name_is_rewritten() {
        let mut m = manifest("SELECT order_id FROM hive.bronze.orders", &[]);
        m.sources
            .get_mut("source.p.bronze.orders")
            .unwrap()
            .database = Some("hive".into());
        let v = analyze(&m, "model.p.orders_header");
        let Verdict::Streamable(s) = v else {
            panic!("expected streamable");
        };
        let sql = s.transform_sql.unwrap();
        assert!(!sql.contains("hive"), "catalog must not survive: {sql}");
    }

    #[test]
    fn a_declared_lookup_is_rewritten_and_left_joined() {
        let mut m = manifest(
            "WITH items AS (SELECT order_id, currency FROM bronze.orders) \
             SELECT items.order_id, fx.exchange_rate \
             FROM items LEFT JOIN bronze.exchange_rates AS fx \
             ON fx.currency = items.currency",
            &["source.p.bronze.exchange_rates"],
        );
        m.sources.insert(
            "source.p.bronze.exchange_rates".to_string(),
            Node {
                name: "exchange_rates".into(),
                resource_type: "source".into(),
                schema: "bronze".into(),
                meta: HashMap::from([(
                    "ddi_lookup".into(),
                    serde_json::Value::String("fx".into()),
                )]),
                ..Default::default()
            },
        );

        let Verdict::Streamable(s) = analyze(&m, "model.p.orders_header") else {
            panic!("a declared left lookup join must be streamable");
        };
        assert_eq!(s.lookups.len(), 1);
        assert_eq!(s.lookups[0].name, "fx");
        let sql = s.transform_sql.as_deref().unwrap();
        assert!(sql.contains("LEFT JOIN fx"), "got: {sql}");
        assert!(!sql.contains("bronze.exchange_rates"), "got: {sql}");
    }

    #[test]
    fn a_lookup_model_can_fan_out_trino_json_arrays() {
        let mut m = manifest(
            "SELECT o.order_id, fx.exchange_rate, entry \
             FROM bronze.orders AS o \
             CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.entries') AS ARRAY(JSON))) \
             AS items(entry) \
             LEFT JOIN bronze.exchange_rates AS fx ON fx.currency = o.currency",
            &["source.p.bronze.exchange_rates"],
        );
        m.sources.insert(
            "source.p.bronze.exchange_rates".to_string(),
            Node {
                name: "exchange_rates".into(),
                resource_type: "source".into(),
                schema: "bronze".into(),
                meta: HashMap::from([(
                    "ddi_lookup".into(),
                    serde_json::Value::String("fx".into()),
                )]),
                ..Default::default()
            },
        );

        let Verdict::Streamable(s) = analyze(&m, "model.p.orders_header") else {
            panic!("a fan-out plus declared lookup must be streamable");
        };
        let sql = s.transform_sql.as_deref().unwrap();
        assert!(sql.contains("fx"), "got: {sql}");
        assert!(!sql.contains("bronze.orders"), "got: {sql}");
    }

    #[test]
    fn a_declared_lookup_may_use_a_different_sql_alias() {
        let mut m = manifest(
            "SELECT o.order_id, fx.exchange_rate FROM bronze.orders AS o \
             LEFT JOIN bronze.exchange_rates AS fx ON fx.currency = o.currency",
            &["source.p.bronze.exchange_rates"],
        );
        m.sources.insert(
            "source.p.bronze.exchange_rates".to_string(),
            Node {
                name: "exchange_rates".into(),
                resource_type: "source".into(),
                schema: "bronze".into(),
                meta: HashMap::from([(
                    "ddi_lookup".into(),
                    serde_json::Value::String("fx_rates".into()),
                )]),
                ..Default::default()
            },
        );

        let Verdict::Streamable(s) = analyze(&m, "model.p.orders_header") else {
            panic!("the SQL alias should not have to equal meta.ddi_lookup");
        };
        let sql = s.transform_sql.as_deref().unwrap();
        assert!(sql.contains("LEFT JOIN fx_rates AS fx"), "got: {sql}");
    }

    #[test]
    fn a_lookup_must_use_left_join_on() {
        let mut m = manifest(
            "SELECT o.order_id FROM bronze.orders AS o \
             INNER JOIN bronze.exchange_rates AS fx ON fx.currency = o.currency",
            &["source.p.bronze.exchange_rates"],
        );
        m.sources.insert(
            "source.p.bronze.exchange_rates".to_string(),
            Node {
                name: "exchange_rates".into(),
                resource_type: "source".into(),
                schema: "bronze".into(),
                meta: HashMap::from([(
                    "ddi_lookup".into(),
                    serde_json::Value::String("fx".into()),
                )]),
                ..Default::default()
            },
        );
        let Verdict::Rejected { reason, .. } = analyze(&m, "model.p.orders_header") else {
            panic!("inner joins cannot be lookup joins");
        };
        assert!(reason.contains("LEFT JOIN"), "got: {reason}");
    }

    #[test]
    fn analyze_all_is_stable_and_covers_every_model() {
        let m = manifest("SELECT order_id FROM bronze.orders", &[]);
        let all = analyze_all(&m);
        assert_eq!(all.len(), 2, "both models are classified");
        let names: Vec<&str> = all.iter().map(|v| v.name()).collect();
        assert_eq!(names, vec!["orders_header", "other"]);
    }
}
