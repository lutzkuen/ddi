//! Is this dbt model streamable?
//!
//! The verdict is derived from the compiled SQL, not from the model's name, tags or
//! config. A model is streamable when its transformation is the same kind of thing `ddi`
//! runs anyway: one source relation in, rows out, no memory of other rows.
//!
//! Rejections carry the reason, because "no" is only useful if it says what to change.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use deltalake::datafusion::sql::parser::{DFParser, Statement};
use deltalake::datafusion::sql::sqlparser::ast::{
    Ident, ObjectName, Statement as SqlStatement, VisitMut, VisitorMut,
};

use crate::dbt::{Manifest, Node};
use crate::transform::sql::SOURCE_TABLE;
use crate::transform::validate::validate_sql;

/// A model that can be streamed, with everything needed to build a pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Streamable {
    pub unique_id: String,
    pub name: String,
    /// `schema.table` of the single upstream relation.
    pub source_relation: String,
    /// `schema.table` of the model itself.
    pub target_relation: String,
    /// The compiled SQL rewritten to read `source`. `None` for a straight copy.
    pub transform_sql: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Streamable(Box<Streamable>),
    Rejected { name: String, reason: String },
}

impl Verdict {
    pub fn name(&self) -> &str {
        match self {
            Verdict::Streamable(s) => &s.name,
            Verdict::Rejected { name, .. } => name,
        }
    }

    pub fn is_streamable(&self) -> bool {
        matches!(self, Verdict::Streamable(_))
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
/// A view has no storage to stream into. `ephemeral` is inlined into its consumers and
/// never exists on its own.
const STREAMABLE_MATERIALIZATIONS: &[&str] = &["table", "incremental"];

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

    // Exactly one upstream, because a pipeline reads exactly one source table.
    let upstream: Vec<&Node> = node
        .depends_on
        .nodes
        .iter()
        .filter_map(|id| manifest.node(id))
        .collect();
    if upstream.len() != 1 {
        return reject(
            &name,
            format!(
                "depends on {} upstream relations; ddi streams from exactly one. Denormalise \
                 upstream, or split the model.",
                upstream.len()
            ),
        );
    }
    let source = upstream[0];

    // Rewrite whatever the compiled SQL calls the upstream table to `source`, which is the
    // only relation ddi registers.
    let (rewritten, seen) = match rewrite_to_source(sql) {
        Ok(v) => v,
        Err(e) => return reject(&name, e),
    };

    if seen.len() > 1 {
        let mut names: Vec<&str> = seen.iter().map(|s| s.as_str()).collect();
        names.sort();
        return reject(
            &name,
            format!(
                "the compiled SQL reads {} relations ({}); ddi registers only the source \
                 batch. A lookup against a second table is a v2 feature.",
                seen.len(),
                names.join(", ")
            ),
        );
    }

    // The real gate: the same validator the daemon applies to any transform_sql.
    if let Err(e) = validate_sql(&rewritten) {
        let detail = match e {
            crate::Error::Config(m) => m,
            other => other.to_string(),
        };
        return reject(&name, detail);
    }

    Verdict::Streamable(Box::new(Streamable {
        unique_id: unique_id.to_string(),
        name,
        source_relation: source.qualified(),
        target_relation: node.qualified(),
        transform_sql: Some(rewritten),
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

/// Replaces every table reference with `source`, returning the rewritten SQL and the set
/// of distinct relation names that were replaced.
///
/// Collecting the names is what lets the caller tell a single-source model from a join:
/// after the rewrite they would all read `source`, so the distinction has to be captured
/// on the way through.
fn rewrite_to_source(sql: &str) -> std::result::Result<(String, BTreeSet<String>), String> {
    let mut statements =
        DFParser::parse_sql(sql).map_err(|e| format!("could not parse the compiled SQL: {e}"))?;

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

    let mut v = RelationRewriter::default();
    let _ = inner.visit(&mut v);
    Ok((inner.to_string(), v.seen))
}

#[derive(Default)]
struct RelationRewriter {
    seen: BTreeSet<String>,
}

impl VisitorMut for RelationRewriter {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        // Record the fully-qualified name before flattening it, so a join against two
        // different tables is still visible as two names.
        self.seen.insert(relation.to_string());
        *relation = ObjectName::from(vec![Ident::new(SOURCE_TABLE)]);
        ControlFlow::Continue(())
    }
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
        assert!(reason.contains("reads 2 relations"), "got: {reason}");
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
        let v = verdict("SELECT order_id FROM hive.bronze.orders");
        let Verdict::Streamable(s) = v else {
            panic!("expected streamable");
        };
        let sql = s.transform_sql.unwrap();
        assert!(!sql.contains("hive"), "catalog must not survive: {sql}");
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
