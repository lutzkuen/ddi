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
use crate::lookup::LookupTableIdChangePolicy;
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
    /// Whether a replacement or unavailable historical snapshot is a hard error or uses the
    /// lookup's current head.
    pub table_id_change_policy: LookupTableIdChangePolicy,
}

/// A dbt model that says what to push when *another* model's batch commits.
///
/// Never a pipeline of its own: no Delta target, no `txn` action, no offset, and no part in
/// [`crate::dbt::watermark`]'s handover. It is a property of the model it rides on, resolved
/// onto that model's `PipelineConfig` in [`crate::dbt::convert::pipelines`].
///
/// It is also, deliberately, an ordinary dbt model — a view over the streamed one. That is
/// what makes the live path testable by an analyst rather than a second aggregation hidden
/// in Rust: `dbt test` runs against it, and because the aggregation is a monoid over an
/// append-only stream, the same SQL is a delta over one committed batch and the running
/// total over the whole table. Which is exactly the baseline a client reloads after a gap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publication {
    pub unique_id: String,
    pub name: String,
    /// The streamed model whose commits trigger this: its single dependency.
    pub host_unique_id: String,
    pub host_relation: String,
    pub kind: crate::config::PublisherKind,
    /// The channel browsers subscribe to. Defaults to the model's own name.
    pub group: String,
    /// Compiled SQL rewritten to read `source` — at run time, the committed batch.
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Streamable(Box<Streamable>),
    /// A realtime payload for another model, not a table to write.
    Publishes(Box<Publication>),
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
            Verdict::Publishes(p) => &p.name,
            Verdict::Rejected { name, .. } | Verdict::Unknown { name, .. } => name,
        }
    }

    pub fn is_streamable(&self) -> bool {
        matches!(self, Verdict::Streamable(_))
    }

    pub fn is_publication(&self) -> bool {
        matches!(self, Verdict::Publishes(_))
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Verdict::Unknown { .. })
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Verdict::Streamable(_) | Verdict::Publishes(_) => None,
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

    // A publish model is a different question, and the streaming gates would always answer
    // "no" to it: it aggregates, and it has no storage. Both are the point of it.
    //
    // The materialization is checked *first*, not after, and that ordering is the whole
    // safety of this branch. Routing on the key alone would mean a `ddi_publish` pasted onto
    // a running `materialized: table` model — the natural copy-paste — took the publish path,
    // failed its gates, and returned a rejection that `convert::pipelines` discards with a
    // bare `continue`. The result of a one-line YAML edit would be a production pipeline that
    // silently stopped being derived. So a streamable model carrying the key is refused
    // loudly instead, naming both facts.
    if node.meta_str("ddi_publish").is_some() {
        if STREAMABLE_MATERIALIZATIONS.contains(&mat) {
            return reject(
                &name,
                format!(
                    "materialized as {mat:?} and carrying meta.ddi_publish. A publish model \
                     describes what to push when *another* model commits, so it belongs on a \
                     separate view model that ref()s this one — as written, this model would \
                     stop being streamed."
                ),
            );
        }
        return analyze_publish(manifest, unique_id, node, &name);
    }

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
        let table_id_change_policy = match lookup.meta_value("ddi_lookup_table_id_change_policy") {
            None => LookupTableIdChangePolicy::Strict,
            Some(value) => match value.as_str() {
                Some("strict") => LookupTableIdChangePolicy::Strict,
                Some("use_current") => LookupTableIdChangePolicy::UseCurrent,
                _ => {
                    return reject(
                            &name,
                            format!(
                                "lookup source {} declares ddi_lookup_table_id_change_policy={value}; it must be \"strict\" or \"use_current\"",
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
            table_id_change_policy,
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

/// Materializations a publish model may have: ones with no storage of their own.
///
/// The complement of [`STREAMABLE_MATERIALIZATIONS`], and the reason is the same fact read
/// the other way. What this SQL selects is a *delta* — the rows one committed batch produced
/// — not a state. A table of those would mean something different from what dbt builds
/// nightly under the same name, so the two would silently disagree.
const PUBLISHABLE_MATERIALIZATIONS: &[&str] = &["view", "ephemeral"];

/// A group name goes into a request path and into a browser's subscription, so it is kept to
/// characters that need no escaping in either. The service permits far more; we do not, so
/// that nothing between here and the wire has to encode or decode it.
fn valid_group(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.starts_with(|c: char| c.is_ascii_alphanumeric())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':'))
}

/// Decide whether a `ddi_publish` model describes a payload ddi can build.
///
/// The gates mirror `analyze`'s, because the runtime is the same: one relation registered as
/// `source`, holding one committed batch, and nothing else in memory to read. What differs is
/// only the grain — see [`crate::transform::validate::Grain`].
fn analyze_publish(manifest: &Manifest, unique_id: &str, node: &Node, name: &str) -> Verdict {
    let declared = node.meta_str("ddi_publish").unwrap_or_default();
    let Some(kind) = crate::config::PublisherKind::parse(declared) else {
        return reject(
            name,
            format!(
                "declares meta.ddi_publish={declared:?}, which is not a publisher this build \
                 knows. Use one of: {}.",
                crate::config::PublisherKind::known()
            ),
        );
    };

    let mat = node.materialized();
    if !PUBLISHABLE_MATERIALIZATIONS.contains(&mat) {
        return reject(
            name,
            format!(
                "is materialized as {mat:?}, and a publish model must not have storage. What \
                 it selects is a delta — the rows one committed batch produced — not a state, \
                 so a table of them would mean something different from what dbt builds \
                 nightly under the same name. Materialize it as one of: {}.",
                PUBLISHABLE_MATERIALIZATIONS.join(", ")
            ),
        );
    }

    let Some(sql) = node.compiled_code.as_deref() else {
        return reject(
            name,
            "no compiled_code in the manifest — run `dbt compile` (or `dbt run`) so the \
             manifest carries the resolved SQL",
        );
    };

    // Exactly one upstream, and it is the pipeline whose commits carry this. There is no
    // lookup equivalent here on purpose: a lookup snapshot is pinned to the *source*
    // commit's timestamp, and re-resolving one when the payload is built would pin it to a
    // different instant. Enrichment belongs in the model being published for.
    if node.depends_on.nodes.len() != 1 {
        return reject(
            name,
            format!(
                "depends on {} relations; a publish model reads exactly one — the ddi model \
                 whose committed batches it describes. Everything it needs must already be in \
                 that model's rows: the batch is the only thing in memory when the payload is \
                 built, and a join here would read a table that has moved on since the commit.",
                node.depends_on.nodes.len()
            ),
        );
    }
    let host_id = &node.depends_on.nodes[0];
    let Some(host) = manifest.node(host_id) else {
        return reject(
            name,
            format!("publishes for {host_id:?}, which is absent from the manifest"),
        );
    };

    // A publication rides on a pipeline's commits, and there are none without one. Asked of
    // the host's own verdict so the reason quoted is the one the analyst will see against
    // that model too, rather than a second opinion that could drift from it.
    match analyze(manifest, host_id) {
        Verdict::Streamable(_) => {}
        other => {
            let detail = other.reason().unwrap_or("it is not streamable").to_string();
            return reject(
                name,
                format!(
                    "publishes for {:?}, which ddi cannot stream itself: {detail} Fix that \
                     model first — a publish model rides on a pipeline's commits.",
                    host.name
                ),
            );
        }
    }

    // Append-only in v1, refused here as well as at config load. A merge replaces the row
    // already stored under a key, so the committed batch does not say what the dashboard
    // delta is: the value it replaced is not in it, and adding the new row would double-count.
    if matches!(
        host.meta_str("ddi_write_mode"),
        Some("upsert") | Some("staged_upsert")
    ) {
        return reject(
            name,
            format!(
                "publishes for {:?}, which is meta.ddi_write_mode = {:?}. Realtime publication \
                 is append-only: a merge replaces the row already stored under a key, so the \
                 committed batch alone does not say what the dashboard delta is — the value it \
                 replaced is not in it, and adding the new row would double-count. Publish from \
                 an append-only model and aggregate current state downstream.",
                host.name,
                host.meta_str("ddi_write_mode").unwrap_or_default()
            ),
        );
    }

    let group = node
        .meta_str("ddi_publish_group")
        .unwrap_or(name)
        .to_string();
    if !valid_group(&group) {
        return reject(
            name,
            format!(
                "declares meta.ddi_publish_group={group:?}. A group name is a channel a \
                 browser subscribes to and goes into a URL, so it must be 1-128 characters of \
                 A-Z a-z 0-9 . _ : - starting with a letter or digit. Omit the key to use the \
                 model's own name."
            ),
        );
    }

    // The same rewrite the streaming path uses, so the host relation becomes `source` — which
    // at run time is the committed batch. No new rewriting code: `add_relation_replacements`
    // registers both `schema.table` and `catalog.schema.table`, and the rewriter is CTE-aware.
    let mut replacements = BTreeMap::new();
    add_relation_replacements(&mut replacements, host, SOURCE_TABLE);
    let rewrite = match rewrite_relations(sql, &replacements) {
        Ok(v) => v,
        // Not a rejection: this parser did not understand the SQL, which says nothing about
        // whether the payload is publishable.
        Err(e) => return unknown(name, e),
    };
    if !rewrite.unknown.is_empty() {
        return reject(
            name,
            format!(
                "reads relation(s) that are neither the model it publishes for nor anything \
                 ddi can register: {}. The publish transform runs over one batch held in \
                 memory, and nothing else exists to read.",
                rewrite.unknown.into_iter().collect::<Vec<_>>().join(", ")
            ),
        );
    }

    // The real gate, at the per-batch grain: aggregation is allowed, and narrowed to the
    // functions a client can apply as a delta.
    if let Err(e) = crate::transform::validate::validate_publish_sql(&rewrite.sql) {
        let detail = match e {
            crate::Error::Config(m) => m,
            other => other.to_string(),
        };
        return reject(name, detail);
    }

    Verdict::Publishes(Box::new(Publication {
        unique_id: unique_id.to_string(),
        name: name.to_string(),
        host_unique_id: host_id.to_string(),
        host_relation: host.qualified(),
        kind,
        group,
        sql: rewrite.sql,
    }))
}

/// Every model in the manifest, in stable order.
pub fn analyze_all(manifest: &Manifest) -> Vec<Verdict> {
    let mut verdicts: Vec<Verdict> = manifest
        .model_ids()
        .iter()
        .map(|id| analyze(manifest, id))
        .collect();
    reject_duplicate_publications(&mut verdicts);
    verdicts
}

/// Two publish models for one host reject **both**, each naming the other.
///
/// One pipeline pushes one payload per commit, so two would describe the same batch and a
/// client could not tell them apart. Both are refused rather than one picked, for the same
/// reason a duplicate `app_id` condemns every sharer: there is no innocent party, and
/// choosing silently would be worse than saying so.
fn reject_duplicate_publications(verdicts: &mut [Verdict]) {
    let mut by_host: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for v in verdicts.iter() {
        if let Verdict::Publishes(p) = v {
            by_host
                .entry(p.host_relation.clone())
                .or_default()
                .push(p.name.clone());
        }
    }
    let contested: BTreeMap<&String, &Vec<String>> =
        by_host.iter().filter(|(_, v)| v.len() > 1).collect();
    if contested.is_empty() {
        return;
    }
    for v in verdicts.iter_mut() {
        let Verdict::Publishes(p) = v else { continue };
        let Some(others) = contested.get(&p.host_relation) else {
            continue;
        };
        let reason = format!(
            "models {} all declare ddi_publish for {:?}. One pipeline pushes one payload per \
             commit: two would each describe the same batch and a client could not tell them \
             apart.",
            others
                .iter()
                .map(|n| format!("{n:?}"))
                .collect::<Vec<_>>()
                .join(" and "),
            p.host_relation
        );
        *v = reject(&p.name, reason);
    }
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

    // ---- ddi_publish models ----
    //
    // The shape under test throughout: a `view` model that ref()s the streamed model, whose
    // aggregation is a delta over one committed batch and the running total over the whole
    // table. It is never a pipeline of its own.

    /// A manifest with a streamable host and one publish model over it.
    fn publish_manifest(publish_meta: &[(&str, &str)], publish_sql: &str, mat: &str) -> Manifest {
        let mut m = manifest("SELECT order_id, amount FROM bronze.orders", &[]);
        let mut meta = HashMap::new();
        for (k, v) in publish_meta {
            meta.insert(k.to_string(), serde_json::Value::from(*v));
        }
        m.nodes.insert(
            "model.p.orders_live".to_string(),
            Node {
                name: "orders_live".into(),
                resource_type: "model".into(),
                schema: "silver".into(),
                compiled_code: Some(publish_sql.into()),
                depends_on: DependsOn {
                    nodes: vec!["model.p.orders_header".to_string()],
                },
                config: NodeConfig {
                    materialized: Some(mat.into()),
                    ..Default::default()
                },
                meta,
                ..Default::default()
            },
        );
        m
    }

    const AGG: &str =
        "SELECT country, sum(amount) AS sales_delta FROM silver.orders_header GROUP BY country";

    fn publish_verdict(meta: &[(&str, &str)], sql: &str, mat: &str) -> Verdict {
        analyze(&publish_manifest(meta, sql, mat), "model.p.orders_live")
    }

    fn publish_reason(meta: &[(&str, &str)], sql: &str, mat: &str) -> String {
        match publish_verdict(meta, sql, mat) {
            Verdict::Rejected { reason, .. } => reason,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn an_aggregating_view_that_refs_a_streamed_model_publishes() {
        let v = publish_verdict(&[("ddi_publish", "webpubsub")], AGG, "view");
        let Verdict::Publishes(p) = v else {
            panic!("expected a publication, got {v:?}");
        };
        assert_eq!(p.host_relation, "silver.orders_header");
        assert_eq!(p.host_unique_id, "model.p.orders_header");
        assert_eq!(p.kind, crate::config::PublisherKind::Webpubsub);
        assert_eq!(p.group, "orders_live", "defaults to the model's own name");
        // Rewritten onto the in-memory batch by exactly the machinery the streaming path
        // uses — the same reason a transform never sees a warehouse name either.
        assert!(p.sql.contains("source"), "got: {}", p.sql);
        assert!(!p.sql.contains("silver"), "got: {}", p.sql);
        assert!(
            p.sql.contains("GROUP BY"),
            "the aggregation survives: {}",
            p.sql
        );
    }

    #[test]
    fn an_explicit_group_overrides_the_model_name() {
        let v = publish_verdict(
            &[("ddi_publish", "webpubsub"), ("ddi_publish_group", "sales")],
            AGG,
            "view",
        );
        let Verdict::Publishes(p) = v else {
            panic!("expected a publication, got {v:?}");
        };
        assert_eq!(p.group, "sales");
    }

    #[test]
    fn a_publish_model_is_not_a_pipeline() {
        // It has no Delta target and must never be derived into one.
        let m = publish_manifest(&[("ddi_publish", "webpubsub")], AGG, "view");
        let verdicts = analyze_all(&m);
        assert_eq!(
            verdicts.iter().filter(|v| v.is_streamable()).count(),
            1,
            "only the host streams: {verdicts:?}"
        );
        assert_eq!(verdicts.iter().filter(|v| v.is_publication()).count(), 1);
    }

    #[test]
    fn ddi_publish_on_a_streamable_model_is_rejected_loudly() {
        // The failure this guard exists for. Routing on the key alone would send a running
        // `materialized: table` model down the publish path, where it fails a gate and is
        // discarded by `convert::pipelines` with a bare `continue` — so a one-line YAML
        // paste would silently stop a production pipeline being derived.
        let mut m = manifest("SELECT order_id FROM bronze.orders", &[]);
        m.nodes
            .get_mut("model.p.orders_header")
            .unwrap()
            .meta
            .insert("ddi_publish".into(), serde_json::Value::from("webpubsub"));

        let v = analyze(&m, "model.p.orders_header");
        let Verdict::Rejected { reason, .. } = v else {
            panic!("a streamable model carrying ddi_publish must be refused, got {v:?}");
        };
        assert!(reason.contains("ddi_publish"), "got: {reason}");
        assert!(
            reason.contains("would stop being streamed"),
            "got: {reason}"
        );
        assert!(
            reason.contains("ref()s this one"),
            "names the fix: {reason}"
        );
    }

    #[test]
    fn a_publish_model_with_storage_of_its_own_is_rejected() {
        // `table` and `incremental` never reach this gate — they are streamable
        // materializations, so the guard above catches them first with the louder message
        // about the pipeline that would stop being derived. What is left is everything that
        // is neither, and the reason is the same fact read once more: these rows are a
        // delta, not a state, so storing them under the model's name would mean something
        // different from what dbt builds there nightly.
        let reason = publish_reason(&[("ddi_publish", "webpubsub")], AGG, "materialized_view");
        assert!(reason.contains("must not have storage"), "got: {reason}");
        assert!(reason.contains("delta"), "says why: {reason}");
        assert!(reason.contains("view"), "names what to use: {reason}");
    }

    #[test]
    fn an_unknown_backend_names_the_ones_this_build_has() {
        let reason = publish_reason(&[("ddi_publish", "webpubsubb")], AGG, "view");
        assert!(reason.contains("webpubsubb"), "got: {reason}");
        assert!(
            reason.contains("webpubsub."),
            "names the alternatives: {reason}"
        );
    }

    #[test]
    fn a_publish_model_reading_a_second_relation_is_rejected() {
        let mut m = publish_manifest(&[("ddi_publish", "webpubsub")], AGG, "view");
        m.nodes
            .get_mut("model.p.orders_live")
            .unwrap()
            .depends_on
            .nodes
            .push("model.p.other".into());
        let v = analyze(&m, "model.p.orders_live");
        let Verdict::Rejected { reason, .. } = v else {
            panic!("expected a rejection, got {v:?}");
        };
        assert!(reason.contains("reads exactly one"), "got: {reason}");
        assert!(
            reason.contains("only thing in memory"),
            "says why: {reason}"
        );
    }

    #[test]
    fn a_publish_model_reading_an_undeclared_relation_is_rejected() {
        let reason = publish_reason(
            &[("ddi_publish", "webpubsub")],
            "SELECT c.name, sum(o.amount) AS d FROM silver.orders_header o, ref.countries c \
             GROUP BY c.name",
            "view",
        );
        assert!(
            reason.contains("not supported") || reason.contains("countries"),
            "got: {reason}"
        );
    }

    #[test]
    fn a_publish_model_on_an_upsert_host_is_rejected() {
        let mut m = publish_manifest(&[("ddi_publish", "webpubsub")], AGG, "view");
        m.nodes
            .get_mut("model.p.orders_header")
            .unwrap()
            .meta
            .insert("ddi_write_mode".into(), serde_json::Value::from("upsert"));
        let v = analyze(&m, "model.p.orders_live");
        let Verdict::Rejected { reason, .. } = v else {
            panic!("expected a rejection, got {v:?}");
        };
        assert!(reason.contains("append-only"), "got: {reason}");
        assert!(reason.contains("double-count"), "says why: {reason}");
    }

    #[test]
    fn a_publish_model_on_an_unstreamable_host_quotes_the_hosts_own_reason() {
        let mut m = publish_manifest(&[("ddi_publish", "webpubsub")], AGG, "view");
        // Break the host: two streaming relations.
        m.nodes
            .get_mut("model.p.orders_header")
            .unwrap()
            .depends_on
            .nodes
            .push("model.p.other".into());
        let v = analyze(&m, "model.p.orders_live");
        let Verdict::Rejected { reason, .. } = v else {
            panic!("expected a rejection, got {v:?}");
        };
        assert!(
            reason.contains("streams from exactly one"),
            "quotes it: {reason}"
        );
        assert!(reason.contains("Fix that model first"), "got: {reason}");
    }

    #[test]
    fn a_group_name_that_would_need_escaping_is_rejected() {
        let reason = publish_reason(
            &[
                ("ddi_publish", "webpubsub"),
                ("ddi_publish_group", "sales/eu"),
            ],
            AGG,
            "view",
        );
        assert!(reason.contains("sales/eu"), "got: {reason}");
        assert!(
            reason.contains("browser subscribes to"),
            "says why: {reason}"
        );
    }

    #[test]
    fn a_non_combinable_aggregate_is_rejected_with_the_validators_reason() {
        let reason = publish_reason(
            &[("ddi_publish", "webpubsub")],
            "SELECT country, avg(amount) AS a FROM silver.orders_header GROUP BY country",
            "view",
        );
        assert!(reason.contains("avg()"), "got: {reason}");
        assert!(
            reason.contains("sum() and count()"),
            "names the fix: {reason}"
        );
    }

    #[test]
    fn a_publish_model_without_compiled_code_says_to_run_dbt_compile() {
        let mut m = publish_manifest(&[("ddi_publish", "webpubsub")], AGG, "view");
        m.nodes
            .get_mut("model.p.orders_live")
            .unwrap()
            .compiled_code = None;
        let v = analyze(&m, "model.p.orders_live");
        let Verdict::Rejected { reason, .. } = v else {
            panic!("expected a rejection, got {v:?}");
        };
        assert!(reason.contains("dbt compile"), "got: {reason}");
    }

    #[test]
    fn two_publish_models_for_one_host_reject_both() {
        // No innocent party: one pipeline pushes one payload per commit, so picking one
        // silently would be worse than refusing both.
        let mut m = publish_manifest(&[("ddi_publish", "webpubsub")], AGG, "view");
        let mut meta = HashMap::new();
        meta.insert("ddi_publish".into(), serde_json::Value::from("webpubsub"));
        m.nodes.insert(
            "model.p.orders_live_v2".to_string(),
            Node {
                name: "orders_live_v2".into(),
                resource_type: "model".into(),
                schema: "silver".into(),
                compiled_code: Some(AGG.into()),
                depends_on: DependsOn {
                    nodes: vec!["model.p.orders_header".to_string()],
                },
                config: NodeConfig {
                    materialized: Some("view".into()),
                    ..Default::default()
                },
                meta,
                ..Default::default()
            },
        );

        let verdicts = analyze_all(&m);
        assert_eq!(
            verdicts.iter().filter(|v| v.is_publication()).count(),
            0,
            "both must be refused: {verdicts:?}"
        );
        for name in ["orders_live", "orders_live_v2"] {
            let v = verdicts.iter().find(|v| v.name() == name).unwrap();
            let reason = v.reason().unwrap_or_default();
            assert!(
                reason.contains("orders_live_v2"),
                "{name} names the other: {reason}"
            );
            assert!(
                reason.contains("could not tell them apart"),
                "got: {reason}"
            );
        }
        // And the host is untouched: a contested dashboard must not stop ingestion.
        assert!(verdicts.iter().any(|v| v.is_streamable()));
    }
}
