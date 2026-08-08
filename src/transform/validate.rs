//! Config-load-time rejection of stateful SQL.
//!
//! The property that makes this tool worth existing is *restart from a version number with
//! no state directory*. Every construct rejected here would break it. Validation runs when
//! the config is parsed, not when the first batch arrives — a pipeline that cannot be
//! correct should never start.
//!
//! Rejections are deliberately **safe by construction, not safe by convention**. `GROUP BY
//! order_id` where every group provably fits in one batch would be fine; accepting it
//! invites `GROUP BY customer_id`, which silently emits partial sums per batch. So all
//! `GROUP BY` is rejected, and the error names the alternative.

use deltalake::datafusion::sql::parser::{DFParser, Statement};
use deltalake::datafusion::sql::sqlparser::ast::{
    Expr, ObjectName, Query, Select, SetExpr, Statement as SqlStatement, TableFactor, VisitMut,
    VisitorMut,
};
use std::ops::ControlFlow;

use crate::error::{Error, Result};
use crate::transform::sql::SOURCE_TABLE;

/// A rejected construct, with the alternative spelled out.
fn reject(what: &str, why: &str, instead: &str) -> Error {
    Error::Config(format!("{what} is not supported: {why} Instead: {instead}"))
}

/// Parse and validate a transform SQL statement.
///
/// Returns the parsed statement so the caller does not parse twice.
pub fn validate_sql(sql: &str) -> Result<Statement> {
    let mut statements = DFParser::parse_sql(sql)
        .map_err(|e| Error::Config(format!("could not parse transform_sql: {e}")))?;

    if statements.is_empty() {
        return Err(Error::Config("transform_sql is empty".into()));
    }
    if statements.len() > 1 {
        return Err(reject(
            "multiple statements",
            "a transform is a single row-to-row projection over the source batch.",
            "use one SELECT; chain pipelines if you need more stages.",
        ));
    }

    let statement = statements.pop_front().expect("checked non-empty");

    let Statement::Statement(inner) = &statement else {
        return Err(reject(
            "this statement type",
            "only a plain SELECT is a valid transform.",
            "use SELECT ... FROM source.",
        ));
    };

    let query = match inner.as_ref() {
        SqlStatement::Query(q) => q.clone(),
        _ => {
            return Err(reject(
                "DDL/DML",
                "the transform may only read the source batch; the sink owns all writes.",
                "use SELECT ... FROM source, and create the target table with external tooling.",
            ));
        }
    };

    check_query(&query)?;
    Ok(statement)
}

fn check_query(query: &Query) -> Result<()> {
    if query.with.is_some() {
        // CTEs are fine in principle but each one needs the same checks; walking them is
        // what the visitor below does, so allow and let the visitor police the contents.
    }
    check_set_expr(&query.body)?;

    // Window functions can appear in ORDER BY / expressions anywhere.
    let mut v = StatefulConstructVisitor::default();
    let mut q = query.clone();
    let _ = q.visit(&mut v);
    if let Some(err) = v.found {
        return Err(err);
    }
    Ok(())
}

fn check_set_expr(body: &SetExpr) -> Result<()> {
    match body {
        SetExpr::Select(select) => check_select(select),
        SetExpr::Query(q) => check_query(q),
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr(left)?;
            check_set_expr(right)
        }
        SetExpr::Values(_) => Err(reject(
            "VALUES",
            "the transform must read from the source batch.",
            "use SELECT ... FROM source.",
        )),
        _ => Err(reject(
            "this query form",
            "only SELECT over the source batch is supported.",
            "use SELECT ... FROM source.",
        )),
    }
}

fn check_select(select: &Select) -> Result<()> {
    // GROUP BY — the headline rejection.
    let grouped = match &select.group_by {
        deltalake::datafusion::sql::sqlparser::ast::GroupByExpr::All(_) => true,
        deltalake::datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            !exprs.is_empty()
        }
    };
    if grouped {
        return Err(reject(
            "GROUP BY",
            "this tool preserves grain: a group that spans batches would emit partial \
             results, and the output would stop being append-only.",
            "use array_sum(line_items, 'price * qty') for intra-row aggregation, or \
             aggregate downstream.",
        ));
    }

    if select.having.is_some() {
        return Err(reject(
            "HAVING",
            "it only exists alongside aggregation.",
            "filter rows with WHERE, or aggregate downstream.",
        ));
    }

    if select.distinct.is_some() {
        return Err(reject(
            "DISTINCT",
            "deduplication is cross-row state: rows in an earlier batch are already \
             committed and cannot be compared against.",
            "deduplicate downstream with a MERGE, or accept append-only duplicates.",
        ));
    }

    // Joins: v1 has no pinned-snapshot machinery, so any join is unpinned by definition.
    //
    // Two FROM items separated by a comma is a join written the old way. It has to be
    // caught here rather than by the per-item `joins` check below, which only sees the
    // `a JOIN b` spelling — and by the relation check in the visitor, which cannot tell
    // `FROM source a, source b` from a plain `FROM source`.
    if select.from.len() > 1 {
        return Err(reject(
            "a comma-separated FROM list (an implicit cross join)",
            "a join against a table that can change between batches makes output \
             non-reproducible, and joining the source to itself is cross-row state.",
            "denormalise upstream; pinned-snapshot lookup joins are planned for v2.",
        ));
    }

    for twj in &select.from {
        if !twj.joins.is_empty() {
            return Err(reject(
                "JOIN",
                "a join against a table that can change between batches makes output \
                 non-reproducible, and a self-join is cross-row state.",
                "denormalise upstream; pinned-snapshot lookup joins are planned for v2.",
            ));
        }
        check_table_factor(&twj.relation)?;
    }

    Ok(())
}

fn check_table_factor(tf: &TableFactor) -> Result<()> {
    match tf {
        TableFactor::Table { .. } => Ok(()),
        TableFactor::Derived { subquery, .. } => check_query(subquery),
        TableFactor::UNNEST { .. } => Ok(()),
        TableFactor::NestedJoin { .. } => Err(reject(
            "JOIN",
            "a join against a table that can change between batches makes output \
             non-reproducible.",
            "denormalise upstream; pinned-snapshot lookup joins are planned for v2.",
        )),
        _ => Ok(()),
    }
}

/// Catches window functions and aggregate calls wherever they hide.
#[derive(Default)]
struct StatefulConstructVisitor {
    found: Option<Error>,
}

/// Aggregate function names that imply cross-row state.
///
/// `array_*` UDFs are intentionally absent: they aggregate *within* a row and cannot
/// reach across rows.
const AGGREGATES: &[&str] = &[
    "sum",
    "avg",
    "min",
    "max",
    "count",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "var",
    "var_pop",
    "var_samp",
    "median",
    "mode",
    "array_agg",
    "string_agg",
    "listagg",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "every",
    "corr",
    "covar",
    "covar_pop",
    "covar_samp",
    "approx_distinct",
    "approx_median",
    "approx_percentile_cont",
    "percentile_cont",
    "percentile_disc",
    "grouping",
    "first_value",
    "last_value",
    "nth_value",
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "cume_dist",
    "ntile",
    "lag",
    "lead",
];

impl VisitorMut for StatefulConstructVisitor {
    type Break = ();

    /// Every table name mentioned anywhere in the statement, including inside subqueries
    /// the `check_select` walk never reaches (an `IN (SELECT ... FROM products)` lives in
    /// the WHERE expression, not in the FROM list).
    ///
    /// Only `source` is registered on the session, so any other name could only ever fail
    /// at plan time — on the first batch, in production, from a daemon that started
    /// clean and passed `ddi validate`. Naming it here keeps the promise that a pipeline
    /// which cannot be correct never starts.
    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        if self.found.is_some() {
            return ControlFlow::Break(());
        }
        let full = relation.to_string();
        let bare = full
            .rsplit('.')
            .next()
            .unwrap_or(&full)
            .trim_matches('"')
            .to_ascii_lowercase();
        if bare != SOURCE_TABLE {
            self.found = Some(reject(
                &format!("the table {full:?}"),
                &format!(
                    "a transform may only read the batch it was given, which is registered \
                     as {SOURCE_TABLE:?}. Reading a second table means joining against \
                     something that can change between batches, which makes the output \
                     non-reproducible."
                ),
                "denormalise upstream, or enrich downstream; pinned-snapshot lookup joins \
                 are planned for v2.",
            ));
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if self.found.is_some() {
            return ControlFlow::Break(());
        }
        if let Expr::Function(f) = expr {
            if f.over.is_some() {
                self.found = Some(reject(
                    "window functions (OVER)",
                    "a window frame reaches across rows, and rows outside the current \
                     batch are already committed.",
                    "compute per-row values here and window downstream.",
                ));
                return ControlFlow::Break(());
            }
            let name = f.name.to_string().to_ascii_lowercase();
            let bare = name.rsplit('.').next().unwrap_or(&name).to_string();
            if AGGREGATES.contains(&bare.as_str()) {
                self.found = Some(reject(
                    &format!("the aggregate function {bare}()"),
                    "it combines values across rows, which cannot be correct when rows \
                     arrive in independent batches.",
                    "use array_sum / array_min / array_max / array_avg / array_length for \
                     intra-row aggregation over an array column, or aggregate downstream.",
                ));
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_of(sql: &str) -> String {
        validate_sql(sql)
            .err()
            .unwrap_or_else(|| panic!("expected {sql:?} to be rejected"))
            .to_string()
    }

    #[test]
    fn plain_projection_is_accepted() {
        validate_sql("SELECT order_id, CAST(total AS DECIMAL(18,4)) AS total FROM source")
            .expect("a cast/rename projection is the core supported case");
    }

    #[test]
    fn filter_is_accepted() {
        validate_sql("SELECT a FROM source WHERE status <> 'DRAFT'").unwrap();
    }

    #[test]
    fn struct_field_access_is_accepted() {
        validate_sql("SELECT customer.id AS customer_id FROM source").unwrap();
    }

    #[test]
    fn unnest_is_accepted() {
        validate_sql(
            "SELECT order_id, li.sku FROM (SELECT order_id, unnest(line_items) AS li FROM source)",
        )
        .expect("unnest is row-local and append-only, so it is a v1 feature");
    }

    #[test]
    fn group_by_is_rejected_and_names_the_alternative() {
        let e = err_of("SELECT customer_id, sum(total) FROM source GROUP BY customer_id");
        assert!(e.contains("GROUP BY is not supported"), "got: {e}");
        assert!(
            e.contains("array_sum"),
            "must name the alternative, got: {e}"
        );
    }

    #[test]
    fn group_by_is_rejected_even_when_the_group_cannot_span_a_batch() {
        // Safe by construction beats safe by convention.
        let e = err_of("SELECT order_id, sum(x) FROM source GROUP BY order_id");
        assert!(e.contains("GROUP BY is not supported"), "got: {e}");
    }

    #[test]
    fn bare_aggregate_without_group_by_is_rejected() {
        // SELECT sum(x) FROM source has no GROUP BY but is still cross-row.
        let e = err_of("SELECT sum(total) AS t FROM source");
        assert!(e.contains("sum()"), "got: {e}");
    }

    #[test]
    fn window_function_is_rejected() {
        let e = err_of("SELECT order_id, row_number() OVER (ORDER BY ts) AS rn FROM source");
        assert!(e.contains("window functions"), "got: {e}");
    }

    #[test]
    fn distinct_is_rejected() {
        let e = err_of("SELECT DISTINCT order_id FROM source");
        assert!(e.contains("DISTINCT"), "got: {e}");
    }

    #[test]
    fn having_is_rejected() {
        let e = err_of("SELECT a FROM source GROUP BY a HAVING count(*) > 1");
        // GROUP BY fires first; either rejection is correct so long as it is rejected.
        assert!(e.contains("not supported"), "got: {e}");
    }

    #[test]
    fn a_comma_separated_from_list_is_rejected_like_any_other_join() {
        // The old spelling of a join. It carries no `joins` on either FROM item, so the
        // per-item check never sees it.
        let e =
            validate_sql("SELECT s.order_id, p.name FROM source s, products p WHERE s.sku = p.sku")
                .unwrap_err();
        assert!(e.to_string().contains("cross join"), "got: {e}");
    }

    #[test]
    fn joining_the_source_to_itself_is_rejected() {
        // Both relations are `source`, so the table-name check cannot catch this one.
        let e = validate_sql("SELECT a.order_id FROM source a, source b").unwrap_err();
        assert!(e.to_string().contains("cross join"), "got: {e}");
    }

    #[test]
    fn a_second_table_in_a_subquery_is_rejected_at_load_not_at_plan_time() {
        // Lives in the WHERE expression, so the FROM walk never reaches it. Without the
        // relation check this passed `ddi validate` and only died on the first batch.
        let e = validate_sql(
            "SELECT order_id FROM source WHERE order_id IN (SELECT order_id FROM products)",
        )
        .unwrap_err();
        assert!(e.to_string().contains("products"), "got: {e}");
        assert!(e.to_string().contains("denormalise"), "got: {e}");
    }

    #[test]
    fn a_second_table_in_a_derived_table_is_rejected() {
        let e = validate_sql("SELECT order_id FROM (SELECT order_id FROM products)").unwrap_err();
        assert!(e.to_string().contains("products"), "got: {e}");
    }

    #[test]
    fn a_qualified_name_for_the_source_is_still_accepted() {
        // Whatever the planner resolves it to, the trailing identifier is what matters.
        validate_sql("SELECT order_id FROM public.source").unwrap();
    }

    #[test]
    fn join_is_rejected() {
        let e = err_of("SELECT a.x FROM source a JOIN other b ON a.k = b.k");
        assert!(e.contains("JOIN"), "got: {e}");
        assert!(e.contains("v2"), "should point at the v2 plan, got: {e}");
    }

    #[test]
    fn ddl_is_rejected() {
        let e = err_of("CREATE TABLE t (a INT)");
        assert!(e.contains("not supported"), "got: {e}");
    }

    #[test]
    fn dml_is_rejected() {
        let e = err_of("DELETE FROM source WHERE a = 1");
        assert!(e.contains("not supported"), "got: {e}");
    }

    #[test]
    fn multiple_statements_are_rejected() {
        let e = err_of("SELECT a FROM source; SELECT b FROM source");
        assert!(e.contains("multiple statements"), "got: {e}");
    }

    #[test]
    fn array_udfs_are_not_mistaken_for_aggregates() {
        // The whole point of the array_* UDFs: intra-row, so they must pass.
        validate_sql("SELECT array_sum(line_items, 'price * qty') AS total FROM source").unwrap();
        validate_sql("SELECT array_length(line_items) AS n FROM source").unwrap();
        validate_sql("SELECT array_min(prices) AS lo, array_max(prices) AS hi FROM source")
            .unwrap();
    }

    #[test]
    fn aggregate_hidden_in_a_subquery_is_still_rejected() {
        let e = err_of("SELECT x FROM (SELECT sum(y) AS x FROM source)");
        assert!(e.contains("not supported"), "got: {e}");
    }

    #[test]
    fn empty_sql_is_rejected() {
        assert!(validate_sql("   ").is_err());
    }
}
