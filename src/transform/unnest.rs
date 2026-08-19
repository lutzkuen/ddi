//! Trino's `CROSS JOIN UNNEST`, rewritten into the form DataFusion executes.
//!
//! # Why a rewrite rather than a feature
//!
//! Expanding an array to child grain is row-local: every output row comes from exactly one
//! input row, batch boundaries cannot affect the result, and the output is append-only. It
//! has been supported since v1. The problem was never the operation — it was the spelling.
//!
//! DataFusion writes it as a function in the projection:
//!
//! ```sql
//! SELECT order_id, li.sku FROM (SELECT order_id, unnest(line_items) AS li FROM source)
//! ```
//!
//! Trino, Starburst, Athena and ANSI SQL write it as a join against a table function:
//!
//! ```sql
//! SELECT o.order_id, li.sku FROM source o CROSS JOIN UNNEST(o.line_items) AS t(li)
//! ```
//!
//! A dbt model contains the second. This tool's whole premise is that a model means the
//! same thing in the warehouse and here — it is why Trino's JSON functions are implemented
//! natively rather than approximated — and until this module existed, the second form was
//! rejected as a `JOIN`, with advice ("denormalise upstream; pinned-snapshot lookup joins
//! are planned for v2") that made no sense for it. `UNNEST` of a column of *this same row*
//! joins nothing: there is no second table, nothing to pin, and no cross-row state.
//!
//! # What it becomes
//!
//! ```sql
//! FROM source o CROSS JOIN UNNEST(o.line_items) AS t(li)
//! -- becomes
//! FROM (SELECT o.*, unnest(o.line_items) AS li FROM source o) AS o
//! ```
//!
//! The outer query is left **exactly** as written — same projection, same `WHERE`, same
//! qualifiers. Keeping the source's alias on the subquery is what allows that: `o.order_id`
//! still resolves, so no expression has to be rewritten and nothing can be broken by the
//! rewriting of one.
//!
//! # Why it is built by parsing rather than by hand
//!
//! The new `FROM` is rendered as text and handed back to the parser. Assembling AST nodes
//! by hand would bind this module to one version of `sqlparser`'s struct layout for no gain;
//! going through the parser means the result is a thing the parser accepts, by construction.

use deltalake::datafusion::sql::parser::DFParser;
use deltalake::datafusion::sql::sqlparser::ast::{
    DataType, Expr, Join, Query, Select, SelectItem, SetExpr, Statement as SqlStatement,
    TableFactor, TableWithJoins, VisitMut, VisitorMut,
};
use std::ops::ControlFlow;

use crate::error::{Error, Result};

/// Turn `CAST(<json> AS ARRAY(JSON))` into the function that produces a real Arrow list.
///
/// # Why a cast has to become a function call
///
/// Bronze usually carries its payload as one JSON string, so the array to expand does not
/// exist as an array until something makes it one. Trino spells that as a cast:
///
/// ```sql
/// CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY(JSON))) AS t(li)
/// ```
///
/// Arrow has no cast from text to a list — no cast kernel parses JSON — so the cast is
/// replaced by [`json_array_elements`](crate::transform::json), which does. Each element
/// comes back as JSON text, which is exactly what `ARRAY(JSON)` promises: the element's
/// shape is not decided here, it is decided by whatever reads it, with the same
/// `json_extract_scalar` that works in both engines.
///
/// The type is recognised by how it renders rather than by matching `sqlparser`'s type
/// enum. There is one type to spot, its spelling is fixed by Trino, and matching text does
/// not care which dialect's parser produced it — `Array(JSON)`, `ARRAY(JSON)` and
/// `ARRAY<JSON>` are all the same thing said three ways.
pub(crate) fn rewrite_json_array_casts(query: &mut Query) -> Result<()> {
    struct V(Option<Error>);
    impl VisitorMut for V {
        type Break = ();
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            let Expr::Cast {
                expr: inner,
                data_type,
                ..
            } = expr
            else {
                return ControlFlow::Continue(());
            };
            if !is_json_array(data_type) {
                return ControlFlow::Continue(());
            }
            match parse_expr(&format!("json_array_elements({inner})")) {
                Ok(replacement) => *expr = replacement,
                Err(e) => {
                    self.0 = Some(e);
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut v = V(None);
    let _ = query.visit(&mut v);
    match v.0 {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Is this the `ARRAY(JSON)` type, however the parser chose to render it?
fn is_json_array(data_type: &DataType) -> bool {
    let rendered: String = data_type
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_ascii_uppercase();
    matches!(rendered.as_str(), "ARRAY(JSON)" | "ARRAY<JSON>")
}

/// Parse a single expression by parsing a query that projects it.
fn parse_expr(expr: &str) -> Result<Expr> {
    let select = parse_select(&format!("SELECT {expr} FROM source"))?;
    match select.projection.into_iter().next() {
        Some(SelectItem::UnnamedExpr(e)) => Ok(e),
        Some(SelectItem::ExprWithAlias { expr, .. }) => Ok(expr),
        _ => Err(Error::Config(format!(
            "could not rewrite CAST(... AS ARRAY(JSON)): {expr}"
        ))),
    }
}

/// Rewrite every `CROSS JOIN UNNEST` in `query`, in place, including inside CTEs and
/// subqueries.
///
/// The JSON cast is rewritten first, so an `UNNEST` over one sees a plain function call by
/// the time it is examined and needs to know nothing about JSON.
pub(crate) fn rewrite(query: &mut Query) -> Result<()> {
    // Idempotent: by the time `validate_sql` calls this, the casts are usually already gone.
    // Kept here so the entry point is complete on its own for any other caller.
    rewrite_json_array_casts(query)?;
    rewrite_unnest(query)
}

fn rewrite_unnest(query: &mut Query) -> Result<()> {
    for cte in query.with.iter_mut().flat_map(|w| w.cte_tables.iter_mut()) {
        rewrite_unnest(&mut cte.query)?;
    }
    rewrite_set_expr(&mut query.body)
}

fn rewrite_set_expr(body: &mut SetExpr) -> Result<()> {
    match body {
        SetExpr::Select(select) => rewrite_select(select),
        SetExpr::Query(q) => rewrite_unnest(q),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left)?;
            rewrite_set_expr(right)
        }
        _ => Ok(()),
    }
}

fn rewrite_select(select: &mut Select) -> Result<()> {
    // Recurse first: a derived table may hold an UNNEST of its own.
    for twj in select.from.iter_mut() {
        if let TableFactor::Derived { subquery, .. } = &mut twj.relation {
            rewrite_unnest(subquery)?;
        }
    }

    let Some(found) = find(select)? else {
        return Ok(());
    };

    let base = &select.from[0].relation;
    // The alias the outer query uses for the source, and therefore the one the subquery has
    // to answer to. Without one, `source` itself is the name to keep, so that both
    // `source.order_id` and a bare `order_id` still resolve.
    let alias = base_alias(base);
    let qualifier = format!("{alias}.");

    let inner = format!(
        "SELECT {qualifier}*, unnest({array}) AS {column} FROM {base}",
        array = found.array,
        column = found.column,
    );
    let from = format!("({inner}) AS {alias}");
    let mut rewritten = parse_from(&from)?;

    // Whatever else was joined here still has to be joined to the fan-out. The `UNNEST` has
    // moved inside the derived table, so the surviving joins reattach to that, in the order
    // they were written -- a pinned lookup's `LEFT JOIN ... ON` reads the expanded rows and
    // is indifferent to the expansion, because it adds columns rather than rows. Whether such
    // a join is permitted at all is still the validator's question; this only preserves it
    // intact for the validator to ask.
    let carried: Vec<Join> = std::mem::take(&mut select.from)
        .into_iter()
        .flat_map(|twj| twj.joins)
        .filter(|join| !matches!(join.relation, TableFactor::UNNEST { .. }))
        .collect();
    if let Some(base) = rewritten.first_mut() {
        base.joins = carried;
    }

    select.from = rewritten;
    Ok(())
}

/// What one `UNNEST` in a `FROM` clause amounts to.
struct Found {
    /// The array expression, as written.
    array: String,
    /// The name the outer query refers to each element by.
    column: String,
}

/// Locate the single `UNNEST` in this `FROM`, if there is one.
///
/// Both Trino spellings land here: the explicit `a CROSS JOIN UNNEST(x) AS t(y)` and the
/// older `a, UNNEST(x) AS t(y)`.
fn find(select: &Select) -> Result<Option<Found>> {
    let mut factors: Vec<&TableFactor> = Vec::new();
    for (i, twj) in select.from.iter().enumerate() {
        if i > 0 {
            factors.push(&twj.relation);
        }
        for join in &twj.joins {
            factors.push(&join.relation);
        }
    }

    let unnests: Vec<&TableFactor> = factors
        .iter()
        .copied()
        .filter(|f| matches!(f, TableFactor::UNNEST { .. }))
        .collect();
    if unnests.is_empty() {
        return Ok(None);
    }
    // A comma-separated factor is an implicit cross join, and rewriting the `FROM` at all
    // means dropping it, so anything sitting there that is not the fan-out is left alone for
    // the validator to reject. An explicit join is different: `rewrite_select` carries it
    // over verbatim, so a declared lookup's `LEFT JOIN ... ON` may accompany a fan-out.
    if select
        .from
        .iter()
        .skip(1)
        .any(|twj| !matches!(twj.relation, TableFactor::UNNEST { .. }))
    {
        return Ok(None);
    }
    if unnests.len() > 1 {
        return Err(unsupported(
            "more than one UNNEST in a single query",
            "each one multiplies the row count by the next, and the batch-size guard is \
             calibrated for a single expansion.",
            "unnest once per pipeline and chain them, or unnest the outer array here and \
             the inner one downstream.",
        ));
    }
    if select.from.len() > 1 && !select.from[0].joins.is_empty() {
        return Ok(None); // a genuine join as well; not ours to rewrite
    }
    if !matches!(select.from[0].relation, TableFactor::Table { .. }) {
        return Ok(None); // unnesting something that is not the source table
    }

    let TableFactor::UNNEST {
        alias,
        array_exprs,
        with_offset,
        with_ordinality,
        ..
    } = unnests[0]
    else {
        unreachable!("filtered above");
    };

    if *with_ordinality || *with_offset {
        return Err(unsupported(
            "UNNEST ... WITH ORDINALITY",
            "the element's position would have to be generated per row, and the only \
             spelling for that is a window function, which is cross-row state.",
            "carry the position in the array's own elements if you need it, or add it \
             downstream.",
        ));
    }
    if array_exprs.len() != 1 {
        return Err(unsupported(
            "UNNEST over several arrays at once",
            "the arrays are expanded in step, which is a zip rather than a projection, and \
             the result depends on their relative lengths.",
            "unnest one array per query.",
        ));
    }

    // `AS t(li)` names the element column `li`; `AS t` alone names it `t`. Trino allows
    // both, and the outer query refers to whichever it is.
    let column = match alias {
        Some(a) => match a.columns.first() {
            Some(c) => c.name.value.clone(),
            None => a.name.value.clone(),
        },
        None => {
            return Err(unsupported(
                "UNNEST without an alias",
                "the outer query has no name to refer to the expanded elements by.",
                "write `CROSS JOIN UNNEST(<array>) AS t(<name>)`.",
            ))
        }
    };

    Ok(Some(Found {
        array: array_exprs[0].to_string(),
        column,
    }))
}

/// The name the source is known by in the outer query.
fn base_alias(base: &TableFactor) -> String {
    match base {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(a) => a.name.value.clone(),
            None => name
                .0
                .last()
                .map(|p| p.to_string())
                .unwrap_or_else(|| crate::transform::sql::SOURCE_TABLE.to_string()),
        },
        _ => crate::transform::sql::SOURCE_TABLE.to_string(),
    }
}

/// Parse a `FROM` clause by parsing a query that has one.
fn parse_from(from: &str) -> Result<Vec<TableWithJoins>> {
    Ok(parse_select(&format!("SELECT 1 FROM {from}"))?.from)
}

/// Parse a whole `SELECT`, so fragments are built by the parser rather than by hand.
fn parse_select(sql: &str) -> Result<Select> {
    let mut statements = DFParser::parse_sql(sql).map_err(|e| {
        Error::Config(format!(
            "could not rewrite into a form this engine runs ({e}). The rewritten SQL was: \
             {sql}"
        ))
    })?;
    let statement = statements.pop_front().ok_or_else(|| {
        Error::Config("could not rewrite CROSS JOIN UNNEST: empty statement".into())
    })?;
    let deltalake::datafusion::sql::parser::Statement::Statement(inner) = statement else {
        return Err(Error::Config(
            "could not rewrite CROSS JOIN UNNEST: unexpected statement".into(),
        ));
    };
    let SqlStatement::Query(q) = *inner else {
        return Err(Error::Config(
            "could not rewrite CROSS JOIN UNNEST: not a query".into(),
        ));
    };
    let SetExpr::Select(select) = *q.body else {
        return Err(Error::Config(
            "could not rewrite CROSS JOIN UNNEST: not a select".into(),
        ));
    };
    Ok(*select)
}

fn unsupported(what: &str, why: &str, instead: &str) -> Error {
    Error::Config(format!("{what} is not supported: {why} Instead: {instead}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse, rewrite, and render back — what the pipeline will actually run.
    fn rewritten(sql: &str) -> Result<String> {
        let mut statements = DFParser::parse_sql(sql).unwrap();
        let statement = statements.pop_front().unwrap();
        let deltalake::datafusion::sql::parser::Statement::Statement(inner) = statement else {
            panic!("not a statement")
        };
        let SqlStatement::Query(mut q) = *inner else {
            panic!("not a query")
        };
        rewrite(&mut q)?;
        Ok(q.to_string())
    }

    #[test]
    fn the_trino_spelling_becomes_the_datafusion_one() {
        let got = rewritten(
            "SELECT o.order_id, li.sku FROM source o CROSS JOIN UNNEST(o.line_items) AS t(li)",
        )
        .unwrap();
        assert_eq!(
            got,
            "SELECT o.order_id, li.sku FROM \
             (SELECT o.*, unnest(o.line_items) AS li FROM source o) AS o",
            "the outer query must survive untouched"
        );
    }

    #[test]
    fn the_older_comma_spelling_is_the_same_thing() {
        let got =
            rewritten("SELECT o.order_id, li.sku FROM source o, UNNEST(o.line_items) AS t(li)")
                .unwrap();
        assert!(got.contains("unnest(o.line_items) AS li"), "got: {got}");
        assert!(!got.contains(", UNNEST"), "the comma join is gone: {got}");
    }

    #[test]
    fn an_unaliased_source_keeps_its_own_name() {
        let got =
            rewritten("SELECT order_id, li.sku FROM source CROSS JOIN UNNEST(line_items) AS t(li)")
                .unwrap();
        assert!(
            got.contains("FROM (SELECT source.*, unnest(line_items) AS li FROM source) AS source"),
            "got: {got}"
        );
    }

    #[test]
    fn an_alias_with_no_column_list_names_the_element_after_itself() {
        // Trino allows `AS t` alone, and then `t` is the element.
        let got =
            rewritten("SELECT o.order_id, t FROM source o CROSS JOIN UNNEST(o.tags) AS t").unwrap();
        assert!(got.contains("unnest(o.tags) AS t"), "got: {got}");
    }

    #[test]
    fn a_json_array_cast_becomes_the_function_that_builds_a_real_list() {
        // The bronze shape: the array does not exist as an array until this makes it one.
        // Arrow has no cast from text to a list, so the cast has to become a call.
        let got = rewritten(
            "SELECT o.id, li FROM source o \
             CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY<JSON>)) AS t(li)",
        )
        .unwrap();
        assert!(
            got.contains("unnest(json_array_elements(json_extract(o.data, '$.lines'))) AS li"),
            "got: {got}"
        );
        assert!(!got.contains("CAST"), "the cast is gone: {got}");
    }

    #[test]
    fn a_cast_to_something_else_is_left_alone() {
        let got = rewritten("SELECT CAST(qty AS BIGINT) AS qty FROM source").unwrap();
        assert!(got.contains("CAST(qty AS BIGINT)"), "got: {got}");
    }

    #[test]
    fn a_query_without_unnest_is_left_alone() {
        let sql = "SELECT order_id, status FROM source WHERE status <> 'DRAFT'";
        assert_eq!(rewritten(sql).unwrap(), sql);
    }

    #[test]
    fn the_datafusion_spelling_still_works_untouched() {
        // The form that has always been supported must not be disturbed.
        let sql = "SELECT order_id, li.sku FROM \
                   (SELECT order_id, unnest(line_items) AS li FROM source)";
        let got = rewritten(sql).unwrap();
        assert!(got.contains("unnest(line_items) AS li"), "got: {got}");
    }

    #[test]
    fn a_real_join_is_not_rewritten_and_stays_for_the_validator_to_reject() {
        // Rewriting is not a licence to accept joins: this must pass through untouched so
        // the validator still refuses it.
        let sql = "SELECT a.x FROM source a CROSS JOIN customers c";
        let got = rewritten(sql).unwrap();
        assert!(got.contains("JOIN customers"), "got: {got}");
    }

    #[test]
    fn ordinality_is_refused_at_load_with_the_reason() {
        let e = rewritten(
            "SELECT o.order_id, li, n FROM source o \
             CROSS JOIN UNNEST(o.line_items) WITH ORDINALITY AS t(li, n)",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("ORDINALITY"), "got: {e}");
        assert!(e.contains("window function"), "says why: {e}");
    }

    #[test]
    fn two_unnests_are_refused_rather_than_half_rewritten() {
        let e = rewritten(
            "SELECT o.order_id, li, tag FROM source o \
             CROSS JOIN UNNEST(o.line_items) AS a(li) CROSS JOIN UNNEST(o.tags) AS b(tag)",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("more than one UNNEST"), "got: {e}");
    }

    #[test]
    fn unnest_inside_a_cte_is_rewritten_too() {
        let got = rewritten(
            "WITH lines AS (SELECT o.order_id, li.sku AS sku FROM source o \
             CROSS JOIN UNNEST(o.line_items) AS t(li)) SELECT order_id, sku FROM lines",
        )
        .unwrap();
        assert!(got.contains("unnest(o.line_items) AS li"), "got: {got}");
    }
}
