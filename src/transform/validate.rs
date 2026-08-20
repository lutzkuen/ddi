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
//!
//! That argument is about rows that get *stored*. A `ddi_publish` model's rows are a message
//! describing one committed batch, and a partial sum is precisely what it is for — so the
//! same rules are applied at a second [`Grain`], under which aggregation is allowed and the
//! aggregate functions narrow to the ones a client can apply as a delta. Everything else —
//! one source relation, no window frames, no foreign tables — is shared, so the two cannot
//! drift into disagreeing about what a batch is.

use deltalake::datafusion::sql::parser::{DFParser, Statement};
use deltalake::datafusion::sql::sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    Ident, Join, JoinConstraint, JoinOperator, ObjectName, Query, Select, SetExpr,
    Statement as SqlStatement, TableFactor, Value, VisitMut, VisitorMut,
};
use std::collections::BTreeSet;
use std::ops::ControlFlow;

use crate::error::{Error, Result};
use crate::transform::sql::SOURCE_TABLE;

/// The only named-zone `from_unixtime` spelling currently used by our Trino models.
///
/// Keeping this explicit is important: a Unix epoch is an instant, whereas a timestamp
/// without a timezone is a wall-clock value. The rewrite below first labels the epoch UTC,
/// then changes its display/calendar timezone to this zone. It therefore has no dependency on
/// the DataFusion session's otherwise-global timezone setting.
const TRINO_FROM_UNIXTIME_TIME_ZONE: &str = "Europe/Amsterdam";

/// A rejected construct, with the alternative spelled out.
fn reject(what: &str, why: &str, instead: &str) -> Error {
    Error::Config(format!("{what} is not supported: {why} Instead: {instead}"))
}

/// What a query's rows *mean*, which is what decides whether aggregation is legal.
///
/// [`Grain::Preserved`] is a transform: its rows are appended to a table that outlives their
/// batch, so a per-batch `sum` would be a partial sum stored forever — the reason `GROUP BY`
/// is the headline rejection in this module.
///
/// [`Grain::PerBatch`] is a *publication*: a message that names the closed source-version
/// range it covers and is never stored. A partial sum is exactly what it is for. That single
/// difference is the whole reason this parameter exists, and it is why the two grains may
/// share every other rule — one source relation, no window frames, no foreign tables — while
/// disagreeing about `GROUP BY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Grain {
    /// One row in, one row out. Rows are appended to a Delta table.
    #[default]
    Preserved,
    /// Rows describe one committed batch and are sent, not stored.
    PerBatch,
}

impl Grain {
    /// What to call the SQL in an error message, so an analyst is told which of their two
    /// kinds of model is wrong.
    fn subject(self) -> &'static str {
        match self {
            Grain::Preserved => "transform_sql",
            Grain::PerBatch => "publish SQL",
        }
    }

    fn aggregation_allowed(self) -> bool {
        matches!(self, Grain::PerBatch)
    }
}

/// Render a statement and read it back with this engine's own parser.
///
/// Used to hand the query from the permissive parser to the native one once the syntax that
/// needed the former has been rewritten away. It is not a formality: the two parsers
/// disagree about the shape of what they read, not just about what they accept.
fn reparse_natively(statement: &SqlStatement, grain: Grain) -> Result<SqlStatement> {
    let text = statement.to_string();
    let subject = grain.subject();
    let mut parsed = DFParser::parse_sql(&text).map_err(|e| {
        Error::Config(format!(
            "{subject} uses a dialect this engine cannot run, and rewriting did not \
             resolve it ({e}). After rewriting it read: {text}"
        ))
    })?;
    match parsed.pop_front() {
        Some(Statement::Statement(inner)) => Ok(*inner),
        _ => Err(Error::Config(format!(
            "could not re-read {subject} after rewriting it: {text}"
        ))),
    }
}

/// Parse, accepting a little more than this engine's own parser does.
///
/// Trino writes a parenthesised type — `CAST(x AS ARRAY(JSON))` — and DataFusion's parser
/// insists on angle brackets, so a model containing the one construct that makes a JSON
/// payload unnestable would not get as far as being read. Falling back to a parser that
/// accepts the parenthesised form lets it in; the rewrite in
/// [`crate::transform::unnest`] then removes it, and [`validate_sql`] proves what comes out
/// parses here before anything is allowed to run.
///
/// The fallback is only ever consulted when this engine's parser has already refused, so a
/// query it can read is never interpreted by the other one.
pub(crate) fn parse_permissively(sql: &str) -> Result<std::collections::VecDeque<Statement>> {
    use deltalake::datafusion::sql::sqlparser::dialect::ClickHouseDialect;

    match DFParser::parse_sql(sql) {
        Ok(s) => Ok(s),
        Err(native) => DFParser::parse_sql_with_dialect(sql, &ClickHouseDialect {})
            // Report the native error: it is the one that describes the engine the query
            // will actually run on, and the fallback's complaint about a different grammar
            // would only mislead.
            .map_err(|_| Error::Config(format!("could not parse transform_sql: {native}"))),
    }
}

/// Parse and validate a transform SQL statement.
///
/// Returns the parsed statement so the caller does not parse twice.
pub fn validate_sql(sql: &str) -> Result<Statement> {
    validate_sql_with_lookups(sql, &BTreeSet::new())
}

/// Parse and validate a transform that may `LEFT JOIN` declared pinned lookup relations.
///
/// Lookups stay deliberately narrow: a source batch remains the only streaming input, while a
/// lookup is a Delta snapshot registered for that batch. The caller supplies only the aliases
/// that were declared in dbt; any other relation remains a configuration error.
pub fn validate_sql_with_lookups(sql: &str, lookups: &BTreeSet<String>) -> Result<Statement> {
    validate_sql_with_grain(sql, lookups, Grain::Preserved)
}

/// Parse and validate the SQL behind a `ddi_publish` model.
///
/// Same rules as a transform in every respect but one: aggregation is the point rather than
/// the headline rejection, because these rows are a message about one committed batch rather
/// than rows appended to a table. Lookups are deliberately not offered — a lookup snapshot is
/// pinned to the *source* commit's timestamp, and re-resolving one when the payload is built
/// would pin it to a different instant. Enrichment belongs in the model being published for.
pub fn validate_publish_sql(sql: &str) -> Result<Statement> {
    validate_sql_with_grain(sql, &BTreeSet::new(), Grain::PerBatch)
}

/// Validate publish SQL and return the text the engine should actually run.
pub fn normalise_publish_sql(sql: &str) -> Result<String> {
    match validate_publish_sql(sql)? {
        Statement::Statement(inner) => Ok(inner.to_string()),
        other => Ok(other.to_string()),
    }
}

fn validate_sql_with_grain(
    sql: &str,
    lookups: &BTreeSet<String>,
    grain: Grain,
) -> Result<Statement> {
    let subject = grain.subject();
    let mut statements = parse_permissively(sql)?;

    if statements.is_empty() {
        return Err(Error::Config(format!("{subject} is empty")));
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

    let mut query = match inner.as_ref() {
        SqlStatement::Query(q) => q.clone(),
        _ => {
            return Err(reject(
                "DDL/DML",
                "the transform may only read the source batch; the sink owns all writes.",
                "use SELECT ... FROM source, and create the target table with external tooling.",
            ));
        }
    };

    // Trino's spellings are turned into the forms this engine runs *before* the checks below
    // see them, which is why the join check does not reject `CROSS JOIN UNNEST`. That
    // ordering is the point: validation and execution then work on the same query, so a
    // transform cannot be accepted here and fail on its first batch.
    //
    // The JSON cast goes first and alone, because it is the only construct the fallback
    // parser was needed for. Rewriting it away means the query can be re-read by this
    // engine's own parser — and it must be, because the two parsers do not agree on the
    // *shape* of what they read: the fallback turns `UNNEST(...)` into an ordinary table
    // function, which the unnest rewrite below would not recognise. Everything after this
    // point therefore works on one parser's AST, whichever one let the text in.
    crate::transform::unnest::rewrite_json_array_casts(&mut query)?;
    rewrite_trino_from_unixtime(&mut query)?;
    if let SqlStatement::Query(q) = reparse_natively(&SqlStatement::Query(query.clone()), grain)? {
        query = q;
    }

    crate::transform::unnest::rewrite(&mut query)?;

    check_query(&query, &BTreeSet::new(), lookups, grain)?;

    let rewritten = Statement::Statement(Box::new(SqlStatement::Query(query)));

    // Everything above may have been read by the fallback parser, so prove the result is
    // something *this* engine can read before letting it near a pipeline. Without this,
    // a dialect difference elsewhere in the query would be discovered on the first batch —
    // the failure mode this whole module exists to close.
    let text = rewritten.to_string();
    DFParser::parse_sql(&text).map_err(|e| {
        Error::Config(format!(
            "{subject} uses a dialect this engine cannot run, and rewriting did not \
             resolve it ({e}). After rewriting it read: {text}"
        ))
    })?;

    Ok(rewritten)
}

/// Validate `sql` and return the text the engine should actually run.
///
/// Identical to what was written, except where a dialect spelling had to be rewritten into
/// one this engine executes -- currently Trino's `CROSS JOIN UNNEST` and named-zone
/// `from_unixtime`; see
/// [`crate::transform::unnest`] and [`rewrite_trino_from_unixtime`].
///
/// The config keeps the model's own text; only the *resolved* pipeline carries this. That
/// way `ddi dbt convert` still pins what dbt says, and what runs is still what dbt meant.
pub fn normalise_sql(sql: &str) -> Result<String> {
    normalise_sql_with_lookups(sql, &BTreeSet::new())
}

/// Validate and return the executable SQL for a transform with declared lookup aliases.
pub fn normalise_sql_with_lookups(sql: &str, lookups: &BTreeSet<String>) -> Result<String> {
    match validate_sql_with_lookups(sql, lookups)? {
        Statement::Statement(inner) => Ok(inner.to_string()),
        other => Ok(other.to_string()),
    }
}

/// Replace Trino's `from_unixtime(<seconds>, 'Europe/Amsterdam')` with the DataFusion
/// equivalent.
///
/// `to_timestamp_seconds` converts its numeric input into a timestamp, but without a session
/// timezone it is an unzoned timestamp. Casting that directly to Amsterdam would interpret the
/// epoch as Amsterdam wall time, shifting the instant. The intermediate `AT TIME ZONE 'UTC'`
/// gives the epoch its correct origin first; the second conversion changes only its named
/// timezone. This preserves local dates across daylight-saving changes without changing the
/// session timezone for unrelated expressions in the same transform.
fn rewrite_trino_from_unixtime(query: &mut Query) -> Result<()> {
    struct V(Option<Error>);

    impl VisitorMut for V {
        type Break = ();

        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            let replacement = match from_unixtime_replacement(expr) {
                Ok(replacement) => replacement,
                Err(e) => {
                    self.0 = Some(e);
                    return ControlFlow::Break(());
                }
            };
            if let Some(replacement) = replacement {
                *expr = replacement;
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

/// Return a replacement for one `from_unixtime` call, or `None` for every other function.
fn from_unixtime_replacement(expr: &Expr) -> Result<Option<Expr>> {
    let Expr::Function(function) = expr else {
        return Ok(None);
    };
    if !function
        .name
        .to_string()
        .eq_ignore_ascii_case("from_unixtime")
    {
        return Ok(None);
    }

    let unsupported = || {
        reject(
            "from_unixtime",
            "this runtime supports only Trino's from_unixtime(<unix seconds>, \
             'Europe/Amsterdam') form.",
            "use that exact form, or express an explicit DataFusion timestamp conversion.",
        )
    };

    let FunctionArguments::List(arguments) = &function.args else {
        return Err(unsupported());
    };
    let [seconds, timezone] = arguments.args.as_slice() else {
        return Err(unsupported());
    };
    let (
        FunctionArg::Unnamed(FunctionArgExpr::Expr(_)),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(timezone))),
    ) = (seconds, timezone)
    else {
        return Err(unsupported());
    };
    if !matches!(
        &timezone.value,
        Value::SingleQuotedString(zone) if zone == TRINO_FROM_UNIXTIME_TIME_ZONE
    ) {
        return Err(unsupported());
    }
    if function.uses_odbc_syntax
        || !matches!(&function.parameters, FunctionArguments::None)
        || arguments.duplicate_treatment.is_some()
        || !arguments.clauses.is_empty()
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(unsupported());
    }

    let mut converted = function.clone();
    converted.name = ObjectName::from(Ident::new("to_timestamp_seconds"));
    let FunctionArguments::List(arguments) = &mut converted.args else {
        unreachable!("checked the function argument form above");
    };
    arguments.args.truncate(1);

    let timestamp = Expr::Function(converted);
    let as_utc = Expr::AtTimeZone {
        timestamp: Box::new(timestamp),
        time_zone: Box::new(timezone_literal("UTC")),
    };
    Ok(Some(Expr::AtTimeZone {
        timestamp: Box::new(as_utc),
        time_zone: Box::new(timezone_literal(TRINO_FROM_UNIXTIME_TIME_ZONE)),
    }))
}

fn timezone_literal(value: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.to_owned()).into())
}

/// Check one query, given the CTE names already in scope from enclosing queries.
///
/// `scope` matters because the checks recurse: a CTE body is examined on its own, and it
/// may legitimately refer to a CTE its parent declared. Without inheriting those names,
/// `WITH a AS (...), b AS (SELECT * FROM a) ...` would report `a` as a foreign table.
fn check_query(
    query: &Query,
    scope: &BTreeSet<String>,
    lookups: &BTreeSet<String>,
    grain: Grain,
) -> Result<()> {
    // Names this query adds, visible to its own CTE bodies and to its body.
    let mut inner = scope.clone();
    inner.extend(cte_names(query));

    if let Some(name) = cte_names(query)
        .into_iter()
        .find(|name| lookups.contains(name))
    {
        return Err(reject(
            &format!("the CTE {name:?}"),
            "it shadows a declared lookup relation, so the SQL would not use the pinned \
             Delta snapshot it declared.",
            "rename the CTE, or join the lookup by its declared name.",
        ));
    }

    // A CTE body is a SELECT like any other and needs the same checks. The visitor below
    // walks the whole tree, but it only knows about window functions, aggregate *calls*
    // and foreign relations — it never sees GROUP BY, DISTINCT or a join, because those
    // live on the Select node rather than in an expression. So `WITH base AS (SELECT
    // DISTINCT ...) SELECT * FROM base` slipped through until this recursed.
    for cte in query.with.iter().flat_map(|w| w.cte_tables.iter()) {
        check_query(&cte.query, &inner, lookups, grain)?;
    }
    check_set_expr(&query.body, &inner, lookups, grain)?;

    // Window functions and relation references can appear in ORDER BY / expressions anywhere.
    // Count the lookup references that are valid direct LEFT JOINs before the general visitor
    // walks expression subqueries too. A lookup inside `WHERE EXISTS (...)`, for example,
    // would otherwise look registered but would let the lookup filter source rows.
    let allowed_lookup_refs = lookup_join_count(query, lookups);
    let mut v = StatefulConstructVisitor {
        ctes: inner,
        lookups: lookups.clone(),
        grain,
        ..Default::default()
    };
    let mut q = query.clone();
    let _ = q.visit(&mut v);
    if let Some(err) = v.found {
        return Err(err);
    }
    if v.lookup_refs != allowed_lookup_refs {
        return Err(reject(
            "a lookup outside a direct LEFT JOIN",
            "a pinned lookup may enrich a source row but may not drive a subquery, filter, or \
             second source of rows.",
            "reference the declared lookup only as `LEFT JOIN <lookup> ON ...`.",
        ));
    }
    Ok(())
}

fn check_set_expr(
    body: &SetExpr,
    scope: &BTreeSet<String>,
    lookups: &BTreeSet<String>,
    grain: Grain,
) -> Result<()> {
    match body {
        SetExpr::Select(select) => check_select(select, scope, lookups, grain),
        SetExpr::Query(q) => check_query(q, scope, lookups, grain),
        SetExpr::SetOperation { left, right, .. } => {
            check_set_expr(left, scope, lookups, grain)?;
            check_set_expr(right, scope, lookups, grain)
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

fn check_select(
    select: &Select,
    scope: &BTreeSet<String>,
    lookups: &BTreeSet<String>,
    grain: Grain,
) -> Result<()> {
    // GROUP BY — the headline rejection for a transform, and the entire point of a
    // publication. A published row describes one committed batch and is never stored, so a
    // partial sum is what the client is asking for; it applies the delta on top of a baseline
    // it read from the same model. See [`Grain`].
    let grouped = match &select.group_by {
        deltalake::datafusion::sql::sqlparser::ast::GroupByExpr::All(_) => true,
        deltalake::datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            !exprs.is_empty()
        }
    };
    if grouped && !grain.aggregation_allowed() {
        return Err(reject(
            "GROUP BY",
            "this tool preserves grain: a group that spans batches would emit partial \
             results, and the output would stop being append-only.",
            "use array_sum(line_items, 'price * qty') for intra-row aggregation, or \
             aggregate downstream.",
        ));
    }

    if select.having.is_some() && !grain.aggregation_allowed() {
        return Err(reject(
            "HAVING",
            "it only exists alongside aggregation.",
            "filter rows with WHERE, or aggregate downstream.",
        ));
    }

    // DISTINCT is GROUP BY over every selected column, so refusing it while allowing the
    // spelling that means the same thing would only teach analysts to write the other one.
    if select.distinct.is_some() && !grain.aggregation_allowed() {
        return Err(reject(
            "DISTINCT",
            "deduplication is cross-row state: rows in an earlier batch are already \
             committed and cannot be compared against.",
            "deduplicate downstream with a MERGE, or accept append-only duplicates.",
        ));
    }

    // Spark's `LATERAL VIEW explode(...)`. The parser accepts it, so without this it passed
    // the load-time gate and then failed on the first batch with "This feature is not
    // implemented: LATERAL VIEWS" — exactly the outcome this module exists to prevent.
    if !select.lateral_views.is_empty() {
        return Err(reject(
            "LATERAL VIEW",
            "this engine does not implement Spark's LATERAL VIEW.",
            "write it as ANSI SQL — `FROM source o CROSS JOIN UNNEST(o.<array>) AS t(x)` — \
             which is rewritten to the engine's own spelling automatically and means the \
             same thing.",
        ));
    }

    // Two FROM items separated by a comma is a join written the old way. It has to be
    // caught here rather than by the per-item `joins` check below, which only sees the
    // `a JOIN b` spelling — and by the relation check in the visitor, which cannot tell
    // `FROM source a, source b` from a plain `FROM source`.
    if select.from.len() > 1 {
        return Err(reject(
            "a comma-separated FROM list (an implicit cross join)",
            "a join against a table that can change between batches makes output \
             non-reproducible, and joining the source to itself is cross-row state.",
            "LEFT JOIN a declared pinned lookup with an ON predicate, or enrich downstream.",
        ));
    }

    for twj in &select.from {
        check_base_table_factor(&twj.relation, scope, lookups, grain)?;
        for join in &twj.joins {
            check_lookup_join(join, lookups)?;
        }
    }

    Ok(())
}

fn check_base_table_factor(
    tf: &TableFactor,
    scope: &BTreeSet<String>,
    lookups: &BTreeSet<String>,
    grain: Grain,
) -> Result<()> {
    match tf {
        TableFactor::Table { .. } if !is_plain_table_relation(tf) => Err(reject(
            "a table-valued or modified FROM relation",
            "the stream executor registers only plain source and lookup relations, not table \
             functions, time-travel references, samples, or engine-specific table modifiers.",
            "read the plain source relation (or a source-derived CTE), optionally LEFT JOINing \
             a declared lookup.",
        )),
        TableFactor::Table { name, .. } if relation_is_lookup(name, lookups) => Err(reject(
            "a lookup as the primary FROM relation",
            "a lookup does not advance an offset and cannot define the output grain.",
            "start from source (or a source-derived CTE) and LEFT JOIN the lookup.",
        )),
        TableFactor::Table { .. } => Ok(()),
        TableFactor::Derived { subquery, .. } => check_query(subquery, scope, lookups, grain),
        TableFactor::UNNEST { .. } => Ok(()),
        TableFactor::NestedJoin { .. } => Err(reject(
            "JOIN",
            "a join against a table that can change between batches makes output \
             non-reproducible.",
            "LEFT JOIN a declared pinned lookup with an ON predicate, or enrich downstream.",
        )),
        other => Err(reject(
            &format!("this FROM relation form ({other:?})"),
            "the stream executor only registers the source batch and declared Delta lookups.",
            "read source (or a source-derived CTE), optionally LEFT JOINing a declared lookup.",
        )),
    }
}

/// Lookup joins are intentionally more constrained than ordinary SQL joins. A lookup is a
/// snapshot selected from the source commit's timestamp, not a second stream: it can enrich a
/// source row, but it may not drive, filter, or cross-product the source batch.
fn check_lookup_join(join: &Join, lookups: &BTreeSet<String>) -> Result<()> {
    let on = match &join.join_operator {
        JoinOperator::LeftOuter(JoinConstraint::On(on))
        | JoinOperator::Left(JoinConstraint::On(on)) => on,
        _ => {
            return Err(reject(
                "JOIN",
                "only LEFT JOIN ... ON is safe for a pinned lookup; inner, right, full and cross \
                 joins can remove or multiply source rows.",
                "LEFT JOIN a declared lookup with an ON predicate, or enrich downstream.",
            ));
        }
    };

    let TableFactor::Table { name, alias, .. } = &join.relation else {
        return Err(reject(
            "JOIN to this relation form",
            "a lookup must be a declared Delta table registered under its own name.",
            "LEFT JOIN the declared lookup name directly.",
        ));
    };
    if !is_plain_table_relation(&join.relation) {
        return Err(reject(
            "a table-valued or modified lookup relation",
            "a pinned lookup must be the plain Delta relation registered for this batch, not a \
             table function, time-travel reference, sample, or engine-specific modifier.",
            "LEFT JOIN the declared lookup name directly.",
        ));
    }
    if !relation_is_lookup(name, lookups) {
        return Err(reject(
            &format!("the joined table {:?}", name.to_string()),
            "it is not a declared pinned lookup, so its snapshot cannot be reproduced on retry.",
            "declare it as a dbt source with meta.ddi_lookup, or enrich downstream.",
        ));
    }
    // The relation is registered under its declared lookup name, but SQL may give it an
    // ordinary table alias. The ON predicate has to use the alias when it has one.
    let lookup_qualifier = alias
        .as_ref()
        .map(|alias| alias.name.value.to_ascii_lowercase())
        .unwrap_or_else(|| {
            name.0
                .first()
                .and_then(|part| part.as_ident())
                .expect("relation_is_lookup checked the single identifier")
                .value
                .to_ascii_lowercase()
        });
    if !has_lookup_key_equality(on, &lookup_qualifier) {
        return Err(reject(
            "the lookup JOIN predicate",
            "it must contain an equality between a lookup-qualified column and a source- or \
             CTE-qualified column. An unbounded predicate can multiply source rows.",
            "join `lookup.key = source_or_cte.key` (additional AND conditions are fine).",
        ));
    }
    Ok(())
}

/// Whether an ON expression has at least one equality that relates the lookup to an existing
/// source/CTE relation. This is intentionally narrow: it rejects `ON true` and predicates that
/// only filter the lookup, both of which can turn an enrichment into a cross product.
fn has_lookup_key_equality(expr: &Expr, lookup_name: &str) -> bool {
    match expr {
        Expr::Nested(inner) => has_lookup_key_equality(inner, lookup_name),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            has_lookup_key_equality(left, lookup_name)
                || has_lookup_key_equality(right, lookup_name)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let left_qualifiers = expression_qualifiers(left);
            let right_qualifiers = expression_qualifiers(right);
            let left_has_lookup = left_qualifiers.contains(lookup_name);
            let right_has_lookup = right_qualifiers.contains(lookup_name);
            let left_has_other = left_qualifiers
                .iter()
                .any(|name| name.as_str() != lookup_name);
            let right_has_other = right_qualifiers
                .iter()
                .any(|name| name.as_str() != lookup_name);
            // One side may compute or normalise a key (`lower(fx.currency)` is valid), but
            // it may not also reference the other side. That rules out predicates such as
            // `fx.k = coalesce(fx.k, source.k)`, which look keyed while still admitting every
            // lookup row when `fx.k` is present.
            (left_has_lookup && !left_has_other && right_has_other && !right_has_lookup)
                || (right_has_lookup && !right_has_other && left_has_other && !left_has_lookup)
        }
        _ => false,
    }
}

/// Relation qualifiers mentioned by an expression. Unqualified columns deliberately do not
/// count: requiring both sides to be explicit is what lets the validator distinguish a key match
/// from `lookup.active = true`.
fn expression_qualifiers(expr: &Expr) -> BTreeSet<String> {
    #[derive(Default)]
    struct Qualifiers(BTreeSet<String>);

    impl VisitorMut for Qualifiers {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
            if let Expr::CompoundIdentifier(parts) = expr {
                if let Some(first) = parts.first() {
                    self.0.insert(first.value.to_ascii_lowercase());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut copy = expr.clone();
    let mut qualifiers = Qualifiers::default();
    let _ = copy.visit(&mut qualifiers);
    qualifiers.0
}

fn relation_is_lookup(relation: &ObjectName, lookups: &BTreeSet<String>) -> bool {
    relation.0.len() == 1
        && relation
            .0
            .first()
            .and_then(|part| part.as_ident())
            .map(|ident| lookups.contains(&ident.value.to_ascii_lowercase()))
            .unwrap_or(false)
}

/// Only a normal named relation can be the source or a pinned lookup. `TableFactor::Table`
/// also represents table-valued calls and several dialect-specific modifiers, none of which is
/// a relation the executor registered in its isolated session.
pub(crate) fn is_plain_table_relation(factor: &TableFactor) -> bool {
    matches!(
        factor,
        TableFactor::Table {
            args: None,
            with_hints,
            version: None,
            with_ordinality: false,
            partitions,
            json_path: None,
            sample: None,
            index_hints,
            ..
        } if with_hints.is_empty() && partitions.is_empty() && index_hints.is_empty()
    )
}

/// Number of lookup relation references that occur in the only permitted place: directly on a
/// `LEFT JOIN ... ON`. It deliberately does not descend into expression subqueries; the generic
/// visitor sees those references and rejects the mismatch with this count.
fn lookup_join_count(query: &Query, lookups: &BTreeSet<String>) -> usize {
    let ctes = query
        .with
        .iter()
        .flat_map(|with| with.cte_tables.iter())
        .map(|cte| lookup_join_count(&cte.query, lookups))
        .sum::<usize>();
    ctes + lookup_join_count_set(&query.body, lookups)
}

fn lookup_join_count_set(body: &SetExpr, lookups: &BTreeSet<String>) -> usize {
    match body {
        SetExpr::Select(select) => lookup_join_count_select(select, lookups),
        SetExpr::Query(query) => lookup_join_count(query, lookups),
        SetExpr::SetOperation { left, right, .. } => {
            lookup_join_count_set(left, lookups) + lookup_join_count_set(right, lookups)
        }
        _ => 0,
    }
}

fn lookup_join_count_select(select: &Select, lookups: &BTreeSet<String>) -> usize {
    select
        .from
        .iter()
        .map(|table_with_joins| {
            lookup_join_count_factor(&table_with_joins.relation, lookups)
                + table_with_joins
                    .joins
                    .iter()
                    .map(|join| {
                        usize::from(matches!(
                            &join.relation,
                            TableFactor::Table { name, .. } if relation_is_lookup(name, lookups)
                        )) + lookup_join_count_factor(&join.relation, lookups)
                    })
                    .sum::<usize>()
        })
        .sum()
}

fn lookup_join_count_factor(factor: &TableFactor, lookups: &BTreeSet<String>) -> usize {
    match factor {
        TableFactor::Derived { subquery, .. } => lookup_join_count(subquery, lookups),
        _ => 0,
    }
}

/// The CTE names a query introduces, lower-cased.
pub(crate) fn cte_names(query: &Query) -> Vec<String> {
    query
        .with
        .iter()
        .flat_map(|w| {
            w.cte_tables
                .iter()
                .map(|c| c.alias.name.value.to_ascii_lowercase())
        })
        .collect()
}

/// True when `relation` refers to a CTE rather than a stored table.
///
/// Only an unqualified single-part name can be a CTE; `schema.name` is always a table.
pub(crate) fn is_cte(relation: &ObjectName, ctes: &BTreeSet<String>) -> bool {
    relation.0.len() == 1
        && relation
            .0
            .first()
            .and_then(|p| p.as_ident())
            .map(|i| ctes.contains(&i.value.to_ascii_lowercase()))
            .unwrap_or(false)
}

/// Catches window functions and aggregate calls wherever they hide.
#[derive(Default)]
struct StatefulConstructVisitor {
    found: Option<Error>,
    ctes: BTreeSet<String>,
    lookups: BTreeSet<String>,
    lookup_refs: usize,
    grain: Grain,
}

/// Aggregates a client can apply as a delta on top of a baseline.
///
/// Strictly narrower than [`AGGREGATES`], and the narrowing is the whole safety argument for
/// letting a publication aggregate at all. A published row is combined with what the client
/// already holds — `+sales_delta`, `count + orders_delta` — so only functions that are
/// monoids under that combination can be correct. `avg` is the canonical counter-example:
/// the average of two batches is not the average of their averages, so a per-batch `avg`
/// published as a delta is silently wrong on every dashboard that applies it. Same for
/// `median`, `mode`, `stddev*`, `var*`, `string_agg` and `listagg`.
///
/// It is also what keeps the useful duality: the same model is a delta over one batch and a
/// running total over the whole table, which is why a client can reload it as the baseline
/// after a gap. That stops being true the moment a non-combinable aggregate is allowed.
const COMBINABLE_AGGREGATES: &[&str] = &["sum", "count", "min", "max"];

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
    /// Only `source` and declared pinned lookups are registered on the session, so any other
    /// name could only ever fail at plan time — on the first batch, in production, from a
    /// daemon that started clean and passed `ddi validate`. Naming it here keeps the promise
    /// that a pipeline which cannot be correct never starts.
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.ctes.extend(cte_names(query));
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        if self.found.is_some() {
            return ControlFlow::Break(());
        }
        // A CTE is a name the query defines for itself, not a second table to read.
        if is_cte(relation, &self.ctes) {
            return ControlFlow::Continue(());
        }
        let full = relation.to_string();
        let bare = full
            .rsplit('.')
            .next()
            .unwrap_or(&full)
            .trim_matches('"')
            .to_ascii_lowercase();
        if self.lookups.contains(&bare) {
            self.lookup_refs += 1;
            return ControlFlow::Continue(());
        }
        if relation.0.len() != 1 || bare != SOURCE_TABLE {
            self.found = Some(reject(
                &format!("the table {full:?}"),
                &format!(
                    "a transform may only read the batch it was given, which is registered \
                     as the unqualified relation {SOURCE_TABLE:?}. Reading a second table means joining against \
                     something that can change between batches, which makes the output \
                     non-reproducible."
                ),
                "LEFT JOIN a declared pinned lookup with an ON predicate, or enrich downstream.",
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
                if !self.grain.aggregation_allowed() {
                    self.found = Some(reject(
                        &format!("the aggregate function {bare}()"),
                        "it combines values across rows, which cannot be correct when rows \
                         arrive in independent batches.",
                        "use array_sum / array_min / array_max / array_avg / array_length for \
                         intra-row aggregation over an array column, or aggregate downstream.",
                    ));
                    return ControlFlow::Break(());
                }
                // Aggregation is legal here, but only for functions whose per-batch result
                // can be combined with what a client already holds. See
                // [`COMBINABLE_AGGREGATES`].
                if !COMBINABLE_AGGREGATES.contains(&bare.as_str()) {
                    self.found = Some(reject(
                        &format!("the aggregate function {bare}()"),
                        "a published row is a delta the client adds to a baseline it already \
                         has, and this function does not combine that way: the value for two \
                         batches is not a function of the value for each.",
                        "publish sum() and count() and derive it in the model, so the same \
                         SQL is a delta over one batch and the baseline over the whole table.",
                    ));
                    return ControlFlow::Break(());
                }
                // `count(DISTINCT x)` is the same problem wearing the name of one that is
                // fine: this batch cannot know which values the last one already counted.
                if let FunctionArguments::List(list) = &f.args {
                    if list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
                        self.found = Some(reject(
                            &format!("{bare}(DISTINCT ...)"),
                            "distinctness cannot be combined across batches: this batch does \
                             not know which values an earlier one already counted, so the \
                             delta would double-count every value that repeats.",
                            "publish the distinct keys themselves and let the client count \
                             them, or count without DISTINCT.",
                        ));
                        return ControlFlow::Break(());
                    }
                }
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

    fn lookup_names() -> BTreeSet<String> {
        ["fx_rates".to_string()].into_iter().collect()
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
    fn spark_lateral_view_is_refused_at_load_not_on_the_first_batch() {
        // It parses, so it used to sail through the gate and then fail at planning time —
        // the one thing this module exists to prevent.
        let e = validate_sql(
            "SELECT order_id, li FROM source LATERAL VIEW explode(line_items) t AS li",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("LATERAL VIEW"), "got: {e}");
        assert!(e.contains("CROSS JOIN UNNEST"), "points at the fix: {e}");
    }

    #[test]
    fn an_array_cast_out_of_a_json_blob_is_accepted_in_either_spelling() {
        // Trino writes `ARRAY(JSON)`; this engine's own parser insists on `ARRAY<JSON>`.
        // Both have to mean the same thing, or a model that runs in the warehouse would be
        // refused here — which is the whole reason the permissive parser exists.
        let trino = normalise_sql(
            "SELECT o.order_id, json_extract_scalar(li, '$.sku') AS sku FROM source o \
             CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY(JSON))) AS t(li)",
        )
        .expect("the Trino spelling is the one a dbt model contains");
        let native = normalise_sql(
            "SELECT o.order_id, json_extract_scalar(li, '$.sku') AS sku FROM source o \
             CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY<JSON>)) AS t(li)",
        )
        .unwrap();
        assert_eq!(trino, native, "both spellings must resolve to one query");
        assert!(
            trino.contains("json_array_elements(json_extract(o.data, '$.lines'))"),
            "the cast became the function that builds a real list: {trino}"
        );
    }

    #[test]
    fn a_json_array_cast_outside_unnest_is_still_rewritten() {
        // It is a cast, not an unnest feature: wherever it appears it has to become the
        // function, or the query would reach the engine with a cast no kernel implements.
        let got =
            normalise_sql("SELECT order_id, json_array_elements(data) AS l FROM source").unwrap();
        assert!(got.contains("json_array_elements(data)"), "got: {got}");
    }

    #[test]
    fn named_trino_from_unixtime_is_normalised_with_an_explicit_timezone() {
        let got = normalise_sql(
            "SELECT CAST(from_unixtime(event_epoch / 1000, 'Europe/Amsterdam') AS DATE) \
             AS local_date FROM source",
        )
        .expect("the model's Trino spelling should be executable by DataFusion");

        assert!(
            got.contains("to_timestamp_seconds(event_epoch / 1000)"),
            "the epoch conversion was retained: {got}"
        );
        assert!(
            got.contains("AT TIME ZONE 'UTC'"),
            "the epoch has an explicit UTC origin: {got}"
        );
        assert!(
            got.contains("AT TIME ZONE 'Europe/Amsterdam'"),
            "the calendar timezone remains Amsterdam: {got}"
        );
        assert!(
            !got.to_ascii_lowercase().contains("from_unixtime"),
            "the unsupported function was fully removed: {got}"
        );
    }

    #[test]
    fn unsupported_trino_from_unixtime_variant_is_rejected_at_config_load() {
        let err = normalise_sql("SELECT from_unixtime(event_epoch, 'UTC') FROM source")
            .expect_err("leaving an unsupported function for first-batch planning is unsafe");
        assert!(err.to_string().contains("from_unixtime"), "got: {err}");
    }

    #[test]
    fn the_trino_spelling_of_unnest_is_accepted() {
        // Row-local: no second table, nothing to pin, no cross-row state. It was rejected as
        // a JOIN until the rewrite ran before these checks.
        validate_sql(
            "SELECT o.order_id, li.sku FROM source o CROSS JOIN UNNEST(o.line_items) AS t(li)",
        )
        .expect("UNNEST of this row's own column is not a join");
        validate_sql("SELECT o.order_id, li.sku FROM source o, UNNEST(o.line_items) AS t(li)")
            .expect("the older comma spelling means the same thing");
    }

    #[test]
    fn a_real_join_is_still_rejected() {
        // The rewrite must not have opened a door: an undeclared relation is still refused.
        let e = validate_sql("SELECT a.x FROM source a CROSS JOIN customers c")
            .unwrap_err()
            .to_string();
        assert!(e.contains("JOIN"), "got: {e}");
    }

    #[test]
    fn a_fan_out_may_carry_a_lookup_join_but_not_a_second_table() {
        let lookups = lookup_names();

        // The unnest rewrite moves the fan-out into a derived table and rebuilds the `FROM`,
        // so the lookup join has to be carried across that rebuild rather than dropped with
        // it. A lookup adds columns, never rows, so it is indifferent to the expansion.
        validate_sql_with_lookups(
            "SELECT o.order_id, li.sku, fx_rates.exchange_rate \
             FROM source AS o \
             CROSS JOIN UNNEST(o.line_items) AS t(li) \
             LEFT JOIN fx_rates ON fx_rates.currency = o.currency",
            &lookups,
        )
        .expect("a pinned lookup survives a fan-out of the row it enriches");

        // Carrying joins over must not become a way to smuggle one past the checks: what is
        // rejected beside a plain source is still rejected beside a fan-out.
        for (sql, want) in [
            (
                "SELECT o.order_id, li.sku, p.name FROM source AS o \
                 CROSS JOIN UNNEST(o.line_items) AS t(li) \
                 LEFT JOIN products AS p ON p.sku = li.sku",
                "products",
            ),
            (
                "SELECT o.order_id, li.sku FROM source AS o \
                 CROSS JOIN UNNEST(o.line_items) AS t(li) \
                 INNER JOIN fx_rates ON fx_rates.currency = o.currency",
                "JOIN",
            ),
        ] {
            let e = validate_sql_with_lookups(sql, &lookups)
                .unwrap_err()
                .to_string();
            assert!(e.contains(want), "{sql}: {e}");
        }
    }

    #[test]
    fn a_declared_lookup_can_only_be_a_direct_left_join() {
        let lookups = lookup_names();
        validate_sql_with_lookups(
            "SELECT orders.order_id, fx_rates.exchange_rate \
             FROM source AS orders LEFT JOIN fx_rates \
             ON fx_rates.currency = orders.currency",
            &lookups,
        )
        .expect("a declared pinned lookup may enrich a source row");

        let e = validate_sql_with_lookups(
            "SELECT order_id FROM source \
             WHERE EXISTS (SELECT 1 FROM fx_rates)",
            &lookups,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("direct LEFT JOIN"), "got: {e}");

        let e = validate_sql_with_lookups(
            "SELECT orders.order_id FROM source AS orders \
             LEFT JOIN fx_rates ON true",
            &lookups,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("equality"), "got: {e}");

        for sql in [
            "SELECT order_id FROM source()",
            "SELECT orders.order_id FROM source AS orders \
             LEFT JOIN fx_rates() ON fx_rates.currency = orders.currency",
        ] {
            let e = validate_sql_with_lookups(sql, &lookups)
                .unwrap_err()
                .to_string();
            assert!(e.contains("table-valued"), "{sql}: {e}");
        }

        for sql in [
            "SELECT order_id FROM source WHERE currency IN (SELECT currency FROM fx_rates)",
            "SELECT order_id, (SELECT exchange_rate FROM fx_rates LIMIT 1) AS rate FROM source",
        ] {
            let e = validate_sql_with_lookups(sql, &lookups)
                .unwrap_err()
                .to_string();
            assert!(e.contains("direct LEFT JOIN"), "{sql}: {e}");
        }

        validate_sql_with_lookups(
            "WITH items AS (SELECT order_id, currency FROM source) \
             SELECT items.order_id, fx_rates.exchange_rate \
             FROM items LEFT JOIN fx_rates \
             ON lower(fx_rates.currency) = lower(items.currency)",
            &lookups,
        )
        .expect("a source-derived CTE may be enriched by a lookup");

        validate_sql_with_lookups(
            "SELECT orders.order_id, fx.exchange_rate \
             FROM source AS orders LEFT JOIN fx_rates AS fx \
             ON fx.currency = orders.currency",
            &lookups,
        )
        .expect("a lookup may use a normal SQL alias");

        let e = validate_sql_with_lookups(
            "SELECT orders.order_id FROM source AS orders \
             LEFT JOIN fx_rates AS fx \
             ON fx.currency = coalesce(fx.currency, orders.currency)",
            &lookups,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("equality"), "got: {e}");
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
        assert!(e.to_string().contains("pinned lookup"), "got: {e}");
    }

    #[test]
    fn a_second_table_in_a_derived_table_is_rejected() {
        let e = validate_sql("SELECT order_id FROM (SELECT order_id FROM products)").unwrap_err();
        assert!(e.to_string().contains("products"), "got: {e}");
    }

    // Every stateful construct, hidden one level down in a CTE. Reported from a real
    // project: `WITH base AS (SELECT DISTINCT ...) SELECT ... FROM base` was accepted and
    // would have streamed silently wrong output.
    #[test]
    fn distinct_inside_a_cte_is_rejected() {
        let e =
            validate_sql("WITH base AS (SELECT DISTINCT a, b FROM source) SELECT a, b FROM base")
                .unwrap_err();
        assert!(e.to_string().contains("DISTINCT"), "got: {e}");
    }

    #[test]
    fn a_bare_group_by_inside_a_cte_is_rejected() {
        // No aggregate function call, so nothing trips the expression visitor. Only
        // walking the CTE's own SELECT catches it.
        let e = validate_sql("WITH base AS (SELECT a FROM source GROUP BY a) SELECT a FROM base")
            .unwrap_err();
        assert!(e.to_string().contains("GROUP BY"), "got: {e}");
    }

    #[test]
    fn a_join_inside_a_cte_is_rejected() {
        let e = validate_sql(
            "WITH base AS (SELECT x.a FROM source x JOIN source y ON x.a = y.a)              SELECT a FROM base",
        )
        .unwrap_err();
        assert!(e.to_string().to_lowercase().contains("join"), "got: {e}");
    }

    #[test]
    fn a_comma_join_inside_a_cte_is_rejected() {
        let e =
            validate_sql("WITH base AS (SELECT x.a FROM source x, source y) SELECT a FROM base")
                .unwrap_err();
        assert!(e.to_string().contains("cross join"), "got: {e}");
    }

    #[test]
    fn a_stateful_construct_nested_two_ctes_deep_is_rejected() {
        let e = validate_sql(
            "WITH a AS (SELECT x FROM source), b AS (SELECT DISTINCT x FROM a)              SELECT x FROM b",
        )
        .unwrap_err();
        assert!(e.to_string().contains("DISTINCT"), "got: {e}");
    }

    #[test]
    fn a_cte_is_not_mistaken_for_a_second_table() {
        // How dbt writes its staging models: `with source as (...), renamed as (...)`.
        // Counting those names as tables would reject every one of them.
        validate_sql(
            "WITH src AS (SELECT * FROM source), renamed AS (SELECT id AS order_id FROM src) \
             SELECT * FROM renamed",
        )
        .unwrap();
    }

    #[test]
    fn a_cte_named_source_still_resolves_to_the_source_batch() {
        // dbt literally names its first CTE `source`, which collides with ours.
        validate_sql("WITH source AS (SELECT * FROM source) SELECT id FROM source").unwrap();
    }

    #[test]
    fn a_real_table_inside_a_cte_body_is_still_rejected() {
        // The CTE exemption must not become a hiding place.
        let e = validate_sql("WITH x AS (SELECT * FROM products) SELECT * FROM x").unwrap_err();
        assert!(e.to_string().contains("products"), "got: {e}");
    }

    #[test]
    fn a_qualified_name_for_the_source_is_rejected() {
        // The executor registers precisely `source`, never a catalog/schema relation. Accepting
        // a tail match would pass validation and then fail when the first batch is planned.
        let e = validate_sql("SELECT order_id FROM public.source").unwrap_err();
        assert!(e.to_string().contains("unqualified"), "got: {e}");
    }

    #[test]
    fn join_is_rejected() {
        let e = err_of("SELECT a.x FROM source a JOIN other b ON a.k = b.k");
        assert!(e.contains("JOIN"), "got: {e}");
        assert!(
            e.contains("LEFT JOIN"),
            "should point at the allowed shape, got: {e}"
        );
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

    // ---- Grain::PerBatch — publish SQL ----
    //
    // The rule these all circle is that a publication is a message about one committed
    // batch, so aggregating is the point; but a client applies it as a delta, so only
    // aggregates that combine that way can be allowed.

    fn publish_err_of(sql: &str) -> String {
        validate_publish_sql(sql)
            .err()
            .unwrap_or_else(|| panic!("expected publish SQL {sql:?} to be rejected"))
            .to_string()
    }

    #[test]
    fn group_by_is_allowed_per_batch_and_still_rejected_for_a_transform() {
        let sql = "SELECT country, sum(amount) AS sales_delta, count(*) AS orders_delta \
                   FROM source GROUP BY country";
        validate_publish_sql(sql).expect("a per-batch aggregation is what a publication is");
        let e = err_of(sql);
        assert!(e.contains("GROUP BY is not supported"), "got: {e}");
    }

    #[test]
    fn avg_is_rejected_per_batch_and_names_sum_and_count() {
        let e = publish_err_of("SELECT country, avg(amount) AS a FROM source GROUP BY country");
        assert!(e.contains("avg()"), "got: {e}");
        assert!(e.contains("sum() and count()"), "got: {e}");
    }

    #[test]
    fn the_other_non_combinable_aggregates_are_rejected_per_batch() {
        for f in [
            "median(amount)",
            "stddev(amount)",
            "var_pop(amount)",
            "mode(amount)",
            "string_agg(name, ',')",
            "array_agg(name)",
        ] {
            let sql = format!("SELECT country, {f} AS x FROM source GROUP BY country");
            let e = publish_err_of(&sql);
            assert!(
                e.contains("does not combine that way"),
                "expected {f} to be refused as non-combinable, got: {e}"
            );
        }
    }

    #[test]
    fn the_combinable_aggregates_are_allowed_per_batch() {
        validate_publish_sql(
            "SELECT country, sum(amount) AS s, count(*) AS c, min(amount) AS lo, \
             max(amount) AS hi FROM source GROUP BY country",
        )
        .expect("sum/count/min/max are exactly the ones a client can apply as a delta");
    }

    #[test]
    fn count_distinct_is_rejected_per_batch() {
        let e = publish_err_of(
            "SELECT country, count(DISTINCT customer_id) AS c FROM source GROUP BY country",
        );
        assert!(e.contains("DISTINCT"), "got: {e}");
        assert!(e.contains("double-count"), "got: {e}");
    }

    #[test]
    fn a_bare_aggregate_with_no_group_by_is_a_valid_publication() {
        // One row describing the whole batch is a legitimate dashboard payload, and is the
        // shape a single-number tile wants.
        validate_publish_sql("SELECT sum(amount) AS total, count(*) AS n FROM source").unwrap();
    }

    #[test]
    fn window_functions_stay_rejected_per_batch() {
        // Row order within a batch is an accident of file order, not a fact about the data.
        let e = publish_err_of("SELECT row_number() OVER (ORDER BY x) AS r FROM source");
        assert!(e.contains("window functions (OVER)"), "got: {e}");
    }

    #[test]
    fn a_publish_model_still_reads_only_the_source_batch() {
        // The rules that are not about aggregation must be identical, because the batch is
        // still the only thing in memory when the payload is built.
        let e = publish_err_of(
            "SELECT c.name, sum(s.amount) AS x FROM source s, countries c \
                                GROUP BY c.name",
        );
        assert!(e.contains("not supported"), "got: {e}");
    }

    #[test]
    fn publish_errors_name_publish_sql_rather_than_transform_sql() {
        let e = validate_publish_sql("   ").unwrap_err().to_string();
        assert!(e.contains("publish SQL is empty"), "got: {e}");
    }

    #[test]
    fn distinct_is_allowed_per_batch_because_group_by_over_the_same_columns_is() {
        validate_publish_sql("SELECT DISTINCT country FROM source").unwrap();
        let e = err_of("SELECT DISTINCT country FROM source");
        assert!(e.contains("DISTINCT is not supported"), "got: {e}");
    }
}
