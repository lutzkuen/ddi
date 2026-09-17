//! Trino spellings this engine's own parser does not read, and the parser that does.
//!
//! A dbt model is written for Starburst first. Most of it parses here unchanged, but a few
//! constructs fail before validation can even look at them, at two different depths:
//!
//! - **No grammar at all.** `FORMAT JSON`, the optional `KEY` in `KEY 'k' VALUE v`, and the
//!   parenthesised type in `CAST(x AS ARRAY(JSON))` are not known to `sqlparser` in any
//!   dialect. They are rewritten at the *token* level, before parsing, into spellings that
//!   are: `x FORMAT JSON` becomes `(x)::JSON`, `KEY` is dropped, `'k' : v` becomes `'k'
//!   VALUE v`, `WITH`/`WITHOUT UNIQUE KEYS` is dropped (a repeated key is an error either
//!   way), `ARRAY(JSON)` becomes `ARRAY<JSON>`. Each is later turned into the function that
//!   implements it — see [`crate::transform::json_build::rewrite_constructors`].
//! - **Grammar behind a dialect flag.** `'key' VALUE expr` is grammar the parser has, gated
//!   by dialect (`x -> expr` lambdas already parse natively). [`TrinoDialect`] switches it
//!   on, along with the group-by and aggregate-filter spellings the validator wants to
//!   refuse by name rather than by parse error, and is only consulted after the engine's
//!   own parser has refused the text.
//!
//! Neither layer changes what a query *means*: the text handed on is what Trino would have
//! read, said in a way this parser accepts, and [`crate::transform::validate::validate_sql`]
//! proves the result parses natively before anything runs.
//!
//! The first layer is one-way, and that matters for SQL that leaves this engine. A statement
//! parsed here and rendered again says `(x)::JSON`, which Trino has no grammar for, so
//! [`render_for_trino`] puts each of those back as the `FORMAT JSON` it was. Not its
//! `ENCODING`, which the rewrite drops: Trino reads an encoding only from varbinary, and this
//! engine reads JSON only from text, so no model both run can carry one. The other rewrites
//! need nothing putting back: `KEY` and the unique-keys clause are optional in Trino (a
//! repeated key is an error there either way), `'k' VALUE v` is one of its two member
//! spellings, and `ARRAY<JSON>` is its legacy array type.

use deltalake::datafusion::sql::sqlparser::ast::{
    CastKind, DataType, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    ObjectNamePart, Statement, VisitMut, VisitorMut,
};
use deltalake::datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use deltalake::datafusion::sql::sqlparser::keywords::Keyword;
use deltalake::datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use std::ops::ControlFlow;

/// The dialect the fallback parse uses: Trino's spellings that are gated behind flags.
///
/// Everything not listed here follows `sqlparser`'s defaults, which are the ones the
/// engine's own `GenericDialect` mostly shares. Kept minimal on purpose: each flag admits
/// a spelling, and every admitted spelling has to be rewritten into one the engine runs
/// or refused by the validator with its name.
#[derive(Debug, Default)]
pub(crate) struct TrinoDialect;

impl Dialect for TrinoDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        GenericDialect {}.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        GenericDialect {}.is_identifier_part(ch)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    /// `transform(arr, x -> expr)` and `filter(arr, x -> expr)`.
    fn supports_lambda_functions(&self) -> bool {
        true
    }

    /// `json_object('key' VALUE expr)`: the key is an expression, not an identifier.
    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        true
    }

    /// Refused by the validator, by name; without this a `GROUP BY (a, b)` would be a
    /// parse error instead.
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    /// Same: `sum(x) FILTER (WHERE ..)` is an aggregate the validator names.
    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    /// `U&'..'` is Trino's spelling too; without this the fallback would read `U & '..'`.
    fn supports_unicode_string_literal(&self) -> bool {
        true
    }
}

/// What kind of call an open parenthesis belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    JsonObject,
    JsonArray,
    /// `json_value`, `json_query`, `json_exists`: their input may say `FORMAT JSON` too.
    JsonInput,
    Other,
}

impl Call {
    /// May `FORMAT JSON` appear among this call's arguments?
    fn takes_format_json(self) -> bool {
        !matches!(self, Call::Other)
    }
}

/// One open parenthesis and what has been seen since.
#[derive(Debug)]
struct Frame {
    call: Call,
    /// Index into the output at which the current argument's *value* expression starts:
    /// after `(` or `,` for any call, and after `VALUE` for a `json_object` pair. This is
    /// what `FORMAT JSON` applies to.
    value_start: usize,
    /// A `RETURNING` clause has begun in this call, so a following `FORMAT JSON` describes
    /// the return type rather than a value.
    returning: bool,
    /// The current `json_object` member has had its `VALUE` (or `:`), so a later `VALUE`
    /// word is a column called `value`, not the separator.
    seen_value: bool,
}

/// Rewrite the Trino spellings that have no grammar here into ones that do.
///
/// Returns the text unchanged — the same `String` — when none of them occur, which is
/// the common case, so nothing is ever re-rendered for a query that did not need it.
/// Tokenising is done without unescaping, so the text put back is the text taken out.
pub(crate) fn prepare_trino_text(sql: &str) -> String {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql)
        .with_unescape(false)
        .tokenize()
    else {
        // The parser will report this in its own words.
        return sql.to_string();
    };

    let mut out: Vec<String> = Vec::with_capacity(tokens.len());
    let mut stack: Vec<Frame> = Vec::new();
    let mut changed = false;
    let mut i = 0;

    while i < tokens.len() {
        let tok = &tokens[i];

        match tok {
            Token::LParen => {
                let call = match previous_significant(&out) {
                    Some(prev) if is_bare_word(&prev, "json_object") => Call::JsonObject,
                    Some(prev) if is_bare_word(&prev, "json_array") => Call::JsonArray,
                    Some(prev)
                        if is_bare_word(&prev, "json_value")
                            || is_bare_word(&prev, "json_query")
                            || is_bare_word(&prev, "json_exists") =>
                    {
                        Call::JsonInput
                    }
                    _ => Call::Other,
                };
                out.push(tok.to_string());
                stack.push(Frame {
                    call,
                    value_start: out.len(),
                    returning: false,
                    seen_value: false,
                });
                i += 1;
                continue;
            }
            Token::RParen => {
                stack.pop();
                out.push(tok.to_string());
                i += 1;
                continue;
            }
            Token::Comma => {
                out.push(tok.to_string());
                if let Some(frame) = stack.last_mut() {
                    frame.value_start = out.len();
                    frame.seen_value = false;
                }
                i += 1;
                continue;
            }
            // `'k' : v` — the SQL/JSON standard's other member spelling.
            Token::Colon
                if stack
                    .last()
                    .is_some_and(|f| f.call == Call::JsonObject && !f.seen_value) =>
            {
                changed = true;
                if !out.last().is_some_and(|s| is_blank(s)) {
                    out.push(" ".to_string());
                }
                out.push("VALUE".to_string());
                if !tokens.get(i + 1).is_some_and(is_whitespace) {
                    out.push(" ".to_string());
                }
                let frame = stack.last_mut().expect("checked above");
                frame.value_start = out.len();
                frame.seen_value = true;
                i += 1;
                continue;
            }
            Token::Word(w) if w.quote_style.is_none() => {
                // `'k' VALUE v`: the value expression starts after the member's VALUE. A
                // second VALUE word in the same member is a column called `value`, and so
                // is one that opens the member — `json_object(value VALUE 1)`.
                if w.keyword == Keyword::VALUE {
                    let opens_member = matches!(
                        previous_significant(&out),
                        Some(Token::LParen) | Some(Token::Comma)
                    );
                    out.push(tok.to_string());
                    if let Some(frame) = stack.last_mut() {
                        if frame.call == Call::JsonObject && !frame.seen_value && !opens_member {
                            frame.value_start = out.len();
                            frame.seen_value = true;
                        }
                    }
                    i += 1;
                    continue;
                }

                // `WITH UNIQUE KEYS` / `WITHOUT UNIQUE KEYS`: a repeated key is an error
                // under either, so the clause says nothing here.
                if matches!(w.keyword, Keyword::WITH | Keyword::WITHOUT)
                    && stack.last().map(|f| f.call) == Some(Call::JsonObject)
                {
                    if let Some(u) = next_significant(&tokens, i + 1) {
                        if is_keyword(&tokens[u], Keyword::UNIQUE) {
                            let mut end = u;
                            if let Some(k) = next_significant(&tokens, u + 1) {
                                if is_keyword(&tokens[k], Keyword::KEYS) {
                                    end = k;
                                }
                            }
                            changed = true;
                            i = end + 1;
                            continue;
                        }
                    }
                }

                if w.keyword == Keyword::RETURNING {
                    if let Some(frame) = stack.last_mut() {
                        frame.returning = true;
                    }
                    out.push(tok.to_string());
                    i += 1;
                    continue;
                }

                // `KEY 'k' VALUE v` — the KEY is optional in Trino and unknown here. A
                // column called `key` is the key itself when VALUE follows it directly.
                if w.keyword == Keyword::KEY
                    && stack.last().map(|f| f.call) == Some(Call::JsonObject)
                    && matches!(
                        previous_significant(&out),
                        Some(Token::LParen) | Some(Token::Comma)
                    )
                    && !next_significant(&tokens, i + 1)
                        .is_some_and(|n| is_keyword(&tokens[n], Keyword::VALUE))
                {
                    changed = true;
                    i += 1;
                    continue;
                }

                // `x FORMAT JSON [ENCODING UTF8]` → `(x)::JSON`
                if w.keyword == Keyword::FORMAT {
                    if let Some(next) = next_significant(&tokens, i + 1) {
                        if is_keyword(&tokens[next], Keyword::JSON) {
                            let mut end = next;
                            if let Some(enc) = next_significant(&tokens, end + 1) {
                                if is_keyword(&tokens[enc], Keyword::ENCODING) {
                                    if let Some(name) = next_significant(&tokens, enc + 1) {
                                        if matches!(&tokens[name], Token::Word(_)) {
                                            end = name;
                                        }
                                    }
                                }
                            }
                            match stack.last() {
                                // `RETURNING VARCHAR FORMAT JSON`: the return type is text
                                // either way here, so the clause says nothing.
                                Some(frame) if frame.returning => changed = true,
                                Some(frame) if frame.call.takes_format_json() => {
                                    changed = true;
                                    let mut start = frame.value_start;
                                    while start < out.len() && is_blank(&out[start]) {
                                        start += 1;
                                    }
                                    while out.len() > start
                                        && out.last().is_some_and(|s| is_blank(s))
                                    {
                                        out.pop();
                                    }
                                    let start = start.min(out.len());
                                    out.insert(start, "(".to_string());
                                    out.push(")::JSON".to_string());
                                }
                                // Anywhere else — a subquery, some other call, no call at
                                // all — the words are not this clause: a column `format`
                                // aliased `json`, say. Leave them to the parser.
                                _ => {
                                    for t in &tokens[i..=end] {
                                        out.push(t.to_string());
                                    }
                                }
                            }
                            i = end + 1;
                            continue;
                        }
                    }
                }

                // `CAST(x AS ARRAY(JSON))` → `CAST(x AS ARRAY<JSON>)`. Only after AS, so
                // that a call to a function named `array` is left alone.
                if w.keyword == Keyword::ARRAY
                    && previous_significant(&out).is_some_and(|p| is_keyword(&p, Keyword::AS))
                {
                    if let Some(open) = next_significant(&tokens, i + 1) {
                        if tokens[open] == Token::LParen {
                            if let Some(close) = matching_paren(&tokens, open) {
                                changed = true;
                                out.push(tok.to_string());
                                out.push("<".to_string());
                                for t in &tokens[open + 1..close] {
                                    out.push(t.to_string());
                                }
                                out.push(">".to_string());
                                i = close + 1;
                                continue;
                            }
                        }
                    }
                }

                out.push(tok.to_string());
                i += 1;
            }
            _ => {
                out.push(tok.to_string());
                i += 1;
            }
        }
    }

    if changed {
        out.concat()
    } else {
        sql.to_string()
    }
}

/// The calls [`prepare_trino_text`] reads `FORMAT JSON` in, and so the ones it is put back in.
const FORMAT_JSON_CALLS: [&str; 5] = [
    "json_object",
    "json_array",
    "json_value",
    "json_query",
    "json_exists",
];

/// Render a statement for Trino as well as for this engine.
///
/// A statement read through [`prepare_trino_text`] holds `x FORMAT JSON` as the cast
/// `(x)::JSON`, and rendering it as it stands says exactly that. This engine reads either
/// spelling the same way, but Trino rejects the `::` at parse time, so text that another
/// engine may be handed as well — the `transform_sql` that `ddi dbt convert` writes — has
/// every such cast put back as the clause it was made from.
///
/// Only a cast that is a whole argument of one of [`FORMAT_JSON_CALLS`] is put back: that is
/// the only place the rewrite makes one, and the only place Trino has a clause to say it
/// with. The one pair of parentheses around the value is the rewrite's own and is dropped;
/// the analyst's are kept. A `::` anywhere else was written as such and is left alone.
///
/// Consumes the statement. The clause has no node in this parser's tree, so it is written
/// into the tree as text, and the tree is fit for nothing but rendering afterwards.
pub(crate) fn render_for_trino(mut statement: Statement) -> String {
    struct PutBackFormatJson;

    impl VisitorMut for PutBackFormatJson {
        type Break = ();

        // After the arguments have been visited, so a nested call has already had its own
        // casts put back by the time this one renders its values into text.
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
            let Expr::Function(f) = expr else {
                return ControlFlow::Continue(());
            };
            if !takes_format_json(&f.name.0) {
                return ControlFlow::Continue(());
            }
            let FunctionArguments::List(list) = &mut f.args else {
                return ControlFlow::Continue(());
            };
            for arg in &mut list.args {
                let (FunctionArg::Named { arg, .. }
                | FunctionArg::ExprNamed { arg, .. }
                | FunctionArg::Unnamed(arg)) = arg;
                if let FunctionArgExpr::Expr(value) = arg {
                    if let Some(text) = format_json_clause(value) {
                        // An unquoted identifier renders its value verbatim.
                        *value = Expr::Identifier(Ident::new(text));
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }

    let _ = statement.visit(&mut PutBackFormatJson);
    statement.to_string()
}

/// Is this call one of [`FORMAT_JSON_CALLS`], named the way [`prepare_trino_text`] recognises
/// it: one unquoted part?
fn takes_format_json(name: &[ObjectNamePart]) -> bool {
    match name {
        [ObjectNamePart::Identifier(ident)] => {
            ident.quote_style.is_none()
                && FORMAT_JSON_CALLS
                    .iter()
                    .any(|call| ident.value.eq_ignore_ascii_case(call))
        }
        _ => false,
    }
}

/// `value FORMAT JSON`, when `value` is the `(value)::JSON` that clause was read as.
fn format_json_clause(expr: &Expr) -> Option<String> {
    let Expr::Cast {
        kind: CastKind::DoubleColon,
        expr: value,
        data_type,
        array: false,
        format: None,
    } = expr
    else {
        return None;
    };
    if !(matches!(data_type, DataType::JSON) || data_type.to_string().eq_ignore_ascii_case("JSON"))
    {
        return None;
    }
    let value = match value.as_ref() {
        Expr::Nested(inner) => inner.as_ref(),
        other => other,
    };
    Some(format!("{value} FORMAT JSON"))
}

fn is_bare_word(tok: &Token, name: &str) -> bool {
    matches!(tok, Token::Word(Word { value, quote_style: None, .. }) if value.eq_ignore_ascii_case(name))
}

fn is_keyword(tok: &Token, keyword: Keyword) -> bool {
    matches!(tok, Token::Word(Word { keyword: k, quote_style: None, .. }) if *k == keyword)
}

fn is_whitespace(tok: &Token) -> bool {
    matches!(tok, Token::Whitespace(_))
}

/// The last emitted token that is not whitespace, re-tokenised from its text.
fn previous_significant(out: &[String]) -> Option<Token> {
    let text = out.iter().rev().find(|s| !is_blank(s))?;
    // Everything in `out` came from a token or is one of the fragments this module emits;
    // the fragments (`(`, `)::JSON`, `<`, `>`) are all single tokens or start with one.
    let mut toks = Tokenizer::new(&GenericDialect {}, text)
        .with_unescape(false)
        .tokenize()
        .ok()?;
    toks.retain(|t| !is_whitespace(t));
    toks.into_iter().next()
}

fn next_significant(tokens: &[Token], from: usize) -> Option<usize> {
    (from..tokens.len()).find(|&j| !is_whitespace(&tokens[j]))
}

fn matching_paren(tokens: &[Token], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (j, t) in tokens.iter().enumerate().skip(open) {
        match t {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(j);
                }
            }
            _ => {}
        }
    }
    None
}

/// Whitespace or a comment: a piece that carries no token the parser would see.
fn is_blank(s: &str) -> bool {
    Tokenizer::new(&GenericDialect {}, s)
        .with_unescape(false)
        .tokenize()
        .map(|t| t.iter().all(is_whitespace))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_that_needs_nothing_comes_back_untouched() {
        let sql = "SELECT a, b FROM source WHERE 'FORMAT JSON' <> x  -- a comment\n";
        assert_eq!(prepare_trino_text(sql), sql);
    }

    #[test]
    fn format_json_on_a_value_becomes_a_json_cast_of_the_whole_value() {
        let got = prepare_trino_text(
            "SELECT json_object('a' VALUE x || y FORMAT JSON, 'b' VALUE 1) FROM source",
        );
        assert_eq!(
            got,
            "SELECT json_object('a' VALUE (x || y)::JSON, 'b' VALUE 1) FROM source"
        );
    }

    #[test]
    fn format_json_with_an_encoding_is_the_same_thing() {
        let got = prepare_trino_text("SELECT json_array(x FORMAT JSON ENCODING UTF8) FROM source");
        assert_eq!(got, "SELECT json_array((x)::JSON) FROM source");
    }

    #[test]
    fn format_json_applies_per_array_element() {
        let got = prepare_trino_text("SELECT json_array(a, b FORMAT JSON, c) FROM source");
        assert_eq!(got, "SELECT json_array(a, (b)::JSON, c) FROM source");
    }

    #[test]
    fn a_nested_call_with_format_json_is_wrapped_as_a_unit() {
        let got = prepare_trino_text(
            "SELECT json_object('data' VALUE json_object('k' VALUE v) FORMAT JSON) FROM source",
        );
        assert_eq!(
            got,
            "SELECT json_object('data' VALUE (json_object('k' VALUE v))::JSON) FROM source"
        );
    }

    #[test]
    fn the_optional_key_keyword_is_dropped() {
        let got = prepare_trino_text(
            "SELECT json_object(KEY 'a' VALUE 1, key 'b' VALUE 2) AS j FROM source",
        );
        assert_eq!(
            got,
            "SELECT json_object( 'a' VALUE 1,  'b' VALUE 2) AS j FROM source"
        );
    }

    #[test]
    fn a_column_called_value_is_not_the_separator() {
        // The finder that found this: `value` is exactly the column an outbox table has.
        let got = prepare_trino_text(
            "SELECT json_object('k' VALUE key, 'v' VALUE value FORMAT JSON, \
             'w' VALUE t.value + value FORMAT JSON) AS msg FROM source",
        );
        assert_eq!(
            got,
            "SELECT json_object('k' VALUE key, 'v' VALUE (value)::JSON, \
             'w' VALUE (t.value + value)::JSON) AS msg FROM source"
        );
    }

    #[test]
    fn a_column_called_value_used_as_the_key_is_the_key() {
        assert_eq!(
            prepare_trino_text("SELECT json_object(value VALUE x FORMAT JSON, value : 1) FROM t"),
            "SELECT json_object(value VALUE (x)::JSON, value VALUE 1) FROM t"
        );
    }

    #[test]
    fn a_unicode_string_literal_survives_the_fallback_parse() {
        // Only the fallback dialect reads `'k' VALUE v`; it must read `U&'..'` too.
        let got = crate::transform::validate::normalise_sql(
            "SELECT json_object(upper(k) VALUE U&'caf\\00e9') AS j FROM source",
        )
        .unwrap();
        let lower = got.to_ascii_lowercase();
        assert!(
            lower.contains("café") || lower.contains("u&'caf\\00e9'"),
            "got: {got}"
        );
    }

    #[test]
    fn a_column_called_key_used_as_the_key_is_not_the_keyword() {
        let sql = "SELECT json_object(key VALUE 1, KEY key VALUE 2) AS j FROM source";
        assert_eq!(
            prepare_trino_text(sql),
            "SELECT json_object(key VALUE 1,  key VALUE 2) AS j FROM source"
        );
    }

    #[test]
    fn the_colon_member_spelling_and_the_unique_keys_clause() {
        let got = prepare_trino_text(
            "SELECT json_object('a' : 1, 'b' : x FORMAT JSON WITHOUT UNIQUE KEYS) AS j, \
             json_object('c' VALUE 2 WITH UNIQUE) AS k FROM source",
        );
        assert_eq!(
            got,
            "SELECT json_object('a' VALUE 1, 'b' VALUE (x)::JSON ) AS j, \
             json_object('c' VALUE 2 ) AS k FROM source"
        );
        // A cast's double colon is a different token, and a colon elsewhere is not ours.
        let sql = "SELECT json_object('a' VALUE x::INT) AS j, CASE WHEN a THEN 1 END FROM source";
        assert_eq!(prepare_trino_text(sql), sql);
        assert_eq!(
            prepare_trino_text("SELECT json_object('a':1) FROM t"),
            "SELECT json_object('a' VALUE 1) FROM t"
        );
    }

    #[test]
    fn format_json_words_outside_a_json_call_are_left_to_the_parser() {
        for sql in [
            "WITH c AS (SELECT t.format json FROM t) SELECT json FROM c",
            "SELECT * FROM (SELECT format json FROM t) s",
            "SELECT coalesce(format, json) FROM t",
        ] {
            assert_eq!(prepare_trino_text(sql), sql);
        }
        // But the JSON path functions do take it on their input.
        assert_eq!(
            prepare_trino_text("SELECT json_value(data FORMAT JSON, '$.a') FROM source"),
            "SELECT json_value((data)::JSON, '$.a') FROM source"
        );
    }

    #[test]
    fn a_column_called_key_outside_json_object_is_left_alone() {
        let sql = "SELECT key, lower(key) FROM source WHERE key = 'x'";
        assert_eq!(prepare_trino_text(sql), sql);
    }

    #[test]
    fn format_json_on_a_returning_clause_is_dropped() {
        let got = prepare_trino_text(
            "SELECT json_object('a' VALUE 1 RETURNING VARCHAR FORMAT JSON) FROM source",
        );
        assert_eq!(
            got,
            "SELECT json_object('a' VALUE 1 RETURNING VARCHAR ) FROM source"
        );
    }

    #[test]
    fn the_parenthesised_array_type_becomes_the_angle_bracket_one() {
        let got = prepare_trino_text(
            "SELECT o.id FROM source o \
             CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY(JSON))) AS t(li)",
        );
        assert!(got.contains("AS ARRAY<JSON>"), "got: {got}");
        assert!(!got.contains("ARRAY(JSON)"), "got: {got}");
    }

    #[test]
    fn a_function_called_array_is_not_a_type() {
        let sql = "SELECT array(1, 2) AS xs FROM source";
        assert_eq!(prepare_trino_text(sql), sql);
    }

    #[test]
    fn string_literals_and_comments_survive_verbatim() {
        let sql = "SELECT json_object('it''s' VALUE 'FORMAT JSON' FORMAT JSON) /* KEY */ \
                   FROM source -- FORMAT JSON\n";
        let got = prepare_trino_text(sql);
        assert_eq!(
            got,
            "SELECT json_object('it''s' VALUE ('FORMAT JSON')::JSON) /* KEY */ \
             FROM source -- FORMAT JSON\n"
        );
    }

    #[test]
    fn a_lambda_body_is_left_for_the_parser() {
        let sql = "SELECT transform(xs, x -> json_object('v' VALUE x)) FROM source";
        assert_eq!(prepare_trino_text(sql), sql);
    }

    /// Read `sql` the way `ddi dbt convert` does, and render it for Trino.
    fn rendered(sql: &str) -> String {
        use deltalake::datafusion::sql::parser::Statement as DfStatement;
        let mut statements =
            crate::transform::validate::parse_permissively(sql, "the test SQL").unwrap();
        let Some(DfStatement::Statement(inner)) = statements.pop_front() else {
            panic!("not a statement: {sql}");
        };
        render_for_trino(*inner)
    }

    /// `FORMAT JSON` in every place [`prepare_trino_text`] reads it, each in a query this
    /// engine runs.
    const FORMAT_JSON_EVERYWHERE: &[&str] = &[
        "SELECT json_object('a' VALUE x || y FORMAT JSON, 'b' VALUE 1) AS j FROM source",
        "SELECT json_array(x FORMAT JSON ENCODING UTF8) AS j FROM source",
        "SELECT json_array(a, b FORMAT JSON, c) AS j FROM source",
        "SELECT json_object('data' VALUE json_object('k' VALUE v) FORMAT JSON) AS j FROM source",
        "SELECT json_object('k' VALUE key, 'v' VALUE value FORMAT JSON) AS j FROM source",
        "SELECT json_object(value VALUE x FORMAT JSON, value : 1) AS j FROM source",
        "SELECT json_object(KEY 'a' : x FORMAT JSON WITHOUT UNIQUE KEYS) AS j FROM source",
        "SELECT json_value(data FORMAT JSON, 'lax $.a') AS v, \
         json_query(data FORMAT JSON, 'lax $.b') AS q, \
         json_exists(data FORMAT JSON, 'lax $.c') AS e FROM source",
        "SELECT json_object('d' VALUE json_array(x FORMAT JSON) FORMAT JSON) AS j FROM source",
        "SELECT json_object('it''s' VALUE 'FORMAT JSON' FORMAT JSON) AS j FROM source",
        "SELECT json_object('a' VALUE CASE WHEN b THEN json_format(CAST(c AS JSON)) \
         ELSE '[]' END FORMAT JSON) AS j FROM source",
        "SELECT json_object('a' VALUE (x) FORMAT JSON) AS j FROM source",
        "SELECT transform(xs, x -> json_object('v' VALUE x FORMAT JSON)) AS j FROM source",
        "select json_object('a' value json_format(cast(xs as json)) format json, \
         'b' value json_array(1, json_format(cast(ys as json)) format json)) as j from source",
        "SELECT json_object(\n  'items' VALUE json_format(\n    CAST(xs AS JSON)\n  )\n  \
         FORMAT\n  JSON\n) AS j FROM source",
    ];

    #[test]
    fn format_json_is_rendered_as_the_clause_it_was_read_from() {
        assert_eq!(
            rendered(
                "select json_object('items' value json_format(cast(transform(xs, x -> x + 1) \
                 as json)) format json) as j from source"
            ),
            "SELECT json_object('items' VALUE json_format(CAST(transform(xs, x -> x + 1) AS JSON)) \
             FORMAT JSON) AS j FROM source"
        );
        // Inside out: the inner call's value is put back before the outer one is rendered.
        let nested = "SELECT json_object('d' VALUE json_array(x FORMAT JSON) FORMAT JSON) AS j \
                      FROM source";
        assert_eq!(rendered(nested), nested);
        // The rewrite's own parentheses go; the analyst's stay.
        assert_eq!(
            rendered("SELECT json_array(a || b FORMAT JSON, (c) FORMAT JSON) AS j FROM source"),
            "SELECT json_array(a || b FORMAT JSON, (c) FORMAT JSON) AS j FROM source"
        );
    }

    #[test]
    fn rendered_text_says_no_cast_trino_cannot_read_and_means_the_same_here() {
        for sql in FORMAT_JSON_EVERYWHERE {
            let out = rendered(sql);
            assert!(!out.contains("::"), "{sql}\nrendered as\n{out}");
            assert!(out.contains("FORMAT JSON"), "{sql}\nrendered as\n{out}");
            // Read back through the same front door, it renders to itself ...
            assert_eq!(rendered(&out), out, "{sql}");
            // ... and this engine runs it exactly as it runs what the analyst wrote.
            let normalise = crate::transform::validate::normalise_sql;
            assert_eq!(
                normalise(&out).unwrap_or_else(|e| panic!("{out}: {e}")),
                normalise(sql).unwrap_or_else(|e| panic!("{sql}: {e}")),
                "{sql}"
            );
        }
    }

    #[test]
    fn the_calls_put_back_are_the_calls_rewritten() {
        // Two lists of the same five names, one per direction; this keeps them one list.
        for call in FORMAT_JSON_CALLS {
            let trino = format!("SELECT {call}(x FORMAT JSON) AS j FROM source");
            assert_eq!(
                prepare_trino_text(&trino),
                format!("SELECT {call}((x)::JSON) AS j FROM source")
            );
            assert_eq!(rendered(&trino), trino);
        }
    }

    #[test]
    fn a_json_cast_written_as_one_is_left_as_written() {
        // No rewrite made these, and no clause says them: part of a larger value, outside
        // any JSON call, or in a call that is not the one this module knows by that name.
        let sql = "SELECT json_object('a' VALUE x || y::JSON) AS j, z::JSON AS k, \
                   \"json_array\"((w)::JSON) AS l FROM source";
        let out = rendered(sql);
        for kept in ["x || y::JSON", "z::JSON AS k", "\"json_array\"((w)::JSON)"] {
            assert!(out.contains(kept), "{kept} not in {out}");
        }
    }
}
