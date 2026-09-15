//! Trino spellings this engine's own parser does not read, and the parser that does.
//!
//! A dbt model is written for Starburst first. Most of it parses here unchanged, but a few
//! constructs fail before validation can even look at them, at two different depths:
//!
//! - **No grammar at all.** `FORMAT JSON`, the optional `KEY` in `KEY 'k' VALUE v`, and the
//!   parenthesised type in `CAST(x AS ARRAY(JSON))` are not known to `sqlparser` in any
//!   dialect. They are rewritten at the *token* level, before parsing, into spellings that
//!   are: `x FORMAT JSON` becomes `(x)::JSON`, `KEY` is dropped, `ARRAY(JSON)` becomes
//!   `ARRAY<JSON>`. Each is later turned into the function that implements it — see
//!   [`crate::transform::json_build::rewrite_constructors`].
//! - **Grammar behind a dialect flag.** `'key' VALUE expr` and `x -> expr` are grammar the
//!   parser has, gated by dialect. [`TrinoDialect`] switches exactly those on and is only
//!   consulted after the engine's own parser has refused the text.
//!
//! Neither layer changes what a query *means*: the text handed on is what Trino would have
//! read, said in a way this parser accepts, and [`crate::transform::validate::validate_sql`]
//! proves the result parses natively before anything runs.

use deltalake::datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use deltalake::datafusion::sql::sqlparser::keywords::Keyword;
use deltalake::datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};

/// The dialect the fallback parse uses: Trino's spellings that are gated behind flags.
///
/// Everything not listed here follows `sqlparser`'s defaults, which are the ones the
/// engine's own `GenericDialect` mostly shares. Kept minimal on purpose: each flag admits
/// a spelling, and every admitted spelling has to be rewritten into one the engine runs.
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

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }
}

/// What kind of call an open parenthesis belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    JsonObject,
    JsonArray,
    Other,
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
                    _ => Call::Other,
                };
                out.push(tok.to_string());
                stack.push(Frame {
                    call,
                    value_start: out.len(),
                    returning: false,
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
                }
                i += 1;
                continue;
            }
            Token::Word(w) if w.quote_style.is_none() => {
                // `'k' VALUE v`: the value expression starts after VALUE.
                if w.keyword == Keyword::VALUE {
                    out.push(tok.to_string());
                    if let Some(frame) = stack.last_mut() {
                        if frame.call == Call::JsonObject {
                            frame.value_start = out.len();
                        }
                    }
                    i += 1;
                    continue;
                }

                if w.keyword == Keyword::RETURNING {
                    if let Some(frame) = stack.last_mut() {
                        frame.returning = true;
                    }
                    out.push(tok.to_string());
                    i += 1;
                    continue;
                }

                // `KEY 'k' VALUE v` — the KEY is optional in Trino and unknown here.
                if w.keyword == Keyword::KEY
                    && stack.last().map(|f| f.call) == Some(Call::JsonObject)
                    && matches!(
                        previous_significant(&out),
                        Some(Token::LParen) | Some(Token::Comma)
                    )
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
                            changed = true;
                            match stack.last() {
                                // `RETURNING VARCHAR FORMAT JSON`: the return type is text
                                // either way here, so the clause says nothing.
                                Some(frame) if frame.returning => {}
                                Some(frame) => {
                                    let mut start = frame.value_start;
                                    while start < out.len() && is_blank(&out[start]) {
                                        start += 1;
                                    }
                                    while out.last().is_some_and(|s| is_blank(s)) {
                                        out.pop();
                                    }
                                    out.insert(start, "(".to_string());
                                    out.push(")::JSON".to_string());
                                }
                                // Not inside any call: nothing to attach it to, so leave the
                                // parser to complain in its own words.
                                None => {
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
}
