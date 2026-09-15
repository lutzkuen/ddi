//! JSON text, read and written the way Trino does it.
//!
//! Trino's JSON functions are Jackson underneath, and Jackson has opinions that a generic
//! JSON library does not share: `json_parse` sorts object keys and re-spells floats through
//! `BigDecimal`; `json_extract` keeps the source order and its repeats but re-spells floats
//! through `Double.toString`, as Jackson's structure copy does; `FORMAT JSON`, `json_query`
//! and `json_array_get` read a tree, which keeps the order, resolves a repeated key to its
//! last value and re-spells floats the same way; and `json_object` emits its keys in the
//! order a `java.util.HashMap` iterates them. A model has to produce the
//! same bytes here as in the warehouse, so these are reproduced rather than approximated:
//! this module is a small strict parser that keeps every number as the text it was, plus
//! the writers and the two Java number spellings.
//!
//! Nothing here knows about Arrow or DataFusion; it is the text layer under
//! [`crate::transform::json`] and [`crate::transform::json_build`].

use std::cmp::Ordering;

/// A parsed JSON value. Numbers are kept as their text; objects keep member order and
/// duplicates, so a writer can decide what Jackson would have done with them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    pub(crate) fn is_container(&self) -> bool {
        matches!(self, Json::Array(_) | Json::Object(_))
    }

    pub(crate) fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(a) => Some(a),
            _ => None,
        }
    }

    /// The member under `key`; the last one wins when the text repeats it, as it does in
    /// every reader Trino uses.
    pub(crate) fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(members) => members.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub(crate) fn index(&self, i: usize) -> Option<&Json> {
        self.as_array().and_then(|a| a.get(i))
    }

    /// Trino's scalar rendering: a string loses its quotes, other scalars print themselves,
    /// and containers are **not** scalars.
    pub(crate) fn scalar_text(&self) -> Option<String> {
        match self {
            Json::Null | Json::Array(_) | Json::Object(_) => None,
            Json::Bool(b) => Some(b.to_string()),
            Json::Number(n) => Some(n.clone()),
            Json::String(s) => Some(s.clone()),
        }
    }

    /// Members or elements, for `json_size`: 0 for a scalar.
    pub(crate) fn size(&self) -> usize {
        match self {
            Json::Array(a) => a.len(),
            Json::Object(o) => o.len(),
            _ => 0,
        }
    }

    /// Equality as a value: numbers compare by value where both parse, so `1` and `1.0`
    /// are the same element, and otherwise as text.
    pub(crate) fn same_value(&self, other: &Json) -> bool {
        match (self, other) {
            (Json::Number(a), Json::Number(b)) => match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(x), Ok(y)) => x == y,
                _ => a == b,
            },
            (Json::Array(a), Json::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.same_value(y))
            }
            (Json::Object(a), Json::Object(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .all(|(k, v)| b.iter().any(|(k2, v2)| k == k2 && v.same_value(v2)))
            }
            (a, b) => a == b,
        }
    }
}

// ---------------------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------------------

/// Deeper than this is not a payload, it is an attack on the stack.
const MAX_DEPTH: usize = 512;

/// Parse strictly: RFC 8259, one value, nothing after it.
pub(crate) fn parse(text: &str) -> Result<Json, String> {
    let mut p = Parser {
        chars: text.char_indices().peekable(),
        text,
        depth: 0,
    };
    p.skip_ws();
    let value = p.value()?;
    p.skip_ws();
    match p.chars.peek() {
        None => Ok(value),
        Some((at, c)) => Err(format!("unexpected {c:?} at byte {at} after the value")),
    }
}

struct Parser<'a> {
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
    text: &'a str,
    depth: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while matches!(self.chars.peek(), Some((_, ' ' | '\t' | '\n' | '\r'))) {
            self.chars.next();
        }
    }

    fn at(&mut self) -> usize {
        self.chars
            .peek()
            .map(|(i, _)| *i)
            .unwrap_or(self.text.len())
    }

    fn expect(&mut self, want: char) -> Result<(), String> {
        match self.chars.next() {
            Some((_, c)) if c == want => Ok(()),
            Some((at, c)) => Err(format!("expected {want:?} at byte {at}, found {c:?}")),
            None => Err(format!("expected {want:?}, found the end of the text")),
        }
    }

    fn literal(&mut self, word: &str, value: Json) -> Result<Json, String> {
        let at = self.at();
        for want in word.chars() {
            match self.chars.next() {
                Some((_, c)) if c == want => {}
                _ => return Err(format!("invalid literal at byte {at}")),
            }
        }
        Ok(value)
    }

    fn value(&mut self) -> Result<Json, String> {
        match self.chars.peek() {
            None => Err("unexpected end of the text".into()),
            Some((_, '{')) => self.object(),
            Some((_, '[')) => self.array(),
            Some((_, '"')) => self.string().map(Json::String),
            Some((_, 't')) => self.literal("true", Json::Bool(true)),
            Some((_, 'f')) => self.literal("false", Json::Bool(false)),
            Some((_, 'n')) => self.literal("null", Json::Null),
            Some((_, '-' | '0'..='9')) => self.number(),
            Some((at, c)) => Err(format!("unexpected {c:?} at byte {at}")),
        }
    }

    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(format!("nested deeper than {MAX_DEPTH} levels"));
        }
        Ok(())
    }

    fn object(&mut self) -> Result<Json, String> {
        self.enter()?;
        self.expect('{')?;
        let mut members = Vec::new();
        self.skip_ws();
        if matches!(self.chars.peek(), Some((_, '}'))) {
            self.chars.next();
            self.depth -= 1;
            return Ok(Json::Object(members));
        }
        loop {
            self.skip_ws();
            let key = match self.chars.peek() {
                Some((_, '"')) => self.string()?,
                Some((at, c)) => return Err(format!("expected a key at byte {at}, found {c:?}")),
                None => return Err("expected a key, found the end of the text".into()),
            };
            self.skip_ws();
            self.expect(':')?;
            self.skip_ws();
            let value = self.value()?;
            members.push((key, value));
            self.skip_ws();
            match self.chars.next() {
                Some((_, ',')) => continue,
                Some((_, '}')) => break,
                Some((at, c)) => {
                    return Err(format!("expected ',' or '}}' at byte {at}, found {c:?}"))
                }
                None => return Err("expected ',' or '}', found the end of the text".into()),
            }
        }
        self.depth -= 1;
        Ok(Json::Object(members))
    }

    fn array(&mut self) -> Result<Json, String> {
        self.enter()?;
        self.expect('[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if matches!(self.chars.peek(), Some((_, ']'))) {
            self.chars.next();
            self.depth -= 1;
            return Ok(Json::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value()?);
            self.skip_ws();
            match self.chars.next() {
                Some((_, ',')) => continue,
                Some((_, ']')) => break,
                Some((at, c)) => {
                    return Err(format!("expected ',' or ']' at byte {at}, found {c:?}"))
                }
                None => return Err("expected ',' or ']', found the end of the text".into()),
            }
        }
        self.depth -= 1;
        Ok(Json::Array(items))
    }

    fn string(&mut self) -> Result<String, String> {
        let start = self.at();
        self.expect('"')?;
        let mut out = String::new();
        loop {
            let Some((at, c)) = self.chars.next() else {
                return Err(format!("unterminated string starting at byte {start}"));
            };
            match c {
                '"' => return Ok(out),
                '\\' => {
                    let Some((_, e)) = self.chars.next() else {
                        return Err(format!("unterminated escape at byte {at}"));
                    };
                    match e {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let unit = self.hex4(at)?;
                            let ch = if (0xD800..0xDC00).contains(&unit) {
                                // A high surrogate needs its low half.
                                let low = match (self.chars.next(), self.chars.next()) {
                                    (Some((_, '\\')), Some((_, 'u'))) => self.hex4(at)?,
                                    _ => return Err(format!("lone surrogate at byte {at}")),
                                };
                                if !(0xDC00..0xE000).contains(&low) {
                                    return Err(format!("invalid surrogate pair at byte {at}"));
                                }
                                let code = 0x10000
                                    + ((unit as u32 - 0xD800) << 10)
                                    + (low as u32 - 0xDC00);
                                char::from_u32(code)
                                    .ok_or_else(|| format!("invalid escape at byte {at}"))?
                            } else {
                                char::from_u32(unit as u32)
                                    .ok_or_else(|| format!("lone surrogate at byte {at}"))?
                            };
                            out.push(ch);
                        }
                        other => return Err(format!("invalid escape \\{other} at byte {at}")),
                    }
                }
                c if (c as u32) < 0x20 => {
                    return Err(format!(
                        "control character {c:?} at byte {at} must be escaped"
                    ))
                }
                c => out.push(c),
            }
        }
    }

    fn hex4(&mut self, at: usize) -> Result<u16, String> {
        let mut v: u16 = 0;
        for _ in 0..4 {
            let Some((_, h)) = self.chars.next() else {
                return Err(format!("truncated \\u escape at byte {at}"));
            };
            let d = h
                .to_digit(16)
                .ok_or_else(|| format!("invalid \\u escape at byte {at}"))?;
            v = (v << 4) | d as u16;
        }
        Ok(v)
    }

    /// `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`, kept as text.
    fn number(&mut self) -> Result<Json, String> {
        let start = self.at();
        let bytes = self.text.as_bytes();
        let mut j = start;
        let digits = |j: &mut usize| {
            let from = *j;
            while *j < bytes.len() && bytes[*j].is_ascii_digit() {
                *j += 1;
            }
            *j - from
        };
        if j < bytes.len() && bytes[j] == b'-' {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'0' {
            // A leading zero stands alone.
            j += 1;
            if j < bytes.len() && bytes[j].is_ascii_digit() {
                return Err(format!("number with a leading zero at byte {start}"));
            }
        } else if digits(&mut j) == 0 {
            return Err(format!("invalid number at byte {start}"));
        }
        if j < bytes.len() && bytes[j] == b'.' {
            j += 1;
            if digits(&mut j) == 0 {
                return Err(format!("number with no digits after '.' at byte {start}"));
            }
        }
        if j < bytes.len() && (bytes[j] == b'e' || bytes[j] == b'E') {
            j += 1;
            if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
                j += 1;
            }
            if digits(&mut j) == 0 {
                return Err(format!("number with no exponent digits at byte {start}"));
            }
        }
        // Everything scanned was ASCII, so byte and char positions agree here.
        while self.at() < j {
            self.chars.next();
        }
        Ok(Json::Number(self.text[start..j].to_string()))
    }
}

// ---------------------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------------------

/// How a number that was text becomes text again. An integer is always its digits; the
/// two spellings differ on anything with a point or an exponent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Numbers {
    /// Through `BigDecimal.toString()`: what `json_parse` stores.
    BigDecimal,
    /// Through `Double.toString()`: what `json_extract` copies and what `FORMAT JSON`,
    /// `json_query` and `json_array_get` read back.
    JavaDouble,
}

/// How an object's members are ordered and deduplicated on the way out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Members {
    /// As written, repeats included: a streaming copy.
    Verbatim,
    /// As written, a repeated key keeping its first position and its last value: what a
    /// tree reader does.
    LastWins,
    /// Sorted by key, after the same deduplication: what `json_parse` stores.
    Sorted,
}

/// Render `json` compactly.
pub(crate) fn write(json: &Json, members: Members, numbers: Numbers, out: &mut String) {
    match json {
        Json::Null => out.push_str("null"),
        Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Json::Number(n) => out.push_str(&number_text(n, numbers)),
        Json::String(s) => out.push_str(&quote(s)),
        Json::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(item, members, numbers, out);
            }
            out.push(']');
        }
        Json::Object(pairs) => {
            let ordered: Vec<(&String, &Json)> = match members {
                Members::Verbatim => pairs.iter().map(|(k, v)| (k, v)).collect(),
                Members::LastWins | Members::Sorted => {
                    let mut seen: Vec<(&String, &Json)> = Vec::with_capacity(pairs.len());
                    for (k, v) in pairs {
                        match seen.iter_mut().find(|(k2, _)| *k2 == k) {
                            Some(slot) => slot.1 = v,
                            None => seen.push((k, v)),
                        }
                    }
                    if members == Members::Sorted {
                        // Java compares strings by UTF-16 code unit, which agrees with
                        // byte order everywhere but astral characters.
                        seen.sort_by(|(a, _), (b, _)| java_string_cmp(a, b));
                    }
                    seen
                }
            };
            out.push('{');
            for (i, (k, v)) in ordered.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&quote(k));
                out.push(':');
                write(v, members, numbers, out);
            }
            out.push('}');
        }
    }
}

pub(crate) fn to_string(json: &Json, members: Members, numbers: Numbers) -> String {
    let mut out = String::new();
    write(json, members, numbers, &mut out);
    out
}

/// A JSON string: quoted and escaped the way Jackson does it — `"`, `\` and control
/// characters escaped (the short forms where they exist, `\u00XX` with upper-case hex
/// otherwise), everything else, `/` and non-ASCII included, left alone.
pub(crate) fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn number_text(n: &str, numbers: Numbers) -> String {
    let is_float = n.contains(['.', 'e', 'E']);
    match numbers {
        _ if !is_float => n.to_string(),
        Numbers::BigDecimal => java_bigdecimal_text(n),
        Numbers::JavaDouble => match n.parse::<f64>() {
            Ok(v) if v.is_finite() => java_double(v),
            _ => n.to_string(),
        },
    }
}

fn java_string_cmp(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

// ---------------------------------------------------------------------------------------
// Java's number spellings
// ---------------------------------------------------------------------------------------

/// `Double.toString(v)`: the shortest digits that round-trip, laid out as `123.0` between
/// 10⁻³ and 10⁷ and as `1.23E7` / `1.23E-4` outside, always with a fractional digit.
///
/// Non-finite values are the words Java uses; the caller decides how to embed them.
pub(crate) fn java_double(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    let sign = if v < 0.0 { "-" } else { "" };
    let magnitude = v.abs();
    // Rust's `{:e}` is the shortest round-tripping representation, as Java's is.
    let sci = format!("{magnitude:e}");
    let (mantissa, exponent) = sci.split_once('e').expect("{:e} always has an exponent");
    let exponent: i32 = exponent.parse().expect("an integer exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let body = if (1e-3..1e7).contains(&magnitude) {
        plain_layout(&digits, exponent)
    } else {
        let (first, rest) = digits.split_at(1);
        let rest = if rest.is_empty() { "0" } else { rest };
        format!("{first}.{rest}E{exponent}")
    };
    format!("{sign}{body}")
}

/// `Float.toString(v)`, the same layout over the shortest `f32` digits.
pub(crate) fn java_float(v: f32) -> String {
    if v.is_nan() || v.is_infinite() || v == 0.0 {
        return java_double(f64::from(v));
    }
    let sign = if v < 0.0 { "-" } else { "" };
    let magnitude = v.abs();
    let sci = format!("{magnitude:e}");
    let (mantissa, exponent) = sci.split_once('e').expect("{:e} always has an exponent");
    let exponent: i32 = exponent.parse().expect("an integer exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let body = if (1e-3..1e7).contains(&magnitude) {
        plain_layout(&digits, exponent)
    } else {
        let (first, rest) = digits.split_at(1);
        let rest = if rest.is_empty() { "0" } else { rest };
        format!("{first}.{rest}E{exponent}")
    };
    format!("{sign}{body}")
}

/// `digits × 10^(exponent − digits.len() + 1)` written plainly with at least one digit on
/// each side of the point.
fn plain_layout(digits: &str, exponent: i32) -> String {
    let point = exponent + 1; // digits before the point
    if point <= 0 {
        format!("0.{}{}", "0".repeat((-point) as usize), digits)
    } else if (point as usize) >= digits.len() {
        format!("{}{}.0", digits, "0".repeat(point as usize - digits.len()))
    } else {
        let (int, frac) = digits.split_at(point as usize);
        format!("{int}.{frac}")
    }
}

/// `new BigDecimal(text).toString()` for a JSON number.
pub(crate) fn java_bigdecimal_text(text: &str) -> String {
    let (negative, rest) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text),
    };
    let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().unwrap_or(0)),
        None => (rest, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{int}{frac}");
    let scale = frac.len() as i64 - exponent;
    java_bigdecimal(negative, &digits, scale)
}

/// `BigDecimal.toString()` of `±digits × 10^-scale`.
///
/// Plain notation when the scale is non-negative and the adjusted exponent is at least
/// −6; scientific otherwise, with an explicit `+` on a positive exponent. The scale is
/// kept, so `DECIMAL(10,2)` one is `1.00` and `0.0000001` is `1E-7`.
pub(crate) fn java_bigdecimal(negative: bool, digits: &str, scale: i64) -> String {
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let precision = digits.len() as i64;
    let adjusted = -scale + (precision - 1);
    // `BigDecimal` has no negative zero.
    let sign = if negative && digits != "0" { "-" } else { "" };
    let body = if scale == 0 {
        digits.to_string()
    } else if scale > 0 && adjusted >= -6 {
        if precision > scale {
            let (int, frac) = digits.split_at((precision - scale) as usize);
            format!("{int}.{frac}")
        } else {
            format!("0.{}{}", "0".repeat((scale - precision) as usize), digits)
        }
    } else {
        let (first, rest) = digits.split_at(1);
        let mantissa = if rest.is_empty() {
            first.to_string()
        } else {
            format!("{first}.{rest}")
        };
        let exp = if adjusted >= 0 {
            format!("+{adjusted}")
        } else {
            adjusted.to_string()
        };
        format!("{mantissa}E{exp}")
    };
    format!("{sign}{body}")
}

/// `String.hashCode()`: a 31-polynomial over UTF-16 code units in wrapping `int`.
fn java_string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, u| h.wrapping_mul(31).wrapping_add(i32::from(u)))
}

/// The order a `java.util.HashMap` built from `keys`, in this order, iterates them.
///
/// This is how Trino's `json_object` orders its members — neither as written nor sorted
/// (its own test expects `key_1, key_2` to come out `key_2, key_1`). The map is built the
/// way Java builds it: 16 slots; a key goes to slot `(h ^ (h >>> 16)) & (slots - 1)` at the
/// end of that slot's chain; the table doubles when the count exceeds three quarters of it,
/// and also when a chain reaches nine while the table is under 64 slots (Java would
/// otherwise turn the chain into a tree); doubling re-slots every key, chains keeping their
/// order. Iteration is slot by slot, chain order within a slot. A genuine tree bin — nine
/// keys in one slot of a table of 64 or more — is not modelled; its chain order is used.
/// Returns indexes into `keys`.
pub(crate) fn java_hashmap_order(keys: &[&str]) -> Vec<usize> {
    const TREEIFY_THRESHOLD: usize = 8;
    const MIN_TREEIFY_CAPACITY: usize = 64;

    let spread: Vec<u32> = keys
        .iter()
        .map(|k| {
            let h = java_string_hash(k) as u32;
            h ^ (h >> 16)
        })
        .collect();
    let mut capacity = 16usize;
    let mut slots: Vec<Vec<usize>> = vec![Vec::new(); capacity];
    let resize = |slots: &mut Vec<Vec<usize>>, capacity: &mut usize, spread: &[u32]| {
        *capacity *= 2;
        let mut next: Vec<Vec<usize>> = vec![Vec::new(); *capacity];
        for chain in slots.iter() {
            for &i in chain {
                next[(spread[i] as usize) & (*capacity - 1)].push(i);
            }
        }
        *slots = next;
    };
    for i in 0..keys.len() {
        let slot = (spread[i] as usize) & (capacity - 1);
        let chain_before = slots[slot].len();
        slots[slot].push(i);
        // `putVal`: a chain that has just grown past the treeify threshold is treeified,
        // and `treeifyBin` resizes instead while the table is small.
        if chain_before >= TREEIFY_THRESHOLD && capacity < MIN_TREEIFY_CAPACITY {
            resize(&mut slots, &mut capacity, &spread);
        }
        // `++size > threshold`.
        if i + 1 > capacity * 3 / 4 {
            resize(&mut slots, &mut capacity, &spread);
        }
    }
    slots.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(text: &str, members: Members, numbers: Numbers) -> String {
        to_string(&parse(text).unwrap(), members, numbers)
    }

    #[test]
    fn extract_keeps_order_and_repeats_and_compacts() {
        // A streaming copy: members as written, repeats included, floats as doubles.
        assert_eq!(
            roundtrip(
                r#"{ "b" : 1.10 , "a" : [ 1e2 , "x\/y" , null ] , "b" : 2 }"#,
                Members::Verbatim,
                Numbers::JavaDouble
            ),
            r#"{"b":1.1,"a":[100.0,"x/y",null],"b":2}"#
        );
    }

    #[test]
    fn parse_sorts_dedupes_and_spells_floats_like_bigdecimal() {
        assert_eq!(
            roundtrip(
                r#"{"b":1.10,"a":{"z":1e2,"y":0.0000001},"b":2,"c":-0.5}"#,
                Members::Sorted,
                Numbers::BigDecimal
            ),
            r#"{"a":{"y":1E-7,"z":1E+2},"b":2,"c":-0.5}"#
        );
    }

    #[test]
    fn format_json_keeps_order_dedupes_and_spells_floats_like_double() {
        assert_eq!(
            roundtrip(
                r#"{"b":1.10,"a":1e2,"b":2,"c":12e-1,"d":1e7,"e":0.0001}"#,
                Members::LastWins,
                Numbers::JavaDouble
            ),
            r#"{"b":2,"a":100.0,"c":1.2,"d":1.0E7,"e":1.0E-4}"#
        );
    }

    #[test]
    fn strings_are_escaped_like_jackson() {
        let j = parse(r#""q\" bs\\ nl\n tab\t ctl\u0001 slash\/ é \ud83d\ude00""#).unwrap();
        assert_eq!(
            to_string(&j, Members::Verbatim, Numbers::JavaDouble),
            r#""q\" bs\\ nl\n tab\t ctl\u0001 slash/ é 😀""#
        );
        // Jackson spells the hex digits of a `\u` escape in upper case, and leaves DEL alone.
        assert_eq!(quote("a\u{1b}b"), "\"a\\u001Bb\"");
        assert_eq!(quote("\u{b}\u{7f}"), "\"\\u000B\u{7f}\"");
    }

    #[test]
    fn strictness() {
        for bad in [
            "",
            "{",
            "[1,]",
            "{\"a\":}",
            "01",
            "1.",
            "1e",
            "\"\u{1}\"",
            "tru",
            "{\"a\":1} x",
            "\"\\ud83d\"",
            "[1] [2]",
            "NaN",
        ] {
            assert!(parse(bad).is_err(), "{bad:?} should not parse");
        }
        assert_eq!(parse(" -0.5e+3 ").unwrap(), Json::Number("-0.5e+3".into()));
    }

    #[test]
    fn java_double_layouts() {
        for (v, want) in [
            (1.0, "1.0"),
            (100.0, "100.0"),
            (0.25, "0.25"),
            (0.001, "0.001"),
            (0.0001, "1.0E-4"),
            (1.2, "1.2"),
            (9_999_999.0, "9999999.0"),
            (1e7, "1.0E7"),
            (12_345_678.9, "1.23456789E7"),
            (-2.5, "-2.5"),
            (1e300, "1.0E300"),
            (0.1 + 0.2, "0.30000000000000004"),
        ] {
            assert_eq!(java_double(v), want, "{v}");
        }
        assert_eq!(java_double(f64::NAN), "NaN");
        assert_eq!(java_double(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(java_float(1.5), "1.5");
        assert_eq!(java_float(0.1), "0.1");
    }

    #[test]
    fn java_bigdecimal_layouts() {
        for (text, want) in [
            ("1.10", "1.10"),
            ("0.0000001", "1E-7"),
            ("1e2", "1E+2"),
            ("1.0E2", "1.0E+2"),
            ("0.0", "0.0"),
            ("-0.5", "-0.5"),
            ("123.4500", "123.4500"),
            ("0.000001", "0.000001"),
            ("12E-1", "1.2"),
        ] {
            assert_eq!(java_bigdecimal_text(text), want, "{text}");
        }
        assert_eq!(java_bigdecimal(false, "123400", 4), "12.3400");
        assert_eq!(java_bigdecimal(true, "50", 2), "-0.50");
        assert_eq!(java_bigdecimal(false, "0", 4), "0.0000");
    }

    #[test]
    fn hashmap_order_matches_trinos_own_examples() {
        fn order<'a>(keys: &[&'a str]) -> Vec<&'a str> {
            java_hashmap_order(keys)
                .into_iter()
                .map(|i| keys[i])
                .collect()
        }
        // From TestJsonObjectFunction: key_1, key_2 come out the other way round.
        assert_eq!(order(&["key_1", "key_2"]), vec!["key_2", "key_1"]);
        // From the documentation examples.
        assert_eq!(order(&["key1", "key2"]), vec!["key1", "key2"]);
        assert_eq!(order(&["x", "y"]), vec!["x", "y"]);
        // Hashing is over UTF-16 code units, so an astral character counts as two.
        assert_eq!(
            order(&["naïve", "😀", "a", "日本", "b"]),
            vec!["a", "b", "naïve", "😀", "日本"]
        );
        // Thirteen keys spill the table to 32 slots; twelve do not.
        let many: Vec<String> = (0..13).map(|i| format!("k{i}")).collect();
        let refs: Vec<&str> = many.iter().map(String::as_str).collect();
        assert_eq!(
            order(&refs),
            vec!["k0", "k1", "k2", "k3", "k4", "k5", "k11", "k6", "k10", "k7", "k8", "k12", "k9"]
        );
        assert_eq!(
            order(&[
                "id", "type", "sku", "qty", "price", "currency", "customer", "country", "status",
                "created", "updated", "version"
            ]),
            vec![
                "country", "price", "created", "qty", "currency", "id", "type", "sku", "updated",
                "version", "customer", "status"
            ]
        );
    }

    #[test]
    fn value_equality_is_numeric_for_numbers() {
        assert!(Json::Number("1".into()).same_value(&Json::Number("1.0".into())));
        assert!(!Json::Number("1".into()).same_value(&Json::String("1".into())));
    }
}
