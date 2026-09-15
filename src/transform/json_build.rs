//! Trino's JSON constructors: `json_object`, `json_array` and `CAST(x AS JSON)`.
//!
//! The readers in [`crate::transform::json`] take a payload apart. These put one together,
//! which is what an outbox model needs: one message per source row, with an envelope and a
//! child array composed from that row and nothing else. Row-local and stateless like the
//! readers, so batch boundaries cannot change the answer.
//!
//! # Why Trino's spelling, and Trino's rules
//!
//! Hand-concatenating a message with `||` puts JSON escaping in the model author's hands.
//! A bespoke `ddi` function would stream, but the same SQL would then fail to parse in
//! Starburst — and in an estate where the target table's columns are derived from the model
//! by `DESCRIBE OUTPUT`, a model Starburst cannot read has no table. So the constructors
//! are the ones Starburst already runs, spelt the way people write them — `'key' VALUE
//! expr`, the optional `KEY`, `FORMAT JSON`, `NULL ON NULL` / `ABSENT ON NULL`,
//! `RETURNING` — and, more to the point, *behaving* the way Starburst does, including the
//! parts nobody would design that way:
//!
//! - `json_object` and `json_array` return **text**, not JSON, unless told `RETURNING
//!   JSON`. A constructor nested directly inside another is embedded as JSON anyway — the
//!   analyzer treats the nesting as an implicit `FORMAT JSON` — but a constructor's result
//!   that arrives any other way (through a `CASE`, a `coalesce`, a CTE column) is text,
//!   and is embedded as an escaped string unless the model says `FORMAT JSON`.
//! - A JSON-typed value — `json_extract`, `json_array_get`, `json_parse`, `CAST(.. AS
//!   JSON)` — used as a member without `FORMAT JSON` is cast to varchar first. That works
//!   for a scalar and **fails for an object or array**, in Starburst at run time and here
//!   too, with the spelling that works: `json_format(<value>) FORMAT JSON`. `FORMAT JSON`
//!   directly on a JSON-typed value is an analysis error there and a config-load one here.
//! - The members of a `json_object` come out in the order a `java.util.HashMap` iterates
//!   them: neither as written nor sorted. See [`crate::transform::jsonval`], which
//!   reproduces it so the two engines produce the same bytes. A repeated key is an error.
//! - `json_object` defaults to `NULL ON NULL`, `json_array` to `ABSENT ON NULL`.
//!
//! # How a value is rendered
//!
//! | value | as a member | under `CAST(.. AS JSON)` |
//! |---|---|---|
//! | text | string, escaped | string, escaped |
//! | text after `FORMAT JSON` | embedded, re-serialised as Jackson would | — |
//! | JSON-typed text | scalar as a string; object or array is an error | embedded verbatim |
//! | integers | number | number |
//! | decimals | number at the declared scale, `12.3400`, as `BigDecimal` spells it | same |
//! | doubles | as `Double.toString` spells it: `1.0`, `1.0E7`; `NaN` becomes the string `"NaN"` | same |
//! | boolean | `true` / `false` | same |
//! | date | `"2024-03-31"` | same |
//! | timestamp | `"2024-03-31 22:30:00.123456"`; zoned values render in UTC at millisecond precision with ` UTC` appended, the way Starburst reads a Delta `timestamp` | naive only; zoned is not castable there either |
//! | array, row | an error naming the fix: Starburst would cast it to varchar text | array / object, recursively |
//! | NULL | `null`, or absent | SQL NULL; `null` inside a container |

use std::any::Any;
use std::ops::ControlFlow;
use std::sync::Arc;

use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait, StringArray,
};
use deltalake::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Float32Type, Float64Type, Int64Type,
    TimeUnit,
};
use deltalake::datafusion::common::{Result as DFResult, ScalarValue};
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::datafusion::sql::sqlparser::ast::{
    CastKind, DataType as SqlType, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgOperator, FunctionArgumentClause, FunctionArgumentList, FunctionArguments, Ident,
    JsonNullClause, ObjectName, Query, Value, VisitMut, VisitorMut,
};

use crate::error::{Error, Result};
use crate::transform::json::{json_field, mark, marker_of, Marker};
use crate::transform::jsonval::{self, Json, Members, Numbers};
use crate::transform::validate::reject;

/// `ddi_json_object(<flags>, key, value, key, value, ...)` — what `json_object(...)` becomes.
pub(crate) const OBJECT: &str = "ddi_json_object";
/// `ddi_json_array(<flags>, value, value, ...)` — what `json_array(...)` becomes.
pub(crate) const ARRAY: &str = "ddi_json_array";
/// `ddi_to_json(x)` — what `CAST(x AS JSON)` becomes.
pub(crate) const TO_JSON: &str = "ddi_to_json";
/// `ddi_format_json(x)` — what `x FORMAT JSON` becomes.
pub(crate) const FORMAT_JSON: &str = "ddi_format_json";
/// `ddi_embed_json(x)` — what a constructor nested directly in another becomes: the
/// implicit `FORMAT JSON` Trino's analyzer applies, which also takes a `RETURNING JSON`
/// result as it is.
pub(crate) const EMBED_JSON: &str = "ddi_embed_json";

/// The functions of this module, registered on every transform session.
pub fn register(ctx: &SessionContext) {
    for kind in [
        Build::Object,
        Build::Array,
        Build::ToJson,
        Build::FormatJson,
        Build::Embed,
    ] {
        ctx.register_udf(ScalarUDF::from(BuildFn::new(kind)));
    }
}

// ---------------------------------------------------------------------------------------
// The rewrite: Trino's calls into this module's functions
// ---------------------------------------------------------------------------------------

/// Replace every `json_object`, `json_array`, `CAST(.. AS JSON)` and `FORMAT JSON` in
/// `query`, in place.
///
/// Runs before validation, so a constructor written the way Starburst reads it is what the
/// engine runs, and a spelling this module does not implement is refused at config load
/// with the construct named rather than on the first batch.
pub(crate) fn rewrite_constructors(query: &mut Query) -> Result<()> {
    struct V(Option<Error>);
    impl VisitorMut for V {
        type Break = ();
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            match replacement(expr) {
                Ok(Some(r)) => *expr = r,
                Ok(None) => {}
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

fn replacement(expr: &Expr) -> Result<Option<Expr>> {
    match expr {
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            format,
            ..
        } if is_json_type(data_type) => {
            if format.is_some() {
                return Err(reject(
                    "CAST(.. AS JSON FORMAT ..)",
                    "a cast format is a BigQuery spelling this engine does not run.",
                    "write CAST(x AS JSON).",
                ));
            }
            match kind {
                // `x FORMAT JSON` is spelt `(x)::JSON` by the time it is parsed — see
                // `crate::transform::dialect` — and reads text as JSON.
                CastKind::DoubleColon => {
                    let inner = unparenthesised(inner);
                    if let Some(what) = json_typed_call(inner) {
                        return Err(reject(
                            &format!("FORMAT JSON on {what}, which is a JSON-typed value"),
                            "Starburst reads FORMAT JSON only from text, and refuses it on \
                             its JSON type.",
                            "write json_format(<value>) FORMAT JSON.",
                        ));
                    }
                    Ok(Some(call(FORMAT_JSON, vec![inner.clone()])))
                }
                // `CAST(x AS JSON)` converts a value: text becomes a JSON *string*.
                CastKind::Cast => Ok(Some(call(TO_JSON, vec![(**inner).clone()]))),
                CastKind::TryCast | CastKind::SafeCast => Err(reject(
                    "TRY_CAST(.. AS JSON)",
                    "a value that cannot be rendered as JSON is a data-quality failure, not \
                     a row to null out.",
                    "write CAST(x AS JSON).",
                )),
            }
        }
        Expr::Function(f) => match bare_name(&f.name).as_str() {
            "json_object" => rewrite_object(f).map(Some),
            "json_array" => rewrite_array(f).map(Some),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn is_json_type(data_type: &SqlType) -> bool {
    matches!(data_type, SqlType::JSON) || data_type.to_string().eq_ignore_ascii_case("JSON")
}

fn unparenthesised(e: &Expr) -> &Expr {
    match e {
        Expr::Nested(inner) => unparenthesised(inner),
        other => other,
    }
}

/// The name of a JSON-typed call, when `e` is one: what Trino's analyzer would refuse
/// `FORMAT JSON` on.
fn json_typed_call(e: &Expr) -> Option<String> {
    let Expr::Function(f) = e else {
        return None;
    };
    let name = bare_name(&f.name);
    match name.as_str() {
        "json_extract" | "json_array_get" | "json_parse" => Some(format!("{name}(..)")),
        n if n == TO_JSON => Some("CAST(.. AS JSON)".into()),
        _ => None,
    }
}

/// A constructor nested directly in another: Trino embeds it as JSON without being told.
fn is_nested_constructor(e: &Expr) -> bool {
    match unparenthesised(e) {
        Expr::Function(f) => {
            let name = bare_name(&f.name);
            name == OBJECT || name == ARRAY || name == "json_query"
        }
        _ => false,
    }
}

/// The unqualified, lower-cased name of a function.
fn bare_name(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|p| p.to_string())
        .unwrap_or_default()
        .trim_matches('"')
        .to_ascii_lowercase()
}

/// Build a plain call to one of this engine's functions.
fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn literal(text: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(text.to_string()).into())
}

/// What the clauses of a constructor call asked for, as the flags literal encodes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Flags {
    absent_on_null: bool,
    returns_json: bool,
}

impl Flags {
    fn encode(self) -> String {
        format!(
            "{},{}",
            if self.absent_on_null {
                "absent"
            } else {
                "null"
            },
            if self.returns_json { "json" } else { "varchar" }
        )
    }

    fn decode(text: &str) -> Option<Self> {
        let (nulls, returns) = text.split_once(',')?;
        Some(Self {
            absent_on_null: match nulls {
                "absent" => true,
                "null" => false,
                _ => return None,
            },
            returns_json: match returns {
                "json" => true,
                "varchar" => false,
                _ => return None,
            },
        })
    }
}

fn parse_flags<'f>(
    what: &str,
    f: &'f Function,
    absent_on_null: bool,
) -> Result<(Flags, &'f FunctionArgumentList)> {
    if f.uses_odbc_syntax
        || !matches!(f.parameters, FunctionArguments::None)
        || f.filter.is_some()
        || f.null_treatment.is_some()
        || f.over.is_some()
        || !f.within_group.is_empty()
    {
        return Err(reject(
            &format!("{what} with a FILTER, OVER or WITHIN GROUP clause"),
            "it is a scalar constructor over one row's values.",
            &format!("write a plain {what}(...)."),
        ));
    }
    let FunctionArguments::List(list) = &f.args else {
        return Err(reject(
            &format!("{what} without an argument list"),
            "the constructor takes its members as arguments.",
            &format!("write {what}(...), with () for an empty one."),
        ));
    };
    if list.duplicate_treatment.is_some() {
        return Err(reject(
            &format!("{what}(DISTINCT ..)"),
            "there is nothing to deduplicate in a constructor.",
            &format!("write a plain {what}(...)."),
        ));
    }
    let mut flags = Flags {
        absent_on_null,
        returns_json: false,
    };
    for clause in &list.clauses {
        match clause {
            FunctionArgumentClause::JsonNullClause(JsonNullClause::NullOnNull) => {
                flags.absent_on_null = false
            }
            FunctionArgumentClause::JsonNullClause(JsonNullClause::AbsentOnNull) => {
                flags.absent_on_null = true
            }
            FunctionArgumentClause::JsonReturningClause(r) => {
                let ty = r.data_type.to_string().to_ascii_uppercase();
                if ty == "JSON" {
                    flags.returns_json = true;
                } else if !(ty.starts_with("VARCHAR") || ty.starts_with("CHAR") || ty == "STRING") {
                    return Err(reject(
                        &format!("{what}(.. RETURNING {})", r.data_type),
                        "only JSON and VARCHAR are types a JSON value can be returned as.",
                        "write RETURNING JSON, or leave the clause out for VARCHAR.",
                    ));
                }
            }
            other => {
                return Err(reject(
                    &format!("{what}(.. {other})"),
                    "this clause is not part of a JSON constructor.",
                    "write only NULL ON NULL, ABSENT ON NULL or RETURNING JSON / VARCHAR.",
                ));
            }
        }
    }
    Ok((flags, list))
}

fn rewrite_object(f: &Function) -> Result<Expr> {
    let (flags, list) = parse_flags("json_object", f, false)?;
    let mut args = vec![literal(&flags.encode())];
    for arg in &list.args {
        let (key, value) = match arg {
            FunctionArg::ExprNamed {
                name,
                arg: FunctionArgExpr::Expr(value),
                operator: FunctionArgOperator::Value,
            } => (name.clone(), value.clone()),
            // The engine's own parser reads a bare identifier key as a named argument.
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(value),
                operator: FunctionArgOperator::Value,
            } => (Expr::Identifier(name.clone()), value.clone()),
            other => {
                return Err(reject(
                    &format!("json_object({other})"),
                    "each member is a `'key' VALUE value` pair; a positional argument list \
                     is a MySQL spelling Starburst would not read either.",
                    "write json_object('key' VALUE value, ...) — KEY 'key' VALUE value also \
                     works, and FORMAT JSON after a value embeds text that holds JSON.",
                ));
            }
        };
        args.push(key);
        args.push(member(value));
    }
    Ok(call(OBJECT, args))
}

fn rewrite_array(f: &Function) -> Result<Expr> {
    let (flags, list) = parse_flags("json_array", f, true)?;
    let mut args = vec![literal(&flags.encode())];
    for arg in &list.args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(value)) => args.push(member(value.clone())),
            other => {
                return Err(reject(
                    &format!("json_array({other})"),
                    "each element is a plain value.",
                    "write json_array(value, value, ...), with FORMAT JSON after a value \
                     that is text holding JSON.",
                ));
            }
        }
    }
    Ok(call(ARRAY, args))
}

/// A member as Starburst would read it: a constructor nested directly is JSON, without
/// being told.
fn member(value: Expr) -> Expr {
    if is_nested_constructor(&value) {
        call(EMBED_JSON, vec![value])
    } else {
        value
    }
}

// ---------------------------------------------------------------------------------------
// The functions
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Build {
    Object,
    Array,
    ToJson,
    FormatJson,
    /// `FormatJson` for a directly nested constructor: implicit, so a `RETURNING JSON`
    /// result is taken as it is rather than refused.
    Embed,
}

impl Build {
    fn name(self) -> &'static str {
        match self {
            Build::Object => OBJECT,
            Build::Array => ARRAY,
            Build::ToJson => TO_JSON,
            Build::FormatJson => FORMAT_JSON,
            Build::Embed => EMBED_JSON,
        }
    }

    /// The name the author wrote, for messages.
    fn spelling(self) -> &'static str {
        match self {
            Build::Object => "json_object",
            Build::Array => "json_array",
            Build::ToJson => "CAST(.. AS JSON)",
            Build::FormatJson | Build::Embed => "FORMAT JSON",
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BuildFn {
    kind: Build,
    signature: Signature,
}

impl BuildFn {
    fn new(kind: Build) -> Self {
        let signature = match kind {
            Build::ToJson | Build::FormatJson | Build::Embed => {
                Signature::any(1, Volatility::Immutable)
            }
            _ => Signature::variadic_any(Volatility::Immutable),
        };
        Self { kind, signature }
    }

    fn err(&self, msg: impl std::fmt::Display) -> DataFusionError {
        DataFusionError::Execution(format!("{}: {msg}", self.kind.spelling()))
    }

    fn flags(&self, first: Option<&ScalarValue>) -> DFResult<Flags> {
        match first {
            Some(ScalarValue::Utf8(Some(s))) => Flags::decode(s).ok_or_else(|| {
                self.err(format!(
                    "{} is internal; write {}(...)",
                    self.kind.name(),
                    self.kind.spelling()
                ))
            }),
            _ => Err(self.err(format!(
                "{} is internal; write {}(...)",
                self.kind.name(),
                self.kind.spelling()
            ))),
        }
    }
}

impl ScalarUDFImpl for BuildFn {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.kind.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let field = match self.kind {
            Build::Object | Build::Array => {
                let flags = self.flags(args.scalar_arguments.first().copied().flatten())?;
                if self.kind == Build::Object {
                    // `(flags, key, value, ...)`: the pairs must pair up, and a key is text.
                    if args.arg_fields.len() % 2 != 1 {
                        return Err(self.err("keys and values do not pair up"));
                    }
                    for (n, key) in args.arg_fields[1..].iter().step_by(2).enumerate() {
                        if !is_text(key.data_type()) || marker_of(key).is_some() {
                            return Err(self.err(format!(
                                "key #{} has type {}; a key must be varchar",
                                n + 1,
                                describe(key)
                            )));
                        }
                    }
                }
                let plain = Field::new(self.name(), DataType::Utf8, false);
                if flags.returns_json {
                    mark(plain, Marker::Typed)
                } else {
                    plain
                }
            }
            Build::ToJson => {
                let nullable = args.arg_fields.first().is_none_or(|f| f.is_nullable());
                json_field(self.name(), nullable)
            }
            Build::FormatJson | Build::Embed => {
                let Some(input) = args.arg_fields.first() else {
                    return Err(self.err("missing its argument"));
                };
                // The analysis error Starburst gives, at the same point: before any row.
                // A nested constructor's own JSON result is the one exception.
                let refused = match marker_of(input) {
                    Some(Marker::Typed) if self.kind == Build::Embed => None,
                    other => other,
                };
                if let Some(marker) = refused {
                    let what = match marker {
                        Marker::Typed => "a JSON-typed value",
                        Marker::Formatted => "a value already read with FORMAT JSON",
                    };
                    return Err(DataFusionError::Plan(format!(
                        "FORMAT JSON on {what}: Starburst reads FORMAT JSON only from text. \
                         Write json_format(<value>) FORMAT JSON."
                    )));
                }
                if !is_text(input.data_type()) && input.data_type() != &DataType::Null {
                    return Err(DataFusionError::Plan(format!(
                        "FORMAT JSON on a value of type {}: only text can be read as JSON. \
                         Build the value with json_object / json_array, or CAST it to JSON.",
                        input.data_type()
                    )));
                }
                mark(
                    Field::new(self.name(), DataType::Utf8, input.is_nullable()),
                    Marker::Formatted,
                )
            }
        };
        Ok(Arc::new(field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let who = self.kind.spelling();

        let out: Vec<Option<String>> = match self.kind {
            Build::ToJson => {
                let array = to_array(&args.args[0], rows)?;
                render(&array, &args.arg_fields[0], Mode::Cast, who, 1)?
            }
            Build::FormatJson | Build::Embed => {
                let array = to_array(&args.args[0], rows)?;
                let text = as_text(&array).map_err(|e| self.err(e))?;
                let verbatim = self.kind == Build::Embed
                    && marker_of(&args.arg_fields[0]) == Some(Marker::Typed);
                let mut out = Vec::with_capacity(rows);
                for i in 0..rows {
                    if text.is_null(i) {
                        out.push(None);
                        continue;
                    }
                    if verbatim {
                        out.push(Some(text.value(i).to_string()));
                        continue;
                    }
                    let doc = jsonval::parse(text.value(i)).map_err(|e| {
                        self.err(format!(
                            "row {i} is not valid JSON: {e}. Input is a typed column, not \
                             arbitrary text, so this is a data-quality failure rather than a \
                             row to skip."
                        ))
                    })?;
                    // What Jackson's tree reader hands back: the order kept, a repeated key
                    // resolved to its last value, whitespace gone, floats as doubles.
                    out.push(Some(jsonval::to_string(
                        &doc,
                        Members::LastWins,
                        Numbers::JavaDouble,
                    )));
                }
                out
            }
            Build::Object | Build::Array => {
                let first = match args.args.first() {
                    Some(ColumnarValue::Scalar(s)) => Some(s),
                    _ => None,
                };
                let flags = self.flags(first)?;
                let mut columns = Vec::with_capacity(args.args.len() - 1);
                for (n, (value, field)) in
                    args.args[1..].iter().zip(&args.arg_fields[1..]).enumerate()
                {
                    let array = to_array(value, rows)?;
                    // Position as the author counts: member 1, 2, ... for an array;
                    // for an object, key 1, value 1, key 2, ...
                    let position = if self.kind == Build::Object {
                        n / 2 + 1
                    } else {
                        n + 1
                    };
                    columns.push(render(&array, field, Mode::Member, who, position)?);
                }
                if self.kind == Build::Object {
                    build_objects(&columns, rows, flags.absent_on_null).map_err(|e| self.err(e))?
                } else {
                    build_arrays(&columns, rows, flags.absent_on_null)
                }
            }
        };
        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(out)) as ArrayRef
        ))
    }
}

/// `{"k":v,...}` per row from rendered key and value columns, alternating.
///
/// Keys were rendered as JSON strings, so they are already quoted; a `Json::String` is
/// parsed back out of that to order them the way Trino does.
fn build_objects(
    columns: &[Vec<Option<String>>],
    rows: usize,
    absent_on_null: bool,
) -> std::result::Result<Vec<Option<String>>, String> {
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut keys: Vec<String> = Vec::with_capacity(columns.len() / 2);
        let mut members: Vec<(String, Option<&str>)> = Vec::with_capacity(columns.len() / 2);
        for (n, pair) in columns.chunks(2).enumerate() {
            let (rendered_keys, values) = (&pair[0], &pair[1]);
            let Some(quoted) = &rendered_keys[row] else {
                return Err(format!(
                    "key #{} is NULL in row {row}; a key must not be null",
                    n + 1
                ));
            };
            let key = match jsonval::parse(quoted) {
                Ok(Json::String(k)) => k,
                _ => return Err(format!("key #{} is not text in row {row}", n + 1)),
            };
            if keys.contains(&key) {
                return Err(format!(
                    "cannot construct a JSON object with duplicate key {key:?} (row {row}); \
                     Starburst refuses it too"
                ));
            }
            keys.push(key);
            members.push((quoted.clone(), values[row].as_deref()));
        }
        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let mut s = String::from("{");
        let mut first = true;
        for i in jsonval::java_hashmap_order(&key_refs) {
            let (quoted, value) = &members[i];
            let value = match value {
                Some(v) => v,
                None if absent_on_null => continue,
                None => "null",
            };
            if !first {
                s.push(',');
            }
            first = false;
            s.push_str(quoted);
            s.push(':');
            s.push_str(value);
        }
        s.push('}');
        out.push(Some(s));
    }
    Ok(out)
}

/// `[v,...]` per row from rendered element columns.
fn build_arrays(
    columns: &[Vec<Option<String>>],
    rows: usize,
    absent_on_null: bool,
) -> Vec<Option<String>> {
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let mut s = String::from("[");
        let mut first = true;
        for values in columns {
            let value = match &values[row] {
                Some(v) => v.as_str(),
                None if absent_on_null => continue,
                None => "null",
            };
            if !first {
                s.push(',');
            }
            first = false;
            s.push_str(value);
        }
        s.push(']');
        out.push(Some(s));
    }
    out
}

fn to_array(v: &ColumnarValue, rows: usize) -> DFResult<ArrayRef> {
    Ok(match v {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(rows)?,
    })
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

fn describe(field: &Field) -> String {
    match marker_of(field) {
        Some(Marker::Typed) => "json".into(),
        _ => field.data_type().to_string(),
    }
}

// ---------------------------------------------------------------------------------------
// Rendering: one Arrow column to JSON text per row
// ---------------------------------------------------------------------------------------

/// Where a value is going, which decides what Trino does with its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// A member of `json_object` / `json_array`: JSON-typed text is cast to varchar first,
    /// and an array or row is refused.
    Member,
    /// `CAST(.. AS JSON)`, and everything nested inside one: JSON-typed text is embedded,
    /// arrays and rows recurse.
    Cast,
}

/// Render every row of `array` as JSON text. `None` is a SQL NULL, for the caller to
/// decide about — inside a nested array or row it is always `null`.
///
/// `field` is the column's own field, because the JSON marker lives on it.
fn render(
    array: &ArrayRef,
    field: &Field,
    mode: Mode,
    who: &str,
    position: usize,
) -> DFResult<Vec<Option<String>>> {
    let n = array.len();
    let bad = |msg: String| DataFusionError::Execution(format!("{who}: {msg}"));

    let rendered: Vec<Option<String>> = match array.data_type() {
        DataType::Null => vec![None; n],

        DataType::Boolean => {
            let a = array.as_boolean();
            (0..n)
                .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
                .collect()
        }

        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => as_text(array)
            .map_err(bad)?
            .iter()
            .map(|v| v.map(str::to_string))
            .collect(),

        DataType::Float32 => {
            let a = array.as_primitive::<Float32Type>();
            (0..n)
                .map(|i| (!a.is_null(i)).then(|| double_json(jsonval::java_float(a.value(i)))))
                .collect()
        }
        DataType::Float64 => {
            let a = array.as_primitive::<Float64Type>();
            (0..n)
                .map(|i| (!a.is_null(i)).then(|| double_json(jsonval::java_double(a.value(i)))))
                .collect()
        }

        DataType::Decimal128(_, scale) => {
            let a = array.as_primitive::<Decimal128Type>();
            (0..n)
                .map(|i| {
                    (!a.is_null(i)).then(|| {
                        let v = a.value(i);
                        jsonval::java_bigdecimal(
                            v < 0,
                            &v.unsigned_abs().to_string(),
                            i64::from(*scale),
                        )
                    })
                })
                .collect()
        }
        DataType::Decimal256(_, scale) => {
            let a = array.as_primitive::<Decimal256Type>();
            (0..n)
                .map(|i| {
                    (!a.is_null(i)).then(|| {
                        let text = a.value(i).to_string();
                        let (negative, digits) = match text.strip_prefix('-') {
                            Some(d) => (true, d.to_string()),
                            None => (false, text),
                        };
                        jsonval::java_bigdecimal(negative, &digits, i64::from(*scale))
                    })
                })
                .collect()
        }

        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let text = as_text(array).map_err(bad)?;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                if text.is_null(i) {
                    out.push(None);
                    continue;
                }
                let s = text.value(i);
                out.push(match (marker_of(field), mode) {
                    // Plain text is a string.
                    (None, _) => Some(jsonval::quote(s)),
                    // Read by FORMAT JSON, or a JSON value under a cast: embedded.
                    (Some(Marker::Formatted), _) | (Some(Marker::Typed), Mode::Cast) => {
                        Some(s.to_string())
                    }
                    // A JSON-typed member: Starburst casts it to varchar first, and that
                    // cast has no answer for a container.
                    (Some(Marker::Typed), Mode::Member) => {
                        let doc = jsonval::parse(s).map_err(|e| {
                            bad(format!(
                                "row {i}: member #{position} is not valid JSON: {e}"
                            ))
                        })?;
                        match doc {
                            Json::Null => None,
                            Json::Bool(b) => Some(jsonval::quote(&b.to_string())),
                            Json::String(inner) => Some(jsonval::quote(&inner)),
                            Json::Number(num) => {
                                Some(jsonval::quote(&json_number_as_varchar(&num)))
                            }
                            Json::Array(_) | Json::Object(_) => {
                                return Err(bad(format!(
                                    "member #{position} is a JSON-typed {}, which Starburst \
                                     casts to varchar here and cannot. Write \
                                     json_format(<value>) FORMAT JSON to embed it as JSON.",
                                    if doc.as_array().is_some() {
                                        "array"
                                    } else {
                                        "object"
                                    }
                                )))
                            }
                        }
                    }
                });
            }
            out
        }

        DataType::Timestamp(unit, tz) => {
            if mode == Mode::Cast && tz.is_some() {
                return Err(bad(
                    "a timestamp with time zone cannot be cast to JSON, in Starburst either. \
                     Cast it to VARCHAR first."
                        .into(),
                ));
            }
            let a = deltalake::arrow::compute::cast(array, &DataType::Int64)
                .map_err(|e| bad(format!("could not read timestamp values: {e}")))?;
            let a = a.as_primitive::<Int64Type>();
            (0..n)
                .map(|i| {
                    if a.is_null(i) {
                        Ok(None)
                    } else {
                        timestamp_text(a.value(i), unit, tz.is_some())
                            .map(|s| Some(jsonval::quote(&s)))
                            .ok_or_else(|| bad(format!("row {i}: timestamp out of range")))
                    }
                })
                .collect::<DFResult<_>>()?
        }

        DataType::Time32(_) | DataType::Time64(_) if mode == Mode::Cast => {
            return Err(bad(
                "a time cannot be cast to JSON, in Starburst either. Cast it to VARCHAR first."
                    .into(),
            ))
        }

        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => as_text(array)
            .map_err(bad)?
            .iter()
            .map(|v| v.map(jsonval::quote))
            .collect(),

        DataType::List(inner) if mode == Mode::Cast => {
            render_list(array.as_list::<i32>(), inner, who)?
        }
        DataType::LargeList(inner) if mode == Mode::Cast => {
            render_list(array.as_list::<i64>(), inner, who)?
        }
        DataType::FixedSizeList(inner, _) if mode == Mode::Cast => {
            let as_list =
                deltalake::arrow::compute::cast(array, &DataType::List(Arc::clone(inner)))
                    .map_err(|e| bad(format!("could not read a fixed-size list: {e}")))?;
            render_list(as_list.as_list::<i32>(), inner, who)?
        }
        DataType::Struct(fields) if mode == Mode::Cast => {
            let sa = array.as_struct();
            let mut cols = Vec::with_capacity(fields.len());
            for (f, col) in fields.iter().zip(sa.columns()) {
                cols.push((
                    jsonval::quote(f.name()),
                    render(col, f, Mode::Cast, who, position)?,
                ));
            }
            (0..n)
                .map(|i| {
                    if sa.is_null(i) {
                        return None;
                    }
                    let mut s = String::from("{");
                    for (k, (key, vals)) in cols.iter().enumerate() {
                        if k > 0 {
                            s.push(',');
                        }
                        s.push_str(key);
                        s.push(':');
                        s.push_str(vals[i].as_deref().unwrap_or("null"));
                    }
                    s.push('}');
                    Some(s)
                })
                .collect()
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            return Err(bad(format!(
                "member #{position} is an array, which Starburst would cast to varchar text \
                 here. Write json_format(CAST(<value> AS JSON)) FORMAT JSON to embed it as a \
                 JSON array."
            )))
        }
        DataType::Struct(_) => {
            return Err(bad(format!(
                "member #{position} is a row, which Starburst would cast to varchar text \
                 here. Write json_format(CAST(<value> AS JSON)) FORMAT JSON to embed it as a \
                 JSON object."
            )))
        }

        DataType::Dictionary(_, value_type) => {
            let plain = deltalake::arrow::compute::cast(array, value_type)
                .map_err(|e| bad(format!("could not read a dictionary column: {e}")))?;
            let plain_field = Field::new(field.name(), (**value_type).clone(), true)
                .with_metadata(field.metadata().clone());
            render(&plain, &plain_field, mode, who, position)?
        }

        other => {
            return Err(bad(format!(
                "a value of type {other} cannot be rendered as JSON. Cast it to a text, \
                 numeric, boolean, date or timestamp type in the model first."
            )))
        }
    };
    Ok(rendered)
}

fn render_list<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    inner: &FieldRef,
    who: &str,
) -> DFResult<Vec<Option<String>>> {
    let elements = render(list.values(), inner, Mode::Cast, who, 1)?;
    let offsets = list.value_offsets();
    Ok((0..list.len())
        .map(|i| {
            if list.is_null(i) {
                return None;
            }
            let (start, end) = (offsets[i].as_usize(), offsets[i + 1].as_usize());
            let mut s = String::from("[");
            for (k, e) in elements[start..end].iter().enumerate() {
                if k > 0 {
                    s.push(',');
                }
                s.push_str(e.as_deref().unwrap_or("null"));
            }
            s.push(']');
            Some(s)
        })
        .collect())
}

/// Every row as text, via Arrow's own rendering of the type.
fn as_text(array: &ArrayRef) -> std::result::Result<StringArray, String> {
    let cast = deltalake::arrow::compute::cast(array, &DataType::Utf8)
        .map_err(|e| format!("could not render {} as text: {e}", array.data_type()))?;
    Ok(cast.as_string::<i32>().clone())
}

/// A double the way Jackson writes one: a number, unless it is not one — `NaN` and the
/// infinities become strings, because JSON has no spelling for them and that is what
/// Jackson does by default.
fn double_json(java: String) -> String {
    if java == "NaN" || java.ends_with("Infinity") {
        jsonval::quote(&java)
    } else {
        java
    }
}

/// A JSON number cast to varchar, as Trino does it: an integer keeps its digits, a float
/// goes through the double-to-varchar cast, which always writes an exponent (`1.5E0`).
fn json_number_as_varchar(text: &str) -> String {
    if !text.contains(['.', 'e', 'E']) {
        return text.to_string();
    }
    match text.parse::<f64>() {
        Ok(v) if v.is_finite() => {
            let sci = format!("{:e}", v.abs());
            let (mantissa, exponent) = sci.split_once('e').expect("{:e} always has an exponent");
            let mantissa = if mantissa.contains('.') {
                mantissa.to_string()
            } else {
                format!("{mantissa}.0")
            };
            format!("{}{mantissa}E{exponent}", if v < 0.0 { "-" } else { "" })
        }
        _ => text.to_string(),
    }
}

/// Trino's spelling of a timestamp: `YYYY-MM-DD HH:MM:SS[.fff]`.
///
/// A naive value shows the unit's precision. A zoned one shows three digits and ` UTC`,
/// rendered in UTC: that is how Starburst reads a Delta `timestamp` — as `timestamp(3)
/// with time zone` — and what its varchar cast prints.
fn timestamp_text(value: i64, unit: &TimeUnit, zoned: bool) -> Option<String> {
    let (secs, nanos, digits) = match unit {
        TimeUnit::Second => (value, 0u32, 0usize),
        TimeUnit::Millisecond => (
            value.div_euclid(1_000),
            (value.rem_euclid(1_000) * 1_000_000) as u32,
            3,
        ),
        TimeUnit::Microsecond => (
            value.div_euclid(1_000_000),
            (value.rem_euclid(1_000_000) * 1_000) as u32,
            6,
        ),
        TimeUnit::Nanosecond => (
            value.div_euclid(1_000_000_000),
            value.rem_euclid(1_000_000_000) as u32,
            9,
        ),
    };
    let digits = if zoned { 3 } else { digits };
    let dt = chrono::DateTime::from_timestamp(secs, nanos)?;
    let mut s = dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string();
    if digits > 0 {
        s.push('.');
        s.push_str(&format!("{nanos:09}")[..digits]);
    }
    if zoned {
        s.push_str(" UTC");
    }
    Some(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::{SqlTransform, Transform};
    use deltalake::arrow::array::{
        BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch,
        TimestampMicrosecondArray,
    };
    use deltalake::arrow::datatypes::Schema;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("paid", DataType::Boolean, true),
                Field::new("amount", DataType::Decimal128(30, 4), true),
                Field::new("ratio", DataType::Float64, true),
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Field::new(
                    "zts",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                ),
                Field::new("data", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("O\"Brien \\ \u{1F600}"), None])),
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(
                    Decimal128Array::from(vec![Some(123_400i128), None])
                        .with_precision_and_scale(30, 4)
                        .unwrap(),
                ),
                Arc::new(Float64Array::from(vec![Some(0.25), Some(2.0)])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_711_924_200_123_456i64),
                    None,
                ])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1_711_924_200_123_456i64), None])
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"lines":[{"sku":"A","qty":2},{"sku":"B","qty":1.10}],"note":null}"#),
                    Some(r#"{"lines":[]}"#),
                ])),
            ],
        )
        .unwrap()
    }

    async fn rows(expr: &str) -> Vec<Option<String>> {
        let out = SqlTransform::new(format!("SELECT {expr} AS v FROM source"))
            .apply(vec![batch()])
            .await
            .unwrap_or_else(|e| panic!("{expr} failed: {e}"));
        let mut got = Vec::new();
        for b in &out {
            let c = b.column(0).as_string::<i32>();
            for i in 0..c.len() {
                got.push((!c.is_null(i)).then(|| c.value(i).to_string()));
            }
        }
        got
    }

    async fn first(expr: &str) -> String {
        rows(expr)
            .await
            .into_iter()
            .next()
            .unwrap()
            .expect("not null")
    }

    async fn fails(sql: &str) -> String {
        SqlTransform::new(sql)
            .apply(vec![batch()])
            .await
            .expect_err("should fail")
            .to_string()
    }

    #[tokio::test]
    async fn members_come_out_in_javas_hashmap_order_with_their_types() {
        // Trino's own test: key_1, key_2 come out the other way round.
        assert_eq!(
            first("json_object('key_1' VALUE id, 'key_2' VALUE name)").await,
            r#"{"key_2":"O\"Brien \\ 😀","key_1":7}"#
        );
        assert_eq!(
            first("json_object('id' VALUE id, 'paid' VALUE paid, 'amount' VALUE amount)").await,
            r#"{"amount":12.3400,"paid":true,"id":7}"#
        );
    }

    #[tokio::test]
    async fn null_on_null_is_the_default_for_an_object_and_absent_for_an_array() {
        let got = rows("json_object('id' VALUE id, 'name' VALUE name)").await;
        assert_eq!(got[1].as_deref(), Some(r#"{"name":null,"id":8}"#));
        let got = rows("json_object('id' VALUE id, 'name' VALUE name ABSENT ON NULL)").await;
        assert_eq!(got[1].as_deref(), Some(r#"{"id":8}"#));

        let got = rows("json_array(id, name, paid)").await;
        assert_eq!(got[1].as_deref(), Some("[8]"));
        let got = rows("json_array(id, name, paid NULL ON NULL)").await;
        assert_eq!(got[1].as_deref(), Some("[8,null,null]"));
    }

    #[tokio::test]
    async fn an_empty_object_and_array_are_themselves() {
        assert_eq!(first("json_object()").await, "{}");
        assert_eq!(first("json_array()").await, "[]");
    }

    #[tokio::test]
    async fn a_directly_nested_constructor_is_embedded_as_json() {
        // The implicit FORMAT JSON Trino's analyzer applies to a nested constructor.
        assert_eq!(
            first("json_object('data' VALUE json_object('id' VALUE id))").await,
            r#"{"data":{"id":7}}"#
        );
        assert_eq!(
            first("json_array(json_object('id' VALUE id), json_array(1, 2))").await,
            r#"[{"id":7},[1,2]]"#
        );
        assert_eq!(
            first("json_object('q' VALUE json_query(data, '$.lines[0]'))").await,
            r#"{"q":{"sku":"A","qty":2}}"#
        );
    }

    #[tokio::test]
    async fn a_constructor_that_arrives_any_other_way_is_text() {
        // Through a CASE it is a varchar, and a varchar is a string — as in Starburst.
        assert_eq!(
            first("json_object('d' VALUE CASE WHEN paid THEN json_object('id' VALUE id) END)")
                .await,
            r#"{"d":"{\"id\":7}"}"#
        );
        // And FORMAT JSON is how the model says otherwise.
        assert_eq!(
            first(
                "json_object('d' VALUE CASE WHEN paid THEN json_object('id' VALUE id) END \
                 FORMAT JSON)"
            )
            .await,
            r#"{"d":{"id":7}}"#
        );
        let out = SqlTransform::new(
            "WITH built AS (SELECT id, json_object('id' VALUE id) AS payload FROM source) \
             SELECT json_object('as_text' VALUE payload, 'as_json' VALUE payload FORMAT JSON) \
             AS v FROM built",
        )
        .apply(vec![batch()])
        .await
        .unwrap();
        assert_eq!(
            out[0].column(0).as_string::<i32>().value(0),
            r#"{"as_text":"{\"id\":7}","as_json":{"id":7}}"#
        );
    }

    #[tokio::test]
    async fn format_json_reads_text_the_way_jackson_does() {
        assert_eq!(
            first("json_object('raw' VALUE '{\"b\": 1.10, \"a\": 1e2, \"b\": 2}' FORMAT JSON)")
                .await,
            r#"{"raw":{"b":2,"a":100.0}}"#
        );
        assert_eq!(
            first("json_array('{\"a\":1}' FORMAT JSON, '{\"a\":1}')").await,
            r#"[{"a":1},"{\"a\":1}"]"#
        );
        let e = fails("SELECT json_object('raw' VALUE name FORMAT JSON) AS v FROM source").await;
        assert!(e.contains("not valid JSON"), "got: {e}");
    }

    #[tokio::test]
    async fn a_json_typed_member_is_cast_to_varchar_like_starburst_does() {
        // A scalar survives the cast, as a string; a container does not.
        assert_eq!(
            first(
                "json_object('n' VALUE json_extract(data, '$.lines[0].qty'), \
                   's' VALUE json_extract(data, '$.lines[0].sku'), \
                   'f' VALUE json_extract(data, '$.lines[1].qty'), \
                   'none' VALUE json_extract(data, '$.note'))"
            )
            .await,
            r#"{"s":"A","f":"1.1E0","none":null,"n":"2"}"#
        );
        let e =
            fails("SELECT json_object('l' VALUE json_extract(data, '$.lines')) AS v FROM source")
                .await;
        assert!(e.contains("JSON-typed array"), "got: {e}");
        assert!(
            e.contains("json_format(<value>) FORMAT JSON"),
            "names the fix: {e}"
        );
        // The spelling that works there works here, and keeps the source order.
        assert_eq!(
            first("json_object('l' VALUE json_format(json_extract(data, '$.lines')) FORMAT JSON)")
                .await,
            r#"{"l":[{"sku":"A","qty":2},{"sku":"B","qty":1.1}]}"#
        );
    }

    #[test]
    fn format_json_directly_on_a_json_typed_call_is_refused_at_config_load() {
        let e = crate::transform::validate::validate_sql(
            "SELECT json_object('l' VALUE json_extract(data, '$.lines') FORMAT JSON) AS v FROM source",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("FORMAT JSON on json_extract(..)"), "got: {e}");
        assert!(e.contains("json_format(<value>) FORMAT JSON"), "got: {e}");
    }

    #[tokio::test]
    async fn format_json_on_a_json_typed_column_is_refused_before_any_row() {
        let e = fails(
            "WITH j AS (SELECT json_extract(data, '$.lines') AS l FROM source) \
             SELECT json_object('l' VALUE l FORMAT JSON) AS v FROM j",
        )
        .await;
        assert!(e.contains("FORMAT JSON on a JSON-typed value"), "got: {e}");
    }

    #[tokio::test]
    async fn a_nested_constructor_returning_json_is_embedded_too() {
        assert_eq!(
            first("json_object('d' VALUE json_object('id' VALUE id RETURNING JSON))").await,
            r#"{"d":{"id":7}}"#
        );
    }

    #[tokio::test]
    async fn returning_json_makes_the_result_json_typed() {
        // Cast to JSON, a JSON-typed value is embedded verbatim.
        assert_eq!(
            first("json_format(CAST(json_object('id' VALUE id RETURNING JSON) AS JSON))").await,
            r#"{"id":7}"#
        );
        // Text is a string.
        assert_eq!(
            first("json_format(CAST(json_object('id' VALUE id) AS JSON))").await,
            r#""{\"id\":7}""#
        );
    }

    #[tokio::test]
    async fn cast_to_json_follows_trinos_rules() {
        assert_eq!(first("CAST(name AS JSON)").await, r#""O\"Brien \\ 😀""#);
        assert_eq!(first("CAST(id AS JSON)").await, "7");
        assert_eq!(first("CAST(amount AS JSON)").await, "12.3400");
        assert_eq!(first("CAST(ratio AS JSON)").await, "0.25");
        assert_eq!(
            first("CAST(ts AS JSON)").await,
            r#""2024-03-31 22:30:00.123456""#
        );
        assert_eq!(
            first("CAST(CAST(ts AS DATE) AS JSON)").await,
            r#""2024-03-31""#
        );
        assert_eq!(
            first("CAST(CAST(json_extract(data, '$.lines') AS ARRAY(JSON)) AS JSON)").await,
            r#"[{"sku":"A","qty":2},{"sku":"B","qty":1.10}]"#
        );
        // A NULL is a NULL, not the text `null`.
        assert_eq!(rows("CAST(name AS JSON)").await[1], None);
        let e = fails("SELECT CAST(zts AS JSON) AS v FROM source").await;
        assert!(e.contains("time zone"), "got: {e}");
    }

    #[tokio::test]
    async fn numbers_and_timestamps_render_as_java_spells_them() {
        assert_eq!(
            first("json_array(ratio, amount, ratio * 4e7)").await,
            "[0.25,12.3400,1.0E7]"
        );
        assert_eq!(
            first("json_array(ts, zts)").await,
            r#"["2024-03-31 22:30:00.123456","2024-03-31 22:30:00.123 UTC"]"#
        );
        assert_eq!(first("json_array(ratio / 0.0)").await, r#"["Infinity"]"#);
    }

    #[tokio::test]
    async fn a_duplicate_or_null_key_is_an_error() {
        let e = fails("SELECT json_object('a' VALUE 1, 'a' VALUE 2) AS v FROM source").await;
        assert!(e.contains("duplicate key"), "got: {e}");
        let e = fails("SELECT json_object(name VALUE id) AS v FROM source").await;
        assert!(e.contains("must not be null"), "got: {e}");
    }

    #[tokio::test]
    async fn an_array_member_is_refused_with_the_spelling_that_works() {
        let e = fails(
            "SELECT json_object('l' VALUE CAST(json_extract(data, '$.lines') AS ARRAY(JSON))) \
             AS v FROM source",
        )
        .await;
        assert!(e.contains("member #1 is an array"), "got: {e}");
        assert_eq!(
            first(
                "json_object('l' VALUE json_format(CAST(CAST(json_extract(data, '$.lines') \
                 AS ARRAY(JSON)) AS JSON)) FORMAT JSON)"
            )
            .await,
            r#"{"l":[{"sku":"A","qty":2},{"sku":"B","qty":1.1}]}"#
        );
    }

    #[tokio::test]
    async fn the_output_carries_no_marker() {
        let out = SqlTransform::new(
            "SELECT json_object('id' VALUE id RETURNING JSON) AS v, CAST(id AS JSON) AS w FROM source",
        )
        .apply(vec![batch()])
        .await
        .unwrap();
        assert!(out[0]
            .schema()
            .fields()
            .iter()
            .all(|f| f.metadata().is_empty()));
    }

    #[test]
    fn positional_json_object_is_refused_at_config_load_with_the_spelling() {
        let e =
            crate::transform::validate::validate_sql("SELECT json_object('a', 1) AS j FROM source")
                .unwrap_err()
                .to_string();
        assert!(e.contains("'key' VALUE value"), "got: {e}");
    }

    #[test]
    fn the_rewrite_is_what_runs() {
        let got = crate::transform::validate::normalise_sql(
            "SELECT json_object(KEY 'a' VALUE x, 'b' VALUE y FORMAT JSON, \
             'c' VALUE json_array(z) ABSENT ON NULL RETURNING JSON) AS j, \
             json_array(x, y) AS k, CAST(x AS JSON) AS c FROM source",
        )
        .unwrap();
        assert_eq!(
            got,
            "SELECT ddi_json_object('absent,json', 'a', x, 'b', ddi_format_json(y), 'c', \
             ddi_embed_json(ddi_json_array('absent,varchar', z))) AS j, \
             ddi_json_array('absent,varchar', x, y) AS k, ddi_to_json(x) AS c FROM source"
        );
    }
}
