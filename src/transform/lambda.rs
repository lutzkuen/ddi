//! Trino's `transform(array, x -> expr)` and `filter(array, x -> expr)`, within one row.
//!
//! # Why they are admissible
//!
//! A lambda here is evaluated once per element of one row's own array and sees nothing
//! else: the element, and values of the same row. The output is a function of that one
//! input row, so no batch split can change it. That is the safety argument `array_sum`
//! already rests on, and it does not widen `GROUP BY` at either grain — the row-local
//! alternative the validator's own header names is exactly this. The rejected alternative
//! (`CROSS JOIN UNNEST` then `array_agg .. GROUP BY`) cannot be made safe: `ddi` cannot
//! prove a group is one source row, and a redelivered key split across batches would emit
//! two partial messages. An array never leaves its row.
//!
//! # How a lambda runs
//!
//! DataFusion has no lambdas, so a call is rewritten at config load into a scalar function
//! whose body is text:
//!
//! ```sql
//! transform(entries, e -> json_extract_scalar(e, '$.qty') * fx.rate)
//! -- becomes
//! ddi_transform(entries, 'e', 'json_extract_scalar(e, ''$.qty'') * __capture_0', fx.rate)
//! ```
//!
//! Every name in the body that is not the parameter is a *capture*: a value of the current
//! row, passed as an extra argument and renamed. The body is then planned by DataFusion's
//! own expression planner against a schema of `(element, captures...)` — so it supports
//! the same scalar language as the `SELECT` list, `CASE`, `CAST`, arithmetic and every
//! JSON function included, and means the same thing there as it does here. At execution the
//! elements of the whole batch are flattened into one column, the captures repeated per
//! element, the body evaluated once over all of them, and the results folded back into a
//! list per row. Planning happens once per lambda per process.
//!
//! # What is refused, at config load
//!
//! A subquery, an aggregate, a window function or a second lambda-taking function inside
//! a body; a lambda anywhere but as the second argument of `transform` or `filter`; a
//! lambda with more than one parameter. Each names the construct.

use std::any::Any;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, OnceLock};

use deltalake::arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, Int32Array, ListArray, RecordBatch, UInt32Array,
};
use deltalake::arrow::buffer::OffsetBuffer;
use deltalake::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use deltalake::datafusion::common::{DFSchema, Result as DFResult, ScalarValue};
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use deltalake::datafusion::physical_expr::PhysicalExpr;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    OneOrManyWithParens, Query, Value, VisitMut, VisitorMut,
};

use crate::error::{Error, Result};
use crate::transform::validate::{aggregates, reject};

pub(crate) const TRANSFORM: &str = "ddi_transform";
pub(crate) const FILTER: &str = "ddi_filter";

/// The functions of this module, registered on every transform session.
pub fn register(ctx: &SessionContext) {
    for kind in [Kind::Transform, Kind::Filter] {
        ctx.register_udf(ScalarUDF::from(LambdaFn::new(kind)));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Transform,
    Filter,
}

impl Kind {
    fn from_spelling(name: &str) -> Option<Self> {
        match name {
            "transform" => Some(Kind::Transform),
            "filter" => Some(Kind::Filter),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Kind::Transform => TRANSFORM,
            Kind::Filter => FILTER,
        }
    }

    /// The name the author wrote, for messages.
    fn spelling(self) -> &'static str {
        match self {
            Kind::Transform => "transform",
            Kind::Filter => "filter",
        }
    }
}

// ---------------------------------------------------------------------------------------
// The rewrite
// ---------------------------------------------------------------------------------------

/// Fold every `transform` / `filter` lambda in `query` into a call this engine runs.
///
/// Inside-out, so a lambda nested in another's body is folded first and the outer body
/// sees only a function call — whose arguments it captures like any other value.
pub(crate) fn rewrite(query: &mut Query) -> Result<()> {
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
    if let Some(e) = v.0 {
        return Err(e);
    }

    // Whatever lambda is left was not the second argument of transform() or filter().
    struct Leftover(bool);
    impl VisitorMut for Leftover {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            if matches!(expr, Expr::Lambda(_)) {
                self.0 = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }
    let mut left = Leftover(false);
    let _ = query.visit(&mut left);
    if left.0 {
        return Err(reject(
            "a lambda outside transform() or filter()",
            "those are the two functions that evaluate one, per element of one row's array.",
            "write transform(<array>, x -> <expr>) or filter(<array>, x -> <expr>).",
        ));
    }
    Ok(())
}

fn replacement(expr: &Expr) -> Result<Option<Expr>> {
    let Expr::Function(f) = expr else {
        return Ok(None);
    };
    let name = bare_name(&f.name);
    let Some(kind) = Kind::from_spelling(&name) else {
        // Any other function handed a lambda is one this engine does not evaluate.
        if let FunctionArguments::List(list) = &f.args {
            if list.args.iter().any(|a| {
                matches!(
                    a,
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Lambda(_)))
                )
            }) {
                return Err(reject(
                    &format!("the lambda function {name}()"),
                    "only transform() and filter() evaluate a lambda here, per element of \
                     one row's array.",
                    "write it with transform(<array>, x -> <expr>) and filter(<array>, \
                     x -> <expr>), or compute the value downstream.",
                ));
            }
        }
        return Ok(None);
    };
    let spelling = kind.spelling();

    if f.over.is_some()
        || f.filter.is_some()
        || f.null_treatment.is_some()
        || !f.within_group.is_empty()
        || f.uses_odbc_syntax
        || !matches!(f.parameters, FunctionArguments::None)
    {
        return Err(reject(
            &format!("{spelling}() with a FILTER, OVER or WITHIN GROUP clause"),
            "it is a scalar function over one row's array.",
            &format!("write a plain {spelling}(<array>, x -> <expr>)."),
        ));
    }
    let FunctionArguments::List(list) = &f.args else {
        return Err(usage(spelling));
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return Err(usage(spelling));
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(array)), FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Lambda(lambda)))] =
        list.args.as_slice()
    else {
        return Err(usage(spelling));
    };

    let param = match &lambda.params {
        OneOrManyWithParens::One(p) => p,
        OneOrManyWithParens::Many(ps) if ps.len() == 1 => &ps[0],
        OneOrManyWithParens::Many(ps) => {
            return Err(reject(
                &format!("{spelling}() with a {}-parameter lambda", ps.len()),
                "the lambda sees one element at a time, and its position is not something \
                 a row-local function can know.",
                &format!("write {spelling}(<array>, x -> <expr>) with one parameter."),
            ));
        }
    };
    // The body is planned against a field of this name. Unquoted identifiers are
    // lower-cased by the planner, so the field is too; a quoted one is kept as written.
    let param_name = if param.quote_style.is_some() {
        param.value.clone()
    } else {
        param.value.to_ascii_lowercase()
    };

    check_body(&lambda.body, spelling)?;

    let mut body = (*lambda.body).clone();
    let captures = capture(&mut body, param);

    let mut args = vec![
        array.clone(),
        literal(&param_name),
        literal(&body.to_string()),
    ];
    args.extend(captures);
    let mut call = f.clone();
    call.name = ObjectName::from(vec![Ident::new(kind.name())]);
    call.args = FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: None,
        args: args
            .into_iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
            .collect(),
        clauses: vec![],
    });
    Ok(Some(Expr::Function(call)))
}

fn usage(spelling: &str) -> Error {
    reject(
        &format!("this form of {spelling}()"),
        "it takes an array and a lambda over its elements, nothing else.",
        &format!("write {spelling}(<array>, x -> <expr>)."),
    )
}

/// Refuse a body that reaches outside its element and its row.
///
/// Reported in terms of the construct, as the validator does for the query around it. The
/// body becomes text after this, so nothing later can look inside it: this is the gate.
fn check_body(body: &Expr, spelling: &str) -> Result<()> {
    struct Check<'a> {
        found: Option<Error>,
        spelling: &'a str,
    }
    impl VisitorMut for Check<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
            self.found = Some(reject(
                &format!("a subquery inside a {}() lambda", self.spelling),
                "the body is evaluated per element of one row's array and reads no \
                 relation — not even the source batch.",
                "compute the value in the SELECT list and use it in the body: a column of \
                 the current row may be referenced there directly.",
            ));
            ControlFlow::Break(())
        }

        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            if self.found.is_some() {
                return ControlFlow::Break(());
            }
            let Expr::Function(f) = expr else {
                return ControlFlow::Continue(());
            };
            let name = bare_name(&f.name);
            if f.over.is_some() {
                self.found = Some(reject(
                    &format!(
                        "a window function ({name}() OVER) inside a {}() lambda",
                        self.spelling
                    ),
                    "a window frame reaches across rows, and a lambda sees one element of \
                     one row.",
                    "compute per-row values here and window downstream.",
                ));
                return ControlFlow::Break(());
            }
            if aggregates().contains(&name) {
                self.found = Some(reject(
                    &format!(
                        "the aggregate function {name}() inside a {}() lambda",
                        self.spelling
                    ),
                    "it combines values across rows, and a lambda sees one element of one \
                     row.",
                    "reduce within the row with array_sum / array_min / array_max / \
                     array_avg / array_length, or aggregate downstream.",
                ));
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }

    let mut check = Check {
        found: None,
        spelling,
    };
    let mut copy = body.clone();
    let _ = copy.visit(&mut check);
    match check.found {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Replace every reference to something other than the parameter with a numbered capture,
/// and return the expressions captured, in order.
///
/// A compound name whose head is the parameter — `e.price` on an array of structs — is a
/// field access on the element and stays. Any other compound name is a qualified column of
/// the row (`fx_rates.rate`) and is captured whole.
fn capture(body: &mut Expr, param: &Ident) -> Vec<Expr> {
    struct Captures<'a> {
        param: &'a Ident,
        seen: Vec<Expr>,
    }
    impl Captures<'_> {
        fn is_param(&self, id: &Ident) -> bool {
            if self.param.quote_style.is_some() || id.quote_style.is_some() {
                id.value == self.param.value
            } else {
                id.value.eq_ignore_ascii_case(&self.param.value)
            }
        }
        fn index(&mut self, e: &Expr) -> usize {
            if let Some(i) = self.seen.iter().position(|s| s == e) {
                return i;
            }
            self.seen.push(e.clone());
            self.seen.len() - 1
        }
    }
    impl VisitorMut for Captures<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            let captured = match expr {
                Expr::Identifier(id) => !self.is_param(id),
                Expr::CompoundIdentifier(parts) => !parts.first().is_some_and(|p| self.is_param(p)),
                _ => false,
            };
            if captured {
                let n = self.index(expr);
                *expr = Expr::Identifier(Ident::new(capture_name(n)));
            }
            ControlFlow::Continue(())
        }
    }
    let mut c = Captures {
        param,
        seen: Vec::new(),
    };
    let _ = body.visit(&mut c);
    c.seen
}

fn capture_name(n: usize) -> String {
    format!("__capture_{n}")
}

fn literal(text: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(text.to_string()).into())
}

fn bare_name(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|p| p.to_string())
        .unwrap_or_default()
        .trim_matches('"')
        .to_ascii_lowercase()
}

// ---------------------------------------------------------------------------------------
// The functions
// ---------------------------------------------------------------------------------------

/// A lambda body, planned once against the schema it is evaluated over.
struct Compiled {
    /// `(param, __capture_0, __capture_1, ...)`
    schema: SchemaRef,
    expr: Arc<dyn PhysicalExpr>,
    /// What the body evaluates to, marker included.
    out: FieldRef,
}

/// Planned bodies, by function, text and input schema. A lambda is planned once per
/// process rather than once per batch: the planner is not free, and the body does not
/// change.
fn cache() -> &'static Mutex<HashMap<String, Arc<Compiled>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Compiled>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn compile(
    kind: Kind,
    param: &str,
    body: &str,
    element: &FieldRef,
    captures: &[FieldRef],
) -> DFResult<Arc<Compiled>> {
    let mut fields = vec![Field::new(param, element.data_type().clone(), true)
        .with_metadata(element.metadata().clone())];
    for (n, c) in captures.iter().enumerate() {
        fields.push(
            Field::new(capture_name(n), c.data_type().clone(), true)
                .with_metadata(c.metadata().clone()),
        );
    }
    let schema = Arc::new(Schema::new(fields));
    let key = format!("{}\u{0}{param}\u{0}{body}\u{0}{schema:?}", kind.name());
    if let Some(hit) = cache().lock().expect("lambda cache").get(&key) {
        return Ok(Arc::clone(hit));
    }

    let spelling = kind.spelling();
    let ctx = SessionContext::new();
    crate::transform::udf::register_udfs(&ctx);
    let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
    let logical = ctx.parse_sql_expr(body, &df_schema).map_err(|e| {
        DataFusionError::Plan(format!(
            "{spelling}: the lambda body `{body}` does not plan over an element of type {} \
             (as `{param}`): {e}",
            element.data_type()
        ))
    })?;
    let expr = ctx.create_physical_expr(logical, &df_schema).map_err(|e| {
        DataFusionError::Plan(format!(
            "{spelling}: the lambda body `{body}` does not plan: {e}"
        ))
    })?;
    let out = expr.return_field(&schema)?;
    if kind == Kind::Filter && out.data_type() != &DataType::Boolean {
        return Err(DataFusionError::Plan(format!(
            "filter: the lambda body `{body}` evaluates to {}, and a predicate must be a \
             boolean",
            out.data_type()
        )));
    }

    let compiled = Arc::new(Compiled { schema, expr, out });
    cache()
        .lock()
        .expect("lambda cache")
        .insert(key, Arc::clone(&compiled));
    Ok(compiled)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct LambdaFn {
    kind: Kind,
    signature: Signature,
}

impl LambdaFn {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    fn err(&self, msg: impl std::fmt::Display) -> DataFusionError {
        DataFusionError::Execution(format!("{}: {msg}", self.kind.spelling()))
    }

    /// The element field of the array argument.
    fn element(&self, array: &FieldRef) -> DFResult<FieldRef> {
        match array.data_type() {
            DataType::List(f) | DataType::LargeList(f) => Ok(Arc::clone(f)),
            other => Err(self.err(format!(
                "the first argument must be an array, got {other}. A JSON array becomes one \
                 with CAST(<json> AS ARRAY(JSON))."
            ))),
        }
    }

    fn texts(&self, scalars: &[Option<&ScalarValue>]) -> DFResult<(String, String)> {
        match (scalars.get(1), scalars.get(2)) {
            (
                Some(Some(ScalarValue::Utf8(Some(param)))),
                Some(Some(ScalarValue::Utf8(Some(body)))),
            ) => Ok((param.clone(), body.clone())),
            _ => Err(self.err(format!(
                "{} is internal; write {}(<array>, x -> <expr>)",
                self.kind.name(),
                self.kind.spelling()
            ))),
        }
    }
}

impl ScalarUDFImpl for LambdaFn {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.kind.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        match (self.kind, arg_types.first()) {
            (Kind::Filter, Some(t)) => Ok(t.clone()),
            _ => Err(self.err(
                "the return type depends on the lambda body, which return_field_from_args \
                 plans; this path should not be reached",
            )),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let Some(array) = args.arg_fields.first() else {
            return Err(self.err("missing its array argument"));
        };
        let element = self.element(array)?;
        let (param, body) = self.texts(args.scalar_arguments)?;
        let captures = args.arg_fields.get(3..).unwrap_or_default();
        let compiled = compile(self.kind, &param, &body, &element, captures)?;
        let field = match self.kind {
            Kind::Filter => array.as_ref().clone().with_name(self.name()),
            Kind::Transform => Field::new(
                self.name(),
                DataType::List(Arc::new(item_field(&compiled.out))),
                array.is_nullable(),
            ),
        };
        Ok(Arc::new(field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let rows = args.number_rows;
        let list = self.list(&args.args[0], rows)?;
        let element = self.element(&args.arg_fields[0])?;
        let scalars: Vec<Option<&ScalarValue>> = args
            .args
            .iter()
            .map(|a| match a {
                ColumnarValue::Scalar(s) => Some(s),
                ColumnarValue::Array(_) => None,
            })
            .collect();
        let (param, body) = self.texts(&scalars)?;
        let capture_fields = args.arg_fields.get(3..).unwrap_or_default();
        let compiled = compile(self.kind, &param, &body, &element, capture_fields)?;

        // Flatten: every element of every non-null row, remembering the row it came from.
        let offsets = list.value_offsets();
        let mut element_index: Vec<i32> = Vec::new();
        let mut row_index: Vec<u32> = Vec::new();
        let mut lengths: Vec<usize> = Vec::with_capacity(rows);
        for row in 0..list.len() {
            if list.is_null(row) {
                lengths.push(0);
                continue;
            }
            let (start, end) = (offsets[row], offsets[row + 1]);
            element_index.extend(start..end);
            row_index.extend(std::iter::repeat_n(row as u32, (end - start) as usize));
            lengths.push((end - start) as usize);
        }
        let count = element_index.len();

        let elements = deltalake::arrow::compute::take(
            list.values().as_ref(),
            &Int32Array::from(element_index),
            None,
        )?;
        let row_index = UInt32Array::from(row_index);
        let mut columns = vec![Arc::clone(&elements)];
        for capture in &args.args[3..] {
            let whole = match capture {
                ColumnarValue::Array(a) => Arc::clone(a),
                ColumnarValue::Scalar(s) => s.to_array_of_size(rows)?,
            };
            columns.push(deltalake::arrow::compute::take(
                whole.as_ref(),
                &row_index,
                None,
            )?);
        }

        let result = if count == 0 {
            new_empty_array(compiled.out.data_type())
        } else {
            let batch = RecordBatch::try_new(Arc::clone(&compiled.schema), columns)?;
            compiled.expr.evaluate(&batch)?.into_array(count)?
        };

        let out: ArrayRef = match self.kind {
            Kind::Transform => Arc::new(ListArray::new(
                Arc::new(item_field(&compiled.out)),
                OffsetBuffer::<i32>::from_lengths(lengths),
                result,
                list.nulls().cloned(),
            )),
            Kind::Filter => {
                let verdicts = result.as_boolean();
                // NULL is not true: the element goes, as it does in Trino.
                let keep: Vec<bool> = (0..count)
                    .map(|i| !verdicts.is_null(i) && verdicts.value(i))
                    .collect();
                let mut kept_lengths = Vec::with_capacity(rows);
                let mut at = 0;
                for len in &lengths {
                    kept_lengths.push(keep[at..at + len].iter().filter(|k| **k).count());
                    at += len;
                }
                let kept = deltalake::arrow::compute::filter(
                    elements.as_ref(),
                    &deltalake::arrow::array::BooleanArray::from(keep),
                )?;
                let DataType::List(field) = list.data_type() else {
                    unreachable!("a ListArray has a list type")
                };
                Arc::new(ListArray::new(
                    Arc::clone(field),
                    OffsetBuffer::<i32>::from_lengths(kept_lengths),
                    kept,
                    list.nulls().cloned(),
                ))
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

impl LambdaFn {
    /// The array argument as a `ListArray`, whatever shape it arrived in.
    fn list(&self, value: &ColumnarValue, rows: usize) -> DFResult<ListArray> {
        let array = match value {
            ColumnarValue::Array(a) => Arc::clone(a),
            ColumnarValue::Scalar(s) => s.to_array_of_size(rows)?,
        };
        match array.data_type() {
            DataType::List(_) => Ok(array.as_list::<i32>().clone()),
            DataType::LargeList(f) => {
                let narrowed =
                    deltalake::arrow::compute::cast(&array, &DataType::List(Arc::clone(f)))?;
                Ok(narrowed.as_list::<i32>().clone())
            }
            other => Err(self.err(format!(
                "the first argument must be an array, got {other}. A JSON array becomes one \
                 with CAST(<json> AS ARRAY(JSON))."
            ))),
        }
    }
}

/// The element field of a `transform` result: what the body produced, under the name
/// DataFusion gives list items, nullable because any body may yield NULL.
fn item_field(out: &FieldRef) -> Field {
    Field::new("item", out.data_type().clone(), true).with_metadata(out.metadata().clone())
}

#[cfg(test)]
mod tests {
    use crate::transform::validate::{normalise_sql, validate_sql};

    #[test]
    fn a_lambda_becomes_a_call_with_its_body_as_text() {
        let got = normalise_sql(
            "SELECT transform(xs, x -> json_extract_scalar(x, '$.qty') * rate) AS ys FROM source",
        )
        .unwrap();
        assert_eq!(
            got,
            "SELECT ddi_transform(xs, 'x', 'json_extract_scalar(x, ''$.qty'') * __capture_0', \
             rate) AS ys FROM source"
        );
    }

    #[test]
    fn captures_are_deduplicated_and_qualified_names_are_captured_whole() {
        let got = normalise_sql(
            "SELECT filter(o.xs, e -> e > fx.rate AND e < fx.rate * o.scale) AS ys \
             FROM source AS o",
        )
        .unwrap();
        assert_eq!(
            got,
            "SELECT ddi_filter(o.xs, 'e', 'e > __capture_0 AND e < __capture_0 * __capture_1', \
             fx.rate, o.scale) AS ys FROM source AS o"
        );
    }

    #[test]
    fn a_field_of_the_element_is_not_a_capture() {
        let got =
            normalise_sql("SELECT transform(items, li -> li.price * li.qty) AS t FROM source")
                .unwrap();
        assert!(got.contains("'li.price * li.qty'"), "got: {got}");
        assert!(
            got.ends_with("'li.price * li.qty') AS t FROM source"),
            "no captures: {got}"
        );
    }

    #[test]
    fn nested_lambdas_fold_inside_out() {
        let got = normalise_sql(
            "SELECT transform(outer_xs, a -> filter(inner_xs, b -> b > a)) AS t FROM source",
        )
        .unwrap();
        assert_eq!(
            got,
            "SELECT ddi_transform(outer_xs, 'a', 'ddi_filter(__capture_0, ''b'', ''b > \
             __capture_0'', a)', inner_xs) AS t FROM source"
        );
    }

    #[test]
    fn the_parameter_name_is_lower_cased_like_the_planner_would() {
        let got =
            normalise_sql("SELECT transform(xs, Entry -> Entry + 1) AS t FROM source").unwrap();
        assert!(got.contains("'entry', 'Entry + 1'"), "got: {got}");
    }

    #[test]
    fn a_subquery_in_a_body_is_refused_by_name() {
        let e =
            validate_sql("SELECT transform(xs, x -> (SELECT max(y) FROM source)) AS t FROM source")
                .unwrap_err()
                .to_string();
        assert!(
            e.contains("a subquery inside a transform() lambda"),
            "got: {e}"
        );
    }

    #[test]
    fn an_aggregate_in_a_body_is_refused_by_name() {
        let e = validate_sql("SELECT transform(xs, x -> x + sum(y)) AS t FROM source")
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("aggregate function sum() inside a transform() lambda"),
            "got: {e}"
        );
    }

    #[test]
    fn a_window_function_in_a_body_is_refused_by_name() {
        let e = validate_sql(
            "SELECT filter(xs, x -> x > row_number() OVER (ORDER BY y)) AS t FROM source",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("window function"), "got: {e}");
        assert!(e.contains("filter() lambda"), "got: {e}");
    }

    #[test]
    fn a_two_parameter_lambda_is_refused() {
        let e = validate_sql("SELECT transform(xs, (x, i) -> x + i) AS t FROM source")
            .unwrap_err()
            .to_string();
        assert!(e.contains("2-parameter lambda"), "got: {e}");
    }

    #[test]
    fn another_lambda_function_is_refused_by_name() {
        let e = validate_sql("SELECT reduce(xs, 0, (s, x) -> s + x, s -> s) AS t FROM source")
            .unwrap_err()
            .to_string();
        assert!(e.contains("the lambda function reduce()"), "got: {e}");
        let e = validate_sql("SELECT any_match(xs, x -> x > 1) AS t FROM source")
            .unwrap_err()
            .to_string();
        assert!(e.contains("any_match()"), "got: {e}");
    }

    #[test]
    fn a_lambda_in_the_wrong_place_is_refused() {
        let e = validate_sql("SELECT (x -> x + 1) AS t FROM source")
            .unwrap_err()
            .to_string();
        assert!(e.contains("outside transform() or filter()"), "got: {e}");
        let e = validate_sql("SELECT transform(xs, 1) AS t FROM source")
            .unwrap_err()
            .to_string();
        assert!(e.contains("this form of transform()"), "got: {e}");
    }

    #[test]
    fn group_by_and_windows_stay_refused_around_a_lambda() {
        let e = validate_sql("SELECT k, transform(xs, x -> x + 1) AS t FROM source GROUP BY k, xs")
            .unwrap_err()
            .to_string();
        assert!(e.contains("GROUP BY is not supported"), "got: {e}");
    }
}
