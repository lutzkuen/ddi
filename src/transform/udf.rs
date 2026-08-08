//! Scalar UDFs for intra-row aggregation.
//!
//! These are the sanctioned alternative to `GROUP BY`: they aggregate *within* a single
//! row's array column and are row-local by construction, so they cannot reach across rows
//! and cannot be affected by how input is split into batches.
//!
//! `array_sum(line_items, 'price * qty')` is the headline case — summing an expression
//! over an array of structs, which is what order line items actually look like.

use std::any::Any;
use std::sync::Arc;

use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Float64Builder, Int64Array, ListArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Float64Type, Int64Type};
use deltalake::datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use deltalake::datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use deltalake::datafusion::prelude::SessionContext;

/// Register every intra-row UDF on a session.
pub fn register_udfs(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ArrayLength::new()));
    for op in [Reduce::Sum, Reduce::Min, Reduce::Max, Reduce::Avg] {
        ctx.register_udf(ScalarUDF::from(ArrayReduce::new(op)));
    }
}

/// `array_length(array) -> int64`
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayLength {
    signature: Signature,
}

impl ArrayLength {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayLength {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_length"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: deltalake::datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        let arr = to_array(&args.args[0], args.number_rows)?;
        let list = as_list(&arr)?;
        // Explicit choice (plan §3): an empty array yields 0, a NULL array yields NULL.
        // It never drops the row — that is `explode` semantics, not this.
        let out: Int64Array = (0..list.len())
            .map(|i| {
                if list.is_null(i) {
                    None
                } else {
                    Some(list.value(i).len() as i64)
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Reduce {
    Sum,
    Min,
    Max,
    Avg,
}

impl Reduce {
    fn name(&self) -> &'static str {
        match self {
            Reduce::Sum => "array_sum",
            Reduce::Min => "array_min",
            Reduce::Max => "array_max",
            Reduce::Avg => "array_avg",
        }
    }

    /// Combine the per-element values of one row.
    ///
    /// Returns `None` for an empty (or all-null) array: there is no meaningful sum of
    /// nothing, and emitting 0 would silently invent data.
    fn apply(&self, vals: &[f64]) -> Option<f64> {
        if vals.is_empty() {
            return None;
        }
        Some(match self {
            Reduce::Sum => vals.iter().sum(),
            Reduce::Min => vals.iter().copied().fold(f64::INFINITY, f64::min),
            Reduce::Max => vals.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            Reduce::Avg => vals.iter().sum::<f64>() / vals.len() as f64,
        })
    }
}

/// `array_sum(array) -> float64` and `array_sum(array_of_structs, 'expr') -> float64`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayReduce {
    op: Reduce,
    signature: Signature,
}

impl ArrayReduce {
    fn new(op: Reduce) -> Self {
        Self {
            op,
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ArrayReduce {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.op.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(
        &self,
        args: deltalake::datafusion::logical_expr::ScalarFunctionArgs,
    ) -> DFResult<ColumnarValue> {
        let arr = to_array(&args.args[0], args.number_rows)?;
        let list = as_list(&arr)?;

        // Optional second argument: an arithmetic expression over the struct's fields.
        let expr = match args.args.get(1) {
            None => None,
            Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))) => Some(parse_expr(s)?),
            Some(_) => {
                return Err(DataFusionError::Execution(format!(
                    "{}: the second argument must be a string literal expression, e.g. \
                     {}(line_items, 'price * qty')",
                    self.op.name(),
                    self.op.name()
                )));
            }
        };

        let mut out = Float64Builder::with_capacity(list.len());
        for i in 0..list.len() {
            if list.is_null(i) {
                out.append_null();
                continue;
            }
            let elems = list.value(i);
            let vals = match &expr {
                None => numeric_values(&elems)?,
                Some(e) => struct_expr_values(&elems, e, self.op.name())?,
            };
            match self.op.apply(&vals) {
                Some(v) => out.append_value(v),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

fn to_array(v: &ColumnarValue, rows: usize) -> DFResult<ArrayRef> {
    Ok(match v {
        ColumnarValue::Array(a) => a.clone(),
        ColumnarValue::Scalar(s) => s.to_array_of_size(rows)?,
    })
}

fn as_list(a: &ArrayRef) -> DFResult<ListArray> {
    match a.data_type() {
        DataType::List(_) => Ok(a.as_list::<i32>().clone()),
        other => Err(DataFusionError::Execution(format!(
            "expected an array/list column, got {other}. These functions aggregate within \
             one row's array; to aggregate across rows, aggregate downstream."
        ))),
    }
}

/// Every non-null element of a numeric list, as f64.
fn numeric_values(elems: &ArrayRef) -> DFResult<Vec<f64>> {
    let n = elems.len();
    let mut out = Vec::with_capacity(n);
    match elems.data_type() {
        DataType::Float64 => {
            let a = elems.as_primitive::<Float64Type>();
            for i in 0..n {
                if !a.is_null(i) {
                    out.push(a.value(i));
                }
            }
        }
        DataType::Int64 => {
            let a = elems.as_primitive::<Int64Type>();
            for i in 0..n {
                if !a.is_null(i) {
                    out.push(a.value(i) as f64);
                }
            }
        }
        other => {
            // Cast anything else numeric via arrow rather than enumerating every type.
            let casted =
                deltalake::arrow::compute::cast(elems, &DataType::Float64).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "array elements of type {other} are not numeric: {e}"
                    ))
                })?;
            let a = casted.as_primitive::<Float64Type>();
            for i in 0..n {
                if !a.is_null(i) {
                    out.push(a.value(i));
                }
            }
        }
    }
    Ok(out)
}

/// A minimal arithmetic expression over an array-of-structs' fields.
///
/// Deliberately tiny: identifiers, numbers, `+ - * /`, and parentheses. Anything richer
/// belongs in the SELECT list, not inside an aggregation helper.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldExpr {
    Field(String),
    Lit(f64),
    Bin(Box<FieldExpr>, char, Box<FieldExpr>),
}

pub fn parse_expr(s: &str) -> DFResult<FieldExpr> {
    use deltalake::datafusion::sql::sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Value};
    use deltalake::datafusion::sql::sqlparser::dialect::GenericDialect;
    use deltalake::datafusion::sql::sqlparser::parser::Parser;

    let mut parser = Parser::new(&GenericDialect {})
        .try_with_sql(s)
        .map_err(|e| DataFusionError::Execution(format!("cannot parse {s:?}: {e}")))?;
    let expr = parser
        .parse_expr()
        .map_err(|e| DataFusionError::Execution(format!("cannot parse {s:?}: {e}")))?;

    fn conv(e: &SqlExpr) -> DFResult<FieldExpr> {
        match e {
            SqlExpr::Identifier(id) => Ok(FieldExpr::Field(id.value.clone())),
            SqlExpr::CompoundIdentifier(parts) => Ok(FieldExpr::Field(
                parts.last().map(|p| p.value.clone()).unwrap_or_default(),
            )),
            SqlExpr::Nested(inner) => conv(inner),
            SqlExpr::Value(v) => match &v.value {
                Value::Number(n, _) => n
                    .parse::<f64>()
                    .map(FieldExpr::Lit)
                    .map_err(|_| DataFusionError::Execution(format!("not a number: {n}"))),
                other => Err(DataFusionError::Execution(format!(
                    "unsupported literal in array expression: {other}"
                ))),
            },
            SqlExpr::BinaryOp { left, op, right } => {
                let c = match op {
                    BinaryOperator::Plus => '+',
                    BinaryOperator::Minus => '-',
                    BinaryOperator::Multiply => '*',
                    BinaryOperator::Divide => '/',
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "unsupported operator {other} in array expression; only + - * / \
                             are allowed"
                        )));
                    }
                };
                Ok(FieldExpr::Bin(
                    Box::new(conv(left)?),
                    c,
                    Box::new(conv(right)?),
                ))
            }
            other => Err(DataFusionError::Execution(format!(
                "unsupported expression {other} — use field names, numbers and + - * /"
            ))),
        }
    }
    conv(&expr)
}

/// Evaluate `expr` for every element of an array-of-structs.
fn struct_expr_values(elems: &ArrayRef, expr: &FieldExpr, fname: &str) -> DFResult<Vec<f64>> {
    let DataType::Struct(fields) = elems.data_type() else {
        return Err(DataFusionError::Execution(format!(
            "{fname}: the two-argument form needs an array of structs, got array of {}. \
             Use the one-argument form for a plain numeric array.",
            elems.data_type()
        )));
    };
    let sa = elems.as_struct();

    // Resolve each referenced field once, cast to f64.
    fn collect_fields(e: &FieldExpr, out: &mut Vec<String>) {
        match e {
            FieldExpr::Field(f) => out.push(f.clone()),
            FieldExpr::Bin(l, _, r) => {
                collect_fields(l, out);
                collect_fields(r, out);
            }
            FieldExpr::Lit(_) => {}
        }
    }
    let mut names = Vec::new();
    collect_fields(expr, &mut names);

    let mut resolved: Vec<(String, Float64Array)> = Vec::new();
    for name in names {
        if resolved.iter().any(|(n, _)| n == &name) {
            continue;
        }
        let idx = fields
            .iter()
            .position(|f: &Arc<Field>| f.name() == &name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{fname}: struct has no field {name:?}; available: [{}]",
                    fields
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;
        let col = sa.column(idx);
        let casted = deltalake::arrow::compute::cast(col, &DataType::Float64).map_err(|e| {
            DataFusionError::Execution(format!("{fname}: field {name:?} is not numeric: {e}"))
        })?;
        resolved.push((name, casted.as_primitive::<Float64Type>().clone()));
    }

    fn eval(e: &FieldExpr, row: usize, cols: &[(String, Float64Array)]) -> Option<f64> {
        match e {
            FieldExpr::Lit(v) => Some(*v),
            FieldExpr::Field(f) => {
                let (_, a) = cols.iter().find(|(n, _)| n == f)?;
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            }
            FieldExpr::Bin(l, op, r) => {
                let a = eval(l, row, cols)?;
                let b = eval(r, row, cols)?;
                Some(match op {
                    '+' => a + b,
                    '-' => a - b,
                    '*' => a * b,
                    '/' => {
                        if b == 0.0 {
                            return None;
                        }
                        a / b
                    }
                    _ => return None,
                })
            }
        }
    }

    let mut out = Vec::with_capacity(elems.len());
    for row in 0..elems.len() {
        if let Some(v) = eval(expr, row, &resolved) {
            out.push(v);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_product_of_two_fields() {
        assert_eq!(
            parse_expr("price * qty").unwrap(),
            FieldExpr::Bin(
                Box::new(FieldExpr::Field("price".into())),
                '*',
                Box::new(FieldExpr::Field("qty".into()))
            )
        );
    }

    #[test]
    fn parses_literals_and_precedence() {
        // qty * price + 1  =>  (qty*price) + 1
        let e = parse_expr("qty * price + 1").unwrap();
        match e {
            FieldExpr::Bin(l, '+', r) => {
                assert!(matches!(*l, FieldExpr::Bin(_, '*', _)));
                assert_eq!(*r, FieldExpr::Lit(1.0));
            }
            other => panic!("unexpected parse: {other:?}"),
        }
    }

    #[test]
    fn rejects_function_calls_in_the_mini_expression() {
        assert!(parse_expr("sum(price)").is_err());
    }

    #[test]
    fn rejects_unsupported_operators() {
        assert!(parse_expr("price % qty").is_err());
    }

    #[test]
    fn reduce_of_empty_is_none_not_zero() {
        // Emitting 0 for "no line items" would invent data.
        assert_eq!(Reduce::Sum.apply(&[]), None);
        assert_eq!(Reduce::Avg.apply(&[]), None);
    }

    #[test]
    fn reduce_arithmetic() {
        let v = [1.0, 2.0, 3.0];
        assert_eq!(Reduce::Sum.apply(&v), Some(6.0));
        assert_eq!(Reduce::Min.apply(&v), Some(1.0));
        assert_eq!(Reduce::Max.apply(&v), Some(3.0));
        assert_eq!(Reduce::Avg.apply(&v), Some(2.0));
    }
}
