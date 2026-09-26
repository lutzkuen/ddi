//! DECIMAL to DOUBLE and REAL, as Trino converts them.
//!
//! Arrow's cast divides in floating point: it turns the unscaled integer into a double,
//! divides by `10f64.powi(scale)`, and for a REAL narrows that result with `as f32`. Once the
//! integer is past 2^53 the first step already rounds, and the division rounds again, so the
//! answer can be a neighbour of the nearest double. DECIMAL(38,17) `0.49979999999999997`
//! comes out as `0.4998`, and `0.45909999999999995` as `0.4590999999999999`: the values of
//! issue #12, where serde_json's default float parser does the same arithmetic. A
//! reconciliation against the warehouse flags every one.
//!
//! Trino converts a *long* decimal — more than 18 digits, held in 128 bits — with
//! `DecimalConversions`, which divides only when both operands are exact as doubles — the
//! integer within ±2^53 and the power of ten at most 10^22 — because one IEEE division of
//! exact operands is correctly rounded. Anything else it reads through `Double.parseDouble`,
//! which is correctly rounded too. A REAL is correctly rounded to a float in the same way,
//! never by narrowing a rounded double. This module does the same for a long decimal: one
//! division where it is exact (below 2^53 and 10^22, or 2^24 and 10^10 for a REAL), and
//! otherwise the decimal's text read by Rust's float parser.
//!
//! A *short* decimal, of 18 digits or fewer, Trino divides as it stands: the unscaled `long`
//! as a double over the power of ten as a double, or both as floats for a REAL. Past 2^53
//! (2^24) that rounds twice, and the answer is not always the nearest — DECIMAL(17,17)
//! `0.49979999999999997` is `0.4998` there too — but it is Trino's, so it is this module's.
//! For a DOUBLE it is also Arrow's; for a REAL it is not, since Arrow divides as doubles and
//! narrows after. Which of the two a value is follows its type's precision, so a computed
//! decimal has to have Trino's type. A product does in both engines, and so do `min` and `max`.
//! An integer literal beside a decimal would not — DataFusion reads it as a BIGINT, ten digits
//! wider than Trino's INTEGER — so it is typed as Trino types it first
//! ([`retype_integer_literals`]). Nor would `sum`: DataFusion gives it ten digits more than its
//! argument and Trino 38, so over 8 digits or fewer it would be short here and long there, and
//! its argument is widened to 38 digits first ([`widen_decimal_sums`]). `avg` differs too,
//! DECIMAL(p+4, s+4) against Trino's DECIMAL(p, s), but no model can use it: a transform
//! aggregates nothing, and a publication only what a client can add up. One computed decimal
//! still differs: a quotient of decimals, which DataFusion computes to fewer digits
//! (DECIMAL(9,2) / DECIMAL(9,2) is DECIMAL(15,6) there and DECIMAL(21,12) in Trino, and the
//! digits themselves differ).
//!
//! What goes through it: a CAST or TRY_CAST in a model, including the casts DataFusion's
//! coercion inserts (`dec * 1e0`), and the same in a lambda body; casts of a list of decimals
//! to a list of doubles; a DECIMAL column landing in a DOUBLE or REAL target
//! ([`crate::schema::SchemaCoercer`]); and the `array_*` aggregates over decimals. What does
//! not: `log` and `power`, which DataFusion computes on the decimal itself, and not as Trino
//! does; and, keeping Arrow's division, a decimal inside a ROW or MAP being cast, and
//! `arrow_cast`, whose cast is only made after the analyzer has run.
//!
//! Two types DataFusion decides otherwise than Trino are left as they are. A literal with a
//! decimal point is a DOUBLE there and a DECIMAL in Trino; and a decimal beside a DOUBLE or
//! REAL, compared with it or in a `CASE` or `coalesce` with it, is converted to the float in
//! Trino, where DataFusion casts the float to a DECIMAL. Once there is a plan, `0.5` and
//! `0.5e0` are the same DOUBLE, so no rule here can tell the literal Trino reads as a decimal
//! from the one it reads as a double. The README gives the spelling that means the same in
//! both.

use std::any::Any;
use std::sync::Arc;

use deltalake::arrow::array::{Array, ArrayRef, AsArray, FixedSizeListArray, GenericListArray};
use deltalake::arrow::array::{ArrowPrimitiveType, OffsetSizeTrait};
use deltalake::arrow::compute::CastOptions;
use deltalake::arrow::datatypes::{
    i256, DataType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type, DecimalType,
    Field, FieldRef, Float32Type, Float64Type,
};
use deltalake::arrow::error::ArrowError;
use deltalake::datafusion::common::config::ConfigOptions;
use deltalake::datafusion::common::tree_node::{Transformed, TreeNode};
use deltalake::datafusion::common::{DFSchema, Result as DFResult, ScalarValue};
use deltalake::datafusion::logical_expr::expr::{Cast, ScalarFunction, TryCast};
use deltalake::datafusion::logical_expr::expr_rewriter::NamePreserver;
use deltalake::datafusion::logical_expr::utils::merge_schema;
use deltalake::datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use deltalake::datafusion::optimizer::analyzer::AnalyzerRule;

/// 10^0 to 10^22: every power of ten a double holds exactly.
const POW10_F64: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
];

/// 10^0 to 10^10: every power of ten a float holds exactly.
const POW10_F32: [f32; 11] = [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10];

/// The most digits a DECIMAL can have and still be one Trino holds in a `long`, and
/// converts by dividing it as it stands.
const TRINO_MAX_SHORT_PRECISION: u8 = 18;

/// The power of ten a short DECIMAL(`precision`, `scale`) is divided by, as Trino divides
/// it, or `None` for a long one. A negative scale, which Trino has no type for, is long.
fn short_power(precision: u8, scale: i8) -> Option<f64> {
    if precision > TRINO_MAX_SHORT_PRECISION || scale < 0 {
        return None;
    }
    POW10_F64.get(usize::from(scale.unsigned_abs())).copied()
}

/// `unscaled × 10^-scale` as Trino makes a DOUBLE of a DECIMAL(`precision`, `scale`): the
/// nearest double for a long decimal, one division for a short one.
pub(crate) fn decimal_to_f64(unscaled: i128, precision: u8, scale: i8) -> f64 {
    if let Some(p) = short_power(precision, scale) {
        // `(double) unscaled / (double) 10^scale`. The power is exact up to 10^22.
        return unscaled as f64 / p;
    }
    if unscaled.unsigned_abs() <= 1u128 << 53 {
        let exact = unscaled as f64;
        let power = usize::from(scale.unsigned_abs());
        if let Some(p) = POW10_F64.get(power) {
            return if scale >= 0 { exact / p } else { exact * p };
        }
    }
    parse(unscaled, scale)
}

/// `unscaled × 10^-scale` as Trino makes a REAL of a DECIMAL(`precision`, `scale`): the
/// nearest float for a long decimal, never rounded through a double first, and one division
/// of floats for a short one.
pub(crate) fn decimal_to_f32(unscaled: i128, precision: u8, scale: i8) -> f32 {
    if let Some(p) = short_power(precision, scale) {
        // `(float) unscaled / (float) 10^scale`: both rounded to the nearest float, as Java's
        // `l2f` rounds them, and the power from its exact double.
        return unscaled as f32 / p as f32;
    }
    if unscaled.unsigned_abs() <= 1u128 << 24 {
        let exact = unscaled as f32;
        let power = usize::from(scale.unsigned_abs());
        if let Some(p) = POW10_F32.get(power) {
            return if scale >= 0 { exact / p } else { exact * p };
        }
    }
    parse(unscaled, scale)
}

fn wide_to_f64(unscaled: i256, precision: u8, scale: i8) -> f64 {
    match unscaled.to_i128() {
        Some(v) => decimal_to_f64(v, precision, scale),
        None => parse(unscaled, scale),
    }
}

fn wide_to_f32(unscaled: i256, precision: u8, scale: i8) -> f32 {
    match unscaled.to_i128() {
        Some(v) => decimal_to_f32(v, precision, scale),
        None => parse(unscaled, scale),
    }
}

/// The decimal spelt as `<unscaled>e<-scale>` and read by Rust's float parser, which is
/// correctly rounded — as `Double.parseDouble` and `Float.parseFloat` are.
fn parse<F>(unscaled: impl std::fmt::Display, scale: i8) -> F
where
    F: std::str::FromStr,
    F::Err: std::fmt::Debug,
{
    format!("{unscaled}e{}", -i32::from(scale))
        .parse()
        .expect("an integer with an exponent is always a float literal")
}

/// True when casting `from` to `to` converts decimals to DOUBLE or REAL: directly, or as the
/// elements of a list, however deeply nested.
///
/// A ROW, a MAP or a dictionary holding decimals is not recognised and keeps Arrow's cast.
pub(crate) fn is_decimal_to_float(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    match (from, to) {
        (Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..), Float32 | Float64) => {
            true
        }
        (List(f), List(t)) | (LargeList(f), LargeList(t)) => {
            is_decimal_to_float(f.data_type(), t.data_type())
        }
        (FixedSizeList(f, n), FixedSizeList(t, m)) => {
            n == m && is_decimal_to_float(f.data_type(), t.data_type())
        }
        _ => false,
    }
}

/// Convert an array [`is_decimal_to_float`] accepts, keeping its nulls and, for a list, its
/// offsets; the lists are rebuilt as Arrow's own cast rebuilds them, with the target's
/// element field.
pub(crate) fn decimal_to_float_array(
    array: &dyn Array,
    to: &DataType,
) -> Result<ArrayRef, ArrowError> {
    match (array.data_type(), to) {
        (DataType::Decimal32(p, s), _) => {
            let (p, s) = (*p, *s);
            leaf::<Decimal32Type>(
                array,
                to,
                |v| decimal_to_f64(v.into(), p, s),
                |v| decimal_to_f32(v.into(), p, s),
            )
        }
        (DataType::Decimal64(p, s), _) => {
            let (p, s) = (*p, *s);
            leaf::<Decimal64Type>(
                array,
                to,
                |v| decimal_to_f64(v.into(), p, s),
                |v| decimal_to_f32(v.into(), p, s),
            )
        }
        (DataType::Decimal128(p, s), _) => {
            let (p, s) = (*p, *s);
            leaf::<Decimal128Type>(
                array,
                to,
                |v| decimal_to_f64(v, p, s),
                |v| decimal_to_f32(v, p, s),
            )
        }
        (DataType::Decimal256(p, s), _) => {
            let (p, s) = (*p, *s);
            leaf::<Decimal256Type>(
                array,
                to,
                |v| wide_to_f64(v, p, s),
                |v| wide_to_f32(v, p, s),
            )
        }
        (DataType::List(_), DataType::List(t)) => list::<i32>(array, t),
        (DataType::LargeList(_), DataType::LargeList(t)) => list::<i64>(array, t),
        (DataType::FixedSizeList(_, _), DataType::FixedSizeList(t, n)) => {
            let l = array.as_fixed_size_list();
            let values = decimal_to_float_array(l.values().as_ref(), t.data_type())?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                Arc::clone(t),
                *n,
                values,
                l.nulls().cloned(),
            )?))
        }
        (from, to) => Err(ArrowError::CastError(format!(
            "{from} -> {to} is not a conversion of decimals to DOUBLE or REAL"
        ))),
    }
}

fn leaf<D: DecimalType>(
    array: &dyn Array,
    to: &DataType,
    double: impl Fn(<D as ArrowPrimitiveType>::Native) -> f64,
    real: impl Fn(<D as ArrowPrimitiveType>::Native) -> f32,
) -> Result<ArrayRef, ArrowError> {
    let a = array.as_primitive::<D>();
    match to {
        DataType::Float64 => Ok(Arc::new(a.unary::<_, Float64Type>(double))),
        DataType::Float32 => Ok(Arc::new(a.unary::<_, Float32Type>(real))),
        other => Err(ArrowError::CastError(format!(
            "{} -> {other} is not a conversion of decimals to DOUBLE or REAL",
            a.data_type()
        ))),
    }
}

fn list<O: OffsetSizeTrait>(array: &dyn Array, to: &FieldRef) -> Result<ArrayRef, ArrowError> {
    let l = array.as_list::<O>();
    let values = decimal_to_float_array(l.values().as_ref(), to.data_type())?;
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(to),
        l.offsets().clone(),
        values,
        l.nulls().cloned(),
    )?))
}

/// Arrow's `cast_with_options`, except that decimals to DOUBLE or REAL convert as in Trino.
pub(crate) fn cast_with_options(
    array: &ArrayRef,
    to: &DataType,
    options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    if is_decimal_to_float(array.data_type(), to) {
        return decimal_to_float_array(array.as_ref(), to);
    }
    deltalake::arrow::compute::cast_with_options(array, to, options)
}

/// Arrow's `cast`, except that decimals to DOUBLE or REAL convert as in Trino.
pub(crate) fn cast(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    cast_with_options(array, to, &CastOptions::default())
}

// ------------------------------------------------------------------------ in a query

/// What a CAST of decimals to DOUBLE or REAL becomes in a plan.
///
/// A function, not a cast through text: it converts without a column of strings in between,
/// most values take the single division, and no optimizer rule that reasons about casts can
/// take it apart again. Built per rewrite and never registered, so a model cannot call it by
/// name.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DecimalToFloat {
    name: &'static str,
    to: DataType,
    /// Standing in for a TRY_CAST, whose result is always nullable. A CAST's is as nullable
    /// as its input.
    try_cast: bool,
    signature: Signature,
}

impl DecimalToFloat {
    fn new(to: DataType, try_cast: bool) -> Self {
        let mut leaf = &to;
        while let DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) = leaf
        {
            leaf = f.data_type();
        }
        let name = if leaf == &DataType::Float32 {
            "ddi_decimal_to_real"
        } else {
            "ddi_decimal_to_double"
        };
        Self {
            name,
            to,
            try_cast,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DecimalToFloat {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(self.to.clone())
    }

    /// The field the replaced cast would have had, so that no schema above it changes:
    /// a CAST keeps its input's field and changes only the type, a TRY_CAST is a new,
    /// nullable one.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        let field = if self.try_cast {
            Field::new(self.name, self.to.clone(), true)
        } else {
            args.arg_fields[0]
                .as_ref()
                .clone()
                .with_data_type(self.to.clone())
        };
        Ok(Arc::new(field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Array(a) => Ok(ColumnarValue::Array(decimal_to_float_array(
                a.as_ref(),
                &self.to,
            )?)),
            // A literal: the simplifier folds it, and expects a scalar back.
            ColumnarValue::Scalar(s) => {
                let out = decimal_to_float_array(s.to_array()?.as_ref(), &self.to)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
            }
        }
    }
}

/// Replace every CAST and TRY_CAST of decimals to DOUBLE or REAL in `expr` with Trino's
/// conversion. `schema` is what `expr`'s columns resolve against.
///
/// Never fails on the expression's account: one whose input type cannot be worked out is
/// left as it was, with Arrow's cast.
pub(crate) fn rewrite_expr(expr: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    expr.transform_up(|e| {
        let (inner, to, try_cast) = match e {
            Expr::Cast(Cast { expr, data_type }) => (expr, data_type, false),
            Expr::TryCast(TryCast { expr, data_type }) => (expr, data_type, true),
            other => return Ok(Transformed::no(other)),
        };
        if !inner
            .get_type(schema)
            .is_ok_and(|from| is_decimal_to_float(&from, &to))
        {
            return Ok(Transformed::no(if try_cast {
                Expr::TryCast(TryCast::new(inner, to))
            } else {
                Expr::Cast(Cast::new(inner, to))
            }));
        }
        let udf = Arc::new(ScalarUDF::from(DecimalToFloat::new(to, try_cast)));
        Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(udf, vec![*inner]),
        )))
    })
}

/// Type every integer literal in `expr` that meets a DECIMAL as Trino types it: an INTEGER,
/// where DataFusion reads a BIGINT. `schema` is what `expr`'s columns resolve against.
///
/// Before coercion, which is what widens the literal: DataFusion makes a BIGINT
/// DECIMAL(20,0) beside a decimal, and Trino an INTEGER DECIMAL(10,0). Ten digits of the
/// result's precision decide which side of the 18-digit line it falls on, and so how it
/// converts to DOUBLE or REAL: `coalesce(amount, 0)` over a DECIMAL(18,8) is DECIMAL(28,8) in
/// DataFusion, correctly rounded, and DECIMAL(18,8) in Trino, divided. So a literal that fits
/// an INTEGER becomes one in arithmetic (`+ - * / %`), in the branches of a CASE, and among the
/// arguments of `coalesce`, `nullif`, `greatest` and `least`, wherever a decimal is beside it.
/// A literal past an INTEGER, and a BIGINT column, stay DECIMAL(20,0) where Trino's BIGINT is
/// DECIMAL(19,0): the result is a long decimal in both, so it converts the same.
///
/// Never fails on the expression's account: one whose type cannot be worked out is taken
/// for no decimal.
pub(crate) fn retype_integer_literals(
    expr: Expr,
    schema: &DFSchema,
) -> DFResult<Transformed<Expr>> {
    use deltalake::datafusion::logical_expr::Operator;

    expr.transform_up(|mut e| {
        let changed = match &mut e {
            Expr::BinaryExpr(b)
                if matches!(
                    b.op,
                    Operator::Plus
                        | Operator::Minus
                        | Operator::Multiply
                        | Operator::Divide
                        | Operator::Modulo
                ) =>
            {
                narrow_beside_a_decimal([b.left.as_mut(), b.right.as_mut()], schema)
            }
            Expr::Case(c) => narrow_beside_a_decimal(
                c.when_then_expr
                    .iter_mut()
                    .map(|(_, then)| then.as_mut())
                    .chain(c.else_expr.as_deref_mut()),
                schema,
            ),
            Expr::ScalarFunction(f)
                if matches!(f.name(), "coalesce" | "nullif" | "greatest" | "least") =>
            {
                narrow_beside_a_decimal(f.args.iter_mut(), schema)
            }
            _ => false,
        };
        Ok(Transformed::new_transformed(e, changed))
    })
}

/// Make each integer literal among `values` an INTEGER, when one of `values` is a decimal.
/// True when anything changed.
fn narrow_beside_a_decimal<'a>(
    values: impl IntoIterator<Item = &'a mut Expr>,
    schema: &DFSchema,
) -> bool {
    let values: Vec<&mut Expr> = values.into_iter().collect();
    if !values
        .iter()
        .any(|v| v.get_type(schema).is_ok_and(|t| t.is_decimal()))
    {
        return false;
    }
    let mut changed = false;
    for v in values {
        if let Expr::Literal(value, _) = v {
            if let ScalarValue::Int64(Some(n)) = *value {
                if let Ok(n) = i32::try_from(n) {
                    *value = ScalarValue::Int32(Some(n));
                    changed = true;
                }
            }
        }
    }
    changed
}

/// Type every `sum` over a DECIMAL in `expr` as Trino types it: DECIMAL(38, s), whatever its
/// argument's precision. `schema` is what `expr`'s columns resolve against.
///
/// DataFusion gives a sum ten digits more than its argument, up to 38, so over a DECIMAL of 8
/// digits or fewer it is a short decimal, which converts to DOUBLE or REAL by dividing, where
/// Trino's DECIMAL(38, s) is correctly rounded: over DECIMAL(8,2), 1413830.04 is the REAL
/// 1413830.0 in Trino and 1413830.125 divided. So the argument is cast to DECIMAL(38, s),
/// which DataFusion sums to DECIMAL(38, s) too. Both sums are exact, so only the type changes.
///
/// Before coercion, like [`retype_integer_literals`], because this changes the type of the
/// aggregate's column, and coercion is what fits every node above it to that. The scale is
/// the one coercion gives the argument, asked of coercion itself: before it, a CASE is typed
/// by its first branch alone. Never fails on the expression's account: an argument whose type
/// cannot be worked out is left as it was.
pub(crate) fn widen_decimal_sums(expr: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    use deltalake::datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;

    expr.transform_up(|mut e| {
        let Expr::AggregateFunction(sum) = &mut e else {
            return Ok(Transformed::no(e));
        };
        if sum.func.name() != "sum" {
            return Ok(Transformed::no(e));
        }
        let [arg] = sum.params.args.as_mut_slice() else {
            return Ok(Transformed::no(e));
        };
        let mut coercion = TypeCoercionRewriter::new(schema);
        let scale = match arg
            .clone()
            .rewrite(&mut coercion)
            .and_then(|coerced| coerced.data.get_type(schema))
        {
            Ok(DataType::Decimal128(38, _)) => None,
            Ok(
                DataType::Decimal32(_, s) | DataType::Decimal64(_, s) | DataType::Decimal128(_, s),
            ) => Some(s),
            _ => None,
        };
        let Some(scale) = scale else {
            return Ok(Transformed::no(e));
        };
        *arg = Expr::Cast(Cast::new(
            Box::new(std::mem::take(arg)),
            DataType::Decimal128(38, scale),
        ));
        Ok(Transformed::yes(e))
    })
}

/// DataFusion's analyzer rules, with this module's three where they belong: Trino's integer
/// literals and decimal sums just before `TypeCoercion`, which is what widens the one and fits
/// the plan to the other, and Trino's DECIMAL casts at the end, after it, so that they also
/// see the casts coercion inserts.
pub(crate) fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    use deltalake::datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
    use deltalake::datafusion::optimizer::Analyzer;

    let mut rules = Analyzer::new().rules;
    let coercion = TypeCoercion::new();
    let at = rules
        .iter()
        .position(|r| r.name() == coercion.name())
        .unwrap_or(0);
    rules.insert(at, Arc::new(TrinoDecimalSums));
    rules.insert(at, Arc::new(TrinoIntegerLiterals));
    rules.push(Arc::new(TrinoDecimalCasts));
    rules
}

/// The analyzer pass that applies [`retype_integer_literals`] to every expression of a plan
/// and its subqueries. Handled node by node as [`TrinoDecimalCasts`] handles them.
#[derive(Debug, Default)]
pub(crate) struct TrinoIntegerLiterals;

impl AnalyzerRule for TrinoIntegerLiterals {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|p| rewrite_plan(p, retype_integer_literals))?
            .data)
    }

    fn name(&self) -> &str {
        "ddi_trino_integer_literals"
    }
}

/// The analyzer pass that applies [`widen_decimal_sums`] to every expression of a plan and its
/// subqueries, after [`TrinoIntegerLiterals`]. Handled node by node as [`TrinoDecimalCasts`]
/// handles them, so an unaliased `sum(amount)` keeps its name.
#[derive(Debug, Default)]
pub(crate) struct TrinoDecimalSums;

impl AnalyzerRule for TrinoDecimalSums {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|p| rewrite_plan(p, widen_decimal_sums))?
            .data)
    }

    fn name(&self) -> &str {
        "ddi_trino_decimal_sums"
    }
}

/// The analyzer pass that applies [`rewrite_expr`] to every expression of a plan and its
/// subqueries.
///
/// It runs after DataFusion's own `TypeCoercion`, so it also sees the casts coercion
/// inserted. Each node is handled the way `TypeCoercion` handles it: expressions resolve
/// against the node's inputs (and a scan's source), a rewritten expression keeps the name it
/// had, so an unaliased `CAST(dec AS DOUBLE)` column is still called that, and the node's
/// schema is recomputed when anything changed.
#[derive(Debug, Default)]
pub(crate) struct TrinoDecimalCasts;

impl AnalyzerRule for TrinoDecimalCasts {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|p| rewrite_plan(p, rewrite_expr))?
            .data)
    }

    fn name(&self) -> &str {
        "ddi_trino_decimal_casts"
    }
}

fn rewrite_plan(
    plan: LogicalPlan,
    rewrite: fn(Expr, &DFSchema) -> DFResult<Transformed<Expr>>,
) -> DFResult<Transformed<LogicalPlan>> {
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(ts) = &plan {
        let source =
            DFSchema::try_from_qualified_schema(ts.table_name.clone(), &ts.source.schema())?;
        schema.merge(&source);
    }

    let names = NamePreserver::new(&plan);
    let rewritten = plan.map_expressions(|expr| {
        let name = names.save(&expr);
        Ok(rewrite(expr, &schema)?.update_data(|e| name.restore(e)))
    })?;
    if rewritten.transformed {
        rewritten.map_data(|p| p.recompute_schema())
    } else {
        Ok(rewritten)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{
        Decimal128Array, Decimal256Array, Float64Array, Int32Array, ListArray,
    };
    use deltalake::arrow::buffer::OffsetBuffer;

    #[test]
    fn the_issue_rows_convert_to_the_nearest_double() {
        for (unscaled, scale, text) in [
            (49979999999999997i128, 17i8, "0.49979999999999997"),
            (90170000000000010, 17, "0.9017000000000001"),
            (9017000000000001, 16, "0.9017000000000001"),
            (45909999999999995, 17, "0.45909999999999995"),
            (40380000000000005, 17, "0.40380000000000005"),
            (-49979999999999997, 17, "-0.49979999999999997"),
            (-40380000000000005, 17, "-0.40380000000000005"),
            (i128::MAX, 0, "170141183460469231731687303715884105727"),
            (i128::MIN, 0, "-170141183460469231731687303715884105728"),
            (i128::MAX, 38, "1.70141183460469231731687303715884105727"),
            (i128::MIN, 38, "-1.70141183460469231731687303715884105728"),
            (12345, -3, "12345000"),
            (98765432109876543, -5, "9876543210987654300000"),
        ] {
            let got = decimal_to_f64(unscaled, 38, scale);
            let want: f64 = text.parse().unwrap();
            assert_eq!(
                got.to_bits(),
                want.to_bits(),
                "{unscaled}e-{scale}: got {got:?}, the nearest double is {want:?}"
            );
        }
        // Zero is positive zero, as `0 / 10^s` is.
        assert_eq!(decimal_to_f64(0, 38, 17).to_bits(), 0f64.to_bits());
        assert_eq!(decimal_to_f32(0, 38, 17).to_bits(), 0f32.to_bits());
        assert_eq!(decimal_to_f64(0, 17, 17).to_bits(), 0f64.to_bits());
        assert_eq!(decimal_to_f32(0, 17, 17).to_bits(), 0f32.to_bits());

        // And Arrow's division really does miss, or none of this would be needed.
        let arrow = 49979999999999997i128 as f64 / 10f64.powi(17);
        assert_eq!(arrow, 0.4998, "Arrow's cast of 0.49979999999999997");
    }

    #[test]
    fn a_short_decimal_is_divided_as_trino_divides_it() {
        // DecimalCasts.shortDecimalToDouble is `(double) unscaled / (double) 10^scale`, and
        // shortDecimalToReal the same in floats, for every DECIMAL of up to 18 digits. What
        // Trino 480 returned for each of these, read back from a table it wrote.
        for (unscaled, precision, scale, trino) in [
            (49979999999999997i128, 17u8, 17i8, "0.4998"),
            (45909999999999995, 17, 17, "0.4590999999999999"),
            (22205956120782495, 17, 17, "0.22205956120782497"),
            (40380000000000005, 18, 17, "0.4038000000000001"),
            (59356256244421893, 18, 6, "59356256244.4219"),
        ] {
            let want: f64 = trino.parse().unwrap();
            let got = decimal_to_f64(unscaled, precision, scale);
            assert_eq!(
                got.to_bits(),
                want.to_bits(),
                "DECIMAL({precision},{scale}) {unscaled}: got {got:?}, Trino gives {want:?}"
            );
            // One digit more and it is a long decimal, which Trino rounds correctly.
            let nearest: f64 = format!("{unscaled}e-{scale}").parse().unwrap();
            assert_ne!(nearest.to_bits(), want.to_bits(), "the premise");
            assert_eq!(
                decimal_to_f64(unscaled, 19, scale).to_bits(),
                nearest.to_bits()
            );
        }

        // DECIMAL(9,2) 1413830.04: 141383004 is past 2^24, so it is rounded to a float before
        // it is divided. A long decimal reads the text instead.
        // The float 1413830.125, which is the one `1_413_830.1` names.
        assert_eq!(decimal_to_f32(141383004, 9, 2), 1_413_830.1);
        assert_eq!(decimal_to_f32(141383004, 19, 2), 1413830.0);
        assert_eq!(
            "1413830.04".parse::<f32>().unwrap(),
            1413830.0,
            "the nearest float"
        );
    }

    #[test]
    fn the_fast_path_and_the_text_path_agree() {
        // Deterministic, so a failure reproduces.
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let edge = 1i128 << 53;
        let mut cases: Vec<i128> = vec![edge - 1, edge, edge + 1, -edge - 1, -edge, -edge + 1];
        cases.extend([(1 << 24) - 1, 1 << 24, (1 << 24) + 1, -(1 << 24) - 1]);
        for _ in 0..4_000 {
            let bits = 1 + next() % 60;
            let magnitude = (next() & ((1u64 << bits) - 1)) as i128;
            cases.push(if next() % 2 == 0 {
                magnitude
            } else {
                -magnitude
            });
        }
        for (i, unscaled) in cases.into_iter().enumerate() {
            let scale = (i % 44) as i8 - 5; // -5..=38
            let text = format!("{unscaled}e{}", -i32::from(scale));
            let double: f64 = text.parse().unwrap();
            let real: f32 = text.parse().unwrap();
            assert_eq!(
                decimal_to_f64(unscaled, 38, scale).to_bits(),
                double.to_bits(),
                "{text} as a double"
            );
            assert_eq!(
                decimal_to_f32(unscaled, 38, scale).to_bits(),
                real.to_bits(),
                "{text} as a real"
            );
        }

        // Past i128, and a null that stays null.
        let big = i256::from_i128(i128::MAX).wrapping_mul(i256::from_i128(1000));
        let wide = Decimal256Array::from(vec![Some(big), None])
            .with_precision_and_scale(76, 40)
            .unwrap();
        let out = decimal_to_float_array(&wide, &DataType::Float64).unwrap();
        let out = out.as_primitive::<Float64Type>();
        let want: f64 = format!("{big}e-40").parse().unwrap();
        assert_eq!(out.value(0).to_bits(), want.to_bits());
        assert!(out.is_null(1));
    }

    #[test]
    fn a_list_of_decimals_converts_element_by_element() {
        let dec = DataType::Decimal128(38, 17);
        let list = |t: &DataType| DataType::List(Arc::new(Field::new("item", t.clone(), true)));
        let large =
            |t: &DataType| DataType::LargeList(Arc::new(Field::new("item", t.clone(), true)));
        let fixed = |t: &DataType| {
            DataType::FixedSizeList(Arc::new(Field::new("item", t.clone(), true)), 2)
        };
        assert!(is_decimal_to_float(&dec, &DataType::Float64));
        assert!(is_decimal_to_float(&dec, &DataType::Float32));
        assert!(is_decimal_to_float(&list(&dec), &list(&DataType::Float64)));
        assert!(is_decimal_to_float(
            &large(&dec),
            &large(&DataType::Float64)
        ));
        assert!(is_decimal_to_float(
            &fixed(&dec),
            &fixed(&DataType::Float32)
        ));
        assert!(is_decimal_to_float(
            &list(&list(&dec)),
            &list(&list(&DataType::Float64))
        ));
        assert!(!is_decimal_to_float(
            &list(&dec),
            &large(&DataType::Float64)
        ));
        assert!(!is_decimal_to_float(&list(&dec), &DataType::Float64));
        assert!(!is_decimal_to_float(&DataType::Utf8, &DataType::Float64));
        assert!(!is_decimal_to_float(&dec, &DataType::Utf8));
        let row = |t: &DataType| DataType::Struct(vec![Field::new("x", t.clone(), true)].into());
        assert!(
            !is_decimal_to_float(&row(&dec), &row(&DataType::Float64)),
            "a ROW is a documented gap"
        );

        // [[a, NULL], NULL, [b]]
        let values =
            Decimal128Array::from(vec![Some(49979999999999997), None, Some(45909999999999995)])
                .with_precision_and_scale(38, 17)
                .unwrap();
        let input = ListArray::new(
            Arc::new(Field::new("item", dec.clone(), true)),
            OffsetBuffer::new(vec![0, 2, 2, 3].into()),
            Arc::new(values),
            Some(vec![true, false, true].into()),
        );
        let target = Arc::new(Field::new("element", DataType::Float64, true));
        let out = decimal_to_float_array(&input, &DataType::List(Arc::clone(&target))).unwrap();
        let out = out.as_list::<i32>();
        assert_eq!(out.value_offsets(), &[0, 2, 2, 3]);
        assert!(out.is_null(1), "a null list stays null");
        assert_eq!(
            out.data_type(),
            &DataType::List(target),
            "with the target's element field"
        );
        let elements = out.values().as_primitive::<Float64Type>();
        assert!(elements.is_null(1), "a null element stays null");
        assert_eq!(
            elements.value(0).to_bits(),
            "0.49979999999999997".parse::<f64>().unwrap().to_bits()
        );
        assert_eq!(
            elements.value(2).to_bits(),
            "0.45909999999999995".parse::<f64>().unwrap().to_bits()
        );

        // Everything else is Arrow's business, untouched.
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        let doubles = cast(&ints, &DataType::Float64).unwrap();
        assert_eq!(
            doubles.as_any().downcast_ref::<Float64Array>().unwrap(),
            &Float64Array::from(vec![7.0])
        );
    }
}
