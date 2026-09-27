//! DECIMAL to DOUBLE and REAL, as Trino converts them, except a short decimal to REAL.
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
//! as a double over the power of ten as a double. Past 2^53 that rounds twice, and the answer
//! is not always the nearest — DECIMAL(17,17) `0.49979999999999997` is `0.4998` there too —
//! but it is Trino's, and Arrow's, so it is this module's DOUBLE. For a REAL Trino divides
//! the two as floats, and this module does not: it narrows that double, as Arrow's cast does
//! and `ddi` 0.3.1 did. Trino's float division rounds the unscaled integer to a float first,
//! so past 2^24 unscaled, eight digits, whether a decimal is short or long decides its REAL,
//! and a decimal DataFusion computes is often on the other side of the 18-digit line from
//! Trino's (below): `sum` over a DECIMAL(8,2) is a DECIMAL(18,2) here and a DECIMAL(38,2) in
//! Trino, and 1413830.04 divided as floats would be 1413830.125, where Trino and the narrowed
//! double both give 1413830.0. The price is a decimal that is short in both engines, a
//! DECIMAL(9,2) column say: there the narrowed double can be an ULP of the float off Trino's
//! float division, where that rounds twice, and 1413830.04 is the REAL 1413830.125 in Trino
//! and 1413830.0 here, as in 0.3.1. Trino's REAL of every short decimal needs Trino's types,
//! which this module does not give yet.
//!
//! Which way a value converts follows its type's precision, and a computed decimal has
//! DataFusion's type, which is not always Trino's; making it Trino's is not done yet. A
//! product has the same type in both engines, and so do `min` and `max`. An integer literal
//! beside a decimal, and an integer expression beside one that a literal makes a BIGINT —
//! `coalesce(amount, 0)`, `amount + 1`, `amount / nullif(qty, 0)` over an INTEGER `qty` — is
//! DataFusion's BIGINT, ten digits wider than Trino's INTEGER. `sum` has ten digits more than
//! its argument, where Trino's has 38. A quotient has four digits of scale more than its
//! dividend, where Trino gives it `s1 + p2 + 1`, at least six (DECIMAL(9,2) / DECIMAL(9,2) is
//! DECIMAL(15,6) here and DECIMAL(21,12) in Trino), and its last digit is truncated where
//! Trino rounds it. And `floor`, `ceil` and `round(x)` keep their argument's precision, where
//! Trino narrows it, and `round(x, n)` narrows the scale to `n`, where Trino keeps it. So, as
//! in 0.3.1, the value itself can differ from Trino's there, as a quotient's digits do, and so
//! can its DOUBLE, on the other side of the 18-digit line: `coalesce(amount, 0)` over a
//! DECIMAL(18,8) is a long DECIMAL(28,8) here, correctly rounded, and a short DECIMAL(18,8) in
//! Trino, divided. `avg` differs too, DECIMAL(p+4, s+4) against Trino's DECIMAL(p, s), but no
//! model can use it: a transform aggregates nothing, and a publication only what a client can
//! add up.
//!
//! What goes through it: a CAST or TRY_CAST in a model, including the casts DataFusion's
//! coercion inserts (`dec * 1e0`), and the same in a lambda body; casts of a list of decimals
//! to a list of doubles; a DECIMAL column landing in a DOUBLE or REAL target
//! ([`crate::schema::SchemaCoercer`]); and the `array_*` aggregates over decimals. What does
//! not: `log` and `power`, which DataFusion computes on the decimal itself, and not as Trino
//! does; and, keeping Arrow's division, a decimal inside a ROW or MAP being cast, and
//! `arrow_cast`, whose cast is only made after the analyzer has run.
//!
//! Two more types DataFusion decides otherwise than Trino are left as they are. A literal
//! with a decimal point is a DOUBLE there and a DECIMAL in Trino; and a decimal beside a
//! DOUBLE or REAL, compared with it or in a `CASE` or `coalesce` with it, is converted to the
//! float in Trino, where DataFusion casts the float to a DECIMAL. Once there is a plan, `0.5`
//! and `0.5e0` are the same DOUBLE, so no rule here can tell the literal Trino reads as a
//! decimal from the one it reads as a double. The README gives the spelling that means the
//! same in both.

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

/// `unscaled × 10^-scale` as a REAL of a DECIMAL(`precision`, `scale`): for a long decimal
/// the nearest float, as Trino makes it, never rounded through a double first; for a short
/// one the DOUBLE [`decimal_to_f64`] divides, narrowed, as Arrow's cast makes it.
pub(crate) fn decimal_to_f32(unscaled: i128, precision: u8, scale: i8) -> f32 {
    if short_power(precision, scale).is_some() {
        // Arrow's `(x as f64 / 10f64.powi(scale)) as f32`, as `ddi` 0.3.1 had it, rather than
        // Trino's `(float) unscaled / (float) 10^scale`, which rounds the unscaled integer to a
        // float first: past 2^24 that makes a short decimal's REAL differ from a long one's,
        // and without Trino's types a computed decimal is often short here and long there.
        // `sum` over a DECIMAL(8,2) 1413830.04 would be 1413830.125, where Trino gives
        // 1413830.0. See the module doc.
        return decimal_to_f64(unscaled, precision, scale) as f32;
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

/// Arrow's `cast_with_options`, except that a long decimal to DOUBLE or REAL is correctly
/// rounded, as in Trino.
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

/// Arrow's `cast`, except that a long decimal to DOUBLE or REAL is correctly rounded, as in
/// Trino.
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

/// Replace every CAST and TRY_CAST of decimals to DOUBLE or REAL in `expr` with this
/// module's conversion. `schema` is what `expr`'s columns resolve against.
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
        Ok(plan.transform_up_with_subqueries(rewrite_plan)?.data)
    }

    fn name(&self) -> &str {
        "ddi_trino_decimal_casts"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(ts) = &plan {
        let source =
            DFSchema::try_from_qualified_schema(ts.table_name.clone(), &ts.source.schema())?;
        schema.merge(&source);
    }

    let names = NamePreserver::new(&plan);
    let rewritten = plan.map_expressions(|expr| {
        let name = names.save(&expr);
        Ok(rewrite_expr(expr, &schema)?.update_data(|e| name.restore(e)))
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
        // DecimalCasts.shortDecimalToDouble is `(double) unscaled / (double) 10^scale`, for
        // every DECIMAL of up to 18 digits. What Trino 480 returned for each of these, read
        // back from a table it wrote.
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
    }

    #[test]
    fn a_short_decimal_converts_as_arrows_cast_does() {
        // To DOUBLE that is Trino's division too. To REAL it is that double narrowed, as
        // `ddi` 0.3.1 converted it, and not Trino's division of floats: DECIMAL(9,2)
        // 1413830.04 is the nearest float, 1413830.0, as a long decimal's REAL is, where
        // Trino rounds 141383004 to a float before it divides and gets 1413830.125, the float
        // `1_413_830.1` names. The module doc says why, and the README lists the difference.
        assert_eq!(decimal_to_f32(141383004, 9, 2), 1413830.0);
        assert_eq!(decimal_to_f32(141383004, 19, 2), 1413830.0);
        assert_eq!(
            "1413830.04".parse::<f32>().unwrap(),
            1413830.0,
            "the nearest float"
        );
        assert_eq!(
            141383004i64 as f32 / 100f32,
            1_413_830.1,
            "Trino's shortDecimalToReal"
        );

        // Bit for bit, at every short precision and scale. Deterministic, so a failure
        // reproduces.
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for precision in 1..=TRINO_MAX_SHORT_PRECISION {
            for scale in 0..=precision as i8 {
                let bound = 10u64.pow(u32::from(precision));
                let values: Vec<i128> = (0..64)
                    .map(|i| {
                        let v = (next() % bound) as i128;
                        if i % 2 == 0 {
                            v
                        } else {
                            -v
                        }
                    })
                    .collect();
                let array: ArrayRef = Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(precision, scale)
                        .unwrap(),
                );
                // Compared as bytes: equal data is equal bits.
                for to in [DataType::Float32, DataType::Float64] {
                    let ours = decimal_to_float_array(array.as_ref(), &to).unwrap();
                    let arrows = deltalake::arrow::compute::cast(&array, &to).unwrap();
                    assert_eq!(
                        ours.to_data(),
                        arrows.to_data(),
                        "DECIMAL({precision},{scale}) as {to}"
                    );
                }
            }
        }
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
