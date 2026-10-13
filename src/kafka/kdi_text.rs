//! kafka-delta-ingest's text for values going into STRING columns (docs/kafka-design.md §5.6
//! steps 5 and 6, §6.4).
//!
//! # What kdi wrote
//!
//! kdi turned each message into a serde_json value (lib.rs:838-848): apache-avro decoded it
//! with its writer schema, and `TryFrom<Value> for serde_json::Value` (apache-avro 0.21.0
//! types.rs:299-356) made a JSON object with a key for **every field of the writer schema**,
//! null or not, at every depth of records. Then its coercions walked the target's Delta schema
//! (coercions.rs:30-121): wherever a STRING column, a STRING child of a struct, or a STRING
//! element of an array was, a value that was not a JSON string became its JSON text,
//! `Value::String(value.to_string())` (coercions.rs:82-86). A JSON null is not a string, so a
//! null under a key became the text `"null"`; a key that was not there stayed absent, and
//! arrow-json read it as NULL. So:
//!
//! - **By type** (`kdi_compat`): a number is its JSON text, a timestamp, date or time is the raw
//!   number the Avro long or int held (types.rs:335-349), bytes, `fixed`, decimals and durations
//!   are arrays of byte values (types.rs:313-316, 336-337, 350-352), a record or map is an
//!   object with **sorted keys** (serde_json's `Map` is a `BTreeMap` without `preserve_order`,
//!   which neither lock enables), and a union is its branch's value (types.rs:319). Floats are
//!   widened to doubles first (types.rs:307-309), and a NaN or an infinity has no JSON number:
//!   apache-avro failed the whole message (`ConvertF64ToJson`) and kdi dropped it.
//! - **`"null"`** (`kdi_null_text`) only where kdi's JSON had the key, and only where its
//!   coercion reached: a top-level column, a struct's child, an array's element of a STRING or
//!   struct type. Nothing inside a map, and nothing inside an array of arrays or of maps:
//!   `build_coercion_node` makes no node there (coercions.rs:42-65).
//!
//! # How ddi knows where kdi had a key
//!
//! The rows' values are Arrow, and the SQL path aligns every writer schema's rows to the union
//! `U` (§5.5): a v1 row has a null under a field only v2 has, where kdi's JSON had no key. So
//! the key is read off the row's own writer schema, by its id in `<prefix>value_schema_id`:
//! its [`Shape`], which says which fields each record has, at every depth.
//!
//! Decimals need the writer schema too: kdi wrote a decimal's bytes as they were on the wire
//! (decimal.rs:87-104 sign-extends the value back to the length it was read with), and the
//! `fixed` a decimal is written on has a size its Arrow type does not keep.
//!
//! So does every value `U` converted: kdi wrote an older row's int as `5` where `U` holds the
//! double `5.0`, its bytes as an array where `U` holds a string. A value goes into `U` only where
//! it comes back unchanged (crate::kafka::conform), so it is written as the row's own writer
//! schema typed it.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use deltalake::arrow::array::{Array, ArrayRef, AsArray, UnionArray};
use deltalake::arrow::compute::CastOptions;
use deltalake::arrow::datatypes::{
    i256, DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Decimal32Type,
    Decimal64Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Field, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};

use crate::kafka::decode::{decoded_schema, fixed_decimals, raw_schema};
use crate::kafka::registry::RegisteredSchema;

// ======================================================================== the writer's shape

/// What a writer schema's JSON had keys for, as kdi built it: the fields of each record at every
/// depth, through arrays, maps and unions. Read off the decoder's own Arrow schema for it, so
/// names and union branches are spelt as the decoded rows spell them.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Shape {
    /// A record, its fields by name.
    Record(BTreeMap<String, Shape>),
    /// An array, its items.
    List(Box<Shape>),
    /// A map, its values (the keys are the data's own).
    Map(Box<Shape>),
    /// A general union, its branches by the name arrow-avro gives them (codec.rs:1201-1213).
    Union(BTreeMap<String, Shape>),
    /// Any other value: its Arrow type as decoded from this writer schema, and the size of the
    /// `fixed` a decimal is written on.
    Leaf {
        arrow: DataType,
        fixed: Option<usize>,
    },
}

impl Shape {
    /// The shape of `schema`'s rows: a record, its fields as the decoder reads them. Fails where
    /// the decoder would refuse the writer schema; such a writer schema has no rows.
    pub(crate) fn of(schema: &RegisteredSchema) -> Result<Self, String> {
        let raw = raw_schema(schema)?;
        let decoded = decoded_schema(schema)?;
        let fixed = fixed_decimals(&schema.schema);
        Ok(Shape::Record(
            raw.fields()
                .iter()
                .zip(decoded.fields())
                .map(|(f, d)| (f.name().clone(), Shape::field(f, d.data_type(), &fixed)))
                .collect(),
        ))
    }

    /// The shape of `field`, of the decoder's own schema, whose rows are of type `decoded` once
    /// normalised (the same fields, in the same order, at every depth).
    fn field(
        field: &Field,
        decoded: &DataType,
        fixed: &HashMap<(String, Option<String>), usize>,
    ) -> Self {
        use arrow_avro::schema::{AVRO_NAMESPACE_METADATA_KEY, AVRO_NAME_METADATA_KEY};
        let leaf = |fixed: Option<usize>| Shape::Leaf {
            arrow: decoded.clone(),
            fixed,
        };
        match (field.data_type(), decoded) {
            (DataType::Struct(fields), DataType::Struct(normalised)) => Shape::Record(
                fields
                    .iter()
                    .zip(normalised)
                    .map(|(f, d)| (f.name().clone(), Shape::field(f, d.data_type(), fixed)))
                    .collect(),
            ),
            (
                DataType::List(item) | DataType::LargeList(item),
                DataType::List(normalised) | DataType::LargeList(normalised),
            ) => Shape::List(Box::new(Shape::field(item, normalised.data_type(), fixed))),
            (DataType::Map(entries, _), DataType::Map(normalised, _)) => {
                match (entries.data_type(), normalised.data_type()) {
                    (DataType::Struct(kv), DataType::Struct(nkv))
                        if kv.len() == 2 && nkv.len() == 2 =>
                    {
                        Shape::Map(Box::new(Shape::field(&kv[1], nkv[1].data_type(), fixed)))
                    }
                    _ => leaf(None),
                }
            }
            (DataType::Union(fields, _), DataType::Union(normalised, _)) => Shape::Union(
                fields
                    .iter()
                    .zip(normalised.iter())
                    .map(|((_, f), (_, d))| {
                        (f.name().clone(), Shape::field(f, d.data_type(), fixed))
                    })
                    .collect(),
            ),
            (
                DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..),
                _,
            ) => {
                let metadata = field.metadata();
                let size = metadata.get(AVRO_NAME_METADATA_KEY).and_then(|name| {
                    fixed
                        .get(&(
                            name.clone(),
                            metadata.get(AVRO_NAMESPACE_METADATA_KEY).cloned(),
                        ))
                        .copied()
                });
                leaf(size)
            }
            _ => leaf(None),
        }
    }

    /// A record's field `name`.
    pub(crate) fn field_named(&self, name: &str) -> Option<&Shape> {
        match self {
            Shape::Record(fields) => fields.get(name),
            _ => None,
        }
    }

    fn element(&self) -> Option<&Shape> {
        match self {
            Shape::List(item) => Some(item),
            _ => None,
        }
    }

    fn value(&self) -> Option<&Shape> {
        match self {
            Shape::Map(value) => Some(value),
            _ => None,
        }
    }

    /// A union's branch `name`. A value that is not a union in the writer schema, read into a
    /// union that a newer writer schema widened it to (§5.5), is that branch itself.
    fn branch(&self, name: &str) -> Option<&Shape> {
        match self {
            Shape::Union(branches) => branches.get(name),
            plain => Some(plain),
        }
    }

    fn fixed(&self) -> Option<usize> {
        match self {
            Shape::Leaf { fixed, .. } => *fixed,
            _ => None,
        }
    }

    /// `other`'s fields added to this one's, at every depth; where both have one, this one's
    /// wins. The shape of `U` (§5.5) when merged newest first.
    pub(crate) fn merge(&mut self, other: &Shape) {
        match (self, other) {
            (Shape::Record(a), Shape::Record(b)) | (Shape::Union(a), Shape::Union(b)) => {
                for (name, shape) in b {
                    match a.get_mut(name) {
                        Some(mine) => mine.merge(shape),
                        None => {
                            a.insert(name.clone(), shape.clone());
                        }
                    }
                }
            }
            (Shape::List(a), Shape::List(b)) | (Shape::Map(a), Shape::Map(b)) => a.merge(b),
            _ => {}
        }
    }
}

// ============================================================================ where it stood

/// Where one value stood in kdi's JSON (§5.6 step 6).
#[derive(Clone, Copy, Debug)]
pub(crate) struct At<'s> {
    /// kdi's JSON had this key, or this array position: a null here was the JSON `null`, and
    /// became the text `"null"` where kdi's coercion reached.
    pub(crate) key: bool,
    /// The writer schema's type here; `None` for a value no writer field holds (SQL computed it),
    /// whose every child is a key of it.
    pub(crate) shape: Option<&'s Shape>,
}

impl<'s> At<'s> {
    /// Not in kdi's JSON at all: under a null parent, or a computed column's null.
    pub(crate) const ABSENT: At<'static> = At {
        key: false,
        shape: None,
    };

    /// The top-level column `name` of a row written with `writer` (a record's shape).
    pub(crate) fn column(writer: &'s Shape, name: &str) -> Self {
        let shape = writer.field_named(name);
        At {
            key: shape.is_some(),
            shape,
        }
    }

    /// A top-level column that copies the column `source` of a row written with `writer`, as
    /// kdi's `-t 'name: source'` copied a field's JSON: the keys inside are the source field's,
    /// but the column itself has none, since kdi set nothing for a null (transforms.rs:343-347).
    pub(crate) fn copy(writer: &'s Shape, source: &str) -> Self {
        At {
            key: false,
            shape: writer.field_named(source),
        }
    }

    /// The child `name` of this value, which is not null.
    pub(crate) fn child(self, name: &str) -> At<'s> {
        let shape = self.shape.and_then(|s| s.field_named(name));
        At {
            key: self.shape.is_none() || shape.is_some(),
            shape,
        }
    }

    /// An element of this list, which is not null.
    pub(crate) fn element(self) -> At<'s> {
        At {
            key: true,
            shape: self.shape.and_then(Shape::element),
        }
    }

    /// A value of this map, which is not null.
    pub(crate) fn value(self) -> At<'s> {
        At {
            key: true,
            shape: self.shape.and_then(Shape::value),
        }
    }

    /// This union's value in its branch `name`.
    pub(crate) fn branch(self, name: &str) -> At<'s> {
        At {
            key: self.key,
            shape: self.shape.and_then(|s| s.branch(name)),
        }
    }
}

/// Whether kdi's coercion reaches the elements of an array of `item`: an array of strings,
/// timestamps or structs, never of arrays or maps (coercions.rs:57-63).
pub(crate) fn reaches_elements(item: &DataType) -> bool {
    !matches!(
        item,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::Map(..)
    )
}

/// Whether a value of type `t`, at a place kdi's coercion reaches when `reach`, holds a STRING
/// that a null could become `"null"` in.
pub(crate) fn takes_null_text(t: &DataType, reach: bool) -> bool {
    reach
        && match t {
            DataType::Utf8 => true,
            DataType::Struct(fields) => fields.iter().any(|f| takes_null_text(f.data_type(), true)),
            DataType::List(item) | DataType::LargeList(item) => {
                takes_null_text(item.data_type(), reaches_elements(item.data_type()))
            }
            _ => false,
        }
}

/// Whether a value of type `t` holds a map, whose entries kdi wrote in its own order.
pub(crate) fn has_map(t: &DataType) -> bool {
    match t {
        DataType::Map(..) => true,
        DataType::Struct(fields) => fields.iter().any(|f| has_map(f.data_type())),
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            has_map(item.data_type())
        }
        _ => false,
    }
}

// ================================================================================ the text

/// kdi's text of value `i` of `array`, which is not null, going into a STRING: a JSON string as
/// it is, anything else its JSON text. `shape` is the writer schema's type of it, if any. An
/// error says why the value has none; its row is set aside.
pub(crate) fn text(array: &dyn Array, i: usize, shape: Option<&Shape>) -> Result<String, String> {
    if let Some(written) = as_written(array, i, shape) {
        return text(written.as_ref(), 0, shape);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().value(i).to_string()),
        DataType::LargeUtf8 => Ok(array.as_string::<i64>().value(i).to_string()),
        DataType::Utf8View => Ok(array.as_string_view().value(i).to_string()),
        DataType::Dictionary(..) => {
            let value = dictionary_value(array, i)?;
            text(value.as_ref(), 0, shape)
        }
        DataType::Union(..) => {
            let (child, offset, name) = union_value(array, i);
            text(child.as_ref(), offset, shape.and_then(|s| s.branch(name)))
        }
        _ => {
            let mut out = String::new();
            json(array, i, shape, &mut out)?;
            Ok(out)
        }
    }
}

/// Whether value `i` of `array` is null, a union's by its branch and a dictionary's by its
/// value, as serde_json saw it.
pub(crate) fn is_null(array: &dyn Array, i: usize) -> bool {
    match array.data_type() {
        DataType::Null => true,
        DataType::Union(..) => {
            let (child, offset, _) = union_value(array, i);
            is_null(child.as_ref(), offset)
        }
        DataType::Dictionary(..) => {
            array.is_null(i) || dictionary_value(array, i).is_ok_and(|v| is_null(v.as_ref(), 0))
        }
        _ => array.is_null(i),
    }
}

/// Value `i` of `array`, which is not null, as compact JSON, appended to `out`.
fn json(
    array: &dyn Array,
    i: usize,
    shape: Option<&Shape>,
    out: &mut String,
) -> Result<(), String> {
    if let Some(written) = as_written(array, i, shape) {
        return json(written.as_ref(), 0, shape, out);
    }
    macro_rules! int {
        ($t:ty) => {{
            let _ = write!(out, "{}", array.as_primitive::<$t>().value(i));
        }};
    }
    match array.data_type() {
        DataType::Null => out.push_str("null"),
        DataType::Boolean => out.push_str(if array.as_boolean().value(i) {
            "true"
        } else {
            "false"
        }),
        DataType::Int8 => int!(Int8Type),
        DataType::Int16 => int!(Int16Type),
        DataType::Int32 => int!(Int32Type),
        DataType::Int64 => int!(Int64Type),
        DataType::UInt8 => int!(UInt8Type),
        DataType::UInt16 => int!(UInt16Type),
        DataType::UInt32 => int!(UInt32Type),
        DataType::UInt64 => int!(UInt64Type),
        DataType::Float16 => float(f64::from(array.as_primitive::<Float16Type>().value(i)), out)?,
        DataType::Float32 => float(f64::from(array.as_primitive::<Float32Type>().value(i)), out)?,
        DataType::Float64 => float(array.as_primitive::<Float64Type>().value(i), out)?,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            out.push_str(&quoted(&text(array, i, None)?))
        }
        DataType::Binary => bytes(array.as_binary::<i32>().value(i), out),
        DataType::LargeBinary => bytes(array.as_binary::<i64>().value(i), out),
        DataType::BinaryView => bytes(array.as_binary_view().value(i), out),
        DataType::FixedSizeBinary(_) => bytes(array.as_fixed_size_binary().value(i), out),
        DataType::Decimal32(..) => {
            let v = array.as_primitive::<Decimal32Type>().value(i);
            bytes(&decimal_bytes(&i128::from(v).to_be_bytes(), shape), out)
        }
        DataType::Decimal64(..) => {
            let v = array.as_primitive::<Decimal64Type>().value(i);
            bytes(&decimal_bytes(&i128::from(v).to_be_bytes(), shape), out)
        }
        DataType::Decimal128(..) => {
            let v = array.as_primitive::<Decimal128Type>().value(i);
            bytes(&decimal_bytes(&v.to_be_bytes(), shape), out)
        }
        DataType::Decimal256(..) => {
            let v: i256 = array.as_primitive::<Decimal256Type>().value(i);
            bytes(&decimal_bytes(&v.to_be_bytes(), shape), out)
        }
        DataType::Date32 => int!(Date32Type),
        DataType::Date64 => int!(Date64Type),
        DataType::Time32(TimeUnit::Second) => int!(Time32SecondType),
        DataType::Time32(_) => int!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => int!(Time64MicrosecondType),
        DataType::Time64(_) => int!(Time64NanosecondType),
        DataType::Timestamp(TimeUnit::Second, _) => int!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => int!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => int!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => int!(TimestampNanosecondType),
        DataType::Duration(TimeUnit::Second) => int!(DurationSecondType),
        DataType::Duration(TimeUnit::Millisecond) => int!(DurationMillisecondType),
        DataType::Duration(TimeUnit::Microsecond) => int!(DurationMicrosecondType),
        DataType::Duration(TimeUnit::Nanosecond) => int!(DurationNanosecondType),
        DataType::Interval(unit) => {
            let (months, days, millis) = match unit {
                IntervalUnit::MonthDayNano => {
                    let v = array.as_primitive::<IntervalMonthDayNanoType>().value(i);
                    (v.months, v.days, v.nanoseconds.div_euclid(1_000_000))
                }
                IntervalUnit::YearMonth => {
                    (array.as_primitive::<IntervalYearMonthType>().value(i), 0, 0)
                }
                IntervalUnit::DayTime => {
                    let v = array.as_primitive::<IntervalDayTimeType>().value(i);
                    (0, v.days, i64::from(v.milliseconds))
                }
            };
            bytes(&avro_duration(months, days, millis), out)
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => {
            let (values, range) = list_value(array, i);
            let element = shape.and_then(Shape::element);
            out.push('[');
            for (k, j) in range.enumerate() {
                if k > 0 {
                    out.push(',');
                }
                if is_null(values.as_ref(), j) {
                    out.push_str("null");
                } else {
                    json(values.as_ref(), j, element, out)?;
                }
            }
            out.push(']');
        }
        DataType::Struct(fields) => {
            let s = array.as_struct();
            // serde_json's map keeps one value per key, the last one inserted (a struct the
            // SQL built may repeat a name), in the keys' byte order.
            let mut keys: BTreeMap<&str, usize> = BTreeMap::new();
            for (k, f) in fields.iter().enumerate() {
                keys.insert(f.name(), k);
            }
            let at = At { key: true, shape };
            out.push('{');
            let mut first = true;
            for (name, k) in keys {
                let child = s.column(k);
                let null = is_null(child.as_ref(), i);
                let child_at = at.child(name);
                if null && !child_at.key {
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str(&quoted(name));
                out.push(':');
                if null {
                    out.push_str("null");
                } else {
                    json(child.as_ref(), i, child_at.shape, out)?;
                }
            }
            out.push('}');
        }
        DataType::Map(..) => {
            let m = array.as_map();
            let (start, end) = (
                m.value_offsets()[i] as usize,
                m.value_offsets()[i + 1] as usize,
            );
            let (keys, values) = (m.keys(), m.values());
            let value_shape = shape.and_then(Shape::value);
            // apache-avro holds a map in a HashMap, serde_json in a BTreeMap: one value per
            // key, the last one read.
            let mut entries: BTreeMap<String, usize> = BTreeMap::new();
            for j in start..end {
                entries.insert(text(keys.as_ref(), j, None)?, j);
            }
            out.push('{');
            for (k, (key, j)) in entries.into_iter().enumerate() {
                if k > 0 {
                    out.push(',');
                }
                out.push_str(&quoted(&key));
                out.push(':');
                if is_null(values.as_ref(), j) {
                    out.push_str("null");
                } else {
                    json(values.as_ref(), j, value_shape, out)?;
                }
            }
            out.push('}');
        }
        DataType::Union(..) => {
            let (child, offset, name) = union_value(array, i);
            let branch = shape.and_then(|s| s.branch(name));
            if is_null(child.as_ref(), offset) {
                out.push_str("null");
            } else {
                json(child.as_ref(), offset, branch, out)?;
            }
        }
        DataType::Dictionary(..) => {
            let value = dictionary_value(array, i)?;
            json(value.as_ref(), 0, shape, out)?;
        }
        other => {
            return Err(format!(
                "a value of type {other} has no text kafka-delta-ingest could have written"
            ))
        }
    }
    Ok(())
}

/// Whether a value of type `t` standing where `shape` is was converted from the type its row's
/// writer schema gave it: on the SQL path, an older row aligned to a newer writer schema's type
/// in `U` (§5.5).
pub(crate) fn converted(t: &DataType, shape: Option<&Shape>) -> bool {
    match shape {
        Some(Shape::Leaf { arrow, .. }) => arrow != t && plain(arrow) && plain(t),
        _ => false,
    }
}

/// A type that holds one value, not values of other types.
fn plain(t: &DataType) -> bool {
    !t.is_nested() && !matches!(t, DataType::Dictionary(..) | DataType::Null)
}

/// Value `i` of `array` in the type its row's writer schema gave it, as a one-value array, where
/// it was converted from that type ([`converted`]) and is exactly a value of it: kdi wrote the
/// value its own writer schema decoded. `None` otherwise, a value the SQL computed under a
/// writer field's name among them, which is written by its own type.
fn as_written(array: &dyn Array, i: usize, shape: Option<&Shape>) -> Option<ArrayRef> {
    let t = array.data_type();
    if !converted(t, shape) {
        return None;
    }
    let Some(Shape::Leaf { arrow: writer, .. }) = shape else {
        return None;
    };
    let options = CastOptions {
        safe: true,
        ..Default::default()
    };
    let one = array.slice(i, 1);
    let written = crate::transform::decimal::cast_with_options(&one, writer, &options).ok()?;
    let back = crate::transform::decimal::cast_with_options(&written, t, &options).ok()?;
    (written.is_valid(0) && back.to_data() == one.to_data()).then_some(written)
}

/// A double as serde_json writes it (ser.rs:169-180, 1716-1723), which is what kdi's
/// `value.to_string()` gave. NaN and the infinities are no JSON number: apache-avro refused to
/// convert them (types.rs:307-312), and kdi dropped the message.
fn float(v: f64, out: &mut String) -> Result<(), String> {
    match serde_json::Number::from_f64(v) {
        Some(n) => {
            let _ = write!(out, "{n}");
            Ok(())
        }
        None => Err(format!(
            "{v} is not a JSON number; kafka-delta-ingest could not read a message holding it \
             (apache-avro ConvertF64ToJson) and dropped the message"
        )),
    }
}

/// A JSON string, escaped as serde_json escapes it.
fn quoted(s: &str) -> String {
    serde_json::to_string(s).expect("a string always serialises")
}

/// Bytes as serde_json wrote apache-avro's `Vec<u8>`: an array of numbers.
fn bytes(b: &[u8], out: &mut String) {
    out.push('[');
    for (k, byte) in b.iter().enumerate() {
        if k > 0 {
            out.push(',');
        }
        let _ = write!(out, "{byte}");
    }
    out.push(']');
}

/// A decimal's unscaled value, given as big-endian two's complement, as the bytes it was
/// written with: on a `fixed(n)`, sign-extended to `n` bytes (what apache-avro's `to_vec`
/// gave back, decimal.rs:87-104); on `bytes`, the fewest that hold it, which is what Java's
/// `BigInteger.toByteArray()` and so Confluent's serializers write. A producer that padded its
/// `bytes` wrote more, and kdi kept them (docs/kafka-design.md §5.6).
pub(crate) fn decimal_bytes(be: &[u8], shape: Option<&Shape>) -> Vec<u8> {
    let mut start = 0;
    while start + 1 < be.len() {
        let (b, next) = (be[start], be[start + 1]);
        if (b == 0x00 && next & 0x80 == 0) || (b == 0xFF && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    let minimal = &be[start..];
    match shape.and_then(Shape::fixed) {
        Some(n) if n > minimal.len() => {
            let fill = if minimal[0] & 0x80 != 0 { 0xFF } else { 0x00 };
            let mut out = vec![fill; n - minimal.len()];
            out.extend_from_slice(minimal);
            out
        }
        _ => minimal.to_vec(),
    }
}

/// An Avro duration's 12 bytes (apache-avro duration.rs:127-135): months, days and
/// milliseconds, each a little-endian u32. arrow-avro read the milliseconds into nanoseconds
/// (reader/record.rs:1329-1336); a value no Avro duration could hold wraps as kdi's u32 would.
fn avro_duration(months: i32, days: i32, millis: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&(months as u32).to_le_bytes());
    out.extend_from_slice(&(days as u32).to_le_bytes());
    out.extend_from_slice(&(millis as u32).to_le_bytes());
    out
}

/// The child array of value `i` of a union, its offset there, and its branch's name.
fn union_value(array: &dyn Array, i: usize) -> (deltalake::arrow::array::ArrayRef, usize, &str) {
    let u = array
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("matched as a union");
    let DataType::Union(fields, _) = array.data_type() else {
        unreachable!("matched as a union")
    };
    let type_id = u.type_id(i);
    let name = fields
        .iter()
        .find(|(t, _)| *t == type_id)
        .map(|(_, f)| f.name().as_str())
        .unwrap_or_default();
    (u.child(type_id).clone(), u.value_offset(i), name)
}

/// The values array of list `i` and the range of its elements in it.
fn list_value(
    array: &dyn Array,
    i: usize,
) -> (deltalake::arrow::array::ArrayRef, std::ops::Range<usize>) {
    match array.data_type() {
        DataType::List(_) => {
            let l = array.as_list::<i32>();
            let o = l.value_offsets();
            (l.values().clone(), o[i] as usize..o[i + 1] as usize)
        }
        DataType::LargeList(_) => {
            let l = array.as_list::<i64>();
            let o = l.value_offsets();
            (l.values().clone(), o[i] as usize..o[i + 1] as usize)
        }
        _ => {
            let l = array.as_fixed_size_list();
            let n = l.value_length() as usize;
            (l.values().clone(), i * n..(i + 1) * n)
        }
    }
}

/// Value `i` of a dictionary, as a one-value array of its values' type.
fn dictionary_value(
    array: &dyn Array,
    i: usize,
) -> Result<deltalake::arrow::array::ArrayRef, String> {
    let DataType::Dictionary(_, values) = array.data_type() else {
        unreachable!("matched as a dictionary")
    };
    deltalake::arrow::compute::cast(&array.slice(i, 1), values).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fewest bytes on `bytes`, sign-extended on a `fixed`, as Java's `toByteArray()` and
    /// apache-avro's `to_sign_extended_bytes_with_len` give them.
    #[test]
    fn decimal_bytes_are_minimal_or_sign_extended() {
        let minimal = |v: i128| decimal_bytes(&v.to_be_bytes(), None);
        assert_eq!(minimal(0), [0x00]);
        assert_eq!(minimal(1), [0x01]);
        assert_eq!(minimal(127), [0x7F]);
        assert_eq!(minimal(128), [0x00, 0x80]);
        assert_eq!(minimal(-1), [0xFF]);
        assert_eq!(minimal(-128), [0x80]);
        assert_eq!(minimal(-129), [0xFF, 0x7F]);
        assert_eq!(minimal(1234), [0x04, 0xD2]);
        assert_eq!(minimal(i128::MIN), i128::MIN.to_be_bytes());
        let fixed = Shape::Leaf {
            arrow: DataType::Decimal128(10, 2),
            fixed: Some(4),
        };
        let extended = |v: i128| decimal_bytes(&v.to_be_bytes(), Some(&fixed));
        assert_eq!(extended(1234), [0, 0, 0x04, 0xD2]);
        assert_eq!(extended(-1234), [0xFF, 0xFF, 0xFB, 0x2E]);
        assert_eq!(extended(0), [0, 0, 0, 0]);
        let v = i256::from_i128(-5);
        assert_eq!(decimal_bytes(&v.to_be_bytes(), None), [0xFB]);
    }

    /// The text of a double is serde_json's (zmij in serde_json 1.0.151), not Rust's `Display`:
    /// a whole number keeps its `.0`, and an exponent is written past 1e16 and below 1e-5.
    #[test]
    fn floats_are_written_as_serde_json_writes_them() {
        let f = |v: f64| {
            let mut out = String::new();
            float(v, &mut out).map(|()| out)
        };
        for (v, text) in [
            (1.0, "1.0"),
            (-0.0, "-0.0"),
            (0.1, "0.1"),
            (f64::from(0.1f32), "0.10000000149011612"),
            (123456.789, "123456.789"),
            (1e15, "1000000000000000.0"),
            (1e-5, "0.00001"),
        ] {
            assert_eq!(f(v).unwrap(), text, "{v}");
            assert_eq!(serde_json::json!(v).to_string(), text, "{v}");
        }
        for v in [1e16, 1e21, 1e-7, 5e-324, f64::MAX] {
            assert_eq!(f(v).unwrap(), serde_json::json!(v).to_string(), "{v}");
        }
        assert!(f(f64::NAN).unwrap_err().contains("not a JSON number"));
        assert!(f(f64::NEG_INFINITY).is_err());
    }
}
