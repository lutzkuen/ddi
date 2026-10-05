//! docs/kafka-design.md §9.1 "M2, conform", Parity: kafka-delta-ingest's text (§5.6 steps 5 and
//! 6, §6.4), against kafka-delta-ingest's own path.
//!
//! kdi's path is run here as it ran, step by step ([`kdi`]): apache-avro 0.21.0 decodes the
//! message with its writer schema and makes serde_json of it, kdi's coercions (ported below from
//! coercions.rs as they are) turn the target's STRING values into text, and arrow-json reads the
//! result against the target's Arrow schema. ddi's rows come from the same bytes through the
//! decoder, the conform step and the coercer. Each case says whether the two agree, and where
//! they do not, why.

use super::tests::{
    field, reasons, record, registered, rejected_offsets, s, strct, through, Schemas,
};
use super::*;
use crate::kafka::decode::{KafkaDecoder, KafkaMessage, KafkaTimestamp};
use apache_avro::types::Value as Avro;
use deltalake::arrow::datatypes::Int64Type;
use deltalake::arrow::util::display::{ArrayFormatter, FormatOptions};
use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::kernel::{ArrayType, DataType as Delta, MapType, StructField, StructType};

const TEXT: KdiText = KdiText {
    compat: true,
    null_text: true,
};

// ===================================================== kafka-delta-ingest, as it ran

/// kafka-delta-ingest's path from a message to a row (lib.rs:838-848, writer.rs:605-614), with
/// schema_registry_converter 4.10.0's apache-avro 0.21.0 and arrow 55:
/// 1. apache-avro decodes the body with the writer schema alone;
/// 2. `serde_json::Value::try_from` (types.rs:299-356); a failure dropped the message
///    (lib.rs:866-879, the no-op dead-letter queue);
/// 3. its `-t` transforms: here only the Kafka metadata ones, which set a field unless its
///    value is null (transforms.rs:343-372, 441-475);
/// 4. its coercions, ported from coercions.rs:9-145 as they are, only the Delta types being
///    the kernel's ddi links;
/// 5. arrow-json's `Decoder::serialize` against the target's Arrow schema; a failure failed
///    the batch, and kdi stopped on it (lib.rs:932-937).
///
/// Step 5 runs on ddi's arrow-json 58.4.0 with kdi's options (`ReaderBuilder::new`: no
/// `coerce_primitive`, not strict). Its decoders for the types here read a value as 55's did
/// (string_array.rs, decimal_array.rs, primitive_array.rs, timestamp_array.rs differ only in
/// formatting under `coerce_primitive` and the new `ignore_type_conflicts`, both off), except
/// two things modelled or left out: 55 refused a BINARY column outright (reader/mod.rs:742-744,
/// "is not supported by JSON"), modelled below; and arrow-cast 55's `parse_decimal` took a
/// fraction digit on at scale 0, which `double_to_decimal_via_shortest_text` pins instead.
mod kdi {
    use std::collections::HashMap;
    use std::str::FromStr;

    use chrono::prelude::*;
    use deltalake::arrow::datatypes::{DataType as Arrow, SchemaRef};
    use deltalake::arrow::json::ReaderBuilder;
    use deltalake::arrow::record_batch::RecordBatch;
    use deltalake::kernel::{DataType, PrimitiveType, StructType};
    use serde_json::Value;

    // ------------------------------------------------- coercions.rs:9-145, as they are

    #[derive(Debug, Clone, PartialEq)]
    #[allow(unused)]
    enum CoercionNode {
        Coercion(Coercion),
        Tree(CoercionTree),
        ArrayTree(CoercionTree),
        ArrayPrimitive(Coercion),
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Coercion {
        ToString,
        ToTimestamp,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(super) struct CoercionTree {
        root: HashMap<String, CoercionNode>,
    }

    pub(super) fn create_coercion_tree(schema: &StructType) -> CoercionTree {
        let mut root = HashMap::new();

        for field in schema.fields() {
            if let Some(node) = build_coercion_node(field.data_type()) {
                root.insert(field.name().to_string(), node);
            }
        }

        CoercionTree { root }
    }

    fn build_coercion_node(data_type: &DataType) -> Option<CoercionNode> {
        match data_type {
            DataType::Primitive(primitive) => match primitive {
                PrimitiveType::String => Some(CoercionNode::Coercion(Coercion::ToString)),
                PrimitiveType::Timestamp => Some(CoercionNode::Coercion(Coercion::ToTimestamp)),
                _ => None,
            },
            DataType::Struct(st) => {
                let nested_context = create_coercion_tree(st);
                if !nested_context.root.is_empty() {
                    Some(CoercionNode::Tree(nested_context))
                } else {
                    None
                }
            }
            DataType::Array(array) => {
                build_coercion_node(array.element_type()).and_then(|node| match node {
                    CoercionNode::Coercion(c) => Some(CoercionNode::ArrayPrimitive(c)),
                    CoercionNode::Tree(t) => Some(CoercionNode::ArrayTree(t)),
                    _ => None,
                })
            }
            _ => None,
        }
    }

    pub(super) fn coerce(value: &mut Value, coercion_tree: &CoercionTree) {
        if let Some(context) = value.as_object_mut() {
            for (field_name, coercion) in coercion_tree.root.iter() {
                if let Some(value) = context.get_mut(field_name) {
                    apply_coercion(value, coercion);
                }
            }
        }
    }

    fn apply_coercion(value: &mut Value, node: &CoercionNode) {
        match node {
            CoercionNode::Coercion(Coercion::ToString) => {
                if !value.is_string() {
                    *value = Value::String(value.to_string());
                }
            }
            CoercionNode::Coercion(Coercion::ToTimestamp) => {
                if let Some(as_str) = value.as_str() {
                    if let Some(parsed) = string_to_timestamp(as_str) {
                        *value = parsed
                    }
                }
            }
            CoercionNode::Tree(tree) => {
                for (name, node) in tree.root.iter() {
                    let fields = value.as_object_mut();
                    if let Some(fields) = fields {
                        if let Some(value) = fields.get_mut(name) {
                            apply_coercion(value, node);
                        }
                    }
                }
            }
            CoercionNode::ArrayPrimitive(coercion) => {
                let values = value.as_array_mut();
                if let Some(values) = values {
                    let node = CoercionNode::Coercion(coercion.clone());
                    for value in values {
                        apply_coercion(value, &node);
                    }
                }
            }
            CoercionNode::ArrayTree(tree) => {
                let values = value.as_array_mut();
                if let Some(values) = values {
                    let node = CoercionNode::Tree(tree.clone());
                    for value in values {
                        apply_coercion(value, &node);
                    }
                }
            }
        }
    }

    fn string_to_timestamp(string: &str) -> Option<Value> {
        let parsed = DateTime::from_str(string);
        parsed
            .ok()
            .map(|dt: DateTime<Utc>| Value::Number(dt.timestamp_micros().into()))
    }

    // ------------------------------------------------------------- the other steps

    /// Steps 1 and 2: the message's JSON, or why apache-avro gave none.
    pub(super) fn deserialize(schema: &apache_avro::Schema, body: &[u8]) -> Result<Value, String> {
        let value = apache_avro::from_avro_datum(schema, &mut &body[..], None)
            .map_err(|e| format!("apache-avro could not read it: {e}"))?;
        Value::try_from(value).map_err(|e| format!("apache-avro made no JSON of it: {e}"))
    }

    /// Step 3, for a metadata field: set unless null (transforms.rs:343-347).
    pub(super) fn set_value(value: &mut Value, name: &str, v: Value) {
        if !v.is_null() {
            value
                .as_object_mut()
                .expect("a record")
                .insert(name.to_string(), v);
        }
    }

    /// Step 5: the batch arrow-json made of the coerced values, or why it made none.
    pub(super) fn record_batch(schema: SchemaRef, json: &[Value]) -> Result<RecordBatch, String> {
        fn binary(t: &Arrow) -> bool {
            match t {
                Arrow::Binary | Arrow::LargeBinary | Arrow::FixedSizeBinary(_) => true,
                Arrow::Struct(fs) => fs.iter().any(|f| binary(f.data_type())),
                Arrow::List(f) | Arrow::LargeList(f) | Arrow::Map(f, _) => binary(f.data_type()),
                _ => false,
            }
        }
        if let Some(f) = schema.fields().iter().find(|f| binary(f.data_type())) {
            return Err(format!(
                "arrow-json 55 refuses the column {}: Binary is not supported by JSON",
                f.name()
            ));
        }
        let mut decoder = ReaderBuilder::new(schema)
            .build_decoder()
            .map_err(|e| e.to_string())?;
        decoder.serialize(json).map_err(|e| e.to_string())?;
        decoder
            .flush()
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "no rows".to_string())
    }
}

/// What kdi made of a case's messages.
#[derive(Debug)]
enum KdiOut {
    /// The values, as [`shown`] shows them, in message order.
    Rows(Vec<String>),
    /// apache-avro made no JSON of any message: kdi dropped them.
    Dropped(String),
    /// arrow-json refused the batch: kdi stopped on it.
    Failed(String),
}

/// The metadata kdi's `-t kafka_*: kafka.*` transforms would set on a message's JSON
/// (transforms.rs:441-475): partition 0, the offset, and the timestamp in microseconds.
fn kdi_metadata(json: &mut serde_json::Value, offset: i64, timestamp_ms: Option<i64>) {
    kdi::set_value(json, "kafka_partition", serde_json::json!(0));
    kdi::set_value(json, "kafka_offset", serde_json::json!(offset));
    kdi::set_value(
        json,
        "kafka_timestamp",
        match timestamp_ms {
            Some(ms) => serde_json::json!(ms * 1000),
            None => serde_json::Value::Null,
        },
    );
}

/// kdi's rows for `bodies` (Avro bodies without the Confluent header) of `writer`, into
/// `delta`, column `column` shown in message order.
fn kdi_rows(writer: &str, bodies: &[Vec<u8>], delta: &StructType, column: &str) -> KdiOut {
    let messages: Vec<(&str, &[u8], Option<i64>)> = bodies
        .iter()
        .enumerate()
        .map(|(offset, b)| (writer, b.as_slice(), Some(timestamp_ms(offset))))
        .collect();
    kdi_batch(&messages, delta).map_or_else(|k| k, |batch| KdiOut::Rows(shown(&[batch], column)))
}

/// kdi's batch for `messages` at offsets 0.., each `(its writer schema, its Avro body, its
/// timestamp in milliseconds)`, into `delta`; or what kdi did instead.
fn kdi_batch(
    messages: &[(&str, &[u8], Option<i64>)],
    delta: &StructType,
) -> std::result::Result<RecordBatch, KdiOut> {
    let tree = kdi::create_coercion_tree(delta);
    let mut values = Vec::new();
    let mut dropped = Vec::new();
    for (offset, (writer, body, timestamp)) in messages.iter().enumerate() {
        let schema = apache_avro::Schema::parse_str(writer).unwrap();
        match kdi::deserialize(&schema, body) {
            Ok(mut v) => {
                kdi_metadata(&mut v, offset as i64, *timestamp);
                kdi::coerce(&mut v, &tree);
                values.push(v);
            }
            Err(why) => dropped.push(why),
        }
    }
    if !dropped.is_empty() {
        assert!(
            values.is_empty(),
            "a case's messages are dropped all or none: {dropped:?}"
        );
        return Err(KdiOut::Dropped(dropped[0].clone()));
    }
    let arrow: Schema = delta.try_into_arrow().unwrap();
    kdi::record_batch(Arc::new(arrow), &values).map_err(KdiOut::Failed)
}

/// kdi's batch for messages that it wrote, as [`kdi_batch`].
fn kdi_wrote(messages: &[(&str, &[u8], Option<i64>)], delta: &StructType) -> RecordBatch {
    match kdi_batch(messages, delta) {
        Ok(batch) => batch,
        Err(what) => panic!("kdi wrote nothing: {what:?}"),
    }
}

// ================================================================== ddi's side

/// The timestamp of the message at `offset`, in milliseconds.
fn timestamp_ms(offset: usize) -> i64 {
    1_700_000_000_000 + offset as i64
}

/// `bodies` as messages of writer schema `id` at offsets 0.., behind the Confluent header,
/// decoded.
async fn decode(schemas: &[(u32, &str)], bodies: Vec<(u32, Vec<u8>)>) -> Vec<RecordBatch> {
    let messages = bodies
        .into_iter()
        .enumerate()
        .map(|(offset, (id, body))| {
            let mut payload = vec![0u8];
            payload.extend(id.to_be_bytes());
            payload.extend(body);
            KafkaMessage {
                partition: 0,
                offset: offset as i64,
                timestamp: KafkaTimestamp::CreateTime(timestamp_ms(offset)),
                key: None,
                value: Some(payload),
                headers: Vec::new(),
            }
        })
        .collect();
    decode_messages(schemas, messages).await
}

async fn decode_messages(schemas: &[(u32, &str)], messages: Vec<KafkaMessage>) -> Vec<RecordBatch> {
    let map = schemas
        .iter()
        .map(|(id, json)| (*id, registered(*id, json)))
        .collect();
    let mut d = KafkaDecoder::new("orders", "kafka_", Arc::new(Schemas(map)));
    for m in messages {
        d.feed(m).await.unwrap();
    }
    let out = d.finish().unwrap();
    assert!(out.undecodable.is_empty(), "{:?}", out.undecodable);
    out.batches
}

/// A conformer into `t` writing kdi's text as `text` says, told of `schemas`.
fn kdi_conformer(t: SchemaRef, text: KdiText, schemas: &[(u32, &str)]) -> Conformer {
    let mut c = Conformer::new(t, "kafka_", text).unwrap();
    let members: Vec<_> = schemas.iter().map(|(id, j)| registered(*id, j)).collect();
    c.observe(&members);
    c
}

/// Column `column` of `batches`, each value as Arrow displays it (`NULL` for a null), in the
/// order of the batches' `id` column when they have one.
fn shown(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let options = FormatOptions::default().with_null("NULL");
    let mut rows: Vec<(i64, String)> = Vec::new();
    for b in batches {
        let col = b.column_by_name(column).unwrap();
        let f = ArrayFormatter::try_new(col.as_ref(), &options).unwrap();
        let ids = b.column_by_name("id").map(|c| {
            deltalake::arrow::compute::cast(c, &DataType::Int64)
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        });
        for i in 0..b.num_rows() {
            let id = ids.as_ref().map_or(rows.len() as i64, |ids| ids[i]);
            rows.push((id, f.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, v)| v).collect()
}

/// ddi's rows for `bodies` of `writer` (id 1) into `t`: column `column` in message order, a
/// rejected message shown as `REJECTED`.
async fn ddi_rows(writer: &str, bodies: Vec<Vec<u8>>, t: SchemaRef, column: &str) -> Vec<String> {
    let schemas = [(1, writer)];
    let batches = decode(&schemas, bodies.into_iter().map(|b| (1, b)).collect()).await;
    let mut c = kdi_conformer(t, TEXT, &schemas);
    let (good, rejects) = through(&mut c, batches);
    let mut rows: Vec<(i64, String)> = Vec::new();
    for b in &good {
        let ids = b.column_by_name("id").unwrap();
        let ids = deltalake::arrow::compute::cast(ids, &DataType::Int64).unwrap();
        let values = shown(std::slice::from_ref(b), column);
        for (i, v) in values.into_iter().enumerate() {
            rows.push((ids.as_primitive::<Int64Type>().value(i), v));
        }
    }
    for r in &rejects {
        for offset in r
            .rows
            .column_by_name("kafka_offset")
            .unwrap()
            .as_primitive::<Int64Type>()
            .values()
        {
            rows.push((*offset, "REJECTED".to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, v)| v).collect()
}

// ================================================================== the matrix

/// One message's value of field `v`: written by apache-avro, or given as the bytes of `v`.
#[derive(Clone)]
enum Body {
    Avro(Avro),
    Raw(Vec<u8>),
}

/// What a case expects of ddi, against kdi.
enum Expect {
    /// ddi writes what kdi wrote.
    Same,
    /// kdi dropped every message (apache-avro made no JSON of it); ddi writes these.
    KdiDropped(&'static [&'static str]),
    /// kdi's arrow-json refused the batch, and kdi stopped on it; ddi writes these.
    KdiFailed(&'static [&'static str]),
    /// Both write, differently: ddi these, for the reason given.
    Differs(&'static [&'static str], &'static str),
}

struct Case {
    /// The Avro type, as the matrix names it.
    avro: &'static str,
    /// The Avro type of field `v`, as schema JSON.
    schema: &'static str,
    values: Vec<Body>,
    /// The target types `v` is written into, each with what is expected there.
    targets: Vec<(&'static str, Delta, Expect)>,
}

/// The writer schema of a case: `id`, then `v` of the case's type.
fn writer_of(v: &str) -> String {
    format!(
        r#"{{"type":"record","name":"R","namespace":"ns","fields":[{{"name":"id","type":"int"}},{{"name":"v","type":{v}}}]}}"#
    )
}

fn decimal(bytes: &[u8]) -> Avro {
    Avro::Decimal(apache_avro::Decimal::from(bytes.to_vec()))
}

fn union(branch: u32, v: Avro) -> Avro {
    Avro::Union(branch, Box::new(v))
}

fn cases() -> Vec<Case> {
    use std::str::FromStr as _;
    use Body::{Avro as A, Raw};
    use Expect::*;
    let string = || Delta::STRING;
    let array = |t: Delta| Delta::from(ArrayType::new(t, true));
    let map = |v: Delta| Delta::from(MapType::new(Delta::STRING, v, true));
    let dec = |p: u8, s: u8| Delta::decimal(p, s).unwrap();
    vec![
        Case {
            avro: "null",
            schema: r#""null""#,
            values: vec![A(Avro::Null)],
            targets: vec![("STRING", string(), Same), ("BIGINT", Delta::LONG, Same)],
        },
        Case {
            avro: "boolean",
            schema: r#""boolean""#,
            values: vec![A(Avro::Boolean(true)), A(Avro::Boolean(false))],
            targets: vec![
                ("STRING", string(), Same),
                ("BOOLEAN", Delta::BOOLEAN, Same),
            ],
        },
        Case {
            avro: "int",
            schema: r#""int""#,
            values: vec![A(Avro::Int(0)), A(Avro::Int(-5)), A(Avro::Int(i32::MAX))],
            targets: vec![
                ("STRING", string(), Same),
                ("INT", Delta::INTEGER, Same),
                ("BIGINT", Delta::LONG, Same),
            ],
        },
        Case {
            avro: "long",
            schema: r#""long""#,
            values: vec![
                A(Avro::Long(i64::MIN)),
                A(Avro::Long(0)),
                A(Avro::Long(1_636_668_718_000_000)),
            ],
            targets: vec![("STRING", string(), Same), ("BIGINT", Delta::LONG, Same)],
        },
        Case {
            avro: "long",
            schema: r#""long""#,
            values: vec![A(Avro::Long(0)), A(Avro::Long(1_636_668_718_000_001))],
            targets: vec![("TIMESTAMP", Delta::TIMESTAMP, Same)],
        },
        Case {
            avro: "float",
            schema: r#""float""#,
            values: [0.1f32, 1.0, -0.0, f32::MAX, 1e-7, 1.5e16, 3.0e-39]
                .into_iter()
                .map(|f| A(Avro::Float(f)))
                .collect(),
            targets: vec![
                ("STRING", string(), Same),
                ("FLOAT", Delta::FLOAT, Same),
                ("DOUBLE", Delta::DOUBLE, Same),
            ],
        },
        Case {
            avro: "float NaN, ±inf",
            schema: r#""float""#,
            values: vec![
                A(Avro::Float(f32::NAN)),
                A(Avro::Float(f32::INFINITY)),
                A(Avro::Float(f32::NEG_INFINITY)),
            ],
            targets: vec![
                (
                    "STRING",
                    string(),
                    KdiDropped(&["REJECTED", "REJECTED", "REJECTED"]),
                ),
                ("FLOAT", Delta::FLOAT, KdiDropped(&["NaN", "inf", "-inf"])),
            ],
        },
        Case {
            avro: "double",
            schema: r#""double""#,
            values: [
                0.1,
                1e21,
                1e16,
                1e15,
                123456.789,
                -1.5e-10,
                5e-324,
                f64::MAX,
            ]
            .into_iter()
            .map(|d| A(Avro::Double(d)))
            .collect(),
            targets: vec![("STRING", string(), Same), ("DOUBLE", Delta::DOUBLE, Same)],
        },
        Case {
            avro: "double",
            schema: r#""double""#,
            values: [0.125, -1.5, 12_345_678.99, 0.1]
                .into_iter()
                .map(|d| A(Avro::Double(d)))
                .collect(),
            targets: vec![("DECIMAL(10,2)", dec(10, 2), Same)],
        },
        Case {
            avro: "double too large",
            schema: r#""double""#,
            values: vec![A(Avro::Double(1e21))],
            targets: vec![("DECIMAL(10,2)", dec(10, 2), KdiFailed(&["REJECTED"]))],
        },
        Case {
            avro: "double NaN",
            schema: r#""double""#,
            values: vec![A(Avro::Double(f64::NAN))],
            targets: vec![
                ("STRING", string(), KdiDropped(&["REJECTED"])),
                ("DOUBLE", Delta::DOUBLE, KdiDropped(&["NaN"])),
            ],
        },
        Case {
            avro: "bytes",
            schema: r#""bytes""#,
            values: vec![
                A(Avro::Bytes(vec![1, 2, 3])),
                A(Avro::Bytes(vec![])),
                A(Avro::Bytes(vec![255, 0])),
            ],
            targets: vec![
                ("STRING", string(), Same),
                ("BINARY", Delta::BINARY, KdiFailed(&["010203", "", "ff00"])),
            ],
        },
        Case {
            avro: "string",
            schema: r#""string""#,
            values: vec![A(s("a")), A(s("")), A(s("é \"q\"\n")), A(s("null"))],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "string",
            schema: r#""string""#,
            values: vec![
                A(s("2021-11-11T22:11:58Z")),
                A(s("2021-11-11T22:11:58.5+01:00")),
                A(s("2021-11-11 22:11:58")),
            ],
            targets: vec![("TIMESTAMP", Delta::TIMESTAMP, Same)],
        },
        Case {
            avro: "string, not a time",
            schema: r#""string""#,
            values: vec![A(s("not a time"))],
            targets: vec![("TIMESTAMP", Delta::TIMESTAMP, KdiFailed(&["REJECTED"]))],
        },
        Case {
            avro: "record",
            schema: r#"{"type":"record","name":"Inner","fields":[
                {"name":"z","type":"int"},{"name":"a","type":"string"},
                {"name":"n","type":["null","double"]}]}"#,
            values: vec![
                A(record(vec![
                    ("z", Avro::Int(1)),
                    ("a", s("x")),
                    ("n", union(0, Avro::Null)),
                ])),
                A(record(vec![
                    ("z", Avro::Int(2)),
                    ("a", s("y")),
                    ("n", union(1, Avro::Double(2.5))),
                ])),
            ],
            targets: vec![
                ("STRING", string(), Same),
                (
                    "STRUCT<z INT, a STRING, n DOUBLE>",
                    strct(vec![
                        field("z", Delta::INTEGER, true),
                        field("a", Delta::STRING, true),
                        field("n", Delta::DOUBLE, true),
                    ]),
                    Same,
                ),
                (
                    "STRUCT<a, n, z STRING>",
                    strct(vec![
                        field("a", Delta::STRING, true),
                        field("n", Delta::STRING, true),
                        field("z", Delta::STRING, true),
                    ]),
                    Same,
                ),
            ],
        },
        Case {
            avro: "enum",
            schema: r#"{"type":"enum","name":"E","symbols":["A","B"]}"#,
            values: vec![A(Avro::Enum(0, "A".into())), A(Avro::Enum(1, "B".into()))],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "array<int>",
            schema: r#"{"type":"array","items":"int"}"#,
            values: vec![
                A(Avro::Array(vec![Avro::Int(1), Avro::Int(2)])),
                A(Avro::Array(vec![])),
            ],
            targets: vec![
                ("STRING", string(), Same),
                ("ARRAY<INT>", array(Delta::INTEGER), Same),
                ("ARRAY<STRING>", array(Delta::STRING), Same),
            ],
        },
        Case {
            avro: "array<[null,string]>",
            schema: r#"{"type":"array","items":["null","string"]}"#,
            values: vec![A(Avro::Array(vec![union(1, s("a")), union(0, Avro::Null)]))],
            targets: vec![
                ("STRING", string(), Same),
                ("ARRAY<STRING>", array(Delta::STRING), Same),
            ],
        },
        Case {
            avro: "array<array<int>>",
            schema: r#"{"type":"array","items":{"type":"array","items":"int"}}"#,
            values: vec![A(Avro::Array(vec![
                Avro::Array(vec![Avro::Int(1)]),
                Avro::Array(vec![Avro::Int(2), Avro::Int(3)]),
            ]))],
            targets: vec![
                ("ARRAY<ARRAY<INT>>", array(array(Delta::INTEGER)), Same),
                (
                    "ARRAY<ARRAY<STRING>>",
                    array(array(Delta::STRING)),
                    KdiFailed(&["[[1], [2, 3]]"]),
                ),
                ("STRING", string(), Same),
            ],
        },
        Case {
            avro: "map<int>",
            schema: r#"{"type":"map","values":"int"}"#,
            // One block of two entries, `b` then `a`, and the same key twice (Avro forbids it,
            // and nothing checks): a producer's order, which kdi's JSON object did not keep.
            values: vec![
                Raw(vec![4, 2, b'b', 4, 2, b'a', 2, 0]),
                Raw(vec![4, 2, b'k', 2, 2, b'k', 6, 0]),
            ],
            targets: vec![
                ("STRING", string(), Same),
                ("MAP<STRING,INT>", map(Delta::INTEGER), Same),
                (
                    "MAP<STRING,STRING>",
                    map(Delta::STRING),
                    KdiFailed(&["{a: 1, b: 2}", "{k: 3}"]),
                ),
            ],
        },
        Case {
            avro: "map<[null,int]>",
            schema: r#"{"type":"map","values":["null","int"]}"#,
            // `b` then `a`: the decoded type is the target's own, and the order kdi's still.
            values: vec![Raw(vec![4, 2, b'b', 2, 4, 2, b'a', 0, 0])],
            targets: vec![("MAP<STRING,INT>", map(Delta::INTEGER), Same)],
        },
        Case {
            avro: "map<[null,string]>",
            schema: r#"{"type":"map","values":["null","string"]}"#,
            values: vec![A(Avro::Map(
                [("k".to_string(), union(0, Avro::Null))]
                    .into_iter()
                    .collect(),
            ))],
            targets: vec![
                ("MAP<STRING,STRING>", map(Delta::STRING), Same),
                ("STRING", string(), Same),
            ],
        },
        Case {
            avro: "union [null,int,string]",
            schema: r#"["null","int","string"]"#,
            values: vec![
                A(union(0, Avro::Null)),
                A(union(1, Avro::Int(5))),
                A(union(2, s("x"))),
            ],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "union [null,int,string]",
            schema: r#"["null","int","string"]"#,
            values: vec![A(union(0, Avro::Null)), A(union(1, Avro::Int(5)))],
            targets: vec![("BIGINT", Delta::LONG, Same)],
        },
        Case {
            avro: "fixed(4)",
            schema: r#"{"type":"fixed","name":"F4","size":4}"#,
            values: vec![A(Avro::Fixed(4, vec![0, 1, 2, 255]))],
            targets: vec![
                ("STRING", string(), Same),
                ("BINARY", Delta::BINARY, KdiFailed(&["000102ff"])),
            ],
        },
        Case {
            avro: "decimal(10,2) on bytes",
            schema: r#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"#,
            // 12.34, -1.28, 0, 99999999.99: the fewest bytes, as Java writes them.
            values: vec![
                A(decimal(&[0x04, 0xD2])),
                A(decimal(&[0x80])),
                A(decimal(&[0x00])),
                A(decimal(&[0x02, 0x54, 0x0B, 0xE3, 0xFF])),
            ],
            targets: vec![
                ("STRING", string(), Same),
                (
                    "DECIMAL(10,2)",
                    dec(10, 2),
                    KdiFailed(&["12.34", "-1.28", "0.00", "99999999.99"]),
                ),
            ],
        },
        Case {
            avro: "decimal(10,2) on padded bytes",
            schema: r#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"#,
            values: vec![A(decimal(&[0x00, 0x00, 0x04, 0xD2]))],
            targets: vec![(
                "STRING",
                string(),
                Differs(
                    &["[4,210]"],
                    "kdi kept the producer's padding; the decoded value no longer has it",
                ),
            )],
        },
        Case {
            avro: "decimal(10,2) on fixed(8)",
            schema: r#"{"type":"fixed","name":"F8","size":8,"logicalType":"decimal","precision":10,"scale":2}"#,
            values: vec![
                A(decimal(&[0, 0, 0, 0, 0, 0, 0x04, 0xD2])),
                A(decimal(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFB, 0x2E])),
            ],
            targets: vec![
                ("STRING", string(), Same),
                ("DECIMAL(10,2)", dec(10, 2), KdiFailed(&["12.34", "-12.34"])),
            ],
        },
        Case {
            avro: "decimal(10,2) on fixed(8), named again",
            schema: r#"{"type":"record","name":"Two","fields":[
                {"name":"a","type":{"type":"fixed","name":"F8","size":8,"logicalType":"decimal","precision":10,"scale":2}},
                {"name":"b","type":"F8"}]}"#,
            values: vec![Raw(vec![
                0, 0, 0, 0, 0, 0, 0x04, 0xD2, 0, 0, 0, 0, 0, 0, 0x04, 0xD3,
            ])],
            targets: vec![(
                "STRING",
                string(),
                KdiDropped(&[r#"{"a":[0,0,0,0,0,0,4,210],"b":[0,0,0,0,0,0,4,211]}"#]),
            )],
        },
        Case {
            avro: "decimal, scale 5 > precision 2 (invalid: its bytes)",
            schema: r#"{"type":"bytes","logicalType":"decimal","precision":2,"scale":5}"#,
            values: vec![A(Avro::Bytes(vec![1, 2]))],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "decimal, scale -2 (invalid: its bytes)",
            schema: r#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":-2}"#,
            values: vec![A(Avro::Bytes(vec![0x30, 0x39]))],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "decimal(77,2) on bytes (more than Arrow holds: its bytes)",
            schema: r#"{"type":"bytes","logicalType":"decimal","precision":77,"scale":2}"#,
            values: vec![A(decimal(&[0x00, 0x04, 0xD2]))],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "decimal(10,2) on fixed(1) (more than it holds: its bytes)",
            schema: r#"{"type":"fixed","name":"F1","size":1,"logicalType":"decimal","precision":10,"scale":2}"#,
            values: vec![A(decimal(&[0xFB]))],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "uuid on string",
            schema: r#"{"type":"string","logicalType":"uuid"}"#,
            values: vec![
                A(Avro::Uuid(
                    apache_avro::Uuid::parse_str("550E8400-E29B-41D4-A716-446655440000").unwrap(),
                )),
                // Raw: a string in the simple form, which both read as a uuid.
                Raw({
                    let text = b"550e8400e29b41d4a716446655440000";
                    let mut b = vec![(text.len() * 2) as u8];
                    b.extend_from_slice(text);
                    b
                }),
            ],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "uuid on fixed(16)",
            schema: r#"{"type":"fixed","name":"U16","size":16,"logicalType":"uuid"}"#,
            values: vec![Raw(vec![
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ])],
            targets: vec![(
                "STRING",
                string(),
                KdiDropped(&["550e8400-e29b-41d4-a716-446655440000"]),
            )],
        },
        Case {
            avro: "date",
            schema: r#"{"type":"int","logicalType":"date"}"#,
            values: vec![A(Avro::Date(19000)), A(Avro::Date(-1))],
            targets: vec![("STRING", string(), Same), ("DATE", Delta::DATE, Same)],
        },
        Case {
            avro: "time-millis",
            schema: r#"{"type":"int","logicalType":"time-millis"}"#,
            values: vec![A(Avro::TimeMillis(3_600_000))],
            targets: vec![("STRING", string(), Same), ("INT", Delta::INTEGER, Same)],
        },
        Case {
            avro: "time-micros",
            schema: r#"{"type":"long","logicalType":"time-micros"}"#,
            values: vec![A(Avro::TimeMicros(3_600_000_001))],
            targets: vec![("STRING", string(), Same), ("BIGINT", Delta::LONG, Same)],
        },
        Case {
            avro: "timestamp-millis",
            schema: r#"{"type":"long","logicalType":"timestamp-millis"}"#,
            values: vec![A(Avro::TimestampMillis(1_636_668_718_123))],
            targets: vec![
                ("STRING", string(), Same),
                (
                    "TIMESTAMP",
                    Delta::TIMESTAMP,
                    Differs(
                        &["2021-11-11T22:11:58.123Z"],
                        "kdi read the milliseconds as microseconds (§6.4 1)",
                    ),
                ),
                ("BIGINT", Delta::LONG, Same),
            ],
        },
        Case {
            avro: "timestamp-micros",
            schema: r#"{"type":"long","logicalType":"timestamp-micros"}"#,
            values: vec![A(Avro::TimestampMicros(1_636_668_718_123_456))],
            targets: vec![
                ("STRING", string(), Same),
                ("TIMESTAMP", Delta::TIMESTAMP, Same),
                ("BIGINT", Delta::LONG, Same),
            ],
        },
        Case {
            avro: "timestamp-nanos",
            schema: r#"{"type":"long","logicalType":"timestamp-nanos"}"#,
            values: vec![A(Avro::TimestampNanos(1_636_668_718_123_456_789))],
            targets: vec![
                ("STRING", string(), Same),
                (
                    "TIMESTAMP",
                    Delta::TIMESTAMP,
                    Differs(
                        &["2021-11-11T22:11:58.123456Z"],
                        "kdi read the nanoseconds as microseconds",
                    ),
                ),
                ("BIGINT", Delta::LONG, Same),
            ],
        },
        Case {
            avro: "local-timestamp-millis",
            schema: r#"{"type":"long","logicalType":"local-timestamp-millis"}"#,
            values: vec![A(Avro::LocalTimestampMillis(1_636_668_718_123))],
            targets: vec![
                ("STRING", string(), Same),
                (
                    "TIMESTAMP_NTZ",
                    Delta::TIMESTAMP_NTZ,
                    Differs(
                        &["2021-11-11T22:11:58.123"],
                        "kdi read the milliseconds as microseconds (§6.4 1)",
                    ),
                ),
            ],
        },
        Case {
            avro: "local-timestamp-micros",
            schema: r#"{"type":"long","logicalType":"local-timestamp-micros"}"#,
            values: vec![A(Avro::LocalTimestampMicros(1_636_668_718_123_456))],
            targets: vec![
                ("STRING", string(), Same),
                ("TIMESTAMP_NTZ", Delta::TIMESTAMP_NTZ, Same),
                ("TIMESTAMP", Delta::TIMESTAMP, Same),
            ],
        },
        Case {
            avro: "local-timestamp-nanos",
            schema: r#"{"type":"long","logicalType":"local-timestamp-nanos"}"#,
            values: vec![A(Avro::LocalTimestampNanos(1_636_668_718_123_456_789))],
            targets: vec![
                ("STRING", string(), Same),
                (
                    "TIMESTAMP_NTZ",
                    Delta::TIMESTAMP_NTZ,
                    Differs(
                        &["2021-11-11T22:11:58.123456"],
                        "kdi read the nanoseconds as microseconds",
                    ),
                ),
            ],
        },
        Case {
            avro: "duration",
            schema: r#"{"type":"fixed","name":"D12","size":12,"logicalType":"duration"}"#,
            values: vec![A(Avro::Duration(apache_avro::Duration::new(
                apache_avro::Months::new(1),
                apache_avro::Days::new(2),
                apache_avro::Millis::new(300_000),
            )))],
            targets: vec![("STRING", string(), Same)],
        },
        Case {
            avro: "big-decimal",
            schema: r#"{"type":"bytes","logicalType":"big-decimal"}"#,
            values: vec![A(Avro::BigDecimal(
                apache_avro::BigDecimal::from_str("12.34").unwrap(),
            ))],
            targets: vec![(
                "STRING",
                string(),
                Differs(
                    &["[4,4,210,4]"],
                    "kdi's apache-avro wrote the bytes with their length in front \
                     (bigdecimal.rs:39-48); arrow-avro knows no big-decimal and reads the bytes",
                ),
            )],
        },
    ]
}

/// Every Avro type and logical type a Confluent producer writes (and big-decimal, which none
/// does by default), into STRING, its natural type and kdi's coercion targets: ddi's rows
/// against kdi's, run as kdi ran (see [`kdi`]). Prints the matrix (`--nocapture`).
#[tokio::test]
async fn kdi_parity_matrix() {
    let mut failures = Vec::new();
    println!("| Avro | target | kdi | ddi | verdict |\n|---|---|---|---|---|");
    for case in cases() {
        let writer = writer_of(case.schema);
        let schema = apache_avro::Schema::parse_str(&writer).unwrap();
        let bodies: Vec<Vec<u8>> = case
            .values
            .iter()
            .enumerate()
            .map(|(i, body)| match body {
                Body::Avro(v) => apache_avro::to_avro_datum(
                    &schema,
                    record(vec![("id", Avro::Int(i as i32)), ("v", v.clone())]),
                )
                .unwrap(),
                Body::Raw(v) => {
                    let mut b = vec![(i * 2) as u8];
                    b.extend_from_slice(v);
                    b
                }
            })
            .collect();
        for (label, t, expect) in &case.targets {
            let delta = StructType::try_new(vec![
                field("id", Delta::INTEGER, true),
                field("v", t.clone(), true),
            ])
            .unwrap();
            let arrow: Schema = (&delta).try_into_arrow().unwrap();
            let kdi = kdi_rows(&writer, &bodies, &delta, "v");
            let ddi = ddi_rows(&writer, bodies.clone(), Arc::new(arrow), "v").await;
            let (ok, verdict) = match (expect, &kdi) {
                (Expect::Same, KdiOut::Rows(k)) => (k == &ddi, "same".to_string()),
                (Expect::KdiDropped(want), KdiOut::Dropped(_)) => {
                    (ddi == *want, "kdi dropped the message".to_string())
                }
                (Expect::KdiFailed(want), KdiOut::Failed(_)) => {
                    (ddi == *want, "kdi stopped (arrow-json)".to_string())
                }
                (Expect::Differs(want, why), KdiOut::Rows(k)) => {
                    (ddi == *want && k != &ddi, format!("differs: {why}"))
                }
                _ => (false, "unexpected".to_string()),
            };
            let kdi_shown = match &kdi {
                KdiOut::Rows(r) => format!("{r:?}"),
                KdiOut::Dropped(why) => format!("dropped ({why})"),
                KdiOut::Failed(why) => format!("failed ({why})"),
            };
            println!(
                "| {} | {label} | {kdi_shown} | {ddi:?} | {}{verdict} |",
                case.avro,
                if ok { "" } else { "MISMATCH: " }
            );
            if !ok {
                failures.push(format!(
                    "{} into {label}: kdi {kdi_shown}, ddi {ddi:?}",
                    case.avro
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// ============================================================ the other parity tests

/// `v`, a record of `writer`, as apache-avro writes its body.
fn body(writer: &str, v: Avro) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(writer).unwrap();
    apache_avro::to_avro_datum(&schema, v).unwrap()
}

/// The message at `offset`: `body` of writer schema `id` behind the Confluent header.
fn message(offset: usize, id: u32, body: &[u8], timestamp_ms: Option<i64>) -> KafkaMessage {
    let mut payload = vec![0u8];
    payload.extend(id.to_be_bytes());
    payload.extend_from_slice(body);
    KafkaMessage {
        partition: 0,
        offset: offset as i64,
        timestamp: timestamp_ms.map_or(KafkaTimestamp::NotAvailable, KafkaTimestamp::CreateTime),
        key: None,
        value: Some(payload),
        headers: Vec::new(),
    }
}

fn arrow_of(delta: &StructType) -> SchemaRef {
    let schema: Schema = delta.try_into_arrow().unwrap();
    Arc::new(schema)
}

/// The SQL path: `batches` aligned to the `U` of `members`, `sql` run over them, and the output
/// conformed into `t` and coerced.
async fn through_sql(
    sql: &str,
    members: &[(u32, &str)],
    batches: Vec<RecordBatch>,
    t: SchemaRef,
    text: KdiText,
) -> (Conformer, Vec<RecordBatch>, Vec<Rejected>) {
    let registered: Vec<_> = members.iter().map(|(id, j)| registered(*id, j)).collect();
    let u = UnionSchema::build(&registered, "kafka_");
    let mut rejects = Vec::new();
    let aligned = u.align_all(batches, &mut rejects).unwrap();
    assert!(rejects.is_empty(), "{:?}", reasons(&rejects));
    let out = SqlTransform::new(sql)
        .run(u.schema(), aligned)
        .await
        .unwrap();
    let mut c = kdi_conformer(t, text, members);
    let (good, rejects) = through(&mut c, out);
    (c, good, rejects)
}

const N: &str = r#"{"type":"record","name":"N","fields":[
    {"name":"id","type":"int"},
    {"name":"top","type":["null","string"]},
    {"name":"num","type":["null","long"]},
    {"name":"nothing","type":"null"},
    {"name":"rec","type":["null",{"type":"record","name":"Rec","fields":[
        {"name":"s","type":["null","string"]},{"name":"n","type":["null","int"]}]}]},
    {"name":"strs","type":["null",{"type":"array","items":["null","string"]}]},
    {"name":"recs","type":{"type":"array","items":["null","Rec"]}},
    {"name":"m","type":{"type":"map","values":["null","string"]}},
    {"name":"nested","type":{"type":"array","items":{"type":"array","items":["null","string"]}}}]}"#;

fn n_rows() -> Vec<Avro> {
    let null = || union(0, Avro::Null);
    let rec = |s_: Avro, n: Avro| record(vec![("s", s_), ("n", n)]);
    vec![
        record(vec![
            ("id", Avro::Int(0)),
            ("top", null()),
            ("num", null()),
            ("nothing", Avro::Null),
            ("rec", null()),
            ("strs", null()),
            (
                "recs",
                Avro::Array(vec![null(), union(1, rec(null(), null()))]),
            ),
            (
                "m",
                Avro::Map([("k".to_string(), null())].into_iter().collect()),
            ),
            ("nested", Avro::Array(vec![Avro::Array(vec![null()])])),
        ]),
        record(vec![
            ("id", Avro::Int(1)),
            ("top", union(1, s("x"))),
            ("num", union(1, Avro::Long(7))),
            ("nothing", Avro::Null),
            ("rec", union(1, rec(null(), union(1, Avro::Int(5))))),
            (
                "strs",
                union(1, Avro::Array(vec![union(1, s("a")), null()])),
            ),
            ("recs", Avro::Array(vec![])),
            ("m", Avro::Map(HashMap::new())),
            ("nested", Avro::Array(vec![])),
        ]),
    ]
}

fn n_target(extra: Vec<StructField>) -> StructType {
    let rec = || {
        strct(vec![
            field("s", Delta::STRING, true),
            field("n", Delta::STRING, true),
        ])
    };
    let mut fields = vec![
        field("id", Delta::INTEGER, true),
        field("top", Delta::STRING, true),
        field("num", Delta::STRING, true),
        field("nothing", Delta::STRING, true),
        field("rec", rec(), true),
        field("strs", ArrayType::new(Delta::STRING, true), true),
        field("recs", ArrayType::new(rec(), true), true),
        field("m", MapType::new(Delta::STRING, Delta::STRING, true), true),
        field(
            "nested",
            ArrayType::new(Delta::from(ArrayType::new(Delta::STRING, true)), true),
            true,
        ),
        field("kafka_timestamp", Delta::STRING, true),
        field("kafka_offset", Delta::STRING, true),
        field("absent", Delta::STRING, true),
    ];
    fields.extend(extra);
    StructType::try_new(fields).unwrap()
}

/// Where a null becomes the text `"null"`: exactly where kdi's JSON had the key and its coercion
/// reached. At the top level and in a struct whose value is there; in an array of strings, an
/// element of one that is there; in an array of structs, element by element; for a field of the
/// `null` type, always. Never under a null struct or array, inside a map, inside an array of
/// arrays (kdi built no coercion node there, coercions.rs:57-64), for a metadata column (kdi's
/// `-t kafka_timestamp: kafka.timestamp` set nothing for no timestamp, transforms.rs:343-347,
/// 468-475), for a target column nothing produces, nor for a column the SQL computed as null. A
/// struct the SQL built has every child as a key. kdi's rows, run as kdi ran, agree everywhere.
#[tokio::test]
async fn kdi_null_text_matrix() {
    let bodies: Vec<Vec<u8>> = n_rows().into_iter().map(|v| body(N, v)).collect();
    let timestamps = [None, Some(timestamp_ms(1))];
    let messages = (0..2)
        .map(|o| message(o, 1, &bodies[o], timestamps[o]))
        .collect();
    let batches = decode_messages(&[(1, N)], messages).await;
    let delta = n_target(vec![]);
    let mut c = kdi_conformer(arrow_of(&delta), TEXT, &[(1, N)]);
    let (good, rejects) = through(&mut c, batches.clone());
    assert!(rejects.is_empty(), "{:?}", reasons(&rejects));
    let kdi = kdi_wrote(
        &[
            (N, &bodies[0], timestamps[0]),
            (N, &bodies[1], timestamps[1]),
        ],
        &delta,
    );
    let expected: [(&str, [&str; 2]); 11] = [
        ("top", ["null", "x"]),
        ("num", ["null", "7"]),
        ("nothing", ["null", "null"]),
        ("rec", ["NULL", "{s: null, n: 5}"]),
        ("strs", ["NULL", "[a, null]"]),
        ("recs", ["[NULL, {s: null, n: null}]", "[]"]),
        ("m", ["{k: NULL}", "{}"]),
        ("nested", ["[[NULL]]", "[]"]),
        ("kafka_timestamp", ["NULL", "1700000000001000"]),
        ("kafka_offset", ["0", "1"]),
        ("absent", ["NULL", "NULL"]),
    ];
    for (col, want) in expected {
        assert_eq!(shown(&good, col), want, "ddi, {col}");
        assert_eq!(shown(std::slice::from_ref(&kdi), col), want, "kdi, {col}");
    }
    assert!(!c.null_text_approximate());

    // Through SQL that keeps every field and computes two columns: the writer's fields as
    // above; a computed null has no key; a computed struct's null child is a key of it.
    let delta = n_target(vec![
        field("extra", Delta::STRING, true),
        field("made", strct(vec![field("s", Delta::STRING, true)]), true),
    ]);
    let (c, good, rejects) = through_sql(
        "SELECT *, CAST(NULL AS VARCHAR) AS extra, \
         named_struct('s', CAST(NULL AS VARCHAR)) AS made FROM source",
        &[(1, N)],
        batches,
        arrow_of(&delta),
        TEXT,
    )
    .await;
    assert!(rejects.is_empty(), "{:?}", reasons(&rejects));
    for (col, want) in expected {
        assert_eq!(shown(&good, col), want, "ddi through SQL, {col}");
    }
    assert_eq!(shown(&good, "extra"), ["NULL", "NULL"]);
    assert_eq!(shown(&good, "made"), ["{s: null}", "{s: null}"]);
    assert!(!c.null_text_approximate());

    // kdi_null_text alone: the nulls the same, everything else Arrow's cast.
    let delta = n_target(vec![]);
    let text = KdiText {
        compat: false,
        null_text: true,
    };
    let (_, good, _) = through_sql(
        "SELECT * FROM source",
        &[(1, N)],
        decode_messages(
            &[(1, N)],
            (0..2)
                .map(|o| message(o, 1, &bodies[o], timestamps[o]))
                .collect(),
        )
        .await,
        arrow_of(&delta),
        text,
    )
    .await;
    assert_eq!(shown(&good, "top"), ["null", "x"]);
    assert_eq!(shown(&good, "rec"), ["NULL", "{s: null, n: 5}"]);
    assert_eq!(shown(&good, "kafka_timestamp")[0], "NULL");
}

const P1: &str = r#"{"type":"record","name":"P","fields":[
    {"name":"id","type":"int"},
    {"name":"a","type":["null","string"]},
    {"name":"r","type":{"type":"record","name":"PR","fields":[
        {"name":"x","type":["null","string"]}]}}]}"#;

const P2: &str = r#"{"type":"record","name":"P","fields":[
    {"name":"id","type":"int"},
    {"name":"a","type":["null","string"]},
    {"name":"b","type":["null","string"]},
    {"name":"r","type":{"type":"record","name":"PR","fields":[
        {"name":"x","type":["null","string"]},{"name":"y","type":["null","string"]}]}}]}"#;

/// Four messages: v1 and v2 of `P` alternating, the first two with every nullable field null,
/// the last two with every field set.
fn p_messages() -> Vec<(u32, &'static str, Vec<u8>)> {
    let null = || union(0, Avro::Null);
    let some = |v: &str| union(1, s(v));
    vec![
        (
            1,
            P1,
            body(
                P1,
                record(vec![
                    ("id", Avro::Int(0)),
                    ("a", null()),
                    ("r", record(vec![("x", null())])),
                ]),
            ),
        ),
        (
            2,
            P2,
            body(
                P2,
                record(vec![
                    ("id", Avro::Int(1)),
                    ("a", null()),
                    ("b", null()),
                    ("r", record(vec![("x", null()), ("y", null())])),
                ]),
            ),
        ),
        (
            1,
            P1,
            body(
                P1,
                record(vec![
                    ("id", Avro::Int(2)),
                    ("a", some("A")),
                    ("r", record(vec![("x", some("X"))])),
                ]),
            ),
        ),
        (
            2,
            P2,
            body(
                P2,
                record(vec![
                    ("id", Avro::Int(3)),
                    ("a", some("A")),
                    ("b", some("B")),
                    ("r", record(vec![("x", some("X")), ("y", some("Y"))])),
                ]),
            ),
        ),
    ]
}

fn p_target() -> StructType {
    StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("a", Delta::STRING, true),
        field("b", Delta::STRING, true),
        field(
            "r",
            strct(vec![
                field("x", Delta::STRING, true),
                field("y", Delta::STRING, true),
            ]),
            true,
        ),
    ])
    .unwrap()
}

async fn p_decoded() -> Vec<RecordBatch> {
    let messages = p_messages()
        .iter()
        .enumerate()
        .map(|(o, (id, _, b))| message(o, *id, b, Some(timestamp_ms(o))))
        .collect();
    decode_messages(&[(1, P1), (2, P2)], messages).await
}

/// A field added in v2 (`b`, and `r.y` inside a record): a v1 row has no key for it in kdi's
/// JSON, so its null stays NULL, while a v2 row's null is `"null"`. Read off each row's writer
/// schema by `kafka_value_schema_id`, on the identity path (a batch per writer schema) as on
/// the SQL path (every row aligned to `U`, where a v1 row has a null under `b` too), and as kdi
/// wrote them.
#[tokio::test]
async fn null_text_presence_by_schema_id() {
    let members = [(1, P1), (2, P2)];
    let delta = p_target();
    let want_b = ["NULL", "null", "NULL", "B"];
    let want_r = [
        "{x: null, y: NULL}",
        "{x: null, y: null}",
        "{x: X, y: NULL}",
        "{x: X, y: Y}",
    ];
    let want_a = ["null", "null", "A", "A"];

    let mut c = kdi_conformer(arrow_of(&delta), TEXT, &members);
    let (identity, rejects) = through(&mut c, p_decoded().await);
    assert!(rejects.is_empty());
    let (c_sql, sql, rejects) = through_sql(
        "SELECT * FROM source",
        &members,
        p_decoded().await,
        arrow_of(&delta),
        TEXT,
    )
    .await;
    assert!(rejects.is_empty());
    let messages = p_messages();
    let kdi = kdi_wrote(
        &messages
            .iter()
            .enumerate()
            .map(|(o, (_, w, b))| (*w, b.as_slice(), Some(timestamp_ms(o))))
            .collect::<Vec<_>>(),
        &delta,
    );
    for (rows, path) in [(identity, "identity"), (sql, "SQL"), (vec![kdi], "kdi")] {
        assert_eq!(shown(&rows, "a"), want_a, "{path}");
        assert_eq!(shown(&rows, "b"), want_b, "{path}");
        assert_eq!(shown(&rows, "r"), want_r, "{path}");
    }
    assert!(!c.null_text_approximate() && !c_sql.null_text_approximate());
}

/// SQL that drops `kafka_value_schema_id` leaves no row saying its writer schema: where kdi had
/// a key is then read off every writer schema seen (`U`'s fields), with a warning once and the
/// gauge's flag set. The rows that differ from kdi are exactly the v1 rows' `b` and `r.y`,
/// fields only v2 has, which get `"null"` where kdi had nothing.
#[tokio::test]
async fn null_text_approximate_without_schema_id_column() {
    let members = [(1, P1), (2, P2)];
    let delta = p_target();
    let (c, good, rejects) = through_sql(
        "SELECT id, a, b, r FROM source",
        &members,
        p_decoded().await,
        arrow_of(&delta),
        TEXT,
    )
    .await;
    assert!(rejects.is_empty());
    assert!(c.null_text_approximate());
    assert_eq!(shown(&good, "a"), ["null", "null", "A", "A"]);
    assert_eq!(shown(&good, "b"), ["null", "null", "null", "B"]);
    assert_eq!(
        shown(&good, "r"),
        [
            "{x: null, y: null}",
            "{x: null, y: null}",
            "{x: X, y: null}",
            "{x: X, y: Y}"
        ]
    );

    // Without kdi's text there is nothing to approximate.
    let (c, _, _) = through_sql(
        "SELECT id, a, b, r FROM source",
        &members,
        p_decoded().await,
        arrow_of(&delta),
        KdiText::default(),
    )
    .await;
    assert!(!c.null_text_approximate());
}

/// A conformer writing kdi's text must know the writer schema of every row: one it was not
/// told of is an internal error, never a guess.
#[tokio::test]
async fn an_unobserved_writer_schema_is_an_internal_error() {
    let delta = p_target();
    let mut c = kdi_conformer(arrow_of(&delta), TEXT, &[(1, P1)]);
    let err = c.conform(&p_decoded().await[1]).unwrap_err();
    assert!(err.to_string().contains("writer schema 2"), "{err}");
}

const T: &str = r#"{"type":"record","name":"T","fields":[
    {"name":"id","type":"int"},
    {"name":"l","type":"long"},
    {"name":"d","type":"double"},
    {"name":"b","type":"bytes"},
    {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
    {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}},
    {"name":"e","type":{"type":"enum","name":"E","symbols":["X","Y"]}},
    {"name":"inner","type":{"type":"record","name":"In","fields":[
        {"name":"l","type":"long"},
        {"name":"d","type":"double"},
        {"name":"b","type":"bytes"},
        {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"dt","type":{"type":"int","logicalType":"date"}},
        {"name":"tags","type":{"type":"array","items":"long"}}]}},
    {"name":"longs","type":{"type":"array","items":"long"}},
    {"name":"stamps","type":{"type":"array","items":{"type":"long","logicalType":"timestamp-micros"}}},
    {"name":"ins","type":{"type":"array","items":"In"}},
    {"name":"m","type":{"type":"map","values":"double"}},
    {"name":"u","type":["null","long","In"]}]}"#;

fn t_rows() -> Vec<Avro> {
    let inner = |l: i64, d: f64, tags: Vec<i64>| {
        record(vec![
            ("l", Avro::Long(l)),
            ("d", Avro::Double(d)),
            ("b", Avro::Bytes(vec![9])),
            ("ts", Avro::TimestampMillis(1)),
            ("dt", Avro::Date(19000)),
            (
                "tags",
                Avro::Array(tags.into_iter().map(Avro::Long).collect()),
            ),
        ])
    };
    let row = |id: i32, u: Avro| {
        record(vec![
            ("id", Avro::Int(id)),
            ("l", Avro::Long(5)),
            ("d", Avro::Double(0.5)),
            ("b", Avro::Bytes(vec![1, 2])),
            ("ts", Avro::TimestampMillis(1_636_668_718_123)),
            ("dec", decimal(&[0x04, 0xD2])),
            ("e", Avro::Enum(1, "Y".into())),
            ("inner", inner(-1, 1e21, vec![1, 2])),
            ("longs", Avro::Array(vec![Avro::Long(1), Avro::Long(-2)])),
            (
                "stamps",
                Avro::Array(vec![Avro::TimestampMicros(1_636_668_718_123_456)]),
            ),
            ("ins", Avro::Array(vec![inner(3, 0.25, vec![])])),
            (
                "m",
                Avro::Map([("x".to_string(), Avro::Double(1.5))].into_iter().collect()),
            ),
            ("u", u),
        ])
    };
    vec![
        row(0, union(2, inner(4, -0.0, vec![7]))),
        row(1, union(1, Avro::Long(7))),
        row(2, union(0, Avro::Null)),
    ]
}

/// A value going into a STRING is written by its Arrow type wherever it sits: a top-level
/// column, a struct's child, a list's element, a union's branch, and a whole record, list or map
/// as one JSON text whose leaves follow the same rules. Each as kdi wrote it.
#[tokio::test]
async fn render_by_arrow_type_at_depth() {
    let bodies: Vec<Vec<u8>> = t_rows().into_iter().map(|v| body(T, v)).collect();
    let messages: Vec<(&str, &[u8], Option<i64>)> = bodies
        .iter()
        .enumerate()
        .map(|(o, b)| (T, b.as_slice(), Some(timestamp_ms(o))))
        .collect();
    let decoded_t = || decode(&[(1, T)], bodies.iter().map(|b| (1, b.clone())).collect());
    let string = || Delta::STRING;
    let strings = || ArrayType::new(Delta::STRING, true);

    // Each leaf where it sits.
    let at_depth = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("l", string(), true),
        field("d", string(), true),
        field("b", string(), true),
        field("ts", string(), true),
        field("dec", string(), true),
        field("e", string(), true),
        field(
            "inner",
            strct(vec![
                field("l", string(), true),
                field("d", string(), true),
                field("b", string(), true),
                field("ts", string(), true),
                field("dt", string(), true),
                field("tags", strings(), true),
            ]),
            true,
        ),
        field("longs", strings(), true),
        field("stamps", strings(), true),
        field(
            "ins",
            ArrayType::new(
                strct(vec![
                    field("l", string(), true),
                    field("tags", string(), true),
                ]),
                true,
            ),
            true,
        ),
        field("u", string(), true),
    ])
    .unwrap();
    // Whole values as one text each.
    let whole = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("inner", string(), true),
        field("longs", string(), true),
        field("stamps", string(), true),
        field("ins", string(), true),
        field("m", string(), true),
    ])
    .unwrap();
    for delta in [&at_depth, &whole] {
        let mut c = kdi_conformer(arrow_of(delta), TEXT, &[(1, T)]);
        let (good, rejects) = through(&mut c, decoded_t().await);
        assert!(rejects.is_empty(), "{:?}", reasons(&rejects));
        let kdi = kdi_wrote(&messages, delta);
        for f in delta.fields() {
            assert_eq!(
                shown(&good, f.name()),
                shown(std::slice::from_ref(&kdi), f.name()),
                "{}",
                f.name()
            );
        }
    }

    // What that is, spelt out.
    let mut c = kdi_conformer(arrow_of(&at_depth), TEXT, &[(1, T)]);
    let (good, _) = through(&mut c, decoded_t().await);
    assert_eq!(shown(&good, "l")[0], "5");
    assert_eq!(shown(&good, "d")[0], "0.5");
    assert_eq!(shown(&good, "b")[0], "[1,2]");
    assert_eq!(shown(&good, "ts")[0], "1636668718123");
    assert_eq!(shown(&good, "dec")[0], "[4,210]");
    assert_eq!(shown(&good, "e")[0], "Y");
    assert_eq!(
        shown(&good, "inner")[0],
        "{l: -1, d: 1e+21, b: [9], ts: 1, dt: 19000, tags: [1, 2]}"
    );
    assert_eq!(shown(&good, "longs")[0], "[1, -2]");
    assert_eq!(shown(&good, "stamps")[0], "[1636668718123456]");
    assert_eq!(shown(&good, "ins")[0], "[{l: 3, tags: []}]");
    assert_eq!(
        shown(&good, "u"),
        [
            r#"{"b":[9],"d":-0.0,"dt":19000,"l":4,"tags":[7],"ts":1}"#,
            "7",
            "null"
        ]
    );
    let mut c = kdi_conformer(arrow_of(&whole), TEXT, &[(1, T)]);
    let (good, _) = through(&mut c, decoded_t().await);
    assert_eq!(
        shown(&good, "inner")[0],
        r#"{"b":[9],"d":1e+21,"dt":19000,"l":-1,"tags":[1,2],"ts":1}"#
    );
    assert_eq!(shown(&good, "m")[0], r#"{"x":1.5}"#);

    // A map's values into a STRING map value: kdi's arrow-json refused them (its coercion
    // never reached into a map); ddi writes them by type all the same.
    let map = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("m", MapType::new(Delta::STRING, Delta::STRING, true), true),
    ])
    .unwrap();
    let mut c = kdi_conformer(arrow_of(&map), TEXT, &[(1, T)]);
    let (good, _) = through(&mut c, decoded_t().await);
    assert_eq!(shown(&good, "m")[0], "{x: 1.5}");
    assert!(matches!(kdi_batch(&messages, &map), Err(KdiOut::Failed(_))));

    // Computed by SQL, by type the same.
    let computed = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("l1", string(), true),
        field("d2", string(), true),
        field("whole", string(), true),
    ])
    .unwrap();
    let (_, good, rejects) = through_sql(
        "SELECT id, l + 1 AS l1, d * 2 AS d2, inner AS whole FROM source",
        &[(1, T)],
        decoded_t().await,
        arrow_of(&computed),
        TEXT,
    )
    .await;
    assert!(rejects.is_empty());
    assert_eq!(shown(&good, "l1")[0], "6");
    assert_eq!(shown(&good, "d2")[0], "1.0");
    assert_eq!(
        shown(&good, "whole")[0],
        r#"{"b":[9],"d":1e+21,"dt":19000,"l":-1,"tags":[1,2],"ts":1}"#
    );
}

/// A timestamp going into a STRING is the raw number of its unit, as kdi's JSON held it,
/// whatever its name: aliased by the SQL, or the message's own timestamp, which kdi's
/// `kafka.timestamp` gave in microseconds (transforms.rs:468-475). Text the SQL produced itself
/// passes unchanged. A writer field the SQL renamed is computed: its null has no key, so no
/// `"null"`, as kdi's `-t nn: n` set nothing for a null.
#[tokio::test]
async fn aliased_timestamp_renders_raw_number() {
    const W: &str = r#"{"type":"record","name":"W","fields":[
        {"name":"id","type":"int"},
        {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"tu","type":{"type":"long","logicalType":"timestamp-micros"}},
        {"name":"n","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}]}"#;
    let row = record(vec![
        ("id", Avro::Int(0)),
        ("ts", Avro::TimestampMillis(1_636_668_718_123)),
        ("tu", Avro::TimestampMicros(1_636_668_718_123_456)),
        ("n", union(0, Avro::Null)),
    ]);
    let bodies = vec![(1, body(W, row))];
    let delta = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("when", Delta::STRING, true),
        field("whenu", Delta::STRING, true),
        field("kt", Delta::STRING, true),
        field("ko", Delta::STRING, true),
        field("ts", Delta::TIMESTAMP, true),
        field("txt", Delta::STRING, true),
        field("nn", Delta::STRING, true),
        field("n", Delta::STRING, true),
    ])
    .unwrap();
    let (_, good, rejects) = through_sql(
        r#"SELECT id, ts AS "when", tu AS whenu, kafka_timestamp AS kt, kafka_offset AS ko,
                  ts, to_char(ts, '%Y-%m-%d') AS txt, n AS nn, n FROM source"#,
        &[(1, W)],
        decode(&[(1, W)], bodies.clone()).await,
        arrow_of(&delta),
        TEXT,
    )
    .await;
    assert!(rejects.is_empty(), "{:?}", reasons(&rejects));
    assert_eq!(shown(&good, "when"), ["1636668718123"]);
    assert_eq!(shown(&good, "whenu"), ["1636668718123456"]);
    assert_eq!(shown(&good, "kt"), ["1700000000000000"]);
    assert_eq!(shown(&good, "ko"), ["0"]);
    assert_eq!(shown(&good, "ts"), ["2021-11-11T22:11:58.123Z"]);
    assert_eq!(shown(&good, "txt"), ["2021-11-11"]);
    assert_eq!(shown(&good, "nn"), ["NULL"]);
    assert_eq!(shown(&good, "n"), ["null"]);

    // Without SQL, the same field into a STRING column of its own name.
    let plain = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("ts", Delta::STRING, true),
        field("kafka_timestamp", Delta::STRING, true),
    ])
    .unwrap();
    let mut c = kdi_conformer(arrow_of(&plain), TEXT, &[(1, W)]);
    let (good, _) = through(&mut c, decode(&[(1, W)], bodies).await);
    assert_eq!(shown(&good, "ts"), ["1636668718123"]);
    assert_eq!(shown(&good, "kafka_timestamp"), ["1700000000000000"]);
}

/// A decimal going into a STRING is the bytes it was written with, as kdi wrote them: on
/// `bytes` the fewest that hold it (what Java's `BigInteger.toByteArray()`, and so Confluent's
/// serializers, write), on a `fixed(8)` sign-extended to 8, wherever it sits: a top-level field,
/// a record's, an array's element, a union's branch, and inside a record written whole. The
/// size comes from the row's writer schema: a fixed decimal the SQL renamed has lost it, and is
/// written in the fewest bytes, where kdi kept 8. Into a DECIMAL column it is the number.
#[tokio::test]
async fn decimal_bytes_minimal_and_fixed_sign_extended() {
    // Each fixed defined where it is used: apache-avro 0.21.0 cannot read a decimal `fixed`
    // named again (see the matrix), and kdi dropped such messages.
    const D: &str = r#"{"type":"record","name":"D","namespace":"ns","fields":[
        {"name":"id","type":"int"},
        {"name":"db","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
        {"name":"df","type":{"type":"fixed","name":"F8","size":8,"logicalType":"decimal","precision":10,"scale":2}},
        {"name":"r","type":{"type":"record","name":"DR","fields":[
            {"name":"df","type":{"type":"fixed","name":"G8","namespace":"other","size":8,"logicalType":"decimal","precision":10,"scale":2}},
            {"name":"db","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}},
        {"name":"dfs","type":{"type":"array","items":{"type":"fixed","name":"H8","size":8,"logicalType":"decimal","precision":10,"scale":2}}},
        {"name":"u","type":["null",{"type":"fixed","name":"I8","size":8,"logicalType":"decimal","precision":10,"scale":2}]},
        {"name":"g","type":["null","string",{"type":"fixed","name":"J8","size":8,"logicalType":"decimal","precision":10,"scale":2}]}]}"#;
    // 12.34, -12.34 and 0: on bytes and on fixed(8).
    let values: [(&[u8], [u8; 8]); 3] = [
        (&[0x04, 0xD2], [0, 0, 0, 0, 0, 0, 0x04, 0xD2]),
        (
            &[0xFB, 0x2E],
            [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFB, 0x2E],
        ),
        (&[0x00], [0; 8]),
    ];
    let bodies: Vec<(u32, Vec<u8>)> = values
        .iter()
        .enumerate()
        .map(|(i, (b, f))| {
            let row = record(vec![
                ("id", Avro::Int(i as i32)),
                ("db", decimal(b)),
                ("df", decimal(f)),
                ("r", record(vec![("df", decimal(f)), ("db", decimal(b))])),
                ("dfs", Avro::Array(vec![decimal(f)])),
                ("u", union(1, decimal(f))),
                ("g", union(2, decimal(f))),
            ]);
            (1, body(D, row))
        })
        .collect();
    let string = || Delta::STRING;
    let delta = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("db", string(), true),
        field("df", string(), true),
        field(
            "r",
            strct(vec![
                field("df", string(), true),
                field("db", string(), true),
            ]),
            true,
        ),
        field("dfs", ArrayType::new(string(), true), true),
        field("u", string(), true),
        field("g", string(), true),
    ])
    .unwrap();
    let mut c = kdi_conformer(arrow_of(&delta), TEXT, &[(1, D)]);
    let (good, rejects) = through(&mut c, decode(&[(1, D)], bodies.clone()).await);
    assert!(rejects.is_empty(), "{:?}", reasons(&rejects));
    let kdi = kdi_wrote(
        &bodies
            .iter()
            .enumerate()
            .map(|(o, (_, b))| (D, b.as_slice(), Some(timestamp_ms(o))))
            .collect::<Vec<_>>(),
        &delta,
    );
    for f in delta.fields() {
        assert_eq!(
            shown(&good, f.name()),
            shown(std::slice::from_ref(&kdi), f.name()),
            "{}",
            f.name()
        );
    }
    assert_eq!(shown(&good, "db"), ["[4,210]", "[251,46]", "[0]"]);
    let fixed = [
        "[0,0,0,0,0,0,4,210]",
        "[255,255,255,255,255,255,251,46]",
        "[0,0,0,0,0,0,0,0]",
    ];
    for col in ["df", "u", "g"] {
        assert_eq!(shown(&good, col), fixed, "{col}");
    }
    assert_eq!(
        shown(&good, "r")[1],
        "{df: [255,255,255,255,255,255,251,46], db: [251,46]}"
    );
    assert_eq!(shown(&good, "dfs")[0], "[[0,0,0,0,0,0,4,210]]");

    // A whole record: the same bytes inside its JSON.
    let whole = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("r", string(), true),
        field("df_typed", Delta::decimal(10, 2).unwrap(), true),
    ])
    .unwrap();
    let (_, good, rejects) = through_sql(
        "SELECT *, df AS df_typed, df AS dfa FROM source",
        &[(1, D)],
        decode(&[(1, D)], bodies.clone()).await,
        arrow_of(&whole),
        TEXT,
    )
    .await;
    assert!(rejects.is_empty());
    assert_eq!(
        shown(&good, "r")[0],
        r#"{"db":[4,210],"df":[0,0,0,0,0,0,4,210]}"#
    );
    assert_eq!(shown(&good, "df_typed"), ["12.34", "-12.34", "0.00"]);

    // Renamed by the SQL, the fixed size is lost: the fewest bytes (kdi's `-t dfa: df` kept 8).
    let renamed = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("dfa", string(), true),
    ])
    .unwrap();
    let (_, good, _) = through_sql(
        "SELECT id, df AS dfa FROM source",
        &[(1, D)],
        decode(&[(1, D)], bodies).await,
        arrow_of(&renamed),
        TEXT,
    )
    .await;
    assert_eq!(shown(&good, "dfa"), ["[4,210]", "[251,46]", "[0]"]);
}

/// The message key has no kdi equivalent, so `kafka_key` into a STRING is Arrow's cast, as
/// without the options: its bytes as UTF-8, a key that is not UTF-8 set aside by the coercer,
/// and no key NULL (a metadata column, never `"null"`). The rule is by name: the key renamed by
/// the SQL is bytes like any other, written as their values.
#[tokio::test]
async fn kafka_key_cast_not_rendered() {
    const K: &str = r#"{"type":"record","name":"K","fields":[{"name":"id","type":"int"}]}"#;
    let keys: [Option<&[u8]>; 3] = [Some(b"order-1"), Some(&[0xFF, 0xFE]), None];
    let messages = || {
        keys.iter()
            .enumerate()
            .map(|(o, key)| {
                let mut m = message(
                    o,
                    1,
                    &body(K, record(vec![("id", Avro::Int(o as i32))])),
                    Some(timestamp_ms(o)),
                );
                m.key = key.map(<[u8]>::to_vec);
                m
            })
            .collect::<Vec<_>>()
    };
    let delta = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("kafka_key", Delta::STRING, true),
    ])
    .unwrap();
    let mut c = kdi_conformer(arrow_of(&delta), TEXT, &[(1, K)]);
    let (good, rejects) = through(&mut c, decode_messages(&[(1, K)], messages()).await);
    assert_eq!(shown(&good, "kafka_key"), ["order-1", "NULL"]);
    assert_eq!(rejected_offsets(&rejects), [1]);

    let renamed = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("k", Delta::STRING, true),
    ])
    .unwrap();
    let (_, good, rejects) = through_sql(
        "SELECT id, kafka_key AS k FROM source",
        &[(1, K)],
        decode_messages(&[(1, K)], messages()).await,
        arrow_of(&renamed),
        TEXT,
    )
    .await;
    assert!(rejects.is_empty());
    assert_eq!(
        shown(&good, "k"),
        ["[111,114,100,101,114,45,49]", "[255,254]", "NULL"]
    );
}

/// A record or map written whole into a STRING has its keys in byte order, as serde_json's
/// `BTreeMap` held them: upper case before `_` before lower case, `b10` before `b2`, whatever
/// the writer schema's or the producer's order. A null field is `null` in it, as a key kdi had.
#[tokio::test]
async fn sorted_json_keys() {
    const S: &str = r#"{"type":"record","name":"S","fields":[
        {"name":"id","type":"int"},
        {"name":"rec","type":{"type":"record","name":"SR","fields":[
            {"name":"zeta","type":"int"},
            {"name":"alpha","type":"string"},
            {"name":"b2","type":"int"},
            {"name":"Alpha","type":"string"},
            {"name":"_x","type":["null","int"]},
            {"name":"b10","type":"int"}]}},
        {"name":"m","type":{"type":"map","values":"int"}}]}"#;
    let row = record(vec![
        ("id", Avro::Int(0)),
        (
            "rec",
            record(vec![
                ("zeta", Avro::Int(1)),
                ("alpha", s("a")),
                ("b2", Avro::Int(2)),
                ("Alpha", s("A")),
                ("_x", union(0, Avro::Null)),
                ("b10", Avro::Int(10)),
            ]),
        ),
        (
            "m",
            Avro::Map(
                [("b", 1), ("a", 2), ("B", 3), ("é", 4), ("_", 5)]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), Avro::Int(v)))
                    .collect(),
            ),
        ),
    ]);
    let b = body(S, row);
    let delta = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("rec", Delta::STRING, true),
        field("m", Delta::STRING, true),
    ])
    .unwrap();
    let mut c = kdi_conformer(arrow_of(&delta), TEXT, &[(1, S)]);
    let (good, _) = through(&mut c, decode(&[(1, S)], vec![(1, b.clone())]).await);
    let rec = r#"{"Alpha":"A","_x":null,"alpha":"a","b10":10,"b2":2,"zeta":1}"#;
    let m = r#"{"B":3,"_":5,"a":2,"b":1,"é":4}"#;
    assert_eq!(shown(&good, "rec"), [rec]);
    assert_eq!(shown(&good, "m"), [m]);
    let kdi = kdi_wrote(&[(S, &b, Some(timestamp_ms(0)))], &delta);
    let kdi = std::slice::from_ref(&kdi);
    assert_eq!(
        (shown(kdi, "rec"), shown(kdi, "m")),
        (vec![rec.to_string()], vec![m.to_string()])
    );
}

/// A NaN or an infinity has no JSON number. apache-avro refused to make JSON of a message
/// holding one (types.rs:307-312), and kdi dropped the message; under `kdi_compat` ddi sets the
/// row aside when such a value goes into a STRING, at the top level or inside a record written
/// whole, naming the column. Into a DOUBLE column it is written as it is (kdi dropped that
/// message too, §6.4). Without `kdi_compat` a STRING gets Arrow's cast, `NaN`.
#[tokio::test]
async fn nan_float_rejected_under_kdi_compat() {
    const F: &str = r#"{"type":"record","name":"F","fields":[
        {"name":"id","type":"int"},
        {"name":"f","type":"float"},
        {"name":"d","type":"double"},
        {"name":"rec","type":{"type":"record","name":"FR","fields":[{"name":"d","type":"double"}]}}]}"#;
    let row = |id: i32, f: f32, d: f64, rd: f64| {
        record(vec![
            ("id", Avro::Int(id)),
            ("f", Avro::Float(f)),
            ("d", Avro::Double(d)),
            ("rec", record(vec![("d", Avro::Double(rd))])),
        ])
    };
    let rows = [
        row(0, 1.5, 2.5, 3.5),
        row(1, 1.5, f64::NAN, 3.5),
        row(2, 1.5, 2.5, f64::INFINITY),
        row(3, f32::NEG_INFINITY, 2.5, 3.5),
    ];
    let bodies: Vec<(u32, Vec<u8>)> = rows.into_iter().map(|r| (1, body(F, r))).collect();
    let delta = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("f", Delta::STRING, true),
        field("d", Delta::STRING, true),
        field("rec", Delta::STRING, true),
    ])
    .unwrap();
    let mut c = kdi_conformer(arrow_of(&delta), TEXT, &[(1, F)]);
    let (good, rejects) = through(&mut c, decode(&[(1, F)], bodies.clone()).await);
    assert_eq!(shown(&good, "d"), ["2.5"]);
    assert_eq!(shown(&good, "rec"), [r#"{"d":3.5}"#]);
    assert_eq!(rejected_offsets(&rejects), [1, 2, 3]);
    let why = reasons(&rejects);
    assert!(why[0].starts_with("d: NaN is not a JSON number"), "{why:?}");
    assert!(
        why[1].starts_with("rec: inf is not a JSON number"),
        "{why:?}"
    );
    assert!(
        why[2].starts_with("f: -inf is not a JSON number"),
        "{why:?}"
    );
    assert_eq!(
        rejects[0].columns,
        [Some("d".into()), Some("rec".into()), Some("f".into())]
    );
    // kdi dropped every one of those messages.
    for (_, b) in &bodies[1..] {
        let schema = apache_avro::Schema::parse_str(F).unwrap();
        assert!(kdi::deserialize(&schema, b).is_err());
    }

    // Into a DOUBLE: written.
    let typed = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("d", Delta::DOUBLE, true),
    ])
    .unwrap();
    let mut c = kdi_conformer(arrow_of(&typed), TEXT, &[(1, F)]);
    let (good, rejects) = through(&mut c, decode(&[(1, F)], bodies.clone()).await);
    assert!(rejects.is_empty());
    assert_eq!(shown(&good, "d")[1], "NaN");

    // Without kdi_compat: Arrow's cast.
    let only_null_text = KdiText {
        compat: false,
        null_text: true,
    };
    let floats = StructType::try_new(vec![
        field("id", Delta::INTEGER, true),
        field("d", Delta::STRING, true),
    ])
    .unwrap();
    let mut c = kdi_conformer(arrow_of(&floats), only_null_text, &[(1, F)]);
    let (good, rejects) = through(&mut c, decode(&[(1, F)], bodies).await);
    assert!(rejects.is_empty());
    assert_eq!(shown(&good, "d"), ["2.5", "NaN", "2.5", "2.5"]);
}
