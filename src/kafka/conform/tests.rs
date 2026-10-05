//! docs/kafka-design.md §9.1 "M2, conform": the mapping into the target and its guards (§5.6),
//! the union `U` of the SQL path (§5.5), and the partition-column check (§5.6 step 0).
//!
//! Rows come from Avro messages written with apache-avro 0.21.0 and decoded by the decoder under
//! test, as a Kafka pipeline's rows do, except where a test needs a shape only SQL makes. Targets
//! are Delta schemas converted to Arrow by the Delta kernel, as `Pipeline::open` reads them, so
//! their container names and nullability are the ones a real table has. No broker; the one
//! registry is the test stub.

use super::*;
use crate::error::KafkaClass;
use crate::kafka::decode::{KafkaDecoder, KafkaMessage, KafkaTimestamp};
use crate::kafka::registry::{Lookup, WriterSchemas};
use crate::schema::SchemaCoercer;
use apache_avro::types::Value as Avro;
use async_trait::async_trait;
use deltalake::arrow::array::{Int32Array, Int64Array, StringArray};
use deltalake::arrow::datatypes::{Int64Type, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::kernel::{ArrayType, DataType as Delta, MapType, StructField, StructType};

// ------------------------------------------------------------------------------ fixtures

/// Writer schemas by id, for the decoder.
pub(super) struct Schemas(pub(super) HashMap<u32, Arc<RegisteredSchema>>);

#[async_trait]
impl WriterSchemas for Schemas {
    async fn writer_schema(&self, id: u32) -> Result<Lookup> {
        Ok(match self.0.get(&id) {
            Some(s) => Lookup::Avro(s.clone()),
            None => Lookup::Undecodable {
                reason: format!("schema id {id} is not in the Schema Registry"),
                deterministic: false,
            },
        })
    }
}

pub(super) fn registered(id: u32, json: &str) -> Arc<RegisteredSchema> {
    Arc::new(RegisteredSchema {
        id,
        schema: json.to_string(),
        schema_type: None,
        references: Vec::new(),
        subject: None,
        version: None,
    })
}

/// `rows` as messages at offsets 0.. of partition 0, each `(writer schema id, value)` written by
/// apache-avro behind the Confluent header, and decoded: one batch per writer schema.
pub(super) async fn decoded(schemas: &[(u32, &str)], rows: Vec<(u32, Avro)>) -> Vec<RecordBatch> {
    let map = schemas
        .iter()
        .map(|(id, json)| (*id, registered(*id, json)))
        .collect();
    let mut d = KafkaDecoder::new("orders", "kafka_", Arc::new(Schemas(map)));
    for (offset, (id, value)) in rows.into_iter().enumerate() {
        let json = schemas.iter().find(|(i, _)| *i == id).unwrap().1;
        let schema = apache_avro::Schema::parse_str(json).unwrap();
        let mut payload = vec![0u8];
        payload.extend(id.to_be_bytes());
        payload.extend(apache_avro::to_avro_datum(&schema, value).unwrap());
        d.feed(KafkaMessage {
            partition: 0,
            offset: offset as i64,
            timestamp: KafkaTimestamp::CreateTime(1_700_000_000_000 + offset as i64),
            key: None,
            value: Some(payload),
            headers: Vec::new(),
        })
        .await
        .unwrap();
    }
    let out = d.finish().unwrap();
    assert!(out.undecodable.is_empty(), "{:?}", out.undecodable);
    out.batches
}

pub(super) fn record(fields: Vec<(&str, Avro)>) -> Avro {
    Avro::Record(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

pub(super) fn s(v: &str) -> Avro {
    Avro::String(v.to_string())
}

/// A Delta table's schema as the kernel gives it to Arrow, which is what `Pipeline::open` reads
/// a target's as.
pub(super) fn target(fields: Vec<StructField>) -> SchemaRef {
    let schema: Schema = (&StructType::try_new(fields).unwrap())
        .try_into_arrow()
        .unwrap();
    Arc::new(schema)
}

pub(super) fn field(name: &str, t: impl std::convert::Into<Delta>, nullable: bool) -> StructField {
    StructField::new(name, t, nullable)
}

pub(super) fn strct(fields: Vec<StructField>) -> Delta {
    Delta::try_struct_type(fields).unwrap()
}

fn conformer(target: SchemaRef) -> Conformer {
    Conformer::new(target, "kafka_", KdiText::default()).unwrap()
}

/// What `Pipeline::process` does with a Kafka batch on its way to the target: conform, then the
/// coercer, rows that do not fit to the data-quality table.
pub(super) fn through(
    c: &mut Conformer,
    batches: Vec<RecordBatch>,
) -> (Vec<RecordBatch>, Vec<Rejected>) {
    let mut rejects = Vec::new();
    let conformed = c.conform_all(batches, &mut rejects, true).unwrap();
    let coercer = SchemaCoercer::new(c.target.clone());
    let mut good = Vec::new();
    for b in &conformed {
        let split = coercer.coerce_quarantining(b).unwrap();
        rejects.extend(split.bad);
        good.push(split.good);
    }
    (good, rejects)
}

/// Rows as JSON objects, nulls written out.
pub(super) fn json(batches: &[RecordBatch]) -> Vec<serde_json::Value> {
    use deltalake::arrow::json::writer::{ArrayWriter, WriterBuilder};
    let mut out = Vec::new();
    for b in batches {
        let mut w: ArrayWriter<Vec<u8>> = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build(Vec::new());
        w.write(b).unwrap();
        w.finish().unwrap();
        let buf = w.into_inner();
        if !buf.is_empty() {
            let v: Vec<serde_json::Value> = serde_json::from_slice(&buf).unwrap();
            out.extend(v);
        }
    }
    out
}

pub(super) fn reasons(rejects: &[Rejected]) -> Vec<String> {
    rejects.iter().flat_map(|r| r.reasons.clone()).collect()
}

/// The `kafka_offset` of every rejected row, in order.
pub(super) fn rejected_offsets(rejects: &[Rejected]) -> Vec<i64> {
    rejects
        .iter()
        .flat_map(|r| {
            r.rows
                .column_by_name("kafka_offset")
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect()
}

// ------------------------------------------------------------------------------- mapping

const ORDER_ADDRESS: &str = r#"{"type":"record","name":"Order","fields":[
    {"name":"id","type":"long"},
    {"name":"address","type":{"type":"record","name":"Address","fields":[
        {"name":"city","type":"string"},
        {"name":"street","type":"string"},
        {"name":"floor","type":"int"}]}}]}"#;

/// A record goes into a struct column child by child, each by its exact name, whatever the order
/// on either side: a child the target lacks is dropped, one the record lacks is null, and a child
/// of another type is cast where it sits. Arrow's own cast, which the coercer would reach, pairs
/// the children by position once a target child is missing from the record
/// (arrow-cast cast/mod.rs:2223-2252), and writes the city into the street column: §0.3 fact 1.
#[tokio::test]
async fn nested_struct_by_name() {
    let batches = decoded(
        &[(1, ORDER_ADDRESS)],
        vec![
            (
                1,
                record(vec![
                    ("id", Avro::Long(1)),
                    (
                        "address",
                        record(vec![
                            ("city", s("Berlin")),
                            ("street", s("Unter den Linden")),
                            ("floor", Avro::Int(3)),
                        ]),
                    ),
                ]),
            ),
            (
                1,
                record(vec![
                    ("id", Avro::Long(2)),
                    (
                        "address",
                        record(vec![
                            ("city", s("Paris")),
                            ("street", s("Rue de Rivoli")),
                            ("floor", Avro::Int(-1)),
                        ]),
                    ),
                ]),
            ),
        ],
    )
    .await;
    let address = strct(vec![
        field("street", Delta::STRING, true),
        field("zip", Delta::STRING, true),
        field("floor", Delta::LONG, true),
    ]);
    let t = target(vec![
        field("id", Delta::LONG, false),
        field("address", address, true),
    ]);
    let mut c = conformer(t.clone());
    let (good, rejects) = through(&mut c, batches.clone());
    assert!(rejects.is_empty(), "{:?}", reasons(&rejects));
    assert_eq!(
        json(&good),
        vec![
            serde_json::json!({"id": 1, "address": {"street": "Unter den Linden", "zip": null, "floor": 3}}),
            serde_json::json!({"id": 2, "address": {"street": "Rue de Rivoli", "zip": null, "floor": -1}}),
        ]
    );
    // The coercer passed the conformed column through as it was: its type is the target's.
    let conformed = c.conform(&batches[0]).unwrap().0;
    assert_eq!(
        conformed.column_by_name("address").unwrap().data_type(),
        t.field_with_name("address").unwrap().data_type()
    );

    // Arrow's cast of the same column, by position: the city lands in `street`.
    let raw = batches[0].column_by_name("address").unwrap();
    let two = DataType::Struct(Fields::from(vec![
        Field::new("street", DataType::Utf8, true),
        Field::new("zip", DataType::Utf8, true),
        Field::new("floor", DataType::Int64, true),
    ]));
    let positional = deltalake::arrow::compute::cast(raw, &two).unwrap();
    let street = positional
        .as_struct()
        .column(0)
        .as_string::<i32>()
        .value(0)
        .to_string();
    assert_eq!(street, "Berlin", "arrow's cast pairs by position here");
}

/// SQL builds lists and maps with Arrow's names (`item`, `entries` with `keys`/`values`), a Delta
/// target has the kernel's (`element`, `key_value` with `key`/`value`): the containers are
/// rebuilt with the target's names and its element and value fields, and a struct inside a list
/// is matched by name as well. The conformed column has exactly the target's type, so the coercer
/// casts none of it.
#[test]
fn list_and_map_renamed_to_target() {
    // [{b: 1, a: "x"}, {b: 2, a: "y"}] and [] ; {"k": 5} and null
    let item_fields = Fields::from(vec![
        Field::new("b", DataType::Int64, true),
        Field::new("a", DataType::Utf8, true),
    ]);
    let items = StructArray::try_new(
        item_fields.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
        None,
    )
    .unwrap();
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Struct(item_fields), true)),
        OffsetBuffer::from_lengths([2, 0]),
        Arc::new(items),
        None,
    )
    .unwrap();
    let entry_fields = Fields::from(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Int32, true),
    ]);
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["k"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![5])),
        ],
        None,
    )
    .unwrap();
    let map = MapArray::try_new(
        Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
        OffsetBuffer::from_lengths([1, 0]),
        entries,
        Some(vec![true, false].into()),
        false,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("lines", list.data_type().clone(), true),
            Field::new("attrs", map.data_type().clone(), true),
        ])),
        vec![Arc::new(list), Arc::new(map)],
    )
    .unwrap();

    let line = strct(vec![
        field("a", Delta::STRING, true),
        field("b", Delta::LONG, true),
    ]);
    let t = target(vec![
        field("lines", ArrayType::new(line, true), true),
        field(
            "attrs",
            MapType::new(Delta::STRING, Delta::LONG, true),
            true,
        ),
    ]);
    let mut c = conformer(t.clone());
    let (out, rejects) = c.conform(&batch).unwrap();
    assert!(rejects.is_none());
    for name in ["lines", "attrs"] {
        assert_eq!(
            out.column_by_name(name).unwrap().data_type(),
            t.field_with_name(name).unwrap().data_type(),
            "{name}"
        );
    }
    let DataType::List(element) = out.column_by_name("lines").unwrap().data_type() else {
        panic!()
    };
    assert_eq!(element.name(), "element");
    let DataType::Map(entries, _) = out.column_by_name("attrs").unwrap().data_type() else {
        panic!()
    };
    assert_eq!(entries.name(), "key_value");

    let coerced = SchemaCoercer::new(t).coerce_quarantining(&out).unwrap();
    assert!(coerced.bad.is_none());
    assert_eq!(
        json(&[coerced.good]),
        vec![
            serde_json::json!({"lines": [{"a": "x", "b": 1}, {"a": "y", "b": 2}], "attrs": {"k": 5}}),
            serde_json::json!({"lines": [], "attrs": null}),
        ]
    );
}

const V1: &str = r#"{"type":"record","name":"Order","fields":[
    {"name":"id","type":"long"},
    {"name":"name","type":"string"},
    {"name":"legacy","type":"string"}]}"#;

const V2: &str = r#"{"type":"record","name":"Order","fields":[
    {"name":"id","type":"long"},
    {"name":"note","type":["null","string"],"default":null},
    {"name":"name","type":"string"}]}"#;

/// Two writer versions in one batch: one has a field the other dropped, the other one it added.
/// Each row's own fields land in their columns, and the columns its version lacks are null.
#[tokio::test]
async fn field_added_and_removed() {
    let batches = decoded(
        &[(1, V1), (2, V2)],
        vec![
            (
                1,
                record(vec![
                    ("id", Avro::Long(1)),
                    ("name", s("a")),
                    ("legacy", s("old")),
                ]),
            ),
            (
                2,
                record(vec![
                    ("id", Avro::Long(2)),
                    ("note", Avro::Union(1, Box::new(s("new")))),
                    ("name", s("b")),
                ]),
            ),
            (
                1,
                record(vec![
                    ("id", Avro::Long(3)),
                    ("name", s("c")),
                    ("legacy", s("older")),
                ]),
            ),
        ],
    )
    .await;
    assert_eq!(batches.len(), 2, "one batch per writer schema");
    let t = target(vec![
        field("id", Delta::LONG, false),
        field("name", Delta::STRING, true),
        field("note", Delta::STRING, true),
        field("legacy", Delta::STRING, true),
    ]);
    let mut c = conformer(t);
    let (good, rejects) = through(&mut c, batches);
    assert!(rejects.is_empty());
    let mut rows = json(&good);
    rows.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"id": 1, "name": "a", "note": null, "legacy": "old"}),
            serde_json::json!({"id": 2, "name": "b", "note": "new", "legacy": null}),
            serde_json::json!({"id": 3, "name": "c", "note": null, "legacy": "older"}),
        ]
    );
    // The gauge says what the last batch lacked: version 2's rows have no `legacy`.
    assert_eq!(c.unmatched_target_columns(), ["legacy"]);
}

const UNION: &str = r#"{"type":"record","name":"Reading","fields":[
    {"name":"id","type":"long"},
    {"name":"v","type":["null","int","string",{"type":"record","name":"Box","fields":[{"name":"x","type":"int"}]}]},
    {"name":"w","type":["int","string",{"type":"record","name":"Box2","fields":[{"name":"x","type":"int"}]}]}]}"#;

/// A general union into a plain column takes the branch each row holds, cast to the column's
/// type: an int as it is, a string that parses, null as null. A branch that does not convert, a
/// string that does not parse or a record into a number, sets its row aside with the branch named.
#[tokio::test]
async fn union_branch_to_target() {
    let boxed = |x: i32| record(vec![("x", Avro::Int(x))]);
    let rows = vec![
        (
            Avro::Union(1, Box::new(Avro::Int(5))),
            Avro::Union(0, Box::new(Avro::Int(5))),
        ),
        (
            Avro::Union(2, Box::new(s("12"))),
            Avro::Union(1, Box::new(s("twelve"))),
        ),
        (
            Avro::Union(0, Box::new(Avro::Null)),
            Avro::Union(0, Box::new(Avro::Int(-1))),
        ),
        (
            Avro::Union(2, Box::new(s("x"))),
            Avro::Union(0, Box::new(Avro::Int(0))),
        ),
        (
            Avro::Union(3, Box::new(boxed(1))),
            Avro::Union(0, Box::new(Avro::Int(0))),
        ),
        (
            Avro::Union(1, Box::new(Avro::Int(6))),
            Avro::Union(2, Box::new(boxed(2))),
        ),
    ];
    let rows = rows
        .into_iter()
        .enumerate()
        .map(|(i, (v, w))| {
            (
                1,
                record(vec![("id", Avro::Long(i as i64)), ("v", v), ("w", w)]),
            )
        })
        .collect();
    let batches = decoded(&[(1, UNION)], rows).await;
    assert!(matches!(
        batches[0].column_by_name("v").unwrap().data_type(),
        DataType::Union(..)
    ));
    let t = target(vec![
        field("id", Delta::LONG, false),
        field("v", Delta::LONG, true),
        field("w", Delta::STRING, true),
    ]);
    let mut c = conformer(t);
    let (good, rejects) = through(&mut c, batches);
    assert_eq!(
        json(&good),
        vec![
            serde_json::json!({"id": 0, "v": 5, "w": "5"}),
            serde_json::json!({"id": 1, "v": 12, "w": "twelve"}),
            serde_json::json!({"id": 2, "v": null, "w": "-1"}),
        ]
    );
    assert_eq!(rejected_offsets(&rejects), [3, 4, 5]);
    let why = reasons(&rejects);
    assert!(why[0].contains("v (union branch string)"), "{why:?}");
    assert!(why[0].contains("Utf8 to Int64"), "{why:?}");
    assert!(why[1].contains("v (union branch Box)"), "{why:?}");
    assert!(why[2].contains("w (union branch Box2)"), "{why:?}");
    assert_eq!(
        rejects[0].columns,
        [Some("v".into()), Some("v".into()), Some("w".into())]
    );
}

/// A float goes into a decimal as kafka-delta-ingest read one: its shortest round-trip text, read
/// by `parse_decimal`, which keeps the digits the scale has room for (arrow-json 55
/// decimal_array.rs:80-90 did `f64::to_string` and then that). Arrow's cast multiplies and
/// rounds instead, and the two differ: 0.125 is 0.12 here and 0.13 there; 1.1 at scale 18 is
/// exactly 1.1 here and carries the double's binary error there. A float is widened to a double
/// first, as serde_json held it. NaN, the infinities and a value too large for the decimal are
/// set aside; and the same holds inside a list.
#[tokio::test]
async fn double_to_decimal_via_shortest_text() {
    const FLOATS: &str = r#"{"type":"record","name":"Price","fields":[
        {"name":"id","type":"long"},
        {"name":"d2","type":"double"},
        {"name":"d18","type":"double"},
        {"name":"f18","type":"float"},
        {"name":"d0","type":"double"},
        {"name":"ds","type":{"type":"array","items":"double"}}]}"#;
    let row = |id: i64, d2: f64, d18: f64, f18: f32, d0: f64, ds: Vec<f64>| {
        (
            1,
            record(vec![
                ("id", Avro::Long(id)),
                ("d2", Avro::Double(d2)),
                ("d18", Avro::Double(d18)),
                ("f18", Avro::Float(f18)),
                ("d0", Avro::Double(d0)),
                (
                    "ds",
                    Avro::Array(ds.into_iter().map(Avro::Double).collect()),
                ),
            ]),
        )
    };
    let batches = decoded(
        &[(1, FLOATS)],
        vec![
            row(0, 0.125, 1.1, 0.1, 12.5, vec![0.125, -0.999]),
            row(1, f64::NAN, 0.0, 0.0, 0.0, vec![]),
            row(2, 1e300, 0.0, 0.0, 0.0, vec![]),
            row(3, -1.0, -0.0, f32::INFINITY, 0.0, vec![]),
            row(4, 1.0, 1.0, 1.0, 1.0, vec![1e9]),
        ],
    )
    .await;
    let t = target(vec![
        field("id", Delta::LONG, false),
        field("d2", Delta::decimal(10, 2).unwrap(), true),
        field("d18", Delta::decimal(38, 18).unwrap(), true),
        field("f18", Delta::decimal(38, 18).unwrap(), true),
        field("d0", Delta::decimal(5, 0).unwrap(), true),
        field(
            "ds",
            ArrayType::new(Delta::decimal(10, 2).unwrap(), true),
            true,
        ),
    ]);
    let mut c = conformer(t);
    let (good, rejects) = through(&mut c, batches.clone());
    assert_eq!(rejected_offsets(&rejects), [1, 2, 3, 4]);
    let why = reasons(&rejects);
    assert!(
        why[0].starts_with("d2: the value does not convert from Float64"),
        "{why:?}"
    );
    assert!(why[2].starts_with("f18:"), "{why:?}");
    assert!(
        why[3].starts_with("ds[]:"),
        "a list element too large: {why:?}"
    );
    let d = |name: &str| {
        good[0]
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Decimal128Type>()
            .value_as_string(0)
    };
    assert_eq!(d("d2"), "0.12");
    assert_eq!(d("d18"), "1.100000000000000000");
    // The float 0.1 is the double 0.10000000149011612, as serde_json held it.
    assert_eq!(d("f18"), "0.100000001490116120");
    // kafka-delta-ingest's arrow 55 `parse_decimal` read "12.5" at scale 0 as 125: it took
    // fraction digits on while `scale == 0` (arrow-cast 55.0.0 parse.rs:919,
    // `fractionals == scale && scale != 0`). ddi's arrow 58 keeps the integer part, as at every
    // other scale (arrow-cast 58.4.0 parse.rs:914).
    assert_eq!(d("d0"), "12");
    let ds = good[0]
        .column_by_name("ds")
        .unwrap()
        .as_list::<i32>()
        .value(0);
    let ds = ds.as_primitive::<Decimal128Type>();
    assert_eq!(
        (ds.value_as_string(0), ds.value_as_string(1)),
        ("0.12".to_string(), "-0.99".to_string())
    );

    // Arrow's cast of the same doubles rounds, and multiplies in binary.
    let cast = |col: &str, t: &DataType| {
        let a =
            deltalake::arrow::compute::cast(batches[0].column_by_name(col).unwrap(), t).unwrap();
        a.as_primitive::<Decimal128Type>().value_as_string(0)
    };
    assert_eq!(cast("d2", &DataType::Decimal128(10, 2)), "0.13");
    assert_ne!(
        cast("d18", &DataType::Decimal128(38, 18)),
        "1.100000000000000000"
    );
}

// -------------------------------------------------------------------------------- guards

const CUSTOMER: &str = r#"{"type":"record","name":"Customer","fields":[
    {"name":"id","type":"long"},
    {"name":"customerId","type":"string"},
    {"name":"address","type":{"type":"record","name":"A","fields":[{"name":"Zip","type":"string"}]}}]}"#;

/// A nullable target column nothing produces is written null, and said so: a warning once per
/// writer schema id (also for rows that do not name theirs, when SQL dropped
/// `kafka_value_schema_id`), and the gauge's count. A NOT NULL one is the coercer's to refuse,
/// as for any source.
#[tokio::test]
async fn unmatched_target_column_warns() {
    let rows = |id: i64| {
        record(vec![
            ("id", Avro::Long(id)),
            ("customerId", s("c")),
            ("address", record(vec![("Zip", s("10115"))])),
        ])
    };
    let batches = decoded(&[(1, CUSTOMER)], vec![(1, rows(1)), (1, rows(2))]).await;
    let t = target(vec![
        field("id", Delta::LONG, false),
        field("email", Delta::STRING, true),
    ]);
    let mut c = conformer(t.clone());
    let (good, _) = through(&mut c, batches.clone());
    assert_eq!(json(&good)[0], serde_json::json!({"id": 1, "email": null}));
    assert_eq!(c.unmatched_target_columns(), ["email"]);
    assert_eq!(c.warned(), &BTreeSet::from([Some(1)]));

    // The same writer schema again: nothing new to warn about.
    through(&mut c, batches.clone());
    assert_eq!(c.warned(), &BTreeSet::from([Some(1)]));

    // Rows without their writer schema id, as SQL that dropped the column leaves them.
    let anonymous = batches[0].project(&[0]).unwrap();
    through(&mut c, vec![anonymous]);
    assert_eq!(c.warned(), &BTreeSet::from([None, Some(1)]));

    // A batch that has every column clears the gauge.
    let full = target(vec![field("id", Delta::LONG, false)]);
    let mut c = conformer(full);
    through(&mut c, batches.clone());
    assert!(c.unmatched_target_columns().is_empty());

    // NOT NULL and nowhere: not counted here, and the coercer refuses the batch.
    let strict = target(vec![
        field("id", Delta::LONG, false),
        field("email", Delta::STRING, false),
    ]);
    let mut c = conformer(strict.clone());
    let mut rejects = Vec::new();
    let out = c.conform_all(batches, &mut rejects, true).unwrap();
    assert!(c.unmatched_target_columns().is_empty());
    let err = SchemaCoercer::new(strict)
        .coerce_quarantining(&out[0])
        .unwrap_err()
        .to_string();
    assert!(err.contains("\"email\""), "{err}");
}

/// Names match exactly, case included, at the top and inside a struct, as kafka-delta-ingest's
/// arrow-json and ddi's coercer match them: `customerId` does not fill `customerid`, nor `Zip`
/// `zip`. The warning names the near miss.
#[tokio::test]
async fn case_mismatch_not_matched() {
    let batches = decoded(
        &[(1, CUSTOMER)],
        vec![(
            1,
            record(vec![
                ("id", Avro::Long(1)),
                ("customerId", s("c-1")),
                ("address", record(vec![("Zip", s("10115"))])),
            ]),
        )],
    )
    .await;
    let t = target(vec![
        field("id", Delta::LONG, false),
        field("customerid", Delta::STRING, true),
        field(
            "address",
            strct(vec![field("zip", Delta::STRING, true)]),
            true,
        ),
    ]);
    let mut c = conformer(t);
    let (good, rejects) = through(&mut c, batches.clone());
    assert!(rejects.is_empty());
    assert_eq!(
        json(&good),
        vec![serde_json::json!({"id": 1, "customerid": null, "address": {"zip": null}})]
    );
    assert_eq!(c.unmatched_target_columns(), ["customerid"]);
    assert_eq!(
        case_only(&batches[0].schema(), c.unmatched_target_columns()),
        ["customerid (the rows have customerId)"]
    );
}

/// §5.6 step 2's "verify (M2)": a NOT NULL child inside a struct, or a NOT NULL list element,
/// that a row has no value for. The design expected the coercer to quarantine the row; it does
/// not: it casts a nested column strictly (schema.rs:161-173) and refuses the whole batch. So the
/// conform step sets the row aside itself, with the field named, and the coercer never meets it.
/// Under a null parent nothing is missing: that row is written, its struct null.
#[tokio::test]
async fn a_missing_not_null_child_rejects_its_row() {
    const W: &str = r#"{"type":"record","name":"Order","fields":[
        {"name":"id","type":"long"},
        {"name":"customer","type":["null",{"type":"record","name":"C","fields":[
            {"name":"name","type":"string"}]}]},
        {"name":"tags","type":{"type":"array","items":["null","string"]}}]}"#;
    let batches = decoded(
        &[(1, W)],
        vec![
            (
                1,
                record(vec![
                    ("id", Avro::Long(0)),
                    (
                        "customer",
                        Avro::Union(1, Box::new(record(vec![("name", s("a"))]))),
                    ),
                    ("tags", Avro::Array(vec![Avro::Union(1, Box::new(s("t")))])),
                ]),
            ),
            (
                1,
                record(vec![
                    ("id", Avro::Long(1)),
                    ("customer", Avro::Union(0, Box::new(Avro::Null))),
                    ("tags", Avro::Array(vec![])),
                ]),
            ),
            (
                1,
                record(vec![
                    ("id", Avro::Long(2)),
                    ("customer", Avro::Union(0, Box::new(Avro::Null))),
                    (
                        "tags",
                        Avro::Array(vec![Avro::Union(0, Box::new(Avro::Null))]),
                    ),
                ]),
            ),
        ],
    )
    .await;
    let customer = strct(vec![
        field("name", Delta::STRING, true),
        field("customer_id", Delta::LONG, false),
    ]);
    let customer_only = target(vec![
        field("id", Delta::LONG, false),
        field("customer", customer.clone(), true),
    ]);
    let mut c = conformer(customer_only.clone());
    let (good, rejects) = through(&mut c, batches.clone());
    assert_eq!(rejected_offsets(&rejects), [0]);
    assert_eq!(
        reasons(&rejects),
        [
            "customer.customer_id is NOT NULL in the target, and nothing produces it (the row's \
          writer schema has no such field)"
        ]
    );
    assert_eq!(
        json(&good),
        vec![
            serde_json::json!({"id": 1, "customer": null}),
            serde_json::json!({"id": 2, "customer": null}),
        ]
    );

    let tags = target(vec![
        field("id", Delta::LONG, false),
        field("tags", ArrayType::new(Delta::STRING, false), true),
    ]);
    let mut c = conformer(tags);
    let (good, rejects) = through(&mut c, batches.clone());
    assert_eq!(rejected_offsets(&rejects), [2]);
    assert_eq!(
        reasons(&rejects),
        ["tags[] is NOT NULL in the target, and this value is null"]
    );
    assert_eq!(good.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    // The coercer alone, given a struct whose NOT NULL child it has to fill: the batch fails.
    let alone = SchemaCoercer::new(customer_only).coerce_quarantining(&batches[0]);
    assert!(alone.is_err(), "the coercer quarantines no nested row");
}

/// A local timestamp (no zone) into a TIMESTAMP column is read as UTC, the same number, as the
/// §5.6 table's "verify (M2)" says; a timestamp-millis is the same instant in microseconds. At
/// the top level the coercer casts it; inside a struct the conform step does, by the same cast.
#[tokio::test]
async fn local_timestamp_read_as_utc() {
    const T: &str = r#"{"type":"record","name":"E","fields":[
        {"name":"local","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"millis","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"inner","type":{"type":"record","name":"I","fields":[
            {"name":"local","type":{"type":"long","logicalType":"local-timestamp-millis"}}]}}]}"#;
    const PHYSICAL: &str = r#"{"type":"record","name":"E","fields":[
        {"name":"local","type":"long"},
        {"name":"millis","type":"long"},
        {"name":"inner","type":{"type":"record","name":"I","fields":[
            {"name":"local","type":"long"}]}}]}"#;
    // Written in the physical schema, decoded in the logical one, as a producer's bytes are.
    let schema = apache_avro::Schema::parse_str(PHYSICAL).unwrap();
    let mut payload = vec![0u8, 0, 0, 0, 1];
    payload.extend(
        apache_avro::to_avro_datum(
            &schema,
            record(vec![
                ("local", Avro::Long(1_700_000_000_123_456)),
                ("millis", Avro::Long(1_700_000_000_123)),
                (
                    "inner",
                    record(vec![("local", Avro::Long(1_700_000_000_123))]),
                ),
            ]),
        )
        .unwrap(),
    );
    let mut d = KafkaDecoder::new(
        "t",
        "kafka_",
        Arc::new(Schemas(HashMap::from([(1, registered(1, T))]))),
    );
    d.feed(KafkaMessage {
        partition: 0,
        offset: 0,
        timestamp: KafkaTimestamp::NotAvailable,
        key: None,
        value: Some(payload),
        headers: Vec::new(),
    })
    .await
    .unwrap();
    let batches = d.finish().unwrap().batches;
    assert_eq!(
        batches[0].column_by_name("local").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    let t = target(vec![
        field("local", Delta::TIMESTAMP, true),
        field("millis", Delta::TIMESTAMP, true),
        field(
            "inner",
            strct(vec![field("local", Delta::TIMESTAMP, true)]),
            true,
        ),
    ]);
    let mut c = conformer(t);
    let (good, rejects) = through(&mut c, batches);
    assert!(rejects.is_empty());
    let micros = |a: &ArrayRef| {
        a.as_primitive::<deltalake::arrow::datatypes::TimestampMicrosecondType>()
            .value(0)
    };
    assert_eq!(
        micros(good[0].column_by_name("local").unwrap()),
        1_700_000_000_123_456
    );
    assert_eq!(
        micros(good[0].column_by_name("millis").unwrap()),
        1_700_000_000_123_000
    );
    let inner = good[0].column_by_name("inner").unwrap().as_struct();
    assert_eq!(micros(inner.column(0)), 1_700_000_000_123_000);
}

/// Without a data-quality table, a row that does not fit fails the batch, as the coercer's strict
/// cast does; nothing is set aside where nothing would keep it.
#[tokio::test]
async fn without_a_data_quality_table_a_row_that_does_not_fit_fails_the_batch() {
    let batches = decoded(
        &[(1, ORDER_ADDRESS)],
        vec![(
            1,
            record(vec![
                ("id", Avro::Long(1)),
                (
                    "address",
                    record(vec![
                        ("city", s("Berlin")),
                        ("street", s("x")),
                        ("floor", Avro::Int(3)),
                    ]),
                ),
            ]),
        )],
    )
    .await;
    let t = target(vec![field(
        "address",
        strct(vec![field("zip", Delta::STRING, false)]),
        true,
    )]);
    let mut c = conformer(t);
    let mut rejects = Vec::new();
    let err = c.conform_all(batches, &mut rejects, false).unwrap_err();
    assert!(matches!(err, Error::Schema(_)), "{err:?}");
    assert!(err.to_string().contains("address.zip is NOT NULL"), "{err}");
    assert!(rejects.is_empty());
}

// ----------------------------------------------------------------------------- union U

/// What `U` is built from, as `SubjectSchemas` gathers it, with the registry stub: the subject's
/// latest version (3), registered but not seen yet, and the id a message was decoded with (1).
/// Version 2 was never seen, and so is no member.
async fn latest_and_seen() -> (crate::kafka::sr_stub::Stub, Vec<Arc<RegisteredSchema>>) {
    use crate::config::kafka::{EnvRef, ResolvedSchemaRegistry};
    use crate::kafka::registry::{SchemaRegistry, SubjectSchemas};
    let body = |id: u32| {
        let json = match id {
            1 => {
                r#"{"type":"record","name":"O","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"legacy","type":"string"}]}"#
            }
            2 => {
                r#"{"type":"record","name":"O","fields":[{"name":"id","type":"long"},{"name":"name","type":"long"}]}"#
            }
            _ => {
                r#"{"type":"record","name":"O","fields":[{"name":"channel","type":["null","string"]},{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#
            }
        };
        format!(
            "{{\"subject\":\"orders-value\",\"version\":{id},\"id\":{id},\"schema\":{}}}",
            serde_json::to_string(json).unwrap()
        )
    };
    let stub = crate::kafka::sr_stub::Stub::start(move |seen| {
        let target = seen.target();
        if target.starts_with("/subjects/orders-value/versions/latest") {
            return (200, body(3));
        }
        let id: u32 = target
            .trim_start_matches("/schemas/ids/")
            .split('?')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        (200, body(id))
    })
    .await;
    let cfg = ResolvedSchemaRegistry {
        url: stub.url.clone(),
        username_env: EnvRef::Disabled,
        password_env: EnvRef::Disabled,
        proxy_env: EnvRef::Disabled,
        ca_pem_env: EnvRef::Disabled,
    };
    let registry =
        Arc::new(SchemaRegistry::with_cache(&cfg, &|_: &str| None, Arc::default()).unwrap());
    let mut subject = SubjectSchemas::open(registry.clone(), "orders-value")
        .await
        .unwrap();
    let Lookup::Avro(one) = registry.lookup(1).await.unwrap() else {
        panic!()
    };
    subject.observe(&one).await.unwrap();
    (stub, subject.members())
}

/// `U` is the latest registered version and the writer schemas seen, newest first: the latest's
/// fields in its order, then those only older members have, then the metadata columns. Every
/// field is nullable, since an older row may lack it.
#[tokio::test]
async fn union_u_latest_plus_seen() {
    let (stub, members) = latest_and_seen().await;
    let u = UnionSchema::build(&members, "kafka_");
    assert_eq!(u.members(), [3, 1]);
    let schema = u.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "channel",
            "id",
            "name",
            "legacy",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_timestamp_type",
            "kafka_key",
            "kafka_headers",
            "kafka_value_schema_id"
        ]
    );
    assert!(u.schema().fields().iter().all(|f| f.is_nullable()));
    // Version 2 was neither fetched nor taken in.
    assert!(
        !stub
            .seen()
            .iter()
            .any(|s| s.target().starts_with("/schemas/ids/2")),
        "{:?}",
        stub.seen()
    );
}

/// An old version nobody reads any more had another type for `name`. It was never seen, so it is
/// not in `U`: nothing conflicts, the SQL plans, and the pipeline starts (PO-M7).
#[tokio::test]
async fn union_u_unseen_old_conflict_ignored() {
    let (_stub, members) = latest_and_seen().await;
    let u = UnionSchema::build(&members, "kafka_");
    assert!(u.changes().is_empty(), "{:?}", u.changes());
    assert_eq!(
        u.schema().field_with_name("name").unwrap().data_type(),
        &DataType::Utf8
    );
    let sql = SqlTransform::new("SELECT id, upper(name) AS name, channel, legacy FROM source");
    let out = u.plan(&sql).await.unwrap();
    assert_eq!(out.field(1).data_type(), &DataType::Utf8);

    // Had it been seen, `U` would say how the type changed, and still take the newest.
    let two = registered(
        2,
        r#"{"type":"record","name":"O","fields":[{"name":"id","type":"long"},{"name":"name","type":"long"}]}"#,
    );
    let mut with_two = members.clone();
    with_two.push(two);
    let u = UnionSchema::build(&with_two, "kafka_");
    assert_eq!(
        u.changes(),
        [TypeChange {
            field: "name".into(),
            newer: (3, DataType::Utf8),
            older: (2, DataType::Int64),
        }]
    );
    assert_eq!(
        u.schema().field_with_name("name").unwrap().data_type(),
        &DataType::Utf8
    );
}

const Q1: &str = r#"{"type":"record","name":"Q","fields":[
    {"name":"id","type":"long"},
    {"name":"qty","type":"string"},
    {"name":"price","type":"float"},
    {"name":"address","type":{"type":"record","name":"A","fields":[{"name":"zip","type":"int"}]}}]}"#;

const Q2: &str = r#"{"type":"record","name":"Q","fields":[
    {"name":"id","type":"long"},
    {"name":"qty","type":"long"},
    {"name":"price","type":"double"},
    {"name":"address","type":{"type":"record","name":"A","fields":[
        {"name":"zip","type":"string"},{"name":"city","type":"string"}]}}]}"#;

/// A field's type in `U` is the newest writer schema's; an older row's value is cast to it, a
/// nested one too, and a value that does not convert sets its row aside with both writer schemas
/// named. The aligned batches share one schema, so the SQL runs over all of them at once.
#[tokio::test]
async fn union_u_newest_type_wins_old_rows_cast_or_rejected() {
    let q1 = |id: i64, qty: &str| {
        (
            1,
            record(vec![
                ("id", Avro::Long(id)),
                ("qty", s(qty)),
                ("price", Avro::Float(1.5)),
                ("address", record(vec![("zip", Avro::Int(10115))])),
            ]),
        )
    };
    let batches = decoded(
        &[(1, Q1), (2, Q2)],
        vec![
            q1(0, "12"),
            q1(1, "x"),
            (
                2,
                record(vec![
                    ("id", Avro::Long(2)),
                    ("qty", Avro::Long(7)),
                    ("price", Avro::Double(2.25)),
                    (
                        "address",
                        record(vec![("zip", s("75001")), ("city", s("Paris"))]),
                    ),
                ]),
            ),
        ],
    )
    .await;
    let u = UnionSchema::build(&[registered(1, Q1), registered(2, Q2)], "kafka_");
    assert_eq!(u.members(), [2, 1]);
    let types: Vec<(String, DataType)> = u
        .changes()
        .iter()
        .map(|c| (c.field.clone(), c.newer.1.clone()))
        .collect();
    assert_eq!(
        types,
        [
            ("qty".to_string(), DataType::Int64),
            ("price".to_string(), DataType::Float64),
            ("address.zip".to_string(), DataType::Utf8),
        ]
    );

    let mut rejects = Vec::new();
    let aligned = u.align_all(batches, &mut rejects).unwrap();
    assert!(aligned.iter().all(|b| b.schema() == u.schema()));
    assert_eq!(rejected_offsets(&rejects), [1]);
    assert_eq!(
        reasons(&rejects),
        [
            "writer schema 1 field qty: Utf8 does not convert to Int64 (its type in writer schema \
          2, the newest that has it)"
        ]
    );

    let sql = SqlTransform::new(
        "SELECT id, qty, price, address.zip AS zip, address.city AS city FROM source",
    );
    let out = sql.run(u.schema(), aligned).await.unwrap();
    let mut rows = json(&out);
    rows.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"id": 0, "qty": 12, "price": 1.5, "zip": "10115", "city": null}),
            serde_json::json!({"id": 2, "qty": 7, "price": 2.25, "zip": "75001", "city": "Paris"}),
        ]
    );
}

/// A newer writer schema may narrow a type (a double to a long, as FORWARD compatibility
/// allows; a double to a float; a decimal's scale or a timestamp's unit under NONE), and `U`
/// takes the newest type. An older row's value that the narrower type holds exactly is kept;
/// one it would change (a fraction truncated, digits or precision dropped, a double rounded to a
/// float) is rejected with its writer schema named, never written as another value, as was a
/// value it cannot hold at all.
#[tokio::test]
async fn union_u_never_changes_an_older_value() {
    const W1: &str = r#"{"type":"record","name":"N","fields":[
        {"name":"id","type":"long"},
        {"name":"qty","type":"double"},
        {"name":"rate","type":"double"},
        {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}},
        {"name":"ts","type":{"type":"long","logicalType":"timestamp-micros"}},
        {"name":"big","type":"long"}]}"#;
    const W2: &str = r#"{"type":"record","name":"N","fields":[
        {"name":"id","type":"long"},
        {"name":"qty","type":"long"},
        {"name":"rate","type":"float"},
        {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
        {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"big","type":"int"}]}"#;
    // FORWARD: a reader of W1 reads W2's longs and floats as doubles.
    let narrowing = |t1: &str, t2: &str| {
        let s = |t: &str| {
            apache_avro::Schema::parse_str(&format!(
                r#"{{"type":"record","name":"N","fields":[{{"name":"v","type":"{t}"}}]}}"#
            ))
            .unwrap()
        };
        apache_avro::schema_compatibility::SchemaCompatibility::can_read(&s(t2), &s(t1))
    };
    assert!(narrowing("double", "long").is_ok());
    assert!(narrowing("double", "float").is_ok());

    // A positive unscaled value in the fewest bytes of two's complement, as Java writes it.
    let decimal = |unscaled: i64| {
        let bytes = unscaled.to_be_bytes();
        let mut start = 0;
        while start < 7 && bytes[start] == 0 && bytes[start + 1] & 0x80 == 0 {
            start += 1;
        }
        Avro::Decimal(apache_avro::Decimal::from(bytes[start..].to_vec()))
    };
    // Exact in every narrower type: 2.0, 1.5, 1.230, a whole millisecond, 7.
    let v1 = |id: i64, change: &str| {
        let v = |field: &str, exact: Avro, changed: Avro| match field == change {
            true => (field.to_string(), changed),
            false => (field.to_string(), exact),
        };
        Avro::Record(vec![
            ("id".into(), Avro::Long(id)),
            v("qty", Avro::Double(2.0), Avro::Double(1.5)),
            v("rate", Avro::Double(1.5), Avro::Double(0.1)),
            v("amt", decimal(1230), decimal(1235)),
            v(
                "ts",
                Avro::TimestampMicros(1_700_000_000_123_000),
                Avro::TimestampMicros(1_700_000_000_123_456),
            ),
            v("big", Avro::Long(7), Avro::Long(1 << 40)),
        ])
    };
    let batches = decoded(
        &[(1, W1), (2, W2)],
        vec![
            (1, v1(0, "")),
            (1, v1(1, "qty")),
            (1, v1(2, "rate")),
            (1, v1(3, "amt")),
            (1, v1(4, "ts")),
            (1, v1(5, "big")),
            (
                2,
                record(vec![
                    ("id", Avro::Long(6)),
                    ("qty", Avro::Long(3)),
                    ("rate", Avro::Float(0.25)),
                    ("amt", decimal(250)),
                    ("ts", Avro::TimestampMillis(1_700_000_000_456)),
                    ("big", Avro::Int(9)),
                ]),
            ),
        ],
    )
    .await;
    let u = UnionSchema::build(&[registered(1, W1), registered(2, W2)], "kafka_");
    assert_eq!(
        u.schema().field_with_name("qty").unwrap().data_type(),
        &DataType::Int64
    );
    let mut rejects = Vec::new();
    let aligned = u.align_all(batches, &mut rejects).unwrap();
    assert_eq!(rejected_offsets(&rejects), [1, 2, 3, 4, 5]);
    let why = reasons(&rejects);
    for (reason, field) in why.iter().zip(["qty", "rate", "amt", "ts"]) {
        assert!(
            reason.starts_with(&format!("writer schema 1 field {field}: "))
                && reason.contains("without changing the value"),
            "{why:#?}"
        );
    }
    assert!(
        why[4].contains("field big: Int64 does not convert to Int32"),
        "{why:#?}"
    );

    let sql = SqlTransform::new("SELECT id, qty, rate, CAST(amt AS VARCHAR) AS amt FROM source");
    let mut rows = json(&sql.run(u.schema(), aligned).await.unwrap());
    rows.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"id": 0, "qty": 2, "rate": 1.5, "amt": "1.23"}),
            serde_json::json!({"id": 6, "qty": 3, "rate": 0.25, "amt": "2.50"}),
        ]
    );
}

/// A newer version widens a plain field into a general union that has the old type as a branch:
/// an older row's value becomes that branch, its null the union's null branch. The union then
/// goes into the target like any union, branch by branch.
#[tokio::test]
async fn union_widening_wraps_branch() {
    const C1: &str = r#"{"type":"record","name":"C","fields":[
        {"name":"id","type":"long"},{"name":"code","type":["null","int"]}]}"#;
    const C2: &str = r#"{"type":"record","name":"C","fields":[
        {"name":"id","type":"long"},{"name":"code","type":["null","int","string"]}]}"#;
    let batches = decoded(
        &[(1, C1), (2, C2)],
        vec![
            (
                1,
                record(vec![
                    ("id", Avro::Long(0)),
                    ("code", Avro::Union(1, Box::new(Avro::Int(7)))),
                ]),
            ),
            (
                1,
                record(vec![
                    ("id", Avro::Long(1)),
                    ("code", Avro::Union(0, Box::new(Avro::Null))),
                ]),
            ),
            (
                2,
                record(vec![
                    ("id", Avro::Long(2)),
                    ("code", Avro::Union(2, Box::new(s("A7")))),
                ]),
            ),
            (
                2,
                record(vec![
                    ("id", Avro::Long(3)),
                    ("code", Avro::Union(1, Box::new(Avro::Int(8)))),
                ]),
            ),
        ],
    )
    .await;
    let u = UnionSchema::build(&[registered(1, C1), registered(2, C2)], "kafka_");
    let DataType::Union(fields, UnionMode::Dense) = u
        .schema()
        .field_with_name("code")
        .unwrap()
        .data_type()
        .clone()
    else {
        panic!("{:?}", u.schema())
    };
    let mut rejects = Vec::new();
    let aligned = u.align_all(batches, &mut rejects).unwrap();
    assert!(rejects.is_empty());
    // Version 1's rows: 7 in the int branch, the null in the null branch.
    let code = aligned[0].column_by_name("code").unwrap();
    let code = code.as_any().downcast_ref::<UnionArray>().unwrap();
    let branch = |t: i8| {
        fields
            .iter()
            .find(|(id, _)| *id == t)
            .unwrap()
            .1
            .name()
            .clone()
    };
    assert_eq!(branch(code.type_id(0)), "int");
    assert_eq!(code.value(0).as_primitive::<Int32Type>().value(0), 7);
    assert_eq!(branch(code.type_id(1)), "null");

    let as_text = target(vec![
        field("id", Delta::LONG, false),
        field("code", Delta::STRING, true),
    ]);
    let (good, rejects) = through(&mut conformer(as_text), aligned.clone());
    assert!(rejects.is_empty());
    let mut rows = json(&good);
    rows.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"id": 0, "code": "7"}),
            serde_json::json!({"id": 1, "code": null}),
            serde_json::json!({"id": 2, "code": "A7"}),
            serde_json::json!({"id": 3, "code": "8"}),
        ]
    );
    let as_number = target(vec![
        field("id", Delta::LONG, false),
        field("code", Delta::LONG, true),
    ]);
    let (good, rejects) = through(&mut conformer(as_number), aligned);
    assert_eq!(rejected_offsets(&rejects), [2]);
    assert_eq!(good.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

/// Row `i` of a union column: its branch's name, and the branch's struct as text fields.
fn union_row(col: &dyn Array, i: usize) -> (String, Vec<(String, Option<String>)>) {
    let u = col.as_any().downcast_ref::<UnionArray>().unwrap();
    let DataType::Union(fields, _) = col.data_type() else {
        panic!("{:?}", col.data_type())
    };
    let t = u.type_id(i);
    let name = fields
        .iter()
        .find(|(id, _)| *id == t)
        .unwrap()
        .1
        .name()
        .clone();
    let offset = u.value_offset(i);
    let values = match u.child(t).as_struct_opt() {
        Some(st) => st
            .fields()
            .iter()
            .zip(st.columns())
            .map(|(f, c)| {
                let c = c.as_string::<i32>();
                (
                    f.name().clone(),
                    c.is_valid(offset).then(|| c.value(offset).into()),
                )
            })
            .collect(),
        None => Vec::new(),
    };
    (name, values)
}

/// A union of records: a record goes into the branch of its own name, as Avro's resolution puts
/// it, never just into the first record branch, which dropped its fields: in one writer schema's
/// union, from an older plain record a newer one widened into a union, and when the records'
/// fields alone cannot tell them apart. A field the chosen branch lacks would be lost, so a row
/// holding a value there is rejected, and one holding a null is kept.
#[tokio::test]
async fn union_of_records_by_name() {
    const PAY: &str = r#"{"type":"record","name":"Order","fields":[
        {"name":"id","type":"long"},
        {"name":"payment","type":["null",
            {"type":"record","name":"Card","fields":[{"name":"last4","type":"string"}]},
            {"type":"record","name":"Bank","fields":[{"name":"iban","type":"string"}]}]}]}"#;
    let pay = |id: i64, branch: u32, v: Avro| {
        record(vec![
            ("id", Avro::Long(id)),
            ("payment", Avro::Union(branch, Box::new(v))),
        ])
    };
    let iban = "DE89370400440532013000";
    let batches = decoded(
        &[(1, PAY)],
        vec![
            (1, pay(0, 1, record(vec![("last4", s("1234"))]))),
            (1, pay(1, 2, record(vec![("iban", s(iban))]))),
            (1, pay(2, 0, Avro::Null)),
        ],
    )
    .await;
    let u = UnionSchema::build(&[registered(1, PAY)], "kafka_");
    let mut rejects = Vec::new();
    let aligned = u.align_all(batches, &mut rejects).unwrap();
    assert!(rejects.is_empty(), "{:?}", reasons(&rejects));
    let payment = aligned[0].column_by_name("payment").unwrap();
    assert_eq!(
        union_row(payment.as_ref(), 1),
        ("Bank".into(), vec![("iban".into(), Some(iban.into()))])
    );
    let t = target(vec![
        field("id", Delta::LONG, false),
        field(
            "payment",
            strct(vec![
                field("last4", Delta::STRING, true),
                field("iban", Delta::STRING, true),
            ]),
            true,
        ),
    ]);
    let out = SqlTransform::new("SELECT * FROM source")
        .run(u.schema(), aligned)
        .await
        .unwrap();
    let (good, rejects) = through(&mut conformer(t), out);
    assert!(rejects.is_empty());
    let mut rows = json(&good);
    rows.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(
        rows[1]["payment"],
        serde_json::json!({"last4": null, "iban": iban})
    );

    // An older plain record, widened into the union: the branch of its name, whatever comes
    // first, and also where the two records' fields are the same.
    const BANK: &str = r#"{"type":"record","name":"Order","fields":[
        {"name":"id","type":"long"},
        {"name":"payment","type":{"type":"record","name":"Bank","fields":[{"name":"iban","type":"string"}]}}]}"#;
    const SAME1: &str = r#"{"type":"record","name":"Order","fields":[
        {"name":"id","type":"long"},
        {"name":"payment","type":{"type":"record","name":"Bank","namespace":"pay","fields":[
            {"name":"ref","type":"string"}]}}]}"#;
    const SAME2: &str = r#"{"type":"record","name":"Order","fields":[
        {"name":"id","type":"long"},
        {"name":"payment","type":["null",
            {"type":"record","name":"Card","namespace":"pay","fields":[{"name":"ref","type":"string"}]},
            {"type":"record","name":"Bank","namespace":"pay","fields":[{"name":"ref","type":"string"}]}]}]}"#;
    for (old, new, field_name, branch) in [
        (BANK, PAY, "iban", "Bank"),
        (SAME1, SAME2, "ref", "pay.Bank"),
    ] {
        let batches = decoded(
            &[(1, old), (2, new)],
            vec![(
                1,
                record(vec![
                    ("id", Avro::Long(0)),
                    ("payment", record(vec![(field_name, s(iban))])),
                ]),
            )],
        )
        .await;
        let u = UnionSchema::build(&[registered(1, old), registered(2, new)], "kafka_");
        let (aligned, rejected) = u.align(&batches[0]).unwrap();
        assert!(rejected.is_none());
        let payment = aligned.column_by_name("payment").unwrap();
        assert_eq!(
            union_row(payment.as_ref(), 0),
            (branch.into(), vec![(field_name.into(), Some(iban.into()))])
        );
    }

    // A newer Card without `exp`: an older Card's `exp` would be lost where it has a value.
    const EXP: &str = r#"{"type":"record","name":"Order","fields":[
        {"name":"id","type":"long"},
        {"name":"payment","type":["null",
            {"type":"record","name":"Card","fields":[
                {"name":"last4","type":"string"},{"name":"exp","type":["null","string"]}]},
            {"type":"record","name":"Bank","fields":[{"name":"iban","type":"string"}]}]}]}"#;
    let card = |exp: Option<&str>| {
        record(vec![
            ("last4", s("1234")),
            (
                "exp",
                match exp {
                    Some(e) => Avro::Union(1, Box::new(s(e))),
                    None => Avro::Union(0, Box::new(Avro::Null)),
                },
            ),
        ])
    };
    let batches = decoded(
        &[(1, EXP), (2, PAY)],
        vec![
            (1, pay(0, 1, card(Some("12/30")))),
            (1, pay(1, 1, card(None))),
        ],
    )
    .await;
    let u = UnionSchema::build(&[registered(1, EXP), registered(2, PAY)], "kafka_");
    let mut rejects = Vec::new();
    let aligned = u.align_all(batches, &mut rejects).unwrap();
    assert_eq!(rejected_offsets(&rejects), [0]);
    assert!(
        reasons(&rejects)[0].contains("payment (union branch Card).exp"),
        "{:?}",
        reasons(&rejects)
    );
    let payment = aligned[0].column_by_name("payment").unwrap();
    assert_eq!(
        union_row(payment.as_ref(), 0),
        ("Card".into(), vec![("last4".into(), Some("1234".into()))])
    );
}

const G1: &str = r#"{"type":"record","name":"G","fields":[
    {"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
const G2: &str = r#"{"type":"record","name":"G","fields":[
    {"name":"id","type":"long"},{"name":"name","type":"string"},
    {"name":"note","type":["null","string"],"default":null}]}"#;
const G3: &str = r#"{"type":"record","name":"G","fields":[
    {"name":"id","type":"long"},{"name":"name","type":"string"},
    {"name":"note","type":["null","string"],"default":null},
    {"name":"channel","type":["null","string"],"default":null}]}"#;

/// SQL naming a field a just-registered version added plans before the first message of that
/// version arrives, because `U` holds the latest version. When a newer one adds another field,
/// `U` grows, the batches read so far are aligned to it again, and SQL naming the new field runs
/// over them.
#[tokio::test]
async fn union_u_growth_keeps_sql_planning() {
    let old = decoded(
        &[(1, G1)],
        vec![(1, record(vec![("id", Avro::Long(0)), ("name", s("a"))]))],
    )
    .await;
    // Seen: 1; latest: 2.
    let u = UnionSchema::build(&[registered(2, G2), registered(1, G1)], "kafka_");
    let sql = SqlTransform::new("SELECT id, note FROM source");
    u.plan(&sql).await.unwrap();
    let mut rejects = Vec::new();
    let aligned = u.align_all(old.clone(), &mut rejects).unwrap();
    let out = sql.run(u.schema(), aligned).await.unwrap();
    assert_eq!(json(&out), vec![serde_json::json!({"id": 0, "note": null})]);

    // Version 3 arrives with `channel`; the batch of version 1 is still buffered.
    let new = decoded(
        &[(3, G3)],
        vec![(
            3,
            record(vec![
                ("id", Avro::Long(1)),
                ("name", s("b")),
                ("note", Avro::Union(1, Box::new(s("n")))),
                ("channel", Avro::Union(1, Box::new(s("web")))),
            ]),
        )],
    )
    .await;
    let grown = UnionSchema::build(
        &[registered(3, G3), registered(2, G2), registered(1, G1)],
        "kafka_",
    );
    assert!(grown.schema().field_with_name("channel").is_ok());
    let sql = SqlTransform::new("SELECT id, note, channel FROM source");
    assert!(u.plan(&sql).await.is_err(), "the old union has no channel");
    grown.plan(&sql).await.unwrap();
    let mut buffered = old;
    buffered.extend(new);
    let aligned = grown.align_all(buffered, &mut rejects).unwrap();
    assert!(rejects.is_empty());
    let mut rows = json(&sql.run(grown.schema(), aligned).await.unwrap());
    rows.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"id": 0, "note": null, "channel": null}),
            serde_json::json!({"id": 1, "note": "n", "channel": "web"}),
        ]
    );

    // A batch aligned before its writer schema joined `U` would lose a field: refused.
    let err = u.align(
        &decoded(
            &[(3, G3)],
            vec![(
                3,
                record(vec![
                    ("id", Avro::Long(1)),
                    ("name", s("b")),
                    ("note", Avro::Union(0, Box::new(Avro::Null))),
                    ("channel", Avro::Union(0, Box::new(Avro::Null))),
                ]),
            )],
        )
        .await[0],
    );
    assert!(err.unwrap_err().to_string().contains("\"channel\""));
}

/// A new version turns `amount` from a number into a record. `U` takes the record, and SQL that
/// adds to `amount` no longer plans: the pipeline holds with `KafkaSchemaConflict`, naming the
/// field and both writer schemas, until the SQL is adjusted. SQL that does not read `amount`
/// plans on.
#[tokio::test]
async fn union_u_sql_no_longer_plans_blocks() {
    const A1: &str = r#"{"type":"record","name":"P","fields":[
        {"name":"id","type":"long"},{"name":"amount","type":"long"}]}"#;
    const A2: &str = r#"{"type":"record","name":"P","fields":[
        {"name":"id","type":"long"},
        {"name":"amount","type":{"type":"record","name":"M","fields":[
            {"name":"value","type":"long"},{"name":"currency","type":"string"}]}}]}"#;
    let sql = SqlTransform::new("SELECT id, amount + 1 AS next FROM source");
    let before = UnionSchema::build(&[registered(1, A1)], "kafka_");
    before.plan(&sql).await.unwrap();

    let after = UnionSchema::build(&[registered(1, A1), registered(2, A2)], "kafka_");
    let err = after.plan(&sql).await.unwrap_err();
    let Error::KafkaSchemaConflict { fields, .. } = &err else {
        panic!("{err:?}")
    };
    assert_eq!(fields.len(), 1, "{fields:?}");
    assert!(fields[0].starts_with("amount ("), "{fields:?}");
    assert!(fields[0].contains("writer schema 2"), "{fields:?}");
    assert!(fields[0].contains("Int64 in writer schema 1"), "{fields:?}");
    assert_eq!(err.kafka_class(), KafkaClass::BlockingPipeline);
    assert!(err.to_string().contains("adjust the SQL"), "{err}");

    after
        .plan(&SqlTransform::new("SELECT id FROM source"))
        .await
        .unwrap();
    // SQL that fails without naming a changed field fails for its own reasons, and says so.
    let typo = after
        .plan(&SqlTransform::new("SELECT idd FROM source"))
        .await
        .unwrap_err();
    assert!(matches!(typo, Error::Transform(_)), "{typo:?}");
}

// ---------------------------------------------------------------------- partition columns

/// §5.6 step 0. A target partitioned by `ingest_date` needs the column produced: by the SQL,
/// under exactly that name, or without SQL by the writer schemas or the metadata columns. When
/// no writer schema is known yet the check waits for the first batch.
#[tokio::test]
async fn partition_column_unmapped_refused() {
    const WITH: &str = r#"{"type":"record","name":"E","fields":[
        {"name":"id","type":"long"},{"name":"ingest_date","type":"string"}]}"#;
    const WITHOUT: &str = r#"{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}"#;
    let partitions = ["ingest_date".to_string()];
    let with = UnionSchema::build(&[registered(1, WITH)], "kafka_");
    let without = UnionSchema::build(&[registered(2, WITHOUT)], "kafka_");
    let check = |sql: Option<&'static str>, u: UnionSchema, parts: Vec<String>| async move {
        let sql = sql.map(SqlTransform::new);
        check_partition_columns("/lake/events", &parts, sql.as_ref(), &u).await
    };

    // Identity: the writer schema has it, or a metadata column is it.
    assert_eq!(
        check(None, with.clone(), partitions.to_vec())
            .await
            .unwrap(),
        PartitionCheck::Passed
    );
    assert_eq!(
        check(None, without.clone(), vec!["kafka_partition".into()])
            .await
            .unwrap(),
        PartitionCheck::Passed
    );
    let err = check(None, without.clone(), partitions.to_vec())
        .await
        .unwrap_err();
    let Error::KafkaPartitionColumnUnmapped { columns, sql, .. } = &err else {
        panic!("{err:?}")
    };
    assert_eq!((columns.as_slice(), *sql), (&partitions[..], false));
    assert!(err.to_string().contains("Add a transform_sql"), "{err}");
    assert_eq!(err.kafka_class(), KafkaClass::BlockingPipeline);

    // SQL: produced under exactly its name, or refused, the near miss in the message.
    let computed = "SELECT id, CAST(kafka_timestamp AS DATE) AS ingest_date FROM source";
    assert_eq!(
        check(Some(computed), without.clone(), partitions.to_vec())
            .await
            .unwrap(),
        PartitionCheck::Passed
    );
    let cased = "SELECT id, CAST(kafka_timestamp AS DATE) AS \"Ingest_Date\" FROM source";
    let err = check(Some(cased), without.clone(), partitions.to_vec())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::KafkaPartitionColumnUnmapped { sql: true, produced, .. }
            if produced.contains(&"Ingest_Date".to_string())),
        "{err:?}"
    );
    assert!(err.to_string().contains("Ingest_Date"), "{err}");

    // No writer schema known (a subject the registry lacks, nothing seen): wait for a batch.
    let nothing = UnionSchema::build(&[], "kafka_");
    assert_eq!(
        check(None, nothing, partitions.to_vec()).await.unwrap(),
        PartitionCheck::Deferred
    );
    // And an unpartitioned target passes whatever is produced.
    assert_eq!(
        check(None, without, Vec::new()).await.unwrap(),
        PartitionCheck::Passed
    );
}

/// A decoded batch's metadata columns go into `U`'s as they are: the same values, under the
/// relaxed nullability every field of `U` has.
#[tokio::test]
async fn metadata_columns_align_unchanged() {
    let batches = decoded(
        &[(1, G1)],
        vec![(1, record(vec![("id", Avro::Long(4)), ("name", s("a"))]))],
    )
    .await;
    let u = UnionSchema::build(&[registered(1, G1)], "kafka_");
    let (aligned, rejects) = u.align(&batches[0]).unwrap();
    assert!(rejects.is_none());
    for name in [
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "kafka_value_schema_id",
    ] {
        assert_eq!(
            aligned.column_by_name(name).unwrap().as_ref(),
            batches[0].column_by_name(name).unwrap().as_ref(),
            "{name}"
        );
    }
    let headers = aligned
        .column_by_name("kafka_headers")
        .unwrap()
        .as_list::<i32>();
    assert_eq!(headers.value_length(0), 0);
}
