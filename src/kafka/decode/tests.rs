//! docs/kafka-design.md §9.1 "M2, decode", and the arrow-avro go/no-go evidence of §5.3.
//!
//! Fixtures are written with apache-avro 0.21.0, an Avro implementation independent of the
//! arrow-avro under test, and with arrow-avro's own writer where the design says so; both
//! behind the Confluent header (§5.1). No broker, no registry: writer schemas come from a map.

use super::*;
use crate::error::{KafkaClass, SchemaRegistryFailure};
use apache_avro::types::Value as Avro;
use async_trait::async_trait;
use deltalake::arrow::array::{
    BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array, Float64Array,
    IntervalMonthDayNanoArray, Time32MillisecondArray, Time64MicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
use deltalake::arrow::datatypes::{Int32Type, Int64Type, IntervalMonthDayNano};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

// ------------------------------------------------------------------------------ fixtures

/// Writer schemas by id; an id not in the map is one the registry does not know.
#[derive(Default)]
struct Schemas {
    by_id: Mutex<HashMap<u32, Lookup>>,
    asked: AtomicUsize,
    /// While set, every lookup fails as an unavailable registry.
    down: std::sync::atomic::AtomicBool,
}

impl Schemas {
    fn with(entries: &[(u32, &str)]) -> Arc<Self> {
        let s = Self::default();
        for (id, json) in entries {
            s.by_id
                .lock()
                .unwrap()
                .insert(*id, Lookup::Avro(registered(*id, json)));
        }
        Arc::new(s)
    }
}

#[async_trait]
impl WriterSchemas for Schemas {
    async fn writer_schema(&self, id: u32) -> Result<Lookup> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        if self.down.load(Ordering::SeqCst) {
            return Err(Error::SchemaRegistry {
                url: "http://sr:8081".into(),
                failure: SchemaRegistryFailure::Unavailable,
                detail: "HTTP 503".into(),
            });
        }
        Ok(self
            .by_id
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .unwrap_or_else(|| Lookup::Undecodable {
                reason: format!("schema id {id} is not in the Schema Registry (HTTP 404)"),
                deterministic: false,
            }))
    }
}

fn registered(id: u32, json: &str) -> Arc<RegisteredSchema> {
    Arc::new(RegisteredSchema {
        id,
        schema: json.to_string(),
        schema_type: None,
        references: Vec::new(),
        subject: None,
        version: None,
    })
}

const V1: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[
    {"name":"id","type":"long"},
    {"name":"name","type":"string"}]}"#;

const V2: &str = r#"{"type":"record","name":"Order","namespace":"com.acme","fields":[
    {"name":"id","type":"long"},
    {"name":"name","type":"string"},
    {"name":"note","type":["null","string"],"default":null}]}"#;

/// `value` in `schema`, as apache-avro writes it, behind the Confluent header.
fn framed(id: u32, schema: &str, value: Avro) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(schema).unwrap();
    let mut out = vec![0u8];
    out.extend(id.to_be_bytes());
    out.extend(apache_avro::to_avro_datum(&schema, value).unwrap());
    out
}

fn v1(id: i64, name: &str) -> Avro {
    Avro::Record(vec![
        ("id".into(), Avro::Long(id)),
        ("name".into(), Avro::String(name.into())),
    ])
}

fn v2(id: i64, name: &str, note: Option<&str>) -> Avro {
    Avro::Record(vec![
        ("id".into(), Avro::Long(id)),
        ("name".into(), Avro::String(name.into())),
        (
            "note".into(),
            match note {
                None => Avro::Union(0, Box::new(Avro::Null)),
                Some(n) => Avro::Union(1, Box::new(Avro::String(n.into()))),
            },
        ),
    ])
}

/// A message at `offset` of partition 0 with `value`, its key and headers derived from the
/// offset so that a row's metadata can be checked to be its own message's.
fn message(offset: i64, value: Option<Vec<u8>>) -> KafkaMessage {
    KafkaMessage {
        partition: (offset % 3) as i32,
        offset,
        timestamp: KafkaTimestamp::CreateTime(1_700_000_000_000 + offset),
        key: Some(format!("k{offset}").into_bytes()),
        value,
        headers: vec![
            KafkaHeader {
                key: "trace".into(),
                value: Some(format!("t{offset}").into_bytes()),
            },
            KafkaHeader {
                key: "trace".into(),
                value: None,
            },
        ],
    }
}

fn decoder(schemas: Arc<Schemas>) -> KafkaDecoder {
    KafkaDecoder::new("orders", "kafka_", schemas)
}

async fn feed_all(d: &mut KafkaDecoder, messages: Vec<KafkaMessage>) {
    for m in messages {
        d.feed(m).await.unwrap();
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a ArrayRef {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name} in {:?}", batch.schema()))
}

fn i64s(batch: &RecordBatch, name: &str) -> Vec<i64> {
    column(batch, name)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    column(batch, name)
        .as_string::<i32>()
        .iter()
        .map(|s| s.map(str::to_string))
        .collect()
}

/// Every output row, as (offset, id field), across batches, sorted by offset.
fn rows_by_offset(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut rows: Vec<(i64, i64)> = batches
        .iter()
        .flat_map(|b| i64s(b, "kafka_offset").into_iter().zip(i64s(b, "id")))
        .collect();
    rows.sort();
    rows
}

/// Every row's metadata columns are its own message's (§6.1), and its `id` field is the offset
/// the fixture wrote into it.
fn assert_rows_are_their_messages(batches: &[RecordBatch]) {
    for b in batches {
        let offsets = i64s(b, "kafka_offset");
        let ids = i64s(b, "id");
        let partitions = column(b, "kafka_partition").as_primitive::<Int32Type>();
        let keys = column(b, "kafka_key").as_binary::<i32>();
        let ts = column(b, "kafka_timestamp")
            .as_primitive::<deltalake::arrow::datatypes::TimestampMicrosecondType>();
        let headers = column(b, "kafka_headers").as_list::<i32>();
        for (i, offset) in offsets.iter().enumerate() {
            assert_eq!(ids[i], *offset, "row {i} holds another message's value");
            assert_eq!(partitions.value(i), (offset % 3) as i32);
            assert_eq!(keys.value(i), format!("k{offset}").as_bytes());
            assert_eq!(ts.value(i), (1_700_000_000_000 + offset) * 1000);
            let h = headers.value(i);
            let h = h.as_struct();
            assert_eq!(h.len(), 2);
            assert_eq!(
                h.column(1).as_binary::<i32>().value(0),
                format!("t{offset}").as_bytes()
            );
            assert!(h.column(1).is_null(1));
        }
    }
}

// ------------------------------------------------------------------------------- framing

#[test]
fn framing_reads_the_confluent_header() {
    assert_eq!(frame(&[0, 0, 0, 1, 2, 9]), Ok(258));
    assert_eq!(frame(&[0, 0xff, 0xff, 0xff, 0xff]), Ok(u32::MAX));
    let e = frame(br#"{"id":1}"#).unwrap_err();
    assert!(e.contains("0x7b") && e.contains("JSON"), "{e}");
    let e = frame(&[0, 0, 1]).unwrap_err();
    assert!(
        e.contains("3 byte(s)") && e.contains("5-byte header"),
        "{e}"
    );
    assert!(frame(&[]).unwrap_err().contains("empty"));
    let e = frame(&[0x4f, 0x62, 0x6a, 0x01]).unwrap_err();
    assert!(e.contains("0x4f") && !e.contains("JSON"), "{e}");
}

#[tokio::test]
async fn tombstones_are_counted_and_nothing_else() {
    let mut d = decoder(Schemas::with(&[(1, V1)]));
    feed_all(
        &mut d,
        vec![
            message(0, None),
            message(1, Some(framed(1, V1, v1(1, "a")))),
            message(2, None),
        ],
    )
    .await;
    let out = d.finish().unwrap();
    assert_eq!(out.tombstones, 2);
    assert_eq!(rows_by_offset(&out.batches), [(1, 1)]);
    assert!(out.undecodable.is_empty() && out.rejects.is_none());

    // The next batch starts from nothing.
    assert!(d.is_empty());
    let out = d.finish().unwrap();
    assert_eq!((out.tombstones, out.batches.len()), (0, 0));
}

// ------------------------------------------------------------------------- the matrix

/// Every Avro type and logical type a Confluent producer writes, through arrow-avro, against
/// the Arrow type and value expected after normalisation (§5.4, §5.6's table). The bytes are
/// apache-avro's, written in the schema's physical form (the wire form of a logical type is its
/// base type's), and apache-avro 0.21.0 reads them back in the logical form beside, so the two
/// implementations are shown to agree on every value they both read.
#[tokio::test]
async fn logical_type_matrix() {
    const MATRIX: &str = r#"{"type":"record","name":"Matrix","namespace":"com.acme","fields":[
        {"name":"b","type":"boolean"},
        {"name":"i","type":"int"},
        {"name":"l","type":"long"},
        {"name":"f","type":"float"},
        {"name":"d","type":"double"},
        {"name":"s","type":"string"},
        {"name":"by","type":"bytes"},
        {"name":"fx","type":{"type":"fixed","name":"Four","size":4}},
        {"name":"en","type":{"type":"enum","name":"Colour","symbols":["RED","GREEN","BLUE"]}},
        {"name":"u","type":{"type":"string","logicalType":"uuid"}},
        {"name":"dt","type":{"type":"int","logicalType":"date"}},
        {"name":"tm","type":{"type":"int","logicalType":"time-millis"}},
        {"name":"tu","type":{"type":"long","logicalType":"time-micros"}},
        {"name":"ts_ms","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"ts_us","type":{"type":"long","logicalType":"timestamp-micros"}},
        {"name":"ts_ns","type":{"type":"long","logicalType":"timestamp-nanos"}},
        {"name":"lts_ms","type":{"type":"long","logicalType":"local-timestamp-millis"}},
        {"name":"lts_us","type":{"type":"long","logicalType":"local-timestamp-micros"}},
        {"name":"dec_b","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
        {"name":"dec_f","type":{"type":"fixed","name":"Dec9","size":9,"logicalType":"decimal","precision":20,"scale":4}},
        {"name":"dur","type":{"type":"fixed","name":"Dur","size":12,"logicalType":"duration"}},
        {"name":"rec","type":{"type":"record","name":"Inner","fields":[
            {"name":"a","type":"int"},
            {"name":"b","type":["null","string"],"default":null}]}},
        {"name":"m","type":{"type":"map","values":"long"}},
        {"name":"arr","type":{"type":"array","items":"string"}},
        {"name":"n1","type":["null","long"]},
        {"name":"n2","type":["long","null"]},
        {"name":"un","type":["int","string"]}]}"#;

    let dec_b_a: i128 = -12_345; // -123.45
    let dec_f_a: i128 = 123_456_789_012; // 12345678.9012
    let rows = [
        Avro::Record(vec![
            ("b".into(), Avro::Boolean(true)),
            ("i".into(), Avro::Int(-7)),
            ("l".into(), Avro::Long(1 << 40)),
            ("f".into(), Avro::Float(0.1)),
            ("d".into(), Avro::Double(-2.5)),
            ("s".into(), Avro::String("héllo".into())),
            ("by".into(), Avro::Bytes(vec![1, 2, 255])),
            ("fx".into(), Avro::Fixed(4, vec![0xde, 0xad, 0xbe, 0xef])),
            ("en".into(), Avro::Enum(1, "GREEN".into())),
            (
                "u".into(),
                Avro::String("550E8400-E29B-41D4-A716-446655440000".into()),
            ),
            ("dt".into(), Avro::Int(19_000)),
            ("tm".into(), Avro::Int(3_600_001)),
            ("tu".into(), Avro::Long(86_399_999_999)),
            ("ts_ms".into(), Avro::Long(1_700_000_000_123)),
            ("ts_us".into(), Avro::Long(1_700_000_000_123_456)),
            ("ts_ns".into(), Avro::Long(1_700_000_000_123_456_789)),
            ("lts_ms".into(), Avro::Long(1_700_000_000_123)),
            ("lts_us".into(), Avro::Long(1_700_000_000_123_456)),
            ("dec_b".into(), Avro::Bytes(minimal_be(dec_b_a))),
            ("dec_f".into(), Avro::Fixed(9, sign_extended(dec_f_a, 9))),
            ("dur".into(), Avro::Fixed(12, duration_bytes(1, 2, 3_000))),
            (
                "rec".into(),
                Avro::Record(vec![
                    ("a".into(), Avro::Int(1)),
                    (
                        "b".into(),
                        Avro::Union(1, Box::new(Avro::String("x".into()))),
                    ),
                ]),
            ),
            (
                "m".into(),
                Avro::Map([("k".to_string(), Avro::Long(5))].into_iter().collect()),
            ),
            (
                "arr".into(),
                Avro::Array(vec![Avro::String("a".into()), Avro::String("b".into())]),
            ),
            ("n1".into(), Avro::Union(0, Box::new(Avro::Null))),
            ("n2".into(), Avro::Union(0, Box::new(Avro::Long(7)))),
            (
                "un".into(),
                Avro::Union(1, Box::new(Avro::String("u".into()))),
            ),
        ]),
        Avro::Record(vec![
            ("b".into(), Avro::Boolean(false)),
            ("i".into(), Avro::Int(i32::MAX)),
            ("l".into(), Avro::Long(i64::MIN)),
            ("f".into(), Avro::Float(f32::NAN)),
            ("d".into(), Avro::Double(f64::INFINITY)),
            ("s".into(), Avro::String(String::new())),
            ("by".into(), Avro::Bytes(vec![])),
            ("fx".into(), Avro::Fixed(4, vec![0, 0, 0, 0])),
            ("en".into(), Avro::Enum(2, "BLUE".into())),
            (
                "u".into(),
                Avro::String("00000000-0000-0000-0000-000000000000".into()),
            ),
            ("dt".into(), Avro::Int(-1)),
            ("tm".into(), Avro::Int(0)),
            ("tu".into(), Avro::Long(0)),
            ("ts_ms".into(), Avro::Long(-1)),
            ("ts_us".into(), Avro::Long(0)),
            ("ts_ns".into(), Avro::Long(-1)),
            ("lts_ms".into(), Avro::Long(0)),
            ("lts_us".into(), Avro::Long(-1)),
            ("dec_b".into(), Avro::Bytes(minimal_be(5))),
            ("dec_f".into(), Avro::Fixed(9, sign_extended(-1, 9))),
            ("dur".into(), Avro::Fixed(12, duration_bytes(0, 0, 0))),
            (
                "rec".into(),
                Avro::Record(vec![
                    ("a".into(), Avro::Int(-1)),
                    ("b".into(), Avro::Union(0, Box::new(Avro::Null))),
                ]),
            ),
            ("m".into(), Avro::Map(Default::default())),
            ("arr".into(), Avro::Array(vec![])),
            ("n1".into(), Avro::Union(1, Box::new(Avro::Long(3)))),
            ("n2".into(), Avro::Union(1, Box::new(Avro::Null))),
            ("un".into(), Avro::Union(0, Box::new(Avro::Int(42)))),
        ]),
    ];
    let physical = physical(MATRIX);
    let payloads: Vec<Vec<u8>> = rows
        .iter()
        .map(|r| framed(7, &physical, r.clone()))
        .collect();

    let mut d = decoder(Schemas::with(&[(7, MATRIX)]));
    for (o, p) in payloads.iter().enumerate() {
        d.feed(message(o as i64, Some(p.clone()))).await.unwrap();
    }
    let out = d.finish().unwrap();
    assert!(out.undecodable.is_empty(), "{:?}", out.undecodable);
    assert_eq!(out.batches.len(), 1);
    let b = &out.batches[0];
    assert_eq!(b.num_rows(), 2);

    let utc = |u| DataType::Timestamp(u, Some("UTC".into()));
    let expected_types: Vec<(&str, DataType)> = vec![
        ("b", DataType::Boolean),
        ("i", DataType::Int32),
        ("l", DataType::Int64),
        ("f", DataType::Float32),
        ("d", DataType::Float64),
        ("s", DataType::Utf8),
        ("by", DataType::Binary),
        ("fx", DataType::FixedSizeBinary(4)),
        ("en", DataType::Utf8),
        ("u", DataType::Utf8),
        ("dt", DataType::Date32),
        ("tm", DataType::Time32(TimeUnit::Millisecond)),
        ("tu", DataType::Time64(TimeUnit::Microsecond)),
        ("ts_ms", utc(TimeUnit::Millisecond)),
        ("ts_us", utc(TimeUnit::Microsecond)),
        ("ts_ns", utc(TimeUnit::Nanosecond)),
        ("lts_ms", DataType::Timestamp(TimeUnit::Millisecond, None)),
        ("lts_us", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ("dec_b", DataType::Decimal128(10, 2)),
        ("dec_f", DataType::Decimal128(20, 4)),
        (
            "dur",
            DataType::Interval(deltalake::arrow::datatypes::IntervalUnit::MonthDayNano),
        ),
    ];
    for (name, t) in &expected_types {
        assert_eq!(column(b, name).data_type(), t, "{name}");
        assert!(
            !b.schema().field_with_name(name).unwrap().is_nullable(),
            "{name}"
        );
    }
    // Nothing of Avro's metadata stays on a field.
    assert!(b.schema().fields().iter().all(|f| f.metadata().is_empty()));

    let get = |name: &str| column(b, name).clone();
    let bools = get("b");
    let bools = bools.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!((bools.value(0), bools.value(1)), (true, false));
    assert_eq!(
        column(b, "i").as_primitive::<Int32Type>().values().to_vec(),
        [-7, i32::MAX]
    );
    assert_eq!(i64s(b, "l"), [1 << 40, i64::MIN]);
    let f = get("f");
    let f = f.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(f.value(0), 0.1f32);
    assert!(f.value(1).is_nan());
    let dbl = get("d");
    let dbl = dbl.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!((dbl.value(0), dbl.value(1)), (-2.5, f64::INFINITY));
    assert_eq!(strings(b, "s"), [Some("héllo".into()), Some(String::new())]);
    let by = column(b, "by").as_binary::<i32>();
    assert_eq!((by.value(0), by.value(1)), (&[1u8, 2, 255][..], &[][..]));
    let fx = get("fx");
    let fx = fx.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
    assert_eq!(fx.value(0), [0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(
        strings(b, "en"),
        [Some("GREEN".into()), Some("BLUE".into())]
    );
    // Lowercase hyphenated, whatever case the producer wrote: apache-avro's form.
    assert_eq!(
        strings(b, "u"),
        [
            Some("550e8400-e29b-41d4-a716-446655440000".into()),
            Some("00000000-0000-0000-0000-000000000000".into())
        ]
    );
    let dt = get("dt");
    assert_eq!(
        dt.as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .values()
            .to_vec(),
        [19_000, -1]
    );
    let tm = get("tm");
    assert_eq!(
        tm.as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap()
            .values()
            .to_vec(),
        [3_600_001, 0]
    );
    let tu = get("tu");
    assert_eq!(
        tu.as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap()
            .values()
            .to_vec(),
        [86_399_999_999, 0]
    );
    let ts_ms = get("ts_ms");
    assert_eq!(
        ts_ms
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values()
            .to_vec(),
        [1_700_000_000_123, -1]
    );
    let ts_ns = get("ts_ns");
    assert_eq!(
        ts_ns
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .values()
            .to_vec(),
        [1_700_000_000_123_456_789, -1]
    );
    let dec_b = get("dec_b");
    let dec_b = dec_b.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!((dec_b.value(0), dec_b.value(1)), (dec_b_a, 5));
    assert_eq!(dec_b.value_as_string(0), "-123.45");
    let dec_f = get("dec_f");
    let dec_f = dec_f.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!((dec_f.value(0), dec_f.value(1)), (dec_f_a, -1));
    let dur = get("dur");
    let dur = dur
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .unwrap();
    assert_eq!(dur.value(0), IntervalMonthDayNano::new(1, 2, 3_000_000_000));

    // A record by name, its nullable child null where it was.
    let rec = column(b, "rec").as_struct();
    assert_eq!(
        rec.column_by_name("a")
            .unwrap()
            .as_primitive::<Int32Type>()
            .values()
            .to_vec(),
        [1, -1]
    );
    let rb = rec.column_by_name("b").unwrap().as_string::<i32>();
    assert_eq!((rb.value(0), rb.is_null(1)), ("x", true));
    // Maps and lists named as the Delta kernel names them.
    let DataType::Map(entries, false) = column(b, "m").data_type() else {
        panic!("{:?}", column(b, "m").data_type())
    };
    assert_eq!(entries.name(), "key_value");
    let DataType::Struct(kv) = entries.data_type() else {
        panic!()
    };
    assert_eq!(
        kv.iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
        ["key", "value"]
    );
    let m = column(b, "m").as_map();
    assert_eq!((m.value_length(0), m.value_length(1)), (1, 0));
    assert_eq!(m.keys().as_string::<i32>().value(0), "k");
    assert_eq!(m.values().as_primitive::<Int64Type>().value(0), 5);
    let DataType::List(item) = column(b, "arr").data_type() else {
        panic!()
    };
    assert_eq!(item.name(), "element");
    let arr = column(b, "arr").as_list::<i32>();
    assert_eq!(arr.value(0).as_string::<i32>().value(1), "b");
    assert_eq!(arr.value_length(1), 0);
    // Both spellings of a nullable union are a nullable column.
    for name in ["n1", "n2"] {
        assert!(
            b.schema().field_with_name(name).unwrap().is_nullable(),
            "{name}"
        );
    }
    let n1 = column(b, "n1").as_primitive::<Int64Type>();
    assert_eq!((n1.is_null(0), n1.value(1)), (true, 3));
    let n2 = column(b, "n2").as_primitive::<Int64Type>();
    assert_eq!((n2.value(0), n2.is_null(1)), (7, true));
    // A general union stays a union, its active branch per row.
    let un = get("un");
    let un = un.as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(un.value(0).as_string::<i32>().value(0), "u");
    assert_eq!(un.value(1).as_primitive::<Int32Type>().value(0), 42);

    // apache-avro 0.21.0 reads the same bytes in the logical schema to the same values.
    let logical = apache_avro::Schema::parse_str(MATRIX).unwrap();
    for (row, payload) in payloads.iter().enumerate() {
        let value =
            apache_avro::from_avro_datum(&logical, &mut &payload[HEADER_LEN..], None).unwrap();
        let Avro::Record(fields) = value else {
            panic!()
        };
        let field = |n: &str| fields.iter().find(|(k, _)| k == n).unwrap().1.clone();
        let ts = column(b, "ts_ms")
            .as_primitive::<deltalake::arrow::datatypes::TimestampMillisecondType>();
        assert_eq!(field("ts_ms"), Avro::TimestampMillis(ts.value(row)));
        let us = column(b, "ts_us")
            .as_primitive::<deltalake::arrow::datatypes::TimestampMicrosecondType>();
        assert_eq!(field("ts_us"), Avro::TimestampMicros(us.value(row)));
        let lus = column(b, "lts_us")
            .as_primitive::<deltalake::arrow::datatypes::TimestampMicrosecondType>();
        assert_eq!(field("lts_us"), Avro::LocalTimestampMicros(lus.value(row)));
        assert_eq!(
            field("dt"),
            Avro::Date(
                column(b, "dt")
                    .as_primitive::<deltalake::arrow::datatypes::Date32Type>()
                    .value(row)
            )
        );
        let Avro::Uuid(u) = field("u") else { panic!() };
        assert_eq!(Some(u.to_string()), strings(b, "u")[row]);
        assert_eq!(
            field("en"),
            Avro::Enum(
                if row == 0 { 1 } else { 2 },
                strings(b, "en")[row].clone().unwrap()
            )
        );
        let Avro::Decimal(dec) = field("dec_b") else {
            panic!()
        };
        let unscaled: Vec<u8> = Vec::<u8>::try_from(&dec).unwrap();
        assert_eq!(unscaled, minimal_be(dec_b.value(row)));
    }
}

/// A `fixed(16)` with logicalType uuid is text too, though arrow-avro knows that logical type
/// on `string` only: it is told by the name arrow-avro keeps on the field, also where the type
/// is used again by name and inside containers. apache-avro 0.21.0 cannot read such a field: it
/// reads a length before the 16 bytes (decode.rs:123-136), so kdi never ingested one.
#[tokio::test]
async fn fixed_uuid_is_text_as_well() {
    const FIXED: &str = r#"{"type":"record","name":"R","namespace":"com.acme","fields":[
        {"name":"a","type":{"type":"fixed","name":"Uuid16","size":16,"logicalType":"uuid"}},
        {"name":"b","type":["null","Uuid16"]},
        {"name":"c","type":{"type":"array","items":"com.acme.Uuid16"}},
        {"name":"d","type":{"type":"fixed","name":"Plain16","size":16}}]}"#;
    let bytes: Vec<u8> = (0u8..16).collect();
    let row = Avro::Record(vec![
        ("a".into(), Avro::Fixed(16, bytes.clone())),
        (
            "b".into(),
            Avro::Union(1, Box::new(Avro::Fixed(16, bytes.clone()))),
        ),
        (
            "c".into(),
            Avro::Array(vec![Avro::Fixed(16, bytes.clone())]),
        ),
        ("d".into(), Avro::Fixed(16, bytes.clone())),
    ]);
    let payload = framed(3, &physical(FIXED), row);
    let mut d = decoder(Schemas::with(&[(3, FIXED)]));
    d.feed(message(0, Some(payload.clone()))).await.unwrap();
    let out = d.finish().unwrap();
    assert!(out.undecodable.is_empty(), "{:?}", out.undecodable);
    let b = &out.batches[0];
    let text = "00010203-0405-0607-0809-0a0b0c0d0e0f";
    assert_eq!(strings(b, "a"), [Some(text.into())]);
    assert_eq!(strings(b, "b"), [Some(text.into())]);
    assert_eq!(
        column(b, "c")
            .as_list::<i32>()
            .value(0)
            .as_string::<i32>()
            .value(0),
        text
    );
    // A plain fixed(16) is bytes.
    assert_eq!(column(b, "d").data_type(), &DataType::FixedSizeBinary(16));

    let logical = apache_avro::Schema::parse_str(FIXED).unwrap();
    let read = apache_avro::from_avro_datum(&logical, &mut &payload[HEADER_LEN..], None);
    assert!(
        !matches!(read, Ok(Avro::Record(ref f)) if f[0].1 == Avro::Uuid(apache_avro::Uuid::from_slice(&bytes).unwrap())),
        "apache-avro 0.21.0 read a fixed uuid right: {read:?}"
    );
}

#[tokio::test]
async fn uuid_and_enum_normalised() {
    // Enums and uuids at every depth: top level, nullable, in a record, a list, a map.
    const NESTED: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
        {"name":"ne","type":["null","E"]},
        {"name":"u","type":["null",{"type":"string","logicalType":"uuid"}]},
        {"name":"rec","type":{"type":"record","name":"In","fields":[
            {"name":"e","type":"E"},
            {"name":"u","type":{"type":"string","logicalType":"uuid"}}]}},
        {"name":"le","type":{"type":"array","items":"E"}},
        {"name":"me","type":{"type":"map","values":"E"}}]}"#;
    let uuid = "0e8a2c3c-9b1f-4c6a-8d2e-1f0a9b8c7d6e";
    let row = |nulls: bool| {
        Avro::Record(vec![
            ("e".into(), Avro::Enum(1, "B".into())),
            (
                "ne".into(),
                if nulls {
                    Avro::Union(0, Box::new(Avro::Null))
                } else {
                    Avro::Union(1, Box::new(Avro::Enum(0, "A".into())))
                },
            ),
            (
                "u".into(),
                if nulls {
                    Avro::Union(0, Box::new(Avro::Null))
                } else {
                    Avro::Union(1, Box::new(Avro::String(uuid.into())))
                },
            ),
            (
                "rec".into(),
                Avro::Record(vec![
                    ("e".into(), Avro::Enum(0, "A".into())),
                    ("u".into(), Avro::String(uuid.into())),
                ]),
            ),
            (
                "le".into(),
                Avro::Array(vec![Avro::Enum(1, "B".into()), Avro::Enum(0, "A".into())]),
            ),
            (
                "me".into(),
                Avro::Map(
                    [("x".to_string(), Avro::Enum(1, "B".into()))]
                        .into_iter()
                        .collect(),
                ),
            ),
        ])
    };
    let mut d = decoder(Schemas::with(&[(2, NESTED)]));
    for (o, nulls) in [(0, false), (1, true)] {
        d.feed(message(o, Some(framed(2, &physical(NESTED), row(nulls)))))
            .await
            .unwrap();
    }
    let out = d.finish().unwrap();
    assert!(out.undecodable.is_empty(), "{:?}", out.undecodable);
    let b = &out.batches[0];
    assert_eq!(strings(b, "e"), [Some("B".into()), Some("B".into())]);
    assert_eq!(strings(b, "ne"), [Some("A".into()), None]);
    assert_eq!(strings(b, "u"), [Some(uuid.into()), None]);
    let rec = column(b, "rec").as_struct();
    assert_eq!(
        rec.column_by_name("e").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        rec.column_by_name("u").unwrap().as_string::<i32>().value(0),
        uuid
    );
    let DataType::List(item) = column(b, "le").data_type() else {
        panic!()
    };
    assert_eq!(item.data_type(), &DataType::Utf8);
    assert_eq!(
        column(b, "le")
            .as_list::<i32>()
            .value(0)
            .as_string::<i32>()
            .value(0),
        "B"
    );
    assert_eq!(
        column(b, "me")
            .as_map()
            .values()
            .as_string::<i32>()
            .value(0),
        "B"
    );
    // No dictionary and no fixed-size binary is left anywhere.
    fn no_raw(t: &DataType) -> bool {
        match t {
            DataType::Dictionary(..) | DataType::FixedSizeBinary(_) => false,
            DataType::Struct(fs) => fs.iter().all(|f| no_raw(f.data_type())),
            DataType::List(f) | DataType::Map(f, _) => no_raw(f.data_type()),
            _ => true,
        }
    }
    assert!(b.schema().fields().iter().all(|f| no_raw(f.data_type())));
}

/// Timestamps say `UTC`, never arrow-avro's default `+00:00`; local timestamps stay naive, and a
/// cast into a `UTC` column reads them as UTC (§5.6, verify M2). The metadata timestamp is
/// kdi's µs (ms × 1000), null where Kafka has none, with kdi's type codes.
#[tokio::test]
async fn timestamps_are_utc() {
    const TS: &str = r#"{"type":"record","name":"T","fields":[
        {"name":"id","type":"long"},
        {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"at_us","type":["null",{"type":"long","logicalType":"timestamp-micros"}]},
        {"name":"local","type":{"type":"long","logicalType":"local-timestamp-micros"}}]}"#;
    let row = |id: i64| {
        Avro::Record(vec![
            ("id".into(), Avro::Long(id)),
            ("at".into(), Avro::Long(1_700_000_000_123)),
            (
                "at_us".into(),
                Avro::Union(1, Box::new(Avro::Long(1_700_000_000_123_456))),
            ),
            ("local".into(), Avro::Long(1_700_000_000_123_456)),
        ])
    };
    let mut d = decoder(Schemas::with(&[(4, TS)]));
    let mut timestamps = [
        KafkaTimestamp::CreateTime(1_700_000_000_000),
        KafkaTimestamp::LogAppendTime(1_700_000_000_001),
        KafkaTimestamp::NotAvailable,
        // Too far out to be microseconds: null, never an overflow.
        KafkaTimestamp::CreateTime(i64::MAX),
    ]
    .into_iter();
    for o in 0..4 {
        let mut m = message(o, Some(framed(4, &physical(TS), row(o))));
        m.timestamp = timestamps.next().unwrap();
        d.feed(m).await.unwrap();
    }
    let b = &d.finish().unwrap().batches[0];
    let utc = Some("UTC".into());
    assert_eq!(
        column(b, "at").data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, utc.clone())
    );
    assert_eq!(
        column(b, "at_us").data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, utc.clone())
    );
    assert_eq!(
        column(b, "local").data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    let as_utc = deltalake::arrow::compute::cast(
        column(b, "local"),
        &DataType::Timestamp(TimeUnit::Microsecond, utc.clone()),
    )
    .unwrap();
    assert_eq!(
        as_utc
            .as_primitive::<deltalake::arrow::datatypes::TimestampMicrosecondType>()
            .value(0),
        1_700_000_000_123_456,
        "a naive timestamp is read as UTC"
    );

    let meta = column(b, "kafka_timestamp");
    assert_eq!(
        meta.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, utc)
    );
    let meta = meta.as_primitive::<deltalake::arrow::datatypes::TimestampMicrosecondType>();
    assert_eq!(meta.value(0), 1_700_000_000_000_000);
    assert_eq!(meta.value(1), 1_700_000_000_001_000);
    assert!(meta.is_null(2) && meta.is_null(3));
    assert_eq!(
        column(b, "kafka_timestamp_type")
            .as_primitive::<Int32Type>()
            .values()
            .to_vec(),
        [0, 1, -1, 0]
    );
}

// ------------------------------------------------------------------- writer schemas

/// Interleaved writer versions: each message goes to its own schema's decoder, and each
/// decoder's rows come out as one batch at the end, never a row at a time.
#[tokio::test]
async fn writer_schema_switch_mid_batch() {
    let mut d = decoder(Schemas::with(&[(1, V1), (2, V2)]));
    // Written with arrow-avro's own writer, as §9.1 says: the Confluent framing of
    // `FingerprintStrategy::Id` and `AvroSoeFormat`, here per writer version.
    let v1_messages = arrow_avro_framed(1, V1, &[0, 2, 4, 6, 8], false);
    let v2_messages = arrow_avro_framed(2, V2, &[1, 3, 5, 7, 9], true);
    let mut all: Vec<(i64, Vec<u8>)> = v1_messages.into_iter().chain(v2_messages).collect();
    all.sort_by_key(|(o, _)| *o);
    for (o, payload) in all {
        d.feed(message(o, Some(payload))).await.unwrap();
    }
    let out = d.finish().unwrap();
    assert!(out.undecodable.is_empty(), "{:?}", out.undecodable);
    assert_eq!(out.batches.len(), 2, "one batch per writer schema");
    assert!(out.batches.iter().all(|b| b.num_rows() == 5));
    for b in &out.batches {
        let ids = column(b, "kafka_value_schema_id").as_primitive::<Int32Type>();
        let id = ids.value(0);
        assert!(ids.values().iter().all(|i| *i == id));
        let offsets = i64s(b, "kafka_offset");
        assert!(
            offsets.iter().all(|o| (o % 2 == 0) == (id == 1)),
            "{offsets:?}"
        );
        // v2 has its own field, v1 does not.
        assert_eq!(b.column_by_name("note").is_some(), id == 2);
    }
    assert_rows_are_their_messages(&out.batches);
    assert_eq!(rows_by_offset(&out.batches).len(), 10);
}

/// A decoder holds at most [`CHUNK_ROWS`] rows: the next message flushes them into a batch.
#[tokio::test]
async fn chunks_flush_at_8192_rows() {
    let mut d = decoder(Schemas::with(&[(1, V1)]));
    let payload = |o: i64| framed(1, V1, v1(o, "n"));
    for o in 0..(CHUNK_ROWS as i64 + 5) {
        d.feed(message(o, Some(payload(o)))).await.unwrap();
    }
    let out = d.finish().unwrap();
    assert_eq!(
        out.batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        [CHUNK_ROWS, 5]
    );
    assert_rows_are_their_messages(&out.batches);
}

// ---------------------------------------------------------------------------- isolation

#[tokio::test]
async fn truncated_message_isolated() {
    // Cut inside a string (arrow-avro waits for the rest and reports success) and inside a
    // varint (it reports a bad varint), and after the header.
    let good = |o: i64| framed(1, V1, v1(o, "a fairly long name"));
    let mut cut_in_string = good(2);
    cut_in_string.truncate(cut_in_string.len() - 3);
    let mut cut_in_varint = framed(1, V1, v1(1 << 40, "x"));
    cut_in_varint.truncate(HEADER_LEN + 2);
    let header_only = good(6)[..HEADER_LEN].to_vec();

    let mut d = decoder(Schemas::with(&[(1, V1)]));
    feed_all(
        &mut d,
        vec![
            message(0, Some(good(0))),
            message(1, Some(good(1))),
            message(2, Some(cut_in_string)),
            message(3, Some(good(3))),
            message(4, Some(cut_in_varint)),
            message(5, Some(good(5))),
            message(6, Some(header_only)),
            message(7, Some(good(7))),
        ],
    )
    .await;
    let out = d.finish().unwrap();
    assert_eq!(
        rows_by_offset(&out.batches),
        [(0, 0), (1, 1), (3, 3), (5, 5), (7, 7)]
    );
    assert_rows_are_their_messages(&out.batches);
    let bad: Vec<(i64, &str)> = out
        .undecodable
        .iter()
        .map(|u| (u.offset, u.reason.as_str()))
        .collect();
    assert_eq!(bad.len(), 3, "{bad:?}");
    assert_eq!(bad[0].0, 2);
    assert!(bad[0].1.contains("truncated"), "{bad:?}");
    assert_eq!(bad[1].0, 4);
    assert!(
        bad[1].1.contains("malformed") || bad[1].1.contains("truncated"),
        "{bad:?}"
    );
    assert_eq!(bad[2].0, 6);
    assert!(bad[2].1.contains("no Avro body"), "{bad:?}");
    assert!(out
        .undecodable
        .iter()
        .all(|u| u.deterministic && u.schema_id == Some(1)));
}

#[tokio::test]
async fn trailing_bytes_isolated() {
    let good = |o: i64| framed(1, V1, v1(o, "n"));
    // A second record's body after the first, a whole second message after the first, and one
    // stray byte.
    let mut second_body = good(10);
    second_body.extend_from_slice(&good(11)[HEADER_LEN..]);
    let mut second_message = good(12);
    second_message.extend(good(13));
    let mut stray = good(14);
    stray.push(0xff);

    let mut d = decoder(Schemas::with(&[(1, V1)]));
    feed_all(
        &mut d,
        vec![
            message(9, Some(good(9))),
            message(10, Some(second_body)),
            message(12, Some(second_message)),
            message(14, Some(stray)),
            message(15, Some(good(15))),
        ],
    )
    .await;
    let out = d.finish().unwrap();
    assert_eq!(rows_by_offset(&out.batches), [(9, 9), (15, 15)]);
    assert_rows_are_their_messages(&out.batches);
    let bad: Vec<i64> = out.undecodable.iter().map(|u| u.offset).collect();
    assert_eq!(bad, [10, 12, 14]);
    assert!(
        out.undecodable[1].reason.contains("2 records"),
        "{:?}",
        out.undecodable[1]
    );
    for u in &out.undecodable {
        assert!(
            u.reason.contains("after the record") || u.reason.contains("records, not one"),
            "{u:?}"
        );
    }
}

#[tokio::test]
async fn unknown_schema_id_isolated() {
    let schemas = Schemas::with(&[(1, V1)]);
    let mut d = decoder(schemas.clone());
    feed_all(
        &mut d,
        vec![
            message(0, Some(framed(1, V1, v1(0, "a")))),
            message(1, Some(framed(99, V1, v1(1, "b")))),
            message(2, Some(framed(1, V1, v1(2, "c")))),
            message(3, Some(framed(99, V1, v1(3, "d")))),
        ],
    )
    .await;
    let out = d.finish().unwrap();
    assert_eq!(rows_by_offset(&out.batches), [(0, 0), (2, 2)]);
    assert_rows_are_their_messages(&out.batches);
    assert_eq!(out.undecodable.len(), 2);
    for u in &out.undecodable {
        assert_eq!(u.schema_id, Some(99));
        assert!(!u.deterministic, "it may be registered later");
        assert!(u.reason.contains("schema id 99"), "{u:?}");
    }
    // Asked every time (the registry has its own short memory of a 404), the known id once.
    assert_eq!(schemas.asked.load(Ordering::SeqCst), 3);

    // Once registered, the id decodes.
    schemas
        .by_id
        .lock()
        .unwrap()
        .insert(99, Lookup::Avro(registered(99, V1)));
    d.feed(message(4, Some(framed(99, V1, v1(4, "e")))))
        .await
        .unwrap();
    assert_eq!(rows_by_offset(&d.finish().unwrap().batches), [(4, 4)]);
}

#[tokio::test]
async fn no_magic_byte_isolated() {
    let mut d = decoder(Schemas::with(&[(1, V1)]));
    feed_all(
        &mut d,
        vec![
            message(0, Some(framed(1, V1, v1(0, "a")))),
            message(1, Some(br#"{"id":1,"name":"json"}"#.to_vec())),
            message(2, Some(vec![0, 0, 0])),
            message(3, Some(Vec::new())),
            message(4, Some(framed(1, V1, v1(4, "b")))),
        ],
    )
    .await;
    let out = d.finish().unwrap();
    assert_eq!(rows_by_offset(&out.batches), [(0, 0), (4, 4)]);
    assert_rows_are_their_messages(&out.batches);
    let reasons: Vec<&str> = out.undecodable.iter().map(|u| u.reason.as_str()).collect();
    assert!(
        reasons[0].contains("0x7b") && reasons[0].contains("JSON"),
        "{reasons:?}"
    );
    assert!(reasons[1].contains("5-byte header"), "{reasons:?}");
    assert!(reasons[2].contains("empty"), "{reasons:?}");
    assert!(out.undecodable.iter().all(|u| u.schema_id.is_none()));

    // As rows for the data-quality table (§5.7): coordinates, bytes in base64, the reason.
    let r = out.rejects.unwrap();
    assert_eq!(r.len(), 3);
    assert!(r.columns.iter().all(Option::is_none));
    assert!(
        r.reasons[0].starts_with("avro: not Confluent-framed"),
        "{:?}",
        r.reasons
    );
    let schema = r.rows.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_key_b64",
            "kafka_value_b64",
            "kafka_value_schema_id"
        ]
    );
    assert_eq!(i64s(&r.rows, "kafka_offset"), [1, 2, 3]);
    assert_eq!(
        strings(&r.rows, "kafka_value_b64")[0].as_deref(),
        Some(
            base64::engine::general_purpose::STANDARD
                .encode(br#"{"id":1,"name":"json"}"#)
                .as_str()
        )
    );
    assert_eq!(
        strings(&r.rows, "kafka_key_b64")[0].as_deref(),
        Some("azE=")
    ); // "k1"
    assert!(column(&r.rows, "kafka_value_schema_id").is_null(0));
}

#[tokio::test]
async fn non_record_top_level_undecodable() {
    let schemas = Schemas::with(&[
        (5, r#""string""#),
        (6, r#"{"type":"array","items":"int"}"#),
        (7, r#"["null","string"]"#),
    ]);
    let mut d = decoder(schemas.clone());
    let string_value = {
        let mut v = vec![0, 0, 0, 0, 5];
        v.extend(
            apache_avro::to_avro_datum(
                &apache_avro::Schema::parse_str(r#""string""#).unwrap(),
                Avro::String("hello".into()),
            )
            .unwrap(),
        );
        v
    };
    feed_all(
        &mut d,
        vec![
            message(0, Some(string_value.clone())),
            message(1, Some(vec![0, 0, 0, 0, 6, 0])),
            message(2, Some(vec![0, 0, 0, 0, 7, 0])),
            message(3, Some(string_value)),
        ],
    )
    .await;
    let out = d.finish().unwrap();
    assert!(out.batches.is_empty());
    let reasons: Vec<&str> = out.undecodable.iter().map(|u| u.reason.as_str()).collect();
    assert!(
        reasons[0].contains("not a record at the top level but of type string"),
        "{reasons:?}"
    );
    assert!(reasons[1].contains("of type array"), "{reasons:?}");
    assert!(reasons[2].contains("a union"), "{reasons:?}");
    assert!(out.undecodable.iter().all(|u| u.deterministic));
    // A schema's shape never changes: asked once per id.
    assert_eq!(schemas.asked.load(Ordering::SeqCst), 3);
}

/// Isolation keeps every row with its own message: bad messages of several kinds, interleaved
/// with two writer schemas, leave each good row's metadata and values its own.
#[tokio::test]
async fn metadata_aligned_after_isolation() {
    let mut d = decoder(Schemas::with(&[(1, V1), (2, V2)]));
    let mut messages = Vec::new();
    for o in 0..40i64 {
        let payload = match o % 10 {
            3 => {
                let mut p = framed(1, V1, v1(o, "truncated"));
                p.truncate(p.len() - 2);
                p
            }
            5 => {
                let mut p = framed(2, V2, v2(o, "trailing", None));
                p.push(1);
                p
            }
            7 => b"{}".to_vec(),
            n if n % 2 == 0 => framed(1, V1, v1(o, "v1")),
            _ => framed(2, V2, v2(o, "v2", Some("note"))),
        };
        messages.push(message(o, Some(payload)));
    }
    feed_all(&mut d, messages).await;
    let out = d.finish().unwrap();
    assert_rows_are_their_messages(&out.batches);
    let good: Vec<i64> = rows_by_offset(&out.batches).iter().map(|r| r.0).collect();
    let expected: Vec<i64> = (0..40).filter(|o| ![3, 5, 7].contains(&(o % 10))).collect();
    assert_eq!(good, expected);
    let mut bad: Vec<i64> = out.undecodable.iter().map(|u| u.offset).collect();
    bad.sort();
    assert_eq!(bad, [3, 5, 7, 13, 15, 17, 23, 25, 27, 33, 35, 37]);
    // Each row's writer schema id is the one its message named.
    for b in &out.batches {
        let ids = column(b, "kafka_value_schema_id").as_primitive::<Int32Type>();
        for (i, o) in i64s(b, "kafka_offset").iter().enumerate() {
            assert_eq!(ids.value(i), if o % 2 == 0 { 1 } else { 2 });
        }
    }
    assert_eq!(out.rejects.unwrap().len(), 12);
}

/// A chunk that cannot be flushed as a whole is flushed one message at a time, so the row that
/// fails is one message's and the others keep theirs.
#[tokio::test]
async fn a_chunk_that_cannot_be_flushed_is_isolated() {
    /// arrow-avro, except that a flush holding the poisoned message fails as a whole.
    struct Poisoned;
    struct PoisonedDecoder {
        inner: Box<dyn Decode>,
        poisoned: bool,
    }
    impl DecodeFactory for Poisoned {
        fn build(&self, id: u32, schema: &str) -> std::result::Result<Box<dyn Decode>, String> {
            Ok(Box::new(PoisonedDecoder {
                inner: ArrowAvro.build(id, schema)?,
                poisoned: false,
            }))
        }
    }
    impl Decode for PoisonedDecoder {
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }
        fn decode_one(&mut self, framed: &[u8]) -> std::result::Result<(), String> {
            self.inner.decode_one(framed)?;
            self.poisoned |= framed.ends_with(b"poison");
            Ok(())
        }
        fn flush(&mut self) -> std::result::Result<Option<RecordBatch>, String> {
            let b = self.inner.flush()?;
            if std::mem::take(&mut self.poisoned) {
                return Err("cannot build the column".into());
            }
            Ok(b)
        }
    }
    let mut d = KafkaDecoder::with_factory(
        "orders",
        "kafka_",
        Schemas::with(&[(1, V1)]),
        Arc::new(Poisoned),
    );
    feed_all(
        &mut d,
        (0..5)
            .map(|o| {
                let name = if o == 2 { "poison" } else { "fine" };
                message(o, Some(framed(1, V1, v1(o, name))))
            })
            .collect(),
    )
    .await;
    let out = d.finish().unwrap();
    assert_eq!(
        rows_by_offset(&out.batches),
        [(0, 0), (1, 1), (3, 3), (4, 4)]
    );
    assert_rows_are_their_messages(&out.batches);
    assert_eq!(out.undecodable.len(), 1);
    assert_eq!(out.undecodable[0].offset, 2);
    assert!(out.undecodable[0]
        .reason
        .contains("cannot build the column"));
}

// ------------------------------------------------------------- what holds the pipeline

#[tokio::test]
async fn metadata_prefix_collision_blocks() {
    const CLASH: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"kafka_offset","type":"long"}]}"#;
    let payload = framed(
        8,
        CLASH,
        Avro::Record(vec![
            ("id".into(), Avro::Long(1)),
            ("kafka_offset".into(), Avro::Long(77)),
        ]),
    );
    let mut d = decoder(Schemas::with(&[(8, CLASH)]));
    let m = message(0, Some(payload.clone()));
    let NotFed {
        error,
        message: back,
    } = d.feed(m.clone()).await.unwrap_err();
    assert_eq!(back, m, "the message is handed back untouched");
    assert!(
        matches!(&error, Error::KafkaMetadataCollision { schema_id: 8, field, prefix } if field == "kafka_offset" && prefix == "kafka_"),
        "{error:?}"
    );
    assert_eq!(error.kafka_class(), KafkaClass::BlockingPipeline);
    assert!(error.to_string().contains("metadata_prefix"), "{error}");
    // Every message of that schema, every time, until the prefix changes.
    assert!(d.feed(m.clone()).await.is_err());
    assert!(d.is_empty());

    // Under another prefix the field is payload, and the metadata has its own names.
    let mut d = KafkaDecoder::new("orders", "meta_", Schemas::with(&[(8, CLASH)]));
    d.feed(m).await.unwrap();
    let b = &d.finish().unwrap().batches[0];
    assert_eq!(i64s(b, "kafka_offset"), [77]);
    assert_eq!(i64s(b, "meta_offset"), [0]);
}

/// A registry that cannot answer hands the message back untouched; fed again once it can,
/// the message decodes as if for the first time.
#[tokio::test]
async fn a_registry_outage_hands_the_message_back() {
    let schemas = Schemas::with(&[(1, V1)]);
    schemas.down.store(true, Ordering::SeqCst);
    let mut d = decoder(schemas.clone());
    let m = message(0, Some(framed(1, V1, v1(0, "a"))));
    let NotFed { error, message } = d.feed(m.clone()).await.unwrap_err();
    assert_eq!(message, m);
    assert_eq!(error.kafka_class(), KafkaClass::Transient);
    assert!(d.is_empty());

    schemas.down.store(false, Ordering::SeqCst);
    d.feed(message).await.unwrap();
    assert_eq!(rows_by_offset(&d.finish().unwrap().batches), [(0, 0)]);
}

// ---------------------------------------------------------------------------- measuring

#[tokio::test]
async fn decoded_bytes_measured_by_slice_not_capacity() {
    let mut d = decoder(Schemas::with(&[(1, V1)]));
    let messages: Vec<KafkaMessage> = (0..3)
        .map(|o| message(o, Some(framed(1, V1, v1(o, "name")))))
        .collect();
    let raw: u64 = messages.iter().map(KafkaMessage::raw_len).sum();
    feed_all(&mut d, messages).await;
    // Nothing flushed yet: the rows inside the decoder count at 4× their raw size.
    assert_eq!(d.decoded_bytes(), 4 * raw);

    let out = d.finish().unwrap();
    let b = &out.batches[0];
    let used: u64 = b
        .columns()
        .iter()
        .map(|c| c.to_data().get_slice_memory_size().unwrap() as u64)
        .sum();
    let capacity: u64 = b
        .columns()
        .iter()
        .map(|c| c.get_array_memory_size() as u64)
        .sum();
    assert_eq!(out.decoded_bytes, used);
    // arrow-avro sizes its builders for 1024 rows: three rows use a sliver of that.
    assert!(capacity > 10 * used, "used {used}, capacity {capacity}");

    // Once a chunk has flushed, the rows still inside a decoder count at the ratio it showed.
    let mut d = decoder(Schemas::with(&[(1, V1)]));
    for o in 0..(CHUNK_ROWS as i64) {
        d.feed(message(o, Some(framed(1, V1, v1(o, "name")))))
            .await
            .unwrap();
    }
    let flushed = d.decoded_bytes();
    let next = message(1 << 20, Some(framed(1, V1, v1(1 << 20, "name"))));
    let next_raw = next.raw_len();
    d.feed(next).await.unwrap();
    let per_raw = d.out.flushed_bytes as f64 / d.out.flushed_raw as f64;
    assert_eq!(
        d.decoded_bytes(),
        flushed + (next_raw as f64 * per_raw) as u64
    );
    assert!(per_raw < 4.0 * 4.0, "{per_raw}");
}

#[tokio::test]
async fn discard_drops_rows_inside_decoders() {
    let schemas = Schemas::with(&[(1, V1)]);
    let mut d = decoder(schemas.clone());
    feed_all(
        &mut d,
        vec![
            message(0, Some(framed(1, V1, v1(0, "a")))),
            message(1, None),
            message(2, Some(b"x".to_vec())),
        ],
    )
    .await;
    assert!(!d.is_empty());
    d.discard();
    assert!(d.is_empty());
    d.feed(message(3, Some(framed(1, V1, v1(3, "b")))))
        .await
        .unwrap();
    let out = d.finish().unwrap();
    assert_eq!(rows_by_offset(&out.batches), [(3, 3)]);
    assert_eq!((out.tombstones, out.undecodable.len()), (0, 0));
    assert_eq!(
        schemas.asked.load(Ordering::SeqCst),
        1,
        "the schema stays known"
    );
}

// ------------------------------------------------------- arrow-avro go/no-go evidence (§5.3)

/// Why the strict check and the isolation exist (§0.3 fact 2): arrow-avro's `Decoder` reports
/// a truncated body as success, keeps waiting for the rest, and then reads the next message's
/// bytes as that rest.
#[test]
fn arrow_avro_swallows_a_truncated_body() {
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    store
        .set(Fingerprint::Id(1), AvroSchema::new(V1.to_string()))
        .unwrap();
    let mut raw = ReaderBuilder::new()
        .with_writer_schema_store(store)
        .build_decoder()
        .unwrap();
    let mut truncated = framed(1, V1, v1(1, "a name"));
    truncated.truncate(truncated.len() - 2);
    // "Success", having read only the header, with no row.
    assert_eq!(raw.decode(&truncated).unwrap(), HEADER_LEN);
    assert_eq!(raw.capacity(), raw.batch_size());
    // The next message's bytes are taken for the missing rest.
    let next = framed(1, V1, v1(2, "b"));
    let read = raw.decode(&next);
    let flushed = raw.flush();
    assert!(
        read.is_err()
            || !matches!(&flushed, Ok(Some(b)) if b.num_rows() == 1 && i64s(b, "id") == [2]),
        "arrow-avro decoded the message after a truncated one cleanly: {read:?} {flushed:?}"
    );

    // ddi's check refuses the truncated one and decodes the next on a fresh decoder.
    let mut strict = ArrowAvro.build(1, V1).unwrap();
    assert!(strict
        .decode_one(&truncated)
        .unwrap_err()
        .contains("truncated"));
    let mut strict = ArrowAvro.build(1, V1).unwrap();
    strict.decode_one(&next).unwrap();
    assert_eq!(i64s(&strict.flush().unwrap().unwrap(), "id"), [2]);
}

/// A row that fails half-way leaves the fields it did decode in the builders: a flush after it
/// fails or misaligns, so the decoder is replaced, never reused.
#[test]
fn arrow_avro_keeps_part_of_a_failed_row() {
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    store
        .set(Fingerprint::Id(1), AvroSchema::new(V1.to_string()))
        .unwrap();
    let mut raw = ReaderBuilder::new()
        .with_writer_schema_store(store)
        .build_decoder()
        .unwrap();
    // `id` = 1, then a string of length -1.
    let bad = [0, 0, 0, 0, 1, 2, 1];
    assert!(raw.decode(&bad).is_err());
    assert_eq!(raw.capacity(), raw.batch_size(), "no row counted");
    let good = framed(1, V1, v1(2, "x"));
    let after = raw.decode(&good).and_then(|_| raw.flush());
    assert!(
        !matches!(&after, Ok(Some(b)) if b.num_rows() == 1 && i64s(b, "id") == [2]),
        "a failed row left nothing behind: {after:?}"
    );
}

/// Schemas as Java producers register them decode: the attributes Avro's Java tooling and
/// Kafka Connect add, docs, defaults, aliases, namespaces, a named type used again by name.
#[tokio::test]
async fn java_and_connect_schemas_decode() {
    const JAVA: &str = r#"{"type":"record","name":"Event","namespace":"com.acme.events",
        "doc":"An event","connect.name":"com.acme.events.Event","fields":[
        {"name":"id","type":{"type":"string","avro.java.string":"String"},"doc":"the id"},
        {"name":"at","type":{"type":"long","logicalType":"timestamp-millis","connect.version":1,
            "connect.name":"org.apache.kafka.connect.data.Timestamp"}},
        {"name":"from","type":{"type":"record","name":"Address","fields":[
            {"name":"city","type":["null",{"type":"string","avro.java.string":"String"}],"default":null}]}},
        {"name":"to","type":["null","Address"],"default":null,"aliases":["destination"]},
        {"name":"kind","type":{"type":"enum","name":"Kind","symbols":["A","B"],"default":"A"},"default":"A"},
        {"name":"tags","type":{"type":"array","items":{"type":"string","avro.java.string":"String"}},"default":[]}]}"#;
    let address = |c: Option<&str>| {
        Avro::Record(vec![(
            "city".into(),
            match c {
                None => Avro::Union(0, Box::new(Avro::Null)),
                Some(c) => Avro::Union(1, Box::new(Avro::String(c.into()))),
            },
        )])
    };
    let row = Avro::Record(vec![
        ("id".into(), Avro::String("e-1".into())),
        ("at".into(), Avro::Long(1_700_000_000_000)),
        ("from".into(), address(Some("Berlin"))),
        ("to".into(), Avro::Union(1, Box::new(address(None)))),
        ("kind".into(), Avro::Enum(1, "B".into())),
        ("tags".into(), Avro::Array(vec![Avro::String("t".into())])),
    ]);
    let mut d = decoder(Schemas::with(&[(11, JAVA)]));
    d.feed(message(0, Some(framed(11, &physical(JAVA), row))))
        .await
        .unwrap();
    let out = d.finish().unwrap();
    assert!(out.undecodable.is_empty(), "{:?}", out.undecodable);
    let b = &out.batches[0];
    assert_eq!(strings(b, "id"), [Some("e-1".into())]);
    assert_eq!(strings(b, "kind"), [Some("B".into())]);
    let to = column(b, "to").as_struct();
    assert!(to.column_by_name("city").unwrap().is_null(0));
    assert!(!to.is_null(0));
}

/// Avro allows a record to contain itself; Arrow has no such type. arrow-avro refuses such a
/// schema when the decoder is built (its name is registered only after its fields,
/// codec.rs:1499-1522), so every message of it is undecodable, with the reason, rather than the
/// process overflowing its stack.
#[tokio::test]
async fn a_recursive_schema_is_undecodable() {
    const LIST: &str = r#"{"type":"record","name":"Node","fields":[
        {"name":"v","type":"long"},
        {"name":"next","type":["null","Node"]}]}"#;
    let row = Avro::Record(vec![
        ("v".into(), Avro::Long(1)),
        ("next".into(), Avro::Union(0, Box::new(Avro::Null))),
    ]);
    let mut d = decoder(Schemas::with(&[(12, LIST)]));
    d.feed(message(0, Some(framed(12, LIST, row))))
        .await
        .unwrap();
    let out = d.finish().unwrap();
    assert_eq!(out.undecodable.len(), 1);
    assert!(
        out.undecodable[0]
            .reason
            .contains("cannot decode writer schema 12"),
        "{:?}",
        out.undecodable
    );
}

/// An array whose items take no bytes (here `null`) lets the producer's block count alone
/// decide how long arrow-avro loops, with nothing to read that would run out: such a writer
/// schema is refused rather than decoded.
#[tokio::test]
async fn an_array_of_zero_width_items_is_refused() {
    const NULLS: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"r","type":{"type":"record","name":"In","fields":[
            {"name":"nothing","type":{"type":"array","items":"null"}}]}}]}"#;
    let mut d = decoder(Schemas::with(&[(13, NULLS)]));
    // id = 1, then a block count of 2^62 null items.
    let mut hostile = vec![0, 0, 0, 0, 13, 2];
    hostile.extend(zigzag_varint(1 << 62));
    hostile.push(0);
    d.feed(message(0, Some(hostile))).await.unwrap();
    let out = d.finish().unwrap();
    assert_eq!(out.undecodable.len(), 1);
    assert!(
        out.undecodable[0].reason.contains("r.nothing"),
        "{:?}",
        out.undecodable
    );
}

/// Lengths and counts on the wire are the producer's to write, and arrow-avro checks a length
/// against the bytes that are there before it takes them (reader/cursor.rs, `get_bytes`), and
/// reads an item per count until the bytes run out (reader/record.rs, `process_blockwise`). So a
/// hostile length or count is an undecodable message, never an allocation of its size: the
/// largest single allocation while each is fed is a few kilobytes, and the good message after
/// them decodes. A debug build's overflow check on a block count of `i64::MIN` panics inside
/// arrow-avro; that is caught, and the message is the one to blame (§5.4, "hostile inputs").
#[tokio::test]
async fn huge_length_prefix_errors_without_allocation() {
    const HOSTILE: &str = r#"{"type":"record","name":"H","fields":[
        {"name":"id","type":"long"},
        {"name":"s","type":"string"},
        {"name":"by","type":"bytes"},
        {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
        {"name":"arr","type":{"type":"array","items":"long"}},
        {"name":"strs","type":{"type":"array","items":"string"}},
        {"name":"m","type":{"type":"map","values":"long"}}]}"#;
    // id = 1, then valid values for the fields before the hostile one, then the hostile bytes.
    let long = zigzag_varint;
    let cat = |parts: &[Vec<u8>]| parts.concat();
    let before = [
        cat(&[long(1), b"x".to_vec()]), // s
        long(0),                        // by
        cat(&[long(1), vec![1]]),       // dec
        long(0),                        // arr
        long(0),                        // strs
    ];
    let body = |fields_before: usize, hostile: Vec<u8>| {
        cat(&[
            vec![0, 0, 0, 0, 14],
            long(1),
            before[..fields_before].concat(),
            hostile,
        ])
    };
    let hostile: Vec<(&str, Vec<u8>)> = vec![
        (
            "a string of 2^62 bytes",
            body(0, cat(&[long(1 << 62), b"abc".to_vec()])),
        ),
        ("a string of i64::MAX bytes", body(0, long(i64::MAX))),
        ("a string of -5 bytes", body(0, long(-5))),
        (
            "bytes of 2^40",
            body(1, cat(&[long(1 << 40), vec![1, 2, 3]])),
        ),
        ("a decimal of 2^50 bytes", body(2, long(1 << 50))),
        (
            "2^62 longs",
            body(3, cat(&[long(1 << 62), long(7), long(8)])),
        ),
        ("i64::MAX longs", body(3, long(i64::MAX))),
        (
            "a negative block of i64::MIN items",
            body(3, cat(&[long(i64::MIN), long(16), long(1)])),
        ),
        (
            "2^62 strings of 2^62 bytes",
            body(4, cat(&[long(1 << 62), long(1 << 62)])),
        ),
        (
            "a map of 2^62 entries",
            body(5, cat(&[long(1 << 62), long(1), b"k".to_vec(), long(1)])),
        ),
        ("a 10-byte varint that overflows", body(0, vec![0xff; 10])),
        ("a varint that never ends", body(0, vec![0x80; 64])),
    ];
    let good = |o: i64| {
        let row = Avro::Record(vec![
            ("id".into(), Avro::Long(o)),
            ("s".into(), Avro::String("s".into())),
            ("by".into(), Avro::Bytes(vec![1])),
            ("dec".into(), Avro::Bytes(vec![1])),
            ("arr".into(), Avro::Array(vec![Avro::Long(1)])),
            ("strs".into(), Avro::Array(vec![])),
            ("m".into(), Avro::Map(Default::default())),
        ]);
        framed(14, &physical(HOSTILE), row)
    };

    // The probe sees an allocation of the size this test is about.
    let (_, largest) = alloc_probe::largest_during(|| vec![0u8; 4 << 20]);
    assert!(
        largest >= 4 << 20,
        "the probe missed an allocation: {largest}"
    );

    // The decoder alone.
    for (what, payload) in &hostile {
        let mut dec = ArrowAvro.build(14, HOSTILE).unwrap();
        let (result, largest) = alloc_probe::largest_during(|| dec.decode_one(payload));
        assert!(result.is_err(), "{what}: decoded");
        assert!(
            largest < 64 << 10,
            "{what}: allocated {largest} bytes at once"
        );
    }

    // Through the pipeline's decoder, between good messages: each hostile one is undecodable,
    // the good ones keep their rows.
    let mut d = decoder(Schemas::with(&[(14, HOSTILE)]));
    d.feed(message(0, Some(good(0)))).await.unwrap();
    for (i, (what, payload)) in hostile.iter().enumerate() {
        let o = 2 * i as i64 + 1;
        let m = message(o, Some(payload.clone()));
        // A current-thread runtime: the feed runs on this thread.
        alloc_probe::reset();
        d.feed(m).await.unwrap();
        let largest = alloc_probe::largest();
        assert!(
            largest < 1 << 20,
            "{what}: allocated {largest} bytes at once"
        );
        d.feed(message(o + 1, Some(good(o + 1)))).await.unwrap();
    }
    let out = d.finish().unwrap();
    assert_eq!(
        out.undecodable.len(),
        hostile.len(),
        "{:?}",
        out.undecodable
    );
    assert!(out
        .undecodable
        .iter()
        .all(|u| u.offset % 2 == 1 && u.deterministic));
    let good_offsets: Vec<i64> = rows_by_offset(&out.batches).iter().map(|r| r.0).collect();
    assert_eq!(
        good_offsets,
        (0..=2 * hostile.len() as i64)
            .step_by(2)
            .collect::<Vec<_>>()
    );
    assert_rows_are_their_messages(&out.batches);
}

/// A record that takes no bytes on the wire (no field, or only `null` ones) is a 5-byte header
/// alone, which arrow-avro's `Decoder` never decodes: it waits for a first byte of the body.
/// Such a writer schema is refused with that reason, rather than each message reading as
/// truncated.
#[tokio::test]
async fn a_record_of_zero_bytes_is_undecodable_with_its_reason() {
    const EMPTY: &str = r#"{"type":"record","name":"Nothing","fields":[
        {"name":"n","type":"null"}]}"#;
    let mut d = decoder(Schemas::with(&[(15, EMPTY)]));
    d.feed(message(0, Some(vec![0, 0, 0, 0, 15])))
        .await
        .unwrap();
    let out = d.finish().unwrap();
    assert_eq!(out.undecodable.len(), 1);
    assert!(
        out.undecodable[0].reason.contains("zero bytes"),
        "{:?}",
        out.undecodable
    );
    assert!(out.undecodable[0].deterministic);

    // Why: arrow-avro takes the header and waits.
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    store
        .set(Fingerprint::Id(15), AvroSchema::new(EMPTY.to_string()))
        .unwrap();
    let mut raw = ReaderBuilder::new()
        .with_writer_schema_store(store)
        .build_decoder()
        .unwrap();
    assert_eq!(raw.decode(&[0, 0, 0, 0, 15]).unwrap(), HEADER_LEN);
    assert!(raw.batch_is_empty(), "no row from a header alone");
}

// --------------------------------------------------------------------------- helpers

/// The schema with every `logicalType` removed: the form its values travel in.
fn physical(schema: &str) -> String {
    fn strip(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(o) => {
                o.remove("logicalType");
                o.values_mut().for_each(strip);
            }
            serde_json::Value::Array(a) => a.iter_mut().for_each(strip),
            _ => {}
        }
    }
    let mut v: serde_json::Value = serde_json::from_str(schema).unwrap();
    strip(&mut v);
    v.to_string()
}

/// The shortest big-endian two's complement of `v`, as Java's `BigDecimal` serializers write
/// a `bytes` decimal.
fn minimal_be(v: i128) -> Vec<u8> {
    let bytes = v.to_be_bytes();
    let mut start = 0;
    while start < 15 {
        let (b, next) = (bytes[start], bytes[start + 1]);
        if (b == 0 && next & 0x80 == 0) || (b == 0xff && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

/// `v` sign-extended to `n` bytes, as a `fixed(n)` decimal holds it.
fn sign_extended(v: i128, n: usize) -> Vec<u8> {
    v.to_be_bytes()[16 - n..].to_vec()
}

fn duration_bytes(months: u32, days: u32, millis: u32) -> Vec<u8> {
    [months, days, millis]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect()
}

fn zigzag_varint(v: i64) -> Vec<u8> {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    let mut out = Vec::new();
    loop {
        let byte = (z & 0x7f) as u8;
        z >>= 7;
        if z == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// Messages at `offsets` written by arrow-avro's own writer with the Confluent framing of
/// `FingerprintStrategy::Id(id)`, the Avro schema given verbatim (`avro.schema`); each row's
/// `id` is its offset.
fn arrow_avro_framed(id: u32, avro: &str, offsets: &[i64], with_note: bool) -> Vec<(i64, Vec<u8>)> {
    use arrow_avro::schema::{FingerprintStrategy, SCHEMA_METADATA_KEY};
    use arrow_avro::writer::{format::AvroSoeFormat, WriterBuilder};
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(offsets.to_vec())),
        Arc::new(StringArray::from_iter_values(
            offsets.iter().map(|o| format!("n{o}")),
        )),
    ];
    if with_note {
        fields.push(Field::new("note", DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from_iter(
            offsets
                .iter()
                .map(|o| (o % 3 == 0).then(|| format!("note {o}"))),
        )));
    }
    let schema = Schema::new_with_metadata(
        fields,
        [(SCHEMA_METADATA_KEY.to_string(), avro.to_string())].into(),
    );
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap();
    let mut encoder = WriterBuilder::new(schema)
        .with_fingerprint_strategy(FingerprintStrategy::Id(id))
        .build_encoder::<AvroSoeFormat>()
        .unwrap();
    encoder.encode(&batch).unwrap();
    let rows = encoder.flush();
    offsets
        .iter()
        .zip(rows.iter())
        .map(|(o, r)| (*o, r.to_vec()))
        .collect()
}

/// The largest single allocation the current thread asks for, so that a test can show a hostile
/// length was not allocated (`huge_length_prefix_errors_without_allocation`). A pass-through to
/// the system allocator, which is what the test binary would use anyway; per thread, so tests
/// running beside each other do not see each other's allocations.
mod alloc_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    pub struct Largest;

    thread_local! {
        static LARGEST: Cell<usize> = const { Cell::new(0) };
    }

    fn note(size: usize) {
        // Not during the thread's teardown, when the slot may be gone.
        let _ = LARGEST.try_with(|l| {
            if size > l.get() {
                l.set(size)
            }
        });
    }

    unsafe impl GlobalAlloc for Largest {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            note(layout.size());
            System.alloc(layout)
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            note(layout.size());
            System.alloc_zeroed(layout)
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            note(new_size);
            System.realloc(ptr, layout, new_size)
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout)
        }
    }

    /// Forgets what this thread allocated so far.
    pub fn reset() {
        LARGEST.with(|l| l.set(0));
    }

    /// The largest single allocation this thread asked for since [`reset`].
    pub fn largest() -> usize {
        LARGEST.with(Cell::get)
    }

    /// `f`'s result, and the largest single allocation this thread asked for while it ran.
    pub fn largest_during<R>(f: impl FnOnce() -> R) -> (R, usize) {
        reset();
        let r = f();
        (r, largest())
    }
}

#[global_allocator]
static ALLOCATIONS: alloc_probe::Largest = alloc_probe::Largest;
