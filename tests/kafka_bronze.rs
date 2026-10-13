//! The production shape of a bronze table: one envelope every topic shares, its payload kept as
//! kafka-delta-ingest's JSON text, and the same twelve columns the 22 raw tables have
//! (docs/kafka-design.md §6.5, M-1).
//!
//! Ignored unless a broker is there: see tests/kafka_common/mod.rs for how to run them.

#![cfg(feature = "kafka")]

mod common;
mod kafka_common;

use apache_avro::types::Value;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use deltalake::kernel::{DataType as Delta, PrimitiveType, StructField};

use kafka_common::*;

/// The envelope every production topic carries, with an invented payload record.
const ENVELOPE: &str = r#"{"type":"record","name":"Envelope","namespace":"ddi.test","fields":[
  {"name":"messageId","type":"string"},
  {"name":"messageTime","type":"long"},
  {"name":"correlationId","type":["null","string"],"default":null},
  {"name":"producer","type":"string"},
  {"name":"producerVersion","type":"string"},
  {"name":"producerHost","type":"string"},
  {"name":"data","type":{"type":"record","name":"Payload","fields":[
    {"name":"orderId","type":"string"},
    {"name":"captureGrossAmount","type":{"type":"record","name":"Money","fields":[
      {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
      {"name":"currency","type":"string"}]}},
    {"name":"additionalFees","type":["null","string"],"default":null},
    {"name":"tags","type":{"type":"array","items":"string"}},
    {"name":"attributes","type":{"type":"map","values":"string"}},
    {"name":"createdAt","type":"long"}
  ]}}
]}"#;

/// The transform every one of the 22 streams gets: the envelope in lower case, the Kafka
/// coordinates, and `ingest_date`, the UTC date of the producer's `messageTime`.
pub const BRONZE_SQL: &str = r#"SELECT
  "messageId" AS messageid,
  to_timestamp_millis("messageTime") AS messagetime,
  "correlationId" AS correlationid,
  producer,
  "producerVersion" AS producerversion,
  "producerHost" AS producerhost,
  data,
  kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
  to_char(to_timestamp_millis("messageTime"), '%Y-%m-%d') AS ingest_date
FROM source"#;

fn string(name: &str) -> StructField {
    StructField::new(name, Delta::Primitive(PrimitiveType::String), true)
}

fn bronze_columns() -> Vec<StructField> {
    vec![
        string("messageid"),
        StructField::new(
            "messagetime",
            Delta::Primitive(PrimitiveType::Timestamp),
            true,
        ),
        string("correlationid"),
        string("producer"),
        string("producerversion"),
        string("producerhost"),
        string("data"),
        string("kafka_topic"),
        StructField::new(
            "kafka_partition",
            Delta::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new("kafka_offset", Delta::Primitive(PrimitiveType::Long), true),
        StructField::new(
            "kafka_timestamp",
            Delta::Primitive(PrimitiveType::Timestamp),
            true,
        ),
        string("ingest_date"),
    ]
}

fn s(v: &str) -> Value {
    Value::String(v.into())
}

fn message(id: &str, millis: i64, correlation: Option<&str>, fee: Option<&str>) -> Value {
    let union = |v: Option<&str>| match v {
        Some(x) => Value::Union(1, Box::new(s(x))),
        None => Value::Union(0, Box::new(Value::Null)),
    };
    // Entries put in on purpose out of order: kdi wrote its JSON with sorted keys.
    let attributes = Value::Map(
        [("b".to_string(), s("2")), ("a".to_string(), s("1"))]
            .into_iter()
            .collect(),
    );
    let money = Value::Record(vec![
        (
            "amount".into(),
            Value::Decimal(apache_avro::Decimal::from(vec![1u8, 44])),
        ),
        ("currency".into(), s("EUR")),
    ]);
    let payload = Value::Record(vec![
        ("orderId".into(), s(&format!("o-{id}"))),
        ("captureGrossAmount".into(), money),
        ("additionalFees".into(), union(fee)),
        ("tags".into(), Value::Array(vec![s("x"), s("y")])),
        ("attributes".into(), attributes),
        ("createdAt".into(), Value::Long(millis)),
    ]);
    Value::Record(vec![
        ("messageId".into(), s(id)),
        ("messageTime".into(), Value::Long(millis)),
        ("correlationId".into(), union(correlation)),
        ("producer".into(), s("cart2order")),
        ("producerVersion".into(), s("1.2.3")),
        ("producerHost".into(), s("host-1")),
        ("data".into(), payload),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn the_production_envelope_lands_as_the_twelve_bronze_columns() {
    let f = Fixture::bare(1).await;
    create_table_with(&f.target, bronze_columns(), &["ingest_date"], &[]).await;
    create_dq(&f.dq).await;
    let id = register(&f.subject(), ENVELOPE).await;
    let producer = Producer::new();
    // Either side of midnight UTC, and the second message's correlation id is NULL.
    let late = 1_791_935_999_999i64; // 2026-10-13T23:59:59.999Z
    let early = 1_791_936_000_000i64; // 2026-10-14T00:00:00.000Z
    for (m, value) in [
        ("m-1", message("m-1", late, Some("c-1"), Some("5.00"))),
        ("m-2", message("m-2", early, None, None)),
    ] {
        let _ = m;
        producer
            .send(&f.topic, 0, None, Some(&framed(ENVELOPE, id, value)))
            .await;
    }
    let toml = f.toml(
        "bronze",
        "",
        &format!(
            "kdi_compat = true\nkdi_null_text = false\ntransform_sql = \"\"\"{BRONZE_SQL}\"\"\""
        ),
    );
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    for _ in 0..1000 {
        if p.step().await.unwrap() == StepOutcome::CaughtUp {
            break;
        }
    }
    drop(p);

    let mut rows = Vec::new();
    for b in scan(&f.target).await {
        let col = |n: &str| {
            b.column_by_name(n)
                .unwrap_or_else(|| panic!("column {n}"))
                .clone()
        };
        for i in 0..b.num_rows() {
            rows.push((
                text(col("messageid").as_ref(), i).unwrap(),
                text(col("correlationid").as_ref(), i),
                text(col("ingest_date").as_ref(), i).unwrap(),
                text(col("data").as_ref(), i).unwrap(),
                text(col("kafka_offset").as_ref(), i).unwrap(),
                text(col("messagetime").as_ref(), i).unwrap(),
            ));
        }
        assert_eq!(b.num_columns(), 12, "{:?}", b.schema());
    }
    rows.sort();
    assert_eq!(rows.len(), 2, "{rows:?}");

    // The partition value is the UTC date of the producer's time, whatever Kafka's clock said.
    assert_eq!(rows[0].2, "2026-10-13");
    assert_eq!(rows[1].2, "2026-10-14");
    // A union's null is a NULL, never the text "null" (kdi_null_text = false).
    assert_eq!(rows[0].1.as_deref(), Some("c-1"));
    assert_eq!(rows[1].1, None);
    assert!(
        rows[0].5.starts_with("2026-10-13T23:59:59.999"),
        "{}",
        rows[0].5
    );

    // `data` is kdi's text: keys sorted at every depth, no blanks, a decimal as its two's
    // complement bytes, a null as null, a number kept a number.
    assert_eq!(
        rows[0].3,
        r#"{"additionalFees":"5.00","attributes":{"a":"1","b":"2"},"captureGrossAmount":{"amount":[1,44],"currency":"EUR"},"createdAt":1791935999999,"orderId":"o-m-1","tags":["x","y"]}"#
    );
    assert_eq!(
        rows[1].3,
        r#"{"additionalFees":null,"attributes":{"a":"1","b":"2"},"captureGrossAmount":{"amount":[1,44],"currency":"EUR"},"createdAt":1791936000000,"orderId":"o-m-2","tags":["x","y"]}"#
    );
}
