//! The production bronze shape, shared by the tests that read it (docs/kafka-design.md §6.5):
//! the envelope every topic carries, the transform every stream gets, and the twelve columns.

#![allow(dead_code)]

use apache_avro::types::Value;
use deltalake::kernel::{DataType as Delta, PrimitiveType, StructField};

/// The envelope every production topic carries, with an invented payload record.
pub const ENVELOPE: &str = r#"{"type":"record","name":"Envelope","namespace":"ddi.test","fields":[
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

pub const ENVELOPE_V2: &str = r#"{"type":"record","name":"Envelope","namespace":"ddi.test","fields":[
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
    {"name":"createdAt","type":"long"},
    {"name":"channel","type":["null","string"],"default":null}
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
  kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, kafka_value_schema_id,
  to_char(to_timestamp_millis("messageTime"), '%Y-%m-%d') AS ingest_date
FROM source"#;

pub fn string(name: &str) -> StructField {
    StructField::new(name, Delta::Primitive(PrimitiveType::String), true)
}

pub fn bronze_columns() -> Vec<StructField> {
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

pub fn s(v: &str) -> Value {
    Value::String(v.into())
}

pub fn message(id: &str, millis: i64, correlation: Option<&str>, fee: Option<&str>) -> Value {
    build(id, millis, correlation, fee, None)
}

/// A message of [`ENVELOPE_V2`]: the payload also says which channel it came through.
pub fn message_v2(
    id: &str,
    millis: i64,
    correlation: Option<&str>,
    fee: Option<&str>,
    channel: Option<&str>,
) -> Value {
    build(id, millis, correlation, fee, Some(channel))
}

fn build(
    id: &str,
    millis: i64,
    correlation: Option<&str>,
    fee: Option<&str>,
    channel: Option<Option<&str>>,
) -> Value {
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
    let mut fields = vec![
        ("orderId".into(), s(&format!("o-{id}"))),
        ("captureGrossAmount".into(), money),
        ("additionalFees".into(), union(fee)),
        ("tags".into(), Value::Array(vec![s("x"), s("y")])),
        ("attributes".into(), attributes),
        ("createdAt".into(), Value::Long(millis)),
    ];
    if let Some(channel) = channel {
        fields.push(("channel".into(), union(channel)));
    }
    let payload = Value::Record(fields);
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
