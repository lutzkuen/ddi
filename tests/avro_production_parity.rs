//! The production kafka-delta-ingest turns Confluent Avro into JSON with apache-avro 0.14.0
//! (the one schema_registry_converter 3.1.0 decodes with; M-1 read it off the binaries), while
//! the parity matrices behind `kdi_compat` are written against 0.21.0. This puts one datum of
//! every Avro kind through both and compares the JSON each gives: where they agree, the matrices
//! hold for production, and where they differ, `kdi_text` has a rendering to change.
//!
//! No broker needed.

use apache_avro::types::Value as New;

/// One record holding a field of every Avro kind this tool reads, as a schema.
const SCHEMA: &str = r#"{"type":"record","name":"All","fields":[
  {"name":"n","type":"null"},
  {"name":"b","type":"boolean"},
  {"name":"i","type":"int"},
  {"name":"l","type":"long"},
  {"name":"f","type":"float"},
  {"name":"d","type":"double"},
  {"name":"by","type":"bytes"},
  {"name":"s","type":"string"},
  {"name":"fx","type":{"type":"fixed","name":"Fx","size":3}},
  {"name":"en","type":{"type":"enum","name":"En","symbols":["A","B"]}},
  {"name":"ar","type":{"type":"array","items":"int"}},
  {"name":"mp","type":{"type":"map","values":"string"}},
  {"name":"un","type":["null","string"]},
  {"name":"un2","type":["null","string"]},
  {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":2}},
  {"name":"decfx","type":{"type":"fixed","name":"DecFx","size":6,"logicalType":"decimal","precision":12,"scale":2}},
  {"name":"date","type":{"type":"int","logicalType":"date"}},
  {"name":"tm","type":{"type":"int","logicalType":"time-millis"}},
  {"name":"tu","type":{"type":"long","logicalType":"time-micros"}},
  {"name":"tsm","type":{"type":"long","logicalType":"timestamp-millis"}},
  {"name":"tsu","type":{"type":"long","logicalType":"timestamp-micros"}},
  {"name":"rec","type":{"type":"record","name":"Inner","fields":[{"name":"z","type":"long"},{"name":"a","type":["null","long"]}]}}
]}"#;

fn datum() -> New {
    New::Record(vec![
        ("n".into(), New::Null),
        ("b".into(), New::Boolean(true)),
        ("i".into(), New::Int(-7)),
        ("l".into(), New::Long(1_791_935_999_999)),
        ("f".into(), New::Float(1.5)),
        ("d".into(), New::Double(0.1)),
        ("by".into(), New::Bytes(vec![0, 255, 7])),
        ("s".into(), New::String("é\"\n".into())),
        ("fx".into(), New::Fixed(3, vec![1, 2, 3])),
        ("en".into(), New::Enum(1, "B".into())),
        ("ar".into(), New::Array(vec![New::Int(2), New::Int(1)])),
        (
            "mp".into(),
            New::Map(
                [
                    ("b".to_string(), New::String("2".into())),
                    ("a".to_string(), New::String("1".into())),
                ]
                .into_iter()
                .collect(),
            ),
        ),
        ("un".into(), New::Union(0, Box::new(New::Null))),
        (
            "un2".into(),
            New::Union(1, Box::new(New::String("x".into()))),
        ),
        (
            "dec".into(),
            New::Decimal(apache_avro::Decimal::from(vec![0u8, 128, 29])),
        ),
        (
            "decfx".into(),
            New::Decimal(apache_avro::Decimal::from(vec![0u8, 0, 0, 0, 1, 44])),
        ),
        ("date".into(), New::Date(20_000)),
        ("tm".into(), New::TimeMillis(43_200_000)),
        ("tu".into(), New::TimeMicros(43_200_000_001)),
        ("tsm".into(), New::TimestampMillis(1_791_935_999_999)),
        ("tsu".into(), New::TimestampMicros(1_791_935_999_999_999)),
        (
            "rec".into(),
            New::Record(vec![
                ("z".into(), New::Long(1)),
                ("a".into(), New::Union(1, Box::new(New::Long(5)))),
            ]),
        ),
    ])
}

/// Each field alone, so that a kind one version cannot read does not hide the others.
#[test]
fn both_avro_versions_make_the_same_json_of_every_kind() {
    let all: serde_json::Value = serde_json::from_str(SCHEMA).unwrap();
    let fields = all["fields"].as_array().unwrap().clone();
    let New::Record(entries) = datum() else {
        unreachable!()
    };
    let mut differences = Vec::new();
    let mut unreadable = Vec::new();
    for (name, value) in entries {
        let field = fields.iter().find(|f| f["name"] == name.as_str()).unwrap();
        let schema = serde_json::json!({"type":"record","name":"One","fields":[field]}).to_string();
        let new_schema = apache_avro::Schema::parse_str(&schema).unwrap();
        let bytes =
            apache_avro::to_avro_datum(&new_schema, New::Record(vec![(name.clone(), value)]))
                .unwrap();
        let new = apache_avro::from_avro_datum(&new_schema, &mut &bytes[..], None).unwrap();
        let new = serde_json::Value::try_from(new).unwrap();

        let old = apache_avro_014::Schema::parse_str(&schema)
            .and_then(|s| apache_avro_014::from_avro_datum(&s, &mut &bytes[..], None));
        match old {
            Err(e) => unreadable.push(format!("{name}: 0.14 cannot read it ({e})")),
            Ok(old) => {
                let old = serde_json::Value::try_from(old).unwrap();
                if old != new {
                    differences.push(format!("{name}: 0.21 = {new}, 0.14 = {old}"));
                }
            }
        }
    }
    // Every kind 0.14.0 reads, it renders as 0.21.0 does (the byte-array decimal, the raw
    // numbers of the date and time kinds, unions as their branch, maps with sorted keys...).
    assert!(
        differences.is_empty(),
        "production's apache-avro makes other JSON than the one the parity matrices use:\n{}",
        differences.join("\n")
    );
    // The one kind it cannot read at all is a decimal on a `fixed`: its schema does not parse,
    // so in production every message of a topic with one was dropped, and the 22 raw tables,
    // all of which hold rows, have none. M-1's open question about `fixed` decimals is closed by
    // this, with no schema read.
    assert_eq!(unreadable.len(), 1, "{unreadable:?}");
    assert!(unreadable[0].starts_with("decfx"), "{unreadable:?}");
}
