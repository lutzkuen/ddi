//! The spellings docs/kafka-design.md §6.3 gives for kafka-delta-ingest's `-t` expressions,
//! run through the validator the config uses and the engine that evaluates them (M4's
//! "verify" items: `* REPLACE`, quoted multi-level access, `to_char`, string to timestamp).
//!
//! No broker needed.

use std::collections::BTreeSet;
use std::sync::Arc;

use delta_delta_ingest::transform::validate::normalise_sql_with_lookups;
use delta_delta_ingest::transform::{SqlTransform, Transform};
use deltalake::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use deltalake::arrow::datatypes::{DataType, Field, Fields, Schema};

async fn run(sql: &str, batch: RecordBatch) -> RecordBatch {
    // The statement a config would hold is the validated one.
    let sql = normalise_sql_with_lookups(sql, &BTreeSet::new())
        .unwrap_or_else(|e| panic!("the validator refuses {sql}: {e}"));
    let out = SqlTransform::new(sql.clone())
        .apply(vec![batch])
        .await
        .unwrap_or_else(|e| panic!("{sql} fails: {e}"));
    assert_eq!(out.len(), 1);
    out.into_iter().next().unwrap()
}

fn strings(b: &RecordBatch, column: &str) -> Vec<Option<String>> {
    let a = b
        .column_by_name(column)
        .unwrap_or_else(|| panic!("column {column}"));
    let a = deltalake::arrow::compute::cast(a, &DataType::Utf8).unwrap();
    let a = a.as_any().downcast_ref::<StringArray>().unwrap();
    (0..a.len())
        .map(|i| a.is_valid(i).then(|| a.value(i).to_string()))
        .collect()
}

fn simple() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("when", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(1_626_823_098),
                Some(1_626_823_098),
            ])),
            Arc::new(StringArray::from(vec![
                Some("2021-07-20 23:18:18"),
                Some("2021-07-21T00:00:00"),
            ])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn replace_overwrites_a_column_and_a_null_result_keeps_the_original() {
    // kdi: a transform whose result is null does not set the field.
    let out = run(
        r#"SELECT * REPLACE (coalesce(upper("name"), "name") AS "name") FROM source"#,
        simple(),
    )
    .await;
    assert_eq!(strings(&out, "name"), [Some("A".to_string()), None]);
    assert_eq!(out.num_columns(), 3, "every other column stays");
}

#[tokio::test]
async fn quoted_multi_level_access_plans() {
    let c = Field::new("c", DataType::Utf8, true);
    let b_fields = Fields::from(vec![c.clone()]);
    let a_fields = Fields::from(vec![Field::new(
        "b",
        DataType::Struct(b_fields.clone()),
        true,
    )]);
    let c_array = Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef;
    let b = StructArray::new(b_fields, vec![c_array], None);
    let a = StructArray::new(a_fields.clone(), vec![Arc::new(b) as ArrayRef], None);
    let schema = Schema::new(vec![Field::new("a", DataType::Struct(a_fields), true)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a) as ArrayRef]).unwrap();
    let out = run(r#"SELECT "a"."b"."c" AS x FROM source"#, batch).await;
    assert_eq!(
        strings(&out, "x"),
        [Some("x".to_string()), Some("y".to_string())]
    );
}

#[tokio::test]
async fn to_char_writes_kdis_iso_text() {
    // kdi's epoch_seconds_to_iso8601: `2021-07-20T23:18:18Z`, a fraction only when non-zero.
    let out = run(
        r#"SELECT to_char(to_timestamp_seconds("when"), '%Y-%m-%dT%H:%M:%S%.fZ') AS iso FROM source"#,
        simple(),
    )
    .await;
    let text = strings(&out, "iso")[0].clone().unwrap();
    assert!(text.starts_with("2021-07-20T23:18:18"), "{text}");
    assert!(text.ends_with('Z'), "{text}");
}

#[tokio::test]
async fn a_string_casts_to_a_timestamp() {
    let out = run(
        r#"SELECT CAST("s" AS TIMESTAMP) AS ts FROM source"#,
        simple(),
    )
    .await;
    let ts = strings(&out, "ts");
    assert!(
        ts[0].as_deref().unwrap().starts_with("2021-07-20T23:18:18"),
        "{ts:?}"
    );
    assert!(
        ts[1].as_deref().unwrap().starts_with("2021-07-21T00:00:00"),
        "{ts:?}"
    );
}
