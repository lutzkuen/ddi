//! Row-local array transforms and JSON construction: one outbox message per source row.
//!
//! The shape from the issue that asked for this: a bronze row holds one order with its
//! items inline as `$.orderEntries[]`, and the message a downstream system wants is one
//! object per order with one array element per item. No cross-row state; the array is a
//! function of that single row and nothing else, so batch boundaries cannot change it.

use std::sync::Arc;

use delta_delta_ingest::transform::{SqlTransform, Transform};
use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use deltalake::arrow::buffer::OffsetBuffer;
use deltalake::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};

/// Four deliveries of `webshop.checkout.orderCreated`, as the raw layer carries them.
///
/// - A1 has two entries, one of which the model filters out.
/// - B2 has an empty entry list.
/// - C3 has no entry list at all.
/// - D4 has one entry with a quantity of three.
fn deliveries() -> RecordBatch {
    let a1 = r#"{"webshopOrderId":"A1","orderEntries":[
        {"quantity":2,"product":{"variantArticleId":"V1","fulfillmentModel":"CP_SOLD_CP_FULFILLED"},"price":{"amount":[1,44]}},
        {"quantity":1,"product":{"variantArticleId":"V2","fulfillmentModel":"3P"},"price":{"amount":[0,250]}}
    ]}"#;
    let b2 = r#"{"webshopOrderId":"B2","orderEntries":[]}"#;
    let c3 = r#"{"webshopOrderId":"C3"}"#;
    let d4 = r#"{"webshopOrderId":"D4","orderEntries":[
        {"quantity":3,"product":{"variantArticleId":"V3","fulfillmentModel":"3P"},"price":{"amount":[1,44]}}
    ]}"#;
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("messageid", DataType::Utf8, false),
            Field::new(
                "kafka_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("data", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["m1", "m2", "m3", "m4"])) as ArrayRef,
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_711_924_200_123_456i64; 4])
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec![a1, b2, c3, d4])),
        ],
    )
    .unwrap()
}

/// The model from the issue, in the spelling that runs in Starburst: the item objects are
/// built `RETURNING JSON` so the array of them is an array of JSON rather than of text,
/// and the array is embedded with `json_format(..) FORMAT JSON`, the one way Starburst
/// accepts a JSON-typed value as a member. The FX rate is a CTE column here; a pinned
/// lookup's column is captured the same way, see the lookup test below.
const OUTBOX: &str = "
with orders as (
    select
        o.messageid as message_id,
        o.kafka_timestamp,
        json_extract_scalar(o.data, '$.webshopOrderId') as webshop_order_id,
        cast(json_extract(o.data, '$.orderEntries') as array(json)) as order_entries,
        1.1 as fx_rate
    from source as o
    where json_extract_scalar(o.data, '$.webshopOrderId') is not null
)
select
    orders.message_id,
    json_object(
        'messageId' value orders.message_id,
        'messageTime' value orders.kafka_timestamp,
        'data' value json_object(
            'orderCode' value orders.webshop_order_id,
            'currency' value 'EUR',
            'items' value json_format(cast(
                transform(
                    filter(
                        orders.order_entries,
                        e -> json_extract_scalar(e, '$.product.fulfillmentModel')
                             <> 'CP_SOLD_CP_FULFILLED'
                    ),
                    e -> json_object(
                        'productVariantId' value
                            json_extract_scalar(e, '$.product.variantArticleId'),
                        'quantity' value cast(json_extract_scalar(e, '$.quantity') as integer),
                        'nmvBeforeCancellation' value cast(
                            /* a two-byte big-endian amount at scale 2, times the quantity,
                               converted with the captured rate */
                            (cast(json_array_get(json_extract(e, '$.price.amount'), 0) as integer) * 256
                             + cast(json_array_get(json_extract(e, '$.price.amount'), 1) as integer))
                            / 100.0
                            * cast(json_extract_scalar(e, '$.quantity') as integer)
                            * orders.fx_rate
                            as decimal(30, 4)
                        )
                        returning json
                    )
                ) as json
            )) format json
        )
    ) as json_message
from orders
";

async fn run(sql: &str, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    SqlTransform::new(sql)
        .apply(batches)
        .await
        .unwrap_or_else(|e| panic!("transform failed: {e}"))
}

fn texts(out: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    let mut got = Vec::new();
    for b in out {
        let c = deltalake::arrow::compute::cast(
            b.column(b.schema().index_of(column).unwrap()),
            &DataType::Utf8,
        )
        .unwrap();
        let c = c.as_string::<i32>();
        for i in 0..c.len() {
            got.push((!c.is_null(i)).then(|| c.value(i).to_string()));
        }
    }
    got
}

#[tokio::test]
async fn one_message_per_order_with_that_orders_own_items() {
    let out = run(OUTBOX, vec![deliveries()]).await;
    let got = texts(&out, "json_message");

    // Member order is Java's HashMap order, which is what Starburst emits; numbers that
    // pass through FORMAT JSON are re-read as doubles there, and so here.
    assert_eq!(
        got,
        vec![
            Some(
                r#"{"messageTime":"2024-03-31 22:30:00.123 UTC","data":{"orderCode":"A1","currency":"EUR","items":[{"quantity":1,"productVariantId":"V2","nmvBeforeCancellation":2.75}]},"messageId":"m1"}"#
                    .to_string()
            ),
            // An empty array maps to `[]` ...
            Some(
                r#"{"messageTime":"2024-03-31 22:30:00.123 UTC","data":{"orderCode":"B2","currency":"EUR","items":[]},"messageId":"m2"}"#
                    .to_string()
            ),
            // ... and a NULL array to NULL. Neither drops the row.
            Some(
                r#"{"messageTime":"2024-03-31 22:30:00.123 UTC","data":{"orderCode":"C3","currency":"EUR","items":null},"messageId":"m3"}"#
                    .to_string()
            ),
            Some(
                r#"{"messageTime":"2024-03-31 22:30:00.123 UTC","data":{"orderCode":"D4","currency":"EUR","items":[{"quantity":3,"productVariantId":"V3","nmvBeforeCancellation":9.9}]},"messageId":"m4"}"#
                    .to_string()
            ),
        ]
    );
}

#[tokio::test]
async fn splitting_the_same_rows_into_different_batches_is_byte_identical() {
    // The property that justifies admitting a lambda at Grain::Preserved at all.
    let whole = texts(&run(OUTBOX, vec![deliveries()]).await, "json_message");

    let src = deliveries();
    let t = SqlTransform::new(OUTBOX);
    let mut split = Vec::new();
    for (start, len) in [(0, 1), (1, 2), (3, 1)] {
        let out = t.apply(vec![src.slice(start, len)]).await.unwrap();
        split.extend(texts(&out, "json_message"));
    }
    assert_eq!(whole, split, "batch boundaries changed a row-local result");

    let mut one_by_one = Vec::new();
    for i in 0..4 {
        let out = t.apply(vec![src.slice(i, 1)]).await.unwrap();
        one_by_one.extend(texts(&out, "json_message"));
    }
    assert_eq!(whole, one_by_one);
}

#[tokio::test]
async fn the_model_is_accepted_as_a_transform_and_what_runs_is_what_was_validated() {
    // `ddi validate` accepts it, and the executable text is the lambda folded into calls.
    let normalised = delta_delta_ingest::transform::validate::normalise_sql(OUTBOX).unwrap();
    assert!(normalised.contains("ddi_transform("), "{normalised}");
    assert!(normalised.contains("ddi_filter("), "{normalised}");
    assert!(normalised.contains("ddi_json_object("), "{normalised}");
    assert!(
        !normalised.contains("->"),
        "no lambda survives to the engine: {normalised}"
    );

    // And a model that reaches across rows inside the lambda is still refused, by name.
    let e = delta_delta_ingest::transform::validate::validate_sql(
        "SELECT transform(xs, x -> x + max(y) OVER ()) AS t FROM source",
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("window function"), "got: {e}");
    let e = delta_delta_ingest::transform::validate::validate_sql(
        "SELECT k, transform(xs, x -> x) AS t FROM source GROUP BY k, xs",
    )
    .unwrap_err()
    .to_string();
    assert!(e.contains("GROUP BY is not supported"), "got: {e}");
}

/// Two orders with typed line items: #1 has two, #2 has one. The same fixture the
/// `array_*` tests use.
fn orders_with_line_items() -> RecordBatch {
    let item_fields: Fields = vec![
        Arc::new(Field::new("sku", DataType::Utf8, false)),
        Arc::new(Field::new("qty", DataType::Int64, false)),
        Arc::new(Field::new("price", DataType::Float64, false)),
    ]
    .into();
    let items = StructArray::new(
        item_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2i64, 1, 3])) as ArrayRef,
            Arc::new(Float64Array::from(vec![10.0f64, 5.5, 2.0])) as ArrayRef,
        ],
        None,
    );
    let list_field = Arc::new(Field::new("item", DataType::Struct(item_fields), false));
    let line_items = ListArray::new(
        list_field.clone(),
        OffsetBuffer::new(vec![0, 2, 3].into()),
        Arc::new(items),
        None,
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("line_items", DataType::List(list_field), false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
            Arc::new(line_items) as ArrayRef,
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn a_lambda_over_an_array_of_structs_sees_the_elements_fields() {
    let out = run(
        "SELECT order_id, \
                array_sum(transform(line_items, li -> li.price * li.qty)) AS total, \
                array_length(filter(line_items, li -> li.qty > 1)) AS big_lines, \
                json_format(CAST(transform(line_items, li -> li.sku) AS JSON)) AS skus \
         FROM source",
        vec![orders_with_line_items()],
    )
    .await;
    assert_eq!(
        texts(&out, "total"),
        vec![Some("25.5".into()), Some("6.0".into())],
        "10*2 + 5.5*1, and 2*3"
    );
    assert_eq!(
        texts(&out, "big_lines"),
        vec![Some("1".into()), Some("1".into())]
    );
    assert_eq!(
        texts(&out, "skus"),
        vec![Some(r#"["A","B"]"#.into()), Some(r#"["C"]"#.into())]
    );
}

#[tokio::test]
async fn null_elements_are_evaluated_and_a_null_predicate_drops_the_element() {
    // As in Trino: transform binds a NULL element and filter keeps only TRUE.
    let out = run(
        "SELECT json_format(CAST(transform(CAST('[1,null,3]' AS ARRAY(JSON)), e -> e IS NULL) AS JSON)) AS nulls, \
                json_format(CAST(filter(CAST('[1,null,3]' AS ARRAY(JSON)), e -> CAST(e AS INTEGER) > 1) AS JSON)) AS big, \
                json_format(CAST(transform(CAST(NULL AS ARRAY(JSON)), e -> e) AS JSON)) AS none, \
                json_format(CAST(transform(CAST('[]' AS ARRAY(JSON)), e -> e) AS JSON)) AS empty \
         FROM source",
        vec![deliveries().slice(0, 1)],
    )
    .await;
    assert_eq!(
        texts(&out, "nulls"),
        vec![Some("[false,true,false]".into())]
    );
    assert_eq!(texts(&out, "big"), vec![Some("[3]".into())]);
    assert_eq!(texts(&out, "none"), vec![None]);
    assert_eq!(texts(&out, "empty"), vec![Some("[]".into())]);
}

#[tokio::test]
async fn a_body_that_does_not_plan_names_the_lambda_and_the_reason() {
    let e = SqlTransform::new(
        "SELECT transform(CAST(json_extract(data, '$.orderEntries') AS ARRAY(JSON)), \
                e -> no_such_function(e)) AS t FROM source",
    )
    .apply(vec![deliveries()])
    .await
    .unwrap_err()
    .to_string();
    assert!(e.contains("transform: the lambda body"), "got: {e}");
    assert!(e.contains("no_such_function"), "got: {e}");

    let e = SqlTransform::new(
        "SELECT filter(CAST(json_extract(data, '$.orderEntries') AS ARRAY(JSON)), \
                e -> json_extract_scalar(e, '$.quantity')) AS t FROM source",
    )
    .apply(vec![deliveries()])
    .await
    .unwrap_err()
    .to_string();
    assert!(e.contains("must be a boolean"), "got: {e}");
}

/// A pinned Delta lookup's column, captured inside a lambda body.
///
/// The FX rate in the issue's model is the one input that is not in the source row, and it
/// comes from a `LEFT JOIN` to a declared lookup. A lambda captures it like any other
/// column of the current row: it is a value, evaluated once per element.
#[tokio::test]
async fn a_lookup_column_is_captured_inside_the_lambda() {
    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
    use deltalake::kernel::StructType;
    use deltalake::protocol::SaveMode;
    use deltalake::{ensure_table_uri, open_table, DeltaTable};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fx_rates");
    let path = path.to_str().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("currency", DataType::Utf8, false),
        Field::new("exchange_rate", DataType::Float64, false),
    ]));
    let columns: StructType = schema.as_ref().try_into_kernel().unwrap();
    DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(columns.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();
    let rates = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["EUR", "USD"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
        ],
    )
    .unwrap();
    let table = open_table(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .write(vec![rates])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    let snapshot = delta_delta_ingest::lookup::LookupSnapshot {
        name: "fx_rates".into(),
        version: table.version().unwrap_or(0) as u64,
        table_id: None,
        used_pre_history: false,
        used_current: false,
        table,
    };

    let lookups: std::collections::BTreeSet<String> = ["fx_rates".to_string()].into();
    let sql = "WITH o AS (SELECT messageid, data, 'USD' AS currency FROM source) \
               SELECT o.messageid, \
                      json_format(CAST(transform( \
                          CAST(json_extract(o.data, '$.orderEntries') AS ARRAY(JSON)), \
                          e -> CAST(json_extract_scalar(e, '$.quantity') AS INTEGER) \
                               * fx_rates.exchange_rate) AS JSON)) AS scaled \
               FROM o LEFT JOIN fx_rates ON fx_rates.currency = o.currency";
    let out = SqlTransform::new_with_lookups(sql, &lookups)
        .apply_with_lookups(vec![deliveries()], &[snapshot])
        .await
        .unwrap_or_else(|e| panic!("transform failed: {e}"));
    assert_eq!(
        texts(&out, "scaled"),
        vec![
            Some("[4.0,2.0]".into()),
            Some("[]".into()),
            None,
            Some("[6.0]".into())
        ],
        "quantities 2,1 | none | no list | 3 — each doubled by the USD rate"
    );
}
