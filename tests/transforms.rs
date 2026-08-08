//! Plan Milestones 5 & 6 — unnest and the intra-row array UDFs, on real nested data.

use std::sync::Arc;

use delta_delta_ingest::transform::{SqlTransform, Transform};
use deltalake::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};
use deltalake::arrow::buffer::OffsetBuffer;
use deltalake::arrow::datatypes::{DataType, Field, Fields, Schema};

/// Two orders: #1 has two line items, #2 has one.
fn orders_with_line_items() -> RecordBatch {
    let item_fields: Fields = vec![
        Arc::new(Field::new("sku", DataType::Utf8, false)),
        Arc::new(Field::new("qty", DataType::Int64, false)),
        Arc::new(Field::new("price", DataType::Float64, false)),
    ]
    .into();

    // Flat element values: (A,2,10.0) (B,1,5.5) | (C,3,2.0)
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

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("line_items", DataType::List(list_field), false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
            Arc::new(line_items) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn run(sql: &str) -> Vec<RecordBatch> {
    SqlTransform::new(sql)
        .apply(vec![orders_with_line_items()])
        .await
        .expect("transform should succeed")
}

fn f64s(b: &[RecordBatch], col: &str) -> Vec<Option<f64>> {
    let mut out = Vec::new();
    for batch in b {
        let idx = batch.schema().index_of(col).unwrap();
        let a = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("expected float64");
        for i in 0..a.len() {
            out.push(if a.is_null(i) { None } else { Some(a.value(i)) });
        }
    }
    out
}

#[tokio::test]
async fn array_sum_over_an_expression_computes_the_order_total() {
    // The headline case: sum price*qty within each row's line items.
    // Order 1: 10.0*2 + 5.5*1 = 25.5   Order 2: 2.0*3 = 6.0
    let out =
        run("SELECT order_id, array_sum(line_items, 'price * qty') AS total FROM source").await;
    assert_eq!(f64s(&out, "total"), vec![Some(25.5), Some(6.0)]);
}

#[tokio::test]
async fn array_length_counts_line_items() {
    let out = run("SELECT order_id, array_length(line_items) AS n FROM source").await;
    let mut got = Vec::new();
    for b in &out {
        let idx = b.schema().index_of("n").unwrap();
        let a = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..a.len() {
            got.push(a.value(i));
        }
    }
    assert_eq!(got, vec![2, 1]);
}

#[tokio::test]
async fn array_min_max_avg_over_a_field() {
    let out = run("SELECT array_min(line_items, 'price') AS lo, \
                array_max(line_items, 'price') AS hi, \
                array_avg(line_items, 'qty')   AS avg_qty \
         FROM source")
    .await;
    assert_eq!(f64s(&out, "lo"), vec![Some(5.5), Some(2.0)]);
    assert_eq!(f64s(&out, "hi"), vec![Some(10.0), Some(2.0)]);
    assert_eq!(f64s(&out, "avg_qty"), vec![Some(1.5), Some(3.0)]);
}

#[tokio::test]
async fn array_udfs_are_row_local_so_batch_splitting_cannot_change_the_answer() {
    // The property that justifies allowing these at all: run the same rows as one batch
    // and as two, and every row's value must be identical.
    let whole = run("SELECT array_sum(line_items, 'price * qty') AS total FROM source").await;

    let src = orders_with_line_items();
    let a = src.slice(0, 1);
    let b = src.slice(1, 1);
    let t = SqlTransform::new("SELECT array_sum(line_items, 'price * qty') AS total FROM source");
    let split_a = t.apply(vec![a]).await.unwrap();
    let split_b = t.apply(vec![b]).await.unwrap();

    let mut split = f64s(&split_a, "total");
    split.extend(f64s(&split_b, "total"));
    assert_eq!(
        f64s(&whole, "total"),
        split,
        "batch boundaries changed a row-local result"
    );
}

#[tokio::test]
async fn unnest_expands_to_line_item_grain() {
    let out = run("SELECT order_id, li.sku, li.qty FROM \
         (SELECT order_id, unnest(line_items) AS li FROM source)")
    .await;
    let rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "two line items plus one");
}

#[tokio::test]
async fn a_missing_struct_field_is_a_clear_error_listing_what_exists() {
    let err = SqlTransform::new("SELECT array_sum(line_items, 'nope') AS t FROM source")
        .apply(vec![orders_with_line_items()])
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("nope"), "got: {msg}");
    assert!(msg.contains("available"), "should list the fields: {msg}");
}

#[tokio::test]
async fn a_non_array_argument_is_rejected_with_a_pointer_to_the_right_tool() {
    let err = SqlTransform::new("SELECT array_sum(order_id) AS t FROM source")
        .apply(vec![orders_with_line_items()])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("aggregate downstream"),
        "got: {err}"
    );
}
