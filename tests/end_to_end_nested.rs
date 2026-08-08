//! The whole thing, composed: a nested bronze table streamed into two silver tables.
//!
//! `transforms.rs` proves the SQL in isolation, on in-memory batches that never reach a
//! Delta table. `exactly_once.rs` proves the streaming, on a flat two-column schema that
//! needs no casting. Neither proves the composition, and the composition is where the
//! interesting failures live: a struct field landing in a target column, a float total
//! landing in a `DECIMAL`, an unnested child grain resuming from its own offset while the
//! parent grain resumes from a different one.
//!
//! This is the README's fan-out example, executed.

mod common;

use std::sync::Arc;

use common::pipeline_cfg;
use delta_delta_ingest::pipeline::Pipeline;
use deltalake::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray,
};
use deltalake::arrow::buffer::OffsetBuffer;
use deltalake::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};
use futures::TryStreamExt;

const HEADER_SQL: &str = "\
SELECT order_id,
       CAST(created_at AS TIMESTAMP)        AS created_at,
       customer.id                          AS customer_id,
       customer.country                     AS customer_country,
       array_length(line_items)             AS line_count,
       array_sum(line_items, 'price * qty') AS order_total
FROM source
WHERE order_status <> 'DRAFT'";

const LINES_SQL: &str = "\
SELECT order_id,
       li.sku                          AS sku,
       li.qty                          AS qty,
       CAST(li.price AS DECIMAL(18,4)) AS price
FROM (SELECT order_id, unnest(line_items) AS li FROM source)";

// ---------------------------------------------------------------- source shape

struct Order {
    id: i64,
    created_at: &'static str,
    status: &'static str,
    customer: (i64, &'static str),
    items: Vec<(&'static str, i64, f64)>,
}

fn item_fields() -> Fields {
    vec![
        Arc::new(Field::new("sku", DataType::Utf8, false)),
        Arc::new(Field::new("qty", DataType::Int64, false)),
        Arc::new(Field::new("price", DataType::Float64, false)),
    ]
    .into()
}

fn customer_fields() -> Fields {
    vec![
        Arc::new(Field::new("id", DataType::Int64, false)),
        Arc::new(Field::new("country", DataType::Utf8, false)),
    ]
    .into()
}

/// Bronze: strings where timestamps belong, doubles where money belongs, one array of
/// structs per row. Deliberately the shape real bronze arrives in.
fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("order_status", DataType::Utf8, false),
        Field::new("customer", DataType::Struct(customer_fields()), false),
        Field::new(
            "line_items",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(item_fields()),
                false,
            ))),
            false,
        ),
    ]))
}

fn source_batch(orders: &[Order]) -> RecordBatch {
    let (mut skus, mut qtys, mut prices) = (Vec::new(), Vec::new(), Vec::new());
    let mut offsets = vec![0i32];
    for o in orders {
        for (s, q, p) in &o.items {
            skus.push(*s);
            qtys.push(*q);
            prices.push(*p);
        }
        offsets.push(skus.len() as i32);
    }

    let items = StructArray::new(
        item_fields(),
        vec![
            Arc::new(StringArray::from(skus)) as ArrayRef,
            Arc::new(Int64Array::from(qtys)) as ArrayRef,
            Arc::new(Float64Array::from(prices)) as ArrayRef,
        ],
        None,
    );
    let line_items = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(item_fields()), false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(items),
        None,
    );
    let customer = StructArray::new(
        customer_fields(),
        vec![
            Arc::new(Int64Array::from(
                orders.iter().map(|o| o.customer.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                orders.iter().map(|o| o.customer.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
        None,
    );

    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from(
                orders.iter().map(|o| o.id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                orders.iter().map(|o| o.created_at).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                orders.iter().map(|o| o.status).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(customer) as ArrayRef,
            Arc::new(line_items) as ArrayRef,
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------- target shapes

/// Silver header grain: a real TIMESTAMP and a real DECIMAL. The target schema is the
/// contract; the transform has to meet it.
fn header_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("customer_country", DataType::Utf8, true),
        Field::new("line_count", DataType::Int64, true),
        Field::new("order_total", DataType::Decimal128(18, 4), true),
    ]))
}

fn lines_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("sku", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
        Field::new("price", DataType::Decimal128(18, 4), true),
    ]))
}

// ---------------------------------------------------------------- table helpers

async fn create_table(path: &str, schema: SchemaRef) {
    let delta: StructType = schema.as_ref().try_into_kernel().expect("arrow -> delta");
    let url = ensure_table_uri(path).unwrap();
    DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();
}

async fn append_orders(path: &str, orders: &[Order]) {
    let url = ensure_table_uri(path).unwrap();
    deltalake::open_table(url)
        .await
        .unwrap()
        .write(vec![source_batch(orders)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
}

async fn scan(path: &str) -> Vec<RecordBatch> {
    let url = ensure_table_uri(path).unwrap();
    let (_t, stream) = open_table(url).await.unwrap().scan_table().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    batches.into_iter().filter(|b| b.num_rows() > 0).collect()
}

fn i64s(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let a = b.column(b.schema().index_of(col).unwrap());
        let a = a.as_any().downcast_ref::<Int64Array>().expect("int64");
        out.extend((0..a.len()).map(|i| a.value(i)));
    }
    out
}

fn strings(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let a = b.column(b.schema().index_of(col).unwrap());
        // The scan may hand back Utf8View or LargeUtf8 rather than Utf8 depending on how
        // DataFusion planned the read; normalise before reading values.
        let a = deltalake::arrow::compute::cast(a, &DataType::Utf8).expect("castable to utf8");
        let a = a.as_any().downcast_ref::<StringArray>().unwrap();
        out.extend((0..a.len()).map(|i| a.value(i).to_string()));
    }
    out
}

/// Decimal128 values as their unscaled i128, so scale is asserted rather than assumed.
fn decimals(batches: &[RecordBatch], col: &str) -> Vec<i128> {
    let mut out = Vec::new();
    for b in batches {
        let a = b.column(b.schema().index_of(col).unwrap());
        let a = a
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128 — the target schema is the contract");
        out.extend((0..a.len()).map(|i| a.value(i)));
    }
    out
}

/// Sort rows by `order_id` then `sku` so assertions do not depend on file order.
fn sorted_by_key(batches: &[RecordBatch], key: &str) -> Vec<(i64, usize, usize)> {
    let mut idx: Vec<(i64, usize, usize)> = Vec::new();
    for (bi, b) in batches.iter().enumerate() {
        let a = b.column(b.schema().index_of(key).unwrap());
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..a.len() {
            idx.push((a.value(i), bi, i));
        }
    }
    idx.sort();
    idx
}

// ---------------------------------------------------------------- the test

struct Lakehouse {
    _dir: tempfile::TempDir,
    bronze: String,
    header: String,
    lines: String,
}

async fn lakehouse() -> Lakehouse {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let bronze = root.join("bronze/orders").to_str().unwrap().to_string();
    let header = root.join("silver/orders").to_str().unwrap().to_string();
    let lines = root
        .join("silver/order_lines")
        .to_str()
        .unwrap()
        .to_string();

    create_table(&bronze, source_schema()).await;
    create_table(&header, header_schema()).await;
    create_table(&lines, lines_schema()).await;

    Lakehouse {
        _dir: dir,
        bronze,
        header,
        lines,
    }
}

fn first_orders() -> Vec<Order> {
    vec![
        Order {
            id: 1001,
            created_at: "2026-01-15T10:30:00",
            status: "PAID",
            customer: (7, "DE"),
            items: vec![("A", 2, 10.0), ("B", 1, 5.5)],
        },
        Order {
            id: 1002,
            created_at: "2026-01-15T11:00:00",
            status: "PAID",
            customer: (8, "FR"),
            items: vec![("C", 3, 2.0)],
        },
        Order {
            id: 1003,
            created_at: "2026-01-15T11:30:00",
            status: "DRAFT",
            customer: (9, "NL"),
            items: vec![("D", 1, 3.25)],
        },
    ]
}

async fn run(lake: &Lakehouse, name: &str, target: &str, sql: &str) -> usize {
    let mut cfg = pipeline_cfg(name, &lake.bronze, target);
    cfg.transform_sql = Some(sql.to_string());
    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap()
}

#[tokio::test]
async fn nested_bronze_fans_out_into_two_typed_silver_tables() {
    let lake = lakehouse().await;
    append_orders(&lake.bronze, &first_orders()).await;

    run(&lake, "header", &lake.header, HEADER_SQL).await;
    run(&lake, "lines", &lake.lines, LINES_SQL).await;

    // --- header grain: cast + struct access + intra-row calculation, all at once.
    let h = scan(&lake.header).await;
    let order = sorted_by_key(&h, "order_id");
    assert_eq!(
        order.iter().map(|t| t.0).collect::<Vec<_>>(),
        vec![1001, 1002],
        "DRAFT must be filtered out by the transform's WHERE"
    );

    assert_eq!(
        i64s(&h, "customer_id"),
        vec![7, 8],
        "struct field extracted"
    );
    assert_eq!(strings(&h, "customer_country"), vec!["DE", "FR"]);
    assert_eq!(i64s(&h, "line_count"), vec![2, 1], "array_length");

    // 1001: 10.0*2 + 5.5*1 = 25.5   1002: 2.0*3 = 6.0
    // Stored at DECIMAL(18,4), so the unscaled values are x * 10^4.
    assert_eq!(
        decimals(&h, "order_total"),
        vec![255_000, 60_000],
        "array_sum of price*qty, cast from double into DECIMAL(18,4)"
    );

    // The cast actually happened: bronze held a string.
    let created_at = h[0].column(h[0].schema().index_of("created_at").unwrap());
    assert_eq!(
        created_at.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "created_at must land as a real timestamp, not a string"
    );
    assert_eq!(created_at.null_count(), 0, "every timestamp parsed");

    // --- line grain: the same source row expanded, with its own independent offset.
    let l = scan(&lake.lines).await;
    let rows: usize = l.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 4,
        "2 + 1 + 1 line items — the lines pipeline has no status filter, so DRAFT's line \
         is present here even though its header row is not. The two targets are \
         independent."
    );
    assert_eq!(
        decimals(&l, "price"),
        vec![100_000, 55_000, 20_000, 32_500],
        "each line's price cast from double into DECIMAL(18,4)"
    );
    assert_eq!(strings(&l, "sku"), vec!["A", "B", "C", "D"]);
    assert_eq!(i64s(&l, "qty"), vec![2, 1, 3, 1]);
}

#[tokio::test]
async fn a_later_bronze_commit_streams_through_without_replaying_the_first() {
    let lake = lakehouse().await;
    append_orders(&lake.bronze, &first_orders()).await;

    run(&lake, "header", &lake.header, HEADER_SQL).await;
    run(&lake, "lines", &lake.lines, LINES_SQL).await;

    // A second bronze commit, as a live source would produce.
    append_orders(
        &lake.bronze,
        &[Order {
            id: 1004,
            created_at: "2026-01-16T09:00:00",
            status: "PAID",
            customer: (7, "DE"),
            items: vec![("E", 4, 1.5), ("F", 2, 0.25)],
        }],
    )
    .await;

    let n = run(&lake, "header", &lake.header, HEADER_SQL).await;
    assert!(n > 0, "the new commit must be picked up");
    run(&lake, "lines", &lake.lines, LINES_SQL).await;

    let h = scan(&lake.header).await;
    assert_eq!(
        sorted_by_key(&h, "order_id")
            .iter()
            .map(|t| t.0)
            .collect::<Vec<_>>(),
        vec![1001, 1002, 1004],
        "exactly the paid orders, each once"
    );
    // 1004: 1.5*4 + 0.25*2 = 6.5
    let mut totals = decimals(&h, "order_total");
    totals.sort();
    assert_eq!(totals, vec![60_000, 65_000, 255_000]);

    let l = scan(&lake.lines).await;
    let rows: usize = l.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 6, "4 existing line items plus 2 new ones");

    // And a third run, with nothing new, must be a no-op on both targets.
    let before = (
        open_table(ensure_table_uri(&lake.header).unwrap())
            .await
            .unwrap()
            .version(),
        open_table(ensure_table_uri(&lake.lines).unwrap())
            .await
            .unwrap()
            .version(),
    );
    assert_eq!(run(&lake, "header", &lake.header, HEADER_SQL).await, 0);
    assert_eq!(run(&lake, "lines", &lake.lines, LINES_SQL).await, 0);
    let after = (
        open_table(ensure_table_uri(&lake.header).unwrap())
            .await
            .unwrap()
            .version(),
        open_table(ensure_table_uri(&lake.lines).unwrap())
            .await
            .unwrap()
            .version(),
    );
    assert_eq!(before, after, "a caught-up run must not commit anything");
}
