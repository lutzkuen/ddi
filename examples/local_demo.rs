//! A local lakehouse you can run `ddi` against, with no cloud storage and no cluster.
//!
//! ```bash
//! cargo run --example local_demo -- seed /tmp/ddi-demo
//! cargo run --bin ddi -- once --config /tmp/ddi-demo/pipelines.toml
//! cargo run --example local_demo -- show /tmp/ddi-demo
//!
//! # then stream a new bronze commit through the same pipelines
//! cargo run --example local_demo -- append /tmp/ddi-demo
//! cargo run --bin ddi -- once --config /tmp/ddi-demo/pipelines.toml
//! cargo run --example local_demo -- show /tmp/ddi-demo
//! ```
//!
//! Bronze is deliberately shaped the way bronze actually arrives: timestamps as strings,
//! money as doubles, and one array of structs per row. Silver is typed the way silver
//! should be, and the transforms in `pipelines.toml` are what bridge the two.

use std::sync::Arc;

use deltalake::arrow::array::{
    ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};
use deltalake::arrow::buffer::OffsetBuffer;
use deltalake::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use deltalake::arrow::util::display::{ArrayFormatter, FormatOptions};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};
use futures::TryStreamExt;

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

fn bronze_schema() -> SchemaRef {
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

fn batch(orders: &[Order]) -> RecordBatch {
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
        bronze_schema(),
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

async fn create(path: &str, schema: SchemaRef) {
    let delta: StructType = schema.as_ref().try_into_kernel().unwrap();
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

async fn append(path: &str, orders: &[Order]) {
    let url = ensure_table_uri(path).unwrap();
    open_table(url)
        .await
        .unwrap()
        .write(vec![batch(orders)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
}

/// Print a table as a plain text grid — enough for a demo, and it avoids depending on
/// arrow's prettyprint feature.
async fn show(path: &str, title: &str) {
    let url = ensure_table_uri(path).unwrap();
    let table = open_table(url).await.unwrap();
    let version = table.version();
    let (_t, stream) = table.scan_table().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let batches: Vec<_> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();

    println!("\n{title}  (delta version {version:?})");
    let Some(first) = batches.first() else {
        println!("  <empty>");
        return;
    };

    let names: Vec<String> = first
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let opts = FormatOptions::default().with_null("NULL");
    let mut rows: Vec<Vec<String>> = Vec::new();
    for b in &batches {
        // ArrayFormatter renders every Arrow type, including the struct and list columns
        // that cannot simply be cast to text.
        let fmts: Vec<ArrayFormatter> = (0..b.num_columns())
            .map(|i| ArrayFormatter::try_new(b.column(i), &opts).expect("formattable"))
            .collect();
        for r in 0..b.num_rows() {
            rows.push(fmts.iter().map(|f| f.value(r).to_string()).collect());
        }
    }

    let width: Vec<usize> = names
        .iter()
        .enumerate()
        .map(|(i, n)| {
            rows.iter()
                .map(|r| r[i].len())
                .max()
                .unwrap_or(0)
                .max(n.len())
        })
        .collect();
    let line = |cells: &[String]| {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:<w$}", c, w = width[i]))
            .collect::<Vec<_>>()
            .join("  ")
    };
    println!("  {}", line(&names));
    println!(
        "  {}",
        width
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for r in &rows {
        println!("  {}", line(r));
    }
    println!("  {} row(s)", rows.len());
}

const PIPELINES: &str = r#"# Generated by `cargo run --example local_demo -- seed`.
[defaults]
allowed_latency_secs = 1

[[pipeline]]
name       = "orders_header"
app_id     = "ddi.demo.orders_header"
source_uri = "{ROOT}/bronze/orders"
target_uri = "{ROOT}/silver/orders"
transform_sql = """
SELECT order_id,
       CAST(created_at AS TIMESTAMP)        AS created_at,
       customer.id                          AS customer_id,
       customer.country                     AS customer_country,
       array_length(line_items)             AS line_count,
       array_sum(line_items, 'price * qty') AS order_total
FROM source
WHERE order_status <> 'DRAFT'
"""

[[pipeline]]
name       = "orders_lines"
app_id     = "ddi.demo.orders_lines"
source_uri = "{ROOT}/bronze/orders"
target_uri = "{ROOT}/silver/order_lines"
transform_sql = """
SELECT order_id,
       li.sku                          AS sku,
       li.qty                          AS qty,
       CAST(li.price AS DECIMAL(18,4)) AS price
FROM (SELECT order_id, unnest(line_items) AS li FROM source)
"""
"#;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_default();
    let root = args.next().unwrap_or_else(|| "/tmp/ddi-demo".to_string());

    let bronze = format!("{root}/bronze/orders");
    let header = format!("{root}/silver/orders");
    let lines = format!("{root}/silver/order_lines");

    match cmd.as_str() {
        "seed" => {
            if std::path::Path::new(&root).exists() {
                eprintln!("{root} already exists; remove it first");
                std::process::exit(1);
            }
            std::fs::create_dir_all(&root).unwrap();

            create(&bronze, bronze_schema()).await;
            // ddi never creates target tables — external tooling owns them. Here, that is
            // this example.
            create(&header, header_schema()).await;
            create(&lines, lines_schema()).await;

            append(
                &bronze,
                &[
                    Order {
                        id: 1001,
                        created_at: "2026-01-15T10:30:00",
                        status: "PAID",
                        customer: (7, "DE"),
                        items: vec![("WIDGET-A", 2, 10.0), ("WIDGET-B", 1, 5.5)],
                    },
                    Order {
                        id: 1002,
                        created_at: "2026-01-15T11:00:00",
                        status: "PAID",
                        customer: (8, "FR"),
                        items: vec![("WIDGET-C", 3, 2.0)],
                    },
                    Order {
                        id: 1003,
                        created_at: "2026-01-15T11:30:00",
                        status: "DRAFT",
                        customer: (9, "NL"),
                        items: vec![("WIDGET-D", 1, 3.25)],
                    },
                ],
            )
            .await;

            let cfg = format!("{root}/pipelines.toml");
            std::fs::write(&cfg, PIPELINES.replace("{ROOT}", &root)).unwrap();

            show(
                &bronze,
                "BRONZE  bronze/orders  (strings, doubles, nested arrays)",
            )
            .await;
            println!("\nwrote {cfg}");
            println!("next:  cargo run --bin ddi -- once --config {cfg}");
        }
        "append" => {
            append(
                &bronze,
                &[Order {
                    id: 1004,
                    created_at: "2026-01-16T09:00:00",
                    status: "PAID",
                    customer: (7, "DE"),
                    items: vec![("WIDGET-E", 4, 1.5), ("WIDGET-F", 2, 0.25)],
                }],
            )
            .await;
            println!("appended order 1004 as a new bronze commit");
        }
        "show" => {
            show(&bronze, "BRONZE  bronze/orders").await;
            show(&header, "SILVER  silver/orders        (header grain)").await;
            show(&lines, "SILVER  silver/order_lines   (line-item grain)").await;
        }
        _ => {
            eprintln!("usage: local_demo <seed|append|show> [root-dir]");
            std::process::exit(2);
        }
    }
}
