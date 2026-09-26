//! Plan Milestones 5 & 6 — unnest and the intra-row array UDFs, on real nested data.

mod common;

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
async fn the_trino_spelling_of_unnest_expands_to_the_same_grain() {
    // The form a dbt-trino model actually contains. It used to be rejected as a JOIN, which
    // it is not: UNNEST of a column of this same row reads no second table.
    let out = run("SELECT o.order_id, li.sku, li.qty FROM source o \
         CROSS JOIN UNNEST(o.line_items) AS t(li)")
    .await;
    let rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "two line items plus one");
}

/// A bronze row whose whole payload is one JSON blob — the common shape, and the one the
/// typed `line_items` fixture above does not cover.
fn orders_with_json_payload() -> RecordBatch {
    use deltalake::arrow::array::StringArray;
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                r#"{"status":"PAID","lines":[{"sku":"A","qty":2},{"sku":"B","qty":1}]}"#,
                r#"{"status":"PAID","lines":[{"sku":"C","qty":7}]}"#,
            ])) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn run_on(batch: RecordBatch, sql: &str) -> Vec<RecordBatch> {
    SqlTransform::new(sql)
        .apply(vec![batch])
        .await
        .expect("transform should succeed")
}

#[tokio::test]
async fn an_array_cast_out_of_a_json_blob_can_be_unnested() {
    // The shape bronze actually arrives in: the array does not exist as an array until the
    // cast makes it one. Written exactly as Trino would, so the same model runs in both.
    let out = run_on(
        orders_with_json_payload(),
        "SELECT o.order_id, \
                json_extract_scalar(li, '$.sku')                  AS sku, \
                CAST(json_extract_scalar(li, '$.qty') AS BIGINT)  AS qty \
         FROM source o \
         CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY(JSON))) AS t(li)",
    )
    .await;

    let rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "two lines from order 1, one from order 2");

    let b = &out[0];
    let sku = deltalake::arrow::compute::cast(
        b.column(b.schema().index_of("sku").unwrap()),
        &DataType::Utf8,
    )
    .unwrap();
    let sku = sku
        .as_any()
        .downcast_ref::<deltalake::arrow::array::StringArray>()
        .unwrap();
    let got: Vec<&str> = (0..sku.len()).map(|i| sku.value(i)).collect();
    assert_eq!(got, vec!["A", "B", "C"]);
}

#[tokio::test]
async fn an_order_with_no_lines_contributes_no_rows_rather_than_failing() {
    use deltalake::arrow::array::StringArray;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(r#"{"lines":[{"sku":"A"}]}"#),
                Some(r#"{"status":"DRAFT"}"#), // no lines at all
                None,                          // no payload at all
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let out = run_on(
        batch,
        "SELECT o.order_id, json_extract_scalar(li, '$.sku') AS sku \
         FROM source o \
         CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY(JSON))) AS t(li)",
    )
    .await;
    let rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "a missing path is NULL and a NULL array expands to nothing, as in Trino"
    );
}

#[tokio::test]
async fn the_two_spellings_of_unnest_agree() {
    // The premise of the whole tool: the model means the same thing in the warehouse and
    // here. If these ever diverge, one of the two engines is being lied to.
    let trino =
        run("SELECT o.order_id, li.sku FROM source o CROSS JOIN UNNEST(o.line_items) AS t(li)")
            .await;
    let datafusion =
        run("SELECT order_id, li.sku FROM (SELECT order_id, unnest(line_items) AS li FROM source)")
            .await;
    let count = |b: &Vec<deltalake::arrow::array::RecordBatch>| -> usize {
        b.iter().map(|x| x.num_rows()).sum()
    };
    assert_eq!(count(&trino), count(&datafusion));
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

// ---------------------------------------------------------------- DECIMAL to DOUBLE

/// `SEVENTEEN_DIGIT_DOUBLES` as DECIMAL(38,17) values, unscaled.
const UNSCALED: [i128; 4] = [
    49979999999999997,
    90170000000000010,
    45909999999999995,
    40380000000000005,
];

/// One row per value: `dec` is it as a DECIMAL(38,17), `doc` as a JSON number.
fn decimals() -> RecordBatch {
    use deltalake::arrow::array::Decimal128Array;

    let docs: Vec<String> = common::SEVENTEEN_DIGIT_DOUBLES
        .iter()
        .map(|t| format!("{{\"x\":{t}}}"))
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("dec", DataType::Decimal128(38, 17), false),
            Field::new("doc", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(
                Decimal128Array::from(UNSCALED.to_vec())
                    .with_precision_and_scale(38, 17)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(StringArray::from(docs)) as ArrayRef,
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn a_decimal_casts_to_the_double_trino_gives() {
    // Arrow divides the unscaled integer by 10^17 in floating point, which rounds twice:
    // 0.49979999999999997 came out as 0.4998. Trino's DecimalConversions reads a long
    // decimal exactly, and so does every spelling here — except a short decimal, of 18
    // digits or fewer, which Trino divides just as Arrow does.
    let texts = common::SEVENTEEN_DIGIT_DOUBLES;
    let sql = "SELECT CAST(dec AS DOUBLE) AS cast, \
                      TRY_CAST(dec AS DOUBLE) AS try_cast, \
                      dec * CAST(1 AS DOUBLE) AS coerced, \
                      CAST(CAST(json_extract_scalar(doc, '$.x') AS DECIMAL(38,17)) AS DOUBLE) \
                          AS from_json, \
                      CAST(CAST(json_extract_scalar(doc, '$.x') AS DECIMAL(18,17)) AS DOUBLE) \
                          AS short, \
                      CAST(ARRAY[dec] AS ARRAY(DOUBLE))[1] AS element, \
                      CAST(dec AS REAL) AS real \
               FROM source";
    let out = run_on(decimals(), sql).await;
    for name in ["cast", "try_cast", "coerced", "from_json", "element"] {
        common::assert_nearest_doubles(&common::column_of(&out, name), &texts);
    }
    common::assert_nearest_reals(&common::column_of(&out, "real"), &texts);
    // What Trino 480 returns for `CAST(CAST(x AS DECIMAL(18,17)) AS DOUBLE)`: not the nearest
    // double, but its own `(double) unscaled / 1e17`.
    common::assert_nearest_doubles(
        &common::column_of(&out, "short"),
        &[
            "0.4998",
            "0.9017000000000002",
            "0.4590999999999999",
            "0.4038000000000001",
        ],
    );

    // Through a derived table, whose projection is planned as a node of its own.
    let out = run_on(
        decimals(),
        "SELECT v FROM (SELECT CAST(dec AS DOUBLE) AS v FROM source) AS t",
    )
    .await;
    common::assert_nearest_doubles(&common::column_of(&out, "v"), &texts);

    // Unaliased, the column keeps the name DataFusion gives a cast.
    let out = run_on(decimals(), "SELECT CAST(dec AS DOUBLE) FROM source").await;
    let name = out[0].schema().field(0).name().clone();
    assert_eq!(name, "source.dec");
    common::assert_nearest_doubles(&common::column_of(&out, &name), &texts);
}

#[tokio::test]
async fn an_integer_literal_beside_a_decimal_is_typed_as_trino_types_it() {
    // DataFusion reads `0` as a BIGINT, which beside a decimal is DECIMAL(20,0); Trino reads it
    // as an INTEGER, DECIMAL(10,0). Those ten digits decide whether `coalesce(r, 0)` is a short
    // decimal, which Trino divides on its way to a DOUBLE or REAL, or a long one, which it
    // rounds correctly: ddi rounded these where Trino divides. Types and values are what
    // Trino 480 returns.
    use delta_delta_ingest::schema::SchemaCoercer;
    use deltalake::arrow::array::{AsArray, Decimal128Array};
    use deltalake::arrow::datatypes::{Float32Type, Float64Type};

    // DECIMAL(9,2) 1413830.04, past 2^24 unscaled, and DECIMAL(18,8) 1175317522.91864620,
    // past 2^53: both divide to something other than the nearest float and double.
    let batch = || {
        let decimal = |v: i128, p: u8, s: i8| {
            Arc::new(
                Decimal128Array::from(vec![v])
                    .with_precision_and_scale(p, s)
                    .unwrap(),
            ) as ArrayRef
        };
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("r", DataType::Decimal128(9, 2), false),
                Field::new("d", DataType::Decimal128(18, 8), false),
            ])),
            vec![decimal(141383004, 9, 2), decimal(117531752291864620, 18, 8)],
        )
        .unwrap()
    };
    let real: f32 = "1413830.125".parse().unwrap();
    let double: f64 = "1175317522.9186463".parse().unwrap();

    let out = run_on(
        batch(),
        "SELECT coalesce(r, 0) AS coalesced, \
                CASE WHEN r > 0 THEN d ELSE 0 END AS branched, \
                nullif(d, 0) AS nulled, \
                greatest(d, 0) AS greatest, \
                r + 2 AS added, \
                2 - r AS subtracted, \
                r * 2 AS multiplied \
         FROM source",
    )
    .await;
    let schema = out[0].schema();
    for (name, (p, s)) in [
        ("coalesced", (12, 2)),
        ("branched", (18, 8)),
        ("nulled", (18, 8)),
        ("greatest", (18, 8)),
        ("added", (13, 2)),
        ("subtracted", (13, 2)),
        ("multiplied", (20, 2)),
    ] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &DataType::Decimal128(p, s),
            "{name}: Trino's decimal({p},{s})"
        );
    }

    let out = run_on(
        batch(),
        "SELECT CAST(coalesce(r, 0) AS REAL) AS coalesced, \
                CAST(CASE WHEN r > 0 THEN r ELSE 0 END AS REAL) AS branched, \
                transform(ARRAY[r], x -> CAST(coalesce(x, 0) AS REAL))[1] AS in_lambda, \
                CAST(coalesce(d, 0) AS DOUBLE) AS double \
         FROM source",
    )
    .await;
    for name in ["coalesced", "branched", "in_lambda"] {
        let got = common::column_of(&out, name)
            .as_primitive::<Float32Type>()
            .value(0);
        assert_eq!(got.to_bits(), real.to_bits(), "{name}: got {got:?}");
    }
    let got = common::column_of(&out, "double")
        .as_primitive::<Float64Type>()
        .value(0);
    assert_eq!(got.to_bits(), double.to_bits(), "got {got:?}");

    // And landing in a REAL or DOUBLE target column, which converts by the type the
    // transform produced.
    let out = run_on(
        batch(),
        "SELECT coalesce(r, 0) AS r, coalesce(d, 0) AS d FROM source",
    )
    .await;
    let target = SchemaCoercer::new(Arc::new(Schema::new(vec![
        Field::new("r", DataType::Float32, false),
        Field::new("d", DataType::Float64, false),
    ])));
    let landed = target.coerce(&out[0]).unwrap();
    let got = landed.column(0).as_primitive::<Float32Type>().value(0);
    assert_eq!(
        got.to_bits(),
        real.to_bits(),
        "in a REAL column: got {got:?}"
    );
    let got = landed.column(1).as_primitive::<Float64Type>().value(0);
    assert_eq!(
        got.to_bits(),
        double.to_bits(),
        "in a DOUBLE column: got {got:?}"
    );
}

#[tokio::test]
async fn a_quotient_floor_ceil_and_round_of_a_decimal_are_typed_as_trino_types_them() {
    // Arrow gives a quotient four digits of scale more than its dividend, where Trino gives it
    // the divisor's precision and one more; `floor`, `ceil` and `round` keep their argument's
    // precision, or its type, where Trino narrows them; and `round(x, n)` narrows the scale,
    // where Trino keeps it. On the way to a REAL each was divided where Trino rounds it, or the
    // other way about — and more of them once an integer literal beside a decimal was typed as
    // Trino's INTEGER, ten digits narrower than DataFusion's BIGINT, as was one in an
    // `ARRAY[..]` not yet. Types and values are what Trino 480 returns.
    use deltalake::arrow::array::{AsArray, Decimal128Array};
    use deltalake::arrow::datatypes::Float32Type;

    // DECIMAL(9,2) 1413830.04, DECIMAL(12,0) 141383007, DECIMAL(10,2) 125.00 and DECIMAL(18,2)
    // 2000000.12: results past 2^24 unscaled, which divide to something other than the
    // nearest float.
    let batch = || {
        let decimal = |v: i128, p: u8, s: i8| {
            Arc::new(
                Decimal128Array::from(vec![v])
                    .with_precision_and_scale(p, s)
                    .unwrap(),
            ) as ArrayRef
        };
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("r", DataType::Decimal128(9, 2), false),
                Field::new("amount", DataType::Decimal128(12, 0), false),
                Field::new("a", DataType::Decimal128(10, 2), false),
                Field::new("big", DataType::Decimal128(18, 2), false),
            ])),
            vec![
                decimal(141383004, 9, 2),
                decimal(141383007, 12, 0),
                decimal(12500, 10, 2),
                decimal(200000012, 18, 2),
            ],
        )
        .unwrap()
    };

    let out = run_on(
        batch(),
        "SELECT amount / 100 AS quotient, \
                coalesce(amount, 0) / 100 AS coalesced, \
                123456789 / a AS literal_dividend, \
                floor(r) AS floored, \
                floor(coalesce(r, 0)) AS floored_coalesced, \
                ceil(r - 1) AS ceiled, \
                round(r) AS rounded, \
                round(big, 1) AS rounded_to_one \
         FROM source",
    )
    .await;
    let schema = out[0].schema();
    for (name, (p, s)) in [
        ("quotient", (23, 11)),
        ("coalesced", (23, 11)),
        ("literal_dividend", (23, 11)),
        ("floored", (8, 0)),
        ("floored_coalesced", (11, 0)),
        ("ceiled", (12, 0)),
        ("rounded", (8, 0)),
        ("rounded_to_one", (19, 2)),
    ] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &DataType::Decimal128(p, s),
            "{name}: Trino's decimal({p},{s})"
        );
    }

    let out = run_on(
        batch(),
        "SELECT CAST(amount / 100 AS REAL) AS quotient, \
                CAST(coalesce(amount, 0) / 100 AS REAL) AS coalesced, \
                CAST(123456789 / a AS REAL) AS literal_dividend, \
                CAST(floor(r) AS REAL) AS floored, \
                CAST(floor(coalesce(r, 0)) AS REAL) AS floored_coalesced, \
                CAST(ceil(r - 1) AS REAL) AS ceiled, \
                CAST(round(big, 1) AS REAL) AS rounded_to_one, \
                CAST(ARRAY[r, 0] AS ARRAY(REAL))[1] AS in_an_array, \
                transform(ARRAY[amount], x -> CAST(coalesce(x, 0) / 100 AS REAL))[1] \
                    AS in_lambda \
         FROM source",
    )
    .await;
    for (name, trino) in [
        ("quotient", "1413830.125"),
        ("coalesced", "1413830.125"),
        ("literal_dividend", "987654.3125"),
        ("floored", "1413830.0"),
        ("floored_coalesced", "1413830.0"),
        ("ceiled", "1413830.0"),
        ("rounded_to_one", "2000000.125"),
        ("in_an_array", "1413830.125"),
        ("in_lambda", "1413830.125"),
    ] {
        let want: f32 = trino.parse().unwrap();
        let got = common::column_of(&out, name)
            .as_primitive::<Float32Type>()
            .value(0);
        assert_eq!(
            got.to_bits(),
            want.to_bits(),
            "{name}: got {got:?}, Trino gives {want:?}"
        );
    }
}

#[tokio::test]
async fn a_sum_over_a_decimal_is_typed_as_trino_types_it() {
    // DataFusion sums a DECIMAL(p,s) to DECIMAL(p+10,s), so over 8 digits or fewer to a short
    // decimal, which Trino divides on its way to a REAL; Trino's own sum is DECIMAL(38,s)
    // whatever p is, and correctly rounded. Summed in a publication, the one place a model
    // may aggregate, 706915.02 twice as a DECIMAL(8,2) was the REAL 1413830.125, where Trino
    // gives 1413830.0. Types and values are what Trino 480 returns.
    use delta_delta_ingest::schema::SchemaCoercer;
    use deltalake::arrow::array::{AsArray, Decimal128Array};
    use deltalake::arrow::datatypes::{Decimal128Type, Float32Type, Float64Type};

    // DECIMAL(8,2) 706915.02 and DECIMAL(8,3) 80164.834, twice: sums past 2^24 unscaled that
    // divide to something other than the nearest float.
    let batch = || {
        let decimal = |v: i128, p: u8, s: i8| {
            Arc::new(
                Decimal128Array::from(vec![v, v])
                    .with_precision_and_scale(p, s)
                    .unwrap(),
            ) as ArrayRef
        };
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("r", DataType::Decimal128(8, 2), false),
                Field::new("y", DataType::Decimal128(8, 3), false),
            ])),
            vec![decimal(70691502, 8, 2), decimal(80164834, 8, 3)],
        )
        .unwrap()
    };
    let publish = |sql: &'static str| async move {
        let b = batch();
        SqlTransform::new_per_batch(sql)
            .run(b.schema(), vec![b])
            .await
            .expect("publication should succeed")
    };

    let out = publish(
        "SELECT sum(r), sum(y) AS y_sum, min(r) AS r_min, max(y) AS y_max, \
                sum(CASE WHEN r < 0 THEN r ELSE y END) AS branched \
         FROM source",
    )
    .await;
    let schema = out[0].schema();
    for (name, (p, s)) in [
        // Unaliased, and still called what DataFusion calls it.
        ("sum(source.r)", (38, 2)),
        ("y_sum", (38, 3)),
        ("r_min", (8, 2)),
        ("y_max", (8, 3)),
        // The branches coerce to DECIMAL(9,3); typed by its first alone, the CASE would have
        // been summed at a scale of 2 and lost a digit.
        ("branched", (38, 3)),
    ] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &DataType::Decimal128(p, s),
            "{name}: Trino's decimal({p},{s})"
        );
    }
    let branched = common::column_of(&out, "branched");
    assert_eq!(
        branched.as_primitive::<Decimal128Type>().value(0),
        160329668,
        "160329.668"
    );

    let out = publish(
        "SELECT CAST(sum(r) AS REAL) AS r_real, CAST(sum(r) AS DOUBLE) AS r_double, \
                CAST(sum(y) AS REAL) AS y_real, CAST(sum(y) AS DOUBLE) AS y_double, \
                CAST(sum(r) * 2 AS REAL) AS doubled, CAST(sum(r) + 1 AS DOUBLE) AS plus_one \
         FROM source \
         HAVING sum(r) > 1000",
    )
    .await;
    for (name, want) in [
        ("r_real", "1413830.0"),
        ("y_real", "160329.67"),
        ("doubled", "2827660.0"),
    ] {
        let want: f32 = want.parse().unwrap();
        let got = common::column_of(&out, name)
            .as_primitive::<Float32Type>()
            .value(0);
        assert_eq!(got.to_bits(), want.to_bits(), "{name}: got {got:?}");
    }
    for (name, want) in [
        ("r_double", "1413830.04"),
        ("y_double", "160329.668"),
        ("plus_one", "1413831.04"),
    ] {
        let want: f64 = want.parse().unwrap();
        let got = common::column_of(&out, name)
            .as_primitive::<Float64Type>()
            .value(0);
        assert_eq!(got.to_bits(), want.to_bits(), "{name}: got {got:?}");
    }

    // And landing in a REAL target column, which converts by the type the model produced.
    let out = publish("SELECT sum(r) AS r FROM source").await;
    let target = SchemaCoercer::new(Arc::new(Schema::new(vec![Field::new(
        "r",
        DataType::Float32,
        false,
    )])));
    let landed = target.coerce(&out[0]).unwrap();
    let got = landed.column(0).as_primitive::<Float32Type>().value(0);
    let want: f32 = "1413830.0".parse().unwrap();
    assert_eq!(
        got.to_bits(),
        want.to_bits(),
        "in a REAL column: got {got:?}"
    );
}

#[tokio::test]
async fn array_sum_over_decimals_starts_from_the_nearest_doubles() {
    // The array aggregates read their elements as doubles, and a decimal element becomes the
    // nearest one: a one-element array sums to exactly it.
    use deltalake::arrow::array::Decimal128Array;

    let dec = DataType::Decimal128(38, 17);
    let values = || {
        Arc::new(
            Decimal128Array::from(UNSCALED.to_vec())
                .with_precision_and_scale(38, 17)
                .unwrap(),
        ) as ArrayRef
    };
    let one_each = || OffsetBuffer::new(vec![0, 1, 2, 3, 4].into());
    let plain = ListArray::new(
        Arc::new(Field::new("item", dec.clone(), true)),
        one_each(),
        values(),
        None,
    );
    let item_fields: Fields = vec![Arc::new(Field::new("price", dec.clone(), true))].into();
    let items = ListArray::new(
        Arc::new(Field::new(
            "item",
            DataType::Struct(item_fields.clone()),
            true,
        )),
        one_each(),
        Arc::new(StructArray::new(item_fields, vec![values()], None)),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("prices", plain.data_type().clone(), false),
            Field::new("items", items.data_type().clone(), false),
        ])),
        vec![Arc::new(plain) as ArrayRef, Arc::new(items) as ArrayRef],
    )
    .unwrap();

    let out = run_on(
        batch,
        "SELECT array_sum(prices) AS plain, array_sum(items, 'price') AS field FROM source",
    )
    .await;
    for name in ["plain", "field"] {
        common::assert_nearest_doubles(
            &common::column_of(&out, name),
            &common::SEVENTEEN_DIGIT_DOUBLES,
        );
    }
}
