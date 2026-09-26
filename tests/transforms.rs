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
async fn a_quotient_rounds_its_last_digit_as_trino_rounds_it() {
    // Arrow truncates a quotient where Trino rounds its last digit half up, so the dividend is
    // scaled for Arrow to compute one digit more, which the cast to Trino's type rounds. Past
    // 38 digits in 128 bits there was no room for that digit, and a big enough dividend
    // overflowed where Trino divides. Values are what Trino 480 returns.
    use deltalake::arrow::array::{AsArray, Decimal128Array};
    use deltalake::arrow::datatypes::Decimal128Type;

    let decimal = |v: i128, p: u8, s: i8| {
        Arc::new(
            Decimal128Array::from(vec![v])
                .with_precision_and_scale(p, s)
                .unwrap(),
        ) as ArrayRef
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("d", DataType::Decimal128(18, 8), false),
            Field::new("r", DataType::Decimal128(9, 2), false),
            Field::new("cent", DataType::Decimal128(5, 2), false),
            Field::new("rate", DataType::Decimal128(2, 1), false),
            Field::new("two", DataType::Decimal128(1, 0), false),
            Field::new("big", DataType::Decimal128(38, 0), false),
            Field::new("big33", DataType::Decimal128(33, 0), false),
            Field::new("seven", DataType::Decimal128(5, 0), false),
        ])),
        vec![
            decimal(117531752291864620, 18, 8),
            decimal(141383004, 9, 2),
            decimal(1, 5, 2),
            decimal(64, 2, 1),
            decimal(2, 1, 0),
            decimal(2 * 10i128.pow(33), 38, 0),
            decimal(2 * 10i128.pow(32), 33, 0),
            decimal(7, 5, 0),
        ],
    )
    .unwrap();

    let out = run_on(
        batch,
        "SELECT d / r AS inexact, \
                -d / r AS negative, \
                cent / rate AS tie, \
                -cent / rate AS negative_tie, \
                two / 3 AS thirds, \
                big / 100 AS big, \
                big33 / seven AS big_and_inexact \
         FROM source",
    )
    .await;
    for (name, (p, s), trino) in [
        // Truncated, each of these ends a digit lower.
        ("inexact", (30, 18), 831300431923660498825i128),
        ("negative", (30, 18), -831300431923660498825),
        ("tie", (10, 6), 1563),
        ("negative_tie", (10, 6), -1563),
        ("thirds", (12, 11), 66666666667),
        // Of 38 digits, which overflowed in 128 bits, or had no room for the digit to round by.
        ("big", (38, 6), 2 * 10i128.pow(37)),
        (
            "big_and_inexact",
            (38, 6),
            28571428571428571428571428571428571429,
        ),
    ] {
        let column = common::column_of(&out, name);
        assert_eq!(
            column.data_type(),
            &DataType::Decimal128(p, s),
            "{name}: Trino's decimal({p},{s})"
        );
        assert_eq!(
            column.as_primitive::<Decimal128Type>().value(0),
            trino,
            "{name}: unscaled, at scale {s}"
        );
    }
}

#[tokio::test]
async fn floor_and_ceil_at_the_top_of_a_decimals_range_round_as_trino_rounds_them() {
    // DataFusion checks `floor` and `ceil` against their argument's own precision, so rounding
    // DECIMAL(5,2) 999.50 up to 1000.00 failed, and the row was set aside as one the model
    // cannot evaluate, where Trino's DECIMAL(4,0) holds 1000. As a DECIMAL(2,2), every positive
    // fraction's `ceil` failed.
    use deltalake::arrow::array::{AsArray, Decimal128Array};
    use deltalake::arrow::datatypes::Decimal128Type;

    let decimal = |v: i128, p: u8, s: i8| {
        Arc::new(
            Decimal128Array::from(vec![v])
                .with_precision_and_scale(p, s)
                .unwrap(),
        ) as ArrayRef
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("price", DataType::Decimal128(5, 2), false),
            Field::new("fraction", DataType::Decimal128(2, 2), false),
            Field::new("widest", DataType::Decimal128(38, 2), false),
        ])),
        vec![
            decimal(99950, 5, 2),
            decimal(25, 2, 2),
            decimal(10i128.pow(38) - 50, 38, 2),
        ],
    )
    .unwrap();

    let out = run_on(
        batch,
        "SELECT ceil(price) AS ceiled, \
                floor(-price) AS floored, \
                ceil(fraction) AS fraction_ceiled, \
                floor(-fraction) AS fraction_floored, \
                ceil(widest) AS widest \
         FROM source",
    )
    .await;
    for (name, (p, s), trino) in [
        ("ceiled", (4, 0), 1000i128),
        ("floored", (4, 0), -1000),
        ("fraction_ceiled", (1, 0), 1),
        ("fraction_floored", (1, 0), -1),
        ("widest", (37, 0), 10i128.pow(36)),
    ] {
        let column = common::column_of(&out, name);
        assert_eq!(
            column.data_type(),
            &DataType::Decimal128(p, s),
            "{name}: Trino's decimal({p},{s})"
        );
        assert_eq!(
            column.as_primitive::<Decimal128Type>().value(0),
            trino,
            "{name}"
        );
    }
}

#[tokio::test]
async fn an_integer_expression_beside_a_decimal_is_typed_as_trino_types_it() {
    // DataFusion makes an INTEGER combined with a literal a BIGINT, and Trino keeps the
    // INTEGER: `amount / nullif(qty, 0)`, the usual safe division, was a DECIMAL(32,22) where
    // Trino's is DECIMAL(23,13), so its DOUBLE had nine digits Trino's has not; and
    // `p * coalesce(q, 1)` was a long decimal, correctly rounded to a REAL, where Trino's
    // DECIMAL(18,2) is divided. Types and values are what Trino 480 returns.
    use deltalake::arrow::array::{AsArray, Decimal128Array, Int32Array};
    use deltalake::arrow::datatypes::{Float32Type, Float64Type};

    let decimal = |v: i128, p: u8, s: i8| {
        Arc::new(
            Decimal128Array::from(vec![v])
                .with_precision_and_scale(p, s)
                .unwrap(),
        ) as ArrayRef
    };
    let batch = || {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Decimal128(12, 2), false),
                Field::new("qty", DataType::Int32, false),
                Field::new("p", DataType::Decimal128(7, 2), false),
                Field::new("q", DataType::Int32, false),
            ])),
            vec![
                decimal(10000, 12, 2),
                Arc::new(Int32Array::from(vec![3])) as ArrayRef,
                decimal(9425507, 7, 2),
                Arc::new(Int32Array::from(vec![15])) as ArrayRef,
            ],
        )
        .unwrap()
    };

    let out = run_on(
        batch(),
        "SELECT amount / nullif(qty, 0) AS nulled, \
                amount / (60 * 60) AS product_of_literals, \
                amount / coalesce(qty, 1) AS coalesced, \
                amount / (qty + 1) AS added, \
                amount / -qty AS negated, \
                amount / CASE WHEN qty > 0 THEN 100 ELSE 1000 END AS branched, \
                amount + (qty + 1) AS sum_of_them, \
                p * coalesce(q, 1) AS multiplied \
         FROM source",
    )
    .await;
    let schema = out[0].schema();
    for (name, (p, s)) in [
        ("nulled", (23, 13)),
        ("product_of_literals", (23, 13)),
        ("coalesced", (23, 13)),
        ("added", (23, 13)),
        ("negated", (23, 13)),
        ("branched", (23, 13)),
        ("sum_of_them", (13, 2)),
        ("multiplied", (18, 2)),
    ] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &DataType::Decimal128(p, s),
            "{name}: Trino's decimal({p},{s})"
        );
    }

    let out = run_on(
        batch(),
        "SELECT CAST(amount / nullif(qty, 0) AS DOUBLE) AS nulled, \
                CAST(amount / (60 * 60) AS DOUBLE) AS product_of_literals, \
                CAST(p * coalesce(q, 1) AS REAL) AS multiplied \
         FROM source",
    )
    .await;
    for (name, trino) in [
        ("nulled", "33.3333333333333"),
        ("product_of_literals", "0.0277777777778"),
    ] {
        let want: f64 = trino.parse().unwrap();
        let got = common::column_of(&out, name)
            .as_primitive::<Float64Type>()
            .value(0);
        assert_eq!(
            got.to_bits(),
            want.to_bits(),
            "{name}: got {got:?}, Trino gives {want:?}"
        );
    }
    let want: f32 = "1413826.125".parse().unwrap();
    let got = common::column_of(&out, "multiplied")
        .as_primitive::<Float32Type>()
        .value(0);
    assert_eq!(
        got.to_bits(),
        want.to_bits(),
        "got {got:?}, Trino gives {want:?}"
    );
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
async fn a_quotient_or_round_of_a_sum_is_typed_from_trinos_sum() {
    // Trino sums a DECIMAL(p,s) to DECIMAL(38,s), and a quotient or a `round` of the sum is
    // typed from that. Here they were typed from DataFusion's DECIMAL(p+10,s), before the sum
    // was widened: `sum(s) / count(*)` over a DECIMAL(8,2) was a DECIMAL(38,22) where Trino's
    // is DECIMAL(38,6), with sixteen digits Trino's DOUBLE has not, and `round(sum(t), 2)` over
    // a DECIMAL(7,2) a short DECIMAL(18,2), divided on its way to a REAL, where Trino's
    // DECIMAL(38,2) is correctly rounded. Types and values are what Trino 480 returns.
    use deltalake::arrow::array::{AsArray, Decimal128Array};
    use deltalake::arrow::datatypes::{Float32Type, Float64Type};

    let column = |name: &str, p: u8, s: i8, unscaled: Vec<i128>| {
        let values = Decimal128Array::from(unscaled)
            .with_precision_and_scale(p, s)
            .unwrap();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                name,
                DataType::Decimal128(p, s),
                false,
            )])),
            vec![Arc::new(values) as ArrayRef],
        )
        .unwrap()
    };
    let publish = |batch: RecordBatch, sql: &'static str| async move {
        SqlTransform::new_per_batch(sql)
            .run(batch.schema(), vec![batch])
            .await
            .expect("publication should succeed")
    };
    // 1413831.28, whose thirds and sevenths are inexact.
    let means = || column("s", 8, 2, vec![70691502, 70691503, 123]);
    // 14 × 99999.99 and 13830.18, 1413830.04: past 2^24 unscaled.
    let totals = || {
        let mut rows = vec![9999999; 14];
        rows.push(1383018);
        column("t", 7, 2, rows)
    };

    let out = publish(
        means(),
        "SELECT sum(s) / count(*) AS mean, sum(s) / 7 AS seventh FROM source",
    )
    .await;
    let out_rounded = publish(totals(), "SELECT round(sum(t), 2) AS rounded FROM source").await;
    for (out, name, (p, s)) in [
        (&out, "mean", (38, 6)),
        (&out, "seventh", (38, 6)),
        (&out_rounded, "rounded", (38, 2)),
    ] {
        assert_eq!(
            out[0].schema().field_with_name(name).unwrap().data_type(),
            &DataType::Decimal128(p, s),
            "{name}: Trino's decimal({p},{s})"
        );
    }

    let out = publish(
        means(),
        "SELECT CAST(sum(s) / count(*) AS DOUBLE) AS mean, \
                CAST(sum(s) / 7 AS DOUBLE) AS seventh \
         FROM source",
    )
    .await;
    for (name, trino) in [("mean", "471277.093333"), ("seventh", "201975.897143")] {
        let want: f64 = trino.parse().unwrap();
        let got = common::column_of(&out, name)
            .as_primitive::<Float64Type>()
            .value(0);
        assert_eq!(
            got.to_bits(),
            want.to_bits(),
            "{name}: got {got:?}, Trino gives {want:?}"
        );
    }
    let out = publish(
        totals(),
        "SELECT CAST(round(sum(t), 2) AS REAL) AS rounded FROM source",
    )
    .await;
    let want: f32 = "1413830.0".parse().unwrap();
    let got = common::column_of(&out, "rounded")
        .as_primitive::<Float32Type>()
        .value(0);
    assert_eq!(
        got.to_bits(),
        want.to_bits(),
        "got {got:?}, Trino gives {want:?}"
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
