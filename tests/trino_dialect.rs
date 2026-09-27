//! The SQL `ddi dbt convert` writes, read by a real Trino.
//!
//! A converted `transform_sql` has two readers. This engine is one, and every other test
//! covers it. The other is whatever a deployment hands the same text to, and for a dbt
//! project written in Trino's spelling that is usually Trino itself. Rendering a parsed
//! model can say things Trino has no grammar for — every `FORMAT JSON` once came out as a
//! `::JSON` cast — and nothing short of Trino's own parser proves it does not.
//!
//! Each model here is valid Trino, and is checked to be. Its converted text then has to
//! parse there too. Parsing is all that is asked: the relations do not exist, so Trino's
//! analyser refuses every statement for naming a relation it cannot resolve, and that
//! refusal is what proves the parser accepted it. Any other refusal fails the test, since a
//! Trino still starting up refuses everything before parsing anything.
//!
//! One more question only a real Trino settles is what its JSON functions write. Jackson
//! has two writers and Trino uses both: one that writes bytes and escapes an emoji as its
//! UTF-16 surrogate pair, and one that writes a Java `String` and keeps it. Which function
//! goes through which is read from Trino's source; `json_text_spells_an_emoji_as_trino_does`
//! asks Trino itself, and compares its bytes with this engine's. In the same way,
//! `from_unixtime_falls_on_the_date_trino_gives` asks it which day an epoch falls on in a
//! zone, in each form of `from_unixtime` this engine rewrites, and
//! `from_unixtime_reads_the_wall_clock_trino_gives` what the local clock reads. Which DOUBLE
//! or REAL a DECIMAL converts to follows Trino's types as much as its arithmetic, and both
//! were read from its source too; `a_decimal_converts_to_the_double_or_real_trino_gives` asks
//! Trino for the value, and `a_json_number_reads_as_the_double_trino_gives` what a 17-digit
//! JSON number reads as. A float is compared bit for bit.
//!
//! Ignored by default because it needs a Trino listening. CI starts one and runs this with
//! `--ignored`. To run it locally:
//!
//! ```bash
//! docker run -d --name trino -p 8080:8080 trinodb/trino:480
//! DDI_TEST_TRINO=http://127.0.0.1:8080 cargo test --test trino_dialect -- --ignored
//! ```

mod common;

use std::sync::Arc;

use delta_delta_ingest::dbt::analyze::{analyze, Verdict};
use delta_delta_ingest::dbt::Manifest;
use delta_delta_ingest::transform::{SqlTransform, Transform};
use delta_delta_ingest::trino::{TrinoClient, TrinoConnection};
use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray,
};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::{
    DataType, Field, Fields, Float32Type, Float64Type, Schema, TimeUnit,
};

/// Streamable models in Trino's spelling: `FORMAT JSON` in every position this engine reads
/// it, and other constructs a converted model is rendered from its parse tree with.
const MODELS: &[&str] = &[
    // The outbox shape: an array of JSON built per row and embedded as JSON.
    "with orders as (
         select o.id, cast(json_extract(o.data, '$.lines') as array(json)) as lines
         from bronze.orders as o
     )
     select orders.id,
            json_object(
                key 'id' value orders.id,
                'items' value json_format(cast(transform(
                    filter(orders.lines, l -> json_extract_scalar(l, '$.kind') <> 'x'),
                    l -> json_parse(json_object(
                        'sku' value json_extract_scalar(l, '$.sku'),
                        'qty' value cast(json_extract_scalar(l, '$.qty') as integer)))
                ) as json)) format json
            ) as message
     from orders",
    "SELECT json_object('a' VALUE o.x || o.y FORMAT JSON, 'b' VALUE 1) AS j FROM bronze.orders o",
    "SELECT json_array(o.x FORMAT JSON ENCODING UTF8) AS j FROM bronze.orders o",
    "SELECT json_array(o.a, o.b FORMAT JSON, o.c) AS j FROM bronze.orders o",
    "SELECT json_object('data' VALUE json_object('k' VALUE o.v) FORMAT JSON) AS j \
     FROM bronze.orders o",
    "SELECT json_object('a' : 1, 'b' : o.x FORMAT JSON WITHOUT UNIQUE KEYS) AS j \
     FROM bronze.orders o",
    "SELECT json_object('a' VALUE CASE WHEN o.b THEN json_format(CAST(o.c AS JSON)) \
     ELSE '[]' END FORMAT JSON) AS j FROM bronze.orders o",
    "SELECT json_object('a' VALUE (o.x) FORMAT JSON) AS j FROM bronze.orders o",
    "SELECT json_object('a' VALUE 1 RETURNING VARCHAR FORMAT JSON) AS j FROM bronze.orders o",
    "SELECT json_object('a' VALUE o.x NULL ON NULL) AS j, json_array(o.a, o.b ABSENT ON NULL) AS k \
     FROM bronze.orders o",
    "SELECT o.id, t.line FROM bronze.orders o \
     CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY(JSON))) AS t(line)",
    "SELECT json_object(upper(o.k) VALUE U&'caf\\00e9') AS j FROM bronze.orders o",
    // A value that says FORMAT JSON inside a value that says it too.
    "SELECT json_object('d' VALUE json_array(o.x FORMAT JSON) FORMAT JSON) AS j \
     FROM bronze.orders o",
    // The path functions' input.
    "SELECT json_value(o.data FORMAT JSON, 'lax $.a') AS a, \
     json_query(o.data FORMAT JSON, 'lax $.b') AS b, \
     json_exists(o.data FORMAT JSON, 'lax $.c') AS c FROM bronze.orders o",
    "SELECT o.id, \"o\".\"Weird\"\"Name\", from_unixtime(o.ts, 'Europe/Amsterdam') AS local_ts, \
     CAST(o.amount AS DECIMAL(18, 4)) AS amount, o.xs[1] AS first_x, \
     o.a IS DISTINCT FROM o.b AS changed, coalesce(o.c, 'x') || 'y' AS label \
     FROM bronze.orders o WHERE o.id IS NOT NULL",
    // Every from_unixtime form this engine accepts: each is rewritten before it runs here,
    // and the converted text keeps the model's own spelling.
    "SELECT from_unixtime(o.ts) AS a, from_unixtime(o.ts, 'UTC') AS b, \
     from_unixtime(o.ts, 5, 30) AS c, from_unixtime(o.ts) AT TIME ZONE 'UTC' AS d \
     FROM bronze.orders o",
    "SELECT o.ts AT TIME ZONE 'UTC' AS utc_ts, o.d + INTERVAL '1' DAY AS next_day, \
     TIMESTAMP '2024-01-01 00:00:00' AS epoch_ts, DATE '2024-01-01' AS epoch_day FROM bronze.orders o",
    "SELECT TRY_CAST(o.a AS BIGINT) AS a, CAST(o.r AS ROW(x INTEGER, y VARCHAR)) AS r, \
     CAST(o.m AS MAP(VARCHAR, INTEGER)) AS m, CAST(o.xs AS ARRAY(VARCHAR)) AS xs FROM bronze.orders o",
    "SELECT ARRAY[o.a, o.b] AS pair, o.c LIKE 'x!_%' ESCAPE '!' AS like_x, \
     filter(o.xs, x -> x > 0) AS positive FROM bronze.orders o",
];

fn manifest(model: &str) -> Manifest {
    let manifest = serde_json::json!({
        "nodes": {
            "model.p.converted": {
                "name": "converted",
                "resource_type": "model",
                "schema": "silver",
                "compiled_code": model,
                "depends_on": {"nodes": ["source.p.bronze.orders"]},
                "config": {"materialized": "table"}
            }
        },
        "sources": {
            "source.p.bronze.orders": {
                "name": "orders",
                "resource_type": "source",
                "schema": "bronze"
            }
        }
    });
    Manifest::from_json(&manifest.to_string()).unwrap()
}

fn trino() -> TrinoClient {
    let url = std::env::var("DDI_TEST_TRINO")
        .expect("set DDI_TEST_TRINO to a Trino coordinator, e.g. http://127.0.0.1:8080");
    let url = url::Url::parse(&url).expect("DDI_TEST_TRINO is not a URL");
    TrinoClient::new(TrinoConnection {
        host: url
            .host_str()
            .expect("DDI_TEST_TRINO has no host")
            .to_string(),
        port: url
            .port_or_known_default()
            .expect("DDI_TEST_TRINO has no port"),
        user: "ddi-test".into(),
        password: None,
        http_scheme: url.scheme().to_string(),
        catalog: None,
        schema: None,
        verify_tls: true,
    })
    .unwrap()
}

/// What Trino says about a statement naming relations that do not exist, once it has parsed
/// it: the analyser cannot resolve the first one.
const UNRESOLVED_RELATION: [&str; 5] = [
    "[MISSING_CATALOG_NAME]",
    "[MISSING_SCHEMA_NAME]",
    "[CATALOG_NOT_FOUND]",
    "[SCHEMA_NOT_FOUND]",
    "[TABLE_NOT_FOUND]",
];

/// `None` when Trino's parser accepted `sql`, otherwise its syntax error.
async fn syntax_error(trino: &TrinoClient, sql: &str) -> Option<String> {
    let e = match trino.query(sql).await {
        Ok(_) => return None,
        Err(e) => e.to_string(),
    };
    if e.contains("[SYNTAX_ERROR]") {
        return Some(e);
    }
    // Only a refusal that comes after parsing may pass for a parse. Anything else — a Trino
    // still starting, one out of reach — would otherwise pass every statement unread.
    assert!(
        UNRESOLVED_RELATION.iter().any(|name| e.contains(name)),
        "Trino refused the statement before it could say whether it parses: {e}\n{sql}"
    );
    None
}

#[tokio::test]
#[ignore = "needs a Trino coordinator; set DDI_TEST_TRINO"]
async fn converted_transform_sql_parses_in_trino() {
    let trino = trino();
    // Both answers the test relies on, from this Trino, before trusting either.
    assert!(
        syntax_error(
            &trino,
            "SELECT json_array((o.x)::JSON) AS j FROM bronze.orders o"
        )
        .await
        .is_some(),
        "this Trino does not refuse `::`, so it cannot catch what this test is for"
    );
    assert!(syntax_error(&trino, "SELECT o.x FROM bronze.orders o")
        .await
        .is_none());

    let mut failures = Vec::new();
    for model in MODELS {
        if let Some(e) = syntax_error(&trino, model).await {
            panic!("the model itself is not valid Trino, so it proves nothing: {e}\n{model}");
        }
        let sql = match analyze(&manifest(model), "model.p.converted") {
            Verdict::Streamable(s) => s.transform_sql.expect("a streamable model has SQL"),
            other => panic!("expected a streamable model, got {other:?}\n{model}"),
        };
        assert!(!sql.contains("::"), "Trino has no `::`: {sql}");
        if let Some(e) = syntax_error(&trino, &sql).await {
            failures.push(format!("{e}\n  converted: {sql}"));
        }
    }
    assert!(
        failures.is_empty(),
        "converted SQL Trino cannot parse:\n{}",
        failures.join("\n")
    );
}

/// One source both engines read: Trino from `from`, a `VALUES` clause that names its
/// relation `source`, and this engine from `batch`, which holds the same rows.
struct Source {
    from: String,
    batch: RecordBatch,
}

/// The first value of `SELECT <expr> AS v FROM source` for each of `exprs`, in Trino and
/// here, one line per expression where the two differ. `per_batch` plans the statement as a
/// publication, the one place a model may aggregate.
async fn differences<E: AsRef<str>>(
    trino: &TrinoClient,
    source: &Source,
    exprs: &[E],
    per_batch: bool,
) -> Vec<String> {
    let mut differ = Vec::new();
    for expr in exprs {
        let expr = expr.as_ref();
        let theirs = trino
            .query(&format!("SELECT {expr} AS v FROM {}", source.from))
            .await
            .unwrap_or_else(|e| panic!("Trino refused {expr}: {e}"))
            .scalar();
        let sql = format!("SELECT {expr} AS v FROM source");
        let transform = if per_batch {
            SqlTransform::new_per_batch(sql)
        } else {
            SqlTransform::new(sql)
        };
        let out = match transform.apply(vec![source.batch.clone()]).await {
            Ok(out) => out,
            Err(e) => {
                differ.push(format!("{expr}\n  Trino: {theirs:?}\n  ddi:   failed, {e}"));
                continue;
            }
        };
        let (theirs, ours) = spelt_alike(theirs, out[0].column(0));
        if theirs != ours {
            differ.push(format!("{expr}\n  Trino: {theirs:?}\n  ddi:   {ours:?}"));
        }
    }
    differ
}

/// Trino's text for a value and this engine's first value in `col`, spelt so that equal
/// values compare equal. A DOUBLE or REAL is spelt as Rust spells that float, with Trino's
/// text read back as the same type: the two engines spell a float differently, and each
/// spelling reads back exactly, so equal text is equal bits. Anything else is its text.
fn spelt_alike(theirs: Option<String>, col: &ArrayRef) -> (Option<String>, Option<String>) {
    if col.is_null(0) {
        return (theirs, None);
    }
    match col.data_type() {
        DataType::Float64 => (
            theirs.map(|t| t.parse::<f64>().map_or(t, |v| format!("{v:?}"))),
            Some(format!("{:?}", col.as_primitive::<Float64Type>().value(0))),
        ),
        DataType::Float32 => (
            theirs.map(|t| t.parse::<f32>().map_or(t, |v| format!("{v:?}"))),
            Some(format!("{:?}", col.as_primitive::<Float32Type>().value(0))),
        ),
        _ => {
            let text = cast(col, &DataType::Utf8).unwrap();
            (theirs, Some(text.as_string::<i32>().value(0).to_string()))
        }
    }
}

/// A document with an emoji in a key, in a string, in a container and as an array element,
/// spelt as the character and as a lower-case escape.
const EMOJI_DOC: &str = r#"{"！":1,"😊":["😊"],"s":"😊","e":"\ud83d\ude00","l":[{"a":"😊"},"😊"]}"#;

/// The JSON functions and the ways a value reaches a constructor or a cast, over
/// [`EMOJI_DOC`] and a row holding an emoji. `json_query` and `json_value` are missing
/// because no spelling of them runs in both engines yet: Trino requires a `lax` or `strict`
/// mode on their path, and this engine does not read one. So is `CAST(.. AS JSON)` of a
/// MAP, which this engine refuses, and of a `ROW(..)` literal, whose fields DataFusion names
/// `c0`, `c1` where Trino leaves them unnamed.
const EMOJI_JSON: &[&str] = &[
    // Written as bytes: the surrogate pair, upper case.
    "json_format(json_parse(data))",
    "json_parse(data)",
    "json_format(json_parse('\"😊\"'))",
    "json_format(json_extract(data, '$'))",
    "json_extract(data, '$.l')",
    "json_format(json_extract(data, '$.e'))",
    "json_format(json_extract(data, '$[\"😊\"]'))",
    "json_object('k' VALUE json_extract_scalar(data, '$.s'), 'n' VALUE json_array(1, 'x😊'))",
    "json_object(json_extract_scalar(data, '$.s') VALUE 1)",
    // In HashMap order, which hashes the emoji as its two UTF-16 halves.
    "json_object('naïve' VALUE 1, '😀' VALUE 2, 'a' VALUE 3, '日本' VALUE 4, 'b' VALUE 5)",
    "json_array(json_extract_scalar(data, '$.e'), 'x')",
    // A JSON scalar as a member is cast to varchar first, and escaped again as text.
    "json_array(json_extract(data, '$.s'))",
    // Text after FORMAT JSON is read and written again.
    "json_object('d' VALUE json_format(json_extract(data, '$.l')) FORMAT JSON)",
    "json_array(data FORMAT JSON)",
    "json_format(CAST(json_extract_scalar(data, '$.s') AS JSON))",
    "CAST(json_extract_scalar(data, '$.s') AS JSON)",
    "json_format(CAST(ARRAY['😊', json_extract_scalar(data, '$.s')] AS JSON))",
    "json_format(CAST(r AS JSON))",
    "CAST(r AS JSON)",
    "json_object('l' VALUE json_format(CAST(CAST(json_extract(data, '$.l') AS ARRAY(JSON)) \
     AS JSON)) FORMAT JSON)",
    // Written as a Java string, or copied as spelt: the character.
    "json_format(json_array_get(json_extract(data, '$.l'), 0))",
    "json_array_get(json_extract(data, '$.l'), 0)",
    "json_format(CAST(json_array_get(json_extract(data, '$.l'), 0) AS JSON))",
    "json_format(CAST(CAST(json_extract(data, '$.l') AS ARRAY(JSON)) AS JSON))",
    "json_format(CAST(json_extract(data, '$.l') AS ARRAY(JSON))[2])",
    // Not JSON at all: the character.
    "json_extract_scalar(data, '$.s')",
    "json_extract_scalar(data, '$.e')",
    "json_array_get(json_extract(data, '$.l'), 1)",
    "CAST(json_extract(data, '$.s') AS VARCHAR)",
];

#[tokio::test]
#[ignore = "needs a Trino coordinator; set DDI_TEST_TRINO"]
async fn json_text_spells_an_emoji_as_trino_does() {
    // `r` is a row, as a Delta struct column reaches a model: Trino has no other spelling of a
    // row with named fields that this engine reads.
    let fields = Fields::from(vec![
        Field::new("s", DataType::Utf8, true),
        Field::new("n", DataType::Int32, true),
    ]);
    let row = StructArray::new(
        fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["😊"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1])),
        ],
        None,
    );
    let source = Source {
        from: format!(
            "(VALUES ('{EMOJI_DOC}', CAST(ROW('😊', 1) AS ROW(s VARCHAR, n INTEGER)))) \
             AS source(data, r)"
        ),
        batch: RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("data", DataType::Utf8, true),
                Field::new("r", DataType::Struct(fields), true),
            ])),
            vec![Arc::new(StringArray::from(vec![EMOJI_DOC])), Arc::new(row)],
        )
        .unwrap(),
    };
    let differ = differences(&trino(), &source, EMOJI_JSON, false).await;
    assert!(
        differ.is_empty(),
        "JSON text differs:\n{}",
        differ.join("\n")
    );
}

/// The DECIMAL columns of [`decimals`]: name, precision, scale and value.
const DECIMAL_COLUMNS: &[(&str, u8, i8, &str)] = &[
    // Issue #12's first row as a short decimal, which Trino divides: 0.4998, the double below
    // the nearest; and as a long one, which it rounds correctly.
    ("short17", 17, 17, "0.49979999999999997"),
    ("long17", 38, 17, "0.49979999999999997"),
    // Past 2^24 unscaled. As a long decimal its REAL is the nearest, 1413830.0; Trino divides
    // the short one as floats, 1413830.125, and ddi narrows its double, as the README says.
    ("r", 9, 2, "1413830.04"),
    // Past 2^53 unscaled, so divided it is a double off the nearest.
    ("d", 18, 8, "1175317522.91864620"),
    // Long, and read as text: past 2^53 unscaled, past 10^22 in scale, and halfway between
    // two doubles.
    ("wide", 38, 3, "12345678901234567890123456789012345.678"),
    ("tiny", 38, 38, "0.00000000000000000000000000000012345678"),
    ("half", 38, 1, "9007199254740993.5"),
];

/// `rows` copies of one row of DECIMAL `columns`, each `(name, precision, scale, value)`.
fn decimals(columns: &[(&str, u8, i8, &str)], rows: usize) -> Source {
    let row = columns
        .iter()
        .map(|(_, p, s, v)| format!("CAST('{v}' AS DECIMAL({p}, {s}))"))
        .collect::<Vec<_>>()
        .join(", ");
    let names = columns.iter().map(|c| c.0).collect::<Vec<_>>().join(", ");
    let from = format!(
        "(VALUES {}) AS source({names})",
        vec![format!("({row})"); rows].join(", ")
    );
    let (fields, arrays): (Vec<_>, Vec<_>) = columns
        .iter()
        .map(|&(name, p, s, v)| {
            let (whole, fraction) = v.split_once('.').unwrap_or((v, ""));
            assert_eq!(fraction.len(), s as usize, "{v} is not at scale {s}");
            let unscaled: i128 = format!("{whole}{fraction}").parse().unwrap();
            let array = Decimal128Array::from(vec![unscaled; rows])
                .with_precision_and_scale(p, s)
                .unwrap();
            (
                Field::new(name, DataType::Decimal128(p, s), false),
                Arc::new(array) as ArrayRef,
            )
        })
        .unzip();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    Source { from, batch }
}

/// A DECIMAL converted to DOUBLE or REAL, each way a model reaches the conversion: a
/// `CAST`, a `TRY_CAST`, the cast coercion inserts, a list cast and a lambda. A decimal
/// compared with a DOUBLE, or beside one in a `CASE` or `coalesce`, a literal with a decimal
/// point, and a decimal computed beside an integer literal are missing: DataFusion types each
/// otherwise than Trino. So is a short decimal to REAL where Trino's division of floats
/// rounds twice, which `ddi` narrows from its double instead. The README lists each.
const DECIMAL_TO_FLOAT: &[&str] = &[
    "CAST(short17 AS DOUBLE)",
    "CAST(long17 AS DOUBLE)",
    "TRY_CAST(short17 AS DOUBLE)",
    "TRY_CAST(long17 AS DOUBLE)",
    "CAST(long17 AS REAL)",
    "CAST(r AS DOUBLE)",
    "CAST(CAST(r AS DECIMAL(38, 2)) AS REAL)",
    "CAST(d AS DOUBLE)",
    "CAST(d AS REAL)",
    "CAST(wide AS DOUBLE)",
    "CAST(wide AS REAL)",
    "CAST(-wide AS DOUBLE)",
    "CAST(tiny AS DOUBLE)",
    "CAST(tiny AS REAL)",
    "CAST(half AS DOUBLE)",
    "CAST(half AS REAL)",
    "short17 * 1e0",
    "long17 * 1e0",
    "d * 1e0",
    "short17 + 0e0",
    "CAST(-short17 AS DOUBLE)",
    "CAST(ARRAY[short17] AS ARRAY(DOUBLE))[1]",
    "CAST(ARRAY[long17, short17] AS ARRAY(DOUBLE))[2]",
    "transform(ARRAY[short17], x -> CAST(x AS DOUBLE))[1]",
    // The spellings the README gives for a literal with a decimal point beside a decimal.
    "CAST(short17 AS DOUBLE) = 0.4998e0",
    "CAST(coalesce(short17, CAST(0.0 AS DECIMAL(17, 17))) AS DOUBLE)",
    "CAST(coalesce(d, CAST(0.0 AS DECIMAL(18, 8))) AS DOUBLE)",
];

#[tokio::test]
#[ignore = "needs a Trino coordinator; set DDI_TEST_TRINO"]
async fn a_decimal_converts_to_the_double_or_real_trino_gives() {
    let differ = differences(
        &trino(),
        &decimals(DECIMAL_COLUMNS, 1),
        DECIMAL_TO_FLOAT,
        false,
    )
    .await;
    assert!(differ.is_empty(), "values differ:\n{}", differ.join("\n"));
}

/// A JSON number of 16 or 17 significant digits, each way a model reads it as a DOUBLE or
/// REAL: issue #12's four rows, where a parser that divides is one ULP off.
const JSON_NUMBERS: &[&str] = &[
    "CAST(json_extract_scalar(doc, '$.a') AS DOUBLE)",
    "CAST(json_extract_scalar(doc, '$.b') AS DOUBLE)",
    "CAST(json_extract_scalar(doc, '$.c') AS DOUBLE)",
    "CAST(json_extract_scalar(doc, '$.d') AS DOUBLE)",
    "CAST(json_extract_scalar(doc, '$.a') AS REAL)",
    "CAST(json_extract(doc, '$.b') AS DOUBLE)",
    "CAST(json_array_get(json_extract(doc, '$.l'), 2) AS DOUBLE)",
    "CAST(CAST(json_extract(doc, '$.l') AS ARRAY(JSON))[4] AS DOUBLE)",
    "transform(CAST(json_extract(doc, '$.l') AS ARRAY(JSON)), x -> CAST(x AS DOUBLE))[1]",
    "CAST(CAST(json_extract_scalar(doc, '$.a') AS DECIMAL(38, 17)) AS DOUBLE)",
    "CAST(CAST(json_extract_scalar(doc, '$.a') AS DECIMAL(17, 17)) AS DOUBLE)",
];

#[tokio::test]
#[ignore = "needs a Trino coordinator; set DDI_TEST_TRINO"]
async fn a_json_number_reads_as_the_double_trino_gives() {
    let [a, b, c, d] = common::SEVENTEEN_DIGIT_DOUBLES;
    let doc = format!(r#"{{"a":{a},"b":{b},"c":{c},"d":{d},"l":[{a},{b},{c},{d}]}}"#);
    let source = Source {
        from: format!("(VALUES '{doc}') AS source(doc)"),
        batch: RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("doc", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec![doc]))],
        )
        .unwrap(),
    };
    let differ = differences(&trino(), &source, JSON_NUMBERS, false).await;
    assert!(differ.is_empty(), "values differ:\n{}", differ.join("\n"));
}

/// Columns for `from_unixtime`: `n` a NULL BIGINT, `t` a NULL zoned timestamp, and `x` an
/// epoch in 2376.
fn epochs() -> Source {
    let from = "(VALUES (CAST(NULL AS BIGINT), CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), \
                BIGINT '12828758400')) AS source(n, t, x)";
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("x", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![None])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![None]).with_timezone("UTC")),
            Arc::new(Int64Array::from(vec![12828758400])),
        ],
    )
    .unwrap();
    Source {
        from: from.into(),
        batch,
    }
}

/// `from_unixtime` in each form it is accepted in, as the calendar date it falls on: where
/// the zone decides the answer, before 1970, and around and past 2262, where the nanosecond
/// range ends and Trino's does not. Before 1970 only in a zone the tz database has not
/// merged into another, and after 2099 in a zone with daylight saving only away from local
/// midnight: either can differ, as the README says.
const FROM_UNIXTIME: &[&str] = &[
    "from_unixtime(1711924200, 'Europe/Amsterdam')",
    "from_unixtime(12828758400, 'Europe/Amsterdam')",
    "from_unixtime(12828758400)",
    "from_unixtime(12828758400, -5, -30)",
    "from_unixtime(1711924200, 'Etc/GMT+5')",
    "from_unixtime(1711924200, 'UTC+05:30')",
    "from_unixtime(1711924200) AT TIME ZONE 'Asia/Kolkata'",
    "from_unixtime(x) AT TIME ZONE 'Australia/Sydney'",
    "from_unixtime(1711924200, 'GMT-3')",
    // Rounded to the millisecond as Java rounds, across local midnight.
    "from_unixtime(1711922399.9996, 'Europe/Amsterdam')",
    // Summer and winter in Sydney, each half an hour from local midnight.
    "from_unixtime(1705325400, 'Australia/Sydney')",
    "from_unixtime(1721050200, 'Australia/Sydney')",
    // A fixed offset, either side of local midnight.
    "from_unixtime(1711922400, '+02:00')",
    "from_unixtime(1711922399, '+02:00')",
    // Before 1970: Berlin's double summer time of 1947, and Java's rounding below zero.
    "from_unixtime(-712722600, 'Europe/Berlin')",
    "from_unixtime(-1)",
    "from_unixtime(-1800, 1, 0)",
    "from_unixtime(-1800, 'Australia/Sydney')",
    "from_unixtime(-0.0015, 'UTC')",
    "from_unixtime(-0.0005, 'UTC')",
    // The last second the nanosecond range holds, and the next.
    "from_unixtime(9223372036, 'UTC')",
    "from_unixtime(9223372037, 'UTC')",
    "from_unixtime(9223372036, '+02:00')",
    "from_unixtime(9223372036, 0, 13)",
    "from_unixtime(9223328836, 'Australia/Sydney')",
    "from_unixtime(9223328836, 'Europe/Amsterdam')",
    // Past it, from a column as well as a literal.
    "from_unixtime(x, 'Australia/Sydney')",
    "from_unixtime(x, 'UTC')",
    "from_unixtime(253402300799, 'UTC')",
];

#[tokio::test]
#[ignore = "needs a Trino coordinator; set DDI_TEST_TRINO"]
async fn from_unixtime_falls_on_the_date_trino_gives() {
    let dates: Vec<String> = FROM_UNIXTIME
        .iter()
        .map(|expr| format!("CAST(CAST({expr} AS DATE) AS VARCHAR)"))
        .collect();
    let differ = differences(&trino(), &epochs(), &dates, false).await;
    assert!(differ.is_empty(), "dates differ:\n{}", differ.join("\n"));
}

/// The local wall clock of `from_unixtime`, in an offset that is not whole hours, that
/// changes with the season, or that is three hours, as Berlin's was in the summer of 1947.
const FROM_UNIXTIME_CLOCK: &[&str] = &[
    "EXTRACT(HOUR FROM from_unixtime(1711924200, 'Europe/Amsterdam'))",
    "EXTRACT(HOUR FROM from_unixtime(1705325400, 'Australia/Sydney'))",
    "EXTRACT(HOUR FROM from_unixtime(1721050200, 'Australia/Sydney'))",
    "EXTRACT(MINUTE FROM from_unixtime(1711924200, 5, 45))",
    "EXTRACT(HOUR FROM from_unixtime(-712722600, 'Europe/Berlin'))",
    "EXTRACT(HOUR FROM from_unixtime(x, '+02:00'))",
    "EXTRACT(MINUTE FROM from_unixtime(x, -5, -30))",
];

#[tokio::test]
#[ignore = "needs a Trino coordinator; set DDI_TEST_TRINO"]
async fn from_unixtime_reads_the_wall_clock_trino_gives() {
    let differ = differences(&trino(), &epochs(), FROM_UNIXTIME_CLOCK, false).await;
    assert!(differ.is_empty(), "clocks differ:\n{}", differ.join("\n"));
}

/// A `coalesce`, `CASE` or `nullif` whose branches are bare names that are not JSON, over
/// [`epochs`]: each keeps its own type, and a zoned timestamp past 2262 its date. `if` is
/// missing because DataFusion has no such function.
const BRANCHES: &[&str] = &[
    "coalesce(n, 0)",
    "coalesce(n, x)",
    "CASE WHEN x > 0 THEN x END",
    "CASE WHEN n IS NULL THEN x ELSE n END",
    "nullif(x, 0)",
    "coalesce(n, 0) + 1",
    "CAST(CAST(coalesce(t, from_unixtime(x, 'UTC')) AS DATE) AS VARCHAR)",
    "CAST(CAST(CASE WHEN t IS NULL THEN from_unixtime(x, 'UTC') ELSE t END AS DATE) AS VARCHAR)",
    "EXTRACT(YEAR FROM coalesce(t, from_unixtime(x, 'UTC')))",
];

#[tokio::test]
#[ignore = "needs a Trino coordinator; set DDI_TEST_TRINO"]
async fn a_coalesce_over_names_gives_the_value_trino_gives() {
    let differ = differences(&trino(), &epochs(), BRANCHES, false).await;
    assert!(differ.is_empty(), "values differ:\n{}", differ.join("\n"));
}
