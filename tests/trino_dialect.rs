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
//! Ignored by default because it needs a Trino listening. CI starts one and runs this with
//! `--ignored`. To run it locally:
//!
//! ```bash
//! docker run -d --name trino -p 8080:8080 trinodb/trino:480
//! DDI_TEST_TRINO=http://127.0.0.1:8080 cargo test --test trino_dialect -- --ignored
//! ```

use delta_delta_ingest::dbt::analyze::{analyze, Verdict};
use delta_delta_ingest::dbt::Manifest;
use delta_delta_ingest::trino::{TrinoClient, TrinoConnection};

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
