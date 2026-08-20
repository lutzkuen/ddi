//! Resolving table locations from the catalog, against a stand-in Trino server.
//!
//! A table name is not a location, and the pointer can change without anything in storage
//! saying so. These cover the three behaviours that follow from that: read the location
//! from the catalog, notice when it changes, and keep going when the catalog is briefly
//! unreachable.
//!
//! The server here answers Trino's HTTP protocol rather than mocking the client, so the
//! pagination the real protocol requires is exercised: a statement returns a `nextUri`
//! that has to be followed before results appear.

use std::sync::{Arc, Mutex};

use delta_delta_ingest::config::ResolvedPipeline;
use delta_delta_ingest::locate::{moved, Locator};
use delta_delta_ingest::trino::{location_from_ddl, TrinoClient, TrinoConnection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A minimal Trino, serving one statement at a time.
struct FakeTrino {
    port: u16,
    /// What `SHOW CREATE TABLE` should report. Changing it simulates a relocation.
    location: Arc<Mutex<String>>,
    /// Set to stop answering, simulating an unreachable cluster.
    down: Arc<Mutex<bool>>,
}

async fn spawn_fake_trino(initial: &str) -> FakeTrino {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let location = Arc::new(Mutex::new(initial.to_string()));
    let down = Arc::new(Mutex::new(false));

    let loc = location.clone();
    let is_down = down.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                continue;
            };
            if *is_down.lock().unwrap() {
                // Accept then hang up, which is what a cluster refusing work looks like.
                let _ = sock.shutdown().await;
                continue;
            }

            let mut buf = vec![0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]).to_string();

            // The catalogs query, answered on the first response so the test stays short.
            let body = if head.contains("system.metadata.catalogs") {
                serde_json::json!({
                    "columns": [{"name": "catalog_name"}, {"name": "connector_name"}],
                    "data": [
                        ["delta", "delta_lake"],
                        ["lake",  "delta_lake"],
                        ["ice",   "iceberg"],
                        ["hive",  "hive"],
                    ],
                })
                .to_string()
            } else if head.starts_with("GET /v1/next") {
                let ddl = format!(
                    "CREATE TABLE hive.silver.orders_stg (order_id bigint)\nWITH (\n   \
                     format = 'PARQUET',\n   location = '{}'\n)",
                    loc.lock().unwrap()
                );
                serde_json::json!({
                    "columns": [{"name": "Create Table"}],
                    "data": [[ddl]],
                })
                .to_string()
            } else {
                serde_json::json!({
                    "nextUri": format!("http://127.0.0.1:{port}/v1/next"),
                })
                .to_string()
            };

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(response.as_bytes()).await;
            let _ = sock.shutdown().await;
        }
    });

    FakeTrino {
        port,
        location,
        down,
    }
}

fn client(port: u16) -> TrinoClient {
    TrinoClient::new(TrinoConnection {
        host: "127.0.0.1".into(),
        port,
        user: "ddi".into(),
        password: None,
        http_scheme: "http".into(),
        catalog: Some("hive".into()),
        schema: Some("silver".into()),
        verify_tls: true,
    })
    .unwrap()
}

fn pipeline(declared_source: &str, declared_target: &str) -> ResolvedPipeline {
    ResolvedPipeline {
        name: "orders_stg".into(),
        app_id: "ddi.orders_stg".into(),
        source_uri: declared_source.into(),
        target_uri: declared_target.into(),
        lookups: vec![],
        source_relation: Some("hive.bronze.orders_raw".into()),
        target_relation: Some("hive.silver.orders_stg".into()),
        publish: None,
        publish_to: None,
        starting_version: 0,
        change_policy: Default::default(),
        transform_sql: None,
        allowed_latency_secs: 1,
        max_bytes_per_batch: 1,
        max_files_per_batch: 1,
        max_output_rows_per_batch: 1,
        target_file_size: 1,
        watermark_uri: None,
        dedup_timestamp: None,
        dedup_key: None,
        write_mode: Default::default(),
        upsert_key: None,
        upsert_lookback: None,
        upsert_tiebreak: Vec::new(),
        stage_for: None,
        dq_uri: None,
        storage: Default::default(),
    }
}

#[tokio::test]
async fn a_location_is_read_from_the_catalog_following_the_next_uri() {
    let trino = spawn_fake_trino("abfss://lake@acct.dfs.core.windows.net/silver/orders_stg").await;
    let found = client(trino.port)
        .table_location("hive.silver.orders_stg")
        .await
        .expect("should resolve");
    assert_eq!(
        found,
        "abfss://lake@acct.dfs.core.windows.net/silver/orders_stg"
    );
}

#[tokio::test]
async fn the_catalog_overrides_what_dbt_declared() {
    // The gap this exists to close: a name is not a location, and the manifest's guess
    // can be stale or simply wrong.
    let trino = spawn_fake_trino("abfss://lake@acct.dfs.core.windows.net/actually/here").await;
    let locator = Locator::with_client(client(trino.port));

    let declared = pipeline("s3://guessed/bronze", "s3://guessed/silver");
    let resolved = locator.refresh(&declared).await;

    assert_eq!(
        resolved.target_uri,
        "abfss://lake@acct.dfs.core.windows.net/actually/here"
    );
    assert!(moved(&declared, &resolved));
}

#[tokio::test]
async fn a_table_that_moves_is_followed() {
    let trino = spawn_fake_trino("abfss://lake@acct.dfs.core.windows.net/silver/v1").await;
    let locator = Locator::with_client(client(trino.port));

    let declared = pipeline("s3://x/bronze", "s3://x/silver");
    let first = locator.refresh(&declared).await;
    assert!(first.target_uri.ends_with("/silver/v1"));

    // Someone rebuilds the model somewhere else. Nothing in storage says so.
    *trino.location.lock().unwrap() = "abfss://lake@acct.dfs.core.windows.net/silver/v2".into();

    let second = locator.refresh(&first).await;
    assert!(
        second.target_uri.ends_with("/silver/v2"),
        "{}",
        second.target_uri
    );
    assert!(
        moved(&first, &second),
        "the move must be visible to the caller, so it can reopen"
    );
}

#[tokio::test]
async fn an_unreachable_catalog_falls_back_to_the_last_location_it_gave() {
    let trino = spawn_fake_trino("abfss://lake@acct.dfs.core.windows.net/silver/known").await;
    let locator = Locator::with_client(client(trino.port));

    let declared = pipeline("s3://x/bronze", "s3://x/silver");
    let good = locator.refresh(&declared).await;
    assert!(good.target_uri.ends_with("/silver/known"));

    // The cluster goes away. Streaming should carry on from what it last knew, rather
    // than stopping or silently reverting to the manifest's guess.
    *trino.down.lock().unwrap() = true;

    let during_outage = locator.refresh(&good).await;
    assert_eq!(
        during_outage.target_uri, good.target_uri,
        "an outage must not change where we think the table is"
    );
    assert!(!moved(&good, &during_outage));
}

#[tokio::test]
async fn only_catalogs_on_a_delta_connector_are_reported_as_delta() {
    // The question a mixed warehouse forces: most tables here are Iceberg, and a
    // row-wise model over one of them is not a Delta-to-Delta pipeline however
    // streamable its SQL looks. The catalog's *name* is no guide; the connector is.
    let trino = spawn_fake_trino("s3://unused").await;
    let delta = client(trino.port).delta_catalogs().await.unwrap();

    assert!(delta.contains("delta"));
    assert!(
        delta.contains("lake"),
        "a Delta catalog need not be called delta"
    );
    assert!(!delta.contains("ice"), "iceberg is not delta");
    assert!(!delta.contains("hive"));
}

#[tokio::test]
async fn with_no_catalog_nothing_is_resolved() {
    let locator = Locator::none();
    let declared = pipeline("s3://x/bronze", "s3://x/silver");
    let after = locator.refresh(&declared).await;
    assert_eq!(after.source_uri, "s3://x/bronze");
    assert!(!moved(&declared, &after));
}

#[test]
fn a_managed_table_with_no_location_is_reported_rather_than_guessed() {
    assert_eq!(location_from_ddl("CREATE TABLE t (a bigint)"), None);
}
