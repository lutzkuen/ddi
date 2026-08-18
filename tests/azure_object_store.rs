//! Delta-to-Delta streaming against real object storage.
//!
//! Two bugs shipped in v0.1.0 that a local filesystem cannot expose, because the local
//! store is more permissive than a remote one:
//!
//! 1. Storage credentials were dropped when the stream loaded a schema, because that path
//!    rebuilt a table from its URL rather than reusing the configured log store. On a
//!    local path there are no credentials to drop.
//! 2. Parquet footers were read with a suffix range request ("give me the last N bytes"),
//!    which Azure Blob Storage does not implement. Local files support it.
//!
//! This exercises both. Any successful batch proves the first, since every batch loads a
//! schema; reading the data proves the second.
//!
//! Ignored by default because it needs an object store listening. CI starts Azurite --
//! the official Azure emulator, which speaks the real Blob protocol -- and runs it with
//! `--ignored`. To run it locally:
//!
//! ```bash
//! npm install -g azurite
//! azurite-blob --skipApiVersionCheck --location /tmp/azurite --blobPort 10000 &
//! python -c "
//! from azure.storage.blob import BlobServiceClient
//! BlobServiceClient.from_connection_string(__import__('os').environ['AZURE_STORAGE_CONNECTION_STRING']).create_container('lake')"
//! cargo test --test azure_object_store -- --ignored
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use delta_delta_ingest::config::ResolvedPipeline;
use delta_delta_ingest::pipeline::Pipeline;
use delta_delta_ingest::source::ChangePolicy;
use delta_delta_ingest::storage::Storage;
use deltalake::arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, DeltaTableBuilder};
use futures::TryStreamExt;

/// Azurite's well-known development account. Overridable so the same test can be pointed
/// at a real storage account.
fn storage_options() -> HashMap<String, String> {
    let mut o = HashMap::new();
    o.insert(
        "azure_storage_account_name".into(),
        std::env::var("DDI_TEST_AZURE_ACCOUNT").unwrap_or_else(|_| "devstoreaccount1".into()),
    );
    o.insert(
        "azure_storage_account_key".into(),
        std::env::var("DDI_TEST_AZURE_KEY").unwrap_or_else(|_| {
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into()
        }),
    );
    if std::env::var("DDI_TEST_AZURE_ACCOUNT").is_err() {
        // Emulator: allow plain HTTP and the fixed local endpoint.
        o.insert("azure_storage_use_emulator".into(), "true".into());
        o.insert("azure_allow_http".into(), "true".into());
    }
    o
}

fn container() -> String {
    std::env::var("DDI_TEST_AZURE_CONTAINER").unwrap_or_else(|_| "lake".into())
}

/// A distinct prefix per run, so repeated runs and parallel jobs never collide.
fn run_prefix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("az://{}/ddi-it-{nanos}", container())
}

fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn stg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("status", DataType::Utf8, true),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn raw_batch(ids: std::ops::RangeInclusive<i64>) -> RecordBatch {
    let ids: Vec<i64> = ids.collect();
    RecordBatch::try_new(
        raw_schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| format!(r#"{{"customer_id":{},"status":"paid"}}"#, i * 7))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                ids.iter().map(|i| i * 60_000_000).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn create(uri: &str, schema: SchemaRef) {
    let delta: StructType = schema.as_ref().try_into_kernel().unwrap();
    let url = ensure_table_uri(uri).unwrap();
    DeltaTableBuilder::from_url(url)
        .expect("table uri")
        .with_storage_options(storage_options())
        .build()
        .expect("open for create — is the container present and Azurite running?")
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .expect("create table");
}

async fn append(uri: &str, ids: std::ops::RangeInclusive<i64>) {
    let url = ensure_table_uri(uri).unwrap();
    DeltaTableBuilder::from_url(url)
        .expect("table uri")
        .with_storage_options(storage_options())
        .load()
        .await
        .expect("open for append")
        .write(vec![raw_batch(ids)])
        .with_save_mode(SaveMode::Append)
        .await
        .expect("append");
}

async fn read_order_ids(uri: &str) -> Vec<i64> {
    let url = ensure_table_uri(uri).unwrap();
    let table = deltalake::open_table_with_storage_options(url, storage_options())
        .await
        .unwrap();
    let (_t, stream) = table.scan_table().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut out = Vec::new();
    for b in batches {
        if b.num_rows() == 0 {
            continue;
        }
        let idx = b.schema().index_of("order_id").unwrap();
        let a = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        out.extend((0..a.len()).map(|i| a.value(i)));
    }
    out.sort_unstable();
    out
}

fn pipeline(source: &str, target: &str) -> ResolvedPipeline {
    ResolvedPipeline {
        name: "orders_stg".into(),
        app_id: format!("ddi.it.{}", target.rsplit('/').next().unwrap()),
        source_uri: source.into(),
        target_uri: target.into(),
        lookups: vec![],
        starting_version: 0,
        change_policy: ChangePolicy::Fail,
        // Parses JSON and casts, so the batch really has to read the parquet -- which is
        // what needs the footer, which is what Azure would not serve by suffix range.
        transform_sql: Some(
            "SELECT order_id, \
                    CAST(json_extract_scalar(data, '$.customer_id') AS BIGINT) AS customer_id, \
                    json_extract_scalar(data, '$.status') AS status, \
                    _timestamp \
             FROM source"
                .into(),
        ),
        allowed_latency_secs: 1,
        max_bytes_per_batch: 256 * 1024 * 1024,
        max_files_per_batch: 1_000,
        max_output_rows_per_batch: 5_000_000,
        target_file_size: 128 * 1024 * 1024,
        watermark_uri: None,
        dedup_timestamp: Some("_timestamp".into()),
        dedup_key: Some("order_id".into()),
        write_mode: Default::default(),
        upsert_key: None,
        upsert_lookback: None,
        upsert_tiebreak: Vec::new(),
        dq_uri: None,
        storage: Storage::new(storage_options()),
        source_relation: None,
        target_relation: None,
    }
}

#[tokio::test]
#[ignore = "needs an object store listening; CI starts Azurite and runs with --ignored"]
async fn streams_delta_to_delta_on_azure_blob_storage() {
    let prefix = run_prefix();
    let source = format!("{prefix}/orders_raw");
    let target = format!("{prefix}/orders_stg");

    create(&source, raw_schema()).await;
    create(&target, stg_schema()).await;
    append(&source, 1..=5).await;

    // Reads parquet from blob storage and loads a schema per batch: the two paths that
    // were broken.
    let n = Pipeline::open(pipeline(&source, &target))
        .await
        .expect("open pipeline against azure")
        .run_until_caught_up()
        .await
        .expect("stream from azure");
    assert!(n > 0, "should have committed at least one batch");
    assert_eq!(read_order_ids(&target).await, vec![1, 2, 3, 4, 5]);

    // Resume is a no-op.
    let n = Pipeline::open(pipeline(&source, &target))
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(n, 0, "a caught-up restart must commit nothing");
    assert_eq!(read_order_ids(&target).await, vec![1, 2, 3, 4, 5]);

    // And it keeps up incrementally.
    append(&source, 6..=9).await;
    Pipeline::open(pipeline(&source, &target))
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    let got = read_order_ids(&target).await;
    assert_eq!(got, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(
        got.len(),
        got.iter().collect::<std::collections::HashSet<_>>().len(),
        "no duplicates"
    );
}

#[tokio::test]
#[ignore = "needs an object store listening; CI starts Azurite and runs with --ignored"]
async fn validate_resolves_azure_credentials_without_touching_storage() {
    // `ddi validate` runs this for every pipeline before a daemon starts.
    let s = Storage::new(storage_options());
    s.check(&format!("az://{}/anything", container()))
        .expect("azure backend and credentials should resolve");
}
