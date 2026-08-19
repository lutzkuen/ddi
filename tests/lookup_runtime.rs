//! A pinned Delta lookup is a real second provider, not merely validator syntax.
//!
//! This deliberately uses separate local Delta tables and controlled log-object mtimes so the
//! source commit selects the FX snapshot deterministically. A successful run proves the
//! runtime registers the version-pinned lookup in DataFusion and writes its numeric result.

mod common;

use std::fs::{File, FileTimes};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common::pipeline_cfg;
use delta_delta_ingest::lookup::{resolve, LookupConfig, LookupTableIdChangePolicy};
use delta_delta_ingest::pipeline::Pipeline;
use deltalake::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};
use futures::TryStreamExt;

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("currency", DataType::Utf8, false),
    ]))
}

fn lookup_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("currency", DataType::Utf8, false),
        Field::new("exchange_rate", DataType::Float64, false),
    ]))
}

fn target_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("exchange_rate", DataType::Float64, true),
    ]))
}

async fn create_table(path: &str, schema: SchemaRef) {
    let columns: StructType = schema.as_ref().try_into_kernel().unwrap();
    DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(columns.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();
}

async fn append(path: &str, batch: RecordBatch) {
    open_table(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
}

fn set_log_mtime(path: &str, version: u64, seconds: u64) {
    let log = Path::new(path)
        .join("_delta_log")
        .join(format!("{version:020}.json"));
    File::open(log)
        .unwrap()
        .set_times(
            FileTimes::new().set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)),
        )
        .unwrap();
}

async fn target_rows(path: &str) -> Vec<(i64, Option<f64>)> {
    let table = open_table(ensure_table_uri(path).unwrap()).await.unwrap();
    let (_table, stream) = table.scan_table().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(batch.schema().index_of("order_id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rates = batch
            .column(batch.schema().index_of("exchange_rate").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                (!rates.is_null(row)).then(|| rates.value(row)),
            ));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

fn fx_pipeline(
    source: &str,
    lookup: &str,
    target: &str,
    table_id_change_policy: LookupTableIdChangePolicy,
) -> delta_delta_ingest::config::ResolvedPipeline {
    let mut cfg = pipeline_cfg("fx-lookup", source, target);
    cfg.lookups = vec![resolve(&LookupConfig {
        name: "fx_rates".into(),
        uri: lookup.into(),
        relation: None,
        // The table-replacement policy is deliberately orthogonal to the explicit baseline
        // for source history that predates every retained lookup log entry.
        pre_history_version: Some(0),
        table_id_change_policy,
    })
    .unwrap()];
    cfg.transform_sql = Some(
        "SELECT o.order_id, fx.exchange_rate \
         FROM source AS o LEFT JOIN fx_rates AS fx \
         ON fx.currency = o.currency"
            .into(),
    );
    cfg
}

async fn write_lookup(path: &str, rate: f64) {
    append(
        path,
        RecordBatch::try_new(
            lookup_schema(),
            vec![
                Arc::new(StringArray::from(vec!["USD"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![rate])) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .await;
}

async fn write_source(path: &str, order_id: i64) {
    append(
        path,
        RecordBatch::try_new(
            source_schema(),
            vec![
                Arc::new(Int64Array::from(vec![order_id])) as ArrayRef,
                Arc::new(StringArray::from(vec!["USD"])) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .await;
}

async fn recreate_lookup(path: &str, rate: f64) {
    std::fs::remove_dir_all(path).expect("old lookup table can be replaced");
    create_table(path, lookup_schema()).await;
    write_lookup(path, rate).await;
}

async fn latest_target_commit(path: &str) -> String {
    let table = open_table(ensure_table_uri(path).unwrap()).await.unwrap();
    let version = table.version().expect("target has a commit");
    std::fs::read_to_string(
        Path::new(path)
            .join("_delta_log")
            .join(format!("{version:020}.json")),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_fx_lookup_left_join_enriches_the_source_batch() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let lookup = root.join("fx_rates").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_table(&source, source_schema()).await;
    create_table(&lookup, lookup_schema()).await;
    create_table(&target, target_schema()).await;

    append(
        &lookup,
        RecordBatch::try_new(
            lookup_schema(),
            vec![
                Arc::new(StringArray::from(vec!["USD"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.2345])) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .await;

    append(
        &source,
        RecordBatch::try_new(
            source_schema(),
            vec![
                Arc::new(Int64Array::from(vec![42])) as ArrayRef,
                Arc::new(StringArray::from(vec!["USD"])) as ArrayRef,
            ],
        )
        .unwrap(),
    )
    .await;

    // Delta time travel uses the JSON object's last-modified timestamp, not CommitInfo. Give
    // the lookup v1 and source v1 distinct, fixed milliseconds so this is immune to test-host
    // filesystem granularity and demonstrates the intended source-commit selection rule.
    set_log_mtime(&lookup, 0, 1_700_000_000);
    set_log_mtime(&lookup, 1, 1_700_000_001);
    set_log_mtime(&source, 0, 1_700_000_002);
    set_log_mtime(&source, 1, 1_700_000_003);

    let mut cfg = pipeline_cfg("fx-lookup", &source, &target);
    cfg.lookups = vec![resolve(&LookupConfig {
        name: "fx_rates".into(),
        uri: lookup,
        relation: None,
        pre_history_version: None,
        table_id_change_policy: LookupTableIdChangePolicy::Strict,
    })
    .unwrap()];
    cfg.transform_sql = Some(
        "SELECT o.order_id, fx.exchange_rate \
         FROM source AS o LEFT JOIN fx_rates AS fx \
         ON fx.currency = o.currency"
            .into(),
    );

    let mut pipeline = Pipeline::open(cfg).await.expect("lookup pipeline opens");
    pipeline
        .run_until_caught_up()
        .await
        .expect("lookup join executes");

    assert_eq!(target_rows(&target).await, vec![(42, Some(1.2345))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn strict_policy_rejects_a_lookup_replaced_while_the_pipeline_is_running() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let lookup = root.join("fx_rates").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_table(&source, source_schema()).await;
    create_table(&lookup, lookup_schema()).await;
    create_table(&target, target_schema()).await;

    let mut pipeline = Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::Strict,
    ))
    .await
    .expect("pipeline opens against the original lookup");

    recreate_lookup(&lookup, 2.0).await;
    write_source(&source, 42).await;
    set_log_mtime(&lookup, 0, 1_700_000_100);
    set_log_mtime(&lookup, 1, 1_700_000_101);
    set_log_mtime(&source, 0, 1_700_000_102);
    set_log_mtime(&source, 1, 1_700_000_103);

    let error = pipeline
        .run_until_caught_up()
        .await
        .expect_err("strict policy must not silently cross the replacement");
    assert!(
        error.to_string().contains("changed Delta table id"),
        "got: {error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn use_current_falls_back_to_lookup_head_and_records_the_override() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let lookup = root.join("fx_rates").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_table(&source, source_schema()).await;
    create_table(&lookup, lookup_schema()).await;
    create_table(&target, target_schema()).await;

    let mut pipeline = Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::UseCurrent,
    ))
    .await
    .expect("pipeline opens against the original lookup");

    recreate_lookup(&lookup, 2.0).await;
    write_source(&source, 42).await;
    set_log_mtime(&lookup, 0, 1_700_000_100);
    set_log_mtime(&lookup, 1, 1_700_000_101);
    set_log_mtime(&source, 0, 1_700_000_102);
    set_log_mtime(&source, 1, 1_700_000_103);

    pipeline
        .run_until_caught_up()
        .await
        .expect("opt-in policy uses the current replacement rather than stopping");

    assert_eq!(target_rows(&target).await, vec![(42, Some(2.0))]);
    let commit = latest_target_commit(&target).await;
    assert!(
        commit.contains(r#""ddi.lookup.fx_rates.current":true"#),
        "the target commit must record the non-pinned lookup choice: {commit}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_after_a_lookup_replacement_requires_or_honours_the_opt_in_policy() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let lookup = root.join("fx_rates").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_table(&source, source_schema()).await;
    create_table(&lookup, lookup_schema()).await;
    create_table(&target, target_schema()).await;
    write_lookup(&lookup, 1.0).await;
    write_source(&source, 41).await;
    set_log_mtime(&lookup, 0, 1_700_000_200);
    set_log_mtime(&lookup, 1, 1_700_000_201);
    set_log_mtime(&source, 0, 1_700_000_202);
    set_log_mtime(&source, 1, 1_700_000_203);

    {
        let mut pipeline = Pipeline::open(fx_pipeline(
            &source,
            &lookup,
            &target,
            LookupTableIdChangePolicy::Strict,
        ))
        .await
        .expect("original strict pipeline opens");
        pipeline
            .run_until_caught_up()
            .await
            .expect("original strict pipeline commits its lookup identity");
    }

    recreate_lookup(&lookup, 2.0).await;

    let strict = Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::Strict,
    ))
    .await;
    let error = match strict {
        Ok(_) => panic!("strict policy must reject the recorded/current id mismatch"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("changed Delta table id"),
        "got: {error}"
    );

    Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::UseCurrent,
    ))
    .await
    .expect("opt-in policy accepts the recorded/current id mismatch");
}
