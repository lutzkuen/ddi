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
use delta_delta_ingest::lookup::{resolve, table_id, LookupConfig, LookupTableIdChangePolicy};
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
    // The table-replacement policy is deliberately orthogonal to the explicit baseline
    // for source history that predates every retained lookup log entry.
    fx_pipeline_with(source, lookup, target, table_id_change_policy, Some(0))
}

/// The same pipeline without an approved pre-history baseline.
///
/// Most tests configure one, and that is exactly what hides the ordinary retention shape: a
/// baseline turns "nothing retained predates this commit" into a different code path before
/// the policy is ever consulted.
fn fx_pipeline_without_baseline(
    source: &str,
    lookup: &str,
    target: &str,
    table_id_change_policy: LookupTableIdChangePolicy,
) -> delta_delta_ingest::config::ResolvedPipeline {
    fx_pipeline_with(source, lookup, target, table_id_change_policy, None)
}

fn fx_pipeline_with(
    source: &str,
    lookup: &str,
    target: &str,
    table_id_change_policy: LookupTableIdChangePolicy,
    pre_history_version: Option<u64>,
) -> delta_delta_ingest::config::ResolvedPipeline {
    let mut cfg = pipeline_cfg("fx-lookup", source, target);
    cfg.lookups = vec![resolve(&LookupConfig {
        name: "fx_rates".into(),
        uri: lookup.into(),
        relation: None,
        pre_history_version,
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

async fn overwrite_lookup(path: &str, rate: f64) {
    open_table(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .write(vec![RecordBatch::try_new(
            lookup_schema(),
            vec![
                Arc::new(StringArray::from(vec!["USD"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![rate])) as ArrayRef,
            ],
        )
        .unwrap()])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();
}

/// Delta's CREATE OR REPLACE keeps the old log history at the URI but writes a new metadata
/// action (and therefore table id). It is the real production shape in which time travel can
/// select an old-id snapshot while the URI's head has the replacement id.
async fn create_or_replace_lookup(path: &str, rate: f64) {
    let columns: StructType = lookup_schema().as_ref().try_into_kernel().unwrap();
    DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(columns.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();
    write_lookup(path, rate).await;
}

async fn lookup_id(path: &str) -> String {
    table_id(&open_table(ensure_table_uri(path).unwrap()).await.unwrap())
        .expect("lookup table has an id")
}

/// Model log-retention VACUUM: a current checkpoint keeps the head readable after the historical
/// JSON entries are gone, while attempting to open the approved historical baseline must fail.
async fn checkpoint_then_remove_lookup_history(path: &str, before_version: u64) {
    let table = open_table(ensure_table_uri(path).unwrap()).await.unwrap();
    deltalake::protocol::checkpoints::create_checkpoint(&table, None)
        .await
        .unwrap();
    for version in 0..before_version {
        std::fs::remove_file(
            Path::new(path)
                .join("_delta_log")
                .join(format!("{version:020}.json")),
        )
        .unwrap();
    }
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
async fn use_current_keeps_timestamp_pinning_when_the_lookup_id_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let lookup = root.join("fx_rates").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_table(&source, source_schema()).await;
    create_table(&lookup, lookup_schema()).await;
    create_table(&target, target_schema()).await;
    write_lookup(&lookup, 1.0).await;
    let before_update = lookup_id(&lookup).await;
    write_source(&source, 42).await;
    // This is an ordinary newer Delta version, not a new table lineage. The head has rate 2.0,
    // but the source commit predates it and must continue to see the historical rate 1.0.
    overwrite_lookup(&lookup, 2.0).await;
    assert_eq!(lookup_id(&lookup).await, before_update);

    set_log_mtime(&lookup, 0, 1_700_000_000);
    set_log_mtime(&lookup, 1, 1_700_000_001);
    set_log_mtime(&source, 0, 1_700_000_002);
    set_log_mtime(&source, 1, 1_700_000_003);
    set_log_mtime(&lookup, 2, 1_700_000_004);

    let mut pipeline = Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::UseCurrent,
    ))
    .await
    .expect("pipeline opens against the newer same-id lookup head");
    pipeline
        .run_until_caught_up()
        .await
        .expect("same-id lookup update remains timestamp-pinned");

    assert_eq!(target_rows(&target).await, vec![(42, Some(1.0))]);
    assert!(
        !latest_target_commit(&target)
            .await
            .contains(r#""ddi.lookup.fx_rates.current":true"#),
        "an ordinary same-id update must not be marked as a current-head fallback"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn use_current_uses_the_head_when_vacuumed_lookup_history_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let lookup = root.join("fx_rates").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();
    let strict_target = root.join("strict_target").to_str().unwrap().to_string();

    create_table(&source, source_schema()).await;
    create_table(&lookup, lookup_schema()).await;
    create_table(&target, target_schema()).await;
    create_table(&strict_target, target_schema()).await;
    write_lookup(&lookup, 1.0).await;
    write_source(&source, 42).await;
    overwrite_lookup(&lookup, 2.0).await;
    // A checkpoint makes v2/current readable; VACUUM-like retention removes v0-v1, including
    // the explicitly configured pre-history baseline for the older source commit.
    checkpoint_then_remove_lookup_history(&lookup, 2).await;
    set_log_mtime(&source, 0, 1_700_000_002);
    set_log_mtime(&source, 1, 1_700_000_003);
    set_log_mtime(&lookup, 2, 1_700_000_004);

    let mut strict = Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &strict_target,
        LookupTableIdChangePolicy::Strict,
    ))
    .await
    .expect("strict pipeline can still inspect the current lookup head");
    let error = strict
        .run_until_caught_up()
        .await
        .expect_err("strict policy must retain its historical-snapshot requirement");
    assert!(
        error
            .to_string()
            .contains("cannot load a retained timestamp-pinned snapshot"),
        "got: {error}"
    );

    let mut pipeline = Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::UseCurrent,
    ))
    .await
    .expect("current lookup head remains readable after history retention");
    pipeline
        .run_until_caught_up()
        .await
        .expect("explicit availability policy falls back rather than pausing");

    assert_eq!(target_rows(&target).await, vec![(42, Some(2.0))]);
    assert!(
        latest_target_commit(&target)
            .await
            .contains(r#""ddi.lookup.fx_rates.current":true"#),
        "vacuumed-history fallback must be visible in target provenance"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn use_current_handles_create_or_replace_historic_selection_at_the_same_uri() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let lookup = root.join("fx_rates").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_table(&source, source_schema()).await;
    create_table(&lookup, lookup_schema()).await;
    create_table(&target, target_schema()).await;
    write_lookup(&lookup, 1.0).await;
    let old_id = lookup_id(&lookup).await;
    write_source(&source, 42).await;
    create_or_replace_lookup(&lookup, 2.0).await;
    let new_id = lookup_id(&lookup).await;
    assert_ne!(
        old_id, new_id,
        "CREATE OR REPLACE must create a new Delta id"
    );

    // Source v1 is after old lookup v1 but before CREATE OR REPLACE v2. The retained history
    // therefore selects the old id, while the URI's current head is the replacement id.
    set_log_mtime(&lookup, 0, 1_700_000_000);
    set_log_mtime(&lookup, 1, 1_700_000_001);
    set_log_mtime(&source, 0, 1_700_000_002);
    set_log_mtime(&source, 1, 1_700_000_003);
    set_log_mtime(&lookup, 2, 1_700_000_004);
    set_log_mtime(&lookup, 3, 1_700_000_005);

    let resolved = resolve(&LookupConfig {
        name: "fx_rates".into(),
        uri: lookup.clone(),
        relation: None,
        pre_history_version: Some(0),
        table_id_change_policy: LookupTableIdChangePolicy::UseCurrent,
    })
    .unwrap();
    let snapshots = resolved
        .snapshots(
            &delta_delta_ingest::storage::Storage::default(),
            chrono::DateTime::from_timestamp(1_700_000_003, 0).unwrap(),
        )
        .await
        .expect("historical lookup snapshot resolves");
    assert_eq!(
        snapshots.selected.table_id.as_deref(),
        Some(old_id.as_str())
    );
    assert_eq!(snapshots.head.table_id.as_deref(), Some(new_id.as_str()));

    let mut pipeline = Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::UseCurrent,
    ))
    .await
    .expect("pipeline opens against replacement head");
    pipeline
        .run_until_caught_up()
        .await
        .expect("opt-in uses coherent current head for the replacement");

    assert_eq!(target_rows(&target).await, vec![(42, Some(2.0))]);
    assert!(
        latest_target_commit(&target)
            .await
            .contains(r#""ddi.lookup.fx_rates.current":true"#),
        "the replacement-head decision must be recorded in target provenance"
    );
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

/// After a restart across a replacement, a batch that lies wholly inside the new lineage is
/// still pinned.
///
/// The startup marker says the *target's* last commit named an older lineage. That is a fact
/// about the target, not about this batch, and the two come apart the moment the pipeline
/// restarts: the very next source commit may sit entirely after the replacement, with its
/// timestamp-selected snapshot and the head agreeing on identity. Substituting the head there
/// enriches from a lookup commit written *after* the source commit — the one thing pinning
/// exists to prevent — and `use_current` promises not to do it while the ids agree.
#[tokio::test(flavor = "multi_thread")]
async fn use_current_still_pins_a_batch_that_lies_wholly_after_the_replacement() {
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
            LookupTableIdChangePolicy::UseCurrent,
        ))
        .await
        .expect("the original pipeline opens");
        pipeline
            .run_until_caught_up()
            .await
            .expect("the original pipeline records the old lookup identity");
    }

    // The replacement, then two further lookup commits in the new lineage, then a source
    // commit that falls between them.
    create_or_replace_lookup(&lookup, 2.0).await;
    set_log_mtime(&lookup, 2, 1_700_000_204);
    set_log_mtime(&lookup, 3, 1_700_000_205);
    write_source(&source, 42).await;
    set_log_mtime(&source, 2, 1_700_000_206);
    overwrite_lookup(&lookup, 3.0).await;
    set_log_mtime(&lookup, 4, 1_700_000_207);

    let mut pipeline = Pipeline::open(fx_pipeline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::UseCurrent,
    ))
    .await
    .expect("the restarted pipeline opens");
    pipeline
        .run_until_caught_up()
        .await
        .expect("the restarted pipeline commits");

    let rows = target_rows(&target).await;
    assert_eq!(
        rows,
        vec![(41, Some(1.0)), (42, Some(2.0))],
        "order 42 must be enriched from lookup version 3, the newest commit strictly before \
         its source commit — 3.0 is version 4, written afterwards"
    );

    let commit = latest_target_commit(&target).await;
    assert!(
        !commit.contains("ddi.lookup.fx_rates.current"),
        "a pinned batch must not be marked as having used the current head: {commit}"
    );
}

/// Ordinary log retention, with no approved baseline to fall back to.
///
/// This is the shape the opt-in exists for and the one every other test misses: Delta time
/// travel does not fail on a truncated log, it clamps to the oldest version it can still see.
/// So "the lookup has no snapshot before this commit" arrives as an ordinary selection, and
/// only the version it landed on says whether that is retention or a lookup younger than the
/// source.
#[tokio::test(flavor = "multi_thread")]
async fn use_current_survives_truncated_lookup_history_without_a_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let lookup = root.join("fx_rates").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_table(&source, source_schema()).await;
    create_table(&lookup, lookup_schema()).await;
    create_table(&target, target_schema()).await;
    write_lookup(&lookup, 1.0).await;
    // Overwrite, so the surviving head holds one row per currency and the assertion below is
    // about which snapshot was used rather than about join fan-out.
    overwrite_lookup(&lookup, 2.0).await;
    write_source(&source, 42).await;
    set_log_mtime(&lookup, 0, 1_700_000_200);
    set_log_mtime(&lookup, 1, 1_700_000_201);
    // The only lookup commit still retained is later than the source commit.
    set_log_mtime(&lookup, 2, 1_700_000_300);
    set_log_mtime(&source, 0, 1_700_000_249);
    set_log_mtime(&source, 1, 1_700_000_250);

    checkpoint_then_remove_lookup_history(&lookup, 2).await;

    let strict = Pipeline::open(fx_pipeline_without_baseline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::Strict,
    ))
    .await
    .expect("strict opens; the head is still readable")
    .run_until_caught_up()
    .await;
    let error = match strict {
        Ok(_) => panic!("strict must not silently enrich from a snapshot it could not pin to"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(
        message.contains("use_current"),
        "strict must name the opt-in that trades determinism for availability: {message}"
    );

    let mut pipeline = Pipeline::open(fx_pipeline_without_baseline(
        &source,
        &lookup,
        &target,
        LookupTableIdChangePolicy::UseCurrent,
    ))
    .await
    .expect("the opt-in pipeline opens");
    pipeline
        .run_until_caught_up()
        .await
        .expect("the opt-in pipeline falls back to the lookup head rather than failing");

    assert_eq!(
        target_rows(&target).await,
        vec![(42, Some(2.0))],
        "the surviving head is what use_current substitutes"
    );
    let commit = latest_target_commit(&target).await;
    assert!(
        commit.contains("\"ddi.lookup.fx_rates.current\":true"),
        "and the substitution must be recorded in target provenance: {commit}"
    );
}
