//! Shared helpers for integration tests against real local-filesystem Delta tables.

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;

use deltalake::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, DeltaTable};

use delta_delta_ingest::config::ResolvedPipeline;
use delta_delta_ingest::source::ChangePolicy;

pub fn arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

pub fn delta_columns() -> Vec<StructField> {
    vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), false),
        StructField::new(
            "name",
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ]
}

pub fn batch(ids: &[i64]) -> RecordBatch {
    let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Create an empty Delta table at `path`.
pub async fn create_table(path: &str) -> DeltaTable {
    let url = ensure_table_uri(path).unwrap();
    DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(delta_columns())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap()
}

/// Open an existing table fresh from storage.
pub async fn open(path: &str) -> DeltaTable {
    let url = ensure_table_uri(path).unwrap();
    deltalake::open_table(url).await.unwrap()
}

/// Append one commit containing `ids`.
pub async fn append(path: &str, ids: &[i64]) -> DeltaTable {
    let t = open(path).await;
    t.write(vec![batch(ids)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

/// Read every `id` currently in the table.
pub async fn read_ids(path: &str) -> Vec<i64> {
    let t = open(path).await;
    let (_t, stream) = t.scan_table().await.unwrap();
    use futures::TryStreamExt;
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of("id").unwrap();
        let col = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id must be int64");
        for i in 0..col.len() {
            out.push(col.value(i));
        }
    }
    out.sort_unstable();
    out
}

pub fn has_duplicates(v: &[i64]) -> bool {
    let mut seen = HashSet::new();
    v.iter().any(|x| !seen.insert(*x))
}

/// A pipeline config wired to two local paths.
pub fn pipeline_cfg(name: &str, source: &str, target: &str) -> ResolvedPipeline {
    ResolvedPipeline {
        name: name.into(),
        app_id: format!("ddi.test.{name}"),
        source_uri: source.into(),
        target_uri: target.into(),
        lookups: vec![],
        starting_version: 0,
        change_policy: ChangePolicy::Fail,
        transform_sql: None,
        allowed_latency_secs: 1,
        max_bytes_per_batch: 256 * 1024 * 1024,
        max_files_per_batch: 1_000,
        max_output_rows_per_batch: 5_000_000,
        target_file_size: 128 * 1024 * 1024,
        watermark_uri: None,
        dedup_timestamp: None,
        dedup_key: None,
        write_mode: Default::default(),
        upsert_key: None,
        upsert_lookback: None,
        upsert_tiebreak: Vec::new(),
        stage_for: None,
        dq_uri: None,
        source_relation: None,
        target_relation: None,
        storage: delta_delta_ingest::storage::Storage::default(),
    }
}

/// A temp dir plus source/target paths inside it.
pub struct Fixture {
    pub dir: tempfile::TempDir,
    pub source: String,
    pub target: String,
}

impl Fixture {
    pub async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let source = root.join("source").to_str().unwrap().to_string();
        let target = root.join("target").to_str().unwrap().to_string();
        create_table(&source).await;
        create_table(&target).await;
        Self {
            dir,
            source,
            target,
        }
    }

    pub fn cfg(&self, name: &str) -> ResolvedPipeline {
        pipeline_cfg(name, &self.source, &self.target)
    }
}
