//! What a pipeline actually allocates between starting and its first commit.
//!
//! Ignored by default: these build multi-million-row tables and take minutes. They are here
//! because every memory incident this tool has had was diagnosed from outside the process,
//! by correlation, and twice that was wrong. Measuring costs less than guessing.
//!
//! ```text
//! cargo test --profile release-lean --test memory_shape -- --ignored --nocapture --test-threads=1
//! ROWS=6000000 BUDGET_MB=512 cargo test --profile release-lean --test memory_shape -- --ignored --nocapture
//! ```
//!
//! `ROWS`, `PER_COMMIT`, `FILES`, `ROWS_PER_FILE`, `COMMITS` size the fixtures; `BUDGET_MB`
//! installs a [`Budget`] so the bounded and unbounded shapes can be compared directly.
mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use deltalake::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, DeltaTable};

use delta_delta_ingest::storage::Storage;

fn rss_mib() -> f64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap();
    let pages: u64 = s.split_whitespace().nth(1).unwrap().parse().unwrap();
    (pages * 4096) as f64 / (1024.0 * 1024.0)
}

/// Samples RSS in the background so a phase's *peak* is visible, not just its end.
struct Peak {
    stop: Arc<AtomicBool>,
    max: Arc<AtomicU64>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Peak {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let max = Arc::new(AtomicU64::new(0));
        let (s, m) = (stop.clone(), max.clone());
        let handle = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                let now = (rss_mib() * 1024.0) as u64;
                m.fetch_max(now, Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        });
        Self {
            stop,
            max,
            handle: Some(handle),
        }
    }

    fn finish(mut self, label: &str, before: f64) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        let peak = self.max.load(Ordering::Relaxed) as f64 / 1024.0;
        println!(
            "  {label:<38} rss {:>8.0} MiB  peak {:>8.0} MiB  (+{:>7.0} MiB)",
            rss_mib(),
            peak,
            peak - before
        );
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("n", DataType::Int64, true),
    ]))
}

fn columns() -> Vec<StructField> {
    vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::String), false),
        StructField::new(
            "_timestamp",
            DeltaDataType::Primitive(PrimitiveType::Timestamp),
            false,
        ),
        StructField::new("n", DeltaDataType::Primitive(PrimitiveType::Long), true),
    ]
}

/// A 12-character key that looks random, so the hash table behaves like production's.
fn key_of(i: u64) -> String {
    let mut h = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= h >> 29;
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^= h >> 32;
    const A: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..12)
        .map(|k| A[((h >> (k * 5)) % A.len() as u64) as usize] as char)
        .collect()
}

async fn build(path: &str, rows: u64, per_commit: u64) {
    std::fs::create_dir_all(path).unwrap();
    DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(columns())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();

    let mut written = 0u64;
    while written < rows {
        let n = per_commit.min(rows - written);
        let ids: Vec<String> = (written..written + n).map(key_of).collect();
        let ts: Vec<i64> = (written..written + n)
            .map(|i| 1_770_000_000_000_000 + i as i64)
            .collect();
        let ns: Vec<i64> = (written..written + n).map(|i| i as i64).collect();
        let b = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(ids)) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")) as ArrayRef,
                Arc::new(Int64Array::from(ns)) as ArrayRef,
            ],
        )
        .unwrap();
        let t = Storage::default().open(path).await.unwrap();
        t.write(vec![b])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        written += n;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "builds multi-million-row tables; run deliberately"]
async fn startup_memory() {
    let rows: u64 = std::env::var("ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let per_commit: u64 = std::env::var("PER_COMMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500_000);

    if let Ok(b) = std::env::var("BUDGET_MB") {
        let bytes: u64 = b.parse::<u64>().unwrap() * 1024 * 1024;
        delta_delta_ingest::budget::Budget::resolve(Some(bytes), 1).install();
        println!("(budget installed: {b} MiB for one pipeline)");
    }

    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let target = root.join("target").to_str().unwrap().to_string();

    println!("\n=== {rows} rows, {per_commit} per commit ===");
    println!("  {:<38} rss {:>8.0} MiB", "baseline", rss_mib());

    build(&target, rows, per_commit).await;
    println!(
        "  {:<38} rss {:>8.0} MiB",
        "after building the table",
        rss_mib()
    );

    // --- phase 1: open
    let before = rss_mib();
    let p = Peak::start();
    let table = Storage::default().open(&target).await.unwrap();
    p.finish("Storage::open", before);

    // --- phase 2: the watermark read
    let before = rss_mib();
    let p = Peak::start();
    let d = delta_delta_ingest::dedup::Dedup::read(&table, "_timestamp", Some("id"))
        .await
        .unwrap();
    p.finish("Dedup::read", before);
    drop(d);

    // --- phase 3: preflight, which holds every key
    use deltalake::delta_datafusion::DataFusionMixins;
    let target_schema = table.snapshot().unwrap().snapshot().read_schema();
    let before = rss_mib();
    let p = Peak::start();
    delta_delta_ingest::upsert::preflight(
        &table,
        &target_schema,
        "id",
        "_timestamp",
        None,
        &[],
        Default::default(),
    )
    .await
    .unwrap();
    p.finish("upsert::preflight", before);

    println!("  {:<38} rss {:>8.0} MiB", "after everything", rss_mib());
}

/// A source shaped like a raw Kafka table: a key, a clock, and a fat JSON-ish payload.
fn wide_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn wide_columns() -> Vec<StructField> {
    vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::String), false),
        StructField::new(
            "_timestamp",
            DeltaDataType::Primitive(PrimitiveType::Timestamp),
            false,
        ),
        StructField::new(
            "payload",
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ]
}

/// What one batch costs in memory, against the budget that is supposed to bound it.
///
/// `max_bytes_per_batch` counts the `Add`'s recorded size — parquet on disk, compressed.
/// What the process actually holds is that decoded into Arrow, for every file in the batch
/// at once, and then whatever the transform makes of it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "builds multi-million-row tables; run deliberately"]
async fn batch_decode_amplification() {
    use delta_delta_ingest::pipeline::Pipeline;

    let rows_per_file: u64 = std::env::var("ROWS_PER_FILE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    let files: u64 = std::env::var("FILES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);

    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("s").to_str().unwrap().to_string();
    let target = root.join("t").to_str().unwrap().to_string();

    for p in [&source, &target] {
        std::fs::create_dir_all(p).unwrap();
        DeltaTable::try_from_url(ensure_table_uri(p).unwrap())
            .await
            .unwrap()
            .create()
            .with_columns(wide_columns())
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap();
    }

    // A payload that does not compress to nothing, so parquet-on-disk and decoded-Arrow
    // are in a realistic ratio rather than an absurd one.
    for f in 0..files {
        let base = f * rows_per_file;
        let ids: Vec<String> = (base..base + rows_per_file).map(key_of).collect();
        let ts: Vec<i64> = (base..base + rows_per_file)
            .map(|i| 1_770_000_000_000_000 + i as i64)
            .collect();
        let payload: Vec<String> = (base..base + rows_per_file)
            .map(|i| {
                let k = key_of(i);
                format!(
                    "{{\"sku\":\"{k}\",\"n\":{i},\"desc\":\"{k}{k}{k}\",\"tags\":[\"{k}\",\"{k}\"]}}"
                )
            })
            .collect();
        let b = RecordBatch::try_new(
            wide_schema(),
            vec![
                Arc::new(StringArray::from(ids)) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")) as ArrayRef,
                Arc::new(StringArray::from(payload)) as ArrayRef,
            ],
        )
        .unwrap();
        let t = Storage::default().open(&source).await.unwrap();
        t.write(vec![b])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    let on_disk: u64 = walkdir_parquet_bytes(&source);
    println!(
        "\n=== {files} files x {rows_per_file} rows: {:.0} MiB of parquet on disk ===",
        on_disk as f64 / 1048576.0
    );

    if let Ok(b) = std::env::var("BUDGET_MB") {
        let bytes: u64 = b.parse::<u64>().unwrap() * 1024 * 1024;
        delta_delta_ingest::budget::Budget::resolve(Some(bytes), 1).install();
        println!("  (budget installed: {b} MiB for one pipeline)");
    }

    let mut cfg = common::pipeline_cfg("decode", &source, &target);
    // Admit the whole table in one batch, as a cold pipeline with a 256MB budget does.
    cfg.max_bytes_per_batch = 8 * 1024 * 1024 * 1024;
    cfg.max_files_per_batch = 10_000;

    let mut p = Pipeline::open(cfg).await.unwrap();
    let before = rss_mib();
    let peak = Peak::start();
    let mut steps = 0;
    while !matches!(
        p.step().await.unwrap(),
        delta_delta_ingest::pipeline::StepOutcome::CaughtUp
    ) {
        steps += 1;
    }
    peak.finish(&format!("catching up in {steps} step(s)"), before);
    println!(
        "  amplification vs parquet on disk: {:.1}x",
        (rss_mib() - before).max(0.0) * 1048576.0 / on_disk as f64
    );
}

fn walkdir_parquet_bytes(path: &str) -> u64 {
    let mut total = 0;
    for e in std::fs::read_dir(path).unwrap().flatten() {
        let m = e.metadata().unwrap();
        if m.is_file() && e.file_name().to_string_lossy().ends_with(".parquet") {
            total += m.len();
        }
    }
    total
}

/// The other candidate: opening by replaying the whole log, which is what a target whose
/// newest checkpoint another engine wrote now does on every start.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "builds multi-million-row tables; run deliberately"]
async fn log_replay_open() {
    use deltalake::arrow::array::StructArray;
    use deltalake::arrow::datatypes::Fields;
    use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use deltalake::parquet::arrow::ArrowWriter;

    let commits: u64 = std::env::var("COMMITS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);

    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let target = root.join("t").to_str().unwrap().to_string();

    println!("\n=== {commits} commits, one small file each ===");
    build(&target, commits * 100, 100).await;

    // A checkpoint at the head, so a plain open reads one file instead of the whole log.
    let t = Storage::default().open(&target).await.unwrap();
    deltalake::checkpoints::create_checkpoint(&t, None)
        .await
        .unwrap();
    drop(t);

    let before = rss_mib();
    let p = Peak::start();
    let t = deltalake::open_table(ensure_table_uri(&target).unwrap())
        .await
        .unwrap();
    p.finish("open via checkpoint", before);
    drop(t);

    // Now make that checkpoint unreadable, exactly as a Trino OPTIMIZE does, so `open`
    // falls back to replaying every commit from version 0.
    let version = commits; // one commit per batch, plus the CREATE at 0
    let cp = format!("{target}/_delta_log/{version:020}.checkpoint.parquet");
    if std::path::Path::new(&cp).exists() {
        let batches: Vec<RecordBatch> =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&cp).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap())
                .collect();
        let value_fields: Fields = vec![Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]
        .into();
        let stats_fields: Fields = vec![Field::new(
            "minValues",
            DataType::Struct(value_fields.clone()),
            true,
        )]
        .into();
        let mut out = Vec::new();
        for b in &batches {
            let idx = b.schema().index_of("add").unwrap();
            use deltalake::arrow::array::AsArray;
            let add = b.column(idx).as_struct();
            let n = add.len();
            let sp: ArrayRef = Arc::new(StructArray::new(
                stats_fields.clone(),
                vec![Arc::new(StructArray::new(
                    value_fields.clone(),
                    vec![
                        Arc::new(deltalake::arrow::array::TimestampMillisecondArray::from(
                            vec![Some(0i64); n],
                        )) as ArrayRef,
                    ],
                    None,
                )) as ArrayRef],
                add.nulls().cloned(),
            ));
            let mut fields: Vec<Arc<Field>> = add.fields().iter().cloned().collect();
            fields.push(Arc::new(Field::new(
                "stats_parsed",
                DataType::Struct(stats_fields.clone()),
                true,
            )));
            let mut cols: Vec<ArrayRef> = add.columns().to_vec();
            cols.push(sp);
            let new_add = StructArray::new(fields.into(), cols, add.nulls().cloned());
            let mut top: Vec<Arc<Field>> = b.schema().fields().iter().cloned().collect();
            top[idx] = Arc::new(Field::new("add", new_add.data_type().clone(), true));
            let mut tc = b.columns().to_vec();
            tc[idx] = Arc::new(new_add) as ArrayRef;
            out.push(RecordBatch::try_new(Arc::new(Schema::new(top)), tc).unwrap());
        }
        let f = std::fs::File::create(&cp).unwrap();
        let mut w = ArrowWriter::try_new(f, out[0].schema(), None).unwrap();
        for b in &out {
            w.write(b).unwrap();
        }
        w.close().unwrap();
    } else {
        println!("  (no checkpoint at {version}; listing:)");
        for e in std::fs::read_dir(format!("{target}/_delta_log")).unwrap() {
            let n = e.unwrap().file_name();
            if n.to_string_lossy().contains("checkpoint") {
                println!("    {n:?}");
            }
        }
    }

    let before = rss_mib();
    let p = Peak::start();
    let t = Storage::default().open(&target).await.unwrap();
    p.finish("open by replaying the log", before);
    println!("  version = {:?}", t.version());
}
