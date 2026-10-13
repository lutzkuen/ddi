//! A cut-off kept per Kafka partition, for a source that orders its sequence only within one.
//!
//! A table written by kafka-delta-ingest carries `kafka_partition` and `kafka_offset` beside
//! every row, and each ingester commit holds a slice of every partition's backlog. Partition
//! `A` (0) runs ahead here and `B` (1) lags, so B's late rows are older than A's newest. A
//! single cut-off — the newest timestamp in the target — takes them for rows a rebuild already
//! wrote, and the rescan after the rebuild can start past them. With `dedup_partition`, each
//! row is measured against its own partition's newest value in the target, and the rescan
//! starts after the newest source commit whose rows are all covered that way.
//!
//! Offsets are on deliberately different scales per partition (A near a million, B near one),
//! as they are on any real topic, so a test that passes on offsets is not passing by accident.

mod common;

use std::sync::Arc;

use delta_delta_ingest::config::{ResolvedPipeline, WriteMode};
use delta_delta_ingest::dedup::{
    bounded_rescan_start, bounded_rescan_start_partitioned, rescan_start, CoverageReason, Dedup,
    CUTOFF_PARTITION_COLUMNS_KEY, CUTOFF_REASON_KEY,
};
use delta_delta_ingest::pipeline::{CoverageWindow, Pipeline, StepOutcome};
use delta_delta_ingest::Error;
use deltalake::arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::arrow::util::display::array_value_to_string;
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable, TableProperty};
use futures::TryStreamExt;

const A: i32 = 0;
const B: i32 = 1;

// ---------------------------------------------------------------- shapes

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        // Nullable, so a row without one reaches the pipeline rather than the writer's refusal.
        Field::new("kafka_partition", DataType::Int32, true),
        Field::new("kafka_offset", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

/// One Kafka message: its id, partition, offset, what it says, and when.
#[derive(Clone, Copy, Debug)]
struct Msg {
    id: i64,
    partition: Option<i32>,
    offset: i64,
    status: &'static str,
    ts: i64,
}

/// A message whose offset is derived from its timestamp, on its partition's own scale.
fn msg(id: i64, partition: i32, ts: i64) -> Msg {
    Msg {
        id,
        partition: Some(partition),
        offset: if partition == A { 1_000_000 + ts } else { ts },
        status: "a",
        ts,
    }
}

fn said(id: i64, partition: i32, status: &'static str, ts: i64) -> Msg {
    Msg {
        status,
        ..msg(id, partition, ts)
    }
}

fn batch(rows: &[Msg]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.partition).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.offset).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.status).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| Some(r.ts)).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

fn window(reason: CoverageReason, through: Option<u64>, resumed: bool) -> CoverageWindow {
    CoverageWindow {
        reason,
        through,
        resumed,
    }
}

/// What the rebuild in most of these tests read: A's lead and B's first message. Version 1 of
/// the source, as version 0 is the table's creation.
fn v0() -> Vec<Msg> {
    vec![msg(1, A, 10), msg(2, A, 11), msg(3, B, 1)]
}

/// B's backlog, delivered after A had already reached 11.
fn v1() -> Vec<Msg> {
    vec![msg(4, B, 2), msg(5, B, 3), msg(6, B, 4), msg(7, B, 5)]
}

// ---------------------------------------------------------------- lakehouse

struct Lake {
    _dir: tempfile::TempDir,
    source: String,
    target: String,
}

#[derive(Default)]
struct Source {
    /// `delta.dataSkippingStatsColumns`, when not every column is to have statistics.
    stats_columns: Option<&'static str>,
    /// Delta partition columns.
    partition_by: Vec<&'static str>,
}

async fn create(path: &str, source: &Source) {
    let delta: StructType = schema().as_ref().try_into_kernel().unwrap();
    let mut create = DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists);
    if let Some(columns) = source.stats_columns {
        create = create
            .with_configuration_property(TableProperty::DataSkippingStatsColumns, Some(columns));
    }
    if !source.partition_by.is_empty() {
        create = create.with_partition_columns(source.partition_by.clone());
    }
    create.await.unwrap();
}

async fn write(path: &str, rows: &[Msg], mode: SaveMode) {
    open_table(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .write(vec![batch(rows)])
        .with_save_mode(mode)
        .await
        .unwrap();
}

async fn table(path: &str) -> DeltaTable {
    open_table(ensure_table_uri(path).unwrap()).await.unwrap()
}

impl Lake {
    async fn new() -> Self {
        Self::with(Source::default()).await
    }

    async fn with(source: Source) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let src = root.join("events_raw").to_str().unwrap().to_string();
        let target = root.join("events").to_str().unwrap().to_string();
        create(&src, &source).await;
        create(&target, &Source::default()).await;
        Self {
            _dir: dir,
            source: src,
            target,
        }
    }

    /// One ingester commit.
    async fn arrive(&self, rows: &[Msg]) {
        write(&self.source, rows, SaveMode::Append).await;
    }

    /// A batch job's full refresh of the target.
    async fn rebuild(&self, rows: &[Msg]) {
        write(&self.target, rows, SaveMode::Overwrite).await;
    }

    /// Rows appended straight into the target by another writer.
    async fn backfill(&self, rows: &[Msg]) {
        write(&self.target, rows, SaveMode::Append).await;
    }

    /// The single cut-off: what every pipeline had before the option.
    fn global_cfg(&self) -> ResolvedPipeline {
        let mut c = common::pipeline_cfg("events", &self.source, &self.target);
        c.dedup_timestamp = Some("_timestamp".into());
        c.dedup_key = Some("id".into());
        c
    }

    /// One cut-off per Kafka partition.
    fn cfg(&self) -> ResolvedPipeline {
        let mut c = self.global_cfg();
        c.dedup_partition = vec!["kafka_partition".into()];
        c
    }

    fn upsert_cfg(&self) -> ResolvedPipeline {
        let mut c = self.cfg();
        c.write_mode = WriteMode::Upsert;
        c.upsert_key = Some("id".into());
        c
    }

    /// The target as `(id, status)`, sorted, duplicates kept.
    async fn rows(&self) -> Vec<(i64, String)> {
        let (_t, stream) = table(&self.target).await.scan_table().await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out = Vec::new();
        for b in batches {
            let get = |n: &str| b.column(b.schema().index_of(n).unwrap()).clone();
            let ids = get("id");
            let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
            let st = deltalake::arrow::compute::cast(&get("status"), &DataType::Utf8).unwrap();
            let st = st.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                out.push((ids.value(i), st.value(i).to_string()));
            }
        }
        out.sort();
        out
    }

    async fn ids(&self) -> Vec<i64> {
        self.rows().await.into_iter().map(|r| r.0).collect()
    }

    /// The cut-off as the pipeline would read it after a rebuild.
    async fn dedup(&self) -> Dedup {
        Dedup::read_partitioned(
            &table(&self.target).await,
            "_timestamp",
            Some("id"),
            &["kafka_partition".to_string()],
        )
        .await
        .unwrap()
    }

    /// Where the rescan after a rebuild would start.
    async fn rescan_from(&self) -> u64 {
        bounded_rescan_start_partitioned(&table(&self.source).await, &self.dedup().await, 0, 10_000)
            .await
            .unwrap()
    }

    /// The `commitInfo` of the target's newest commit.
    fn last_commit_info(&self) -> serde_json::Map<String, serde_json::Value> {
        let log = format!("{}/_delta_log", self.target);
        let newest = std::fs::read_dir(&log)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "json"))
            .max()
            .unwrap();
        std::fs::read_to_string(newest)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .find_map(|a| a.get("commitInfo").and_then(|i| i.as_object()).cloned())
            .unwrap()
    }
}

async fn run(cfg: ResolvedPipeline) -> usize {
    Pipeline::open(cfg)
        .await
        .expect("pipeline should open")
        .run_until_caught_up()
        .await
        .expect("streaming should succeed")
}

/// Each partition's mark, as `(partition, value)` text.
fn marks(d: &Dedup) -> Vec<(String, String)> {
    d.partition_marks()
        .into_iter()
        .map(|(p, m)| (p.to_string(), array_value_to_string(&m, 0).unwrap()))
        .collect()
}

/// What `f` logs, at DEBUG and up, beside what it returns.
async fn logged<T>(f: impl std::future::Future<Output = T>) -> (T, String) {
    #[derive(Clone, Default)]
    struct Buf(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let buf = Buf::default();
    let w = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(move || w.clone())
        .finish();
    let out = {
        // The test runtime is single-threaded, so the future runs where this is the default.
        let _g = tracing::subscriber::set_default(subscriber);
        f.await
    };
    let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    (out, text)
}

/// The source's data files, by path.
fn source_files(lake: &Lake) -> std::collections::BTreeSet<std::path::PathBuf> {
    std::fs::read_dir(&lake.source)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect()
}

// ================================================================ the loss, and the fix

#[tokio::test]
async fn without_a_partition_column_a_rebuild_loses_a_lagging_partitions_late_rows() {
    // The control: what a single cut-off does with this lake, so that the fix below is
    // visibly the option and not the lake.
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;
    run(lake.global_cfg()).await;
    assert_eq!(lake.ids().await, (1..=7).collect::<Vec<_>>());

    // The batch job read through version 1, the first after the table's creation.
    lake.rebuild(&v0()).await;

    let target = table(&lake.target).await;
    let single = Dedup::read(&target, "_timestamp", Some("id"))
        .await
        .unwrap();
    assert_eq!(
        bounded_rescan_start(
            &table(&lake.source).await,
            "_timestamp",
            single.watermark().unwrap(),
            0,
            10_000
        )
        .await
        .unwrap(),
        3,
        "version 2's newest row, B's 5, is below A's 11: taken as covered"
    );
    run(lake.global_cfg()).await;
    assert_eq!(
        lake.ids().await,
        vec![1, 2, 3],
        "B's late rows are gone, and nothing re-reads them"
    );
}

#[tokio::test]
async fn a_rebuild_keeps_a_lagging_partitions_late_rows() {
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;
    run(lake.cfg()).await;
    lake.rebuild(&v0()).await;

    assert_eq!(
        marks(&lake.dedup().await),
        [
            ("0".to_string(), "1970-01-01T00:00:00.000011".to_string()),
            ("1".to_string(), "1970-01-01T00:00:00.000001".to_string())
        ]
    );
    assert_eq!(
        lake.rescan_from().await,
        2,
        "every row of version 1 is at or below its partition's mark; version 2's are not"
    );

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Rebuilt, Some(2), false))
    );
    let first = p.step().await.unwrap();
    let StepOutcome::Progressed { rows, covered, .. } = first else {
        panic!("expected B's backlog to be written, got {first:?}");
    };
    assert_eq!((rows, covered), (4, 0));
    assert_eq!(
        p.coverage(),
        None,
        "B2 is past B's mark, so the window is over"
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, (1..=7).collect::<Vec<_>>());
}

#[tokio::test]
async fn offsets_work_as_the_dedup_timestamp() {
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;
    let mut cfg = lake.cfg();
    cfg.dedup_timestamp = Some("kafka_offset".into());
    run(cfg.clone()).await;
    lake.rebuild(&v0()).await;

    let d = Dedup::read_partitioned(
        &table(&lake.target).await,
        "kafka_offset",
        Some("id"),
        &["kafka_partition".to_string()],
    )
    .await
    .unwrap();
    assert_eq!(
        marks(&d),
        [
            ("0".to_string(), "1000011".to_string()),
            ("1".to_string(), "1".to_string())
        ]
    );
    assert_eq!(
        rescan_start(&table(&lake.source).await, &d, 0, 10_000)
            .await
            .unwrap(),
        2
    );

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert!(p.coverage().is_some());
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, (1..=7).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_bootstrap_against_a_populated_target_keeps_lagging_rows() {
    // Loaded by a batch job from version 1 before ddi first started on it.
    let lake = Lake::new().await;
    lake.backfill(&v0()).await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Bootstrap, Some(2), false))
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, (1..=7).collect::<Vec<_>>());

    // And the single cut-off, on the same lake, drops B's backlog as covered.
    let lake = Lake::new().await;
    lake.backfill(&v0()).await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;
    run(lake.global_cfg()).await;
    assert_eq!(lake.ids().await, vec![1, 2, 3]);
}

#[tokio::test]
async fn a_replaced_source_keeps_lagging_rows() {
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    run(lake.cfg()).await;

    // Dropped and recreated, then re-seeded with the same history and B's backlog.
    std::fs::remove_dir_all(&lake.source).unwrap();
    create(&lake.source, &Source::default()).await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::SourceReplaced, None, false))
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, (1..=7).collect::<Vec<_>>());
    assert_eq!(p.coverage(), None);
}

// ================================================================ when the window closes

#[tokio::test]
async fn the_window_closes_on_a_row_past_its_own_partitions_mark() {
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    run(lake.cfg()).await;
    lake.rebuild(&v0()).await;

    // B2 is below A's 11 — under a single cut-off it would be covered — but past B's 1.
    lake.arrive(&[msg(4, B, 2)]).await;
    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert!(p.coverage().is_some());
    let first = p.step().await.unwrap();
    assert!(
        matches!(
            first,
            StepOutcome::Progressed {
                rows: 1,
                covered: 0,
                ..
            }
        ),
        "{first:?}"
    );
    assert_eq!(p.coverage(), None);

    // From here an old A row is a late one, not a covered one.
    lake.arrive(&[msg(5, A, 3)]).await;
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn a_row_in_a_partition_the_target_lacks_is_kept_and_closes_the_window() {
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    run(lake.cfg()).await;
    lake.rebuild(&v0()).await;

    lake.arrive(&[msg(4, 2, 1)]).await;
    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert!(p.coverage().is_some());
    let first = p.step().await.unwrap();
    assert!(
        matches!(
            first,
            StepOutcome::Progressed {
                rows: 1,
                covered: 0,
                ..
            }
        ),
        "{first:?}"
    );
    assert_eq!(p.coverage(), None);
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4]);
}

// ================================================================ reopening mid-window

#[tokio::test]
async fn a_reopen_mid_window_resumes_partitioned() {
    let lake = Lake::new().await;
    lake.backfill(&v0()).await;
    lake.arrive(&v0()[..2]).await;
    lake.arrive(&v0()[2..]).await;
    lake.arrive(&v1()).await;
    let mut cfg = lake.cfg();
    cfg.max_files_per_batch = 1;

    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    let first = p.step().await.unwrap();
    assert!(
        matches!(first, StepOutcome::Skipped { covered: 2, .. }),
        "{first:?}"
    );
    drop(p);
    let info = lake.last_commit_info();
    assert_eq!(
        info.get(CUTOFF_REASON_KEY),
        Some(&serde_json::json!("bootstrap.partitioned"))
    );
    assert_eq!(
        CoverageReason::parse("bootstrap.partitioned"),
        None,
        "a build from before dedup_partition reads the reason with this same table, so it \
         resumes as though the window had closed rather than with one cut-off"
    );
    assert_eq!(
        info.get(CUTOFF_PARTITION_COLUMNS_KEY),
        Some(&serde_json::json!(["kafka_partition"])),
        "the form is recorded with the window"
    );

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Bootstrap, Some(3), true))
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, (1..=7).collect::<Vec<_>>());
    assert_eq!(p.coverage(), None);
}

#[tokio::test]
async fn a_window_recorded_with_a_single_cut_off_is_not_resumed_under_dedup_partition() {
    // Its commits do not record which dedup_timestamp they were kept on, and adopting the
    // option is when that tends to change — to an offset, whose one global maximum (A's,
    // near a million) would cover every row of B. So it is taken as closed: duplicates,
    // never a gap. Under either sequence.
    for ts in ["_timestamp", "kafka_offset"] {
        let lake = Lake::new().await;
        lake.backfill(&v0()).await;
        lake.arrive(&v0()[..1]).await;
        lake.arrive(&v0()[1..2]).await;
        lake.arrive(&v0()[2..]).await;
        let mut global = lake.global_cfg();
        global.max_files_per_batch = 1;
        let mut p = Pipeline::open(global).await.unwrap();
        p.step().await.unwrap();
        drop(p);
        let info = lake.last_commit_info();
        assert_eq!(
            info.get(CUTOFF_REASON_KEY),
            Some(&serde_json::json!("bootstrap"))
        );
        assert!(!info.contains_key(CUTOFF_PARTITION_COLUMNS_KEY));

        let mut cfg = lake.cfg();
        cfg.dedup_timestamp = Some(ts.into());
        let mut p = Pipeline::open(cfg).await.unwrap();
        assert_eq!(p.coverage(), None, "{ts}");
        p.run_until_caught_up().await.unwrap();
        assert_eq!(
            lake.ids().await,
            vec![1, 2, 2, 3, 3],
            "{ts}: the rest of the load is written again rather than anything being dropped"
        );
    }
}

#[tokio::test]
async fn a_window_recorded_under_another_partition_column_is_not_resumed() {
    // A partitioned window's commits can hold rows above other partitions' marks. Re-read with
    // a single cut-off they would over-cover, so the window is taken as closed: duplicates,
    // never a gap.
    let lake = Lake::new().await;
    lake.backfill(&v0()).await;
    lake.arrive(&v0()[..2]).await;
    lake.arrive(&v0()[2..]).await;
    let mut cfg = lake.cfg();
    cfg.max_files_per_batch = 1;
    let mut p = Pipeline::open(cfg).await.unwrap();
    p.step().await.unwrap();
    drop(p);

    let p = Pipeline::open(lake.global_cfg()).await.unwrap();
    assert_eq!(p.coverage(), None);
    let mut other = lake.cfg();
    other.dedup_partition = vec!["status".into()];
    let mut p = Pipeline::open(other).await.unwrap();
    assert_eq!(p.coverage(), None);
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        lake.ids().await,
        vec![1, 2, 3, 3],
        "B1 is written again rather than anything being dropped"
    );
}

// ================================================================ the rescan bound

#[tokio::test]
async fn an_idle_partition_does_not_push_the_rescan_back_to_the_beginning() {
    // B delivered once and went quiet, so its mark is old. The rescan still starts right after
    // the newest commit the rebuild read.
    let lake = Lake::new().await;
    lake.arrive(&[msg(1, A, 1), msg(2, B, 1)]).await;
    lake.arrive(&[msg(3, A, 2)]).await;
    lake.arrive(&[msg(4, A, 3)]).await;
    lake.rebuild(&[msg(1, A, 1), msg(2, B, 1), msg(3, A, 2), msg(4, A, 3)])
        .await;
    lake.arrive(&[msg(5, A, 4)]).await;
    lake.arrive(&[msg(6, A, 5)]).await;
    assert_eq!(lake.rescan_from().await, 4);
}

#[tokio::test]
async fn a_rescan_without_partition_statistics_still_bounds_by_reading_the_columns() {
    let lake = Lake::with(Source {
        stats_columns: Some("id"),
        ..Default::default()
    })
    .await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;
    lake.rebuild(&v0()).await;
    assert_eq!(lake.rescan_from().await, 2);
}

#[tokio::test]
async fn a_delta_partitioned_source_bounds_by_partition_values() {
    let lake = Lake::with(Source {
        partition_by: vec!["kafka_partition"],
        ..Default::default()
    })
    .await;
    lake.arrive(&[msg(1, A, 1), msg(2, B, 1)]).await;
    lake.arrive(&[msg(3, A, 2), msg(4, B, 2)]).await;
    lake.rebuild(&[msg(1, A, 1), msg(2, B, 1), msg(3, A, 2), msg(4, B, 2)])
        .await;
    lake.arrive(&[msg(5, A, 3), msg(6, B, 3)]).await;
    assert_eq!(lake.rescan_from().await, 3);

    // And the pipeline reads it as it reads any other source.
    run(lake.cfg()).await;
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn a_redelivered_old_row_after_the_rebuild_does_not_move_the_rescan_start() {
    // A whole commit past the rebuild is not covered just because one row in it is old.
    let lake = Lake::new().await;
    lake.arrive(&[msg(1, A, 1), msg(2, A, 2), msg(3, B, 1)])
        .await;
    lake.rebuild(&[msg(1, A, 1), msg(2, A, 2), msg(3, B, 1)])
        .await;
    lake.arrive(&[msg(4, B, 2), msg(5, A, 3)]).await;
    lake.arrive(&[msg(6, A, 4), msg(1, A, 1)]).await;
    assert_eq!(lake.rescan_from().await, 2);
}

#[tokio::test]
async fn a_createtime_straggler_in_a_later_commit_does_not_skip_versions() {
    // A producer whose clock runs behind writes a new row into B below B's mark.
    let lake = Lake::new().await;
    lake.arrive(&[msg(1, A, 1), msg(2, A, 2), msg(3, B, 5)])
        .await;
    lake.rebuild(&[msg(1, A, 1), msg(2, A, 2), msg(3, B, 5)])
        .await;
    lake.arrive(&[msg(4, B, 6), msg(5, A, 3)]).await;
    lake.arrive(&[msg(6, A, 4), msg(7, B, 4)]).await;
    assert_eq!(lake.rescan_from().await, 2);
}

#[tokio::test]
async fn a_change_commit_after_the_rebuild_proves_nothing() {
    // A retention DELETE on the source rewrites version 1's file, full of old rows.
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;
    lake.rebuild(&v0()).await;
    let (_t, _m) = table(&lake.source)
        .await
        .delete()
        .with_predicate("id = 2")
        .await
        .unwrap();
    assert_eq!(lake.rescan_from().await, 2);
}

#[tokio::test]
async fn a_restore_after_the_rebuild_proves_nothing() {
    // A RESTORE that only re-adds a file removes nothing, so it reads as a plain append.
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;
    lake.rebuild(&v0()).await;
    let (_t, _m) = table(&lake.source)
        .await
        .delete()
        .with_predicate("id <= 3")
        .await
        .unwrap();
    let (_t, _m) = table(&lake.source)
        .await
        .restore()
        .with_version_to_restore(1)
        .await
        .unwrap();
    assert_eq!(lake.rescan_from().await, 2);
}

#[tokio::test]
async fn several_partition_columns_bound_the_rescan_from_partition_values_and_statistics() {
    // `status` is a Delta partition column of the source, so its value comes from the log;
    // `kafka_partition` comes from statistics, or from the file. Both are joined into the one
    // key the target's read produced, or the bound would silently become a full rescan.
    let lake = Lake::with(Source {
        partition_by: vec!["status"],
        ..Default::default()
    })
    .await;
    lake.arrive(&[said(1, A, "a", 1), said(2, B, "b", 1)]).await;
    lake.arrive(&[said(3, A, "a", 2), said(4, B, "b", 2)]).await;
    lake.rebuild(&[
        said(1, A, "a", 1),
        said(2, B, "b", 1),
        said(3, A, "a", 2),
        said(4, B, "b", 2),
    ])
    .await;
    lake.arrive(&[said(5, A, "a", 3), said(6, B, "b", 3)]).await;

    let d = Dedup::read_partitioned(
        &table(&lake.target).await,
        "_timestamp",
        Some("id"),
        &["status".to_string(), "kafka_partition".to_string()],
    )
    .await
    .unwrap();
    assert_eq!(
        bounded_rescan_start_partitioned(&table(&lake.source).await, &d, 0, 10_000)
            .await
            .unwrap(),
        3
    );

    let mut cfg = lake.cfg();
    cfg.dedup_partition = vec!["status".into(), "kafka_partition".into()];
    run(cfg).await;
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn a_source_without_the_key_column_is_still_bounded() {
    // A silver model that derives its key: the target's `event_id` is nowhere in the source.
    // Without it, a row exactly at its partition's mark cannot be shown covered, but rows
    // below the marks still settle a commit, so the rescan is bounded rather than full.
    let lake = Lake::with(Source {
        // No statistics on the timestamp or the partition, so every file is read.
        stats_columns: Some("id"),
        ..Default::default()
    })
    .await;
    lake.arrive(&[msg(1, A, 1), msg(2, B, 1)]).await;
    lake.arrive(&[msg(3, A, 2), msg(4, B, 2)]).await;
    lake.arrive(&[msg(5, A, 3), msg(6, B, 3)]).await;

    let silver_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("kafka_partition", DataType::Int32, true),
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let silver = format!("{}_silver", lake.target);
    let delta: StructType = silver_schema.as_ref().try_into_kernel().unwrap();
    DeltaTable::try_from_url(ensure_table_uri(&silver).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .await
        .unwrap();
    // What the model made of versions 1 and 2.
    let rows = [(1, A, 1), (2, B, 1), (3, A, 2), (4, B, 2)];
    table(&silver)
        .await
        .write(vec![RecordBatch::try_new(
            silver_schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| 100 + r.0).collect::<Vec<i64>>(),
                )) as ArrayRef,
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<i32>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<i64>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap()])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    let d = Dedup::read_partitioned(
        &table(&silver).await,
        "_timestamp",
        Some("event_id"),
        &["kafka_partition".to_string()],
    )
    .await
    .unwrap();
    assert_eq!(
        bounded_rescan_start_partitioned(&table(&lake.source).await, &d, 0, 10_000)
            .await
            .unwrap(),
        2,
        "version 2's rows sit exactly at the marks, which without the key is not covered; \
         version 1's are below them"
    );
}

#[tokio::test]
async fn commits_after_the_rebuild_that_mix_partitions_are_settled_without_being_read() {
    // Every file an ingester writes after the rebuild mixes partitions, and its lowest
    // timestamp is past every partition's mark: its statistics say so, and nothing is read.
    // The files are gone to prove it — a read of one would fail, and be logged.
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    lake.rebuild(&v0()).await;
    let read_by_the_rebuild = source_files(&lake);
    for i in 0..5 {
        lake.arrive(&[msg(100 + i, A, 20 + i), msg(200 + i, B, 20 + i)])
            .await;
    }
    for f in source_files(&lake).difference(&read_by_the_rebuild) {
        std::fs::remove_file(f).unwrap();
    }
    let (from, log) = logged(lake.rescan_from()).await;
    assert_eq!(from, 2);
    assert!(
        !log.contains("cannot read this file for the rescan bound"),
        "a post-rebuild file was read: {log}"
    );
}

#[tokio::test]
async fn a_model_that_filters_out_a_partition_cannot_be_bounded_and_says_so() {
    // `WHERE kafka_partition = 0`: B's rows are in every source commit and never in the
    // target, so no commit can be shown covered, on every handover. Nothing is lost — the
    // rescan starts at the beginning — but it is slow, and the log says why.
    let lake = Lake::new().await;
    for t in 1..=3 {
        lake.arrive(&[msg(2 * t, A, 10 * t), msg(2 * t + 1, B, 10 * t + 1)])
            .await;
    }
    lake.rebuild(&[msg(2, A, 10), msg(4, A, 20), msg(6, A, 30)])
        .await;
    lake.arrive(&[msg(8, A, 40), msg(9, B, 41)]).await;

    let source = table(&lake.source).await;
    let dedup = lake.dedup().await;
    let (from, log) = logged(bounded_rescan_start_partitioned(&source, &dedup, 0, 10_000)).await;
    assert_eq!(from.unwrap(), 0, "back to the starting version");
    assert!(
        log.contains("the rescan could not be bounded") && log.contains("partition=1"),
        "names the partition the target lacks: {log}"
    );

    // The single cut-off does not ask which partitions the target holds.
    let single = Dedup::read(&table(&lake.target).await, "_timestamp", Some("id"))
        .await
        .unwrap();
    assert_eq!(rescan_start(&source, &single, 0, 10_000).await.unwrap(), 3);
}

// ================================================================ nulls, columns, limits

#[tokio::test]
async fn a_null_partition_stops_an_append_pipeline_on_an_ordinary_resume() {
    let lake = Lake::new().await;
    lake.arrive(&[msg(1, A, 1)]).await;
    run(lake.cfg()).await;
    lake.arrive(&[Msg {
        partition: None,
        ..msg(2, A, 2)
    }])
    .await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(p.coverage(), None);
    let e = p.run_until_caught_up().await.unwrap_err();
    assert!(matches!(e, Error::Schema(_)), "got: {e}");
    assert!(e.to_string().contains("kafka_partition"), "got: {e}");
    assert_eq!(lake.ids().await, vec![1], "and nothing was written");
}

#[tokio::test]
async fn a_null_partition_stops_an_upsert_pipeline_on_an_ordinary_resume() {
    // The merge refuses a row with no timestamp on every batch, but knows nothing of
    // partitions: accepted here, the row would stop the window the next rebuild opens.
    let lake = Lake::new().await;
    lake.arrive(&[msg(1, A, 1)]).await;
    run(lake.upsert_cfg()).await;
    lake.arrive(&[Msg {
        partition: None,
        ..msg(2, A, 2)
    }])
    .await;

    let mut p = Pipeline::open(lake.upsert_cfg()).await.unwrap();
    assert_eq!(p.coverage(), None);
    let e = p.run_until_caught_up().await.unwrap_err();
    assert!(matches!(e, Error::Schema(_)), "got: {e}");
    assert!(e.to_string().contains("kafka_partition"), "got: {e}");
    assert_eq!(lake.ids().await, vec![1], "and nothing was written");
}

#[tokio::test]
async fn an_upsert_transform_that_leaves_out_the_partition_stops_on_its_first_batch() {
    // Upsert lets a transform leave out a column it does not update. The partition column
    // cannot be one of them: the window after the next rebuild would stop on it, so the
    // first batch does instead.
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    let mut cfg = lake.upsert_cfg();
    cfg.transform_sql = Some("SELECT id, kafka_offset, status, _timestamp FROM source".into());
    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None, "an empty target opens no window");
    let e = p.run_until_caught_up().await.unwrap_err();
    assert!(matches!(e, Error::Schema(_)), "got: {e}");
    assert!(e.to_string().contains("kafka_partition"), "got: {e}");
    assert!(lake.ids().await.is_empty());
}

#[tokio::test]
async fn a_null_partition_stops_a_window_too() {
    let lake = Lake::new().await;
    lake.backfill(&v0()).await;
    lake.arrive(&[Msg {
        partition: None,
        ..msg(9, A, 20)
    }])
    .await;
    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert!(p.coverage().is_some());
    let e = p.run_until_caught_up().await.unwrap_err();
    assert!(matches!(e, Error::Schema(_)), "got: {e}");
}

#[tokio::test]
async fn null_partitions_in_the_target_are_ignored_not_fatal() {
    let lake = Lake::new().await;
    lake.backfill(&[
        msg(1, A, 10),
        Msg {
            partition: None,
            ..msg(2, B, 50)
        },
    ])
    .await;
    assert_eq!(
        marks(&lake.dedup().await),
        [("0".to_string(), "1970-01-01T00:00:00.000010".to_string())],
        "the row with no partition claims coverage for none"
    );
}

#[tokio::test]
async fn a_dedup_partition_the_target_lacks_is_refused_on_every_open() {
    let lake = Lake::new().await;
    lake.arrive(&[msg(1, A, 1)]).await;
    run(lake.cfg()).await;

    let mut cfg = lake.cfg();
    cfg.dedup_partition = vec!["nope".into()];
    let Err(e) = Pipeline::open(cfg).await else {
        panic!("a dedup_partition the target does not have must not open");
    };
    let msg = e.to_string();
    assert!(matches!(e, Error::Config(_)), "got: {msg}");
    assert!(msg.contains("nope"), "names the column: {msg}");
    assert!(
        msg.contains("kafka_partition"),
        "and lists the real ones: {msg}"
    );
}

#[tokio::test]
async fn an_unsupported_partition_type_is_refused_at_open() {
    let lake = Lake::new().await;
    lake.arrive(&[msg(1, A, 1)]).await;
    let mut cfg = lake.cfg();
    cfg.dedup_timestamp = Some("kafka_offset".into());
    cfg.dedup_partition = vec!["_timestamp".into()];
    let Err(e) = Pipeline::open(cfg).await else {
        panic!("a timestamp cannot key a cut-off");
    };
    assert!(matches!(e, Error::Config(_)), "got: {e}");
    assert!(e.to_string().contains("integer or a string"), "got: {e}");
}

#[tokio::test]
async fn too_many_partition_values_are_refused() {
    let lake = Lake::new().await;
    lake.backfill(&[msg(1, A, 1), msg(2, B, 1), msg(3, 2, 1)])
        .await;
    let e = Dedup::read_bounded(
        &table(&lake.target).await,
        "_timestamp",
        Some("id"),
        &["kafka_partition".to_string()],
        2,
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::Config(_)), "got: {e}");
    assert!(e.to_string().contains("low-cardinality"), "got: {e}");
}

// ================================================================ reading the marks

#[tokio::test]
async fn per_partition_maxima_are_carried_across_files() {
    let lake = Lake::new().await;
    lake.backfill(&[msg(1, A, 30), msg(2, B, 1)]).await;
    lake.backfill(&[msg(3, A, 10), msg(4, B, 2)]).await;
    lake.backfill(&[msg(5, A, 20), msg(6, B, 3)]).await;
    let d = lake.dedup().await;
    assert_eq!(
        marks(&d),
        [
            ("0".to_string(), "1970-01-01T00:00:00.000030".to_string()),
            ("1".to_string(), "1970-01-01T00:00:00.000003".to_string())
        ]
    );
    assert_eq!(d.boundary_key_count(), 2, "one key at each mark");
}

#[tokio::test]
async fn a_higher_value_in_one_partition_discards_only_that_partitions_keys() {
    let lake = Lake::new().await;
    lake.backfill(&[msg(1, A, 5), msg(2, A, 5), msg(3, B, 5), msg(4, B, 5)])
        .await;
    lake.backfill(&[msg(5, A, 6)]).await;
    let d = lake.dedup().await;
    assert_eq!(
        d.boundary_key_count(),
        3,
        "A's mark moved to 6 and holds key 5; B's still holds 3 and 4"
    );

    // And each is used against its own partition: B4 at B's mark is held, A2 below A's is too,
    // B9 at B's mark is new.
    let f = d
        .filter(batch(&[msg(4, B, 5), msg(2, A, 5), msg(9, B, 5)]))
        .unwrap();
    let kept = f
        .kept
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(kept.values().to_vec(), vec![9]);
    assert_eq!(f.covered, 2);
}

// ================================================================ upsert

#[tokio::test]
async fn an_upsert_rebuild_keeps_the_newest_delivery_of_a_lagging_partitions_keys() {
    // Under upsert the dropped row would have been its key's newest delivery, so the key would
    // just stay on a stale version.
    let lake = Lake::new().await;
    let before = [
        said(100, A, "a", 10),
        said(101, A, "a", 11),
        said(1, B, "old", 1),
    ];
    lake.arrive(&before).await;
    lake.arrive(&[said(1, B, "new", 2), said(2, B, "new", 3)])
        .await;
    run(lake.upsert_cfg()).await;
    lake.rebuild(&before).await;

    let mut p = Pipeline::open(lake.upsert_cfg()).await.unwrap();
    assert!(p.coverage().is_some());
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        lake.rows().await,
        vec![
            (1, "new".to_string()),
            (2, "new".to_string()),
            (100, "a".to_string()),
            (101, "a".to_string()),
        ]
    );
}

#[tokio::test]
async fn a_staged_upsert_apply_keeps_a_lagging_partitions_late_rows() {
    // The apply half merges into the target but reads the stage, so it is the stage the
    // per-partition bound walks after a rebuild of the target, and its window has no head.
    let lake = Lake::new().await;
    lake.arrive(&v0()).await;
    lake.arrive(&v1()).await;
    let r = delta_delta_ingest::config::Config::from_toml_str(&format!(
        r#"
[[pipeline]]
name = "events"
app_id = "ddi.events"
source_uri = "{}"
target_uri = "{}"
write_mode = "staged_upsert"
dedup_timestamp = "_timestamp"
dedup_key = "id"
dedup_partition = "kafka_partition"
upsert_key = "id"
"#,
        lake.source, lake.target
    ))
    .unwrap()
    .resolve()
    .unwrap();
    assert_eq!(r.len(), 2);
    let (ingest, apply) = (r[0].clone(), r[1].clone());
    assert_eq!(apply.dedup_partition, ["kafka_partition"]);
    run(ingest.clone()).await;
    run(apply.clone()).await;
    assert_eq!(lake.ids().await, (1..=7).collect::<Vec<_>>());

    lake.rebuild(&v0()).await;
    let mut p = Pipeline::open(apply).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Rebuilt, None, false))
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        lake.ids().await,
        (1..=7).collect::<Vec<_>>(),
        "B's backlog is re-read from the stage and merged again"
    );
}

// ================================================================ dbt

/// One model with a malformed `ddi_partition` among three: that model is held back, by name,
/// and the others run. As an error it stopped every model in the project.
#[test]
fn a_malformed_ddi_partition_holds_back_that_model_alone() {
    use delta_delta_ingest::config::Config;

    let resolve = |bad: serde_json::Value| {
        let model = |name: &str, meta: serde_json::Value| {
            serde_json::json!({
                "name": name,
                "resource_type": "model",
                "schema": "silver",
                "compiled_code": format!(
                    "SELECT order_id, _timestamp, kafka_partition FROM bronze.{name}_raw"
                ),
                "depends_on": {"nodes": [format!("source.p.bronze.{name}_raw")]},
                "config": {"materialized": "table"},
                "meta": meta,
            })
        };
        let source = |name: &str| {
            serde_json::json!({
                "name": format!("{name}_raw"),
                "resource_type": "source",
                "schema": "bronze",
            })
        };
        let manifest = serde_json::json!({
            "nodes": {
                "model.p.orders": model("orders", serde_json::json!({})),
                "model.p.payments": model("payments", serde_json::json!({})),
                "model.p.refunds": model("refunds", serde_json::json!({"ddi_partition": bad})),
            },
            "sources": {
                "source.p.bronze.orders_raw": source("orders"),
                "source.p.bronze.payments_raw": source("payments"),
                "source.p.bronze.refunds_raw": source("refunds"),
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest.to_string()).unwrap();
        let toml = format!(
            "manifest = {:?}\n\n[storage]\nuri_template = \"file:///tmp/lake/{{schema}}/{{name}}\"\n",
            path.display().to_string()
        );
        Config::from_toml_str(&toml).unwrap().resolve_all()
    };

    let r = resolve(serde_json::json!("kafka_partition")).unwrap();
    assert_eq!(r.pipelines.len(), 3);
    assert!(r.rejected.is_empty());

    for bad in [
        serde_json::json!(0),
        serde_json::json!(["kafka_partition", 1]),
        serde_json::json!({"column": "kafka_partition"}),
        serde_json::json!(null),
        serde_json::json!([]),
        serde_json::json!(""),
    ] {
        let r = resolve(bad.clone()).unwrap_or_else(|e| panic!("{bad}: the project failed: {e}"));
        let mut running: Vec<_> = r.pipelines.iter().map(|p| p.name.as_str()).collect();
        running.sort();
        assert_eq!(running, ["orders", "payments"], "{bad}");
        assert_eq!(r.rejected.len(), 1, "{bad}");
        assert_eq!(r.rejected[0].name, "refunds", "{bad}");
        assert!(
            r.rejected[0].reason.contains("partition"),
            "{bad}: {}",
            r.rejected[0].reason
        );
    }
}
