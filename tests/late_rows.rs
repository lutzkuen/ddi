//! A source whose timestamps do not increase with arrival, and the only places where that
//! is allowed to matter.
//!
//! A Delta table written from a multi-partition Kafka topic is append-only and still orders
//! `_timestamp` only within a partition. Each ingester commit carries a slice of every
//! partition's backlog, so a later commit routinely holds rows from a lagging partition that
//! are older than rows another partition has already delivered.
//!
//! An ordinary restart must deliver every one of them: the `txn` offset is exact on its own.
//! The timestamp cut-off belongs only to the coverage windows where the target's contents have
//! to be inferred from its data — a rebuild, a first start against a populated target, and a
//! reopen part-way through either — and each window has to end, and be resumed, at the right
//! place. Partition `A` runs ahead here and `B` lags; `-` is a row whose partition does not
//! matter.

mod common;

use std::sync::Arc;

use delta_delta_ingest::config::{ResolvedPipeline, WriteMode};
use delta_delta_ingest::dedup::{bounded_rescan_start, CoverageReason, Dedup};
use delta_delta_ingest::pipeline::{CoverageWindow, Pipeline, StepOutcome};
use delta_delta_ingest::Error;
use deltalake::arrow::array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable, TableProperty};
use futures::TryStreamExt;

// ---------------------------------------------------------------- shapes

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("part", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        // Nullable, so a row without one reaches the pipeline rather than the writer's refusal.
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

/// One delivery: its id, the partition it came through, what it says, and when.
#[derive(Clone, Copy, Debug)]
struct Row {
    id: i64,
    part: &'static str,
    status: &'static str,
    ts: Option<i64>,
}

fn row(id: i64, part: &'static str, ts: i64) -> Row {
    Row {
        id,
        part,
        status: "a",
        ts: Some(ts),
    }
}

fn said(id: i64, status: &'static str, ts: i64) -> Row {
    Row {
        id,
        part: "-",
        status,
        ts: Some(ts),
    }
}

/// Rows whose timestamp is their id: the well-ordered case.
fn ordered(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
    ids.map(|i| row(i, "-", i)).collect()
}

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.part).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.status).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

fn want(rows: &[(i64, &str, i64)]) -> Vec<(i64, String, i64)> {
    rows.iter().map(|r| (r.0, r.1.to_string(), r.2)).collect()
}

fn window(reason: CoverageReason, through: Option<u64>, resumed: bool) -> CoverageWindow {
    CoverageWindow {
        reason,
        through,
        resumed,
    }
}

// ---------------------------------------------------------------- lakehouse

struct Lake {
    _dir: tempfile::TempDir,
    source: String,
    target: String,
}

async fn create(path: &str, stats_columns: Option<&str>) {
    let delta: StructType = schema().as_ref().try_into_kernel().unwrap();
    let mut create = DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists);
    if let Some(columns) = stats_columns {
        create = create
            .with_configuration_property(TableProperty::DataSkippingStatsColumns, Some(columns));
    }
    create.await.unwrap();
}

async fn write(path: &str, rows: &[Row], mode: SaveMode) {
    open_table(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .write(vec![batch(rows)])
        .with_save_mode(mode)
        .await
        .unwrap();
}

impl Lake {
    async fn new() -> Self {
        Self::with_source_stats(None).await
    }

    /// A source whose files carry no statistics for `_timestamp`, so a rescan after a rebuild
    /// cannot be bounded and starts from the beginning — the case where a window spans many
    /// source commits, and a restart can land in the middle of it.
    async fn without_timestamp_stats() -> Self {
        Self::with_source_stats(Some("id")).await
    }

    async fn with_source_stats(stats_columns: Option<&str>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let source = root.join("events_raw").to_str().unwrap().to_string();
        let target = root.join("events").to_str().unwrap().to_string();
        create(&source, stats_columns).await;
        create(&target, None).await;
        Self {
            _dir: dir,
            source,
            target,
        }
    }

    /// One ingester commit.
    async fn arrive(&self, rows: &[Row]) {
        write(&self.source, rows, SaveMode::Append).await;
    }

    /// A batch job's full refresh of the target.
    async fn rebuild(&self, rows: &[Row]) {
        write(&self.target, rows, SaveMode::Overwrite).await;
    }

    /// Rows appended straight into the target by another writer: a backfill, or a repair.
    async fn backfill(&self, rows: &[Row]) {
        write(&self.target, rows, SaveMode::Append).await;
    }

    async fn optimize_source(&self) {
        let (_t, stats) = open_table(ensure_table_uri(&self.source).unwrap())
            .await
            .unwrap()
            .optimize()
            .await
            .unwrap();
        assert!(
            stats.num_files_added > 0 || stats.num_files_removed > 0,
            "optimize was a no-op, so this proves nothing"
        );
    }

    /// Append, with the cut-off configured the way a dbt model would declare it.
    fn cfg(&self) -> ResolvedPipeline {
        let mut c = common::pipeline_cfg("events", &self.source, &self.target);
        c.dedup_timestamp = Some("_timestamp".into());
        c.dedup_key = Some("id".into());
        c
    }

    fn upsert_cfg(&self) -> ResolvedPipeline {
        let mut c = self.cfg();
        c.write_mode = WriteMode::Upsert;
        c.upsert_key = Some("id".into());
        c
    }

    /// The target as `(id, status, ts)`, sorted, duplicates kept.
    async fn rows(&self) -> Vec<(i64, String, i64)> {
        let (_t, stream) = open_table(ensure_table_uri(&self.target).unwrap())
            .await
            .unwrap()
            .scan_table()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out = Vec::new();
        for b in batches {
            let get = |n: &str| b.column(b.schema().index_of(n).unwrap()).clone();
            let ids = get("id");
            let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
            let st = deltalake::arrow::compute::cast(&get("status"), &DataType::Utf8).unwrap();
            let st = st.as_any().downcast_ref::<StringArray>().unwrap();
            let ts = get("_timestamp");
            let ts = ts
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.push((ids.value(i), st.value(i).to_string(), ts.value(i)));
            }
        }
        out.sort();
        out
    }

    async fn ids(&self) -> Vec<i64> {
        self.rows().await.into_iter().map(|r| r.0).collect()
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

// ================================================================ an ordinary restart

#[tokio::test]
async fn a_restart_keeps_the_late_rows_of_a_lagging_partition() {
    // The loss this file exists for. After commit 1 the target's newest timestamp is A's 11,
    // and everything B delivers next is older than that. Nothing rebuilt the target, so the
    // restart has no coverage to infer: all of commit 2 is new.
    let lake = Lake::new().await;
    lake.arrive(&[row(1, "A", 10), row(2, "A", 11), row(3, "B", 1)])
        .await;
    run(lake.cfg()).await;

    lake.arrive(&[
        row(4, "B", 2),
        row(5, "B", 3),
        row(6, "B", 4),
        row(7, "B", 5),
    ])
    .await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(
        p.coverage(),
        None,
        "a restart resumes from its own offset, which is exact"
    );
    p.run_until_caught_up().await.unwrap();

    assert_eq!(
        lake.ids().await,
        (1..=7).collect::<Vec<_>>(),
        "every row of commit 2, once each"
    );
}

#[tokio::test]
async fn a_restart_keeps_the_newest_delivery_of_a_lagging_partitions_keys() {
    // The same loss under upsert is quieter still: the dropped row was its key's newest
    // delivery, so the key just stays on a stale version.
    let lake = Lake::new().await;
    lake.arrive(&[said(100, "a", 10), said(101, "a", 11), said(1, "old", 1)])
        .await;
    run(lake.upsert_cfg()).await;

    lake.arrive(&[
        said(1, "new", 2),
        said(2, "new", 3),
        said(3, "new", 4),
        said(4, "new", 5),
    ])
    .await;
    run(lake.upsert_cfg()).await;

    assert_eq!(
        lake.rows().await,
        want(&[
            (1, "new", 2),
            (2, "new", 3),
            (3, "new", 4),
            (4, "new", 5),
            (100, "a", 10),
            (101, "a", 11),
        ])
    );
}

#[tokio::test]
async fn a_foreign_append_to_the_target_does_not_become_a_cut_off() {
    // A repair appended straight into the target, far newer than anything streamed. It is
    // not a rebuild — it removed nothing — so it says nothing about what this pipeline still
    // owes the target, and must not become a watermark every later row is measured against.
    let lake = Lake::new().await;
    lake.arrive(&ordered(1..=3)).await;
    run(lake.cfg()).await;

    lake.backfill(&[row(100, "-", 100)]).await;
    lake.arrive(&ordered(4..=5)).await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(p.coverage(), None);
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4, 5, 100]);
}

// ================================================================ where coverage is inferred

#[tokio::test]
async fn a_rebuild_is_still_de_duplicated_and_its_window_closes_on_the_first_newer_row() {
    let lake = Lake::new().await;
    lake.arrive(&ordered(1..=3)).await;
    lake.arrive(&ordered(4..=6)).await;
    run(lake.cfg()).await;

    // The batch job read through 5.
    lake.rebuild(&ordered(1..=5)).await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Rebuilt, Some(2), false))
    );
    let first = p.step().await.unwrap();
    let StepOutcome::Progressed { rows, covered, .. } = first else {
        panic!("expected the rescan to write something, got {first:?}");
    };
    assert_eq!(
        (rows, covered),
        (1, 2),
        "4 and 5 are the rebuild's; 6 is not"
    );
    assert_eq!(
        p.coverage(),
        None,
        "6 lies past anything the rebuild read, so the window is over"
    );

    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, (1..=6).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_bootstrap_window_closes_at_the_source_head_it_opened_against() {
    // The target was loaded from this source by a batch job before ddi first started on it.
    let lake = Lake::new().await;
    let loaded = [row(1, "A", 10), row(2, "A", 11), row(3, "B", 1)];
    lake.backfill(&loaded).await;
    lake.arrive(&loaded).await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Bootstrap, Some(1), false))
    );

    // B's backlog lands after the open, older than the watermark and not in the target. Were
    // it read in one batch with version 1, it would be filtered along with it.
    lake.arrive(&[row(5, "B", 2)]).await;
    let first = p.step().await.unwrap();
    assert!(
        matches!(first, StepOutcome::Skipped { covered: 3, .. }),
        "version 1 is the target's already, and read on its own: {first:?}"
    );
    assert_eq!(p.coverage(), None);
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, vec![1, 2, 3, 5]);

    lake.arrive(&[row(6, "B", 3)]).await;
    let next = p.step().await.unwrap();
    assert!(
        matches!(
            next,
            StepOutcome::Progressed {
                rows: 1,
                covered: 0,
                ..
            }
        ),
        "{next:?}"
    );

    lake.arrive(&[row(7, "B", 4)]).await;
    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(p.coverage(), None, "and a restart is an ordinary one");
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, vec![1, 2, 3, 5, 6, 7]);
}

#[tokio::test]
async fn a_reopen_part_way_through_a_bootstrap_continues_its_window() {
    let lake = Lake::new().await;
    lake.backfill(&ordered(1..=3)).await;
    lake.arrive(&ordered(1..=2)).await;
    lake.arrive(&ordered(3..=3)).await;
    lake.arrive(&ordered(4..=4)).await;
    let mut cfg = lake.cfg();
    cfg.max_files_per_batch = 1;

    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    let first = p.step().await.unwrap();
    assert!(
        matches!(first, StepOutcome::Skipped { covered: 2, .. }),
        "{first:?}"
    );
    // Killed here. Its commit is the newest in the target and carries its own txn, which on
    // its own reads exactly like an ordinary restart.
    drop(p);

    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Bootstrap, Some(3), true))
    );
    let second = p.step().await.unwrap();
    assert!(
        matches!(second, StepOutcome::Skipped { covered: 1, .. }),
        "3 is the target's already: {second:?}"
    );
    let third = p.step().await.unwrap();
    assert!(
        matches!(
            third,
            StepOutcome::Progressed {
                rows: 1,
                covered: 0,
                ..
            }
        ),
        "{third:?}"
    );
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4]);

    // Closed by 4: from here an older timestamp is a late row.
    lake.arrive(&[row(5, "-", 1)]).await;
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4, 5]);

    let p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None);
}

#[tokio::test]
async fn a_reopen_part_way_through_a_rebuild_rescan_continues_its_window() {
    let lake = Lake::without_timestamp_stats().await;
    for i in 1..=3 {
        lake.arrive(&ordered(i..=i)).await;
    }
    run(lake.cfg()).await;
    lake.rebuild(&ordered(1..=2)).await;

    // The premise: nothing bounds the rescan, so it starts from the beginning and spans
    // every source commit — which is what gives a restart somewhere to land inside it.
    let target = open_table(ensure_table_uri(&lake.target).unwrap())
        .await
        .unwrap();
    let source = open_table(ensure_table_uri(&lake.source).unwrap())
        .await
        .unwrap();
    let dedup = Dedup::read(&target, "_timestamp", Some("id"))
        .await
        .unwrap();
    assert_eq!(
        bounded_rescan_start(&source, "_timestamp", dedup.watermark().unwrap(), 0, 10_000)
            .await
            .unwrap(),
        0
    );

    let mut cfg = lake.cfg();
    cfg.max_files_per_batch = 1;
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Rebuilt, Some(3), false))
    );
    let first = p.step().await.unwrap();
    assert!(
        matches!(first, StepOutcome::Skipped { covered: 1, .. }),
        "{first:?}"
    );
    drop(p);

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(window(CoverageReason::Rebuilt, Some(3), true))
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        lake.ids().await,
        vec![1, 2, 3],
        "2 is the rebuild's and must not come back; 3 is not and must"
    );
}

#[tokio::test]
async fn an_upsert_reopen_mid_window_does_not_re_insert_a_key_the_rebuild_left_out() {
    // A merge would take a redelivery of a key the target holds as a no-op, but a key the
    // rebuild deliberately left out is not held at all: only the cut-off keeps it out.
    let lake = Lake::without_timestamp_stats().await;
    lake.arrive(&[said(1, "a", 1)]).await;
    lake.arrive(&[said(9, "a", 2)]).await;
    lake.arrive(&[said(2, "a", 3)]).await;
    lake.arrive(&[said(3, "a", 4)]).await;
    run(lake.upsert_cfg()).await;
    lake.rebuild(&[said(1, "a", 1), said(2, "a", 3)]).await;

    let mut cfg = lake.upsert_cfg();
    cfg.max_files_per_batch = 1;
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.step().await.unwrap();
    drop(p);

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert!(
        p.coverage().is_some_and(|w| w.resumed),
        "{:?}",
        p.coverage()
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        lake.rows().await,
        want(&[(1, "a", 1), (2, "a", 3), (3, "a", 4)])
    );
}

#[tokio::test]
async fn a_reopen_before_the_windows_first_commit_re_derives_its_head() {
    // Nothing records a window until its first commit, so a reopen before that infers it
    // again, from the head as it is then.
    let lake = Lake::without_timestamp_stats().await;
    for i in 1..=3 {
        lake.arrive(&ordered(i..=i)).await;
    }
    run(lake.cfg()).await;
    lake.rebuild(&ordered(1..=2)).await;

    let p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(p.coverage().and_then(|w| w.through), Some(3));
    drop(p);

    lake.arrive(&ordered(4..=4)).await;
    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(p.coverage().and_then(|w| w.through), Some(4));
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn a_window_whose_tail_is_only_compaction_is_not_resumed() {
    // A window recorded open whose remaining versions are all OPTIMIZE has nothing left to
    // filter. Resuming it would re-read the whole target on every restart of an idle
    // pipeline, and drop the next late row for no reason.
    let lake = Lake::new().await;
    lake.backfill(&ordered(1..=3)).await;
    lake.arrive(&ordered(1..=2)).await;
    lake.arrive(&ordered(3..=3)).await;
    lake.optimize_source().await;
    let mut cfg = lake.cfg();
    cfg.max_files_per_batch = 1;

    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    assert_eq!(p.coverage().and_then(|w| w.through), Some(3));
    let first = p.step().await.unwrap();
    let second = p.step().await.unwrap();
    let third = p.step().await.unwrap();
    assert!(
        matches!(first, StepOutcome::Skipped { covered: 2, .. }),
        "{first:?}"
    );
    assert!(
        matches!(second, StepOutcome::Skipped { covered: 1, .. }),
        "{second:?}"
    );
    assert_eq!(third, StepOutcome::CaughtUp);
    assert_eq!(
        p.coverage(),
        None,
        "the compaction carried the cursor past the window's head"
    );
    drop(p);

    // The last commit recorded the window open; only the compaction lies past it.
    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None);
    lake.arrive(&[row(4, "-", 1)]).await;
    p.run_until_caught_up().await.unwrap();
    assert_eq!(lake.ids().await, vec![1, 2, 3, 4]);
}

// ================================================================ what the cut-off also checked

#[tokio::test]
async fn a_null_timestamp_still_stops_an_append_pipeline_on_an_ordinary_resume() {
    // The cut-off refused a row with no timestamp on every batch, and it no longer runs on
    // every batch. The refusal has to outlive it: once written, such a row is one the next
    // rebuild handover can place on neither side of the watermark.
    let lake = Lake::new().await;
    lake.arrive(&[row(1, "-", 1)]).await;
    run(lake.cfg()).await;
    lake.arrive(&[Row {
        id: 2,
        part: "-",
        status: "a",
        ts: None,
    }])
    .await;

    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    assert_eq!(p.coverage(), None);
    let e = p.run_until_caught_up().await.unwrap_err();
    assert!(matches!(e, Error::Schema(_)), "got: {e}");
    assert!(e.to_string().contains("_timestamp"), "got: {e}");
    assert_eq!(lake.ids().await, vec![1], "and nothing was written");
}

#[tokio::test]
async fn a_dedup_timestamp_the_target_lacks_is_refused_on_every_open() {
    // Not only on the open that needs the cut-off, which may be months away.
    let lake = Lake::new().await;
    lake.arrive(&[row(1, "-", 1)]).await;
    run(lake.cfg()).await;

    let mut cfg = lake.cfg();
    cfg.dedup_timestamp = Some("nope".into());
    let Err(e) = Pipeline::open(cfg).await else {
        panic!("a dedup_timestamp the target does not have must not open");
    };
    let msg = e.to_string();
    assert!(matches!(e, Error::Config(_)), "got: {msg}");
    assert!(msg.contains("nope"), "names the column: {msg}");
    assert!(msg.contains("_timestamp"), "and lists the real ones: {msg}");
}
