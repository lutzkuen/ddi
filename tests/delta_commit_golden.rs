//! The Delta commits a pipeline writes, pinned against checked-in golden files.
//!
//! The Kafka source (docs/kafka-design.md §1.2) splits `Pipeline::step` into a source half
//! and a `process()` half, and routes `Pipeline::commit` through an `Offsets` enum. None of
//! that may change what a Delta-to-Delta pipeline writes. The rest of the suite asserts what
//! the tables read back as — rows, offsets, rejects — and not what each commit says, so a
//! refactor could drop a `ddi.*` key that nothing reads back, such as `ddi.sourceVersion`, or
//! give a zero-row commit a file, and stay green. This file records every `_delta_log/*.json`
//! of the target and its data-quality table, and compares them with tests/golden/delta_commit/.
//!
//! Eight shapes, one test each:
//! - a plain append;
//! - a batch whose every row is rejected, with a data-quality table: a zero-row commit that
//!   carries the offset, after the data-quality table's own commit;
//! - an upsert: a merge that inserts, then one that updates and inserts;
//! - an upsert whose merge changes nothing, which falls back to the zero-row commit;
//! - an append to a target partitioned by a string column;
//! - the same partitioned target with a batch whose every row is rejected;
//! - a coverage window: zero-row commits that record the window (`ddi.cutoff.*`), then the
//!   commit that closes it and records none;
//! - two batches committed by one pipeline, the second with every row rejected: what one
//!   batch leaves in the sink, read by the next commit.
//!
//! The first six are the ones docs/kafka-design.md §1.3 names. The last two were added after
//! the refactor, from golden files the code before it wrote, and pass unchanged after it.
//!
//! # What is compared
//!
//! Each commit becomes `{ version, commitInfo, actions }`. `commitInfo` is compared as a set
//! of keys and values: it is a map, and its key order was never stable byte for byte. The
//! other actions are compared as a list sorted by content: delta-rs keeps a partitioned
//! batch's writers in a `HashMap` and adds their files in its order, which changes from run to
//! run, and a Delta commit gives the order no meaning. The version-0 `CREATE` of each table is the test's own, and is kept so
//! the golden file shows the table every later commit was written to.
//!
//! # What is normalised, and why
//!
//! Only what differs from run to run:
//! - wall-clock times: `commitInfo.timestamp`, `add.modificationTime`,
//!   `remove.deletionTimestamp`, `txn.lastUpdated`, `metaData.createdTime`, and the min/max
//!   statistics of the data-quality table's `_timestamp`, which is when the reject was
//!   recorded. Each becomes `"<timestamp>"`.
//! - timing metrics: every `operationMetrics` key ending in `_time_ms` becomes `"<ms>"`.
//! - table ids: each becomes the table it belongs to (`<source table id>`, `<target table id>`,
//!   `<dq table id>`), so `ddi.sourceTableId` stays pinned to the source.
//! - file names: the uuid in a data file's name becomes `<file-N>`, numbered in order of first
//!   appearance, so a `remove` still names the `add` it undoes. The partition directory and the
//!   part number are kept. Any other uuid would become `<uuid-N>` the same way.
//! - the temporary directory the tables live in, which a `CREATE` records as their location:
//!   it becomes `<lake>`.
//! - the size of a file a merge wrote, in its `add` and in any later `remove`: the merge's join
//!   emits rows in no fixed order, so the same rows compress to a byte more or less from run to
//!   run. It becomes `"<size>"`; the file's statistics are kept. An append's sizes are kept.
//!
//! An `add`'s `stats`, and any operation parameter delta-rs writes as a JSON string, are
//! compared parsed, which is what lets the times inside them be normalised.
//!
//! # Updating the golden files
//!
//! ```text
//! DDI_UPDATE_GOLDEN=1 cargo test --test delta_commit_golden
//! ```
//!
//! rewrites tests/golden/delta_commit/*.json from what the code writes now, and passes. Do it
//! only for a change that is meant to alter what a commit says, and review the diff of those
//! files as part of that change. The Kafka refactor (M0) must leave them byte-for-byte as they
//! are.

mod common;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::pipeline_cfg;
use delta_delta_ingest::config::{ResolvedPipeline, WriteMode};
use delta_delta_ingest::dedup::DEFAULT_TIMESTAMP_COLUMN;
use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use deltalake::arrow::array::TimestampMicrosecondArray;
use deltalake::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::{StructField, StructType};
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};
use serde_json::{Map, Value};

/// Set to rewrite the golden files instead of comparing against them.
const UPDATE_ENV: &str = "DDI_UPDATE_GOLDEN";

// ---------------------------------------------------------------- shapes
//
// `orders_raw -> orders_stg`, as in data_quality.rs and upsert.rs, rather than the `(id, name)`
// tables of tests/common: a reject needs a column the target types differently from the
// source, and an upsert needs a sequence column. The pipeline is tests/common's
// `pipeline_cfg`, as everywhere else.

/// Bronze: `amount` is text, as it arrives.
fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Utf8, true),
        Field::new(
            DEFAULT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// Silver: `amount` is a number, so a row whose text is not one is rejected.
fn stg_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new(
            DEFAULT_TIMESTAMP_COLUMN,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// One order as bronze holds it: `(order_id, region, amount, _timestamp)`.
type Row = (i64, &'static str, &'static str, i64);

fn raw_batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        raw_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------- lakehouse

/// How the target is laid out.
#[derive(Clone, Copy)]
struct Layout {
    /// Partitioned by `region`, a string column.
    partitioned: bool,
    /// With a data-quality table beside it, so a reject is set aside rather than stopping
    /// the pipeline.
    dq: bool,
}

const PLAIN: Layout = Layout {
    partitioned: false,
    dq: false,
};

struct Lake {
    _dir: tempfile::TempDir,
    /// The directory every table of this lake is in, as the tables spell it.
    root: String,
    raw: String,
    stg: String,
    dq: Option<String>,
}

async fn create(path: &str, columns: Vec<StructField>, partition_by: &[&str]) {
    let mut create = DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(columns)
        .with_save_mode(SaveMode::ErrorIfExists);
    if !partition_by.is_empty() {
        create = create.with_partition_columns(partition_by.to_vec());
    }
    create.await.unwrap();
}

fn columns(schema: SchemaRef) -> Vec<StructField> {
    let delta: StructType = schema.as_ref().try_into_kernel().unwrap();
    delta.fields().cloned().collect()
}

impl Lake {
    async fn new(layout: Layout) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let raw = root.join("orders_raw").to_str().unwrap().to_string();
        let stg = root.join("orders_stg").to_str().unwrap().to_string();
        create(&raw, columns(raw_schema()), &[]).await;
        let partition_by: &[&str] = if layout.partitioned { &["region"] } else { &[] };
        create(&stg, columns(stg_schema()), partition_by).await;
        let dq = match layout.dq {
            true => {
                let dq = delta_delta_ingest::dq::uri_for(&stg);
                create(&dq, delta_delta_ingest::dq::columns(), &[]).await;
                Some(dq)
            }
            false => None,
        };
        Self {
            _dir: dir,
            root: root.to_str().unwrap().to_string(),
            raw,
            stg,
            dq,
        }
    }

    /// Rows arrive in bronze, one Delta commit per call.
    async fn arrive(&self, rows: &[Row]) {
        open_table(ensure_table_uri(&self.raw).unwrap())
            .await
            .unwrap()
            .write(vec![raw_batch(rows)])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    /// What a batch job that loaded the target before the pipeline first ran leaves there:
    /// the same orders, written straight to silver.
    async fn fill_target(&self, rows: &[Row]) {
        let batch = RecordBatch::try_new(
            stg_schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| r.2.parse::<i64>().unwrap())
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap();
        open_table(ensure_table_uri(&self.stg).unwrap())
            .await
            .unwrap()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    /// No transform: the coercer is what casts `amount`, and what rejects a row that will
    /// not cast.
    fn cfg(&self) -> ResolvedPipeline {
        pipeline_cfg("orders_stg", &self.raw, &self.stg)
    }

    fn upsert_cfg(&self) -> ResolvedPipeline {
        let mut c = self.cfg();
        c.dedup_timestamp = Some(DEFAULT_TIMESTAMP_COLUMN.to_string());
        c.dedup_key = Some("order_id".to_string());
        c.write_mode = WriteMode::Upsert;
        c.upsert_key = Some("order_id".to_string());
        c
    }

    /// Open a pipeline and run it until it has read everything, as a restart does.
    async fn stream(&self, cfg: ResolvedPipeline) -> usize {
        Pipeline::open(cfg)
            .await
            .expect("pipeline should open")
            .run_until_caught_up()
            .await
            .expect("streaming should succeed")
    }

    /// Every commit of the target and of its data-quality table, normalised, then compared
    /// with the golden file for `shape` (or written to it).
    fn check(&self, shape: &str) {
        let mut n = Normaliser {
            root: self.root.clone(),
            ..Normaliser::default()
        };
        for (path, label) in [
            (Some(&self.raw), "<source table id>"),
            (Some(&self.stg), "<target table id>"),
            (self.dq.as_ref(), "<dq table id>"),
        ] {
            if let Some(path) = path {
                n.labels
                    .insert(table_id(Path::new(path)), label.to_string());
            }
        }

        let mut got = Map::new();
        got.insert("target".into(), n.log(Path::new(&self.stg), &[]));
        if let Some(dq) = &self.dq {
            // The one wall-clock column: when the reject was recorded.
            got.insert(
                "dq".into(),
                n.log(Path::new(dq), &[DEFAULT_TIMESTAMP_COLUMN]),
            );
        }
        compare_with_golden(shape, Value::Object(got));
    }
}

// ---------------------------------------------------------------- the shapes

#[tokio::test]
async fn plain_append() {
    let lake = Lake::new(PLAIN).await;
    lake.arrive(&[(1, "eu", "100", 10), (2, "us", "200", 11)])
        .await;
    assert_eq!(lake.stream(lake.cfg()).await, 1);
    lake.check("plain_append");
}

#[tokio::test]
async fn all_rejected_with_a_data_quality_table() {
    // Nothing reaches the target, so the offset rides in a commit with no rows, written
    // after the data-quality table's own.
    let lake = Lake::new(Layout { dq: true, ..PLAIN }).await;
    lake.arrive(&[(1, "eu", "n/a", 10), (2, "us", "also bad", 11)])
        .await;
    assert_eq!(lake.stream(lake.cfg()).await, 1);
    lake.check("all_rejected");
}

#[tokio::test]
async fn upsert() {
    // A merge into an empty target, then one that updates order 1 and inserts order 3.
    let lake = Lake::new(PLAIN).await;
    lake.arrive(&[(1, "eu", "100", 10), (2, "us", "200", 11)])
        .await;
    assert_eq!(lake.stream(lake.upsert_cfg()).await, 1);
    lake.arrive(&[(1, "eu", "150", 20), (3, "eu", "300", 21)])
        .await;
    assert_eq!(lake.stream(lake.upsert_cfg()).await, 1);
    lake.check("upsert");
}

#[tokio::test]
async fn upsert_whose_merge_changes_nothing() {
    // The second delivery is older than the stored row, so the merge changes nothing and
    // delta-rs writes no commit of its own; `Sink::upsert` then commits the offset with no
    // rows. An ordinary reopen infers no coverage, so the row does reach the merge.
    let lake = Lake::new(PLAIN).await;
    lake.arrive(&[(1, "eu", "100", 30)]).await;
    assert_eq!(lake.stream(lake.upsert_cfg()).await, 1);
    lake.arrive(&[(1, "eu", "50", 10)]).await;

    // The pipeline's own zero-row commit, for a batch nothing of which reached the merge,
    // would look the same in the log, so which of the two wrote it is asserted here.
    let mut p = Pipeline::open(lake.upsert_cfg()).await.unwrap();
    let step = p.step().await.unwrap();
    let StepOutcome::Progressed {
        upsert: Some(noop), ..
    } = step
    else {
        panic!("expected the row to reach the merge, got {step:?}");
    };
    assert_eq!((noop.updated, noop.inserted, noop.committed), (0, 0, false));
    assert_eq!(p.step().await.unwrap(), StepOutcome::CaughtUp);
    lake.check("upsert_noop_merge");
}

#[tokio::test]
async fn append_to_a_partitioned_target() {
    // Two partitions, so one commit adds two files.
    let lake = Lake::new(Layout {
        partitioned: true,
        ..PLAIN
    })
    .await;
    lake.arrive(&[
        (1, "eu", "100", 10),
        (2, "us", "200", 11),
        (3, "eu", "300", 12),
    ])
    .await;
    assert_eq!(lake.stream(lake.cfg()).await, 1);
    lake.check("partitioned_append");
}

#[tokio::test]
async fn all_rejected_on_a_partitioned_target() {
    // The zero-row commit on a table whose writer has partitions to split by, and none to
    // write.
    let lake = Lake::new(Layout {
        partitioned: true,
        dq: true,
    })
    .await;
    lake.arrive(&[(1, "eu", "n/a", 10), (2, "us", "also bad", 11)])
        .await;
    assert_eq!(lake.stream(lake.cfg()).await, 1);
    lake.check("partitioned_all_rejected");
}

#[tokio::test]
async fn bootstrap_window() {
    // A target a batch job loaded before the pipeline first ran. The pipeline opens a
    // coverage window and drops what the target already holds, one source commit per batch,
    // until it has read to the source head it opened against. Each commit inside the window
    // records it, so that a reopen part-way carries on filtering; the commit that closes it
    // records nothing.
    let lake = Lake::new(PLAIN).await;
    let loaded = [
        (1, "eu", "100", 10),
        (2, "us", "200", 11),
        (3, "eu", "300", 12),
    ];
    lake.fill_target(&loaded).await;
    lake.arrive(&loaded[..2]).await;
    lake.arrive(&loaded[2..]).await;
    lake.arrive(&[(4, "us", "400", 13)]).await;

    let mut cfg = lake.cfg();
    cfg.dedup_timestamp = Some(DEFAULT_TIMESTAMP_COLUMN.to_string());
    cfg.dedup_key = Some("order_id".to_string());
    cfg.max_files_per_batch = 1;
    let mut p = Pipeline::open(cfg).await.unwrap();
    assert!(p.coverage().is_some(), "a first start on a loaded target");
    for covered in [2, 1] {
        let step = p.step().await.unwrap();
        assert!(
            matches!(step, StepOutcome::Skipped { covered: c, .. } if c == covered),
            "a batch the target holds already: {step:?}"
        );
    }
    let closing = p.step().await.unwrap();
    assert!(
        matches!(
            closing,
            StepOutcome::Progressed {
                rows: 1,
                covered: 0,
                ..
            }
        ),
        "{closing:?}"
    );
    assert_eq!(
        p.coverage(),
        None,
        "closed by the batch that read to its head"
    );
    assert_eq!(p.step().await.unwrap(), StepOutcome::CaughtUp);
    lake.check("bootstrap_window");
}

#[tokio::test]
async fn two_batches_in_one_process() {
    // Every other shape opens a pipeline per commit, so what one batch leaves in the sink
    // never reaches a second commit of the same pipeline. Here one pipeline commits twice: a
    // batch with a reject, then one whose every row is rejected, so a zero-row commit follows
    // a commit with rows, in both tables.
    let lake = Lake::new(Layout { dq: true, ..PLAIN }).await;
    lake.arrive(&[(1, "eu", "100", 10), (2, "us", "n/a", 11)])
        .await;
    let mut p = Pipeline::open(lake.cfg()).await.unwrap();
    let first = p.step().await.unwrap();
    assert!(
        matches!(
            first,
            StepOutcome::Progressed {
                rows: 1,
                rejected: 1,
                ..
            }
        ),
        "{first:?}"
    );

    lake.arrive(&[(3, "eu", "bad", 20)]).await;
    let second = p.step().await.unwrap();
    assert!(
        matches!(second, StepOutcome::Skipped { rejected: 1, .. }),
        "{second:?}"
    );
    assert_eq!(p.step().await.unwrap(), StepOutcome::CaughtUp);
    lake.check("two_batches_in_one_process");
}

// ---------------------------------------------------------------- reading the log

/// A table's id, from the `metaData` action of its first commit.
fn table_id(table: &Path) -> String {
    commit_actions(&table.join("_delta_log").join(format!("{:020}.json", 0)))
        .into_iter()
        .find_map(|a| a.get("metaData")?.get("id")?.as_str().map(str::to_string))
        .expect("version 0 creates the table, so it carries its metaData")
}

/// The actions of one commit file, one JSON object per line.
fn commit_actions(file: &Path) -> Vec<Value> {
    std::fs::read_to_string(file)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()))
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str(l)
                .unwrap_or_else(|e| panic!("{}: not a JSON action ({e}): {l}", file.display()))
        })
        .collect()
}

/// The table's commit files, `(version, path)`, oldest first. Checkpoints and
/// `_last_checkpoint` are not commits and are left out.
fn commit_files(table: &Path) -> Vec<(u64, PathBuf)> {
    let mut out: Vec<(u64, PathBuf)> = std::fs::read_dir(table.join("_delta_log"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?;
            let version = name.strip_suffix(".json")?;
            (version.len() == 20 && version.bytes().all(|b| b.is_ascii_digit()))
                .then(|| (version.parse().unwrap(), p.clone()))
        })
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------- normalising

/// Action fields that hold the wall-clock time a commit was written at.
const WALL_CLOCK_FIELDS: [&str; 6] = [
    "timestamp",
    "inCommitTimestamp",
    "modificationTime",
    "deletionTimestamp",
    "lastUpdated",
    "createdTime",
];

/// Replaces what differs from run to run, and keeps everything else.
#[derive(Default)]
struct Normaliser {
    /// The lake's temporary directory, written as `<lake>`.
    root: String,
    /// A uuid seen so far, and what it is written as.
    labels: HashMap<String, String>,
    files: usize,
    others: usize,
    /// Data files a merge wrote, by their path as the log spells it.
    unordered_files: HashSet<String>,
}

impl Normaliser {
    /// Every commit of one table. `wall_clock_columns` are columns whose values are the time
    /// a row was written, so their statistics differ on every run.
    fn log(&mut self, table: &Path, wall_clock_columns: &[&str]) -> Value {
        let commits = commit_files(table);
        assert!(
            !commits.is_empty(),
            "{} has no commits at all",
            table.display()
        );
        Value::Array(
            commits
                .into_iter()
                .map(|(version, file)| {
                    self.commit(version, commit_actions(&file), wall_clock_columns)
                })
                .collect(),
        )
    }

    fn commit(&mut self, version: u64, lines: Vec<Value>, wall_clock_columns: &[&str]) -> Value {
        let mut info = None;
        let mut actions = Vec::new();
        for line in lines {
            let Value::Object(action) = line else {
                panic!("version {version}: an action that is not an object: {line}");
            };
            assert_eq!(
                action.len(),
                1,
                "version {version}: one action per line, got {action:?}"
            );
            let (kind, body) = action.into_iter().next().unwrap();
            let body = without_wall_clock(body, wall_clock_columns);
            if kind == "commitInfo" {
                assert!(info.is_none(), "version {version}: two commitInfo actions");
                info = Some(body);
            } else {
                actions.push((kind, body));
            }
        }

        // A merge's join emits its rows in no fixed order, so the file it writes holds the
        // same rows in a different order from run to run, and compresses to a byte more or
        // less. Its size is left out, wherever the file appears; its statistics are kept.
        let merge = info
            .as_ref()
            .and_then(|i| i.get("operation"))
            .is_some_and(|op| op == "MERGE");
        for (kind, body) in &mut actions {
            let Some(path) = body.get("path").and_then(Value::as_str).map(str::to_string) else {
                continue;
            };
            if merge && kind == "add" {
                self.unordered_files.insert(path.clone());
            }
            if self.unordered_files.contains(&path) {
                if let Some(size) = body.get_mut("size") {
                    *size = Value::from("<size>");
                }
            }
        }

        // Sorted with the uuids blanked, so the order is the same on every run, and only
        // then labelled, so the labels are too.
        let mut actions: Vec<Value> = actions
            .into_iter()
            .map(|(kind, body)| {
                let mut action = Map::new();
                action.insert(kind, body);
                Value::Object(action)
            })
            .collect();
        actions.sort_by_cached_key(|a| map_uuids(&a.to_string(), |_| String::new()));
        let actions = actions
            .into_iter()
            .map(|a| self.labelled(a, false))
            .collect();
        let info = info.map(|i| self.labelled(i, false)).unwrap_or(Value::Null);

        let mut commit = Map::new();
        commit.insert("version".into(), Value::from(version));
        commit.insert("commitInfo".into(), info);
        commit.insert("actions".into(), Value::Array(actions));
        Value::Object(commit)
    }

    /// `v` with the lake's directory and every uuid in it replaced by their labels. A uuid
    /// first seen in a `path` is a data file's, and is labelled as one.
    fn labelled(&mut self, v: Value, in_path: bool) -> Value {
        match v {
            Value::String(mut s) => {
                if !self.root.is_empty() {
                    s = s.replace(&self.root, "<lake>");
                }
                Value::String(map_uuids(&s, |uuid| {
                    if let Some(label) = self.labels.get(uuid) {
                        return label.clone();
                    }
                    let label = if in_path {
                        self.files += 1;
                        format!("<file-{}>", self.files)
                    } else {
                        self.others += 1;
                        format!("<uuid-{}>", self.others)
                    };
                    self.labels.insert(uuid.to_string(), label.clone());
                    label
                }))
            }
            Value::Array(a) => {
                Value::Array(a.into_iter().map(|v| self.labelled(v, in_path)).collect())
            }
            Value::Object(o) => Value::Object(
                sorted(o)
                    .into_iter()
                    .map(|(k, v)| {
                        let in_path = in_path || k == "path";
                        (k, self.labelled(v, in_path))
                    })
                    .collect(),
            ),
            other => other,
        }
    }
}

/// `v` with its wall-clock times and timing metrics replaced, its JSON-in-a-string values
/// parsed, and its keys sorted.
fn without_wall_clock(v: Value, wall_clock_columns: &[&str]) -> Value {
    match v {
        Value::Object(o) => Value::Object(
            sorted(o)
                .into_iter()
                .map(|(k, v)| {
                    let v = if WALL_CLOCK_FIELDS.contains(&k.as_str()) && v.is_number() {
                        Value::from("<timestamp>")
                    } else if k.ends_with("_time_ms") && v.is_number() {
                        Value::from("<ms>")
                    } else if k == "stats" {
                        stats_without_wall_clock(v, wall_clock_columns)
                    } else {
                        without_wall_clock(parsed(v), wall_clock_columns)
                    };
                    (k, v)
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(
            a.into_iter()
                .map(|v| without_wall_clock(v, wall_clock_columns))
                .collect(),
        ),
        other => other,
    }
}

/// An `add`'s statistics, parsed, with the min and max of each wall-clock column replaced.
fn stats_without_wall_clock(stats: Value, wall_clock_columns: &[&str]) -> Value {
    let mut stats = match parsed(stats) {
        Value::Object(o) => o,
        other => return other,
    };
    for bound in ["minValues", "maxValues"] {
        if let Some(Value::Object(values)) = stats.get_mut(bound) {
            for column in wall_clock_columns {
                if let Some(v) = values.get_mut(*column) {
                    *v = Value::from("<timestamp>");
                }
            }
        }
    }
    Value::Object(sorted(stats))
}

/// A string holding a JSON object or array, parsed; anything else unchanged.
fn parsed(v: Value) -> Value {
    match &v {
        Value::String(s) if s.starts_with('{') || s.starts_with('[') => {
            serde_json::from_str(s).unwrap_or(v)
        }
        _ => v,
    }
}

/// The same map with its keys in sorted order, whether or not `serde_json` keeps insertion
/// order.
fn sorted(o: Map<String, Value>) -> Map<String, Value> {
    let mut entries: Vec<(String, Value)> = o.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries.into_iter().collect()
}

/// `s` with every hyphenated uuid in it replaced by `label(uuid)`.
fn map_uuids(s: &str, mut label: impl FnMut(&str) -> String) -> String {
    fn is_uuid(b: &[u8]) -> bool {
        b.len() == 36
            && b.iter().enumerate().all(|(i, c)| match i {
                8 | 13 | 18 | 23 => *c == b'-',
                _ => c.is_ascii_hexdigit(),
            })
    }
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        // All 36 bytes are ASCII when this matches, so both ends are char boundaries.
        if i + 36 <= s.len() && is_uuid(&bytes[i..i + 36]) {
            out.push_str(&label(&s[i..i + 36]));
            i += 36;
        } else {
            let c = s[i..].chars().next().unwrap();
            out.push(c);
            i += c.len_utf8();
        }
    }
    out
}

// ---------------------------------------------------------------- the golden files

fn golden_path(shape: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/delta_commit")
        .join(format!("{shape}.json"))
}

fn compare_with_golden(shape: &str, got: Value) {
    let path = golden_path(shape);
    let rendered = format!("{}\n", serde_json::to_string_pretty(&got).unwrap());

    if std::env::var_os(UPDATE_ENV).is_some_and(|v| !v.is_empty() && v != "0") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, rendered).unwrap();
        return;
    }

    let golden = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read the golden file {} ({e}). Run with {UPDATE_ENV}=1 to write it.",
            path.display()
        )
    });
    let want: Value = serde_json::from_str(&golden)
        .unwrap_or_else(|e| panic!("{} is not JSON: {e}", path.display()));
    if got == want {
        return;
    }

    // Values differ, so the canonical renderings do too; point at the first line that does.
    let want_rendered = serde_json::to_string_pretty(&want).unwrap();
    let (line, (w, g)) = want_rendered
        .lines()
        .chain(std::iter::repeat(""))
        .zip(rendered.lines().chain(std::iter::repeat("")))
        .enumerate()
        .find(|(_, (w, g))| w != g)
        .expect("the renderings differ somewhere");
    panic!(
        "the commits of shape {shape:?} no longer match {}.\n\
         First difference, at line {}:\n  golden: {w}\n  now:    {g}\n\
         If this change to the Delta log is intended, rerun with {UPDATE_ENV}=1 and review \
         the diff of the golden file. Everything written now:\n{rendered}",
        path.display(),
        line + 1,
    );
}
