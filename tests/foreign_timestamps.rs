//! Reading a lakehouse that other engines also write.
//!
//! The Delta protocol says `timestamp` is microseconds. Trino writes milliseconds — into
//! the files its `OPTIMIZE` rewrites, and into the `stats_parsed` of the checkpoints it
//! leaves behind. Both are already in production lakehouses in volume, put there by a
//! compaction job owned by a different team on a different schedule, and neither is going
//! away. A reader that only accepts files its own writer produced cannot be pointed at a
//! lakehouse other tools write, which is the whole premise of this one.
//!
//! So the rule these tests pin down is: **the table's Delta schema is authoritative, and a
//! physical column that differs from it in precision alone is widened on read.** Nothing
//! else changes — everything that used to be refused still is, in the words it always used,
//! and everything that used to be accepted still is. A change that narrows what is refused
//! must not quietly refuse something new, and the Spark INT96 test below is here because
//! this one very nearly did.
//!
//! The fixtures matter more than the assertions here, because nobody writes this bug by
//! accident — it has to be built. `trinoize_checkpoint` and `retype_file_timestamps`
//! construct exactly the two artefacts a Trino `OPTIMIZE` leaves: a checkpoint whose
//! `stats_parsed` is typed after the table's columns at millisecond precision, and data
//! files rewritten the same way.
//!
//! Several of these tests would pass with the fix reverted, because `SchemaCoercer` already
//! absorbs a precision difference one batch at a time. The ones that genuinely pin it are
//! `one_batch_spanning_two_writers_is_still_one_batch`, where a `transform_sql` forces two
//! files to become one schema, and `a_timestamp_landing_in_a_numeric_column_carries_the_
//! declared_unit`, where the unit reaches the target as a number. Both were checked by
//! reverting the call site and watching them fail.

mod common;

use std::str::FromStr;
use std::sync::Arc;

use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
};
use deltalake::arrow::datatypes::{
    DataType, Field, Fields, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType,
};
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, DeltaTable};

use delta_delta_ingest::dedup::Dedup;
use delta_delta_ingest::pipeline::Pipeline;
use delta_delta_ingest::storage::Storage;

/// Two instants a thousand-fold error could not hide in: one second apart, and far enough
/// from the epoch that reading microseconds as milliseconds lands in 1970 rather than
/// merely a little early.
const T0: i64 = 1_770_000_000_000_000; // 2026-02-02T02:40:00Z
const T1: i64 = 1_770_000_001_000_000;
const T2: i64 = 1_770_000_002_000_000;

// ------------------------------------------------------------------ table fixtures

fn ts_columns() -> Vec<StructField> {
    vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), false),
        StructField::new(
            "kafka_timestamp",
            DeltaDataType::Primitive(PrimitiveType::Timestamp),
            true,
        ),
    ]
}

fn ts_arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "kafka_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

async fn create_ts_table(path: &str) -> DeltaTable {
    create_ts_table_with(path, &[]).await
}

async fn create_ts_table_with(path: &str, props: &[(&str, &str)]) -> DeltaTable {
    std::fs::create_dir_all(path).unwrap();
    let url = ensure_table_uri(path).unwrap();
    let mut create = DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(ts_columns())
        .with_save_mode(SaveMode::ErrorIfExists);
    for (k, v) in props {
        create = create
            .with_configuration_property(deltalake::TableProperty::from_str(k).unwrap(), Some(*v));
    }
    create.await.unwrap()
}

/// Append one commit, written the way delta-rs writes: microseconds, UTC.
async fn append_ts(path: &str, rows: &[(i64, i64)]) -> DeltaTable {
    let url = ensure_table_uri(path).unwrap();
    let t = deltalake::open_table(url).await.unwrap();
    let b = RecordBatch::try_new(
        ts_arrow_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(
                TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ) as ArrayRef,
        ],
    )
    .unwrap();
    t.write(vec![b])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

// ------------------------------------------------------- what a Trino OPTIMIZE leaves

/// Rewrite the parquet files added by commit `version` at millisecond precision, the way
/// Trino's `OPTIMIZE` writes them.
fn demote_to_millis(table_path: &str, version: u64) {
    // No timezone at all, which is the sharp part: Delta's `timestamp` is UTC-adjusted by
    // definition, so an absent zone must read as UTC and never as local.
    retype_file_timestamps(
        table_path,
        version,
        DataType::Timestamp(TimeUnit::Millisecond, None),
    )
}

/// Retype every timestamp column of the parquet files added by commit `version`, and
/// correct each `Add`'s recorded size — the reader is told the size from the log rather
/// than probing for it, so a stale one fails the read outright.
fn retype_file_timestamps(table_path: &str, version: u64, to: DataType) {
    use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use deltalake::parquet::arrow::ArrowWriter;
    use deltalake::parquet::file::properties::WriterProperties;

    let commit = format!("{table_path}/_delta_log/{version:020}.json");
    let mut lines: Vec<String> = Vec::new();

    for line in std::fs::read_to_string(&commit).unwrap().lines() {
        let mut action: serde_json::Value = serde_json::from_str(line).unwrap();
        let Some(add) = action.get_mut("add") else {
            lines.push(line.to_string());
            continue;
        };
        let rel = add["path"].as_str().unwrap().to_string();
        let file = format!("{table_path}/{rel}");

        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();

        let demoted: SchemaRef = Arc::new(Schema::new(
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Timestamp(_, _) => {
                        Arc::new(Field::new(f.name(), to.clone(), f.is_nullable()))
                    }
                    _ => f.clone(),
                })
                .collect::<Vec<_>>(),
        ));
        let recast: Vec<RecordBatch> = batches
            .iter()
            .map(|b| {
                let cols = b
                    .columns()
                    .iter()
                    .zip(demoted.fields())
                    .map(|(c, f)| deltalake::arrow::compute::cast(c, f.data_type()).unwrap())
                    .collect();
                RecordBatch::try_new(demoted.clone(), cols).unwrap()
            })
            .collect();

        let props = WriterProperties::builder()
            .set_created_by("parquet-mr-trino version 480-e.5".into())
            .build();
        let out = std::fs::File::create(&file).unwrap();
        let mut w = ArrowWriter::try_new(out, demoted.clone(), Some(props)).unwrap();
        for b in &recast {
            w.write(b).unwrap();
        }
        w.close().unwrap();

        add["size"] = serde_json::json!(std::fs::metadata(&file).unwrap().len());
        lines.push(serde_json::to_string(&action).unwrap());
    }

    std::fs::write(&commit, format!("{}\n", lines.join("\n"))).unwrap();
}

/// Replace the checkpoint at `version` with one shaped the way Trino writes it: carrying a
/// `stats_parsed` struct typed after the table's columns, so a `timestamp` column becomes
/// `timestamp[ms]` — a physical type the Delta protocol does not have.
fn trinoize_checkpoint(table_path: &str, version: u64) {
    use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use deltalake::parquet::arrow::ArrowWriter;
    use deltalake::parquet::file::properties::WriterProperties;

    let cp = format!("{table_path}/_delta_log/{version:020}.checkpoint.parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&cp).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();

    let value_fields: Fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "kafka_timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]
    .into();
    let count_fields: Fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("kafka_timestamp", DataType::Int64, true),
    ]
    .into();
    let stats_fields: Fields = vec![
        Field::new("numRecords", DataType::Int64, true),
        Field::new("minValues", DataType::Struct(value_fields.clone()), true),
        Field::new("maxValues", DataType::Struct(value_fields.clone()), true),
        Field::new("nullCount", DataType::Struct(count_fields.clone()), true),
    ]
    .into();

    let mut out = Vec::new();
    for b in &batches {
        let add_idx = b.schema().index_of("add").unwrap();
        let add: &StructArray = b.column(add_idx).as_struct();
        let stats_idx = add
            .fields()
            .iter()
            .position(|f| f.name() == "stats")
            .unwrap();
        let stats = add.column(stats_idx).as_string::<i32>();

        let n = add.len();
        let mut num_records = Vec::with_capacity(n);
        let (mut min_id, mut max_id) = (Vec::new(), Vec::new());
        let (mut min_ts, mut max_ts) = (Vec::new(), Vec::new());
        let (mut nc_id, mut nc_ts) = (Vec::new(), Vec::new());

        for i in 0..n {
            let parsed: Option<serde_json::Value> =
                (!stats.is_null(i)).then(|| serde_json::from_str(stats.value(i)).unwrap());
            let at = |side: &str, col: &str| -> Option<serde_json::Value> {
                parsed
                    .as_ref()
                    .and_then(|p| p.get(side))
                    .and_then(|m| m.get(col))
                    .cloned()
            };
            let millis = |v: Option<serde_json::Value>| -> Option<i64> {
                v.and_then(|v| v.as_str().map(str::to_string))
                    .and_then(|s| {
                        delta_delta_ingest::stats::parse_timestamp_micros(&s).map(|us| us / 1000)
                    })
            };
            num_records.push(parsed.as_ref().and_then(|p| p["numRecords"].as_i64()));
            min_id.push(at("minValues", "id").and_then(|v| v.as_i64()));
            max_id.push(at("maxValues", "id").and_then(|v| v.as_i64()));
            nc_id.push(at("nullCount", "id").and_then(|v| v.as_i64()));
            nc_ts.push(at("nullCount", "kafka_timestamp").and_then(|v| v.as_i64()));
            min_ts.push(millis(at("minValues", "kafka_timestamp")));
            max_ts.push(millis(at("maxValues", "kafka_timestamp")));
        }

        let side = |ids: Vec<Option<i64>>, ts: Vec<Option<i64>>| -> ArrayRef {
            Arc::new(StructArray::new(
                value_fields.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)) as ArrayRef,
                    Arc::new(TimestampMillisecondArray::from(ts)) as ArrayRef,
                ],
                None,
            ))
        };
        let stats_parsed: ArrayRef = Arc::new(StructArray::new(
            stats_fields.clone(),
            vec![
                Arc::new(Int64Array::from(num_records)) as ArrayRef,
                side(min_id, min_ts),
                side(max_id, max_ts),
                Arc::new(StructArray::new(
                    count_fields.clone(),
                    vec![
                        Arc::new(Int64Array::from(nc_id)) as ArrayRef,
                        Arc::new(Int64Array::from(nc_ts)) as ArrayRef,
                    ],
                    None,
                )) as ArrayRef,
            ],
            add.nulls().cloned(),
        ));

        let mut fields: Vec<Arc<Field>> = add.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(
            "stats_parsed",
            DataType::Struct(stats_fields.clone()),
            true,
        )));
        let mut columns: Vec<ArrayRef> = add.columns().to_vec();
        columns.push(stats_parsed);
        let new_add = StructArray::new(fields.into(), columns, add.nulls().cloned());

        let mut top: Vec<Arc<Field>> = b.schema().fields().iter().cloned().collect();
        top[add_idx] = Arc::new(Field::new("add", new_add.data_type().clone(), true));
        let mut cols = b.columns().to_vec();
        cols[add_idx] = Arc::new(new_add) as ArrayRef;
        out.push(RecordBatch::try_new(Arc::new(Schema::new(top)), cols).unwrap());
    }

    let props = WriterProperties::builder()
        .set_created_by("parquet-mr-trino version 480-e.5".into())
        .build();
    let f = std::fs::File::create(&cp).unwrap();
    let mut w = ArrowWriter::try_new(f, out[0].schema(), Some(props)).unwrap();
    for b in &out {
        w.write(b).unwrap();
    }
    w.close().unwrap();
}

// -------------------------------------------------------------------------- readers

async fn read_timestamps(path: &str) -> Vec<(i64, i64)> {
    use futures::TryStreamExt;
    let url = ensure_table_uri(path).unwrap();
    let t = Storage::default().open(url.as_str()).await.unwrap();
    let (_t, stream) = t.scan_table().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column(b.schema().index_of("id").unwrap())
            .as_primitive::<deltalake::arrow::datatypes::Int64Type>();
        let ts = b.column(b.schema().index_of("kafka_timestamp").unwrap());
        assert_eq!(
            ts.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "a scan must hand back the table's declared precision, whatever the file holds"
        );
        let ts = ts.as_primitive::<TimestampMicrosecondType>();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), ts.value(i)));
        }
    }
    out.sort_unstable();
    out
}

/// Append through a handle that can survive a foreign checkpoint, which a plain
/// `open_table` cannot once one is the newest thing in the log.
async fn append_healed(path: &str, rows: &[(i64, i64)]) {
    let t = Storage::default().open(path).await.unwrap();
    let b = RecordBatch::try_new(
        ts_arrow_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(
                TimestampMicrosecondArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ) as ArrayRef,
        ],
    )
    .unwrap();
    t.write(vec![b])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
}

// ------------------------------------------------------------- part 1: the checkpoint

#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_another_engine_wrote_does_not_decide_whether_the_table_opens() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let path = root.join("t").to_str().unwrap().to_string();

    create_ts_table(&path).await;
    append_ts(&path, &[(1, T0), (2, T1)]).await;
    let t = append_ts(&path, &[(3, T2)]).await;
    deltalake::checkpoints::create_checkpoint(&t, None)
        .await
        .unwrap();
    trinoize_checkpoint(&path, 2);

    // What production saw: the kernel validates the checkpoint's physical types and refuses
    // a table whose own schema is perfectly correct.
    let raw = deltalake::open_table(ensure_table_uri(&path).unwrap()).await;
    let raw = raw.expect_err("the fixture must reproduce the failure it is guarding against");
    assert!(
        raw.to_string().contains("Invalid data type for Delta Lake"),
        "got: {raw}"
    );

    // And what this tool does about it: the checkpoint is a derived cache of the commits,
    // so it replays them instead.
    let opened = Storage::default().open(&path).await.expect("must open");
    assert_eq!(opened.version(), Some(2));
    assert_eq!(
        read_timestamps(&path).await,
        vec![(1, T0), (2, T1), (3, T2)],
        "and every row is there, at the declared precision"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_watermark_is_the_same_with_that_checkpoint_and_without_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let plain = root.join("plain").to_str().unwrap().to_string();
    let trino = root.join("trino").to_str().unwrap().to_string();

    for path in [&plain, &trino] {
        create_ts_table(path).await;
        append_ts(path, &[(1, T0), (2, T1)]).await;
        append_ts(path, &[(3, T2)]).await;
    }
    let t = Storage::default().open(&trino).await.unwrap();
    deltalake::checkpoints::create_checkpoint(&t, None)
        .await
        .unwrap();
    trinoize_checkpoint(&trino, 2);

    let of = |p: &str| {
        let p = p.to_string();
        async move {
            let t = Storage::default().open(&p).await.unwrap();
            let d = Dedup::read(&t, "kafka_timestamp", Some("id"))
                .await
                .unwrap();
            let w = d.watermark().expect("the table is not empty").clone();
            let w = w.as_primitive::<TimestampMicrosecondType>();
            w.value(0)
        }
    };

    assert_eq!(of(&plain).await, T2);
    assert_eq!(
        of(&trino).await,
        of(&plain).await,
        "ignoring the checkpoint must not change a single answer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_checkpoint_below_the_newest_one_does_not_stop_the_stream() {
    // `open` steps over a checkpoint it cannot parse — but only the one it opens at. A
    // table that has been compacted more than once has *older* checkpoints too, and the
    // newest may be perfectly readable while an earlier one is not. Such a table opens, its
    // watermark reads, its preflight passes, and then a batch reaches a version the old
    // checkpoint covers and the stream stops. It stops on every batch after that, because
    // reopening finds nothing wrong with the version it opens at.
    //
    // This is what production hit, and it is worth being exact about where: not in the
    // merge, and not in any read of a data file, but in asking the log what the schema was
    // at a version — a question whose answer is in `metaData` and needs no files at all.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_ts_table(&source).await;
    create_ts_table(&target).await;

    // A compaction at v2 leaves a checkpoint this build cannot parse.
    append_ts(&source, &[(1, T0)]).await;
    let t = append_ts(&source, &[(2, T1)]).await;
    deltalake::checkpoints::create_checkpoint(&t, None)
        .await
        .unwrap();
    trinoize_checkpoint(&source, 2);

    // Streaming continues, and a later checkpoint — written by anything that emits
    // microseconds — buries it. From here the table opens perfectly well.
    append_healed(&source, &[(3, T2)]).await;
    append_healed(&source, &[(4, T2)]).await;
    let t = Storage::default().open(&source).await.unwrap();
    deltalake::checkpoints::create_checkpoint(&t, None)
        .await
        .unwrap();
    deltalake::open_table(ensure_table_uri(&source).unwrap())
        .await
        .expect("the newest checkpoint is fine, so the table opens the ordinary way");

    // One file per batch, so the stream has to ask for the schema at each version in turn —
    // including the one the unreadable checkpoint is newest for.
    let mut cfg = common::pipeline_cfg("under-a-newer-checkpoint", &source, &target);
    cfg.max_files_per_batch = 1;
    let mut p = Pipeline::open(cfg).await.expect("the pipeline must open");
    p.run_until_caught_up()
        .await
        .expect("and must not stop at the version the old checkpoint covers");

    assert_eq!(
        read_timestamps(&target).await,
        vec![(1, T0), (2, T1), (3, T2), (4, T2)],
        "every commit must arrive, not just the ones above the buried checkpoint"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_log_that_no_longer_reaches_version_zero_is_refused_rather_than_half_read() {
    // The trap under the whole idea of "just replay the log". Once a checkpoint exists the
    // protocol permits deleting the commits below it, and Delta-Spark does so by default
    // after its log-retention window — on a table old enough to have been compacted, which
    // is exactly this one.
    //
    // The kernel does not catch it. It requires a log segment to be contiguous and a
    // checkpoint to have no gap after it, but *not* that a checkpoint-less segment start at
    // version 0. Hand it the surviving tail with the checkpoint hidden and it builds a
    // perfectly valid-looking snapshot of that tail: the table opens, the older rows are
    // gone, and nothing says so. Worse, the handle is the one the sink commits through, so
    // delta-rs would eventually write that truncated file set out as a real checkpoint and
    // make the loss everybody's.
    //
    // Half a table is not an acceptable answer to an unreadable checkpoint. Refusing is.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let path = root.join("t").to_str().unwrap().to_string();

    create_ts_table(&path).await;
    append_ts(&path, &[(1, T0)]).await;
    append_ts(&path, &[(2, T1)]).await;
    let t = append_ts(&path, &[(3, T2)]).await;
    deltalake::checkpoints::create_checkpoint(&t, None)
        .await
        .unwrap();
    trinoize_checkpoint(&path, 3);

    // Log retention removes what the checkpoint stands in for. This is legal.
    for v in 0..=2u64 {
        std::fs::remove_file(format!("{path}/_delta_log/{v:020}.json")).unwrap();
    }

    let e = Storage::default()
        .open(&path)
        .await
        .expect_err("a table that can only be read in part must not open at all")
        .to_string();
    assert!(
        e.contains("version 0"),
        "the message must name why replaying was not an option: {e}"
    );
    assert!(
        e.contains("Invalid data type for Delta Lake"),
        "and still carry the original cause: {e}"
    );
    assert!(
        e.contains("log retention"),
        "and point at what to do about it: {e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_opened_that_way_is_still_fully_writable() {
    // The handle `open` hands back is not read-only: it is the one the sink commits
    // through, and delta-rs writes a checkpoint of its own as a post-commit hook. So the
    // filtered view of the store has to carry every write untouched, or a target that
    // merely *had* a Trino checkpoint would stop accepting data — a worse failure than the
    // one this replaces.
    //
    // The hook itself is invoked explicitly at the end rather than by interval. `ddi` folds
    // a batch of source commits into a single target commit, so which target version the
    // interval lands on is not this test's to predict, and a test that quietly stops
    // exercising the thing it names is worse than no test.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_ts_table(&source).await;
    create_ts_table(&target).await;

    // Give the target a checkpoint of Trino's making, so it can only be opened by replay.
    let t = append_ts(&target, &[(0, T0)]).await;
    deltalake::checkpoints::create_checkpoint(&t, None)
        .await
        .unwrap();
    trinoize_checkpoint(&target, 1);
    assert!(
        deltalake::open_table(ensure_table_uri(&target).unwrap())
            .await
            .is_err(),
        "the fixture must leave the target unopenable the ordinary way"
    );

    for row in [(1i64, T1), (2, T2), (3, T2 + 1_000_000)] {
        append_ts(&source, &[row]).await;
    }
    let mut p = Pipeline::open(common::pipeline_cfg("writable", &source, &target))
        .await
        .expect("the target must open");
    p.run_until_caught_up().await.expect("and take writes");

    assert_eq!(
        read_timestamps(&target).await,
        vec![(0, T0), (1, T1), (2, T2), (3, T2 + 1_000_000)],
        "every row committed through the filtered store must be in the table"
    );

    // What the post-commit hook does, on whatever handle it was given — here, the filtered
    // one. It writes the checkpoint parquet and then *reads it back* to record its size,
    // which is the read a wrapper that hid checkpoints unconditionally would fail.
    let t = Storage::default().open(&target).await.unwrap();
    deltalake::checkpoints::create_checkpoint(&t, None)
        .await
        .expect("a checkpoint must be writable through the filtered store");

    // And that heals the table: a newer, well-typed checkpoint supersedes Trino's, so the
    // ordinary open works again and the slow path stops being needed.
    let healed = deltalake::open_table(ensure_table_uri(&target).unwrap())
        .await
        .expect("the fresh checkpoint must supersede the one that could not be read");
    assert_eq!(healed.version(), t.version());
}

// --------------------------------------------------------------- part 2: data files

#[tokio::test(flavor = "multi_thread")]
async fn a_file_written_at_milliseconds_reads_at_the_declared_precision() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_ts_table(&source).await;
    create_ts_table(&target).await;

    // One commit each way, so the batch spans both spellings of the same column.
    append_ts(&source, &[(1, T0), (2, T1)]).await;
    demote_to_millis(&source, 1);
    append_ts(&source, &[(3, T2)]).await;

    let mut cfg = common::pipeline_cfg("mixed", &source, &target);
    cfg.starting_version = 0;
    let mut p = Pipeline::open(cfg).await.expect("the pipeline must open");
    p.run_until_caught_up().await.expect("and it must run");

    assert_eq!(
        read_timestamps(&target).await,
        vec![(1, T0), (2, T1), (3, T2)],
        "no value may shift: reading milliseconds as microseconds is a 1000x error that \
         looks entirely plausible in a spot check"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_batch_spanning_two_writers_is_still_one_batch() {
    // This is the case that makes the read-side coercion load-bearing rather than merely
    // tidy, and it is worth being precise about why.
    //
    // With no `transform_sql`, mixed precision costs nothing: `SchemaCoercer` casts each
    // batch to the target separately, so two schemas in one `Vec<RecordBatch>` never meet.
    // A `transform_sql` does make them meet — `SqlTransform::apply` takes the *first*
    // batch's schema and registers the whole list against it, and DataFusion refuses a list
    // whose members disagree. One Trino-written file and one delta-rs-written file in the
    // same source commit range is then a hard stall, on a pipeline whose only crime is
    // having a SELECT in it.
    //
    // Reading both at the table's declared precision is what makes them one schema again.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_ts_table(&source).await;
    create_ts_table(&target).await;

    append_ts(&source, &[(1, T0), (2, T1)]).await;
    demote_to_millis(&source, 1);
    append_ts(&source, &[(3, T2)]).await;

    let mut cfg = common::pipeline_cfg("mixed-sql", &source, &target);
    cfg.transform_sql = Some("SELECT id, kafka_timestamp FROM source".into());
    let mut p = Pipeline::open(cfg).await.expect("the pipeline must open");
    let batches = p.run_until_caught_up().await.expect("and it must run");

    assert_eq!(
        batches, 1,
        "both files must arrive in one batch, or this pins nothing"
    );
    assert_eq!(
        read_timestamps(&target).await,
        vec![(1, T0), (2, T1), (3, T2)],
        "and the transform must see one schema, not two"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_timestamp_landing_in_a_numeric_column_carries_the_declared_unit() {
    // The sharpest form of the 1000x error, because nothing about it looks wrong. A target
    // that stores the instant as a BIGINT — an epoch column, which bronze layers are full
    // of — gets whatever unit the source column happened to be *read* at. Before this,
    // that was whichever engine last wrote the file: microseconds from delta-rs, and
    // milliseconds from anything Trino had compacted, into the same column, silently.
    //
    // Reading at the declared precision first is what makes the answer a property of the
    // table rather than of the writer.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_ts_table(&source).await;
    std::fs::create_dir_all(&target).unwrap();
    DeltaTable::try_from_url(ensure_table_uri(&target).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(vec![
            StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), false),
            StructField::new(
                "kafka_timestamp",
                DeltaDataType::Primitive(PrimitiveType::Long),
                true,
            ),
        ])
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();

    append_ts(&source, &[(1, T0)]).await;
    demote_to_millis(&source, 1);

    let mut p = Pipeline::open(common::pipeline_cfg("epoch", &source, &target))
        .await
        .expect("the pipeline must open");
    p.run_until_caught_up().await.expect("and it must run");

    use futures::TryStreamExt;
    let t = Storage::default().open(&target).await.unwrap();
    let (_t, stream) = t.scan_table().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let stored = batches[0]
        .column(batches[0].schema().index_of("kafka_timestamp").unwrap())
        .as_primitive::<deltalake::arrow::datatypes::Int64Type>()
        .value(0);

    assert_eq!(
        stored,
        T0,
        "microseconds, because that is what the table declares the source column to be — \
         not {} milliseconds, which is what the file happens to hold",
        T0 / 1000
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_two_spellings_order_against_each_other_when_deduplicating() {
    // The scenario this tool exists for: a batch rebuild has overwritten the target, and
    // `ddi` reads `max(kafka_timestamp)` out of it to find what is already covered. Here the
    // rebuild's output has since been compacted by Trino, so that maximum lives in a
    // millisecond file.
    //
    // A silent unit mismatch would read T2 as a thousandth of itself — 1970 — and nothing
    // would be suppressed at all: every row the rebuild already wrote would be appended a
    // second time, including a duplicate of the very row the watermark came from. The
    // assertion is therefore run twice, against a target written each way, because the
    // property is not "some particular set of rows" but "the precision on disk makes no
    // difference".
    async fn covered_rows(root: &std::path::Path, name: &str, demote: bool) -> Vec<(i64, i64)> {
        let source = root
            .join(format!("{name}-source"))
            .to_str()
            .unwrap()
            .to_string();
        let target = root
            .join(format!("{name}-target"))
            .to_str()
            .unwrap()
            .to_string();
        create_ts_table(&source).await;
        create_ts_table(&target).await;

        append_ts(&target, &[(3, T2)]).await;
        if demote {
            demote_to_millis(&target, 1);
        }
        append_ts(&source, &[(1, T0), (2, T1), (3, T2)]).await;

        let mut cfg = common::pipeline_cfg(name, &source, &target);
        cfg.dedup_timestamp = Some("kafka_timestamp".into());
        cfg.dedup_key = Some("id".into());
        let mut p = Pipeline::open(cfg).await.expect("the pipeline must open");
        p.run_until_caught_up().await.expect("and it must run");
        read_timestamps(&target).await
    }

    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();

    let micros = covered_rows(&root, "us", false).await;
    let millis = covered_rows(&root, "ms", true).await;

    assert_eq!(
        micros,
        vec![(3, T2)],
        "the target already covers up to T2, so none of the three rows is new"
    );
    assert_eq!(
        millis, micros,
        "and a target compacted to milliseconds must answer identically — reading its \
         watermark as microseconds would put it in 1970, suppress nothing, and duplicate \
         key 3"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn upserting_across_the_two_spellings_collapses_on_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_ts_table(&source).await;
    create_ts_table(&target).await;

    // The target holds an older version of key 1, at milliseconds.
    append_ts(&target, &[(1, T0)]).await;
    demote_to_millis(&target, 1);

    // The source restates it, plus a new key.
    append_ts(&source, &[(1, T2), (2, T1)]).await;

    let mut cfg = common::pipeline_cfg("upsert-mixed", &source, &target);
    cfg.dedup_timestamp = Some("kafka_timestamp".into());
    cfg.upsert_key = Some("id".into());
    cfg.write_mode = delta_delta_ingest::config::WriteMode::Upsert;
    let mut p = Pipeline::open(cfg).await.expect("the pipeline must open");
    p.run_until_caught_up().await.expect("and it must run");

    assert_eq!(
        read_timestamps(&target).await,
        vec![(1, T2), (2, T1)],
        "key 1 must be replaced rather than duplicated, which needs the sequence column to \
         order across the two precisions"
    );
}

// ------------------------------------------------------ what must still be refused

#[tokio::test(flavor = "multi_thread")]
async fn a_column_that_is_genuinely_the_wrong_type_still_fails() {
    // The point of this change is to narrow what is refused, not to stop refusing. A file
    // holding text where the schema says `timestamp` is not a precision difference and
    // cannot be widened into one, so it fails exactly as it always has.
    use delta_delta_ingest::schema::SchemaCoercer;

    let file = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("kafka_timestamp", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        file,
        vec![
            Arc::new(Int64Array::from(vec![1i64])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("not a time")])) as ArrayRef,
        ],
    )
    .unwrap();

    // Reading it as declared leaves it alone — it is not a precision difference — so the
    // failure lands where it always did, in the coercer, with the wording it always had.
    let read = delta_delta_ingest::schema::read_as_declared(batch, &ts_arrow_schema())
        .expect("a wrong type is not this function's to refuse");
    assert_eq!(read.column(1).data_type(), &DataType::Utf8);

    let e = SchemaCoercer::new(ts_arrow_schema())
        .coerce(&read)
        .expect_err("but it must not reach the target")
        .to_string();
    assert!(e.contains("kafka_timestamp"), "got: {e}");
    assert!(e.contains("cannot cast"), "got: {e}");
    assert!(e.contains("without loss"), "got: {e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spark_written_nanosecond_file_still_flows_through_untouched() {
    // The regression this change could most easily have caused. Spark writes Delta
    // timestamps as INT96, and the parquet reader decodes INT96 as `Timestamp(ns)` whatever
    // the table declares — so on a Spark-written lakehouse *every* file arrives finer than
    // the schema. Those values are microsecond-resolution by construction and have always
    // been read by casting down in the coercer.
    //
    // Widening is the only claim this change makes. Refusing the other direction here would
    // have read as safety and behaved as an outage: every such pipeline stalled at its
    // current version, with no setting that lets it advance.
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();

    create_ts_table(&source).await;
    create_ts_table(&target).await;
    append_ts(&source, &[(1, T0), (2, T1)]).await;
    retype_file_timestamps(&source, 1, DataType::Timestamp(TimeUnit::Nanosecond, None));

    let mut p = Pipeline::open(common::pipeline_cfg("ns", &source, &target))
        .await
        .expect("the pipeline must open");
    p.run_until_caught_up().await.expect("and it must run");

    assert_eq!(
        read_timestamps(&target).await,
        vec![(1, T0), (2, T1)],
        "a finer file is narrowed by the coercer exactly as it always was"
    );
}

#[test]
fn a_finer_file_is_handed_on_rather_than_refused() {
    // The unit-level statement of the same rule: `read_as_declared` does not narrow, and
    // does not refuse a narrowing either. It leaves it for the coercer.
    use deltalake::arrow::array::TimestampNanosecondArray;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "kafka_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )])),
        vec![Arc::new(TimestampNanosecondArray::from(vec![T0 * 1000])) as ArrayRef],
    )
    .unwrap();

    let out = delta_delta_ingest::schema::read_as_declared(batch, &ts_arrow_schema())
        .expect("a finer file is not this function's to refuse");
    assert_eq!(
        out.column(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        "and not its to convert either — it arrives at the coercer as it left the file"
    );
}

#[test]
fn a_file_that_already_agrees_is_handed_straight_back() {
    // The common path, and it has to stay free — no cast, no copy, no allocation.
    let s = ts_arrow_schema();
    let batch = RecordBatch::try_new(
        s.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![T0]).with_timezone("UTC")) as ArrayRef,
        ],
    )
    .unwrap();
    // On the schema as well as the column: cloning every column into a freshly built batch
    // would leave the column pointers equal and still have done all the work, so the schema
    // is the one that actually pins the early return.
    let schema_before = batch.schema();
    let column_before = batch.column(1).clone();
    let after = delta_delta_ingest::schema::read_as_declared(batch, &s).unwrap();
    assert!(
        Arc::ptr_eq(&schema_before, &after.schema()),
        "an agreeing batch must not be rebuilt"
    );
    assert!(
        Arc::ptr_eq(&column_before, after.column(1)),
        "and no buffer may be copied"
    );
}
