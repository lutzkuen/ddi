//! Reading the target's watermark without reading the target.
//!
//! `Dedup::read` answers two small questions — the highest timestamp, and the keys tied at
//! it — about a table that is not small. It used to answer them by collecting every column
//! of every row into memory, which cost gigabytes on a silver table carrying JSON payloads,
//! was paid again on every restart, and grew until the pipeline could no longer start.
//!
//! It is now a streaming pass over two columns, which means the running maximum is carried
//! across files rather than computed at the end. That is the part with edges: a maximum in
//! the *first* file, ties spread over several, and a later file that beats everything
//! gathered so far.

mod common;

use std::sync::Arc;

use delta_delta_ingest::dedup::Dedup;
use deltalake::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaTable};

/// A target shaped like a real one: two columns that matter, and one fat payload that must
/// never be read.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        // Nullable, because a target may allow it and `max` has to ignore it.
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("payload", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]))
}

fn batch(rows: &[(&str, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            // Deliberately bulky: if this is ever read again, it should be felt.
            Arc::new(StringArray::from(
                rows.iter().map(|_| "x".repeat(4096)).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|_| 1i64).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

/// A target built one commit — and therefore one file — per call.
async fn target(files: &[&[(&str, Option<i64>)]]) -> (tempfile::TempDir, DeltaTable) {
    let dir = tempfile::tempdir().unwrap();
    let path = std::fs::canonicalize(dir.path())
        .unwrap()
        .join("orders")
        .to_str()
        .unwrap()
        .to_string();

    let delta: StructType = schema().as_ref().try_into_kernel().unwrap();
    DeltaTable::try_from_url(ensure_table_uri(&path).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();

    for rows in files {
        open_table(ensure_table_uri(&path).unwrap())
            .await
            .unwrap()
            .write(vec![batch(rows)])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }
    let t = open_table(ensure_table_uri(&path).unwrap()).await.unwrap();
    (dir, t)
}

/// Which of `rows` survive the filter — the observable contract, rather than the internals.
fn kept(d: &Dedup, rows: &[(&str, Option<i64>)]) -> Vec<String> {
    let out = d.apply(batch(rows)).expect("filter should apply");
    let ids = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..out.num_rows())
        .map(|i| ids.value(i).to_string())
        .collect()
}

#[tokio::test]
async fn the_highest_value_wins_even_when_it_is_not_in_the_last_file() {
    // Streaming means the answer is carried forward rather than computed at the end, so a
    // maximum that arrives early has to survive everything after it.
    let (_d, t) = target(&[
        &[("a", Some(100)), ("b", Some(200))],
        &[("c", Some(5))],
        &[("d", Some(7))],
    ])
    .await;

    let d = Dedup::read(&t, "_timestamp", Some("order_id"))
        .await
        .unwrap();
    assert!(d.watermark_is_known());
    assert_eq!(
        kept(&d, &[("x", Some(150)), ("y", Some(300))]),
        vec!["y"],
        "the watermark is 200, from the first file"
    );
}

#[tokio::test]
async fn keys_tied_at_the_watermark_are_gathered_across_files() {
    // The tie set is what a bare `>` would drop, and it does not respect file boundaries.
    let (_d, t) = target(&[
        &[("a", Some(200))],
        &[("b", Some(200))],
        &[("c", Some(100))],
    ])
    .await;

    let d = Dedup::read(&t, "_timestamp", Some("order_id"))
        .await
        .unwrap();
    assert_eq!(d.boundary_key_count(), 2, "both a and b sit at 200");
    assert_eq!(
        kept(&d, &[("a", Some(200)), ("b", Some(200)), ("z", Some(200))]),
        vec!["z"],
        "a and b are already stored at 200; z shares the instant but is new"
    );
}

#[tokio::test]
async fn a_later_higher_value_discards_the_keys_gathered_before_it() {
    // The running maximum has to invalidate the tie set it had collected, or keys from an
    // older instant would be treated as if they sat at the watermark.
    let (_d, t) = target(&[&[("a", Some(100))], &[("b", Some(200))]]).await;

    let d = Dedup::read(&t, "_timestamp", Some("order_id"))
        .await
        .unwrap();
    assert_eq!(d.boundary_key_count(), 1, "only b is at the watermark");
    assert_eq!(
        kept(&d, &[("a", Some(200)), ("b", Some(200))]),
        vec!["a"],
        "a is not stored at 200, so it is genuinely new there; b is"
    );
}

#[tokio::test]
async fn a_null_timestamp_in_the_target_does_not_become_the_watermark() {
    // `max` ignores nulls, as the SQL this replaced did.
    let (_d, t) = target(&[&[("a", Some(100)), ("b", None)]]).await;

    let d = Dedup::read(&t, "_timestamp", Some("order_id"))
        .await
        .unwrap();
    assert!(d.watermark_is_known());
    assert_eq!(kept(&d, &[("x", Some(50)), ("y", Some(150))]), vec!["y"]);
}

#[tokio::test]
async fn an_empty_target_covers_nothing() {
    let (_d, t) = target(&[]).await;
    let d = Dedup::read(&t, "_timestamp", Some("order_id"))
        .await
        .unwrap();
    assert!(
        d.is_inert(),
        "nothing has been written, so nothing is covered"
    );
    assert_eq!(kept(&d, &[("x", Some(1))]), vec!["x"]);
}

#[tokio::test]
async fn a_column_serving_as_both_sequence_and_key_is_read_once() {
    // A monotonic id is a legitimate choice for both. Naming it twice in the projection is
    // rejected by the scan, which is how this first surfaced.
    let (_d, t) = target(&[&[("a", Some(100)), ("b", Some(200))]]).await;
    let d = Dedup::read(&t, "_timestamp", Some("_timestamp"))
        .await
        .expect("the same column may be both");
    assert!(d.watermark_is_known());
}

#[tokio::test]
async fn a_missing_column_names_what_the_target_does_have() {
    let (_d, t) = target(&[&[("a", Some(1))]]).await;
    let e = Dedup::read(&t, "nope", None).await.unwrap_err().to_string();
    assert!(e.contains("nope"), "got: {e}");
    assert!(e.contains("order_id"), "should list the real columns: {e}");
}
