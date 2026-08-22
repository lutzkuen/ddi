//! The startup uniqueness check, at a ceiling too small to hold the target's keys.
//!
//! Its own test binary because it installs a process-wide spill budget, and because the one
//! property worth proving here is negative: however many passes the check takes, **it writes
//! nothing to a temporary directory**. The check it replaced was a grouped aggregate that
//! spilled the whole key space, and doing that in a container is what evicted a pod.
//!
//! The ceilings here are absurdly small on purpose. [`Ceiling::MIN`] is sixteen megabytes,
//! which is close to two million keys — building a target that overflows a real ceiling would
//! be a benchmark, not a test. `Ceiling::exactly` is the seam that lets a two-hundred-row table
//! exercise the same code path a two-billion-row one would.

mod common;

use std::sync::{Arc, OnceLock};

use deltalake::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, DeltaTable};

use delta_delta_ingest::grain::{self, Ceiling, Grain};
use delta_delta_ingest::spill;

/// Tight enough that a spilling check would fail rather than merely be slow — which is what
/// makes "it wrote nothing" an assertion rather than a hope.
const SPILL_BUDGET: u64 = 2 * 1024 * 1024;

static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();

fn installed_spill() -> Arc<spill::Spill> {
    let dir = DIR.get_or_init(|| tempfile::tempdir().unwrap());
    let s = spill::Spill::resolve(dir.path().to_str(), Some(SPILL_BUDGET)).unwrap();
    let _ = spill::install(s);
    spill::current()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("n", DataType::Int64, false),
    ]))
}

fn columns() -> Vec<StructField> {
    vec![
        StructField::new("key", DeltaDataType::Primitive(PrimitiveType::String), true),
        StructField::new("n", DeltaDataType::Primitive(PrimitiveType::Long), false),
    ]
}

/// A table holding exactly `keys`, in three commits so the check has several files to stream.
async fn table_of(keys: &[Option<&str>]) -> (tempfile::TempDir, DeltaTable) {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let path = root.join("target");
    let uri = path.to_str().unwrap();

    let url = ensure_table_uri(uri).unwrap();
    DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(columns())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();

    for (i, chunk) in keys.chunks(keys.len().div_ceil(3).max(1)).enumerate() {
        let ks: Vec<Option<String>> = chunk.iter().map(|k| k.map(str::to_string)).collect();
        let ns: Vec<i64> = (0..chunk.len() as i64)
            .map(|j| j + i as i64 * 1000)
            .collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(ks)) as ArrayRef,
                Arc::new(Int64Array::from(ns)) as ArrayRef,
            ],
        )
        .unwrap();
        let url = ensure_table_uri(uri).unwrap();
        deltalake::open_table(url)
            .await
            .unwrap()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }
    let url = ensure_table_uri(uri).unwrap();
    let t = deltalake::open_table(url).await.unwrap();
    (dir, t)
}

fn distinct(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("order-{i:08}")).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_uniqueness_check_writes_nothing_to_the_temporary_directory() {
    // The headline property, and the whole reason this module exists. The spill budget is two
    // megabytes and the ceiling holds fourteen keys at a time, so a check that spilled at all
    // would be visible here — and one that spilled like the grouped aggregate it replaced
    // would fail outright.
    let installed = installed_spill();
    let keys = distinct(600);
    let (_d, t) = table_of(&keys.iter().map(|k| Some(k.as_str())).collect::<Vec<_>>()).await;

    let before = installed.used_bytes();
    let outcome = grain::check(&t, "key", Ceiling::exactly(128), 3)
        .await
        .expect("a bounded check answers rather than running out");

    match outcome {
        Grain::Unique { rows, passes } => {
            assert_eq!(rows, Some(600), "the live row count comes from the log");
            assert!(
                passes > 1,
                "a ceiling of fourteen keys cannot read six hundred in one pass"
            );
        }
        other => panic!("six hundred distinct keys are unique: {other:?}"),
    }
    assert_eq!(
        installed.used_bytes(),
        before,
        "the check must not spill — at any target size, under any configuration"
    );
    assert_eq!(installed.active_files(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_target_that_already_holds_a_key_twice_is_refused_even_when_the_scan_is_split() {
    // Equal keys hash equally, so a duplicate pair is congruent modulo anything and lands in
    // the same class however finely the key space is divided. That is the whole correctness
    // argument for splitting, exercised against a real table.
    installed_spill();
    let mut keys: Vec<String> = distinct(400);
    keys.push("order-00000042".into()); // already present, so this is the second copy
    let (_d, t) = table_of(&keys.iter().map(|k| Some(k.as_str())).collect::<Vec<_>>()).await;

    match grain::check(&t, "key", Ceiling::exactly(128), 3)
        .await
        .unwrap()
    {
        Grain::Duplicated { examples, passes } => {
            assert!(passes > 1, "this ceiling forces more than one pass");
            assert_eq!(examples.len(), 1);
            assert_eq!(examples[0].key, "order-00000042");
            assert_eq!(examples[0].rows, 2);
        }
        other => panic!("the duplicate must be found however the space is split: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_generous_ceiling_reads_the_target_exactly_once() {
    // The ordinary case, and the one that matters for cost: with room for the key space this
    // is a single projected scan with no plan, no repartition and no aggregate — strictly
    // cheaper than the `GROUP BY` it replaced.
    installed_spill();
    let keys = distinct(300);
    let (_d, t) = table_of(&keys.iter().map(|k| Some(k.as_str())).collect::<Vec<_>>()).await;

    match grain::check(&t, "key", Ceiling::exactly(Ceiling::DEFAULT), 3)
        .await
        .unwrap()
    {
        Grain::Unique { passes, .. } => assert_eq!(passes, 1),
        other => panic!("expected a clean target: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_null_key_that_appears_twice_is_a_duplicate_just_as_group_by_made_it() {
    // Pinning the semantics the aggregate had. `GROUP BY` puts two NULLs in one group, and the
    // row encoding this replaced it with encodes NULL distinctly and consistently — so the
    // answer does not change. A target keyed on a nullable column that has collected two NULLs
    // is exactly as unmergeable as one that has collected two of anything else.
    installed_spill();
    let (_d, t) = table_of(&[Some("a"), None, Some("b"), None, Some("c")]).await;

    match grain::check(&t, "key", Ceiling::exactly(Ceiling::DEFAULT), 3)
        .await
        .unwrap()
    {
        Grain::Duplicated { examples, .. } => {
            assert_eq!(examples.len(), 1);
            assert_eq!(
                examples[0].key, "NULL",
                "shown the way the aggregate showed it"
            );
            assert_eq!(examples[0].rows, 2);
        }
        other => panic!("two NULL keys are two rows under one key: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_keys_that_collide_on_the_hash_are_not_reported_as_duplicates() {
    // The exactness claim. Sixty-four bits is not enough on its own — at two billion keys the
    // birthday bound puts about a tenth of a collision in every run — so a pass nominates
    // rather than answers, and a second pass resolves the nominees against the real keys.
    // Refusing a correct table is the worse of the two errors this check can make, so it is
    // the one pinned with a hash that collides on purpose.
    installed_spill();
    let (_d, t) = table_of(&[Some("a"), Some("b"), Some("c"), Some("d")]).await;

    let everything_collides = |_: &[u8]| 42u64;
    match grain::check_with(
        &t,
        "key",
        Ceiling::exactly(Ceiling::DEFAULT),
        3,
        &everything_collides,
    )
    .await
    .unwrap()
    {
        Grain::Unique { .. } => {}
        Grain::Duplicated { examples, .. } => {
            panic!("four distinct keys sharing a hash are not duplicates, but got {examples:?}")
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_is_still_found_when_the_hash_is_deliberately_terrible() {
    // The other half of the same seam: a hash that tells the check nothing must make it
    // slower, never wrong.
    installed_spill();
    let (_d, t) = table_of(&[Some("a"), Some("b"), Some("a"), Some("d")]).await;

    let everything_collides = |_: &[u8]| 42u64;
    match grain::check_with(
        &t,
        "key",
        Ceiling::exactly(Ceiling::DEFAULT),
        3,
        &everything_collides,
    )
    .await
    .unwrap()
    {
        Grain::Duplicated { examples, .. } => {
            assert_eq!(examples.len(), 1);
            assert_eq!(examples[0].key, "a");
            assert_eq!(examples[0].rows, 2);
        }
        other => panic!("a real duplicate survives a useless hash: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_check_that_would_need_more_than_max_passes_is_refused_before_reading_a_byte() {
    // A cost, not a failure — so it is refused with the two ways out rather than run silently
    // for hours. Nothing is read: the row count comes from the target's own log.
    installed_spill();
    let keys = distinct(600);
    let (_d, t) = table_of(&keys.iter().map(|k| Some(k.as_str())).collect::<Vec<_>>()).await;

    // One hash per pass against six hundred rows is six hundred and six classes, which is past
    // the guard rail. The arithmetic is done from the log alone, so the refusal costs one
    // metadata read and no data file is opened.
    let e = grain::check(&t, "key", Ceiling::exactly(8), 3)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("passes"), "{e}");
    assert!(e.contains("runtime.max_grain_check_memory"), "{e}");
    assert!(e.contains("Nothing has been read"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_target_holds_one_row_per_key() {
    installed_spill();
    let (_d, t) = table_of(&[]).await;
    match grain::check(&t, "key", Ceiling::exactly(Ceiling::DEFAULT), 3)
        .await
        .unwrap()
    {
        Grain::Unique { rows, passes } => {
            assert_eq!(rows, Some(0));
            assert_eq!(passes, 1);
        }
        other => panic!("an empty target cannot hold a key twice: {other:?}"),
    }
}
