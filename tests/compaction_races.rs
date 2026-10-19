//! How delta-rs 1.1.2's OPTIMIZE breaks when two compactors meet, pinned as it happens today.
//!
//! These are characterisation tests for the gap `src/compact.rs` has to close: they assert the
//! behaviour of the library, not of ddi, so a delta-rs bump that fixes a hole fails them on
//! purpose and the matching guard can be revisited.

mod common;

use common::*;
use deltalake::DeltaTable;

async fn small_files_table(path: &str, commits: i64) {
    create_table(path).await;
    for i in 0..commits {
        append(path, &[i * 10 + 1, i * 10 + 2]).await;
    }
}

fn root(dir: &tempfile::TempDir) -> String {
    dir.path().join("t").to_str().unwrap().to_string()
}

async fn optimize(
    t: DeltaTable,
) -> Result<(DeltaTable, deltalake::operations::optimize::Metrics), deltalake::DeltaTableError> {
    t.optimize().await
}

/// Both compactors plan from the same snapshot and rewrite the same files into different
/// outputs. delta-rs does not see the second as a conflict, so every row is stored twice.
#[tokio::test]
async fn two_compactions_of_the_same_files_both_commit_and_duplicate_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = root(&dir);
    small_files_table(&path, 4).await;
    let want = read_ids(&path).await;
    assert_eq!(want.len(), 8);

    // Two processes, each holding the same view of the table.
    let a = open(&path).await;
    let b = open(&path).await;

    let (a, ma) = optimize(a).await.expect("first compaction");
    assert_eq!(ma.num_files_removed, 4);
    let second = optimize(b).await;

    let got = read_ids(&path).await;
    match second {
        Ok(_) => {
            assert!(
                has_duplicates(&got),
                "the second compaction committed on top of the first; rows are now {got:?}"
            );
            assert_eq!(got.len(), 16, "every row is stored twice");
        }
        Err(e) => panic!(
            "delta-rs now rejects a second compaction of the same files ({e}); \
             the lease in compact.rs may be redundant for ddi-vs-foreign OPTIMIZE"
        ),
    }
    drop(a);
}

/// A compaction commit carries no `txn`: delta-rs forwards only `app_metadata`, so a compaction
/// cannot assert an app id, which is what would make two of them conflict.
#[tokio::test]
async fn optimize_drops_an_app_transaction() {
    use deltalake::kernel::transaction::CommitProperties;
    let dir = tempfile::tempdir().unwrap();
    let path = root(&dir);
    small_files_table(&path, 3).await;

    let t = open(&path).await;
    let props = CommitProperties::default()
        .with_application_transaction(deltalake::kernel::Transaction::new("ddi.compact.test", 1));
    let (t, _) = t.optimize().with_commit_properties(props).await.unwrap();

    assert_eq!(
        t.snapshot()
            .unwrap()
            .transaction_version(t.log_store().as_ref(), "ddi.compact.test")
            .await
            .unwrap(),
        None,
        "the app transaction was dropped"
    );
}

/// A ddi append that lands while a compaction of older files is running is not a conflict:
/// the append is blind, the compaction removes only files the append never read.
#[tokio::test]
async fn an_append_after_the_compactions_snapshot_does_not_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let path = root(&dir);
    small_files_table(&path, 4).await;

    let planned = open(&path).await;
    append(&path, &[1000, 1001]).await;
    let (_t, m) = optimize(planned)
        .await
        .expect("compaction beside an append");
    assert_eq!(m.num_files_removed, 4);

    let got = read_ids(&path).await;
    assert!(!has_duplicates(&got));
    assert_eq!(got.len(), 10);
}

// ---- the guard: exact against another engine's compaction ---------------------------------

use delta_delta_ingest::compact::guarded;

fn is_delete_delete(e: &deltalake::DeltaTableError) -> bool {
    format!("{e:?}").contains("ConcurrentDeleteDelete")
}

/// The failure above, closed: a compaction planned before another engine's compaction landed
/// is refused, whichever engine wrote the other one, and the rows stay as they were.
#[tokio::test]
async fn a_guarded_compaction_loses_to_one_that_landed_first() {
    let dir = tempfile::tempdir().unwrap();
    let path = root(&dir);
    small_files_table(&path, 4).await;

    let ours = guarded(&open(&path).await).await.unwrap();
    // "Trino", "Databricks": anything that commits a rewrite of the same files first.
    let (_t, m) = open(&path).await.optimize().await.unwrap();
    assert_eq!(m.num_files_removed, 4);

    let e = ours.optimize().await.expect_err("must not land");
    assert!(
        is_delete_delete(&e),
        "expected ConcurrentDeleteDelete, got {e:?}"
    );
    let got = read_ids(&path).await;
    assert!(!has_duplicates(&got));
    assert_eq!(got.len(), 8);
}

/// The guard costs a compaction nothing when the commits that landed meanwhile are unrelated:
/// ddi's own appends, a claim, a compaction of other files.
#[tokio::test]
async fn a_guarded_compaction_lands_beside_unrelated_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = root(&dir);
    small_files_table(&path, 4).await;

    let ours = guarded(&open(&path).await).await.unwrap();
    append(&path, &[900, 901]).await;
    append(&path, &[902]).await;
    let (t, m) = ours.optimize().await.expect("compaction beside appends");
    assert_eq!(m.num_files_removed, 4);

    let got = read_ids(&path).await;
    assert!(!has_duplicates(&got));
    assert_eq!(got.len(), 11);
    assert!(t.version().unwrap() > 6);
}

/// The rival's compaction is followed by an unrelated append, so the version ours first tries
/// is two ahead of what it planned on and the guard has two commits to read.
#[tokio::test]
async fn a_guarded_compaction_planned_before_a_compaction_and_an_append_still_loses() {
    let dir = tempfile::tempdir().unwrap();
    let path = root(&dir);
    small_files_table(&path, 4).await;

    let ours = guarded(&open(&path).await).await.unwrap();
    open(&path).await.optimize().await.unwrap();
    append(&path, &[700]).await;
    let e = ours.optimize().await.expect_err("must not land");
    assert!(is_delete_delete(&e), "{e:?}");
    assert_eq!(read_ids(&path).await.len(), 9);
}
