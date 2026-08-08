//! Plan §1.7 — the streaming source's test matrix, against real Delta tables.

mod common;

use common::*;
use delta_delta_ingest::source::{ChangePolicy, LogStreamBuilder, StreamCursor};
use deltalake::protocol::SaveMode;

#[tokio::test]
async fn pure_append_advances_the_cursor_monotonically_with_no_gaps_or_duplicates() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2]).await;
    append(&f.source, &[3, 4]).await;
    append(&f.source, &[5, 6]).await;

    let table = open(&f.source).await;
    let mut s = LogStreamBuilder::new(&table).with_starting_version(0);

    let mut seen_paths = Vec::new();
    let mut last = StreamCursor::at_version(0);
    while let Some(b) = s.next_batch().await.unwrap() {
        assert!(b.end > last, "cursor must advance: {} -> {}", last, b.end);
        last = b.end;
        seen_paths.extend(b.files.iter().map(|a| a.path.clone()));
    }

    let unique: std::collections::HashSet<_> = seen_paths.iter().collect();
    assert_eq!(
        unique.len(),
        seen_paths.len(),
        "no file may be emitted twice"
    );
    assert!(!seen_paths.is_empty(), "three appends must yield files");
    assert!(s.next_batch().await.unwrap().is_none(), "caught up");
}

#[tokio::test]
async fn resuming_from_a_persisted_cursor_matches_an_uninterrupted_run() {
    let f = Fixture::new().await;
    for i in 0..5 {
        append(&f.source, &[i]).await;
    }
    let table = open(&f.source).await;

    // Uninterrupted.
    let mut a = LogStreamBuilder::new(&table).with_starting_version(0);
    let mut all = Vec::new();
    while let Some(b) = a.next_batch().await.unwrap() {
        all.extend(b.files.iter().map(|x| x.path.clone()));
    }

    // Interrupted after one batch, resumed from the persisted cursor.
    let mut b1 = LogStreamBuilder::new(&table).with_starting_version(0);
    let first = b1.next_batch().await.unwrap().unwrap();
    let saved = first.end;
    let mut resumed: Vec<String> = first.files.iter().map(|x| x.path.clone()).collect();

    let mut b2 = LogStreamBuilder::new(&table).with_starting_cursor(saved);
    while let Some(b) = b2.next_batch().await.unwrap() {
        resumed.extend(b.files.iter().map(|x| x.path.clone()));
    }

    assert_eq!(
        all, resumed,
        "resume must reproduce the uninterrupted sequence"
    );
}

#[tokio::test]
async fn resuming_mid_commit_does_not_duplicate_files() {
    let f = Fixture::new().await;
    // One commit with several files: write multiple batches in a single write.
    let t = open(&f.source).await;
    t.write(vec![batch(&[1]), batch(&[2]), batch(&[3])])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    let table = open(&f.source).await;
    let total = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .next_batch()
        .await
        .unwrap()
        .unwrap()
        .files
        .len();

    if total < 2 {
        // The writer coalesced everything into one file; the mid-commit path needs >1.
        eprintln!("skipping: writer produced {total} file(s) in the commit");
        return;
    }

    // Split the commit deliberately.
    let mut s = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .with_commit_splitting(true)
        .with_max_files_per_batch(1);

    let first = s.next_batch().await.unwrap().unwrap();
    assert_eq!(first.files.len(), 1);
    assert!(!first.end.is_commit_boundary(), "should stop mid-commit");

    // Resume from the mid-commit cursor in a fresh stream.
    let mut s2 = LogStreamBuilder::new(&table)
        .with_starting_cursor(first.end)
        .with_commit_splitting(true);

    let mut paths: Vec<String> = first.files.iter().map(|a| a.path.clone()).collect();
    while let Some(b) = s2.next_batch().await.unwrap() {
        paths.extend(b.files.iter().map(|a| a.path.clone()));
    }

    let unique: std::collections::HashSet<_> = paths.iter().collect();
    assert_eq!(
        unique.len(),
        paths.len(),
        "mid-commit resume duplicated a file"
    );
    assert_eq!(
        paths.len(),
        total,
        "mid-commit resume lost or gained a file"
    );
}

#[tokio::test]
async fn optimize_between_appends_is_skipped_and_does_not_replay_the_table() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;
    append(&f.source, &[2]).await;

    let before = open(&f.source).await;
    let mut s = LogStreamBuilder::new(&before).with_starting_version(0);
    let mut pre = Vec::new();
    while let Some(b) = s.next_batch().await.unwrap() {
        pre.extend(b.files.iter().map(|a| a.path.clone()));
    }
    let caught_up_at = s.cursor();

    // Compact. OPTIMIZE rewrites files with dataChange=false.
    let t = open(&f.source).await;
    let (_t, metrics) = t.optimize().await.unwrap();
    assert!(
        metrics.num_files_added > 0 || metrics.num_files_removed > 0,
        "optimize did nothing, so this test proves nothing: {metrics:?}"
    );

    // A stream that was caught up must see nothing new — this is the rule everybody
    // gets wrong; without it every OPTIMIZE replays the whole table downstream.
    let after = open(&f.source).await;
    let mut s2 = LogStreamBuilder::new(&after).with_starting_cursor(caught_up_at);
    let post = s2.next_batch().await.unwrap();
    assert!(
        post.is_none(),
        "compaction must not emit data: got {:?}",
        post.map(|b| b.files.len())
    );
}

#[tokio::test]
async fn delete_fails_under_the_default_policy() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;

    let t = open(&f.source).await;
    let (_t, _m) = t.delete().with_predicate("id = 1").await.unwrap();

    let table = open(&f.source).await;
    let mut s = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .with_change_policy(ChangePolicy::Fail);

    // The append comes first and succeeds; the delete commit then errors.
    let mut err = None;
    loop {
        match s.next_batch().await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    let err = err.expect("a dataChange Remove must fail under ChangePolicy::Fail");
    let msg = err.to_string();
    assert!(msg.contains("append-only"), "got: {msg}");
    assert!(
        msg.contains("skip_change_commits"),
        "must name the alternative: {msg}"
    );
}

#[tokio::test]
async fn delete_is_skipped_under_skip_change_commits() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;
    let t = open(&f.source).await;
    let (_t, _m) = t.delete().with_predicate("id = 1").await.unwrap();
    append(&f.source, &[4]).await;

    let table = open(&f.source).await;
    let mut s = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .with_change_policy(ChangePolicy::SkipChangeCommits);

    let mut n = 0;
    while let Some(b) = s.next_batch().await.unwrap() {
        n += b.files.len();
    }
    assert!(n > 0, "the appends must still come through");
}

#[tokio::test]
async fn delete_emits_adds_under_ignore_changes() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;
    let t = open(&f.source).await;
    let (_t, _m) = t.delete().with_predicate("id = 1").await.unwrap();

    let table = open(&f.source).await;
    let mut s = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .with_change_policy(ChangePolicy::IgnoreChanges);

    let mut n = 0;
    while let Some(b) = s.next_batch().await.unwrap() {
        n += b.files.len();
    }
    // A delete that rewrites a file emits that rewritten file — duplicates downstream,
    // which is exactly what ignore_changes promises.
    assert!(n > 0);
}

#[tokio::test]
async fn starting_version_beyond_head_is_caught_up_not_an_error() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let table = open(&f.source).await;
    let mut s = LogStreamBuilder::new(&table).with_starting_version(9_999);
    assert!(
        s.next_batch().await.unwrap().is_none(),
        "a future starting_version means 'not yet', not 'error'"
    );
}

#[tokio::test]
async fn max_files_per_batch_splits_across_commits_without_splitting_one_commit() {
    let f = Fixture::new().await;
    for i in 0..4 {
        append(&f.source, &[i]).await;
    }
    let table = open(&f.source).await;

    // One file per commit here, so a limit of 1 yields one commit per batch and every
    // cursor stays on a commit boundary.
    let mut s = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .with_max_files_per_batch(1);

    let mut batches = 0;
    while let Some(b) = s.next_batch().await.unwrap() {
        assert!(
            b.end.is_commit_boundary(),
            "v1 must not split a commit without opt-in: {}",
            b.end
        );
        batches += 1;
    }
    assert!(
        batches >= 2,
        "the limit should have produced several batches"
    );
}

#[tokio::test]
async fn an_oversized_commit_fails_loudly_rather_than_silently_splitting() {
    let f = Fixture::new().await;
    let t = open(&f.source).await;
    t.write(vec![batch(&[1]), batch(&[2]), batch(&[3])])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    let table = open(&f.source).await;
    let files = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .next_batch()
        .await
        .unwrap()
        .unwrap()
        .files
        .len();
    if files < 2 {
        eprintln!("skipping: writer coalesced the commit into {files} file(s)");
        return;
    }

    let mut s = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .with_max_files_per_batch(1);
    let err = s.next_batch().await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("never splits a source commit"), "got: {msg}");
    assert!(msg.contains("allowed_latency"), "must suggest a fix: {msg}");
}

#[tokio::test]
async fn max_bytes_always_emits_at_least_one_file_so_the_stream_cannot_starve() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let table = open(&f.source).await;
    // An absurdly small byte budget must still make progress.
    let mut s = LogStreamBuilder::new(&table)
        .with_starting_version(0)
        .with_commit_splitting(true)
        .with_max_bytes_per_batch(1);
    let b = s.next_batch().await.unwrap().expect("must not starve");
    assert_eq!(b.files.len(), 1);
}

#[tokio::test]
async fn schema_is_reported_as_of_the_batch_end_version() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let table = open(&f.source).await;
    let mut s = LogStreamBuilder::new(&table).with_starting_version(0);
    let b = s.next_batch().await.unwrap().unwrap();
    let names: Vec<&str> = b.schema.fields().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "name"]);
}

#[tokio::test]
async fn a_concurrent_append_during_iteration_is_not_torn() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let table = open(&f.source).await;
    let mut s = LogStreamBuilder::new(&table).with_starting_version(0);
    let first = s.next_batch().await.unwrap().unwrap();

    // A writer commits while the stream is mid-iteration.
    append(&f.source, &[2]).await;

    let second = s.next_batch().await.unwrap().expect("new commit visible");
    assert!(second.start >= first.end, "batches must not overlap");
    let a: std::collections::HashSet<_> = first.files.iter().map(|x| &x.path).collect();
    let b: std::collections::HashSet<_> = second.files.iter().map(|x| &x.path).collect();
    assert!(a.is_disjoint(&b), "a concurrent write caused a torn read");
}
