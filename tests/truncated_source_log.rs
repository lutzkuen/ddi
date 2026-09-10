//! A source log that no longer reaches back to where a pipeline was told to start.
//!
//! The incident: four raw pipelines, declared but never run, on source tables that had since
//! turned thirty days old. Delta's default `delta.logRetentionDuration` is thirty days, so
//! those sources no longer had a commit 0 — and every one of those pipelines retried every
//! five minutes, forever, behind `delta error: Invalid table version: 0`. Nothing in that
//! line named the pipeline, the table, the versions involved, or a way out, and there was no
//! setting that recovered it. Their healthy neighbours in the same process were healthy for
//! one reason only: their own sources still had a commit 0.
//!
//! Two separate things were wrong, and this file pins both.
//!
//! The first is that the error was not ddi's. Resolving a source's head went through
//! `LogStore::get_latest_version(0)`, and that floor is a *requirement*: the kernel rejects a
//! log segment whose first commit is not exactly the version asked for. So a reclaimed commit
//! 0 failed the head lookup itself, reported as though version 0 were what somebody wanted,
//! on every poll — and `Pipeline::open` died before the reader existed, which is why none of
//! the reader's own typed errors could describe it.
//!
//! The second is that there was no recovery. `starting_version` was the right knob and could
//! not be used, because nothing distinguished "this pipeline has never committed, so tell it
//! where to start" from "this pipeline has committed and the versions it owed the target are
//! gone". What is pinned here is that the two now read differently, that the recoverable one
//! says which value to set, and — the part worth the most — that the unrecoverable one still
//! refuses, advances no offset and writes no rows.

mod common;

use std::sync::atomic::Ordering;

use common::{append, open, read_ids, Fixture};
use delta_delta_ingest::metrics::Metrics;
use delta_delta_ingest::offset::OffsetStore;
use delta_delta_ingest::pipeline::Pipeline;
use delta_delta_ingest::source::{earliest_readable_commit, LogStreamBuilder};
use delta_delta_ingest::Error;

/// Reclaim every commit below `keep_from`, the way a log-retention cleanup does.
///
/// A checkpoint is written first, and that ordering is the whole fixture: a checkpoint is what
/// leaves the table loadable once its early commits are gone, and it is what Delta's own
/// cleanup requires before it will delete anything. Without one this would not be a truncated
/// table but a broken one, which is a different bug with a different answer.
///
/// The deletion is done here rather than through `cleanup_metadata` because that honours the
/// table's thirty-day retention against the real clock, and a commit written a moment ago is
/// never expired. `VACUUM`'s clock can be overridden for a test; this cleanup's cannot.
async fn reclaim_commits_below(table_path: &str, keep_from: u64) {
    let t = open(table_path).await;
    deltalake::protocol::checkpoints::create_checkpoint(&t, None)
        .await
        .unwrap();

    for version in 0..keep_from {
        let commit = format!("{table_path}/_delta_log/{version:020}.json");
        std::fs::remove_file(&commit).unwrap_or_else(|e| panic!("removing {commit}: {e}"));
    }

    // The fixture has to be the situation, not an approximation of it: the table must still
    // load (the checkpoint survived) while the commit the pipeline wants must be gone.
    assert!(
        std::fs::metadata(format!(
            "{table_path}/_delta_log/{:020}.json",
            keep_from - 1
        ))
        .is_err(),
        "the commit below the floor must be gone, or this file proves nothing"
    );
    assert!(
        open(table_path).await.version().is_some(),
        "the table must still load from its checkpoint, or the test is about a broken table"
    );
}

/// Four commits, then everything below version 3 reclaimed. Version 3 holds row 3.
async fn truncated_fixture() -> Fixture {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await; // v1
    append(&f.source, &[2]).await; // v2
    append(&f.source, &[3]).await; // v3
    reclaim_commits_below(&f.source, 3).await;
    f
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bootstrap_below_the_log_floor_is_named_rather_than_blamed_on_version_zero() {
    let f = truncated_fixture().await;

    let mut p = Pipeline::open(f.cfg("copy")).await;
    let e = match &mut p {
        Err(e) => e,
        Ok(_) => panic!("a pipeline whose starting_version is gone must not open as healthy"),
    };

    match e {
        Error::BootstrapUnreachable {
            source_uri,
            configured,
            oldest_available,
            oldest_available_committed_at,
            head,
        } => {
            assert_eq!(
                *configured, 0,
                "the version the pipeline was told to start at"
            );
            assert_eq!(
                *oldest_available, 3,
                "the oldest commit the log still holds"
            );
            assert_eq!(*head, 3, "and the head, so the operator can see the gap");
            assert_eq!(source_uri, &f.source, "the relation an operator has to fix");
            assert!(
                oldest_available_committed_at.is_some(),
                "the floor's own commit timestamp is what turns a version number into a \
                 decision about how much history is being given up"
            );
        }
        other => panic!("expected BootstrapUnreachable, got {other:?}"),
    }

    let msg = e.to_string();
    assert!(
        !msg.contains("Invalid table version"),
        "the whole point is that delta-rs's head-lookup failure no longer surfaces raw: {msg}"
    );
    assert!(
        msg.contains("starting_version = 3"),
        "the message must name the value to set, not merely that a value exists: {msg}"
    );
    assert!(
        msg.contains("delta.logRetentionDuration"),
        "and the setting that prevents a recurrence: {msg}"
    );
    // The two recoveries that work for a pipeline which has committed do nothing here, and an
    // operator who reaches for them spends a maintenance window finding that out.
    assert!(
        msg.contains("Recreating the target changes nothing"),
        "the message must rule out the wrong answer it would otherwise invite: {msg}"
    );
}

/// The recovery, end to end: one value, and the pipeline runs.
///
/// Also pins what the value *costs*, because that is the half an operator is agreeing to:
/// rows from the reclaimed commits are not ingested, and nothing pretends otherwise.
#[tokio::test(flavor = "multi_thread")]
async fn setting_starting_version_to_the_floor_recovers_the_pipeline_and_accepts_the_gap() {
    let f = truncated_fixture().await;

    let mut cfg = f.cfg("copy");
    cfg.starting_version = 3;

    let mut p = Pipeline::open(cfg)
        .await
        .expect("a readable floor must open");
    p.run_until_caught_up().await.unwrap();

    assert_eq!(
        read_ids(&f.target).await,
        vec![3],
        "version 3 is ingested, and rows 1 and 2 are not — their commits are gone, so no \
         correct run could have produced them"
    );

    // And it is durable: the recovery is consumed once, and the offset now carries the
    // pipeline forward on its own.
    let offset = OffsetStore::new("ddi.test.copy", 3)
        .last_committed_version(&open(&f.target).await)
        .await
        .unwrap();
    assert_eq!(offset, Some(3), "the first batch recorded its own offset");

    append(&f.source, &[4]).await;
    let mut p = Pipeline::open(f.cfg("copy")).await.expect("reopen");
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&f.target).await,
        vec![3, 4],
        "a reopened pipeline resumes from its txn action, so the recovery value is not \
         consulted again and cannot re-skip anything"
    );
}

/// The reader's own head resolution, which is where `Invalid table version: 0` came from.
///
/// Exercised directly because it is reached on *every* poll, not only at startup: a stream
/// that opened successfully before its source's commit 0 was reclaimed would have started
/// failing here mid-run, and a pipeline that has committed never goes through the bootstrap
/// gate at all.
#[tokio::test(flavor = "multi_thread")]
async fn the_reader_resolves_a_head_without_assuming_version_zero_survives() {
    let f = truncated_fixture().await;
    let source = open(&f.source).await;

    let mut s = LogStreamBuilder::new(&source).with_starting_version(3);
    let batch = s
        .next_batch()
        .await
        .expect("resolving the head must not require a commit 0 that retention reclaimed")
        .expect("version 3 is a data commit, so there is a batch");

    assert_eq!(batch.through_version, 3);
    assert_eq!(
        s.last_known_head(),
        Some(3),
        "the head was actually resolved"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_log_floor_and_its_timestamp_are_read_from_the_store() {
    let f = truncated_fixture().await;
    let (version, at) = earliest_readable_commit(&open(&f.source).await)
        .await
        .unwrap()
        .expect("a truncated log still holds commits");
    assert_eq!(version, 3);
    assert!(
        at.is_some(),
        "the commit object's storage metadata is the same clock Delta time travel uses, and \
         it is the only thing that says how much history the gap is"
    );

    // A log that reaches back to 0 answers 0, so the healthy case is not special-cased.
    let g = Fixture::new().await;
    append(&g.source, &[1]).await;
    let (version, _) = earliest_readable_commit(&open(&g.source).await)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(version, 0);
}

/// The dangerous twin, and the one this whole change must not make easier.
///
/// A pipeline that has committed owes its target the versions it has not read yet. When one of
/// those is gone, there is no value an operator can set that recovers it — so the recoverable
/// error must not be raised here, nothing may advance, and the refusal has to survive the
/// reopen that the supervisor performs every five minutes.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_pipeline_never_skips_a_version_it_can_no_longer_read() {
    let f = Fixture::new().await;

    // v1 consumed normally, so there is a durable offset to fall behind from.
    append(&f.source, &[1]).await;
    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1]);
    drop(p);

    // The source runs on while the pipeline is stopped, and retention then reclaims a prefix
    // that includes version 2 — the one it has to resume on. The log still reaches well past
    // it, to a floor of 4 and a head of 5, which is exactly the shape that could tempt an
    // implementation into starting at what it can read instead of at what it owes.
    for id in [2, 3, 4, 5] {
        append(&f.source, &[id]).await;
    }
    reclaim_commits_below(&f.source, 4).await;

    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    assert_eq!(
        p.cursor().version,
        2,
        "precondition: the pipeline resumes at the commit that is missing"
    );
    let e = p.run_until_caught_up().await.unwrap_err();

    match &e {
        Error::CursorUnavailable { source_uri, cursor } => {
            assert_eq!(cursor.version, 2, "the version it cannot read");
            assert_eq!(source_uri, &f.source);
        }
        other => {
            panic!("a committed pipeline must refuse, not be offered a starting_version: {other:?}")
        }
    }
    assert!(
        !matches!(e, Error::BootstrapUnreachable { .. }),
        "the recoverable error must never be raised for a pipeline that has committed — its \
         recovery would skip rows this pipeline owed the target"
    );
    assert!(
        e.to_string()
            .contains("starting_version cannot move the resume point"),
        "and the message must say why the other error's recovery does not apply here: {e}"
    );

    let offset = OffsetStore::new("ddi.test.copy", 0)
        .last_committed_version(&open(&f.target).await)
        .await
        .unwrap();
    assert_eq!(
        offset,
        Some(1),
        "the unreadable version did not advance the offset"
    );
    assert_eq!(read_ids(&f.target).await, vec![1], "and wrote nothing");
    assert_eq!(
        earliest_readable_commit(&open(&f.source).await)
            .await
            .unwrap()
            .map(|(v, _)| v),
        Some(4),
        "precondition: the log does reach back to 4, so refusing is a choice and not an \
         inability to read anything at all"
    );

    // The supervisor's retry reopens, re-derives the cursor from that offset, and must land on
    // the same refusal rather than stepping over the gap on a later pass.
    let mut p = Pipeline::open(f.cfg("copy")).await.unwrap();
    let again = p.run_until_caught_up().await.unwrap_err();
    assert!(
        matches!(again, Error::CursorUnavailable { cursor, .. } if cursor.version == 2),
        "a reopened pipeline must say the same thing: {again}"
    );
    assert_eq!(
        read_ids(&f.target).await,
        vec![1],
        "and still wrote nothing"
    );
}

/// A `starting_version` the source has not reached yet is waiting, not broken.
///
/// The reader has always treated a cursor above the head as "caught up", but `Pipeline::open`
/// never agreed: `adjust_for_replaced_source` read the same cursor as a log that had gone
/// *backwards* — a dropped and recreated source — and refused to open at all. A pipeline
/// declared ahead of the source it reads is an ordinary thing to do, and this is also the
/// shape an operator lands on by typing one digit too many into the recovery above, so it
/// must not become the new way to be permanently stuck.
#[tokio::test(flavor = "multi_thread")]
async fn a_starting_version_above_the_head_waits_instead_of_failing_to_open() {
    let f = Fixture::new().await;
    append(&f.source, &[1]).await;

    let mut cfg = f.cfg("copy");
    cfg.starting_version = 9_999;

    let mut p = Pipeline::open(cfg)
        .await
        .expect("a source that has not reached that version yet is not a replaced source");
    p.run_until_caught_up()
        .await
        .expect("and polling it is 'nothing yet', not an error");
    assert!(
        read_ids(&f.target).await.is_empty(),
        "nothing has been written, because nothing in range exists yet"
    );
}

/// The gauges the supervisor drives, exercised through the call the supervisor makes.
///
/// Two series rather than one because the recoveries are opposites, and an alert that cannot
/// tell "set a value" from "rebuild this target" cannot route to anybody.
#[tokio::test]
async fn the_two_unreachable_log_gauges_are_distinct_and_hold_until_a_step_succeeds() {
    let metrics = Metrics::new();
    let m = metrics.pipeline("copy");

    assert_eq!(m.bootstrap_unreachable.load(Ordering::Relaxed), 0);
    assert_eq!(m.resume_unreachable.load(Ordering::Relaxed), 0);

    m.observe_error(&Error::BootstrapUnreachable {
        source_uri: "abfss://raw@acct.dfs.core.windows.net/orders".into(),
        configured: 0,
        oldest_available: 1203,
        oldest_available_committed_at: Some("2026-08-11T03:14:00+00:00".into()),
        head: 1290,
    });
    assert_eq!(m.bootstrap_unreachable.load(Ordering::Relaxed), 1);
    assert_eq!(
        m.resume_unreachable.load(Ordering::Relaxed),
        0,
        "a pipeline that has never committed has lost nothing it owed the target, and an \
         alert that conflated the two would page for a rebuild that is not needed"
    );
    assert_eq!(m.up.load(Ordering::Relaxed), 0);

    // Most of an attempt runs before the log is consulted — both tables reopened, the resume
    // point resolved — so a later attempt failing some other way is not evidence the log grew
    // back. Dropping the gauge there would resolve the alert, and reset any `for` clause on
    // it, while the pipeline is still stuck on the same thing.
    m.observe_error(&Error::Transform("the target timed out on reopen".into()));
    assert_eq!(
        m.bootstrap_unreachable.load(Ordering::Relaxed),
        1,
        "an unrelated failure says nothing about the log, so the alert must hold"
    );

    m.mark_progress();
    assert_eq!(
        m.bootstrap_unreachable.load(Ordering::Relaxed),
        0,
        "a step that succeeded read the commit it needed, so the log reaches far enough now"
    );

    let m2 = metrics.pipeline("resume");
    m2.observe_error(&Error::CursorUnavailable {
        source_uri: "abfss://raw@acct.dfs.core.windows.net/orders".into(),
        cursor: delta_delta_ingest::source::StreamCursor::at_version(1203),
    });
    assert_eq!(m2.resume_unreachable.load(Ordering::Relaxed), 1);
    assert_eq!(
        m2.bootstrap_unreachable.load(Ordering::Relaxed),
        0,
        "and the reverse: a committed pipeline must not light the recoverable gauge"
    );

    let out = metrics.render();
    assert!(
        out.contains("# TYPE ddi_bootstrap_unreachable gauge"),
        "{out}"
    );
    assert!(out.contains("# TYPE ddi_resume_unreachable gauge"), "{out}");
    assert!(
        out.contains("ddi_resume_unreachable{pipeline=\"resume\"} 1"),
        "{out}"
    );
    assert!(
        out.contains("ddi_bootstrap_unreachable{pipeline=\"copy\"} 0"),
        "{out}"
    );
}
