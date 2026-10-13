//! A handle older than `handle_max_age`, refreshed before a commit, finds that another writer
//! of the same app_id moved a partition (docs/kafka-design.md §3.3, `commit_batch` step 1,
//! EO-m8): a batch that read the moved partition is dropped, and every partition that moved is
//! read again from its new offset, whether the batch read it or not, forward or backward.
//!
//! Every pipeline here runs with `DDI_TEST_FAULT=handle_max_age:0`, so each commit refreshes
//! its handle first. The variable is the test process's, which is why these tests have a file
//! of their own. Ignored unless a broker is there: see tests/kafka_common/mod.rs.

#![cfg(all(feature = "kafka", feature = "fault-injection"))]

mod kafka_common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};
use delta_delta_ingest::sink::{KafkaOffsets, Sink};
use deltalake::arrow::array::RecordBatch;
use rdkafka::producer::Producer as _;

use kafka_common::*;

/// Every commit of this process's pipelines refreshes its handle first.
fn age_every_handle() {
    std::env::set_var("DDI_TEST_FAULT", "handle_max_age:0");
}

async fn produce_to(f: &Fixture, id: u32, p: i32, n: i64) -> BTreeSet<(i32, i64)> {
    let producer = Producer::new();
    let mut out = BTreeSet::new();
    for i in 0..n {
        let o = producer
            .send(
                &f.topic,
                p,
                None,
                Some(&framed(EVENT_V1, id, event(i, None))),
            )
            .await;
        out.insert((p, o));
    }
    out
}

async fn run_to_end(p: &mut Pipeline) {
    for _ in 0..600 {
        if p.step().await.unwrap() == StepOutcome::CaughtUp {
            return;
        }
    }
    panic!("once-mode did not finish");
}

async fn once(toml: &str) {
    let mut p = Pipeline::open_with(resolve(toml, true), None)
        .await
        .unwrap();
    run_to_end(&mut p).await;
}

/// What `ddi kafka set-offsets --partition p --next next` writes (M5): a txn-only commit of
/// this fixture's app_id with partition `p` at `next - 1`.
async fn set_offset(f: &Fixture, p: i32, next: i64, reason: &str) {
    let table = open(&f.target).await;
    let schema = {
        use deltalake::delta_datafusion::DataFusionMixins;
        table.snapshot().unwrap().snapshot().read_schema()
    };
    let offsets = KafkaOffsets {
        topic: f.topic.clone(),
        group_id: "set-offsets".into(),
        ranges: BTreeMap::new(),
        assert: BTreeMap::from([(p, next - 1)]),
        marks: BTreeMap::new(),
        cleanup_logs: false,
        baseline: false,
        override_reason: Some(reason.to_string()),
        takeover_marker: false,
    };
    Sink::new(&f.app_id, 128 * 1024 * 1024)
        .commit_kafka(table, vec![RecordBatch::new_empty(schema)], &offsets)
        .await
        .unwrap();
}

/// The RESTORE runbook (§10.6) for partition `p`: its rows from `next` on are gone from the
/// target, and `set-offsets --next next --allow-replay` moves its offset back to them.
async fn restore(f: &Fixture, p: i32, next: i64) {
    let predicate = format!("kafka_partition = {p} AND kafka_offset >= {next}");
    open(&f.target)
        .await
        .delete()
        .with_predicate(predicate.as_str())
        .await
        .unwrap();
    set_offset(f, p, next, "restored").await;
}

/// A batch partition moved forward: another deployment of the same app_id (another group)
/// committed the offsets this member's batch read. The aged refresh finds it, the batch is
/// dropped, and nothing lands twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn an_aged_refresh_drops_a_batch_whose_partition_moved_forward() {
    age_every_handle();
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = produce_to(&f, id, 0, 10).await;
    let toml = f.toml("aged", "", "max_messages_per_batch = 10");
    once(&toml).await;
    produced.extend(produce_to(&f, id, 0, 10).await);

    // Assigned at offset 9, read before the other deployment's commit.
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    let other = toml.replace(
        &format!("group_id = \"g-{}\"", f.app_id),
        "group_id = \"another-deployment\"",
    );
    once(&other).await;
    assert_eq!(txns(&f.target, &f.app_id, 1).await[&0], Some(19));
    run_to_end(&mut p).await;

    assert_exactly(&coordinates(&f.target).await, &produced);
    assert_eq!(txns(&f.target, &f.app_id, 1).await[&0], Some(19));
}

/// A batch partition moved backward: the RESTORE runbook ran while this member's batch was
/// read. The batch is dropped, and the partition is read again from the restored offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn an_aged_refresh_drops_a_batch_whose_partition_moved_backward() {
    age_every_handle();
    let f = Fixture::new(1).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = produce_to(&f, id, 0, 20).await;
    let toml = f.toml("restored", "", "max_messages_per_batch = 10");
    once(&toml).await;
    produced.extend(produce_to(&f, id, 0, 10).await);

    // Assigned at offset 19; the target is restored to offset 9 before the batch commits.
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    restore(&f, 0, 10).await;
    run_to_end(&mut p).await;

    assert_exactly(&coordinates(&f.target).await, &produced);
    assert_eq!(txns(&f.target, &f.app_id, 1).await[&0], Some(29));
}

/// A partition the batch did not read moved backward: the commit's refresh finds it, and it is
/// read again from the restored offset instead of from where this member left it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn an_aged_refresh_rereads_a_partition_outside_the_batch_that_moved_backward() {
    age_every_handle();
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = produce_to(&f, id, 0, 10).await;
    produced.extend(produce_to(&f, id, 1, 20).await);
    let toml = f.toml("outside", "", "max_messages_per_batch = 10");
    once(&toml).await;
    produced.extend(produce_to(&f, id, 0, 10).await);

    // Partition 1 is at its end; partition 1's rows from 10 on are restored away while the
    // member reads partition 0.
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    restore(&f, 1, 10).await;
    run_to_end(&mut p).await;

    assert_exactly(&coordinates(&f.target).await, &produced);
    let stored = txns(&f.target, &f.app_id, 2).await;
    assert_eq!((stored[&0], stored[&1]), (Some(19), Some(19)));
}

/// A partition the batch did not read moved forward: an acknowledged skip while a running
/// member read another partition. The skipped offsets, there but not yet readable (an open
/// transaction), are never written once they are.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn an_aged_refresh_rereads_a_partition_outside_the_batch_that_moved_forward() {
    age_every_handle();
    let f = Fixture::new(2).await;
    let id = register(&f.subject(), EVENT_V1).await;
    let mut produced = produce_to(&f, id, 0, 10).await;
    produced.extend(produce_to(&f, id, 1, 10).await);
    let toml = f.toml("skipped", "", "max_messages_per_batch = 10");
    once(&toml).await;

    let mut p = Pipeline::open_with(resolve(&toml, false), None)
        .await
        .unwrap();
    for _ in 0..60 {
        p.step().await.unwrap();
        if p.kafka_progress().unwrap().assigned == 2 {
            break;
        }
    }
    assert_eq!(p.kafka_progress().unwrap().assigned, 2);

    // Ten messages for partition 1, in a transaction not yet committed (at the offsets the
    // broker gives them: the transaction may take an offset of its own before them).
    let tx = Producer::with(&[("transactional.id", &unique("tx"))]);
    tx.inner()
        .init_transactions(Duration::from_secs(30))
        .unwrap();
    tx.inner().begin_transaction().unwrap();
    let mut pending = BTreeSet::new();
    for i in 0..10 {
        let o = tx
            .send(
                &f.topic,
                1,
                None,
                Some(&framed(EVENT_V1, id, event(100 + i, None))),
            )
            .await;
        pending.insert((1, o));
    }
    produced.extend(produce_to(&f, id, 0, 10).await);
    set_offset(&f, 1, 15, "skip 10..14").await;
    produced.extend(pending.iter().copied().filter(|(_, o)| *o >= 15));
    let last = pending.iter().map(|(_, o)| *o).max();

    // The member commits partition 0's batch, refreshing first.
    for _ in 0..60 {
        p.step().await.unwrap();
        if coordinates(&f.target).await.len() >= 30 {
            break;
        }
    }
    assert!(coordinates(&f.target).await.len() >= 30);
    tx.inner()
        .commit_transaction(Duration::from_secs(30))
        .unwrap();
    for _ in 0..60 {
        p.step().await.unwrap();
        if txns(&f.target, &f.app_id, 2).await[&1] == last {
            break;
        }
    }
    // A few more steps, for anything read twice to land.
    for _ in 0..4 {
        p.step().await.unwrap();
    }
    assert_exactly(&coordinates(&f.target).await, &produced);
    assert_eq!(txns(&f.target, &f.app_id, 2).await[&1], last);
}
