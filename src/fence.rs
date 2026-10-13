//! Partitioned offsets in a Delta table: one `txn` action per partition.
//!
//! A Delta source's offset is one number, held in one `txn` action named by the app_id. A
//! source with partitions, as a Kafka topic has, keeps one per partition, each under its own
//! appId, so that every partition advances and is fenced on its own. Nothing here knows
//! about Kafka; it is about how such offsets are named, read and defended in a Delta table
//! (docs/kafka-design.md §4.3).
//!
//! # Reading them
//!
//! [`read_txns`] answers "what is every one of these appIds' newest `txn`, as of this
//! handle's version" in one pass over the log. The kernel can answer it too, one appId at a
//! time, but each answer fetches every commit since the newest checkpoint: thirty partitions
//! would mean thirty times that. Walking backwards instead finds the answer where it almost
//! always is — in the newest commit, which in steady state is our own and carries every
//! partition — and is bounded by the newest checkpoint, whose `txn` column holds the rest.
//!
//! # Defending them
//!
//! [`Fence`] keeps, for the partitions one writer owns, the offset each last held in the
//! target, and builds every commit's `txn` actions from it. delta-rs refuses a commit when a
//! commit that landed after its snapshot wrote a `txn` with one of the same appIds, so a
//! commit that re-asserts every owned partition is fenced against any other writer of any of
//! them. What delta-rs cannot see is what the snapshot itself already holds; that is what
//! [`Fence::verify`] checks, for a partition no commit of ours has carried since the handle
//! last moved.
//!
//! # Taking them over
//!
//! ddi resumes offsets kafka-delta-ingest wrote, under the same appIds. The first commit ddi
//! makes under an app_id also writes the takeover marker ([`takeover_marker_app_id`]), a
//! `txn` nothing removes, so from then on the table says for good that ddi has taken them
//! over ([`era`]). Before that, [`takeover_check`] is the one check a takeover needs, and
//! [`takeover_baseline`] the commit that records it with every acknowledgment the check
//! accepted. Every `txn` ddi writes for Kafka carries [`KAFKA_TXN_LAST_UPDATED`], a
//! `lastUpdated` no engine expires.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use deltalake::arrow::array::{Array, AsArray, RecordBatch};
use deltalake::arrow::datatypes::{DataType, Int64Type};
use deltalake::kernel::transaction::PROTOCOL;
use deltalake::logstore::object_store::{ObjectStore, ObjectStoreExt};
use deltalake::{DeltaTable, ObjectMeta, Path};
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::sink::{KafkaOffsets, MessageMark};

/// The table property under which Delta forgets a `txn` whose `lastUpdated` is too old.
const TXN_RETENTION: &str = "delta.setTransactionRetentionDuration";

/// The table property that turns column mapping on.
const COLUMN_MAPPING_MODE: &str = "delta.columnMapping.mode";

/// How many commit files the backward walk keeps in flight.
const IN_FLIGHT: usize = 4;

/// How many commits below a checkpoint [`read_txns`] reads, at most, for the marks the
/// checkpoint answered without.
const BELOW_CHECKPOINT_READS: u64 = 256;

/// How far below the handle's version the first listing for a checkpoint reaches, when
/// `_last_checkpoint` does not say where to look. The kernel scans backwards in windows of
/// the same size.
const LISTING_WINDOW: u64 = 1000;

/// The `txn` appId that holds partition `partition`'s offset for `app_id`.
///
/// kafka-delta-ingest's naming, kept exactly, so a table it wrote resumes under ddi where
/// it stopped.
pub fn txn_app_id(app_id: &str, partition: i32) -> String {
    format!("{app_id}-{partition}")
}

/// The version the takeover marker is written with. Any would do: what counts is that the
/// appId has a `txn` at all.
pub const TAKEOVER_MARKER_VERSION: i64 = 1;

/// The `lastUpdated` of every `txn` a Kafka commit writes, the takeover marker included:
/// 9999-12-31T23:59:59.999Z, later than any cutoff `delta.setTransactionRetentionDuration`
/// sets (docs/kafka-design.md §4.6).
///
/// Neither obvious choice survives every engine that writes these tables:
/// - no `lastUpdated` is kept by the kernel (delta-rs) under the property, but dropped by
///   Spark, whose log replay keeps only the `txn`s whose `lastUpdated` is after the cutoff
///   while the property is set (`InMemoryLogReplay.getTransactions`); and Trino's checkpoint
///   writes 0 for a `txn` without one, which every engine expires;
/// - the time of the commit, as kafka-delta-ingest stamps it, expires a partition no commit
///   has re-asserted for longer than the duration, and the marker, which no commit after the
///   first re-asserts, once the duration has passed.
///
/// A present time past any cutoff is kept as it is by Trino and Spark, and expired by none.
/// Only a checkpoint writer that drops the field (delta-rs up to 0.17.3 nulls it, as
/// kafka-delta-ingest's `-c` builds before 2024-07 did; 0.26.2, which its `-c` runs today,
/// writes no `lastUpdated` column at all) makes it null again, which Spark
/// alone then expires, and only while the property is set. The target is refused while it is
/// ([`refuse_txn_retention`]); the stamp is what keeps ddi's offsets and its marker when the
/// property was set, a checkpoint written and the property removed again between two
/// refreshes. A Delta pipeline's `txn` is written as it always was, without one.
pub const KAFKA_TXN_LAST_UPDATED: i64 = 253_402_300_799_999;

/// The `txn` appId that records that ddi has taken over `app_id`'s offsets in a target: the
/// takeover marker (docs/kafka-design.md §4.3).
///
/// Written once, by the first Kafka commit of `app_id` whose read snapshot lacks it
/// ([`Fence::takeover_marker_due`]), and by no commit after. Nothing removes it: the protocol
/// has no action that takes a `txn` out of a table, and every checkpoint writer keeps every
/// `txn` but one `delta.setTransactionRetentionDuration` has expired. A target is refused
/// while that is set, and the marker carries [`KAFKA_TXN_LAST_UPDATED`], which no engine
/// expires even then. Its presence is therefore "ddi has committed under this app_id here".
/// No property of the partitions' own `txn`s can say that: kafka-delta-ingest stamps each with
/// the time it wrote it, but checkpoint writers rewrite the field (delta-rs up to 0.17.3 nulls
/// it, Trino writes 0 for a `txn` without one), and a commit's metadata does not survive a
/// checkpoint at all.
///
/// One sequence can still lose it: a checkpoint by a writer that nulls the field, then the
/// property set and a Spark checkpoint written under it, then the property removed. A
/// [`Fence`] that sees the property refused reads the marker again ([`Fence::reset`]), so a
/// running member writes it once more; an open after such a loss reads kafka-delta-ingest's
/// era, or a fresh one on a target with rows, and goes through the takeover check again
/// ([`takeover_check`]), which refuses a partition it cannot account for rather than read it
/// twice.
///
/// Never the appId of a partition's offset, `<a>-<p>`, of any app_id `a`: it ends in `.ddi`,
/// not in digits.
pub fn takeover_marker_app_id(app_id: &str) -> String {
    format!("{app_id}.ddi")
}

/// The partition `txn_app_id` holds the offset of for `app_id`: `p` for `<app_id>-<p>`.
pub fn partition_of(app_id: &str, txn_app_id: &str) -> Option<i32> {
    let digits = txn_app_id.strip_prefix(app_id)?.strip_prefix('-')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Who has written one app_id's offsets in a target (docs/kafka-design.md §4.4 step 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Era {
    /// Nobody: no partition has an offset, and there is no takeover marker.
    Fresh,
    /// kafka-delta-ingest, and ddi has not taken them over: some partition has an offset, and
    /// there is no takeover marker.
    Kdi,
    /// ddi: the takeover marker is there.
    Ddi,
}

/// The era of `app_id`'s offsets, from `txns` as [`read_txns`] returns them for every
/// partition of its topic and for [`takeover_marker_app_id`].
///
/// Exact: the marker is there or it is not, and a partition has an offset or it has not.
/// Nothing is inferred from who wrote a `txn`, nor from its `lastUpdated`.
pub fn era(app_id: &str, txns: &BTreeMap<String, TxnState>) -> Era {
    let held = |id: &str| txns.get(id).is_some_and(|t| t.version.is_some());
    if held(&takeover_marker_app_id(app_id)) {
        Era::Ddi
    } else if txns
        .iter()
        .any(|(id, t)| t.version.is_some() && partition_of(app_id, id).is_some())
    {
        Era::Kdi
    } else {
        Era::Fresh
    }
}

/// The one check taking over offsets kafka-delta-ingest may have written needs, made when a
/// pipeline opens, before it joins its group (docs/kafka-design.md §4.4 step 3).
///
/// `partitions` holds every partition of the topic, from metadata, with its offset in the
/// target (`None`: no `txn`) and its high watermark; `acknowledged` the partitions an explicit
/// per-partition `starting_offsets` entry names (`"*"`, `earliest` and `latest` name none).
/// `holds_rows` says whether the target holds rows that may be this topic's: it has data
/// files, and nothing shows that none of them is (docs/kafka-design.md §4.4 step 2).
///
/// It applies in [`Era::Kdi`], and in [`Era::Fresh`] on a target that `holds_rows`:
/// - In kafka-delta-ingest's era, a partition without an offset whose topic holds messages is
///   one it never wrote a row for, or one whose offset expired under
///   `delta.setTransactionRetentionDuration` while it ran, and the table cannot tell which.
/// - A fresh app_id on a target with rows may be kafka-delta-ingest's era with every offset
///   expired, which is how they expire where one instance ran: it re-stamps every partition in
///   every commit, so all of them go once the whole topic has been quiet for the duration. The
///   rows may as well be another pipeline's, or a backfill's; the table cannot tell which
///   either.
///
/// Reading such a partition from offset 0 could duplicate what is in the table, so unless
/// every such partition is acknowledged this refuses with [`Error::KafkaOffsetMissing`],
/// naming all of them. A partition whose topic held nothing had no message, so nothing of it
/// can be in the table.
///
/// In [`Era::Ddi`] there is nothing to check. The commit that wrote the marker was made by an
/// open that passed this check while the marker was absent: in kafka-delta-ingest's era its
/// takeover baseline ([`takeover_baseline`]), which made every acknowledgment an offset; in a
/// fresh one its baseline or first commit. And ddi's offsets do not expire
/// ([`KAFKA_TXN_LAST_UPDATED`]). A partition without one is new, or one nothing of is in the
/// table ([`start_without_offset`]). [`Era::Fresh`] on a target without rows starts where
/// `starting_offsets` says.
pub fn takeover_check(
    app_id: &str,
    era: Era,
    holds_rows: bool,
    partitions: &BTreeMap<i32, (Option<i64>, i64)>,
    acknowledged: &BTreeSet<i32>,
) -> Result<()> {
    let fresh = match era {
        Era::Kdi => false,
        Era::Fresh if holds_rows => true,
        Era::Fresh | Era::Ddi => return Ok(()),
    };
    let missing: Vec<i32> = partitions
        .iter()
        .filter(|(p, (stored, high))| stored.is_none() && *high > 0 && !acknowledged.contains(p))
        .map(|(p, _)| *p)
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(Error::KafkaOffsetMissing {
        app_id: app_id.to_string(),
        partitions: missing,
        fresh,
    })
}

/// The txn-only commit an open in [`Era::Kdi`] makes once every check of the open has passed,
/// just before it joins its group: the takeover baseline (docs/kafka-design.md §4.4 step 8),
/// committed with the takeover marker and `ddi.kafka.baseline`. `None` in any other era.
///
/// `stored` holds every partition of the topic with its offset in the target, `explicit` the
/// per-partition `starting_offsets` entries. It asserts every partition that has an offset
/// at that offset, and every partition without one that an entry `s > 0` acknowledges at
/// `s − 1`:
/// - The acknowledgments become offsets in the very commit that ends kafka-delta-ingest's era.
///   After it, a partition without an offset starts at offset 0 unless an entry says
///   otherwise, so an acknowledgment held only by the config would last only as long as the
///   config keeps it: a partition whose owner has not committed yet, with its entry removed as
///   done, would be read again from 0. Each start is the one the entry gives
///   ([`start_without_offset`] against the `Some` rows of §4.4): `s − 1` with `s` in the topic
///   resumes at `s`, below its low watermark blocks as unavailable, past its high one as
///   regressed. An entry of 0 needs no offset, as none starts at 0 too.
/// - Every offset kafka-delta-ingest wrote is asserted again at its value, now with
///   [`KAFKA_TXN_LAST_UPDATED`], so none of its stamps is left in ddi's era for the property to
///   expire, an idle partition's included.
/// - It carries every partition's `txn`, so a commit to any of them since the open read them
///   makes it fail (`ConcurrentTransaction`), as another member's takeover baseline does,
///   through the marker. The open then reads again, and finds ddi's era.
pub fn takeover_baseline(
    era: Era,
    stored: &BTreeMap<i32, Option<i64>>,
    explicit: &BTreeMap<i32, i64>,
) -> Option<BTreeMap<i32, i64>> {
    if era != Era::Kdi {
        return None;
    }
    let assert = stored
        .iter()
        .filter_map(|(p, stored)| match (stored, explicit.get(p)) {
            (Some(offset), _) => Some((*p, *offset)),
            (None, Some(next)) if *next > 0 => Some((*p, next - 1)),
            (None, _) => None,
        })
        .collect();
    Some(assert)
}

/// Where a partition without an offset starts reading, in a target that is not fresh
/// (docs/kafka-design.md §4.4, the `None` rows).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoTxnStart {
    /// At its explicit `starting_offsets` entry, an acknowledgment.
    Explicit(i64),
    /// At exactly offset 0: nothing of the partition is in the table.
    Zero,
    /// Blocked, `KafkaOffsetsUnavailable`: the topic no longer holds offset `wanted`.
    Unavailable { wanted: i64, low: i64 },
    /// Blocked, `KafkaOffsetsRegressed`: the topic does not hold offset `wanted` yet.
    Regressed { wanted: i64, high: i64 },
}

/// Where a partition without an offset starts, from its watermarks and its explicit
/// `starting_offsets` entry, if it has one: there, if the topic holds it; otherwise at
/// exactly offset 0, unless messages ddi never read were trimmed (EO-M4a).
///
/// The same in both eras of a target that is not fresh. In [`Era::Kdi`], [`takeover_check`]
/// has refused at open every partition this would start at 0 when it may have had an offset,
/// and [`takeover_baseline`] has made every acknowledgment an offset.
pub fn start_without_offset(low: i64, high: i64, explicit: Option<i64>) -> NoTxnStart {
    match explicit {
        Some(wanted) if wanted < low => NoTxnStart::Unavailable { wanted, low },
        Some(wanted) if wanted > high => NoTxnStart::Regressed { wanted, high },
        Some(wanted) => NoTxnStart::Explicit(wanted),
        None if low == 0 => NoTxnStart::Zero,
        None => NoTxnStart::Unavailable { wanted: 0, low },
    }
}

/// One appId's newest `txn`, and what the commit that wrote it says about it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxnState {
    /// The `txn`'s version: for a partition, the last offset accounted for. `None` when the
    /// table holds no `txn` under this appId.
    pub version: Option<i64>,
    /// The message at that offset, as the commit that wrote the `txn` recorded it in
    /// `ddi.kafka.marks`. `None` when that commit recorded none, or recorded one for a
    /// different offset, and when the `txn` was found only in a checkpoint, which keeps no
    /// commit metadata, and the commit below it was not reached.
    pub mark: Option<MessageMark>,
}

/// What reading offsets has cost, for `ddi_kafka_offset_lookups_total`,
/// `ddi_kafka_offset_lookup_seconds_total`, `ddi_kafka_offset_lookup_fallbacks_total`,
/// `ddi_kafka_offset_lookup_below_checkpoint_reads_total` and
/// `ddi_kafka_offset_lookup_unverifiable_total`.
///
/// Shared rather than returned, so that whoever renders the metrics can hold the same
/// counters the readers add to.
#[derive(Debug, Default)]
pub struct LookupCounters {
    lookups: AtomicU64,
    micros: AtomicU64,
    fallbacks: AtomicU64,
    below_checkpoint_reads: AtomicU64,
    unverifiable: AtomicU64,
}

impl LookupCounters {
    /// Calls of [`read_txns`] that read the log: one per call however many appIds it asks
    /// about.
    pub fn lookups(&self) -> u64 {
        self.lookups.load(Ordering::Relaxed)
    }

    /// Time spent in those calls.
    pub fn seconds(&self) -> f64 {
        self.micros.load(Ordering::Relaxed) as f64 / 1e6
    }

    /// appIds the kernel had to look up one at a time, because a checkpoint could not be read
    /// here.
    pub fn fallbacks(&self) -> u64 {
        self.fallbacks.load(Ordering::Relaxed)
    }

    /// Commits [`read_txns`] read below a checkpoint, for the marks it answered without: at
    /// most [`BELOW_CHECKPOINT_READS`] per call, and none for a checkpoint a [`Fence`] has
    /// read below already.
    pub fn below_checkpoint_reads(&self) -> u64 {
        self.below_checkpoint_reads.load(Ordering::Relaxed)
    }

    /// Partitions' offsets a read for marks returned without one: the commit that wrote the
    /// offset recorded none (kafka-delta-ingest's, or ddi's for a partition that has not
    /// carried data since), or a checkpoint answered it and the walk below did not reach that
    /// commit (log cleanup removed one on the way, it could not be read, or the walk read as
    /// many as it may). Each is a partition whose resume cannot be checked against its mark
    /// (docs/kafka-design.md §4.4, Unverifiable), counted once per read that returns it.
    pub fn unverifiable(&self) -> u64 {
        self.unverifiable.load(Ordering::Relaxed)
    }
}

/// What the walk below one checkpoint found for each partition's offset it went after: the
/// version the checkpoint answered, and the mark beside it in the commit that wrote it, or
/// none when that commit recorded none or was not reached.
///
/// The same for every read down to that checkpoint: a checkpoint's answer never changes, and
/// commits below it are only ever removed, never rewritten. So a [`Fence`] keeps it, and its
/// resets for a blocked partition's recheck or an aging handle do not repeat up to
/// [`BELOW_CHECKPOINT_READS`] GETs. Only the newest checkpoint's is kept.
#[derive(Debug, Default)]
struct BelowCheckpoint {
    checkpoint: Option<u64>,
    found: BTreeMap<String, (i64, Option<MessageMark>)>,
}

/// Every appId in `app_ids`, with its newest `txn` as of `table`'s version, read in one pass.
///
/// 1. The commit at the handle's version, alone. In steady state that is our own last
///    commit, which carries every partition we own, and this is the only request made.
/// 2. Otherwise, the newest complete checkpoint at or below that version: `_last_checkpoint`
///    says where to list from, and the listing says what is really there (§4.3).
/// 3. The commits from the handle's version back to that checkpoint, [`IN_FLIGHT`] at a
///    time, stopping as soon as every appId has been found. Each found `txn` takes its mark
///    from `ddi.kafka.marks` in the same commit, when that commit is ddi's (`ddi.appId`).
/// 4. For the appIds still missing, the checkpoint's `txn` column, read once. What it
///    answers comes without a mark: a checkpoint keeps no commitInfo.
/// 5. Only if that checkpoint cannot be read here: the kernel, one appId at a time, counted
///    in [`LookupCounters::fallbacks`].
/// 6. For the partitions' offsets steps 4 and 5 answered (appIds `<a>-<p>`: no other `txn`
///    has a mark, the takeover marker among them), the commits below the checkpoint, newest
///    first, [`IN_FLIGHT`] at a time, until each of those appIds has been met: the newest
///    commit with its `txn` gives back the mark beside it, when that `txn` holds the version
///    answered. Without them the re-read check of a resume
///    (docs/kafka-design.md §4.4, I19) is off for the partition until it carries data again.
///    Bounded twice: by [`BELOW_CHECKPOINT_READS`] commits, counted in
///    [`LookupCounters::below_checkpoint_reads`], and by the first commit it cannot use,
///    which ends it like the first commit log cleanup has removed: below a gap a `txn` is not
///    known to be the newest. A commit it cannot use is one that does not parse, or one the
///    store will not serve (refused, not found); a store fault that may pass, an outage,
///    fails the read instead, so that it is retried rather than settling for less. Every
///    partition's offset returned without a mark is counted in
///    [`LookupCounters::unverifiable`].
///
/// The walk is contiguous, so what it finds is the newest; what it does not find is as the
/// checkpoint at its bottom says, or absent when it reached version 0. A commit missing from
/// the middle is an error, never a reason to look further down: an answer older than the
/// truth would move an offset backwards. Step 6 only ever adds a mark to an answer, so it
/// stops at a commit it cannot use instead, and never fails a read but for an outage.
///
/// Refuses a table that sets `delta.setTransactionRetentionDuration`, as
/// [`refuse_txn_retention`] does, so that nothing read here has to be filtered for expiry.
///
/// The map has an entry for every appId asked about; `version` is `None` for one with no
/// `txn`.
pub async fn read_txns(
    table: &DeltaTable,
    app_ids: &[String],
    counters: &LookupCounters,
) -> Result<BTreeMap<String, TxnState>> {
    refuse_txn_retention(table)?;
    let mut below = BelowCheckpoint::default();
    read(table, app_ids, counters, Below::Marks(&mut below)).await
}

/// Whether [`read_txns`]' step 6 reads below the checkpoint.
#[derive(Debug)]
enum Below<'a> {
    /// It does, for the caller keeps the marks, and remembers what it found in there.
    Marks(&'a mut BelowCheckpoint),
    /// It does not: the caller wants the versions alone.
    Nothing,
}

/// Steps 1-6 of [`read_txns`], counted as one lookup.
async fn read(
    table: &DeltaTable,
    app_ids: &[String],
    counters: &LookupCounters,
    mut below: Below<'_>,
) -> Result<BTreeMap<String, TxnState>> {
    let wanted: BTreeSet<&str> = app_ids.iter().map(String::as_str).collect();
    let mut found = BTreeMap::new();
    if !wanted.is_empty() {
        let head = table.version().ok_or_else(|| not_loaded(table))?;
        let started = Instant::now();
        let walked = walk(table, head, &wanted, &mut found, counters, &mut below).await;
        counters.lookups.fetch_add(1, Ordering::Relaxed);
        counters
            .micros
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        walked?;
    }
    if matches!(below, Below::Marks(_)) {
        let unverifiable = found
            .iter()
            .filter(|(id, txn)| may_carry_a_mark(id) && txn.version.is_some() && txn.mark.is_none())
            .count();
        counters
            .unverifiable
            .fetch_add(unverifiable as u64, Ordering::Relaxed);
    }
    Ok(wanted
        .into_iter()
        .map(|id| {
            let state = found.remove(id).unwrap_or_default();
            (id.to_string(), state)
        })
        .collect())
}

/// Steps 1-6 of [`read_txns`], filling `found` with every `txn` it finds.
async fn walk(
    table: &DeltaTable,
    head: u64,
    wanted: &BTreeSet<&str>,
    found: &mut BTreeMap<String, TxnState>,
    counters: &LookupCounters,
    below: &mut Below<'_>,
) -> Result<()> {
    let log_store = table.log_store();
    let all_found = |found: &BTreeMap<String, TxnState>| found.len() == wanted.len();

    let head_commit = log_store.read_commit_entry(head).await?;
    if let Some(bytes) = &head_commit {
        record_commit(table, head, bytes, wanted, found)?;
        if all_found(found) {
            return Ok(());
        }
    }

    // Checkpoints are found and read where all of them can be seen, which the handle's own
    // store is not when it was opened by replaying the log; commits are read as the handle
    // reads them.
    let store = crate::storage::with_every_checkpoint(log_store.as_ref());
    let checkpoint = newest_checkpoint(store.as_ref(), log_store.log_path(), head).await?;
    // Without a checkpoint the walk has to reach version 0, which must therefore still be
    // there. The head's own commit may be missing only where a checkpoint at the head stands
    // in for it, so that is what is compared: `bottom` is 0 without a checkpoint too, and a
    // handle at version 0 would otherwise take a missing commit 0 for an empty log.
    let bottom = checkpoint.as_ref().map_or(0, |c| c.version);
    if head_commit.is_none() && checkpoint.as_ref().map(|c| c.version) != Some(head) {
        return Err(missing_commit(table, head));
    }

    // The commit at the checkpoint's version is read too when it is still there: it repeats
    // what the checkpoint says, and adds the marks the checkpoint cannot.
    let mut commits = futures::stream::iter((bottom..head).rev())
        .map(|v| {
            let log_store = log_store.clone();
            async move { (v, log_store.read_commit_entry(v).await) }
        })
        .buffered(IN_FLIGHT);
    while let Some((v, read)) = commits.next().await {
        match read? {
            Some(bytes) => record_commit(table, v, &bytes, wanted, found)?,
            // Log cleanup keeps the commit a checkpoint stands for, but nothing obliges a
            // writer to. Whatever it held, the checkpoint holds.
            None if checkpoint.is_some() && v == bottom => {}
            None => return Err(missing_commit(table, v)),
        }
        if all_found(found) {
            return Ok(());
        }
    }

    let Some(checkpoint) = checkpoint else {
        // The walk reached version 0: what it did not find has no txn.
        return Ok(());
    };
    let rest: BTreeSet<&str> = wanted
        .iter()
        .copied()
        .filter(|id| !found.contains_key(*id))
        .collect();
    match checkpoint_txns(store, &checkpoint, &rest).await {
        Ok(txns) => {
            for (id, version) in txns {
                found.insert(
                    id,
                    TxnState {
                        version: Some(version),
                        ..TxnState::default()
                    },
                );
            }
        }
        Err(why) => {
            warn!(
                table = %table.table_url(),
                checkpoint_version = checkpoint.version,
                checkpoint = %checkpoint.files[0].location,
                app_ids = rest.len(),
                "cannot read the txn column of this checkpoint ({why}); asking the kernel for \
                 each offset instead, which reads the log once per appId"
            );
            kernel_lookups(table, &rest, found, counters).await?;
        }
    }
    let answered: BTreeSet<&str> = rest
        .into_iter()
        .filter(|id| found.contains_key(*id) && may_carry_a_mark(id))
        .collect();
    if let Below::Marks(memo) = below {
        if !answered.is_empty() {
            below_checkpoint(table, checkpoint.version, answered, found, counters, memo).await?;
        }
    }
    Ok(())
}

/// Step 6 of [`read_txns`]: for the appIds in `answered`, which the checkpoint at `bottom` or
/// the kernel answered, what the commit below `bottom` that wrote each one's `txn` says. What
/// `memo` already knows for `bottom` is taken from it, and what is found goes into it.
async fn below_checkpoint(
    table: &DeltaTable,
    bottom: u64,
    mut answered: BTreeSet<&str>,
    found: &mut BTreeMap<String, TxnState>,
    counters: &LookupCounters,
    memo: &mut BelowCheckpoint,
) -> Result<()> {
    if memo.checkpoint != Some(bottom) {
        *memo = BelowCheckpoint {
            checkpoint: Some(bottom),
            found: BTreeMap::new(),
        };
    }
    answered.retain(|id| {
        let Some(answer) = found.get_mut(*id) else {
            return false;
        };
        match memo.found.get(*id) {
            Some((version, mark)) if answer.version == Some(*version) => {
                answer.mark = *mark;
                false
            }
            _ => true,
        }
    });
    if answered.is_empty() {
        return Ok(());
    }

    let log_store = table.log_store();
    let floor = bottom.saturating_sub(BELOW_CHECKPOINT_READS);
    let mut commits = futures::stream::iter((floor..bottom).rev())
        .map(|v| {
            let log_store = log_store.clone();
            async move {
                // Counted as it is sent: one the walk stops before it is ever polled is not.
                counters
                    .below_checkpoint_reads
                    .fetch_add(1, Ordering::Relaxed);
                (v, log_store.read_commit_entry(v).await)
            }
        })
        .buffered(IN_FLIGHT);
    while let Some((v, read)) = commits.next().await {
        let bytes = match read {
            Ok(Some(bytes)) => bytes,
            // Removed by log cleanup: below a gap, a txn is not known to be the newest.
            Ok(None) => break,
            Err(e) if is_outage(&e) => return Err(e.into()),
            Err(e) => {
                warn!(
                    table = %table.table_url(),
                    version = v,
                    "cannot read this commit below the checkpoint ({e}); the offsets whose marks \
                     were looked for below it resume unverified"
                );
                break;
            }
        };
        let mut met = BTreeMap::new();
        if let Err(e) = record_commit(table, v, &bytes, &answered, &mut met) {
            warn!(
                table = %table.table_url(),
                version = v,
                "cannot parse this commit below the checkpoint ({e}); the offsets whose marks \
                 were looked for below it resume unverified"
            );
            break;
        }
        for (id, written) in met {
            answered.remove(id.as_str());
            let Some(answer) = found.get_mut(&id) else {
                continue;
            };
            let Some(version) = answer.version else {
                continue;
            };
            // Another version is an older txn than the one answered, which the commit at
            // `bottom` moved on before log cleanup took it. Its mark is not this one's.
            if written.version == answer.version {
                answer.mark = written.mark;
            }
            memo.found.insert(id, (version, answer.mark));
        }
        if answered.is_empty() {
            return Ok(());
        }
    }
    for id in &answered {
        if let Some(version) = found.get(*id).and_then(|a| a.version) {
            memo.found.insert(id.to_string(), (version, None));
        }
    }
    debug!(
        table = %table.table_url(),
        checkpoint_version = bottom,
        app_ids = answered.len(),
        "the commits below this checkpoint that wrote these offsets are gone, unreadable or too \
         far down; their resume cannot be checked against a mark"
    );
    Ok(())
}

/// Whether a failed read of a commit is the object store's fault that may pass: a 5xx or a
/// timeout after the store's own retries, a dropped connection, a failed task. Not one about
/// the commit or the credentials (refused, not found, an invalid path), which reading it again
/// gives again.
fn is_outage(e: &deltalake::DeltaTableError) -> bool {
    use deltalake::ObjectStoreError;
    matches!(
        e,
        deltalake::DeltaTableError::ObjectStore {
            source: ObjectStoreError::Generic { .. } | ObjectStoreError::JoinError { .. }
        }
    )
}

/// The appIds the kernel has to answer one at a time, because the checkpoint at the bottom of
/// the walk could not be read here.
///
/// The kernel reads the log segment the handle was loaded with, which does not depend on
/// that checkpoint at all when the handle predates it, and otherwise reads it in its own way.
/// Either way its answer is the table's.
async fn kernel_lookups(
    table: &DeltaTable,
    rest: &BTreeSet<&str>,
    found: &mut BTreeMap<String, TxnState>,
    counters: &LookupCounters,
) -> Result<()> {
    let snapshot = table.snapshot().map_err(Error::Delta)?;
    let log_store = table.log_store();
    for id in rest {
        counters.fallbacks.fetch_add(1, Ordering::Relaxed);
        let version = snapshot
            .transaction_version(log_store.as_ref(), *id)
            .await
            .map_err(Error::Delta)?;
        if version.is_some() {
            found.insert(
                id.to_string(),
                TxnState {
                    version,
                    ..TxnState::default()
                },
            );
        }
    }
    Ok(())
}

/// One line of a commit or of a JSON checkpoint, reduced to the two actions read here.
///
/// Everything else on a line is skipped unparsed, so an action this build has never heard
/// of — the protocol keeps adding them — cannot make a commit unreadable here, as it can
/// through delta-rs's own `Action` enum.
#[derive(Deserialize)]
struct LogLine {
    txn: Option<TxnAction>,
    #[serde(rename = "commitInfo")]
    commit_info: Option<CommitInfo>,
}

/// A `txn` action, as far as it is read here: its `lastUpdated` is not, since nothing here
/// depends on who wrote a `txn` or when.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxnAction {
    app_id: String,
    version: i64,
}

/// The keys of a commitInfo this module reads. ddi writes them at its top level, beside
/// delta-rs's own.
#[derive(Deserialize)]
struct CommitInfo {
    #[serde(rename = "ddi.appId")]
    app_id: Option<serde_json::Value>,
    #[serde(rename = "ddi.kafka.marks")]
    marks: Option<serde_json::Value>,
}

/// Add the wanted `txn`s commit `version` writes to `found`, unless a newer commit already
/// did.
fn record_commit(
    table: &DeltaTable,
    version: u64,
    bytes: &[u8],
    wanted: &BTreeSet<&str>,
    found: &mut BTreeMap<String, TxnState>,
) -> Result<()> {
    let mut txns = Vec::new();
    let mut info = None;
    for line in serde_json::Deserializer::from_slice(bytes).into_iter::<LogLine>() {
        let line = line.map_err(|e| {
            Error::Other(format!(
                "cannot read the offsets in {}: commit {version} is not valid JSON: {e}",
                table.table_url()
            ))
        })?;
        if let Some(txn) = line.txn {
            if wanted.contains(txn.app_id.as_str()) && !found.contains_key(&txn.app_id) {
                txns.push(txn);
            }
        }
        if line.commit_info.is_some() {
            info = line.commit_info;
        }
    }
    let ddi_app_id = info
        .as_ref()
        .and_then(|i| i.app_id.as_ref())
        .and_then(serde_json::Value::as_str);
    let marks = info.as_ref().and_then(|i| i.marks.as_ref());
    for txn in txns {
        let mark = ddi_app_id.and_then(|app_id| mark_for(marks, app_id, &txn));
        found.insert(
            txn.app_id,
            TxnState {
                version: Some(txn.version),
                mark,
            },
        );
    }
    Ok(())
}

/// The mark `ddi.kafka.marks` records for the partition `txn` names, when it describes the
/// offset the `txn` holds.
///
/// A txn `<app_id>-<p>` in a commit of app_id `<app_id>` is partition p's; anything else in
/// that commit (a data-quality mark, another pipeline's offset) has no mark here.
fn mark_for(
    marks: Option<&serde_json::Value>,
    app_id: &str,
    txn: &TxnAction,
) -> Option<MessageMark> {
    let partition = partition_of(app_id, &txn.app_id)?.to_string();
    let [offset, timestamp_ms, value_len] = marks?.get(&partition)?.as_array()?.as_slice() else {
        return None;
    };
    let mark = MessageMark {
        offset: offset.as_i64()?,
        timestamp_ms: timestamp_ms.as_i64()?,
        value_len: u32::try_from(value_len.as_u64()?).ok()?,
    };
    (mark.offset == txn.version).then_some(mark)
}

/// Whether a `txn` with this appId can have a mark beside it: one that names a partition,
/// `<a>-<p>`. The takeover marker and a Delta pipeline's offset cannot, so the walk below a
/// checkpoint is not sent after them.
fn may_carry_a_mark(app_id: &str) -> bool {
    app_id
        .rsplit_once('-')
        .is_some_and(|(a, p)| partition_of(a, app_id).is_some() && !p.is_empty())
}

/// A complete checkpoint: every file that makes it up.
#[derive(Debug)]
struct Checkpoint {
    version: u64,
    /// One file, or every part of a multi-part checkpoint, in part order.
    files: Vec<ObjectMeta>,
}

/// What a file's name in `_delta_log` says it is, for the checkpoints among them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointFile {
    /// `<v>.checkpoint.parquet`: the classic single file. Its content may be a V2 checkpoint
    /// with sidecars, whose top-level file still holds every `txn`.
    Single,
    /// `<v>.checkpoint.<uuid>.json` or `.parquet`: a V2 checkpoint's top-level file.
    /// Sidecars hold only `add` and `remove` actions, so the `txn`s are all in here.
    Uuid,
    /// `<v>.checkpoint.<part>.<parts>.parquet`, one part of a multi-part checkpoint.
    Part { part: u32, parts: u32 },
}

/// Parse a listed file as a checkpoint file, by the same rules the kernel's log listing uses.
fn checkpoint_file(meta: &ObjectMeta, log_path: &Path) -> Option<(u64, CheckpointFile)> {
    let mut rest = meta.location.prefix_match(log_path)?;
    let name = rest.next()?;
    if rest.next().is_some() {
        return None; // _sidecars/, _staged_commits/: not a checkpoint's top-level file
    }
    let mut split = name.as_ref().split('.');
    let version = split.next()?;
    if version.len() != 20 {
        return None;
    }
    let version = version.parse::<u64>().ok()?;
    let kind = match split.collect::<Vec<_>>().as_slice() {
        ["checkpoint", "parquet"] => CheckpointFile::Single,
        ["checkpoint", uuid, "json" | "parquet"] if uuid.len() == 36 => CheckpointFile::Uuid,
        ["checkpoint", part, parts, "parquet"] if part.len() == 10 && parts.len() == 10 => {
            let (part, parts) = (part.parse::<u32>().ok()?, parts.parse::<u32>().ok()?);
            if part == 0 || part > parts {
                return None;
            }
            CheckpointFile::Part { part, parts }
        }
        _ => return None,
    };
    Some((version, kind))
}

/// The newest complete checkpoint at or below `version`, if the log has one.
///
/// `_last_checkpoint` is a hint, and is treated as one: it may be missing, name a checkpoint
/// that has since been deleted, or trail a newer one another engine wrote without updating
/// it. So it only says where to start listing. The listing from there finds the newest
/// checkpoint that is really there and complete; if none is, the log is listed from version
/// 0, as the kernel does without a hint.
async fn newest_checkpoint(
    store: &dyn ObjectStore,
    log_path: &Path,
    version: u64,
) -> Result<Option<Checkpoint>> {
    let from = match last_checkpoint_hint(store, log_path).await? {
        Some(hint) if hint <= version => hint,
        _ => version.saturating_sub(LISTING_WINDOW - 1),
    };
    if let Some(found) = checkpoint_between(store, log_path, from, version).await? {
        return Ok(Some(found));
    }
    if from == 0 {
        return Ok(None);
    }
    checkpoint_between(store, log_path, 0, version).await
}

/// The version `_last_checkpoint` names, if it exists and can be read.
async fn last_checkpoint_hint(store: &dyn ObjectStore, log_path: &Path) -> Result<Option<u64>> {
    #[derive(Deserialize)]
    struct Hint {
        version: u64,
    }
    let path = log_path.clone().join("_last_checkpoint");
    let bytes = match store.get(&path).await {
        Ok(got) => got.bytes().await.map_err(store_error)?,
        Err(deltalake::ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(store_error(e)),
    };
    match serde_json::from_slice::<Hint>(&bytes) {
        Ok(hint) => Ok(Some(hint.version)),
        Err(e) => {
            debug!(path = %path, "ignoring an unreadable _last_checkpoint: {e}");
            Ok(None)
        }
    }
}

/// The newest complete checkpoint with a version in `from..=to`.
///
/// Reads the whole listing rather than stopping at `to`, because not every object store lists
/// in order. Empty files are skipped, as the kernel skips them: a checkpoint that failed half
/// way through writing is not one.
async fn checkpoint_between(
    store: &dyn ObjectStore,
    log_path: &Path,
    from: u64,
    to: u64,
) -> Result<Option<Checkpoint>> {
    // Every file of version `from` sorts after its bare 20-digit prefix, so this lists them.
    let offset = log_path.clone().join(format!("{from:020}").as_str());
    let mut by_version: BTreeMap<u64, Vec<(CheckpointFile, ObjectMeta)>> = BTreeMap::new();
    let mut listing = store.list_with_offset(Some(log_path), &offset);
    while let Some(meta) = listing.try_next().await.map_err(store_error)? {
        let Some((version, kind)) = checkpoint_file(&meta, log_path) else {
            continue;
        };
        if (from..=to).contains(&version) && meta.size > 0 {
            by_version.entry(version).or_default().push((kind, meta));
        }
    }
    Ok(by_version
        .into_iter()
        .rev()
        .find_map(|(version, files)| complete(version, files)))
}

/// A complete checkpoint among `files`, all of one version, if there is one.
///
/// Every complete checkpoint of a version describes the same state, so which one is read
/// does not matter; a single file is preferred because it is one read.
fn complete(version: u64, files: Vec<(CheckpointFile, ObjectMeta)>) -> Option<Checkpoint> {
    let single = |want: CheckpointFile| {
        files
            .iter()
            .find(|(kind, _)| *kind == want)
            .map(|(_, meta)| Checkpoint {
                version,
                files: vec![meta.clone()],
            })
    };
    if let Some(found) = single(CheckpointFile::Single).or_else(|| single(CheckpointFile::Uuid)) {
        return Some(found);
    }
    let mut sets: BTreeMap<u32, BTreeMap<u32, ObjectMeta>> = BTreeMap::new();
    for (kind, meta) in files {
        if let CheckpointFile::Part { part, parts } = kind {
            sets.entry(parts).or_default().insert(part, meta);
        }
    }
    sets.into_iter()
        .find(|(parts, set)| set.len() == *parts as usize)
        .map(|(_, set)| Checkpoint {
            version,
            files: set.into_values().collect(),
        })
}

/// One `txn` of a checkpoint: its appId and version.
type CheckpointTxn = (String, i64);

/// The version each wanted appId has in `checkpoint`, or why the checkpoint cannot be read.
///
/// Reads the `txn` column and nothing else: a parquet checkpoint is projected to it, so the
/// columns another engine writes in its own way (`stats_parsed` above all) are never decoded.
/// The `Err` is a reason for the warning, not an error to return: any checkpoint this cannot
/// read sends the appIds to the kernel instead.
async fn checkpoint_txns(
    store: Arc<dyn ObjectStore>,
    checkpoint: &Checkpoint,
    wanted: &BTreeSet<&str>,
) -> std::result::Result<BTreeMap<String, i64>, String> {
    let mut txns = BTreeMap::new();
    for file in &checkpoint.files {
        let in_file = if file.location.as_ref().ends_with(".json") {
            json_txns(store.as_ref(), file).await?
        } else {
            parquet_txns(store.clone(), file).await?
        };
        for (id, version) in in_file {
            if !wanted.contains(id.as_str()) {
                continue;
            }
            // A checkpoint is the reconciled state: one txn per appId. Two that disagree mean
            // it is not a checkpoint this can read.
            if let Some(other) = txns.insert(id.clone(), version) {
                if other != version {
                    return Err(format!(
                        "it holds two txns for appId {id:?}, versions {other} and {version}"
                    ));
                }
            }
        }
    }
    Ok(txns)
}

/// Every `txn` in a JSON checkpoint file.
async fn json_txns(
    store: &dyn ObjectStore,
    file: &ObjectMeta,
) -> std::result::Result<Vec<CheckpointTxn>, String> {
    let bytes = store
        .get(&file.location)
        .await
        .map_err(|e| e.to_string())?
        .bytes()
        .await
        .map_err(|e| e.to_string())?;
    let mut txns = Vec::new();
    for line in serde_json::Deserializer::from_slice(&bytes).into_iter::<LogLine>() {
        if let Some(txn) = line.map_err(|e| e.to_string())?.txn {
            txns.push((txn.app_id, txn.version));
        }
    }
    Ok(txns)
}

/// Every `txn` in a parquet checkpoint file, read through its `txn` column alone.
async fn parquet_txns(
    store: Arc<dyn ObjectStore>,
    file: &ObjectMeta,
) -> std::result::Result<Vec<CheckpointTxn>, String> {
    use deltalake::parquet::arrow::async_reader::{
        ParquetObjectReader, ParquetRecordBatchStreamBuilder,
    };
    use deltalake::parquet::arrow::ProjectionMask;

    let reader = ParquetObjectReader::new(store, file.location.clone()).with_file_size(file.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| e.to_string())?;
    let column = builder
        .schema()
        .index_of("txn")
        .map_err(|_| "it has no txn column".to_string())?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), [column]);
    let batches: Vec<RecordBatch> = builder
        .with_projection(projection)
        .build()
        .map_err(|e| e.to_string())?
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;

    let mut txns = Vec::new();
    for batch in &batches {
        let txn = batch
            .column(0)
            .as_struct_opt()
            .ok_or("its txn column is not a struct")?;
        let field = |name: &str| {
            txn.column_by_name(name)
                .ok_or(format!("its txn column has no {name} field"))
        };
        let (app_ids, versions) = (field("appId")?, field("version")?);
        if !matches!(
            app_ids.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) || !matches!(versions.data_type(), DataType::Int64 | DataType::Int32)
        {
            return Err(format!(
                "its txn column is {:?}, not appId: string, version: long",
                txn.data_type()
            ));
        }
        // Lossless both ways: text to text, and a narrower integer to a wider one.
        let cast = |a: &Arc<dyn Array>, to: &DataType| {
            deltalake::arrow::compute::cast(a, to).map_err(|e| e.to_string())
        };
        let app_ids = cast(app_ids, &DataType::Utf8)?;
        let app_ids = app_ids.as_string::<i32>();
        let versions = cast(versions, &DataType::Int64)?;
        let versions = versions.as_primitive::<Int64Type>();
        // `lastUpdated` is not read: checkpoint writers do not agree on it (delta-rs up to
        // 0.17.3 nulls it, 0.26.2 has no such column, Trino writes 0 for a txn without one),
        // and nothing here needs it.
        for row in 0..txn.len() {
            if txn.is_null(row) || app_ids.is_null(row) {
                continue; // a row for another kind of action
            }
            if versions.is_null(row) {
                return Err(format!(
                    "its txn for appId {:?} has no version",
                    app_ids.value(row)
                ));
            }
            txns.push((app_ids.value(row).to_string(), versions.value(row)));
        }
    }
    Ok(txns)
}

fn store_error(e: deltalake::ObjectStoreError) -> Error {
    Error::Delta(e.into())
}

fn not_loaded(table: &DeltaTable) -> Error {
    Error::Other(format!(
        "internal: the offsets in {} were asked for on a handle that has not loaded the table",
        table.table_url()
    ))
}

fn missing_commit(table: &DeltaTable, version: u64) -> Error {
    Error::OffsetLogTruncated {
        table_uri: table.table_url().to_string(),
        version,
    }
}

/// The offsets one writer holds for the partitions it owns, and the `txn`s each of its
/// commits carries.
///
/// The rules, from docs/kafka-design.md §4.3:
/// - Every commit asserts every owned partition whose offset is known: the batch's own at
///   their new offset, every other at its unchanged one. delta-rs then refuses the commit if
///   anybody wrote any of them after the commit's snapshot.
/// - What delta-rs does not check is the snapshot itself. A partition the handle's version
///   has been read for, and every commit of ours has asserted since, is *carried*: its offset
///   at the handle's version is known. One that is not — a partition without an offset, which
///   no commit asserts, after a commit of ours moved the handle over somebody else's — is
///   looked up at the commit's snapshot before it carries data ([`Self::verify`]).
/// - The handle a commit is made from changes only through our own commits, which leave
///   carried exactly what they asserted ([`Self::committed`]), and through deliberate
///   refreshes, each followed by [`Self::reset`].
/// - The first commit whose read snapshot lacks the takeover marker writes it, and no commit
///   after ([`Self::takeover_marker_due`]).
#[derive(Debug)]
pub struct Fence {
    app_id: String,
    /// For each owned partition, the last offset durably in the target, and the message at it
    /// when known. `None` for a partition with no offset yet.
    base: BTreeMap<i32, (Option<i64>, Option<MessageMark>)>,
    /// Owned partitions whose offset is known at the handle's version: read there by
    /// [`Self::reset`] or [`Self::verify`], or asserted by every commit since.
    carried: BTreeSet<i32>,
    /// Whether the takeover marker ([`takeover_marker_app_id`]) is in the table at the
    /// handle's version: read there by [`Self::reset`] or [`Self::verify`], or written by a
    /// commit of ours since. `None` until a read succeeds.
    ///
    /// Once `Some(true)`, not read again: nothing removes a marker, so it is in the table at
    /// every later version, whichever handle reads it. That keeps a reset at our own newest
    /// commit, which does not carry it, to the one GET. Read again only after the target was
    /// refused for `delta.setTransactionRetentionDuration`, the one property under which a
    /// `txn` can expire ([`takeover_marker_app_id`] says how the marker still could).
    marker: Option<bool>,
    /// What [`read_txns`]' walk below the newest checkpoint found for the owned partitions.
    below: BelowCheckpoint,
    lookups: Arc<LookupCounters>,
}

impl Fence {
    /// A fence for `app_id` that owns nothing yet.
    pub fn new(app_id: impl Into<String>) -> Self {
        Self::with_lookups(app_id, Arc::new(LookupCounters::default()))
    }

    /// As [`Self::new`], adding what its reads cost to `lookups`: counters that outlive it,
    /// such as a pipeline's metrics, which a reopened consumer's new fence keeps adding to.
    pub fn with_lookups(app_id: impl Into<String>, lookups: Arc<LookupCounters>) -> Self {
        Self {
            app_id: app_id.into(),
            base: BTreeMap::new(),
            carried: BTreeSet::new(),
            marker: None,
            below: BelowCheckpoint::default(),
            lookups,
        }
    }

    /// The partitions this fence owns.
    pub fn owned(&self) -> Vec<i32> {
        self.base.keys().copied().collect()
    }

    /// Whether the next commit writes the takeover marker: it was not in the table at the
    /// handle's version (docs/kafka-design.md §4.3).
    ///
    /// Exact once [`Self::reset`] or [`Self::verify`] has read the handle's version, as the
    /// latter does before every commit. Before any read it says yes: writing the marker where
    /// it already is costs at most one `ConcurrentTransaction` against another member doing
    /// the same, while leaving it out of the table would make the next open check a takeover
    /// that has already happened.
    ///
    /// Two members that both find it absent both write it, and delta-rs refuses the second
    /// commit (`ConcurrentTransaction`), as it refuses any commit carrying a `txn` that another
    /// wrote after its snapshot. That member replans, finds the marker, and writes it no more.
    pub fn takeover_marker_due(&self) -> bool {
        self.marker != Some(true)
    }

    /// The appIds a read at the handle's version asks about for `partitions`, and the takeover
    /// marker's when it is not known to be there.
    fn ids_to_read(&self, partitions: &[i32]) -> Vec<String> {
        let mut ids: Vec<String> = partitions
            .iter()
            .map(|p| txn_app_id(&self.app_id, *p))
            .collect();
        if self.marker != Some(true) {
            ids.push(takeover_marker_app_id(&self.app_id));
        }
        ids
    }

    /// Take the takeover marker's state from a read at the handle's version that asked for it.
    fn read_marker(&mut self, txns: &BTreeMap<String, TxnState>) {
        if let Some(txn) = txns.get(&takeover_marker_app_id(&self.app_id)) {
            self.marker = Some(txn.version.is_some());
        }
    }

    /// The counters every read this fence makes adds to.
    pub fn lookups(&self) -> &Arc<LookupCounters> {
        &self.lookups
    }

    /// An owned partition's offset in the target and the message at it, or `None` when the
    /// partition is not owned.
    pub fn base(&self, partition: i32) -> Option<(Option<i64>, Option<MessageMark>)> {
        self.base.get(&partition).copied()
    }

    /// Own exactly `owned`, with each partition's offset as of `table`'s version, read in one
    /// pass ([`read_txns`]), and the mark beside it: from below a checkpoint too, when the
    /// checkpoint answered the offset (step 6). A partition left without one resumes
    /// Unverifiable (docs/kafka-design.md §4.4) until it carries data again.
    ///
    /// Every owned partition is then carried: all of them were just read at the handle's
    /// version. So is the takeover marker, in the same pass, unless it is known to be there
    /// already. On an error nothing is carried, so that no offset is trusted until a reset
    /// succeeds, and the marker is not known unless it was before; after a refusal of
    /// `delta.setTransactionRetentionDuration` not even then, so that a marker the property
    /// expired is found missing and written again.
    ///
    /// An owned partition this fence held at an offset that the table no longer has is
    /// [`Error::KafkaOffsetVanished`], and nothing is carried. Nothing removes a `txn` but
    /// expiry under that property, so the partition's rows up to that offset are in the table,
    /// and starting it as a partition without an offset would read them again. The fence keeps
    /// the offset it held, so every reset after refuses the same until the table has one again.
    pub async fn reset(&mut self, table: &DeltaTable, owned: &[i32]) -> Result<()> {
        let ids = self.ids_to_read(owned);
        let read = match refuse_txn_retention(table) {
            Ok(()) => read(table, &ids, &self.lookups, Below::Marks(&mut self.below)).await,
            Err(e) => Err(e),
        };
        let txns = match read {
            Ok(txns) => txns,
            Err(e) => {
                self.carried.clear();
                if self.marker == Some(false) || matches!(e, Error::KafkaTxnRetention { .. }) {
                    self.marker = None;
                }
                return Err(e);
            }
        };
        self.read_marker(&txns);
        let vanished: Vec<(i32, i64)> = owned
            .iter()
            .zip(&ids)
            .filter_map(|(p, id)| {
                let held = self.base.get(p)?.0?;
                let now = txns.get(id).and_then(|t| t.version);
                now.is_none().then_some((*p, held))
            })
            .collect();
        if !vanished.is_empty() {
            self.carried.clear();
            return Err(Error::KafkaOffsetVanished {
                target_uri: table.table_url().to_string(),
                app_id: self.app_id.clone(),
                partitions: vanished,
            });
        }
        self.base = owned
            .iter()
            .zip(&ids)
            .map(|(p, id)| {
                let txn = txns.get(id).cloned().unwrap_or_default();
                (*p, (txn.version, txn.mark))
            })
            .collect();
        self.carried = owned.iter().copied().collect();
        Ok(())
    }

    /// Before a commit of a batch that read `ranges`: every partition of it that is not
    /// carried must hold, at `table`'s version, the offset this fence has for it.
    ///
    /// `table` is the handle the commit will be planned from, which is its snapshot. In
    /// steady state every batch partition is carried and nothing is read. A mismatch is
    /// [`Error::Fenced`]: somebody else committed the partition, and the batch was read from
    /// the wrong place.
    ///
    /// The takeover marker is read in the same pass when it is not known at the handle's
    /// version, so that [`Self::takeover_marker_due`] is exact for the commit that follows.
    pub async fn verify(
        &mut self,
        table: &DeltaTable,
        ranges: &BTreeMap<i32, (i64, i64)>,
    ) -> Result<()> {
        let unverified: Vec<i32> = ranges
            .keys()
            .copied()
            .filter(|p| !self.carried.contains(p))
            .collect();
        if unverified.is_empty() && self.marker.is_some() {
            return Ok(());
        }
        if let Some(p) = unverified.iter().find(|p| !self.base.contains_key(p)) {
            return Err(Error::Other(format!(
                "internal: a batch for app_id {:?} read partition {p}, which this writer does \
                 not own",
                self.app_id
            )));
        }
        let mut ids: Vec<String> = unverified
            .iter()
            .map(|p| txn_app_id(&self.app_id, *p))
            .collect();
        if self.marker.is_none() {
            ids.push(takeover_marker_app_id(&self.app_id));
        }
        // [`read_txns`] without its step 6: only the versions are compared.
        if let Err(e) = refuse_txn_retention(table) {
            // As in `reset`: under the property the marker may expire, so it is read again.
            self.marker = None;
            return Err(e);
        }
        let txns = read(table, &ids, &self.lookups, Below::Nothing).await?;
        self.read_marker(&txns);
        for (p, id) in unverified.iter().zip(&ids) {
            let expected = self.base[p].0;
            let found = txns.get(id).and_then(|t| t.version);
            if found != expected {
                return Err(Error::Fenced {
                    app_id: self.app_id.clone(),
                    partition: *p,
                    expected,
                    found,
                });
            }
        }
        self.carried.extend(unverified);
        Ok(())
    }

    /// The `txn`s, and the marks beside them, of a commit of a batch that read `ranges`.
    ///
    /// Each batch partition at the last offset it read, with the mark of the message there
    /// from `marks`. Every other carried partition with a known offset at that offset, with
    /// its mark. A partition that is not carried is not re-asserted: its offset is not known
    /// at the handle's version, and asserting a stale one would move it.
    ///
    /// Every partition with a known offset is carried (a reset carries all, and every commit
    /// asserts and so carries again each one it had), so every commit of an owner holds the
    /// mark of every partition it owns whose mark is known: its newest commit is all a
    /// [`read_txns`] below a checkpoint needs to find.
    ///
    /// A mark is kept only where it describes the offset it sits beside.
    pub fn assert_map(
        &self,
        ranges: &BTreeMap<i32, (i64, i64)>,
        marks: &BTreeMap<i32, MessageMark>,
    ) -> (BTreeMap<i32, i64>, BTreeMap<i32, MessageMark>) {
        let mut assert = BTreeMap::new();
        let mut asserted_marks = BTreeMap::new();
        for (p, (_, last)) in ranges {
            assert.insert(*p, *last);
            if let Some(mark) = marks.get(p).filter(|m| m.offset == *last) {
                asserted_marks.insert(*p, *mark);
            }
        }
        for p in &self.carried {
            if ranges.contains_key(p) {
                continue;
            }
            if let Some((Some(base), mark)) = self.base.get(p) {
                assert.insert(*p, *base);
                if let Some(mark) = mark.filter(|m| m.offset == *base) {
                    asserted_marks.insert(*p, mark);
                }
            }
        }
        (assert, asserted_marks)
    }

    /// A commit carrying `offsets` landed, and the handle is now the one it returned.
    ///
    /// Every owned partition it asserted now has that offset, and is carried; nothing else
    /// is, because the commit may have been rebased over another writer's, which this fence
    /// has not read. A partition forgotten since the commit was built stays forgotten. The
    /// takeover marker is in the table if the commit wrote it; otherwise what this fence knew
    /// of it stands, which is all [`Self::takeover_marker_due`] needs (one that was there is
    /// there still).
    pub fn committed(&mut self, offsets: &KafkaOffsets) {
        if offsets.takeover_marker {
            self.marker = Some(true);
        }
        let mut carried = BTreeSet::new();
        for (p, version) in &offsets.assert {
            if let Some(base) = self.base.get_mut(p) {
                let mark = offsets
                    .marks
                    .get(p)
                    .copied()
                    .filter(|m| m.offset == *version);
                *base = (Some(*version), mark);
                carried.insert(*p);
            }
        }
        self.carried = carried;
    }

    /// Stop owning `partition`, which was revoked: no commit of this writer asserts it again
    /// (I6).
    pub fn forget(&mut self, partition: i32) {
        self.base.remove(&partition);
        self.carried.remove(&partition);
    }
}

/// The appIds of every `txn` in `table`'s newest `max_commits` commits and in its newest
/// checkpoint: what the typo guard compares a fresh app_id against (docs/kafka-design.md §4.4
/// step 7).
///
/// Evidence for a refusal and its message, not an offset, so it is lenient where
/// [`read_txns`] is strict: the walk stops at the first commit log cleanup removed, a line
/// that does not parse is skipped, and a checkpoint that cannot be read here is left out with
/// a warning. Leaving an appId out can only make the guard refuse where it might not have,
/// never let a typo through.
pub async fn recent_txn_app_ids(table: &DeltaTable, max_commits: u64) -> Result<BTreeSet<String>> {
    let head = table.version().ok_or_else(|| not_loaded(table))?;
    let log_store = table.log_store();
    let mut ids = BTreeSet::new();
    let bottom = head.saturating_sub(max_commits.max(1) - 1);
    let mut commits = futures::stream::iter((bottom..=head).rev())
        .map(|v| {
            let log_store = log_store.clone();
            async move { log_store.read_commit_entry(v).await }
        })
        .buffered(IN_FLIGHT);
    while let Some(read) = commits.next().await {
        let Some(bytes) = read? else { break };
        for line in serde_json::Deserializer::from_slice(&bytes).into_iter::<LogLine>() {
            if let Ok(LogLine { txn: Some(txn), .. }) = line {
                ids.insert(txn.app_id);
            }
        }
    }
    let store = crate::storage::with_every_checkpoint(log_store.as_ref());
    if let Some(checkpoint) = newest_checkpoint(store.as_ref(), log_store.log_path(), head).await? {
        for file in &checkpoint.files {
            let in_file = if file.location.as_ref().ends_with(".json") {
                json_txns(store.as_ref(), file).await
            } else {
                parquet_txns(store.clone(), file).await
            };
            match in_file {
                Ok(txns) => ids.extend(txns.into_iter().map(|(id, _)| id)),
                Err(why) => warn!(
                    table = %table.table_url(),
                    checkpoint = %file.location,
                    "cannot read the txn column of this checkpoint ({why}); the appIds it holds \
                     are left out of the typo guard's evidence"
                ),
            }
        }
    }
    Ok(ids)
}

/// Whether the commit at `table`'s own version is still in its log: one HEAD.
///
/// A writer about to commit from a handle at version `v` writes version `v + 1` if nothing is
/// there. Under a table dropped and recreated in place, nothing is, and the commit would land in
/// the new table at the old one's next version. The new log lacks `v` itself, unless the new
/// table already has as many versions; a missing `v` is the cue to look at the table id
/// ([`stored_table_id`]). Log cleanup never removes the commit a handle refreshed within the
/// last ten minutes stands at (docs/kafka-design.md §4.3, aging).
pub async fn commit_exists(table: &DeltaTable) -> Result<bool> {
    let Some(version) = table.version() else {
        return Ok(true);
    };
    let log_store = table.log_store();
    let path = log_store
        .log_path()
        .clone()
        .join(format!("{version:020}.json").as_str());
    match log_store.object_store(None).head(&path).await {
        Ok(_) => Ok(true),
        Err(deltalake::ObjectStoreError::NotFound { .. }) => Ok(false),
        Err(e) => Err(store_error(e)),
    }
}

/// Whether a commit that landed after `table`'s version changes the protocol.
///
/// delta-rs's conflict check refuses a winning protocol only when it raises the reader
/// version or lowers the writer version (conflict_checker.rs:409-436), so an append is rebased
/// over another engine's writer-only upgrade (identity columns, check constraints, appendOnly)
/// and lands in a table this build may not write. A writer reads the commits after its handle's
/// version before committing, as the conflict check will (one 404 when there are none), and a
/// protocol among them is a conflict: the reset that follows asks the new protocol
/// ([`refuse_unwritable_protocol`]) before anything is written (EO-m19).
pub async fn protocol_changed_since(table: &DeltaTable) -> Result<bool> {
    #[derive(Deserialize)]
    struct Line {
        protocol: Option<serde_json::Value>,
    }
    let Some(mut version) = table.version() else {
        return Ok(false);
    };
    let log_store = table.log_store();
    loop {
        version += 1;
        let Some(bytes) = log_store.read_commit_entry(version).await? else {
            return Ok(false);
        };
        let changed = serde_json::Deserializer::from_slice(&bytes)
            .into_iter::<Line>()
            .any(|l| l.is_ok_and(|l| l.protocol.is_some()));
        if changed {
            return Ok(true);
        }
    }
}

/// The id of the table at `table`'s location as storage holds it now, not as the handle
/// remembers it: from commit 0's `metaData`, or from the newest checkpoint's where log cleanup
/// removed commit 0. `None` when neither can be read.
///
/// An incremental refresh cannot see a table dropped and recreated under a running writer: it
/// reads the commits after the handle's version, which the new table may not have, and never
/// reads a `metaData` again. Its handle keeps the old id, and the next commit would land in the
/// new table at the old table's next version. One GET (commit 0) answers whether the table is
/// still the one opened (docs/kafka-design.md §3.3 reset step 2, I18).
pub async fn stored_table_id(table: &DeltaTable) -> Result<Option<String>> {
    #[derive(Deserialize)]
    struct Line {
        #[serde(rename = "metaData")]
        metadata: Option<Meta>,
    }
    #[derive(Deserialize)]
    struct Meta {
        id: String,
    }
    let from_json = |bytes: &[u8]| {
        serde_json::Deserializer::from_slice(bytes)
            .into_iter::<Line>()
            .find_map(|l| l.ok().and_then(|l| l.metadata).map(|m| m.id))
    };
    let log_store = table.log_store();
    if let Some(bytes) = log_store.read_commit_entry(0).await? {
        return Ok(from_json(&bytes));
    }
    let store = crate::storage::with_every_checkpoint(log_store.as_ref());
    let head = table.version().unwrap_or(0);
    let Some(checkpoint) = newest_checkpoint(store.as_ref(), log_store.log_path(), head).await?
    else {
        return Ok(None);
    };
    for file in &checkpoint.files {
        if file.location.as_ref().ends_with(".json") {
            let bytes = store
                .get(&file.location)
                .await
                .map_err(store_error)?
                .bytes()
                .await
                .map_err(store_error)?;
            if let Some(id) = from_json(&bytes) {
                return Ok(Some(id));
            }
            continue;
        }
        if let Ok(Some(id)) = parquet_metadata_id(store.clone(), file).await {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

/// The `metaData.id` of a parquet checkpoint file, read through that column alone.
async fn parquet_metadata_id(
    store: Arc<dyn ObjectStore>,
    file: &ObjectMeta,
) -> std::result::Result<Option<String>, String> {
    use deltalake::parquet::arrow::async_reader::{
        ParquetObjectReader, ParquetRecordBatchStreamBuilder,
    };
    use deltalake::parquet::arrow::ProjectionMask;

    let reader = ParquetObjectReader::new(store, file.location.clone()).with_file_size(file.size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| e.to_string())?;
    let Ok(column) = builder.schema().index_of("metaData") else {
        return Ok(None);
    };
    let projection = ProjectionMask::roots(builder.parquet_schema(), [column]);
    let batches: Vec<RecordBatch> = builder
        .with_projection(projection)
        .build()
        .map_err(|e| e.to_string())?
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;
    for batch in &batches {
        let Some(meta) = batch.column(0).as_struct_opt() else {
            continue;
        };
        let Some(ids) = meta.column_by_name("id") else {
            continue;
        };
        let ids =
            deltalake::arrow::compute::cast(ids, &DataType::Utf8).map_err(|e| e.to_string())?;
        let ids = ids.as_string::<i32>();
        for row in 0..meta.len() {
            if meta.is_valid(row) && ids.is_valid(row) {
                return Ok(Some(ids.value(row).to_string()));
            }
        }
    }
    Ok(None)
}

/// Refuse a target that sets `delta.setTransactionRetentionDuration`.
///
/// Under it the kernel forgets a `txn` whose `lastUpdated` is at or before the cutoff, and
/// Spark one whose `lastUpdated` is not after it, a missing one included. kafka-delta-ingest
/// stamped every `txn` with the time it wrote it, so the offset of a partition it had not
/// written for that long would be forgotten — and a checkpoint written meanwhile drops it for
/// good. ddi's own carry [`KAFKA_TXN_LAST_UPDATED`], which no engine expires, but a table
/// under the property can hold ones that do, so it is refused when it is opened and on every
/// refresh, rather than read with an expiry filter (docs/kafka-design.md §4.6).
pub fn refuse_txn_retention(table: &DeltaTable) -> Result<()> {
    let snapshot = table.snapshot().map_err(Error::Delta)?;
    // Spelled as the protocol spells it everywhere that matters, but not every engine keeps
    // the case of a property it was given. Refusing too much is the safe side.
    let retention = snapshot
        .metadata()
        .configuration()
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(TXN_RETENTION));
    match retention {
        None => Ok(()),
        Some((_, retention)) => Err(Error::KafkaTxnRetention {
            target_uri: table.table_url().to_string(),
            retention: retention.clone(),
        }),
    }
}

/// Refuse a target this build of delta-rs cannot write.
///
/// delta-rs checks the protocol only once a commit is attempted, and reads a column-mapped
/// table it then refuses to write. Asking first turns a table another engine upgraded while
/// the pipeline ran into one clear error at open or refresh, rather than a failed commit
/// after every batch.
pub fn refuse_unwritable_protocol(table: &DeltaTable) -> Result<()> {
    let snapshot = table.snapshot().map_err(Error::Delta)?;
    let refuse = |detail: String| {
        Err(Error::TargetProtocolUnsupported {
            target_uri: table.table_url().to_string(),
            detail,
        })
    };
    if let Err(e) = PROTOCOL.can_write_to(snapshot) {
        return refuse(format!(
            "its protocol (reader {}, writer {}) asks for what delta-rs 0.32.4 cannot write: {e}",
            snapshot.protocol().min_reader_version(),
            snapshot.protocol().min_writer_version()
        ));
    }
    let mode = snapshot.metadata().configuration().get(COLUMN_MAPPING_MODE);
    if let Some(mode) = mode.filter(|m| !m.eq_ignore_ascii_case("none")) {
        return refuse(format!(
            "it uses column mapping ({COLUMN_MAPPING_MODE} = {mode}), which delta-rs reads but \
             does not write"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    use deltalake::arrow::datatypes::SchemaRef;
    use deltalake::kernel::transaction::{CommitConflictError, TransactionError};
    use deltalake::kernel::{DataType as DeltaType, PrimitiveType, StructField};
    use deltalake::logstore::object_store::{
        self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };
    use deltalake::protocol::SaveMode;
    use deltalake::{ensure_table_uri, DeltaTableError};
    use futures::stream::BoxStream;

    use crate::sink::Sink;

    const APP: &str = "app";

    async fn create_with(path: &str, configuration: &[(&str, &str)]) -> DeltaTable {
        DeltaTable::try_from_url(ensure_table_uri(path).unwrap())
            .await
            .unwrap()
            .create()
            .with_columns(vec![
                StructField::new("id", DeltaType::Primitive(PrimitiveType::Long), false),
                StructField::new("name", DeltaType::Primitive(PrimitiveType::String), true),
            ])
            .with_configuration(configuration.iter().map(|(k, v)| (*k, Some(*v))))
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .unwrap()
    }

    async fn create(path: &str) -> DeltaTable {
        create_with(path, &[]).await
    }

    async fn open(path: &str) -> DeltaTable {
        deltalake::open_table(ensure_table_uri(path).unwrap())
            .await
            .unwrap()
    }

    fn schema_of(table: &DeltaTable) -> SchemaRef {
        use deltalake::delta_datafusion::DataFusionMixins;
        table.snapshot().unwrap().snapshot().read_schema()
    }

    /// A batch that read `ranges` and asserts `assert`, with `marks` beside them.
    fn offsets(
        ranges: &[(i32, (i64, i64))],
        assert: &BTreeMap<i32, i64>,
        marks: &BTreeMap<i32, MessageMark>,
    ) -> KafkaOffsets {
        KafkaOffsets {
            topic: "orders".into(),
            group_id: "ddi-orders".into(),
            ranges: ranges.iter().copied().collect(),
            assert: assert.clone(),
            marks: marks.clone(),
            cleanup_logs: false,
            baseline: false,
            override_reason: None,
            takeover_marker: false,
        }
    }

    /// A txn-only commit of `app` asserting `assert` (partition, offset), with no rows.
    async fn assert_txns(table: DeltaTable, assert: &[(i32, i64)]) -> DeltaTable {
        let assert: BTreeMap<i32, i64> = assert.iter().copied().collect();
        let ranges: Vec<_> = assert.iter().map(|(p, o)| (*p, (*o, *o))).collect();
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new(APP, 0)
            .commit_kafka(
                table,
                vec![empty],
                &offsets(&ranges, &assert, &BTreeMap::new()),
            )
            .await
            .unwrap()
    }

    /// The commit a fence would make for a batch that read `ranges`, landed through the sink.
    async fn fenced_commit(
        fence: &mut Fence,
        table: DeltaTable,
        ranges: &[(i32, (i64, i64))],
    ) -> Result<DeltaTable> {
        let ranges_map: BTreeMap<i32, (i64, i64)> = ranges.iter().copied().collect();
        fence.verify(&table, &ranges_map).await?;
        let (assert, marks) = fence.assert_map(&ranges_map, &BTreeMap::new());
        let mut offsets = offsets(ranges, &assert, &marks);
        offsets.takeover_marker = fence.takeover_marker_due();
        let empty = RecordBatch::new_empty(schema_of(&table));
        let table = Sink::new(APP, 0)
            .commit_kafka(table, vec![empty], &offsets)
            .await?;
        fence.committed(&offsets);
        Ok(table)
    }

    /// A txn-only commit that writes the takeover marker and nothing else: a table ddi has
    /// taken over, before any partition of it has an offset.
    async fn take_over(table: DeltaTable) -> DeltaTable {
        let mut marked = offsets(&[], &BTreeMap::new(), &BTreeMap::new());
        marked.takeover_marker = true;
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new(APP, 0)
            .commit_kafka(table, vec![empty], &marked)
            .await
            .unwrap()
    }

    /// The txns commit `version` of the table at `dir` writes, (appId, version), read from its
    /// JSON as it lies in the log.
    fn txns_in_commit(dir: &std::path::Path, version: u64) -> Vec<(String, i64)> {
        let commit = std::fs::read_to_string(log_file(dir, &commit_name(version))).unwrap();
        commit
            .lines()
            .filter_map(|l| {
                let line: serde_json::Value = serde_json::from_str(l).unwrap();
                let txn = line.get("txn")?;
                Some((
                    txn["appId"].as_str().unwrap().to_string(),
                    txn["version"].as_i64().unwrap(),
                ))
            })
            .collect()
    }

    /// The `lastUpdated` of every txn commit `version` of the table at `dir` writes, by appId,
    /// read from its JSON as it lies in the log.
    fn stamps_in_commit(dir: &std::path::Path, version: u64) -> BTreeMap<String, Option<i64>> {
        let commit = std::fs::read_to_string(log_file(dir, &commit_name(version))).unwrap();
        commit
            .lines()
            .filter_map(|l| {
                let line: serde_json::Value = serde_json::from_str(l).unwrap();
                let txn = line.get("txn")?;
                Some((
                    txn["appId"].as_str().unwrap().to_string(),
                    txn.get("lastUpdated").and_then(serde_json::Value::as_i64),
                ))
            })
            .collect()
    }

    /// Set `delta.setTransactionRetentionDuration` to `duration` on the table at `path`, as
    /// another engine would, ddi refusing the table meanwhile.
    async fn set_retention(path: &str, duration: &str) -> DeltaTable {
        open(path)
            .await
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([(
                TXN_RETENTION.to_string(),
                duration.to_string(),
            )]))
            .await
            .unwrap()
    }

    fn ids(partitions: impl IntoIterator<Item = i32>) -> Vec<String> {
        partitions.into_iter().map(|p| txn_app_id(APP, p)).collect()
    }

    /// `read_txns`' versions, by partition.
    async fn versions(
        table: &DeltaTable,
        partitions: impl IntoIterator<Item = i32>,
        counters: &LookupCounters,
    ) -> BTreeMap<i32, Option<i64>> {
        let partitions: Vec<i32> = partitions.into_iter().collect();
        let txns = read_txns(table, &ids(partitions.clone()), counters)
            .await
            .unwrap();
        partitions
            .into_iter()
            .map(|p| (p, txns[&txn_app_id(APP, p)].version))
            .collect()
    }

    /// The same, from the kernel, one appId at a time: the reference `read_txns` must agree
    /// with.
    async fn kernel_versions(
        table: &DeltaTable,
        partitions: impl IntoIterator<Item = i32>,
    ) -> BTreeMap<i32, Option<i64>> {
        let mut out = BTreeMap::new();
        for p in partitions {
            let v = table
                .snapshot()
                .unwrap()
                .transaction_version(table.log_store().as_ref(), txn_app_id(APP, p))
                .await
                .unwrap();
            out.insert(p, v);
        }
        out
    }

    fn log_file(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        dir.join("_delta_log").join(name)
    }

    fn commit_name(version: u64) -> String {
        format!("{version:020}.json")
    }

    /// Write a commit by hand, as kafka-delta-ingest would have: a txn with `lastUpdated`, and
    /// a commitInfo without ddi's keys.
    fn kdi_commit(dir: &std::path::Path, version: u64, app_id: &str, offset: i64, at: i64) {
        let commit = format!(
            "{{\"commitInfo\":{{\"timestamp\":{at},\"operation\":\"STREAMING UPDATE\",\
             \"operationParameters\":{{\"outputMode\":\"Complete\",\"queryId\":\"kdi\",\
             \"epochId\":\"{offset}\"}},\"clientVersion\":\"delta-rs.0.26.0\"}}}}\n\
             {{\"txn\":{{\"appId\":\"{app_id}\",\"version\":{offset},\"lastUpdated\":{at}}}}}\n"
        );
        std::fs::write(log_file(dir, &commit_name(version)), commit).unwrap();
    }

    /// An object store that records what is asked of it, and passes everything through.
    #[derive(Debug)]
    struct Counting {
        inner: Arc<dyn ObjectStore>,
        gets: Mutex<Vec<String>>,
        heads: AtomicUsize,
        lists: AtomicUsize,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        /// GETs of these paths fail, as the store says.
        failing: Mutex<BTreeMap<String, Failure>>,
    }

    /// How a GET [`Counting`] is told to fail does.
    #[derive(Clone, Copy, Debug)]
    enum Failure {
        /// 403: the store refuses this object, and will again.
        Denied,
        /// An outage, after the store's own retries: it may pass.
        Outage,
    }

    impl Counting {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                gets: Mutex::new(Vec::new()),
                heads: AtomicUsize::new(0),
                lists: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                failing: Mutex::new(BTreeMap::new()),
            }
        }

        /// GETs of commit `version` fail with `failure` from now on.
        fn fail(&self, version: u64, failure: Failure) {
            self.failing
                .lock()
                .unwrap()
                .insert(format!("_delta_log/{}", commit_name(version)), failure);
        }

        fn reset(&self) {
            self.gets.lock().unwrap().clear();
            self.heads.store(0, Ordering::SeqCst);
            self.lists.store(0, Ordering::SeqCst);
            self.max_in_flight.store(0, Ordering::SeqCst);
        }

        fn gets(&self) -> Vec<String> {
            self.gets.lock().unwrap().clone()
        }

        /// The versions of the commit files that were fetched, in order.
        fn commits_read(&self) -> Vec<u64> {
            self.gets()
                .iter()
                .filter_map(|p| p.strip_prefix("_delta_log/")?.strip_suffix(".json"))
                .filter_map(|v| v.parse().ok())
                .collect()
        }
    }

    impl std::fmt::Display for Counting {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Counting({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Counting {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.heads.fetch_add(1, Ordering::SeqCst);
                return self.inner.get_opts(location, options).await;
            }
            self.gets.lock().unwrap().push(location.to_string());
            let failure = self.failing.lock().unwrap().get(location.as_ref()).copied();
            match failure {
                Some(Failure::Denied) => {
                    return Err(object_store::Error::PermissionDenied {
                        path: location.to_string(),
                        source: "403 This request is not authorized".into(),
                    })
                }
                Some(Failure::Outage) => {
                    return Err(object_store::Error::Generic {
                        store: "Counting",
                        source: "503 Server Busy, after 10 retries".into(),
                    })
                }
                None => {}
            }
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            let got = self.inner.get_opts(location, options).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            got
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The table at `path`, reached through a [`Counting`] store at its root.
    async fn counted(path: &str) -> (DeltaTable, Arc<Counting>) {
        let plain = open(path).await;
        let store = plain.log_store();
        let counting = Arc::new(Counting::new(store.object_store(None)));
        let log_store = deltalake::logstore::default_logstore(
            counting.clone(),
            store.root_object_store(None),
            store.config().location(),
            store.config().options(),
        );
        let mut table = DeltaTable::new(log_store, plain.config.clone());
        table.load().await.unwrap();
        counting.reset();
        (table, counting)
    }

    fn is_concurrent_transaction(e: &Error) -> bool {
        matches!(
            e,
            Error::Delta(DeltaTableError::Transaction {
                source: TransactionError::CommitConflict(
                    CommitConflictError::ConcurrentTransaction
                ),
            })
        )
    }

    #[test]
    fn a_partition_offset_is_named_as_kafka_delta_ingest_names_it() {
        assert_eq!(txn_app_id("orders", 0), "orders-0");
        assert_eq!(txn_app_id("orders", 12), "orders-12");
    }

    #[test]
    fn every_spelling_of_a_checkpoint_file_is_parsed_as_the_kernel_parses_it() {
        let log = Path::from("_delta_log");
        let parse = |name: &str| {
            let meta = ObjectMeta {
                location: Path::from(name),
                last_modified: chrono::Utc::now(),
                size: 1,
                e_tag: None,
                version: None,
            };
            checkpoint_file(&meta, &log)
        };
        let v = "00000000000000000010";
        assert_eq!(
            parse(&format!("_delta_log/{v}.checkpoint.parquet")),
            Some((10, CheckpointFile::Single))
        );
        let uuid = "3a0d65cd-4056-49b8-937b-95f9e3ee90e5";
        for ext in ["json", "parquet"] {
            assert_eq!(
                parse(&format!("_delta_log/{v}.checkpoint.{uuid}.{ext}")),
                Some((10, CheckpointFile::Uuid))
            );
        }
        assert_eq!(
            parse(&format!(
                "_delta_log/{v}.checkpoint.0000000002.0000000003.parquet"
            )),
            Some((10, CheckpointFile::Part { part: 2, parts: 3 }))
        );
        for not_one in [
            format!("_delta_log/{v}.json"),
            format!("_delta_log/{v}.crc"),
            format!("_delta_log/{v}.checkpoint.json"),
            format!("_delta_log/{v}.checkpoint.0000000000.0000000002.parquet"),
            format!("_delta_log/{v}.checkpoint.0000000003.0000000002.parquet"),
            format!("_delta_log/{v}.checkpoint.{uuid}.foo"),
            format!("_delta_log/_sidecars/{v}.checkpoint.parquet"),
            "_delta_log/0010.checkpoint.parquet".to_string(),
            "_delta_log/_last_checkpoint".to_string(),
        ] {
            assert_eq!(parse(&not_one), None, "{not_one}");
        }
    }

    #[tokio::test]
    async fn fence_disjoint_partitions_do_not_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        // Taken over already: the first commits' race to write the marker is
        // `two_members_racing_to_take_over_conflict_once`.
        take_over(create(path).await).await;

        // Two members of one group, each with its own handle on version 1.
        let (mut a_table, mut b_table) = (open(path).await, open(path).await);
        let (mut a, mut b) = (Fence::new(APP), Fence::new(APP));
        a.reset(&a_table, &[0, 1]).await.unwrap();
        b.reset(&b_table, &[2, 3]).await.unwrap();

        // Interleaved, each from a handle the other's last commit is not in: delta-rs rebases
        // every one of them, because no two assert the same appId.
        a_table = fenced_commit(&mut a, a_table, &[(0, (0, 9))])
            .await
            .unwrap();
        b_table = fenced_commit(&mut b, b_table, &[(2, (0, 4)), (3, (0, 6))])
            .await
            .unwrap();
        a_table = fenced_commit(&mut a, a_table, &[(0, (10, 12)), (1, (0, 3))])
            .await
            .unwrap();
        b_table = fenced_commit(&mut b, b_table, &[(2, (5, 8))])
            .await
            .unwrap();
        assert_eq!(a_table.version(), Some(4));
        assert_eq!(b_table.version(), Some(5));

        let table = open(path).await;
        let counters = LookupCounters::default();
        assert_eq!(
            versions(&table, 0..4, &counters).await,
            BTreeMap::from([(0, Some(12)), (1, Some(3)), (2, Some(8)), (3, Some(6))])
        );
        // B's last commit re-asserted 3 at its base, so it carries all of B's partitions.
        assert_eq!(b.base(3), Some((Some(6), None)));
        assert_eq!(a.base(1), Some((Some(3), None)));
    }

    #[tokio::test]
    async fn fence_base_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;

        // This member owns 0 and 1; 1 has no offset yet.
        let table = assert_txns(open(path).await, &[(0, 4)]).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        assert_eq!(fence.base(1), Some((None, None)));

        // A second deployment with the same app_id writes partition 1, and this member's next
        // commit, which does not assert 1 (it has no offset to assert), is rebased over it.
        assert_txns(open(path).await, &[(1, 7)]).await;
        let table = fenced_commit(&mut fence, table, &[(0, (5, 9))])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(3), "rebased over the other writer");

        // The handle now holds the other writer's offset for 1, which is not this fence's.
        let ranges = BTreeMap::from([(1, (0, 2))]);
        let e = fence.verify(&table, &ranges).await.unwrap_err();
        assert!(
            matches!(
                e,
                Error::Fenced {
                    partition: 1,
                    expected: None,
                    found: Some(7),
                    ..
                }
            ),
            "got: {e}"
        );

        // When the other writer lands only after the handle's version, the fence's read finds
        // nothing amiss, and it is delta-rs that refuses the commit: one layer or the other,
        // never neither.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 4)]).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        let table = fenced_commit(&mut fence, table, &[(0, (5, 9))])
            .await
            .unwrap();
        assert_txns(open(path).await, &[(1, 7)]).await;
        let e = fenced_commit(&mut fence, table, &[(1, (0, 2))])
            .await
            .unwrap_err();
        assert!(is_concurrent_transaction(&e), "got: {e}");
        let counters = LookupCounters::default();
        assert_eq!(
            versions(&open(path).await, [0, 1], &counters).await,
            BTreeMap::from([(0, Some(9)), (1, Some(7))])
        );
    }

    #[tokio::test]
    async fn none_partition_verified_before_first_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;

        let table = assert_txns(open(path).await, &[(0, 4)]).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        let after_reset = fence.lookups().lookups();
        assert_eq!(after_reset, 1, "one read for every owned partition");

        // Partition 1 has no offset, so this commit does not assert it, and the handle it
        // returns may hold a commit of somebody else's that does.
        let table = fenced_commit(&mut fence, table, &[(0, (5, 9))])
            .await
            .unwrap();
        assert_eq!(
            fence.lookups().lookups(),
            after_reset,
            "partition 0 was carried: nothing to read"
        );

        // So before 1 carries data, it is read at the commit's snapshot.
        let ranges = BTreeMap::from([(1, (0, 2))]);
        fence.verify(&table, &ranges).await.unwrap();
        assert_eq!(fence.lookups().lookups(), after_reset + 1);
        // Once read there, it is not read again on the same handle.
        fence.verify(&table, &ranges).await.unwrap();
        assert_eq!(fence.lookups().lookups(), after_reset + 1);

        let (assert, _) = fence.assert_map(&ranges, &BTreeMap::new());
        assert_eq!(assert, BTreeMap::from([(0, 9), (1, 2)]));
        let table = fenced_commit(&mut fence, table, &[(1, (0, 2))])
            .await
            .unwrap();

        // From here on both are asserted by every commit, and neither is read again.
        let table = fenced_commit(&mut fence, table, &[(0, (10, 11)), (1, (3, 3))])
            .await
            .unwrap();
        assert_eq!(fence.lookups().lookups(), after_reset + 1);
        let counters = LookupCounters::default();
        assert_eq!(
            versions(&table, [0, 1], &counters).await,
            BTreeMap::from([(0, Some(11)), (1, Some(3))])
        );
    }

    #[tokio::test]
    async fn revoked_partition_not_reasserted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;

        let table = take_over(open(path).await).await;
        let table = assert_txns(table, &[(0, 1), (1, 2), (2, 3)]).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1, 2]).await.unwrap();
        let ranges = BTreeMap::from([(0, (2, 5))]);
        // Built before the revoke, and committed after it.
        let (assert, marks) = fence.assert_map(&ranges, &BTreeMap::new());
        assert_eq!(assert, BTreeMap::from([(0, 5), (1, 2), (2, 3)]));
        let stale = offsets(&[(0, (2, 5))], &assert, &marks);

        fence.forget(1);
        fence.committed(&stale);
        assert_eq!(fence.base(1), None, "a commit does not bring it back");
        let (assert, _) = fence.assert_map(&ranges, &BTreeMap::new());
        assert_eq!(assert, BTreeMap::from([(0, 5), (2, 3)]));

        // The new owner moves 1 on; this member's later commits, from a handle that never saw
        // that, land beside it because they no longer assert 1.
        let mut new_owner = Fence::new(APP);
        let other = open(path).await;
        new_owner.reset(&other, &[1]).await.unwrap();
        fenced_commit(&mut new_owner, other, &[(1, (3, 8))])
            .await
            .unwrap();
        let table = fenced_commit(&mut fence, table, &[(0, (6, 7))])
            .await
            .unwrap();

        let txns: Vec<String> = {
            let raw = table
                .log_store()
                .read_commit_entry(table.version().unwrap())
                .await
                .unwrap()
                .unwrap();
            deltalake::logstore::get_actions(table.version().unwrap(), &raw)
                .unwrap()
                .into_iter()
                .filter_map(|a| match a {
                    deltalake::kernel::Action::Txn(t) => Some(t.app_id),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(txns, vec!["app-0".to_string(), "app-2".to_string()]);
        let counters = LookupCounters::default();
        assert_eq!(
            versions(&open(path).await, 0..3, &counters).await,
            BTreeMap::from([(0, Some(7)), (1, Some(8)), (2, Some(3))])
        );
    }

    #[tokio::test]
    async fn set_transaction_retention_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create_with(path, &[(TXN_RETENTION, "interval 1 days")]).await;

        let refused = |e: Error| match e {
            Error::KafkaTxnRetention { retention, .. } => retention == "interval 1 days",
            _ => false,
        };
        assert!(refused(refuse_txn_retention(&table).unwrap_err()));
        let counters = LookupCounters::default();
        assert!(refused(
            read_txns(&table, &ids([0]), &counters).await.unwrap_err()
        ));
        let mut fence = Fence::new(APP);
        assert!(refused(fence.reset(&table, &[0]).await.unwrap_err()));
        assert_eq!(counters.lookups(), 0, "refused before anything is read");

        let message = refuse_txn_retention(&table).unwrap_err().to_string();
        assert!(
            message.contains("delta.setTransactionRetentionDuration = interval 1 days"),
            "{message}"
        );

        let dir = tempfile::tempdir().unwrap();
        let plain = create(dir.path().to_str().unwrap()).await;
        refuse_txn_retention(&plain).unwrap();
    }

    /// What the refusal protects against, and why ddi's own offsets and its takeover marker
    /// need no protecting: one stamped [`KAFKA_TXN_LAST_UPDATED`] survives the property being
    /// set after the fact, and a checkpoint written under it; one with an old `lastUpdated`,
    /// as kafka-delta-ingest wrote them, does not.
    #[tokio::test]
    async fn ddi_txns_never_expire() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let mut first = offsets(&[(0, (5, 5))], &BTreeMap::from([(0, 5)]), &BTreeMap::new());
        first.takeover_marker = true;
        let table = open(path).await;
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new(APP, 0)
            .commit_kafka(table, vec![empty], &first)
            .await
            .unwrap();
        kdi_commit(dir.path(), 2, "app-1", 7, 1_000);
        let table = open(path)
            .await
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([(
                TXN_RETENTION.to_string(),
                "interval 1 seconds".to_string(),
            )]))
            .await
            .unwrap();
        assert_eq!(table.version(), Some(3));

        let reopened = open(path).await;
        assert_eq!(
            kernel_versions(&reopened, [0, 1]).await,
            BTreeMap::from([(0, Some(5)), (1, None)]),
            "ours kept, the kdi-style one expired"
        );
        deltalake::checkpoints::create_checkpoint(&reopened, None)
            .await
            .unwrap();
        let reopened = open(path).await;
        assert_eq!(
            kernel_versions(&reopened, [0, 1]).await,
            BTreeMap::from([(0, Some(5)), (1, None)]),
            "and a checkpoint written under the property keeps ours"
        );
        let marker = reopened
            .snapshot()
            .unwrap()
            .transaction_version(reopened.log_store().as_ref(), takeover_marker_app_id(APP))
            .await
            .unwrap();
        assert_eq!(marker, Some(TAKEOVER_MARKER_VERSION), "the marker too");

        assert!(matches!(
            refuse_txn_retention(&reopened),
            Err(Error::KafkaTxnRetention { .. })
        ));
    }

    #[tokio::test]
    async fn read_txns_one_get_in_steady_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        // Enough history, and a checkpoint, that anything but the head would show. The first
        // commit took the table over, and no later one carries the marker.
        let mut table = take_over(create(path).await).await; // 1
        for offset in 0..12 {
            table = assert_txns(table, &[(0, offset), (1, 100 + offset), (2, 200 + offset)]).await;
        } // 2..=13
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        assert_txns(table, &[(0, 12), (1, 112), (2, 212)]).await; // 14

        // A fence's first reset looks for the marker too, which the head does not carry: back
        // to the checkpoint, once in the fence's life.
        let (table, store) = counted(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1, 2]).await.unwrap();
        assert!(!fence.takeover_marker_due());
        assert!(store
            .gets()
            .contains(&"_delta_log/00000000000000000013.checkpoint.parquet".to_string()));

        // Every reset after it, a GET of the head and nothing else.
        store.reset();
        fence.reset(&table, &[0, 1, 2]).await.unwrap();
        assert_eq!(store.gets(), vec!["_delta_log/00000000000000000014.json"]);
        assert_eq!(store.lists.load(Ordering::SeqCst), 0);
        assert_eq!(store.heads.load(Ordering::SeqCst), 0);
        assert_eq!(fence.base(1), Some((Some(112), None)));

        // Batches of carried partitions are committed without reading an offset first.
        let mut table = table;
        for (first, last) in [(13, 20), (21, 30)] {
            table = fenced_commit(&mut fence, table, &[(0, (first, last)), (1, (first, last))])
                .await
                .unwrap();
        }
        assert_eq!(
            fence.lookups().lookups(),
            2,
            "only the resets read anything"
        );

        // And the walk after it still stops at the head.
        store.reset();
        let counters = LookupCounters::default();
        assert_eq!(
            versions(&table, 0..3, &counters).await,
            BTreeMap::from([(0, Some(30)), (1, Some(30)), (2, Some(212))])
        );
        assert_eq!(store.gets().len(), 1, "{:?}", store.gets());
    }

    #[tokio::test]
    async fn read_txns_walks_back_to_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 10), (1, 20), (2, 30)]).await; // 1
        let table = assert_txns(table, &[(0, 11)]).await; // 2
        let table = assert_txns(table, &[(0, 12)]).await; // 3
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        let table = assert_txns(table, &[(0, 13), (3, 40)]).await; // 4
        let table = assert_txns(table, &[(0, 14)]).await; // 5
        let table = assert_txns(table, &[(0, 15)]).await; // 6
        let table = assert_txns(table, &[(0, 16)]).await; // 7
        assert_txns(table, &[(0, 17)]).await; // 8

        let (table, store) = counted(path).await;
        let counters = LookupCounters::default();
        let got = versions(&table, 0..5, &counters).await;
        assert_eq!(
            got,
            BTreeMap::from([
                (0, Some(17)),
                (1, Some(20)),
                (2, Some(30)),
                (3, Some(40)),
                (4, None)
            ])
        );
        assert_eq!(got, kernel_versions(&table, 0..5).await);

        // Back to the checkpoint for the versions: 1 and 2 came out of its txn column. Then
        // below it for their marks, as far as commit 1, which wrote them; commit 0 at most in
        // flight when it was read.
        let mut read = store.commits_read();
        read.sort_unstable();
        read.retain(|v| *v != 0);
        assert_eq!(read, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let below = counters.below_checkpoint_reads();
        assert!((2..=3).contains(&below), "{below}");
        assert_eq!(
            counters.unverifiable(),
            4,
            "partitions 0 to 3: assert_txns records no marks"
        );
        assert!(store
            .gets()
            .iter()
            .any(|p| p == "_delta_log/00000000000000000003.checkpoint.parquet"));
        assert!(store
            .gets()
            .iter()
            .any(|p| p == "_delta_log/_last_checkpoint"));
        let in_flight = store.max_in_flight.load(Ordering::SeqCst);
        assert!(in_flight <= IN_FLIGHT, "{in_flight} GETs in flight at once");
        assert_eq!(counters.lookups(), 1, "one walk, for five appIds");
        assert_eq!(counters.fallbacks(), 0);

        // Where only the versions are wanted, as in `Fence::verify`, the walk goes no further
        // than the checkpoint.
        store.reset();
        super::read(&table, &ids(0..5), &counters, Below::Nothing)
            .await
            .unwrap();
        let mut read = store.commits_read();
        read.sort_unstable();
        assert_eq!(read, vec![3, 4, 5, 6, 7, 8]);
        assert_eq!(counters.below_checkpoint_reads(), below, "unchanged");

        // An appId the newer commits answer stops the walk before the checkpoint is read.
        store.reset();
        let txns = read_txns(&table, &ids([0, 3]), &counters).await.unwrap();
        assert_eq!(txns["app-3"].version, Some(40));
        assert!(
            !store.gets().iter().any(|p| p.contains(".checkpoint.")),
            "{:?}",
            store.gets()
        );

        // The table moves on past this handle, and checkpoints there. The handle's answer is
        // still the one at its own version: a checkpoint above it is not its state.
        let newer = assert_txns(open(path).await, &[(1, 21), (2, 31)]).await; // 9
        deltalake::checkpoints::create_checkpoint(&newer, None)
            .await
            .unwrap();
        assert_eq!(
            versions(&table, 0..5, &counters).await,
            BTreeMap::from([
                (0, Some(17)),
                (1, Some(20)),
                (2, Some(30)),
                (3, Some(40)),
                (4, None)
            ])
        );
    }

    /// A commit missing between the handle's version and any checkpoint is an error. Reading
    /// past it would answer with an offset older than the truth whenever the missing commit
    /// had moved it, and that offset would be re-asserted.
    #[tokio::test]
    async fn read_txns_missing_commit_is_an_error_not_an_older_answer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 1), (1, 2)]).await; // 1
        let table = assert_txns(table, &[(0, 3), (1, 5)]).await; // 2
        assert_txns(table, &[(0, 4)]).await; // 3
        let table = open(path).await;
        std::fs::remove_file(log_file(dir.path(), &commit_name(2))).unwrap();

        let counters = LookupCounters::default();
        // The head answers partition 0 by itself, so nothing below it is needed.
        assert_eq!(
            versions(&table, [0], &counters).await,
            BTreeMap::from([(0, Some(4))])
        );
        // Partition 1 is 5 since commit 2; commit 1 says 2.
        let e = read_txns(&table, &ids([1]), &counters).await.unwrap_err();
        assert!(
            e.to_string().contains("commit 2 is no longer in its log"),
            "{e}"
        );
    }

    /// Without a checkpoint the walk has to reach version 0, and that holds when version 0 is
    /// the handle's own: a table created with its offsets in commit 0, dropped and recreated
    /// at the same path under a handle still on it, has no commit 0 to read them from, which
    /// is not the same as having none.
    #[tokio::test]
    async fn read_txns_missing_commit_zero_is_an_error_at_version_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let zero = log_file(dir.path(), &commit_name(0));
        let mut commit = std::fs::read_to_string(&zero).unwrap();
        if !commit.ends_with('\n') {
            commit.push('\n');
        }
        commit.push_str("{\"txn\":{\"appId\":\"app-0\",\"version\":5}}\n");
        std::fs::write(&zero, commit).unwrap();

        let table = open(path).await;
        assert_eq!(table.version(), Some(0));
        let counters = LookupCounters::default();
        assert_eq!(
            versions(&table, [0], &counters).await,
            BTreeMap::from([(0, Some(5))])
        );

        std::fs::remove_file(&zero).unwrap();
        let e = read_txns(&table, &ids([0]), &counters).await.unwrap_err();
        assert!(
            matches!(e, Error::OffsetLogTruncated { version: 0, .. }),
            "got: {e:?}"
        );
    }

    /// A commit log cleanup removed under a handle that still needs it is an error of its own
    /// type, so a retry loop can tell it from an internal fault without reading its text: the
    /// cure is to reopen the table and read the offsets again (docs/kafka-design.md §3.6).
    #[tokio::test]
    async fn read_txns_commit_removed_by_cleanup_is_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 4)]).await; // 1
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        // Partition 1 has no offset, so this commit does not carry it.
        let table = fenced_commit(&mut fence, table, &[(0, (5, 9))])
            .await
            .unwrap(); // 2

        // Another member commits and checkpoints above this handle, and log cleanup removes
        // the commits below that checkpoint which this handle still needs.
        let other = assert_txns(open(path).await, &[(2, 7)]).await; // 3
        deltalake::checkpoints::create_checkpoint(&other, None)
            .await
            .unwrap();
        for v in 0..=1 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }

        let ranges = BTreeMap::from([(1, (0, 2))]);
        let e = fence.verify(&table, &ranges).await.unwrap_err();
        assert!(
            matches!(
                &e,
                Error::OffsetLogTruncated { table_uri, version: 1 }
                    if *table_uri == table.table_url().to_string()
            ),
            "got: {e:?}"
        );
        // Not the type an internal fault has.
        let internal = Fence::new(APP)
            .verify(&table, &BTreeMap::from([(7, (0, 0))]))
            .await
            .unwrap_err();
        assert!(matches!(internal, Error::Other(_)), "{internal:?}");

        // Reopened at the table's version, the offsets come out of the newer checkpoint.
        let table = open(path).await;
        fence.reset(&table, &[0, 1]).await.unwrap();
        fence.verify(&table, &ranges).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(9), None)));
    }

    /// A V2 checkpoint's top-level file, in either format, read where it is listed. Its
    /// sidecars hold only file actions, so the txns are all in the top-level file.
    #[tokio::test]
    async fn read_txns_v2_checkpoint_top_level_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 1), (1, 2), (2, 3)]).await; // 1
        let table = assert_txns(table, &[(0, 4)]).await; // 2
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        assert_txns(table, &[(0, 5)]).await; // 3
        let (table, store) = counted(path).await;

        // The checkpoint, under a V2 name; the commits it stands for, cleaned up.
        let uuid = "3a0d65cd-4056-49b8-937b-95f9e3ee90e5";
        let parquet = format!("00000000000000000002.checkpoint.{uuid}.parquet");
        std::fs::rename(
            log_file(dir.path(), "00000000000000000002.checkpoint.parquet"),
            log_file(dir.path(), &parquet),
        )
        .unwrap();
        for v in 0..=2 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }
        let expected = BTreeMap::from([(0, Some(5)), (1, Some(2)), (2, Some(3)), (9, None)]);
        let counters = LookupCounters::default();
        assert_eq!(versions(&table, [0, 1, 2, 9], &counters).await, expected);
        assert!(store.gets().contains(&format!("_delta_log/{parquet}")));

        // The same as JSON, with the actions a V2 checkpoint has beside the txns.
        std::fs::remove_file(log_file(dir.path(), &parquet)).unwrap();
        let json = format!("00000000000000000002.checkpoint.{uuid}.json");
        std::fs::write(
            log_file(dir.path(), &json),
            "{\"checkpointMetadata\":{\"version\":2}}\n\
             {\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\
             \"readerFeatures\":[\"v2Checkpoint\"],\"writerFeatures\":[\"v2Checkpoint\"]}}\n\
             {\"txn\":{\"appId\":\"app-0\",\"version\":4}}\n\
             {\"txn\":{\"appId\":\"app-1\",\"version\":2}}\n\
             {\"txn\":{\"appId\":\"app-2\",\"version\":3,\"lastUpdated\":1700000000000}}\n\
             {\"sidecar\":{\"path\":\"016ae953-37a9-438e-8683-9a9a4a79a395.parquet\",\
             \"sizeInBytes\":2304522,\"modificationTime\":1700000000000}}\n",
        )
        .unwrap();
        store.reset();
        assert_eq!(versions(&table, [0, 1, 2, 9], &counters).await, expected);
        assert!(store.gets().contains(&format!("_delta_log/{json}")));
        assert_eq!(counters.fallbacks(), 0);
    }

    #[tokio::test]
    async fn read_txns_reads_checkpoint_txn_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 1), (1, 2), (2, 3)]).await; // 1
        let table = assert_txns(table, &[(0, 4)]).await; // 2
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        let hint_at_2 = std::fs::read(log_file(dir.path(), "_last_checkpoint")).unwrap();
        let table = assert_txns(table, &[(1, 6)]).await; // 3
        let table = assert_txns(table, &[(0, 7)]).await; // 4
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        assert_txns(table, &[(0, 8)]).await; // 5

        // Cleaned up as log retention would, up to the newer checkpoint: only it remembers 1
        // and 2. The commit it stands for goes too, which no writer is obliged to keep. And
        // `_last_checkpoint` still names the older one, as when its writer did not update it.
        for v in 0..=4 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }
        std::fs::write(log_file(dir.path(), "_last_checkpoint"), hint_at_2).unwrap();

        let table = open(path).await;
        let expected = BTreeMap::from([(0, Some(8)), (1, Some(6)), (2, Some(3)), (9, None)]);
        assert_eq!(kernel_versions(&table, [0, 1, 2, 9]).await, expected);
        let counters = LookupCounters::default();
        let txns = read_txns(&table, &ids([0, 1, 2, 9]), &counters)
            .await
            .unwrap();
        let got: BTreeMap<i32, Option<i64>> = [0, 1, 2, 9]
            .into_iter()
            .map(|p| (p, txns[&txn_app_id(APP, p)].version))
            .collect();
        assert_eq!(got, expected);
        assert_eq!(txns["app-1"].mark, None, "a checkpoint keeps no commitInfo");

        // And with no `_last_checkpoint` at all.
        std::fs::remove_file(log_file(dir.path(), "_last_checkpoint")).unwrap();
        assert_eq!(versions(&table, [0, 1, 2, 9], &counters).await, expected);
        assert_eq!(counters.fallbacks(), 0);
    }

    /// Rewrite the single-part checkpoint at `version` as two parts, with txn rows in both.
    fn split_checkpoint(dir: &std::path::Path, version: u64) {
        use deltalake::arrow::compute::concat_batches;
        use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use deltalake::parquet::arrow::ArrowWriter;

        let single = log_file(dir, &format!("{version:020}.checkpoint.parquet"));
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&single).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let all = concat_batches(&batches[0].schema(), &batches).unwrap();
        let txn = all.column_by_name("txn").unwrap();
        let txn_rows: Vec<usize> = (0..all.num_rows()).filter(|i| txn.is_valid(*i)).collect();
        assert!(txn_rows.len() >= 2, "need txns for both parts");
        let split = txn_rows[txn_rows.len() / 2];
        for (part, rows) in [
            (1, all.slice(0, split)),
            (2, all.slice(split, all.num_rows() - split)),
        ] {
            let name = format!("{version:020}.checkpoint.{part:010}.{:010}.parquet", 2);
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create(log_file(dir, &name)).unwrap(),
                all.schema(),
                None,
            )
            .unwrap();
            writer.write(&rows).unwrap();
            writer.close().unwrap();
        }
        std::fs::remove_file(single).unwrap();
        std::fs::write(
            log_file(dir, "_last_checkpoint"),
            format!(
                "{{\"version\":{version},\"size\":{},\"parts\":2}}",
                all.num_rows()
            ),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn read_txns_multipart_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 1), (1, 2), (2, 3), (3, 4)]).await; // 1
        let table = assert_txns(table, &[(0, 5)]).await; // 2
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        let table = assert_txns(table, &[(0, 6)]).await; // 3
        assert_txns(table, &[(0, 7)]).await; // 4
        split_checkpoint(dir.path(), 2);
        for v in 0..=2 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }
        // A newer multi-part checkpoint that never finished: one part of two. Read as the
        // state at 3, it would lose every txn of the missing part.
        std::fs::copy(
            log_file(
                dir.path(),
                "00000000000000000002.checkpoint.0000000001.0000000002.parquet",
            ),
            log_file(
                dir.path(),
                "00000000000000000003.checkpoint.0000000001.0000000002.parquet",
            ),
        )
        .unwrap();

        let (table, store) = counted(path).await;
        let counters = LookupCounters::default();
        let got = versions(&table, 0..5, &counters).await;
        assert_eq!(
            got,
            BTreeMap::from([
                (0, Some(7)),
                (1, Some(2)),
                (2, Some(3)),
                (3, Some(4)),
                (4, None)
            ])
        );
        assert_eq!(got, kernel_versions(&table, 0..5).await);
        for part in 1..=2 {
            let name =
                format!("_delta_log/00000000000000000002.checkpoint.{part:010}.0000000002.parquet");
            assert!(store.gets().contains(&name), "{name} in {:?}", store.gets());
        }
        assert!(!store
            .gets()
            .iter()
            .any(|p| p.contains("00000000000000000003.checkpoint")));
        assert_eq!(counters.fallbacks(), 0);
    }

    /// A handle `Storage::open` built by replaying the log, past a checkpoint it could not
    /// read, hides every checkpoint from its own store: the one that was there, and every one
    /// written since through any other handle. The offsets are still read back to the newest
    /// checkpoint and out of its `txn` column, rather than from every commit down to version 0.
    /// Here the commits below each checkpoint are cleaned up, so a walk that did not see it
    /// would fail on the first of them.
    #[tokio::test]
    async fn read_txns_on_a_replayed_handle_still_reads_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 10), (1, 20)]).await; // 1
        let table = assert_txns(table, &[(0, 11)]).await; // 2
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        assert_txns(table, &[(0, 12)]).await; // 3

        let url = ensure_table_uri(path).unwrap();
        let mut replayed = crate::storage::Storage::default()
            .open_replaying_the_log(&url)
            .await
            .unwrap();
        assert_eq!(replayed.version(), Some(3));
        for v in 0..=1 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }
        let counters = LookupCounters::default();
        assert_eq!(
            versions(&replayed, 0..3, &counters).await,
            BTreeMap::from([(0, Some(12)), (1, Some(20)), (2, None)])
        );

        // A checkpoint another handle writes later, and the handle moved on to it.
        let other = assert_txns(open(path).await, &[(1, 21)]).await; // 4
        deltalake::checkpoints::create_checkpoint(&other, None)
            .await
            .unwrap();
        for v in 2..=3 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }
        replayed.update_incremental(None).await.unwrap();
        assert_eq!(replayed.version(), Some(4));
        assert_eq!(
            versions(&replayed, 0..3, &counters).await,
            BTreeMap::from([(0, Some(12)), (1, Some(21)), (2, None)])
        );
        assert_eq!(counters.fallbacks(), 0);
    }

    #[tokio::test]
    async fn read_txns_foreign_checkpoint_falls_back_to_kernel() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let table = assert_txns(open(path).await, &[(0, 10), (1, 20), (2, 30)]).await; // 1
        let table = assert_txns(table, &[(0, 11)]).await; // 2
        assert_txns(table, &[(0, 12)]).await; // 3

        // The handle is loaded first, from the commits alone. Then another engine writes a
        // checkpoint at 2 that this cannot read, and an empty one at 3 that never finished.
        let table = open(path).await;
        std::fs::write(
            log_file(dir.path(), "00000000000000000002.checkpoint.parquet"),
            b"written by an engine in a format of its own",
        )
        .unwrap();
        std::fs::write(
            log_file(dir.path(), "00000000000000000003.checkpoint.parquet"),
            b"",
        )
        .unwrap();
        std::fs::write(
            log_file(dir.path(), "_last_checkpoint"),
            "{\"version\":3,\"size\":0}",
        )
        .unwrap();

        let counters = LookupCounters::default();
        let got = versions(&table, 0..4, &counters).await;
        assert_eq!(
            got,
            BTreeMap::from([(0, Some(12)), (1, Some(20)), (2, Some(30)), (3, None)])
        );
        assert_eq!(got, kernel_versions(&table, 0..4).await);
        assert_eq!(
            counters.fallbacks(),
            3,
            "the three appIds the walk did not find went to the kernel, one each"
        );
        assert_eq!(counters.lookups(), 1);
    }

    #[tokio::test]
    async fn read_txns_returns_marks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create(path).await;

        let mark = MessageMark {
            offset: 9,
            timestamp_ms: 1_700_000_000_000,
            value_len: 42,
        };
        // A mark that does not describe the offset beside it is not one.
        let wrong = MessageMark {
            offset: 3,
            timestamp_ms: 1_700_000_000_001,
            value_len: 7,
        };
        let assert = BTreeMap::from([(0, 9), (1, 4)]);
        let marks = BTreeMap::from([(0, mark), (1, wrong)]);
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new(APP, 0)
            .commit_kafka(
                table,
                vec![empty],
                &offsets(&[(0, (5, 9)), (1, (4, 4))], &assert, &marks),
            )
            .await
            .unwrap();
        kdi_commit(dir.path(), 2, "app-2", 7, 1_700_000_000_000);

        let table = open(path).await;
        let counters = LookupCounters::default();
        let txns = read_txns(&table, &ids(0..4), &counters).await.unwrap();
        assert_eq!(
            txns["app-0"],
            TxnState {
                version: Some(9),
                mark: Some(mark),
            }
        );
        assert_eq!(
            txns["app-1"],
            TxnState {
                version: Some(4),
                mark: None,
            }
        );
        assert_eq!(
            txns["app-2"],
            TxnState {
                version: Some(7),
                mark: None,
            },
            "kafka-delta-ingest's commit records no mark"
        );
        assert_eq!(txns["app-3"], TxnState::default());

        // The fence takes the marks with the offsets.
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1, 2]).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(9), Some(mark))));
        let (_, asserted_marks) = fence.assert_map(&BTreeMap::new(), &BTreeMap::new());
        assert_eq!(asserted_marks, BTreeMap::from([(0, mark)]));
    }

    /// `rows` ids in `table`'s shape, for a commit with data in it.
    fn rows(table: &DeltaTable, ids: &[i64]) -> RecordBatch {
        use deltalake::arrow::array::{ArrayRef, Int64Array, StringArray};
        RecordBatch::try_new(
            schema_of(table),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|i| format!("row-{i}")),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    /// A table ddi has taken over, where another pipeline sharing it as a target commits
    /// often enough for delta-rs to checkpoint above ddi's last commit. The offsets and the
    /// takeover marker are then read from the checkpoint, which keeps no commitInfo: the era is
    /// still ddi's, and the marks come back from below the checkpoint. With the commits below it
    /// cleaned up, the marks are gone, and the era is not.
    #[tokio::test]
    async fn a_checkpoint_above_our_last_commit_keeps_the_era_and_the_marks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create_with(path, &[("delta.checkpointInterval", "10")]).await;
        let mark = MessageMark {
            offset: 9,
            timestamp_ms: 1_700_000_000_000,
            value_len: 42,
        };
        let empty = RecordBatch::new_empty(schema_of(&table));
        let mut first = offsets(
            &[(0, (5, 9)), (1, (4, 4))],
            &BTreeMap::from([(0, 9), (1, 4)]),
            &BTreeMap::from([(0, mark)]),
        );
        first.takeover_marker = true;
        Sink::new(APP, 0)
            .commit_kafka(table, vec![empty], &first)
            .await
            .unwrap(); // 1

        // The other pipeline, on a topic of its own: delta-rs checkpoints at 9 by itself.
        let mut table = open(path).await;
        for i in 0..11 {
            let assert = BTreeMap::from([(0, 100 + i)]);
            let empty = RecordBatch::new_empty(schema_of(&table));
            table = Sink::new("other", 0)
                .commit_kafka(
                    table,
                    vec![empty],
                    &offsets(&[(0, (100 + i, 100 + i))], &assert, &BTreeMap::new()),
                )
                .await
                .unwrap();
        }
        assert_eq!(table.version(), Some(12));
        assert!(log_file(dir.path(), "00000000000000000009.checkpoint.parquet").exists());

        let mut wanted = ids(0..2);
        wanted.push(takeover_marker_app_id(APP));
        let counters = LookupCounters::default();
        let txns = read_txns(&open(path).await, &wanted, &counters)
            .await
            .unwrap();
        assert_eq!(
            txns["app-0"],
            TxnState {
                version: Some(9),
                mark: Some(mark),
            },
            "its mark comes back from commit 1, below the checkpoint"
        );
        assert_eq!(txns["app-1"].version, Some(4));
        assert_eq!(era(APP, &txns), Era::Ddi);

        // Log cleanup takes the commits below the checkpoint.
        for v in 0..=8 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }
        let txns = read_txns(&open(path).await, &wanted, &counters)
            .await
            .unwrap();
        assert_eq!(txns["app-0"].version, Some(9));
        assert_eq!(txns["app-0"].mark, None, "the commit that had it is gone");
        assert_eq!(era(APP, &txns), Era::Ddi, "the marker is a txn, and stays");

        // Another app_id's offsets, which kafka-delta-ingest wrote and ddi never took over.
        kdi_commit(dir.path(), 13, "kdi-0", 7, 1_700_000_000_000);
        let kdi = read_txns(
            &open(path).await,
            &["kdi-0".to_string(), takeover_marker_app_id("kdi")],
            &counters,
        )
        .await
        .unwrap();
        assert_eq!(era("kdi", &kdi), Era::Kdi, "{kdi:?}");
    }

    /// The same when the commit at the checkpoint is another engine's own, as when Trino's
    /// OPTIMIZE lands at a checkpoint-interval version and checkpoints there: the walk reads
    /// that commit, which carries no txn, and then the checkpoint.
    #[tokio::test]
    async fn a_foreign_checkpoint_at_the_head_keeps_the_era() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut table = create(path).await;
        for i in 0..3i64 {
            let assert = BTreeMap::from([(0, i), (1, 100 + i)]);
            let batch = rows(&table, &[i]);
            let mut batch_offsets = offsets(
                &[(0, (i, i)), (1, (100 + i, 100 + i))],
                &assert,
                &BTreeMap::new(),
            );
            batch_offsets.takeover_marker = i == 0;
            table = Sink::new(APP, 0)
                .commit_kafka(table, vec![batch], &batch_offsets)
                .await
                .unwrap();
        }
        let (table, _) = open(path).await.optimize().await.unwrap();
        assert_eq!(table.version(), Some(4));
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();

        let mut wanted = ids(0..2);
        wanted.push(takeover_marker_app_id(APP));
        let counters = LookupCounters::default();
        let txns = read_txns(&open(path).await, &wanted, &counters)
            .await
            .unwrap();
        assert_eq!(txns["app-0"].version, Some(2));
        assert_eq!(txns["app-1"].version, Some(102));
        assert_eq!(era(APP, &txns), Era::Ddi, "{txns:?}");
    }

    /// Commit `version` by hand: commit `from`'s `metaData`, without the table property `key`,
    /// as an engine that unsets a property writes it. delta-rs has no operation for that.
    fn unset_property(dir: &std::path::Path, from: u64, version: u64, key: &str) {
        let commit = std::fs::read_to_string(log_file(dir, &commit_name(from))).unwrap();
        let mut metadata = commit
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .find(|l| l.get("metaData").is_some())
            .unwrap();
        let removed = metadata["metaData"]["configuration"]
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(removed.is_some(), "{key} was set");
        let info = serde_json::json!({"commitInfo": {
            "timestamp": 1_700_000_000_000i64, "operation": "UNSET TBLPROPERTIES"
        }});
        std::fs::write(
            log_file(dir, &commit_name(version)),
            format!("{info}\n{metadata}\n"),
        )
        .unwrap();
    }

    /// kafka-delta-ingest's offsets, answered by a checkpoint alone, on a table that once set
    /// `delta.setTransactionRetentionDuration`: two partitions' offsets expired under it, the
    /// checkpoint written meanwhile dropped them, and log cleanup has taken every commit that
    /// shows the property was ever set. The takeover check needs none of that history: in
    /// kafka-delta-ingest's era, a partition without an offset whose topic holds messages is
    /// refused until it is acknowledged (docs/kafka-design.md §4.4, EO-M4b).
    #[tokio::test]
    async fn expired_kdi_offsets_are_caught_by_the_takeover_check() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let now = chrono::Utc::now().timestamp_millis();
        kdi_commit(dir.path(), 1, "app-0", 7, 1_000);
        kdi_commit(dir.path(), 2, "app-1", 9, now);
        kdi_commit(dir.path(), 3, "app-2", 4, 1_000);
        let table = open(path)
            .await
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([(
                TXN_RETENTION.to_string(),
                "interval 1 days".to_string(),
            )]))
            .await
            .unwrap(); // 4
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        unset_property(dir.path(), 4, 5, TXN_RETENTION); // 5
        let table = open(path).await;
        assert_eq!(table.version(), Some(5));
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        for v in 0..=4 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }

        let table = open(path).await;
        let mut wanted = ids(0..3);
        wanted.push(takeover_marker_app_id(APP));
        let counters = LookupCounters::default();
        let txns = read_txns(&table, &wanted, &counters).await.unwrap();
        assert_eq!(
            [0, 1, 2].map(|p| txns[&txn_app_id(APP, p)].version),
            [None, Some(9), None],
            "{txns:?}"
        );
        assert_eq!(counters.fallbacks(), 0);
        assert_eq!(era(APP, &txns), Era::Kdi);

        // Partitions 0 and 2 have no offset and their topic holds messages: a read from 0
        // could duplicate them.
        let partitions: BTreeMap<i32, (Option<i64>, i64)> = [(0, 8), (1, 10), (2, 5)]
            .into_iter()
            .map(|(p, high)| (p, (txns[&txn_app_id(APP, p)].version, high)))
            .collect();
        let e =
            takeover_check(APP, era(APP, &txns), true, &partitions, &BTreeSet::new()).unwrap_err();
        assert!(
            matches!(&e, Error::KafkaOffsetMissing { partitions, .. } if partitions == &vec![0, 2]),
            "{e:?}"
        );
        takeover_check(
            APP,
            era(APP, &txns),
            true,
            &partitions,
            &BTreeSet::from([0, 2]),
        )
        .unwrap();
    }

    /// The mark of the message at `offset`, made up for a test.
    fn mark_at(offset: i64) -> MessageMark {
        MessageMark {
            offset,
            timestamp_ms: 1_700_000_000_000 + offset,
            value_len: 40 + offset as u32,
        }
    }

    /// Write commit `version` by hand, as ddi writes one: `txns` (appId, version) without
    /// `lastUpdated`, and a commitInfo with `ddi.appId` and `ddi.kafka.marks` for `marks`.
    fn ddi_commit(dir: &std::path::Path, version: u64, txns: &[(&str, i64)], marks: &[(i32, i64)]) {
        let marks: serde_json::Map<String, serde_json::Value> = marks
            .iter()
            .map(|(p, o)| {
                let m = mark_at(*o);
                (
                    p.to_string(),
                    serde_json::json!([m.offset, m.timestamp_ms, m.value_len]),
                )
            })
            .collect();
        let mut commit = serde_json::json!({"commitInfo": {
            "timestamp": 1_700_000_000_000i64, "operation": "WRITE",
            "ddi.appId": APP, "ddi.kafka.marks": marks
        }})
        .to_string();
        commit.push('\n');
        for (app_id, v) in txns {
            commit.push_str(&format!(
                "{{\"txn\":{{\"appId\":\"{app_id}\",\"version\":{v}}}}}\n"
            ));
        }
        std::fs::write(log_file(dir, &commit_name(version)), commit).unwrap();
    }

    /// A rebalance hands partitions 0 and 1 to a new owner after the other members' commits
    /// pushed a checkpoint above the previous owner's last commit. The checkpoint answers
    /// their offsets and drops the marks beside them; the commit below it still has them, and
    /// the re-read check (I19) needs them.
    #[tokio::test]
    async fn reset_recovers_marks_from_below_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create_with(path, &[("delta.checkpointInterval", "10")]).await;
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new(APP, 0)
            .commit_kafka(
                table,
                vec![empty],
                &offsets(
                    &[(0, (5, 9)), (1, (4, 4))],
                    &BTreeMap::from([(0, 9), (1, 4)]),
                    &BTreeMap::from([(0, mark_at(9)), (1, mark_at(4))]),
                ),
            )
            .await
            .unwrap(); // 1
        let mut table = open(path).await;
        for i in 0..11 {
            table = assert_txns(table, &[(2, 100 + i)]).await; // 2..=12
        }
        assert!(log_file(dir.path(), "00000000000000000009.checkpoint.parquet").exists());

        let (table, store) = counted(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(9), Some(mark_at(9)))));
        assert_eq!(fence.base(1), Some((Some(4), Some(mark_at(4)))));
        // Down to commit 1, which wrote both; commit 0 at most in flight when it was read.
        let mut read = store.commits_read();
        read.sort_unstable();
        read.retain(|v| *v != 0);
        assert_eq!(read, (1..=12).collect::<Vec<_>>());
        assert_eq!(fence.lookups().unverifiable(), 0);
    }

    /// An owner's commit of a batch that read one partition carries the marks of all it owns:
    /// [`Fence::assert_map`] re-asserts every other base with its mark. So its newest commit
    /// alone resumes any of them with the re-read check, also when a checkpoint lands above it.
    #[tokio::test]
    async fn every_commit_carries_every_owned_mark() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create(path).await;
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new(APP, 0)
            .commit_kafka(
                table,
                vec![empty],
                &offsets(
                    &[(0, (5, 9)), (1, (4, 4)), (2, (7, 7))],
                    &BTreeMap::from([(0, 9), (1, 4), (2, 7)]),
                    &BTreeMap::from([(0, mark_at(9)), (1, mark_at(4)), (2, mark_at(7))]),
                ),
            )
            .await
            .unwrap(); // 1

        let table = open(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1, 2]).await.unwrap();
        let ranges = BTreeMap::from([(0, (10, 12))]);
        fence.verify(&table, &ranges).await.unwrap();
        let (assert, marks) = fence.assert_map(&ranges, &BTreeMap::from([(0, mark_at(12))]));
        let expected = BTreeMap::from([(0, mark_at(12)), (1, mark_at(4)), (2, mark_at(7))]);
        assert_eq!(marks, expected);
        let batch = offsets(&[(0, (10, 12))], &assert, &marks);
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new(APP, 0)
            .commit_kafka(table, vec![empty], &batch)
            .await
            .unwrap(); // 2
        fence.committed(&batch);
        assert_eq!(fence.base(1), Some((Some(4), Some(mark_at(4)))));

        let (table, store) = counted(path).await;
        let counters = LookupCounters::default();
        let txns = read_txns(&table, &ids(0..3), &counters).await.unwrap();
        let read: BTreeMap<i32, MessageMark> = (0..3)
            .filter_map(|p| Some((p, txns[&txn_app_id(APP, p)].mark?)))
            .collect();
        assert_eq!(read, expected);
        assert_eq!(store.commits_read(), vec![2], "the head alone");
    }

    /// The walk below a checkpoint stops at the first commit log cleanup removed: what lies
    /// below a gap is not known to be the newest. Here commit 3 is gone, so partition 1's mark
    /// in commit 1 is not taken, though commit 1 is still there; partition 0's above the gap
    /// is. A txn below the checkpoint that holds another version than the checkpoint's is not
    /// the one it answered, and its mark is not taken either (partition 3: commit 9, at the
    /// checkpoint, moved it on, and is gone).
    #[tokio::test]
    async fn reset_below_the_checkpoint_stops_at_a_missing_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        ddi_commit(dir.path(), 1, &[("app-1", 40)], &[(1, 40)]);
        ddi_commit(dir.path(), 2, &[("app-9", 2)], &[]);
        ddi_commit(dir.path(), 3, &[("app-9", 3)], &[]);
        ddi_commit(
            dir.path(),
            4,
            &[("app-0", 90), ("app-3", 50)],
            &[(0, 90), (3, 50)],
        );
        for v in 5..=8 {
            ddi_commit(dir.path(), v, &[("app-9", v as i64)], &[]);
        }
        ddi_commit(dir.path(), 9, &[("app-3", 55)], &[]);
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        ddi_commit(dir.path(), 10, &[("app-9", 10)], &[]);
        for v in [3, 9] {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }

        let table = open(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1, 3]).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(90), Some(mark_at(90)))));
        assert_eq!(fence.base(1), Some((Some(40), None)), "below the gap");
        assert_eq!(
            fence.base(3),
            Some((Some(55), None)),
            "not the txn it answered"
        );
        assert_eq!(fence.lookups().unverifiable(), 2, "partitions 1 and 3");
    }

    /// The walk below a checkpoint reads at most [`BELOW_CHECKPOINT_READS`] commits: a mark
    /// within them is recovered, one further down is not, and the reset goes on without it.
    #[tokio::test]
    async fn reset_below_the_checkpoint_reads_at_most_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        ddi_commit(dir.path(), 1, &[("app-1", 40)], &[(1, 40)]);
        for v in 2..300 {
            if v == 290 {
                ddi_commit(dir.path(), v, &[("app-0", 90)], &[(0, 90)]);
            } else {
                ddi_commit(dir.path(), v, &[("app-9", v as i64)], &[]);
            }
        }
        ddi_commit(dir.path(), 300, &[("app-9", 300)], &[]);
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        ddi_commit(dir.path(), 301, &[("app-9", 301)], &[]);

        let (table, store) = counted(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(90), Some(mark_at(90)))));
        assert_eq!(fence.base(1), Some((Some(40), None)), "beyond the cap");
        let below: Vec<u64> = store
            .commits_read()
            .into_iter()
            .filter(|v| *v < 300)
            .collect();
        assert_eq!(below.len(), 256, "{below:?}");
        assert_eq!(below.iter().min(), Some(&44));
        assert_eq!(fence.lookups().below_checkpoint_reads(), 256);
        assert_eq!(fence.lookups().unverifiable(), 1, "partition 1");
    }

    // ------------------------------------------------------------ the takeover marker

    /// The marker is written by the first commit whose read snapshot lacks it, and by no
    /// commit after: re-asserted by every commit, a shared appId would make every two
    /// members' concurrent commits conflict.
    #[tokio::test]
    async fn takeover_marker_written_once_and_only_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let marker = takeover_marker_app_id(APP);
        assert_eq!(marker, "app.ddi");

        let table = open(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        assert!(
            fence.takeover_marker_due(),
            "absent at the handle's version"
        );
        let table = fenced_commit(&mut fence, table, &[(0, (0, 4))])
            .await
            .unwrap(); // 1
        assert_eq!(
            txns_in_commit(dir.path(), 1),
            vec![("app-0".to_string(), 4), (marker.clone(), 1)]
        );
        assert!(!fence.takeover_marker_due(), "its own commit wrote it");

        let table = fenced_commit(&mut fence, table, &[(0, (5, 6)), (1, (0, 2))])
            .await
            .unwrap(); // 2
        assert_eq!(
            txns_in_commit(dir.path(), 2),
            vec![("app-0".to_string(), 6), ("app-1".to_string(), 2)],
            "a commit after the first does not carry it"
        );

        // A member that reads the table afterwards finds it, and never writes it either.
        let mut other = Fence::new(APP);
        other.reset(&open(path).await, &[2]).await.unwrap();
        assert!(!other.takeover_marker_due());
        fenced_commit(&mut other, open(path).await, &[(2, (0, 1))])
            .await
            .unwrap(); // 3
        assert_eq!(
            txns_in_commit(dir.path(), 3),
            vec![("app-2".to_string(), 1)]
        );

        // Once seen it stays seen: a marker is never removed. A reset at our own newest
        // commit, which does not carry it, reads the head alone.
        let (counted_table, store) = counted(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&counted_table, &[2]).await.unwrap();
        assert!(!fence.takeover_marker_due());
        store.reset();
        fence.reset(&counted_table, &[2]).await.unwrap();
        assert_eq!(store.gets(), vec!["_delta_log/00000000000000000003.json"]);
        assert!(!fence.takeover_marker_due());
        drop(table);
    }

    /// Two members that both read the table before either took it over both write the marker
    /// in their first commit. delta-rs refuses the second, as it refuses any commit carrying
    /// a txn another wrote after its snapshot; that member re-reads, finds the marker, and
    /// commits without it.
    #[tokio::test]
    async fn two_members_racing_to_take_over_conflict_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let (a_table, b_table) = (open(path).await, open(path).await);
        let (mut a, mut b) = (Fence::new(APP), Fence::new(APP));
        a.reset(&a_table, &[0]).await.unwrap();
        b.reset(&b_table, &[1]).await.unwrap();
        assert!(a.takeover_marker_due() && b.takeover_marker_due());

        fenced_commit(&mut a, a_table, &[(0, (0, 3))])
            .await
            .unwrap(); // 1
        let e = fenced_commit(&mut b, b_table, &[(1, (0, 5))])
            .await
            .unwrap_err();
        assert!(is_concurrent_transaction(&e), "got: {e}");

        // The Replan: re-read at the table's version.
        let b_table = open(path).await;
        b.reset(&b_table, &[1]).await.unwrap();
        assert!(!b.takeover_marker_due());
        fenced_commit(&mut b, b_table, &[(1, (0, 5))])
            .await
            .unwrap(); // 2
        assert_eq!(
            txns_in_commit(dir.path(), 2),
            vec![("app-1".to_string(), 5)]
        );
        let counters = LookupCounters::default();
        let txns = read_txns(&open(path).await, &[takeover_marker_app_id(APP)], &counters)
            .await
            .unwrap();
        assert_eq!(txns[&takeover_marker_app_id(APP)].version, Some(1));
    }

    /// How another checkpoint writer spells a txn's `lastUpdated`.
    #[derive(Clone, Copy, Debug)]
    enum Restamp {
        /// Null on every txn, as delta-rs up to 0.17.3 wrote it, kafka-delta-ingest's included.
        Null,
        /// 0 for a txn without one, as Trino writes it; every other kept.
        Trino,
        /// No `lastUpdated` field at all, as delta-rs 0.26.2 writes it: kafka-delta-ingest's
        /// `-c` today (its kernel/arrow/mod.rs:206-209 checkpoint schema).
        Absent,
    }

    /// Rewrite the single-part checkpoint at `version` as another writer would have written
    /// it: every txn's `lastUpdated` spelled as `restamp` says.
    fn restamp_checkpoint(dir: &std::path::Path, version: u64, restamp: Restamp) {
        use deltalake::arrow::array::{ArrayRef, Int64Array, StructArray};
        use deltalake::arrow::compute::concat_batches;
        use deltalake::arrow::datatypes::{Fields, Schema};
        use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use deltalake::parquet::arrow::ArrowWriter;

        let file = log_file(dir, &format!("{version:020}.checkpoint.parquet"));
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let all = concat_batches(&batches[0].schema(), &batches).unwrap();
        let index = all.schema().index_of("txn").unwrap();
        let txn = all.column(index).as_struct().clone();
        let (fields, children, nulls) = txn.into_parts();
        let kept = |f: &&Arc<deltalake::arrow::datatypes::Field>| {
            !matches!(
                (f.name().as_str(), restamp),
                ("lastUpdated", Restamp::Absent)
            )
        };
        let children: Vec<ArrayRef> = fields
            .iter()
            .zip(children)
            .filter(|(f, _)| kept(f))
            .map(|(f, child)| match (f.name().as_str(), restamp) {
                ("lastUpdated", Restamp::Null) => {
                    Arc::new(Int64Array::from(vec![None::<i64>; child.len()])) as ArrayRef
                }
                ("lastUpdated", Restamp::Trino) => {
                    let stamps = child.as_primitive::<Int64Type>();
                    let rewritten: Int64Array = (0..stamps.len())
                        .map(|i| {
                            Some(if stamps.is_null(i) {
                                0
                            } else {
                                stamps.value(i)
                            })
                        })
                        .collect();
                    Arc::new(rewritten) as ArrayRef
                }
                _ => child,
            })
            .collect();
        let fields: Fields = fields.iter().filter(kept).cloned().collect();
        let restamped = StructArray::new(fields, children, nulls);
        let mut schema_fields = all.schema().fields().to_vec();
        schema_fields[index] = Arc::new(
            schema_fields[index]
                .as_ref()
                .clone()
                .with_data_type(restamped.data_type().clone()),
        );
        let schema = Arc::new(Schema::new(schema_fields));
        let mut columns = all.columns().to_vec();
        columns[index] = Arc::new(restamped);
        let rewritten = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&file).unwrap(), schema, None).unwrap();
        writer.write(&rewritten).unwrap();
        writer.close().unwrap();
    }

    /// The era read from a checkpoint alone, the commits below it cleaned up: the marker is a
    /// txn, which every checkpoint writer keeps, whatever it does to `lastUpdated`. Nothing
    /// is read from `lastUpdated`, so a checkpoint that nulled every stamp (old delta-rs) or
    /// stamped an unstamped txn with 0 (Trino) changes no answer.
    #[tokio::test]
    async fn takeover_marker_survives_checkpoints() {
        let ids_and_marker = || {
            let mut ids = ids(0..3);
            ids.push(takeover_marker_app_id(APP));
            ids
        };
        let era_of = |table: DeltaTable| async move {
            let counters = LookupCounters::default();
            let txns = read_txns(&table, &ids_and_marker(), &counters)
                .await
                .unwrap();
            assert_eq!(counters.fallbacks(), 0);
            era(APP, &txns)
        };

        // kafka-delta-ingest's offsets, then ddi's takeover; checkpointed, every way.
        for stamp in [None, Some(Restamp::Null), Some(Restamp::Trino)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap();
            create(path).await;
            kdi_commit(dir.path(), 1, "app-0", 7, 1_700_000_000_000);
            kdi_commit(dir.path(), 2, "app-1", 9, 1_700_000_000_000);
            assert_eq!(era_of(open(path).await).await, Era::Kdi, "{stamp:?}");
            let mut fence = Fence::new(APP);
            let table = open(path).await;
            fence.reset(&table, &[0, 1]).await.unwrap();
            let table = fenced_commit(&mut fence, table, &[(0, (8, 10))])
                .await
                .unwrap(); // 3
            deltalake::checkpoints::create_checkpoint(&table, None)
                .await
                .unwrap();
            if let Some(stamp) = stamp {
                restamp_checkpoint(dir.path(), 3, stamp);
            }
            for v in 0..=3 {
                std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
            }
            assert_eq!(era_of(open(path).await).await, Era::Ddi, "{stamp:?}");
        }

        // kafka-delta-ingest's alone: still its, however the checkpoint stamped them.
        for stamp in [None, Some(Restamp::Null), Some(Restamp::Trino)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_str().unwrap();
            create(path).await;
            kdi_commit(dir.path(), 1, "app-0", 7, 1_700_000_000_000);
            kdi_commit(dir.path(), 2, "app-1", 9, 1_700_000_000_000);
            deltalake::checkpoints::create_checkpoint(&open(path).await, None)
                .await
                .unwrap();
            if let Some(stamp) = stamp {
                restamp_checkpoint(dir.path(), 2, stamp);
            }
            for v in 0..=2 {
                std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
            }
            assert_eq!(era_of(open(path).await).await, Era::Kdi, "{stamp:?}");
        }
    }

    /// kafka-delta-ingest's `-c` checkpoints today (delta-rs 0.26.2) carry no `lastUpdated` at
    /// all. ddi reads the offsets and the era from one the same: kdi's own at a takeover, and
    /// ddi's and the marker after a rollback to kdi (docs/kafka-design.md §0.3.9, §10.5).
    #[tokio::test]
    async fn offsets_and_era_read_from_a_checkpoint_without_last_updated() {
        let ids_and_marker = || {
            let mut ids = ids(0..3);
            ids.push(takeover_marker_app_id(APP));
            ids
        };
        let read = |table: DeltaTable| async move {
            let counters = LookupCounters::default();
            let txns = read_txns(&table, &ids_and_marker(), &counters)
                .await
                .unwrap();
            assert_eq!(counters.fallbacks(), 0, "read from the checkpoint itself");
            let offsets: Vec<Option<i64>> =
                (0..3).map(|p| txns[&txn_app_id(APP, p)].version).collect();
            (era(APP, &txns), offsets)
        };

        let txn_fields = |dir: &std::path::Path, version: u64| {
            use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            let file = log_file(dir, &format!("{version:020}.checkpoint.parquet"));
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file).unwrap())
                    .unwrap();
            match reader.schema().field_with_name("txn").unwrap().data_type() {
                DataType::Struct(fields) => {
                    fields.iter().map(|f| f.name().clone()).collect::<Vec<_>>()
                }
                other => panic!("txn is {other:?}"),
            }
        };

        // kafka-delta-ingest's offsets, checkpointed by kafka-delta-ingest.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        kdi_commit(dir.path(), 1, "app-0", 7, 1_700_000_000_000);
        kdi_commit(dir.path(), 2, "app-1", 9, 1_700_000_000_000);
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        restamp_checkpoint(dir.path(), 2, Restamp::Absent);
        assert_eq!(txn_fields(dir.path(), 2), ["appId", "version"]);
        for v in 0..=2 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }
        assert_eq!(
            read(open(path).await).await,
            (Era::Kdi, vec![Some(7), Some(9), None])
        );

        // ddi's takeover, then kafka-delta-ingest's checkpoint after a rollback.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        kdi_commit(dir.path(), 1, "app-0", 7, 1_700_000_000_000);
        kdi_commit(dir.path(), 2, "app-1", 9, 1_700_000_000_000);
        let mut fence = Fence::new(APP);
        let table = open(path).await;
        fence.reset(&table, &[0, 1]).await.unwrap();
        let table = fenced_commit(&mut fence, table, &[(0, (8, 10))])
            .await
            .unwrap(); // 3
        let before = read(table.clone()).await;
        assert_eq!(before.0, Era::Ddi);
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        restamp_checkpoint(dir.path(), 3, Restamp::Absent);
        assert_eq!(txn_fields(dir.path(), 3), ["appId", "version"]);
        for v in 0..=3 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }
        assert_eq!(read(open(path).await).await, before);
    }

    #[test]
    fn era_is_read_from_the_marker_and_the_partition_txns_alone() {
        let state = |version: Option<i64>| TxnState {
            version,
            ..TxnState::default()
        };
        /// The txns read: (appId, version).
        type Read<'a> = &'a [(&'a str, Option<i64>)];
        let txns = |entries: Read| -> BTreeMap<String, TxnState> {
            entries
                .iter()
                .map(|(id, v)| (id.to_string(), state(*v)))
                .collect()
        };
        let cases: &[(Read, Era)] = &[
            (&[], Era::Fresh),
            (
                &[("app-0", None), ("app-1", None), ("app.ddi", None)],
                Era::Fresh,
            ),
            // Not this app_id's partition offsets, nor a marker.
            (
                &[
                    ("other-0", Some(3)),
                    ("app", Some(3)),
                    ("app.dq", Some(3)),
                    ("app-", Some(3)),
                    ("app-x", Some(3)),
                    ("app-1.5", Some(3)),
                    ("app.ddi-0", Some(3)),
                    ("other.ddi", Some(1)),
                ],
                Era::Fresh,
            ),
            (&[("app-0", Some(4)), ("app-1", None)], Era::Kdi),
            (&[("app-12", Some(0))], Era::Kdi),
            (&[("app-0", Some(4)), ("app.ddi", Some(1))], Era::Ddi),
            (&[("app-0", None), ("app.ddi", Some(1))], Era::Ddi),
        ];
        for (entries, expected) in cases {
            assert_eq!(era(APP, &txns(entries)), *expected, "{entries:?}");
        }
    }

    /// The one check a takeover of kafka-delta-ingest's offsets makes, at open, over every
    /// partition of the topic: a partition without an offset whose topic holds messages may
    /// be one whose offset expired, and needs an explicit `starting_offsets` entry. So does
    /// every such partition of a fresh app_id on a target that holds rows, which may be
    /// kafka-delta-ingest's with every offset expired. Once ddi has taken over, a partition
    /// without an offset is one nothing of is in the table.
    #[test]
    fn takeover_check_needs_an_acknowledgment_until_ddi_took_over() {
        // (stored, high) per partition.
        let partitions = BTreeMap::from([
            (0, (Some(41), 50)),
            (1, (None, 7)),
            (2, (None, 0)),
            (3, (None, 12)),
        ]);
        let none = BTreeSet::new();
        for holds_rows in [true, false] {
            let e = takeover_check(APP, Era::Kdi, holds_rows, &partitions, &none).unwrap_err();
            assert!(
                matches!(&e, Error::KafkaOffsetMissing { app_id, partitions, fresh: false }
                    if app_id == APP && partitions == &vec![1, 3]),
                "every such partition, named: {e:?}"
            );
            assert!(e.to_string().contains("[1, 3]"), "{e}");

            // One acknowledged is not enough.
            let e = takeover_check(APP, Era::Kdi, holds_rows, &partitions, &BTreeSet::from([1]))
                .unwrap_err();
            assert!(
                matches!(&e, Error::KafkaOffsetMissing { partitions, .. } if partitions == &vec![3]),
                "{e:?}"
            );
            takeover_check(
                APP,
                Era::Kdi,
                holds_rows,
                &partitions,
                &BTreeSet::from([1, 3]),
            )
            .unwrap();

            // ddi's era asks nothing.
            takeover_check(APP, Era::Ddi, holds_rows, &partitions, &none).unwrap();
        }

        // A fresh app_id: nothing on a target without rows, every partition with messages on
        // one with rows.
        let fresh = BTreeMap::from([(0, (None, 50)), (1, (None, 0)), (2, (None, 12))]);
        takeover_check(APP, Era::Fresh, false, &fresh, &none).unwrap();
        let e = takeover_check(APP, Era::Fresh, true, &fresh, &none).unwrap_err();
        assert!(
            matches!(&e, Error::KafkaOffsetMissing { partitions, fresh: true, .. }
                if partitions == &vec![0, 2]),
            "{e:?}"
        );
        assert!(e.to_string().contains("may all have expired"), "{e}");
        takeover_check(APP, Era::Fresh, true, &fresh, &BTreeSet::from([0, 2])).unwrap();

        // Where a partition without an offset starts then, in either era.
        assert_eq!(start_without_offset(0, 7, None), NoTxnStart::Zero);
        assert_eq!(start_without_offset(0, 0, None), NoTxnStart::Zero);
        assert_eq!(
            start_without_offset(3, 7, None),
            NoTxnStart::Unavailable { wanted: 0, low: 3 },
            "messages ddi never read were trimmed"
        );
        assert_eq!(
            start_without_offset(3, 12, Some(5)),
            NoTxnStart::Explicit(5)
        );
        assert_eq!(
            start_without_offset(3, 12, Some(12)),
            NoTxnStart::Explicit(12)
        );
        assert_eq!(
            start_without_offset(0, 12, Some(0)),
            NoTxnStart::Explicit(0)
        );
        assert_eq!(
            start_without_offset(3, 12, Some(2)),
            NoTxnStart::Unavailable { wanted: 2, low: 3 }
        );
        assert_eq!(
            start_without_offset(3, 12, Some(13)),
            NoTxnStart::Regressed {
                wanted: 13,
                high: 12
            }
        );
    }

    /// The walk below a checkpoint only ever adds a mark to an answer, so a commit it cannot
    /// use ends it as a missing one does, and never fails the read: one that is not a commit
    /// this can parse, or one the store refuses to serve. Only a store fault that may pass, an
    /// outage, fails the read, so that the reset is retried rather than settling for less.
    #[tokio::test]
    async fn reset_below_the_checkpoint_stops_at_an_unreadable_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        ddi_commit(dir.path(), 1, &[("app-1", 40)], &[(1, 40)]);
        ddi_commit(dir.path(), 2, &[("app-9", 2)], &[]);
        ddi_commit(dir.path(), 3, &[("app-0", 90)], &[(0, 90)]);
        for v in 4..=8 {
            ddi_commit(dir.path(), v, &[("app-9", v as i64)], &[]);
        }
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        ddi_commit(dir.path(), 9, &[("app-9", 9)], &[]);
        let expect = |fence: &Fence| {
            assert_eq!(fence.base(0), Some((Some(90), Some(mark_at(90)))));
            assert_eq!(
                fence.base(1),
                Some((Some(40), None)),
                "below what cannot be read"
            );
        };

        // Commit 2 is not JSON.
        let two = log_file(dir.path(), &commit_name(2));
        let readable = std::fs::read(&two).unwrap();
        std::fs::write(&two, b"{\"add\": not json at all").unwrap();
        let mut fence = Fence::new(APP);
        fence.reset(&open(path).await, &[0, 1]).await.unwrap();
        expect(&fence);
        assert_eq!(fence.lookups().unverifiable(), 1, "partition 1");
        std::fs::write(&two, readable).unwrap();

        // The store refuses commit 2.
        let (table, store) = counted(path).await;
        store.fail(2, Failure::Denied);
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        expect(&fence);

        // An outage while reading it fails the reset, which is retried.
        let (table, store) = counted(path).await;
        store.fail(2, Failure::Outage);
        let mut fence = Fence::new(APP);
        let e = fence.reset(&table, &[0, 1]).await.unwrap_err();
        assert!(
            matches!(
                e,
                Error::Delta(deltalake::DeltaTableError::ObjectStore {
                    source: object_store::Error::Generic { .. }
                })
            ),
            "{e:?}"
        );
    }

    /// The walk below a checkpoint finds the same thing every time for one checkpoint, so a
    /// fence does it once per checkpoint: a blocked partition's recheck and an aging reset do
    /// not repeat up to 256 GETs. A newer checkpoint is walked below again.
    #[tokio::test]
    async fn reset_below_the_checkpoint_is_read_once_per_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        ddi_commit(dir.path(), 1, &[("app-1", 40)], &[(1, 40)]);
        for v in 2..300 {
            if v == 290 {
                ddi_commit(dir.path(), v, &[("app-0", 90)], &[(0, 90)]);
            } else {
                ddi_commit(dir.path(), v, &[("app-9", v as i64)], &[]);
            }
        }
        ddi_commit(dir.path(), 300, &[("app-9", 300)], &[]);
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        ddi_commit(dir.path(), 301, &[("app-9", 301)], &[]);

        let (table, store) = counted(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1]).await.unwrap();
        assert_eq!(fence.lookups().below_checkpoint_reads(), 256);
        assert_eq!(fence.base(0), Some((Some(90), Some(mark_at(90)))));

        // A blocked partition's recheck: the same answer, and not one GET below the checkpoint.
        store.reset();
        fence.reset(&table, &[0, 1]).await.unwrap();
        assert_eq!(fence.lookups().below_checkpoint_reads(), 256, "none more");
        assert!(
            store.commits_read().iter().all(|v| *v >= 300),
            "{:?}",
            store.commits_read()
        );
        assert_eq!(fence.base(0), Some((Some(90), Some(mark_at(90)))));
        assert_eq!(fence.base(1), Some((Some(40), None)));
        assert_eq!(
            fence.lookups().unverifiable(),
            2,
            "partition 1, once per reset"
        );

        // A newer checkpoint, after the table moved on: walked below again.
        ddi_commit(dir.path(), 302, &[("app-9", 302)], &[]);
        let newer = open(path).await;
        deltalake::checkpoints::create_checkpoint(&newer, None)
            .await
            .unwrap();
        ddi_commit(dir.path(), 303, &[("app-9", 303)], &[]);
        let (table, _) = counted(path).await;
        fence.reset(&table, &[0, 1]).await.unwrap();
        assert!(fence.lookups().below_checkpoint_reads() > 256);
        assert_eq!(fence.base(0), Some((Some(90), Some(mark_at(90)))));
    }

    /// Every offset a reset reads without the mark that would verify its resume counts as
    /// unverifiable: one whose commit below the checkpoint recorded none, as
    /// kafka-delta-ingest's do; one whose commit there is gone or out of reach; and one read
    /// above the checkpoint from a commit that recorded none. A partition without an offset is
    /// not one, nor is the takeover marker.
    #[tokio::test]
    async fn every_offset_read_without_a_mark_is_unverifiable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        kdi_commit(dir.path(), 1, "app-1", 40, 1_700_000_000_000);
        ddi_commit(dir.path(), 2, &[("app-0", 90)], &[(0, 90)]);
        for v in 3..=8 {
            ddi_commit(dir.path(), v, &[("app-9", v as i64)], &[]);
        }
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        kdi_commit(dir.path(), 9, "app-2", 7, 1_700_000_000_000);

        let mut fence = Fence::new(APP);
        fence.reset(&open(path).await, &[0, 1, 2, 3]).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(90), Some(mark_at(90)))));
        assert_eq!(fence.base(1), Some((Some(40), None)));
        assert_eq!(fence.base(2), Some((Some(7), None)));
        assert_eq!(fence.base(3), Some((None, None)));
        assert_eq!(fence.lookups().unverifiable(), 2, "partitions 1 and 2");
    }

    /// A table dropped and recreated in place under a handle: the handle's own commit is gone,
    /// and the id storage holds is the new table's, though the handle, refreshed incrementally,
    /// still has the old one (I18).
    #[tokio::test]
    async fn a_recreated_table_is_seen_through_an_incremental_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let mut handle = open(path).await;
        for _ in 0..2 {
            let schema = schema_of(&handle);
            handle = handle
                .write(vec![RecordBatch::new_empty(schema)])
                .with_save_mode(SaveMode::Append)
                .await
                .unwrap();
        }
        let opened = crate::lookup::table_id(&handle);
        assert!(commit_exists(&handle).await.unwrap());
        assert_eq!(stored_table_id(&handle).await.unwrap(), opened);

        std::fs::remove_dir_all(path).unwrap();
        create(path).await;
        handle.update_incremental(None).await.unwrap();
        assert_eq!(
            crate::lookup::table_id(&handle),
            opened,
            "the handle cannot tell"
        );
        assert!(!commit_exists(&handle).await.unwrap());
        let now = stored_table_id(&handle).await.unwrap();
        assert!(now.is_some() && now != opened, "{now:?} vs {opened:?}");
    }

    /// delta-rs rebases an append over a writer-only protocol upgrade; the commits since the
    /// handle say there was one (EO-m19).
    #[tokio::test]
    async fn a_protocol_change_since_the_handle_is_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let handle = open(path).await;
        assert!(!protocol_changed_since(&handle).await.unwrap());
        // Another writer's plain append is no protocol change.
        let other = open(path).await;
        let schema = schema_of(&other);
        other
            .write(vec![RecordBatch::new_empty(schema)])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert!(!protocol_changed_since(&handle).await.unwrap());
        std::fs::write(
            log_file(dir.path(), &commit_name(2)),
            "{\"commitInfo\":{\"timestamp\":1700000000000,\"operation\":\"UPGRADE PROTOCOL\"}}\n\
             {\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":7,\
             \"writerFeatures\":[\"identityColumns\"]}}\n",
        )
        .unwrap();
        assert!(protocol_changed_since(&handle).await.unwrap());
    }

    #[tokio::test]
    async fn unwritable_protocol_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        refuse_unwritable_protocol(&open(path).await).unwrap();

        // Another engine upgrades the protocol to a writer feature delta-rs does not write.
        std::fs::write(
            log_file(dir.path(), &commit_name(1)),
            "{\"commitInfo\":{\"timestamp\":1700000000000,\"operation\":\"UPGRADE PROTOCOL\"}}\n\
             {\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":7,\
             \"writerFeatures\":[\"identityColumns\"]}}\n",
        )
        .unwrap();
        let e = refuse_unwritable_protocol(&open(path).await).unwrap_err();
        assert!(
            matches!(e, Error::TargetProtocolUnsupported { .. }),
            "got: {e}"
        );
        assert!(e.to_string().contains("writer 7"), "{e}");
    }

    /// Trino's checkpoint writes 0 for a txn without `lastUpdated`, which every engine expires
    /// once `delta.setTransactionRetentionDuration` is set, and Spark drops one without it
    /// outright while the property is. ddi stamps every Kafka txn past any cutoff, so neither
    /// its offsets nor its marker go when the property is set, a checkpoint written under it,
    /// and the property removed again, all between two of a member's refreshes: an idle
    /// partition's offset, which no commit re-asserts, and the marker, which none does after
    /// the first, included.
    #[tokio::test]
    async fn ddi_offsets_and_marker_survive_a_trino_checkpoint_then_one_under_retention() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        // Member b owns partition 1 and takes the table over; a owns partition 0.
        let (mut a, mut b) = (Fence::new(APP), Fence::new(APP));
        let table = open(path).await;
        b.reset(&table, &[1]).await.unwrap();
        let table = fenced_commit(&mut b, table, &[(1, (0, 2))]).await.unwrap(); // 1
        let stamps = stamps_in_commit(dir.path(), 1);
        assert_eq!(
            stamps,
            BTreeMap::from([
                ("app-1".to_string(), Some(KAFKA_TXN_LAST_UPDATED)),
                (takeover_marker_app_id(APP), Some(KAFKA_TXN_LAST_UPDATED)),
            ]),
            "the offset and the marker, each stamped"
        );
        a.reset(&table, &[0]).await.unwrap();
        let table = fenced_commit(&mut a, table, &[(0, (0, 4))]).await.unwrap(); // 2
                                                                                 // Trino checkpoints; partition 1 then goes idle, partition 0 does not.
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        restamp_checkpoint(dir.path(), 2, Restamp::Trino);
        fenced_commit(&mut a, open(path).await, &[(0, (5, 6))])
            .await
            .unwrap(); // 3
                       // Between two refreshes: the property set, a checkpoint written under it, removed.
        assert_eq!(
            set_retention(path, "interval 1 seconds").await.version(),
            Some(4)
        );
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        unset_property(dir.path(), 4, 5, TXN_RETENTION); // 5

        let table = open(path).await;
        let mut wanted = ids(0..2);
        wanted.push(takeover_marker_app_id(APP));
        let counters = LookupCounters::default();
        let txns = read_txns(&table, &wanted, &counters).await.unwrap();
        assert_eq!(
            [
                txns["app-0"].version,
                txns["app-1"].version,
                txns[&takeover_marker_app_id(APP)].version
            ],
            [Some(6), Some(2), Some(TAKEOVER_MARKER_VERSION)],
            "{txns:?}"
        );
        assert_eq!(era(APP, &txns), Era::Ddi);
        assert_eq!(
            kernel_versions(&table, [0, 1]).await,
            BTreeMap::from([(0, Some(6)), (1, Some(2))])
        );
        b.reset(&table, &[1]).await.unwrap();
        assert_eq!(b.base(1), Some((Some(2), None)));
    }

    /// Nothing removes a txn but expiry, so an offset a fence held that the table no longer
    /// has is one that expired between two of its refreshes: the property set, a checkpoint
    /// written under it, the property removed. Its rows are in the table, so the partition is
    /// refused, not started as one without an offset, until the table has the offset again.
    #[tokio::test]
    async fn a_vanished_offset_is_refused_until_it_is_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let now = chrono::Utc::now().timestamp_millis();
        // kafka-delta-ingest's offsets, partition 0's stamp long past; then ddi took the
        // table over, and partition 0 has been idle since.
        kdi_commit(dir.path(), 1, "app-0", 7, 1_000);
        kdi_commit(dir.path(), 2, "app-1", 9, now);
        take_over(open(path).await).await; // 3
        let mut fence = Fence::new(APP);
        fence.reset(&open(path).await, &[0, 1]).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(7), None)));

        set_retention(path, "interval 1 days").await; // 4
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        unset_property(dir.path(), 4, 5, TXN_RETENTION); // 5
        let table = open(path).await;
        let counters = LookupCounters::default();
        assert_eq!(
            versions(&table, [0, 1], &counters).await,
            BTreeMap::from([(0, None), (1, Some(9))]),
            "partition 0's offset expired"
        );

        let e = fence.reset(&table, &[0, 1]).await.unwrap_err();
        assert!(
            matches!(&e, Error::KafkaOffsetVanished { app_id, partitions, .. }
                if app_id == APP && partitions == &vec![(0, 7)]),
            "{e:?}"
        );
        assert_eq!(e.kafka_class(), crate::error::KafkaClass::BlockingPipeline);
        assert!(e.to_string().contains("--next <last + 1>"), "{e}");
        // Nothing is carried, and the recheck refuses the same.
        let e = fence.reset(&table, &[0, 1]).await.unwrap_err();
        assert!(matches!(e, Error::KafkaOffsetVanished { .. }), "{e:?}");

        // An operator's `set-offsets --next 8` puts it back.
        let table = assert_txns(table, &[(0, 7)]).await;
        fence.reset(&table, &[0, 1]).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(7), None)));
        assert_eq!(fence.base(1), Some((Some(9), None)));
    }

    /// A refusal of the property makes a fence read the marker again, rather than trust that
    /// it saw it once: a marker that lost its stamp to a checkpoint writer (here as Trino
    /// writes 0) expires under the property, and a running member then writes it again.
    #[tokio::test]
    async fn a_retention_refusal_makes_the_fence_read_the_marker_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        let commit = format!(
            "{}\n{{\"txn\":{{\"appId\":\"{}\",\"version\":1,\"lastUpdated\":0}}}}\n\
             {{\"txn\":{{\"appId\":\"app-0\",\"version\":4,\"lastUpdated\":{KAFKA_TXN_LAST_UPDATED}}}}}\n",
            serde_json::json!({"commitInfo": {
                "timestamp": 1_700_000_000_000i64, "operation": "WRITE", "ddi.appId": APP
            }}),
            takeover_marker_app_id(APP),
        );
        std::fs::write(log_file(dir.path(), &commit_name(1)), commit).unwrap();
        let mut fence = Fence::new(APP);
        fence.reset(&open(path).await, &[0]).await.unwrap();
        assert!(!fence.takeover_marker_due());

        set_retention(path, "interval 1 days").await; // 2
        let e = fence.reset(&open(path).await, &[0]).await.unwrap_err();
        assert!(matches!(e, Error::KafkaTxnRetention { .. }), "{e:?}");
        assert!(fence.takeover_marker_due(), "not known any more");
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        unset_property(dir.path(), 2, 3, TXN_RETENTION); // 3

        let table = open(path).await;
        fence.reset(&table, &[0]).await.unwrap();
        assert_eq!(fence.base(0), Some((Some(4), None)));
        assert!(fence.takeover_marker_due(), "the marker expired");
        fenced_commit(&mut fence, table, &[(0, (5, 6))])
            .await
            .unwrap(); // 4
        assert_eq!(
            stamps_in_commit(dir.path(), 4),
            BTreeMap::from([
                ("app-0".to_string(), Some(KAFKA_TXN_LAST_UPDATED)),
                (takeover_marker_app_id(APP), Some(KAFKA_TXN_LAST_UPDATED)),
            ]),
            "written again"
        );
        let mut wanted = ids([0]);
        wanted.push(takeover_marker_app_id(APP));
        let counters = LookupCounters::default();
        let txns = read_txns(&open(path).await, &wanted, &counters)
            .await
            .unwrap();
        assert_eq!(era(APP, &txns), Era::Ddi);
    }

    /// The takeover baseline an open in kafka-delta-ingest's era commits: the marker, every
    /// offset kafka-delta-ingest wrote at its value, now stamped, and every acknowledgment
    /// `s > 0` as `s - 1`. So once ddi's era begins, no start depends on the config still
    /// holding an entry: a partition whose owner has not committed yet resumes from its
    /// offset even after the entry is removed as done.
    #[tokio::test]
    async fn takeover_baseline_makes_every_acknowledgment_an_offset() {
        // Partition 0 has an offset; 1 was acknowledged at 4200; 2 at 0, which needs none; 3
        // had no message; 4 is acknowledged though it has an offset, which is ignored.
        let stored = BTreeMap::from([(0, Some(41)), (1, None), (2, None), (3, None), (4, Some(8))]);
        let explicit = BTreeMap::from([(1, 4200), (2, 0), (4, 99)]);
        assert_eq!(
            takeover_baseline(Era::Kdi, &stored, &explicit),
            Some(BTreeMap::from([(0, 41), (1, 4199), (4, 8)]))
        );
        assert_eq!(takeover_baseline(Era::Ddi, &stored, &explicit), None);
        assert_eq!(takeover_baseline(Era::Fresh, &stored, &explicit), None);

        // On a table: kafka-delta-ingest's offset of partition 0; partition 1's has expired,
        // and its topic holds 4200 messages it wrote. Two members open.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        kdi_commit(dir.path(), 1, "app-0", 41, 1_000);
        let read_era = |table: DeltaTable| async move {
            let mut wanted = ids(0..2);
            wanted.push(takeover_marker_app_id(APP));
            let counters = LookupCounters::default();
            let txns = read_txns(&table, &wanted, &counters).await.unwrap();
            let stored: BTreeMap<i32, Option<i64>> = (0..2)
                .map(|p| (p, txns[&txn_app_id(APP, p)].version))
                .collect();
            (era(APP, &txns), stored)
        };
        let baseline_commit = |table: DeltaTable, assert: BTreeMap<i32, i64>| async move {
            let mut baseline = offsets(&[], &assert, &BTreeMap::new());
            baseline.baseline = true;
            baseline.takeover_marker = true;
            let empty = RecordBatch::new_empty(schema_of(&table));
            Sink::new(APP, 0)
                .commit_kafka(table, vec![empty], &baseline)
                .await
        };
        let explicit = BTreeMap::from([(1, 4200)]);
        let partitions = BTreeMap::from([(0, (Some(41), 50)), (1, (None, 4200))]);
        let (a_table, b_table) = (open(path).await, open(path).await);
        let (a_era, a_stored) = read_era(a_table.clone()).await;
        let (b_era, b_stored) = read_era(b_table.clone()).await;
        assert_eq!((a_era, b_era), (Era::Kdi, Era::Kdi));
        let acknowledged: BTreeSet<i32> = explicit.keys().copied().collect();
        takeover_check(APP, a_era, true, &partitions, &acknowledged).unwrap();
        takeover_check(APP, b_era, true, &partitions, &acknowledged).unwrap();

        let a_baseline = takeover_baseline(a_era, &a_stored, &explicit).unwrap();
        baseline_commit(a_table, a_baseline).await.unwrap(); // 2
        assert_eq!(
            stamps_in_commit(dir.path(), 2),
            BTreeMap::from([
                ("app-0".to_string(), Some(KAFKA_TXN_LAST_UPDATED)),
                ("app-1".to_string(), Some(KAFKA_TXN_LAST_UPDATED)),
                (takeover_marker_app_id(APP), Some(KAFKA_TXN_LAST_UPDATED)),
            ])
        );
        assert_eq!(
            txns_in_commit(dir.path(), 2),
            vec![
                ("app-0".to_string(), 41),
                ("app-1".to_string(), 4199),
                (takeover_marker_app_id(APP), TAKEOVER_MARKER_VERSION),
            ]
        );
        // The other member's takeover baseline loses on the marker, and its open reads again.
        let b_baseline = takeover_baseline(b_era, &b_stored, &explicit).unwrap();
        let e = baseline_commit(b_table, b_baseline).await.unwrap_err();
        assert!(is_concurrent_transaction(&e), "{e}");
        let (b_era, b_stored) = read_era(open(path).await).await;
        assert_eq!(b_era, Era::Ddi);
        assert_eq!(takeover_baseline(b_era, &b_stored, &explicit), None);

        // With the entry gone from the config, partition 1 still resumes where it was
        // acknowledged: it has an offset now, and the takeover check has nothing to ask.
        assert_eq!(b_stored, BTreeMap::from([(0, Some(41)), (1, Some(4199))]));
        let partitions = BTreeMap::from([(0, (Some(41), 50)), (1, (Some(4199), 4200))]);
        takeover_check(APP, b_era, true, &partitions, &BTreeSet::new()).unwrap();
    }

    /// One kafka-delta-ingest instance re-stamps every partition in every commit, so its
    /// offsets expire together once the whole topic has been quiet for the duration: then no
    /// partition of the app_id has an offset, there is no marker, and the era reads fresh. On
    /// a target that holds rows that may be the topic's, the takeover check asks for an entry
    /// per partition all the same, as for any partition of kafka-delta-ingest's era.
    #[tokio::test]
    async fn all_expired_kdi_offsets_are_caught_by_the_takeover_check() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        kdi_commit(dir.path(), 1, "app-0", 7, 1_000);
        kdi_commit(dir.path(), 2, "app-1", 9, 1_000);
        set_retention(path, "interval 1 days").await; // 3
        deltalake::checkpoints::create_checkpoint(&open(path).await, None)
            .await
            .unwrap();
        unset_property(dir.path(), 3, 4, TXN_RETENTION); // 4
        let table = open(path).await;
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        for v in 0..=3 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }

        let mut wanted = ids(0..2);
        wanted.push(takeover_marker_app_id(APP));
        let counters = LookupCounters::default();
        let txns = read_txns(&open(path).await, &wanted, &counters)
            .await
            .unwrap();
        assert!(txns.values().all(|t| t.version.is_none()), "{txns:?}");
        assert_eq!(era(APP, &txns), Era::Fresh);

        let partitions = BTreeMap::from([(0, (None, 8)), (1, (None, 10))]);
        let e = takeover_check(APP, Era::Fresh, true, &partitions, &BTreeSet::new()).unwrap_err();
        assert!(
            matches!(&e, Error::KafkaOffsetMissing { partitions, fresh: true, .. }
                if partitions == &vec![0, 1]),
            "{e:?}"
        );
        takeover_check(APP, Era::Fresh, true, &partitions, &BTreeSet::from([0, 1])).unwrap();
        // Where nothing shows rows that may be the topic's, a fresh app_id is just that.
        takeover_check(APP, Era::Fresh, false, &partitions, &BTreeSet::new()).unwrap();
    }
}
