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

/// One appId's newest `txn`, and what the commit that wrote it says about it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxnState {
    /// The `txn`'s version: for a partition, the last offset accounted for. `None` when the
    /// table holds no `txn` under this appId.
    pub version: Option<i64>,
    /// The message at that offset, as the commit that wrote the `txn` recorded it in
    /// `ddi.kafka.marks`. `None` when that commit recorded none, or recorded one for a
    /// different offset, and when the `txn` was found only in a checkpoint, which keeps no
    /// commit metadata.
    pub mark: Option<MessageMark>,
    /// The `txn`'s `lastUpdated`, which says who wrote it ([`Self::writer`]) and whether it
    /// can expire ([`Self::may_expire`]).
    pub last_updated: LastUpdated,
}

impl TxnState {
    /// Who wrote this `txn`, as its `lastUpdated` says.
    pub fn writer(&self) -> Writer {
        match (self.version, self.last_updated) {
            (Some(_), LastUpdated::Absent) => Writer::Ddi,
            (Some(_), LastUpdated::At(_)) => Writer::Other,
            (Some(_), LastUpdated::Unknown) | (None, _) => Writer::Unknown,
        }
    }

    /// Whether this `txn` can expire under `delta.setTransactionRetentionDuration`: it has a
    /// `lastUpdated`, or it was read where its `lastUpdated` could not be seen.
    pub fn may_expire(&self) -> bool {
        self.version.is_some() && self.last_updated != LastUpdated::Absent
    }
}

/// A `txn`'s `lastUpdated`.
///
/// The protocol expires a `txn` under `delta.setTransactionRetentionDuration` by this field
/// alone, and one without it never (set_transaction.rs:10-21). It is also what tells the two
/// writers of a Kafka pipeline's offsets apart: kafka-delta-ingest stamps every `txn` with
/// the time it wrote it (delta_helpers.rs:29-40), ddi never stamps one (`Transaction::new`).
/// A checkpoint keeps it with the `txn`, so the answer is the same whether the `txn` was read
/// from the commit that wrote it or from a checkpoint above that commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LastUpdated {
    /// The `txn` has none.
    Absent,
    /// The `txn` has one: milliseconds since the epoch.
    At(i64),
    /// Not known: there is no `txn`, or the kernel answered for a checkpoint this cannot read,
    /// and the kernel gives the version alone.
    #[default]
    Unknown,
}

/// Who wrote an appId's newest `txn`, as its `lastUpdated` says.
///
/// ddi and a stopped kafka-delta-ingest are the only writers of a Kafka pipeline's offsets
/// (docs/kafka-design.md §4.3), and [`LastUpdated`] tells them apart exactly, wherever the
/// `txn` was read. Who wrote the *commit* is not asked: a `txn` read from a checkpoint has no
/// commit, and on a table only ddi has written offsets to that happens whenever a checkpoint
/// lands above ddi's last commit — another pipeline sharing the target, or another engine's
/// OPTIMIZE at a checkpoint-interval version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Writer {
    /// ddi: the `txn` has no `lastUpdated`, and never expires.
    Ddi,
    /// kafka-delta-ingest: the `txn` has a `lastUpdated`, and can expire.
    Other,
    /// Not known: there is no `txn`, or its `lastUpdated` could not be seen.
    Unknown,
}

/// Whether kafka-delta-ingest wrote these offsets: some `txn` among them is its, and none is
/// ddi's (docs/kafka-design.md §4.4 step 2).
///
/// `txns` are one app_id's, as [`read_txns`] returns them for every partition of its topic.
pub fn kdi_era(txns: &BTreeMap<String, TxnState>) -> bool {
    let writers: Vec<Writer> = txns.values().map(TxnState::writer).collect();
    writers.contains(&Writer::Other) && !writers.contains(&Writer::Ddi)
}

/// Whether a partition that has no offset among `txns` may have had one that expired, so
/// that reading it again from offset 0 could duplicate it (docs/kafka-design.md §4.4, the
/// `None` row; EO-M4b).
///
/// Only a `txn` with a `lastUpdated` can expire, and the writer that stamps one stamps every
/// `txn` it writes. So it may when some `txn` of this app_id can expire ([`TxnState::may_expire`],
/// a `lastUpdated` not seen counting as one), and the table's history ever set
/// `delta.setTransactionRetentionDuration`, which `retention_ever_set` says. Who wrote the
/// commits does not enter into it: kafka-delta-ingest's offsets read from a checkpoint alone
/// can have lost a sibling exactly as those read from its commits can.
///
/// `txns` are one app_id's, as [`read_txns`] returns them for every partition of its topic.
pub fn offset_may_have_expired(
    txns: &BTreeMap<String, TxnState>,
    retention_ever_set: bool,
) -> bool {
    retention_ever_set && txns.values().any(TxnState::may_expire)
}

/// What reading offsets has cost, for `ddi_kafka_offset_lookups_total`,
/// `ddi_kafka_offset_lookup_seconds_total`, `ddi_kafka_offset_lookup_fallbacks_total`,
/// `ddi_kafka_offset_lookup_below_checkpoint_reads_total` and
/// `ddi_kafka_offset_lookup_marks_unfound_total`.
///
/// Shared rather than returned, so that whoever renders the metrics can hold the same
/// counters the readers add to.
#[derive(Debug, Default)]
pub struct LookupCounters {
    lookups: AtomicU64,
    micros: AtomicU64,
    fallbacks: AtomicU64,
    below_checkpoint_reads: AtomicU64,
    marks_unfound: AtomicU64,
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
    /// most [`BELOW_CHECKPOINT_READS`] per call.
    pub fn below_checkpoint_reads(&self) -> u64 {
        self.below_checkpoint_reads.load(Ordering::Relaxed)
    }

    /// appIds a checkpoint or the kernel answered whose commit those reads did not reach,
    /// because they met a commit log cleanup had removed, or read as many as they may. Each
    /// is a partition whose resume cannot be checked against its mark (docs/kafka-design.md
    /// §4.4, Unverifiable).
    pub fn marks_unfound(&self) -> u64 {
        self.marks_unfound.load(Ordering::Relaxed)
    }
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
///    answers comes with its `lastUpdated` but no mark: a checkpoint keeps no commitInfo.
/// 5. Only if that checkpoint cannot be read here: the kernel, one appId at a time, counted
///    in [`LookupCounters::fallbacks`]. It answers with the version alone.
/// 6. For what steps 4 and 5 answered, the commits below the checkpoint, newest first,
///    [`IN_FLIGHT`] at a time, until each of those appIds has been met: the newest commit
///    with its `txn` gives back the mark beside it, and for a kernel answer its `lastUpdated`,
///    when that `txn` holds the version answered. Without them the re-read check of a resume
///    (docs/kafka-design.md §4.4, I19) is off for the partition until it carries data again.
///    Bounded twice: by [`BELOW_CHECKPOINT_READS`] commits, counted in
///    [`LookupCounters::below_checkpoint_reads`], and by the first commit log cleanup has
///    removed, below which a `txn` is not known to be the newest. An appId neither reached
///    keeps no mark, counted in [`LookupCounters::marks_unfound`].
///
/// The walk is contiguous, so what it finds is the newest; what it does not find is as the
/// checkpoint at its bottom says, or absent when it reached version 0. A commit missing from
/// the middle is an error, never a reason to look further down: an answer older than the
/// truth would move an offset backwards. Step 6 only ever adds a mark to an answer, so it
/// stops at a missing commit instead.
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
    read(table, app_ids, counters, Below::Marks).await
}

/// [`read_txns`] on a table every `txn` of which is ddi's own, without its refusal of
/// `delta.setTransactionRetentionDuration`, and without its step 6.
///
/// The refusal is there because a `txn` with a `lastUpdated`, as kafka-delta-ingest wrote
/// them, can expire under the property, and the walk does not filter for that. ddi's own
/// carry none and never expire, so on a table only ddi writes `txn`s to — the data-quality
/// table's marks ([`crate::dq::DataQuality::write_kafka`]) — the property changes nothing
/// read here, and refusing it would stop rejects being recorded for nothing. Its marks are
/// read on every write, where a mark a checkpoint answered is simply not skipped by, so the
/// commits below the checkpoint are not worth reading.
pub(crate) async fn read_own_txns(
    table: &DeltaTable,
    app_ids: &[String],
    counters: &LookupCounters,
) -> Result<BTreeMap<String, TxnState>> {
    read(table, app_ids, counters, Below::Nothing).await
}

/// Whether [`read_txns`]' step 6 reads below the checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Below {
    /// It does: the caller keeps the marks.
    Marks,
    /// It does not: the caller wants the versions alone.
    Nothing,
}

/// Steps 1-6 of [`read_txns`], counted as one lookup.
async fn read(
    table: &DeltaTable,
    app_ids: &[String],
    counters: &LookupCounters,
    below: Below,
) -> Result<BTreeMap<String, TxnState>> {
    let wanted: BTreeSet<&str> = app_ids.iter().map(String::as_str).collect();
    let mut found = BTreeMap::new();
    if !wanted.is_empty() {
        let head = table.version().ok_or_else(|| not_loaded(table))?;
        let started = Instant::now();
        let walked = walk(table, head, &wanted, &mut found, counters, below).await;
        counters.lookups.fetch_add(1, Ordering::Relaxed);
        counters
            .micros
            .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        walked?;
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
    below: Below,
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
            for (id, (version, last_updated)) in txns {
                found.insert(
                    id,
                    TxnState {
                        version: Some(version),
                        mark: None,
                        last_updated,
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
        .filter(|id| found.contains_key(*id))
        .collect();
    if below == Below::Marks && !answered.is_empty() {
        below_checkpoint(table, checkpoint.version, answered, found, counters).await?;
    }
    Ok(())
}

/// Step 6 of [`read_txns`]: for the appIds in `answered`, which the checkpoint at `bottom` or
/// the kernel answered, what the commit below `bottom` that wrote each one's `txn` says.
async fn below_checkpoint(
    table: &DeltaTable,
    bottom: u64,
    mut answered: BTreeSet<&str>,
    found: &mut BTreeMap<String, TxnState>,
    counters: &LookupCounters,
) -> Result<()> {
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
        // Removed by log cleanup: below a gap, a txn is not known to be the newest.
        let Some(bytes) = read? else { break };
        let mut met = BTreeMap::new();
        record_commit(table, v, &bytes, &answered, &mut met)?;
        for (id, written) in met {
            answered.remove(id.as_str());
            let Some(answer) = found.get_mut(&id) else {
                continue;
            };
            // Another version is an older txn than the one answered, which the commit that
            // moved it on — at or above `bottom`, and gone — wrote. Its mark is not this one's.
            if written.version == answer.version {
                answer.mark = written.mark;
                if answer.last_updated == LastUpdated::Unknown {
                    answer.last_updated = written.last_updated;
                }
            }
        }
        if answered.is_empty() {
            return Ok(());
        }
    }
    counters
        .marks_unfound
        .fetch_add(answered.len() as u64, Ordering::Relaxed);
    debug!(
        table = %table.table_url(),
        checkpoint_version = bottom,
        app_ids = answered.len(),
        "the commits below this checkpoint that wrote these offsets are gone or too far down; \
         their resume cannot be checked against a mark"
    );
    Ok(())
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxnAction {
    app_id: String,
    version: i64,
    /// Absent and `null` alike mean the `txn` has none.
    #[serde(default)]
    last_updated: Option<i64>,
}

impl TxnAction {
    fn last_updated(&self) -> LastUpdated {
        self.last_updated
            .map_or(LastUpdated::Absent, LastUpdated::At)
    }
}

/// The two keys of a commitInfo this module reads. ddi writes them at its top level, beside
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
        let last_updated = txn.last_updated();
        found.insert(
            txn.app_id,
            TxnState {
                version: Some(txn.version),
                mark,
                last_updated,
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
    let partition = txn.app_id.strip_prefix(app_id)?.strip_prefix('-')?;
    if partition.is_empty() || !partition.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let [offset, timestamp_ms, value_len] = marks?.get(partition)?.as_array()?.as_slice() else {
        return None;
    };
    let mark = MessageMark {
        offset: offset.as_i64()?,
        timestamp_ms: timestamp_ms.as_i64()?,
        value_len: u32::try_from(value_len.as_u64()?).ok()?,
    };
    (mark.offset == txn.version).then_some(mark)
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

/// One `txn` of a checkpoint: its appId, version and `lastUpdated`.
type CheckpointTxn = (String, i64, LastUpdated);

/// The version and `lastUpdated` each wanted appId has in `checkpoint`, or why the checkpoint
/// cannot be read.
///
/// Reads the `txn` column and nothing else: a parquet checkpoint is projected to it, so the
/// columns another engine writes in its own way (`stats_parsed` above all) are never decoded.
/// The `Err` is a reason for the warning, not an error to return: any checkpoint this cannot
/// read sends the appIds to the kernel instead.
async fn checkpoint_txns(
    store: Arc<dyn ObjectStore>,
    checkpoint: &Checkpoint,
    wanted: &BTreeSet<&str>,
) -> std::result::Result<BTreeMap<String, (i64, LastUpdated)>, String> {
    let mut txns = BTreeMap::new();
    for file in &checkpoint.files {
        let in_file = if file.location.as_ref().ends_with(".json") {
            json_txns(store.as_ref(), file).await?
        } else {
            parquet_txns(store.clone(), file).await?
        };
        for (id, version, last_updated) in in_file {
            if !wanted.contains(id.as_str()) {
                continue;
            }
            // A checkpoint is the reconciled state: one txn per appId. Two that disagree mean
            // it is not a checkpoint this can read.
            if let Some(other) = txns.insert(id.clone(), (version, last_updated)) {
                if other != (version, last_updated) {
                    return Err(format!(
                        "it holds two txns for appId {id:?}, {other:?} and {:?}",
                        (version, last_updated)
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
            let last_updated = txn.last_updated();
            txns.push((txn.app_id, txn.version, last_updated));
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
        // The protocol's checkpoint schema has it. A checkpoint without it, or with it in a
        // shape that is no timestamp, does not say whether its txns can expire.
        let last_updated = match txn.column_by_name("lastUpdated") {
            Some(a) if matches!(a.data_type(), DataType::Int64 | DataType::Int32) => {
                Some(cast(a, &DataType::Int64)?)
            }
            _ => None,
        };
        let last_updated = last_updated.as_ref().map(|a| a.as_primitive::<Int64Type>());
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
            let stamp = match last_updated {
                None => LastUpdated::Unknown,
                Some(a) if a.is_null(row) => LastUpdated::Absent,
                Some(a) => LastUpdated::At(a.value(row)),
            };
            txns.push((app_ids.value(row).to_string(), versions.value(row), stamp));
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
#[derive(Debug)]
pub struct Fence {
    app_id: String,
    /// For each owned partition, the last offset durably in the target, and the message at it
    /// when known. `None` for a partition with no offset yet.
    base: BTreeMap<i32, (Option<i64>, Option<MessageMark>)>,
    /// Owned partitions whose offset is known at the handle's version: read there by
    /// [`Self::reset`] or [`Self::verify`], or asserted by every commit since.
    carried: BTreeSet<i32>,
    lookups: Arc<LookupCounters>,
}

impl Fence {
    /// A fence for `app_id` that owns nothing yet.
    pub fn new(app_id: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
            base: BTreeMap::new(),
            carried: BTreeSet::new(),
            lookups: Arc::new(LookupCounters::default()),
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
    /// version. On an error nothing is carried, so that no offset is trusted until a reset
    /// succeeds.
    pub async fn reset(&mut self, table: &DeltaTable, owned: &[i32]) -> Result<()> {
        let ids: Vec<String> = owned.iter().map(|p| txn_app_id(&self.app_id, *p)).collect();
        let txns = match read_txns(table, &ids, &self.lookups).await {
            Ok(txns) => txns,
            Err(e) => {
                self.carried.clear();
                return Err(e);
            }
        };
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
        if unverified.is_empty() {
            return Ok(());
        }
        if let Some(p) = unverified.iter().find(|p| !self.base.contains_key(p)) {
            return Err(Error::Other(format!(
                "internal: a batch for app_id {:?} read partition {p}, which this writer does \
                 not own",
                self.app_id
            )));
        }
        let ids: Vec<String> = unverified
            .iter()
            .map(|p| txn_app_id(&self.app_id, *p))
            .collect();
        // [`read_txns`] without its step 6: only the versions are compared.
        refuse_txn_retention(table)?;
        let txns = read(table, &ids, &self.lookups, Below::Nothing).await?;
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
    /// has not read. A partition forgotten since the commit was built stays forgotten.
    pub fn committed(&mut self, offsets: &KafkaOffsets) {
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

/// Refuse a target that sets `delta.setTransactionRetentionDuration`.
///
/// Under it the kernel forgets a `txn` whose `lastUpdated` is older than the duration.
/// kafka-delta-ingest wrote one on every `txn`, so the offset of a partition it had not
/// written for that long would be forgotten — and a checkpoint written meanwhile drops it for
/// good. ddi's own `txn`s carry no `lastUpdated` and never expire, but a table under the
/// property can hold ones that do, so it is refused when it is opened and on every refresh,
/// rather than read with an expiry filter (docs/kafka-design.md §4.6).
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
        let offsets = offsets(ranges, &assert, &marks);
        let empty = RecordBatch::new_empty(schema_of(&table));
        let table = Sink::new(APP, 0)
            .commit_kafka(table, vec![empty], &offsets)
            .await?;
        fence.committed(&offsets);
        Ok(table)
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
            }
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
        create(path).await;

        // Two members of one group, each with its own handle on version 0.
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
        assert_eq!(a_table.version(), Some(3));
        assert_eq!(b_table.version(), Some(4));

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

        let table = assert_txns(open(path).await, &[(0, 1), (1, 2), (2, 3)]).await;
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

    /// What the refusal protects against, and why ddi's own offsets need no protecting: a
    /// `txn` without `lastUpdated` survives the property being set after the fact, and a
    /// checkpoint written under it; one with an old `lastUpdated`, as kafka-delta-ingest
    /// wrote them, does not.
    #[tokio::test]
    async fn txns_without_last_updated_never_expire() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        assert_txns(open(path).await, &[(0, 5)]).await;
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

        assert!(matches!(
            refuse_txn_retention(&reopened),
            Err(Error::KafkaTxnRetention { .. })
        ));
    }

    #[tokio::test]
    async fn read_txns_one_get_in_steady_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        create(path).await;
        // Enough history, and a checkpoint, that anything but the head would show.
        let mut table = open(path).await;
        for offset in 0..12 {
            table = assert_txns(table, &[(0, offset), (1, 100 + offset), (2, 200 + offset)]).await;
        }
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
        assert_txns(table, &[(0, 12), (1, 112), (2, 212)]).await;

        let (table, store) = counted(path).await;
        let mut fence = Fence::new(APP);
        fence.reset(&table, &[0, 1, 2]).await.unwrap();
        assert_eq!(store.gets(), vec!["_delta_log/00000000000000000013.json"]);
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
        assert_eq!(fence.lookups().lookups(), 1, "only the reset read anything");

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
        assert_eq!(counters.marks_unfound(), 0);
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

        // Where only the versions are wanted, the walk goes no further than the checkpoint.
        store.reset();
        read_own_txns(&table, &ids(0..5), &counters).await.unwrap();
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
        assert_eq!(txns["app-0"].writer(), Writer::Ddi);
        assert_eq!(
            txns["app-1"].last_updated,
            LastUpdated::Absent,
            "a checkpoint keeps the txn's lastUpdated, and ddi's has none"
        );
        assert_eq!(txns["app-1"].writer(), Writer::Ddi);
        assert_eq!(txns["app-9"].writer(), Writer::Unknown);

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

        // The kernel gives the version alone; commit 1, below the checkpoint it stood in for,
        // gives back the lastUpdated of the two txns it wrote.
        let txns = read_txns(&table, &ids(0..4), &counters).await.unwrap();
        assert_eq!(txns["app-0"].writer(), Writer::Ddi, "read from commit 3");
        assert_eq!(txns["app-1"].last_updated, LastUpdated::Absent);
        assert_eq!(txns["app-2"].writer(), Writer::Ddi);
        assert!(!offset_may_have_expired(&txns, true), "{txns:?}");
    }

    /// A txn whose `lastUpdated` could not be seen — the kernel answered it, and the commit
    /// that wrote it was not found — counts as one that can expire, and as nobody's.
    #[test]
    fn a_txn_whose_last_updated_is_not_known_may_expire() {
        let state = |version: Option<i64>, last_updated| TxnState {
            version,
            mark: None,
            last_updated,
        };
        let cases = [
            (state(Some(1), LastUpdated::Absent), Writer::Ddi, false),
            (state(Some(1), LastUpdated::At(5)), Writer::Other, true),
            (state(Some(1), LastUpdated::Unknown), Writer::Unknown, true),
            (state(None, LastUpdated::Unknown), Writer::Unknown, false),
        ];
        for (txn, writer, may_expire) in cases {
            assert_eq!(txn.writer(), writer, "{txn:?}");
            assert_eq!(txn.may_expire(), may_expire, "{txn:?}");
        }
        let one = |txn: TxnState| BTreeMap::from([("app-0".to_string(), txn)]);
        assert!(offset_may_have_expired(
            &one(state(Some(1), LastUpdated::Unknown)),
            true
        ));
        assert!(!offset_may_have_expired(
            &one(state(Some(1), LastUpdated::Absent)),
            true
        ));
        assert!(!kdi_era(&one(state(Some(1), LastUpdated::Unknown))));
    }

    #[tokio::test]
    async fn read_txns_returns_marks_and_writer() {
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
                last_updated: LastUpdated::Absent,
            }
        );
        assert_eq!(
            txns["app-1"],
            TxnState {
                version: Some(4),
                mark: None,
                last_updated: LastUpdated::Absent,
            }
        );
        assert_eq!(
            txns["app-2"],
            TxnState {
                version: Some(7),
                mark: None,
                last_updated: LastUpdated::At(1_700_000_000_000),
            }
        );
        assert_eq!(txns["app-3"], TxnState::default());
        assert_eq!(
            [0, 1, 2, 3].map(|p| txns[&txn_app_id(APP, p)].writer()),
            [Writer::Ddi, Writer::Ddi, Writer::Other, Writer::Unknown]
        );

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

    /// A table only ddi has written offsets to, where another pipeline sharing it as a target
    /// commits often enough for delta-rs to checkpoint above ddi's last commit. The offsets are
    /// then read from the checkpoint, which keeps no commitInfo but keeps each txn's
    /// `lastUpdated`: they are still ddi's, and must not read as somebody else's, or the table
    /// would look like kafka-delta-ingest's.
    #[tokio::test]
    async fn read_txns_checkpoint_above_our_last_commit_still_reads_as_ddis() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let table = create_with(path, &[("delta.checkpointInterval", "10")]).await;
        let mark = MessageMark {
            offset: 9,
            timestamp_ms: 1_700_000_000_000,
            value_len: 42,
        };
        let empty = RecordBatch::new_empty(schema_of(&table));
        Sink::new(APP, 0)
            .commit_kafka(
                table,
                vec![empty],
                &offsets(
                    &[(0, (5, 9)), (1, (4, 4))],
                    &BTreeMap::from([(0, 9), (1, 4)]),
                    &BTreeMap::from([(0, mark)]),
                ),
            )
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

        let counters = LookupCounters::default();
        let txns = read_txns(&open(path).await, &ids(0..2), &counters)
            .await
            .unwrap();
        assert_eq!(
            txns["app-0"],
            TxnState {
                version: Some(9),
                mark: Some(mark),
                last_updated: LastUpdated::Absent,
            },
            "and its mark comes back from commit 1, below the checkpoint"
        );
        assert_eq!(txns["app-1"].version, Some(4));
        assert_eq!(txns["app-1"].writer(), Writer::Ddi);
        assert!(!kdi_era(&txns), "ddi wrote every offset here: {txns:?}");
        assert!(
            !offset_may_have_expired(&txns, true),
            "and none of them can expire: {txns:?}"
        );

        // A txn kafka-delta-ingest wrote, in a commit still in the log, is another writer's.
        kdi_commit(dir.path(), 13, "kdi-0", 7, 1_700_000_000_000);
        let kdi = read_txns(&open(path).await, &["kdi-0".to_string()], &counters)
            .await
            .unwrap();
        assert_eq!(kdi["kdi-0"].writer(), Writer::Other);
        assert!(kdi_era(&kdi), "{kdi:?}");
        assert!(offset_may_have_expired(&kdi, true), "{kdi:?}");
        assert!(!offset_may_have_expired(&kdi, false), "{kdi:?}");
    }

    /// The same when the commit at the checkpoint is another engine's own, as when Trino's
    /// OPTIMIZE lands at a checkpoint-interval version and checkpoints there: the walk reads
    /// that commit, which carries no txn, and then the checkpoint.
    #[tokio::test]
    async fn read_txns_foreign_checkpoint_at_the_head_still_reads_as_ddis() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let mut table = create(path).await;
        for i in 0..3i64 {
            let assert = BTreeMap::from([(0, i), (1, 100 + i)]);
            let batch = rows(&table, &[i]);
            table = Sink::new(APP, 0)
                .commit_kafka(
                    table,
                    vec![batch],
                    &offsets(
                        &[(0, (i, i)), (1, (100 + i, 100 + i))],
                        &assert,
                        &BTreeMap::new(),
                    ),
                )
                .await
                .unwrap();
        }
        let (table, _) = open(path).await.optimize().await.unwrap();
        assert_eq!(table.version(), Some(4));
        deltalake::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();

        let counters = LookupCounters::default();
        let txns = read_txns(&open(path).await, &ids(0..2), &counters)
            .await
            .unwrap();
        assert_eq!(txns["app-0"].version, Some(2));
        assert_eq!(txns["app-1"].version, Some(102));
        assert!(txns.values().all(|t| t.writer() == Writer::Ddi), "{txns:?}");
        assert!(!kdi_era(&txns), "ddi wrote every offset here: {txns:?}");
        assert!(!offset_may_have_expired(&txns, true), "{txns:?}");
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

    /// kafka-delta-ingest's offsets, answered by a checkpoint alone, on a table whose history
    /// set `delta.setTransactionRetentionDuration`: two partitions' offsets expired under it,
    /// and the checkpoint written meanwhile dropped them. Who wrote the one left is in that
    /// checkpoint, as its `lastUpdated`, and so a partition without an offset is one whose
    /// offset may have expired (docs/kafka-design.md §4.4), not one to read again from 0.
    #[tokio::test]
    async fn kdi_txns_answered_by_a_checkpoint_guard_a_missing_offset() {
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
        // Log cleanup took kafka-delta-ingest's commits; the one that set the property is
        // still there for M3's history scan.
        for v in 0..=3 {
            std::fs::remove_file(log_file(dir.path(), &commit_name(v))).unwrap();
        }

        let table = open(path).await;
        let counters = LookupCounters::default();
        let txns = read_txns(&table, &ids(0..3), &counters).await.unwrap();
        assert_eq!(
            [0, 1, 2].map(|p| txns[&txn_app_id(APP, p)].version),
            [None, Some(9), None],
            "{txns:?}"
        );
        assert_eq!(counters.fallbacks(), 0);
        assert_eq!(
            txns["app-1"].last_updated,
            LastUpdated::At(now),
            "the checkpoint keeps lastUpdated"
        );
        assert_eq!(txns["app-1"].writer(), Writer::Other);
        assert!(kdi_era(&txns), "{txns:?}");
        // Partitions 0 and 2 have no offset; a read from 0 would duplicate them.
        assert!(offset_may_have_expired(&txns, true), "{txns:?}");
        // Without the property in the table's history, nothing can have expired.
        assert!(!offset_may_have_expired(&txns, false), "{txns:?}");
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
        assert_eq!(fence.lookups().marks_unfound(), 0);
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
        assert_eq!(fence.lookups().marks_unfound(), 1, "partition 1");
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
        assert_eq!(fence.lookups().marks_unfound(), 1, "partition 1");
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
}
