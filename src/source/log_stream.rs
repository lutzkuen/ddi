//! Resumable, incremental log-diff streaming source.
//!
//! Given a starting cursor, yields successive batches of newly added data files and
//! advances a cursor that survives process restart. This is the non-CDF path — the
//! equivalent of Spark's Delta source with `readChangeFeed=false`.
//!
//! This module is deliberately self-contained and depends on delta-rs only for
//! `LogStore` (reading raw commit bytes) and the action model. Plan §1.9: if an
//! equivalent lands upstream in delta-rs, swapping to it should be a dependency change,
//! not a rewrite.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use deltalake::kernel::{Action, Add, Remove, StructType};
use deltalake::logstore::object_store::ObjectStoreExt;
use deltalake::logstore::{commit_uri_from_version, get_actions, LogStore};
use deltalake::{DeltaTable, DeltaTableConfig, DeltaTableError};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::error::{Error, Result};
use crate::source::cursor::{StreamCursor, Version};

/// How to treat a commit that removes data (`Remove` with `dataChange: true`).
///
/// Mirrors Spark's Delta source options so the semantics are already documented knowledge.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangePolicy {
    /// Error on any `dataChange=true` `Remove`. Matches Spark's default.
    #[default]
    Fail,
    /// Skip commits containing a `dataChange=true` `Remove` entirely, including their
    /// `Add`s. Spark's `skipChangeCommits`.
    SkipChangeCommits,
    /// Ignore the `Remove`s and emit the `Add`s from the same commit. Spark's
    /// `ignoreChanges`. Rewritten rows are re-emitted, so downstream sees duplicates.
    IgnoreChanges,
}

/// What a single commit turned out to be. Plan §1.5.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitClass {
    /// `Add`s with `dataChange: true` and no `dataChange` `Remove`s. Emit.
    Data { adds: usize },
    /// Only `dataChange: false` actions — an `OPTIMIZE`/compaction. Skip silently.
    ///
    /// This is the rule that makes the tool usable: without it every `OPTIMIZE` on the
    /// source replays the entire table downstream.
    Compaction,
    /// Contains a `dataChange: true` `Remove` — a DELETE/UPDATE/MERGE.
    Change { adds: usize },
    /// A `txn`-only marker, a lone `commitInfo`, or a genuinely empty commit. Skip.
    NoData,
}

/// A bounded set of files to process, plus the cursor to persist once they are durable.
#[derive(Clone, Debug)]
pub struct LogBatch {
    /// Where this batch started (the cursor passed in).
    pub start: StreamCursor,
    /// Cursor to persist **after** this batch is durably processed.
    pub end: StreamCursor,
    /// The `dataChange: true` `Add` actions to read, in commit then log order.
    pub files: Vec<Add>,
    /// The source version that added each entry of `files`, positionally.
    ///
    /// Built with `files` in one `unzip`, so the two cannot drift apart. A batch spans as
    /// many commits as its byte and file limits allow, so this is the only thing that can
    /// say which commit a given file came from — `through_version` is the last of them,
    /// not the one that matters when a single file turns out to be unreadable.
    pub file_versions: Vec<Version>,
    /// Schema as of `end`'s last consumed version.
    pub schema: Arc<StructType>,
    /// Highest source version fully represented in `files`.
    pub through_version: Version,
    /// Last-modified timestamp of `through_version`'s Delta log object, when a pipeline needs
    /// deterministic lookup snapshots. It deliberately uses the same storage clock as Delta
    /// time travel, not an optional writer-provided `commitInfo.timestamp`.
    pub through_log_timestamp: Option<DateTime<Utc>>,
}

impl LogBatch {
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// The source version that added `files[index]`.
    ///
    /// Falls back to `through_version` rather than panicking: this exists to make an error
    /// message specific, and an error about an error is the worst way to learn that.
    pub fn version_of(&self, index: usize) -> Version {
        self.file_versions
            .get(index)
            .copied()
            .unwrap_or(self.through_version)
    }

    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|a| a.size.max(0) as u64).sum()
    }
}

/// Builder / iterator over a table's commit log.
pub struct LogStreamBuilder {
    log_store: Arc<dyn LogStore>,
    cursor: StreamCursor,
    max_files_per_batch: usize,
    max_bytes_per_batch: u64,
    policy: ChangePolicy,
    allow_commit_splitting: bool,
    /// Lookup pipelines intentionally take one data commit per batch. That gives every source
    /// version one deterministic lookup timestamp irrespective of byte/file batch settings.
    pin_lookup_snapshots: bool,
    /// Schema cache keyed by the version it was read at, so a batch that spans many
    /// commits with no schema change costs one snapshot load, not one per commit.
    schema_cache: HashMap<Version, Arc<StructType>>,
    /// Source head as of the last `next_batch` poll. `None` before the first poll.
    /// Recorded rather than re-fetched: `next_batch` already pays for this read.
    head: Option<Version>,
    /// How the operator spells this table, for errors to name it by.
    ///
    /// The log store's own root URL is a normalised `file://`/`abfss://` form that need not
    /// match what is in anybody's config, and an error an operator cannot grep their own
    /// configuration for is most of the way to being no error at all. Defaults to that
    /// normalised form so a reader built without one still names something real.
    source_uri: String,
    /// A version of the source log this stream has seen exist, to floor the head lookup at.
    ///
    /// Not an optimisation, though it is also one — a listing that starts at the head is
    /// shorter than one that starts at the beginning of a year-old table. It exists because
    /// `LogStore::get_latest_version` takes that floor as a *requirement*: the kernel's
    /// `LogSegment::for_table_changes` rejects a segment whose first commit is not exactly
    /// the version asked for. Flooring at a hardcoded `0` therefore stopped working the
    /// moment the source's own `delta.logRetentionDuration` reclaimed commit 0, and did so
    /// as `delta error: Invalid table version: 0` — a failure to read the head, reported as
    /// if version 0 were the one being asked for, on every poll, for every pipeline on that
    /// source.
    ///
    /// Seeded from the loaded snapshot's version, which is present by construction, and
    /// advanced to each head it resolves so that it stays recent and therefore stays inside
    /// whatever the retention window is.
    version_floor: Version,
    /// What decoding this source's files has cost, per byte the log said they were.
    ///
    /// Shared with the pipeline, which updates it after every read. It is the only thing
    /// that connects `max_bytes_per_batch` — a count of *compressed* bytes — to the memory
    /// the batch will actually occupy.
    amplification: Arc<crate::budget::Amplification>,
}

impl LogStreamBuilder {
    pub fn new(table: &DeltaTable) -> Self {
        Self {
            log_store: table.log_store(),
            cursor: StreamCursor::at_version(0),
            max_files_per_batch: 1_000,
            max_bytes_per_batch: 256 * 1024 * 1024,
            policy: ChangePolicy::default(),
            allow_commit_splitting: false,
            pin_lookup_snapshots: false,
            schema_cache: HashMap::new(),
            head: None,
            source_uri: table.log_store().root_url().to_string(),
            // `Storage::open` loads the table, so this is the head and it is readable. The
            // `unwrap_or(0)` is unreachable for a loaded handle, and 0 is the right answer
            // for an unloaded one: a table with no snapshot has no later version to floor at.
            version_floor: table.version().unwrap_or(0),
            amplification: Arc::new(crate::budget::Amplification::default()),
        }
    }

    /// The running estimate this stream sizes its batches by, for the reader to update.
    pub fn amplification(&self) -> Arc<crate::budget::Amplification> {
        self.amplification.clone()
    }

    /// The most bytes this batch may *combine*, as opposed to the most one commit may be.
    ///
    /// Two limits, and the distinction is the whole of the design. `max_bytes_per_batch` is
    /// a contract: a commit that fits it has always been delivered, and a memory budget
    /// must not turn a pipeline that worked yesterday into one that errors today. The
    /// budget's limit is advice about how much to put together at once, so it only ever
    /// stops accumulation early — the first commit of a batch is admitted against the
    /// configured limit however tight memory is.
    ///
    /// That is enough, because the shape that kills the process is not one enormous commit.
    /// It is a cold pipeline filling 256 MB of *compressed* budget with many files, all
    /// decoded at once into five or six times that.
    fn combined_ceiling(&self) -> u64 {
        match crate::budget::current().bytes_per_batch(self.amplification.get()) {
            Some(b) => self.max_bytes_per_batch.min(b),
            None => self.max_bytes_per_batch,
        }
    }

    /// Name this source the way the operator's configuration does.
    pub fn with_source_uri(mut self, uri: impl Into<String>) -> Self {
        self.source_uri = uri.into();
        self
    }

    pub fn with_starting_cursor(mut self, c: StreamCursor) -> Self {
        self.cursor = c;
        self
    }

    pub fn with_starting_version(mut self, v: Version) -> Self {
        self.cursor = StreamCursor::at_version(v);
        self
    }

    pub fn with_max_files_per_batch(mut self, n: usize) -> Self {
        self.max_files_per_batch = n.max(1);
        self
    }

    pub fn with_max_bytes_per_batch(mut self, n: u64) -> Self {
        self.max_bytes_per_batch = n.max(1);
        self
    }

    pub fn with_change_policy(mut self, p: ChangePolicy) -> Self {
        self.policy = p;
        self
    }

    /// Permit a batch to stop part-way through a commit (cursor `index > 0`).
    ///
    /// Off by default: the v1 daemon stores its offset as a bare version number in the
    /// Delta `txn` action, which cannot express a mid-commit position. Plan §2.3.
    pub fn with_commit_splitting(mut self, yes: bool) -> Self {
        self.allow_commit_splitting = yes;
        self
    }

    /// Process one source data commit at a time and pin lookup selection to its Delta-log
    /// object timestamp.
    ///
    /// A lookup is selected as-of that timestamp. Taking one data commit avoids making the
    /// snapshot depend on how several source commits happened to fit under a batch cap.
    pub fn with_pinned_lookup_snapshots(mut self, yes: bool) -> Self {
        self.pin_lookup_snapshots = yes;
        self
    }

    pub fn cursor(&self) -> StreamCursor {
        self.cursor
    }

    /// Resolve a starting cursor from a timestamp, as of the table's commit history.
    pub async fn with_starting_timestamp(
        mut self,
        ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<Self> {
        // Reuse the log store rather than rebuilding one from the URL: it already
        // carries the object-store credentials, and a rebuilt one would not. Without
        // files, for the reasons in `schema_at`: the answer is a version number.
        let mut table = DeltaTable::new(self.log_store.clone(), without_files());
        table.load_with_datetime(ts).await.map_err(Error::Delta)?;
        let v = table.version().unwrap_or(0);
        self.cursor = StreamCursor::at_version(v);
        Ok(self)
    }

    /// The table's current head version.
    ///
    /// Floored at a version this stream has watched exist rather than at `0`; see
    /// [`Self::version_floor`] for why that distinction is the whole of this function.
    ///
    /// The floor can still go stale in two ways, and the kernel reports both identically, as
    /// `InvalidVersion`: retention can reclaim the floor itself on a stream that has been
    /// idle long enough, and a source that was dropped and recreated can have a head *below*
    /// the floor. So a stale floor is re-resolved once, from the log rather than from the
    /// stale snapshot, and the lookup retried — which covers both, and leaves
    /// `adjust_for_replaced_source` to decide what a log that went backwards means.
    pub async fn latest_version(&mut self) -> Result<Version> {
        let stale = match self.log_store.get_latest_version(self.version_floor).await {
            Ok(v) => {
                // Forward only. A head below the floor cannot be reported by this call — the
                // kernel errors instead — so this is just the ordinary advance.
                self.version_floor = v;
                return Ok(v);
            }
            Err(DeltaTableError::InvalidVersion(_)) if self.version_floor != 0 => None,
            // A floor of 0 that is rejected is the original bug's own shape, and there is no
            // newer floor to fall back to: the log does not reach back to 0 and this stream
            // has never seen a version that it does. Re-resolve from the log all the same —
            // it is one listing, and it is the difference between recovering and not.
            Err(DeltaTableError::InvalidVersion(v)) => Some(v),
            Err(e) => return Err(Error::Delta(e)),
        };

        let mut table = DeltaTable::new(self.log_store.clone(), without_files());
        table.load().await.map_err(Error::Delta)?;
        let Some(resolved) = table.version() else {
            // Never store this as a floor: 0 is exactly the value that does not work here,
            // and storing it would pay for this re-resolve on every poll from now on while
            // still failing.
            return Err(Error::Other(format!(
                "cannot resolve the head version of {}: its log does not reach back to \
                 version {}, and reloading it produced no version at all. The table may be \
                 mid-creation, or its log may have been truncated with no checkpoint left to \
                 rebuild a snapshot from.",
                self.log_store.root_url(),
                stale.unwrap_or(self.version_floor),
            )));
        };
        debug!(
            stale_floor = self.version_floor,
            resolved, "source log no longer reaches the floor we held; re-resolved it"
        );
        self.version_floor = resolved;
        Ok(self.log_store.get_latest_version(resolved).await?)
    }

    /// The source head as observed by the last [`Self::next_batch`] poll.
    ///
    /// `None` before the first poll. This is what lag is measured against, and it is
    /// deliberately the *cached* value: re-reading the head to report a gauge would add a
    /// storage round-trip per scrape.
    pub fn last_known_head(&self) -> Option<Version> {
        self.head
    }

    /// Pull the next bounded batch of files.
    ///
    /// Returns `Ok(None)` when caught up. Non-blocking: the caller controls polling.
    pub async fn next_batch(&mut self) -> Result<Option<LogBatch>> {
        let latest = self.latest_version().await?;
        self.head = Some(latest);

        // `startingVersion` beyond the current head is "caught up", not an error —
        // the source simply has not produced that commit yet.
        if self.cursor.version > latest {
            return Ok(None);
        }

        let start = self.cursor;
        // Each file is carried with the version that added it, so a batch spanning several
        // commits can still say which one any single file came from.
        let mut files: Vec<(Version, Add)> = Vec::new();
        let mut bytes: u64 = 0;
        let mut cursor = self.cursor;
        let mut through: Option<Version> = None;
        let mut through_log_timestamp: Option<DateTime<Utc>> = None;

        while cursor.version <= latest {
            let version = cursor.version;
            let Some(raw) = self.log_store.read_commit_entry(version).await? else {
                // A gap inside the range we were asked to read means the log has been
                // truncated underneath us. Silently skipping would drop data.
                if files.is_empty() {
                    return Err(Error::CursorUnavailable {
                        source_uri: self.source_uri.clone(),
                        cursor,
                    });
                }
                break;
            };

            let actions = get_actions(version, &raw)?;
            let class = classify(&actions);

            let adds: Vec<Add> = match (&class, self.policy) {
                (CommitClass::Compaction, _) | (CommitClass::NoData, _) => {
                    debug!(version, ?class, "skipping non-data commit");
                    cursor = cursor.next_version();
                    continue;
                }
                (CommitClass::Change { .. }, ChangePolicy::Fail) => {
                    return Err(Error::ChangeCommit { version });
                }
                (CommitClass::Change { .. }, ChangePolicy::SkipChangeCommits) => {
                    warn!(version, "skipping change commit (skip_change_commits)");
                    cursor = cursor.next_version();
                    continue;
                }
                (CommitClass::Change { .. }, ChangePolicy::IgnoreChanges) => {
                    warn!(
                        version,
                        "emitting Adds from a change commit (ignore_changes); rewritten \
                         rows will be duplicated downstream"
                    );
                    data_adds(&actions)
                }
                (CommitClass::Data { .. }, _) => data_adds(&actions),
            };

            // Deletion vectors would make a wholesale file copy emit deleted rows.
            for a in &adds {
                if a.deletion_vector.is_some() {
                    return Err(Error::DeletionVectorUnsupported {
                        version,
                        path: a.path.clone(),
                    });
                }
            }

            // Resume mid-commit: drop what a previous batch already consumed.
            let already = cursor.index.min(adds.len());
            let remaining = &adds[already..];

            if remaining.is_empty() {
                cursor = cursor.next_version();
                continue;
            }

            let commit_bytes: u64 = remaining.iter().map(|a| a.size.max(0) as u64).sum();
            let fits_files = files.len() + remaining.len() <= self.max_files_per_batch;
            // Nothing accumulated yet, so this commit is measured against the configured
            // limit alone — see `combined_ceiling`. Only a batch that is already carrying
            // something is asked to stop early for memory.
            let ceiling = if files.is_empty() {
                self.max_bytes_per_batch
            } else {
                self.combined_ceiling()
            };
            let fits_bytes = bytes + commit_bytes <= ceiling;

            if fits_files && fits_bytes {
                files.extend(remaining.iter().map(|a| (version, a.clone())));
                bytes += commit_bytes;
                through = Some(version);
                if self.pin_lookup_snapshots {
                    through_log_timestamp = Some(self.commit_log_timestamp(version).await?);
                }
                cursor = cursor.next_version();
                if self.pin_lookup_snapshots {
                    break;
                }
                continue;
            }

            // Does not fit. If we already have data, stop here and let the next call
            // start cleanly at this commit — never split unless explicitly allowed.
            if !files.is_empty() {
                break;
            }

            if !self.allow_commit_splitting {
                return Err(Error::CommitTooLarge {
                    version,
                    files: remaining.len(),
                    bytes: commit_bytes,
                    max_files: self.max_files_per_batch,
                    max_bytes: self.max_bytes_per_batch,
                });
            }

            // Splitting enabled and this single commit is oversized: take as much as
            // fits, but always at least one file so the stream cannot starve.
            let mut take = 0usize;
            let mut taken_bytes = 0u64;
            for a in remaining {
                let sz = a.size.max(0) as u64;
                let next_files = take + 1;
                if next_files > self.max_files_per_batch
                    || (taken_bytes + sz > self.max_bytes_per_batch && take > 0)
                {
                    break;
                }
                taken_bytes += sz;
                take += 1;
            }
            let take = take.max(1).min(remaining.len());
            files.extend(remaining[..take].iter().map(|a| (version, a.clone())));
            // (bytes not re-read: we break out of the loop immediately below)
            cursor = cursor.advanced_by(take);
            through = Some(version);
            if self.pin_lookup_snapshots {
                through_log_timestamp = Some(self.commit_log_timestamp(version).await?);
            }
            break;
        }

        if files.is_empty() {
            // Nothing emitted, but skipped commits still advance the cursor so we do not
            // re-read them forever.
            self.cursor = cursor;
            return Ok(None);
        }

        let (file_versions, files): (Vec<Version>, Vec<Add>) = files.into_iter().unzip();
        let through_version = through.unwrap_or(start.version);
        let schema = self.schema_at(through_version).await?;
        self.cursor = cursor;

        Ok(Some(LogBatch {
            start,
            end: cursor,
            files,
            file_versions,
            schema,
            through_version,
            through_log_timestamp,
        }))
    }

    /// The timestamp Delta time travel itself uses for this commit: the log JSON object's
    /// storage metadata. `commitInfo.timestamp` is supplied by writers and can be absent,
    /// skewed, or rewritten independently of the object-store clock used by lookups.
    async fn commit_log_timestamp(&self, version: Version) -> Result<DateTime<Utc>> {
        let object = self
            .log_store
            .object_store(None)
            .head(&commit_uri_from_version(Some(version)))
            .await
            .map_err(|e| {
                Error::Other(format!(
                    "cannot read Delta-log timestamp for source commit {version}: {e}"
                ))
            })?;
        Ok(object.last_modified)
    }

    /// Schema as of `version`, cached.
    ///
    /// Loaded **without files**, and that is not only an optimisation. The schema lives in
    /// the `metaData` action, so the list of live files is answering a question nobody
    /// asked — on a large source it is the whole file set of the table, rebuilt whenever a
    /// batch reaches a version this has not seen.
    ///
    /// It is also what keeps this readable on a lakehouse other engines compact. Delta-rs
    /// replays protocol and metadata from the commits alone and only reads a checkpoint's
    /// *file* actions when files are required — so a checkpoint another engine wrote at a
    /// precision the protocol does not have is never parsed here. Without this, a table
    /// whose newest checkpoint is fine still fails the moment a batch asks for the schema
    /// at a version an older, foreign checkpoint covers: `open` steps over such a
    /// checkpoint, but only for the version it opens at.
    async fn schema_at(&mut self, version: Version) -> Result<Arc<StructType>> {
        if let Some(s) = self.schema_cache.get(&version) {
            return Ok(s.clone());
        }
        let mut table = DeltaTable::new(self.log_store.clone(), without_files());
        table.load_version(version).await.map_err(Error::Delta)?;
        let snapshot = table.snapshot().map_err(Error::Delta)?;
        let schema = snapshot.schema();
        // Keep the cache small; schema changes are rare and we only ever look backwards
        // by one version in practice.
        if self.schema_cache.len() > 8 {
            self.schema_cache.clear();
        }
        self.schema_cache.insert(version, schema.clone());
        Ok(schema)
    }
}

/// The oldest commit a table's log still holds, and when the store says it was written.
///
/// What an operator has to know to recover a pipeline whose `starting_version` has aged out,
/// and the one fact nothing else in ddi reports: the head is in every log line, the
/// configured version is in their own config, and the floor is only in the object store.
///
/// Read by listing `_delta_log` rather than by probing versions one at a time, because the
/// gap can be any size and a probe loop is one request per reclaimed commit. Commit objects
/// are zero-padded to a fixed width, so lexicographic order is numeric order and the answer
/// is the first match — `list` on every store ddi supports yields keys in that order.
///
/// `None` means the log holds no commit at all, which is not a truncated table but an
/// unreadable one: callers must say so rather than substituting a version.
pub async fn earliest_readable_commit(
    table: &DeltaTable,
) -> Result<Option<(Version, Option<DateTime<Utc>>)>> {
    use futures::TryStreamExt;

    let log_store = table.log_store();
    let store = log_store.object_store(None);
    let mut listing = store.list(Some(log_store.log_path()));

    let mut oldest: Option<(Version, Option<DateTime<Utc>>)> = None;
    while let Some(meta) = listing.try_next().await.map_err(|e| {
        Error::Other(format!(
            "cannot list the commit log of {} to find its oldest surviving version: {e}",
            log_store.root_url()
        ))
    })? {
        let Some(name) = meta.location.filename() else {
            continue;
        };
        // `NNNNNNNNNNNNNNNNNNNN.json` and nothing else. Deliberately not delta-rs's
        // `extract_version_from_filename`, which also matches `.checkpoint.parquet` and
        // `.json` sidecars — a checkpoint at a version whose commit is gone is exactly the
        // shape this function exists to look at, so counting it would answer the opposite
        // of the question.
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let Ok(version) = stem.parse::<Version>() else {
            continue;
        };
        if oldest.is_none_or(|(held, _)| version < held) {
            oldest = Some((version, Some(meta.last_modified)));
        }
    }
    Ok(oldest)
}

/// Load the log, but not the list of files it leaves live.
///
/// Both uses here ask the log a question about *itself* — what the schema was, which
/// version a timestamp lands on — and neither needs to know which files survived. Saying so
/// is what stops delta-rs materialising them, which costs a full replay of the file set and,
/// on a table another engine has compacted, means parsing that engine's checkpoint.
fn without_files() -> DeltaTableConfig {
    DeltaTableConfig {
        require_files: false,
        ..Default::default()
    }
}

/// The `dataChange: true` `Add` actions of a commit, in log order.
fn data_adds(actions: &[Action]) -> Vec<Add> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Add(add) if add.data_change => Some(add.clone()),
            _ => None,
        })
        .collect()
}

fn data_removes(actions: &[Action]) -> Vec<&Remove> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Remove(r) if r.data_change => Some(r),
            _ => None,
        })
        .collect()
}

/// Classify a commit. Plan §1.5 — this table is the substance of the source.
pub fn classify(actions: &[Action]) -> CommitClass {
    let adds = actions
        .iter()
        .filter(|a| matches!(a, Action::Add(add) if add.data_change))
        .count();
    let removes = data_removes(actions).len();

    if removes > 0 {
        return CommitClass::Change { adds };
    }
    if adds > 0 {
        return CommitClass::Data { adds };
    }

    // No dataChange actions at all. Distinguish a compaction (which *did* touch files,
    // with dataChange=false) from a commit that carried no file actions whatsoever.
    let touched_files = actions
        .iter()
        .any(|a| matches!(a, Action::Add(_) | Action::Remove(_)));
    if touched_files {
        CommitClass::Compaction
    } else {
        CommitClass::NoData
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::kernel::Transaction;

    fn add(path: &str, data_change: bool) -> Action {
        Action::Add(Add {
            path: path.into(),
            data_change,
            size: 100,
            modification_time: 0,
            ..Default::default()
        })
    }

    fn remove(path: &str, data_change: bool) -> Action {
        Action::Remove(Remove {
            path: path.into(),
            data_change,
            ..Default::default()
        })
    }

    #[test]
    fn plain_append_is_a_data_commit() {
        assert_eq!(
            classify(&[add("a", true), add("b", true)]),
            CommitClass::Data { adds: 2 }
        );
    }

    #[test]
    fn optimize_is_compaction_not_data() {
        // The rule everybody gets wrong: OPTIMIZE rewrites files with dataChange=false.
        // Treating it as data replays the whole table downstream on every compaction.
        let actions = [
            add("compacted", false),
            remove("small-1", false),
            remove("small-2", false),
        ];
        assert_eq!(classify(&actions), CommitClass::Compaction);
    }

    #[test]
    fn delete_is_a_change_commit() {
        assert_eq!(
            classify(&[remove("a", true)]),
            CommitClass::Change { adds: 0 }
        );
    }

    #[test]
    fn update_rewrites_are_change_commits_even_though_they_add_files() {
        // UPDATE/MERGE emit both an Add and a Remove with dataChange=true. The Remove
        // must dominate, otherwise ChangePolicy::Fail would never fire on an UPDATE.
        assert_eq!(
            classify(&[add("new", true), remove("old", true)]),
            CommitClass::Change { adds: 1 }
        );
    }

    #[test]
    fn txn_only_commit_has_no_data() {
        let actions = [Action::Txn(Transaction::new("some-app", 4))];
        assert_eq!(classify(&actions), CommitClass::NoData);
    }

    #[test]
    fn empty_commit_has_no_data() {
        assert_eq!(classify(&[]), CommitClass::NoData);
    }

    #[test]
    fn data_adds_filters_out_non_data_change_adds() {
        let actions = [add("keep", true), add("drop", false), remove("r", true)];
        let got = data_adds(&actions);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].path, "keep");
    }
}
