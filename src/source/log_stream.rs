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
use deltalake::logstore::object_store::ObjectStore;
use deltalake::logstore::{commit_uri_from_version, get_actions, LogStore};
use deltalake::{DeltaTable, DeltaTableConfig};
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
    pub async fn latest_version(&self) -> Result<Version> {
        Ok(self.log_store.get_latest_version(0).await?)
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
        let mut files: Vec<Add> = Vec::new();
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
                    return Err(Error::CursorUnavailable { cursor });
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
                files.extend_from_slice(remaining);
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
            files.extend_from_slice(&remaining[..take]);
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

        let through_version = through.unwrap_or(start.version);
        let schema = self.schema_at(through_version).await?;
        self.cursor = cursor;

        Ok(Some(LogBatch {
            start,
            end: cursor,
            files,
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
