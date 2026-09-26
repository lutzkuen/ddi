//! The dbt handover: surviving a nightly overwrite of the target.
//!
//! # The hazard
//!
//! `ddi` stores its offset as a `txn` action in the target's log, and `txn` actions
//! survive an overwrite — they live in the log, not in the data. So when dbt rebuilds the
//! target, `ddi` wakes up still believing it processed through version N and resumes at
//! N+1. Everything it streamed *after* dbt began its read was wiped by the overwrite and
//! is never re-emitted:
//!
//! ```text
//! 00:00  dbt reads bronze@100
//! 00:03  ddi streams 101, 102  -> appended to silver
//! 00:05  dbt OVERWRITE silver = f(bronze@100)   <- 101 and 102 are gone
//! 00:06  ddi resumes at 103                     <- and never come back
//! ```
//!
//! Silent, and it compounds nightly. This module closes it.
//!
//! # The handover
//!
//! dbt records the source version it consumed in a small watermark table. `ddi` notices
//! that its target was overwritten by someone else and resumes from that watermark
//! instead of from its own `txn` offset, re-streaming the gap.
//!
//! The watermark table is plain SQL on purpose — an `INSERT` any dbt adapter can run,
//! rather than a `txn` action only the Spark writer can produce. That is what keeps this
//! agnostic across dbt-trino, dbt-databricks and the rest.
//!
//! ```sql
//! -- schema: app_id VARCHAR, source_version BIGINT
//! INSERT INTO lake.meta.ddi_watermark VALUES ('ddi.silver.orders', 100)
//! ```
//!
//! # Which rebuild a watermark belongs to
//!
//! The table only grows, so its newest row can be one an earlier rebuild recorded: this
//! rebuild's post-hook has not run yet, or the rewrite records nothing at all — an `UPDATE`,
//! `DELETE` or `MERGE` by another writer, which reads just like a rebuild. Resuming from that
//! row would append again everything since it, and bring back what the rewrite deleted. So
//! each commit of ours carries the newest source version the rebuild at our last handover can
//! have read ([`HANDOVER_SOURCE_HEAD_KEY`]): the watermark we resumed from, or where we
//! rescanned, the source head as read after the target. A row newer than that was recorded
//! since, for a later rebuild. So is, in effect, a row naming the version our own offset is at:
//! resuming from it is resuming from our own offset, whichever rebuild recorded it. With
//! `dedup_timestamp` set only such a row counts, and a rewrite without one falls back to the
//! rescan; with no timestamp there is nothing to fall back on, and the newest row is used
//! whatever it is.
//!
//! That takes a few rebuilds' own rows for earlier ones', and their rows go through the rescan's
//! cut-off: one that read exactly where our last rescan found the source while we streamed on
//! past it; one already running when a pipeline first opened with a watermark table; and the
//! first after upgrading from ddi 0.3.1, or after setting `watermark_uri`, whose commits
//! recorded no handover — each unless it read exactly as far as our own offset.
//!
//! # Ordering
//!
//! Prefer a **pre-hook** that records the version and a model that pins its read to it
//! (`FOR VERSION AS OF` in Trino, `VERSION AS OF` in Spark). Then the watermark is on
//! disk before the overwrite lands and there is no window at all.
//!
//! With a post-hook the watermark appears one commit after the overwrite. If `ddi` looks
//! in between it sees the previous rebuild's watermark. With `dedup_timestamp` it takes that
//! for what it is where we handed over from that rebuild, and falls back to the rescan, whose
//! cut-off can drop a lagging partition's late rows; where we missed it, it cannot tell the two
//! apart, and re-streams from there. Without one it re-streams from there, which duplicates rows
//! rather than dropping them. That asymmetry is deliberate: duplicates are visible and the next
//! dbt run erases them, whereas a gap is silent and permanent.

use std::collections::BTreeMap;

use deltalake::kernel::Action;
use deltalake::logstore::get_actions;
use deltalake::DeltaTable;
use futures::TryStreamExt;
use tracing::warn;

use crate::dedup::RecordedCutoff;
use crate::error::{Error, Result};
use crate::source::Version;

/// Reads the watermark dbt records for a pipeline.
#[derive(Debug, Clone)]
pub struct WatermarkStore {
    uri: String,
    storage: crate::storage::Storage,
}

impl WatermarkStore {
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            storage: crate::storage::Storage::default(),
        }
    }

    pub fn with_storage(mut self, storage: crate::storage::Storage) -> Self {
        self.storage = storage;
        self
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// The highest source version dbt has declared for `app_id`, if any.
    ///
    /// The maximum rather than the most recently written row: watermarks advance, and
    /// taking the max is immune to row ordering and to a post-hook that appends without
    /// deleting. A pipeline that genuinely needs to rewind should be reset explicitly
    /// rather than by writing a lower watermark.
    ///
    /// [`Error::WatermarkUnusable`] when the table cannot be used however often it is asked:
    /// a URI no backend here reaches, no table there, a column missing or of another type, or
    /// a row no rebuild can have written. Any other error is a read that failed this time.
    pub async fn last(&self, app_id: &str) -> Result<Option<Version>> {
        use deltalake::arrow::array::{Array, AsArray, RecordBatch};
        use deltalake::arrow::datatypes::Int64Type;

        // Touches no storage, so everything it refuses is configuration.
        self.storage.check(&self.uri).map_err(|e| match e {
            Error::Config(why) => Error::WatermarkUnusable(why),
            e => e,
        })?;
        let Some(table) = self.storage.open_if_exists(&self.uri).await? else {
            return Err(Error::WatermarkUnusable(format!(
                "watermark table {:?} does not exist: there is no Delta table there. The \
                 watermark table must exist before a pipeline that shares its target with dbt \
                 can start; create it as (app_id VARCHAR, source_version BIGINT)",
                self.uri
            )));
        };
        use deltalake::delta_datafusion::DataFusionMixins;
        let declared = table
            .snapshot()
            .map_err(Error::Delta)?
            .snapshot()
            .read_schema();

        let (_t, stream) = table
            .scan_table()
            .with_session_state(std::sync::Arc::new(crate::budget::session(&table)?))
            .await
            .map_err(Error::Delta)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| {
            Error::Other(format!("cannot read watermark table {:?}: {e}", self.uri))
        })?;
        // As the table declares its columns. This table is written by dbt, against whatever
        // warehouse the project targets, so it is the likeliest of all of them to be typed
        // by an engine with its own ideas about precision.
        let batches: Vec<RecordBatch> = batches
            .into_iter()
            .map(|b| crate::schema::read_as_declared(b, &declared))
            .collect::<Result<_>>()?;

        let mut best: Option<Version> = None;
        for b in &batches {
            let app = b.schema().index_of("app_id").map_err(|_| {
                Error::WatermarkUnusable(format!(
                    "watermark table {:?} has no app_id column; expected \
                     (app_id VARCHAR, source_version BIGINT)",
                    self.uri
                ))
            })?;
            let ver = b.schema().index_of("source_version").map_err(|_| {
                Error::WatermarkUnusable(format!(
                    "watermark table {:?} has no source_version column; expected \
                     (app_id VARCHAR, source_version BIGINT)",
                    self.uri
                ))
            })?;

            // Normalise the id column: a scan may hand back Utf8, LargeUtf8 or Utf8View.
            let ids = deltalake::arrow::compute::cast(
                b.column(app),
                &deltalake::arrow::datatypes::DataType::Utf8,
            )
            .map_err(|e| Error::WatermarkUnusable(format!("watermark app_id is not text: {e}")))?;
            let ids = ids.as_string::<i32>();
            let versions = b
                .column(ver)
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(|| {
                    Error::WatermarkUnusable(format!(
                        "watermark table {:?}: source_version must be a BIGINT",
                        self.uri
                    ))
                })?;

            for i in 0..b.num_rows() {
                if ids.is_null(i) || versions.is_null(i) || ids.value(i) != app_id {
                    continue;
                }
                let v = versions.value(i);
                if v < 0 {
                    return Err(Error::WatermarkUnusable(format!(
                        "watermark table {:?} holds a negative source_version ({v}) for \
                         app_id {app_id:?}; refusing to guess a resume point",
                        self.uri
                    )));
                }
                let v = v as Version;
                best = Some(best.map_or(v, |b: Version| b.max(v)));
            }
        }
        Ok(best)
    }
}

/// What the target's log says about who wrote it last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetState {
    /// The most recent meaningful commit is ours, or the table is untouched. Resume
    /// normally, from our own `txn` offset.
    OursOrUntouched,
    /// Someone else rewrote the data after our last append — a dbt rebuild. Our offset
    /// describes rows that no longer exist.
    OverwrittenAt(Version),
}

/// Walk the target's log backwards to see whether it was rewritten since our last append.
///
/// Backwards because the answer is almost always in the last commit or two: either we
/// appended most recently, or dbt overwrote most recently. Scanning forward from zero
/// would read the entire history to learn something about its tail.
///
/// `max_scan` bounds the walk so a pathological log cannot stall startup; exceeding it is
/// reported as an overwrite, because "we could not prove our offset is still valid" must
/// fail towards duplicates, never towards a gap.
pub async fn target_state(target: &DeltaTable, app_id: &str, max_scan: u64) -> Result<TargetState> {
    let Some(head) = target.version() else {
        return Ok(TargetState::OursOrUntouched);
    };
    let log = target.log_store();

    let mut scanned = 0u64;
    let mut v = head;
    loop {
        if scanned >= max_scan {
            warn!(
                app_id,
                head, max_scan, "could not find our own txn action within max_scan commits"
            );
            return Ok(TargetState::OverwrittenAt(v));
        }
        let Some(raw) = log.read_commit_entry(v).await? else {
            break;
        };
        let actions = get_actions(v, &raw)?;

        // Ours: any commit carrying our txn action. Everything before it is irrelevant.
        //
        // This test MUST come before the `Remove` test below, and the order is load-bearing
        // rather than stylistic. An upsert pipeline (`write_mode = "upsert"`) writes its
        // data, its `Remove`s and its `txn` action in one commit, so its own commits look
        // exactly like a foreign rewrite to the second test. Checking ours first is what
        // stops the daemon diagnosing itself as a dbt rebuild and rescanning on every
        // restart — or, with `watermark_uri` set and no watermark row, refusing to start at
        // all. Pinned by `an_upsert_commit_of_ours_is_not_a_foreign_rebuild`.
        if actions
            .iter()
            .any(|a| matches!(a, Action::Txn(t) if t.app_id == app_id))
        {
            return Ok(TargetState::OursOrUntouched);
        }

        // Someone else's rewrite: a Remove that actually deleted data, in a commit that
        // carries no txn of ours (established immediately above).
        if actions
            .iter()
            .any(|a| matches!(a, Action::Remove(r) if r.data_change))
        {
            return Ok(TargetState::OverwrittenAt(v));
        }

        scanned += 1;
        if v == 0 {
            break;
        }
        v -= 1;
    }
    Ok(TargetState::OursOrUntouched)
}

/// What our own last commit to the target recorded about itself.
#[derive(Debug, Clone, Default)]
pub struct OurLastCommit {
    /// Target version of the newest commit carrying this pipeline's txn action. `None` means
    /// this app id has never written the target (or the bounded scan could not find it).
    pub commit_version: Option<Version>,
    /// The source table id we were reading. `None` for commits written before this was
    /// recorded, or when we have never written to this target.
    pub source_table_id: Option<String>,
    /// A source version that had `source_table_id`. `None` for commits written before this
    /// was recorded. See [`SOURCE_TABLE_ID_VERSION_KEY`].
    pub source_table_id_version: Option<Version>,
    /// The newest source version the rebuild at our last handover can have read. `None` for
    /// commits written before this was recorded, and without a watermark table. See
    /// [`HANDOVER_SOURCE_HEAD_KEY`].
    pub handover_source_head: Option<Version>,
    /// Delta identities of the pinned lookups that produced the commit. A table recreated at
    /// the same URI has a new id; resuming against it would silently change an old join.
    /// Empty for pre-lookup commits and for tables we have never written.
    pub lookup_table_ids: BTreeMap<String, String>,
    /// The coverage window that commit was made inside, when the window was still open after
    /// it. `None` outside one — and for the commit that closed one, which is what stops a
    /// reopen from resuming a window that has already ended. See [`crate::dedup`].
    pub cutoff: Option<RecordedCutoff>,
}

/// `commitInfo` key naming a source version that had the table id a commit of ours records
/// (`ddi.sourceTableId`): the newest the stream had loaded. A reopen that finds another id
/// loads that version again, and a log that still gives it the recorded id is the same log,
/// replaced in place.
pub const SOURCE_TABLE_ID_VERSION_KEY: &str = "ddi.sourceTableIdVersion";

/// `commitInfo` key naming the newest source version the rebuild this pipeline last handed over
/// from can have read — the watermark it resumed from, or where it rescanned, the source head
/// read after the target — or, before its first handover, the head at the first open that
/// recorded one. A watermark above it was recorded since, so for a later rebuild. Recorded only
/// while `watermark_uri` is set.
pub const HANDOVER_SOURCE_HEAD_KEY: &str = "ddi.handover.sourceHead";

/// Walk the target log backwards for the most recent commit that carries our txn action,
/// and report what it said about the source it came from.
///
/// Same walk as [`target_state`], and normally just as short.
pub async fn our_last_commit(
    target: &DeltaTable,
    app_id: &str,
    max_scan: u64,
) -> Result<OurLastCommit> {
    let Some(head) = target.version() else {
        return Ok(OurLastCommit::default());
    };
    let log = target.log_store();

    let mut v = head;
    let mut scanned = 0u64;
    loop {
        if scanned >= max_scan {
            return Ok(OurLastCommit::default());
        }
        let Some(raw) = log.read_commit_entry(v).await? else {
            return Ok(OurLastCommit::default());
        };
        let actions = get_actions(v, &raw)?;

        if actions
            .iter()
            .any(|a| matches!(a, Action::Txn(t) if t.app_id == app_id))
        {
            let info = |key: &str| {
                actions.iter().find_map(|a| match a {
                    Action::CommitInfo(ci) => ci.info.get(key).cloned(),
                    _ => None,
                })
            };
            let source_table_id =
                info("ddi.sourceTableId").and_then(|v| v.as_str().map(str::to_string));
            let source_table_id_version =
                info(SOURCE_TABLE_ID_VERSION_KEY).and_then(|v| v.as_u64());
            let handover_source_head = info(HANDOVER_SOURCE_HEAD_KEY).and_then(|v| v.as_u64());
            let lookup_table_ids = actions
                .iter()
                .filter_map(|a| match a {
                    Action::CommitInfo(ci) => Some(&ci.info),
                    _ => None,
                })
                .flat_map(|info| info.iter())
                .filter_map(|(key, value)| {
                    let name = key.strip_prefix("ddi.lookup.")?.strip_suffix(".tableId")?;
                    let id = value.as_str()?;
                    Some((name.to_string(), id.to_string()))
                })
                .collect();
            let cutoff = actions.iter().find_map(|a| match a {
                Action::CommitInfo(ci) => RecordedCutoff::from_commit_info(&ci.info),
                _ => None,
            });
            return Ok(OurLastCommit {
                commit_version: Some(v),
                source_table_id,
                source_table_id_version,
                handover_source_head,
                lookup_table_ids,
                cutoff,
            });
        }

        if v == 0 {
            return Ok(OurLastCommit::default());
        }
        v -= 1;
        scanned += 1;
    }
}

/// How far back to walk the target log before giving up. Generous: a busy pipeline
/// commits often, so our own txn is normally within a handful of commits.
pub const DEFAULT_MAX_SCAN: u64 = 10_000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_store_remembers_its_uri() {
        let s = WatermarkStore::new("/lake/meta/ddi_watermark");
        assert_eq!(s.uri(), "/lake/meta/ddi_watermark");
    }

    #[test]
    fn overwrite_states_are_distinguishable() {
        assert_ne!(
            TargetState::OursOrUntouched,
            TargetState::OverwrittenAt(4),
            "the caller must be able to tell these apart; conflating them is the bug \
             this module exists to prevent"
        );
    }
}
