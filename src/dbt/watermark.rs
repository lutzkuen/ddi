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
//! # Ordering
//!
//! Prefer a **pre-hook** that records the version and a model that pins its read to it
//! (`FOR VERSION AS OF` in Trino, `VERSION AS OF` in Spark). Then the watermark is on
//! disk before the overwrite lands and there is no window at all.
//!
//! With a post-hook the watermark appears one commit after the overwrite. If `ddi` looks
//! in between it sees the previous night's watermark and re-streams from there, which
//! duplicates rows rather than dropping them. That asymmetry is deliberate: duplicates
//! are visible and the next dbt run erases them, whereas a gap is silent and permanent.

use deltalake::kernel::Action;
use deltalake::logstore::get_actions;
use deltalake::DeltaTable;
use futures::TryStreamExt;
use tracing::warn;

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
    pub async fn last(&self, app_id: &str) -> Result<Option<Version>> {
        use deltalake::arrow::array::{Array, AsArray, RecordBatch};
        use deltalake::arrow::datatypes::Int64Type;

        let table = self.storage.open(&self.uri).await.map_err(|e| {
            Error::Config(format!(
                "{e}. The watermark table must exist before a pipeline that shares its \
                 target with dbt can start."
            ))
        })?;
        let (_t, stream) = table.scan_table().await.map_err(Error::Delta)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| {
            Error::Other(format!("cannot read watermark table {:?}: {e}", self.uri))
        })?;

        let mut best: Option<Version> = None;
        for b in &batches {
            let app = b.schema().index_of("app_id").map_err(|_| {
                Error::Config(format!(
                    "watermark table {:?} has no app_id column; expected \
                     (app_id VARCHAR, source_version BIGINT)",
                    self.uri
                ))
            })?;
            let ver = b.schema().index_of("source_version").map_err(|_| {
                Error::Config(format!(
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
            .map_err(|e| Error::Config(format!("watermark app_id is not text: {e}")))?;
            let ids = ids.as_string::<i32>();
            let versions = b
                .column(ver)
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(|| {
                    Error::Config(format!(
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
                    return Err(Error::Other(format!(
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
        if actions
            .iter()
            .any(|a| matches!(a, Action::Txn(t) if t.app_id == app_id))
        {
            return Ok(TargetState::OursOrUntouched);
        }

        // Someone else's rewrite: a Remove that actually deleted data.
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
    /// The source table id we were reading. `None` for commits written before this was
    /// recorded, or when we have never written to this target.
    pub source_table_id: Option<String>,
}

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
            let source_table_id = actions.iter().find_map(|a| match a {
                Action::CommitInfo(ci) => ci
                    .info
                    .get("ddi.sourceTableId")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                _ => None,
            });
            return Ok(OurLastCommit { source_table_id });
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
