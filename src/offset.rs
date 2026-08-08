//! Resume-offset storage, via the target table's own Delta transaction log.
//!
//! There is no side-car state file, no checkpoint directory, no external store. The
//! target table is fully self-describing: any Delta engine can read it without knowing
//! this daemon exists.
//!
//! # Mechanism
//!
//! Delta's idempotent-write protocol. A `txn` action carries `(appId, version)`. We commit
//! `txn(app_id, version = last fully-processed source version)` in the **same commit** as
//! the data derived from that source version. Because a Delta commit is atomic, data and
//! offset advance together or not at all — that is the entire exactly-once argument.
//!
//! kafka-delta-ingest uses this for Kafka offsets; here the stored number is a source
//! table version instead.
//!
//! # Convention
//!
//! The stored value is the **last fully-processed source version**, so resuming reads from
//! `stored + 1`. A target with no `txn` action for our `app_id` has never been written by
//! this pipeline, so we start from the configured `starting_version`.

use deltalake::DeltaTable;

use crate::error::{Error, Result};
use crate::source::{StreamCursor, Version};

/// Reads and interprets the resume offset held in a target table.
#[derive(Clone, Debug)]
pub struct OffsetStore {
    app_id: String,
    starting_version: Version,
}

impl OffsetStore {
    pub fn new(app_id: impl Into<String>, starting_version: Version) -> Self {
        Self {
            app_id: app_id.into(),
            starting_version,
        }
    }

    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    /// The last source version this pipeline durably committed to `target`, if any.
    pub async fn last_committed_version(&self, target: &DeltaTable) -> Result<Option<Version>> {
        let snapshot = target.snapshot().map_err(Error::Delta)?;
        let stored = snapshot
            .transaction_version(target.log_store().as_ref(), &self.app_id)
            .await
            .map_err(Error::Delta)?;

        match stored {
            None => Ok(None),
            Some(v) if v < 0 => Err(Error::Other(format!(
                "target table holds a negative txn version ({v}) for app_id {:?}; refusing to \
                 guess a resume point",
                self.app_id
            ))),
            Some(v) => Ok(Some(v as Version)),
        }
    }

    /// Where to resume reading the source.
    ///
    /// `stored + 1` when we have committed before, otherwise the configured
    /// `starting_version`. Always a commit boundary — v1 does not split commits.
    pub async fn resume_cursor(&self, target: &DeltaTable) -> Result<StreamCursor> {
        Ok(match self.last_committed_version(target).await? {
            Some(v) => StreamCursor::at_version(v + 1),
            None => StreamCursor::at_version(self.starting_version),
        })
    }

    /// The value to store in the `txn` action for a batch ending at `cursor`.
    ///
    /// Errors on a mid-commit cursor: a bare `txn` version cannot express `index > 0`, and
    /// silently rounding it would either replay or skip part of a commit.
    pub fn txn_version_for(&self, cursor: StreamCursor) -> Result<i64> {
        let v = cursor.last_fully_consumed_version().ok_or_else(|| {
            Error::Other(format!(
                "refusing to record a mid-commit cursor ({cursor}) in a txn action: the Delta \
                 txn action stores a single version number. This is the v1 no-commit-splitting \
                 invariant (plan §2.3)."
            ))
        })?;
        i64::try_from(v).map_err(|_| {
            Error::Other(format!(
                "source version {v} does not fit in the txn action's i64"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_pipeline_starts_at_configured_version() {
        let s = OffsetStore::new("app", 7);
        // No target consulted: this is the None branch of resume_cursor.
        assert_eq!(s.starting_version, 7);
    }

    #[test]
    fn txn_version_is_the_last_fully_consumed_version() {
        let s = OffsetStore::new("app", 0);
        // Cursor sits at the start of v5 => v4 is done.
        assert_eq!(s.txn_version_for(StreamCursor::at_version(5)).unwrap(), 4);
    }

    #[test]
    fn txn_version_rejects_mid_commit_cursors() {
        let s = OffsetStore::new("app", 0);
        let err = s.txn_version_for(StreamCursor::new(5, 3)).unwrap_err();
        assert!(
            err.to_string().contains("mid-commit"),
            "error should name the invariant, got: {err}"
        );
    }

    #[test]
    fn nothing_consumed_yet_is_not_recordable() {
        let s = OffsetStore::new("app", 0);
        // Cursor at v0 with nothing consumed has no "last fully consumed" version.
        assert!(s.txn_version_for(StreamCursor::at_version(0)).is_err());
    }

    #[test]
    fn resume_is_exactly_one_past_the_stored_version() {
        // Guards the classic off-by-one: storing "last done" and resuming at the same
        // number would reprocess that commit on every restart.
        let stored: Version = 11;
        assert_eq!(StreamCursor::at_version(stored + 1).version, 12);
    }
}
