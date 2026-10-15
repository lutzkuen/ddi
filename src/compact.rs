//! Compaction that stays safe when another engine compacts the same table.
//!
//! delta-rs's conflict checker lets two OPTIMIZE runs commit over the same files: it ignores a
//! `Remove` whose `dataChange` is false, which is every `Remove` a compaction writes (the
//! protocol asks every writer to treat two removes of one file as a conflict, and Spark does).
//! Both rewrites then land and every row is stored twice. `tests/compaction_races.rs` pins it.
//!
//! The guard here closes that in the commit itself. [`guarded`] wraps a table's log store so
//! that, immediately before delta-rs puts commit N, every commit between the snapshot the
//! compaction planned on and N is read, and the write is refused as a
//! [`CommitConflictError::ConcurrentDeleteDelete`] if one of them removed a file this commit
//! removes. The put that follows is put-if-absent, so the check and the write are one atomic
//! step: a commit that slips in between makes the put fail, delta-rs retries at a higher
//! version, and the check runs again over what is new. Exact against any engine, by
//! construction, with no lease and no clock.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use deltalake::kernel::transaction::{CommitConflictError, TransactionError};
use deltalake::logstore::object_store::{ObjectStore, ObjectStoreExt};
use deltalake::logstore::{CommitOrBytes, LogStore, LogStoreConfig, LogStoreRef};
use deltalake::{DeltaResult, DeltaTable};
use uuid::Uuid;

use crate::error::{Error, Result};

/// The paths a commit file removes.
fn removed_paths(commit: &[u8]) -> HashSet<String> {
    let mut out = HashSet::new();
    for line in commit.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(line) else {
            continue;
        };
        if let Some(p) = v
            .get("remove")
            .and_then(|r| r.get("path"))
            .and_then(|p| p.as_str())
        {
            out.insert(p.to_string());
        }
    }
    out
}

/// A log store that refuses a commit removing a file a commit since `checked` already removed.
struct GuardedLogStore {
    inner: LogStoreRef,
    /// The highest version known to hold no conflicting remove.
    checked: Mutex<u64>,
}

impl GuardedLogStore {
    async fn our_bytes(&self, commit: &CommitOrBytes, op: Uuid) -> DeltaResult<Bytes> {
        match commit {
            CommitOrBytes::LogBytes(b) => Ok(b.clone()),
            CommitOrBytes::TmpCommit(path) => {
                let store = self.inner.object_store(Some(op));
                Ok(store.get(path).await?.bytes().await?)
            }
        }
    }
}

#[async_trait::async_trait]
impl LogStore for GuardedLogStore {
    fn name(&self) -> String {
        // The inner store's own name: delta-rs picks the shape of a commit (bytes or a tmp
        // file) from it.
        self.inner.name()
    }

    async fn refresh(&self) -> DeltaResult<()> {
        self.inner.refresh().await
    }

    async fn read_commit_entry(&self, version: u64) -> DeltaResult<Option<Bytes>> {
        self.inner.read_commit_entry(version).await
    }

    async fn write_commit_entry(
        &self,
        version: u64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> std::result::Result<(), TransactionError> {
        let ours = removed_paths(
            &self
                .our_bytes(&commit_or_bytes, operation_id)
                .await
                .map_err(|e| TransactionError::LogStoreError {
                    msg: e.to_string(),
                    source: Box::new(e),
                })?,
        );
        if !ours.is_empty() {
            let from = *self.checked.lock().unwrap() + 1;
            for v in from..version {
                let theirs = self.inner.read_commit_entry(v).await.map_err(|e| {
                    TransactionError::LogStoreError {
                        msg: format!("reading commit {v} to check it against a compaction: {e}"),
                        source: Box::new(e),
                    }
                })?;
                // A commit the log no longer holds cannot be checked, and a compaction that
                // cannot be checked does not land.
                let Some(theirs) = theirs else {
                    return Err(TransactionError::CommitConflict(
                        CommitConflictError::ConcurrentDeleteDelete,
                    ));
                };
                if !removed_paths(&theirs).is_disjoint(&ours) {
                    return Err(TransactionError::CommitConflict(
                        CommitConflictError::ConcurrentDeleteDelete,
                    ));
                }
            }
            let mut checked = self.checked.lock().unwrap();
            *checked = (*checked).max(version - 1);
        }
        self.inner
            .write_commit_entry(version, commit_or_bytes, operation_id)
            .await
    }

    async fn abort_commit_entry(
        &self,
        version: u64,
        commit_or_bytes: CommitOrBytes,
        operation_id: Uuid,
    ) -> std::result::Result<(), TransactionError> {
        self.inner
            .abort_commit_entry(version, commit_or_bytes, operation_id)
            .await
    }

    async fn get_latest_version(&self, start_version: u64) -> DeltaResult<u64> {
        self.inner.get_latest_version(start_version).await
    }

    fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.inner.object_store(operation_id)
    }

    fn root_object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.inner.root_object_store(operation_id)
    }

    fn config(&self) -> &LogStoreConfig {
        self.inner.config()
    }
}

/// `table` as a handle whose commits are checked against everything committed since `table`'s
/// own version, which is the snapshot a compaction of it plans on.
pub async fn guarded(table: &DeltaTable) -> Result<DeltaTable> {
    let base = table
        .version()
        .ok_or_else(|| Error::Other("cannot guard a table that is not loaded".into()))?;
    let store: LogStoreRef = Arc::new(GuardedLogStore {
        inner: table.log_store(),
        checked: Mutex::new(base),
    });
    let mut out = DeltaTable::new(store);
    out.load().await.map_err(Error::Delta)?;
    // `load` reads head, which may be past `base`: plan on what was asked for.
    if out.version() != Some(base) {
        out.load_version(base).await.map_err(Error::Delta)?;
    }
    Ok(out)
}
