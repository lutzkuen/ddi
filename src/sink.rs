//! Delta sink: write parquet and commit the offset in the **same** Delta transaction.
//!
//! The atomicity of that single commit is the entire exactly-once argument. If the process
//! dies at any point before the commit lands, nothing was written and the offset did not
//! move. If it dies after, both moved. There is no window in which data exists without its
//! offset, or vice versa.

use std::num::NonZeroU64;

use deltalake::arrow::array::RecordBatch;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::Transaction;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use tracing::debug;

use crate::error::{Error, Result};

pub struct Sink {
    app_id: String,
    target_file_size: Option<NonZeroU64>,
}

impl Sink {
    pub fn new(app_id: impl Into<String>, target_file_size: u64) -> Self {
        Self {
            app_id: app_id.into(),
            target_file_size: NonZeroU64::new(target_file_size),
        }
    }

    /// Append `batches` and record `source_version` in a `txn` action, atomically.
    ///
    /// Returns the table at its new version.
    ///
    /// `SaveMode::Append` is not a choice — this tool is append-only by design, and an
    /// overwrite would destroy the very history the offset refers to.
    pub async fn commit(
        &self,
        table: DeltaTable,
        batches: Vec<RecordBatch>,
        source_version: i64,
    ) -> Result<DeltaTable> {
        let txn = Transaction::new(&self.app_id, source_version);
        let props = CommitProperties::default()
            .with_application_transaction(txn)
            // Recorded for operators and for v2's mid-commit cursor work. Purely
            // informational today: the txn action above is the authority.
            .with_metadata([
                (
                    "ddi.sourceVersion".to_string(),
                    serde_json::Value::from(source_version),
                ),
                (
                    "ddi.appId".to_string(),
                    serde_json::Value::from(self.app_id.clone()),
                ),
            ]);

        let mut write = table
            .write(batches)
            .with_save_mode(SaveMode::Append)
            // NOTE the polarity: `safe = true` makes a failed cast produce NULL, which is
            // precisely the silent data-quality failure this tool exists to avoid. `false`
            // makes it an error. No schema_mode is set, so the target schema is never
            // evolved — a mismatch fails instead (plan §2.7).
            .with_cast_safety(false)
            .with_commit_properties(props);

        if let Some(size) = self.target_file_size {
            write = write.with_target_file_size(Some(size));
        }

        let table = write.await.map_err(Error::Delta)?;
        debug!(
            app_id = %self.app_id,
            source_version,
            target_version = ?table.version(),
            "committed data + txn atomically"
        );
        Ok(table)
    }
}
