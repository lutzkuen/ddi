//! Delta sink: write parquet and commit the offset in the **same** Delta transaction.
//!
//! The atomicity of that single commit is the entire exactly-once argument. If the process
//! dies at any point before the commit lands, nothing was written and the offset did not
//! move. If it dies after, both moved. There is no window in which data exists without its
//! offset, or vice versa.
//!
//! That argument holds for a MERGE exactly as it does for an append —
//! `CommitProperties::with_application_transaction` puts the `txn` action in the merge's own
//! commit — with one wrinkle delta-rs introduces and [`Sink::upsert`] has to close: a merge
//! that changes nothing writes **no commit at all**, and would take the offset down with it.

use std::num::NonZeroU64;

use deltalake::arrow::array::RecordBatch;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::Transaction;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTable;
use tracing::debug;

use crate::error::{Error, Result};
use crate::lookup::LookupSnapshot;
use crate::upsert::MergePlan;

pub struct Sink {
    app_id: String,
    target_file_size: Option<NonZeroU64>,
    /// Which source table this pipeline is reading, so a later run can tell whether it is
    /// still the same one. A dropped-and-recreated source keeps its path but gets a new
    /// id, and nothing else in the log records that.
    source_table_id: Option<String>,
    /// The exact lookup snapshots that enriched the source batch currently being committed.
    lookup_snapshots: Vec<LookupCommit>,
}

#[derive(Clone, Debug)]
struct LookupCommit {
    name: String,
    version: i64,
    table_id: Option<String>,
    used_pre_history: bool,
    used_current: bool,
}

impl Sink {
    pub fn new(app_id: impl Into<String>, target_file_size: u64) -> Self {
        Self {
            app_id: app_id.into(),
            target_file_size: NonZeroU64::new(target_file_size),
            source_table_id: None,
            lookup_snapshots: Vec::new(),
        }
    }

    pub fn with_source_table_id(mut self, id: Option<String>) -> Self {
        self.source_table_id = id;
        self
    }

    /// Replace the provenance recorded with the next target commit.
    ///
    /// This is set immediately after snapshots are selected and before any work that could
    /// write the target. A retry repeats selection from the same source commit timestamp, so
    /// the metadata also tells an operator exactly which FX version produced a row.
    pub fn set_lookup_snapshots(&mut self, snapshots: &[LookupSnapshot]) {
        self.lookup_snapshots = snapshots
            .iter()
            .map(|snapshot| LookupCommit {
                name: snapshot.name.clone(),
                version: snapshot.version as i64,
                table_id: snapshot.table_id.clone(),
                used_pre_history: snapshot.used_pre_history,
                used_current: snapshot.used_current,
            })
            .collect();
    }

    /// The commit properties every write of ours carries, whatever its shape.
    fn properties(&self, source_version: i64) -> CommitProperties {
        let mut metadata = vec![
            (
                "ddi.sourceVersion".to_string(),
                serde_json::Value::from(source_version),
            ),
            (
                "ddi.appId".to_string(),
                serde_json::Value::from(self.app_id.clone()),
            ),
        ];
        if let Some(id) = &self.source_table_id {
            metadata.push((
                "ddi.sourceTableId".to_string(),
                serde_json::Value::from(id.clone()),
            ));
        }
        for lookup in &self.lookup_snapshots {
            let prefix = format!("ddi.lookup.{}", lookup.name);
            metadata.push((
                format!("{prefix}.version"),
                serde_json::Value::from(lookup.version),
            ));
            if let Some(id) = &lookup.table_id {
                metadata.push((
                    format!("{prefix}.tableId"),
                    serde_json::Value::from(id.clone()),
                ));
            }
            if lookup.used_pre_history {
                metadata.push((
                    format!("{prefix}.preHistory"),
                    serde_json::Value::from(true),
                ));
            }
            if lookup.used_current {
                metadata.push((format!("{prefix}.current"), serde_json::Value::from(true)));
            }
        }

        CommitProperties::default()
            .with_application_transaction(Transaction::new(&self.app_id, source_version))
            // Recorded for operators and for v2's mid-commit cursor work. Purely
            // informational today: the txn action above is the authority.
            .with_metadata(metadata)
    }

    /// Append `batches` and record `source_version` in a `txn` action, atomically.
    ///
    /// Returns the table at its new version.
    ///
    /// `SaveMode::Append` is not a choice here. Overwrite would destroy the very history
    /// the offset refers to; replacing rows in place is [`Self::upsert`], and it is a
    /// different method rather than a save mode because it needs a key, a window, and a
    /// rule for which row wins.
    pub async fn commit(
        &self,
        table: DeltaTable,
        batches: Vec<RecordBatch>,
        source_version: i64,
    ) -> Result<DeltaTable> {
        let props = self.properties(source_version);

        // The same runtime every other session in this process gets. A plain append has little
        // to spill today — an in-memory input, and no partition columns anywhere in this
        // tool's config — but delta-rs builds its own SessionState when it is not given one,
        // with an unbounded memory pool and a DiskManager this process has never seen. A
        // runtime nobody configured is precisely the hole `crate::spill` exists to close, and
        // leaving one open because it happens to be quiet is how it gets loud later. Taken
        // before `write` moves the table, and `delta_session` rather than `session` because a
        // write is planned through delta-rs's own node — see there.
        let session = std::sync::Arc::new(crate::budget::delta_session(&table)?);

        let mut write = table
            .write(batches)
            .with_session_state(session)
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

        let table = write
            .await
            .map_err(|e| crate::spill::classify_delta(e, "append: writing the batch"))?;
        debug!(
            app_id = %self.app_id,
            source_version,
            target_version = ?table.version(),
            "committed data + txn atomically"
        );
        Ok(table)
    }

    /// Merge `batch` onto the keys it already has, and record `source_version`, atomically.
    ///
    /// `batch` must already hold at most one row per key — see [`crate::upsert::collapse`],
    /// which is not optional: delta-rs aborts a merge whose source matches one target row
    /// twice.
    ///
    /// # Why the version is checked afterwards
    ///
    /// delta-rs returns early without committing when a merge produced no file actions
    /// (`operations/merge/mod.rs`: `if actions.is_empty() { return Ok(...) }`), and it does
    /// that *after* the `txn` action has been attached — so the offset silently fails to
    /// advance. This is not an edge case here, it is a guaranteed one: the whole point of
    /// `s.<seq> > t.<seq>` is that re-delivered old rows change nothing, and a batch of
    /// them produces exactly zero actions. Left alone, the pipeline would re-read the same
    /// source commits forever.
    ///
    /// So when the table did not move, an empty append carries the `txn` action instead.
    /// A crash between the two is safe in the only direction that matters: the offset stays
    /// put and the merge runs again, which by key is a no-op.
    pub async fn upsert(
        &self,
        table: DeltaTable,
        batch: RecordBatch,
        source_version: i64,
        plan: &MergePlan,
    ) -> Result<(DeltaTable, UpsertStats)> {
        use deltalake::datafusion::prelude::SessionContext;

        let before = table.version();
        let rows = batch.num_rows();
        let target_schema = batch.schema();

        let session = std::sync::Arc::new(crate::budget::session(&table)?);
        let ctx = SessionContext::new_with_state((*session).clone());
        let source = ctx
            .read_batch(batch)
            .map_err(|e| Error::Other(format!("upsert: cannot register the batch: {e}")))?;

        let mut merge = table
            .merge(source, plan.predicate.clone())
            .with_source_alias(crate::upsert::SOURCE_ALIAS)
            .with_target_alias(crate::upsert::TARGET_ALIAS)
            // The append path pins both of these; the merge path needs them pinned
            // separately, because it casts inside its own UPDATE/INSERT projections rather
            // than through SchemaCoercer.
            //
            // NOTE the polarity of safe_cast, as in `commit` above: `true` would turn a
            // failed cast into a NULL, which is the silent data-quality failure this tool
            // exists to avoid. `false` makes it an error.
            .with_safe_cast(false)
            // The target schema is the contract and is never evolved. This is the merge's
            // equivalent of `commit`'s "no schema_mode is set".
            .with_merge_schema(false)
            // Leave the source stats pass on: it is what lets delta-rs turn
            // `t.key = s.key` into a static key range and skip target files by it. The
            // batch is an in-memory table, so re-planning it is cheap.
            .with_streaming(false)
            // The merge reads the target and joins it against the batch, and both of those
            // are DataFusion's to hold. Giving it the pipeline's share is what makes a
            // merge window that turned out wider than expected spill instead of OOM.
            .with_session_state(session)
            .with_commit_properties(self.properties(source_version));

        merge = merge
            .when_matched_update(|update| {
                let mut u = update.predicate(plan.newer_than_stored.clone());
                for c in &plan.update_columns {
                    u = u.update(quoted(c), source_col(c));
                }
                u
            })
            .map_err(Error::Delta)?
            .when_not_matched_insert(|insert| {
                let mut i = insert;
                for c in &plan.insert_columns {
                    i = i.set(quoted(c), source_col(c));
                }
                i
            })
            .map_err(Error::Delta)?;

        // Classified rather than wrapped: a merge joins a slice of the target against the
        // batch, so it is the largest spiller here, and a full spill directory arriving as a
        // plain Delta error is indistinguishable from a wrong answer to the retry loop.
        let (table, metrics) = merge
            .await
            .map_err(|e| crate::spill::classify_delta(e, "upsert: merging into the target"))?;

        let stats = UpsertStats {
            rows_in: rows,
            updated: metrics.num_target_rows_updated,
            inserted: metrics.num_target_rows_inserted,
            files_scanned: metrics.num_target_files_scanned,
            committed: table.version() != before,
            window_bounded: plan.window.is_bounded(),
            window_clamped: plan.window.clamped,
            // The caller owns both: the wait happened before this sink was entered, and the
            // retry loop that may run this method more than once is out there too.
            queue_millis: 0,
            merge_millis: 0,
        };

        if stats.committed {
            debug!(
                app_id = %self.app_id,
                source_version,
                target_version = ?table.version(),
                updated = stats.updated,
                inserted = stats.inserted,
                files_scanned = stats.files_scanned,
                "merged data + txn atomically"
            );
            return Ok((table, stats));
        }

        // Nothing changed, so delta-rs wrote nothing — including our txn action. Advance
        // the offset on its own, or these source commits are re-read forever.
        debug!(
            app_id = %self.app_id,
            source_version,
            rows,
            "merge changed nothing; committing the offset on its own"
        );
        let empty = RecordBatch::new_empty(target_schema);
        let table = self.commit(table, vec![empty], source_version).await?;
        Ok((table, stats))
    }
}

/// What one merge did, for logs and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpsertStats {
    /// Rows handed to the merge, after collapsing duplicate keys.
    pub rows_in: usize,
    /// Existing rows replaced by a newer one.
    pub updated: usize,
    /// Keys the target did not have.
    pub inserted: usize,
    /// Target files the merge had to open. The cost the window exists to keep down.
    pub files_scanned: usize,
    /// False when the merge changed nothing and the offset had to be committed separately.
    pub committed: bool,
    /// False when the target's statistics could not bound the window at all, so the whole
    /// table was read.
    pub window_bounded: bool,
    /// True when `upsert_lookback` held the window above what completeness asked for, so a
    /// key may have been inserted alongside an older row instead of replacing it.
    pub window_clamped: bool,
    /// Milliseconds spent waiting for a merge permit, before any work began.
    ///
    /// Filled in by the caller rather than here: the wait happens outside this sink, and a
    /// sink that timed its own queue would be reporting a wait it never did.
    pub queue_millis: u64,
    /// Milliseconds spent merging, permit in hand, including any replanned attempts. Those
    /// attempts really were spent, so counting only the last one would understate what the
    /// target cost.
    pub merge_millis: u64,
}

impl UpsertStats {
    /// Rows that actually landed. Deliberately excludes delta-rs's "copied" count, which
    /// counts untouched rows rewritten as a side effect of rewriting their file.
    pub fn rows_written(&self) -> usize {
        self.updated + self.inserted
    }
}

/// `s."col"`, quoted so a column called `order` survives the planner.
fn source_col(name: &str) -> deltalake::datafusion::prelude::Expr {
    deltalake::datafusion::prelude::col(format!("{}.{}", crate::upsert::SOURCE_ALIAS, quoted(name)))
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
