//! The core loop.

use std::sync::Arc;
use std::time::Instant;

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::delta_datafusion::DataFusionMixins;
use deltalake::DeltaTable;
use futures::TryStreamExt;
use tracing::{debug, info, warn};

use crate::config::ResolvedPipeline;
use crate::dbt::watermark;
use crate::dedup::{self, Dedup};
use crate::dq::DataQuality;
use crate::error::{Error, Result};
use crate::offset::OffsetStore;
use crate::schema::{Rejected, SchemaCoercer};
use crate::sink::{Sink, UpsertStats};
use crate::source::{LogBatch, LogStreamBuilder, StreamCursor, Version};
use crate::transform::{Identity, SqlTransform, Transform};
use crate::upsert::{self, MergePlan};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    /// Nothing new in the source.
    CaughtUp,
    /// A batch was transformed and committed.
    Progressed {
        through_version: Version,
        files: usize,
        rows: usize,
        target_version: Option<Version>,
        /// What the merge did, when this pipeline upserts. `None` in append mode.
        upsert: Option<UpsertStats>,
        /// Rows the target would not take, written to the data-quality table instead.
        rejected: usize,
    },

    /// Source commits existed but produced no rows for the target — every row was filtered
    /// out, already covered, or rejected. The offset still advances so the same commits are
    /// not re-read forever.
    Skipped {
        through_version: Version,
        /// Rows the target would not take. Non-zero here is the loud case: the batch had
        /// rows and *none* of them made it, which is usually a schema change upstream.
        rejected: usize,
    },
}

pub struct Pipeline {
    cfg: ResolvedPipeline,
    source: DeltaTable,
    target: DeltaTable,
    stream: LogStreamBuilder,
    transform: Box<dyn Transform>,
    sink: Sink,
    offsets: OffsetStore,
    coercer: SchemaCoercer,
    /// What the target already held when this pipeline opened. Rows it covers are
    /// suppressed, because a rebuild already wrote them.
    dedup: Dedup,
    /// Where rows the target will not take are put. `None` when there is no such table, in
    /// which case a bad row still stops the pipeline — see [`crate::dq`].
    dq: Option<DataQuality>,
    /// What decoding this source costs, shared with the stream that sizes batches by it.
    amplification: Arc<crate::budget::Amplification>,
}

impl Pipeline {
    /// Open both tables, resolve the resume point, and prepare the loop.
    pub async fn open(cfg: ResolvedPipeline) -> Result<Self> {
        let source = cfg
            .storage
            .open(&cfg.source_uri)
            .await
            .map_err(|e| Error::Config(format!("pipeline {:?}: source: {e}", cfg.name)))?;
        let target = cfg.storage.open(&cfg.target_uri).await.map_err(|e| {
            Error::Config(format!(
                "pipeline {:?}: target: {e}. This tool never creates the target table — \
                 create it with external tooling first (plan §2.7).",
                cfg.name
            ))
        })?;

        let offsets = OffsetStore::new(&cfg.app_id, cfg.starting_version);
        let cursor = resume_cursor(&cfg, &offsets, &source, &target).await?;

        let cursor = adjust_for_replaced_source(&cfg, &source, &target, cursor).await?;

        let target_schema: SchemaRef = target
            .snapshot()
            .map_err(Error::Delta)?
            .snapshot()
            .read_schema();

        let stream = LogStreamBuilder::new(&source)
            .with_starting_cursor(cursor)
            .with_change_policy(cfg.change_policy)
            .with_max_files_per_batch(cfg.max_files_per_batch)
            .with_max_bytes_per_batch(cfg.max_bytes_per_batch);
        let amplification = stream.amplification();

        let transform: Box<dyn Transform> = match &cfg.transform_sql {
            Some(sql) => Box::new(SqlTransform::new(sql.clone())),
            None => Box::new(Identity),
        };

        let sink =
            Sink::new(&cfg.app_id, cfg.target_file_size).with_source_table_id(table_id(&source));

        info!(
            pipeline = %cfg.name,
            app_id = %cfg.app_id,
            resume_from = %cursor,
            transform = %transform.describe(),
            "pipeline ready"
        );

        let dedup = match &cfg.dedup_timestamp {
            Some(ts) => {
                let d = Dedup::read(&target, ts, cfg.dedup_key.as_deref()).await?;
                info!(
                    pipeline = %cfg.name,
                    dedup_timestamp = %ts,
                    dedup_key = ?cfg.dedup_key,
                    watermark_known = d.watermark_is_known(),
                    boundary_keys = d.boundary_key_count(),
                    "rows the target already covers will be skipped"
                );
                d
            }
            None => Dedup::default(),
        };

        if cfg.write_mode.is_upsert() {
            // resolve() guarantees both of these are set for an upsert pipeline.
            let key = cfg.upsert_key.as_deref().expect("checked in resolve");
            let sequence = cfg.dedup_timestamp.as_deref().expect("checked in resolve");
            upsert::preflight(&target, &target_schema, key, sequence, cfg.upsert_lookback)
                .await
                .map_err(|e| Error::Config(format!("pipeline {:?}: {e}", cfg.name)))?;
            info!(
                pipeline = %cfg.name,
                upsert_key = %key,
                sequence = %sequence,
                lookback = ?cfg.upsert_lookback,
                "rows will replace the key they already have, when they are newer"
            );
        }

        // Whether bad rows can be set aside is decided here, once, rather than on the batch
        // that meets one — so an operator learns which mode a pipeline is in from its
        // startup line, not from an incident.
        let dq_uri = cfg.dq_uri();
        let dq = DataQuality::open(&cfg.storage, &dq_uri, &cfg.app_id, &cfg.name).await?;
        match &dq {
            Some(_) => info!(
                pipeline = %cfg.name,
                dq_uri = %dq_uri,
                "rows the target will not take will be written here instead of stopping the \
                 pipeline"
            ),
            None => info!(
                pipeline = %cfg.name,
                dq_uri = %dq_uri,
                "no data-quality table; a row the target will not take stops this pipeline \
                 (which then retries). Create the table to have such rows set aside instead."
            ),
        }

        Ok(Self {
            cfg,
            source,
            target,
            stream,
            transform,
            sink,
            offsets,
            coercer: SchemaCoercer::new(target_schema),
            dedup,
            dq,
            amplification,
        })
    }

    pub fn name(&self) -> &str {
        &self.cfg.name
    }

    pub fn cursor(&self) -> StreamCursor {
        self.stream.cursor()
    }

    /// Source head as of the last step. `None` before the first step.
    pub fn source_head_version(&self) -> Option<Version> {
        self.stream.last_known_head()
    }

    /// One iteration: pull, read, transform, coerce, commit — all or nothing.
    pub async fn step(&mut self) -> Result<StepOutcome> {
        let started = Instant::now();

        // 1. Pull one bounded batch of new files from the source log.
        let Some(batch) = self.stream.next_batch().await? else {
            return Ok(StepOutcome::CaughtUp);
        };
        let files = batch.files.len();
        let through = batch.through_version;

        // 2. Read those files as Arrow.
        let input = self.scan(&batch).await?;
        let in_rows: usize = input.iter().map(|b| b.num_rows()).sum();

        // What that cost, so the next batch can be sized by it rather than by a constant.
        // `max_bytes_per_batch` counts the compressed bytes the log recorded; this is what
        // they became once decoded, and the ratio between them is the whole reason a
        // 256 MB setting can hold a gigabyte and a half.
        let decoded: u64 = input.iter().map(|b| b.get_array_memory_size() as u64).sum();
        self.amplification.observe(batch.total_bytes(), decoded);

        // 3. Transform. Stateless, row-local, validated at config load.
        let output = self.transform.apply(input).await?;

        // Which target columns the transform actually produced, read *before* coercion —
        // afterwards every target column is present, because that is what coercion does,
        // and the ones it invented are nulls an upsert must not write. See
        // `SchemaCoercer::columns_present_in`.
        let produced: Vec<String> = output
            .iter()
            .find(|b| b.num_rows() > 0)
            .map(|b| self.coercer.columns_present_in(b))
            .unwrap_or_default();

        // 4. Cast to the target schema. Never a silent null: either the whole batch fails,
        // or the rows that will not convert are set aside for the data-quality table and
        // the rest goes on. Which of those happens is decided once, at open, by whether
        // there is a table to set them aside in.
        let mut coerced = Vec::with_capacity(output.len());
        let mut rejects: Vec<Rejected> = Vec::new();
        for b in &output {
            if b.num_rows() == 0 {
                continue;
            }
            let c = match self.dq.is_some() {
                true => {
                    let split = self.coercer.coerce_quarantining(b)?;
                    if let Some(bad) = split.bad {
                        rejects.push(bad);
                    }
                    split.good
                }
                false => self.coercer.coerce(b)?,
            };
            let c = self.dedup.apply(c)?;
            if c.num_rows() > 0 {
                coerced.push(c);
            }
        }
        let out_rows: usize = coerced.iter().map(|b| b.num_rows()).sum();
        let rejected_rows: usize = rejects.iter().map(Rejected::len).sum();

        // Unnest amplification guard (plan §3): a 64 MB source file must not become 6 GB
        // of RAM downstream. Checked after the transform because that is when the real
        // row count is known.
        if out_rows > self.cfg.max_output_rows_per_batch {
            warn!(
                pipeline = %self.cfg.name,
                out_rows,
                limit = self.cfg.max_output_rows_per_batch,
                "batch exceeded max_output_rows_per_batch; consider lowering \
                 max_bytes_per_batch for this pipeline"
            );
        }

        // The offset must advance even when a batch produces no rows — otherwise a
        // fully-filtered commit would be re-read forever.
        let txn_version = self.offsets.txn_version_for(batch.end)?;

        // 4b. Rejects go to their own table *before* the target commits, and they are the
        // only thing here that is not covered by the batch's own atomicity — two tables
        // cannot share one Delta commit. Writing them first is what makes the failure mode
        // a duplicate rather than a gap: a crash in between replays the batch, and the
        // reject is written again unless the data-quality table's own txn action says it
        // is already there. See `crate::dq`.
        if rejected_rows > 0 {
            let written = self.write_rejects(&rejects, txn_version).await?;
            if in_rows > 0 && out_rows == 0 {
                // Every row failed. That is far more likely to be an upstream type change
                // than a batch of uniformly bad data, and it is invisible in the target —
                // which simply stops growing — so it is said out loud here.
                warn!(
                    pipeline = %self.cfg.name,
                    through_version = through,
                    rejected = rejected_rows,
                    reason = rejects
                        .first()
                        .and_then(|r| r.reasons.first())
                        .map(String::as_str)
                        .unwrap_or("unknown"),
                    "every row in this batch went to the data-quality table. If this repeats, \
                     the source's schema has probably changed rather than its data going bad."
                );
            }
            debug!(
                pipeline = %self.cfg.name,
                rejected = rejected_rows,
                written,
                "rows the target would not take"
            );
        }

        if coerced.is_empty() {
            // Nothing to write, but the source version was still consumed, so the offset
            // must advance or these commits would be re-read forever. A zero-row batch in
            // the target schema (rather than an empty batch *list*, which delta-rs
            // rejects) commits the txn action with no data attached.
            //
            // Always the append writer, never the merge, whatever the write mode: delta-rs
            // declines to commit a merge that produced no actions, which would take the
            // txn action down with it and strand the offset here forever.
            let empty = RecordBatch::new_empty(self.coercer.target());
            self.commit(vec![empty], txn_version).await?;
            return Ok(StepOutcome::Skipped {
                through_version: through,
                rejected: rejected_rows,
            });
        }

        // 5+6. Write parquet and commit the data actions AND the txn action atomically.
        let (out_rows, upsert) = if self.cfg.write_mode.is_upsert() {
            let stats = self.upsert(coerced, produced, txn_version).await?;
            (stats.rows_written(), Some(stats))
        } else {
            self.commit(coerced, txn_version).await?;
            (out_rows, None)
        };

        let target_version = self.target.version();
        info!(
            pipeline = %self.cfg.name,
            through_version = through,
            files,
            in_rows,
            out_rows,
            target_version = ?target_version,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "committed"
        );

        Ok(StepOutcome::Progressed {
            through_version: through,
            files,
            rows: out_rows,
            target_version,
            upsert,
            rejected: rejected_rows,
        })
    }

    /// Write this batch's rejects to the data-quality table.
    ///
    /// Errors rather than swallowing: if the rejects cannot be recorded, the target must not
    /// commit either, or the offset would advance past rows that went nowhere. Failing here
    /// leaves the whole batch to be retried, which is why the write is idempotent.
    async fn write_rejects(&mut self, rejects: &[Rejected], txn_version: i64) -> Result<usize> {
        let Some(dq) = self.dq.as_mut() else {
            // Unreachable: rejects are only produced when a table was opened.
            return Err(Error::Other(
                "internal: rows were rejected but there is no data-quality table".into(),
            ));
        };
        let now = chrono::Utc::now().naive_utc().and_utc().timestamp_micros();
        dq.write(rejects, txn_version, now).await
    }

    /// Collapse the batch to one row per key, work out how much of the target the merge has
    /// to see, and merge.
    async fn upsert(
        &mut self,
        batches: Vec<RecordBatch>,
        update_columns: Vec<String>,
        txn_version: i64,
    ) -> Result<UpsertStats> {
        let key = self.cfg.upsert_key.as_deref().expect("checked in resolve");
        let sequence = self
            .cfg
            .dedup_timestamp
            .as_deref()
            .expect("checked in resolve");

        let batch = upsert::collapse(&batches, key, sequence)?;
        let collapsed_away: usize =
            batches.iter().map(|b| b.num_rows()).sum::<usize>() - batch.num_rows();

        // A merge reads the target, so unlike a blind append it can lose a commit race —
        // and the writer most likely to be racing is the nightly rebuild this tool is built
        // to coexist with. delta-rs reports the conflict rather than replanning, because
        // the plan it holds was built against files that no longer exist.
        //
        // Replanning is exactly what is needed, and it is safe to do: merging by key is
        // idempotent, so redoing the work cannot double-apply it. A blind retry of the same
        // plan would not be — it would merge against a stale window.
        for attempt in 1..=MERGE_ATTEMPTS {
            let plan = MergePlan::resolve(
                &self.target,
                &batch,
                key,
                sequence,
                self.cfg.upsert_lookback,
                update_columns.clone(),
            )?;
            self.warn_about_window(&plan);

            let table = std::mem::replace(&mut self.target, DeltaTable::new_in_memory());
            match self
                .sink
                .upsert(table, batch.clone(), txn_version, &plan)
                .await
            {
                Ok((t, stats)) => {
                    self.target = t;
                    info!(
                        pipeline = %self.cfg.name,
                        updated = stats.updated,
                        inserted = stats.inserted,
                        collapsed_away,
                        target_files_scanned = stats.files_scanned,
                        window = %plan.window.lower_bound_display(),
                        window_bounded = plan.window.is_bounded(),
                        window_clamped = plan.window.clamped,
                        candidate_files = plan.window.candidate_files,
                        attempt,
                        "merged"
                    );
                    return Ok(stats);
                }
                Err(e) => {
                    // Restore a usable handle either way — and note that reopening is what
                    // makes the second of these two worth retrying at all, because
                    // `Storage::open` is where a checkpoint this build cannot parse gets
                    // stepped over.
                    self.target = self.cfg.storage.open(&self.cfg.target_uri).await?;
                    if !worth_replanning(&e) || attempt == MERGE_ATTEMPTS {
                        return Err(e);
                    }
                    warn!(
                        pipeline = %self.cfg.name,
                        attempt,
                        of = MERGE_ATTEMPTS,
                        error = %e,
                        "another writer committed to the target while this merge was running; \
                         replanning against what it left behind"
                    );
                }
            }
        }
        unreachable!("the loop returns on its last attempt")
    }

    fn warn_about_window(&self, plan: &MergePlan) {
        if plan.window.clamped {
            warn!(
                pipeline = %self.cfg.name,
                lower_bound = %plan.window.lower_bound_display(),
                candidate_files = plan.window.candidate_files,
                "upsert_lookback stopped the merge window short of what the target's \
                 statistics asked for. A key in this batch may have an older row below the \
                 floor; it will be inserted alongside rather than replacing it. Raise \
                 upsert_lookback, or remove it to let completeness win."
            );
        } else if !plan.window.is_bounded() {
            warn!(
                pipeline = %self.cfg.name,
                reason = plan.window.unbounded_because.unwrap_or("unknown"),
                "the merge has to read the whole target: its statistics cannot rule any file \
                 out. Correct, but the cost grows with the table."
            );
        }
    }

    async fn commit(&mut self, batches: Vec<RecordBatch>, txn_version: i64) -> Result<()> {
        // The sink consumes the table and returns it at its new version.
        let table = std::mem::replace(&mut self.target, DeltaTable::new_in_memory());
        match self.sink.commit(table, batches, txn_version).await {
            Ok(t) => {
                self.target = t;
                Ok(())
            }
            Err(e) => {
                // Restore a usable handle so a retry can proceed.
                self.target = self.cfg.storage.open(&self.cfg.target_uri).await?;
                Err(e)
            }
        }
    }

    /// Read exactly the files in `batch` — not the whole table.
    ///
    /// Reads each `Add`'s parquet straight from the table's object store, rather than
    /// going through a DataFusion listing table. Two reasons: the read stays aligned with
    /// the log we classified (a concurrent writer cannot slip in unaccounted files), and
    /// the Delta object store is prefixed at the table root, which makes URL-based listing
    /// silently return zero rows instead of erroring.
    ///
    /// Delta keeps partition column values in the `Add` action rather than in the parquet
    /// file, so they are re-attached here.
    ///
    /// The files are read as the source table's schema declares them, not as they happen to
    /// be typed on disk. `OPTIMIZE` run by another engine rewrites files at that engine's
    /// idea of precision — Trino writes `timestamp` as milliseconds — and every one of them
    /// is a legal member of a table whose schema says microseconds. See
    /// [`crate::schema::read_as_declared`]. Without it a batch spanning one Trino-written
    /// file and one delta-rs-written file would not even be a batch: two schemas, one
    /// transform.
    async fn scan(&self, batch: &LogBatch) -> Result<Vec<RecordBatch>> {
        use deltalake::parquet::arrow::async_reader::{
            ParquetObjectReader, ParquetRecordBatchStreamBuilder,
        };
        use deltalake::Path as StorePath;

        let store = self.source.log_store().object_store(None);
        let declared = arrow_schema_of(&batch.schema)?;
        let partition_cols: Vec<String> = self
            .source
            .snapshot()
            .map_err(Error::Delta)?
            .metadata()
            .partition_columns()
            .to_vec();

        let mut out = Vec::new();
        for add in &batch.files {
            let path = StorePath::parse(&add.path)
                .map_err(|e| Error::Other(format!("bad file path {:?}: {e}", add.path)))?;
            // Supply the size from the Add action rather than letting the reader probe
            // for it. The probe is a suffix range request ("last N bytes"), which Azure
            // Blob Storage does not implement — on local disk it works, so this only
            // surfaces against real object storage.
            let reader = ParquetObjectReader::new(store.clone(), path)
                .with_file_size(add.size.max(0) as u64);
            let stream = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|e| Error::Transform(format!("cannot open {:?}: {e}", add.path)))?
                .build()
                .map_err(|e| Error::Transform(format!("cannot read {:?}: {e}", add.path)))?;

            let batches: Vec<RecordBatch> = stream
                .try_collect()
                .await
                .map_err(|e| Error::Transform(format!("read failed for {:?}: {e}", add.path)))?;

            for b in batches {
                // Before the partition columns, which come from the log as text and are
                // cast by the coercer, not from the file.
                let b = crate::schema::read_as_declared(b, &declared)
                    .map_err(|e| Error::Schema(format!("{:?}: {e}", add.path)))?;
                out.push(if partition_cols.is_empty() {
                    b
                } else {
                    attach_partition_columns(b, &partition_cols, &add.partition_values)?
                });
            }
        }
        Ok(out)
    }

    /// Run until caught up, then return how many batches were committed.
    pub async fn run_until_caught_up(&mut self) -> Result<usize> {
        let mut n = 0;
        loop {
            match self.step().await? {
                StepOutcome::CaughtUp => return Ok(n),
                _ => n += 1,
            }
        }
    }
}

/// How many times a merge is replanned against a concurrent writer before giving up.
///
/// Small on purpose. A couple of attempts absorbs a rebuild landing mid-merge; a pipeline
/// that cannot win in three is contending with something that needs looking at, and
/// spinning would only hide it.
const MERGE_ATTEMPTS: u32 = 3;

/// Is this a merge worth trying again against a freshly opened target?
///
/// Both members of this set are the same situation seen from two sides: another writer got
/// to the target first. Everything else — a cast failure, a missing column — would fail the
/// same way every time, and retrying would only make the log harder to read.
fn worth_replanning(e: &Error) -> bool {
    is_commit_conflict(e) || is_foreign_checkpoint(e)
}

/// Did this fail because somebody else committed first?
///
/// Only this class is worth replanning for. A cast failure or a missing column would fail
/// the same way every time.
fn is_commit_conflict(e: &Error) -> bool {
    use deltalake::kernel::transaction::TransactionError;
    use deltalake::DeltaTableError;

    matches!(
        e,
        Error::Delta(DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(_)
                | TransactionError::VersionAlreadyExists(_)
                | TransactionError::MaxCommitAttempts(_),
        })
    )
}

/// Did this fail on a checkpoint another engine wrote, met part-way through a merge?
///
/// The one that is easy to miss, because nothing about the merge is wrong. Losing a commit
/// race sends delta-rs into conflict resolution, which brings the snapshot up to the
/// winner's version — and *that* reads any checkpoint above the version this handle was
/// opened at. A compaction by another engine lands a commit and a checkpoint together, so
/// the two arrive as a pair, and if that engine writes `timestamp` at a precision the Delta
/// protocol does not have, the rebuild cannot parse it.
///
/// Nothing was committed when this happens, so it retries exactly like the conflict it
/// really is. What makes the retry work rather than repeat is the reopen above: the handle
/// it replans against has stepped over that checkpoint. The target's own data files have
/// nothing to do with it — this reproduces with every file at the declared precision.
fn is_foreign_checkpoint(e: &Error) -> bool {
    matches!(e, Error::Delta(d) if crate::storage::is_unreadable_checkpoint(d))
}

/// A Delta schema as Arrow sees it.
///
/// The source's own declaration of what its columns are, which is what a data file has to
/// be read as however it happens to be typed on disk.
fn arrow_schema_of(schema: &deltalake::kernel::StructType) -> Result<SchemaRef> {
    use deltalake::arrow::datatypes::Schema;
    use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;

    let s: Schema = schema.try_into_arrow().map_err(|e| {
        Error::Schema(format!(
            "the source's schema is not expressible in Arrow: {e}"
        ))
    })?;
    Ok(Arc::new(s))
}

/// A Delta table's own identity, which survives nothing but the table itself.
///
/// Read out of the serialized metadata because the kernel keeps the accessor private; the
/// field name is part of the Delta protocol, so this is stable.
fn table_id(t: &DeltaTable) -> Option<String> {
    let snapshot = t.snapshot().ok()?;
    serde_json::to_value(snapshot.metadata())
        .ok()?
        .get("id")?
        .as_str()
        .map(str::to_string)
}

/// Start over when the source is no longer the table we were reading.
///
/// Dropping and recreating a table keeps its path and its name but gives it a new
/// identity and a log that restarts at zero. An offset carried over from the old table
/// then means nothing, and there are two ways it goes wrong: if the new table has fewer
/// commits than we had consumed, we wait forever for commits that will never come; if it
/// already has more, we resume past its early commits and never read them at all. The
/// second is the dangerous one, because nothing looks wrong.
///
/// Starting over is the obvious answer, and it is safe exactly when `dedup_timestamp` is
/// set: the filter drops whatever the target already covers, so re-reading the table from
/// the beginning re-emits only what is genuinely missing. Without it, starting over would
/// append the whole table a second time, so it stops and says so instead.
async fn adjust_for_replaced_source(
    cfg: &ResolvedPipeline,
    source: &DeltaTable,
    target: &DeltaTable,
    cursor: StreamCursor,
) -> Result<StreamCursor> {
    let head = source
        .log_store()
        .get_latest_version(0)
        .await
        .map_err(Error::Delta)?;

    // Identity is the reliable signal. The log going backwards is the fallback for
    // targets written before we recorded it.
    let recorded = watermark::our_last_commit(target, &cfg.app_id, watermark::DEFAULT_MAX_SCAN)
        .await?
        .source_table_id;
    let current = table_id(source);
    let different_table = matches!((&recorded, &current), (Some(a), Some(b)) if a != b);
    let log_went_backwards = cursor.version > head.saturating_add(1);

    if !different_table && !log_went_backwards {
        return Ok(cursor);
    }

    let why = if different_table {
        "the source table has a different id than the one this pipeline last read from"
    } else {
        "the source's log has gone backwards"
    };

    let Some(ts) = cfg.dedup_timestamp.as_deref() else {
        return Err(Error::Config(format!(
            "pipeline {:?}: {why} — source {:?} appears to have been dropped and \
             recreated. Resuming would either wait for commits that never come or skip \
             the new table's early ones. Starting over is safe once dedup_timestamp is \
             set, because rows the target already holds are then filtered out; without it \
             it would append the whole table a second time. Set it (or `meta: \
             {{ddi_timestamp: ...}}` on the dbt model), or choose a new app_id.",
            cfg.name, cfg.source_uri
        )));
    };

    let from = StreamCursor::at_version(cfg.starting_version);
    warn!(
        pipeline = %cfg.name,
        reason = why,
        previous_offset = %cursor,
        restart_from = %from,
        dedup_timestamp = ts,
        "source was replaced; starting over and skipping rows the target already holds"
    );
    Ok(from)
}

/// Where to resume, accounting for a target that another writer may have rebuilt.
///
/// Normally the answer is our own `txn` offset. But when dbt shares the target, that
/// offset can describe rows that no longer exist: `txn` actions survive an overwrite, so
/// after a nightly rebuild we would resume past everything we streamed while dbt was
/// reading, and those rows would never come back. When the target has been rewritten
/// since our last append, dbt's watermark is the authority instead.
///
/// See [`crate::dbt::watermark`] for the full argument.
async fn resume_cursor(
    cfg: &ResolvedPipeline,
    offsets: &OffsetStore,
    source: &DeltaTable,
    target: &DeltaTable,
) -> Result<StreamCursor> {
    let own = offsets.resume_cursor(target).await?;

    if cfg.watermark_uri.is_none() && cfg.dedup_timestamp.is_none() {
        return Ok(own);
    }

    let state = watermark::target_state(target, &cfg.app_id, watermark::DEFAULT_MAX_SCAN).await?;
    let watermark::TargetState::OverwrittenAt(at) = state else {
        return Ok(own);
    };

    // dedup_key needs no cooperation from the rebuilding writer, so it is tried first: the
    // rows to emit are those beyond the highest key the rebuild left behind, and the scan
    // has to start early enough to reach them. Our own offset is not early enough --- it
    // may already be past what the rebuild covered, which is the whole hazard --- so the
    // scan restarts and the key filter suppresses everything already present.
    if let Some(ts) = cfg.dedup_timestamp.as_deref() {
        // How far back the rescan has to reach is a question the source's own file
        // statistics can answer, so ask rather than re-reading history every night.
        let dedup = Dedup::read(target, ts, cfg.dedup_key.as_deref()).await?;
        let from = match dedup.watermark() {
            Some(w) => StreamCursor::at_version(
                dedup::bounded_rescan_start(
                    source,
                    ts,
                    w,
                    cfg.starting_version,
                    watermark::DEFAULT_MAX_SCAN,
                )
                .await?,
            ),
            None => StreamCursor::at_version(cfg.starting_version),
        };
        warn!(
            pipeline = %cfg.name,
            overwritten_at_target_version = at,
            own_offset = %own,
            dedup_timestamp = ts,
            rescan_from = %from,
            bounded = (from.version > cfg.starting_version),
            "target was rebuilt by another writer; rescanning and skipping rows the target \
             already covers"
        );
        return Ok(from);
    }

    let uri = cfg
        .watermark_uri
        .as_deref()
        .expect("checked above: one of watermark_uri or dedup_timestamp is set");
    let store = watermark::WatermarkStore::new(uri).with_storage(cfg.storage.clone());
    let Some(w) = store.last(&cfg.app_id).await? else {
        // Refusing is the whole point. Continuing from our own offset would drop every
        // row we streamed after dbt started reading, silently and for good.
        return Err(Error::Config(format!(
            "pipeline {:?}: target {:?} was rewritten at version {at} by another writer \
             (a dbt rebuild), but the watermark table {uri:?} holds no source_version for \
             app_id {:?}. Resuming from this pipeline's own offset would silently drop \
             every row streamed while dbt was reading. Either have the rebuild record the \
             source version it consumed, or set dedup_timestamp to a column that increases \
             with arrival order so the overlap can be skipped without its cooperation.",
            cfg.name, cfg.target_uri, cfg.app_id
        )));
    };

    let reset = StreamCursor::at_version(w + 1);
    if reset != own {
        warn!(
            pipeline = %cfg.name,
            overwritten_at_target_version = at,
            own_offset = %own,
            watermark = w,
            resume_from = %reset,
            "target was rebuilt by dbt; resuming from dbt's watermark rather than our own \
             offset"
        );
    }
    Ok(reset)
}

/// Re-attach Delta partition values, which live in the log rather than in the parquet.
fn attach_partition_columns(
    batch: RecordBatch,
    partition_cols: &[String],
    values: &std::collections::HashMap<String, Option<String>>,
) -> Result<RecordBatch> {
    use deltalake::arrow::array::{ArrayRef, StringArray};
    use deltalake::arrow::datatypes::{Field, Schema};

    let rows = batch.num_rows();
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    for name in partition_cols {
        if batch.schema().index_of(name).is_ok() {
            continue; // already materialised in the file
        }
        let v = values.get(name).and_then(|o| o.clone());
        let arr: ArrayRef = Arc::new(StringArray::from(vec![v; rows]));
        fields.push(Arc::new(Field::new(name, arr.data_type().clone(), true)));
        columns.push(arr);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| Error::Schema(format!("could not attach partition columns: {e}")))
}
