//! The core loop.

use std::sync::Arc;
use std::time::Instant;

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::delta_datafusion::DataFusionMixins;
use deltalake::{ensure_table_uri, open_table, DeltaTable};
use futures::TryStreamExt;
use tracing::{info, warn};

use crate::config::ResolvedPipeline;
use crate::dbt::watermark;
use crate::dedup::Dedup;
use crate::error::{Error, Result};
use crate::offset::OffsetStore;
use crate::schema::SchemaCoercer;
use crate::sink::Sink;
use crate::source::{LogBatch, LogStreamBuilder, StreamCursor, Version};
use crate::transform::{Identity, SqlTransform, Transform};

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
    },
    /// Source commits existed but produced no rows (all filtered out, or all skipped).
    /// The offset still advances so the same commits are not re-read forever.
    Skipped { through_version: Version },
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
}

impl Pipeline {
    /// Open both tables, resolve the resume point, and prepare the loop.
    pub async fn open(cfg: ResolvedPipeline) -> Result<Self> {
        let source_url = ensure_table_uri(&cfg.source_uri).map_err(Error::Delta)?;
        let target_url = ensure_table_uri(&cfg.target_uri).map_err(Error::Delta)?;
        let source = open_table(source_url).await.map_err(|e| {
            Error::Config(format!(
                "pipeline {:?}: cannot open source {:?}: {e}",
                cfg.name, cfg.source_uri
            ))
        })?;
        let target = open_table(target_url).await.map_err(|e| {
            Error::Config(format!(
                "pipeline {:?}: cannot open target {:?}: {e}. This tool never creates the \
                 target table — create it with external tooling first (plan §2.7).",
                cfg.name, cfg.target_uri
            ))
        })?;

        let offsets = OffsetStore::new(&cfg.app_id, cfg.starting_version);
        let cursor = resume_cursor(&cfg, &offsets, &target).await?;

        // A source whose head is behind where we already read is not the table we were
        // reading. Dropping and recreating it restarts its log at zero, and the cursor
        // would then sit forever past a head that never catches up — a pipeline that
        // looks healthy and silently streams nothing. Say so instead.
        let head = source
            .log_store()
            .get_latest_version(0)
            .await
            .map_err(Error::Delta)?;
        // `head + 1` is the ordinary caught-up position: the cursor names the next
        // commit to read, which does not exist yet. Beyond that, commits we have already
        // consumed are missing from the log.
        if cursor.version > head.saturating_add(1) && cursor.version > cfg.starting_version {
            return Err(Error::Config(format!(
                "pipeline {:?}: source {:?} is at version {head}, but this pipeline has \
                 already consumed through version {}. The source's log has gone backwards, \
                 which happens when a table is dropped and recreated. Streaming on would \
                 wait forever for commits that will never come. Choose a new app_id to \
                 start over against the new table, or restore the old one.",
                cfg.name,
                cfg.source_uri,
                cursor.version.saturating_sub(1)
            )));
        }

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

        let transform: Box<dyn Transform> = match &cfg.transform_sql {
            Some(sql) => Box::new(SqlTransform::new(sql.clone())),
            None => Box::new(Identity),
        };

        let sink = Sink::new(&cfg.app_id, cfg.target_file_size);

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

        // 3. Transform. Stateless, row-local, validated at config load.
        let output = self.transform.apply(input).await?;

        // 4. Cast to the target schema. Hard-fail on mismatch — no silent nulls.
        let mut coerced = Vec::with_capacity(output.len());
        for b in &output {
            if b.num_rows() == 0 {
                continue;
            }
            let c = self.coercer.coerce(b)?;
            let c = self.dedup.apply(c)?;
            if c.num_rows() > 0 {
                coerced.push(c);
            }
        }
        let out_rows: usize = coerced.iter().map(|b| b.num_rows()).sum();

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

        if coerced.is_empty() {
            // Nothing to write, but the source version was still consumed, so the offset
            // must advance or these commits would be re-read forever. A zero-row batch in
            // the target schema (rather than an empty batch *list*, which delta-rs
            // rejects) commits the txn action with no data attached.
            let empty = RecordBatch::new_empty(self.coercer.target());
            self.commit(vec![empty], txn_version).await?;
            return Ok(StepOutcome::Skipped {
                through_version: through,
            });
        }

        // 5+6. Write parquet and commit the Add actions AND the txn action atomically.
        self.commit(coerced, txn_version).await?;

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
        })
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
                let url = ensure_table_uri(&self.cfg.target_uri).map_err(Error::Delta)?;
                self.target = open_table(url).await.map_err(Error::Delta)?;
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
    async fn scan(&self, batch: &LogBatch) -> Result<Vec<RecordBatch>> {
        use deltalake::parquet::arrow::async_reader::{
            ParquetObjectReader, ParquetRecordBatchStreamBuilder,
        };
        use deltalake::Path as StorePath;

        let store = self.source.log_store().object_store(None);
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
            let reader = ParquetObjectReader::new(store.clone(), path);
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
        let from = StreamCursor::at_version(cfg.starting_version);
        warn!(
            pipeline = %cfg.name,
            overwritten_at_target_version = at,
            own_offset = %own,
            dedup_timestamp = ts,
            rescan_from = %from,
            "target was rebuilt by another writer; rescanning and skipping rows the target \
             already covers"
        );
        return Ok(from);
    }

    let uri = cfg
        .watermark_uri
        .as_deref()
        .expect("checked above: one of watermark_uri or dedup_timestamp is set");
    let store = watermark::WatermarkStore::new(uri);
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
