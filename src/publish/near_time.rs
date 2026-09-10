//! Publishing from the source directly, decoupled from the target commit.
//!
//! [`crate::publish`] (the ordinary path) is deliberately downstream of a Delta commit that
//! has already succeeded — that is what makes it at-most-once and lets every envelope carry
//! a confirmed `target_version`. [`NearTimeReader`] is the opposite trade: it reads the
//! *source* table on its own cursor and cadence, entirely independent of whatever the
//! target commit is doing, so a message goes out as soon as this reader gets to the rows
//! rather than after however long the next commit-sized batch takes to write. In exchange:
//!
//! * No at-most-once guarantee. A transient failure that is retried can resend a batch, and
//!   every (re)open re-derives its starting cursor from the target's *durable* offset (see
//!   [`NearTimeReader::open`]), which can be behind where this reader had already got to —
//!   so a restart can replay a bounded window already sent. A restarted reader's first
//!   message carries `prev_source_version: None`, which will not match a client's `last`,
//!   so a compliant client reloads and resets rather than silently double-applying the
//!   resend — the same recovery path any other gap already uses.
//! * `target_version` is always `None` in the envelope: there is no commit to attach one to,
//!   and this reader's batches do not correspond to any single target commit. That also
//!   means a client's `target_version <= B` "already in the baseline I just reloaded" check
//!   never fires for these messages, so a reload (from the restart case above, or an
//!   ordinary gap) can re-apply rows the reload's own baseline already contains — a real,
//!   if usually brief, way to overcount until the *next* reload. See USING_DDI.md §8.
//!
//! A pipeline runs one publisher or the other, never both: `near_time` on
//! [`crate::config::PublishModel`] replaces the post-commit publisher rather than adding to
//! it, so a group only ever carries one gap-detection chain. See `Pipeline::open`, which
//! builds no [`crate::publish::Publisher`] of its own for a `near_time` pipeline.
//!
//! What this deliberately does not do, because doing it right needs a committed batch to
//! anchor to: no [`crate::dedup::Dedup`] (a dbt rebuild can make this transiently republish
//! rows the target ends up suppressing), no data-quality quarantine (a row that fails to
//! coerce is dropped from that message and logged, not routed to a table), and no lookups
//! (refused at config resolve — see `resolve_publish` in [`crate::config`]).

use std::collections::BTreeSet;

use deltalake::arrow::array::RecordBatch;
use deltalake::delta_datafusion::DataFusionMixins;
use deltalake::DeltaTable;
use tracing::{info, warn};

use crate::config::ResolvedPipeline;
use crate::error::{Error, Result};
use crate::offset::OffsetStore;
use crate::pipeline::scan_source;
use crate::publish::{PublishStats, Publisher};
use crate::schema::SchemaCoercer;
use crate::source::{LogStreamBuilder, Version};
use crate::transform::{Identity, SqlTransform, Transform};

/// What one poll of the source did.
#[derive(Debug)]
pub enum NearTimeOutcome {
    /// Nothing new since the last poll.
    CaughtUp,
    /// A batch was read. `published` is `None` when nothing coerced or the breaker was
    /// open — see [`Publisher::send`] for why that is not an error.
    Polled { published: Option<PublishStats> },
}

/// Reads a source table directly and publishes from it, independent of any target commit.
pub struct NearTimeReader {
    cfg: ResolvedPipeline,
    source: DeltaTable,
    stream: LogStreamBuilder,
    transform: Box<dyn Transform>,
    coercer: SchemaCoercer,
    publisher: Publisher,
    /// The last batch's `through_version`, for the gap-detection chain. `None` until this
    /// reader's first publish since it was opened — a restart is a gap, the same reasoning
    /// as the post-commit publisher's `prev_published_through`.
    prev_published_through: Option<Version>,
}

impl NearTimeReader {
    /// Build a near-time reader for this pipeline, or `None` if it does not use one.
    ///
    /// Every way *building* this can go wrong for reasons this feature owns — no
    /// `near_time` model, no sink, a broken connection string, a write mode that merges —
    /// is logged and answered with `None`, mirroring [`Publisher::open`]: a broken
    /// near-time setup must not stop the pipeline it rides on. Reaching the source or
    /// target tables can still fail for reasons that are not this feature's fault (the
    /// storage account is unreachable, say), and those come back as `Err` so the caller's
    /// ordinary retry/backoff applies — exactly as `Pipeline::open` does for the same class
    /// of failure.
    pub async fn open(cfg: &ResolvedPipeline) -> Result<Option<Self>> {
        let Some(model) = &cfg.publish else {
            return Ok(None);
        };
        if !model.near_time {
            return Ok(None);
        }

        // Already warned with the actual reason if this declines.
        let Some(publisher) = Publisher::open(cfg) else {
            return Ok(None);
        };

        let source = cfg.storage.open(&cfg.source_uri).await.map_err(|e| {
            Error::Config(format!("pipeline {:?}: near-time source: {e}", cfg.name))
        })?;
        // Read-only, and only ever used below to derive a starting cursor from the target's
        // durable `txn` position. This reader never writes to the target and never advances
        // that offset — doing so would corrupt the main pipeline's own exactly-once
        // bookkeeping.
        let target = cfg.storage.open(&cfg.target_uri).await.map_err(|e| {
            Error::Config(format!("pipeline {:?}: near-time target: {e}", cfg.name))
        })?;

        let offsets = OffsetStore::new(&cfg.app_id, cfg.starting_version);
        let cursor = offsets.resume_cursor(&target).await?;

        let target_schema = target
            .snapshot()
            .map_err(Error::Delta)?
            .snapshot()
            .read_schema();

        let stream = LogStreamBuilder::new(&source)
            .with_starting_cursor(cursor)
            .with_source_uri(&cfg.source_uri)
            .with_change_policy(cfg.change_policy)
            .with_max_files_per_batch(cfg.max_files_per_batch)
            .with_max_bytes_per_batch(cfg.max_bytes_per_batch);

        // No lookups: `resolve_publish` and the dbt gate both refuse `near_time = true` for
        // a pipeline that configures any, so there are none to register here.
        let transform: Box<dyn Transform> = match &cfg.transform_sql {
            Some(sql) => Box::new(SqlTransform::new_with_lookups(
                sql.clone(),
                &BTreeSet::new(),
            )),
            None => Box::new(Identity),
        };

        info!(
            pipeline = %cfg.name,
            resume_from = %cursor,
            "near-time publisher ready"
        );

        Ok(Some(Self {
            cfg: cfg.clone(),
            source,
            stream,
            transform,
            coercer: SchemaCoercer::new(target_schema),
            publisher,
            prev_published_through: None,
        }))
    }

    /// One poll of the source: read whatever is newly available, transform, coerce, and
    /// publish. Does not sleep or retry — the caller owns cadence and backoff, exactly as
    /// `main.rs`'s `attempt` owns `Pipeline::step`'s.
    pub async fn poll(&mut self) -> Result<NearTimeOutcome> {
        let Some(batch) = self.stream.next_batch().await? else {
            return Ok(NearTimeOutcome::CaughtUp);
        };
        let through = batch.through_version;
        let from = batch.start.version;

        let input = scan_source(&self.source, &self.cfg.source_uri, &batch).await?;
        let output = self.transform.apply_with_lookups(input, &[]).await?;

        let mut coerced = Vec::with_capacity(output.len());
        for b in &output {
            if b.num_rows() == 0 {
                continue;
            }
            match self.coercer.coerce(b) {
                Ok(c) if c.num_rows() > 0 => coerced.push(c),
                Ok(_) => {}
                Err(e) => {
                    // No quarantine table on this path — see the module doc. A row that
                    // will not coerce costs only this one message; the source is
                    // unaffected and the next poll picks up right after it. The chain
                    // still advances, for the same reason `Pipeline::publish` advances it
                    // unconditionally: leaving it behind would make the *next* message
                    // claim to follow one the client never saw.
                    warn!(
                        pipeline = %self.cfg.name,
                        "near-time: a row in this batch did not coerce to the target \
                         schema, so this message is skipped: {e}"
                    );
                    self.prev_published_through = Some(through);
                    return Ok(NearTimeOutcome::Polled { published: None });
                }
            }
        }

        // Mirrors `Pipeline::step`: a paused breaker means the sink has been failing, and
        // running the aggregation for a message that will not be sent is pure waste.
        let pending = if self.publisher.is_paused() {
            None
        } else {
            let batches = if coerced.is_empty() {
                vec![RecordBatch::new_empty(self.coercer.target())]
            } else {
                coerced
            };
            self.publisher.render(self.coercer.target(), batches).await
        };

        let prev = self.prev_published_through;
        // `target_version` is always `None`: there is no commit backing these rows yet, and
        // may never be one that groups them the same way — see the module doc.
        let stats = self
            .publisher
            .send(pending, prev, from, through, None)
            .await;
        self.prev_published_through = Some(through);
        Ok(NearTimeOutcome::Polled {
            published: Some(stats),
        })
    }

    /// Run until caught up, then return how many batches were polled. For tests, mirroring
    /// `Pipeline::run_until_caught_up`.
    pub async fn run_until_caught_up(&mut self) -> Result<usize> {
        let mut n = 0;
        loop {
            match self.poll().await? {
                NearTimeOutcome::CaughtUp => return Ok(n),
                NearTimeOutcome::Polled { .. } => n += 1,
            }
        }
    }
}
