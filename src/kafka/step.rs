//! A Kafka pipeline's step: poll, close a batch, commit it (docs/kafka-design.md §3.3).
//!
//! `impl Pipeline`, because committing a batch is [`Pipeline::process`], which every source
//! shares, and because a revoke (§3.4) commits the buffer in the middle of polling.
//!
//! # A step
//!
//! [`Pipeline::step_kafka`] polls for up to a second ([`super::TICK`]). It returns `Waiting`
//! when nothing closed, and commits a batch when one did: on latency (from the first message
//! after the last commit), message count, raw bytes or measured decoded bytes. A step that
//! returns between messages loses nothing: the buffer, a held batch and a message waiting for
//! the registry all live in the [`KafkaSource`].
//!
//! # When a commit fails
//!
//! What follows is the error's class (§3.6, [`Error::kafka_class`]), never a reopen:
//! - **Replan**: another writer got there first. The target is refreshed and every offset
//!   re-read in place ([`Pipeline::reset_in_place`]); the batch is tried again at once when
//!   each of its partitions still has the offset the batch was read from, and dropped (its
//!   partitions read again from their offsets) when one does not.
//! - **Transient** and **Capacity**: the batch is held, every partition paused, and the batch
//!   tried again after a backoff. The member stays in its group.
//! - **Blocking**: held as well, re-checked every `blocked_recheck_secs`, with the gauge set.
//! - **Consumer-fatal**: returned, and the supervisor drops the consumer and opens a new one.
//!
//! # Handle discipline
//!
//! `self.target` moves only through our own commits and through deliberate refreshes, each
//! followed by a `Fence::reset` (§4.3). A partition whose offset a refresh finds changed is
//! read again from there, and a batch or buffer holding rows of it is dropped.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::delta_datafusion::DataFusionMixins;
use deltalake::DeltaTable;
use rdkafka::consumer::{CommitMode, Consumer as _};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers as _, Message as _};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{Offset, TopicPartitionList};
use tracing::{debug, error, info, warn};

use crate::config::kafka::ValueFormat;
use crate::config::ResolvedPipeline;
use crate::dq::DataQuality;
use crate::error::{Error, KafkaClass, Result};
use crate::fence::{self, Fence};
use crate::kafka::client;
use crate::kafka::conform::{check_partition_columns, Conformer, KdiText, PartitionCheck};
use crate::kafka::context::{DdiContext, Event};
use crate::kafka::decode::{self, KafkaHeader, KafkaMessage, KafkaTimestamp, NotFed};
use crate::kafka::partitions::{Admit, Block, Buffer, Dropped, Limits, Partitions, State};
use crate::kafka::registry::{SchemaRegistry, SubjectSchemas};
use crate::kafka::start::{self, OpenFacts};
use crate::kafka::{
    fault, Decoder, Held, KafkaBatch, KafkaProgress, KafkaSource, RawDecoder, SqlUnion, Stalled,
    Work, BACKOFF_CAPACITY, BACKOFF_MAX, BACKOFF_MIN, ONCE_IDLE, TICK,
};
use crate::metrics::{KafkaMetrics, PartitionGauges};
use crate::pipeline::{Batch, LookupBatch, Pipeline, Position, Source, StepOutcome};
use crate::schema::{Rejected, SchemaCoercer};
use crate::sink::{KafkaOffsets, MessageMark, Sink};
use crate::transform::{Identity, SqlTransform, Transform};

/// How many times an open tries its baseline again after another member's landed first.
const BASELINE_ATTEMPTS: u32 = 5;

/// What a held batch waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeldFor {
    /// Tried again at once, after a reset kept it.
    Replan,
    Transient,
    Capacity,
    /// An operator: re-checked every `blocked_recheck_secs`.
    Operator,
}

impl Pipeline {
    // ------------------------------------------------------------------------- access

    pub(crate) fn kafka(&self) -> &KafkaSource {
        match &self.source {
            Source::Kafka(k) => k,
            Source::Delta(_) => unreachable!("only a Kafka pipeline has a Kafka source"),
        }
    }

    pub(crate) fn kafka_mut(&mut self) -> &mut KafkaSource {
        match &mut self.source {
            Source::Kafka(k) => k,
            Source::Delta(_) => unreachable!("only a Kafka pipeline has a Kafka source"),
        }
    }

    /// The source and the target handle at once.
    pub(crate) fn kafka_and_target(&mut self) -> (&mut KafkaSource, &mut DeltaTable) {
        match &mut self.source {
            Source::Kafka(k) => (k, &mut self.target),
            Source::Delta(_) => unreachable!("only a Kafka pipeline has a Kafka source"),
        }
    }

    /// What a Kafka pipeline tells the supervisor about its progress (§3.9); `None` for any
    /// other source.
    pub fn kafka_progress(&self) -> Option<KafkaProgress> {
        match &self.source {
            Source::Kafka(k) => Some(k.progress()),
            Source::Delta(_) => None,
        }
    }

    // --------------------------------------------------------------------------- open

    /// Open a Kafka pipeline (§4.4): the target, the client, the topic's partitions, the
    /// registry and the union `U`, start resolution with its refusals and baseline; then
    /// subscribe, or in once-mode assign every partition (§3.8). Nothing joins the group
    /// before every refusal has had its say.
    pub(crate) async fn open_kafka(
        cfg: ResolvedPipeline,
        metrics: Arc<KafkaMetrics>,
    ) -> Result<Self> {
        let k = cfg
            .kafka
            .clone()
            .ok_or_else(|| Error::Other("internal: a Kafka open without a Kafka config".into()))?;
        let prefix = k.metadata_prefix.clone();
        let mut target = cfg.storage.open(&cfg.target_uri).await.map_err(|e| {
            Error::Config(format!(
                "pipeline {:?}: target: {e}. This tool never creates the target table — create \
                 it with external tooling first.",
                cfg.name
            ))
        })?;
        fence::refuse_txn_retention(&target)?;
        fence::refuse_unwritable_protocol(&target)?;
        let table_id = crate::lookup::table_id(&target);
        let (target_schema, partition_columns) = schema_of(&target)?;

        // The client, and what the topic is.
        let settings = client::settings(&cfg.name, &k)?;
        let stats_interval = settings
            .property("statistics.interval.ms")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis);
        let context = DdiContext::new(&cfg.name, &k.topic, metrics.clone());
        let consumer = client::consumer(&cfg.name, &settings, context)?;
        let topic_partitions = super::rebalance::topic_partitions(&consumer, &k.topic).await?;

        // How messages decode, and the SQL's union of writer schemas (§5.5).
        let registry = match k.value_format {
            ValueFormat::AvroConfluent => {
                let sr = k.schema_registry.as_ref().ok_or_else(|| {
                    Error::Config(
                        "value_format = \"avro_confluent\" needs schema_registry.url".into(),
                    )
                })?;
                let counted = metrics.clone();
                let registry = SchemaRegistry::new(sr, &client::env)?.with_request_counter(
                    Arc::new(move |outcome| {
                        KafkaMetrics::count(&counted.schema_registry_requests, outcome)
                    }),
                );
                Some(Arc::new(registry))
            }
            ValueFormat::Raw => None,
        };
        let decoder = match &registry {
            Some(r) => Decoder::Avro(decode::KafkaDecoder::new(&k.topic, &prefix, r.clone())),
            None => Decoder::Raw(RawDecoder::new(&k.topic, &prefix)),
        };
        let transform: Box<dyn Transform> = match &cfg.transform_sql {
            Some(sql) => Box::new(SqlTransform::new(sql.clone())),
            None => Box::new(Identity),
        };
        let mut members = Vec::new();
        let mut copies = BTreeMap::new();
        let mut union = None;
        let mut partition_check_deferred = false;
        match (&registry, &cfg.transform_sql) {
            (Some(registry), sql) => {
                let subject = SubjectSchemas::open(registry.clone(), &k.subject).await?;
                members = subject.members();
                let u = crate::kafka::conform::UnionSchema::build(&members, &prefix);
                let sql = sql.as_ref().map(|s| SqlTransform::new(s.clone()));
                if let Some(sql) = &sql {
                    if !u.members().is_empty() {
                        u.plan(sql).await?;
                        copies = sql.column_sources(u.schema()).await?;
                    }
                }
                partition_check_deferred =
                    check_partition_columns(&cfg.target_uri, &partition_columns, sql.as_ref(), &u)
                        .await?
                        == PartitionCheck::Deferred;
                if let Some(sql) = sql {
                    union = Some(SqlUnion {
                        subject,
                        sql,
                        union: u,
                    });
                }
            }
            (None, sql) => {
                // Raw values: the columns are known without a registry.
                let raw: SchemaRef = Arc::new(deltalake::arrow::datatypes::Schema::new(
                    decode::raw_fields(&prefix),
                ));
                let produced = match sql {
                    Some(sql) => SqlTransform::new(sql.clone()).output_schema(raw).await?,
                    None => raw,
                };
                let missing: Vec<String> = partition_columns
                    .iter()
                    .filter(|c| produced.field_with_name(c).is_err())
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    return Err(Error::KafkaPartitionColumnUnmapped {
                        target_uri: cfg.target_uri.clone(),
                        columns: missing,
                        produced: produced.fields().iter().map(|f| f.name().clone()).collect(),
                        sql: sql.is_some(),
                    });
                }
            }
        }

        // Where the topic starts (§4.4), and the txn-only commit that records it. Tried again
        // from the top when another member's baseline landed first: that one usually ends the
        // era this open read.
        let sink = Sink::new(&cfg.app_id, cfg.target_file_size);
        let mut attempt = 0;
        let kdi_compat = loop {
            attempt += 1;
            let mut ids: Vec<String> = topic_partitions
                .iter()
                .map(|p| fence::txn_app_id(&cfg.app_id, *p))
                .collect();
            ids.push(fence::takeover_marker_app_id(&cfg.app_id));
            let txns = fence::read_txns(&target, &ids, &metrics.lookups).await?;
            let era = fence::era(&cfg.app_id, &txns);
            let stored: BTreeMap<i32, Option<i64>> = topic_partitions
                .iter()
                .map(|p| {
                    let id = fence::txn_app_id(&cfg.app_id, *p);
                    (*p, txns.get(&id).and_then(|t| t.version))
                })
                .collect();
            let watermarks =
                super::rebalance::watermarks(&consumer, &k.topic, &topic_partitions).await?;
            let (data_files, holds_rows) = start::target_rows(&target, &prefix, &k.topic)?;
            let app_ids =
                if era == fence::Era::Fresh && data_files > 0 && !k.starting_offsets_explicit {
                    fence::recent_txn_app_ids(&target, start::TYPO_GUARD_COMMITS).await?
                } else {
                    BTreeSet::new()
                };
            let plan = start::plan_open(&OpenFacts {
                app_id: &cfg.app_id,
                topic: &k.topic,
                target_uri: &cfg.target_uri,
                stored: &stored,
                watermarks: &watermarks,
                era,
                data_files,
                holds_rows,
                policy: &k.starting_offsets,
                explicit: k.starting_offsets_explicit,
                kdi_compat: k.kdi_compat,
                neighbours: &k.neighbours,
                app_ids: &app_ids,
            })?;
            info!(
                pipeline = %cfg.name,
                era = ?era,
                partitions = topic_partitions.len(),
                baseline = plan.baseline.is_some(),
                "offsets read from the target"
            );
            let Some(assert) = plan.baseline else {
                break plan.kdi_compat;
            };
            let offsets = KafkaOffsets {
                topic: k.topic.clone(),
                group_id: k.group_id.clone(),
                ranges: BTreeMap::new(),
                assert: assert.clone(),
                marks: BTreeMap::new(),
                cleanup_logs: false,
                baseline: true,
                override_reason: None,
                takeover_marker: true,
            };
            let empty = RecordBatch::new_empty(target_schema.clone());
            match sink
                .commit_kafka(target.clone(), vec![empty], &offsets)
                .await
            {
                Ok(t) => {
                    target = t;
                    info!(
                        pipeline = %cfg.name,
                        era = ?era,
                        offsets = ?assert,
                        target_version = ?target.version(),
                        "committed the baseline: where each partition starts, and the takeover \
                         marker"
                    );
                    break plan.kdi_compat;
                }
                Err(e) if e.kafka_class() == KafkaClass::Replan && attempt < BASELINE_ATTEMPTS => {
                    info!(
                        pipeline = %cfg.name,
                        "another writer committed these offsets first; reading them again: {e}"
                    );
                    target = cfg.storage.open(&cfg.target_uri).await?;
                }
                Err(e) => return Err(e),
            }
        };

        let text = KdiText {
            compat: kdi_compat,
            null_text: k.kdi_null_text.unwrap_or(kdi_compat),
        };
        let mut conformer = Conformer::new(target_schema.clone(), &prefix, text)?;
        conformer.observe(&members);
        conformer.observe_copies(copies.clone());

        let dq_uri = cfg.dq_uri();
        let mut dq = DataQuality::open(&cfg.storage, &dq_uri, &cfg.app_id, &cfg.name).await?;
        if let Some(dq) = dq.as_mut() {
            dq.set_kafka_metadata_prefix(&prefix);
            dq.set_kafka_counters(metrics.dq.clone());
        }
        match &dq {
            Some(_) => {
                info!(pipeline = %cfg.name, dq_uri = %dq_uri, "rows the target will not take, and messages that cannot be decoded, go to the data-quality table")
            }
            None => {
                info!(pipeline = %cfg.name, dq_uri = %dq_uri, "no data-quality table: a message that cannot be decoded, or a row the target will not take, holds this pipeline until the table exists")
            }
        }

        let budget = crate::budget::current();
        let limits = Limits {
            latency: Duration::from_secs(cfg.allowed_latency_secs.max(1)),
            max_messages: k.max_messages_per_batch.map(|m| m as u64),
            max_bytes: cfg.max_bytes_per_batch,
            max_decoded: budget.batch_bytes(),
        };
        for gauge in [
            &metrics.offset_missing,
            &metrics.assigned_partitions,
            &metrics.blocked_partitions,
            &metrics.retrying,
            &metrics.schema_conflict,
            &metrics.undecodable_blocking,
        ] {
            gauge.store(0, Ordering::Relaxed);
        }

        let once = k.once;
        let source = KafkaSource {
            conformer,
            kafka: k.clone(),
            consumer: Some(consumer.clone()),
            fence: Fence::with_lookups(&cfg.app_id, metrics.lookups.clone()),
            parts: Partitions::default(),
            buf: Buffer::default(),
            decoder: std::sync::Mutex::new(decoder),
            union,
            partition_check_deferred,
            partition_columns,
            held: None,
            stalled: None,
            blocked: None,
            next_recheck: None,
            paused: BTreeSet::new(),
            unbased_start: BTreeMap::new(),
            metrics,
            limits,
            table_id,
            refreshed_at: Instant::now(),
            last_commit: None,
            reset_this_step: false,
            once_end: None,
            stats_interval,
            stats_seen: 0,
            caught_up: false,
            copies,
            kdi_text: text,
            held_for: None,
            last_backoff: None,
            pending_outcome: None,
        };
        info!(
            pipeline = %cfg.name,
            app_id = %cfg.app_id,
            topic = %k.topic,
            group = %k.group_id,
            transform = %transform.describe(),
            once,
            "pipeline ready"
        );
        let mut pipeline = Pipeline {
            coercer: SchemaCoercer::new(target_schema),
            cfg,
            source: Source::Kafka(Box::new(source)),
            target,
            transform,
            sink,
            cutoff: None,
            dq,
            grain_check_passes: 0,
            lookup_table_ids: BTreeMap::new(),
            lookup_current_on_open: BTreeSet::new(),
            warned_lookup_table_id_changes: BTreeSet::new(),
            publisher: None,
            prev_published_through: None,
        };
        if once {
            // Manual assignment: every partition, starts resolved as an assign does, and the
            // end of each read now. The group is never joined (§3.8).
            pipeline.assign_partitions(topic_partitions, true).await?;
        } else {
            let topic = pipeline.kafka().kafka.topic.clone();
            pipeline
                .kafka()
                .consumer()
                .subscribe(&[topic.as_str()])
                .map_err(|e| client::classify(&e, "subscribing"))?;
        }
        Ok(pipeline)
    }

    // --------------------------------------------------------------------------- step

    /// One step of a Kafka pipeline (§3.3): poll until a batch closes or a tick passes, and
    /// commit what closed.
    pub(crate) async fn step_kafka(&mut self) -> Result<StepOutcome> {
        self.kafka_mut().reset_this_step = false;
        let work = self.next_work().await;
        let outcome = match work {
            Err(e) => Err(e),
            Ok(Work::Waiting) => Ok(StepOutcome::Waiting),
            Ok(Work::Done) => Ok(StepOutcome::CaughtUp),
            Ok(Work::Committed(o)) => Ok(o),
            Ok(Work::Batch(b) | Work::Retry(b)) => self.commit_batch(b).await,
        };
        self.update_gauges();
        outcome
    }

    async fn next_work(&mut self) -> Result<Work> {
        let tick_end = Instant::now() + TICK;
        loop {
            let now = Instant::now();
            if let Some(work) = self.due_work(now).await? {
                return Ok(work);
            }
            if self.kafka().held.is_none()
                && self.kafka().stalled.is_none()
                && self.kafka().blocked.is_none()
            {
                self.release().await?;
            }
            if let Some(done) = self.once_done(now)? {
                return Ok(done);
            }

            // Wait for a message, but no longer than the next thing due.
            let src = self.kafka();
            let wake = [
                Some(tick_end),
                src.buf.deadline(src.limits.latency),
                src.held.as_ref().map(|h| h.due),
                src.stalled.as_ref().map(|s| s.due),
                src.next_recheck,
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(tick_end);
            let wait = wake.saturating_duration_since(Instant::now());
            let consumer = src.consumer().clone();
            // recv() is cancellation-safe (stream_consumer.rs:290-320); the borrowed message is
            // copied out at once and dies here (EO-m3).
            let polled = match tokio::time::timeout(wait, consumer.recv()).await {
                Ok(Ok(m)) => Some(Ok(copy(&m))),
                Ok(Err(e)) => Some(Err(e)),
                Err(_) => None,
            };
            self.handle_events().await?;
            self.observe_stats();
            match polled {
                Some(Ok(m)) => self.take(m).await?,
                Some(Err(e)) => self.consumption_error(e).await?,
                None => {}
            }
            if let Some(o) = self.kafka_mut().pending_outcome.take() {
                return Ok(Work::Committed(o));
            }
            let src = self.kafka();
            if src.stalled.is_none()
                && src.held.is_none()
                && src
                    .buf
                    .closes(&src.limits, src.decoded_bytes(), Instant::now())
            {
                return Ok(Work::Batch(self.close_batch()?));
            }
            if Instant::now() >= tick_end {
                return Ok(Work::Waiting);
            }
        }
    }

    /// A held batch, a stalled message or a recheck whose time has come.
    async fn due_work(&mut self, now: Instant) -> Result<Option<Work>> {
        if self.kafka().held.as_ref().is_some_and(|h| h.due <= now) {
            let held = self.kafka_mut().held.take().expect("checked above");
            self.kafka_mut().last_backoff = Some(held.backoff);
            if !held.retrying && self.kafka().held_for == Some(HeldFor::Operator) {
                // Whatever an operator was to change may have changed the target too.
                match self.recheck_for_batch(held.batch).await? {
                    Some(batch) => return Ok(Some(Work::Retry(batch))),
                    None => return Ok(None),
                }
            }
            return Ok(Some(Work::Retry(held.batch)));
        }
        if self.kafka().stalled.as_ref().is_some_and(|s| s.due <= now) {
            self.feed_stalled().await?;
        }
        if self.kafka().next_recheck.is_some_and(|t| t <= now) && self.kafka().held.is_none() {
            self.recheck_blocked().await?;
        }
        Ok(None)
    }

    /// Once-mode's end (§3.8): every partition done and nothing left to commit. A partition
    /// still blocked then fails the run, so that its exit code says so.
    fn once_done(&self, now: Instant) -> Result<Option<Work>> {
        let src = self.kafka();
        let Some(ends) = &src.once_end else {
            return Ok(None);
        };
        if !src.buf.is_empty() || src.held.is_some() || src.stalled.is_some() {
            return Ok(None);
        }
        if !ends
            .iter()
            .all(|(p, end)| src.parts.done(*p, *end, ONCE_IDLE, now))
        {
            return Ok(None);
        }
        if let Some((p, block)) = src.parts.blocked().next() {
            return Err(block.error(&src.kafka.topic, p));
        }
        Ok(Some(Work::Done))
    }

    /// Take one message: admit it (§3.3), and feed the decoder what was taken.
    async fn take(&mut self, message: KafkaMessage) -> Result<()> {
        let now = Instant::now();
        let src = self.kafka_mut();
        src.metrics.brokers_up.store(1, Ordering::Relaxed);
        let p = message.partition;
        let mark = mark_of(&message);
        if src.paused.contains(&p) {
            src.metrics.dropped_blocked.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        match src.parts.admit(p, mark, now) {
            Admit::Take { gap } => {
                src.metrics.offset_gaps.fetch_add(gap, Ordering::Relaxed);
                src.buf.take(p, mark, decode::raw_len(&message), now);
                // Awaited to the end: `feed` owns the message while it waits on the registry,
                // and a dropped future would drop it (§5.4).
                if let Err(NotFed { error, message }) = src.decoder_mut().feed(message).await {
                    self.stall(message, error, None)?;
                }
            }
            Admit::Drop(why) => {
                let counter = match why {
                    Dropped::NotOwned => &src.metrics.dropped_not_owned,
                    Dropped::Blocked => &src.metrics.dropped_blocked,
                    Dropped::Duplicate => &src.metrics.dropped_duplicate,
                    Dropped::Reread => &src.metrics.dropped_reread,
                };
                counter.fetch_add(1, Ordering::Relaxed);
            }
            Admit::Block(block) => {
                let e = block.error(&src.kafka.topic, p);
                src.metrics.observe_error(&e);
                error!(pipeline = %self.cfg.name, partition = p, "{e}");
                self.block_partition(p, block);
            }
        }
        Ok(())
    }

    /// A message the decoder handed back, because its writer schema could not be had (§5.2):
    /// every partition is paused while it waits, and it is fed again after a backoff. Nothing
    /// closes meanwhile: the buffer accounts for its offset.
    fn stall(&mut self, message: KafkaMessage, error: Error, last: Option<Duration>) -> Result<()> {
        let class = error.kafka_class();
        let recheck = self.blocked_recheck();
        let src = self.kafka_mut();
        let backoff = match class {
            KafkaClass::ConsumerFatal => return Err(error),
            KafkaClass::Transient => last.map_or(BACKOFF_MIN, |b| (b * 2).min(BACKOFF_MAX)),
            KafkaClass::Capacity => BACKOFF_CAPACITY,
            _ => recheck,
        };
        warn!(
            partition = message.partition,
            offset = message.offset,
            retry_in_ms = backoff.as_millis() as u64,
            "a message waits for its writer schema: {error}"
        );
        src.metrics.observe_error(&error);
        src.stalled = Some(Stalled {
            message,
            due: Instant::now() + backoff,
            backoff,
        });
        self.pause_all();
        Ok(())
    }

    async fn feed_stalled(&mut self) -> Result<()> {
        let Some(stalled) = self.kafka_mut().stalled.take() else {
            return Ok(());
        };
        match self.kafka_mut().decoder_mut().feed(stalled.message).await {
            Ok(()) => {
                info!(pipeline = %self.cfg.name, "the writer schema came; reading goes on");
                let m = &self.kafka().metrics;
                m.schema_conflict.store(0, Ordering::Relaxed);
                Ok(())
            }
            Err(NotFed { error, message }) => self.stall(message, error, Some(stalled.backoff)),
        }
    }

    /// An error librdkafka returned in place of a message (§3.6).
    async fn consumption_error(&mut self, e: KafkaError) -> Result<()> {
        if e.rdkafka_error_code() == Some(RDKafkaErrorCode::AutoOffsetReset) {
            return self.out_of_range().await;
        }
        if client::ends_the_consumer(&e) {
            return Err(client::classify(&e, "consuming"));
        }
        if e.rdkafka_error_code() == Some(RDKafkaErrorCode::AllBrokersDown) {
            self.kafka().metrics.brokers_up.store(0, Ordering::Relaxed);
        }
        debug!(pipeline = %self.cfg.name, "librdkafka retries by itself: {e}");
        Ok(())
    }

    /// `auto.offset.reset = error` (-140): some partition's next offset is not in the topic,
    /// librdkafka has stopped fetching it, and the error does not say which (§0.3.20, EO-m4).
    /// The watermarks do: an offender expects an offset below its topic's low watermark
    /// (retention), or past its high one (the topic recreated smaller). Each offender is
    /// blocked; what it buffered is contiguous and valid, and is committed with the batch. With
    /// no offender (the watermarks moved meanwhile), every Ready partition is sought again at
    /// the offset it expects, which has librdkafka fetch it again (`release`).
    async fn out_of_range(&mut self) -> Result<()> {
        let ready: Vec<(i32, i64)> = self.kafka().parts.ready().collect();
        let partitions: Vec<i32> = ready.iter().map(|(p, _)| *p).collect();
        let topic = self.kafka().kafka.topic.clone();
        let consumer = self.kafka().consumer().clone();
        let watermarks = super::rebalance::watermarks(&consumer, &topic, &partitions).await?;
        let mut any = false;
        for (p, next) in ready {
            let Some((low, high)) = watermarks.get(&p).copied() else {
                continue;
            };
            let block = if next < low {
                Block::Unavailable { wanted: next, low }
            } else if next > high {
                Block::Regressed {
                    stored: next - 1,
                    high,
                }
            } else {
                continue;
            };
            any = true;
            let e = block.error(&topic, p);
            error!(pipeline = %self.cfg.name, partition = p, "{e}");
            self.kafka().metrics.observe_error(&e);
            self.block_partition(p, block);
        }
        if !any {
            warn!(
                pipeline = %self.cfg.name,
                "librdkafka reported an offset out of range, and every owned partition expects \
                 one inside its topic; seeking each to it again"
            );
            self.pause(&partitions);
        }
        Ok(())
    }

    /// Partition `p` waits for an operator: paused, re-asserted at its unchanged offset by
    /// every commit, and re-checked every `blocked_recheck_secs`.
    pub(crate) fn block_partition(&mut self, p: i32, block: Block) {
        let recheck = self.blocked_recheck();
        let src = self.kafka_mut();
        src.parts.block(p, block, Instant::now());
        src.next_recheck
            .get_or_insert_with(|| Instant::now() + recheck);
        self.pause(&[p]);
    }

    fn blocked_recheck(&self) -> Duration {
        Duration::from_secs(self.kafka().kafka.blocked_recheck_secs.max(1))
    }

    // ------------------------------------------------------------------- closing a batch

    /// Close the buffer into a batch: flush every decoder, and note each partition's offset
    /// in the target as the batch was read.
    pub(crate) fn close_batch(&mut self) -> Result<KafkaBatch> {
        let src = self.kafka_mut();
        let buf = std::mem::take(&mut src.buf);
        let decoded = src.decoder_mut().finish()?;
        let bases = buf
            .ranges
            .keys()
            .map(|p| (*p, src.fence.base(*p).and_then(|b| b.0)))
            .collect();
        Ok(KafkaBatch {
            decoded: decoded.batches,
            undecodable: decoded.undecodable.len(),
            first_undecodable: decoded
                .undecodable
                .first()
                .map(|u| (u.partition, u.offset, u.reason.clone())),
            rejects: decoded.rejects,
            tombstones: decoded.tombstones,
            ranges: buf.ranges,
            marks: buf.marks,
            bases,
            messages: buf.messages,
            raw_bytes: buf.raw_bytes,
            writer_schemas: src.writer_schemas(),
        })
    }

    // --------------------------------------------------------------- committing a batch

    /// Commit `batch`, and handle a failure by its class (§3.3, §3.6).
    pub(crate) async fn commit_batch(&mut self, batch: KafkaBatch) -> Result<StepOutcome> {
        match self.try_commit(&batch).await {
            Ok(outcome) => Ok(outcome),
            Err(e) => self.after_failure(batch, e).await,
        }
    }

    /// One attempt at committing `batch`: nothing is held or reset here.
    pub(crate) async fn try_commit(&mut self, batch: &KafkaBatch) -> Result<StepOutcome> {
        let started = Instant::now();
        // 1. A handle older than handle_max_age is refreshed before it plans a commit: the
        // conflict check would otherwise read every commit since, and fail on one log cleanup
        // removed (EO-m8). So is one whose own commit is gone from the log: the table was
        // dropped and recreated under it, and the refresh says so (I18).
        if self.kafka().refreshed_at.elapsed() >= fault::handle_max_age()
            || !fence::commit_exists(&self.target).await?
        {
            // Every partition another writer moved is read again from its new offset, the
            // ones this batch did not read too: left where they were, they would be read on
            // from the old offset and committed over the new one.
            let changed = self.refresh_and_reset().await?;
            self.reposition_changed(&changed, Some(batch));
        }
        // A batch is committed only over the offsets it was read from (I2). A refresh, this
        // one or an earlier one, may have put another writer's offset into the fence, where
        // `verify` takes it as carried: compared with the offset the batch was read from, it
        // drops the batch, and the reset that follows reads the partition again.
        if let Some(p) = self.moved_partition(batch) {
            return Err(self.moved(p, batch));
        }
        // 2. Messages that cannot be decoded and cannot be set aside.
        if batch.undecodable > 0 {
            let (partition, offset, reason) = batch.first_undecodable.clone().unwrap_or_default();
            if self.dq.is_none() {
                return Err(Error::KafkaUndecodable {
                    topic: self.kafka().kafka.topic.clone(),
                    partition,
                    offset,
                    reason,
                    dq_uri: self.cfg.dq_uri(),
                });
            }
            if batch.undecodable > self.cfg.max_evaluation_rejects_per_batch {
                return Err(Error::KafkaTooManyUndecodable {
                    count: batch.undecodable,
                    cap: self.cfg.max_evaluation_rejects_per_batch,
                    reason,
                });
            }
        }
        // 3. Into the union U (§5.5), and the writer schemas told to the conformer.
        let mut rejects: Vec<Rejected> = batch.rejects.iter().cloned().collect();
        let input = self.prepare(batch, &mut rejects).await?;
        // 4. The fence (§4.3): partitions not carried are looked up on this commit's snapshot.
        let (src, target) = self.kafka_and_target();
        src.fence.verify(target, &batch.ranges).await?;
        let (assert, marks) = src.fence.assert_map(&batch.ranges, &batch.marks);
        let offsets = KafkaOffsets {
            topic: src.kafka.topic.clone(),
            group_id: src.kafka.group_id.clone(),
            ranges: batch.ranges.clone(),
            assert,
            marks,
            cleanup_logs: cleans_up(target),
            baseline: false,
            override_reason: None,
            takeover_marker: src.fence.takeover_marker_due(),
        };
        // delta-rs rebases an append over another engine's writer-only protocol upgrade; a
        // protocol among the commits since the handle is a conflict, and the reset that follows
        // refuses a table this build cannot write (EO-m19).
        if fence::protocol_changed_since(&self.target).await? {
            return Err(Error::Delta(deltalake::DeltaTableError::Transaction {
                source: deltalake::kernel::transaction::TransactionError::CommitConflict(
                    deltalake::kernel::transaction::CommitConflictError::ProtocolChanged(
                        "another writer changed the protocol since this handle's version".into(),
                    ),
                ),
            }));
        }
        // 5. The shared part: transform, conform, coerce, the data-quality write, the commit.
        let outcome = self
            .process(Batch {
                started,
                through: 0,
                files: 0,
                input,
                rejects,
                lookups: LookupBatch::none(),
                position: Position::Kafka(offsets.clone()),
            })
            .await?;
        self.committed(batch, &offsets, started.elapsed()).await;
        // The backstop for an upgrade that landed between that read and the commit: delta-rs
        // rebased over it, and the handle it returned has it. Nothing more is written.
        if let Err(e) = fence::refuse_unwritable_protocol(&self.target) {
            error!(pipeline = %self.cfg.name, "the pipeline holds after this commit: {e}");
            let recheck = self.blocked_recheck();
            let src = self.kafka_mut();
            src.blocked = Some(e.to_string());
            src.next_recheck = Some(Instant::now() + recheck);
            self.pause_all();
        }
        Ok(outcome)
    }

    /// A partition of `batch` whose offset in the fence is not the one the batch was read
    /// from, or that this member no longer owns: committing the batch over it would write its
    /// rows twice, or skip a replay (§3.3).
    fn moved_partition(&self, batch: &KafkaBatch) -> Option<i32> {
        let src = self.kafka();
        batch.ranges.keys().copied().find(|p| {
            src.parts.get(*p).is_none()
                || src.fence.base(*p).map(|b| b.0) != batch.bases.get(p).copied()
        })
    }

    /// The `Fenced` a batch whose partition moved under it is dropped with.
    fn moved(&self, p: i32, batch: &KafkaBatch) -> Error {
        Error::Fenced {
            app_id: self.cfg.app_id.clone(),
            partition: p,
            expected: batch.bases.get(&p).copied().flatten(),
            found: self.kafka().fence.base(p).and_then(|b| b.0),
        }
    }

    /// Fit a batch's decoded rows for the transform: on the SQL path aligned to `U`, grown
    /// first by any writer schema it does not have yet (§5.5); either way, every writer schema
    /// told to the conformer, and the partition columns checked if open could not (§5.6).
    async fn prepare(
        &mut self,
        batch: &KafkaBatch,
        rejects: &mut Vec<Rejected>,
    ) -> Result<Vec<RecordBatch>> {
        let target_uri = self.cfg.target_uri.clone();
        let src = self.kafka_mut();
        let prefix = src.kafka.metadata_prefix.clone();
        src.conformer.observe(&batch.writer_schemas);
        if let Some(u) = src.union.as_mut() {
            let mut grew = false;
            for schema in &batch.writer_schemas {
                grew |= u.subject.observe(schema).await?;
            }
            let missing = batch
                .writer_schemas
                .iter()
                .any(|s| !u.union.members().contains(&s.id));
            if grew || missing {
                let rebuilt =
                    crate::kafka::conform::UnionSchema::build(&u.subject.members(), &prefix);
                rebuilt.plan(&u.sql).await?;
                let copies = u.sql.column_sources(rebuilt.schema()).await?;
                u.union = rebuilt;
                src.copies = copies.clone();
                src.conformer.observe_copies(copies);
                src.conformer.observe(&u.subject.members());
                info!(
                    writer_schemas = ?u.union.members(),
                    "the union the SQL runs over grew"
                );
            }
            if src.partition_check_deferred {
                let check = check_partition_columns(
                    &target_uri,
                    &src.partition_columns,
                    Some(&u.sql),
                    &u.union,
                )
                .await?;
                src.partition_check_deferred = check == PartitionCheck::Deferred;
            }
            return u.union.align_all(batch.decoded.clone(), rejects);
        }
        if src.partition_check_deferred && !batch.writer_schemas.is_empty() {
            let u = crate::kafka::conform::UnionSchema::build(&batch.writer_schemas, &prefix);
            let check =
                check_partition_columns(&target_uri, &src.partition_columns, None, &u).await?;
            src.partition_check_deferred = check == PartitionCheck::Deferred;
        }
        Ok(batch.decoded.clone())
    }

    /// A batch committed: the fence, the counters, and what it releases.
    async fn committed(&mut self, batch: &KafkaBatch, offsets: &KafkaOffsets, took: Duration) {
        let version = self.target.version();
        let added = added_files(&self.target, version).await;
        let once = self.kafka().kafka.once;
        let commit_to_kafka = self.kafka().kafka.commit_offsets_to_kafka && !once;
        let src = self.kafka_mut();
        src.fence.committed(offsets);
        let now = Instant::now();
        // Committing is not idle: once-mode's idle rule counts from here (§3.8).
        src.parts.polled_again(None, now);
        src.last_commit = Some(now);
        src.refreshed_at = now;
        src.held_for = None;
        src.last_backoff = None;
        if src.blocked.take().is_some() {
            info!("the pipeline goes on: its batch committed");
        }
        let m = &src.metrics;
        m.messages.fetch_add(batch.messages, Ordering::Relaxed);
        m.bytes.fetch_add(batch.raw_bytes, Ordering::Relaxed);
        m.undecodable
            .fetch_add(batch.undecodable as u64, Ordering::Relaxed);
        m.tombstones.fetch_add(batch.tombstones, Ordering::Relaxed);
        m.commit_millis
            .fetch_add(took.as_millis() as u64, Ordering::Relaxed);
        if let Some((files, bytes)) = added {
            m.files_added.fetch_add(files, Ordering::Relaxed);
            m.bytes_added.fetch_add(bytes, Ordering::Relaxed);
        }
        for gauge in [&m.retrying, &m.undecodable_blocking, &m.schema_conflict] {
            gauge.store(0, Ordering::Relaxed);
        }
        if commit_to_kafka {
            // Informational: lag tooling reads the group's offsets. Never read back.
            let mut tpl = TopicPartitionList::new();
            for (p, last) in &offsets.assert {
                let _ = tpl.add_partition_offset(&src.kafka.topic, *p, Offset::Offset(last + 1));
            }
            if let Err(e) = src.consumer().commit(&tpl, CommitMode::Async) {
                debug!("committing offsets to Kafka: {e}");
            }
        }
    }

    /// What a failed commit leads to, by its class (§3.6).
    async fn after_failure(&mut self, batch: KafkaBatch, e: Error) -> Result<StepOutcome> {
        if e.kafka_class() != KafkaClass::Replan {
            return self.hold_for_class(batch, e);
        }
        let m = &self.kafka().metrics;
        let counter = match &e {
            Error::Fenced { .. } => &m.fenced_base_mismatch,
            Error::Delta(deltalake::DeltaTableError::Transaction {
                source:
                    deltalake::kernel::transaction::TransactionError::CommitConflict(
                        deltalake::kernel::transaction::CommitConflictError::ConcurrentTransaction,
                    ),
            }) => &m.fenced_concurrent_txn,
            _ => &m.fenced_other_conflict,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        info!(
            pipeline = %self.cfg.name,
            "another writer committed first; reading the offsets again in place: {e}"
        );
        self.reset_in_place(Some(batch)).await
    }

    /// Hold `batch` as `e`'s class says (§3.6): a backoff for a transient failure (a Replan
    /// met while resetting counts as one), the full one for capacity, an operator's recheck
    /// for a blocking one. Consumer-fatal is returned.
    fn hold_for_class(&mut self, batch: KafkaBatch, e: Error) -> Result<StepOutcome> {
        let class = e.kafka_class();
        let once = self.kafka().kafka.once;
        let last = self.kafka_mut().last_backoff.take();
        let recheck = self.blocked_recheck();
        match class {
            KafkaClass::ConsumerFatal => Err(e),
            KafkaClass::Replan | KafkaClass::Transient | KafkaClass::Capacity => {
                let (backoff, why) = match class {
                    KafkaClass::Capacity => (BACKOFF_CAPACITY, HeldFor::Capacity),
                    _ => (
                        last.map_or(BACKOFF_MIN, |b| (b * 2).clamp(BACKOFF_MIN, BACKOFF_MAX)),
                        HeldFor::Transient,
                    ),
                };
                warn!(
                    pipeline = %self.cfg.name,
                    retry_in_ms = backoff.as_millis() as u64,
                    "the batch is held and tried again; this member stays in its group: {e}"
                );
                self.hold(batch, backoff, why);
                Ok(StepOutcome::Waiting)
            }
            KafkaClass::BlockingPipeline
            | KafkaClass::BlockingPartition
            | KafkaClass::OpenRefusal => {
                if once {
                    // A batch run has nobody to wait for: its exit code says what is wrong.
                    return Err(e);
                }
                error!(
                    pipeline = %self.cfg.name,
                    recheck_in_secs = recheck.as_secs(),
                    "the pipeline holds until this is fixed; it stays in its group: {e}"
                );
                self.kafka().metrics.observe_error(&e);
                self.kafka_mut().blocked = Some(e.to_string());
                self.hold(batch, recheck, HeldFor::Operator);
                Ok(StepOutcome::Waiting)
            }
        }
    }

    fn hold(&mut self, batch: KafkaBatch, backoff: Duration, why: HeldFor) {
        let src = self.kafka_mut();
        src.held = Some(Held {
            batch,
            due: Instant::now() + backoff,
            backoff,
            retrying: matches!(
                why,
                HeldFor::Transient | HeldFor::Capacity | HeldFor::Replan
            ),
        });
        src.held_for = Some(why);
        self.pause_all();
    }

    // --------------------------------------------------------------------- resetting

    /// Refresh the target and re-read every owned partition's offset, staying in the group
    /// (§3.3, `reset_in_place`). `batch` is kept, and tried again at once, if each of its
    /// partitions still has the offset the batch was read from; otherwise it is dropped, and
    /// every partition that moved, or that it covered, is read again from its offset in the
    /// target.
    pub(crate) async fn reset_in_place(
        &mut self,
        batch: Option<KafkaBatch>,
    ) -> Result<StepOutcome> {
        self.kafka_mut().reset_this_step = true;
        let changed = match self.refresh_and_reset().await {
            Ok(changed) => changed,
            // The reset failed in its own way: the batch is held for that, never reset again
            // from here.
            Err(e) => {
                return match batch {
                    Some(batch) => self.hold_for_class(batch, e),
                    None => Err(e),
                }
            }
        };
        // By the offsets the batch was read from, not by what this refresh changed: an
        // earlier refresh (a commit's aging) may have moved the fence already.
        let survives = batch.filter(|b| self.moved_partition(b).is_none());
        self.reposition_changed(&changed, survives.as_ref());
        if let Some(batch) = survives {
            let src = self.kafka_mut();
            src.held = Some(Held {
                batch,
                due: Instant::now(),
                backoff: Duration::ZERO,
                retrying: true,
            });
            src.held_for = Some(HeldFor::Replan);
        } else {
            info!(
                pipeline = %self.cfg.name,
                partitions = ?changed,
                "the batch was dropped: its partitions are read again from their offsets in the \
                 target"
            );
        }
        Ok(StepOutcome::Waiting)
    }

    /// Partitions to read again after a reset: every owned one whose offset changed, and every
    /// one with rows in the buffer or a dropped batch, from its offset in the target. Paused,
    /// so that nothing arrives from the old position before the seek (`release`).
    fn reposition_changed(&mut self, changed: &BTreeSet<i32>, survives: Option<&KafkaBatch>) {
        let src = self.kafka_mut();
        // A buffer with rows of a partition that moved holds rows read from the wrong place.
        let mut again: BTreeSet<i32> = changed.clone();
        if !src.buf.is_empty() && src.buf.ranges.keys().any(|p| changed.contains(p)) {
            again.extend(src.buf.ranges.keys().copied());
            src.buf = Buffer::default();
            src.decoder_mut().discard();
        }
        // Partitions a dropped batch read, which only the batch accounted for.
        for (p, state) in src.parts.iter() {
            if let State::Ready { next_expected, .. } = state {
                let at = src
                    .fence
                    .base(*p)
                    .and_then(|b| b.0)
                    .map(|b| b + 1)
                    .or_else(|| src.unbased_start.get(p).copied());
                let covered = survives.is_some_and(|b| b.ranges.contains_key(p));
                if !covered
                    && !src.buf.ranges.contains_key(p)
                    && at.is_some_and(|a| a != *next_expected)
                {
                    again.insert(*p);
                }
            }
        }
        let mut pause = Vec::new();
        let now = Instant::now();
        for p in again {
            match src.parts.get(p) {
                Some(State::Ready { .. }) => {
                    let base = src.fence.base(p).and_then(|b| b.0);
                    let at = base
                        .map(|b| b + 1)
                        .or_else(|| src.unbased_start.get(&p).copied())
                        .unwrap_or(0);
                    src.parts.ready_at(p, at, None, now);
                    pause.push(p);
                }
                // A blocked partition whose offset moved (an operator's set-offsets) is
                // resolved again by the recheck, now.
                Some(State::Blocked { .. }) => src.next_recheck = Some(now),
                None => {}
            }
        }
        self.pause(&pause);
    }

    /// Refresh the target, then re-read every owned partition's offset (§3.3 reset steps 1-6).
    /// Returns the owned partitions whose offset in the target is not what it was.
    pub(crate) async fn refresh_and_reset(&mut self) -> Result<BTreeSet<i32>> {
        let owned = self.kafka().parts.owned();
        let before: BTreeMap<i32, Option<i64>> = owned
            .iter()
            .map(|p| (*p, self.kafka().fence.base(*p).and_then(|b| b.0)))
            .collect();
        self.refresh_target().await?;
        let (src, target) = self.kafka_and_target();
        src.fence.reset(target, &owned).await?;
        src.refreshed_at = Instant::now();
        Ok(owned
            .into_iter()
            .filter(|p| src.fence.base(*p).and_then(|b| b.0) != before[p])
            .collect())
    }

    /// §3.3 reset steps 1-5: the target read again, incrementally where it can be; checked to
    /// be the table opened, with no txn retention and a protocol this build writes; the
    /// coercer and the conformer rebuilt if another writer changed its schema.
    pub(crate) async fn refresh_target(&mut self) -> Result<()> {
        let found = match self.target.update_incremental(None).await {
            // The handle remembers the id it was opened with; storage says which table is
            // there now (a table dropped and recreated in place leaves the handle as it was).
            Ok(()) => match fence::stored_table_id(&self.target).await? {
                Some(id) => Some(id),
                None => crate::lookup::table_id(&self.target),
            },
            Err(e) => {
                debug!(pipeline = %self.cfg.name, "incremental refresh failed, opening again: {e}");
                // Storage::open steps over a checkpoint another engine wrote (storage.rs:95-142).
                self.target = self.cfg.storage.open(&self.cfg.target_uri).await?;
                crate::lookup::table_id(&self.target)
            }
        };
        let opened = self.kafka().table_id.clone();
        if opened.is_some() && found != opened {
            return Err(Error::TargetReplaced {
                target_uri: self.cfg.target_uri.clone(),
                opened: opened.unwrap_or_default(),
                found: found.unwrap_or_default(),
            });
        }
        fence::refuse_txn_retention(&self.target)?;
        fence::refuse_unwritable_protocol(&self.target)?;
        let (schema, _) = schema_of(&self.target)?;
        if schema != self.coercer.target() {
            info!(
                pipeline = %self.cfg.name,
                "the target's schema changed under this pipeline; rebuilding its casts"
            );
            self.coercer = SchemaCoercer::new(schema.clone());
            let src = self.kafka_mut();
            let mut conformer = Conformer::new(schema, &src.kafka.metadata_prefix, src.kdi_text)?;
            conformer.observe(&src.writer_schemas());
            if let Some(u) = &src.union {
                conformer.observe(&u.subject.members());
            }
            conformer.observe_copies(src.copies.clone());
            src.conformer = conformer;
        }
        Ok(())
    }

    /// A held batch for an operator is due: look at the target again (and the data-quality
    /// table, which may have been created meanwhile) before trying it. `None` when the batch
    /// was dropped because a partition of it moved.
    async fn recheck_for_batch(&mut self, batch: KafkaBatch) -> Result<Option<KafkaBatch>> {
        if self.dq.is_none() {
            let dq_uri = self.cfg.dq_uri();
            if let Some(mut dq) =
                DataQuality::open(&self.cfg.storage, &dq_uri, &self.cfg.app_id, &self.cfg.name)
                    .await?
            {
                info!(pipeline = %self.cfg.name, dq_uri = %dq_uri, "the data-quality table exists now");
                dq.set_kafka_metadata_prefix(&self.kafka().kafka.metadata_prefix);
                dq.set_kafka_counters(self.kafka().metrics.dq.clone());
                self.dq = Some(dq);
            }
        }
        let changed = match self.refresh_and_reset().await {
            Ok(c) => c,
            Err(e) if e.kafka_class() != KafkaClass::ConsumerFatal => {
                debug!(pipeline = %self.cfg.name, "the recheck could not refresh the target: {e}");
                let recheck = self.blocked_recheck();
                self.kafka_mut().blocked = Some(e.to_string());
                self.hold(batch, recheck, HeldFor::Operator);
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let valid = self.moved_partition(&batch).is_none();
        self.reposition_changed(&changed, valid.then_some(&batch));
        if valid {
            Ok(Some(batch))
        } else {
            self.kafka_mut().blocked = None;
            Ok(None)
        }
    }

    /// Blocked partitions, and a pipeline blocked at assign, looked at again (§3.6): the
    /// target refreshed, their offsets read, their topics' watermarks fetched. A partition
    /// that resolves resumes; the rest wait for the next recheck.
    async fn recheck_blocked(&mut self) -> Result<()> {
        let recheck = self.blocked_recheck();
        self.kafka_mut().next_recheck = None;
        let changed = match self.refresh_and_reset().await {
            Ok(c) => c,
            Err(e) if e.kafka_class() != KafkaClass::ConsumerFatal => {
                debug!(pipeline = %self.cfg.name, "a recheck could not read the target: {e}");
                self.kafka().metrics.observe_error(&e);
                let src = self.kafka_mut();
                if src.parts.blocked().any(|(_, b)| *b == Block::Unresolved) {
                    src.blocked = Some(e.to_string());
                }
                src.next_recheck = Some(Instant::now() + recheck);
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        self.reposition_changed(&changed, None);
        // A partition blocked because the message at its stored offset is another one (I19)
        // waits for that offset to move: the table and the watermarks cannot see a replaced
        // topic, only the re-read can, and resolving it again would re-read and block it again
        // every recheck.
        let src = self.kafka();
        let blocked: Vec<i32> = src
            .parts
            .blocked()
            .filter(|(p, block)| match block {
                Block::Replaced { recorded, .. } => {
                    src.fence.base(*p).and_then(|b| b.0) != Some(recorded.offset)
                }
                _ => true,
            })
            .map(|(p, _)| p)
            .collect();
        if blocked.is_empty() {
            let src = self.kafka_mut();
            src.blocked = None;
            if src.parts.blocked().next().is_some() {
                src.next_recheck = Some(Instant::now() + recheck);
            }
            return Ok(());
        }
        let topic = self.kafka().kafka.topic.clone();
        let consumer = self.kafka().consumer().clone();
        let watermarks = match super::rebalance::watermarks(&consumer, &topic, &blocked).await {
            Ok(w) => w,
            Err(e) => {
                debug!(pipeline = %self.cfg.name, "a recheck could not read the watermarks: {e}");
                self.kafka_mut().next_recheck = Some(Instant::now() + recheck);
                return Ok(());
            }
        };
        let now = Instant::now();
        let src = self.kafka_mut();
        src.blocked = None;
        let explicit = src
            .kafka
            .starting_offsets
            .partitions()
            .cloned()
            .unwrap_or_default();
        for p in blocked {
            let Some((low, high)) = watermarks.get(&p).copied() else {
                continue;
            };
            let (base, mark) = src.fence.base(p).unwrap_or((None, None));
            match start::resolve(base, mark, low, high, explicit.get(&p).copied()) {
                start::Start::Ready { offset, reread } => {
                    info!(partition = p, offset, "the partition resumes");
                    if base.is_none() {
                        src.unbased_start.insert(p, offset);
                    }
                    src.parts.ready_at(p, offset, reread, now);
                }
                start::Start::Blocked(block) => {
                    src.parts.block(p, block, now);
                }
            }
        }
        if src.parts.blocked().next().is_some() {
            src.next_recheck = Some(now + recheck);
        }
        Ok(())
    }

    // -------------------------------------------------------------- rebalance events

    async fn handle_events(&mut self) -> Result<()> {
        let events = self.kafka().consumer().context().drain();
        for event in events {
            match event {
                Event::Assign(tpl) => {
                    self.kafka()
                        .metrics
                        .rebalances_assign
                        .fetch_add(1, Ordering::Relaxed);
                    let topic = self.kafka().kafka.topic.clone();
                    let partitions: Vec<i32> = tpl
                        .elements_for_topic(&topic)
                        .iter()
                        .map(|e| e.partition())
                        .collect();
                    self.assign_partitions(partitions, false).await?;
                }
                Event::Revoke { lost: false, .. } => {
                    self.kafka()
                        .metrics
                        .rebalances_revoke
                        .fetch_add(1, Ordering::Relaxed);
                    let outcome = self.flush_then_unassign().await?;
                    if outcome.is_some() {
                        self.kafka_mut().pending_outcome = outcome;
                    }
                }
                Event::Revoke { lost: true, .. } => {
                    self.kafka()
                        .metrics
                        .rebalances_lost
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        pipeline = %self.cfg.name,
                        "the partitions were lost (this member was out of its group); dropping \
                         what it buffered: the next owner reads it from the target's offsets"
                    );
                    self.drop_and_unassign().await?;
                }
                Event::RebalanceError(e) => {
                    self.kafka()
                        .metrics
                        .rebalances_error
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(pipeline = %self.cfg.name, "rebalance error {e}: treated as lost");
                    self.drop_and_unassign().await?;
                }
                Event::Fatal(detail) => {
                    return Err(Error::KafkaClient {
                        failure: crate::error::KafkaClientFailure::Fatal,
                        detail,
                    })
                }
            }
        }
        Ok(())
    }

    /// Commit what a shutting-down member has buffered, within the revoke time box, before the
    /// consumer closes (§3.9). The close then revokes as usual.
    pub(crate) async fn kafka_shutdown(&mut self) {
        match self.flush("shutdown").await {
            Ok(Some(_)) => info!(pipeline = %self.cfg.name, "committed the buffer before stopping"),
            Ok(None) => {}
            Err(e) => {
                warn!(pipeline = %self.cfg.name, "could not commit the buffer before stopping: {e}")
            }
        }
    }

    // ---------------------------------------------------------------------- statistics

    /// Lag and retention headroom from the last statistics (§3.5), and whether the member is
    /// caught up.
    fn observe_stats(&mut self) {
        let (stats, generation) = self.kafka().consumer().context().stats();
        let src = self.kafka_mut();
        let fresh = match (stats.at, src.stats_interval) {
            (Some(at), Some(interval)) => at.elapsed() <= interval * 2,
            _ => false,
        };
        if generation == src.stats_seen {
            src.caught_up = src.caught_up && fresh;
            return;
        }
        src.stats_seen = generation;
        let mut lag_total = 0;
        let mut lag_max = 0;
        let mut headroom: Option<i64> = None;
        let mut all_zero = !src.parts.is_empty();
        let mut gauges = BTreeMap::new();
        for (p, state) in src.parts.iter() {
            let committed = src.fence.base(*p).and_then(|b| b.0).unwrap_or(-1);
            let lag = match (state, stats.partitions.get(p)) {
                (State::Ready { next_expected, .. }, Some(w)) => {
                    let lag = (w.ls - next_expected).max(0);
                    lag_total += lag;
                    lag_max = lag_max.max(lag);
                    let room = next_expected - w.lo;
                    headroom = Some(headroom.map_or(room, |h| h.min(room)));
                    lag
                }
                _ => -1,
            };
            if lag != 0 {
                all_zero = false;
            }
            gauges.insert(*p, PartitionGauges { committed, lag });
        }
        let m = &src.metrics;
        m.lag_messages.store(lag_total, Ordering::Relaxed);
        m.lag_messages_max.store(lag_max, Ordering::Relaxed);
        m.retention_headroom_messages
            .store(headroom.unwrap_or(-1), Ordering::Relaxed);
        m.set_partitions(gauges);
        src.caught_up = fresh && all_zero;
    }

    /// The gauges a step leaves behind.
    fn update_gauges(&mut self) {
        let src = self.kafka();
        let m = &src.metrics;
        m.assigned_partitions
            .store(src.parts.len() as i64, Ordering::Relaxed);
        let blocked: Vec<&Block> = src.parts.blocked().map(|(_, b)| b).collect();
        m.blocked_partitions
            .store(blocked.len() as i64, Ordering::Relaxed);
        let has = |f: fn(&Block) -> bool| i64::from(blocked.iter().any(|b| f(b)));
        m.offsets_unavailable.store(
            has(|b| matches!(b, Block::Unavailable { .. })),
            Ordering::Relaxed,
        );
        m.offsets_regressed.store(
            has(|b| matches!(b, Block::Regressed { .. })),
            Ordering::Relaxed,
        );
        m.topic_replaced.store(
            has(|b| matches!(b, Block::Replaced { .. })),
            Ordering::Relaxed,
        );
        m.oldest_buffered_seconds.store(
            src.buf
                .opened_at
                .map_or(0, |t| t.elapsed().as_secs() as i64),
            Ordering::Relaxed,
        );
        m.target_columns_unmatched.store(
            src.conformer.unmatched_target_columns().len() as i64,
            Ordering::Relaxed,
        );
        m.null_text_approximate.store(
            i64::from(src.conformer.null_text_approximate()),
            Ordering::Relaxed,
        );
        let retrying = src.stalled.is_some()
            || matches!(src.held_for, Some(HeldFor::Transient)) && src.held.is_some();
        m.retrying.store(i64::from(retrying), Ordering::Relaxed);
        let gauges: BTreeMap<i32, PartitionGauges> = src
            .parts
            .owned()
            .into_iter()
            .map(|p| {
                let mut g = m
                    .partitions
                    .lock()
                    .map(|g| g.get(&p).copied())
                    .ok()
                    .flatten()
                    .unwrap_or(PartitionGauges {
                        committed: -1,
                        lag: -1,
                    });
                g.committed = src.fence.base(p).and_then(|b| b.0).unwrap_or(-1);
                (p, g)
            })
            .collect();
        m.set_partitions(gauges);
    }
}

/// A message copied out of librdkafka, whose own dies with the next poll (EO-m3).
fn copy(m: &rdkafka::message::BorrowedMessage<'_>) -> KafkaMessage {
    let timestamp = match m.timestamp() {
        rdkafka::Timestamp::NotAvailable => KafkaTimestamp::NotAvailable,
        rdkafka::Timestamp::CreateTime(t) => KafkaTimestamp::CreateTime(t),
        rdkafka::Timestamp::LogAppendTime(t) => KafkaTimestamp::LogAppendTime(t),
    };
    let headers = m
        .headers()
        .map(|h| {
            h.iter()
                .map(|h| KafkaHeader {
                    key: h.key.to_string(),
                    value: h.value.map(<[u8]>::to_vec),
                })
                .collect()
        })
        .unwrap_or_default();
    KafkaMessage {
        partition: m.partition(),
        offset: m.offset(),
        timestamp,
        key: m.key().map(<[u8]>::to_vec),
        value: m.payload().map(<[u8]>::to_vec),
        headers,
    }
}

/// The mark a commit records for a message: what a resume compares the message it re-reads
/// there with (§4.4, I19).
pub(crate) fn mark_of(m: &KafkaMessage) -> MessageMark {
    MessageMark {
        offset: m.offset,
        timestamp_ms: m.timestamp.millis().unwrap_or(-1),
        value_len: m.value.as_ref().map_or(0, |v| v.len() as u32),
    }
}

/// The target's schema as Arrow reads it, and its partition columns.
fn schema_of(target: &DeltaTable) -> Result<(SchemaRef, Vec<String>)> {
    let snapshot = target.snapshot().map_err(Error::Delta)?;
    let schema = snapshot.snapshot().read_schema();
    let partitions = snapshot.metadata().partition_columns().to_vec();
    Ok((schema, partitions))
}

/// Whether the commit about to be made from `target` will also checkpoint, and so may clean up
/// the log (§7.2): only then, as in kafka-delta-ingest, rather than after every commit.
fn cleans_up(target: &DeltaTable) -> bool {
    use deltalake::table::config::TablePropertiesExt;
    let Ok(snapshot) = target.snapshot() else {
        return false;
    };
    let properties = snapshot.table_config();
    let next = target.version().map_or(0, |v| v + 1);
    properties.enable_expired_log_cleanup()
        && (next + 1) % properties.checkpoint_interval().get() == 0
}

/// The files commit `version` of `target` added, and their bytes (`ddi_kafka_files_added_total`):
/// delta-rs 0.32.4's writer does not hand its own metrics back, so they are read off the
/// commit, one GET. Best effort: `None` if it cannot be read.
async fn added_files(target: &DeltaTable, version: Option<u64>) -> Option<(u64, u64)> {
    #[derive(serde::Deserialize)]
    struct Line {
        add: Option<Add>,
    }
    #[derive(serde::Deserialize)]
    struct Add {
        #[serde(default)]
        size: u64,
    }
    let bytes = target
        .log_store()
        .read_commit_entry(version?)
        .await
        .ok()
        .flatten()?;
    let (mut files, mut size) = (0, 0);
    for line in serde_json::Deserializer::from_slice(&bytes).into_iter::<Line>() {
        if let Ok(Line { add: Some(add) }) = line {
            files += 1;
            size += add.size;
        }
    }
    Some((files, size))
}

/// Where a revoke or a shutdown leaves the buffer (§3.4).
pub(crate) enum Flushed {
    Committed(StepOutcome),
    Dropped,
    TimedOut,
    Nothing,
}

impl Pipeline {
    /// Commit the buffer, or a held batch, within the revoke time box. Failure, a timeout or a
    /// fence drops it: the next owner reads it from the target's offsets. A blocked pipeline,
    /// and a message waiting for the registry, have nothing that can be committed.
    pub(crate) async fn flush(&mut self, why: &str) -> Result<Option<StepOutcome>> {
        let flushed = self.flush_inner(why).await?;
        let m = &self.kafka().metrics;
        Ok(match flushed {
            Flushed::Committed(o) => {
                m.revoke_flushes_committed.fetch_add(1, Ordering::Relaxed);
                Some(o)
            }
            Flushed::Dropped => {
                m.revoke_flushes_dropped.fetch_add(1, Ordering::Relaxed);
                None
            }
            Flushed::TimedOut => {
                m.revoke_flushes_timeout.fetch_add(1, Ordering::Relaxed);
                None
            }
            Flushed::Nothing => {
                m.revoke_flushes_none.fetch_add(1, Ordering::Relaxed);
                None
            }
        })
    }

    async fn flush_inner(&mut self, why: &str) -> Result<Flushed> {
        let src = self.kafka_mut();
        let has_work = src.held.is_some() || !src.buf.is_empty();
        if !has_work {
            return Ok(Flushed::Nothing);
        }
        if src.blocked.is_some() || src.stalled.is_some() {
            return Ok(Flushed::Dropped);
        }
        let timeout = Duration::from_secs(src.kafka.revoke_flush_timeout_secs.max(1));
        let batch = match src.held.take() {
            Some(held) => held.batch,
            None => self.close_batch()?,
        };
        match tokio::time::timeout(timeout, self.try_commit(&batch)).await {
            Ok(Ok(outcome)) => {
                info!(pipeline = %self.cfg.name, why, "committed the buffer before giving the partitions up");
                Ok(Flushed::Committed(outcome))
            }
            Ok(Err(e)) => {
                warn!(pipeline = %self.cfg.name, why, "could not commit the buffer, dropping it; the next owner reads it again: {e}");
                Ok(Flushed::Dropped)
            }
            Err(_) => {
                warn!(
                    pipeline = %self.cfg.name,
                    why,
                    secs = timeout.as_secs(),
                    "committing the buffer took longer than revoke_flush_timeout_secs; dropping \
                     it, and the next owner reads it again"
                );
                Ok(Flushed::TimedOut)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mark_is_the_offset_timestamp_and_value_length() {
        let m = KafkaMessage {
            partition: 1,
            offset: 41,
            timestamp: KafkaTimestamp::CreateTime(1_700_000_000_123),
            key: None,
            value: Some(vec![0; 17]),
            headers: Vec::new(),
        };
        assert_eq!(
            mark_of(&m),
            MessageMark {
                offset: 41,
                timestamp_ms: 1_700_000_000_123,
                value_len: 17
            }
        );
        let tombstone = KafkaMessage {
            value: None,
            timestamp: KafkaTimestamp::NotAvailable,
            ..m
        };
        assert_eq!(mark_of(&tombstone).value_len, 0);
        assert_eq!(mark_of(&tombstone).timestamp_ms, -1);
    }
}
