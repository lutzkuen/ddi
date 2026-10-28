//! Process metrics, exported in Prometheus text format.
//!
//! Deliberately dependency-free: a handful of atomic counters and a tiny HTTP responder.
//! Pulling in a metrics framework for six counters would be the opposite of the "thin
//! daemon" design.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Default)]
pub struct PipelineMetrics {
    pub batches_committed: AtomicU64,
    pub rows_written: AtomicU64,
    pub files_read: AtomicU64,
    pub errors: AtomicU64,
    pub commits_skipped: AtomicU64,
    /// Last source version durably committed. -1 until the first commit.
    pub last_source_version: AtomicI64,
    /// Source head version at the last poll. -1 until first poll.
    pub source_head_version: AtomicI64,
    /// Next source version to consume. -1 until the first poll.
    ///
    /// Distinct from `last_source_version`, which is the *durable* offset. The two differ
    /// whenever commits are consumed without producing a commit of our own — a run of
    /// `OPTIMIZE` on the source advances the cursor but writes no `txn` action. Lag is
    /// measured from this one, so compaction on the source does not read as backlog.
    pub cursor_version: AtomicI64,

    // Upsert only. All zero on an append pipeline, which is the honest reading: it never
    // updates a row and never reads the target back.
    /// Stored rows replaced by a newer delivery of the same key.
    pub rows_updated: AtomicU64,
    /// Keys the target did not already hold.
    pub rows_inserted: AtomicU64,
    /// Target files a merge had to open. The number the merge window exists to keep down —
    /// watch it against `ddi_batches_committed_total` for files-per-batch.
    pub target_files_scanned: AtomicU64,
    /// Batches whose merge window could not be bounded at all, so the whole target was
    /// read. Anything but zero means the target's statistics cannot answer the question —
    /// usually the key column sitting past `delta.dataSkippingNumIndexedCols`.
    pub upsert_window_unbounded: AtomicU64,
    /// Batches where `upsert_lookback` held the window above what the statistics asked for.
    /// **Each one may have inserted a key alongside an older row instead of replacing it**,
    /// so this is the metric to alert on.
    pub upsert_window_clamped: AtomicU64,
    /// Total milliseconds spent inside merges, permit already in hand.
    ///
    /// A counter rather than an average, so the reader picks the window. Divided by
    /// `ddi_merges_total` it is the mean merge; its *rate* is merge-seconds per second,
    /// which is how many merges this pipeline keeps permanently busy.
    pub merge_millis: AtomicU64,
    /// Total milliseconds spent waiting for a merge permit, before the merge began.
    ///
    /// Kept apart from `merge_millis` because the two have different cures: time inside a
    /// merge is the target's size, time before one is `max_concurrent_upsert_merges`.
    pub merge_queue_millis: AtomicU64,
    /// Merges started. The denominator for both millisecond counters.
    pub merges: AtomicU64,

    /// 1 while this pipeline is streaming, 0 while it is backing off after a failure.
    ///
    /// With hundreds of streams in one process, "is the process alive" stopped being a
    /// useful question — it is alive whenever *any* stream is. This is the per-stream
    /// answer, and the one to build a health check on.
    pub up: AtomicI64,
    /// How many times this pipeline has been reopened after a failure. A number that keeps
    /// climbing is a stream stuck in a loop on something a human has to fix.
    pub restarts: AtomicU64,
    /// Rows the target would not take, written to the data-quality table instead.
    pub rows_rejected: AtomicU64,
    /// Of those, rows the transform could not evaluate at all. Separate because the cure is
    /// different: a coercion reject is a value that does not fit the target, and this one is
    /// a value the model cannot handle — usually worth a change to the model.
    pub rows_rejected_by_transform: AtomicU64,
    /// Runs of a transform beyond the first, spent finding the rows it could not evaluate.
    ///
    /// The cost of those rows. Each run re-plans the query and re-scans any lookup it joins,
    /// so a rate that climbs against `ddi_batches_committed_total` is time the pipeline is
    /// spending on bad rows rather than on new ones.
    pub transform_reevaluations: AtomicU64,
    /// Batches where *every* row was rejected. Far more likely an upstream schema change
    /// than data going bad, and otherwise invisible: the target just stops growing.
    pub batches_fully_rejected: AtomicU64,
    /// Rows dropped because the target was taken to hold them already, while a rebuild, a
    /// first start against a populated target or a replaced source was being caught up.
    ///
    /// Counted because nothing else would show them: they reach neither the target nor the
    /// data-quality table, and the offset moves past them. Rising in step with a rebuild is
    /// the filter doing its job; rising at any other time is rows being lost.
    pub rows_skipped_as_covered: AtomicU64,
    /// 1 while this pipeline is dropping rows the target is taken to hold already, 0 otherwise.
    ///
    /// A gauge as well as the counter, because the window that drops them need not end on its
    /// own: after a replaced source, or on a staged upsert's apply half, only a row newer than
    /// the target's watermark — than its partition's, with `dedup_partition` — closes it. One
    /// that stays at 1 on an idle pipeline is waiting for that row, and a watermark somebody
    /// pushed into the future would hold it there.
    pub coverage_cutoff_active: AtomicI64,
    /// 1 when this pipeline's configuration was accepted, 0 when it was held back at load.
    ///
    /// Distinct from `up`, and the distinction matters: `up = 0` means a stream that was
    /// running has stopped, while `config_valid = 0` means one that never started. Without
    /// it a typo in one entry of three hundred is visible only in the startup log.
    pub config_valid: AtomicI64,
    /// 1 while this pipeline is stopped on a source file the object store no longer has.
    ///
    /// The one failure the backoff cannot clear. Every other error is worth retrying —
    /// storage blips, a commit race, a schema that is about to be fixed — so `up` flapping
    /// to 0 and back is normal and paging on it would be noise. This one holds at 1 until
    /// somebody restores the file or rebuilds the target, and it is the signal that says
    /// which of the two a stuck stream is: a retry that will work eventually, or a retry
    /// that will not.
    pub source_file_vacuumed: AtomicI64,
    /// 1 while this pipeline has never committed and the version it would start from is no
    /// longer in the source's log.
    ///
    /// The same shape as [`Self::source_file_vacuumed`], and added for the same complaint:
    /// before it, a pipeline whose `starting_version` had aged out of
    /// `delta.logRetentionDuration` retried every five minutes forever behind a generic
    /// `delta error`, and the only way to find it was to grep a shared process log. It is
    /// separate from [`Self::resume_unreachable`] because the two have opposite recoveries —
    /// this one is a value an operator sets, that one is a rebuild — so an alert that cannot
    /// tell them apart cannot route.
    pub bootstrap_unreachable: AtomicI64,
    /// 1 while this pipeline has committed before and the version it must resume at is no
    /// longer in the source's log.
    ///
    /// The dangerous twin of [`Self::bootstrap_unreachable`]: rows this pipeline was
    /// responsible for are gone, and no setting recovers them. Worth its own series precisely
    /// because it is indistinguishable from a storage blip from outside — every other
    /// `up = 0` heals on its own, and this one never does.
    pub resume_unreachable: AtomicI64,
    /// 1 once this pipeline ran out of spill space or memory rather than being wrong.
    ///
    /// The same shape as [`Self::source_file_vacuumed`] and for the same reason: from
    /// outside, a pipeline retrying a capacity failure and a pipeline retrying a storage
    /// blip look identical, and only one of them will heal on its own. Raised, never lowered
    /// here; cleared by a step that actually succeeds.
    pub capacity_exhausted: AtomicI64,
    /// Passes the last startup uniqueness check took over this target's key column.
    ///
    /// One is the ordinary answer. More means the target's key space did not fit
    /// `[runtime] max_grain_check_memory` and was read in congruence classes — correct, and
    /// linear in this number, which is the only warning an operator gets that a start is
    /// going to be slow. See [`crate::grain`].
    pub grain_check_passes: AtomicU64,
    /// Unix seconds at the last successful step. -1 until the first one.
    ///
    /// The lag gauge cannot cover a pipeline that fails while *opening*: it never reaches
    /// the code that records head and cursor, so both keep the values they had when it was
    /// healthy and the backlog alert stays quiet while the stream is hours dead. Staleness
    /// is the reading that survives that, whatever the failure was.
    pub last_progress_unixtime: AtomicI64,

    // Realtime publication only. All zero on a pipeline that does not publish, which is the
    // honest reading: it never builds a payload and never sends one.
    //
    // None of these belongs in a health check. A publisher is a leaf: it cannot fail a
    // commit, so a pipeline whose publishes are all failing is *healthy and behind on a
    // dashboard*, not broken. `ddi_pipeline_up` deliberately does not move with them.
    /// Messages the service accepted.
    ///
    /// **Not a delivery count.** A group nobody has joined accepts a message and discards
    /// it, so this counts what left this process, not what any browser saw.
    pub publish_sent: AtomicU64,
    /// Payloads that could not be built or could not be sent. The batch committed anyway.
    pub publish_failed: AtomicU64,
    /// Batches where nothing was attempted because the breaker was open.
    ///
    /// Distinct from `publish_failed` on purpose: a run of failures becomes a run of skips,
    /// and conflating them would hide the moment publication stopped being tried at all.
    pub publish_skipped: AtomicU64,
    /// Messages sent with `complete: false` because the payload was over the size cap.
    ///
    /// Every one of these makes a client reload a baseline. Sustained non-zero means the
    /// publish model is grouping by something with too many values to push.
    pub publish_truncated: AtomicU64,
    /// Rows across all payloads built, whether or not they were sent.
    pub publish_rows: AtomicU64,
    /// Bytes across all payloads sent, for sizing against the service's frame limit.
    pub publish_bytes: AtomicU64,
    /// 1 when this pipeline has a publisher, 0 when it does not.
    ///
    /// The gauge that answers "is this dashboard being fed at all?", which no counter can:
    /// a pipeline that never publishes and one whose publishes all fail both leave
    /// `ddi_publish_sent_total` at zero.
    pub publish_configured: AtomicI64,

    /// A Kafka pipeline's own series (docs/kafka-design.md §7.1), set when it is first opened.
    /// Never set for a Delta pipeline, whose output is therefore what it always was.
    pub kafka: OnceLock<Arc<KafkaMetrics>>,
}

/// One partition's gauges.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PartitionGauges {
    /// The last offset this member committed for it, or -1.
    pub committed: i64,
    /// Messages behind the topic's end, from the last statistics; -1 while unknown.
    pub lag: i64,
}

/// What a Kafka pipeline exports beside the series every pipeline has (docs/kafka-design.md
/// §7.1). Always compiled, with no librdkafka type in it, so `/metrics` is assembled the same
/// way with or without the `kafka` feature.
///
/// Counters only ever rise, across reopens: the supervisor keeps one of these per pipeline
/// and hands it to every consumer it opens. Gauges are what the newest consumer last set.
#[derive(Debug, Default)]
pub struct KafkaMetrics {
    /// Messages of committed batches, tombstones and undecodable ones included.
    pub messages: AtomicU64,
    /// Their key and value bytes.
    pub bytes: AtomicU64,
    pub undecodable: AtomicU64,
    pub tombstones: AtomicU64,
    pub dropped_not_owned: AtomicU64,
    pub dropped_blocked: AtomicU64,
    pub dropped_duplicate: AtomicU64,
    pub dropped_reread: AtomicU64,
    /// Offsets a partition skipped between two messages (compaction, transaction markers).
    pub offset_gaps: AtomicU64,
    pub commit_millis: AtomicU64,
    pub files_added: AtomicU64,
    pub bytes_added: AtomicU64,
    pub rebalances_assign: AtomicU64,
    pub rebalances_revoke: AtomicU64,
    pub rebalances_lost: AtomicU64,
    pub rebalances_error: AtomicU64,
    pub revoke_flushes_committed: AtomicU64,
    pub revoke_flushes_dropped: AtomicU64,
    pub revoke_flushes_timeout: AtomicU64,
    pub revoke_flushes_none: AtomicU64,
    pub fenced_concurrent_txn: AtomicU64,
    pub fenced_base_mismatch: AtomicU64,
    pub fenced_other_conflict: AtomicU64,
    pub null_partition_values: AtomicU64,

    pub assigned_partitions: AtomicI64,
    pub blocked_partitions: AtomicI64,
    pub lag_messages: AtomicI64,
    pub lag_messages_max: AtomicI64,
    pub retention_headroom_messages: AtomicI64,
    pub oldest_buffered_seconds: AtomicI64,
    pub brokers_up: AtomicI64,
    pub retrying: AtomicI64,
    pub offsets_unavailable: AtomicI64,
    pub offsets_regressed: AtomicI64,
    pub offset_missing: AtomicI64,
    pub topic_replaced: AtomicI64,
    pub undecodable_blocking: AtomicI64,
    pub schema_conflict: AtomicI64,
    pub target_columns_unmatched: AtomicI64,
    pub null_text_approximate: AtomicI64,

    /// By partition.
    pub partitions: Mutex<BTreeMap<i32, PartitionGauges>>,
    /// Offset reads (`crate::fence::read_txns`), shared with the pipeline's fence.
    pub lookups: Arc<crate::fence::LookupCounters>,
    /// The data-quality table's conflict retries, shared with its writer.
    pub dq: Arc<crate::dq::KafkaCounters>,
    /// Schema Registry requests by outcome.
    pub schema_registry_requests: Mutex<BTreeMap<String, u64>>,
    /// librdkafka errors by code.
    pub client_errors: Mutex<BTreeMap<String, u64>>,
    /// Errors a step held instead of returning: a failed commit whose batch is held, a
    /// partition or the pipeline blocked, a message waiting for the registry. Counted in
    /// `ddi_errors_total` beside the errors the supervisor sees.
    pub held_errors: AtomicU64,
    /// `ddi_kafka_pipeline_blocked` by reason: 1 for the reason the whole pipeline is held
    /// for now, 0 for every reason it was held for before.
    pub pipeline_blocked: Mutex<BTreeMap<String, i64>>,
}

impl KafkaMetrics {
    /// Count one more under `key` of a labelled counter.
    pub fn count(map: &Mutex<BTreeMap<String, u64>>, key: &str) {
        let mut m = map.lock().unwrap_or_else(|e| e.into_inner());
        *m.entry(key.to_string()).or_default() += 1;
    }

    /// Replace every partition's gauges: the partitions this member owns now, and nothing
    /// for the ones it gave up.
    pub fn set_partitions(&self, gauges: BTreeMap<i32, PartitionGauges>) {
        *self.partitions.lock().unwrap_or_else(|e| e.into_inner()) = gauges;
    }

    /// An error a step held rather than returned: counted in `ddi_errors_total`, and its gauge
    /// raised ([`Self::observe_error`]).
    pub fn observe_held(&self, e: &crate::Error) {
        self.held_errors.fetch_add(1, Ordering::Relaxed);
        self.observe_error(e);
    }

    /// Why the whole pipeline is held now, or `None` when it is not.
    pub fn set_pipeline_blocked(&self, reason: Option<&str>) {
        let mut m = self
            .pipeline_blocked
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for v in m.values_mut() {
            *v = 0;
        }
        if let Some(reason) = reason {
            m.insert(reason.to_string(), 1);
        }
    }

    /// Raise the gauge for an open-time or held error, so the reason a pipeline is not
    /// reading is a series and not only a log line. Raised, never lowered here: the source
    /// clears what its next successful open or step no longer holds.
    pub fn observe_error(&self, e: &crate::Error) {
        use crate::Error;
        let gauge = match e {
            Error::KafkaOffsetMissing { .. } => &self.offset_missing,
            Error::KafkaOffsetsUnavailable { .. } => &self.offsets_unavailable,
            Error::KafkaOffsetsRegressed { .. } => &self.offsets_regressed,
            Error::KafkaTopicReplaced { .. } => &self.topic_replaced,
            Error::KafkaUndecodable { .. } | Error::KafkaTooManyUndecodable { .. } => {
                &self.undecodable_blocking
            }
            Error::KafkaSchemaConflict { .. } => &self.schema_conflict,
            _ => return,
        };
        gauge.store(1, Ordering::Relaxed);
    }
}

impl PipelineMetrics {
    /// Record what the realtime publisher did for one batch.
    ///
    /// Deliberately *not* routed through `mark_progress` or `observe_error`: publication
    /// sits outside the health story entirely. A pipeline whose every publish is failing is
    /// committing to Delta exactly as it should, so letting this touch `up` would page
    /// somebody about a dashboard.
    pub fn observe_publish(&self, stats: &crate::publish::PublishStats) {
        if stats.sent {
            self.publish_sent.fetch_add(1, Ordering::Relaxed);
            self.publish_bytes
                .fetch_add(stats.bytes as u64, Ordering::Relaxed);
        }
        if stats.failed {
            self.publish_failed.fetch_add(1, Ordering::Relaxed);
        }
        if stats.skipped {
            self.publish_skipped.fetch_add(1, Ordering::Relaxed);
        }
        if stats.truncated {
            self.publish_truncated.fetch_add(1, Ordering::Relaxed);
        }
        self.publish_rows
            .fetch_add(stats.rows as u64, Ordering::Relaxed);
    }

    /// Note that this pipeline just did something successfully.
    pub fn mark_progress(&self) {
        self.up.store(1, Ordering::Relaxed);
        // A step that succeeded read whatever it needed, so whatever was missing is not
        // missing any more. Cleared here rather than on reopen: reopening succeeds even
        // while the file is still gone, so clearing there would flap the gauge once per
        // retry instead of holding it up until the stream actually recovers.
        self.source_file_vacuumed.store(0, Ordering::Relaxed);
        // Same argument: a step that succeeded found the room it needed, so whatever was
        // full is not full any more.
        self.capacity_exhausted.store(0, Ordering::Relaxed);
        // And same again for both halves of an unreadable log position: a step that
        // succeeded read the commit it needed, so the log reaches far enough now — either
        // because somebody set a reachable starting_version, or because the target was
        // rebuilt. Note that a `CaughtUp` step counts, which is correct: a pipeline told to
        // start above the head is waiting, not stuck.
        self.bootstrap_unreachable.store(0, Ordering::Relaxed);
        self.resume_unreachable.store(0, Ordering::Relaxed);
        if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            self.last_progress_unixtime
                .store(d.as_secs() as i64, Ordering::Relaxed);
        }
    }

    /// Note that a step failed, and whether it failed in the one way waiting cannot fix.
    ///
    /// Lives here rather than inline in the supervisor so the classification is part of the
    /// library the tests link, and so the three facts a failure changes stay in one place.
    pub fn observe_error(&self, e: &crate::Error) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.up.store(0, Ordering::Relaxed);
        // Raised, never lowered here. A later attempt failing some other way says nothing
        // about the file: most of what an attempt does happens before the read that would
        // find it missing — reopening both tables, resolving the resume point, building the
        // batch — so a target that times out once, or a change commit further up the log,
        // would otherwise drop the alert while the stream is still stuck on the same thing.
        // Only a step that actually succeeded is evidence, and that clears it in
        // `mark_progress`. Erring towards a gauge that stays 1 on a pipeline which still
        // needs a human is the safe direction; erring the other way is a page that never
        // arrives.
        if matches!(e, crate::Error::SourceFileVacuumed { .. }) {
            self.source_file_vacuumed.store(1, Ordering::Relaxed);
        }
        // Raised, never lowered here, for the reason above and one more specific to these
        // two: both are raised while *opening*, which is the earliest thing an attempt does,
        // so a later attempt getting further and failing on something else is the normal way
        // a half-fixed pipeline behaves and says nothing about the log.
        if matches!(e, crate::Error::BootstrapUnreachable { .. }) {
            self.bootstrap_unreachable.store(1, Ordering::Relaxed);
        }
        if matches!(e, crate::Error::CursorUnavailable { .. }) {
            self.resume_unreachable.store(1, Ordering::Relaxed);
        }
        // Not the same thing as `up = 0`, which every failure sets. This one says the machine
        // ran out rather than the data being wrong — which is what decides whether an
        // operator reaches for the config or for the data.
        // Through the data-quality write's wrapper too, where a Kafka pipeline's rejects ran
        // out of room (docs/kafka-design.md §3.6); a Delta pipeline never raises one.
        if e.is_capacity() {
            self.capacity_exhausted.store(1, Ordering::Relaxed);
        }
        if let Some(kafka) = self.kafka.get() {
            kafka.observe_error(e);
        }
    }

    /// The Kafka series of this pipeline, made on first use. Only a Kafka pipeline asks.
    pub fn kafka(&self) -> Arc<KafkaMetrics> {
        self.kafka
            .get_or_init(|| {
                Arc::new(KafkaMetrics {
                    brokers_up: AtomicI64::new(1),
                    ..Default::default()
                })
            })
            .clone()
    }

    /// Seconds since this pipeline last made progress, or -1 before its first step.
    pub fn seconds_since_progress(&self) -> i64 {
        let last = self.last_progress_unixtime.load(Ordering::Relaxed);
        if last < 0 {
            return -1;
        }
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| (d.as_secs() as i64 - last).max(0))
            .unwrap_or(-1)
    }

    /// How far behind the source head this pipeline is, in commits.
    ///
    /// Counts commits *not yet consumed*: the cursor points at the next commit to read,
    /// so sitting on the head means one commit outstanding, and sitting past it means zero.
    pub fn lag(&self) -> i64 {
        let head = self.source_head_version.load(Ordering::Relaxed);
        let cursor = self.cursor_version.load(Ordering::Relaxed);
        if head < 0 || cursor < 0 {
            return 0;
        }
        (head - cursor + 1).max(0)
    }
}

/// name, prometheus type, help text, and how to read the value.
type MetricSpec = (
    &'static str,
    &'static str,
    &'static str,
    fn(&PipelineMetrics) -> i64,
);

#[derive(Clone, Default)]
pub struct Metrics {
    pipelines: Arc<RwLock<HashMap<String, Arc<PipelineMetrics>>>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pipeline(&self, name: &str) -> Arc<PipelineMetrics> {
        if let Some(m) = self.pipelines.read().unwrap().get(name) {
            return m.clone();
        }
        let mut w = self.pipelines.write().unwrap();
        w.entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(PipelineMetrics {
                    last_source_version: AtomicI64::new(-1),
                    source_head_version: AtomicI64::new(-1),
                    cursor_version: AtomicI64::new(-1),
                    last_progress_unixtime: AtomicI64::new(-1),
                    ..Default::default()
                })
            })
            .clone()
    }

    /// Render in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let map = self.pipelines.read().unwrap();
        let mut s = String::new();

        let metrics: [MetricSpec; 38] = [
            (
                "ddi_batches_committed_total",
                "counter",
                "Batches committed.",
                |m| m.batches_committed.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_rows_written_total",
                "counter",
                "Rows written to targets.",
                |m| m.rows_written.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_files_read_total",
                "counter",
                "Source data files read.",
                |m| m.files_read.load(Ordering::Relaxed) as i64,
            ),
            // A Kafka pipeline's step holds most of its errors in place, staying in its group:
            // those count here too.
            ("ddi_errors_total", "counter", "Step errors.", |m| {
                let held = m
                    .kafka
                    .get()
                    .map_or(0, |k| k.held_errors.load(Ordering::Relaxed));
                (m.errors.load(Ordering::Relaxed) + held) as i64
            }),
            (
                "ddi_commits_skipped_total",
                "counter",
                "Source commits consumed that produced no rows.",
                |m| m.commits_skipped.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_last_source_version",
                "gauge",
                "Last source version durably committed.",
                |m| m.last_source_version.load(Ordering::Relaxed),
            ),
            (
                "ddi_source_head_version",
                "gauge",
                "Source head version at the last poll.",
                |m| m.source_head_version.load(Ordering::Relaxed),
            ),
            (
                "ddi_source_lag_versions",
                "gauge",
                "Source commits behind head.",
                |m| m.lag(),
            ),
            (
                "ddi_pipeline_up",
                "gauge",
                "1 while this pipeline is streaming, 0 while it is backing off after a \
                 failure.",
                |m| m.up.load(Ordering::Relaxed),
            ),
            (
                "ddi_pipeline_config_valid",
                "gauge",
                "1 when this pipeline's configuration was accepted, 0 when it was held back \
                 at load and never started.",
                |m| m.config_valid.load(Ordering::Relaxed),
            ),
            (
                "ddi_pipeline_seconds_since_progress",
                "gauge",
                "Seconds since this pipeline last completed a step; -1 before its first. \
                 Unlike lag, this still moves when a pipeline fails while opening.",
                |m| m.seconds_since_progress(),
            ),
            (
                "ddi_source_file_vacuumed",
                "gauge",
                "1 while this pipeline is stopped on a source data file the object store no \
                 longer has. Cleared only by a step that succeeds, so it does not flap. 0 \
                 otherwise.",
                |m| m.source_file_vacuumed.load(Ordering::Relaxed),
            ),
            (
                "ddi_bootstrap_unreachable",
                "gauge",
                "1 while this pipeline has never committed and its starting_version is no \
                 longer in the source's commit log. Recoverable: set starting_version to a \
                 version the log still holds, accepting the gap. Cleared only by a step that \
                 succeeds. 0 otherwise.",
                |m| m.bootstrap_unreachable.load(Ordering::Relaxed),
            ),
            (
                "ddi_resume_unreachable",
                "gauge",
                "1 while this pipeline has committed before and the version it must resume at \
                 is no longer in the source's commit log. Not recoverable by configuration: \
                 rows this pipeline owed the target are gone and it needs a deliberate \
                 rebuild. Cleared only by a step that succeeds. 0 otherwise.",
                |m| m.resume_unreachable.load(Ordering::Relaxed),
            ),
            (
                "ddi_pipeline_restarts_total",
                "counter",
                "Times this pipeline was reopened after a failure.",
                |m| m.restarts.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_rows_rejected_total",
                "counter",
                "Rows the target would not take, written to the data-quality table instead.",
                |m| m.rows_rejected.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_rows_rejected_by_transform_total",
                "counter",
                "Rows the transform could not evaluate, written to the data-quality table \
                 instead. Also counted in ddi_rows_rejected_total.",
                |m| m.rows_rejected_by_transform.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_transform_reevaluations_total",
                "counter",
                "Runs of a transform beyond the first, spent finding the rows it could not \
                 evaluate.",
                |m| m.transform_reevaluations.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_batches_fully_rejected_total",
                "counter",
                "Batches where every row was rejected; usually an upstream schema change.",
                |m| m.batches_fully_rejected.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_rows_skipped_as_covered_total",
                "counter",
                "Rows dropped because the target was taken to hold them already, while a \
                 rebuild, a first start against a populated target, or a replaced source was \
                 being caught up.",
                |m| m.rows_skipped_as_covered.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_coverage_cutoff_active",
                "gauge",
                "1 while this pipeline is dropping rows the target is taken to hold already; 0 \
                 otherwise.",
                |m| m.coverage_cutoff_active.load(Ordering::Relaxed),
            ),
            (
                "ddi_upsert_rows_updated_total",
                "counter",
                "Stored rows replaced by a newer delivery of the same key.",
                |m| m.rows_updated.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_upsert_rows_inserted_total",
                "counter",
                "Rows inserted for a key the target did not hold.",
                |m| m.rows_inserted.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_upsert_target_files_scanned_total",
                "counter",
                "Target files opened by a merge. What the merge window bounds.",
                |m| m.target_files_scanned.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_upsert_window_unbounded_total",
                "counter",
                "Merges that had to read the whole target because its statistics could not \
                 bound the window.",
                |m| m.upsert_window_unbounded.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_upsert_window_clamped_total",
                "counter",
                "Merges where upsert_lookback held the window above what completeness \
                 required; each may have inserted a key alongside an older row.",
                |m| m.upsert_window_clamped.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_merges_total",
                "counter",
                "Merges started. The denominator for the two millisecond counters below.",
                |m| m.merges.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_merge_milliseconds_total",
                "counter",
                "Time spent inside merges, permit already in hand. Rising against a flat \
                 ddi_merges_total means the target, not the queue.",
                |m| m.merge_millis.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_merge_queue_milliseconds_total",
                "counter",
                "Time spent waiting for a merge permit. Rising means \
                 max_concurrent_upsert_merges is the throughput, not the storage.",
                |m| m.merge_queue_millis.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_publish_sent_total",
                "counter",
                "Realtime messages the service accepted. NOT a delivery count: a group \
                 with no subscribers accepts and discards.",
                |m| m.publish_sent.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_publish_failed_total",
                "counter",
                "Realtime payloads that could not be built or sent. The batch committed \
                 anyway — publishing cannot fail a commit.",
                |m| m.publish_failed.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_publish_skipped_total",
                "counter",
                "Batches where publication was not attempted because the breaker was open \
                 after repeated failures.",
                |m| m.publish_skipped.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_publish_truncated_total",
                "counter",
                "Messages sent without their rows because the payload was over the size \
                 cap. Each one makes a client reload a baseline.",
                |m| m.publish_truncated.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_publish_rows_total",
                "counter",
                "Rows across all realtime payloads built.",
                |m| m.publish_rows.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_publish_bytes_total",
                "counter",
                "Bytes across all realtime messages sent. Watch against the service's 1 MB \
                 frame limit.",
                |m| m.publish_bytes.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_capacity_exhausted",
                "gauge",
                "1 once this pipeline ran out of spill space or memory. Raised, never \
                 lowered; cleared only by a step that succeeds.",
                |m| m.capacity_exhausted.load(Ordering::Relaxed),
            ),
            (
                "ddi_grain_check_passes",
                "gauge",
                "Passes the last startup uniqueness check took over this target's key \
                 column. More than one means the key space did not fit \
                 [runtime] max_grain_check_memory.",
                |m| m.grain_check_passes.load(Ordering::Relaxed) as i64,
            ),
            (
                "ddi_publish_configured",
                "gauge",
                "1 when this pipeline has a realtime publisher, 0 when it does not.",
                |m| m.publish_configured.load(Ordering::Relaxed),
            ),
        ];

        for (name, kind, help, get) in metrics {
            s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
            for (pipeline, m) in map.iter() {
                s.push_str(&format!("{name}{{pipeline=\"{pipeline}\"}} {}\n", get(m)));
            }
        }

        // A Kafka pipeline's own families, after the ones every pipeline has, and only when
        // one exists: a Delta-only process renders exactly what it always did.
        let kafka: Vec<(&String, &Arc<KafkaMetrics>)> = map
            .iter()
            .filter_map(|(name, m)| m.kafka.get().map(|k| (name, k)))
            .collect();
        if !kafka.is_empty() {
            render_kafka(&mut s, &kafka);
        }

        // Process-wide, and therefore unlabelled: the limits in `crate::gate` are shared by
        // every pipeline, so attributing a queue to one of them would be a fiction. These
        // are what say whether the fleet is waiting on itself.
        let gate = crate::gate::current();
        s.push_str(&format!(
            "# HELP ddi_merges_in_flight Merges running right now, process-wide.\n\
             # TYPE ddi_merges_in_flight gauge\n\
             ddi_merges_in_flight {}\n\
             # HELP ddi_preflights_in_flight Startup uniqueness checks running right now.\n\
             # TYPE ddi_preflights_in_flight gauge\n\
             ddi_preflights_in_flight {}\n",
            gate.merges_in_flight(),
            gate.preflights_in_flight(),
        ));

        // Also process-wide, and for a harder reason than the gate's: the spill budget is one
        // directory with one counter, shared by every session this process builds. There is
        // no pipeline to attribute a byte of it to — and that is exactly why it needs a
        // gauge. A fleet can walk up to a shared budget together, and without this the first
        // anyone hears about it is a pipeline that will not start.
        let spill = crate::spill::current();
        s.push_str(&format!(
            "# HELP ddi_spill_bytes Bytes DataFusion currently holds in its temporary \
             directory, process-wide.\n\
             # TYPE ddi_spill_bytes gauge\n\
             ddi_spill_bytes {}\n\
             # HELP ddi_spill_files Spill files open right now, process-wide.\n\
             # TYPE ddi_spill_files gauge\n\
             ddi_spill_files {}\n\
             # HELP ddi_spill_limit_bytes The budget those two are measured against. Never \
             zero: unset means DataFusion's own 100GB.\n\
             # TYPE ddi_spill_limit_bytes gauge\n\
             ddi_spill_limit_bytes {}\n",
            spill.used_bytes(),
            spill.active_files(),
            spill.limit_bytes(),
        ));
        s
    }
}

/// A Kafka family read off one pipeline's [`KafkaMetrics`].
type KafkaSpec = (
    &'static str,
    &'static str,
    &'static str,
    fn(&KafkaMetrics) -> i64,
);

/// The Kafka families (docs/kafka-design.md §7.1), one block per family, a line per Kafka
/// pipeline (and per label value, where the family has one).
fn render_kafka(s: &mut String, pipelines: &[(&String, &Arc<KafkaMetrics>)]) {
    fn get(a: &AtomicU64) -> i64 {
        a.load(Ordering::Relaxed) as i64
    }
    let plain: [KafkaSpec; 31] = [
        ("ddi_kafka_messages_total", "counter", "Messages of committed batches, tombstones and undecodable ones included.", |k| get(&k.messages)),
        ("ddi_kafka_bytes_total", "counter", "Key and value bytes of those messages.", |k| get(&k.bytes)),
        ("ddi_kafka_undecodable_total", "counter", "Messages that could not be decoded, written to the data-quality table.", |k| get(&k.undecodable)),
        ("ddi_kafka_tombstones_total", "counter", "Messages without a value, skipped; their offsets advance.", |k| get(&k.tombstones)),
        ("ddi_kafka_offset_gaps_total", "counter", "Offsets a partition skipped between two messages: compaction or transaction markers.", |k| get(&k.offset_gaps)),
        ("ddi_kafka_commit_milliseconds_total", "counter", "Time spent committing batches to the target, the data-quality write included.", |k| get(&k.commit_millis)),
        ("ddi_kafka_files_added_total", "counter", "Data files the target commits added.", |k| get(&k.files_added)),
        ("ddi_kafka_bytes_added_total", "counter", "Bytes of those files.", |k| get(&k.bytes_added)),
        ("ddi_kafka_null_partition_values_total", "counter", "Rows whose target partition column was null: they land in __HIVE_DEFAULT_PARTITION__.", |k| get(&k.null_partition_values)),
        ("ddi_kafka_offset_lookups_total", "counter", "Reads of the target's offsets, however many partitions each asked about.", |k| k.lookups.lookups() as i64),
        ("ddi_kafka_offset_lookup_seconds_total", "counter", "Time spent in those reads.", |k| k.lookups.seconds() as i64),
        ("ddi_kafka_offset_lookup_fallbacks_total", "counter", "Offsets read one at a time through the kernel, because a checkpoint could not be read.", |k| k.lookups.fallbacks() as i64),
        ("ddi_kafka_offset_lookup_below_checkpoint_reads_total", "counter", "Commits read below a checkpoint for the marks it does not keep.", |k| k.lookups.below_checkpoint_reads() as i64),
        ("ddi_kafka_offset_lookup_unverifiable_total", "counter", "Partitions' offsets read without the mark that would verify their resume.", |k| k.lookups.unverifiable() as i64),
        ("ddi_kafka_dq_conflict_retries_total", "counter", "Data-quality writes tried again in place after a conflict.", |k| k.dq.conflict_retries() as i64),
        ("ddi_kafka_assigned_partitions", "gauge", "Partitions this member owns.", |k| k.assigned_partitions.load(Ordering::Relaxed)),
        ("ddi_kafka_blocked_partitions", "gauge", "Owned partitions paused until an operator acts: see the cause gauges and ddi status.", |k| k.blocked_partitions.load(Ordering::Relaxed)),
        ("ddi_kafka_lag_messages", "gauge", "Messages behind the end of the owned partitions, from librdkafka's statistics.", |k| k.lag_messages.load(Ordering::Relaxed)),
        ("ddi_kafka_lag_messages_max", "gauge", "The largest one partition's lag.", |k| k.lag_messages_max.load(Ordering::Relaxed)),
        ("ddi_kafka_retention_headroom_messages", "gauge", "Messages between the next one to read and the oldest the topic still holds, at the closest partition; at 0 retention overtakes this pipeline. -1 while unknown.", |k| k.retention_headroom_messages.load(Ordering::Relaxed)),
        ("ddi_kafka_oldest_buffered_seconds", "gauge", "Age of the oldest message buffered and not yet committed.", |k| k.oldest_buffered_seconds.load(Ordering::Relaxed)),
        ("ddi_kafka_brokers_up", "gauge", "0 while librdkafka reaches no broker.", |k| k.brokers_up.load(Ordering::Relaxed)),
        ("ddi_kafka_retrying", "gauge", "1 while a batch is held and retried after a transient failure; the member stays in its group.", |k| k.retrying.load(Ordering::Relaxed)),
        ("ddi_kafka_offsets_unavailable", "gauge", "1 while a partition's next offset has been removed by retention.", |k| k.offsets_unavailable.load(Ordering::Relaxed)),
        ("ddi_kafka_offsets_regressed", "gauge", "1 while a partition's stored offset is past the end of its topic.", |k| k.offsets_regressed.load(Ordering::Relaxed)),
        ("ddi_kafka_offset_missing", "gauge", "1 while the open is refused for partitions without an offset that the takeover check cannot account for.", |k| k.offset_missing.load(Ordering::Relaxed)),
        ("ddi_kafka_topic_replaced", "gauge", "1 while a partition's resume found another message at its stored offset than the one recorded.", |k| k.topic_replaced.load(Ordering::Relaxed)),
        ("ddi_kafka_undecodable_blocking", "gauge", "1 while undecodable messages hold the pipeline: no data-quality table, or more than the cap.", |k| k.undecodable_blocking.load(Ordering::Relaxed)),
        ("ddi_kafka_schema_conflict", "gauge", "1 while transform_sql no longer plans over the writer schemas.", |k| k.schema_conflict.load(Ordering::Relaxed)),
        ("ddi_kafka_target_columns_unmatched", "gauge", "Nullable target columns the last batch did not produce, written null.", |k| k.target_columns_unmatched.load(Ordering::Relaxed)),
        ("ddi_kafka_null_text_approximate", "gauge", "1 when kdi_null_text read where kafka-delta-ingest had a key off every writer schema, because the SQL dropped the schema id column.", |k| k.null_text_approximate.load(Ordering::Relaxed)),
    ];
    for (name, kind, help, read) in plain {
        s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
        for (pipeline, k) in pipelines {
            s.push_str(&format!("{name}{{pipeline=\"{pipeline}\"}} {}\n", read(k)));
        }
    }

    // name, type, help, the label's name, and each label value with its count.
    type Labelled = (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        fn(&KafkaMetrics) -> Vec<(String, i64)>,
    );
    let labelled: [Labelled; 8] = [
        ("ddi_kafka_messages_dropped_total", "counter", "Messages read and dropped, by why: not owned, its partition blocked, below the next offset, or the stored offset re-read to verify it.", "reason", |k| vec![
            ("not_owned".into(), k.dropped_not_owned.load(Ordering::Relaxed) as i64),
            ("blocked".into(), k.dropped_blocked.load(Ordering::Relaxed) as i64),
            ("duplicate".into(), k.dropped_duplicate.load(Ordering::Relaxed) as i64),
            ("reread".into(), k.dropped_reread.load(Ordering::Relaxed) as i64),
        ]),
        ("ddi_kafka_rebalances_total", "counter", "Rebalance events, by kind.", "kind", |k| vec![
            ("assign".into(), k.rebalances_assign.load(Ordering::Relaxed) as i64),
            ("revoke".into(), k.rebalances_revoke.load(Ordering::Relaxed) as i64),
            ("lost".into(), k.rebalances_lost.load(Ordering::Relaxed) as i64),
            ("error".into(), k.rebalances_error.load(Ordering::Relaxed) as i64),
        ]),
        ("ddi_kafka_revoke_flushes_total", "counter", "What happened to the buffer when partitions were revoked.", "outcome", |k| vec![
            ("committed".into(), k.revoke_flushes_committed.load(Ordering::Relaxed) as i64),
            ("dropped".into(), k.revoke_flushes_dropped.load(Ordering::Relaxed) as i64),
            ("timeout".into(), k.revoke_flushes_timeout.load(Ordering::Relaxed) as i64),
            ("none".into(), k.revoke_flushes_none.load(Ordering::Relaxed) as i64),
        ]),
        ("ddi_kafka_fenced_total", "counter", "Commits another writer of the same offsets got to first; above zero for long means a second deployment or a kafka-delta-ingest still running.", "kind", |k| vec![
            ("concurrent_txn".into(), k.fenced_concurrent_txn.load(Ordering::Relaxed) as i64),
            ("base_mismatch".into(), k.fenced_base_mismatch.load(Ordering::Relaxed) as i64),
            ("other_conflict".into(), k.fenced_other_conflict.load(Ordering::Relaxed) as i64),
        ]),
        ("ddi_kafka_partition_committed_offset", "gauge", "The last offset this member committed for each partition it owns; -1 before its first.", "partition", |k| {
            let p = k.partitions.lock().unwrap_or_else(|e| e.into_inner());
            p.iter().map(|(p, g)| (p.to_string(), g.committed)).collect()
        }),
        ("ddi_kafka_partition_lag_messages", "gauge", "Each owned partition's lag, from librdkafka's statistics; -1 while unknown.", "partition", |k| {
            let p = k.partitions.lock().unwrap_or_else(|e| e.into_inner());
            p.iter().map(|(p, g)| (p.to_string(), g.lag)).collect()
        }),
        ("ddi_kafka_pipeline_blocked", "gauge", "1 while the whole pipeline is held until an operator acts (or a target it cannot read comes back), staying in its group, with the reason; 0 for a reason it was held for before.", "reason", |k| {
            let m = k.pipeline_blocked.lock().unwrap_or_else(|e| e.into_inner());
            m.iter().map(|(r, v)| (r.clone(), *v)).collect()
        }),
        ("ddi_kafka_schema_registry_requests_total", "counter", "Schema Registry requests, by how they ended.", "outcome", |k| {
            let m = k.schema_registry_requests.lock().unwrap_or_else(|e| e.into_inner());
            m.iter().map(|(o, n)| (o.clone(), *n as i64)).collect()
        }),
    ];
    for (name, kind, help, label, read) in labelled {
        s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
        for (pipeline, k) in pipelines {
            for (value, n) in read(k) {
                s.push_str(&format!(
                    "{name}{{pipeline=\"{pipeline}\",{label}=\"{value}\"}} {n}\n"
                ));
            }
        }
    }
    let name = "ddi_kafka_client_errors_total";
    s.push_str(&format!(
        "# HELP {name} Errors librdkafka reported, by code.\n# TYPE {name} counter\n"
    ));
    for (pipeline, k) in pipelines {
        let m = k.client_errors.lock().unwrap_or_else(|e| e.into_inner());
        for (code, n) in m.iter() {
            s.push_str(&format!(
                "{name}{{pipeline=\"{pipeline}\",code=\"{code}\"}} {n}\n"
            ));
        }
    }
}

/// How long one `/metrics` connection may take, from accept to close.
const SCRAPE_DEADLINE: Duration = Duration::from_secs(30);
/// How long to wait for the scraper to close once the whole answer is sent.
const SCRAPE_LINGER: Duration = Duration::from_secs(5);
/// Request head read before answering anyway: every request gets the same answer.
const MAX_REQUEST_HEAD: usize = 16 * 1024;

/// Answers one `/metrics` connection: reads the request, writes the whole registry, and
/// waits for the scraper to close before closing.
///
/// Both reads matter although the answer never depends on the request. A socket closed
/// while bytes it received are still unread, or that receives more after the close, is
/// reset (RFC 1122 §4.2.2.13), and the reset discards whatever of the answer is still in
/// the send buffer. A body larger than the network takes at once was cut off that way
/// (#22: reset after 98,304 bytes of a 141,592-byte body).
pub async fn answer_scrape(mut sock: TcpStream, metrics: Metrics) {
    let _ = tokio::time::timeout(SCRAPE_DEADLINE, async {
        if !read_request_head(&mut sock).await {
            return;
        }
        let body = metrics.render();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        if sock.write_all(resp.as_bytes()).await.is_err() || sock.shutdown().await.is_err() {
            return;
        }
        // Our FIN follows the last byte; the scraper's follows its read of it. Anything it
        // sends until then is read and dropped, so that closing does not reset.
        let _ = tokio::time::timeout(SCRAPE_LINGER, async {
            let mut sink = [0u8; 4096];
            while matches!(sock.read(&mut sink).await, Ok(n) if n > 0) {}
        })
        .await;
    })
    .await;
}

/// Reads up to the end of the request head. False when the peer sent nothing and closed
/// (a TCP probe), so there is nobody to answer.
async fn read_request_head(sock: &mut TcpStream) -> bool {
    let mut head = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match sock.read(&mut buf).await {
            Ok(0) => return !head.is_empty(),
            Ok(n) => head.extend_from_slice(&buf[..n]),
            Err(_) => return false,
        }
        // A bare LF ends a line too (RFC 9112 §2.2), so a blank line is "\n\n" or "\n\r\n".
        let blank_line = |w: &[u8]| w.ends_with(b"\n\n") || w == b"\n\r\n";
        if head.len() >= MAX_REQUEST_HEAD || head.windows(3).any(blank_line) {
            return true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Delta pipeline's `/metrics` has no Kafka family at all (docs/kafka-design.md §1.3
    /// item 7); a Kafka pipeline's has every one, labelled where §7.1 labels it.
    #[test]
    fn kafka_series_are_rendered_only_for_kafka_pipelines() {
        let m = Metrics::new();
        m.pipeline("orders");
        assert!(!m.render().contains("ddi_kafka_"));

        let k = m.pipeline("events").kafka();
        k.messages.store(7, Ordering::Relaxed);
        k.dropped_reread.store(2, Ordering::Relaxed);
        k.set_partitions(BTreeMap::from([(
            3,
            PartitionGauges {
                committed: 41,
                lag: 0,
            },
        )]));
        KafkaMetrics::count(&k.client_errors, "AllBrokersDown");
        let text = m.render();
        assert!(
            text.contains("ddi_kafka_messages_total{pipeline=\"events\"} 7"),
            "{text}"
        );
        assert!(
            !text.contains("ddi_kafka_messages_total{pipeline=\"orders\"}"),
            "{text}"
        );
        assert!(
            text.contains(
                "ddi_kafka_messages_dropped_total{pipeline=\"events\",reason=\"reread\"} 2"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "ddi_kafka_partition_committed_offset{pipeline=\"events\",partition=\"3\"} 41"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "ddi_kafka_client_errors_total{pipeline=\"events\",code=\"AllBrokersDown\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains("ddi_kafka_brokers_up{pipeline=\"events\"} 1"),
            "{text}"
        );
    }

    /// An error a Kafka step held in place counts in `ddi_errors_total` as one the supervisor
    /// saw does, and a pipeline held whole says why, until it is not held any more. A Delta
    /// pipeline's count is its own.
    #[test]
    fn held_errors_count_and_a_held_pipeline_says_why() {
        let m = Metrics::new();
        m.pipeline("orders").errors.fetch_add(2, Ordering::Relaxed);
        let p = m.pipeline("events");
        let k = p.kafka();
        p.errors.fetch_add(1, Ordering::Relaxed);
        k.observe_held(&crate::Error::KafkaUndecodable {
            topic: "events".into(),
            partition: 0,
            offset: 7,
            reason: "no magic byte".into(),
            dq_uri: "/lake/events__ddi_dq".into(),
        });
        k.set_pipeline_blocked(Some("undecodable"));
        let text = m.render();
        assert!(
            text.contains("ddi_errors_total{pipeline=\"events\"} 2"),
            "{text}"
        );
        assert!(
            text.contains("ddi_errors_total{pipeline=\"orders\"} 2"),
            "{text}"
        );
        assert!(
            text.contains("ddi_kafka_undecodable_blocking{pipeline=\"events\"} 1"),
            "{text}"
        );
        assert!(
            text.contains(
                "ddi_kafka_pipeline_blocked{pipeline=\"events\",reason=\"undecodable\"} 1"
            ),
            "{text}"
        );
        k.set_pipeline_blocked(Some("target_replaced"));
        k.set_pipeline_blocked(None);
        let text = m.render();
        for reason in ["undecodable", "target_replaced"] {
            assert!(
                text.contains(&format!(
                    "ddi_kafka_pipeline_blocked{{pipeline=\"events\",reason=\"{reason}\"}} 0"
                )),
                "{text}"
            );
        }
    }

    #[test]
    fn pipeline_handles_are_stable() {
        let m = Metrics::new();
        let a = m.pipeline("orders");
        a.rows_written.fetch_add(5, Ordering::Relaxed);
        let b = m.pipeline("orders");
        assert_eq!(
            b.rows_written.load(Ordering::Relaxed),
            5,
            "same handle reused"
        );
    }

    #[test]
    fn lag_is_zero_before_the_first_commit() {
        let m = Metrics::new();
        assert_eq!(m.pipeline("p").lag(), 0);
    }

    #[test]
    fn lag_counts_commits_not_yet_consumed() {
        let m = Metrics::new();
        let p = m.pipeline("p");
        p.source_head_version.store(10, Ordering::Relaxed);
        // Next to read is v5, so v5..=v10 are outstanding.
        p.cursor_version.store(5, Ordering::Relaxed);
        assert_eq!(p.lag(), 6);
    }

    #[test]
    fn sitting_on_the_head_is_one_commit_of_lag() {
        let m = Metrics::new();
        let p = m.pipeline("p");
        p.source_head_version.store(7, Ordering::Relaxed);
        p.cursor_version.store(7, Ordering::Relaxed);
        assert_eq!(p.lag(), 1, "v7 exists and has not been read yet");
    }

    #[test]
    fn caught_up_is_zero_lag() {
        let m = Metrics::new();
        let p = m.pipeline("p");
        p.source_head_version.store(7, Ordering::Relaxed);
        p.cursor_version.store(8, Ordering::Relaxed);
        assert_eq!(p.lag(), 0);
    }

    #[test]
    fn lag_never_goes_negative() {
        let m = Metrics::new();
        let p = m.pipeline("p");
        p.source_head_version.store(3, Ordering::Relaxed);
        p.cursor_version.store(9, Ordering::Relaxed);
        assert_eq!(p.lag(), 0);
    }

    #[test]
    fn compaction_on_the_source_does_not_read_as_backlog() {
        // The reason lag is measured from the cursor rather than the durable offset: a run
        // of OPTIMIZE commits is consumed without writing a txn action, so the offset stays
        // put while the pipeline is genuinely caught up. Measuring from the offset would
        // page an operator for a source that is fully drained.
        let m = Metrics::new();
        let p = m.pipeline("p");
        p.source_head_version.store(10, Ordering::Relaxed);
        p.last_source_version.store(5, Ordering::Relaxed); // last real data commit
        p.cursor_version.store(11, Ordering::Relaxed); // v6..=v10 were compaction
        assert_eq!(p.lag(), 0);
    }

    #[test]
    fn render_emits_prometheus_format_with_labels() {
        let m = Metrics::new();
        let p = m.pipeline("orders_header");
        p.batches_committed.fetch_add(3, Ordering::Relaxed);
        let out = m.render();
        assert!(
            out.contains("# TYPE ddi_batches_committed_total counter"),
            "{out}"
        );
        assert!(
            out.contains("ddi_batches_committed_total{pipeline=\"orders_header\"} 3"),
            "{out}"
        );
    }

    #[test]
    fn a_publish_failure_does_not_clear_ddi_pipeline_up() {
        // The distinction the whole design rests on. A publisher is a leaf: a pipeline whose
        // every publish fails is committing to Delta exactly as it should, and paging
        // somebody about it would be paging them about a dashboard.
        let m = Metrics::new();
        let p = m.pipeline("orders");
        p.mark_progress();
        assert_eq!(p.up.load(Ordering::Relaxed), 1);

        for _ in 0..5 {
            p.observe_publish(&crate::publish::PublishStats {
                failed: true,
                ..Default::default()
            });
        }

        assert_eq!(p.up.load(Ordering::Relaxed), 1, "still healthy");
        assert_eq!(p.errors.load(Ordering::Relaxed), 0, "and not an error");
        assert_eq!(p.publish_failed.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn publish_counters_separate_sent_failed_skipped_and_truncated() {
        let m = Metrics::new();
        let p = m.pipeline("orders");

        p.observe_publish(&crate::publish::PublishStats {
            sent: true,
            rows: 3,
            bytes: 120,
            ..Default::default()
        });
        p.observe_publish(&crate::publish::PublishStats {
            sent: true,
            truncated: true,
            rows: 9_000,
            bytes: 400,
            ..Default::default()
        });
        p.observe_publish(&crate::publish::PublishStats {
            failed: true,
            ..Default::default()
        });
        // A run of failures becomes a run of skips once the breaker opens; conflating the
        // two would hide the moment publication stopped being attempted at all.
        p.observe_publish(&crate::publish::PublishStats {
            skipped: true,
            ..Default::default()
        });

        assert_eq!(p.publish_sent.load(Ordering::Relaxed), 2);
        assert_eq!(p.publish_failed.load(Ordering::Relaxed), 1);
        assert_eq!(p.publish_skipped.load(Ordering::Relaxed), 1);
        assert_eq!(p.publish_truncated.load(Ordering::Relaxed), 1);
        assert_eq!(p.publish_rows.load(Ordering::Relaxed), 9_003);
        assert_eq!(p.publish_bytes.load(Ordering::Relaxed), 520, "sent only");
    }

    #[test]
    fn the_publish_series_are_rendered_for_every_pipeline() {
        let m = Metrics::new();
        m.pipeline("orders")
            .publish_configured
            .store(1, Ordering::Relaxed);
        let rendered = m.render();
        for name in [
            "ddi_publish_sent_total",
            "ddi_publish_failed_total",
            "ddi_publish_skipped_total",
            "ddi_publish_truncated_total",
            "ddi_publish_rows_total",
            "ddi_publish_bytes_total",
            "ddi_publish_configured",
        ] {
            assert!(
                rendered.contains(name),
                "{name} is missing from:\n{rendered}"
            );
        }
        assert!(
            rendered.contains("ddi_publish_configured{pipeline=\"orders\"} 1"),
            "got:\n{rendered}"
        );
    }

    #[test]
    fn the_evaluation_series_are_rendered() {
        let m = Metrics::new();
        let p = m.pipeline("orders");
        let rendered = m.render();
        assert!(
            rendered.contains("ddi_rows_rejected_by_transform_total{pipeline=\"orders\"} 0"),
            "got:\n{rendered}"
        );
        assert!(
            rendered.contains("ddi_transform_reevaluations_total{pipeline=\"orders\"} 0"),
            "got:\n{rendered}"
        );
        p.rows_rejected_by_transform.fetch_add(3, Ordering::Relaxed);
        p.transform_reevaluations.fetch_add(40, Ordering::Relaxed);
        let rendered = m.render();
        assert!(rendered.contains("ddi_rows_rejected_by_transform_total{pipeline=\"orders\"} 3"));
        assert!(rendered.contains("ddi_transform_reevaluations_total{pipeline=\"orders\"} 40"));
    }

    #[test]
    fn the_coverage_series_are_rendered() {
        let m = Metrics::new();
        let p = m.pipeline("orders");
        let rendered = m.render();
        assert!(
            rendered.contains("# TYPE ddi_rows_skipped_as_covered_total counter"),
            "got:\n{rendered}"
        );
        assert!(
            rendered.contains("# TYPE ddi_coverage_cutoff_active gauge"),
            "got:\n{rendered}"
        );
        assert!(rendered.contains("ddi_coverage_cutoff_active{pipeline=\"orders\"} 0"));

        p.rows_skipped_as_covered.fetch_add(7, Ordering::Relaxed);
        p.coverage_cutoff_active.store(1, Ordering::Relaxed);
        let rendered = m.render();
        assert!(
            rendered.contains("ddi_rows_skipped_as_covered_total{pipeline=\"orders\"} 7"),
            "got:\n{rendered}"
        );
        assert!(
            rendered.contains("ddi_coverage_cutoff_active{pipeline=\"orders\"} 1"),
            "got:\n{rendered}"
        );
    }

    #[test]
    fn a_pipeline_that_does_not_publish_reads_zero_rather_than_absent() {
        // So a dashboard can tell "not configured" from "configured and silent".
        let m = Metrics::new();
        let _ = m.pipeline("orders");
        let rendered = m.render();
        assert!(
            rendered.contains("ddi_publish_configured{pipeline=\"orders\"} 0"),
            "got:\n{rendered}"
        );
        assert!(rendered.contains("ddi_publish_sent_total{pipeline=\"orders\"} 0"));
    }

    /// A registry far past what fits in the network at once, served the way the binary
    /// serves it: a task per accepted connection.
    async fn serve_large_registry() -> (std::net::SocketAddr, String) {
        let m = Metrics::new();
        for i in 0..200 {
            m.pipeline(&format!("a_pipeline_with_a_long_name_{i:03}"));
        }
        let body = m.render();
        assert!(body.len() > 256 * 1024, "only {} bytes", body.len());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                tokio::spawn(answer_scrape(sock, m.clone()));
            }
        });
        (addr, body)
    }

    /// Sends `request`, then `more` once the server has had time to answer, and reads
    /// until the server closes. A small receive buffer and a late first read keep most of
    /// the answer in the server's send buffer when it closes, which is where a reset
    /// discards it.
    async fn scrape(addr: std::net::SocketAddr, request: &[u8], more: &[u8]) -> Vec<u8> {
        let s = tokio::net::TcpSocket::new_v4().unwrap();
        s.set_recv_buffer_size(16 * 1024).unwrap();
        let mut c = s.connect(addr).await.unwrap();
        c.write_all(request).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !more.is_empty() {
            c.write_all(more).await.unwrap();
        }
        let mut got = Vec::new();
        let read = c.read_to_end(&mut got).await;
        assert!(read.is_ok(), "{read:?} after {} bytes", got.len());
        got
    }

    fn body_of(response: &[u8]) -> &str {
        let text = std::str::from_utf8(response).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").unwrap();
        let length: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            body.len(),
            length,
            "Content-Length {length}, got {}",
            body.len()
        );
        body
    }

    /// #22: past ~96KB the answer was cut off by a reset, because the request was never
    /// read and a socket closed with unread input is reset, dropping its unsent output.
    #[tokio::test]
    async fn a_large_registry_is_scraped_whole_every_time() {
        let (addr, expected) = serve_large_registry().await;
        for attempt in 0..50 {
            let got = scrape(addr, b"GET /metrics HTTP/1.1\r\nHost: ddi\r\n\r\n", b"").await;
            assert!(body_of(&got) == expected, "attempt {attempt}: body differs");
        }
    }

    /// Bytes that arrive after the request (a pipelined request; HTTP/1.1 allows it) are
    /// unread input too, and must not reset the connection either.
    #[tokio::test]
    async fn bytes_after_the_request_do_not_cut_the_answer_off() {
        let (addr, expected) = serve_large_registry().await;
        for attempt in 0..10 {
            let got = scrape(
                addr,
                b"GET /metrics HTTP/1.1\r\nHost: ddi\r\n\r\n",
                b"GET /metrics HTTP/1.1\r\nHost: ddi\r\n\r\n",
            )
            .await;
            assert!(body_of(&got) == expected, "attempt {attempt}: body differs");
        }
    }
}
