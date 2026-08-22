//! Process metrics, exported in Prometheus text format.
//!
//! Deliberately dependency-free: a handful of atomic counters and a tiny HTTP responder.
//! Pulling in a metrics framework for six counters would be the opposite of the "thin
//! daemon" design.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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
    /// Batches where *every* row was rejected. Far more likely an upstream schema change
    /// than data going bad, and otherwise invisible: the target just stops growing.
    pub batches_fully_rejected: AtomicU64,
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
        // Not the same thing as `up = 0`, which every failure sets. This one says the machine
        // ran out rather than the data being wrong — which is what decides whether an
        // operator reaches for the config or for the data.
        if matches!(e, crate::Error::Capacity(_)) {
            self.capacity_exhausted.store(1, Ordering::Relaxed);
        }
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

        let metrics: [MetricSpec; 32] = [
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
            ("ddi_errors_total", "counter", "Step errors.", |m| {
                m.errors.load(Ordering::Relaxed) as i64
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
                "ddi_batches_fully_rejected_total",
                "counter",
                "Batches where every row was rejected; usually an upstream schema change.",
                |m| m.batches_fully_rejected.load(Ordering::Relaxed) as i64,
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
             # HELP ddi_spill_files Spill files open right now, process-wide. Zero here with \
             ddi_spill_bytes above zero means the counter is carrying residue from a capacity \
             failure; a restart clears it.\n\
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
