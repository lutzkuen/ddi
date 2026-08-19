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
    /// Unix seconds at the last successful step. -1 until the first one.
    ///
    /// The lag gauge cannot cover a pipeline that fails while *opening*: it never reaches
    /// the code that records head and cursor, so both keep the values they had when it was
    /// healthy and the backlog alert stays quiet while the stream is hours dead. Staleness
    /// is the reading that survives that, whatever the failure was.
    pub last_progress_unixtime: AtomicI64,
}

impl PipelineMetrics {
    /// Note that this pipeline just did something successfully.
    pub fn mark_progress(&self) {
        self.up.store(1, Ordering::Relaxed);
        // A step that succeeded read whatever it needed, so whatever was missing is not
        // missing any more. Cleared here rather than on reopen: reopening succeeds even
        // while the file is still gone, so clearing there would flap the gauge once per
        // retry instead of holding it up until the stream actually recovers.
        self.source_file_vacuumed.store(0, Ordering::Relaxed);
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

        let metrics: [MetricSpec; 20] = [
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
        ];

        for (name, kind, help, get) in metrics {
            s.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
            for (pipeline, m) in map.iter() {
                s.push_str(&format!("{name}{{pipeline=\"{pipeline}\"}} {}\n", get(m)));
            }
        }
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
}
