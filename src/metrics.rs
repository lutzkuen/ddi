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
}

impl PipelineMetrics {
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
                    ..Default::default()
                })
            })
            .clone()
    }

    /// Render in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let map = self.pipelines.read().unwrap();
        let mut s = String::new();

        let metrics: [MetricSpec; 8] = [
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
