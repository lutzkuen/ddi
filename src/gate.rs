//! How many expensive things this process does to targets at once.
//!
//! [`crate::budget`] bounds how much memory one pipeline may use. That is the right bound
//! for the work a pipeline does to *itself* — decoding a batch, planning a transform — and
//! the wrong one for the work it does to a **target**, because the target work is where
//! pipelines stop being independent. A merge reads back a slice of the table it writes, and
//! a startup uniqueness check reads all of it. Both are proportional to the target rather
//! than to the batch, so neither gets smaller when batches do.
//!
//! With hundreds of pipelines starting together, "each one is within its budget" says
//! nothing useful about the process: every one of them is scanning a target at the same
//! instant, on the one morning they are all furthest behind. That is the shape every memory
//! incident here has had, and dividing the budget more finely does not fix it — it only
//! makes each pipeline spill sooner while the same number of scans run concurrently.
//!
//! So this bounds the *count* instead. A merge waits for a permit, and the wait is measured:
//! queue time is the signal that the limit is too low, exactly as spill rate is the signal
//! that the memory budget is. Without that number a queue is indistinguishable from a stall.
//!
//! # Why merges and preflights are counted separately
//!
//! They overlap in time but not in kind. Preflights all happen at once, at startup, and then
//! never again; merges happen forever, at whatever rate commits arrive. One limit covering
//! both would have to be set for the startup burst and would then throttle steady state for
//! the rest of the run.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A permit to do one expensive thing to a target, and what waiting for it cost.
///
/// The permit is released on drop, so a merge that fails or panics does not leak one — which
/// matters more than it sounds, since the thing being counted is the thing most likely to
/// fail under memory pressure.
pub struct Pass {
    _permit: Option<OwnedSemaphorePermit>,
    in_flight: Arc<AtomicI64>,
    /// How long this caller waited for the permit.
    pub queued: std::time::Duration,
}

impl Drop for Pass {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The process-wide limits.
pub struct Gate {
    merges: Option<Arc<Semaphore>>,
    preflights: Option<Arc<Semaphore>>,
    merges_in_flight: Arc<AtomicI64>,
    preflights_in_flight: Arc<AtomicI64>,
    /// Total nanoseconds callers have spent waiting on a merge permit. A counter rather
    /// than a gauge: a rate over it is "seconds of waiting per second", which is the
    /// number of merges permanently queued.
    merge_queue_nanos: AtomicU64,
}

impl Gate {
    /// `None` for either limit means unbounded, which is what a single-pipeline local run is.
    pub fn new(max_merges: Option<usize>, max_preflights: Option<usize>) -> Self {
        Self {
            merges: max_merges.map(|n| Arc::new(Semaphore::new(n.max(1)))),
            preflights: max_preflights.map(|n| Arc::new(Semaphore::new(n.max(1)))),
            merges_in_flight: Arc::new(AtomicI64::new(0)),
            preflights_in_flight: Arc::new(AtomicI64::new(0)),
            merge_queue_nanos: AtomicU64::new(0),
        }
    }

    pub fn unbounded() -> Self {
        Self::new(None, None)
    }

    async fn pass(
        sem: &Option<Arc<Semaphore>>,
        in_flight: &Arc<AtomicI64>,
        record: Option<&AtomicU64>,
    ) -> Pass {
        let started = Instant::now();
        let permit = match sem {
            // `acquire_owned` cannot fail here: nothing ever closes these semaphores, and
            // they live as long as the process. Treating a close as "unbounded" rather than
            // panicking keeps a shutdown race from taking a pipeline down with it.
            Some(s) => s.clone().acquire_owned().await.ok(),
            None => None,
        };
        let queued = started.elapsed();
        if let Some(c) = record {
            c.fetch_add(queued.as_nanos() as u64, Ordering::Relaxed);
        }
        in_flight.fetch_add(1, Ordering::Relaxed);
        Pass {
            _permit: permit,
            in_flight: in_flight.clone(),
            queued,
        }
    }

    /// Wait for permission to run one merge.
    pub async fn merge(&self) -> Pass {
        Self::pass(
            &self.merges,
            &self.merges_in_flight,
            Some(&self.merge_queue_nanos),
        )
        .await
    }

    /// Wait for permission to run one startup uniqueness check.
    ///
    /// Not recorded in the queue-time counter: every pipeline queues here once, at startup,
    /// and folding that predictable burst into the steady-state signal would bury it.
    pub async fn preflight(&self) -> Pass {
        Self::pass(&self.preflights, &self.preflights_in_flight, None).await
    }

    pub fn merges_in_flight(&self) -> i64 {
        self.merges_in_flight.load(Ordering::Relaxed)
    }

    pub fn preflights_in_flight(&self) -> i64 {
        self.preflights_in_flight.load(Ordering::Relaxed)
    }

    /// Total seconds spent waiting for merge permits, as a float for Prometheus.
    pub fn merge_queue_seconds(&self) -> f64 {
        self.merge_queue_nanos.load(Ordering::Relaxed) as f64 / 1e9
    }
}

static INSTALLED: OnceLock<Arc<Gate>> = OnceLock::new();

/// Install the process's limits. The first call wins; later ones are ignored.
///
/// Same shape as [`crate::budget::install`], and for the same reason: the limit is a
/// property of the process, and threading it through every call site would only create
/// opportunities for two of them to disagree.
pub fn install(gate: Gate) {
    let _ = INSTALLED.set(Arc::new(gate));
}

/// The process's limits, unbounded if none were installed.
pub fn current() -> Arc<Gate> {
    INSTALLED
        .get_or_init(|| Arc::new(Gate::unbounded()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_unbounded_gate_never_waits() {
        let g = Gate::unbounded();
        let a = g.merge().await;
        let b = g.merge().await;
        let c = g.merge().await;
        assert_eq!(g.merges_in_flight(), 3);
        drop((a, b, c));
        assert_eq!(g.merges_in_flight(), 0);
    }

    #[tokio::test]
    async fn a_permit_is_returned_even_when_the_merge_panics() {
        // The thing being counted is the thing most likely to die under memory pressure, so
        // a leaked permit would shrink the limit every time it mattered most.
        let g = Arc::new(Gate::new(Some(1), None));
        let g2 = g.clone();
        let _ = tokio::spawn(async move {
            let _pass = g2.merge().await;
            panic!("merge died");
        })
        .await;
        assert_eq!(g.merges_in_flight(), 0);
        // Still acquirable, which is the property that matters.
        let _pass = tokio::time::timeout(std::time::Duration::from_secs(5), g.merge())
            .await
            .expect("the permit was returned");
    }

    #[tokio::test]
    async fn a_second_merge_waits_for_the_first() {
        let g = Arc::new(Gate::new(Some(1), None));
        let held = g.merge().await;
        assert_eq!(g.merges_in_flight(), 1);

        let g2 = g.clone();
        let waiter = tokio::spawn(async move { g2.merge().await });

        // It cannot proceed while the first is held.
        let early = tokio::time::timeout(std::time::Duration::from_millis(50), async {
            tokio::task::yield_now().await;
        })
        .await;
        assert!(early.is_ok());
        assert_eq!(g.merges_in_flight(), 1, "the second is still queued");

        drop(held);
        let pass = waiter.await.expect("the waiter ran");
        assert_eq!(g.merges_in_flight(), 1);
        drop(pass);
        assert_eq!(g.merges_in_flight(), 0);
    }

    #[tokio::test]
    async fn waiting_is_measured() {
        let g = Arc::new(Gate::new(Some(1), None));
        let held = g.merge().await;
        let uncontended = g.merge_queue_seconds();
        assert!(
            uncontended < 0.001,
            "an uncontended permit costs the acquire and nothing else, got {uncontended}"
        );

        let g2 = g.clone();
        let waiter = tokio::spawn(async move { g2.merge().await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(held);
        let _pass = waiter.await.expect("the waiter ran");

        assert!(
            g.merge_queue_seconds() >= 0.015,
            "queue time is the signal that the limit is too low; without it a queue reads \
             as a stall. got {}",
            g.merge_queue_seconds()
        );
    }

    #[tokio::test]
    async fn preflights_and_merges_do_not_share_a_limit() {
        // A startup burst must not be able to starve steady-state merges.
        let g = Gate::new(Some(1), Some(1));
        let _m = g.merge().await;
        let _p = tokio::time::timeout(std::time::Duration::from_secs(5), g.preflight())
            .await
            .expect("a preflight does not wait behind a merge");
        assert_eq!(g.merges_in_flight(), 1);
        assert_eq!(g.preflights_in_flight(), 1);
    }

    #[tokio::test]
    async fn a_zero_limit_is_read_as_one_rather_than_a_deadlock() {
        let g = Gate::new(Some(0), None);
        let _pass = tokio::time::timeout(std::time::Duration::from_secs(5), g.merge())
            .await
            .expect("0 would otherwise mean 'never run a merge again'");
    }
}
