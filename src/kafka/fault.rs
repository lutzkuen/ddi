//! Faults the Kafka integration tests inject into a running `ddi` (docs/kafka-design.md §9.2).
//!
//! Read from `DDI_TEST_FAULT`, a comma-separated list of `<name>[:<arg>]`, and compiled only
//! under `cfg(any(test, feature = "fault-injection"))`: a release build has none of them, and
//! every hook below is then an empty function the compiler removes. A test that needs one
//! runs `ddi` built with `--features kafka,fault-injection`.
//!
//! - `sleep_before_commit:<secs>`: wait before every target commit, as a member that has
//!   overrun `max.poll.interval.ms` does (`zombie_after_poll_interval`).
//! - `stall_commit:<secs>`: the same, named for the revoke flush it outlasts
//!   (`revoke_flush_timeout_drops`).
//! - `abort_after_dq`: abort the process once a batch's rejects are in the data-quality table
//!   and before its target commit (`dq_crash_between_dq_and_target`).
//! - `fail_after_commit`: report the first target commit that lands as failed, as a lost
//!   response to its PUT would (`ambiguous_commit_error_no_duplicate`).
//! - `handle_max_age:<secs>`: refresh a handle older than this before a commit, instead of
//!   after ten minutes (`handle_aged_past_log_retention`, tests/kafka_aging.rs).
//! - `fail_seek:<n>`: fail the first `n` seeks of paused partitions as a seek that timed out
//!   does (`a_seek_that_times_out_is_tried_again`).

use std::time::Duration;

#[cfg(any(test, feature = "fault-injection"))]
mod active {
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::Duration;

    /// The argument of fault `name`, `Some("")` when it has none, `None` when it is not set.
    fn arg(name: &str) -> Option<String> {
        let all = std::env::var("DDI_TEST_FAULT").ok()?;
        all.split(',').find_map(|f| {
            let (n, a) = f.trim().split_once(':').unwrap_or((f.trim(), ""));
            (n == name).then(|| a.to_string())
        })
    }

    fn secs(name: &str) -> Option<Duration> {
        arg(name)
            .and_then(|a| a.parse::<f64>().ok())
            .map(Duration::from_secs_f64)
    }

    pub(super) async fn before_commit() {
        for name in ["sleep_before_commit", "stall_commit"] {
            if let Some(d) = secs(name) {
                tracing::warn!(fault = name, secs = d.as_secs_f64(), "fault injected");
                tokio::time::sleep(d).await;
            }
        }
    }

    pub(super) fn after_dq() {
        if arg("abort_after_dq").is_some() {
            tracing::warn!(fault = "abort_after_dq", "fault injected: aborting");
            std::process::abort();
        }
    }

    pub(super) fn fail_after_commit() -> bool {
        static FIRED: AtomicBool = AtomicBool::new(false);
        arg("fail_after_commit").is_some() && !FIRED.swap(true, Ordering::SeqCst)
    }

    pub(super) fn handle_max_age() -> Option<Duration> {
        secs("handle_max_age")
    }

    pub(super) fn fail_seek() -> bool {
        static FAILED: AtomicU32 = AtomicU32::new(0);
        let Some(n) = arg("fail_seek").and_then(|a| a.parse::<u32>().ok()) else {
            return false;
        };
        let fired = FAILED
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| {
                (f < n).then_some(f + 1)
            })
            .is_ok();
        if fired {
            tracing::warn!(fault = "fail_seek", "fault injected");
        }
        fired
    }
}

/// Before a Kafka batch's target commit.
pub(crate) async fn before_commit() {
    #[cfg(any(test, feature = "fault-injection"))]
    active::before_commit().await;
}

/// After a Kafka batch's rejects are written, before its target commit.
pub(crate) fn after_dq() {
    #[cfg(any(test, feature = "fault-injection"))]
    active::after_dq();
}

/// After a target commit landed: whether to report it as failed anyway, once.
pub(crate) fn fail_after_commit() -> bool {
    #[cfg(any(test, feature = "fault-injection"))]
    return active::fail_after_commit();
    #[cfg(not(any(test, feature = "fault-injection")))]
    false
}

/// Before a seek of paused partitions: whether to fail it, as one that timed out.
pub(crate) fn fail_seek() -> bool {
    #[cfg(any(test, feature = "fault-injection"))]
    return active::fail_seek();
    #[cfg(not(any(test, feature = "fault-injection")))]
    false
}

/// How old a handle may be before a commit refreshes it first: ten minutes (EO-m8).
pub(crate) fn handle_max_age() -> Duration {
    const TEN_MINUTES: Duration = Duration::from_secs(600);
    #[cfg(any(test, feature = "fault-injection"))]
    return active::handle_max_age().unwrap_or(TEN_MINUTES);
    #[cfg(not(any(test, feature = "fault-injection")))]
    TEN_MINUTES
}
