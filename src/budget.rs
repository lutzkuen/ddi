//! One memory number, divided across the pipelines that share the process.
//!
//! Every memory incident this tool has had was the same shape: an allocation proportional
//! to something unbounded, meeting a container limit, with nothing in between. Each was
//! fixed by making one allocation smaller, and none was noticed until it killed the
//! process — which takes the other pipelines and the metrics with it, so the evidence dies
//! with the thing that would explain it.
//!
//! A budget converts that class into the failure mode this tool chose everywhere else: one
//! pipeline stopping, loudly, while the rest keep running.
//!
//! # What the budget is
//!
//! `[runtime] max_memory`, or the container's own limit if it has one, whichever is
//! smaller. Divided by the number of pipelines, because they all start at once and it is
//! that simultaneity that turns a survivable allocation into an OOM.
//!
//! # What it covers, and what that cost
//!
//! Two things, and they are not the same mechanism:
//!
//! - **DataFusion**, through the [`MemoryPool`] every session this tool creates is built
//!   with. Those consumers spill rather than grow, so the budget is a throttle, not a
//!   verdict.
//! - **The batch**, through [`Self::bytes_per_batch`]. This one matters more, and it is the
//!   part no memory pool would have caught: reading a batch decodes parquet into Arrow
//!   outside any pool, and `max_bytes_per_batch` counts *compressed bytes on disk*. A
//!   measured ~5x between the two is the difference between a 256 MB setting and 1.4 GB
//!   resident, per pipeline, on the first batch after a cold start — which is when every
//!   pipeline is furthest behind and asking for the most.
//!
//! The ratio is measured rather than assumed. Guessing it would be one more constant to get
//! wrong twice.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use deltalake::datafusion::common::config::SpillCompression;
use deltalake::datafusion::execution::memory_pool::{
    FairSpillPool, MemoryPool, UnboundedMemoryPool,
};
use deltalake::datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use deltalake::datafusion::prelude::SessionConfig;
use deltalake::DeltaTable;

use crate::error::{Error, Result};

/// What one pipeline may use, and the pool that holds DataFusion to it.
#[derive(Debug, Clone)]
pub struct Budget {
    /// `None` when nothing bounds this process, which is what an unconfigured local run is.
    per_pipeline: Option<u64>,
}

static INSTALLED: OnceLock<Budget> = OnceLock::new();

/// The share of a *container's* limit this process will use.
///
/// Not all of it: the allocator's free lists, the runtime's stacks and the object store's
/// buffers are real and outside anything measured here, and a budget that assumed the whole
/// limit would be a budget that OOMs on the way to reporting that it was exceeded.
const SHARE_OF_CGROUP: f64 = 0.75;

/// The fraction of a pipeline's share the *batch* may occupy.
///
/// The rest is what a pipeline holds besides the batch it is working on — the target's
/// watermark, the merge's plan, the sink's write buffers, and DataFusion's pool.
const BATCH_SHARE: f64 = 0.5;

/// Bytes per batch before anything has been measured.
///
/// The first batch of a cold pipeline is the largest one it will ever ask for, and it is
/// also the one there is no measurement for yet. Starting low and letting the measurement
/// raise it is the only order that cannot OOM on the way to learning the ratio.
const ASSUMED_AMPLIFICATION: f64 = 8.0;

impl Budget {
    /// Work out the budget once, from configuration and from the container.
    ///
    /// `configured` wins when it is smaller; otherwise the container's limit is used, so a
    /// deployment that sets neither still gets a bound and one that sets both gets the
    /// tighter. `pipelines` is what it is divided by.
    pub fn resolve(configured: Option<u64>, pipelines: usize) -> Self {
        let from_cgroup = cgroup_limit().map(|l| (l as f64 * SHARE_OF_CGROUP) as u64);
        let total = match (configured, from_cgroup) {
            (Some(c), Some(g)) => Some(c.min(g)),
            (Some(c), None) => Some(c),
            (None, g) => g,
        };
        Self {
            per_pipeline: total.map(|t| t / (pipelines.max(1) as u64)),
        }
    }

    /// Nothing bounds this process.
    pub fn unbounded() -> Self {
        Self { per_pipeline: None }
    }

    pub fn per_pipeline(&self) -> Option<u64> {
        self.per_pipeline
    }

    /// Make this the budget every session in this process is built against.
    ///
    /// Once, at startup, before any pipeline opens. A second call is ignored rather than
    /// panicking: the budget is advice about size, and losing a race over it should not
    /// take down a process that is otherwise fine.
    pub fn install(self) {
        let _ = INSTALLED.set(self);
    }

    /// A pool holding DataFusion to this pipeline's share.
    ///
    /// `FairSpillPool` rather than `GreedyMemoryPool` because the consumers that matter
    /// here — a grouped aggregate, a sort, a merge's join — can all spill, and a fair share
    /// between them degrades where a greedy one fails.
    pub fn pool(&self) -> Arc<dyn MemoryPool> {
        match self.per_pipeline {
            Some(b) => Arc::new(FairSpillPool::new(b as usize)),
            None => Arc::new(UnboundedMemoryPool::default()),
        }
    }

    /// How many bytes of parquet a batch may name, given what decoding it last cost.
    ///
    /// `None` when nothing bounds the process, in which case the configured
    /// `max_bytes_per_batch` stands as it always did.
    pub fn bytes_per_batch(&self, amplification: f64) -> Option<u64> {
        let share = (self.per_pipeline? as f64) * BATCH_SHARE;
        Some((share / amplification.max(1.0)) as u64)
    }
}

/// The budget in force, or an unbounded one if nobody installed it.
pub fn current() -> Budget {
    INSTALLED.get().cloned().unwrap_or_else(Budget::unbounded)
}

/// True when this process is inside a container that limits its memory.
///
/// Only used to decide whether a missing spill cap is worth a warning: on a workstation
/// DataFusion's 100 GB default is fine, and in a pod it is larger than the ephemeral-storage
/// limit that will kill the process first.
pub fn in_a_container() -> bool {
    cgroup_limit().is_some()
}

/// A DataFusion runtime holding this pipeline's share of memory, and the process's share of
/// disk.
///
/// Two budgets, two lifetimes, and the asymmetry is the point. The memory pool is built fresh
/// here, per operation, because memory is reclaimed when the operation ends and a
/// per-operation pool is the honest shape for it. The disk manager is *not* built here — it is
/// cloned out of [`crate::spill`], so every runtime in this process counts its spill into one
/// atomic. Build one per operation and each gets its own hundred-gigabyte allowance, which is
/// what this process did before and what evicted a pod.
///
/// **Do not add `.with_temp_file_path(..)` or `.with_max_temp_directory_size(..)` to this
/// chain.** Both of those set `disk_manager_builder`, and `RuntimeEnvBuilder::build` *prefers*
/// that over the shared manager — so either call silently constructs a new `DiskManager` and
/// restores exactly the bug this exists to remove. The place to set them is
/// [`crate::spill::Spill::resolve`], once.
pub fn runtime() -> Result<Arc<deltalake::datafusion::execution::runtime_env::RuntimeEnv>> {
    crate::spill::current()
        .runtime_builder()
        .with_memory_pool(current().pool())
        .build_arc()
        .map_err(|e| Error::Other(format!("cannot build a bounded session: {e}")))
}

/// What every session in this process agrees on, before a caller adds its own settings.
///
/// `Lz4Frame` and not `Zstd`: `datafusion-common` declares `arrow-ipc` with the `lz4` feature
/// and default features off, so lz4 is guaranteed by our own dependency graph. `zstd` is
/// active only by incidental feature unification with some other crate in the tree, which an
/// unrelated bump could remove — turning the spill codec into a runtime error on the first
/// spill, which is the worst possible place for a change about spilling to fail.
pub fn session_config() -> SessionConfig {
    let mut c = SessionConfig::new();
    // DataFusion's default is `Uncompressed`. Every byte written here is charged against the
    // process's one disk budget, and against the pod's ephemeral storage underneath it, so
    // trading a little CPU for a smaller footprint is the right way round for this tool.
    c.options_mut().execution.spill_compression = SpillCompression::Lz4Frame;
    c
}

/// A DataFusion session bounded by the budget, able to read `table`.
///
/// The object-store registration is not optional: a session this tool builds itself does
/// not carry the one delta-rs would have registered, and a scan through it would fail to
/// find the table's files.
pub fn session(table: &DeltaTable) -> Result<SessionState> {
    session_with(table, session_config())
}

/// As [`session`], with plan-level settings only the caller knows it needs.
pub fn session_with(table: &DeltaTable, config: SessionConfig) -> Result<SessionState> {
    let runtime = runtime()?;

    let log_store = table.log_store();
    let url = log_store.root_url().clone();
    runtime.register_object_store(&url, log_store.root_object_store(None));

    Ok(SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .build())
}

// ---------------------------------------------------------------- what the container says

/// The memory limit of the cgroup this process is in, if it is in a limited one.
///
/// Both cgroup versions, because a container runtime picks one and the process does not get
/// a say. `"max"` and the v1 sentinel both mean unlimited, which is not a budget.
fn cgroup_limit() -> Option<u64> {
    const V1_UNLIMITED: u64 = 0x7FFF_FFFF_FFFF_F000;

    let read = |p: &str| std::fs::read_to_string(p).ok();
    let parse = |s: String| s.trim().parse::<u64>().ok();

    if let Some(s) = read("/sys/fs/cgroup/memory.max") {
        if s.trim() == "max" {
            return None;
        }
        return parse(s);
    }
    let v1 = parse(read("/sys/fs/cgroup/memory/memory.limit_in_bytes")?)?;
    (v1 < V1_UNLIMITED).then_some(v1)
}

// ------------------------------------------------------------------- the measured ratio

/// What decoding cost last time, per byte of parquet named.
///
/// Kept per pipeline and updated after every batch, so the bound tightens or relaxes with
/// the data rather than with a constant somebody chose. Stored as parts-per-thousand in an
/// atomic because it is read on the hot path and written once per batch.
#[derive(Debug)]
pub struct Amplification(AtomicU64);

impl Default for Amplification {
    fn default() -> Self {
        Self(AtomicU64::new((ASSUMED_AMPLIFICATION * 1000.0) as u64))
    }
}

impl Amplification {
    pub fn get(&self) -> f64 {
        self.0.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Fold in what this batch actually cost.
    ///
    /// The maximum of the old and new estimate rather than the newest, and deliberately: a
    /// batch that happened to decode cheaply must not raise the ceiling for the one after
    /// it, which may not. It decays only on a restart, which is the one moment the process
    /// has a clean slate anyway.
    pub fn observe(&self, parquet_bytes: u64, decoded_bytes: u64) {
        if parquet_bytes == 0 {
            return;
        }
        let seen = ((decoded_bytes as f64 / parquet_bytes as f64) * 1000.0) as u64;
        self.0.fetch_max(seen, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configured_budget_is_divided_between_the_pipelines_that_share_it() {
        let b = Budget::resolve(Some(8 * 1024 * 1024 * 1024), 4);
        assert_eq!(b.per_pipeline(), Some(2 * 1024 * 1024 * 1024));
    }

    #[test]
    fn no_configuration_and_no_container_is_the_behaviour_there_always_was() {
        // Only meaningful off a limited cgroup, which a developer's machine is; on CI in a
        // container the resolved budget is the container's, which is also correct.
        let b = Budget::resolve(None, 4);
        if cgroup_limit().is_none() {
            assert_eq!(b.per_pipeline(), None);
            assert!(b.bytes_per_batch(5.0).is_none());
        }
    }

    #[test]
    fn one_pipeline_gets_the_whole_budget_and_zero_does_not_divide_by_zero() {
        assert_eq!(Budget::resolve(Some(1000), 1).per_pipeline(), Some(1000));
        assert_eq!(Budget::resolve(Some(1000), 0).per_pipeline(), Some(1000));
    }

    #[test]
    fn the_batch_bound_falls_as_decoding_turns_out_to_cost_more() {
        let b = Budget::resolve(Some(4_000_000_000), 4);
        let cheap = b.bytes_per_batch(2.0).unwrap();
        let dear = b.bytes_per_batch(10.0).unwrap();
        assert!(dear < cheap, "{dear} should be under {cheap}");
        // Half a pipeline's share, divided by the ratio.
        assert_eq!(cheap, (1_000_000_000f64 * 0.5 / 2.0) as u64);
    }

    #[test]
    fn the_ratio_only_ratchets_up() {
        let a = Amplification::default();
        a.observe(100, 200); // 2x — below the assumed start, so it must not lower it
        assert_eq!(a.get(), ASSUMED_AMPLIFICATION);
        a.observe(100, 1_200); // 12x
        assert_eq!(a.get(), 12.0);
        a.observe(100, 300); // back down, and ignored
        assert_eq!(a.get(), 12.0);
    }

    #[test]
    fn a_batch_that_named_nothing_teaches_nothing() {
        let a = Amplification::default();
        a.observe(0, 999_999);
        assert_eq!(a.get(), ASSUMED_AMPLIFICATION);
    }
}
