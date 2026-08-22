//! One directory, one budget, one counter — for the whole process.
//!
//! # The incident
//!
//! Eleven pipelines started in one pod. One of them ran the startup uniqueness check over a
//! target with billions of mostly-distinct keys, DataFusion spilled the grouped aggregate,
//! and it spilled into `/tmp` — which inside a container is the writable layer, which the
//! kubelet accounts as local ephemeral storage. At a 32 GiB limit the pod was *evicted*,
//! taking the other ten pipelines and the metrics that would have explained it with it.
//!
//! That is the one failure this tool's supervisor cannot contain, because it does not happen
//! inside the process. Everything else here fails one pipeline loudly; this kills all of them
//! silently, from outside.
//!
//! # Why the limit that already existed did not hold
//!
//! DataFusion has a spill cap and it defaults to 100 GB. It was respected. It was respected
//! eleven times over, because the cap is a `u64` field on a [`DiskManager`] and the counter
//! it is checked against is that same `DiskManager`'s atomic — so the cap is per
//! `DiskManager`, which in stock DataFusion means per `RuntimeEnv`. And this process builds a
//! `RuntimeEnv` per DataFusion *operation*, not per pipeline: one per merge attempt, one per
//! transform batch, one per startup check. The process bound was therefore "one hundred
//! gigabytes times however many operations are in flight", which is not a number anyone can
//! compare against a volume, and is not a number Kubernetes has ever agreed to.
//!
//! # What this module does about it
//!
//! Builds one `RuntimeEnv` at startup and derives every other one from it, so they all share
//! its `Arc<DiskManager>`. One directory, one `used_disk_space` atomic, one cap. That makes
//! `[runtime] max_temp_directory_size` mean what an operator reads it as: bytes this process
//! may have on local disk, full stop.
//!
//! Sharing costs one thing worth naming. The only representation of "use this existing
//! manager" in DataFusion 53 is `DiskManagerConfig::Existing`, which is deprecated — but
//! [`RuntimeEnvBuilder::from_runtime_env`] is *not*, and reaches it internally under
//! DataFusion's own `#[expect(deprecated)]`. So the deprecation is DataFusion's, spelled
//! inside DataFusion, and the whole of this tool's exposure to it is [`Spill::runtime_builder`].
//! If a future release stops sharing there, `two_runtimes_share_one_disk_manager` fails on the
//! next `cargo test` — which is the point of that test, and why it must not be deleted as
//! redundant.
//!
//! # What this does not cover, and must not be read as covering
//!
//! The counter knows only what DataFusion wrote through it. It does not know the filesystem's
//! free space, it does not see anything else in the pod, and it does not see a `RuntimeEnv` a
//! dependency builds for itself. The places delta-rs used to do that now receive one of ours
//! instead — but that is an enumeration, not an invariant. Any new call to a delta-rs builder
//! needs `.with_session_state(..)` or it opens the hole again, and nothing here can catch that
//! at compile time.
//!
//! The cap is also enforced *after* each write rather than as an admission check
//! (`RefCountedTempFile::update_disk_usage`), so it can be overshot by roughly one write
//! buffer per open spill file. Set it below the volume, never equal to it.
//!
//! # The upstream leak this has to clean up after, and when to delete that code
//!
//! Hitting the cap costs more than the query that hit it. `RefCountedTempFile::update_disk_usage`
//! charges the new file size to the shared total, then returns `ResourcesExhausted` *before*
//! recording that size on the file — so the `Drop` that follows subtracts the older, smaller
//! figure and the whole difference stays charged. Not a rounding error: the file that broke a
//! 100 KB budget in a reproduction stranded 490 KB, five times the budget itself. After that
//! the counter sits permanently above the cap and **every** subsequent spill in the process is
//! refused, including a one-kilobyte sort in a pipeline that has nothing to do with the one
//! that overran.
//!
//! That was survivable while each operation built its own manager, because the residue died
//! with the operation. Sharing one manager is exactly what makes it permanent — so this module
//! replaces the manager once it has become useless, and *not before*: a replacement issued
//! while the old one could still accept a byte would leave the process holding two managers
//! with a full cap each, which is the arithmetic that evicted the pod in the first place. See
//! [`wedged`] for the threshold that rules that out, and `ddi_spill_stranded_bytes_total` for
//! how much has been abandoned this way.
//!
//! **This is a workaround for DataFusion 53.1, and it has an expiry date.** Upstream replaced
//! the whole `update_disk_usage` design in 55.0.0: `FileSpillWriter::write` now adds, checks,
//! and *rolls the add back* before returning the error, so nothing is left charged. 53.1 and
//! 54.x have the bug; 55.0.0 and later do not. `deltalake` pins the DataFusion version this
//! crate gets (`Cargo.toml`), so a bump past 54 is the signal to delete
//! [`Spill::recover_if_wedged`], [`wedged`], the `stranded` counter,
//! `ddi_spill_stranded_bytes_total` and the test that pins them, together; the `RwLock` around
//! the prototype exists only for this and can go back to a plain `Arc` at the same time.
//!
//! That signal arrives as a *compile* error rather than a failing assertion, because 55.0.0
//! deletes `RefCountedTempFile::update_disk_usage` outright and the test calls it directly.
//! Which is the loudest reminder available, and the reason the test drives the leak by hand
//! rather than through a query that happens to spill.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::execution::cache::cache_manager::CacheManagerConfig;
use deltalake::datafusion::execution::disk_manager::{
    DiskManager, DiskManagerBuilder, DiskManagerMode,
};
use deltalake::datafusion::execution::object_store::DefaultObjectStoreRegistry;
use deltalake::datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

use crate::error::{Error, Result};

/// The smallest cap that is a cap rather than a refusal.
///
/// DataFusion checks the total *after* each write, so a budget under one spill batch is
/// exceeded by the first one — and every sort, grouped aggregate and merge join in the
/// process would fail rather than run slowly.
///
/// Decimal, not binary, because `bytesize` reads `"1MB"` as a million and the refusal message
/// tells the operator to write exactly that. A binary floor here would reject its own advice.
/// `max_bytes_per_batch` already counts this way.
pub const MIN_TEMP_DIRECTORY_SIZE: u64 = 1_000_000;

/// The process's spill directory and budget, and the prototype that shares them out.
pub struct Spill {
    /// Every runtime in this process is derived from this one, which is what makes them share
    /// its `Arc<DiskManager>` — and therefore its one byte counter.
    ///
    /// Behind a lock only because it is replaced when that counter is wedged, which is a
    /// thing DataFusion's accounting makes necessary rather than a thing this design wanted.
    /// See [`Spill::recover_if_wedged`]. Read on every runtime build, written approximately
    /// never.
    prototype: RwLock<Arc<RuntimeEnv>>,
    /// What to rebuild the prototype from — the same directory and the same cap.
    builder: DiskManagerBuilder,
    directory: Option<PathBuf>,
    configured_cap: bool,
    /// Bytes abandoned by wedged managers this process has replaced, so the loss is visible
    /// rather than merely gone.
    stranded: AtomicU64,
}

impl std::fmt::Debug for Spill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spill")
            .field("directory", &self.directory)
            .field("limit_bytes", &self.limit_bytes())
            .field("configured_cap", &self.configured_cap)
            .finish()
    }
}

/// What to say about a directory this process cannot use.
///
/// One function rather than three call sites, because the advice is the same however the
/// probe failed and it is the advice that is worth the words.
fn unusable(dir: &str, cause: &dyn std::fmt::Display) -> String {
    format!(
        "runtime.temp_directory {dir:?} is not usable: {cause}. Every spill in this process \
         goes here, so it must be creatable and writable by the user this process runs as. It \
         is checked by writing a probe file rather than by reading the mode bits, because a \
         read-only mount and an exhausted quota both look writable to stat. In Kubernetes this \
         is a volumeMount, and the failure above is what an unmounted volume looks like. \
         Remove the key to spill into the OS temporary directory instead — which inside a \
         container is the writable layer, and is exactly what the kubelet counts as local \
         ephemeral storage."
    )
}

impl Spill {
    /// Work out the process's spill budget, and prove the directory exists and takes writes.
    ///
    /// The probe is deliberate and is the point of doing this at startup: a volume that was
    /// not mounted becomes a startup failure with a name on it, rather than a sort failing an
    /// hour later inside a pipeline that has nothing to do with the mistake.
    pub fn resolve(directory: Option<&str>, cap: Option<u64>) -> Result<Self> {
        let mut builder = DiskManagerBuilder::default();
        let dir = match directory {
            None => None,
            Some(d) => {
                let path = PathBuf::from(d);
                // `create_dir_all`, not `create_dir`: DataFusion's own `create_local_dirs`
                // creates a single level, and a spill path is almost always nested under a
                // mount point.
                std::fs::create_dir_all(&path).map_err(|e| Error::Config(unusable(d, &e)))?;
                // Unique per call, not just per process. Two `Spill::resolve` calls racing on
                // one directory — which is what a test binary does, and what a future
                // supervisor restart would do — would otherwise pick the same name, and the
                // slower one's `remove_file` would fail with ENOENT on a directory that is
                // perfectly writable.
                static PROBE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let probe = path.join(format!(
                    "ddi-spill-probe-{}-{}",
                    std::process::id(),
                    PROBE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                ));
                std::fs::write(&probe, b"ddi")
                    .and_then(|_| std::fs::remove_file(&probe))
                    .map_err(|e| Error::Config(unusable(d, &e)))?;
                builder = builder.with_mode(DiskManagerMode::Directories(vec![path.clone()]));
                Some(path)
            }
        };
        if let Some(n) = cap {
            builder = builder.with_max_temp_directory_size(n);
        }
        let prototype = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(builder.clone())
            .build_arc()
            .map_err(|e| {
                Error::Config(match directory {
                    Some(d) => unusable(d, &e),
                    None => format!("the process's spill budget cannot be built: {e}"),
                })
            })?;
        Ok(Self {
            prototype: RwLock::new(prototype),
            builder,
            directory: dir,
            configured_cap: cap.is_some(),
            stranded: AtomicU64::new(0),
        })
    }

    /// Where this process has always spilled: the OS temporary directory, DataFusion's 100 GB.
    ///
    /// The state a workstation run and every unit test are in, and the one a process is in
    /// before [`install`] has been called.
    pub fn unbounded() -> Self {
        let builder = DiskManagerBuilder::default();
        let prototype = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(builder.clone())
            .build_arc()
            .expect("a default RuntimeEnv cannot fail to build");
        Self {
            prototype: RwLock::new(prototype),
            builder,
            directory: None,
            configured_cap: false,
            stranded: AtomicU64::new(0),
        }
    }

    /// The runtime every other one is derived from, after any needed recovery.
    fn prototype(&self) -> Arc<RuntimeEnv> {
        self.recover_if_wedged();
        self.prototype.read().expect("not poisoned").clone()
    }

    /// Replace the shared manager when its counter has been left above the cap.
    ///
    /// DataFusion 53.1 charges a spill write to the shared total and *then* returns the
    /// limit error, without recording that size on the file — so the `Drop` that follows
    /// subtracts the older, smaller figure and the difference stays charged forever
    /// (`RefCountedTempFile::update_disk_usage` steps 2, 3 and 4). A single failure can
    /// therefore strand more than the whole budget, after which every subsequent spill in the
    /// process is refused: not the pipeline that overran, all of them, including a one-kilobyte
    /// sort in a stream that has nothing to do with it.
    ///
    /// That was survivable while each operation built its own manager, because the residue died
    /// with the operation. Sharing one manager is what makes it permanent, so sharing one
    /// manager is what has to clean up after it.
    ///
    /// It waits until the residue reaches the whole cap before acting — see [`wedged`] for why
    /// that specific threshold is what keeps a replacement from doubling the process's budget.
    /// A runtime built before the swap keeps the old manager, and that manager refuses every
    /// spill, so an operation still holding one fails once and retries onto a fresh runtime.
    /// That is the cost, and it is paid against a process that would otherwise never spill
    /// again.
    ///
    /// Fixed upstream in DataFusion 55.0.0; delete this with its test when `deltalake` takes
    /// us past 54. See the module header.
    fn recover_if_wedged(&self) {
        let seen = {
            let guard = self.prototype.read().expect("not poisoned");
            if !wedged(&guard.disk_manager) {
                return;
            }
            guard.clone()
        };

        let mut guard = self.prototype.write().expect("not poisoned");
        // Another thread may have replaced it between the two locks, and its replacement is
        // as good as ours would have been.
        if !Arc::ptr_eq(&guard, &seen) || !wedged(&guard.disk_manager) {
            return;
        }
        let lost = guard.disk_manager.used_disk_space();
        match RuntimeEnvBuilder::new()
            .with_disk_manager_builder(self.builder.clone())
            .build_arc()
        {
            Ok(fresh) => {
                *guard = fresh;
                self.stranded.fetch_add(lost, Ordering::Relaxed);
                tracing::warn!(
                    stranded_bytes = lost,
                    "the spill budget's counter reached its whole {} with nothing actually on \
                     disk — DataFusion charges the write that breaks the cap and then returns \
                     before recording it, so the difference stays charged. Every spill in this \
                     process was going to be refused from here on. A fresh temporary directory \
                     has been opened and spilling continues; nothing was lost but the \
                     accounting. Repeated occurrences mean this process is hitting its spill \
                     cap often — see ddi_spill_stranded_bytes_total.",
                    bytesize::ByteSize(lost)
                );
            }
            Err(e) => tracing::error!(
                "the spill budget's counter is wedged at {} with nothing on disk, and a \
                 replacement directory could not be opened: {e}. Spilling will be refused \
                 until this process restarts.",
                bytesize::ByteSize(lost)
            ),
        }
    }

    /// A builder for a runtime that spills into this budget — **the one place sharing happens**.
    pub fn runtime_builder(&self) -> RuntimeEnvBuilder {
        RuntimeEnvBuilder::from_runtime_env(&self.prototype())
            // `from_runtime_env` carries three things across besides the disk manager, and
            // only the disk manager is meant to be shared. The memory pool is re-set by the
            // caller; these two are reset here.
            //
            // The registry, because `budget::session` registers a per-table object store into
            // it — sharing it would make one pipeline's credentials for a host visible to
            // every other pipeline addressing the same host, and whichever registered last
            // would win. That is not a thing a disk budget should buy.
            //
            // The cache manager, because `from_runtime_env` carries the prototype's file
            // metadata cache over — one shared 50 MB cache living outside the per-pipeline
            // memory budget, which is exactly the kind of unattributed allocation
            // `crate::budget` exists to stop.
            //
            // Audited against datafusion-execution 53.1; a bump that adds a fourth carried
            // field will carry it silently, so re-read `from_runtime_env` when it moves.
            .with_object_store_registry(Arc::new(DefaultObjectStoreRegistry::new()))
            .with_cache_manager(CacheManagerConfig::default())
    }

    pub fn directory(&self) -> Option<&Path> {
        self.directory.as_deref()
    }

    /// True when an operator wrote a cap down, as opposed to inheriting DataFusion's.
    pub fn cap_was_configured(&self) -> bool {
        self.configured_cap
    }

    /// Always a number. DataFusion always has one, and reporting "unset" as unbounded would
    /// let an alert expression divide by zero and an operator read 100 GB as none.
    pub fn limit_bytes(&self) -> u64 {
        self.prototype().disk_manager.max_temp_directory_size()
    }

    pub fn used_bytes(&self) -> u64 {
        self.prototype().disk_manager.used_disk_space()
    }

    pub fn active_files(&self) -> usize {
        self.prototype()
            .disk_manager
            .spilling_progress()
            .active_files_count
    }

    /// Where DataFusion says it will write, which is not the same as what was configured:
    /// the directory is only created on the first spill.
    pub fn temp_dir_paths(&self) -> Vec<PathBuf> {
        self.prototype().disk_manager.temp_dir_paths()
    }

    /// Only for the test that proves there is one of these, not N.
    pub fn disk_manager(&self) -> Arc<DiskManager> {
        Arc::clone(&self.prototype().disk_manager)
    }

    /// Bytes abandoned by wedged managers this process has replaced.
    ///
    /// Zero on almost every process. A number here means the budget has been through a
    /// capacity failure and DataFusion's accounting did not give the space back — the space
    /// itself was returned, the count of it was not.
    pub fn stranded_bytes(&self) -> u64 {
        self.stranded.load(Ordering::Relaxed)
    }
}

/// This manager will refuse every spill from now on, and the bytes it is refusing over are
/// not really there.
///
/// Both halves are load-bearing, and the second one is what makes replacing the manager safe
/// rather than a way of reintroducing the bug this module exists to fix.
///
/// **`used >= max`** — not merely `used > 0`. A manager carrying residue *below* its cap still
/// works; it just has less room, which is a bounded loss and the right one to accept. Replacing
/// it would not be: a runtime built before the swap keeps the old manager, so for a while the
/// process would hold two, each with a full cap, and two times the cap is precisely the
/// arithmetic that evicted a pod. Once the residue reaches the cap the old manager refuses
/// *everything*, so it can contribute no bytes at all and a replacement cannot take the total
/// past one cap. Waiting for that is what keeps the process bound true. It is also two atomic
/// loads, which is what lets this be called on every runtime build.
///
/// **The spill directories are empty** — the proof that the count is residue rather than real
/// usage. `create_tmp_file` builds every spill file with `Builder::tempfile_in`, so a live one
/// is always a linked, visible entry in one of these directories; no entries means nothing is
/// on disk and a replacement loses no accounting.
///
/// The obvious witness — `active_files_count == 0` — is the wrong one, and it took a
/// reproduction to see why. `create_tmp_file` increments that counter *before* creating the
/// file and returns early if the creation fails (`fetch_add`, then `tempfile_in(..)?`), so no
/// `RefCountedTempFile` is ever built to decrement it. One `EMFILE`, one read-only remount, one
/// full volume leaks the count permanently — and it stays leaked after the directory is
/// healthy again, which disarms this recovery for the life of a process that is otherwise
/// perfectly able to spill. The filesystem cannot lie in that direction, and recovering also
/// replaces the leaked counter with a fresh one.
///
/// A directory that cannot be read is treated as not-empty, so an unreadable spill volume
/// declines recovery rather than assuming it. That is re-evaluated on the next call rather than
/// remembered, which is the difference from the counter.
fn wedged(dm: &DiskManager) -> bool {
    if dm.used_disk_space() < dm.max_temp_directory_size() {
        return false;
    }
    dm.temp_dir_paths().iter().all(|dir| {
        std::fs::read_dir(dir)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false)
    })
}

static INSTALLED: OnceLock<Arc<Spill>> = OnceLock::new();

/// Make this the budget every runtime in this process spills into.
///
/// Returns false when something had already asked for [`current`] — which means it built a
/// runtime against the default, and the configured directory and cap are NOT in force. The
/// caller warns rather than failing: the process is still bounded, just not the way the config
/// says, and that is worth a loud line rather than an outage.
pub fn install(spill: Spill) -> bool {
    INSTALLED.set(Arc::new(spill)).is_ok()
}

/// The budget in force, or the one this process has always had if nobody installed it.
pub fn current() -> Arc<Spill> {
    INSTALLED
        .get_or_init(|| Arc::new(Spill::unbounded()))
        .clone()
}

/// The sentence DataFusion writes when the spill directory is full.
///
/// Matched as a substring rather than a whole message because the size it names is formatted
/// in.
const DISK_EXHAUSTED: &str = "used disk space during the spilling process";

/// The prefix `DataFusionError::ResourcesExhausted` renders with.
///
/// Needed because delta-rs stringifies a DataFusion error it does not special-case into
/// `DeltaTableError::Generic`, so by the time a merge or a write failure reaches this crate the
/// variant is gone and the text is all that is left. See [`classify_delta`].
const EXHAUSTED: &str = "Resources exhausted:";

/// Read a DataFusion error as a capacity failure, or leave it alone.
///
/// `ResourcesExhausted` is raised by memory pools too, so the variant alone would misattribute
/// an OOM to the disk budget and tell the operator to raise the wrong knob. When the text does
/// not name the disk, this says memory instead of guessing.
pub fn classify(e: DataFusionError, context: &str) -> Error {
    if !matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)) {
        return Error::Transform(format!("{context}: {e}"));
    }
    capacity(e.to_string(), context)
}

/// Read a delta-rs failure as a capacity failure, or leave it as [`Error::Delta`].
///
/// The merge and the write — the two things here that spill at target scale — reach this crate
/// through delta-rs, which has already collapsed the DataFusion error into a string. So this
/// matches on the text where [`classify`] can match on the variant. Both end up in the same
/// place, and that place is the only reason [`Error::Capacity`] exists: without it a full
/// spill directory is indistinguishable from a wrong answer, and the supervisor retries it a
/// second later, forever, holding the shared budget while it does.
pub fn classify_delta(e: deltalake::DeltaTableError, context: &str) -> Error {
    let s = e.to_string();
    if !s.contains(EXHAUSTED) {
        return Error::Delta(e);
    }
    capacity(s, context)
}

/// Say which budget ran out, given the message that says one did.
fn capacity(s: String, context: &str) -> Error {
    let spill = current();
    let which = if s.contains(DISK_EXHAUSTED) {
        // The byte count as well as the human size, and deliberately: DataFusion renders the
        // same number in binary units inside the sentence above ("4.0 MB" for 4 MiB) and this
        // tool renders it in decimal ones everywhere else. Two spellings of one number in one
        // message reads as two different numbers, so the exact one settles it.
        format!(
            "the process's spill budget ([runtime] max_temp_directory_size, currently {} \
             ({} bytes){})",
            bytesize::ByteSize(spill.limit_bytes()),
            spill.limit_bytes(),
            match spill.directory() {
                Some(d) => format!(", in {}", d.display()),
                None => ", in the OS temporary directory".to_string(),
            }
        )
    } else {
        "a memory pool ([runtime] max_memory, divided by the pipelines running)".to_string()
    };
    Error::Capacity(format!("{context}: {s}\n\nWhat ran out was {which}."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_runtimes_share_one_disk_manager_and_therefore_one_counter() {
        // The DataFusion-upgrade tripwire. If `from_runtime_env` ever stops carrying the
        // existing manager across, every pipeline silently gets its own 100 GB again and the
        // process bound becomes a number nobody can compute. Never delete this as redundant.
        let spill = Spill::unbounded();
        let a = spill.runtime_builder().build_arc().unwrap();
        let b = spill.runtime_builder().build_arc().unwrap();
        assert!(
            Arc::ptr_eq(&a.disk_manager, &b.disk_manager),
            "every runtime in this process must spill into one counter"
        );
        assert!(Arc::ptr_eq(&a.disk_manager, &spill.disk_manager()));
    }

    #[test]
    fn sharing_a_disk_manager_does_not_share_an_object_store_registry() {
        // One pipeline's credentials for a host must not become another's. `from_runtime_env`
        // carries the registry over, so this guards the line that resets it.
        let spill = Spill::unbounded();
        let a = spill.runtime_builder().build_arc().unwrap();
        let b = spill.runtime_builder().build_arc().unwrap();
        assert!(!Arc::ptr_eq(
            &a.object_store_registry,
            &b.object_store_registry
        ));
    }

    #[test]
    fn sharing_a_disk_manager_does_not_share_a_file_metadata_cache() {
        // `from_runtime_env` sets `file_metadata_cache: Some(..)`, which would be one shared
        // 50 MB allocation living outside every per-pipeline memory budget.
        let spill = Spill::unbounded();
        let a = spill.runtime_builder().build_arc().unwrap();
        let b = spill.runtime_builder().build_arc().unwrap();
        assert!(!Arc::ptr_eq(
            &a.cache_manager.get_file_metadata_cache(),
            &b.cache_manager.get_file_metadata_cache()
        ));
    }

    #[test]
    fn the_configured_directory_is_where_datafusion_says_it_will_write() {
        let dir = tempfile::tempdir().unwrap();
        let spill = Spill::resolve(Some(dir.path().to_str().unwrap()), Some(8 * 1024 * 1024))
            .expect("a fresh tempdir is usable");
        assert_eq!(spill.limit_bytes(), 8 * 1024 * 1024);
        assert_eq!(spill.directory(), Some(dir.path()));
        for p in spill.temp_dir_paths() {
            assert!(p.starts_with(dir.path()), "{p:?} is outside {dir:?}");
        }
    }

    #[test]
    fn a_spill_directory_that_does_not_exist_yet_is_created_rather_than_refused() {
        let root = tempfile::tempdir().unwrap();
        let nested = root.path().join("a").join("b").join("spill");
        let spill = Spill::resolve(Some(nested.to_str().unwrap()), None)
            .expect("a nested path under a writable root is created");
        assert!(nested.is_dir());
        assert_eq!(spill.directory(), Some(nested.as_path()));
    }

    #[test]
    fn a_spill_directory_that_is_a_file_is_refused_before_any_pipeline_opens() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("not-a-directory");
        std::fs::write(&file, b"x").unwrap();
        let e = Spill::resolve(Some(file.to_str().unwrap()), None).unwrap_err();
        let m = e.to_string();
        assert!(m.contains("is not usable"), "{m}");
        assert!(m.contains("volumeMount"), "{m}");
    }

    #[cfg(unix)]
    #[test]
    fn a_spill_directory_this_process_cannot_write_is_refused_with_a_probe_not_a_stat() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("read-only");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let outcome = Spill::resolve(dir.to_str(), None);
        // Restore before asserting, so a failure does not leave an undeletable tempdir behind.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        match outcome {
            // Root defeats the mode bits entirely, and CI sometimes runs as root. Passing the
            // probe *is* the correct answer there; asserting a refusal would be a test that
            // fails for a reason that has nothing to do with this code.
            Ok(s) => assert_eq!(s.directory(), Some(dir.as_path())),
            Err(e) => {
                let m = e.to_string();
                assert!(m.contains("is not usable"), "{m}");
                assert!(m.contains("probe file"), "{m}");
            }
        }
    }

    #[test]
    fn an_uninstalled_process_still_spills_where_it_always_did() {
        let spill = Spill::unbounded();
        assert_eq!(spill.directory(), None);
        assert!(!spill.cap_was_configured());
        assert_eq!(spill.limit_bytes(), 100 * 1024 * 1024 * 1024);
    }

    #[test]
    fn the_limit_reported_is_never_zero_for_unset() {
        // So an alert expression on ddi_spill_bytes / ddi_spill_limit_bytes cannot divide by
        // zero, and so an operator cannot read "unset" as "unbounded".
        assert!(Spill::unbounded().limit_bytes() > 0);
        assert!(current().limit_bytes() > 0);
    }

    #[test]
    fn a_capacity_failure_that_arrives_through_delta_rs_is_still_a_capacity_failure() {
        // The path that actually matters, and the one a From<DataFusionError> impl never sees:
        // the merge and the write reach this crate through delta-rs, which collapses anything
        // it does not special-case into `DeltaTableError::Generic(err.to_string())`. The
        // variant is gone by then, so the text is all there is to match on.
        let e = classify_delta(
            deltalake::DeltaTableError::Generic(
                "Resources exhausted: The used disk space during the spilling process has \
                 exceeded the allowable limit of 4.0 MB."
                    .into(),
            ),
            "upsert: merging into the target",
        );
        assert!(matches!(e, Error::Capacity(_)), "{e}");
        let m = e.to_string();
        assert!(m.contains("max_temp_directory_size"), "{m}");

        // A memory pool exhausted inside a merge is still capacity, and still not the disk.
        let e = classify_delta(
            deltalake::DeltaTableError::Generic(
                "Resources exhausted: Failed to allocate additional 1024 bytes for \
                 GroupedHashAggregateStream"
                    .into(),
            ),
            "upsert: merging into the target",
        );
        assert!(matches!(e, Error::Capacity(_)), "{e}");
        assert!(e.to_string().contains("max_memory"), "{e}");

        // Everything else keeps the shape it always had, including the variant — a schema
        // mismatch must not become a capacity failure that waits five minutes to retry.
        let e = classify_delta(
            deltalake::DeltaTableError::Generic("Error during planning: no such column".into()),
            "upsert: merging into the target",
        );
        assert!(matches!(e, Error::Delta(_)), "{e}");
    }

    #[test]
    fn a_full_spill_directory_is_read_as_a_capacity_failure_and_a_full_memory_pool_is_not() {
        let disk = DataFusionError::ResourcesExhausted(
            "The used disk space during the spilling process has exceeded the allowable limit \
             of 100.0 GB. Try increasing the `max_temp_directory_size` in the disk manager \
             configuration."
                .into(),
        );
        let e = classify(disk, "upsert").to_string();
        assert!(e.contains("max_temp_directory_size"), "{e}");
        assert!(e.contains("out of capacity"), "{e}");

        let memory = DataFusionError::ResourcesExhausted(
            "Failed to allocate additional 1024 bytes for GroupedHashAggregateStream".into(),
        );
        let e = classify(memory, "upsert").to_string();
        assert!(e.contains("max_memory"), "{e}");
        assert!(
            !e.contains("max_temp_directory_size, currently"),
            "an OOM must not be blamed on the disk budget: {e}"
        );

        // Everything else keeps the shape it always had.
        let other = DataFusionError::Plan("no such column".into());
        assert!(matches!(classify(other, "upsert"), Error::Transform(_)));
    }
}
