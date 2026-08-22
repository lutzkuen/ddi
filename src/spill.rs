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

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

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
pub const MIN_TEMP_DIRECTORY_SIZE: u64 = 1024 * 1024;

/// The process's spill directory and budget, and the prototype that shares them out.
pub struct Spill {
    /// Built once. Every runtime in this process is derived from it, which is what makes
    /// them share its `Arc<DiskManager>` — and therefore its one byte counter.
    prototype: Arc<RuntimeEnv>,
    directory: Option<PathBuf>,
    configured_cap: bool,
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
                let probe = path.join(format!("ddi-spill-probe-{}", std::process::id()));
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
            .with_disk_manager_builder(builder)
            .build_arc()
            .map_err(|e| {
                Error::Config(match directory {
                    Some(d) => unusable(d, &e),
                    None => format!("the process's spill budget cannot be built: {e}"),
                })
            })?;
        Ok(Self {
            prototype,
            directory: dir,
            configured_cap: cap.is_some(),
        })
    }

    /// Where this process has always spilled: the OS temporary directory, DataFusion's 100 GB.
    ///
    /// The state a workstation run and every unit test are in, and the one a process is in
    /// before [`install`] has been called.
    pub fn unbounded() -> Self {
        let prototype = RuntimeEnvBuilder::new()
            .build_arc()
            .expect("a default RuntimeEnv cannot fail to build");
        Self {
            prototype,
            directory: None,
            configured_cap: false,
        }
    }

    /// A builder for a runtime that spills into this budget — **the one place sharing happens**.
    pub fn runtime_builder(&self) -> RuntimeEnvBuilder {
        RuntimeEnvBuilder::from_runtime_env(&self.prototype)
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
        self.prototype.disk_manager.max_temp_directory_size()
    }

    pub fn used_bytes(&self) -> u64 {
        self.prototype.disk_manager.used_disk_space()
    }

    pub fn active_files(&self) -> usize {
        self.prototype
            .disk_manager
            .spilling_progress()
            .active_files_count
    }

    /// Where DataFusion says it will write, which is not the same as what was configured:
    /// the directory is only created on the first spill.
    pub fn temp_dir_paths(&self) -> Vec<PathBuf> {
        self.prototype.disk_manager.temp_dir_paths()
    }

    /// Only for the test that proves there is one of these, not N.
    pub fn disk_manager(&self) -> &Arc<DiskManager> {
        &self.prototype.disk_manager
    }
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

/// Read a DataFusion error as a capacity failure, or leave it alone.
///
/// `ResourcesExhausted` is raised by memory pools too, so the variant alone would misattribute
/// an OOM to the disk budget and tell the operator to raise the wrong knob. When the text does
/// not name the disk, this says memory instead of guessing.
pub fn classify(e: DataFusionError, context: &str) -> Error {
    if !matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)) {
        return Error::Transform(format!("{context}: {e}"));
    }
    let s = e.to_string();
    let spill = current();
    let which = if s.contains(DISK_EXHAUSTED) {
        format!(
            "the process's spill budget ([runtime] max_temp_directory_size, currently {}{})",
            bytesize::ByteSize(spill.limit_bytes()),
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
        assert!(Arc::ptr_eq(&a.disk_manager, spill.disk_manager()));
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
