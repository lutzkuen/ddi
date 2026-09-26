//! The dbt handover, end to end.
//!
//! `ddi` stores its offset as a `txn` action in the target's log, and `txn` actions
//! survive an overwrite. So when dbt rebuilds a shared target, `ddi` would otherwise wake
//! up believing it processed through version N, resume at N+1, and never re-emit the rows
//! it streamed while dbt was reading. The first test here is that failure, reproduced
//! against a real table; the rest are the fix.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once, OnceLock};

use common::*;
use delta_delta_ingest::config::{ResolvedPipeline, WriteMode};
use delta_delta_ingest::dbt::watermark::{target_state, TargetState, WatermarkStore};
use delta_delta_ingest::dedup::CoverageReason;
use delta_delta_ingest::pipeline::{CoverageWindow, Pipeline, StepOutcome};
use deltalake::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::StructType;
use deltalake::logstore::object_store::local::LocalFileSystem;
use deltalake::logstore::object_store::path::Path;
use deltalake::logstore::object_store::{
    self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use deltalake::logstore::{
    default_logstore, logstore_factories, object_store_factories, LogStore, LogStoreFactory,
    ObjectStoreFactory, ObjectStoreRef, StorageConfig,
};
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, open_table, DeltaResult, DeltaTable};
use futures::stream::BoxStream;
use url::Url;

// ------------------------------------------------------------------ watermark table

fn watermark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("app_id", DataType::Utf8, false),
        Field::new("source_version", DataType::Int64, false),
    ]))
}

async fn create_watermark_table(path: &str) {
    create_watermark_table_partitioned_by(path, &[]).await;
}

async fn create_watermark_table_partitioned_by(path: &str, columns: &[&str]) {
    let delta: StructType = watermark_schema().as_ref().try_into_kernel().unwrap();
    let url = ensure_table_uri(path).unwrap();
    DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(delta.fields().cloned().collect::<Vec<_>>())
        .with_partition_columns(columns.iter().copied())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();
}

/// Every data file under `dir`, in partition directories too.
fn parquet_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() && !path.ends_with("_delta_log") {
            files.extend(parquet_files(&path));
        } else if path.extension().is_some_and(|e| e == "parquet") {
            files.push(path);
        }
    }
    files
}

/// What a dbt post-hook does: `INSERT INTO ddi_watermark VALUES (app_id, version)`.
async fn record_watermark(path: &str, app_id: &str, version: i64) {
    let batch = RecordBatch::try_new(
        watermark_schema(),
        vec![
            Arc::new(StringArray::from(vec![app_id])) as ArrayRef,
            Arc::new(Int64Array::from(vec![version])) as ArrayRef,
        ],
    )
    .unwrap();
    let url = ensure_table_uri(path).unwrap();
    open_table(url)
        .await
        .unwrap()
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
}

/// What the nightly dbt run does: replace the target with its own full recompute.
async fn dbt_rebuild(target: &str, ids: &[i64]) {
    let url = ensure_table_uri(target).unwrap();
    open_table(url)
        .await
        .unwrap()
        .write(vec![batch(ids)])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();
}

/// What another writer's `DELETE` does to the target.
async fn delete_from(target: &str, predicate: &str) {
    let (_t, m) = open(target)
        .await
        .delete()
        .with_predicate(predicate.to_string())
        .await
        .unwrap();
    assert!(m.num_deleted_rows.unwrap_or(0) > 0, "nothing was deleted");
}

// ------------------------------------------------------- a rebuild while ddi opens

type Hook = futures::future::BoxFuture<'static, ()>;

/// What a [`hooked`] table's store does before the next read anyone makes through it, by path.
fn hooks() -> &'static Mutex<HashMap<String, Hook>> {
    static HOOKS: OnceLock<Mutex<HashMap<String, Hook>>> = OnceLock::new();
    HOOKS.get_or_init(Default::default)
}

/// `path` as a URI `ddi` reads through a store that runs what [`before_next_read`] armed for it.
/// A target opened through it lets a test commit to other tables at the one point that
/// matters to a handover: `Pipeline::open` has loaded the source, and not yet the target.
fn hooked(path: &str) -> String {
    static SCHEME: Once = Once::new();
    SCHEME.call_once(|| {
        let scheme = Url::parse("hooked://").unwrap();
        object_store_factories().insert(scheme.clone(), Arc::new(HookedStores));
        logstore_factories().insert(scheme, Arc::new(HookedStores));
    });
    format!("hooked://{path}")
}

fn before_next_read(path: &str, then: impl std::future::Future<Output = ()> + Send + 'static) {
    hooks()
        .lock()
        .unwrap()
        .insert(path.to_string(), Box::pin(then));
}

/// The local file system, doing first what [`before_next_read`] armed for the table it serves.
#[derive(Debug)]
struct Hooked {
    files: LocalFileSystem,
    table: String,
}

impl std::fmt::Display for Hooked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hooked({})", self.table)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Hooked {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.files.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.files.put_multipart_opts(location, opts).await
    }

    /// Loading a table reads `_last_checkpoint` before it lists the log.
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let hook = hooks().lock().unwrap().remove(&self.table);
        if let Some(hook) = hook {
            hook.await;
        }
        self.files.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.files.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        in_order(self.files.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        in_order(self.files.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.files.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.files.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.files.rename_opts(from, to, options).await
    }
}

/// A listing in key order, as every store but the local file system lists — and as the kernel
/// takes any scheme but `file` to list.
fn in_order(
    listing: BoxStream<'static, object_store::Result<ObjectMeta>>,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    use futures::{StreamExt, TryStreamExt};
    futures::stream::once(async move {
        let mut all: Vec<ObjectMeta> = listing.try_collect().await?;
        all.sort_by(|a, b| a.location.cmp(&b.location));
        Ok::<_, object_store::Error>(futures::stream::iter(all.into_iter().map(Ok)))
    })
    .try_flatten()
    .boxed()
}

struct HookedStores;

impl ObjectStoreFactory for HookedStores {
    fn parse_url_opts(
        &self,
        url: &Url,
        _config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let store = Hooked {
            files: LocalFileSystem::new(),
            table: url.path().trim_end_matches('/').to_string(),
        };
        let path = Path::from_url_path(url.path()).map_err(object_store::Error::from)?;
        Ok((Arc::new(store), path))
    }
}

impl LogStoreFactory for HookedStores {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

struct Lake {
    f: Fixture,
    watermark: String,
}

async fn lake() -> Lake {
    let f = Fixture::new().await;
    let watermark = beside_the_target(&f, "ddi_watermark");
    create_watermark_table(&watermark).await;
    Lake { f, watermark }
}

fn beside_the_target(f: &Fixture, name: &str) -> String {
    std::path::Path::new(&f.target)
        .parent()
        .unwrap()
        .join(name)
        .to_str()
        .unwrap()
        .to_string()
}

fn cfg_with_watermark(lake: &Lake, name: &str) -> ResolvedPipeline {
    let mut c = lake.f.cfg(name);
    c.watermark_uri = Some(lake.watermark.clone());
    c
}

/// Both, as every dbt model has them once a watermark table is set: `ddi_timestamp` defaults
/// to `_timestamp`. `id` plays the timestamp here.
fn cfg_with_watermark_and_timestamp(lake: &Lake) -> ResolvedPipeline {
    let mut c = cfg_with_watermark(lake, "copy");
    c.dedup_timestamp = Some("id".into());
    c.dedup_key = Some("id".into());
    c
}

// ------------------------------------------------------------------ the hazard

#[tokio::test]
async fn without_a_watermark_a_dbt_rebuild_strands_streamed_rows() {
    // The bug, reproduced. Kept as a test so the fix cannot silently regress into it.
    let f = Fixture::new().await;
    for i in 1..=3 {
        append(&f.source, &[i]).await;
    }

    let cfg = f.cfg("copy"); // no watermark_uri — the unprotected configuration
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3]);

    // dbt read the source as of version 2 and rebuilds from that. Row 3, streamed while
    // dbt was reading, is not in its output.
    dbt_rebuild(&f.target, &[1, 2]).await;

    append(&f.source, &[4]).await;
    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    assert_eq!(
        read_ids(&f.target).await,
        vec![1, 2, 4],
        "row 3 is gone: streamed by ddi, wiped by dbt, never re-emitted"
    );
}

// ------------------------------------------------------------------ the fix

#[tokio::test]
async fn a_watermark_makes_ddi_re_stream_the_gap_dbt_wiped() {
    let lake = lake().await;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }

    let cfg = cfg_with_watermark(&lake, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3]);

    // dbt rebuilds from source version 2, and records that.
    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;

    append(&lake.f.source, &[4]).await;
    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 4],
        "row 3 must be re-streamed from dbt's watermark, and row 4 must follow"
    );
}

#[tokio::test]
async fn an_overwrite_with_no_watermark_recorded_is_a_loud_error() {
    // The refusal matters as much as the reset: continuing here would drop rows silently.
    let lake = lake().await;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }

    let cfg = cfg_with_watermark(&lake, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await; // ... and no watermark written

    let Err(e) = Pipeline::open(cfg).await else {
        panic!("a target rebuilt with no watermark recorded must not open cleanly");
    };
    let msg = e.to_string();
    assert!(msg.contains("rewritten"), "got: {msg}");
    assert!(msg.contains("watermark"), "got: {msg}");
    assert!(
        msg.contains("silently drop"),
        "the error must say what it is protecting against: {msg}"
    );
}

#[tokio::test]
async fn an_ordinary_restart_still_uses_our_own_offset() {
    // The watermark must only take over after an actual overwrite. If it applied on every
    // restart, a stale watermark would replay the whole stream and duplicate everything.
    let lake = lake().await;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    // A watermark from an older dbt run is sitting there.
    let cfg = cfg_with_watermark(&lake, "copy");
    record_watermark(&lake.watermark, &cfg.app_id, 0).await;

    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3]);

    // Restart with no overwrite in between.
    let n = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(n, 0, "a plain restart must be a no-op");
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3],
        "the stale watermark must not have replayed anything"
    );
}

#[tokio::test]
async fn compaction_of_the_target_is_not_mistaken_for_a_dbt_rebuild() {
    // OPTIMIZE on the target removes files with dataChange=false. Treating that as a
    // rebuild would reset the offset and duplicate the whole tail.
    let lake = lake().await;
    for i in 1..=4 {
        append(&lake.f.source, &[i]).await;
    }
    // One source commit per batch, so the target ends up with four small files for
    // OPTIMIZE to actually merge. Batched together they would be a single file and the
    // compaction would be a no-op, proving nothing.
    let mut cfg = cfg_with_watermark(&lake, "copy");
    cfg.max_files_per_batch = 1;
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    let t = open(&lake.f.target).await;
    let (_t, stats) = t.optimize().await.unwrap();
    assert!(
        stats.num_files_added > 0 || stats.num_files_removed > 0,
        "optimize was a no-op, so this test proves nothing"
    );

    let target = open(&lake.f.target).await;
    assert_eq!(
        target_state(&target, &cfg.app_id, 10_000).await.unwrap(),
        TargetState::OursOrUntouched,
        "a compaction is not a rebuild"
    );

    let n = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn the_watermark_store_reads_the_highest_version_for_its_own_app_id() {
    let lake = lake().await;
    record_watermark(&lake.watermark, "ddi.other", 999).await;
    record_watermark(&lake.watermark, "ddi.mine", 5).await;
    record_watermark(&lake.watermark, "ddi.mine", 11).await;

    let store = WatermarkStore::new(&lake.watermark);
    assert_eq!(store.last("ddi.mine").await.unwrap(), Some(11));
    assert_eq!(
        store.last("ddi.absent").await.unwrap(),
        None,
        "an unknown app_id has no watermark, rather than borrowing someone else's"
    );
}

#[tokio::test]
async fn the_watermark_store_reads_only_the_files_that_can_hold_its_own_app_id() {
    // The table gains a file with every rebuild of every model, and every model reads it after
    // each of its own rebuilds, so read whole it costs models × nights files per read. Delta's
    // file statistics say which files can hold an app_id's rows. Here another model's file is
    // gone — as a read racing a VACUUM finds it — and this model's read never touches it.
    let lake = lake().await;
    record_watermark(&lake.watermark, "ddi.other", 999).await;
    let others: Vec<_> = std::fs::read_dir(&lake.watermark)
        .unwrap()
        .map(|f| f.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    record_watermark(&lake.watermark, "ddi.mine", 5).await;
    for file in others {
        std::fs::remove_file(file).unwrap();
    }

    let store = WatermarkStore::new(&lake.watermark);
    assert_eq!(store.last("ddi.mine").await.unwrap(), Some(5));
}

#[tokio::test]
async fn the_watermark_store_reads_only_the_partition_of_its_own_app_id() {
    // A table partitioned by app_id reaches the scan with the column as a dictionary of its
    // text, which the store took for an app_id it could not filter on: it read every file of
    // every model at every handover. Here another model's partition has lost its file, as a
    // read racing a VACUUM finds it, and this model's read never touches it.
    let lake = lake().await;
    let watermark = beside_the_target(&lake.f, "ddi_watermark_by_app_id");
    create_watermark_table_partitioned_by(&watermark, &["app_id"]).await;
    record_watermark(&watermark, "ddi.other", 999).await;
    let others = parquet_files(std::path::Path::new(&watermark));
    assert!(
        others
            .iter()
            .all(|f| f.to_string_lossy().contains("app_id=ddi.other")),
        "the premise: a partition per app_id, {others:?}"
    );
    record_watermark(&watermark, "ddi.mine", 5).await;
    record_watermark(&watermark, "ddi.mine", 11).await;
    for file in others {
        std::fs::remove_file(file).unwrap();
    }

    let store = WatermarkStore::new(&watermark);
    assert_eq!(store.last("ddi.mine").await.unwrap(), Some(11));
}

#[tokio::test]
async fn a_recorded_watermark_wins_over_the_timestamp_rescan() {
    // Both set, as every dbt model has them: `ddi_timestamp` defaults to `_timestamp`. The
    // watermark is exact however rows arrive, and the rescan is not: here 3 is a lagging
    // Kafka partition's row, landing after 5, so under the target's max(_timestamp) of 5 it
    // reads as already covered, and its commit is not even re-read.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    // Running before dbt reads, as it would be: a watermark counts when it is newer than what
    // this pipeline recorded at its last handover, or at the open that first recorded one.
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in [1, 5, 3] {
        append(&lake.f.source, &[i]).await; // versions 1, 2, 3
    }
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 3, 5]);

    // dbt rebuilds from source version 2, and records that. Row 3 is gone.
    dbt_rebuild(&lake.f.target, &[1, 5]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    append(&lake.f.source, &[4]).await; // older than 5 too

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(
        p.coverage(),
        None,
        "the rebuild said what the target holds, so nothing is inferred from its data"
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 3, 4, 5],
        "3 re-streamed from dbt's watermark and 4 after it, neither dropped for being older \
         than 5"
    );
}

#[tokio::test]
async fn a_recorded_watermark_wins_over_the_timestamp_rescan_for_an_upsert() {
    // The same under write_mode = upsert, whose own commits remove what they replace, and
    // where a cut-off would filter what is merged: from the watermark there is none, and 3 and
    // 4 land on their keys.
    let lake = lake().await;
    let mut cfg = cfg_with_watermark_and_timestamp(&lake);
    cfg.write_mode = WriteMode::Upsert;
    cfg.upsert_key = Some("id".into());
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in [1, 5, 3] {
        append(&lake.f.source, &[i]).await; // versions 1, 2, 3
    }
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 3, 5]);

    dbt_rebuild(&lake.f.target, &[1, 5]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    append(&lake.f.source, &[4]).await;

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None);
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 3, 4, 5],
        "3 and 4 merged in, neither dropped for being older than 5"
    );
}

#[tokio::test]
async fn no_window_our_last_commit_recorded_outlives_a_watermark_handover() {
    // A batch job loaded the target through source version 2 before ddi first started on it,
    // so ddi opened a first start's window, closing after version 3, and recorded it in its
    // commits. 25 is a lagging partition's row, older than the target's newest. The rebuild
    // then recorded that it read version 2, so version 3 is plainly not the target's: resuming
    // the window would read the rebuilt target's newest, 30, and drop 25 for good.
    //
    // The window was opened before watermark_uri was set, as on a first start since upgrading,
    // so no handover was recorded; the watermark counts because it is the version ddi's own
    // offset is at. A window opened with watermark_uri set reaches the same branch after a
    // source was replaced: see the next test.
    let lake = lake().await;
    append(&lake.f.target, &[10, 20, 30]).await;
    append(&lake.f.source, &[10, 20]).await; // version 1
    append(&lake.f.source, &[30]).await; // version 2
    append(&lake.f.source, &[25]).await; // version 3
    let mut cfg = cfg_with_watermark_and_timestamp(&lake);
    cfg.max_files_per_batch = 1;
    let mut before = cfg.clone();
    before.watermark_uri = None;

    let mut p = Pipeline::open(before).await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(CoverageWindow {
            reason: CoverageReason::Bootstrap,
            through: Some(3),
            resumed: false,
        })
    );
    for version in 1..=2 {
        let step = p.step().await.unwrap();
        assert!(
            matches!(step, StepOutcome::Skipped { .. }),
            "version {version} is the target's already: {step:?}"
        );
    }
    assert!(p.coverage().is_some(), "and the window is still open");
    drop(p);

    dbt_rebuild(&lake.f.target, &[10, 20, 30]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(
        p.coverage(),
        None,
        "the rebuild said what the target holds, so the window our last commit recorded, \
         which described the target before it, is not resumed"
    );
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![10, 20, 25, 30],
        "25 is past what the rebuild read, older than 30 or not"
    );
}

#[tokio::test]
async fn no_window_a_replaced_source_opened_outlives_a_watermark_handover() {
    // Bronze dropped and recreated opens a window that closes only on a row newer than the
    // target's, as its re-seed lands after ddi reopened; here nothing newer has come yet. The
    // rebuild then recorded that it read the new log's version 2, so its version 3 is plainly
    // not the target's: resuming the window would drop 15, a lagging partition's row older
    // than 20, for good.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    append(&lake.f.source, &[10]).await;
    append(&lake.f.source, &[20]).await;
    p.run_until_caught_up().await.unwrap();

    std::fs::remove_dir_all(&lake.f.source).unwrap();
    create_table(&lake.f.source).await;
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    append(&lake.f.source, &[10]).await; // version 1, re-seeded
    append(&lake.f.source, &[20]).await; // version 2, re-seeded
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        p.coverage(),
        Some(CoverageWindow {
            reason: CoverageReason::SourceReplaced,
            through: None,
            resumed: false,
        }),
        "the re-seed held nothing newer than the target"
    );
    drop(p);

    dbt_rebuild(&lake.f.target, &[10, 20]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    append(&lake.f.source, &[15]).await; // version 3

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None, "the rebuild said what the target holds");
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![10, 15, 20]);
}

#[tokio::test]
async fn a_rebuild_that_recorded_no_watermark_falls_back_to_the_timestamp() {
    // The same pair of settings, and a rebuild that wrote nothing to the watermark table on
    // its first night: an empty table. With a timestamp to fall back on, that is no reason to
    // refuse. A table holding only earlier rebuilds' rows is the test after this one.
    let lake = lake().await;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }

    let cfg = cfg_with_watermark_and_timestamp(&lake);
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await; // ... and no watermark written
    append(&lake.f.source, &[4]).await;

    Pipeline::open(cfg)
        .await
        .expect("the rescan needs no watermark")
        .run_until_caught_up()
        .await
        .unwrap();
    let got = read_ids(&lake.f.target).await;
    assert_eq!(got, vec![1, 2, 3, 4], "row 3 recovered by the rescan");
}

#[tokio::test]
async fn a_watermark_table_that_is_not_there_falls_back_to_the_timestamp() {
    // `[storage].watermark_uri` is read for every dbt model, and each has a timestamp. Before
    // it was read for them, a table nobody created, or one since moved, stopped nothing: the
    // rescan answered the rebuild. It still does, where it stopped the model at every rebuild.
    // Without a timestamp there is nothing to fall back on, and the open still refuses.
    let lake = lake().await;
    let mut cfg = cfg_with_watermark_and_timestamp(&lake);
    cfg.watermark_uri = Some(beside_the_target(&lake.f, "never_created"));
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    append(&lake.f.source, &[4]).await;

    let mut without_a_timestamp = cfg.clone();
    without_a_timestamp.dedup_timestamp = None;
    without_a_timestamp.dedup_key = None;
    let Err(e) = Pipeline::open(without_a_timestamp).await else {
        panic!("with nothing to fall back on, a missing watermark table must still refuse");
    };
    assert!(e.to_string().contains("must exist"), "got: {e}");

    Pipeline::open(cfg)
        .await
        .expect("the rescan needs no watermark table")
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 4],
        "row 3 recovered by the rescan"
    );
}

#[tokio::test]
async fn a_watermark_table_of_another_shape_falls_back_to_the_timestamp() {
    // What a warehouse writes when the table is declared with an INTEGER source_version: a
    // row the store refuses to read, at every retry.
    let lake = lake().await;
    let mut cfg = cfg_with_watermark_and_timestamp(&lake);
    let watermark = beside_the_target(&lake.f, "ddi_watermark_int");
    cfg.watermark_uri = Some(watermark.clone());
    let int_typed = Arc::new(Schema::new(vec![
        Field::new("app_id", DataType::Utf8, false),
        Field::new("source_version", DataType::Int32, false),
    ]));
    let row = RecordBatch::try_new(
        int_typed,
        vec![
            Arc::new(StringArray::from(vec![cfg.app_id.as_str()])) as ArrayRef,
            Arc::new(deltalake::arrow::array::Int32Array::from(vec![2])) as ArrayRef,
        ],
    )
    .unwrap();
    DeltaTable::try_from_url(ensure_table_uri(&watermark).unwrap())
        .await
        .unwrap()
        .write(vec![row])
        .await
        .unwrap();

    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    append(&lake.f.source, &[4]).await;

    Pipeline::open(cfg)
        .await
        .expect("an INTEGER source_version is refused, and the rescan needs none")
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 4],
        "row 3 recovered by the rescan"
    );
}

#[tokio::test]
async fn a_watermark_table_that_cannot_be_read_stops_the_open_rather_than_falling_back() {
    // A read that failed says nothing about what the table holds, and the rescan's cut-off
    // would drop for good the late rows the watermark kept. Here the file holding this
    // rebuild's row is gone, as a read racing a VACUUM finds it: the open fails, to be retried,
    // rather than taking the rescan.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    for file in std::fs::read_dir(&lake.watermark).unwrap() {
        let path = file.unwrap().path();
        if path.extension().is_some_and(|e| e == "parquet") {
            std::fs::remove_file(path).unwrap();
        }
    }

    let Err(e) = Pipeline::open(cfg).await else {
        panic!("a watermark table that could not be read must not fall back to the rescan");
    };
    assert!(
        e.to_string().contains("cannot read watermark table"),
        "got: {e}"
    );
}

#[tokio::test]
async fn a_watermark_an_earlier_rebuild_recorded_does_not_stand_for_this_one() {
    // The table only grows, so its newest row can be last night's: here the post-hook has not
    // run yet when ddi reopens. Taken for this rebuild's, it appended again every row since
    // last night on top of a rebuild that already held them, and ddi's next commit hid the
    // rebuild, so the row the post-hook then wrote was never read.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await; // versions 1..=3
    }
    p.run_until_caught_up().await.unwrap();

    // Night 1: dbt rebuilds from source version 2 and records it.
    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    append(&lake.f.source, &[4]).await; // version 4
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 5..=6 {
        append(&lake.f.source, &[i]).await; // versions 5, 6
    }
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 4, 5, 6]);

    // Night 2: dbt rebuilds from source version 6, and ddi reopens before the post-hook
    // records it. The newest row is night 1's.
    dbt_rebuild(&lake.f.target, &[1, 2, 3, 4, 5, 6]).await;
    append(&lake.f.source, &[7]).await; // version 7
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    let got = read_ids(&lake.f.target).await;
    assert_eq!(
        got,
        vec![1, 2, 3, 4, 5, 6, 7],
        "the rescan's cut-off, not night 1's watermark: 3 to 6 are not appended again"
    );

    // The post-hook lands late. Night 3 records its version before reading it, as a pre-hook
    // does, and ddi streams 9 while dbt runs: that row counts, and 9 is re-streamed.
    record_watermark(&lake.watermark, &cfg.app_id, 6).await;
    append(&lake.f.source, &[8]).await; // version 8
    p.run_until_caught_up().await.unwrap();
    record_watermark(&lake.watermark, &cfg.app_id, 8).await;
    append(&lake.f.source, &[9]).await; // version 9
    p.run_until_caught_up().await.unwrap();
    dbt_rebuild(&lake.f.target, &[1, 2, 3, 4, 5, 6, 7, 8]).await;

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None, "resumed from night 3's watermark");
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9]
    );
}

#[tokio::test]
async fn a_delete_after_a_handover_is_not_undone_by_the_watermark_before_it() {
    // Another writer's DELETE on the target reads like a rebuild, and records no watermark.
    // The newest row is then the last rebuild's, and resuming from it appended again every
    // row since, the deleted one included.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 4..=6 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 4, 5, 6]);

    // A GDPR delete, and a restart before ddi commits again.
    let (_t, m) = open(&lake.f.target)
        .await
        .delete()
        .with_predicate("id = 4")
        .await
        .unwrap();
    assert!(m.num_deleted_rows.unwrap_or(0) > 0, "nothing was deleted");
    let mut p = Pipeline::open(cfg).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 5, 6],
        "4 stays deleted, and nothing is written twice"
    );

    append(&lake.f.source, &[7]).await;
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 5, 6, 7]);
}

#[tokio::test]
async fn a_rewrite_after_upgrading_does_not_take_the_last_rebuilds_watermark_for_its_own() {
    // Commits made by ddi 0.3.1, or while watermark_uri was unset, record no handover, so there
    // is nothing to tell the last rebuild's row from a rewrite's by. 0.3.1 never read the table
    // where a timestamp was set, and rescanned; the upgrade took the newest row for the
    // rewrite's own. Here that is the last rebuild's, and the rewrite a GDPR delete made while
    // ddi was stopped for the upgrade: resuming from it appended again every row since, the
    // deleted one included.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut before = cfg.clone();
    before.watermark_uri = None;
    for i in 1..=3 {
        append(&lake.f.source, &[i]).await;
    }
    Pipeline::open(before.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&lake.f.target, &[1, 2]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    let mut p = Pipeline::open(before).await.unwrap();
    for i in 4..=6 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 4, 5, 6]);

    delete_from(&lake.f.target, "id = 4").await;
    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 5, 6],
        "4 stays deleted, and nothing is written twice"
    );
}

#[tokio::test]
async fn a_rebuild_of_the_head_the_last_handover_opened_at_is_not_taken_for_an_earlier_one() {
    // ddi handed over at source head 3 from a rebuild that had read 2. The next rebuild read 3,
    // nothing having landed since, and ddi streamed 4 while it ran. ddi had recorded the head
    // it handed over at, so that watermark read as the earlier rebuild's: it rescanned, and
    // the cut-off dropped 4 and the late 2, older than 5, where the watermark says they were
    // never in the rebuild.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in [1, 5, 3] {
        append(&lake.f.source, &[i]).await; // versions 1, 2, 3
    }
    p.run_until_caught_up().await.unwrap();

    dbt_rebuild(&lake.f.target, &[1, 5]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 3, 5]);

    // The next rebuild records version 3 before it reads it, and 4 lands before it commits.
    record_watermark(&lake.watermark, &cfg.app_id, 3).await;
    append(&lake.f.source, &[4]).await; // version 4
    p.run_until_caught_up().await.unwrap();
    dbt_rebuild(&lake.f.target, &[1, 3, 5]).await;
    append(&lake.f.source, &[2]).await; // version 5

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None, "resumed from the rebuild's watermark");
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn a_rebuild_of_a_source_that_had_nothing_new_is_followed_after_a_rescan() {
    // ddi rescanned after a rebuild whose post-hook had not run, and recorded the head it
    // read then, 3. The next rebuild found nothing new either and recorded 3 again, no newer
    // than that — and taken for an earlier rebuild's, it sent ddi into the rescan, whose
    // cut-off dropped 4, landing after it and older than 6. It is also the version ddi's own
    // offset is at, though, and resuming from it is resuming from that offset: whichever
    // rebuild recorded it, nothing is appended twice and nothing the rebuild wiped is skipped.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in [1, 5] {
        append(&lake.f.source, &[i]).await; // versions 1, 2
    }
    p.run_until_caught_up().await.unwrap();

    dbt_rebuild(&lake.f.target, &[1, 5]).await; // its post-hook has not run
    append(&lake.f.source, &[6]).await; // version 3
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 5, 6]);
    record_watermark(&lake.watermark, &cfg.app_id, 2).await; // the post-hook

    record_watermark(&lake.watermark, &cfg.app_id, 3).await;
    dbt_rebuild(&lake.f.target, &[1, 5, 6]).await;
    append(&lake.f.source, &[4]).await; // version 4

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None, "resumed from the rebuild's watermark");
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 4, 5, 6]);
}

#[tokio::test]
async fn a_restart_between_rebuilds_keeps_what_the_last_handover_recorded() {
    // A deploy or an OOM kill between two rebuilds reopens without handing over, and its
    // commits carry on recording what the last handover did, not the head the restart opened
    // at: the next rebuild can have read a version between the two, and taken for an earlier
    // rebuild's, its watermark would be passed over for the rescan, whose cut-off drops 4.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in [1, 5, 3] {
        append(&lake.f.source, &[i]).await; // versions 1, 2, 3
    }
    p.run_until_caught_up().await.unwrap();

    dbt_rebuild(&lake.f.target, &[1, 5]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    append(&lake.f.source, &[6]).await; // version 4
    append(&lake.f.source, &[7]).await; // version 5
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 3, 5, 6, 7]);

    // The next rebuild read version 4.
    record_watermark(&lake.watermark, &cfg.app_id, 4).await;
    dbt_rebuild(&lake.f.target, &[1, 3, 5, 6]).await;
    append(&lake.f.source, &[4]).await; // version 6

    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(p.coverage(), None, "resumed from the rebuild's watermark");
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 3, 4, 5, 6, 7]);
}

#[tokio::test]
async fn a_watermark_recorded_before_bronze_was_recreated_does_not_outrank_the_new_ones() {
    // Bronze dropped and recreated starts its log again at 0, and the watermark table still
    // holds what the old log's rebuilds recorded, here 5. ddi recorded the new log's head at
    // its first rescan since, below that, so at the next rebuild the old row counted as the
    // new one's: ddi resumed past version 5 of the new log, which that rebuild had wiped, and
    // 10 was never read again.
    let lake = lake().await;
    let cfg = cfg_with_watermark_and_timestamp(&lake);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 1..=5 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();
    dbt_rebuild(&lake.f.target, &[1, 2, 3, 4, 5]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 5).await;
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    std::fs::remove_dir_all(&lake.f.source).unwrap();
    create_table(&lake.f.source).await;
    append(&lake.f.source, &[6]).await; // version 1 of the new log
    append(&lake.f.source, &[7]).await; // version 2
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    // A rebuild from the new log's version 2, whose row is below the old log's.
    dbt_rebuild(&lake.f.target, &[1, 2, 3, 4, 5, 6, 7]).await;
    record_watermark(&lake.watermark, &cfg.app_id, 2).await;
    append(&lake.f.source, &[8]).await; // version 3
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    p.run_until_caught_up().await.unwrap();

    // The next reads version 4, and ddi streams 5 and 6 before it commits.
    append(&lake.f.source, &[9]).await; // version 4
    p.run_until_caught_up().await.unwrap();
    record_watermark(&lake.watermark, &cfg.app_id, 4).await;
    append(&lake.f.source, &[10]).await; // version 5
    append(&lake.f.source, &[11]).await; // version 6
    p.run_until_caught_up().await.unwrap();
    dbt_rebuild(&lake.f.target, &[1, 2, 3, 4, 5, 6, 7, 8, 9]).await;

    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        (1..=11).collect::<Vec<_>>(),
        "10 and 11, which the rebuild wiped, re-streamed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rebuild_that_read_past_the_source_this_open_loaded_is_followed() {
    // dbt recorded a version, read it and overwrote the target while ddi was opening: after it
    // had loaded bronze, before it listed the target. The watermark named a version past the
    // head ddi had loaded, which read as bronze's log having gone backwards: ddi took bronze
    // for a table dropped and recreated, and read all of it again under a cut-off that dropped
    // the late 4.
    let lake = lake().await;
    let mut cfg = cfg_with_watermark_and_timestamp(&lake);
    cfg.target_uri = hooked(&lake.f.target);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in [1, 5] {
        append(&lake.f.source, &[i]).await; // versions 1, 2
    }
    p.run_until_caught_up().await.unwrap();

    let (source, target, watermark, app_id) = (
        lake.f.source.clone(),
        lake.f.target.clone(),
        lake.watermark.clone(),
        cfg.app_id.clone(),
    );
    before_next_read(&lake.f.target, async move {
        append(&source, &[3]).await; // version 3
        record_watermark(&watermark, &app_id, 3).await;
        dbt_rebuild(&target, &[1, 3, 5]).await;
    });
    let mut p = Pipeline::open(cfg).await.unwrap();
    assert_eq!(
        p.coverage(),
        None,
        "resumed from the rebuild's watermark, reading bronze on as the table it was"
    );
    append(&lake.f.source, &[4]).await; // version 4
    p.run_until_caught_up().await.unwrap();
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 3, 4, 5]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rebuild_that_read_past_the_source_this_open_loaded_is_not_taken_for_a_later_one() {
    // The same moment, with a post-hook: the rebuild has recorded nothing yet, so ddi rescans,
    // and records how far that rebuild can have read. It recorded the head it had loaded,
    // below the version the post-hook then wrote, so after another writer's DELETE that row
    // read as a later rebuild's: ddi resumed from it and appended again every row since, the
    // deleted one included.
    let lake = lake().await;
    let mut cfg = cfg_with_watermark_and_timestamp(&lake);
    cfg.target_uri = hooked(&lake.f.target);
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    for i in 1..=2 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();

    let (source, target) = (lake.f.source.clone(), lake.f.target.clone());
    before_next_read(&lake.f.target, async move {
        append(&source, &[3]).await; // version 3
        dbt_rebuild(&target, &[1, 2, 3]).await;
    });
    let mut p = Pipeline::open(cfg.clone()).await.unwrap();
    record_watermark(&lake.watermark, &cfg.app_id, 3).await; // the post-hook
    for i in 4..=6 {
        append(&lake.f.source, &[i]).await;
    }
    p.run_until_caught_up().await.unwrap();
    // 3 twice: the rescan's window closes after the head this open loaded, and a rebuild that
    // read past it errs towards the duplicate, which the next rebuild erases.
    assert_eq!(read_ids(&lake.f.target).await, vec![1, 2, 3, 3, 4, 5, 6]);

    delete_from(&lake.f.target, "id = 4").await;
    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(
        read_ids(&lake.f.target).await,
        vec![1, 2, 3, 3, 5, 6],
        "4 stays deleted, and nothing more is written twice"
    );
}

// ------------------------------------------------------- zero-cooperation dedup

/// The mode where the rebuilding writer knows nothing about `ddi`: no watermark table,
/// no hooks. `ddi` reads how far the target already reaches and emits only what lies
/// beyond it.
///
/// These fixtures have no timestamp column, so `id` plays that role — the mechanism only
/// needs a column that increases with arrival order, whatever its type.
fn cfg_with_dedup_key(f: &Fixture, name: &str) -> ResolvedPipeline {
    let mut c = f.cfg(name);
    c.dedup_timestamp = Some("id".into());
    c.dedup_key = Some("id".into());
    c
}

#[tokio::test]
async fn a_dedup_key_needs_no_cooperation_from_the_rebuilding_writer() {
    let f = Fixture::new().await;
    for i in 1..=3 {
        append(&f.source, &[i]).await;
    }

    let cfg = cfg_with_dedup_key(&f, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3]);

    // A vanilla rebuild: it overwrites the target and records nothing anywhere. It read
    // the source as of version 2, so row 3 -- which ddi had already streamed -- is gone.
    dbt_rebuild(&f.target, &[1, 2]).await;

    append(&f.source, &[4]).await;
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    let got = read_ids(&f.target).await;
    assert_eq!(
        got,
        vec![1, 2, 3, 4],
        "row 3 recovered from the rescan, row 4 streamed normally"
    );
    assert!(
        !has_duplicates(&got),
        "and 1 and 2 not written twice: {got:?}"
    );
}

#[tokio::test]
async fn a_rebuild_that_overtook_the_stream_does_not_duplicate_its_rows() {
    // The other direction: dbt is *ahead* of ddi. Everything ddi has yet to read is
    // already in the target, so a rescan must emit nothing at all.
    let f = Fixture::new().await;
    for i in 1..=2 {
        append(&f.source, &[i]).await;
    }

    let cfg = cfg_with_dedup_key(&f, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    // More source arrives, and dbt rebuilds covering all of it before ddi gets there.
    for i in 3..=5 {
        append(&f.source, &[i]).await;
    }
    dbt_rebuild(&f.target, &[1, 2, 3, 4, 5]).await;

    let n = Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    let got = read_ids(&f.target).await;
    assert_eq!(got, vec![1, 2, 3, 4, 5], "no key reprocessed: {got:?}");
    assert!(!has_duplicates(&got));
    assert_eq!(
        n, 0,
        "the rescan is bounded by the source's file statistics, so there was nothing left \
         to read at all — not merely nothing left to emit"
    );
}

#[tokio::test]
async fn an_empty_target_after_a_rebuild_streams_everything() {
    // max(key) over an empty table is NULL, which must mean "nothing covered", not
    // "everything covered".
    let f = Fixture::new().await;
    for i in 1..=3 {
        append(&f.source, &[i]).await;
    }
    let cfg = cfg_with_dedup_key(&f, "copy");
    Pipeline::open(cfg.clone())
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();

    dbt_rebuild(&f.target, &[]).await; // a rebuild that produced no rows

    Pipeline::open(cfg)
        .await
        .unwrap()
        .run_until_caught_up()
        .await
        .unwrap();
    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3]);
}
