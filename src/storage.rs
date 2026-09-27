//! Opening tables, wherever they live.
//!
//! Every table this tool touches is named by URI, and the scheme decides the backend:
//! a bare path or `file://` for local disk, `abfss://` or `az://` for Azure. The handlers
//! register themselves when the corresponding feature is compiled in, so adding a cloud
//! is a feature flag and a set of credentials — no code above this module changes.
//!
//! Credentials are the one thing a dbt project cannot tell us: it knows *which* table,
//! never *how to authenticate*. So they live in `[storage].options` and are threaded to
//! every table open from here.
//!
//! # Azure
//!
//! ```toml
//! [storage.options]
//! azure_storage_account_name = "mylake"
//! azure_storage_account_key  = "..."       # or one of the alternatives below
//! ```
//!
//! Recognised alternatives, in the object-store spelling: `azure_storage_sas_key`,
//! `azure_storage_token`, `azure_client_id` + `azure_client_secret` + `azure_tenant_id`
//! for a service principal, or `azure_use_azure_cli = "true"`. Setting
//! `azure_msi_endpoint`, or nothing at all on a machine with a managed identity, uses
//! that. The same keys are read from the environment in upper case
//! (`AZURE_STORAGE_ACCOUNT_NAME`), which is usually how a container gets them.
//!
//! URIs take either shape:
//!
//! ```text
//! abfss://container@account.dfs.core.windows.net/path/to/table
//! az://container/path/to/table          # account from the options
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

// arrow / parquet / object_store are reached through the deltalake re-exports, never as
// direct dependencies — see the note in Cargo.toml.
use deltalake::logstore::object_store;
use deltalake::{ensure_table_uri, open_table_with_storage_options, DeltaTable};
use tracing::warn;

use crate::error::{Error, Result};

/// How to reach object storage. Cheap to clone; it is a credential bag, not a connection.
#[derive(Clone, Debug, Default)]
pub struct Storage {
    options: HashMap<String, String>,
}

impl Storage {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self { options }
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    /// Check that a URI's backend is compiled in and its credentials assemble, without
    /// touching storage.
    ///
    /// Resolving the log store builds the object store and stops there — no request is
    /// made. That makes it something `ddi validate` can do for every pipeline before a
    /// daemon starts: a missing feature or an unparseable account is a startup problem,
    /// and finding it at startup beats finding it on the first batch.
    pub fn check(&self, uri: &str) -> Result<()> {
        use deltalake::logstore::{logstore_for, StorageConfig};

        let url = ensure_table_uri(uri)
            .map_err(|e| Error::Config(format!("{uri:?} is not a usable table URI: {e}")))?;
        let scheme = url.scheme().to_string();
        let config = StorageConfig::parse_options(self.options.clone()).map_err(|e| {
            Error::Config(format!(
                "storage options are not usable for {uri:?}: {e}{}",
                hint(&scheme, self.options.is_empty())
            ))
        })?;
        logstore_for(&url, config).map_err(|e| {
            Error::Config(format!(
                "no storage backend for {uri:?}: {e}{}",
                hint(&scheme, self.options.is_empty())
            ))
        })?;
        Ok(())
    }

    /// Open a Delta table by URI.
    ///
    /// The error names the scheme, because "not found" for `abfss://…` almost always
    /// means a credential problem or a build without the feature, not a missing table.
    ///
    /// A checkpoint another engine wrote is not allowed to decide whether the table opens:
    /// see [`Self::open_replaying_the_log`].
    pub async fn open(&self, uri: &str) -> Result<DeltaTable> {
        let url = ensure_table_uri(uri)
            .map_err(|e| Error::Config(format!("{uri:?} is not a usable table URI: {e}")))?;
        let scheme = url.scheme().to_string();
        let first = match open_table_with_storage_options(url.clone(), self.options.clone()).await {
            Ok(t) => return Ok(t),
            Err(e) => e,
        };

        if is_unreadable_checkpoint(&first) {
            self.say_once(uri).await;
            match self.open_replaying_the_log(&url).await {
                Ok(t) => return Ok(t),
                Err(second) => {
                    // Almost always one thing: log retention has already removed the
                    // commits the checkpoint stands in for, so the checkpoint is the only
                    // record of the table's early history and there is nothing to replay.
                    // Naming it matters, because the fix is not on this side.
                    return Err(Error::Config(format!(
                        "cannot open {uri:?}: {first}. That checkpoint was written by another \
                         engine at a precision the Delta protocol does not have, so it was \
                         ignored in favour of replaying the commit log — and that did not \
                         work either: {second}. The usual reason is that the commits the \
                         checkpoint stands in for have already been removed by log \
                         retention, leaving it as the only record of them. Have the engine \
                         that owns compaction write a fresh checkpoint (any writer that \
                         emits microsecond timestamps will do, including a delta-rs \
                         OPTIMIZE), or restore the table's log.{}",
                        hint(&scheme, self.options.is_empty())
                    )));
                }
            }
        }

        Err(Error::Config(format!(
            "cannot open {uri:?}: {first}{}",
            hint(&scheme, self.options.is_empty())
        )))
    }

    /// Open the snapshot selected by Delta's timestamp rule.
    ///
    /// [`Self::open`] only has to read the checkpoint at the current head. A historic lookup
    /// can legitimately need an older snapshot whose checkpoint is foreign and unreadable even
    /// when the head opens normally, so the same replay fallback has to cover the time-travel
    /// load itself. This keeps a pinned lookup from failing only when an old source batch is
    /// retried.
    /// Create `uri` with the schema of `model_uri` if it is not already a Delta table.
    ///
    /// Used for one thing only: the staging table of a `staged_upsert`, whose schema is by
    /// definition the target's. This tool otherwise never creates a table — the target and
    /// the data-quality table are both somebody else's to declare — and the exception is
    /// narrow on purpose. A stage is not a table anyone queries or models; it is this
    /// pipeline's own working space, its location is derived rather than typed, and
    /// requiring an operator to hand-write DDL for it would be asking them to restate the
    /// target's schema in a second place that can then disagree with the first.
    ///
    /// Existing tables are left exactly as they are, including their schema: a stage that
    /// has drifted from its target is a real problem, and silently rewriting it here would
    /// destroy the evidence of whichever change caused it.
    pub async fn create_like(&self, uri: &str, model_uri: &str) -> Result<()> {
        if self.open(uri).await.is_ok() {
            return Ok(());
        }
        let model = self.open(model_uri).await?;
        let columns = model
            .snapshot()
            .map_err(Error::Delta)?
            .schema()
            .fields()
            .cloned()
            .collect::<Vec<_>>();

        let url = ensure_table_uri(uri)
            .map_err(|e| Error::Config(format!("{uri:?} is not a usable table URI: {e}")))?;
        deltalake::DeltaTable::try_from_url(url)
            .await
            .map_err(Error::Delta)?
            .create()
            .with_columns(columns)
            .with_storage_options(self.options.clone())
            // Another process in the same fleet may be opening the same pipeline at the same
            // instant. Losing that race is not an error — the table it wanted now exists.
            .with_save_mode(deltalake::protocol::SaveMode::Ignore)
            .await
            .map_err(|e| {
                Error::Config(format!(
                    "cannot create the staging table {uri:?} from the schema of \
                     {model_uri:?}: {e}"
                ))
            })?;
        Ok(())
    }

    pub async fn open_at_datetime(
        &self,
        uri: &str,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<DeltaTable> {
        let url = ensure_table_uri(uri)
            .map_err(|e| Error::Config(format!("{uri:?} is not a usable table URI: {e}")))?;
        let mut table = self.open(uri).await?;
        match table.load_with_datetime(timestamp).await {
            Ok(()) => Ok(table),
            Err(first) if is_unreadable_checkpoint(&first) => {
                self.say_once(uri).await;
                let mut replay = self.open_replaying_the_log(&url).await.map_err(|second| {
                    Error::Config(format!(
                        "cannot load lookup {uri:?} at {timestamp}: {first}. Replaying its \
                         log to bypass the foreign checkpoint also failed: {second}"
                    ))
                })?;
                replay
                    .load_with_datetime(timestamp)
                    .await
                    .map_err(Error::Delta)?;
                Ok(replay)
            }
            Err(e) => Err(Error::Delta(e)),
        }
    }

    /// Open one historic Delta version, including the checkpoint-replay fallback needed when a
    /// lookup's selected version predates a foreign checkpoint at the current head.
    pub async fn open_at_version(
        &self,
        uri: &str,
        version: crate::source::Version,
    ) -> Result<DeltaTable> {
        let url = ensure_table_uri(uri)
            .map_err(|e| Error::Config(format!("{uri:?} is not a usable table URI: {e}")))?;
        let mut table = self.open(uri).await?;
        match table.load_version(version).await {
            Ok(()) => Ok(table),
            Err(first) if is_unreadable_checkpoint(&first) => {
                self.say_once(uri).await;
                let mut replay = self.open_replaying_the_log(&url).await.map_err(|second| {
                    Error::Config(format!(
                        "cannot load lookup {uri:?} at version {version}: {first}. Replaying \
                         its log to bypass the foreign checkpoint also failed: {second}"
                    ))
                })?;
                replay.load_version(version).await.map_err(Error::Delta)?;
                Ok(replay)
            }
            Err(e) => Err(Error::Delta(e)),
        }
    }

    /// Open a table from its commit log alone, as if it had no checkpoint.
    ///
    /// The checkpoint is a derived cache of the commits, so a snapshot built without it is
    /// the same snapshot — only slower to arrive at, because every commit since version 0
    /// has to be read. That is the whole trade, and it is worth making: a checkpoint this
    /// build cannot parse is not a reason to refuse a table whose log is perfectly readable.
    ///
    /// Nothing else is lost. The one thing a checkpoint carries that the commits do not is
    /// `stats_parsed`, a pre-decoded copy of each file's statistics — and this tool never
    /// reads it. [`crate::stats`] parses the `stats` JSON string that both the commits and
    /// the checkpoint carry verbatim.
    ///
    /// Implemented by hiding the checkpoint from the object store rather than by asking
    /// delta-rs not to use one, because delta-rs has no such option: suppressing
    /// `_last_checkpoint` alone is not enough, since the kernel also finds checkpoints by
    /// listing `_delta_log`.
    ///
    /// # What this does not cover
    ///
    /// The version it opens at, and no other. A table compacted more than once carries
    /// older checkpoints too, and the newest being readable says nothing about them — so a
    /// *later* load of an *earlier* version can still meet one, on a handle that opened
    /// perfectly well. The answer is not to widen this: it is for such a load not to need
    /// files, because a checkpoint's file actions are the only part delta-rs parses eagerly.
    /// See [`crate::source::log_stream`], where both version-targeted loads ask the log
    /// about itself and say so.
    ///
    /// # Why version 0 is checked first
    ///
    /// "The same snapshot, only slower" holds only while every commit still exists. Once a
    /// checkpoint has been written, the protocol *permits deleting the commits below it*,
    /// and Delta-Spark does exactly that by default after `delta.logRetentionDuration` — on
    /// a table old enough to have been compacted, which is precisely this one.
    ///
    /// The kernel does not catch that. It insists a log segment be contiguous and that a
    /// checkpoint have no gap after it, but **not** that a checkpoint-less segment start at
    /// version 0. Hand it commits `[71..head]` with the checkpoint hidden and it builds a
    /// perfectly valid-looking snapshot of the last thirty commits, recovering protocol and
    /// metadata from a `.crc` file or any in-range `metaData` action. The table opens, the
    /// rows below version 71 are simply gone, and nothing says so.
    ///
    /// That is far worse than the failure this replaces, and it does not stay a read
    /// problem: the handle is the one the sink commits through, and delta-rs writes a
    /// checkpoint of its own every `delta.checkpointInterval` versions — which would record
    /// the truncated file set as fact, for every engine, permanently.
    ///
    /// So the replay is proved equivalent before it is trusted, by one request: if commit 0
    /// is still there, contiguity does the rest. If it is not, this refuses, and the caller
    /// reports the original failure with its cause named.
    async fn open_replaying_the_log(&self, url: &url::Url) -> Result<DeltaTable> {
        use deltalake::logstore::{logstore_for, logstore_with, StorageConfig};
        use deltalake::{DeltaTable, DeltaTableConfig};
        use std::sync::Arc;

        let config = || {
            StorageConfig::parse_options(self.options.clone())
                .map_err(|e| Error::Config(format!("storage options are not usable: {e}")))
        };

        // Built once to get at the object store the credentials produce, then rebuilt
        // around a filtered view of it. Neither step touches storage.
        let plain = logstore_for(url, config()?).map_err(Error::Delta)?;

        if plain.read_commit_entry(0).await?.is_none() {
            return Err(Error::Other(
                "the log no longer reaches back to version 0, so replaying it would build a \
                 snapshot of only the commits that survive — a table missing every row \
                 written before them, with nothing to say so"
                    .into(),
            ));
        }

        let filtered = Arc::new(WithoutCheckpoints::new(plain.root_object_store(None)));
        let log_store = logstore_with(filtered, url, config()?).map_err(Error::Delta)?;

        let mut table = DeltaTable::new(log_store, DeltaTableConfig::default());
        table.load().await.map_err(Error::Delta)?;
        Ok(table)
    }

    /// Say once per table that its checkpoint is being ignored, and by whom it was written.
    ///
    /// Once, because `open` is also the recovery path after a failed commit, and a line per
    /// retry would bury the batches around it. The writer's name is read out of the
    /// checkpoint's own parquet footer, so an operator learns *which* engine did it rather
    /// than having to infer it from the schedule.
    async fn say_once(&self, uri: &str) {
        if !first_mention_of(uri) {
            return;
        }
        let writer = self
            .checkpoint_writer(uri)
            .await
            .unwrap_or_else(|| "an engine that did not name itself".to_string());
        warn!(
            target_uri = %uri,
            checkpoint_writer = %writer,
            "{uri:?} carries a checkpoint written by {writer} whose stats_parsed types do not \
             match the table schema; replaying the log instead. This is usually an OPTIMIZE \
             from another engine. Opening is slower and the result is identical — this tool \
             reads the stats JSON, never stats_parsed."
        );
    }

    /// Whatever the newest checkpoint's parquet footer says wrote it, best effort.
    ///
    /// Best effort throughout: this runs only to make a warning more useful, so every step
    /// that could fail simply gives up and lets the caller say "unnamed" instead.
    async fn checkpoint_writer(&self, uri: &str) -> Option<String> {
        use deltalake::parquet::arrow::async_reader::{
            ParquetObjectReader, ParquetRecordBatchStreamBuilder,
        };
        use futures::TryStreamExt;

        let url = ensure_table_uri(uri).ok()?;
        let config =
            deltalake::logstore::StorageConfig::parse_options(self.options.clone()).ok()?;
        let log_store = deltalake::logstore::logstore_for(&url, config).ok()?;
        let store = log_store.object_store(None);

        let mut newest: Option<deltalake::ObjectMeta> = None;
        let mut listing = store.list(Some(log_store.log_path()));
        while let Ok(Some(meta)) = listing.try_next().await {
            if !is_checkpoint_artefact(&meta.location) {
                continue;
            }
            if meta
                .location
                .filename()
                .is_some_and(|f| f.ends_with(".parquet"))
                && newest.as_ref().is_none_or(|n| n.location < meta.location)
            {
                newest = Some(meta);
            }
        }
        let newest = newest?;

        let reader = ParquetObjectReader::new(store, newest.location).with_file_size(newest.size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await.ok()?;
        builder
            .metadata()
            .file_metadata()
            .created_by()
            .map(str::to_string)
    }
}

/// True the first time a table is named, false ever after.
///
/// Process-wide, because the `Storage` that asks is cloned into every pipeline and reopened
/// on every recovery, and the point is one line per table rather than one per open.
fn first_mention_of(uri: &str) -> bool {
    static SAID: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    SAID.get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .expect("checkpoint-warning set is never poisoned")
        .insert(uri.to_string())
}

/// Is this the failure of a checkpoint this build cannot parse?
///
/// Deliberately narrow. Replaying the log is always *correct*, so a wider net would not
/// give a wrong answer — it would double the time every genuine failure takes to report,
/// which on a fleet of stalled pipelines is the difference between a legible incident and
/// a slow one. So this matches the one shape that is known to be recoverable that way:
/// a checkpoint whose parsed statistics carry a physical type the Delta protocol does not
/// have, which is what Trino's `OPTIMIZE` writes when a column is a `timestamp`.
///
/// The kernel reports it as an Arrow schema error several layers of `External error:` down,
/// so the text is all there is to match on.
pub fn is_unreadable_checkpoint(e: &deltalake::DeltaTableError) -> bool {
    let msg = e.to_string();
    msg.contains("Invalid data type for Delta Lake")
        || (msg.contains("stats_parsed") && msg.contains("Schema error"))
}

/// True for the files that make up a table's checkpoint, and nothing else.
///
/// Matches on the `_delta_log` directory rather than on the whole path, because the store
/// this filters is rooted at the *bucket*, so every path carries the table's prefix too.
fn is_checkpoint_artefact(path: &deltalake::Path) -> bool {
    let parts: Vec<String> = path.parts().map(|p| p.as_ref().to_string()).collect();
    let Some((name, dirs)) = parts.split_last() else {
        return false;
    };
    match dirs.last().map(String::as_str) {
        // `_last_checkpoint`, and every spelling of a checkpoint file: single-part
        // (`…0.checkpoint.parquet`), multi-part (`…0.checkpoint.0000000001.0000000010.parquet`)
        // and V2 (`…0.checkpoint.<uuid>.json`).
        Some("_delta_log") => name.starts_with("_last_checkpoint") || name.contains(".checkpoint."),
        // A sidecar exists only to be referenced by a checkpoint, so it goes with it.
        Some("_sidecars") => dirs.len() >= 2 && dirs[dirs.len() - 2] == "_delta_log",
        _ => false,
    }
}

/// An object store with a table's *existing* checkpoints edited out of it.
///
/// Everything else is passed straight through. Reads of a hidden checkpoint report "not
/// found" and listings omit it, which is all it takes for the kernel to fall back to
/// replaying the commit log — the same effect as deleting the files, without touching
/// anybody's table. The three ways a checkpoint is reached are all covered:
/// `_last_checkpoint` (a read), the listing of `_delta_log` (a list), and `_sidecars`
/// (both).
///
/// # Why "existing" is the whole of it
///
/// The handle `open` returns is not read-only. It is the one the sink commits through, and
/// delta-rs writes a checkpoint of its own as a post-commit hook every
/// `delta.checkpointInterval` versions — writing the parquet, then **reading it back** to
/// record its size in `_last_checkpoint`. A wrapper that hid checkpoints unconditionally
/// would fail that read, take the post-commit hook down with it, and turn a table that
/// could not be *opened* into one that could not be *written* — a far worse failure than
/// the one this exists to avoid.
///
/// So the rule is narrower than "no checkpoints": a checkpoint written *through this store*
/// is ours, is known to be well-typed, and stays visible. Only the ones that were already
/// there are hidden, which is exactly the set the fallback was opened to get past.
///
/// It also means the table heals. Once delta-rs has written a checkpoint of its own, it
/// supersedes the unreadable one and the ordinary open works again.
#[derive(Debug)]
struct WithoutCheckpoints {
    inner: deltalake::logstore::ObjectStoreRef,
    /// Checkpoint artefacts written through this store, which are therefore not the ones
    /// being hidden. Bounded by how many checkpoints one open handle writes — a handful
    /// over a process lifetime, not a per-request cost. Shared rather than borrowed because
    /// `list` returns a `'static` stream that has to consult it.
    ours: Arc<Mutex<HashSet<deltalake::Path>>>,
}

/// Is this one of the checkpoints that was already there when the table was opened?
fn hidden(ours: &Mutex<HashSet<deltalake::Path>>, path: &deltalake::Path) -> bool {
    is_checkpoint_artefact(path)
        && !ours
            .lock()
            .expect("the written-checkpoint set is never poisoned")
            .contains(path)
}

impl WithoutCheckpoints {
    fn new(inner: deltalake::logstore::ObjectStoreRef) -> Self {
        Self {
            inner,
            ours: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn hidden(&self, path: &deltalake::Path) -> bool {
        hidden(&self.ours, path)
    }

    fn remember(&self, path: &deltalake::Path) {
        if is_checkpoint_artefact(path) {
            self.ours
                .lock()
                .expect("the written-checkpoint set is never poisoned")
                .insert(path.clone());
        }
    }
}

impl std::fmt::Display for WithoutCheckpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WithoutCheckpoints({})", self.inner)
    }
}

#[async_trait::async_trait]
impl deltalake::ObjectStore for WithoutCheckpoints {
    async fn put_opts(
        &self,
        location: &deltalake::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        // Written here, so it is ours and stays visible — including to the read-back that
        // immediately follows a checkpoint write.
        self.remember(location);
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &deltalake::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.remember(location);
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &deltalake::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if self.hidden(location) {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "hidden: this checkpoint is being replaced by a log replay".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<deltalake::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<deltalake::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&deltalake::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<deltalake::ObjectMeta>> {
        use futures::StreamExt;
        let ours = self.ours.clone();
        self.inner
            .list(prefix)
            .filter(move |m| {
                let keep = !m.as_ref().is_ok_and(|meta| hidden(&ours, &meta.location));
                async move { keep }
            })
            .boxed()
    }

    /// Filtered, and still pushed down.
    ///
    /// This is the method the kernel actually enumerates `_delta_log` with, and the Azure
    /// store overrides it to carry the offset in the request marker rather than listing
    /// everything and discarding the front. The default body would call `list` — filtered,
    /// correct, and blind to that: on a table with a long log it would page the whole
    /// directory on every snapshot update. Delegating keeps the pushdown; the filter is
    /// re-applied here because delegating is exactly what loses it.
    fn list_with_offset(
        &self,
        prefix: Option<&deltalake::Path>,
        offset: &deltalake::Path,
    ) -> futures::stream::BoxStream<'static, object_store::Result<deltalake::ObjectMeta>> {
        use futures::StreamExt;
        let ours = self.ours.clone();
        self.inner
            .list_with_offset(prefix, offset)
            .filter(move |m| {
                let keep = !m.as_ref().is_ok_and(|meta| hidden(&ours, &meta.location));
                async move { keep }
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&deltalake::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        let mut got = self.inner.list_with_delimiter(prefix).await?;
        got.objects.retain(|meta| !self.hidden(&meta.location));
        Ok(got)
    }

    async fn copy_opts(
        &self,
        from: &deltalake::Path,
        to: &deltalake::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        // A commit that lands via copy-then-delete goes through here rather than `put`, and
        // `rename_opts` decomposes into it too.
        self.remember(to);
        self.inner.copy_opts(from, to, options).await
    }
}

/// Turn the usual failure into the usual next step.
fn hint(scheme: &str, no_options: bool) -> String {
    match scheme {
        "abfss" | "abfs" | "az" | "adl" if no_options => {
            ". No [storage.options] are set, so this depends entirely on the environment \
             (AZURE_STORAGE_ACCOUNT_NAME and friends) or on a managed identity being \
             available."
                .into()
        }
        "abfss" | "abfs" | "az" | "adl" => {
            ". Check that the account, container and credentials in [storage.options] are \
             right, and that the identity may read the container."
                .into()
        }
        // A scheme with no backend never reaches here: `ensure_table_uri` rejects it
        // first, and its message already lists the schemes that are registered.
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_missing_local_table_says_so_plainly() {
        let s = Storage::default();
        let e = s.open("/definitely/not/a/table").await.unwrap_err();
        assert!(!e.to_string().contains("storage.options"), "got: {e}");
    }

    #[test]
    fn the_azure_backend_is_compiled_in() {
        // `check` resolves the backend without a request, so this stays a unit test
        // rather than something that quietly depends on reaching Azure. If the feature
        // were dropped from Cargo.toml, this is what would catch it.
        let s = Storage::new(HashMap::from([
            ("azure_storage_account_name".to_string(), "acct".to_string()),
            ("azure_storage_account_key".to_string(), "a2V5".to_string()),
        ]));
        s.check("abfss://container@acct.dfs.core.windows.net/some/table")
            .expect("abfss:// must resolve");
        s.check("az://container/some/table")
            .expect("az:// must resolve");
    }

    #[test]
    fn a_local_path_needs_no_credentials() {
        Storage::default().check("/tmp/some/table").unwrap();
    }

    #[test]
    fn an_unusable_credential_is_caught_before_anything_starts() {
        let s = Storage::new(HashMap::from([(
            "azure_storage_use_emulator".to_string(),
            "not-a-bool".to_string(),
        )]));
        let e = s
            .check("abfss://c@acct.dfs.core.windows.net/t")
            .unwrap_err();
        assert!(e.to_string().contains("storage.options"), "got: {e}");
    }

    #[test]
    fn every_spelling_of_a_checkpoint_is_recognised() {
        let yes = |p: &str| {
            assert!(
                is_checkpoint_artefact(&deltalake::Path::from(p)),
                "should be hidden: {p}"
            )
        };
        let no = |p: &str| {
            assert!(
                !is_checkpoint_artefact(&deltalake::Path::from(p)),
                "should be left alone: {p}"
            )
        };

        // The store this filters is rooted at the bucket, so real paths carry a prefix.
        yes("lake/raw/orders/_delta_log/_last_checkpoint");
        yes("lake/raw/orders/_delta_log/00000000000000000099.checkpoint.parquet");
        // Multi-part, and V2, which is a JSON manifest rather than a parquet file.
        yes("_delta_log/00000000000000000099.checkpoint.0000000001.0000000010.parquet");
        yes("_delta_log/00000000000000000099.checkpoint.7b4e-11ef.json");
        yes("_delta_log/_sidecars/016ae1a-9c5a.parquet");

        // The commits are the whole point of replaying, so they must survive.
        no("lake/raw/orders/_delta_log/00000000000000000099.json");
        no("_delta_log/00000000000000000099.crc");
        // Data files, including one whose own name is unlucky.
        no("lake/raw/orders/part-00000-abc.checkpoint.parquet");
        no("lake/raw/orders/date=2026-08-11/part-00000-abc.snappy.parquet");
        // A table that merely lives in a directory with a suggestive name.
        no("_sidecars/whatever.parquet");
    }

    #[tokio::test]
    async fn the_engine_that_wrote_the_checkpoint_is_read_out_of_its_footer() {
        // What turns "something is wrong with this table" into a name the operator can act
        // on, and what the README prints. Only the parquet footer is read, so the fixture
        // needs to be a parquet file at the right path and nothing more.
        use deltalake::arrow::array::{Int64Array, RecordBatch};
        use deltalake::arrow::datatypes::{DataType, Field, Schema};
        use deltalake::parquet::arrow::ArrowWriter;
        use deltalake::parquet::file::properties::WriterProperties;

        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        std::fs::create_dir_all(root.join("_delta_log")).unwrap();

        let batch = RecordBatch::try_new(
            std::sync::Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)])),
            vec![std::sync::Arc::new(Int64Array::from(vec![1i64])) as _],
        )
        .unwrap();
        // Two of them, because the *newest* is the one that matters.
        for (version, writer) in [(1u64, "parquet-rs version 58.4.0"), (2, TRINO)] {
            let f = std::fs::File::create(
                root.join(format!("_delta_log/{version:020}.checkpoint.parquet")),
            )
            .unwrap();
            let props = WriterProperties::builder()
                .set_created_by(writer.into())
                .build();
            let mut w = ArrowWriter::try_new(f, batch.schema(), Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let got = Storage::default()
            .checkpoint_writer(root.to_str().unwrap())
            .await;
        assert_eq!(got.as_deref(), Some(TRINO));
    }

    const TRINO: &str = "parquet-mr-trino version 480-e.5";

    #[tokio::test]
    async fn a_table_with_no_checkpoint_at_all_names_nobody_rather_than_failing() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        std::fs::create_dir_all(root.join("_delta_log")).unwrap();
        assert!(Storage::default()
            .checkpoint_writer(root.to_str().unwrap())
            .await
            .is_none());
    }

    #[test]
    fn a_table_is_mentioned_once_however_often_it_is_reopened() {
        // `open` is also the recovery path after a failed commit, so a line per open would
        // bury the batches around it.
        let uri = "abfss://c@a.dfs.core.windows.net/lake/raw/orders";
        assert!(first_mention_of(uri), "the first open says so");
        assert!(!first_mention_of(uri), "and every one after it does not");
        assert!(
            first_mention_of("abfss://c@a.dfs.core.windows.net/lake/raw/other"),
            "per table, not once per process"
        );
    }

    #[test]
    fn a_checkpoint_written_through_the_wrapper_is_not_one_of_the_hidden_ones() {
        // delta-rs writes a checkpoint as a post-commit hook and reads it straight back to
        // record its size. Hiding that read would fail the hook and take the commit with
        // it — turning a table that could not be opened into one that could not be written.
        use deltalake::logstore::object_store::memory::InMemory;

        let w = WithoutCheckpoints::new(std::sync::Arc::new(InMemory::new()));
        let theirs = deltalake::Path::from("t/_delta_log/00000000000000000099.checkpoint.parquet");
        let ours = deltalake::Path::from("t/_delta_log/00000000000000000150.checkpoint.parquet");

        assert!(w.hidden(&theirs), "the one that was already there");
        assert!(w.hidden(&ours), "and this one, until we write it");

        w.remember(&ours);
        assert!(!w.hidden(&ours), "ours is well-typed and stays visible");
        assert!(w.hidden(&theirs), "theirs is still the reason we are here");

        // A commit is not a checkpoint and was never hidden, so remembering it is a no-op
        // rather than a second way to leak one.
        let commit = deltalake::Path::from("t/_delta_log/00000000000000000150.json");
        w.remember(&commit);
        assert!(!w.hidden(&commit));
    }

    #[test]
    fn only_the_recoverable_failure_is_retried() {
        use deltalake::DeltaTableError;

        let kernel = |m: &str| DeltaTableError::Generic(m.to_string());
        assert!(is_unreadable_checkpoint(&kernel(
            "Kernel error: External error: External error: Schema error: Invalid data type \
             for Delta Lake: Timestamp(ms)"
        )));
        // The other shape the same cause takes, when the mismatch is caught by the
        // projection rather than by the physical-type conversion.
        assert!(is_unreadable_checkpoint(&kernel(
            "Schema error: stats_parsed field maxValues has type Timestamp(Millisecond, \
             None) but requested Timestamp(Microsecond, Some(\"UTC\"))"
        )));
        // Everything else must fail on the spot rather than paying for a second open.
        assert!(!is_unreadable_checkpoint(&kernel(
            "Object at location /x not found"
        )));
        assert!(!is_unreadable_checkpoint(&kernel(
            "Account key is not valid base64"
        )));
    }

    #[test]
    fn only_object_store_schemes_get_a_credential_hint() {
        assert!(hint("abfss", true).contains("AZURE_STORAGE_ACCOUNT_NAME"));
        assert!(hint("abfss", false).contains("[storage.options]"));
        assert!(hint("file", false).is_empty(), "a local path needs no hint");
    }

    #[test]
    fn a_scheme_with_no_backend_is_named_along_with_the_ones_there_are() {
        // The message comes from delta-rs, which lists what is registered — better than
        // anything guessed here, and it is what proves the azure feature took effect.
        let e = Storage::default().check("s3://bucket/table").unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("Unknown scheme: s3"), "got: {msg}");
        assert!(msg.contains("abfss"), "azure must be registered: {msg}");
    }
}
