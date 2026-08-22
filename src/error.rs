//! Error types.
//!
//! Errors are deliberately loud and specific. This tool's whole value proposition is
//! correctness under restart, so anything ambiguous is an error rather than a silent
//! fallback — see the "no dead-letter queue" non-goal in the README.

use deltalake::DeltaTableError;
use thiserror::Error;

use crate::source::StreamCursor;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("delta error: {0}")]
    Delta(#[from] DeltaTableError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("config error: {0}")]
    Config(String),

    /// A commit contained a `Remove` with `dataChange: true` and the policy is `Fail`.
    ///
    /// The message names the alternatives because the fix is always a policy choice,
    /// never a code change.
    #[error(
        "source version {version} contains a dataChange Remove action (a DELETE, UPDATE or \
         MERGE on the source table). This pipeline is append-only, so those rows cannot be \
         propagated. Set change_policy = \"skip_change_commits\" to skip such commits \
         entirely, or \"ignore_changes\" to emit the Adds from them (which may duplicate \
         rewritten rows downstream).\n\
         \n\
         If the source is itself the target of a ddi pipeline running write_mode = \
         \"upsert\", every one of its commits looks like this, and neither of those two is \
         the right answer on its own: \"skip_change_commits\" would drop whole commits \
         including the keys they insert. Set change_policy = \"ignore_changes\" *and* \
         write_mode = \"upsert\" here too, keyed the same way, so the re-emitted rows merge \
         onto their keys instead of accumulating."
    )]
    ChangeCommit { version: u64 },

    /// v1 does not split a source commit across output commits (plan §2.3).
    #[error(
        "source version {version} contains {files} files ({bytes} bytes), which exceeds the \
         batch limit (max_files_per_batch={max_files}, max_bytes_per_batch={max_bytes}). v1 \
         never splits a source commit, because the resume offset is a bare version number \
         stored in the Delta txn action. Raise the batch limits for this pipeline, or reduce \
         the source's commit size (e.g. kafka-delta-ingest's allowed_latency)."
    )]
    CommitTooLarge {
        version: u64,
        files: usize,
        bytes: u64,
        max_files: usize,
        max_bytes: u64,
    },

    #[error(
        "source table uses deletion vectors (version {version}, file {path}). v1 does not \
         support them: the added file's rows are filtered by a deletion vector, so copying \
         the file wholesale would emit rows the source considers deleted. Rewrite the source \
         without deletion vectors, or wait for v2."
    )]
    DeletionVectorUnsupported { version: u64, path: String },

    #[error(
        "source reader version {reader} / writer version {writer} at version {version} is not \
         supported by this build of delta-rs. Upgrade delta-delta-ingest, or pin the source \
         table to a lower protocol version."
    )]
    UnsupportedProtocol {
        version: u64,
        reader: i32,
        writer: i32,
    },

    /// The requested resume point is no longer readable.
    #[error(
        "cannot resume from {cursor}: the source's commit log no longer contains version \
         {}. The log has most likely been truncated by VACUUM or a log-retention policy. \
         Choose a newer starting_version, or rebuild the target from scratch.",
        .cursor.version
    )]
    CursorUnavailable { cursor: StreamCursor },

    /// A file an unconsumed commit added is no longer in storage.
    ///
    /// The data-file twin of [`Error::CursorUnavailable`]: there the commit went and the
    /// files stayed, here the commit survives and the file it added did not. Both mean the
    /// cursor has fallen past what the source can still replay, and neither can be fixed by
    /// waiting — so this one is typed for the same reason that one is, and surfaced
    /// separately as `ddi_source_file_vacuumed` so an operator can alert on it rather than
    /// on a retry loop that looks like every other one.
    #[error(
        "source version {version} of {source_uri} adds {path}, and the object store no longer \
         has that file. Most likely OPTIMIZE retired it and VACUUM then removed it, which \
         is what happens once a consumer falls further behind than the source's \
         delta.deletedFileRetentionDuration — a deliberately excluded pipeline counts as \
         behind.\n\
         \n\
         If that is what happened the rows are not lost — they live on in the files the \
         compaction wrote — but they are not recoverable *as this commit*, and that is the \
         whole difficulty. Reading the replacement files would emit whatever else that \
         compaction swept up, rows from commits already consumed among them, so a stream \
         that cannot be exactly-once stops instead of guessing. A file retired by a DELETE \
         rather than by OPTIMIZE is a different matter: those rows are gone, and no \
         recovery brings them back.\n\
         \n\
         Two recoveries are safe. Restore the named file while the store still has a copy \
         (ADLS soft delete or S3 versioning, if either was switched on before the delete; \
         a backup otherwise). The pipeline resumes on its own — but hold VACUUM off the \
         source until it has caught up, or the next run removes the file again, and expect \
         to restore every retired file the backlog still has to read, not only this one.\n\
         \n\
         Otherwise rebuild this target from a current snapshot of the source, deliberately. \
         Note what that takes: a starting_version past {version} is only consulted when the \
         target holds no txn action for this app_id, and a txn action survives an overwrite \
         — so an in-place rebuild resumes here again. Recreate the target table, or give \
         the pipeline a new app_id. Which rebuild is right is a decision only you can make, \
         because an append target and an upsert target need different ones and something \
         downstream may already have read this one.\n\
         \n\
         To stop it recurring, raise delta.deletedFileRetentionDuration on the source above \
         the longest outage or backlog you intend to allow."
    )]
    SourceFileVacuumed {
        // Not `source`: thiserror reads a field of that name as the error's cause.
        source_uri: String,
        version: u64,
        path: String,
    },

    #[error("transform error: {0}")]
    Transform(String),

    /// Something ran out of a resource rather than being wrong.
    ///
    /// Its own variant rather than [`Self::Transform`], because the two want opposite
    /// handling and from inside a retry loop they are indistinguishable. A transform error is
    /// a fact about the data: retrying is pointless and free. This is a fact about the
    /// machine, and retrying it a second later re-runs the same scan into the same full
    /// directory — every backoff period, for days. That loop, not the single scan, is what
    /// turns one large grain check into sustained ephemeral-storage pressure and a Kubernetes
    /// eviction that takes every healthy pipeline with it.
    ///
    /// `drive` in `main` reads this variant and waits the full backoff instead of a second;
    /// [`crate::metrics::PipelineMetrics`] raises a gauge for it, the way it already does for
    /// [`Self::SourceFileVacuumed`], so an operator can tell "retrying, and it will work" from
    /// "retrying, and it never will".
    #[error(
        "out of capacity: {0}\n\
         \n\
         Nothing was written to the target, and no other pipeline was stopped. Three things \
         fix it, in the order they are usually right. Give the work less to do: a narrower \
         upsert_lookback, or a smaller max_bytes_per_batch. Give the process more room: a \
         larger volume at [runtime] temp_directory with [runtime] max_temp_directory_size \
         raised to match — leave headroom, because the budget is checked after each write and \
         not before. Or run fewer of these at once: [runtime] max_concurrent_upsert_merges and \
         max_concurrent_upsert_preflights."
    )]
    Capacity(String),

    #[error("schema mismatch: {0}")]
    Schema(String),

    #[error("{0}")]
    Other(String),
}

impl From<deltalake::datafusion::error::DataFusionError> for Error {
    /// Not every DataFusion error is a fact about the data.
    ///
    /// `ResourcesExhausted` is a fact about the machine, and mapping it to
    /// [`Error::Transform`] made the two indistinguishable to the retry loop — which then
    /// re-ran the scan that had just run out of disk, a second later, forever. See
    /// [`crate::spill::classify`].
    fn from(e: deltalake::datafusion::error::DataFusionError) -> Self {
        crate::spill::classify(e, "datafusion")
    }
}
