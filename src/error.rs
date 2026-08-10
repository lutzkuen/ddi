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

    #[error("transform error: {0}")]
    Transform(String),

    #[error("schema mismatch: {0}")]
    Schema(String),

    #[error("{0}")]
    Other(String),
}

impl From<deltalake::datafusion::error::DataFusionError> for Error {
    fn from(e: deltalake::datafusion::error::DataFusionError) -> Self {
        Error::Transform(e.to_string())
    }
}
