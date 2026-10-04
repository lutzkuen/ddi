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

    /// The resume point of a pipeline that has already started is no longer readable.
    ///
    /// Raised from the stream, which is reached only once the point the pipeline opened at
    /// *was* readable — so this is the commit that went missing underneath a running
    /// pipeline, and [`Self::BootstrapUnreachable`] is the one that fires before a pipeline
    /// has ever started. They are separate variants because the recoveries are opposites: a
    /// pipeline that has never committed can be told where to start instead, and one that
    /// has cannot.
    #[error(
        "cannot resume from {cursor}: the commit log of {source_uri} no longer contains \
         version {}. Most likely a log-retention policy reclaimed it — \
         delta.logRetentionDuration, 30 days by default, which is a different setting from \
         the file retention behind a vacuumed-file error.\n\
         \n\
         If this pipeline has already committed to this target, that version held rows it \
         was responsible for, and they cannot be recovered from the log. Note in particular \
         what does *not* work: starting_version cannot move the resume point, because it is \
         consulted only while the target holds no txn action for this app_id, and a txn \
         action survives an overwrite. So an in-place rebuild resumes here again. Recreate \
         the target table, or give the pipeline a new app_id — and which rebuild is right is \
         a decision only you can make, because an append target and an upsert target need \
         different ones and something downstream may already have read this one.\n\
         \n\
         To stop it recurring, raise delta.logRetentionDuration on the source above the \
         longest outage or backlog you intend to allow, and keep it at or above \
         delta.deletedFileRetentionDuration — a log that outlives the files it names buys \
         nothing.",
        .cursor.version
    )]
    CursorUnavailable {
        // Not `source`: thiserror reads a field of that name as the error's cause.
        source_uri: String,
        cursor: StreamCursor,
    },

    /// The version a pipeline would start from aged out before it ever committed.
    ///
    /// The bootstrap twin of [`Self::CursorUnavailable`], and its own variant because this is
    /// the one form of the condition whose recovery costs nothing. The target holds no txn
    /// action for this app_id, so no commit of this pipeline's depends on the versions the
    /// log has lost, and `starting_version` is still consulted — which it stops being the
    /// moment one batch lands. Without the distinction an operator is told to rebuild a
    /// target that has never been written to.
    ///
    /// It also exists because of what the log said before it: `delta error: Invalid table
    /// version: 0`, raised from delta-rs and naming neither the pipeline, nor the table, nor
    /// the way out. See `LogStreamBuilder::latest_version` for why that was what surfaced.
    #[error(
        "pipeline has never committed and cannot start: starting_version = {configured} is \
         no longer in the commit log of {source_uri}. The oldest version the log still holds \
         is {oldest_available}{}, and the head is {head}.\n\
         \n\
         Nothing unusual has to have happened for this: delta.logRetentionDuration is 30 \
         days by default, so a source older than that no longer has version 0, and a \
         pipeline that was created but never ran until now starts from a version that is \
         already gone. No VACUUM anybody ran is implicated.\n\
         \n\
         The recovery is one value, set once: starting_version = {oldest_available} on this \
         pipeline. Be clear about what that accepts — rows the source wrote before version \
         {oldest_available} are not ingested by this pipeline, and will not be. Nothing that \
         is still readable is skipped, because no earlier version survives in the log at \
         all; but if the target has to hold those rows, something else has to load them from \
         a snapshot of the source.\n\
         \n\
         Two recoveries that work for a pipeline which has committed do nothing here, and \
         for the same reason in both cases — there is no txn action to clear. Recreating the \
         target changes nothing. Giving the pipeline a new app_id changes nothing.\n\
         \n\
         To stop it recurring, raise delta.logRetentionDuration on the source above the \
         longest gap you intend to allow between declaring a pipeline and first running it.",
        .oldest_available_committed_at
            .as_deref()
            .map(|at| format!(" (committed {at})"))
            .unwrap_or_default()
    )]
    BootstrapUnreachable {
        // Not `source`: thiserror reads a field of that name as the error's cause.
        source_uri: String,
        configured: crate::source::Version,
        oldest_available: crate::source::Version,
        /// RFC 3339, read from the commit object's storage metadata — the same clock Delta
        /// time travel uses. `None` when the object's metadata could not be read, which must
        /// not turn a diagnosable error into an undiagnosable one.
        oldest_available_committed_at: Option<String>,
        head: crate::source::Version,
    },

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

    /// The source stopped being the table this pipeline opened, while it was reading it.
    ///
    /// Raised from the stream, which otherwise has no way to tell: a recreated table at the
    /// same path simply reads as a head that went backwards, or — once it has grown past the
    /// old cursor — as more commits, read from a position that meant something only in the
    /// old table's log. Failing the step hands it to `Pipeline::open`, which is where a
    /// replaced source is recognised and dealt with.
    #[error(
        "source {source_uri} was dropped and recreated, or relocated, while this pipeline was \
         reading it: {detail}. Nothing was written for it. Reopening starts over from \
         starting_version under the dedup_timestamp filter, which skips what the target \
         already holds — or, without dedup_timestamp, refuses and names the fix."
    )]
    SourceReplaced {
        // Not `source`: thiserror reads a field of that name as the error's cause.
        source_uri: String,
        detail: String,
    },

    /// A partition's offset in the target is not the one this writer last read or wrote.
    ///
    /// Raised before a commit by the fence over partitioned offsets (docs/kafka-design.md
    /// §4.3), when a partition the batch carries is looked up at the snapshot the commit is
    /// planned against. Somebody else committed that partition in between, so the batch was
    /// read from the wrong place: the writer re-reads its offsets from the target rather than
    /// retrying it.
    #[error(
        "partition {partition} of app_id {app_id:?}: the target holds offset {} where this \
         writer expected {}. Another writer has committed this partition since: a second \
         deployment with the same app_id, or a consumer the group has since replaced. Nothing \
         was written; the pipeline re-reads its offsets from the target and carries on from \
         there.",
        .found.map_or_else(|| "none".to_string(), |o| o.to_string()),
        .expected.map_or_else(|| "none".to_string(), |o| o.to_string())
    )]
    Fenced {
        app_id: String,
        partition: i32,
        /// The offset this writer last read or committed for the partition.
        expected: Option<i64>,
        /// The offset the target holds.
        found: Option<i64>,
    },

    /// A commit the offsets of a partitioned source have to be read through is gone from the
    /// table's log.
    ///
    /// Log cleanup removes the commits below a checkpoint once they are older than
    /// `delta.logRetentionDuration`, and a handle older than that checkpoint can still need
    /// them. Its own variant because the cure is the one a commit conflict has, not the one
    /// an internal fault has: reopen the table at its current version and read the offsets
    /// there, from the newer checkpoint (docs/kafka-design.md §3.6, Replan). Reading past the
    /// gap instead could answer with an older offset than the truth.
    #[error(
        "cannot read the offsets in {table_uri}: commit {version} is no longer in its log, and \
         no checkpoint at or above it stands in for it. Log cleanup has removed a commit this \
         handle's version still needs; a handle reopened at the table's current version reads \
         its offsets from a newer checkpoint."
    )]
    OffsetLogTruncated { table_uri: String, version: u64 },

    /// A Kafka batch's rejects could not be written to its data-quality table, so the batch
    /// was not committed either.
    ///
    /// Every failure of [`crate::dq::DataQuality::write_kafka`] but a conflict that outlasted
    /// its retries is this one, with what failed as its cause rather than folded into its text,
    /// so that a retry loop can sort them by type (docs/kafka-design.md §3.6): an object store
    /// fault is worth retrying in a moment, a table with the wrong schema is not. Whatever the
    /// cause, it is about the data-quality table, and the target needs no replan for it — not
    /// even when the cause is one that would replan the target had the target raised it, such
    /// as a commit missing under a stale data-quality handle after log cleanup.
    #[error(
        "cannot write {rows} rejected row(s) to the data-quality table {dq_uri:?}: {source}. \
         The batch was not committed, so nothing was lost — but this pipeline cannot make \
         progress until the table is writable."
    )]
    DataQualityWrite {
        dq_uri: String,
        rows: usize,
        source: Box<Error>,
    },

    /// A Kafka batch's rejects kept conflicting with other commits to its data-quality table,
    /// past the retries in place: another engine changing the table's metadata or protocol
    /// each time, or more concurrent commits than delta-rs retries. Another member's rejects
    /// are no conflict: both are plain appends. Nothing was written, and the batch is tried
    /// again (docs/kafka-design.md §5.7).
    #[error(
        "cannot write {rows} rejected row(s) to the data-quality table {dq_uri:?}: the commit \
         conflicted with another, {attempts} times in a row ({detail}). The batch was not \
         committed, so nothing was lost; it is tried again."
    )]
    DataQualityConflict {
        dq_uri: String,
        rows: usize,
        attempts: u32,
        detail: String,
    },

    /// The target sets `delta.setTransactionRetentionDuration`, under which an offset can
    /// expire.
    ///
    /// Delta forgets a `txn` whose recorded time is older than that duration.
    /// kafka-delta-ingest records one on each, so on a table it wrote, the offset of a
    /// partition no commit has re-asserted for that long (a quiet topic, or a gap between
    /// stopping it and starting ddi) is forgotten, and the partition would have to be read
    /// again. ddi's own `txn` actions record no time and never expire, and the property is
    /// refused when the pipeline opens and on every refresh, before any offset can go. See
    /// docs/kafka-design.md §4.6.
    #[error(
        "target {target_uri} sets delta.setTransactionRetentionDuration = {retention}. Delta \
         forgets an offset whose recorded time is older than that, as kafka-delta-ingest's \
         offsets have one, and a partition whose offset is forgotten would have to be read \
         again. Remove the property from the table; nothing was written, and the pipeline goes \
         on once it is gone."
    )]
    KafkaTxnRetention {
        target_uri: String,
        retention: String,
    },

    /// The target's protocol asks for a writer feature this build cannot write.
    ///
    /// Checked when a pipeline opens or refreshes its target, rather than discovered by a
    /// failed commit: another engine can upgrade a table's protocol while a pipeline is
    /// writing it, and delta-rs reads a column-mapped table but refuses to write one. See
    /// docs/kafka-design.md §4.5.
    #[error(
        "target {target_uri} needs a writer this build cannot be: {detail}. Nothing was \
         written. Either write this table with a build that supports it, or point the pipeline \
         at a table without the feature; column mapping in particular cannot be removed from a \
         table in place."
    )]
    TargetProtocolUnsupported { target_uri: String, detail: String },

    /// The target is no longer the table this pipeline opened.
    ///
    /// Its id changed: it was dropped and recreated, or another table was put at its path.
    /// The offsets this pipeline holds were read from the old table, so it writes nothing
    /// more to the new one until a restart, which reads the new table's offsets as it finds
    /// them.
    #[error(
        "target {target_uri} is no longer the table this pipeline opened: its table id was \
         {opened} and is now {found}. It was dropped and recreated, or another table was put \
         in its place. Nothing more is written to it until the pipeline is restarted, which \
         reads the new table's offsets as it finds them."
    )]
    TargetReplaced {
        target_uri: String,
        opened: String,
        found: String,
    },

    /// kafka-delta-ingest wrote this app_id's offsets, ddi has not taken them over yet, and
    /// partitions whose topic holds messages have no offset among them.
    ///
    /// Each is a partition kafka-delta-ingest never wrote a row for, or one whose offset
    /// expired under `delta.setTransactionRetentionDuration` while it ran; the table cannot
    /// tell the two apart, and reading the second again from offset 0 would duplicate it.
    /// Raised once, when a pipeline opens, before it joins its group: an explicit
    /// `starting_offsets` entry per partition is the acknowledgment
    /// (docs/kafka-design.md §4.4).
    #[error(
        "app_id {app_id:?}: kafka-delta-ingest wrote this table's offsets, and partition(s) \
         {partitions:?} have none although their topic holds messages. Each is either a \
         partition kafka-delta-ingest never wrote a row for, or one whose offset expired under \
         delta.setTransactionRetentionDuration, which reading from offset 0 would duplicate; \
         the table cannot tell which. Check the data (`ddi kafka plan` prints the evidence) and \
         give each an explicit starting_offsets entry, the next offset to read. Nothing was \
         read or written, and the pipeline has not joined its group."
    )]
    KafkaOffsetMissing {
        app_id: String,
        partitions: Vec<i32>,
    },

    #[error("transform error: {0}")]
    Transform(String),

    /// The transform could not evaluate a value, rather than failing for any other reason.
    ///
    /// The one transform failure a single row can cause: a cast that does not parse, a
    /// division by zero, a date past what the engine represents. Its own variant because it
    /// alone may be answered by finding that row and setting it aside — see
    /// [`crate::transform::Transform::apply_isolating`]. A plan that does not type-check, a
    /// full spill directory or an unreadable file are none of those, and are never blamed on a
    /// row. Displayed exactly as [`Self::Transform`] is, because to whoever reads the log it
    /// is still a transform that failed.
    #[error("transform error: {0}")]
    Evaluation(String),

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

impl Error {
    /// Whether this ran out of capacity: a [`Self::Capacity`], or one beneath a
    /// [`Self::DataQualityWrite`].
    ///
    /// The wrapper says which table failed, and that the target needs no replan; whether to
    /// wait the full backoff and raise the capacity gauge is its cause's to say
    /// (docs/kafka-design.md §3.6). Matching `Error::Capacity(_)` at the top, as `drive` in
    /// main.rs and the metrics do for a Delta pipeline, misses a Kafka pipeline's
    /// data-quality write that ran out. Only capacity is read through the wrapper here: the
    /// other causes beneath it are classified apart from the same errors raised by the
    /// target, an `OffsetLogTruncated` above all.
    pub fn is_capacity(&self) -> bool {
        match self {
            Error::Capacity(_) => true,
            Error::DataQualityWrite { source, .. } => source.is_capacity(),
            _ => false,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::datafusion::error::DataFusionError;

    #[test]
    fn a_capacity_failure_is_not_read_as_a_transform_error() {
        // The two want opposite handling from the retry loop, and mapping every DataFusion
        // error to `Transform` made them indistinguishable — so a pipeline that had run out
        // of disk retried a second later, forever, holding the whole shared spill budget
        // while it did.
        let e = Error::from(DataFusionError::ResourcesExhausted(
            "The used disk space during the spilling process has exceeded the allowable limit \
             of 100.0 GB."
                .into(),
        ));
        assert!(matches!(e, Error::Capacity(_)), "{e}");

        let e = Error::from(DataFusionError::Plan("no such column".into()));
        assert!(matches!(e, Error::Transform(_)), "{e}");
    }

    #[test]
    fn a_fenced_partition_says_what_the_target_holds_and_what_was_expected() {
        let m = Error::Fenced {
            app_id: "orders".into(),
            partition: 3,
            expected: Some(41),
            found: Some(57),
        }
        .to_string();
        assert!(
            m.contains(
                "partition 3 of app_id \"orders\": the target holds offset 57 where this \
                 writer expected 41"
            ),
            "{m}"
        );

        // A partition with no offset yet is "none", not a number that could be read as one.
        let m = Error::Fenced {
            app_id: "orders".into(),
            partition: 0,
            expected: None,
            found: Some(9),
        }
        .to_string();
        assert!(
            m.contains("offset 9 where this writer expected none"),
            "{m}"
        );
    }

    /// A Kafka data-quality write that ran out of capacity is a `Capacity` beneath a
    /// `DataQualityWrite`, which matching the variant at the top misses (main.rs's `RETRY_MAX`
    /// and the capacity gauge do just that). Nothing else beneath one is capacity, nor is a
    /// conflict past the retries in place.
    #[test]
    fn a_capacity_failure_beneath_a_data_quality_write_is_capacity() {
        let beneath = |cause: Error| Error::DataQualityWrite {
            dq_uri: "/lake/orders__ddi_dq".into(),
            rows: 3,
            source: Box::new(cause),
        };
        let wrapped = beneath(Error::Capacity("the spill directory is full".into()));
        assert!(!matches!(wrapped, Error::Capacity(_)));
        assert!(wrapped.is_capacity());
        assert!(beneath(beneath(Error::Capacity("full".into()))).is_capacity());
        assert!(Error::Capacity("full".into()).is_capacity());

        for not in [
            beneath(Error::OffsetLogTruncated {
                table_uri: "file:///lake/orders__ddi_dq/".into(),
                version: 4,
            }),
            beneath(Error::Other("internal".into())),
            Error::DataQualityConflict {
                dq_uri: "/lake/orders__ddi_dq".into(),
                rows: 3,
                attempts: 4,
                detail: "conflict".into(),
            },
            Error::Transform("no such column".into()),
        ] {
            assert!(!not.is_capacity(), "{not:?}");
        }
    }

    #[test]
    fn a_capacity_failure_names_the_budget_and_the_three_ways_out() {
        let m = Error::Capacity("the spill directory is full".into()).to_string();
        assert!(m.contains("no other pipeline was stopped"), "{m}");
        assert!(m.contains("max_temp_directory_size"), "{m}");
        assert!(m.contains("headroom"), "{m}");
        assert!(m.contains("max_concurrent_upsert_preflights"), "{m}");
    }
}
