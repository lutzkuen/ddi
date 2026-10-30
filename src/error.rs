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
    /// each time; a DELETE, UPDATE or MERGE on it, whose removes (`dataChange: true`) conflict
    /// with an append, which delta-rs counts as having read every file of its snapshot; or more
    /// concurrent commits than delta-rs retries. Another member's rejects are no conflict:
    /// both are plain appends. Nothing was written, and the batch is tried again
    /// (docs/kafka-design.md §5.7).
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
    /// Delta forgets a `txn` whose recorded time is older than that duration (Spark one that
    /// records none, too). kafka-delta-ingest records one on each, so on a table it wrote, the
    /// offset of a partition no commit has re-asserted for that long (a quiet topic, or a gap
    /// between stopping it and starting ddi) is forgotten, and the partition would have to be
    /// read again. ddi's own `txn` actions record a time no engine expires
    /// ([`crate::fence::KAFKA_TXN_LAST_UPDATED`]), and the property is refused when the
    /// pipeline opens and on every refresh, before any offset can go. See
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
    /// docs/kafka-design.md §4.5. A Kafka pipeline's data-quality table is asked the same
    /// before every write, and the refusal is then the cause beneath
    /// [`Self::DataQualityWrite`].
    #[error(
        "table {target_uri} needs a writer this build cannot be: {detail}. Nothing was \
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

    /// ddi has not taken this app_id's offsets over yet, the target may hold rows of its
    /// topic, and partitions whose topic holds messages have no offset.
    ///
    /// When kafka-delta-ingest wrote some of the app_id's offsets, each is a partition it never
    /// wrote a row for, or one whose offset expired under
    /// `delta.setTransactionRetentionDuration` while it ran. When none has an offset at all
    /// (`fresh`) but the target holds rows, they may all have expired, which is how they expire
    /// where one instance of kafka-delta-ingest ran; or the rows are another pipeline's. The
    /// table cannot tell these apart, and reading an expired one again from offset 0 would
    /// duplicate it. Raised once, when a pipeline opens, before it joins its group: an explicit
    /// `starting_offsets` entry per partition is the acknowledgment (docs/kafka-design.md
    /// §4.4 step 3).
    #[error(
        "app_id {app_id:?}: partition(s) {partitions:?} have no offset in the target although \
         their topic holds messages, {}. Reading one from offset 0 could duplicate its rows; \
         the table cannot tell. Check the data (`ddi kafka plan` prints the evidence) and give \
         each an explicit starting_offsets entry, the next offset to read (\"*\", earliest and \
         latest acknowledge none). Nothing was read or written, and the pipeline has not joined \
         its group.",
        if *fresh {
            "and no partition of this app_id has one while the target already holds rows: \
             kafka-delta-ingest's offsets for it may all have expired under \
             delta.setTransactionRetentionDuration, or the rows are another pipeline's (if \
             the app_id is mistyped, fix it instead)"
        } else {
            "while kafka-delta-ingest wrote this table's offsets for others: each is a \
             partition it never wrote a row for, or one whose offset expired under \
             delta.setTransactionRetentionDuration"
        }
    )]
    KafkaOffsetMissing {
        app_id: String,
        partitions: Vec<i32>,
        /// No partition of the app_id has an offset, and the target holds rows that may be its
        /// topic's.
        fresh: bool,
    },

    /// A partition this writer held at an offset no longer has one in the target.
    ///
    /// Nothing removes a `txn` but expiry under `delta.setTransactionRetentionDuration`: set,
    /// a checkpoint written while it was, and removed again between two refreshes, it can take
    /// one whose recorded time it expires. The partition's rows up to that offset are in the
    /// table, so it is not started as one without an offset, which would read them again. Held
    /// until the table has an offset for it again (docs/kafka-design.md §4.6).
    #[error(
        "app_id {app_id:?}: the target {target_uri} no longer holds the offsets of these \
         partitions, given as (partition, last offset this writer held): {partitions:?}. \
         Nothing removes an offset but expiry under delta.setTransactionRetentionDuration, so \
         the rows up to each are in the table, and reading the partition from offset 0 would \
         write them twice. Restore each with `ddi kafka set-offsets --partition P --next \
         <last + 1> --acknowledge-skip \"offset expired\"`; nothing was read or written, and \
         the pipeline goes on once the target has them again."
    )]
    KafkaOffsetVanished {
        target_uri: String,
        app_id: String,
        partitions: Vec<(i32, i64)>,
    },

    /// The Schema Registry did not give a Kafka pipeline the writer schema it asked for, for a
    /// reason that is not the message's (docs/kafka-design.md §5.2).
    ///
    /// A schema id the registry does not know is the message's, and makes that message
    /// undecodable instead; this is the registry itself failing, which no message can be blamed
    /// for, so the batch waits for it. How long, and whether the pipeline stays in its group
    /// meanwhile, is the failure's to say: see [`SchemaRegistryFailure`].
    #[error("Schema Registry {url}: {detail}")]
    SchemaRegistry {
        url: String,
        failure: SchemaRegistryFailure,
        detail: String,
    },

    /// A writer schema refers to schemas under other subjects, which ddi does not resolve yet
    /// (docs/kafka-design.md §5.2, open question 10).
    ///
    /// Not a fact about one message: every message written with this schema needs the same
    /// references, so it holds the pipeline rather than sending each one to the data-quality
    /// table.
    #[error(
        "writer schema {id} in the Schema Registry {url} refers to other subjects ({}), and \
         schema references are not supported yet. Nothing was written; the pipeline holds \
         until a build that resolves them, or a writer schema without references.",
        subjects.join(", ")
    )]
    KafkaSchemaReferences {
        url: String,
        id: u32,
        subjects: Vec<String>,
    },

    /// A writer schema has a top-level field named like a Kafka metadata column
    /// (docs/kafka-design.md §6.1).
    ///
    /// Letting the payload win would silently turn the offset columns the partition watermark
    /// relies on into payload data, and dropping the field would lose it, so the pipeline holds
    /// until the prefix changes.
    #[error(
        "writer schema {schema_id} has the top-level field {field:?}, which starts with \
         metadata_prefix {prefix:?}: it would collide with the Kafka metadata columns \
         ({prefix}topic, {prefix}partition, {prefix}offset, ...). Set another metadata_prefix \
         for this pipeline, and map names in transform_sql if the target needs them. Nothing \
         was written."
    )]
    KafkaMetadataCollision {
        schema_id: u32,
        field: String,
        prefix: String,
    },

    /// A partition column of a Kafka pipeline's target is not among the columns it produces
    /// (docs/kafka-design.md §5.6 step 0).
    ///
    /// Every row would land in the null partition, `__HIVE_DEFAULT_PARTITION__`, which the
    /// table's readers would not look in. Checked when the pipeline opens, against
    /// `transform_sql` planned over the union of its writer schemas, or against those schemas
    /// and the metadata columns when it has no SQL; and on its first batch instead when no
    /// writer schema was known at open. Names are matched exactly, case included.
    #[error(
        "target {target_uri} is partitioned by {columns:?}, and {} {}. Every row would land in \
         the null partition. {}Nothing was read or written.",
        if *sql { "transform_sql produces no such column" } else {
            "neither the writer schemas nor the Kafka metadata columns have one (and there is no \
             transform_sql to compute it)"
        },
        if produced.is_empty() { String::new() } else {
            format!("(it produces: {})", produced.join(", "))
        },
        if *sql { "Produce each partition column in the SQL, under exactly its name. " } else {
            "Add a transform_sql that produces each partition column under exactly its name. "
        }
    )]
    KafkaPartitionColumnUnmapped {
        target_uri: String,
        /// The partition columns that are not produced.
        columns: Vec<String>,
        /// What is produced instead, for the message.
        produced: Vec<String>,
        /// Whether the columns come from `transform_sql`, or from the writer schemas as they
        /// are.
        sql: bool,
    },

    /// A field's type changed between writer schemas, and `transform_sql` no longer plans over
    /// the union of them (docs/kafka-design.md §5.5).
    ///
    /// The union takes each field's type from the newest writer schema that has it, so a new
    /// version that changes a type changes what the SQL reads. Retrying cannot help: the SQL
    /// has to be adjusted to the new type, and the pipeline restarted.
    #[error(
        "transform_sql no longer plans over the writer schemas, whose types changed: {}. ({detail}) \
         The newest writer schema's type is the one the SQL reads; adjust the SQL to it and \
         restart. Nothing was written.",
        fields.join("; ")
    )]
    KafkaSchemaConflict {
        /// Each changed field, with its type in both writer schemas and their ids.
        fields: Vec<String>,
        /// Why planning failed.
        detail: String,
    },

    /// A partition's next offset is no longer in its topic: retention trimmed it, so the
    /// messages from `wanted` up to `low` were never read (docs/kafka-design.md §4.4, I4).
    ///
    /// Only that partition is paused; the others go on. Nothing is skipped without an
    /// operator saying so.
    #[error(
        "partition {partition} of topic {topic:?}: the next offset to read is {wanted}, and \
         the topic now starts at {low}. Retention removed {} message(s) this pipeline never \
         read. The partition is paused and the others go on. To skip them, acknowledge it: \
         `ddi kafka set-offsets --partition {partition} --next {low} --acknowledge-skip \
         \"<reason>\"`; the pipeline notices within blocked_recheck_secs.",
        low - wanted
    )]
    KafkaOffsetsUnavailable {
        topic: String,
        partition: i32,
        wanted: i64,
        low: i64,
    },

    /// A partition's stored offset is at or past the end of its topic: the topic was
    /// recreated, or this is another cluster (docs/kafka-design.md §4.4, §4.7 row 12).
    #[error(
        "partition {partition} of topic {topic:?}: the target says offset {stored} was read, \
         and the topic ends before it (its next offset is {high}). The topic was recreated, \
         or the brokers are another cluster's. The partition is paused and the others go on. \
         Use a new app_id, or move the offset back with `ddi kafka set-offsets --allow-replay`."
    )]
    KafkaOffsetsRegressed {
        topic: String,
        partition: i32,
        stored: i64,
        high: i64,
    },

    /// The message at a partition's stored offset is not the one ddi recorded when it wrote
    /// that offset: the topic was recreated and refilled past it (docs/kafka-design.md §4.4,
    /// I19).
    #[error(
        "partition {partition} of topic {topic:?}: the message at the stored offset {offset} \
         is not the one this pipeline read there (it recorded {recorded}, the topic holds \
         {found}). The topic was recreated and has as many messages again. The partition is \
         paused and the others go on. Use a new app_id, or move the offset with `ddi kafka \
         set-offsets --allow-replay`."
    )]
    KafkaTopicReplaced {
        topic: String,
        partition: i32,
        offset: i64,
        /// The timestamp and length ddi recorded, as text.
        recorded: String,
        /// What the topic holds there now.
        found: String,
    },

    /// A message could not be decoded and there is no data-quality table to put it in
    /// (docs/kafka-design.md §5.7). kafka-delta-ingest dropped such messages silently.
    #[error(
        "partition {partition} offset {offset} of topic {topic:?} ({}) cannot be decoded \
         ({reason}), and there is no data-quality table at {dq_uri} to set it aside in. The \
         pipeline holds, in its group, until the table exists; create it and it goes on \
         within blocked_recheck_secs.",
        writer_schema_id(*schema_id)
    )]
    KafkaUndecodable {
        topic: String,
        partition: i32,
        offset: i64,
        /// The writer schema id the message's header named, when it had one.
        schema_id: Option<u32>,
        reason: String,
        dq_uri: String,
    },

    /// More messages of one batch could not be decoded than `max_evaluation_rejects_per_batch`
    /// allows (docs/kafka-design.md §5.7, "Cap").
    #[error(
        "{count} messages of one batch cannot be decoded, more than \
         max_evaluation_rejects_per_batch = {cap}; the first is partition {partition} offset \
         {offset} of topic {topic:?} ({}): {reason}. This looks like a format or registry \
         misconfiguration rather than bad data, so nothing is written and the pipeline holds. \
         Fix the producer or the registry, or raise the cap and restart.",
        writer_schema_id(*schema_id)
    )]
    KafkaTooManyUndecodable {
        count: usize,
        cap: usize,
        /// The first undecodable message of the batch: its coordinates, the writer schema id its
        /// header named when it had one, and why.
        topic: String,
        partition: i32,
        offset: i64,
        schema_id: Option<u32>,
        reason: String,
    },

    /// A fresh app_id on a target that already holds data, with no explicit
    /// `starting_offsets`: probably a mistyped app_id (docs/kafka-design.md §4.4 step 7).
    #[error(
        "app_id {app_id:?} has no offsets in {target_uri}, which already holds data{}. A \
         mistyped app_id would read the topic into it a second time, so nothing is read until \
         either the app_id is corrected or this pipeline sets starting_offsets itself.",
        if seen.is_empty() {
            " and no offsets of any app_id".to_string()
        } else {
            format!(" and offsets under {} (did you mean {:?}?)", seen.join(", "), seen[0])
        }
    )]
    KafkaAppIdUnknown {
        app_id: String,
        target_uri: String,
        /// The app_ids whose offsets the target holds, most likely first.
        seen: Vec<String>,
    },

    /// `kdi_compat` is unset on a table kafka-delta-ingest may have written
    /// (docs/kafka-design.md §4.4 step 6).
    #[error(
        "{target_uri} may have been written by kafka-delta-ingest ({}). Set kdi_compat = true \
         to keep its STRING rendering (\"null\" text, raw-number timestamps, sorted-key JSON), \
         or kdi_compat = false to accept the documented break; it is not guessed.",
        if *fresh {
            "it holds rows and no offsets of this app_id"
        } else {
            "it holds kafka-delta-ingest's offsets for this app_id"
        }
    )]
    KafkaCompatUnset {
        app_id: String,
        target_uri: String,
        /// The app_id is fresh on a target with rows, rather than in kafka-delta-ingest's era.
        fresh: bool,
    },

    /// A `starting_offsets` map without `"*"` does not list every partition of the topic
    /// (docs/kafka-design.md §4.4 step 8).
    #[error(
        "starting_offsets lists some partitions of topic {topic:?} but not {missing:?}, and has \
         no \"*\" for the rest. Add an entry for each, or a \"*\" = \"earliest\" or \"latest\"."
    )]
    KafkaStartingOffsetsIncomplete { topic: String, missing: Vec<i32> },

    /// librdkafka reported an error the consumer cannot read past by itself
    /// (docs/kafka-design.md §3.6).
    #[error("kafka: {detail}")]
    KafkaClient {
        failure: KafkaClientFailure,
        detail: String,
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

/// Why the Schema Registry did not answer, which decides how a Kafka pipeline waits for it
/// (docs/kafka-design.md §5.2, §3.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaRegistryFailure {
    /// A 5xx, 429 or 408, a timeout or a failed connection, still so after the client's own
    /// retries: Transient. An outage that ends by itself.
    Unavailable,
    /// 401 or 403: the credentials are wrong or lack a permission. Consumer-fatal and
    /// config-class: no message is to blame, so none is ever quarantined for it.
    Unauthorized,
    /// Any other refusal, or an answer that is not the Schema Registry API's (a reverse proxy's
    /// page, a URL that points elsewhere): Blocking, pipeline. Retrying the same request cannot
    /// help until someone changes the registry or the URL.
    Refused,
}

/// What kind of librdkafka error stopped a Kafka consumer (docs/kafka-design.md §3.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaClientFailure {
    /// The brokers could not be reached, or a request timed out: librdkafka retries by itself.
    /// Transient.
    Transport,
    /// librdkafka's own fatal error, a static member fenced by another with its
    /// `group.instance.id`, cooperative rebalancing, or a consumer that cannot be built:
    /// consumer-fatal. The supervisor drops the consumer and opens a new one.
    Fatal,
    /// Authentication failed, the group or topic is not authorised, or the topic does not
    /// exist: consumer-fatal and config-class. Retrying in a second cannot help, so the
    /// supervisor waits the full backoff, as for an open-time refusal.
    Config,
}

/// How a Kafka pipeline handles an error its step raises (docs/kafka-design.md §3.6).
///
/// Only consumer-fatal errors make a member leave its group; everything else is held, paused
/// and retried in place, so that one member's trouble never rebalances the others.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaClass {
    /// Another writer got there first, or the handle fell behind log cleanup: reset in place,
    /// and keep the batch while it is still valid.
    Replan,
    /// Worth retrying in a moment: hold the batch, pause every owned partition, back off
    /// 1 s doubling to 60 s, stay in the group. `ddi_kafka_retrying = 1`.
    Transient,
    /// Out of capacity, a fact about the machine: hold the batch, pause every owned
    /// partition, back off the full `RETRY_MAX`, stay in the group, and raise
    /// `ddi_capacity_exhausted`. Retried a second later, it would re-run the same work into the
    /// same full directory, every backoff period, as [`Error::Capacity`] says.
    Capacity,
    /// Needs an operator, and holds the whole pipeline: pause every owned partition, set the
    /// gauge, stay in the group, re-check every `blocked_recheck_secs`.
    BlockingPipeline,
    /// Needs an operator, and holds one partition: pause it, keep re-asserting it at its
    /// unchanged offset while the others go on, re-check it every `blocked_recheck_secs`.
    BlockingPartition,
    /// Refused when the pipeline opens, before it joins its group: `RETRY_MAX` backoff.
    OpenRefusal,
    /// The consumer cannot go on: the supervisor drops it, backs off and reopens.
    ConsumerFatal,
}

impl Error {
    /// The class of an error a Kafka pipeline's step raised (docs/kafka-design.md §3.6): its
    /// commit, the data-quality write before it, and the refreshes around them.
    ///
    /// Read by variant, never by text, with one exception that has no variant: delta-rs's
    /// error for a checkpoint another engine wrote that it cannot read
    /// ([`crate::storage::is_unreadable_checkpoint`]), which a reset steps over. At open
    /// everything that stops the pipeline refuses (`OpenRefusal`) whatever its class here.
    ///
    /// A [`Self::DataQualityWrite`] is classified by its cause, and apart from the same error
    /// raised by the target: whatever failed, it failed on the data-quality table, so it is
    /// never a Replan of the target (its table of causes is `data_quality_class`).
    pub fn kafka_class(&self) -> KafkaClass {
        use deltalake::kernel::transaction::TransactionError;
        match self {
            Error::Capacity(_) => KafkaClass::Capacity,
            Error::DataQualityWrite { source, .. } => source.data_quality_class(),
            Error::DataQualityConflict { .. } => KafkaClass::Transient,
            Error::Fenced { .. } | Error::OffsetLogTruncated { .. } => KafkaClass::Replan,
            // `InvalidVersion`: a commit missing from the target's log during the conflict
            // check, which log cleanup removed under a handle that trails the head (EO-m8).
            // Retried from the same handle it fails the same way; a reset reads the table as
            // it is. Beneath a `DataQualityWrite` it is Transient instead: `write_kafka` has
            // already reopened that table.
            Error::Delta(
                DeltaTableError::InvalidVersion(_)
                | DeltaTableError::VersionAlreadyExists(_)
                | DeltaTableError::Transaction {
                    source:
                        TransactionError::CommitConflict(_)
                        | TransactionError::VersionAlreadyExists(_)
                        | TransactionError::MaxCommitAttempts(_),
                },
            ) => KafkaClass::Replan,
            Error::Delta(e) if crate::storage::is_unreadable_checkpoint(e) => KafkaClass::Replan,
            Error::Delta(e) if is_unwritable(e) => KafkaClass::BlockingPipeline,
            // A store outage, and anything else delta-rs raises: retried, and visible as
            // `ddi_kafka_retrying` while it lasts.
            Error::Delta(_) | Error::Io(_) => KafkaClass::Transient,
            // A refresh that fell back to a full open, which folds a store outage and a table
            // removed while running into this variant: each retry opens again.
            Error::Config(_) => KafkaClass::Transient,
            Error::KafkaTxnRetention { .. }
            | Error::TargetProtocolUnsupported { .. }
            | Error::TargetReplaced { .. } => KafkaClass::BlockingPipeline,
            // Until M3 has a class for one partition, it holds them all; the recheck's reset
            // refuses until the offsets are back.
            Error::KafkaOffsetVanished { .. } => KafkaClass::BlockingPipeline,
            // The SQL, or the rows against the target's schema: the same batch fails the same
            // way until someone changes one or the other.
            Error::Transform(_) | Error::Evaluation(_) | Error::Schema(_) => {
                KafkaClass::BlockingPipeline
            }
            Error::KafkaOffsetMissing { .. }
            | Error::KafkaAppIdUnknown { .. }
            | Error::KafkaCompatUnset { .. }
            | Error::KafkaStartingOffsetsIncomplete { .. } => KafkaClass::OpenRefusal,
            // One partition's offsets, which an operator has to settle; the others go on.
            Error::KafkaOffsetsUnavailable { .. }
            | Error::KafkaOffsetsRegressed { .. }
            | Error::KafkaTopicReplaced { .. } => KafkaClass::BlockingPartition,
            // Messages that cannot be set aside: no data-quality table, or more of them than
            // the cap. The recheck reopens the table.
            Error::KafkaUndecodable { .. } | Error::KafkaTooManyUndecodable { .. } => {
                KafkaClass::BlockingPipeline
            }
            Error::KafkaClient { failure, .. } => match failure {
                KafkaClientFailure::Transport => KafkaClass::Transient,
                KafkaClientFailure::Fatal | KafkaClientFailure::Config => KafkaClass::ConsumerFatal,
            },
            Error::SchemaRegistry { failure, .. } => match failure {
                SchemaRegistryFailure::Unavailable => KafkaClass::Transient,
                SchemaRegistryFailure::Unauthorized => KafkaClass::ConsumerFatal,
                SchemaRegistryFailure::Refused => KafkaClass::BlockingPipeline,
            },
            // Every message of the writer schema needs the same thing: the pipeline holds
            // rather than quarantining them one by one.
            Error::KafkaSchemaReferences { .. } | Error::KafkaMetadataCollision { .. } => {
                KafkaClass::BlockingPipeline
            }
            // The SQL, or what the target needs of it, until someone changes one or the other.
            // Raised at open (where every refusal is OpenRefusal), or by a batch: the first one
            // when no writer schema was known at open, or one whose writer schema changed a
            // type the SQL reads.
            Error::KafkaPartitionColumnUnmapped { .. } | Error::KafkaSchemaConflict { .. } => {
                KafkaClass::BlockingPipeline
            }
            // An internal invariant, and the errors of a Delta source, which a Kafka pipeline
            // has none of.
            Error::Other(_)
            | Error::ChangeCommit { .. }
            | Error::CommitTooLarge { .. }
            | Error::DeletionVectorUnsupported { .. }
            | Error::UnsupportedProtocol { .. }
            | Error::CursorUnavailable { .. }
            | Error::BootstrapUnreachable { .. }
            | Error::SourceFileVacuumed { .. }
            | Error::SourceReplaced { .. } => KafkaClass::ConsumerFatal,
        }
    }

    /// Why a Kafka pipeline that holds on this error is held, as the `reason` label of
    /// `ddi_kafka_pipeline_blocked` (docs/kafka-design.md §7.1): one word per cause an
    /// operator acts on, and the class for the rest.
    pub fn kafka_block_reason(&self) -> &'static str {
        match self {
            Error::TargetReplaced { .. } => "target_replaced",
            Error::KafkaTxnRetention { .. } => "txn_retention",
            Error::TargetProtocolUnsupported { .. } => "protocol_unsupported",
            Error::Delta(e) if is_unwritable(e) => "protocol_unsupported",
            Error::KafkaOffsetVanished { .. } => "offset_vanished",
            Error::KafkaUndecodable { .. } | Error::KafkaTooManyUndecodable { .. } => "undecodable",
            Error::KafkaSchemaConflict { .. } => "schema_conflict",
            Error::KafkaSchemaReferences { .. } => "schema_references",
            Error::KafkaMetadataCollision { .. } => "metadata_collision",
            Error::KafkaPartitionColumnUnmapped { .. } => "partition_column_unmapped",
            Error::Transform(_) => "transform",
            Error::Evaluation(_) => "evaluation",
            Error::Schema(_) => "schema",
            Error::DataQualityWrite { .. } => "data_quality",
            Error::SchemaRegistry { .. } => "schema_registry",
            other => match other.kafka_class() {
                KafkaClass::Capacity => "capacity",
                KafkaClass::Replan | KafkaClass::Transient => "transient",
                _ => "other",
            },
        }
    }

    /// The class of this error as the cause beneath a [`Self::DataQualityWrite`]: the table of
    /// causes in docs/kafka-design.md §3.6.
    ///
    /// - the store failed, a commit is missing under a stale handle, the reopen after a failure
    ///   failed (`Config`), or any other delta-rs error: Transient, the write is retried and
    ///   reopens the table first;
    /// - a full spill directory: Capacity;
    /// - a table the rejects cannot be written to — its protocol, column mapping, a schema the
    ///   rejects do not fit — or a commit of its log that cannot be read: Blocking, pipeline,
    ///   because retrying the same write cannot help until someone changes the table;
    /// - an internal invariant (a batch that cannot be assembled): Consumer-fatal.
    fn data_quality_class(&self) -> KafkaClass {
        match self {
            Error::DataQualityWrite { source, .. } => source.data_quality_class(),
            Error::Capacity(_) => KafkaClass::Capacity,
            Error::TargetProtocolUnsupported { .. } => KafkaClass::BlockingPipeline,
            Error::Delta(e) if is_store_fault(e) => KafkaClass::Transient,
            Error::Delta(e) if is_unwritable(e) || is_unfit(e) || is_unreadable_log(e) => {
                KafkaClass::BlockingPipeline
            }
            Error::Other(_) => KafkaClass::ConsumerFatal,
            _ => KafkaClass::Transient,
        }
    }

    /// Whether a Kafka pipeline's supervisor waits the full `RETRY_MAX` before opening it
    /// again after this error (docs/kafka-design.md §3.9).
    ///
    /// Only open and consumer-fatal errors reach the supervisor: a step handles the rest in
    /// place. At open, everything that needs an operator refuses whatever its class in a step
    /// (§3.6, open-time refusal), and so does a consumer-fatal error that is a fact about the
    /// configuration (credentials, an authorisation, a topic that does not exist): retried a
    /// second later each would only open the target, read its offsets and fail the same way.
    /// A store or broker outage at open, and an internal fault, retry from a second, doubling.
    pub fn kafka_waits_for_an_operator(&self) -> bool {
        match self.kafka_class() {
            KafkaClass::OpenRefusal
            | KafkaClass::BlockingPipeline
            | KafkaClass::BlockingPartition
            | KafkaClass::Capacity => true,
            KafkaClass::ConsumerFatal => matches!(
                self,
                Error::KafkaClient {
                    failure: KafkaClientFailure::Config,
                    ..
                } | Error::SchemaRegistry {
                    failure: SchemaRegistryFailure::Unauthorized,
                    ..
                }
            ),
            KafkaClass::Replan | KafkaClass::Transient => false,
        }
    }

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

/// An undecodable message's writer schema, as an error names it.
fn writer_schema_id(id: Option<u32>) -> String {
    match id {
        Some(id) => format!("writer schema id {id}"),
        None => "no writer schema id".to_string(),
    }
}

/// The object store failed: worth retrying.
fn is_store_fault(e: &DeltaTableError) -> bool {
    use deltalake::kernel::transaction::TransactionError;
    matches!(
        e,
        DeltaTableError::ObjectStore { .. }
            | DeltaTableError::Io { .. }
            | DeltaTableError::Transaction {
                source: TransactionError::ObjectStore { .. }
                    | TransactionError::LogStoreError { .. },
            }
    )
}

/// The table's protocol asks for a writer this build cannot be, or one the rows' types need is
/// not enabled on it.
fn is_unwritable(e: &DeltaTableError) -> bool {
    use deltalake::kernel::transaction::TransactionError;
    matches!(
        e,
        DeltaTableError::Transaction {
            source: TransactionError::UnsupportedTableFeatures(_)
                | TransactionError::TableFeaturesRequired(_),
        }
    )
}

/// The rows do not fit the table's schema: a field count, a cast.
fn is_unfit(e: &DeltaTableError) -> bool {
    matches!(
        e,
        DeltaTableError::Arrow { .. } | DeltaTableError::SchemaMismatch { .. }
    )
}

/// A commit of the table's log is not one delta-rs can parse.
fn is_unreadable_log(e: &DeltaTableError) -> bool {
    matches!(e, DeltaTableError::InvalidJsonLog { .. })
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

    /// An error that holds the pipeline on undecodable messages names the first of them: its
    /// partition, offset and writer schema id, so it can be looked up.
    #[test]
    fn undecodable_errors_name_the_first_message() {
        let too_many = Error::KafkaTooManyUndecodable {
            count: 5,
            cap: 2,
            topic: "orders".into(),
            partition: 1,
            offset: 41,
            schema_id: Some(7),
            reason: "cannot turn the decoded record into Arrow".into(),
        }
        .to_string();
        assert!(
            too_many.starts_with(
                "5 messages of one batch cannot be decoded, more than \
                 max_evaluation_rejects_per_batch = 2; the first is partition 1 offset 41 of \
                 topic \"orders\" (writer schema id 7): cannot turn the decoded record into \
                 Arrow."
            ),
            "{too_many}"
        );
        let no_dq = Error::KafkaUndecodable {
            topic: "orders".into(),
            partition: 1,
            offset: 41,
            schema_id: None,
            reason: "not Confluent-framed".into(),
            dq_uri: "/lake/orders__ddi_dq".into(),
        }
        .to_string();
        assert!(
            no_dq.starts_with(
                "partition 1 offset 41 of topic \"orders\" (no writer schema id) cannot be \
                 decoded (not Confluent-framed)"
            ),
            "{no_dq}"
        );
    }

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
    /// Every row of docs/kafka-design.md §3.6 that an error of this build can fall in, read
    /// by variant and never by text — the one exception being delta-rs's foreign-checkpoint
    /// error, which has no variant of its own. A cause beneath `DataQualityWrite` is classified
    /// apart from the same error raised by the target: never a Replan.
    #[test]
    fn every_error_has_its_kafka_class() {
        use deltalake::kernel::transaction::{CommitConflictError, TransactionError};
        use deltalake::logstore::object_store;
        use KafkaClass::*;

        let outage = || {
            DeltaTableError::from(object_store::Error::Generic {
                store: "Azure",
                source: "503 Server Busy".into(),
            })
        };
        let transaction = |source| DeltaTableError::Transaction { source };
        let unreadable_commit = || {
            let json_err = serde_json::from_str::<serde_json::Value>("{not json").unwrap_err();
            DeltaTableError::InvalidJsonLog {
                json_err,
                line: "Error at line 1, column 2".into(),
                version: 4,
            }
        };
        let arrow = || DeltaTableError::Arrow {
            source: deltalake::arrow::error::ArrowError::SchemaError("no such field".into()),
        };
        let protocol = || Error::TargetProtocolUnsupported {
            target_uri: "/lake/orders__ddi_dq".into(),
            detail: "it uses column mapping".into(),
        };
        let beneath = |cause: Error| Error::DataQualityWrite {
            dq_uri: "/lake/orders__ddi_dq".into(),
            rows: 3,
            source: Box::new(cause),
        };

        let rows: Vec<(Error, KafkaClass)> = vec![
            // Raised by the target's own commit, or the reads before it.
            (
                Error::Delta(transaction(TransactionError::CommitConflict(
                    CommitConflictError::ConcurrentTransaction,
                ))),
                Replan,
            ),
            (
                Error::Delta(transaction(TransactionError::CommitConflict(
                    CommitConflictError::MetadataChanged,
                ))),
                Replan,
            ),
            (
                Error::Delta(transaction(TransactionError::VersionAlreadyExists(4))),
                Replan,
            ),
            (
                Error::Delta(transaction(TransactionError::MaxCommitAttempts(15))),
                Replan,
            ),
            (
                Error::Delta(DeltaTableError::VersionAlreadyExists(4)),
                Replan,
            ),
            // A commit log cleanup removed under a handle trailing the head, met in the
            // conflict check (EO-m8): a reset, not a retry from the same handle.
            (Error::Delta(DeltaTableError::InvalidVersion(4)), Replan),
            (
                Error::Delta(DeltaTableError::Generic(
                    "Invalid data type for Delta Lake: Timestamp(Millisecond, None)".into(),
                )),
                Replan,
            ),
            (
                Error::OffsetLogTruncated {
                    table_uri: "/lake/orders".into(),
                    version: 4,
                },
                Replan,
            ),
            (
                Error::Fenced {
                    app_id: "orders".into(),
                    partition: 3,
                    expected: Some(4),
                    found: Some(9),
                },
                Replan,
            ),
            (Error::Delta(outage()), Transient),
            (
                Error::Io(std::io::Error::other("connection reset by peer")),
                Transient,
            ),
            (
                Error::Config("cannot open \"/lake/orders\": 503".into()),
                Transient,
            ),
            (
                Error::DataQualityConflict {
                    dq_uri: "/lake/orders__ddi_dq".into(),
                    rows: 3,
                    attempts: 4,
                    detail: "MetadataChanged".into(),
                },
                Transient,
            ),
            (
                Error::Capacity("the spill directory is full".into()),
                Capacity,
            ),
            (
                Error::KafkaTxnRetention {
                    target_uri: "/lake/orders".into(),
                    retention: "interval 1 days".into(),
                },
                BlockingPipeline,
            ),
            (protocol(), BlockingPipeline),
            (
                Error::Delta(transaction(TransactionError::UnsupportedTableFeatures(
                    vec![serde_json::from_str("\"identityColumns\"").unwrap()],
                ))),
                BlockingPipeline,
            ),
            (
                Error::TargetReplaced {
                    target_uri: "/lake/orders".into(),
                    opened: "a".into(),
                    found: "b".into(),
                },
                BlockingPipeline,
            ),
            (
                Error::Transform("no such column: note".into()),
                BlockingPipeline,
            ),
            (
                Error::Schema("column note is missing".into()),
                BlockingPipeline,
            ),
            (
                Error::KafkaOffsetMissing {
                    app_id: "orders".into(),
                    partitions: vec![1, 3],
                    fresh: false,
                },
                OpenRefusal,
            ),
            (
                Error::KafkaOffsetMissing {
                    app_id: "orders".into(),
                    partitions: vec![0, 1],
                    fresh: true,
                },
                OpenRefusal,
            ),
            (
                Error::KafkaOffsetVanished {
                    target_uri: "/lake/orders".into(),
                    app_id: "orders".into(),
                    partitions: vec![(1, 41)],
                },
                BlockingPipeline,
            ),
            // The Schema Registry (§5.2): an outage after the client's own retries, wrong
            // credentials, a refusal or an answer that is not its API's; and the two facts
            // about a writer schema that hold every message of it: references, and a field
            // named like a metadata column (§6.1).
            (
                Error::SchemaRegistry {
                    url: "https://sr.internal:8081".into(),
                    failure: SchemaRegistryFailure::Unavailable,
                    detail: "HTTP 503".into(),
                },
                Transient,
            ),
            (
                Error::SchemaRegistry {
                    url: "https://sr.internal:8081".into(),
                    failure: SchemaRegistryFailure::Unauthorized,
                    detail: "HTTP 401".into(),
                },
                ConsumerFatal,
            ),
            (
                Error::SchemaRegistry {
                    url: "https://sr.internal:8081".into(),
                    failure: SchemaRegistryFailure::Refused,
                    detail: "HTTP 400".into(),
                },
                BlockingPipeline,
            ),
            (
                Error::KafkaSchemaReferences {
                    url: "https://sr.internal:8081".into(),
                    id: 7,
                    subjects: vec!["address-value".into()],
                },
                BlockingPipeline,
            ),
            (
                Error::KafkaMetadataCollision {
                    schema_id: 7,
                    field: "kafka_offset".into(),
                    prefix: "kafka_".into(),
                },
                BlockingPipeline,
            ),
            // The conform step (§5.5, §5.6): partition columns nothing produces, raised at open
            // or by the first batch when no writer schema was known then; and SQL that no
            // longer plans after a writer schema changed a type it reads.
            (
                Error::KafkaPartitionColumnUnmapped {
                    target_uri: "/lake/orders".into(),
                    columns: vec!["ingest_date".into()],
                    produced: vec!["id".into()],
                    sql: true,
                },
                BlockingPipeline,
            ),
            (
                Error::KafkaSchemaConflict {
                    fields: vec!["amount (writer schema 2: Struct, writer schema 1: Int64)".into()],
                    detail: "cannot add Struct and Int64".into(),
                },
                BlockingPipeline,
            ),
            // M3's: start resolution at open (§4.4), one partition's offsets (§4.4 table), the
            // messages that cannot be set aside (§5.7), and librdkafka's own (§3.6).
            (
                Error::KafkaAppIdUnknown {
                    app_id: "kafka_delta_ingset".into(),
                    target_uri: "/lake/orders".into(),
                    seen: vec!["kafka_delta_ingest".into()],
                },
                OpenRefusal,
            ),
            (
                Error::KafkaCompatUnset {
                    app_id: "orders".into(),
                    target_uri: "/lake/orders".into(),
                    fresh: false,
                },
                OpenRefusal,
            ),
            (
                Error::KafkaStartingOffsetsIncomplete {
                    topic: "orders".into(),
                    missing: vec![2],
                },
                OpenRefusal,
            ),
            (
                Error::KafkaOffsetsUnavailable {
                    topic: "orders".into(),
                    partition: 1,
                    wanted: 42,
                    low: 900,
                },
                BlockingPartition,
            ),
            (
                Error::KafkaOffsetsRegressed {
                    topic: "orders".into(),
                    partition: 1,
                    stored: 41,
                    high: 7,
                },
                BlockingPartition,
            ),
            (
                Error::KafkaTopicReplaced {
                    topic: "orders".into(),
                    partition: 1,
                    offset: 41,
                    recorded: "a".into(),
                    found: "b".into(),
                },
                BlockingPartition,
            ),
            (
                Error::KafkaUndecodable {
                    topic: "orders".into(),
                    partition: 1,
                    offset: 41,
                    schema_id: None,
                    reason: "not Confluent-framed".into(),
                    dq_uri: "/lake/orders__ddi_dq".into(),
                },
                BlockingPipeline,
            ),
            (
                Error::KafkaTooManyUndecodable {
                    count: 101,
                    cap: 100,
                    topic: "orders".into(),
                    partition: 1,
                    offset: 41,
                    schema_id: None,
                    reason: "not Confluent-framed".into(),
                },
                BlockingPipeline,
            ),
            (
                Error::KafkaClient {
                    failure: KafkaClientFailure::Transport,
                    detail: "all brokers down".into(),
                },
                Transient,
            ),
            (
                Error::KafkaClient {
                    failure: KafkaClientFailure::Fatal,
                    detail: "fenced by another static member".into(),
                },
                ConsumerFatal,
            ),
            (
                Error::KafkaClient {
                    failure: KafkaClientFailure::Config,
                    detail: "SASL authentication failed".into(),
                },
                ConsumerFatal,
            ),
            (
                Error::Other("internal: a broken invariant".into()),
                ConsumerFatal,
            ),
            (Error::ChangeCommit { version: 4 }, ConsumerFatal),
            // Beneath a failed data-quality write: about that table, never the target's.
            (beneath(Error::Delta(outage())), Transient),
            (
                beneath(Error::Delta(transaction(TransactionError::ObjectStore {
                    source: object_store::Error::Generic {
                        store: "Azure",
                        source: "timeout".into(),
                    },
                }))),
                Transient,
            ),
            (
                beneath(Error::Delta(DeltaTableError::InvalidVersion(1))),
                Transient,
            ),
            (beneath(Error::Config("cannot open".into())), Transient),
            (
                beneath(Error::OffsetLogTruncated {
                    table_uri: "/lake/orders__ddi_dq".into(),
                    version: 1,
                }),
                Transient,
            ),
            (
                beneath(Error::Delta(transaction(TransactionError::CommitConflict(
                    CommitConflictError::ConcurrentTransaction,
                )))),
                Transient,
            ),
            (beneath(Error::Capacity("full".into())), Capacity),
            (beneath(beneath(Error::Capacity("full".into()))), Capacity),
            (beneath(protocol()), BlockingPipeline),
            (
                beneath(Error::Delta(transaction(
                    TransactionError::TableFeaturesRequired(
                        serde_json::from_str("\"timestampNtz\"").unwrap(),
                    ),
                ))),
                BlockingPipeline,
            ),
            (beneath(Error::Delta(arrow())), BlockingPipeline),
            (beneath(Error::Delta(unreadable_commit())), BlockingPipeline),
            (
                beneath(Error::Other("cannot assemble".into())),
                ConsumerFatal,
            ),
        ];
        for (e, class) in rows {
            assert_eq!(e.kafka_class(), class, "{e:?}");
        }
    }

    /// A pipeline held on an error says why in `ddi_kafka_pipeline_blocked{reason}`: by the
    /// cause an operator acts on, and by the class for anything else.
    #[test]
    fn a_held_pipeline_has_a_reason_per_cause() {
        let rows = [
            (
                Error::TargetReplaced {
                    target_uri: "/lake/orders".into(),
                    opened: "a".into(),
                    found: "b".into(),
                },
                "target_replaced",
            ),
            (
                Error::KafkaTxnRetention {
                    target_uri: "/lake/orders".into(),
                    retention: "interval 1 days".into(),
                },
                "txn_retention",
            ),
            (
                Error::TargetProtocolUnsupported {
                    target_uri: "/lake/orders".into(),
                    detail: "identityColumns".into(),
                },
                "protocol_unsupported",
            ),
            (
                Error::KafkaOffsetVanished {
                    target_uri: "/lake/orders".into(),
                    app_id: "a".into(),
                    partitions: vec![(0, 41)],
                },
                "offset_vanished",
            ),
            (Error::Transform("no such column".into()), "transform"),
            (Error::Evaluation("division by zero".into()), "evaluation"),
            (Error::Schema("column note is missing".into()), "schema"),
            (
                Error::Capacity("the spill directory is full".into()),
                "capacity",
            ),
            (
                Error::Config("the target cannot be opened".into()),
                "transient",
            ),
            (Error::Other("internal".into()), "other"),
        ];
        for (e, reason) in rows {
            assert_eq!(e.kafka_block_reason(), reason, "{e:?}");
        }
    }

    /// §9.1 M3's name for the table above, with every Kafka-only variant M3 adds and the
    /// one-partition class: which of them the supervisor waits the full backoff for (§3.9).
    #[test]
    fn error_classification_table() {
        let waits = |e: Error| e.kafka_waits_for_an_operator();
        assert!(waits(Error::KafkaOffsetMissing {
            app_id: "a".into(),
            partitions: vec![0],
            fresh: false,
        }));
        assert!(waits(Error::KafkaTxnRetention {
            target_uri: "/t".into(),
            retention: "interval 1 days".into(),
        }));
        assert!(waits(Error::KafkaClient {
            failure: KafkaClientFailure::Config,
            detail: "unknown topic".into(),
        }));
        assert!(waits(Error::SchemaRegistry {
            url: "https://sr".into(),
            failure: SchemaRegistryFailure::Unauthorized,
            detail: "401".into(),
        }));
        assert!(waits(Error::Capacity("full".into())));
        assert!(!waits(Error::KafkaClient {
            failure: KafkaClientFailure::Fatal,
            detail: "fatal".into(),
        }));
        assert!(!waits(Error::KafkaClient {
            failure: KafkaClientFailure::Transport,
            detail: "all brokers down".into(),
        }));
        assert!(!waits(Error::Config("cannot open: 503".into())));
        assert!(!waits(Error::Other("internal".into())));
    }

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
