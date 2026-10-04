# ddi Kafka source: final design

**Base.** `~/ddi` on `feat/partition-watermark` (24f3dba), 2026-10-04; implemented on `feat/kafka-source`. Paths under `scratchpad/` refer to a throwaway session directory.

**What this is.** The merged design, revised against two critiques: exactly-once ("EO": 6 major, 19 minor, 4 gaps) and parity/ops ("PO": 2 blockers, 8 major, 10 minor, build facts). Every point is accepted or rejected in §12. Three things changed shape:
- **Rebalancing is deferred.** The callback only copies the partition list. The main loop reads offsets from Delta and then calls `assign()` with them; on revoke it commits the buffer before `unassign()`.
- **Errors no longer make a member leave its group.** Transient failures are retried in place, operator-action failures pause only what they affect, and only consumer-fatal errors reach the supervisor.
- **kdi parity is decided by Arrow type and key presence.** kdi wrote the literal text `"null"` into STRING columns.

**What I checked for this revision** (read-only; nothing edited or built by me):
- kdi: coercions.rs:55-120; lib.rs:826-880, 906-920, 1043-1062, 1200-1245.
- librdkafka (rdkafka-sys-4.10.0+2.12.1): rdkafka.h:1960-2030; rdkafka_cgrp.c:1200-1245, 1322-1373, 4572-4626, 6040-6066; rdkafka.c:3630-3660, 3802+; rdkafka_request.c:910-930.
- rdkafka-0.39.0: consumer/mod.rs:46-100, 239-404; consumer/base_consumer.rs:196-219; topic_partition_list.rs:190-195; client.rs:471-483; message.rs:341-351.
- ddi: budget.rs:60-70, 120-135, 265-296; dq.rs:60-300; config.rs:1405-1440; sink.rs:170-185; dedup.rs:390-412, 705-716; dbt/watermark.rs:200-250; pipeline.rs:1382-1402, 1521; main.rs:728-738, 820-852; metrics.rs (family names); transform/validate.rs:1495-1503.
- buoyant_kernel-0.22.2: log_reader/commit.rs:20-35; engine/default/json.rs:110-125; actions/set_transaction.rs:10-25, 60-82.
- apache-avro-0.21.0: types.rs:299-356; decimal.rs:85-151.
- arrow-avro-58.4.0: the public `Decoder` API (reader/mod.rs:677-880).
- `scratchpad/rdk-probe`, built today by the parity-ops reviewer.

Everything else cites the earlier verification pass (§11). Claims a milestone must still settle are marked **verify (Mx)**.

---

## 0. Decisions

### 0.1 One screen

| Topic | Decision |
|---|---|
| Seam | One `Pipeline` struct. Its Delta-only fields move into `Source::Delta(DeltaSource)`, and `Source::Kafka(Box<KafkaSource>)` is added behind the `kafka` feature. `step()` = source pull + shared `process()`: pipeline.rs:604-923 moved, about 10 lines parameterised. A golden commit test lands **before** the refactor; it covers a partitioned target and a DQ commit. |
| Config | `[kafka]` holds the cluster, Schema Registry, auth and defaults; `[[kafka.pipeline]]` holds the entries. Always parsed, and run whether or not `manifest` is set. Kafka uniqueness is per `(target_uri, app_id)` and `(dq_uri, app_id)`, not global. `group_id` defaults to `ddi-<name>`; two pipelines sharing a group in one config are rejected. Without the feature, every entry is a `Rejection`. |
| Client | rdkafka 0.39.0 with librdkafka 2.12.1, static, features `tokio, libz, ssl, zstd, tracing` (this already builds here). Eager protocol only. **Deferred rebalancing:** the callback copies the partition list and returns. The main loop reads offsets from Delta, then calls `assign()` with them. On revoke it commits the buffer (time-boxed), then calls `unassign()`. `ddi once` uses manual assignment and never joins the group. |
| Errors | A member leaves its group only on consumer-fatal errors. Transient failures hold the batch, pause, and retry in place. Operator-action failures pause only the affected partitions (or all of them, when the cause is pipeline-wide) and re-check every 60 s. |
| Offsets | Kept only in the target. txn appId `"<app_id>-<p>"`; `version` = last offset accounted for (kdi's convention). Every commit re-asserts every owned partition whose base is known, so delta-rs's txn conflict check fences everything. Lookups use a backward commit walker bounded by the newest checkpoint: one GET in steady state. |
| Missing offsets | **Fresh app_id:** `starting_offsets` (`earliest`, `latest`, or a complete map with an optional `"*"`). A txn-only baseline is committed when any start is > 0. **Not fresh:** a partition without a txn starts at **exactly offset 0**; `low > 0` is a retention gap that needs an acknowledged `set-offsets`. The typo guard (with a shared-table exemption) and the txn-retention refusal stay. |
| Incarnation | A partition resumes by re-reading its last written offset and comparing it with the timestamp and length ddi recorded. A recreated topic blocks the partition. |
| Decode | arrow-avro =58.4.0, plus a small SR client on reqwest 0.12 (`?deleted=true`). One decoder per writer schema id, a strict per-message check, isolation by re-decoding a chunk. |
| Evolution | **Identity path:** by-name nested conform to the target. **SQL path:** inputs are aligned to `U` = the latest registered version ∪ every id seen. The newest id's type wins; older values are cast, and rows that cannot be cast go to DQ. Never refuses to start because of history. |
| kdi parity | `kdi_compat`: a non-string value going into a STRING column is rendered by its **Arrow type, at every depth**, the way kdi's serde_json rendered it. `kdi_null_text`: a null becomes the text `"null"` where kdi's JSON would have had the key. Unset on a kdi-written table refuses to start; false on new tables. The parity reference is the apache-avro version inside the **production** kdi binary. |
| Bad messages | DQ table with per-partition marks `<app_id>.dq-<p>`. On replay, deterministic rejects at offsets ≤ the mark are skipped. A DQ conflict is retried in place. With no DQ table, that pipeline pauses but stays in its group. |
| Batching | Close on latency (the timer starts at the first message after a commit), message count, raw key+value bytes, or **measured** decoded bytes against the memory budget. Kafka never feeds `Amplification`. |
| Transforms | ddi's SQL, fixed `kafka_*` metadata columns (prefix configurable), and a translation table for kdi's `-t`. |
| Metrics | Prometheus `ddi_kafka_*`. `Waiting` counts as progress only when the member is caught up or committed recently. Alert rules ship with it. statsd only if dashboards need it. |
| Checkpoints | delta-rs post-commit hook. `delta.checkpointInterval = 10` at cutover for tables kdi ran with `-c`. `_delta_log` cleanup runs only on commits that also checkpoint. |
| Effort | About **38 dev-days** (range 33-43) in 8 milestones, starting with a 1.5-day inventory, plus soak. |

### 0.2 Where the two input designs disagreed (final decisions)

Design A was reuse-first, design B correctness-first. "rev." marks a decision changed after the critiques.

| # | Topic | Decision and reason |
|---|---|---|
| 1 | Config shape | **B:** `[kafka]` defaults plus `[[kafka.pipeline]]`. One cluster, one SR and one SCRAM identity; per-pipeline overrides keep the flexibility. |
| 2 | Seam | **A, plus B's golden test first.** One supervisor, metrics, DQ ordering and empty-commit path, instead of a second copy that drifts. |
| 3 | Txns per commit | **B:** every owned partition with a known base, on every commit. No log reads in steady state, and every commit fences a zombie (conflict_checker.rs:590-606). |
| 4 | Start position on assign | **rev. Neither: deferred assign.** librdkafka parks the group in `WAIT_ASSIGN_CALL`, with the old assignment paused and heartbeats running (rdkafka_cgrp.c:1349-1373, 6054-6066), until the application calls `assign()` (4594-4626). ddi therefore reads the true offsets first and assigns with them. This removes the in-memory `known` map, the post-assign seek, the pre-seek drop rule and the stale-low `-140` case. |
| 5 | Decoder | **B, with A's chunking:** one decoder per id, a strict check, 8192-row chunks. |
| 6 | SQL over several writer schemas | **A, rev.:** align to `U`, but `U` = latest ∪ seen. The newest type wins and rows that cannot be cast go to DQ (PO-M7). |
| 7 | STRING-column parity | **rev.:** rendered by Arrow type plus key presence (PO-B1, PO-M2). Default false for new tables; must be set explicitly on kdi tables. |
| 8 | Parity reference | **rev.:** the apache-avro version inside the production kdi image (PO-M1), not the fork's 0.21/0.22. |
| 9 | DQ idempotence | **rev.:** per-partition DQ txns `<app_id>.dq-<p>`, each holding the last offset covered; deterministic rejects at offsets ≤ it are skipped. dq.rs's argument for equality (a version can mean different data after a rewind, dq.rs:262-280) does not carry over: a Kafka offset names one message for the life of a topic incarnation, which §4.4 checks, and `set-offsets` moves the marks with the txns. Members never share a DQ appId (EO-M1). |
| 10 | Retention gap | **B:** recovered only by `ddi kafka set-offsets --acknowledge-skip "<reason>"`, recorded in the commit. |
| 11 | Missing offset while siblings have one | **rev.:** exactly offset 0; `low > 0` needs an acknowledged skip. Never the low watermark (EO-M4a). |
| 12 | Baseline commit | **rev.:** on a fresh app_id whenever any partition starts above 0. |
| 13 | Typo guard | **Adopted, rev.:** exempts targets whose txns all belong to other configured Kafka pipelines (EO-m15). |
| 14 | `max_bytes_per_batch` | **B:** raw key+value bytes, plus a decoded-bytes cap from the budget (EO-M5). |
| 15 | Metadata columns | **B:** always present, with a configurable prefix (PO-minor 6). |
| 16 | Tombstones | Skip (kdi parity, kdi lib.rs:866-871). |
| 17 | Static assignment, `raw` format | No user-facing static assignment; once-mode uses manual assignment internally. Keep `raw`. |
| 18 | `max.poll.interval.ms` | 600000. |
| 19 | Checkpoint extras | B's cleanup gating only. |
| 20 | `dynamic-linking` | Drop. rdkafka-sys requires ≥ 2.12.1 (build.rs:43-75); distributions ship 2.0-2.3. |
| 21 | SR `schemaType` absent | Means AVRO. |
| 22 | Offset lookup cost | **rev.:** the kernel's per-id lookup stops parsing early but still fetches every commit file since the checkpoint (EO-M6, verified). ddi walks the log backwards itself (§4.3). |

### 0.3 Facts that shape the design

**From the earlier pass, still valid:**
1. **Struct casts silently shift.** The coercer reaches `cast_struct_to_struct` (arrow-cast cast/mod.rs:1183, 2223-2252), which zips fields by position unless every target field exists in the source. By-name nested conforming is mandatory.
2. **A truncated Avro body is swallowed.** `Decoder::decode` returns `Ok(5)` and sets `awaiting_body` (reader/mod.rs:706-735), so a strict per-message check is required.
3. **kdi decodes soft-deleted schemas** (`GET /schemas/ids/{id}?deleted=true`; schema_registry_converter 4.10 mocks at async_impl/schema_registry.rs:765-857). **verify (M-1)** against the converter version actually deployed. `?deleted=true` only ever decodes more.
4. **Timezones and container names.** arrow-avro defaults timestamps to `"+00:00"` and names containers `item`/`entries`. ddi uses `Tz::Utc` and the kernel's `element`/`key_value` (buoyant_kernel engine/arrow_conversion/mod.rs:37-40).
5. **The conflict loop.** delta-rs checks every intermediate version and returns a handle that includes the winners (kernel/transaction/mod.rs:868-947, 996). `write()` plans from the handle's snapshot (operations/mod.rs:199-201).
6. **A re-sent commit fences itself** with `ConcurrentTransaction`, because every Kafka commit carries txns.
7. **Dropping a consumer blocks:** `Drop for BaseConsumer` polls until the consumer is closed (base_consumer.rs:762-780).
8. **Callbacks run on our task.** Rebalance and stats are served inside our own `recv()` (base_consumer.rs:85-91, 149-153; client.rs:316-319).
9. **ddi's txns never expire.** `Transaction::new` writes no `lastUpdated`, and the kernel never expires a txn without one (set_transaction.rs:10-21, re-read today). kdi writes `lastUpdated = now`.
10. **DataFusion's GenericDialect parses `SELECT * EXCEPT (..)` and `* REPLACE (..)`.**
11. **delta-rs cannot write column-mapped targets** (write/mod.rs:409-411).

**New today:**

12. **kdi writes the text `"null"`.**
    - `apply_coercion(ToString)` replaces any non-string JSON value, `Null` included, with `value.to_string()` (coercions.rs:82-86).
    - `coerce` and the `Tree`, `ArrayPrimitive` and `ArrayTree` arms apply this to every key present at any depth (coercions.rs:70-118).
    - apache-avro emits every record field (types.rs:331-335). So every null Avro field that landed in a STRING column became `"null"`.
    - Map-typed columns get no coercion node (coercions.rs:55-65).
13. **kdi re-asserts every assigned partition with a known offset**, and advances one only when it buffers a value.
    - Every commit carries `nonempty_partition_offsets` (lib.rs:1233-1241). `reset_state` loads a stored offset for every assigned partition (lib.rs:1043-1060).
    - Offsets advance only from the value buffers (lib.rs:915). Tombstones, undecodables and transform failures return before `value_buffers.add` (lib.rs:838-879).
    - So a kdi partition with no txn never had a row written under that app_id.
14. **librdkafka supports deferred assign and unassign.**
    - The rebalance op pauses the current assignment and parks the group in `WAIT_ASSIGN_CALL`/`WAIT_UNASSIGN_CALL` (rdkafka_cgrp.c:1349-1373).
    - Heartbeats continue in those states (rdkafka_cgrp.c:6054-6066). `assign()`/`unassign()` resume the assignment and complete the state change (rdkafka_cgrp.c:4575-4626).
    - rdkafka-rs lets ddi override `ConsumerContext::rebalance` (consumer/mod.rs:52-100).
    - The partition list is owned by the event (`ManuallyDrop`, base_consumer.rs:201-208); `TopicPartitionList: Clone` copies it (topic_partition_list.rs:190-195).
    - For an error other than assign or revoke, the application must call `assign(NULL)` (rdkafka.h:1989-1993).
15. **`fetch_watermarks` returns the last stable offset (LSO) under `read_committed`.** It calls `rd_kafka_query_watermark_offsets` (client.rs:471-483), which sends ListOffsets with the client's `isolation.level` (rdkafka_request.c:926-930).
16. **`Amplification::observe` only ever rises.** It is a `fetch_max` that decays only on restart (budget.rs:285-295), and `bytes_per_batch = share × 0.5 / amp` (budget.rs:120-133).
17. **The kernel's per-id txn lookup fetches every commit file since the checkpoint.** `CommitReader` hands all of them to one `read_json_files` (log_reader/commit.rs:27-30), which runs `stream::iter(..).buffered(buffer_size)` (json.rs:118-121). The early `break` (set_transaction.rs:69-78) only saves parsing.
18. **ddi has never written a partitioned target**; sink.rs:177-180 says so. The bronze tables are partitioned by `ingest_date`.
19. **The partition watermark refuses null partition values** inside a coverage window (dedup.rs:705-716).
20. **A consumption error carries no partition** (`KafkaError::MessageConsumption(code)`, message.rs:351). An out-of-range error therefore forces a recheck of every owned partition.
21. **Build facts:**
    - rdkafka 0.39 with `tokio, libz, ssl, zstd` builds here and reports librdkafka 2.12.1, with `ENABLE_SSL`, `WITH_SASL_SCRAM`, `WITH_ZSTD` and `WITH_SNAPPY`, and no GSSAPI.
    - A SASL_SSL / SCRAM-SHA-512 consumer is created without error.
    - `cooperative-sticky` is accepted when the consumer is created, so config must reject it.
    - rdkafka-sys reuses ddi's locked zstd-sys 2.0.16; a fresh resolve would pick 2.1.0.

---

## 1. Module layout and the seam into `pipeline.rs`

### 1.1 What `Pipeline::step` (pipeline.rs:566-923) does, and what Kafka reuses

| Lines | Stage | Kafka |
|---|---|---|
| 573-580 | Retire cut-off, `stream.next_batch()` | No: Kafka polls instead |
| 585 | `sink.set_source_identity` | No |
| 590-591 | Lookup snapshots | No: lookups are rejected for Kafka |
| 594-602 | `scan()` parquet, `in_rows`, `amplification.observe` | No: decode instead |
| 604-651 | `apply_isolating`, unevaluable rejects | **Yes** |
| 633-637 | `output`, `rejects` | **Parameterised:** rejects start as `batch.rejects`; the conform hook comes next |
| 653-694 | `produced`, staged full-row check | Yes (a no-op for append) |
| 696-747 | Coerce / `coerce_quarantining`, cut-off filter, `require_*` | **Yes** (cut-off is always `None`) |
| 757-772 | `closes`, `sink.set_cutoff` | Yes (always `None`) |
| 776-787 | `max_output_rows_per_batch` warning | Yes |
| 791 | `offsets.txn_version_for(batch.end)` | **Parameterised** |
| 793-825 | DQ write before the target commit | **Parameterised:** dispatch on `Offsets` |
| 838-850 | Publish render | Yes (publisher is always `None`) |
| 852-879 | Zero-row commit carrying the txns → `Skipped` | **Yes**: tombstone-only or all-rejected batches advance offsets here |
| 881-923 | commit/upsert, log line, `Progressed` | **Yes** (append only) |

The seam below the source is clean:
- `Transform` takes a `Vec<RecordBatch>`.
- `SqlTransform` builds a fresh `SessionContext` per call and registers `source` from the first batch's schema (transform/sql.rs:167-230, 392-411). `MemTable::try_new` needs one schema, which is why §5.5 aligns the inputs.
- `Identity` passes mixed-schema batches through untouched (transform/mod.rs:109-117).
- The coercer works batch by batch, by top-level name (schema.rs:70-133).

### 1.2 Refactor M0 (no Kafka dependency)

**Struct** (pipeline.rs:172-219). Fields become `pub(crate)` so `src/kafka/step.rs` can hold `impl Pipeline`.

```rust
pub struct Pipeline {
    cfg, target, transform, sink, coercer, cutoff, dq, grain_check_passes,
    lookup_table_ids, lookup_current_on_open, warned_lookup_table_id_changes,
    publisher, prev_published_through,          // unchanged
    pub(crate) source: Source,                   // replaces source/stream/offsets/amplification
}
pub(crate) enum Source {
    Delta(DeltaSource),
    #[cfg(feature = "kafka")]
    Kafka(Box<crate::kafka::KafkaSource>),
}
pub(crate) struct DeltaSource {
    table: DeltaTable, stream: LogStreamBuilder, offsets: OffsetStore,
    amplification: Arc<crate::budget::Amplification>,
}
```

The Delta-only references to move:
- `self.stream` at 550, 555, 574, 576 and 585, and the `retire_cutoff_if_read_through` helpers;
- `self.offsets` at 791;
- `self.amplification` at 602;
- `self.source` where it is opened and in replaced-source handling.

**Step split.**

```rust
pub async fn step(&mut self) -> Result<StepOutcome> {
    match &self.source {
        Source::Delta(_) => self.step_delta().await,
        #[cfg(feature = "kafka")]
        Source::Kafka(_) => self.step_kafka().await,       // src/kafka/step.rs
    }
}
async fn step_delta(&mut self) -> Result<StepOutcome> {
    let started = Instant::now();
    // 573-602 verbatim (self.stream → src.stream, …)
    self.process(Batch { started, through, files, input, rejects: Vec::new(),
                         lookups: lookup_batch, position: Position::Delta { start: batch.start, end: batch.end } }).await
}
#[derive(Clone)]                                   // cheap: Arc-backed arrays; lets Kafka retry a batch (§3.3)
pub(crate) struct Batch {
    pub started: Instant, pub through: Version /* Kafka: 0 */, pub files: usize /* Kafka: 0 */,
    pub input: Vec<RecordBatch>, pub rejects: Vec<Rejected> /* undecodables, set aside before the transform */,
    pub lookups: LookupBatch /* make pub(crate) + Clone, add none() */, pub position: Position,
}
#[derive(Clone)]
pub(crate) enum Position {
    Delta { start: StreamCursor, end: StreamCursor },
    #[cfg(feature = "kafka")] Kafka(crate::sink::KafkaOffsets),
}
```

**`process()` is pipeline.rs:604-923 moved verbatim, except these lines:**

| Line today | Today | After |
|---|---|---|
| 595 | `in_rows = input…sum()` (in `step`) | First line of `process()`, same expression |
| 633/637 | `let output = isolated.output; let mut rejects = Vec::new();` | `let mut rejects = batch.rejects;` then `let output = self.conform(output, &mut rejects)?;`. For `Source::Delta` this returns `output` unchanged, with no allocation; for Kafka it is §5.6. |
| 760 | `window_closes_after(c.window.through, batch.end, beyond)` | `position.delta_end().expect("a cut-off implies a Delta source")`, evaluated only inside `cutoff.map` |
| 791 | `let txn_version = self.offsets.txn_version_for(batch.end)?;` | `let offsets = self.offsets_for(&position)?;`. For Delta this is `Offsets::Version(src.offsets.txn_version_for(end)?)`, at the same point in the flow, so errors fire at the same time. |
| ~800 | `write_rejects(&rejects, txn_version)` | `write_rejects(&rejects, &offsets)`: `Version(v)` → the **unchanged** `DataQuality::write(rejects, v, now)`; `Kafka(k)` → the new `DataQuality::write_kafka(rejects, k, now)` (§5.7) |
| 862, 886 | `self.commit(.., txn_version)` | `self.commit(.., &offsets)` |
| 883 | `self.upsert(coerced, produced, txn_version)` | `let Offsets::Version(v) = &offsets else { unreachable_internal!() }; self.upsert(.., *v)`. Kafka is append-only, enforced at config. |
| 868, 909 | `publish(pending, batch.start.version, …)` | `position.delta_start_version().unwrap_or(0)`. The publisher is always `None` for Kafka and `publish` returns early. |

**`Pipeline::commit`** (pipeline.rs:1293-1307) takes `&Offsets` and matches:
- `Version(v)` → `self.sink.commit(table, batches, v)`, the **unchanged** method;
- `Kafka(k)` → `self.sink.commit_kafka(table, batches, k)`.

The reopen-on-error stays for Delta. For Kafka it is replaced by the error classes in §3.6. `cursor()` and `source_head_version()` (549-556) match on `Source`. In M3, `cursor()` becomes `Option<StreamCursor>`; its callers are main.rs:592 and the gauge at about 947.

**sink.rs.** `properties()` (104-159) and `commit()` (169-212) keep their signatures **and** their output. The body of `commit()` from `delta_session` through `.await` (177-204) moves into a private `write(table, batches, props)`. Additions:

```rust
#[derive(Clone, Debug)]
pub enum Offsets { Version(i64), Kafka(KafkaOffsets) }

/// Always compiled (no rdkafka types), so M0 can test N-txn commits without the feature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaOffsets {
    pub topic: String,
    pub group_id: String,
    /// Partitions consumed in this batch: (first, last) offset accounted for.
    pub ranges: BTreeMap<i32, (i64, i64)>,
    /// Every txn this commit writes: `last` for partitions in `ranges`, the unchanged base for every other
    /// owned partition whose base is known (§4.3). Always a superset of `ranges`' keys.
    pub assert: BTreeMap<i32, i64>,
    /// For each asserted partition where known: the message at the asserted offset (§4.4 incarnation check).
    pub marks: BTreeMap<i32, MessageMark>,
    pub cleanup_logs: bool,                      // §7.2
    pub baseline: bool,                          // txn-only start commit (§4.4)
    pub override_reason: Option<String>,         // set-offsets (§4.8)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageMark { pub offset: i64, pub timestamp_ms: i64, pub value_len: u32 }

impl Sink {
    pub async fn commit_kafka(&self, t: DeltaTable, b: Vec<RecordBatch>, k: &KafkaOffsets) -> Result<DeltaTable> {
        let txns = k.assert.iter().map(|(p, v)| Transaction::new(fence::txn_app_id(&self.app_id, *p), *v)).collect();
        let mut meta = vec![
            ("ddi.appId".into(), self.app_id.clone().into()),
            ("ddi.kafka.topic".into(), k.topic.clone().into()),
            ("ddi.kafka.groupId".into(), k.group_id.clone().into()),
            ("ddi.kafka.offsets".into(), json!({ "<p>": [first, last], … })),
            ("ddi.kafka.marks".into(),   json!({ "<p>": [offset, timestamp_ms, value_len], … })),
        ];
        if k.baseline { meta.push(("ddi.kafka.baseline".into(), true.into())) }
        if let Some(r) = &k.override_reason { meta.push(("ddi.kafka.offsetOverride".into(), r.clone().into())) }
        let props = CommitProperties::default()
            .with_application_transactions(txns)         // the plural replaces the vector (kernel/transaction/mod.rs:560-563)
            .with_metadata(meta)
            .with_cleanup_expired_logs(Some(k.cleanup_logs));
        self.write(t, b, props).await
    }
}
```

There is no `ddi.sourceVersion` on Kafka commits: it is written but never read (only dq.rs:219 and sink.rs:107 mention it).

**`src/fence.rs`** (always compiled, Kafka-agnostic: "partitioned offsets in a Delta table") holds the txn naming, the backward `read_txns` walker and the `Fence` (§4.3). Its unit tests run in M0 against local tables.

**dq.rs: additive.** `write` and `already_recorded` (dq.rs:191-290) stay byte-for-byte, so Delta DQ commits do not change. New: `kafka_app_id_for(app_id, p) = "<app_id>.dq-<p>"`, and `write_kafka(rejected, &KafkaOffsets, now)` (§5.7), which uses `fence::read_txns` on the DQ table.

### 1.3 Delta pipelines stay identical, and how that is proven

1. **Golden test first.** `tests/delta_commit_golden.rs` is committed **before** the refactor.
   - It drives the `tests/common` helpers (`pipeline_cfg`, `create_table`, `append`, tests/common/mod.rs:53-185) through six commit shapes:
     - plain append;
     - an all-rejected batch with a DQ table (zero-row commit plus DQ commit);
     - upsert;
     - an upsert whose no-op merge falls back to the zero-row commit;
     - **an append to a target partitioned by a string column;**
     - **the same target with an all-rejected batch** (zero-row commit on a partitioned table).
   - It reads every `_delta_log/*.json` of the target and DQ tables and normalises timestamps, uuids, file paths and `operationMetrics` timings. It then compares the actions plus the key/value set of `commitInfo` against a checked-in golden file. `commitInfo` is a map, so its key order was never stable byte for byte.
2. **Code motion** is reviewed with `git diff --color-moved=dimmed-zebra`. The only non-moved lines are those in the table above, plus the conform hook.
3. **Commit API.** `Sink::commit`/`properties` and `DataQuality::write` are untouched, so Delta commit JSON cannot change.
4. **Dependencies.** No default features. `cargo tree -e normal` without features is identical before and after; `Cargo.lock` gains the optional crates. M1 must **not** run `cargo update`: a fresh resolve moves zstd-sys to 2.1.0 under the Delta path.
5. **Config.** `Config` gains `#[serde(default, skip_serializing_if = "KafkaConfig::is_empty")] kafka: KafkaConfig`. Existing files have no `[kafka]`, so `resolve_all` (config.rs:1372) returns the same `Resolved`. The global app_id check (config.rs:1413-1436) keeps running over Delta pipelines exactly as today.
6. **Suite.** All 28 integration test files and the unit tests pass **unmodified** in M0. In M1, adding `ResolvedPipeline.kafka` needs `kafka: None` in five struct literals: config.rs:~1844, locate.rs:~149, tests/common/mod.rs:~185-204, tests/azure_object_store.rs:~201, tests/trino_locate.rs:~136. That is the same mechanical edit partition-watermark made for `dedup_partition`.
7. **Behaviour that changes only in configs with Kafka:**
   - `Config::budget(running)` reserves 32 MB per Kafka pipeline (librdkafka prefetch plus overhead) before dividing shares.
   - `/metrics` renders `ddi_kafka_*` only for Kafka pipelines.

### 1.4 Files

| File | Gate | Responsibility | ~LOC |
|---|---|---|---|
| src/pipeline.rs | always | `Source`, `Batch`, `Position`, `process()` split, `conform` hook, `commit(&Offsets)` | +140/-50 |
| src/sink.rs | always | `Offsets`, `KafkaOffsets`, `MessageMark`, `commit_kafka`, private `write` | +110 |
| src/fence.rs | always | Txn naming; `read_txns` (backward walk, checkpoint `txn` column, kernel fallback); `Fence`; retention, protocol and table-id refusals | 450 |
| src/dq.rs | always | `kafka_app_id_for`, `write_kafka` (marks, filtering, in-place conflict retry) | +160 |
| src/config.rs | always | `KafkaConfig`, `KafkaPipelineConfig`, `ResolvedKafka`, merge into `resolve_all`, validation, kdi env-name defaults | +500 |
| src/error.rs | always | Kafka error variants (§3.6), with no rdkafka types | +110 |
| src/metrics.rs | always | `kafka: Option<Arc<KafkaMetrics>>` on `PipelineMetrics`, rendered after the existing block only when `Some` | +220 |
| src/budget.rs | always | Per-Kafka-pipeline reservation | +20 |
| src/kafka/mod.rs | kafka | `KafkaSource`, `Work`, `KafkaProgress`, `Drop` (close on a detached thread) | 180 |
| src/kafka/client.rs | kafka | `ClientConfig` assembly: env compat, settings, `settings_env`, forced keys, redaction | 220 |
| src/kafka/context.rs | kafka | `DdiContext`: deferred rebalance events, stats, log, error, fatal | 200 |
| src/kafka/partitions.rs | kafka | Per-partition state, gate, duplicate/gap accounting, re-read verification, buffer, close policy, ranges | 350 |
| src/kafka/rebalance.rs | kafka | `assign_with_delta_offsets`, `flush_then_unassign`, blocked recheck | 250 |
| src/kafka/start.rs | kafka | Start resolution, baseline, typo guard, kdi-era checks | 260 |
| src/kafka/registry.rs | kafka | SR client, process-wide cache, latest subject version | 230 |
| src/kafka/decode.rs | kafka | Framing, per-id decoders, strict check, chunk isolation, normalisation, metadata builders, measured sizes | 480 |
| src/kafka/conform.rs | kafka | `U`, `align_to_union`, by-name nested conform, kdi rendering by type, null text by presence, float→decimal, unmatched-column guard | 800 |
| src/kafka/step.rs | kafka | `impl Pipeline { open_kafka, step_kafka, commit_batch, reset_in_place, kafka_progress }`, error classification, pending batch | 380 |
| src/kafka/tools.rs | kafka | `ddi kafka plan`, `clone-table`, `set-offsets`, `status` (group-less client) | 450 |
| src/main.rs | mixed | `Waiting` outcome and progress rule, gauges, status/validate branches, `kafka` subcommand, `RETRY_MAX` classes, `--version`, shutdown flush | +140 |

About 5.5k production lines and 3.5k test lines.

---

## 2. Config

### 2.1 TOML

```toml
# Shared by every [[kafka.pipeline]]; a pipeline's own key wins.
[kafka]
brokers                   = "b-1.kafka.internal:9096,b-2.kafka.internal:9096"   # kdi -k (required here or per pipeline)
allowed_latency_secs      = 300          # kdi -l; the timer starts at the first message after the last commit
max_bytes_per_batch       = "128MB"      # Kafka key+value bytes per commit; decoded bytes are capped by the budget too
max_messages_per_batch    = 1000000      # optional commit trigger (NOT kdi's -m, see §2.3)
target_file_size          = "128MB"      # kdi -b
starting_offsets          = "earliest"   # fresh app_id: "earliest" | "latest" | { "0" = 1234, "*" = "latest" }
                                         # map values = NEXT offset to read; without "*" it must list every partition
metadata_prefix           = "kafka_"     # metadata column names (§6.1)
commit_offsets_to_kafka   = false        # informational (lag tooling); never read back. kdi never committed.
revoke_flush_timeout_secs = 120          # commit the buffer on revoke within this, else drop it (§3.4)
blocked_recheck_secs      = 60           # how often a paused pipeline/partition re-checks its cause (§3.6)
# kdi_compat    = <unset>                # unset: false on a new table, refused on a kdi-written one (§5.6)
# kdi_null_text = <kdi_compat>
# kdi credential env vars; these are the defaults, "" disables one
scram_json_env = "KAFKA_DELTA_INGEST_SCRAM_JSON"
ssl_cert_env   = "KAFKA_DELTA_INGEST_CERT"
ssl_key_env    = "KAFKA_DELTA_INGEST_KEY"
ssl_ca_env     = "KAFKA_DELTA_INGEST_CA"

[kafka.schema_registry]                  # kdi --avro <url>
url          = "https://sr.internal:8081"
username_env = "SCHEMA_REGISTRY_USERNAME"   # kdi's names (serialization.rs:96-107)
password_env = "SCHEMA_REGISTRY_PASSWORD"
proxy_env    = "SCHEMA_REGISTRY_PROXY"
# ca_pem_env = "SCHEMA_REGISTRY_CA_PEM"     # PEM added to rustls' webpki roots (kdi used the OS store)

[kafka.settings]                         # librdkafka passthrough (kdi -K k=v)
"security.protocol" = "SASL_SSL"
"sasl.mechanism"    = "SCRAM-SHA-512"

[kafka.settings_env]                     # librdkafka key = env var name, read at open
# "sasl.password" = "KAFKA_PASSWORD"

[[kafka.pipeline]]
name         = "catalog_description_changed_raw"
app_id       = "kafka_delta_ingest"                 # MUST equal kdi's -a to take over its table
topic        = "catalog-description-changed"        # exactly one topic, no regex
# group_id   = "ddi-catalog_description_changed_raw" # the default; kdi's -g only for the zero-pause variant (§10.4)
target_uri   = "abfss://lake@acct.dfs.core.windows.net/dat_kafka_raw/catalog_description_changed"
value_format = "avro_confluent"                     # | "raw" (kafka_value BINARY; tests and diagnostics)
subject      = "catalog-description-changed-value"  # default "<topic>-value"; feeds the SQL-path union (§5.5)
kdi_compat   = true                                 # taken over from kdi: keep its STRING rendering
# PLACEHOLDER: the real expression comes from this deployment's kdi -t (inventory, M-1). kdi could only read
# kafka.* as a whole right-hand side, so its ingest_date came from a payload field.
transform_sql = """
SELECT *, substr("meta"."producer"."timestamp", 1, 10) AS ingest_date FROM source
"""
# dq_uri = "..."                                  # default <target_uri>__ddi_dq (dq.rs:69-71)
# max_evaluation_rejects_per_batch = 100          # also caps undecodables per batch (§5.7)
[kafka.pipeline.settings]
"client.id" = "ddi-catalog-description-changed"
```

**Secrets.** Config holds only environment variable names, read in `open_kafka`. `ResolvedPipeline` derives `Debug` (config.rs:513), so no secret value may be stored in it; this follows `PublisherConfig`'s custom `Debug`. Settings whose keys match `password|secret|\.pem|key` are redacted in every log line.

### 2.2 Structs and resolution

`#[serde(flatten)]` cannot be combined with `deny_unknown_fields`. So `KafkaConfig` (the section) and `KafkaPipelineConfig` each declare the shared keys as explicit `Option` fields, and `resolve_kafka` merges them with the pipeline's value winning.

```rust
#[derive(Clone, Debug, Default, Deserialize, Serialize)] #[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    pub brokers: Option<String>, pub allowed_latency_secs: Option<u64>, pub max_bytes_per_batch: Option<String>,
    pub max_messages_per_batch: Option<usize>, pub target_file_size: Option<String>,
    pub starting_offsets: Option<StartingOffsets>,   // #[serde(untagged)] Named(Earliest|Latest) | Map(BTreeMap<String, OffsetOrNamed>)
    pub kdi_compat: Option<bool>, pub kdi_null_text: Option<bool>, pub metadata_prefix: Option<String>,
    pub commit_offsets_to_kafka: Option<bool>, pub revoke_flush_timeout_secs: Option<u64>, pub blocked_recheck_secs: Option<u64>,
    pub scram_json_env: Option<String>, pub ssl_cert_env: Option<String>, pub ssl_key_env: Option<String>, pub ssl_ca_env: Option<String>,
    pub schema_registry: Option<SchemaRegistryConfig>,
    #[serde(default)] pub settings: BTreeMap<String, String>,
    #[serde(default)] pub settings_env: BTreeMap<String, String>,
    #[serde(default, rename = "pipeline")] pub pipelines: Vec<KafkaPipelineConfig>,
}
```

Each entry becomes a `ResolvedPipeline` (config.rs:513-585):

| Field | Value |
|---|---|
| `source_uri` | `kafka://<topic>`. Logs only; `validate` must skip `storage.check` on it (main.rs:301). |
| `transform_sql` | The same `normalise_sql_with_lookups(sql, &{})` validator `[[pipeline]]` uses |
| `allowed_latency_secs`, `max_bytes_per_batch` (`parse_size`), `max_output_rows_per_batch`, `target_file_size`, `dq_uri`, `max_evaluation_rejects_per_batch` | Defaulted from `[kafka]`, then `[runtime]` |
| `lookups`, `dedup_*`, `upsert_*`, `publish`, `publish_to`, `stage_for`, `watermark_uri`, `source_relation`, `target_relation` | Empty or `None`. With no relations, `Locator::refresh` (locate.rs:114-129) costs only a clone per step. |
| `write_mode` | `Append` |
| `kafka` | `Some(Arc<ResolvedKafka>)`: topic, group, brokers, merged settings (values only for non-secret keys), env names, SR config, start policy plus an `explicit` flag, `kdi_compat`/`kdi_null_text` (both `Option`), `metadata_prefix`, limits and timeouts, `commit_offsets_to_kafka`, and `once` (manual assignment, stop at end; set by `ddi once`) |

`resolve_all` (config.rs:1372):
- Kafka entries are appended to whichever set runs, manifest-derived or `[[pipeline]]`. `[[pipeline]]` is ignored when `manifest` is set (config.rs:621-623), and dbt never describes the Kafka leg.
- The "nothing to run" error fires only when both are empty.
- Name uniqueness covers both kinds. app_id rules are in §2.5.
- Cascade checks are untouched: a Delta pipeline reading a Kafka target is the normal bronze→silver path.

### 2.3 kdi CLI/env → ddi

| kdi (main.rs:349-515) | kdi default | ddi |
|---|---|---|
| topic (positional), `KAFKA_TOPIC` | | `topic` |
| table_location, `TABLE_LOCATION` | | `target_uri` |
| `-k`, `KAFKA_BROKERS` | localhost:9092 | `brokers` (required) |
| `-g`, `KAFKA_CONSUMER_GROUP` | kafka_delta_ingest | `group_id`, default `ddi-<name>`. Offsets live in Delta, so a new group loses nothing, but it needs a Read ACL. Set kdi's group only for the zero-pause variant. |
| `-a`, `APP_ID` | kafka_delta_ingest | `app_id` (must match for takeover; several tables may share it) |
| `-o`, `KAFKA_AUTO_OFFSET_RESET` | earliest | `starting_offsets`, on a fresh app_id only. kdi's `-o` also chose the start of partitions without a txn; ddi always uses exactly offset 0 there (§4.4). |
| `--seek_offsets p:o` | | kdi stored `o` as the txn ("last written"), so this is `starting_offsets = { "p" = o+1 }` or `ddi kafka set-offsets --partition p --next o+1`. Not used by you. |
| `-l`, `ALLOWED_LATENCY` | 300 | `allowed_latency_secs` |
| `-m`, `MAX_MESSAGES_PER_BATCH` | 5000 | No equivalent. In kdi this sized the in-memory record batch, not a commit trigger. Do not copy 5000 into `max_messages_per_batch`. |
| `-b`, `MIN_BYTES_PER_FILE` | 134217728 | `target_file_size`. kdi committed at latency or at 128 MB of parquet. Start ddi at `max_bytes_per_batch = "128MB"` (raw Avro) and tune from observed file sizes. |
| `-t`, `TRANSFORMS` | | `transform_sql` (§6) |
| `-c`, `WRITE_CHECKPOINTS` | off | Table property `delta.checkpointInterval = 10` (§7.2) |
| `-K k=v` | | `[kafka.settings]` |
| `-s`, `STATSD_ENDPOINT`, `KDI_METRICS_PREFIX` | localhost:8125, kafka_delta_ingest | `--metrics-addr` (Prometheus, §7); statsd shim only if needed |
| `--avro <url>`, `AVRO_REGISTRY` | | `schema_registry.url` |
| `SCHEMA_REGISTRY_USERNAME/_PASSWORD/_PROXY` (serialization.rs:96-107) | | Same names by default |
| `KAFKA_DELTA_INGEST_SCRAM_JSON/_CERT/_KEY/_CA` (lib.rs:1299-1321) | | Same names by default |
| `-e`, `--ends_at_latest_offsets` | | `ddi once` (§3.8). Not used by you. |
| `--dlq_*`, `--json`, `--soe-avro`, `--decompress_gzip` | | Not ported. A DQ table replaces the DLQ (§5.7). |

### 2.4 librdkafka settings: merge order and forced keys

Later steps win, mirroring kdi (lib.rs:1297-1341):

1. **ddi defaults:**
   - `client.id = ddi-<name>`
   - `statistics.interval.ms = 15000`
   - `max.poll.interval.ms = 600000`
   - `queued.max.messages.kbytes = 16384` (librdkafka's default is 65536 per consumer, CONFIGURATION.md:141, and several pipelines share one process)
2. **kdi environment compatibility** (names from §2.1):
   - SCRAM JSON `{"username","password"}` → `sasl.username`/`sasl.password`. Invalid JSON is a config error; kdi panicked (lib.rs:1310).
   - The PEM variables → `ssl.certificate.pem`, `ssl.key.pem`, `ssl.ca.pem`.
3. `[kafka.settings]`, then `[kafka.pipeline.settings]`.
4. `settings_env`, section first and then pipeline. A missing variable is a config error.
5. **Forced keys.** It is a config error for the user to set any of these:
   - `bootstrap.servers` (from `brokers`) and `group.id` (from `group_id`);
   - `enable.auto.commit=false`, `enable.auto.offset.store=false`;
   - `auto.offset.reset=error`, which makes librdkafka raise `RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET` (-140) instead of jumping (CONFIGURATION.md:203);
   - `group.protocol=classic`, because "Default is `classic`, but will change to `consumer` in next releases" (CONFIGURATION.md:133), and KIP-848 `consumer` is always incremental;
   - `enable.partition.eof=false`.

### 2.5 Validation

Each failure is a `Rejection`: the pipeline is held back and `ddi_pipeline_config_valid` reads 0.

1. **Built without the `kafka` feature:** "this ddi was built without the `kafka` feature". Delta pipelines run.
2. **Required values.** `name`, `app_id`, `topic`, `target_uri` and `brokers` must be non-empty. `value_format = "avro_confluent"` requires an http(s) `schema_registry.url`.
3. **Names** are unique across both kinds.
4. **app_ids.**
   - Delta pipelines keep today's global app_id check among themselves (config.rs:1413-1436).
   - Kafka pipelines must be unique per `(target_uri, app_id)` and per `(dq_uri, app_id)`. Txn appIds live in each table's own log, so one kdi app_id (often the default `kafka_delta_ingest`) may be reused across tables.
   - A Kafka app_id equal to a Delta pipeline's app_id is allowed only on different targets.
5. **Target sharing:**
   - A Kafka target that is also a Delta pipeline's target: reject both. A Delta app_id `x-0` would otherwise collide with Kafka app_id `x`, partition 0.
   - Two Kafka pipelines on one target with the same topic: reject (that would duplicate data).
   - Distinct topics with distinct app_ids may share a table. kdi allowed it, and partition-watermark documents `["kafka_topic","kafka_partition"]` for exactly this (config.rs:341-342). Appids `"<a>-<digits>"` never collide across different `a`.
6. **Groups.** Two Kafka pipelines in one config with the same `group_id`: reject both. They would rebalance each other.
7. **Forced keys** (§2.4) present in any settings map: reject, pointing at the ddi key that owns them.
8. **Rebalancing.** Reject a `partition.assignment.strategy` other than `range` and/or `roundrobin` (kdi used librdkafka's default `range,roundrobin`, CONFIGURATION.md:129), in particular `cooperative-sticky`. Reject a `group.protocol` other than `classic`.
9. **`sasl.mechanism = GSSAPI`:** reject. The build has no libsasl2.
10. **Environment variables.** A `settings_env` entry or an explicitly set `*_env` name pointing at an unset variable: reject at open. The kdi defaults are optional and no-ops when unset.
11. **Inline secrets** (`sasl.password`, `ssl.key.password`, `ssl.key.pem` in `settings`): a **warning**, not a rejection.
12. **Numbers and shapes.**
    - `starting_offsets` map keys must be i32 ≥ 0 or `"*"`. Values must be i64 ≥ 0, or `earliest`/`latest` for `"*"`. Completeness is checked at open against metadata.
    - `allowed_latency_secs ≥ 1`, `max_messages_per_batch ≥ 1`, `max_bytes_per_batch ≥ 1MB`, `revoke_flush_timeout_secs` in [5, 300].
    - `metadata_prefix` matches `[a-z_][a-z0-9_]*`.
13. **`transform_sql`** must pass the normal validator.
14. **Delta-only keys** (`lookups`, `write_mode`, `publish`, `dedup_*`, `upsert_*`) are not in the struct, so `deny_unknown_fields` rejects them.
15. **`isolation.level = read_uncommitted`:** accepted, with a warning.

---

## 3. Consumer loop

### 3.1 Crate and features

```toml
[features]
default = []
kafka = ["dep:rdkafka", "dep:arrow-avro"]

[dependencies]
rdkafka    = { version = "0.39", default-features = false, features = ["tokio", "libz", "ssl", "zstd", "tracing"], optional = true }
# Lockstep with deltalake's arrow (58.4.0). default-features off: the default codecs are for OCF files only,
# and xz would bring back the `links = "lzma"` collision (Cargo.toml:30-33).
arrow-avro = { version = "=58.4.0", default-features = false, optional = true }

[dev-dependencies]
apache-avro = "=<version in the production kdi binary>"   # parity tests only; set in M-1 (0.21.0 if the fork is what runs)
```

- **`ssl`** runs mklove with `--enable-ssl` against system OpenSSL 3. SASL PLAIN and SCRAM-SHA-256/512 come with it; no libsasl2 is needed. This is settled by the probe build (§0.3.21); M1 keeps it as a regression check: `builtin.features ⊇ ssl,sasl_scram,zstd`.
- **`sasl` is not enabled.** It maps to `gssapi`, i.e. Kerberos only (rdkafka-sys Cargo.toml:48-61).
- **`zstd`** reuses the locked `zstd-sys 2.0.16+zstd.1.5.7`, built bundled. Snappy and lz4 are built into librdkafka; gzip uses zlib through `libz-sys`.
- **Not `rskafka`:** it has no consumer groups.

### 3.2 `DdiContext` (src/kafka/context.rs)

The rebalance callback runs synchronously inside our own `recv()` (base_consumer.rs:149-153, 196-209). It **does not** assign or unassign. librdkafka keeps the group waiting, with the old assignment paused and heartbeats running, until the main loop calls `assign()`/`unassign()` (§0.3.14).

```rust
impl ConsumerContext for DdiContext {
    fn rebalance(&self, c: &BaseConsumer<Self>, err: RDKafkaRespErr, tpl: &mut TopicPartitionList) {
        if matches!(c.rebalance_protocol(), RebalanceProtocol::Cooperative) {          // backstop; config rejects it
            let _ = if err == ASSIGN { c.incremental_assign(tpl) } else { c.incremental_unassign(tpl) };
            return self.push(Event::Fatal("cooperative rebalancing is not supported".into()));
        }
        self.push(match err {                                     // the list dies with the event: copy it
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => Event::Assign(tpl.clone()),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => Event::Revoke { tpl: tpl.clone(), lost: c.assignment_lost() },
            other => Event::RebalanceError(other),                // main loop: drop + assign(NULL) (rdkafka.h:1989-1993)
        });
    }
}
impl ClientContext for DdiContext {
    fn stats(&self, s: Statistics) { /* per partition: hi_offset, ls_offset, lo_offset (statistics.rs:268-273) → KafkaMetrics */ }
    fn log(&self, level, fac, msg) { /* tracing event with pipeline = name */ }
    fn error(&self, e: KafkaError, reason: &str) {
        /* count by code; AllBrokersDown → brokers_up = 0 (not fatal);
           Fatal (librdkafka fatal error, incl. FENCED_INSTANCE_ID under static membership) → push Event::Fatal */
    }
}
```

`push` appends to a `Mutex<VecDeque<Event>>` that the main loop drains after every `recv()`.

### 3.3 Per-partition state, the poll loop and the step

Per owned partition (src/kafka/partitions.rs):

```
Ready   { next_expected: i64, reread: Option<MessageMark /* expected */ | Unverifiable>, mark: Option<MessageMark> }
Blocked { reason: KafkaBlock, since: Instant }    // paused; messages dropped; re-checked every blocked_recheck_secs
(absent) = not owned
```

`KafkaSource::next_work(&mut target)` returns one of:
- `Waiting`;
- `Done` (once-mode);
- `Batch(KafkaBatch)`;
- `Retry(KafkaBatch)`, a held batch whose backoff has elapsed.

The buffer and any held batch live in `KafkaSource`, so returning between steps loses nothing.

```
loop until TICK (1 s) elapsed or a batch closes:
    if pending batch due → return Retry
    if blocked recheck due → recheck_blocked(target).await
    deadline = buf.opened_at.map(|t| t + allowed_latency)        // opened by the first message after the last commit (kdi #233)
    polled = timeout(min(TICK left, deadline left), consumer.recv()).await     // cancellation-safe (stream_consumer.rs:290-320)
    msg = polled.map(|r| r.map(|m| OwnedMsg::copy(&m)))          // copy out NOW; the BorrowedMessage dies here (EO-m3)
    handle_events(target).await?        // in order: Assign → assign_with_delta_offsets (§3.4); Revoke → flush_then_unassign;
                                        // RebalanceError → drop buffer + assign(NULL); Fatal → Err(consumer-fatal)
    match msg:
      Some(Ok(m)) =>
        st = parts.get_mut(m.partition) else dropped{not_owned}
        Blocked → dropped{blocked}
        if st.reread is pending:                                  // the last written offset, re-read on purpose (§4.4)
            if m.offset == expected.offset:
                if expected has ts/len and they differ → block p: KafkaTopicReplaced; continue
                st.mark = Some(mark_of(m)); st.next_expected = m.offset + 1; dropped{reread}; continue
            if m.offset > expected.offset → no verification possible (compacted / control record); st.next_expected = expected.offset + 1
            st.reread = None
        if m.offset < st.next_expected → dropped{duplicate}
        if m.offset > st.next_expected → offset_gaps += m.offset - st.next_expected     // compaction, transaction markers
        st.next_expected = m.offset + 1; st.mark = Some(mark_of(m)); ranges[p] = (first ∪ m.offset, m.offset); raw_bytes += key+value
        payload None → tombstones += 1 (offset still accounted)
        else decode.feed(p, m).await                              // may fetch a schema (§5.4)
      Some(Err(e)) if code == AutoOffsetReset → out_of_range(target).await?                     // §3.6
      Some(Err(e)) if consumer-fatal code → Err(Kafka(..))
      Some(Err(e)) → count by code, continue (librdkafka retries itself)
      None → {}
    close when buf non-empty && (now ≥ deadline || messages ≥ max_messages_per_batch
                                 || raw_bytes ≥ max_bytes_per_batch || decoded_bytes ≥ budget::current().batch_bytes())
    once-mode: Done when buf empty && every partition is done (§3.8)
```

- **`decoded_bytes`** is measured, not predicted. After each 8192-row chunk flush it sums `column.to_data().get_slice_memory_size()`, i.e. used bytes, not builder capacity. Rows still inside the decoders are counted at the batch's running decoded/raw ratio, starting at 4×. `budget::current().batch_bytes()` is `per_pipeline × BATCH_SHARE` (budget.rs:68), or unbounded. Kafka never calls `Amplification::observe` (EO-M5).
- **On close,** `decode.finish()` flushes every group and builds a `KafkaBatch { input, rejects, offsets }`:
  - `input`: per-group batches; on the SQL path each is aligned to `U` (§5.5).
  - `rejects`: one `Rejected` holding all undecodables (§5.7).
  - `offsets`: `KafkaOffsets` with `ranges`, `assert` and `marks` from the fence (§4.3).

**`step_kafka` (src/kafka/step.rs):**

```rust
let batch = match src.next_work(&mut self.target).await? {
    Work::Waiting => return Ok(StepOutcome::Waiting),
    Work::Done => return Ok(StepOutcome::CaughtUp),
    Work::Batch(b) | Work::Retry(b) => b,
};
self.commit_batch(batch).await
```

`commit_batch`:
1. **Aging (EO-m8).** If the handle has not advanced through our own commit or a reset for `handle_max_age` (10 min): refresh incrementally, then `fence.reset`. If any batch partition's base changed, drop the batch and `reset_in_place`.
2. **`fence.verify(&self.target, &ranges)`.** A mismatch → `reset_in_place`; counts `fenced_total{kind="base_mismatch"}`.
3. **`self.process(batch.clone())`** (cheap clone, so the batch can be kept). Then:
   - `Ok(o)` → `src.committed(&offsets)`; resume paused partitions; if `commit_offsets_to_kafka`, `commit_async`; return `o`.
   - `Err(e)` in class **Replan** → `reset_in_place`. The batch survives iff every partition it covers still has `base_p = first_p − 1`; it is then retried at once, with nothing re-sought. Otherwise it is dropped and the changed partitions are re-sought (EO-m10).
   - `Err(e)` in class **Transient** → hold the batch, pause all owned partitions, back off (1 s doubling to 60 s), return `Waiting`. Counts `errors_total`; `ddi_kafka_retrying = 1`.
   - `Err(e)` in class **Blocking** → hold the batch while it stays valid, pause (one partition or all, §3.6), set the gauge, re-check every `blocked_recheck_secs`, return `Waiting`.
   - `Err(e)` in class **Consumer-fatal** → `Err(e)`: the supervisor drops the consumer and reopens.

`reset_in_place` returns `Ok(StepOutcome::Waiting)` and does the following, staying in the group:
1. Refresh the target incrementally (`update_incremental`); fall back to `Storage::open` if that fails. `Storage::open` steps over foreign checkpoints (storage.rs:95-142).
2. Table id unchanged since open, else **Blocking** `TargetReplaced`.
3. No `setTransactionRetentionDuration`, else **Blocking** `KafkaTxnRetention`.
4. Writer protocol still writable, else **Blocking** `TargetProtocolUnsupported` (EO-m19).
5. Rebuild `self.coercer` if the target schema changed (a `MetadataChanged` conflict).
6. `fence.reset` over the owned partitions (one backward walk, §4.3).
7. Seek the partitions whose base changed, and those not covered by a surviving batch, to base+1 (`seek_partitions`, `spawn_blocking`, 10 s timeout). Reset their `next_expected`.
8. Count `ddi_kafka_fenced_total{kind}`.

### 3.4 Rebalance handling (eager only, deferred)

**`Assign(tpl)` → `assign_with_delta_offsets`.** The group waits meanwhile, heartbeating; this takes seconds.
1. Refresh the target incrementally, as in `reset_in_place` steps 1-5 (EO-m11). The full `Storage::open` remains for the supervisor and for a failed refresh.
2. `fence.reset(target, partitions)`: bases and marks in one backward walk (§4.3).
3. `fetch_watermarks` per partition (`spawn_blocking`, 8 at a time, 10 s timeout). Under `read_committed` "high" is the LSO (§0.3.15).
4. Resolve each start (§4.4): `Start { offset, reread }` or `Blocked(reason)`.
5. Write the offsets into the copied list; blocked partitions get `Offset::End`. Call `assign(&tpl)`, then `pause` the blocked ones (any message that slips through for them is dropped).
6. Set `parts`, the gauges (`assigned_partitions`, `blocked_partitions`) and, in once-mode, the end offsets.

This replaces "write offsets from a `known` map in the callback, then reseek": no map, no seek right after assign, no pre-seek drop rule (EO-m2).

**`Revoke { lost: false }` → `flush_then_unassign`:**
1. If a held batch or a non-empty buffer exists and the pipeline is not blocked: close the buffer and run `commit_batch` under `timeout(revoke_flush_timeout_secs)`. Success commits it. Failure, timeout or fence → drop it; the next owner re-reads from the txn. Cancelling an in-flight Delta commit is safe: it landed or it did not, and the next owner reads the truth.
2. `unassign()`; drop decoders and `parts`; `fence.forget(p)` for all of them (I6). Count `rebalances_total{kind="revoke"}` and `revoke_flushes_total{outcome="committed|dropped|timeout|none"}`.

The flush runs outside the callback while librdkafka holds our rejoin. The time box (default 120 s) keeps it well inside `max.poll.interval.ms` (600 s), which is how long the other members can be made to wait.

**`Revoke { lost: true }` and `RebalanceError`:** drop the buffer and any held batch, `unassign()` (`assign(NULL)`), forget, count. Lost partitions probably already have a new owner. A flush would be correct, because it is fenced, but would only delay our rejoin.

**Cooperative protocol:** rejected at config (§2.5.8); the runtime guard is a backstop.

**`max.poll.interval.ms`:** librdkafka measures it between our `recv()` calls (CONFIGURATION.md:136). The longest gap is one commit or one time-boxed revoke flush. If it is exceeded anyway, the revoke arrives as `lost`, and the fence keeps any in-flight commit correct (§4.7 row 5).

**Static membership:** `group.instance.id` passes through. It is recommended for StatefulSets, so that a restart within `session.timeout.ms` causes no rebalance (CONFIGURATION.md:128); it needs brokers ≥ 2.3. A `FENCED_INSTANCE_ID` is consumer-fatal (§3.6).

### 3.5 Lag and freshness (from the stats callback, no extra broker calls)

- `ddi_kafka_partition_lag_messages{partition} = max(0, ls_offset − next_expected)`. Under read_committed this uses the LSO, so trailing transaction markers leave a documented residual of at most 1 per transaction.
- `ddi_kafka_lag_messages` is the sum; `_max` is the maximum.
- `ddi_kafka_retention_headroom_messages = min_p(next_expected_p − lo_offset_p)`. As it nears 0, retention is about to overtake us.
- Setting `statistics.interval.ms = 0` turns these off (and with them the "caught up" half of the progress rule, §3.9).

### 3.6 Error classes

| Class | Errors | Handling |
|---|---|---|
| **Replan** | `CommitConflict(_)`, `VersionAlreadyExists`, `MaxCommitAttempts`, foreign checkpoint (pipeline.rs:1382-1403, 1521), a commit file missing during the conflict check (cleaned log, EO-m8) or during `read_txns`' walk (`OffsetLogTruncated`), `Fenced` | `reset_in_place`; the batch is retained when still valid |
| **Transient** | Object store errors (5xx, 429, timeouts, IO); SR 5xx/429/network after its own 3 tries; a DQ commit conflict after 3 in-place tries; librdkafka transport errors | Hold the batch, pause all, back off 1 s → 60 s, stay in the group. `ddi_kafka_retrying = 1`. Never escalates by itself: an outage that hits one member usually hits all, and leaving the group would only add rebalances. |
| **Blocking, one partition** | `KafkaOffsetsUnavailable{p, wanted, low}`, `KafkaOffsetsRegressed{p, stored, high}`, `KafkaOffsetMissing{p}`, `KafkaTopicReplaced{p}` | Pause **that** partition; it keeps being re-asserted at its unchanged base, and the others continue. Gauge `ddi_kafka_blocked_partitions`, plus the per-cause gauges. Re-checked every `blocked_recheck_secs`: refresh, `read_txns` for it, watermarks; it resumes as soon as an operator's `set-offsets` makes it resolvable. |
| **Blocking, pipeline** | `KafkaUndecodable` (no DQ table), undecodables above the cap, `KafkaSchemaConflict` (§5.5), `KafkaMetadataCollision` (§6.1), a DQ table that is missing or has the wrong schema, `TargetProtocolUnsupported`, `TargetReplaced`, `KafkaTxnRetention` | Pause all owned partitions, gauge, keep polling (stay in the group), re-check every `blocked_recheck_secs` (re-open the DQ table, re-check the protocol). `TargetReplaced` clears only on restart, where the new table is treated as fresh. |
| **Open-time refusal** | `KafkaAppIdUnknown` (typo guard), `KafkaCompatUnset`, `KafkaPartitionColumnUnmapped`, `KafkaStartingOffsetsIncomplete`, `KafkaTxnRetention` at open | Raised before subscribing or assigning, so no group is joined. `RETRY_MAX` backoff with the gauge. |
| **Consumer-fatal** | librdkafka fatal error (incl. `FENCED_INSTANCE_ID`), `Event::Fatal`, SASL authentication failure, topic/group authorisation failure, unknown topic, internal invariant violations | `Err` → supervisor: close the consumer on a detached thread, back off (`RETRY_MAX` for config-class), reopen |

**`AutoOffsetReset` (-140)** carries no partition (§0.3.20). The handling (EO-m4):
1. Fetch watermarks for the owned Ready partitions. An offender is one with `next_expected < low`.
2. If there is an offender, first close and commit the current batch: the offender's buffered rows are contiguous and valid.
3. Then block only the offender with `KafkaOffsetsUnavailable{p, wanted: next_expected, low}`.
4. No offender → warn and continue.

`AllBrokersDown` and transport errors only set `ddi_kafka_brokers_up = 0` (cleared on the next fetch); librdkafka reconnects. `MaxPollExceeded` is a warning; the lost revoke follows.

### 3.7 Memory

The peak per Kafka pipeline is roughly:
- **librdkafka:** `queued.max.messages.kbytes` (16 MB of prefetch) plus about 16 MB of threads and buffers. `Config::budget` reserves 32 MB per Kafka pipeline before dividing shares.
- **The batch:** decoded bytes are capped at the batch share (§3.3). A held batch is that same batch, not an extra one.
- **Raw payloads:** at most 8192 retained per active schema group (§5.4).
- **The transform and the parquet writer,** under the existing budgeted session.
- **The bronze target snapshot (all add actions),** outside the budget, exactly as Delta pipelines' targets are today. Incremental refresh keeps one copy. `ddi kafka plan` prints the active-file count so pods can be sized (open question 8).

### 3.8 `ddi once`

- **Assignment.** `run_all` sets `ResolvedKafka.once`. Once-mode **assigns every partition manually** (from metadata, starts resolved as in §4.4) and never joins the group, so it cannot rebalance a running daemon away from its partitions. A concurrent daemon on the same app_id stays correct (fenced), but the two would conflict constantly; the docs say not to do it.
- **End.** `end_p` = "high" from `fetch_watermarks` at start, which is the LSO under read_committed (§0.3.15).
- **Done rule.** Partition p is done when `max(next_expected_p, start_p) ≥ end_p`, or when no message for p has arrived for `once_idle_secs` (10 s). The idle rule covers trailing transaction markers or compaction that leave `next_expected` short of `end_p`. Stopping there loses nothing; the next run starts at the same place.
- `Done` is returned once every partition is done and the buffer has been committed.

This replaces kdi's `--ends_at_latest_offsets` (bug #228) and `position()`, which is `OFFSET_INVALID` before the first message and after every seek (EO-M3). The integration and kill tests drive it.

### 3.9 main.rs changes

- **`attempt`** (main.rs:887-1054):
  - **New outcome `StepOutcome::Waiting`,** produced only by Kafka. It never sleeps (the step already waited up to `TICK` in `recv`) and never exits once-mode.
  - **`Waiting` counts as progress (`mark_progress()`) only when:**
    - zero partitions are assigned (idle by design; alerting covers the group, §7.1); **or**
    - every owned partition has lag 0 in stats no older than two intervals; **or**
    - our last commit was within `allowed_latency + 60 s`.
    It **never** counts while retrying, while blocked, or on a step that reset (PO-M5).
  - `CaughtUp` for Kafka means once-mode `Done`. The idle sleep at about line 967 is skipped for Kafka, since sleeping without polling would break `max.poll.interval`.
  - The cursor and head gauges are guarded by `Option`. Kafka gauges come from `pipeline.kafka_progress()`. `last_source_version` is not stored for Kafka.
- **`drive`** (main.rs:766-854): the open-time refusals and config-class consumer-fatal errors join the `RETRY_MAX` list at about lines 829-836 (`RETRY_MAX` = 300 s, main.rs:738). `observe_error` raises their gauges.
- **Supervisor reopen** happens only for consumer-fatal errors. `Drop for KafkaSource` hands the `Arc<StreamConsumer>` to a detached `std::thread` named `ddi-kafka-close`. That needs no runtime (EO-m7), never delays process exit, and logs if the close takes more than 30 s. The backoff then runs outside the group.
- **No `block_in_place` anywhere.** Blocking librdkafka calls (`fetch_watermarks`, `fetch_metadata`, `seek_partitions`, `assign`) run in `spawn_blocking` on the `Arc`'d consumer, so tests may keep `#[tokio::test]`'s current-thread runtime (EO-m7).
- **`status`** (main.rs:584-597): Kafka uses `kafka::tools::status`, a group-less `BaseConsumer` (no `group.id`, so the main queue is not redirected; base_consumer.rs:85-95). It never joins the group.
- **`validate`** (main.rs:290-338): skip `storage.check(&p.source_uri)` for `kafka://`, check the target and the DQ table, and print `name  kafka://topic (group g) -> target (app_id=…)`.
- **New subcommand `ddi kafka plan|clone-table|set-offsets`** (§4.8), compiled only with the feature. Without it, `ddi kafka` prints the feature message.
- **Shutdown.** SIGTERM finishes the current step. A non-empty buffer is then flushed under the revoke time box, and the consumer is closed, which makes the group revoke normally ("a step is atomic", main.rs:664-665).
- **`--version`** appends `librdkafka <rdkafka::util::get_rdkafka_version()>` when built with `kafka`.

---

## 4. Offsets and exactly-once

### 4.1 Invariants and the tests that pin them

| Id | Invariant | Pinned by |
|---|---|---|
| I1 | For each partition p, the rows for offsets (b, e] and `txn(<app_id>-p) = e` land in one Delta commit, or neither does | `kafka_kill_mid_batch`, `kill_between_upload_and_commit` |
| I2 | A commit with data for p from base b succeeds only if txn_p = b at its read snapshot and no later commit touched txn_p | Unit `fence_concurrent_txn_rejected_by_delta_rs` (pins delta-rs 0.32.4), `fence_base_mismatch_detected`; integration `two_groups_same_app_id_never_duplicate`, `rebalance_two_consumers` |
| I3 | A partition stored at b resumes at b+1; b is re-read only to verify it. Inside (b, e], only broker-side gaps (compaction, transaction markers) | `restart_resumes_at_stored_plus_one`, `kdi_takeover_fixture`, `offset_gap_metric_counts_compaction` |
| I4 | A start position never comes from `auto.offset.reset`, nor from the low watermark of a partition ddi has no record for. Out of range gives a typed error and a gauge, and pauses the partition; nothing is skipped without an acknowledged `set-offsets` | `seek_past_retention_blocks_partition`, `missing_txn_low_above_zero_blocks`, `mid_fetch_retention_commits_buffer_then_blocks`, `topic_recreated_fails` |
| I5 | Missing offsets resolve explicitly (§4.4): fresh → `starting_offsets` plus a baseline; otherwise exactly offset 0. The typo guard and the retention refusal hold | `app_id_typo_refused`, `shared_table_second_pipeline_allowed`, `new_partition_starts_at_zero`, `set_transaction_retention_refused`, `latest_baseline_survives_crash` |
| I6 | After a revoke, the partition's buffered rows have been committed or dropped before `unassign()`, and its txn is never asserted again. Messages for partitions that are not Ready never reach the buffer | `revoke_flushes_before_unassign`, `revoke_flush_timeout_drops`, `revoked_partition_not_reasserted`, `lost_revoke_drops` |
| I7 | Only eager assignment exists | `cooperative_rejected_at_load`, unit `cooperative_protocol_in_callback_is_fatal` |
| I8 | Every consumed offset becomes a target row, a DQ row, a counted tombstone, or a row the SQL filtered. Never an uncounted drop | `accounting_identity` |
| I9 | A reject is never lost. Only an undecodable with coordinates is skipped on a replay; any other reject (no coordinates, schema not found, or a transform, conform or coerce reject, whose classification can change) is written again on a replay after a crash or a failed target commit between the DQ and target commits | `dq_crash_between_dq_and_target`, `dq_marks_skip_recorded_offsets` |
| I10 | One malformed message cannot change or lose another message's row | `truncated_message_isolated`, `trailing_bytes_isolated`, `unknown_schema_id_isolated`, `no_magic_byte_isolated` |
| I11 | Each row is decoded with its own writer schema and mapped to target columns by name, recursively | `writer_schema_switch_mid_batch`, `nested_struct_by_name`, `field_added_and_removed` |
| I12 | With `kdi_compat` and `kdi_null_text`, every (Avro type, nullability, target type) value equals what the production kdi wrote | `kdi_parity_matrix`, `kdi_null_text_matrix`, `kdi_takeover_fixture` |
| I13 | Delta→Delta behaviour is unchanged | `delta_commit_golden`, the full suite in both feature configurations |
| I14 | A partition not carried by our last commit is looked up on the commit's read snapshot before it carries data | `none_partition_verified_before_first_commit` |
| I15 | A metadata change by another writer gives a reset and a fresh coercer, never a failed cast; the decoded batch is retried | `alter_table_mid_stream` |
| I16 | With the feature off, Kafka config parses, the pipeline is held back (`config_valid = 0`) and Delta pipelines run | `kafka_config_without_feature_is_held_back` |
| I17 | An ambiguous commit error never produces a duplicate | `ambiguous_commit_error_no_duplicate` |
| I18 | The target's table id at every refresh equals its id at open | `replaced_target_blocks` |
| I19 | A partition resumed on a recreated topic is blocked, even when the new topic has more data than the stored offset | `topic_recreated_with_more_data_detected` |
| I20 | No member's transient or operator-action error causes a rebalance, and a member that keeps failing never stops the others committing | `transient_storage_outage_stays_in_group`, `blocked_partition_others_progress`, `liveness_under_churn` |
| I21 | Two members never contend on a DQ appId | `two_members_dq_rejects_concurrently` |
| I22 | `Waiting` is not progress while fenced, blocked or retrying | `waiting_not_progress_when_fenced` |

### 4.2 Commit construction

There is one `table.write()` per batch, through `Sink::commit_kafka` and the same private `write` as Delta pipelines:
- `SaveMode::Append`, `cast_safety(false)`, `target_file_size`.
- Partition columns come from the snapshot (operations/write/mod.rs:364-389). The `ingest_date` partitioning works once the batch carries the column; §5.6 step 0 guarantees it does.
- Txns: `assert` (§4.3), each a `Transaction::new` with `lastUpdated: None`.
- Metadata: `ddi.appId`, `ddi.kafka.topic`, `ddi.kafka.groupId`, `ddi.kafka.offsets = {"<p>":[first,last]}`, `ddi.kafka.marks = {"<p>":[offset,ts_ms,len]}`, and optionally `ddi.kafka.baseline` or `ddi.kafka.offsetOverride`. The txn is the authority; the rest is evidence.
- An empty batch (only tombstones, or every row rejected or filtered) takes the zero-row path (pipeline.rs:852-879) and carries txns only. On a partitioned target this is pinned by the golden test.
- The operation is `WRITE` (append) where kdi wrote `STREAMING UPDATE`. Downstream classification is action-based (source/log_stream.rs:782-806), so data commits count as `Data` and txn-only commits as `NoData`, as with kdi.

### 4.3 The fence and the txn reader (src/fence.rs)

```rust
pub fn txn_app_id(app_id: &str, p: i32) -> String { format!("{app_id}-{p}") }       // kdi delta_helpers.rs:70-72

pub struct TxnState { pub version: Option<i64>, pub mark: Option<MessageMark>, pub writer: Writer }
pub enum Writer { Ddi, Other, Unknown }   // Unknown: read from a checkpoint, which keeps no commitInfo

/// Every wanted appId's newest txn as of `table`'s version, in ONE pass (EO-M6):
///  1. Walk `_delta_log/<v>.json` backwards from the handle's version (log_store.read_commit_entry + get_actions,
///     the pattern dbt/watermark.rs:207-245 already uses), 4 GETs in flight, until every wanted appId is
///     found or the newest checkpoint version (from `_last_checkpoint`) is reached. The mark comes from
///     `ddi.kafka.marks` in the same commit; `writer` from `ddi.appId` in its commitInfo.
///  2. For the rest, read the checkpoint's `txn` column once (parquet projection; single- or multi-part).
///     Those come with no mark and `Writer::Unknown`.
///  3. Only if that checkpoint cannot be read (a foreign format): per-id kernel lookups, counted in
///     ddi_kafka_offset_lookup_fallbacks_total.
/// Refuses when delta.setTransactionRetentionDuration is set, so no expiry filter is needed.
pub async fn read_txns(table: &DeltaTable, app_ids: &[String]) -> Result<BTreeMap<String, TxnState>>;

pub struct Fence {
    app_id: String,
    base: BTreeMap<i32, (Option<i64>, Option<MessageMark>)>,   // owned partitions: last offset durably in the target
    carried: BTreeSet<i32>,                                    // verified at the handle's version and re-asserted since
}
impl Fence {
    pub async fn reset(&mut self, table: &DeltaTable, owned: &[i32]) -> Result<()>;        // one read_txns; carried = owned
    /// Before a commit: batch partitions not in `carried` are looked up on `table` (this commit's read snapshot,
    /// since write() plans from the handle) and must equal base[p] (None == None).
    pub async fn verify(&mut self, table: &DeltaTable, ranges: &BTreeMap<i32, (i64, i64)>) -> Result<Result<(), Fenced>>;
    /// last_p for batch partitions; base_p for every other owned partition with Some(base). Marks likewise.
    pub fn assert_map(&self, ranges: &BTreeMap<i32, (i64, i64)>, marks: &BTreeMap<i32, MessageMark>) -> (BTreeMap<i32, i64>, BTreeMap<i32, MessageMark>);
    pub fn committed(&mut self, offsets: &KafkaOffsets);      // base[p] = assert[p]; carried = keys of assert
    pub fn forget(&mut self, p: i32);                         // revoke (I6)
}
pub fn refuse_txn_retention(table: &DeltaTable) -> Result<()>;
pub fn refuse_unwritable_protocol(table: &DeltaTable) -> Result<()>;   // writer features delta-rs 0.32.4 cannot write, column mapping
```

**Handle discipline.** In a Kafka pipeline, `self.target` changes only through:
1. the table returned by our own successful commit, which includes the winners we were rebased over (kernel/transaction/mod.rs:902-903, 996); or
2. a refresh (assign, reset, aging, blocked recheck) or a reopen (supervisor). **Every refresh is followed by `fence.reset`**, which empties `carried` and reads every owned base at the new version.

The handle is never refreshed in between.

**Why this is exactly-once,** given that every writer of these appIds is a fenced ddi or a stopped kdi:
1. Commit C reads at snapshot R and lands at V_C.
2. For each p in C's assert map, txn_p(R) = base_p. Either our previous commit wrote it, in which case the winners we rebased over cannot have touched it (they would have conflicted), or `verify`/`reset` read it at R.
3. txn_p is among C's actions, so delta-rs checked every version in (R, V_C) for a winning txn with appId p and found none (conflict_checker.rs:134-138, 590-606; per version at mod.rs:868-899). So txn_p was still base_p immediately before V_C.
4. Any other writer of offsets above base_p would have moved txn_p inside that window, which is a contradiction. Any writer after V_C sees our value and fails its own fence.
5. Only explicit operator actions move txn_p backwards (`set-offsets --allow-replay`). A running owner then conflicts and resets.

Blind appends on disjoint appIds rebase cleanly (conflict_checker.rs:461-463), so N consumers in one group scale out.

**Cost.**
- **Steady state:** zero lookups. Every owned partition with a known base is carried; only a partition with no txn needs one walk before its first data.
- **Assign, reset, aging:** one walk. That is one GET when our newest commit is the head, since it carries every owned partition; otherwise at most `head − checkpoint` GETs plus one checkpoint read, bounded by `checkpointInterval` (10 after cutover). Never one lookup per partition. The checkpoint is found and read through the table's unfiltered store, also on a handle `Storage::open` built by replaying the log past a Trino checkpoint, whose own store hides every checkpoint from the kernel; otherwise that handle's walk would read every commit down to version 0.
- **Metrics:** `ddi_kafka_offset_lookups_total`, `ddi_kafka_offset_lookup_seconds_total` and `ddi_kafka_offset_lookup_fallbacks_total`.

**Aging (EO-m8).** A quiet member's handle can trail the head by thousands of versions. The conflict loop would then read them one by one, and fail with a missing file if log cleanup has removed any. `commit_batch` therefore refreshes and resets when the handle is older than `handle_max_age` (10 min). A missing commit file during the conflict check is classified Replan, not supervisor-fatal.

### 4.4 Start resolution (src/kafka/start.rs)

**At open,** before subscribing (run mode) or assigning (once-mode):
1. Read the topic's partitions from metadata (`fetch_metadata`, `spawn_blocking`).
2. `read_txns` over every partition → `known`, `fresh` (fresh = no partition has a txn), `kdi_era` (some txn was written by another writer, `Writer::Other`, and none by ddi).
   - A txn read from a checkpoint has `Writer::Unknown`, and counts for neither side. A table only ddi has written offsets to reads that way whenever a checkpoint lands above ddi's last commit (another pipeline sharing the target, or Trino's OPTIMIZE at a checkpoint-interval version); a kdi table whose commits are all below a checkpoint reads that way too.
3. `refuse_txn_retention`, `refuse_unwritable_protocol`; record the table id.
4. **Partition columns** (§5.6 step 0): refuse with `KafkaPartitionColumnUnmapped` when the target's partition columns cannot be produced.
5. **`kdi_compat` unset on a kdi-written table** (`kdi_era`, or the newest commit carrying our txns lacks `ddi.appId`) → `KafkaCompatUnset`: "this table was written by kafka-delta-ingest; set `kdi_compat = true` to keep its STRING rendering ("null" text, raw-number timestamps, sorted-key JSON), or `false` to accept the documented break". Unset elsewhere means false.
   - When every txn's writer is `Unknown`, step 2 cannot tell. M3 reads the commits below the checkpoint while log retention still keeps them, and takes the writer of the newest one carrying our txns; the same read gives back the marks a checkpoint drops, without which the re-read check (I19) is off for those partitions until they carry data again. If they are gone, it refuses with `KafkaCompatUnset` worded as "cannot tell who wrote this table's offsets", never as "written by kafka-delta-ingest".
6. **Typo guard.** Refuse with `KafkaAppIdUnknown` when all of these hold:
   - `fresh`;
   - the target has data files (snapshot file count; accessor **verify (M3)**);
   - `starting_offsets` is not explicit;
   - the target's txn appIds (in the last 100 commits, and in the checkpoint's `txn` column) are **not** all of the form `<a>-<digits>` for another configured Kafka pipeline's app_id on this target (EO-m15).

   The message lists the appIds seen ("did you mean `kafka_delta_ingest`?"). A target with data but no txns at all is refused too.
7. **Fresh start.** Resolve every partition's start:
   - an explicit map, which must cover all partitions unless it has `"*"` (otherwise `KafkaStartingOffsetsIncomplete`);
   - `earliest` → low_p;
   - `latest` → high_p, which is the LSO under read_committed.

   If any start_p > 0, commit a **baseline**: txn-only, `assert[p] = start_p − 1` for every start_p > 0, with `ddi.kafka.baseline = true`.

   A partition starting at 0 gets no txn: its txn would have to be −1, and kdi would read `Offset(-1)` as `End` on a rollback. The "None, not fresh, low == 0" row below then gives the same start.
8. Subscribe (run) or assign (once).

**Per partition,** at assign, reset or blocked recheck, with `(low, high)` from `fetch_watermarks`:

| Stored txn | Condition | Start |
|---|---|---|
| `Some(b)` | `low ≤ b < high` | Assign at **b** and re-read it. If our commits recorded a mark for b, timestamp and length must match, otherwise **Blocked `KafkaTopicReplaced{p}`** (I19). b itself is dropped; consumption continues at b+1. kdi did the same seek-and-drop (lib.rs:1065-1115). |
| `Some(b)` | `b + 1 == low` (b itself already trimmed) | Assign at b+1, no verification |
| `Some(b)` | `b + 1 < low` | **Blocked `KafkaOffsetsUnavailable{p, wanted: b+1, low}`**: a retention gap (I4). Recover with `set-offsets --next <low> --acknowledge-skip "<reason>"`. |
| `Some(b)` | `b ≥ high` | **Blocked `KafkaOffsetsRegressed{p, stored: b, high}`**: topic recreated, or the wrong cluster |
| `None` | explicit `starting_offsets` entry s for p (an acknowledgment, also on a non-fresh table) | s, provided `low ≤ s ≤ high` (else Unavailable or Regressed). Asserted as s−1 from the next commit when s > 0. Entries for partitions that have a txn are ignored, with a warning. |
| `None` | not fresh, `kdi_era`, `high > 0`, and the table's visible history ever set `delta.setTransactionRetentionDuration` (scanned in the walk and in the checkpoint's `metaData`) | **Blocked `KafkaOffsetMissing{p}`**: a kdi txn may have expired, and re-reading from 0 would duplicate the partition (EO-M4b). Acknowledge with an explicit entry after checking the data (`plan` prints the evidence). |
| `None` | not fresh, `low == 0` | **Offset 0 exactly.** In a ddi-written table this is a new partition, or one ddi never saw a message from. In a kdi table it is one kdi never wrote a row for (§0.3.13). Either way nothing from it is in the table. |
| `None` | not fresh, `low > 0` | **Blocked `KafkaOffsetsUnavailable{p, wanted: 0, low}`**: messages ddi never read were trimmed (EO-M4a). Recover with `set-offsets --next <low> --acknowledge-skip`. |

Why no `ddi.kafka.partitionCount` bookkeeping (EO-M4's suggestion): with "exactly 0, block if low > 0", a new partition and an empty one are treated identically and safely. The one ambiguous case (a lost kdi-era txn) is handled by the history rule above. ddi's own txns cannot be lost (§0.3.9).

Why no topic id (EO-M4): rdkafka 0.39 exposes no topic id, and DescribeTopics would mean the first `unsafe` FFI in ddi. The re-read mark check catches a recreated topic at the same cost as kdi's existing seek-and-drop.

### 4.5 Taking over kdi tables (no gap, no duplicate)

- **Same convention.** kdi writes `txn{appId: "<app_id>-<p>", version: last offset written, lastUpdated: now}` (delta_helpers.rs:29-40, 70-72) and resumes by seeking `Offset(o)` and dropping o (`Beginning` for o = 0; lib.rs:1065-1115). ddi assigns at o, verifies it, drops it and continues at o+1. There is no off-by-one.
- **Partitions kdi never wrote.** kdi re-asserts every assigned partition with a known offset (§0.3.13), so a kdi partition with no txn never had a row under that app_id. ddi reads it from 0 and re-reads only what kdi dropped: tombstones, undecodables and transform failures, the last two going to DQ. **Create the DQ tables before cutover.**
- **Holes kdi left.** kdi silently dropped deserialization failures, including those caused by transient Schema Registry outages (lib.rs:866-879). Its bronze tables can therefore have holes below the stored offsets. ddi never re-reads below a stored offset, so it does not fill them. The shadow comparison (§10.3) lists them when offset columns exist; filling them is out of scope (open question 12).
- **Requirements:** the same `app_id`, kdi stopped first, the same topic and cluster, and `kdi_compat` set explicitly. `plan` checks that every partition's stored+1 lies in [low, high].
- **Protocol.** ddi already opens these bronze tables as a source, including the Trino-checkpoint fallback (storage.rs:95-153). `plan` still prints the protocol and writer features, and refuses any writer feature delta-rs 0.32.4 cannot write, column mapping included (write/mod.rs:409-411).
- **Value parity** is §5.6; **overlap safety** is §4.7 row 8.

### 4.6 Txn retention

1. `open_kafka` and every refresh **refuse** a target whose configuration contains `delta.setTransactionRetentionDuration` (table/config.rs:112, 148). kdi's txns carry `lastUpdated`, so the kernel's filter (buoyant_kernel snapshot/mod.rs:896-938; set_transaction.rs:10-21) could expire a quiet partition before ddi's first re-assert.
2. ddi writes `lastUpdated: None`, which never expires. A property set later cannot strand ddi's own txns; the next refresh refuses loudly.
3. A txn already lost to a past setting (a checkpoint written while the property was set drops expired txns) is the kdi-era `KafkaOffsetMissing` row in §4.4. It is not a silent re-read.

### 4.7 Failure matrix

| # | Failure point | Outcome | Recovery |
|---|---|---|---|
| 1 | Crash after the parquet upload, before the commit JSON | Orphan files; txns unchanged | Restart reads from stored+1; VACUUM removes the orphans |
| 2 | Commit landed, then the process died, or the post-commit hook returned `Err` | txn advanced | Restart or reset re-derives from the log (I17) |
| 3 | Commit landed, client saw an error, retried PUT gets 412 | `VersionAlreadyExists` → conflict check finds our own commit → `ConcurrentTransaction` | Replan; re-reads our own txns; no duplicate |
| 4 | Lost the race to blind appends, OPTIMIZE or VACUUM | delta-rs retries up to 15 times with conflict checks (mod.rs:120, 868-947) | Success, or `MaxCommitAttempts` → Replan, batch retained |
| 5 | Zombie: kicked out after `max.poll.interval`, still commits | Its commit re-asserts every partition it thought it owned → `ConcurrentTransaction` | Zombie resets; the new owner is correct (I2) |
| 6 | Two deployments with the same app_id, different groups | Every interleaved commit conflicts; alternating progress, no duplicates | Alert on `fenced_total` rate; fix the deployment |
| 7 | A rebalance while a batch is buffered | Revoke flush commits it (time-boxed), else drops it | Automatic; the next owner reads the txn |
| 8 | kdi still running with the same app_id | ddi side is fenced. kdi side: `are_partition_offsets_match` (lib.rs:1164-1186) plus delta-rs 0.26's checker **(verify in the nightly `kdi_and_ddi_race`)**. kdi re-asserts all its assigned partitions, so during a handover both sides conflict on every commit for the moving partitions | The runbook stops kdi first |
| 9 | DQ write OK, target commit fails | DQ rows and marks present | Replay skips coordinate-carrying undecodables at offsets ≤ the mark (I9); the rest are written again (a visible duplicate, never a loss) |
| 10 | DQ write fails | Nothing advanced | DQ conflict: our own landed commit is recognised by `ddi.kafka.dqWriteId` (row 3 on the DQ table); otherwise up to 3 in-place retries, then Transient. A failure that is not a conflict but may have landed is retried with the batch, and can duplicate the rejects that are never skipped (I9) |
| 11 | Retention passed stored+1 (at assign, or mid-fetch as -140) | Mid-fetch: buffer committed first. Partition blocked (`KafkaOffsetsUnavailable`), others continue | Operator: `set-offsets --next <low> --acknowledge-skip "<reason>"` |
| 12 | stored ≥ high: topic recreated or wrong cluster | Partition blocked (`KafkaOffsetsRegressed`) | Operator: new app_id, or `set-offsets --allow-replay` |
| 13 | Topic recreated and refilled past stored | Re-read mark mismatch → `KafkaTopicReplaced` (I19) | Operator: new app_id, or `set-offsets` |
| 14 | No txn for a partition; partitions added | Offset 0 exactly; `low > 0` blocks | Acknowledged skip if needed |
| 15 | Target has `setTransactionRetentionDuration` | Refused at open and on refresh (pipeline blocked) | Operator unsets it |
| 16 | `app_id` typo | `KafkaAppIdUnknown`, with a suggestion, at open | Fix the config |
| 17 | SR down when a new schema id arrives | Transient: batch held, partitions paused, member stays in group; known ids are cached | Automatic |
| 18 | Azure throttling or outage during a commit | Transient, as row 17 (I20) | Automatic |
| 19 | Poison message | DQ (within the cap); with no DQ table the pipeline is blocked | Create the DQ table; the recheck picks it up |
| 20 | `ALTER TABLE` on bronze concurrently | `MetadataChanged` → reset, coercer rebuilt, batch retained (I15) | Automatic |
| 21 | Foreign (Trino) checkpoint met during conflict resolution | Replan; reset steps over it (pipeline.rs:1521) | Automatic |
| 22 | Trino upgrades the protocol (e.g. deletion vectors) | `TargetProtocolUnsupported`, pipeline blocked, gauge | Operator |
| 23 | Bronze dropped and recreated at the same path while running | Table id mismatch → `TargetReplaced`, pipeline blocked | Operator restarts ddi; the new table is fresh |
| 24 | RESTORE of bronze to an older version | Txns stay ahead of the restored data: silent loss unless acted on | Runbook: `set-offsets --allow-replay` to the restored version's `ddi.kafka.offsets` |
| 25 | DELETE on bronze (e.g. GDPR) | Intentional; txns unchanged | None |
| 26 | Handle aged past log cleanup | Refresh before commit; a missing file in the conflict check → Replan | Automatic |
| 27 | Cooperative protocol configured | Config reject, or runtime `Fatal` | Fix the config |
| 28 | Different app_ids, same topic, same table | Not fenceable → duplicates | Rejected within one config (§2.5.5); documented across processes |
| 29 | Producer retries without idempotence (same payload at two offsets) | Exactly-once is per offset, as in kdi | Deduplicate downstream by key |
| 30 | Aborted transactional messages | `isolation.level = read_committed` (the default) | `read_uncommitted` only with a warning |

### 4.8 Operator tools

All of these use a group-less client and never disturb the group. User-facing offsets are always the **next offset to read** (PO-minor 8); only the txn stores "last written".

**`ddi kafka plan -p <name>`** (M4) is the read-only preflight. It prints:
- **Table:** protocol and writer features, partition columns, and the properties `checkpointInterval`, `logRetentionDuration`, `deletedFileRetentionDuration`, `setTransactionRetentionDuration` (current, and **ever set** in visible history), `enableExpiredLogCleanup`, `appendOnly`, and column mapping. Also the last commit's engine and operation (for example `delta-rs.0.26 STREAMING UPDATE`), the table id, and the active-file count.
- **Per partition:** txn, writer (kdi or ddi), low/high watermark, resume position, lag, and status: OK, RETENTION_GAP, REGRESSED, NO_TXN→offset 0, or NO_TXN→ack needed. For NO_TXN partitions on kdi tables with a `kafka_partition` column, it also says whether file statistics show any rows for p.
- **Other appIds** seen in recent commits.
- **A ready-to-paste `starting_offsets = { "0" = …, … }`** (stored+1), used by the shadow run.
- **Schema mapping** for the latest writer schema per partition, found by scanning backwards from high−1 to the newest non-tombstone message (compacted topics, PO-minor 9). For each target column it shows the source (Avro field, `kafka_*`, transform, or nothing → null), the conversion, and kdi-specific notes such as "timestamp-millis into TIMESTAMP: kdi wrote it 1000× too small".
- **The `kdi_compat` recommendation:** the STRING columns whose rendering differs, and a ready-to-run count query of `= 'null'` rows per STRING column.
- **Metadata-prefix collisions.**

It exits non-zero on blocking findings.

**`ddi kafka clone-table -p <name> --to <uri>`** (M4) creates an empty shadow table by writing a version-0 commit with bronze's own protocol and `metaData`: schema with exact column-name case, partition columns, configuration minus the table id. Trino CTAS lowercases names and may enable column mapping (PO-minor 7).

**`ddi kafka set-offsets -p <name> --partition P --next O`** (M5) writes a txn-only commit with `txn = O − 1`; `O = 0` removes the start requirement by writing nothing and is refused when a txn exists.
- **Guards:**
  - moving forward past offsets that are still retained requires `--acknowledge-skip "<reason>"`, recorded as `ddi.kafka.offsetOverride`;
  - moving backwards requires `--allow-replay`;
  - `--all-partitions` applies one rule to each.
- **DQ marks** for the same partitions are set to `O − 1` in the DQ table, so a replay records its rejects again (duplicates, never a loss).
- **A running owner** conflicts on its next commit, or notices at its blocked recheck, then resets and reads the new value.

**`ddi status`** prints the per-partition stored offset, writer, watermarks and block reason for Kafka pipelines.

---

## 5. Decode

### 5.1 Framing

`payload[0] == 0x00`, then a big-endian u32 schema id in bytes 1-4, then the Avro binary body. This matches arrow-avro's `CONFLUENT_MAGIC` (schema.rs:40) and `Fingerprint::Id` (reader/mod.rs:757-760). ddi parses the header itself to route each message to its decoder.
- A `None` payload is a tombstone: skipped, counted, offset accounted.
- Fewer than 5 bytes, or a wrong magic byte, is undecodable, with a reason such as "not Confluent-framed (first byte 0x7b; is this topic JSON?)".

### 5.2 SR client (src/kafka/registry.rs, reqwest 0.12 rustls, Cargo.toml:57-59)

**`GET {url}/schemas/ids/{id}?deleted=true`**, with `Accept: application/vnd.schemaregistry.v1+json`. The trailing `/` of the URL is trimmed (kdi serialization.rs:91-94).

| Response | Handling |
|---|---|
| 2xx, `schemaType` absent or `AVRO` | Cache `(url, id) → Arc<Registered>` for the life of the process. Ids are immutable; the cache is shared by pipelines on the same URL. |
| `schemaType` present and not `AVRO` | Undecodable (deterministic) |
| Non-empty `references` | Blocking (pipeline): "schema references are not supported yet; referenced subjects: …" (open question 10) |
| 404 / `error_code` 40403 | `SchemaNotFound(id)` → undecodable (**not** deterministic, §5.7) |
| 401 / 403 | Consumer-fatal, config-class: never quarantine |
| 5xx, 429, network | 3 tries (100 ms, 500 ms, 2 s), then Transient |

**Latest subject version** (SQL path only): `GET {url}/subjects/{subject}/versions/latest`, at open and whenever an id not yet in `U` appears. A 404 means another naming strategy; `U` is then built from seen ids only, with a warning.

**Auth and network:**
- Basic auth from `username_env`/`password_env`; the password is optional, as in kdi serialization.rs:96-101.
- `reqwest::Proxy::all(env[proxy_env])`.
- 30 s timeout.

**TLS:**
- reqwest's `rustls-tls` uses webpki roots, where kdi used native-tls (the OS store).
- `ca_pem_env` adds a private CA (`Certificate::from_pem` + `add_root_certificate`).
- We do not turn on `rustls-tls-native-roots`: feature unification would change the Trino and PubSub clients' trust, i.e. Delta pipeline behaviour.

**Not `schema_registry_converter`:** it defaults to native-tls, pulls reqwest 0.13 and apache-avro, and brings a JSON-schema validator.

### 5.3 arrow-avro versus apache-avro

| | arrow-avro 58.4.0 | apache-avro (+ serde_json + arrow-json) |
|---|---|---|
| Output | `RecordBatch` in the same arrow 58.4.0 as `deltalake::arrow`: one arrow, no conversion | `Value` per row, then JSON, then arrow-json: two conversions and heavy allocation |
| Confluent framing | Built in (`FingerprintAlgorithm::Id`, reader/mod.rs:642-881) | By hand |
| Logical types | Typed: Timestamp, Date32, Decimal128/256, Time32/64 (codec.rs:874-947) | Raw numbers in JSON (types.rs:342-349) |
| Error isolation | None: a failed or partial row stays in the builders (hence §5.4) | Per message, stateless |
| kdi parity | Needs the conform layer (§5.6) | Exact by construction |

**Decision:** arrow-avro in production, behind a small `trait Decode` so apache-avro could be swapped in if M2's matrix finds a blocking bug. apache-avro, pinned to the **production** kdi version, is a dev-dependency for the parity tests only. Pin arrow-avro `=58.4.0` and bump it with deltalake; a mismatch is a compile error.

### 5.4 Decoder lifecycle (src/kafka/decode.rs)

```
per schema id G:
  decoder = ReaderBuilder::new()
              .with_writer_schema_store(store with exactly Id(G))     // SchemaStore::new_with_type(Id) (schema.rs:861), set (884)
              .with_active_fingerprint(Fingerprint::Id(G))
              .with_batch_size(8192).with_tz(Tz::Utc).with_utf8_view(false)
              .build_decoder()
  meta    = parallel builders: partition, offset, timestamp, timestamp_type, key, headers, schema_id
  chunk   = raw (payload, meta) of the rows currently inside `decoder` (≤ 8192)
  batches = flushed RecordBatches with metadata columns appended

feed(msg):                                         // msg is an owned copy (§3.3)
  G = frame(msg)? (undecodable on a framing error)
  if G is unknown: registry.get(G).await; top-level not a record → undecodable ("Expected record", reader/record.rs:142);
                   a top-level field named <metadata_prefix>* → Blocking KafkaMetadataCollision (§6.1); build G's decoder
  cap = dec.capacity(); n = dec.decode(payload)
  require: Ok(n) && n == payload.len() && dec.capacity() == cap - 1     // strict per-message check
     ok   → meta.append(msg); chunk.push(msg); if dec.batch_is_full() { flush(G) }
     else → isolate(G, msg)
isolate(G, bad):
  discard G's decoder and meta builders (they may hold a partial row or the swallowed-prefix state, reader/mod.rs:706-735)
  re-decode each chunk message alone with a fresh decoder + flush (they passed before: deterministic) → good 1-row batches
  `bad` → Undecodable{p, o, reason, deterministic}; other groups untouched (I10)
flush(G): rb = dec.flush()?; normalize(rb); append metadata columns from meta; batches[G].push(rb); chunk.clear();
          decoded_bytes += slice size (§3.3)
```

**Why each message gets its own decoder group.** A message is routed to its id's decoder, so no decoder ever switches schema; the switch path at reader/mod.rs:812-824 never triggers. Interleaved writer versions therefore cost nothing.

**What the strict check catches:**
- a truncated body (the decoder consumes only the prefix and waits for more bytes);
- trailing bytes (an extra row, or a "Missing magic bytes" error after a decoded row);
- malformed bodies.

**Normalisation after flush,** recursive through structs, lists and maps:
- enum `Dictionary(Int32, Utf8)` → `Utf8`;
- uuid `FixedSizeBinary(16)` with `logicalType = uuid` (codec.rs:1676-1682) → lowercase hyphenated `Utf8`, as apache-avro produces (types.rs:353).

**Hostile inputs** (huge length prefixes, fuzzed lengths) must produce an error, not a large allocation **(verify M2 with a fuzz-style test)**.

### 5.5 Schema evolution: making the input usable

**Identity path** (no `transform_sql`). Batches of different writer schemas go through `Identity` unchanged (transform/mod.rs:109-117). The conform hook (§5.6) conforms each batch to the target by name, so mixed versions within one batch are fine.

**SQL path.** Every group's batches are aligned to `U` before the transform:
- **What `U` is:** the metadata columns, plus the name-union of the subject's **latest registered version** and **every writer schema id this process has seen**, including ids registered under other subjects (EO-m14). The latest version is included so that SQL referencing a just-added field plans before the first such message arrives. Versions that were never seen are ignored, so old deleted or incompatible versions cannot block a start (PO-M7).
- **Field type:** the type from the **newest** schema that has the field, i.e. the highest schema id (Confluent ids increase monotonically). Field order follows the newest schema; fields only in older schemas follow, newest first.
- **Older groups' values:**
  - a missing column becomes null, matching kdi's JSONPath on a missing key;
  - nested records merge recursively;
  - a different type is cast to `U`'s type with `safe = true`, and rows where a non-null value became null are rejected to DQ ("writer schema <id> field <f>: <t1> does not convert to <t2> (newest writer schema)");
  - a newer general union containing the old type wraps the old column as that branch (union widening);
  - Avro's promotions (int→long→float→double, string↔bytes) are ordinary casts.
- **Growth:** an id not yet in `U` refreshes the latest version, merges, and re-aligns the batches already buffered. `U` only grows in fields.
- **When a field's type changes mid-run** (a new id with a new type), the type follows the newest schema. If the SQL no longer plans or evaluates because of that, the pipeline goes Blocking `KafkaSchemaConflict` with `ddi_kafka_schema_conflict = 1`, naming the field and both ids; the operator adjusts the SQL and restarts.

**Rejected alternatives:**
- per-group SQL, which stalls on new fields;
- Avro reader-schema resolution, which fails under FORWARD-only evolution and substitutes Avro defaults where kdi produced NULL;
- failing at open on any historic conflict (PO-M7).

### 5.6 Into the target: `conform`, then `SchemaCoercer`

**Step 0, at open** (EO-m12). If the target is partitioned, its partition columns must be produced, or open is refused with `KafkaPartitionColumnUnmapped`:
- **SQL path:** the SQL is planned against `U` and the output names checked, case-sensitively.
- **Identity path:** the names must be in `U` or the metadata columns.
- **If `U` is unknown at open** (subject 404 and nothing seen yet), the same check runs on the first batch as a Blocking error.

Null partition values are allowed (kdi parity) but counted in `ddi_kafka_null_partition_values_total`, because they land in `__HIVE_DEFAULT_PARTITION__`.

**Per batch, the hook `Pipeline::conform(output, &mut rejects)` runs after the transform and before the coercer** (where kdi coerced too, after JMESPath):

1. **Top level, by exact name** (case-sensitive, like kdi's arrow-json and ddi's `index_of`, schema.rs:74).
   - A target column that no source produces: if NOT NULL, it is left for the coercer's error. If nullable, it becomes null, with a warning once per schema id and the gauge `ddi_kafka_target_columns_unmatched{pipeline} = n`. A camelCase Avro field against a lowercase column is the typical case.
2. **Struct → Struct by name, recursively.**
   - A missing child becomes null; extra children are dropped.
   - List and Map values recurse, and containers are rebuilt with the target's field names (`element`, `key_value`, `key`, `value`). The cast kernel's positional fallback is never reached (§0.3.1).
   - A missing NOT NULL nested child is filled as nullable and fails in the coercer, which quarantines the row **(verify M2)**.
3. **General union → target type.** Each branch's child is cast to the target type, and the active branch is interleaved per row. A branch that cannot be cast is a row reject.
4. **Float → Decimal** goes through the shortest round-trip text and then `parse_decimal`, as kdi's arrow-json read serde_json's text (PO-minor 2). A plain float multiply can be off by one in the last digit at high scale.
5. **`kdi_compat`: a non-string value into a `Utf8` target, at any depth** (a top-level column, a struct child, a list element), is rendered by its **Arrow type**, whatever its name or origin (PO-M2). The table below gives the rules.
   - Struct, Map and List values are rendered as one compact JSON text with **sorted keys** (serde_json without `preserve_order` in either lock). Leaves inside follow the same rules as JSON values, and nulls inside appear as the JSON token `null`.
   - **Excepted** are `<prefix>key` and `<prefix>headers`, which have no kdi equivalent. They use the plain arrow cast (key bytes as UTF-8; invalid UTF-8 → row reject).
   - A SQL expression that wants other text must produce `Utf8` itself (`to_char`, `CAST(.. AS VARCHAR)`). Utf8 values pass unchanged.
6. **`kdi_null_text`** (default = `kdi_compat`): a **null** going into a `Utf8` target becomes the text `"null"` exactly where kdi's JSON had the key (PO-B1):
   - **Top level:** the column name is a field of **that row's writer schema**.
   - **Nested:** the parent struct value is non-null and the child is a field of that record in the row's writer schema. Arrays of structs are handled element by element.
   - **List elements:** a null element of a present list.
   - **Never** inside a Map (kdi had no coercion node there, coercions.rs:55-65).
   - **Never** for metadata columns or for computed columns that are not writer fields. kdi's `set_value` skipped null results (transforms.rs:343-347), so the key was absent.

   The row's writer schema comes from `<prefix>value_schema_id`. That column is always present on the identity path, and present on the SQL path when the SQL keeps it (`SELECT *, …`). If the SQL drops it, presence falls back to "the name is a field of `U`", with a one-time warning and the gauge `ddi_kafka_null_text_approximate = 1`. The only rows that then differ from kdi are those whose writer version lacked a field `U` has.
7. **Everything else** goes to `SchemaCoercer::coerce` / `coerce_quarantining` (schema.rs:70-137): an arrow cast with `safe = false`, or quarantine.

Conform rejects are added to `rejects` in the same DQ write, so there is one DQ commit per batch.

**Avro → arrow-avro → kdi → ddi:**

| Avro | arrow-avro (codec.rs:874-947) | kdi into STRING | ddi into STRING (`kdi_compat` + `kdi_null_text`) | ddi into a typed column |
|---|---|---|---|---|
| null value of a present field (`["null",T]`) | null | **`"null"`** | **`"null"`** (writer-schema presence rule) | NULL |
| boolean | Boolean | `"true"` | `"true"` | BOOLEAN |
| int, long | Int32, Int64 | `"5"` | `"5"` | cast |
| float | Float32 | serde_json text of the widened f64 (`0.10000000149011612`) | same; NaN/Inf → row reject (kdi: `ConvertF64ToJson`, dropped) | cast |
| double | Float64 | serde_json text | same | cast; into DECIMAL via shortest text |
| string | Utf8 | as is | as is | cast; string→TIMESTAMP uses arrow's parser where kdi used chrono (coercions.rs:126-145), edge formats **verify M4** |
| bytes / fixed(n) | Binary / FixedSizeBinary(n) | `"[1,2,3]"` | `"[1,2,3]"` | BINARY |
| enum | Dictionary → Utf8 | symbol | symbol | STRING |
| uuid | FixedSizeBinary(16) → Utf8 | hyphenated | hyphenated | STRING |
| date | Date32 | `"19000"` | `"19000"` | DATE; INT days |
| time-millis / -micros | Time32(ms) / Time64(µs) | number | number | INT/BIGINT |
| timestamp-millis | Timestamp(ms, UTC) | **ms number**; into TIMESTAMP arrow-json read it as µs, **1000× too small** unless a transform fixed it | ms number (also when aliased) | TIMESTAMP: **the correct instant** (differs from untransformed kdi; `plan` flags it; open question 9). BIGINT: ms |
| timestamp-micros | Timestamp(µs, UTC) | µs number | µs number | TIMESTAMP, same as kdi |
| local-timestamp-* | Timestamp(unit, None) | number | number | TIMESTAMP, naive read as UTC **(verify M2)** |
| decimal (bytes / fixed(n)) | Decimal128/256(p,s) | the wire bytes at their original length (decimal.rs:85-151) | for `bytes`, minimal two's complement (what Java/Confluent serializers write); for `fixed(n)`, sign-extended to n. **verify (M-1/M2)** against the production corpus; a producer writing padded `bytes` would differ | DECIMAL: **correct**. kdi's arrow-json failed here and crash-looped (lib.rs:932-937) |
| duration (fixed 12) | Interval(MonthDayNano) | 12-byte array | 12-byte array | no Delta type → quarantine |
| general union | dense Union (codec.rs:947) | active branch | active branch, rendered | active branch, cast; reject if impossible |
| record | Struct | object, sorted keys | compact JSON, sorted keys | STRUCT by name; null STRING children `"null"` (presence) |
| map | Map<Utf8, V> | object, sorted keys | JSON | MAP; no `"null"` text inside |
| array | List | array | JSON | ARRAY; null elements of `array<string>` → `"null"` |
| (metadata) `kafka.timestamp` | — | µs number | `<prefix>timestamp` is Timestamp(µs) → µs number | TIMESTAMP same instant; BIGINT µs |

### 5.7 Undecodable messages, rejects and the DQ table

**Undecodable** means: no magic byte, schema not found (404), not AVRO, malformed body, trailing bytes, or a top-level schema that is not a record. kdi could not ingest a non-record value either (`ValueNotAnObject`).

**With a DQ table** (`<target>__ddi_dq`, dq.rs:69), each undecodable becomes a `Rejected` (schema.rs:290-300):
- `rows` = a RecordBatch of `<prefix>topic, <prefix>partition, <prefix>offset, <prefix>timestamp, <prefix>key_b64, <prefix>value_b64, <prefix>value_schema_id` (base64 through the existing `base64` dependency);
- `columns` = `None`, and `reasons` = `avro: <error>`. No other reject's reason may start with `avro:` (`dq::KAFKA_UNDECODABLE`): the filter in step 2 below tells undecodables by it, so a union-alignment or conform reject must be worded otherwise.

They travel as `Batch.rejects` and are written **before** the target commit (pipeline.rs:793-825) by `DataQuality::write_kafka` (EO-M1, EO-m9, PO-M6):
1. **Read the marks.** `fence::read_own_txns` on the DQ handle gives `m_p` = txn `<app_id>.dq-<p>` for each batch partition. DQ tables are small, so this is one or two GETs. It is `read_txns` without the refusal of `delta.setTransactionRetentionDuration` (§4.6), which is for the target: a mark has no `lastUpdated` and never expires, so the property changes nothing here.
2. **Filter.** A reject is skipped when it is an undecodable (its reason starts with `avro:`), carries coordinates `(p, o)`, and `o ≤ m_p`.
   - Coordinates come from the `<prefix>partition`/`<prefix>offset` columns of the rejected row. Undecodables and transform-evaluation rejects (source rows) always have them; coerce and conform rejects have them when the SQL kept the columns (`SELECT *`). They count only when they name an offset the batch read, inside its range for that partition: a message's own coordinates always do, so anything else under those names (values the SQL computed, or a payload's own fields read under the default prefix where the pipeline set another) is treated as no coordinates.
   - "Schema not found" rejects are **never** skipped: the registry can change between attempts.
   - Nor is any reject that is not an undecodable: transform-evaluation, conform, coerce and union-alignment rejects are judged against the target's schema, the union `U` (§5.5), the SQL (which may call `now()`), all of which can change between two attempts at one batch. After an `ALTER TABLE` on the target (§4.7 row 20) the retried batch's fresh coercer can reject a row at an offset below the mark that the first attempt wrote to the target's failed commit; skipping it would leave it in neither table. They are written on every attempt.
3. **Commit** the remaining rows, with txns `<app_id>.dq-<p> = max(m_p, last_p)` for every batch partition and metadata `ddi.appId`, `ddi.kafka.offsets`.
   - The DQ row's `source_version` column holds the rejected message's offset, or −1 when unknown. Its partition is in `payload`.
   - Members own disjoint partitions, so their DQ appIds never collide (I21).
4. **On `ConcurrentTransaction`, `VersionAlreadyExists` or `MaxCommitAttempts`:** refresh the DQ handle, then look for this write's own commit among those that landed since its read version. Every Kafka DQ commit records `ddi.kafka.dqWriteId`, unique to its `write_kafka` call.
   - Found: the commit landed and the acknowledgement of its PUT was lost; the retried PUT met it (§4.7 row 3). The write is done. Writing again would duplicate every reject that is never skipped, without any crash.
   - Not found (a zombie or a handover): go back to step 1, up to 3 times; then Transient. A DQ conflict is never a supervisor error.

Why this is safe: Kafka offsets identify one message per topic incarnation, and incarnations are checked by §4.4. `set-offsets` moves the DQ marks with the target txns. Whether a message decodes depends only on its bytes and its writer schema, which a replay cannot change, so an undecodable is rejected again on every replay. So a skipped reject is always already in the table (I9). Delta pipelines keep `write`'s equality rule unchanged.

**Cap.** More than `max_evaluation_rejects_per_batch` undecodables in one batch (default 100, config.rs:37-42) is Blocking: "this looks like a format or registry misconfiguration, not bad data".

**Without a DQ table:** the first undecodable makes the pipeline Blocking with `KafkaUndecodable{partition, offset, reason}` and `ddi_kafka_undecodable_blocking = 1`. The member stays in its group, and the recheck re-opens the DQ table. This is ddi's rule (dq.rs:28-32) and **differs from kdi**, which silently dropped such messages through the default `NoopDeadLetterQueue` (open question 5).

**Transform and coercion rejects** follow the existing semantics, and are written again on every replay (step 2). Keep `<prefix>partition`/`<prefix>offset` in the SQL output (`SELECT *, …`) anyway, so their DQ rows say which message they came from and a duplicate can be told apart.

**Tombstones** are skipped and counted in `ddi_kafka_tombstones_total`; their offsets advance (kdi lib.rs:866-871).

---

## 6. Kafka metadata columns and kdi's `-t` transforms

### 6.1 Columns, always appended after the Avro fields

`<prefix>` is `metadata_prefix`, default `kafka_`.

| Column | Arrow type | kdi equivalent (transforms.rs:441-475) |
|---|---|---|
| `<prefix>topic` | Utf8 | `kafka.topic` |
| `<prefix>partition` | Int32 | `kafka.partition` |
| `<prefix>offset` | Int64 | `kafka.offset` |
| `<prefix>timestamp` | Timestamp(µs, "UTC"); null when NotAvailable | `kafka.timestamp`, a **µs number** (ms × 1000, transforms.rs:468-475). Into BIGINT the cast gives µs (identical); into TIMESTAMP, the same instant; into STRING under `kdi_compat`, µs text. |
| `<prefix>timestamp_type` | Int32: −1 n/a, 0 CreateTime, 1 LogAppendTime | `kafka.timestamp_type`, same codes |
| `<prefix>key` | Binary | — |
| `<prefix>headers` | List<Struct<key Utf8, value Binary>> (keeps order and duplicate keys) | — |
| `<prefix>value_schema_id` | Int32 | — (drives `kdi_null_text` presence, §5.6) |

**Collisions.** An Avro top-level field whose name starts with the prefix makes the pipeline Blocking `KafkaMetadataCollision{schema_id, field}`, and `plan` reports it in advance. Set another `metadata_prefix`, then map names in SQL. Letting the payload win was rejected: it would silently turn the offset columns the partition watermark relies on into payload data.

With no SQL, the coercer keeps the columns whose names exist in the target and drops the rest (schema.rs:110-111). A bronze table with `kafka_partition`/`kafka_offset` columns therefore gets them with no configuration. These are the names partition-watermark expects (`dedup_partition = "kafka_partition"`, `dedup_timestamp = "kafka_offset"`; config.rs:341-353; pipelines.example.toml:99-105).

### 6.2 What ddi's transform engine supports here (verified)

**Supported:**
- one `SELECT … FROM source`, row-local, with `WHERE`, `CASE`, casts and `CROSS JOIN UNNEST`;
- dotted struct access `a.b.c` (validator test at transform/validate.rs:1500-1503);
- Trino JSON functions (transform/json.rs, json_build.rs);
- lambdas (`transform`, `filter`; dialect.rs:63-66);
- `from_unixtime` with zones (rewritten at validate.rs:417-520);
- any DataFusion scalar builtin:
  - `substr` (1-based, counts characters);
  - `to_char`, whose alias `date_format` takes **chrono** patterns (to_char.rs:116);
  - `to_timestamp_seconds/millis/micros` (to_timestamp.rs:508, 584);
  - `named_struct`, `get_field`, `union_extract`.

**Refused at load:** aggregates, `GROUP BY`, `DISTINCT`, windows, set operations (`UNION/INTERSECT/EXCEPT`, validate.rs:279, 348), other relations, and lookups for Kafka.

**Evaluation failures:** a row that fails to evaluate goes to DQ, up to the cap (pipeline.rs:604-651).

**Identifiers:** DataFusion folds unquoted identifiers to lower case, and ddi's `session_config` leaves that on (budget.rs:179-186). So **double-quote camelCase field names and aliases** (`"messageId"`, `AS "messageId"`).

### 6.3 Translation table

kdi evaluates every expression against the **original** message, keeps all original fields, and does not set a field whose result is null (`set_value`, transforms.rs:343-372). `SELECT *, … FROM source` has the same shape.

To overwrite an existing column with kdi's semantics (a null result keeps the original), use `SELECT * REPLACE (coalesce(<expr>, "f") AS "f")`. GenericDialect parses REPLACE (sqlparser generic.rs:232) and DataFusion plans it (datafusion-sql select.rs:847); acceptance by ddi's validator is **verify (M4)**, with an enumerated column list as the fallback.

| kdi `-t` | ddi `transform_sql` |
|---|---|
| `x: kafka.partition` / `.offset` / `.topic` / `.timestamp_type` | Nothing if the target column is `kafka_*`; otherwise `kafka_partition AS x`, etc. kdi recognised `kafka.*` only as the whole right-hand side (transforms.rs:101-111). |
| `x: kafka.timestamp` | `kafka_timestamp AS x`. A BIGINT target gets µs, as in kdi; TIMESTAMP is correct; STRING with `kdi_compat` gets µs text. |
| `meta.kafka.offset: kafka.offset` (nested, merging into an existing `meta`) | `SELECT * REPLACE (named_struct('producer', "meta"."producer", 'kafka', named_struct('offset', kafka_offset, 'partition', kafka_partition, 'topic', kafka_topic)) AS "meta")`. Every existing child must be listed. |
| `x: field` | `"field" AS "x"` |
| `x: a.b.c` | `"a"."b"."c" AS "x"`. Planning of quoted multi-level access **verify (M4)**; fallback `get_field(get_field("a",'b'),'c')`. |
| `f: <expr>` where f already exists | `* REPLACE (coalesce(<expr>, "f") AS "f")` |
| `x: substr(s, \`a\`, \`n\`)` (skip a characters, take n; transforms.rs:216-240) | `substr("s", a + 1, n)` |
| `x: epoch_seconds_to_iso8601(n)`, TIMESTAMP target | `from_unixtime(n, 'UTC')` or `to_timestamp_seconds(n)` |
| same, STRING target, exact kdi text | `to_char(to_timestamp_seconds(n), '%Y-%m-%dT%H:%M:%S%.fZ')`. kdi printed `format!("{:?}", DateTime<Utc>)` (transforms.rs:243-251): `2021-07-20T23:18:18Z`, with a fraction only when non-zero. Both are chrono. **Into STRING, always produce text explicitly:** under `kdi_compat` a Timestamp renders as a raw number. |
| `epoch_millis_to_iso8601(n)` / `epoch_micros_to_iso8601(n)` | `to_timestamp_millis(n)` / `to_timestamp_micros(n)`, or `to_char(…)` for STRING. **If n is an Avro timestamp-millis/-micros field, drop the transform; it is already a timestamp.** |
| `epoch_millis_to_micro(n)` | TIMESTAMP target: nothing if n is logically typed, else `to_timestamp_millis(n)`. BIGINT target: `n * 1000`. |
| the `ingest_date` partition column | From the inventoried `-t` (M-1); kdi read a payload field, never `kafka.timestamp`. Example: `-t 'ingest_date: substr(meta.producer.timestamp, \`0\`, \`10\`)'` → `substr("meta"."producer"."timestamp", 1, 10) AS ingest_date`. The fixture and §10.3 check partition-value equality. |
| JMESPath `to_string`, `to_number`, `length`, `join`, `contains`, `starts_with`, `[0]`, `[?f=='v']`, `not_null(a,b)` | `CAST(… AS VARCHAR)`, `CAST(… AS DOUBLE)`, `length`/`cardinality`, `array_to_string`, `strpos(…) > 0`, `starts_with`, `x[1]`, `filter(arr, e -> e.f = 'v')`, `coalesce(a,b)` |
| A nested record into a STRING column | Nothing: `kdi_compat` renders kdi's sorted-key JSON. `json_format(CAST(col AS JSON))` gives Trino-style text instead (json_build.rs). |

Avoid MySQL-style `date_format` patterns: in DataFusion it is `to_char` with chrono patterns (`%Y-%m-%d`, not `%Y-%m-%d %H:%i`). Kafka transforms are not shared with dbt or Trino, so write DataFusion spellings.

### 6.4 Semantic differences from kdi

1. **Typed timestamps and dates.** No `epoch_*` is needed for logically typed fields. Untransformed kdi wrote timestamp-millis into TIMESTAMP columns 1000× too small; ddi writes the correct instant, a visible discontinuity that `plan` flags.
2. **Decimals, bytes and fixed** arrive typed in typed columns. kdi produced byte arrays, or crash-looped on DECIMAL columns: arrow-json failed, `DeltaWriteFailed` was returned (lib.rs:932-937), and `?` exited the run loop (lib.rs:493-495). In STRING columns `kdi_compat` keeps kdi's text.
3. **Failures.** kdi silently dropped deserialization failures, including those from transient Schema Registry outages (lib.rs:866-879), and transform failures (Noop DLQ). ddi retries transient SR errors, sends deterministic failures to DQ, and pauses without a DQ table. Bronze written by kdi can have holes that ddi-written bronze does not.
4. **Transform order.** kdi applied transforms in `HashMap` order, each reading the original message. SQL expressions also read only `source`, so this matches.
5. **Offsets advance** past tombstones and rejects; kdi's did not. This is visible only in txn values.
6. **`"null"` text and raw-number rendering** happen only with `kdi_compat`/`kdi_null_text`, which are explicit on kdi tables and default to false on new ones. Without them, nulls are NULL and casts are arrow's.
7. **Computed columns into STRING** are rendered by type under `kdi_compat`. A computed Timestamp or Decimal into STRING therefore gives a raw number or a byte array, as kdi's JSON would have. Produce text explicitly when you want text.

### 6.5 Worked example: kdi README.adoc:40-47

kdi:
```
-t 'date: substr(meta.producer.timestamp, `0`, `10`)'
-t 'meta.kafka.offset: kafka.offset'
-t 'meta.kafka.partition: kafka.partition'
-t 'meta.kafka.topic: kafka.topic'
```

ddi:
```sql
SELECT * REPLACE (
         named_struct('producer', "meta"."producer",
                      'kafka', named_struct('offset', kafka_offset, 'partition', kafka_partition, 'topic', kafka_topic)) AS "meta"),
       substr("meta"."producer"."timestamp", 1, 10) AS "date"
FROM source
```

---

## 7. Metrics and checkpoints

### 7.1 Metrics

**What kdi emitted** (statsd via dipstick; prefix `KDI_METRICS_PREFIX`, default `kafka_delta_ingest`; untagged, so deployments are told apart by prefix; kdi metrics.rs:228-321):
- `messages.deserialization.{completed,failed}`, `messages.transform.{completed,failed}`, `messages.size`
- `recordbatch.{started,completed,write_duration}`, `buffered.record_batches`
- `delta.write.{started,completed,failed,duration}`, `delta.add.size`
- `buffer.lag.*`, `delta.write.lag.{num_partitions,total,max,min}`

**Existing ddi families that apply unchanged** (dependency-free Prometheus text exposition):
- `ddi_batches_committed_total` (kdi `delta.write.completed`) and `ddi_rows_written_total` (`recordbatch.completed`);
- `ddi_rows_rejected_total`, `ddi_rows_rejected_by_transform_total`, `ddi_batches_fully_rejected_total`, `ddi_commits_skipped_total`;
- `ddi_errors_total`, `ddi_pipeline_restarts_total`, `ddi_pipeline_up`, `ddi_pipeline_seconds_since_progress` (with the Kafka progress rule, §3.9), `ddi_pipeline_config_valid`.

`PipelineMetrics` gains `kafka: Option<Arc<KafkaMetrics>>`, rendered after the existing block only when `Some`, so Delta pipelines' output is unchanged. Durations follow ddi's `_milliseconds_total` convention (as `ddi_merge_milliseconds_total`).

| Metric | Type | Replaces in kdi |
|---|---|---|
| `ddi_kafka_messages_total`, `ddi_kafka_bytes_total` | counter | `messages.deserialization.completed`, `messages.size` |
| `ddi_kafka_undecodable_total` | counter | `messages.deserialization.failed` |
| `ddi_kafka_tombstones_total` | counter | (logged only) |
| `ddi_kafka_messages_dropped_total{reason="not_owned\|blocked\|duplicate\|reread"}` | counter | — |
| `ddi_kafka_offset_gaps_total` | counter | — (compaction or transaction markers) |
| `ddi_kafka_commit_milliseconds_total` | counter | `delta.write.duration`, `recordbatch.write_duration` |
| `ddi_kafka_files_added_total`, `ddi_kafka_bytes_added_total` | counter | `delta.add.size` (from delta-rs's write metrics; exposure **verify (M3)**) |
| `ddi_kafka_rebalances_total{kind="assign\|revoke\|lost\|error"}` | counter | — |
| `ddi_kafka_revoke_flushes_total{outcome="committed\|dropped\|timeout\|none"}` | counter | — |
| `ddi_kafka_fenced_total{kind="concurrent_txn\|base_mismatch\|other_conflict"}` | counter | kdi's `ConflictingOffsets` path |
| `ddi_kafka_assigned_partitions`, `ddi_kafka_blocked_partitions` | gauge | `*.lag.num_partitions` |
| `ddi_kafka_partition_committed_offset{partition}` | gauge | — |
| `ddi_kafka_partition_lag_messages{partition}`, `ddi_kafka_lag_messages`, `ddi_kafka_lag_messages_max` | gauge | `delta.write.lag.total` / `.max` |
| `ddi_kafka_retention_headroom_messages` | gauge | — |
| `ddi_kafka_oldest_buffered_seconds` | gauge | `buffer.lag` |
| `ddi_kafka_brokers_up`, `ddi_kafka_retrying` | gauge | — |
| `ddi_kafka_offsets_unavailable`, `ddi_kafka_offsets_regressed`, `ddi_kafka_offset_missing`, `ddi_kafka_topic_replaced`, `ddi_kafka_undecodable_blocking`, `ddi_kafka_schema_conflict`, `ddi_kafka_target_columns_unmatched`, `ddi_kafka_null_text_approximate` | gauge, held until the cause clears (like `ddi_source_file_vacuumed`, metrics.rs:245) | — |
| `ddi_kafka_null_partition_values_total` | counter | — |
| `ddi_kafka_offset_lookups_total`, `ddi_kafka_offset_lookup_seconds_total`, `ddi_kafka_offset_lookup_fallbacks_total` | counter | — |
| `ddi_kafka_dq_rows_skipped_total`, `ddi_kafka_dq_conflict_retries_total` | counter | — |
| `ddi_kafka_schema_registry_requests_total{outcome}`, `ddi_kafka_client_errors_total{code}` | counter | — |

**Alert rules** (shipped in docs/alerts.yml, M5):
- `rate(ddi_kafka_fenced_total[10m]) > 0` for 30 minutes (a second deployment, or kdi still running);
- `sum by (pipeline) (ddi_kafka_assigned_partitions) == 0` while `max by (pipeline) (ddi_kafka_lag_messages) > 0`;
- `ddi_kafka_blocked_partitions > 0`, and any of the held gauges `== 1`;
- `ddi_kafka_retrying == 1` for 15 minutes;
- `ddi_pipeline_seconds_since_progress > 3 × allowed_latency`;
- `ddi_kafka_retention_headroom_messages` below a per-topic threshold.

**statsd: not built** unless kdi's statsd names feed live dashboards or alerts (open question 4). If they do, M5 adds `[metrics.statsd] endpoint` plus a per-pipeline `statsd_prefix` (default `kafka_delta_ingest`): a std `UdpSocket` push every 10 s of the kdi-named subset, mapped from these counters, about 1 day. Otherwise a Datadog openmetrics check or an OTel collector can scrape Prometheus.

### 7.2 Checkpoints and log cleanup

**kdi `-c`:**
- writes a checkpoint and runs `cleanup_metadata` whenever `version % 10 == 0` (delta_helpers.rs:42-68), ignoring the table property;
- known problems: skipped checkpoints after restarts (#184), oversized checkpoints (#187).

**delta-rs 0.32.4, after each commit:**
- checkpoints when `(version+1) % delta.checkpointInterval == 0` (kernel/transaction/mod.rs:1078-1101; default interval 100, table/config.rs:259-264), independent of restarts. It skips silently if `require_files` is off (1072-1077), so ddi keeps it on;
- with `enableExpiredLogCleanup` (default true), `cleanup_expired_logs_for` **lists the whole `_delta_log`** on every commit (protocol/checkpoints.rs:125-126).

**Plan:**
1. At cutover, for tables kdi ran with `-c`, set `delta.checkpointInterval = 10` while nothing writes. Use Trino `ALTER TABLE … SET PROPERTIES checkpoint_interval = 10` **(verify that Trino allows it)**, or python `deltalake` `alter.set_table_properties`. The interval also bounds `read_txns`' worst case (§4.3).
2. Kafka commits pass `with_cleanup_expired_logs(Some((table.version()+2) % interval == 0))`. The listing then happens only on commits that also checkpoint, as in kdi. Guessing wrong under contention means one cleanup more or less, which is harmless. Delta pipelines keep `CommitProperties::default()`.

---

## 8. Build and packaging

**Settled today** (§0.3.21): the C build works on this laptop with the chosen features, and no libsasl2 or libzstd is needed. The M1 build in ddi is a regression check, not an experiment.

**Cargo.** As in §3.1. `kafka` is ddi's first `[features]` section. `Cargo.lock` lists the optional crates even with the feature off, and `--locked` builds without features still work. **Do not run `cargo update`.** Add the crates with `cargo add`/targeted resolution, so zstd-sys stays at the locked 2.0.16. New crates:
- rdkafka, rdkafka-sys, openssl-sys, libz-sys, num_enum (+derive), slab, vcpkg (build only);
- arrow-avro, strum_macros 0.28;
- apache-avro (the production version) and its tree, dev only.

**Laptop.**
- Command: `export PATH="$HOME/.cargo/bin:$PATH"; systemd-run --user --slice=build.slice --scope cargo build -j 2 --features kafka`.
- Run at most two agents at once.

**Dockerfile** (docker/Dockerfile:17-46):

```dockerfile
ARG DDI_FEATURES=                     # empty until M3 ships; then "kafka"
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev zlib1g-dev …   # already in rust:1.97-bookworm
RUN … cargo build --release --locked --features "$DDI_FEATURES" …     # lines 24-25 AND 27, so the dependency-cache layer still works
# runtime stage, line 31: add libssl3 explicitly (today it only arrives through ca-certificates → openssl)
RUN /usr/local/bin/ddi --version     # line 42: proves libssl.so.3 resolves and prints librdkafka 2.12.1
```

- From M3 on, the published image is the kdi replacement and builds with `kafka`; `--build-arg DDI_FEATURES=` gives a Delta-only image.
- Downstream repos that build ddi from source by `DDI_REF` (for example `dat_kafka_connect`) must add `--features kafka` and check that their base image has the dev packages, or pull the ghcr image (open question 8).

**CI** (.github/workflows/ci.yml):
- **`check`** (15-28): unchanged. This is the feature-off proof of I13 and I16.
- **New `check-kafka`:** `cargo clippy --all-targets --features kafka -- -D warnings` and `cargo test --all-targets --features kafka`. The unit tests cover decode, SR, conform, fence and DQ; no broker is needed.
  - Until M3 wires them up, the M2 modules carry `#![cfg_attr(not(test), allow(dead_code))]`, removed in M3 (PO-minor 10).
- **New `kafka-integration`,** in the style of the Azurite job (155-185):

  ```yaml
  - run: docker run -d --name rp -p 9092:9092 -p 8081:8081 redpandadata/redpanda:<pinned> redpanda start --mode dev-container --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr 127.0.0.1:9092 --schema-registry-addr 0.0.0.0:8081
  - run: cargo test --features kafka --test 'kafka_*' --no-run
  - run: <wait for :9092 and :8081>
  - run: cargo test --features kafka --test 'kafka_*' -- --ignored --test-threads=1
    env: { DDI_TEST_KAFKA: 127.0.0.1:9092, DDI_TEST_SR: http://127.0.0.1:8081 }
  ```

- **New `kafka-sasl`:** a second Redpanda with SASL/SCRAM-SHA-512 on 19092 (`rpk security user create … --mechanism SCRAM-SHA-512`, `enable_sasl=true`) **(verify flags, M4)**.
- **`image`** job: builds with `kafka` from M3.
- **Nightly `kdi-parity`** (`schedule` + `workflow_dispatch`): `kdi_and_ddi_race` and `kdi_ddi_alternate` (§9.4), using the production kdi image.

**Licensing.**
- `deny.toml` gains `[graph]\nall-features = true`, so the `licenses` job sees the optional crates. All new crates are MIT, Apache-2.0 or BSD, already allowed (deny.toml:11-31).
- cargo-deny cannot see statically linked C. `scripts/third-party-notices.py` appends `rdkafka-sys-*/librdkafka/LICENSES.txt` when rdkafka-sys is in the lockfile:
  - librdkafka is BSD-2-Clause;
  - its bundled third parties are cJSON, crc32c, fnv1a, hdrhistogram, lz4, murmur2, nanopb, opentelemetry protos, pycrc, queue, regexp, snappy, tinycthread and wingetopt;
  - zstd (BSD/GPLv2 dual; BSD taken) is appended as well.
- OpenSSL 3 (Apache-2.0) is linked dynamically from the base image and covered by Debian's copyright files.

---

## 9. Tests

### 9.1 Unit tests (no broker)

**M0, sink, fence and DQ:**
- `delta_commit_golden`, written first, with six shapes including the partitioned target.
- Commits: `kafka_commit_writes_n_txns` (`transaction_version("app-0") == 9`, `"app"` → None, `ddi.kafka.offsets` and `ddi.kafka.marks` present, no `ddi.sourceVersion`); `kafka_empty_commit_carries_txns_only`, also on a partitioned target.
- delta-rs pin: `fence_concurrent_txn_rejected_by_delta_rs`. Two handles on one snapshot both commit `a-0`, and the second fails with `CommitConflict(ConcurrentTransaction)`, so a delta-rs bump that changes this fails CI.
- Fence: `fence_disjoint_partitions_do_not_conflict`, `fence_base_mismatch_detected`, `none_partition_verified_before_first_commit`, `revoked_partition_not_reasserted`.
- Retention: `set_transaction_retention_refused`, `txns_without_last_updated_never_expire` (property set after the fact).
- Retried PUT: `retried_put_of_own_commit_is_concurrent_transaction`, using a log store that fails the first conditional PUT with 412 after writing.
- Txn reader:
  - `read_txns_one_get_in_steady_state`, with a counting object store;
  - `read_txns_walks_back_to_checkpoint`, `read_txns_reads_checkpoint_txn_column`, `read_txns_multipart_checkpoint`;
  - `read_txns_foreign_checkpoint_falls_back_to_kernel`;
  - `read_txns_returns_marks_and_writer`.
- DQ:
  - `dq_marks_skip_recorded_offsets`, `dq_schema_not_found_never_skipped`, `dq_without_coordinates_written`;
  - `dq_two_writers_disjoint_partitions_no_conflict`, `dq_same_partition_conflict_retried_in_place`;
  - `dq_delta_write_unchanged` (equality rule pinned).

**M1, config:**
- every §2.5 rule;
- `kafka_config_without_feature_is_held_back`, `kafka_runs_with_manifest_set`;
- `same_app_id_on_two_kafka_targets_allowed`, `same_target_and_app_id_rejected`, `shared_group_id_rejected`;
- `forced_key_rejected`, `cooperative_rejected_at_load`, `scram_json_env_compat`, `secret_redaction_in_debug`;
- `section_defaults_overridden_by_pipeline`, `starting_offsets_star_and_shapes`.

**M2, registry** (a hand-rolled `tokio::net::TcpListener` HTTP stub; dev-deps already have `net` and `io-util`, Cargo.toml:80):
- `basic_auth_from_kdi_env`, `proxy_env_honoured`;
- `deleted_true_query_sent`, `absent_schema_type_is_avro`;
- `404_40403_is_undecodable_nondeterministic`, `401_is_consumer_fatal`, `retry_on_5xx_then_transient`;
- `cache_one_request_per_id`, `references_block`, `latest_version_fetched_on_unseen_id`.

**M2, decode** (fixtures written with arrow-avro's own writer, `AvroSoeFormat` + `FingerprintStrategy::Id`, as in reader/mod.rs:615-639, plus messages from the M-1 production corpus):
- `logical_type_matrix`;
- `writer_schema_switch_mid_batch` (interleaved ids, no 1-row batches);
- `truncated_message_isolated`, `trailing_bytes_isolated`, `unknown_schema_id_isolated`, `no_magic_byte_isolated`, `non_record_top_level_undecodable`;
- `metadata_aligned_after_isolation`, `huge_length_prefix_errors_without_allocation`;
- `uuid_and_enum_normalised`, `timestamps_are_utc`;
- `decoded_bytes_measured_by_slice_not_capacity`;
- `metadata_prefix_collision_blocks`.

**M2, conform:**
- **Parity:**
  - `kdi_parity_matrix`: every §5.6 case through the production apache-avro version's `Value → serde_json`, then kdi's coercion rules (coercions.rs:70-145, ported into the test), compared with ddi's output;
  - `kdi_null_text_matrix`: top level, nested, arrays of strings, arrays of structs, maps (no text), metadata (no text), computed columns (no text);
  - `null_text_presence_by_schema_id` (a field added in v2: v1 rows NULL, v2 null rows `"null"`), `null_text_approximate_without_schema_id_column`;
  - `render_by_arrow_type_at_depth`, `aliased_timestamp_renders_raw_number`;
  - `decimal_bytes_minimal_and_fixed_sign_extended`, `double_to_decimal_via_shortest_text`, `kafka_key_cast_not_rendered`.
- **Mapping:** `nested_struct_by_name`, `list_and_map_renamed_to_target`, `field_added_and_removed`, `union_branch_to_target`.
- **Guards:** `unmatched_target_column_warns`, `case_mismatch_not_matched`, `sorted_json_keys`, `nan_float_rejected_under_kdi_compat`.
- **Union `U`:**
  - `union_u_latest_plus_seen`, `union_u_unseen_old_conflict_ignored`;
  - `union_u_newest_type_wins_old_rows_cast_or_rejected`, `union_widening_wraps_branch`;
  - `union_u_growth_keeps_sql_planning`, `union_u_sql_no_longer_plans_blocks`.
- **Partition columns:** `partition_column_unmapped_refused`.

**M3, partition state and start:**
- `not_owned_dropped`, `blocked_dropped`, `duplicate_below_next_expected_dropped`;
- `reread_verified_and_dropped`, `reread_mismatch_blocks`, `reread_compacted_skips_verification`;
- `gap_counted`, `batch_close_by_latency_messages_bytes_decoded`, `latency_timer_starts_after_commit` (kdi #233);
- every row of §4.4, `app_id_typo_refused`, `shared_table_second_pipeline_allowed`, `baseline_when_any_start_positive`, `kdi_compat_unset_on_kdi_table_refused`;
- `error_classification_table` (every §3.6 row).

### 9.2 Integration (tests/kafka_*.rs, `#[ignore]`; Redpanda with its built-in SR; local filesystem tables, one Azurite variant)

**Basics and crashes:**
1. `basic_once`: N Avro messages over 6 partitions → `ddi once` → rows equal the produced (p, o) exactly; txns = last per partition; tombstones counted; metadata typed.
2. `restart_resumes_at_stored_plus_one`: a loop of `step()` with the `Pipeline` dropped at random points (as tests/exactly_once.rs) → exactly once; `dropped_total{reason="reread"}` = one per partition per assign.
3. `kafka_kill_mid_batch`: 30 SIGKILLs of `ddi once` under continuous production (tests/kill_mid_batch.rs:37-60 pattern) → exactly once, and at least one run leaves orphan parquet (proof the upload→commit window was hit).
4. `kill_between_upload_and_commit`, deterministic: after the first commit, `chmod a-w <target>/_delta_log`, so the parquet PUT succeeds and the commit fails → orphans exist, txn unchanged, Transient retry. Restore permissions → the held batch commits; exactly once.

**Groups, rebalancing and liveness:**

5. `two_groups_same_app_id_never_duplicate`: two pipelines, same app_id, different groups, interleaved steps → no duplicates, `fenced_total > 0`. Variant: a foreign txn-only commit on `app-3` between our assign and our commit forces a reset.
6. `rebalance_two_consumers`: two `ddi run` subprocesses in one group, 6 partitions, continuous production. SIGTERM one, SIGKILL the other mid-batch, restart both, drain with `ddi once` → exactly once, `rebalances_total > 0`.
7. `revoke_flushes_before_unassign`: member A holds a buffer with `allowed_latency = 120 s`; member B joins → A's buffer is committed before B gets partitions (`revoke_flushes_total{outcome="committed"} ≥ 1`); exactly once. Variant `revoke_flush_timeout_drops` uses a fault hook that stalls the commit.
8. `liveness_under_churn`: 3 members with `allowed_latency = 30 s`; a 4th member restarts every 10 s for 3 minutes → every original member commits at least once per 60 s.
9. `transient_storage_outage_stays_in_group`: the target made read-only for 30 s → no rebalance (`rebalances_total` unchanged), `retrying = 1`, the batch commits after restore. Azurite variant: pause the container.
10. `zombie_after_poll_interval`: `max.poll.interval.ms = 6000` plus the fault hook `sleep_before_commit` → no duplicates.

**Retention, topic changes and missing offsets:**

11. `seek_past_retention_blocks_partition`: `rpk topic trim-prefix` past stored+1 while ddi is down → that partition is blocked and the others progress. `set-offsets --next <low> --acknowledge-skip` unblocks it within `blocked_recheck_secs`, recorded in the commit.
12. `mid_fetch_retention_commits_buffer_then_blocks`.
13. `topic_recreated_fails` (Regressed) and `topic_recreated_with_more_data_detected` (re-read mark).
14. `new_partition_starts_at_zero` (`rpk topic add-partitions` mid-run → exactly once) and `missing_txn_low_above_zero_blocks`.

**Schemas, DQ and recovery:**

15. `schema_evolution`: register v1, then v2 with an added field, then v3 changing a field type; interleave producers. The identity path fills the target; SQL referencing the v2 field gives NULL for v1 rows; v1 rows that cannot be cast go to DQ. No stall.
16. `undecodable_goes_to_dq_or_blocks`: a bad magic byte with no DQ table → blocked (gauge), still in the group. After creating `<t>__ddi_dq` the recheck resumes it, with a DQ row holding partition, offset and base64. Above the cap → blocked.
17. `dq_crash_between_dq_and_target` (fault hook `abort_after_dq`): the replay, with different batch boundaries, writes no duplicate for coordinate-carrying rejects.
18. `two_members_dq_rejects_concurrently`: both write DQ rows for minutes → no step errors, no rebalances, every reject exactly once.
19. `latest_baseline_survives_crash` and `earliest_on_trimmed_topic_baseline`.
20. `cooperative_rejected_at_load`.
21. `alter_table_mid_stream`: add a nullable column on bronze while running → reset, coercer rebuilt, held batch retried, continues.
22. `replaced_target_blocks`.
23. `ambiguous_commit_error_no_duplicate`: an object-store wrapper that returns an error after a successful log PUT.
24. `accounting_identity`: consumed = rows + undecodable + tombstones (+ filtered), from metrics.
25. `zstd_compressed_topic`, `raw_format_once`.
26. `scram_sha512_kdi_env_compat` (SASL Redpanda, `KAFKA_DELTA_INGEST_SCRAM_JSON`; M4).
27. `downstream_partition_watermark`: a Delta→Delta pipeline with `dedup_partition = "kafka_partition"`, `dedup_timestamp = "kafka_offset"` over the ddi-written bronze → exact rescan.

**Once-mode:**

28. `once_finishes_with_idle_empty_and_caught_up_partitions`, `once_with_open_transaction` (read_committed: ends at the LSO), `once_does_not_rebalance_running_group`.

**Targets, handles, budget and progress:**

29. `partitioned_target_ingest_date`: data, an all-tombstone batch (zero-row commit) and partition-value equality; plus `partition_column_unmapped_refused_at_open`.
30. `handle_aged_past_log_retention`: a tiny `logRetentionDuration`, an idle member, another writer and a cleanup → the commit succeeds after the refresh.
31. `small_flush_does_not_shrink_batches`: a budget installed, a 1-row isolation flush, then backlog → batch sizes stay at the cap.
32. `waiting_not_progress_when_fenced`: a second deployment fences continuously → `seconds_since_progress` grows.
33. `protocol_upgrade_blocks`: enable a writer feature delta-rs cannot write → blocked, gauge.

**Fault hooks** come from `DDI_TEST_FAULT=<name>[:arg]`, compiled only under `cfg(any(test, feature = "fault-injection"))`: `sleep_before_commit`, `abort_after_dq`, `stall_commit`.

### 9.3 kdi takeover with a real kdi-written table (fixture built in M-1, test in M4)

1. **Harness** (`tests/kafka/kdi/compose.yml`): Redpanda plus the **production kdi image** (by digest, from the inventory). Only if it cannot be pulled, the fork is built at the production commit with the production crate versions.
2. **Corpus:** about 2,000 Avro messages over 3 explicit partitions. It covers the §5.6 type matrix, null values of present fields at every depth, two writer versions (one adding a field), a nested record into a STRING column, `bytes` and `fixed` decimals as the real producers encode them, and tombstones. Where possible, sampled from real topics with the real schemas (M-1).
3. **Run kdi twice** with your real flag shapes (`-a`, `-g`, `-t` with `kafka.*`, the real `ingest_date` expression, `-c`):
   - `T_kdi_partial`: stopped after offset k per partition;
   - `T_kdi_full`: consumes everything.
4. **Check in** both tables (about 100 KB) plus the corpus and flags under `tests/fixtures/kdi_bronze_prod/`.
5. **`kdi_takeover_fixture`:** produce the same corpus into a fresh topic (identical offsets), copy `T_kdi_partial`, and run `ddi once` with kdi's app_id and `kdi_compat = kdi_null_text = true`. Assert equality with `T_kdi_full` as a multiset per (partition, offset), per column **and per partition value**. This pins I3 and I12 together. The only allowed differences are listed per column (§6.4), including the DQ rows for messages kdi dropped.

### 9.4 Nightly

- `kdi_and_ddi_race`: kdi and ddi on one table under one app_id for 2 minutes → each offset exactly once. Conflicts on every commit for partitions both hold are expected (kdi re-asserts all its assigned partitions).
- `kdi_ddi_alternate`: hand the table back and forth 3 times. This proves rollback, including delta-rs 0.26 reading 0.32 checkpoints and txns without `lastUpdated`.

---

## 10. Cutover from kdi (per topic)

**0. Inventory** is milestone M-1 (§13). It must be done before M0/M2, because it decides scope (PO-M4).

**1. Prepare** (no behaviour change):
- Create `<bronze>__ddi_dq` (schema at dq.rs:80-125).
- **Add `kafka_partition INT`, `kafka_offset BIGINT`, `kafka_topic VARCHAR` to bronze**, with the matching kdi `-t kafka.*` transforms (one kdi restart). The offset-exact comparison below and the downstream partition watermark need them (open question 3). Historical rows stay null.
  - The partition watermark refuses null `kafka_partition` values inside a coverage window (dedup.rs:705-716). Enable `dedup_partition` on silver only once its coverage windows no longer reach pre-cutover rows, or backfill.
- If kdi ran with `-c`, set `delta.checkpointInterval = 10`.
- Group ACLs: allow `ddi-<name>` (or plan to set `group_id` to kdi's).
- Decide `kdi_compat`/`kdi_null_text` per table from `plan`'s `= 'null'` counts.
- Build the image with `kafka`.

**2. Shadow** (at least 24 h, through a schema change if possible):
- `ddi kafka clone-table -p <name> --to <bronze>__ddi_shadow`, which keeps exact column case and adds no column mapping. Not Trino CTAS.
- Configure `[[kafka.pipeline]]` with:
  - `app_id = "ddi.shadow.<topic>"` and `group_id = "ddi-shadow-<topic>"`, a different group so kdi keeps its partitions;
  - `starting_offsets` = the map `plan` printed from kdi's live txns (next offsets), committed as a baseline;
  - `kdi_compat` as decided, and `commit_offsets_to_kafka = false`.

**3. Compare** over each partition's window `[start_p, min(kdi_stored_p, shadow_stored_p)]`, keyed on `(kafka_partition, kafka_offset)`:
- Both directions of `(SELECT cols FROM kdi WHERE <window>) EXCEPT ALL (SELECT cols FROM shadow WHERE <window>)`, **including the partition column values**. Every difference must be on the accepted list (§6.4).
- Offsets present in the shadow but missing in kdi are kdi's silent drops (holes). List them; they are expected and explained by DQ or by SR outages in kdi's logs.
- `count(*)` per partition and `ingest_date`: shadow plus its DQ count must equal kdi plus kdi's holes.
- Every DQ row is explained.
- `ddi_kafka_lag_messages` is steady, memory is within budget, and `fenced_total = 0`.

**4. Switch:**
1. Scale kdi to 0 and wait until its pod is gone and its txns have been stable for one `allowed_latency`.
2. Record kdi's final txns with `plan`.
3. Start ddi with kdi's `app_id`, `group_id = ddi-<name>` (or kdi's), the bronze target, explicit `kdi_compat`, and the translated settings.
4. Verify:
   - the first commit's txns are ≥ the recorded ones;
   - the continuity SQL around the boundary on `(kafka_partition, kafka_offset)`;
   - lag reaches 0, `fenced_total = 0`, no blocked partitions, DQ quiet, and downstream silver pipelines unaffected (same table, appends only).

Optional zero-pause variant: start ddi in kdi's group first (both eager, `range,roundrobin`), then stop kdi. Expect conflicts on the moving partitions during the handover. Use it only after the nightly `kdi_and_ddi_race` is green.

**5. Rollback.** Stop ddi and start kdi with the same flags. kdi reads the same txn convention (a null `lastUpdated` is fine) and resumes at stored.
- Precondition, rehearsed by `kdi_ddi_alternate`: delta-rs 0.26 reads 0.32 checkpoints.
- If it cannot, rollback needs ddi's newest checkpoint and `_last_checkpoint` removed, which is risky. Prefer to fix forward, or delay the first ddi checkpoint and the property change until confidence is high.
- Rows quarantined in DQ stay there; kdi would have dropped them. ddi never writes a txn of −1, so kdi cannot misread one as `End`.

**6. Runbook entries:**
- **RESTORE of bronze:** stop ddi, then `set-offsets --allow-replay` per partition to the restored version's `ddi.kafka.offsets`, then start.
- **DELETE** (for example GDPR): no action.
- **Blocked partition:** read the reason gauge or `ddi status`, fix, then `set-offsets` with an acknowledgment if data is gone.
- **Fencing alert:** look for a second deployment or a kdi still running.

**7. Retire** after every topic has switched and about 2 weeks of soak: archive the fork and close PR #226.

---

## 11. Appendix: references checked

**Earlier pass:**
- **ddi (staged tree):**
  - pipeline.rs:172-219, 223, 549-556, 566-923, 982-991, 1134, 1293-1307, 1382-1403, 1521
  - sink.rs:104-159, 169-212, 325-340; dq.rs:20-51, 69-79, 86-125, 191-290, 300-361; schema.rs:55-133, 290-310
  - transform/mod.rs:36-125; transform/sql.rs:167-260, 392-440; transform/decimal.rs:297-311; transform/dialect.rs:36-91; transform/validate.rs:82-121, 279, 348, 417-520
  - config.rs:341-353, 513-585, 608-640, 908-920, 1372, ~1844; locate.rs:114-145
  - main.rs:140, 286-341, 584-605, 729-738, 766-854, 887-1054; budget.rs:129-138, 270-296
  - Cargo.toml; docker/Dockerfile:1-46; deny.toml; ci.yml:15-28, 108-185; pipelines.example.toml:90-152; tests/common/mod.rs:22-276; tests/kill_mid_batch.rs:30-60
- **kdi:** lib.rs:866-871, 982, 1043-1118, 1164-1186, 1252-1295, 1297-1341; delta_helpers.rs:15-80; coercions.rs:60-145; serialization.rs:86-112, 246-277; transforms.rs:216-260, 336-372, 436-475; main.rs:360-470; Cargo.toml; Cargo.lock (untracked).
- **schema_registry_converter-4.10.0:** async_impl/schema_registry.rs:272-277, 765-901.
- **deltalake-core-0.32.4:**
  - kernel/transaction/conflict_checker.rs:125-160, 250-267, 450-470, 585-606
  - kernel/transaction/mod.rs:120, 476-478, 545-570, 820-967, 975-1101; kernel/models/actions.rs:1040-1064
  - kernel/snapshot/mod.rs:766-784, 1143-1150; table/state.rs:144-153
  - operations/mod.rs:199-201; operations/write/mod.rs:364-392, 400-415, 595-630
  - protocol/checkpoints.rs:95-175; table/config.rs:105-115, 148, 250-266; logstore/mod.rs:373, 690, 799
- **buoyant_kernel-0.22.2:** snapshot/mod.rs:880-941; engine/arrow_conversion/mod.rs:37-40, 268-287.
- **rdkafka-0.39.0:** consumer/base_consumer.rs:55-215, 441-780; consumer/stream_consumer.rs:100-140, 280-320, 394-552; client.rs:275, 300-340; statistics.rs:250-282.
- **rdkafka-sys-4.10.0+2.12.1:** build.rs:40-80, 103-170, 247-306; Cargo.toml features 38-72; src/types.rs:249; librdkafka/CONFIGURATION.md:6, 39, 128-154, 203; rdkafka.h:3799-3866.
- **arrow-avro-58.4.0:** reader/mod.rs:600-881, 982-984, 1153-1196, 1270-1320; reader/record.rs:48, 114-171; schema.rs:40, 861-884; codec.rs:716-738, 870-947, 1655-1683.
- **arrow-cast-58.4.0:** cast/mod.rs:190-235, 1183, 2223-2290.
- **datafusion-functions-53.1.0:** to_char.rs:116-127; to_timestamp.rs:508, 584; named_struct.rs:86; getfield.rs:250; union_extract.rs:77-117.
- **sqlparser-0.61.0:** dialect/generic.rs:92, 232.

**This revision:** listed at the top of the document.

**From the critiques, not re-read by me:**
- rdkafka.h:4533-4538 and rdkafka_partition.c:1791-1796, 1892-1896 (`position()` is INVALID before consumption and after a seek);
- reader/record.rs:2314-2319 (builder capacity);
- datafusion-sql select.rs:847 (REPLACE planning);
- kdi git state: the untracked Cargo.lock, upstream a953af6 and 6c37ffa, and PR #221 unmerged.

**Not verified, flagged in the text:**
- **M-1:** the converter version and `?deleted=true` in production; decimal byte lengths of real producers.
- **M2:** hostile-length Avro inputs, nested NOT NULL fill, local-timestamp cast.
- **M3:** the snapshot file-count accessor; exposure of delta-rs's write metrics.
- **M4:**
  - `* REPLACE`/`* EXCEPT` and quoted multi-level access through ddi's validator and planner;
  - Redpanda SASL flags;
  - string→timestamp parity.
- **Nightly / M6:** delta-rs 0.26 reading 0.32 checkpoints, and kdi 0.26's txn fencing.
- **Cutover:** Trino `ALTER … SET PROPERTIES checkpoint_interval`.

---

## 12. Critique disposition

**Exactly-once critique (EO)**

| Id | Point | Disposition | Where |
|---|---|---|---|
| EO-M1 | Replicas conflict on the shared DQ appId, and each conflict leaves the group | **Accept, different fix.** Per-partition DQ txns holding the last offset covered, a per-offset skip for deterministic coordinate-carrying rejects, and in-place retry on DQ conflicts. Equality on a fingerprint was not used, because it rarely matches on replay (EO-m9). | §5.7, §1.2 |
| EO-M2 | Dropping buffers on revoke plus leaving the group on any error can starve the group | **Accept (a) and (b); reject (c).** Error classes keep members in the group; deferred revoke with a time-boxed flush. (c), keeping buffers across revoke/reassign, is moot once revokes flush. | §3.3, §3.4, §3.6 |
| EO-M3 | `ddi once` never finishes, because `position()` is INVALID | **Accept.** Done when `max(next_expected, start) ≥ end` (LSO) or after 10 s idle; manual assignment. | §3.8 |
| EO-M4 | Missing-txn policy can skip silently (a) or duplicate in bulk (b) | **Accept the problem, partly different fix.** Exactly offset 0, blocked if `low > 0`; kdi-era tables with a retention-property history need an acknowledgment. `partitionCount` bookkeeping is unnecessary under that rule; topic id replaced by the re-read mark check (rdkafka 0.39 has no topic id without `unsafe` FFI). | §4.4, §4.6 |
| EO-M5 | Amplification ratchets upward and shrinks batches | **Accept.** Kafka never observes `Amplification`; it closes on measured decoded bytes (slice size). | §3.3, §3.7 |
| EO-M6 | Txn lookups fetch every commit since the checkpoint | **Accept (verified).** Backward walker plus the checkpoint's `txn` column, in M0, not optional. | §4.3, §0.3.17 |
| EO-m1 | kdi re-asserts every assigned partition | **Accept (verified).** Text and race expectations fixed. | §0.3.13, §4.5, §9.4 |
| EO-m2 | Seek after assign unverified; `known` can be too high | **Accept (verified in librdkafka).** Deferred assign removes both. | §3.4, §0.3.14 |
| EO-m3 | `BorrowedMessage` held across an await | **Accept.** Copied right after `recv`. | §3.3 |
| EO-m4 | Mid-fetch -140 checked against base+1 | **Accept.** Uses `next_expected` vs low; commits the buffer first; blocks only the offender. | §3.6 |
| EO-m5 | Rebalance errors other than assign/revoke | **Accept.** Treated as lost, plus `assign(NULL)`. | §3.4 |
| EO-m6 | Fatal client errors loop forever | **Accept.** Consumer-fatal class. | §3.6 |
| EO-m7 | `block_in_place`, `spawn_blocking` in Drop, exit hang | **Accept.** `spawn_blocking` on an `Arc`'d consumer; close on a detached thread. | §3.9 |
| EO-m8 | Idle handle ages past log retention | **Accept.** `handle_max_age` refresh; a missing file in the conflict check is Replan. | §4.3 |
| EO-m9 | DQ fingerprint rarely matches on replay | **Accept.** Solved by per-offset marks rather than first-batch alignment. | §5.7 |
| EO-m10 | Replan conflicts drop uncommitted work | **Accept.** The batch is retained when bases are unchanged. | §3.3 |
| EO-m11 | Full reopen on every assign | **Accept.** Incremental refresh; full open for the supervisor and failed refreshes; `require_files` stays on. | §3.4, §7.2 |
| EO-m12 | Unmapped partition column writes nulls silently | **Accept.** Refused at open (or blocking at first batch); golden and M3 tests. | §5.6 step 0, §1.3 |
| EO-m13 | Baseline and once-mode use the HWM, not the LSO | **Reject (verified):** `fetch_watermarks` already returns the LSO under read_committed. A test keeps it pinned. | §0.3.15, §9.2 #28 |
| EO-m14 | Mid-run union conflicts unspecified | **Accept.** Newest type wins plus casts; typed blocking error when SQL no longer plans; any seen id is unioned in. | §5.5 |
| EO-m15 | Typo guard blocks a second pipeline on a shared table | **Accept.** Exemption for other configured pipelines' appIds. | §4.4 |
| EO-m16 | Shared `group_id` across pipelines | **Accept.** Rejected. | §2.5.6 |
| EO-m17 | RESTORE or DELETE on bronze | **Accept partly.** RESTORE gets a runbook entry; DELETE is intentional, no action. | §4.7, §10.6 |
| EO-m18 | Incomplete explicit maps | **Accept.** Must be complete or have `"*"`. | §2.5, §4.4 |
| EO-m19 | Protocol upgrade by Trino loops the supervisor | **Accept.** `TargetProtocolUnsupported`, blocking, with a gauge. | §3.6 |
| EO-gap | Missing tests | **Accept.** | §9 |
| EO-gap | `ddi once` joins the daemon's group | **Accept.** Manual assignment. | §3.8 |
| EO-gap | No per-pipeline isolation of transient errors | **Accept.** Same fix as EO-M2a. | §3.6 |
| EO-gap | Snapshot and librdkafka memory not in the budget | **Accept partly.** librdkafka reserved per pipeline; the snapshot is treated as Delta targets are, sized from `plan`. | §3.7 |

**Parity/ops critique (PO)**

| Id | Point | Disposition | Where |
|---|---|---|---|
| PO-B1 | kdi writes `"null"`, not NULL | **Accept (verified).** `kdi_null_text` with presence by writer schema id. | §5.6, §0.3.12 |
| PO-B2 | Global app_id check and `group_id = app_id` break multi-table takeover | **Accept.** `(target, app_id)` uniqueness; `ddi-<name>` default; shared groups rejected. | §2.5 |
| PO-M1 | The parity reference is not the production kdi | **Accept.** M-1 records image digests and crate versions; fixture from the production image; dev-dep pinned to it. | §13 M-1, §9.3 |
| PO-M2 | Name-based rendering breaks on aliases | **Accept.** Rendered by Arrow type at every depth; nulls by presence. | §5.6 |
| PO-M3 | The `ingest_date` example cannot be what kdi did | **Accept.** Removed; taken from the inventory; partition-value equality checked. | §2.1, §6.3, §9.3, §10.3 |
| PO-M4 | Inventory comes too late | **Accept.** It is milestone M-1. | §13 |
| PO-M5 | The stall alert goes blind for Kafka | **Accept.** Progress rule plus alert rules. | §3.9, §7.1 |
| PO-M6 | All members share one DQ appId | **Accept.** Same fix as EO-M1. | §5.7 |
| PO-M7 | The union `U` blocks every pipeline with a transform | **Accept.** `U` = latest ∪ seen; casts and DQ instead of refusing. | §5.5 |
| PO-M8 | Estimates are low | **Accept.** About 38 days (33-43). | §13 |
| PO-1 | kdi's failure modes misdescribed | **Accept.** Corrected; offset columns made part of the plan (default yes). | §6.4, §10.1, Q3 |
| PO-2 | Decimal byte length; double→DECIMAL | **Accept.** Minimal or fixed, verified against the corpus; shortest-text conversion. | §5.6 |
| PO-3 | `-t` overwrite semantics, dotted access, quoting | **Accept.** `* REPLACE (coalesce(..))`, dotted access, quoted aliases; validator check in M4. | §6.3 |
| PO-4 | Metric names, durations, statsd prefix | **Accept.** | §7.1 |
| PO-5 | `kdi_compat` default true | **Accept, with a guard.** False by default; unset on a kdi table refuses to start. | §4.4 step 5 |
| PO-6 | `kafka_*` collision makes everything undecodable | **Accept partly.** Configurable prefix, a single blocking error, reported by `plan`. "Payload wins" rejected: it would corrupt the watermark's columns. | §6.1 |
| PO-7 | Shadow created with Trino CTAS | **Accept.** `ddi kafka clone-table`. | §4.8, §10.2 |
| PO-8 | Two offset conventions | **Accept.** "Next offset to read" everywhere user-facing. | §4.8 |
| PO-9 | Compacted-topic peek; historical null `kafka_partition` | **Accept.** Backward scan; runbook note on dedup.rs:705-716. | §4.8, §10.1 |
| PO-10 | Milestone shippability | **Accept.** `dead_code` allowance until M3; `DDI_FEATURES` empty until M3; fixture before M2. | §8, §13 |
| PO-build | Build facts already settled | **Accept.** Recorded; M1 verify items become regression checks; no `cargo update`. | §0.3.21, §8 |

---

## 13. (a) Milestone plan

Each milestone can be reviewed and merged on its own. Days are dev-days at ddi's test and documentation density. M0 and M1 can run in parallel, then M2 alongside M3's scaffolding; at most two agents at once, `-j 2`.

| # | Increment | Acceptance | Days |
|---|---|---|---|
| **M-1** | **Inventory and evidence.** Per kdi deployment: topic, table, `-a`, `-g`, every `-t`, `-K`, `-l/-m/-b`, `-c`, `-o`, statsd endpoint, replicas, **image digest**, and the crate versions read from the binary (`strings` for apache-avro / schema_registry_converter / rdkafka). Per bronze table: DDL, properties (including txn-retention history), partitioning, a `= 'null'` count per STRING column, and `plan`-equivalent txn/watermark readings (by script, read-only). Per subject: versions, compatibility, references, decimal encodings. Group ACL prefixes, topic retention and compaction. Then the **production-kdi fixture** (§9.3, steps 1-4). | Inventory sheet complete for every topic; fixture tables and corpus checked in; apache-avro dev-dep version fixed; open questions answered or defaulted | 1.5 (+1-2 if the production image must be rebuilt) |
| **M0** | **No-Kafka refactor and offset core.** Golden test first (six shapes, partitioned included); `Source`/`DeltaSource`, `Batch`/`Position`, `process()` with the conform hook; `Offsets`/`KafkaOffsets`/`MessageMark`, `Sink::commit_kafka`, private `write`; `fence.rs` with `read_txns` (walk, checkpoint `txn` column, fallback) and `Fence`; `DataQuality::write_kafka`; error variants; `Pipeline` fields `pub(crate)` | Existing suite **unmodified** and green; `delta_commit_golden` identical before and after; moved-code diff reviewed; §9.1 M0 tests, including the delta-rs pin, one-GET steady state and the DQ marks; `cargo tree` unchanged | 4.5 |
| **M1** | **Config and build.** `[kafka]` config, resolution and validation (always compiled); `ResolvedPipeline.kafka` (+5 mechanical literals); the `kafka` feature with rdkafka and arrow-avro, no `cargo update`; `ddi validate`/`--version`; Docker `DDI_FEATURES` (default empty); CI `check-kafka`; deny `[graph]`; notices appendix; budget reservation | Config unit tests; feature-off rejection (I16); `cargo build --features kafka` on the laptop and in Docker; `builtin.features ⊇ ssl,sasl_scram,zstd`; zstd-sys still 2.0.16; image `--version` shows librdkafka 2.12.1; cargo-deny green | 2.5 |
| **M2** | **Pure decode.** SR client; framing; per-id decoders; strict check; isolation; normalisation; metadata columns with prefix; measured sizes; `U` (latest ∪ seen, newest wins, casts, widening); conform (by name, containers, unions, float→decimal); `kdi_compat` by type; `kdi_null_text` by presence; parity matrices against the production apache-avro | §9.1 registry, decode and conform tests green, including both parity matrices on the M-1 corpus; arrow-avro go/no-go recorded (fallback: `trait Decode` with apache-avro) | 8 |
| **M3** | **`KafkaSource`.** Client config; `DdiContext` with deferred events; `assign_with_delta_offsets`, `flush_then_unassign`; partition state with re-read verification; poll and `step_kafka`/`commit_batch` with error classes, held batch, pauses and blocked rechecks; `reset_in_place` (incremental); start resolution, baseline, typo guard with exemption, kdi-era checks, `kdi_compat` refusal; once-mode manual assignment and done rule; handle aging; `Drop` on a detached thread; main.rs wiring (`Waiting` progress rule, shutdown flush); metrics; `status`/`validate`; `raw` format; dead-code allowance removed; image default `kafka` | Integration tests 1-25 and 27-33 green in `kafka-integration`; §9.1 M3 unit tests; a 2-hour soak on the laptop with 3 members and a churn script shows no duplicates and continuous commits | 12 |
| **M4** | **kdi compatibility and tooling.** SCRAM/SSL env compat and the SASL job (test 26); `ddi kafka plan` (history scan, NO_TXN evidence, mapping via backward peek, `kdi_compat` advice, collisions); `ddi kafka clone-table`; `kdi_takeover_fixture`; translation checks (`* REPLACE`, quoted multi-level access, `to_char`, string→timestamp); cleanup gating | Test 26 and the takeover test green; `plan` golden output on the fixture; the clone keeps column case and has no column mapping | 5 |
| **M5** | **Operations.** `ddi kafka set-offsets` (`--next`, acknowledgments, DQ marks, `--all-partitions`); docs (README Kafka section, USING_DDI, `pipelines.example.toml`, metrics reference, runbook, docs/alerts.yml); optional statsd shim (+1 d) only if open question 4 says so | `set-offsets` guard tests (forward needs an ack, backward needs `--allow-replay`, DQ marks move, a running owner picks it up); doc review | 3 |
| **M6** | **Pilot cutover.** Comparison SQL recipes; runbook executed on one low-traffic topic (shadow → compare → switch); rollback rehearsal | The §10.3 gates pass on the pilot and stay green for 3 days after the switch; `kdi_ddi_alternate` green | 1.5 + soak |

**Total: about 38 dev-days** (range 33-43), plus soak and roughly 0.25 day per further table. This is above the critique's 30-35 because their findings added scope: deferred revoke flush, stay-in-group error handling, the txn walker in M0, null-text presence and per-partition DQ marks.

---

## 14. (b) Open questions for you, with the default I would pick

1. **Where do the kdi deployments live** (manifests, image digests, replica counts)? *Default:* M-1 reads them from whatever deployment repo or cluster you point me to. Until then: one replica per topic, kdi defaults for unset flags.
2. **On taken-over tables, keep kdi's STRING quirks** (`"null"` text, raw-number timestamps, byte-array decimals, sorted-key JSON)? *Default:* yes (`kdi_compat = kdi_null_text = true`) on every table where M-1's `= 'null'` counts show them; false on new tables. Downstream SQL keeps working unchanged.
3. **May bronze gain `kafka_partition INT`, `kafka_offset BIGINT`, `kafka_topic VARCHAR` before the shadow run** (one kdi restart with three extra `-t`)? *Default:* yes. The offset-exact comparison and the downstream partition watermark need them.
4. **Does anything consume kdi's statsd metrics** (dashboards, alerts, which backend)? *Default:* no. Prometheus only, plus the shipped alert rules.
5. **Undecodable messages:** quarantine to `<bronze>__ddi_dq`, and pause the pipeline when that table is missing (kdi silently dropped them)? Skip tombstones? *Default:* yes and yes; the DQ tables are created at cutover.
6. **Retention gaps and trimmed, never-read partitions:** pause the partition until someone runs an acknowledged `set-offsets`? *Default:* yes. Nothing is ever skipped silently.
7. **Consumer groups:** may ddi use new groups `ddi-<name>`, which need a Read ACL? *Default:* yes. If ACLs are exact names, set `group_id` to kdi's group per pipeline.
8. **Topology and image:** publish the ghcr image with `kafka` from M3 on, and run the Kafka pipelines as their own deployment, separate from the Delta→Delta pipelines? *Default:* yes and yes. `dat_kafka_connect` pulls the image rather than building from source.
9. **Timestamp-millis fields that kdi wrote into TIMESTAMP columns 1000× too small:** write correct instants from cutover on (a visible break), without rewriting history? *Default:* yes. `plan` lists the affected columns, and silver SQL that compensated is adjusted at cutover.
10. **Schema Registry:** Confluent with TopicNameStrategy, no schema references, a public CA? *Default:* yes. A private CA goes in `ca_pem_env`; references block the pipeline with a clear error until supported.
11. **Commit group offsets to Kafka for lag tooling?** *Default:* no (kdi never did); ddi exports lag itself.
12. **Fill the holes kdi left** (messages it silently dropped, for example during SR outages)? *Default:* no. The shadow comparison lists them; filling them is a separate, manual task.
