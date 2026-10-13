# Kafka pipelines: runbook

For operators of `[[kafka.pipeline]]` (the `kafka` build). The design, with every guarantee and
the reasons for it, is [kafka-design.md](kafka-design.md); alert rules are in [alerts.yml](alerts.yml).

Offsets in every command and message here are the **next offset to read**. Only the Delta `txn`
stores "last written", and it is `next - 1`.

## What a Kafka pipeline guarantees

Each batch is one Delta commit that carries the rows and one `txn` per partition, so a row is in
the table exactly when its offset is. A restart reads the offsets from the table; Kafka's consumer
group is used only to share partitions and commits nothing. A message ddi cannot decode or a row its
SQL cannot evaluate goes to `<target>__ddi_dq` with the reason, and the offset moves on. A
condition ddi cannot decide for itself (a retention gap, a topic that was recreated, a table
another engine rewrote) **blocks** the partition or the pipeline and says why, rather than guess.

## Daily commands

| | |
|---|---|
| `ddi status` | per partition: stored offset, watermarks, lag, resume verdict |
| `ddi kafka plan --select NAME` | the preflight below; exit status is non-zero when something blocks |
| `ddi kafka set-offsets --select NAME ...` | move offsets, see "Moving offsets" |
| `ddi kafka clone-table --select NAME --to URI` | an empty copy of the target for a shadow run |
| `ddi kafka create-dq --select NAME` | create the data-quality table where it does not exist |
| `ddi once` | read every topic to its end without joining the group, then exit |

None of them joins the consumer group, so a running pipeline is not disturbed.

## Before anything: `plan`

`ddi kafka plan` prints the target's protocol, partition columns and properties, **whose offsets
are in it** (fresh, kafka-delta-ingest's, ddi's), what the takeover check says, every partition
(stored offset, resume position, watermarks, lag, verdict), the app_ids seen in the newest commits,
and a ready `starting_offsets = { ... }` (each stored offset + 1). Read the BLOCKING list first.

| verdict | meaning | what to do |
|---|---|---|
| `OK` | resumes at stored + 1 | nothing |
| `RETENTION_GAP` | the topic trimmed past the stored offset | messages are gone for this table. `set-offsets --next <low> --acknowledge-skip "<why>"` |
| `REGRESSED` | the stored offset is past the topic's end: the topic was recreated, or this is the wrong cluster | check the cluster; if the topic was recreated on purpose, `set-offsets --next <wanted> --allow-replay` |
| `NO_TXN->offset 0` | no offset and the topic starts at 0 | nothing: it is read from 0 |
| `NO_TXN->blocked` | no offset and the topic was trimmed: messages ddi never read are gone | `set-offsets --next <low> --acknowledge-skip "<why>"` |
| `NO_TXN->ack needed` | takes over a table another writer filled, and this partition has messages but no offset | decide per partition: an entry `0` re-reads what the other writer set aside (only if it never wrote a row of it), the next offset after what it wrote otherwise. Put it in `starting_offsets`. |

## Moving offsets

`ddi kafka set-offsets --select NAME --partition P --next O` (or `--all-partitions --next O`).

- **Forward over messages the topic still holds** skips them for good: say why with
  `--acknowledge-skip "<reason>"`. The reason is recorded in the commit (`ddi.kafka.offsetOverride`).
  Forward over messages the topic has already trimmed needs no reason.
- **Backward** reads messages again, so their rows are written twice: `--allow-replay`. ddi then writes
  a checkpoint at the commit. Do this **with every other writer of the table stopped** (Trino's
  INSERT/OPTIMIZE/MERGE included): Trino's own checkpoint keeps the highest offset per app_id, and one
  computed from an older base would silently undo the move. The command re-reads the moved
  partitions after its checkpoint and fails, naming them, if any is not where you put it; stop the
  other writer and run it again.
- **`--next` at the current next offset**, and **`--reassert`** (every partition again at its own
  offset, with ddi's stamp), are neither forward nor back and need no flag. Run `--reassert` once
  after kafka-delta-ingest has stopped and before `delta.setTransactionRetentionDuration` is ever
  set on the table (see "Taking over from kafka-delta-ingest").
- Refused on a fresh app_id (use `starting_offsets`), past the topic's end, below its first offset
  when moving backward, and `--next 0` where an offset exists (a `txn` cannot be removed: use a new
  app_id with `starting_offsets`).
- A running owner conflicts on its next commit or notices at its next blocked-partition re-check,
  then reads the new value. Nothing is written to the DQ table.

## Taking over from kafka-delta-ingest (per topic)

Do one low-traffic topic first, then the rest. The pipeline must use kdi's **app_id** (that is the
whole mechanism: kdi stores `txn{ "<app_id>-<partition>" = last offset }`) and its target table.

1. **Prepare, no behaviour change.**
   - `ddi kafka create-dq --select NAME` creates `<target>__ddi_dq`. Without it a pipeline pauses on the
     first message it cannot decode, rather than discard it.
   - Decide `kdi_compat` for the table: `true` where anything reads kdi's STRING rendering (the text
     `null`, raw-number timestamps, a nested record as sorted-key JSON with decimals as byte arrays).
     `plan` prints a count query of `= 'null'` rows per STRING column.
   - Make sure the image is built `--features kafka` (the published image is).
   - Check the consumer-group ACL: a new group `ddi-<name>` needs Read on it; or set `group_id` to kdi's
     own group (kdi commits no offsets to Kafka, so nothing is lost by reusing it, **only after kdi is stopped**).
2. **Shadow (optional but recommended, at least 24 h, through a schema change if one happens).**
   `ddi kafka clone-table --select NAME --to <bronze>__ddi_shadow`; a pipeline with
   `app_id = "ddi.shadow.<topic>"`, its own `group_id`, `target_uri` = the clone,
   `starting_offsets` = the map `plan` printed. Compare per partition on `(kafka_partition, kafka_offset)`:

   ```sql
   -- rows kdi has and the shadow does not, and the reverse, over the window both cover
   SELECT 'kdi_only' AS side, * FROM (
     SELECT <cols> FROM kdi_table WHERE (kafka_partition = :p AND kafka_offset BETWEEN :start AND :end)
     EXCEPT ALL
     SELECT <cols> FROM shadow_table WHERE (kafka_partition = :p AND kafka_offset BETWEEN :start AND :end))
   UNION ALL
   SELECT 'shadow_only', * FROM (
     SELECT <cols> FROM shadow_table WHERE (kafka_partition = :p AND kafka_offset BETWEEN :start AND :end)
     EXCEPT ALL
     SELECT <cols> FROM kdi_table WHERE (kafka_partition = :p AND kafka_offset BETWEEN :start AND :end));
   ```

   Offsets only in the shadow are messages kdi silently dropped (a failed decode, a failed transform):
   expected, and listed by the `shadow_only` side. Count the shadow's DQ rows by reason and explain each.
   Ready to switch when the differences are explained, lag is steady and `ddi_kafka_fenced_total` is 0.
3. **Switch.**
   1. Scale kdi to zero; wait until its pod is gone and the table's `txn`s have not moved for one
      `allowed_latency`.
   2. `ddi kafka plan`: save it. Every partition should be `OK`; for any `NO_TXN->ack needed`, add the
      `starting_offsets` entry you decided.
   3. Start ddi on the real target with kdi's app_id and an explicit `kdi_compat`. The first commit is
      the **takeover baseline**: the marker `<app_id>.ddi`, every kdi offset again with ddi's stamp, every
      acknowledgment as an offset. From then on the table is in ddi's era; the `starting_offsets` entries
      can be removed.
   4. Check: continuity around the boundary on `(kafka_partition, kafka_offset)` (no gap, no duplicate),
      lag reaches 0, `ddi_kafka_fenced_total` is 0, no blocked partition, DQ quiet, downstream silver
      pipelines unaffected (same table, appends only).
4. **Re-assert.** `ddi kafka set-offsets --select NAME --reassert` once, so no txn keeps a stamp another
   engine could expire. Never set `delta.setTransactionRetentionDuration` on the table: ddi refuses a
   target that has it.
5. **Roll back** by stopping ddi and starting kdi with the same flags: it reads the same `txn`s and
   resumes at the stored offset. Rows ddi quarantined stay in DQ. **Going back to ddi afterwards is a
   second takeover that no check can protect**, unless the retention property stayed unset the whole
   time kdi ran again (design §10.5): the safe route is a new app_id with `starting_offsets` taken from
   kdi's final offsets (`plan`).

## Blocked pipelines (`ddi_kafka_pipeline_blocked{reason}`)

The member stays up and in its group but writes nothing until the cause is fixed; it re-checks every
`blocked_recheck_secs`. `reason` is one of the held errors: `target_replaced` (the table was dropped and
recreated under it: restore or point it at the right table), `protocol_unsupported` (another engine
upgraded the table to a writer feature delta-rs cannot write, column mapping included), `txn_retention`
(the retention property is set: unset it), `offset_vanished` (an offset it held disappeared from the
table: `set-offsets --next <last + 1> --acknowledge-skip "<why>"` from the offset the log names),
`schema`/`transform`/`evaluation` (the writer schema changed in a way the SQL or the target cannot take:
fix the SQL or the target; the messages are not lost, they wait).

## Restore or delete

- **Restoring the table to an older version** (`RESTORE`): stop ddi and every Trino job that writes the
  table. `set-offsets --allow-replay` per partition to the restored version's `ddi.kafka.offsets`. Start
  ddi, then let Trino write again.
- **Deleting rows** (a GDPR erasure): nothing to do; ddi never re-reads below a stored offset.
- **A second deployment or kdi still running** shows as `ddi_kafka_fenced_total` rising. Stop one.

## Capacity notes

- Memory: `[runtime]` budgets apply; a Kafka pipeline is one share of the budget. The kdi streams with
  the biggest payloads (megabyte-sized `data`) are the ones to try against the budget first.
- kdi's `-m` (5000) sized an in-memory batch and is not a commit trigger; do not copy it into
  `max_messages_per_batch`. Commits happen at `allowed_latency_secs` or `max_bytes_per_batch`.
- Start `max_bytes_per_batch` near kdi's `-b` (the Avro bytes are smaller than the parquet they become;
  tune from the file sizes you observe).
