# delta-delta-ingest (`ddi`)

**Exactly-once, append-only, stateless Delta→Delta streaming. No checkpoint directory, no
changelog reconciliation, no cluster. Restart is a version number read from the target's own
transaction log.**

`kafka-delta-ingest` solved Kafka → Delta as a cheap Rust daemon with no cluster. This is the
same idea for Delta → Delta.

## The gap it fills

| Tool | Delta source | Delta sink | Semantics | Weight |
|---|---|---|---|---|
| Spark Structured Streaming | yes | yes | exactly-once | cluster |
| Feldera | yes (follow/cdc) | yes, **as changelog** | **at-least-once** | DBSP platform, pods |
| Arroyo | no | yes | checkpointed | distributed engine |
| Sail (LakeSail) | yes (batch) | yes | streaming partial | Spark-compat engine |
| delta-rs | batch only | batch only | n/a | library |
| **`ddi`** | **yes, resumable** | **yes, as a real table** | **exactly-once** | **one binary** |

Feldera is the closest overlap and still misses on two axes: its Delta input is at-least-once,
and its Delta output is a change log with operation-metadata columns that you reconcile into a
real table with a periodic Spark MERGE. Putting Spark back in the path is exactly what this
removes.

## How exactly-once works

Delta's idempotent-write protocol. A `txn` action carries `(appId, version)`. `ddi` commits
`txn(app_id, last_source_version)` **in the same Delta commit** as the data derived from it.
A Delta commit is atomic, so data and offset advance together or not at all.

On restart it reads the last committed version for its `app_id` straight out of the target
and resumes from the next one. There is no side-car state, no checkpoint directory, no
external store — and the target stays a plain Delta table that any engine can read without
knowing this daemon exists.

This is proven, not asserted: [`tests/kill_mid_batch.rs`](tests/kill_mid_batch.rs) SIGKILLs
the running binary at a spread of points inside the commit cycle, restarts it, and asserts
after **every** kill that the target holds a gap-free prefix of the source with no duplicates.

## Quick start

```bash
cargo build --release

cat > pipelines.toml <<'TOML'
[[pipeline]]
name       = "orders_header"
app_id     = "ddi.orders_header"     # the offset key — unique and stable, forever
source_uri = "/lake/bronze/orders"
target_uri = "/lake/silver/orders"
transform_sql = """
SELECT order_id,
       CAST(created_at AS TIMESTAMP)  AS created_at,
       customer.id                    AS customer_id,
       array_length(line_items)       AS line_count
FROM source
"""
TOML

ddi validate            # parse + reject stateful SQL, touch no storage
ddi status              # where each pipeline would resume from
ddi once                # run until caught up, then exit
ddi run                 # run continuously
```

`ddi` never creates the target table — create it with whatever tooling owns your lakehouse.

### Building on a small machine

Linking `deltalake` + `datafusion` + `arrow` into every test binary is what makes this crate
expensive to build, and dependency *debug info* is the bulk of it. The `dev` profile
therefore keeps line tables for this crate and drops debug info for dependencies entirely,
which takes `target/debug` from ~26 GB to ~4 GB and each binary from ~1.3 GB to ~350 MB. A
clean `cargo build --all-targets -j 2` peaks under 2 GB RSS.

If the release link is still too much — `lto = "thin"` with `codegen-units = 1` is one large
single-threaded step — build the lean profile instead, trading some runtime performance for
a build that fits:

```bash
cargo build --profile release-lean     # binary at target/release-lean/ddi
```

## Scope, and why it is narrow

The property that makes this worth existing is *restart from a version number with no state
directory*. Every accepted feature preserves it.

| Operation | Cross-row state | Output append-only | Status |
|---|---|---|---|
| cast / rename / filter | no | yes | **supported** |
| unnest / explode | no | yes | **supported** |
| intra-row array agg | no | yes | **supported** (`array_sum` etc.) |
| lookup join vs. pinned snapshot | no | yes | v2 |
| `GROUP BY` aggregation | **yes** | **no** | **rejected — different product** |

`GROUP BY` is rejected at config-load time, not at runtime, and the error names the
alternative:

```
GROUP BY is not supported: this tool preserves grain: a group that spans batches would
emit partial results, and the output would stop being append-only. Instead: use
array_sum(line_items, 'price * qty') for intra-row aggregation, or aggregate downstream.
```

It is rejected even when a group provably cannot span a batch. Accepting `GROUP BY order_id`
invites `GROUP BY customer_id`, which silently emits partial sums. Safe by construction beats
safe by convention.

Aggregation would force upsert output (killing the ability to cascade this tool into itself),
make exactly-once depend on merge-commit atomicity, and drag in watermarks the moment anyone
asks for windows. If it is ever wanted it is a separate binary with separate guarantees — not
a flag.

### Intra-row aggregation

Row-local by construction, so batch boundaries cannot affect them:

```sql
SELECT order_id,
       array_sum(line_items, 'price * qty') AS order_total,
       array_length(line_items)             AS line_count,
       array_max(line_items, 'price')       AS most_expensive_line
FROM source
```

Also `array_min` and `array_avg`. For real Rust, implement the `Transform` trait — an escape
hatch that does not require forking.

## Commit classification

| Commit contains | Classification | Action |
|---|---|---|
| `Add` with `dataChange: true` | data commit | emit |
| `Add`/`Remove` with `dataChange: false` | compaction (`OPTIMIZE`) | **skip silently** |
| `Remove` with `dataChange: true` | delete / update / merge | per `change_policy` |
| `txn` only, or empty | marker / no-op | skip, advance offset |

The `dataChange: false` rule is the one everybody gets wrong. Without it, every `OPTIMIZE` on
the source replays the whole table downstream.

`change_policy` mirrors Spark's Delta source options:

- `fail` (default) — error on any `dataChange` `Remove`.
- `skip_change_commits` — skip those commits entirely.
- `ignore_changes` — emit their `Add`s; rewritten rows are duplicated downstream.

## Fan-out

One source, many targets is the common shape for order data:

```toml
[[pipeline]]
name = "orders_header"
app_id = "ddi.orders_header"
source_uri = "/lake/bronze/orders"
target_uri = "/lake/silver/orders"

[[pipeline]]
name = "orders_lines"
app_id = "ddi.orders_lines"
source_uri = "/lake/bronze/orders"
target_uri = "/lake/silver/order_lines"
transform_sql = """
SELECT order_id, li.sku, li.qty, CAST(li.price AS DECIMAL(18,4)) AS price
FROM (SELECT order_id, unnest(line_items) AS li FROM source)
"""
```

Each target carries its own `app_id` and resumes independently. They are **individually
exactly-once but not mutually atomic** — two tables cannot share one Delta commit.

`app_id` uniqueness is validated at startup: duplicates silently corrupt offsets, so they are
a hard error.

## Concurrency

One tokio task per pipeline. No coordination between pipelines, none between processes.
Masterless, like KDI.

Two processes running the *same* `app_id` against the same target is a config error, not a
supported mode. Delta's optimistic concurrency makes one fail its commit and retry, so
correctness still holds (the `txn` action prevents double-apply) — it just wastes work.

## Non-goals (design principles, not apologies)

- **Target table creation.** External tooling, same as KDI.
- **Schema evolution.** Read the target schema, cast, fail on mismatch.
- **Aggregations, stream-stream joins, windows, watermarks.**
- **Dead-letter queue.** Input is typed Parquet, not arbitrary JSON. A cast failure is a
  pipeline failure and stops the world.
- **Deduplication / restatement.** If Kafka emits order v1 then a corrected v2, append-only
  silver holds both. Dedup-to-latest is genuinely stateful — leave it to a downstream MERGE.
- **Deletion propagation.**
- **Deletion vectors** in the source: explicit unsupported error, never a silent wrong result.

## Operational notes

- **Decimals, not doubles.** If bronze declares prices as `double`, precision was already
  destroyed before `ddi` saw the data. Use `decimal(18,4)` at bronze. This is the most common
  silent data-quality failure in order pipelines.
- **Unnest amplification.** A row with a 10k-element array becomes 10k rows. Batches are
  bounded on input bytes *and* `max_output_rows_per_batch`; do not let a 64 MB source file
  become 6 GB of RAM.
- **Empty arrays.** `array_length` returns 0 for an empty array and NULL for a NULL array. It
  never drops the row — that is `explode` semantics, not this. `array_sum` of an empty array
  is NULL, not 0, because emitting 0 would invent data.
- **Target file sizing.** Buffer to `target_file_size` before committing; `allowed_latency`
  is the ceiling.
- **Direct Lake.** If targets feed Power BI over OneLake, verify decimal precision is in the
  supported range — unsupported types silently fall back to DirectQuery, surfacing as a
  mysteriously slow report rather than an error.
- **Star-schema fan trap.** With header + line-item fan-out, denormalised header columns on
  line rows make `SUM` multiply by line count. The tool does not cause it; the design permits
  it.

### Metrics

`ddi --metrics-addr 127.0.0.1:9100` (or `DDI_METRICS_ADDR`) serves Prometheus text on
`/metrics`. Omit the flag and no socket is opened. Every series is labelled `pipeline`.

| Metric | Type | Meaning |
|---|---|---|
| `ddi_batches_committed_total` | counter | Batches committed. |
| `ddi_rows_written_total` | counter | Rows written to targets. |
| `ddi_files_read_total` | counter | Source data files read. |
| `ddi_errors_total` | counter | Step errors. A step error stops the pipeline. |
| `ddi_commits_skipped_total` | counter | Source commits consumed that produced no rows. |
| `ddi_last_source_version` | gauge | Last source version **durably committed** (the `txn` value). |
| `ddi_source_head_version` | gauge | Source head at the last poll. |
| `ddi_source_lag_versions` | gauge | Source commits not yet consumed. |

Lag is measured from the **cursor**, not from `ddi_last_source_version`. The two differ
whenever commits are consumed without producing a commit of our own: a run of `OPTIMIZE` on
the source advances the cursor but writes no `txn` action, so the durable offset legitimately
sits behind the head while the pipeline is fully drained. Subtracting the offset from the
head would page an operator every time bronze compacts. `ddi_last_source_version` is still
the number to look at when reasoning about *restart* behaviour — it is what a restart resumes
from.

Alert on `ddi_source_lag_versions` for backlog and on `increase(ddi_errors_total[5m])` for a
stopped pipeline. There is no dead-letter queue by design, so a non-zero error count means a
pipeline has stopped and needs a human.

## v1 limits

- **A source commit is never split.** The offset is a bare version number, which is what the
  `txn` action stores natively. An oversized commit fails loudly and names the fix rather than
  splitting silently. (`LogStreamBuilder::with_commit_splitting(true)` exists for the
  low-level API and is not used by the daemon.) Bounding source commit size upstream — e.g.
  KDI's `allowed_latency` — makes this a non-issue.
- Mid-stream schema changes surface as a batch whose schema differs from the previous one;
  the target schema is the contract and a mismatch is an error.

## Layout

```
src/
├── source/          # Phase 1: resumable log-diff streaming source
│   ├── cursor.rs    #   StreamCursor — (version, index), totally ordered
│   └── log_stream.rs#   LogStreamBuilder, commit classification
├── offset.rs        # resume point via the target's txn action
├── sink.rs          # data + txn action in ONE atomic commit
├── transform/       # DataFusion SQL, validation, intra-row UDFs
├── schema.rs        # target-schema cast, hard-fail on mismatch
├── pipeline.rs      # the step loop
└── metrics.rs       # prometheus
```

`src/source/` is written against the API an upstream delta-rs contribution would expose
(delta-io/delta-rs#4554), so swapping to it later is a dependency change rather than a
rewrite.

## Licence

Apache-2.0, matching delta-rs and kafka-delta-ingest.
