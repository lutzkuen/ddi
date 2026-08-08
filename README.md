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

### See it work, locally

No cloud storage, no cluster. The example builds a small lakehouse on local disk whose
bronze table is shaped the way bronze really arrives — timestamps as strings, money as
doubles, one array of structs per row:

```bash
cargo run --example local_demo -- seed /tmp/ddi-demo
cargo run --bin ddi -- once --config /tmp/ddi-demo/pipelines.toml
cargo run --example local_demo -- show /tmp/ddi-demo
```

One bronze row fans out into a typed header row and N line-item rows:

```
BRONZE  bronze/orders
  order_id  created_at           order_status  customer              line_items
  1001      2026-01-15T10:30:00  PAID          {id: 7, country: DE}  [{sku: WIDGET-A, qty: 2, price: 10.0}, {sku: WIDGET-B, qty: 1, price: 5.5}]
  1003      2026-01-15T11:30:00  DRAFT         {id: 9, country: NL}  [{sku: WIDGET-D, qty: 1, price: 3.25}]

SILVER  silver/orders        (header grain)
  order_id  created_at           customer_id  customer_country  line_count  order_total
  1001      2026-01-15T10:30:00  7            DE                2           25.5000

SILVER  silver/order_lines   (line-item grain)
  order_id  sku       qty  price
  1001      WIDGET-A  2    10.0000
  1001      WIDGET-B  1    5.5000
```

`created_at` became a real `TIMESTAMP`, `customer_id` came out of a struct, `order_total` is
`array_sum(line_items, 'price * qty')` landed in a `DECIMAL(18,4)`, and the DRAFT order is
absent from the header target but present in the line target — the two pipelines carry
separate offsets and separate filters.

Then stream a further bronze commit through the same pipelines and watch it stay
incremental — the second run reads one new row, not all four:

```bash
cargo run --example local_demo -- append /tmp/ddi-demo
cargo run --bin ddi -- once --config /tmp/ddi-demo/pipelines.toml
```

The same end-to-end path is asserted in [`tests/end_to_end_nested.rs`](tests/end_to_end_nested.rs).

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

### Lookups against a second table

Not in v1. A transform may read exactly one table — the batch it was handed, registered as
`source` — and anything else is rejected at config load:

```
the table "products" is not supported: a transform may only read the batch it was given,
which is registered as "source". Reading a second table means joining against something
that can change between batches, which makes the output non-reproducible. Instead:
denormalise upstream, or enrich downstream; pinned-snapshot lookup joins are planned for v2.
```

This covers `JOIN`, a comma-separated `FROM` list, a second table inside a derived table,
and a second table inside a `WHERE ... IN (SELECT ...)` subquery.

The problem is not the join, it is the *pinning*. This tool's entire restart story is that a
source version number reproduces a batch exactly. Join to `products` unpinned and the same
source version yields different output depending on when it is replayed, so a restart stops
being a no-op and exactly-once quietly becomes exactly-once-modulo-the-dimension.

Until v2, three options that keep the property:

- **Denormalise upstream** — resolve the product attributes in whatever writes bronze. Best
  when the attributes are part of the event's meaning at the time it happened (the price
  actually charged).
- **Enrich downstream** — let `ddi` land silver at source grain, and join to the dimension in
  a downstream view or job. Best when you want current attributes, not historical ones.
- **Inline it** — for a genuinely static handful of values, a `CASE` expression in the
  transform is stateless and reproducible.

v2's shape is to pin the dimension at a specific version and record that version in the
commit alongside the source offset, so a replay resolves the same rows it did the first
time. The `txn` action already carries the source offset; a pinned lookup needs the same
treatment for every table it reads.

## Running alongside dbt

The two-speed lakehouse: dbt rebuilds a model nightly and owns correctness; `ddi` streams
the same transformation continuously in between and owns latency. Not every model can be
streamed, so the first question is which ones can.

```bash
ddi dbt check --manifest target/manifest.json
```

```
streamable  orders_header    bronze.orders -> silver.orders_header
streamable  order_lines      bronze.orders -> silver.order_lines
no          customer_totals  GROUP BY is not supported: this tool preserves grain ...
no          orders_enriched  depends on 2 upstream relations; ddi streams from exactly one ...
no          orders_view      materialized as "view"; ddi appends to a real table ...
no          ranked           window functions (OVER) is not supported ...

2 streamable, 4 not, of 6 model(s).
```

The verdict comes from the compiled SQL, not from tags or naming conventions: the model's
`compiled_code` is rewritten to read `source` and then put through the same validator the
daemon applies to any `transform_sql`. If `ddi` would refuse to run it, `check` says so and
why. A model qualifies when it reads exactly one upstream relation, materializes as a real
table, and transforms row by row.

`convert` turns the streamable ones into a config, listing the rest as comments so the
generated file doubles as the report:

```toml
[dbt]
manifest      = "target/manifest.json"
uri_template  = "abfss://lake@acct.dfs.core.windows.net/{schema}/{name}"
watermark_uri = "abfss://lake@acct.dfs.core.windows.net/meta/ddi_watermark"
```

```bash
ddi dbt convert --out pipelines.toml
ddi run -s orders_header        # one model
ddi run                         # all of them
```

The manifest names relations, not locations, so `uri_template` bridges the two —
`{database}`, `{schema}` and `{name}` expand per model, and a model's own `location_root`
wins where dbt sets one. That template is the only adapter-specific part; everything else
reads `manifest.json`, which is the same shape for dbt-trino, dbt-databricks and dbt-spark.

### The handover, and why it needs a watermark

`ddi` keeps its offset in a `txn` action in the target's log, and **`txn` actions survive an
overwrite** — they live in the log, not in the data. So a nightly dbt rebuild of a shared
target silently strands rows:

```
00:00  dbt reads bronze@100
00:03  ddi streams 101, 102  -> appended to silver
00:05  dbt OVERWRITE silver = f(bronze@100)   <- 101 and 102 are gone
00:06  ddi resumes at 103                     <- and never come back
```

Nothing errors, and it compounds every night. So when dbt shares a target, the dbt run must
record the source version it consumed, and `ddi` resumes from that instead of from its own
offset:

```sql
-- one row per rebuild; app_id VARCHAR, source_version BIGINT
INSERT INTO lake.meta.ddi_watermark VALUES ('ddi.orders_header', 100)
```

Plain SQL on purpose — an `INSERT` any adapter can run, rather than a `txn` action only the
Spark writer can produce.

`ddi` walks the target's log backwards on startup. If the most recent commit that touched
data is not its own, the target was rebuilt, and dbt's watermark takes over. A target
rebuilt with *no* watermark recorded is a hard error, not a guess:

```
pipeline "orders_header": target "..." was rewritten at version 41 by another writer
(a dbt rebuild), but the watermark table "..." holds no source_version for app_id
"ddi.orders_header". Resuming from this pipeline's own offset would silently drop every
row streamed while dbt was reading.
```

Prefer a **pre-hook** that records the version and a model that pins its read to it
(`FOR VERSION AS OF`). Then the watermark is on disk before the overwrite lands and there is
no window at all. With a post-hook the watermark appears one commit later; if `ddi` looks in
between it re-streams from the previous watermark, which duplicates rows rather than dropping
them. That asymmetry is deliberate — duplicates are visible and the next rebuild erases them,
whereas a gap is silent and permanent.

`OPTIMIZE` on the target is not mistaken for a rebuild: its `Remove` actions carry
`dataChange: false`.

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
