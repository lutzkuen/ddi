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
| unnest / explode | no | yes | **supported**, in Trino's spelling or DataFusion's |
| intra-row array agg | no | yes | **supported** (`array_sum` etc.) |
| upsert on a key | no (the *target* holds it) | no | **supported**, opt-in — see [Upserting](#upserting) |
| pinned Delta lookup via `LEFT JOIN` | no | yes | **supported**, declared and version-pinned per source commit |
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

Aggregation would drag in watermarks the moment anyone asks for windows, and a group that
spans batches has no correct partial answer. If it is ever wanted it is a separate binary
with separate guarantees — not a flag.

Upserting is the one thing on that list that turned out to be separable. It gives up
append-only output, and with it the ability to cascade into a downstream pipeline that is
not itself upserting — but it needs no cross-row state of its own, because the state is the
target table. Restating a key is answered by the row and the key, not by a window. So it is
a per-pipeline mode rather than a rejection, and `append` is still the default.

### Unnesting to child grain

Expanding an array to child grain is row-local — every output row comes from exactly one
input row — so it has always been supported. Write it the way your warehouse does:

```sql
-- Trino / Starburst / Athena / ANSI, and what a dbt model contains
SELECT o.order_id, li.sku, li.qty
FROM source o
CROSS JOIN UNNEST(o.line_items) AS t(li)
```

That is rewritten internally to the engine's own spelling, which you can also write directly:

```sql
SELECT order_id, li.sku, li.qty
FROM (SELECT order_id, unnest(line_items) AS li FROM source)
```

The rewrite runs **before** validation, so the two forms cannot diverge: whatever
`ddi validate` accepts is what executes. `UNNEST` of a column of the same row is not a join —
there is no second table, nothing to pin and no cross-row state — so it is not caught by the
join rejection. An arbitrary join against another table still is; the constrained pinned-lookup
exception is documented below.

#### When the array is inside a JSON blob

Bronze usually carries its payload as one JSON string, so the array does not exist as an
array until something makes it one. Trino spells that as a cast, and so does ddi:

```sql
SELECT o.order_id,
       json_extract_scalar(li, '$.sku')                 AS sku,
       CAST(json_extract_scalar(li, '$.qty') AS BIGINT) AS qty
FROM source o
CROSS JOIN UNNEST(CAST(json_extract(o.data, '$.lines') AS ARRAY(JSON))) AS t(li)
```

Each element arrives as JSON text — which is what `ARRAY(JSON)` promises — so its fields come
out with the same `json_extract_scalar` that works in the warehouse. Nothing about the
element's shape is declared, and nothing needs to be.

Arrow has no cast from text to a list, so the cast becomes `json_array_elements(...)`
internally. Two consequences worth knowing:

- A row whose path is missing, or is not an array, contributes **no rows** rather than
  failing — a NULL array expands to nothing, as in Trino. Malformed JSON still stops the
  pipeline, because the input is a typed column rather than arbitrary text.
- A JSON `null` element becomes a SQL NULL element.

Not supported, and refused at config load rather than on the first batch:

| | |
|---|---|
| `WITH ORDINALITY` | The element's position would need generating per row, and the only spelling for that is a window function. Carry the position in the elements, or add it downstream. |
| More than one `UNNEST` | Each multiplies the row count by the next, and `max_output_rows_per_batch` is calibrated for a single expansion. Chain pipelines instead. |
| Spark `LATERAL VIEW` / `explode()` | Not implemented by this engine. Write the ANSI form above. |

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

### Pinned Delta lookups

A transformation still has exactly one streaming input, registered as `source`. It may also
enrich that batch from a small, declared Delta lookup using only a direct `LEFT JOIN … ON`:

```toml
[[pipeline]]
name = "order_items"
source_uri = "abfss://lake@acct/.../order_created"
target_uri = "abfss://lake@acct/.../order_items"
transform_sql = """
SELECT o.order_id, o.currency, fx_rates.exchange_rate
FROM source AS o
LEFT JOIN fx_rates
  ON fx_rates.currency = o.currency
 AND fx_rates.starting_date = o.order_date
"""

[[pipeline.lookups]]
name = "fx_rates"                         # SQL relation name
uri = "abfss://lake@acct/.../fx_rates"
```

This is deliberately not a general second-source join. Inner/right/full/cross joins,
comma joins, lookup subqueries (`IN`, `EXISTS`, scalar subqueries), a lookup as the primary
`FROM`, and unbounded `ON true` predicates are rejected. The predicate must contain an equality
between a lookup-qualified field and a source- or source-derived-CTE field. The lookup itself
must have a uniqueness/non-overlap data contract for its key; SQL alone cannot prove that a
join is one-to-one.

Before processing a source data commit, ddi selects the newest lookup Delta version whose log
object timestamp is **strictly before** the source log object's millisecond. It records the
selected lookup version and Delta table id in the same target commit as the source `txn` offset.
The strict boundary means that a lookup commit appearing later in the same millisecond cannot
change a failed batch's retry. If raw source history predates a lookup table, a
`pre_history_version` must be explicitly approved and configured; ddi otherwise stops rather
than silently applying a future lookup snapshot. Source and lookup log objects must use a
comparable, stable object-store `last_modified` clock (normally the same lake/account), which is
the clock Delta itself uses for timestamp travel.

By default, the table id is checked at startup and again for every selected snapshot.
Dropping/recreating or relocating a lookup at the same URI is therefore a hard error: choose a
new app id and rebuild the target rather than mixing two dimensions in one stream. Keep both the
lookup's log and data files through the maximum source replay horizon. A lookup correction only
affects newly processed source commits; replay/rebuild is the explicit way to re-enrich old
output.

For an explicitly availability-first lookup, opt in per relation instead:

```toml
[[pipeline.lookups]]
name = "fx_rates"
uri = "abfss://lake@acct/.../fx_rates"
table_id_change_policy = "use_current"
```

This does **not** make routine updates non-deterministic: as long as the timestamp-selected
snapshot and current head have the same Delta table id, ddi still uses that timestamp-pinned
snapshot — including the first batch after a restart that crossed a replacement, which is
pinnable whenever the batch itself lies wholly on one side of it. If the table id changes, or
if **log** retention has removed the commits the required historical snapshot needs while the
current head still opens, ddi warns and substitutes the current head for that batch. It
records `ddi.lookup.fx_rates.current = true` alongside the lookup version and id in the target
commit.

It is an intentional trade, and worth reading twice before taking it: data spanning the
replacement or the truncated history can no longer be replayed against its original lookup
lineage, so a failed batch that retries after the lookup has moved on is enriched from the
newer head. Rows the target rejects are written to the data-quality table *before* the batch
commits, so a retried batch of that kind can leave quarantined rows reflecting a different
lookup snapshot than the rows that eventually landed.

What this does not cover is a vacuumed *data* file. Only the Delta log is consulted while
selecting a snapshot, so a snapshot that resolves but whose parquet has been deleted fails
during the join, as an ordinary read error, under both policies. Log retention on a lookup is
what this setting survives; file retention still has to be long enough to read.

In dbt-derived config, declare the equivalent on the lookup source as `meta: {ddi_lookup:
fx_rates, ddi_lookup_table_id_change_policy: use_current}`.

`strict` remains the default, and is now stricter than it was: it refuses as soon as *either*
the timestamp-selected snapshot or the current head stops matching the identity the target
recorded. It used to compare only the selected snapshot, which meant a replacement landing
mid-run was tolerated until the next restart and then refused — the same answer, given later
and from a different place.

Use this for compact, keyed relations such as daily FX rates. It is not a substitute for
joining a many-gigabyte historical RRP/COGS table into every 5,000-row source batch; materialize
a compact lookup with a documented key first, or enrich downstream.

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

### ddi is not where the work is described

Everything a pipeline *does* — which tables, which SQL, which timestamp, which key — comes
from the dbt project, because that is where it is already written down and kept correct.
Two copies of that would only ever disagree. `ddi run` re-derives it from the manifest on
every start, so there is no generated file to regenerate and nothing to drift.

What is left is the part dbt has no opinion about:

```toml
manifest = "target/manifest.json"      # the source of truth

[runtime]                              # how eagerly to run
allowed_latency_secs = 30

[storage.options]                      # how to authenticate
azure_storage_account_name = "mylake"
azure_storage_account_key  = "..."
```

```bash
ddi run -s orders_stg     # one model
ddi run                   # every streamable model in the project
ddi dbt convert           # print what it derived, without running
```

Locations come from dbt wherever dbt knows them — `location_root`, a source's
`delta_table_path`, `meta.ddi_location`. `uri_template` is only a fallback for warehouses
that name relations without locating them; `{database}`, `{schema}` and `{name}` expand per
model. Everything else reads `manifest.json`, which is the same shape for dbt-trino,
dbt-databricks and dbt-spark.

There is a runnable version of all of this in [`examples/dbt/`](examples/dbt/): vanilla
jaffle shop, a real dbt project with no hooks and no mention of `ddi`, rebuilding the same
Delta table that `ddi` streams into.

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

### When the rebuild cannot be changed at all

A watermark table means touching the dbt project. If the batch side must stay untouched —
no hooks, no macros, nothing that knows `ddi` exists — the tables can answer the question
themselves, provided each carries a timestamp that increases with arrival:

```toml
dedup_timestamp = "_timestamp"   # the default
dedup_key       = "order_id"
```

After a rebuild, `ddi` reads `max(_timestamp)` out of the target and emits only rows beyond
it. Nothing has to be recorded by anyone; the answer is already in the data.

**This is what closes the in-flight window.** A batch job reads the source at one instant
and commits its output later, and rows landing in between are in neither — not its
snapshot, and not the target once it overwrites, even though `ddi` had already streamed
them. Their timestamps are later than anything the batch saw, so they come back; the rows
the batch did write carry earlier ones, so they are not duplicated. The schedule stops
mattering, because coverage became a property of the row.

`dedup_key` resolves rows sharing *exactly* the watermark instant, which a bare `>` would
drop and a `>=` would duplicate. That set is small by construction — only the rows tied at
the maximum — so it is compared row by row against the keys already there. Without a key,
ties fall back to `>`: fine for a strictly increasing sequence, wrong for a
second-granularity clock under load.

Declare both in dbt, next to the model — `_timestamp` is the default, so most models say
nothing at all:

```yaml
models:
  - name: orders_stg
    meta:
      ddi_timestamp: _timestamp   # the default
      ddi_key: order_id
```

The timestamp **must be non-decreasing in the order rows reach the source** — a late row
bearing an older timestamp is indistinguishable from one the rebuild already wrote, and
will be dropped. That suits an append-only stream, not a table that gets backfilled.

The rescan is bounded by the source's own file statistics. Delta records `maxValues` per
file, so the log itself says how far back the rebuild's contents reach: walking backwards
from the head, the first commit whose newest row is already covered is the boundary, and
everything before it is covered too. A rebuild of a table with months of history re-reads
the last commit or two, not the history. Where statistics are missing or of a type that
will not line up, it falls back to a full rescan — being slow is a cost, being wrong is
not an option.

`watermark_uri` remains the better choice where you can set it: exact, no rescan, and no
ordering requirement on any column.

### What the watermark costs to read

Once per pipeline start, never per batch — and only two columns. The timestamp and the key
are projected into the Delta scan, so the parquet reader never decodes the rest of the row,
and the pass is streaming: the running answer is one timestamp plus the keys tied with it, so
memory is bounded by that rather than by the table.

This matters more than it sounds. Reading the whole row instead is gigabytes on a silver
table whose rows carry JSON payloads, it is paid again on every restart, and it grows with
the table — so a pipeline that had been starting fine gets slower until it cannot start at
all, and a crash-loop makes it worse rather than better. The startup line reports
`rows_scanned` at debug level if you want to see what a start is costing.

### What else happens to a shared table

[`tests/hardening.rs`](tests/hardening.rs) runs `orders_raw -> orders_stg` through each of
these and asserts the same invariant every time — no key missing, no key twice:

| Event | Behaviour |
|---|---|
| Full refresh of the target | Rescan; rows the rebuild covers are skipped |
| Rows arrive while the batch runs | Re-emitted, by timestamp |
| `OPTIMIZE` on either table | Ignored — `dataChange: false` |
| `DELETE`/`UPDATE` upstream | Skipped per `change_policy`, never propagated |
| `DELETE` behind the target's watermark | Left deleted |
| Target dropped and recreated | Refilled from scratch |
| Source dropped and recreated | Starts over, emitting only what is missing |

The last one is the trap, and not in the obvious direction. Dropping and recreating a
table keeps its path and its name but gives it a new identity and a log that restarts at
zero, so the carried-over offset means nothing. If the new table has *fewer* commits than
were consumed, the pipeline waits for commits that will never arrive. If it already has
*more* — the likelier case, and the dangerous one — the offset still lands comfortably
inside the log, so nothing looks wrong while the new table's early commits are skipped and
never read.

Neither is detectable from the version alone, so `ddi` records the source's table id in
each of its commits and compares it on restart. When the source turns out to be a
different table, it starts over from the beginning; `dedup_timestamp` then drops whatever
the target already holds, so only genuinely missing rows are emitted. Without a
`dedup_timestamp` there is nothing to filter on and starting over would append the whole
table a second time, so it stops and says so.

## Bad rows, and broken streams

Two different failures used to have the same answer — stop — and at one pipeline watched by a
person that was right. At three hundred it is not: the chance that *some* stream meets a
malformed value approaches one, and "wait for a human" turns a single bad row into an outage.

### A row the target will not take

Bronze says `amount` is text. Silver says it is a `BIGINT`. Most rows convert; the one that
says `"n/a"` does not. That row goes to a **data-quality table**, and the rest of the batch
commits:

```
<target_uri>__ddi_dq
```

Derived from the target, so a fleet needs no per-pipeline configuration; override with
`dq_uri` (or `meta.ddi_dq`) where it belongs somewhere else. Like every table here, `ddi`
never creates it — and that absence is the switch. **No table, no quarantine:** a bad row
still fails the pipeline, because quietly discarding rejects on the grounds that nobody made
somewhere to put them is the one outcome worse than stopping. The startup line says which
mode a pipeline is in.

```sql
CREATE TABLE silver.orders__ddi_dq (
  app_id          VARCHAR,
  pipeline        VARCHAR,
  source_version  BIGINT,   -- the batch's last source version, not the row's
  column_name     VARCHAR,  -- the column that rejected it
  reason          VARCHAR,
  payload         VARCHAR,  -- the row as it arrived, as JSON
  _timestamp      TIMESTAMP(6)
) WITH (location = 'abfss://.../silver/orders__ddi_dq')
```

What is **not** given up is the part that matters: nothing is nulled to make it fit and
nothing is dropped. The target holds only values that survived the cast, and every rejected
row is queryable with the reason attached:

```sql
SELECT reason, count(*) FROM silver.orders__ddi_dq
WHERE _timestamp > now() - interval '1' day GROUP BY reason
```

Two things are deliberately *not* quarantined:

- **A structural mismatch** — a target column the transform does not produce at all, a
  transform that will not plan. It is identical on every batch and belongs to no row, so
  setting rows aside would leave a target that silently never grows. It fails the pipeline.
- **A bad value inside a `struct`, `list` or `map`.** Arrow pushes a lenient cast down into
  the *children* and keeps the parent's null buffer, so an unconvertible element becomes a
  `NULL` inside a row that still looks valid from the outside — undetectable per row, and it
  would reach the target. Nested columns therefore keep the strict cast: one bad value fails
  the batch.

Rejects are written **before** the target commits, in their own commit. Two tables cannot
share one Delta commit, so the ordering is the guarantee: a crash in between replays the
batch, which can duplicate a reject but can never lose one. Even that is usually avoided —
the data-quality commit carries a `txn` action of its own under `<app_id>.dq`, and a replay
of the same batch finds it and skips.

### A stream that cannot make progress

A pipeline that hits something it cannot handle no longer ends. It backs off — 1s, doubling
to 5 minutes, jittered so three hundred of them do not return in lockstep after an outage —
and reopens. The backoff resets the moment a step succeeds.

**Its peers are never touched.** Previously the first pipeline to fail cancelled every other
one; worse, in `run` mode the failure was noticed only when every pipeline spawned before it
had finished, which is never — so the stream died silently and nothing restarted it.

The same holds one step earlier, at config load. A pipeline that cannot be correct is still
refused before it starts, but it is refused *alone*: the others run, it is named in the log
and reads `ddi_pipeline_config_valid 0`, and `ddi validate` still exits non-zero listing
every fault so a CI gate keeps working. A typo in one entry of three hundred should not be
one keystroke away from an outage.

Because the process no longer exits when a stream dies, metrics stop being optional:

| Signal | Meaning |
|---|---|
| `ddi_pipeline_up` | 1 while streaming, 0 while backing off. **The health signal.** |
| `ddi_pipeline_config_valid` | 0 for a pipeline held back at load. `up = 0` is a stream that stopped; this is one that never started. |
| `ddi_pipeline_seconds_since_progress` | Staleness. Still moves when a pipeline fails while *opening*, which lag does not. |
| `ddi_pipeline_restarts_total` | Reopens after a failure. Climbing steadily = stuck on something a human must fix. |
| `ddi_source_file_vacuumed` | 1 while a stream is stuck on a source file that no longer exists. The one failure waiting does not fix. |
| `ddi_rows_rejected_total` | Rows sent to the data-quality table. |
| `ddi_batches_fully_rejected_total` | Batches where *every* row was rejected. |

Alert on `ddi_pipeline_up == 0 for 10m`, on `ddi_source_file_vacuumed == 1`, and on
`increase(ddi_batches_fully_rejected_total[15m]) > 0`. The last one matters more than it
looks: there is no bad-row threshold, so an upstream type change quarantines the whole batch
and the target simply stops growing — no error, no lag, nothing else to notice it by.
`ddi_errors_total` is now a *rate* of retried attempts, not a page: a pipeline that lost one
commit race and recovered a second later increments it.

### A source file that is no longer there

Backing off and reopening is the right answer to almost everything, because almost
everything clears: a storage blip, a commit race, a schema somebody is in the middle of
fixing. One failure never clears, and stopping stays the right answer to it — so it gets a
signal of its own rather than a place in the queue of things that recover.

Delta's `OPTIMIZE` retires a data file with `dataChange: false` and writes compacted
replacements. `VACUUM` later deletes the retired object for real. Both are routine, and
neither is a problem — until a consumer is still behind the commit that added the retired
file. Then the commit is in the log, the file it names is not in storage, and the batch that
commit represents cannot be built at all. Not "with difficulty": at all. The rows survive
inside the compacted files, but so do rows from commits already consumed, and there is
nothing in a compaction that says which were which. Replaying it would be a guess, and this
tool stops rather than guessing. Where a `DELETE` retired the file instead, the rows are
simply gone — same symptom, different answer, and worth telling the two apart before
deciding what to do about either.

So `ddi` stops on that pipeline, names the relation, the version and the file, and raises
`ddi_source_file_vacuumed`. It keeps retrying — restoring the file is a real recovery, and a
stream that gave up would need the process bounced to notice — but the gauge is what tells
the two kinds of retry apart:

```
ddi_source_file_vacuumed{pipeline="catalog_description_changed"} 1
```

Alert on it directly. `ddi_source_file_vacuumed == 1` needs no `for` clause and tolerates
one: it is raised by the failure and cleared only by a step that succeeds, so a second,
unrelated failure part-way through the outage cannot drop it and reset a pending alert.
That is the difference from `ddi_pipeline_up == 0`, which flaps by design because almost
every failure it covers does recover.

**The contract this rests on is the source's, not `ddi`'s.**
`delta.deletedFileRetentionDuration` on every source table must exceed the longest a
pipeline reading it may be behind — including one that is *deliberately* excluded, which is
the case people forget, because an excluded pipeline looks like a decision rather than a
backlog. The default is seven days. A pipeline paused over a long weekend and a public
holiday is inside that; one excluded pending a schema review is not.

Two recoveries are safe, and which one applies is a question about your storage, not about
`ddi`:

- **Restore the file.** ADLS soft delete, S3 versioning, a backup — anything that puts the
  named object back where the log says it is. The pipeline picks up on its next retry, and
  this is the better option whenever it exists, because it is the only one that preserves
  exactly-once. Two things it is easy to get wrong: both storage mechanisms are opt-in and
  neither is retroactive, so whether the option exists at all was decided before the
  `VACUUM` ran; and a stream far enough behind will name the *next* missing file on its
  next retry, so restore the whole retired set for the versions still to be read, and hold
  `VACUUM` off the source until the backlog is gone or the next run undoes the work.
- **Rebuild the target deliberately**, from a current snapshot of the source. Note what
  that takes, because the obvious version does not work: `starting_version` is consulted
  only when the target holds no `txn` action for the pipeline's `app_id`, and a `txn`
  action survives an overwrite — so an in-place rebuild resumes at the lost version again.
  Recreate the table, or give the pipeline a new `app_id`. Which rebuild is right is not
  something `ddi` will decide: an append target and an upsert target need different ones,
  the target may have been read downstream already, and only you know which.

What `ddi` will *not* do is skip the commit, or quietly treat the compaction's output as an
equivalent batch. Both would produce a target that is wrong in a way nothing downstream
could detect.

## Upserting

Append-only silver holds every version of a row. If an order is placed, then paid, then
shipped, silver holds three rows for it and something downstream has to pick the last one.
`write_mode = "upsert"` makes silver hold one row per key instead:

```toml
[[pipeline]]
name            = "orders_stg"
app_id          = "ddi.orders_stg"
source_uri      = "/lake/bronze/orders"
target_uri      = "/lake/silver/orders"
write_mode      = "upsert"
dedup_timestamp = "_timestamp"   # decides which of two rows is newer
upsert_key      = "order_id"     # defaults to dedup_key
upsert_lookback = "48h"          # optional; a cost ceiling, not a correctness knob
```

or, in dbt, next to the model:

```yaml
models:
  - name: orders_stg
    meta:
      ddi_write_mode: upsert
      ddi_timestamp:  _timestamp
      ddi_key:        order_id
```

The rule is `WHEN MATCHED AND source._timestamp > target._timestamp THEN UPDATE`, plus
`WHEN NOT MATCHED THEN INSERT`. Re-delivering a row that is older than the stored one is a
no-op, so a replay after a rebuild cannot roll the target backwards.

Exactly-once is unchanged: `CommitProperties::with_application_transaction` puts the `txn`
action in the merge's own commit, so data and offset still advance together or not at all.

### The bounded window

A merge has to read the target, which an append never does. Reading all of it on every batch
is what makes naive MERGE-on-a-stream unusable, so the merge is given a predicate:

```sql
MERGE INTO target t USING batch s
  ON  t.order_id = s.order_id
  AND t._timestamp >= <lo>
```

`<lo>` comes from the target's own transaction log, and no data files are opened to compute
it. Delta records `minValues`/`maxValues` per file, so walking the live files answers "which
of these could hold one of the keys I am holding?" directly; `<lo>` is then the *lowest*
`_timestamp` any of them holds. A file the statistics rule out is never read.

Two details do the real work:

- **The minimum is taken over every candidate file, not just the old ones.** The predicate is
  the `ON` clause of an outer join, so a target row below `<lo>` is unmatched even when its
  file is read — and an unmatched row means `INSERT`, i.e. a duplicate key. Taking the
  minimum over all candidates puts `<lo>` at or under the first row of every file still in
  play. This matters because ddi's own merges create files that straddle any cut-off: a
  matched file is rewritten whole, old rows copied in beside the new ones.
- **The keys are tested as a set, not as a range.** A batch of today's orders plus one
  re-delivered ancient one spans nearly the whole key space as a range but touches only two
  regions of it as a set.

Where the statistics run out — a key column past `delta.dataSkippingNumIndexedCols` (32 by
default), a writer that recorded none, a type that will not line up — the window opens to the
whole target. Slow, and correct. Truncated string statistics are handled rather than trusted:
a `maxValues` of `"ord"` may stand for `"ordz"`, so it never rules a file out.

### `upsert_lookback`

A floor: the window will not open below `min(batch timestamp) - upsert_lookback` however far
back the statistics say it should. It buys bounded cost when the statistics cannot bound
anything themselves — a UUID key, where every file's key range overlaps every other.

It is not free, and the trade is explicit. When the floor wins, a key in that batch may have
an older row below it that will not be matched, and it is inserted alongside instead of
replacing it. That is logged at `warn` and counted in
`ddi_upsert_window_clamped_total` — **alert on it.** Leave `upsert_lookback` unset and
completeness always wins.

### What it costs, and what it rules out

| | Append | Upsert |
|---|---|---|
| Reads the target | never | the part the window admits |
| Rows per key | one per delivery | one |
| Commit contains | `Add` | `Add` + `dataChange` `Remove` |
| Concurrent-writer conflicts | cannot happen | possible; replanned and retried |
| Can feed a downstream `ddi` | yes | only one that also upserts |

That last row is the real cost. A merge rewrites files, so every upsert commit carries a
`dataChange` `Remove` and reads downstream as a change commit. `change_policy =
"skip_change_commits"` would drop those commits **including the keys they insert**, silently.
The only combination that survives is a downstream on `ignore_changes` *and*
`write_mode = "upsert"`, keyed the same way — and `ddi validate` rejects the others rather
than letting you find out in production.

Two more things are checked before the first batch rather than during it: the target must not
be `delta.appendOnly`, and it must not already hold a key twice. The second is what an
append-only target looks like after a key was restated, and a merge would keep those
duplicates forever — it matches on the stored row, so it updates every copy rather than
collapsing them. Collapse the target first:

```sql
CREATE OR REPLACE TABLE silver.orders AS
SELECT * FROM silver.orders
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _timestamp DESC) = 1
```

### Staged upserts

A direct upsert pays for the target on every batch. That is the right trade when a batch
touches a few files and the wrong one for a high-cardinality current-state stream: 5,000 rows
carrying random keys touch *every* file, so the merge rewrites the whole state to apply a
handful of changes. Nothing about the batch makes that cheaper — not better statistics, not
sorting, not `upsert_lookback`, and not a smaller batch, which only multiplies a fixed cost
by more batches.

The cost is per *merge*, so the fix is fewer merges:

```
source ──▶ ingest ──▶ silver.style__ddi_stage ──▶ apply ──▶ silver.style
           append,                                merge,
           per commit                             per accumulation
```

```toml
write_mode             = "staged_upsert"
dedup_timestamp        = "_timestamp"
upsert_key             = "style_id"
apply_max_bytes        = "512MB"   # merge once per this much staged data
apply_max_latency_secs = 900       # ...or this often, whichever comes first
```

or in dbt, `meta: {ddi_write_mode: staged_upsert, ddi_apply_max_bytes: 512MB}`.

One configured pipeline becomes **two running ones**, `style__ingest` and `style__apply`,
each with its own `txn` offset, its own metrics and its own backoff. That is the whole
implementation: everything the two halves need — exactly-once, cursor resume, the merge
window, the data-quality table — already worked for one source and one target, and splitting
at config time means none of it had to change. The staging table is created from the target's
schema if it is not there; it is the only table `ddi` ever creates, because it is the only one
that is its own.

Read the two halves' lag separately: `ddi_source_lag_versions{pipeline="style__ingest"}` is
how far behind the raw stream is, and `{pipeline="style__apply"}` is how much has been staged
but not yet merged.

#### What it costs

**The target is eventually consistent**, by up to `apply_max_latency_secs`. That is the
bargain rather than a defect, and it is the number to publish to whoever reads the table.

**The transform must produce every target column.** A staged row is written now and merged
later, and by then a null cannot be told apart from a column the transform never mentioned —
so merging it would erase whatever the target already held there. Plain `upsert` carries that
distinction with the batch and can leave such columns alone; staging cannot, so it refuses
rather than guessing. If something else owns a column of your target, use `upsert`.

**Ties need a tie-breaker.** The apply half accumulates a different number of commits each
time it runs, so "later in the batch" stops being a stable rule — see
[`upsert_tiebreak`](#upsert_tiebreak).

The staging table is private to the pair that owns it: its rows are appended by one half and
consumed by the other, so `ddi validate` holds back any third pipeline that reads or writes
one.

### `upsert_tiebreak`

Which row wins when two share a `dedup_timestamp`. Compared after the timestamp, left to
right, against rows in hand *and* against the row already stored:

```toml
upsert_tiebreak = ["kafka_partition", "kafka_offset"]
```

Unset, a tie is settled by position in the batch — later in the batch is later in the source.
That is true exactly as long as batch boundaries are, which under `staged_upsert` they are
not. Every column named must be in the target, and none of them may be null.

### JSON payloads

Bronze often carries a payload as text, so Trino's JSON functions are implemented here
too — a model has to mean the same thing in the warehouse and in `ddi`:

| Function | |
|---|---|
| `json_extract(json, path)` | the value at `path`, as JSON |
| `json_extract_scalar(json, path)` | the value at `path`, as text; **NULL for an object or array** |
| `json_size(json, path)` | members of an object, elements of an array, 0 for a scalar |
| `json_array_length(json)` | elements, or NULL if not an array |
| `json_array_contains(json, value)` | |
| `json_array_get(json, index)` | negative indexes count from the end |
| `json_exists(json, path)` | |
| `json_parse` / `json_format` / `is_json_scalar` | |
| `json_value` / `json_query` | the SQL/JSON spellings of scalar / extract |

`json_extract_string` (DuckDB) and `get_json_object` (Spark) are aliases of
`json_extract_scalar`, so a model written against either streams unchanged.

```sql
SELECT order_id,
       CAST(json_extract_scalar(data, '$.customer.id') AS BIGINT) AS customer_id,
       json_extract_scalar(data, '$.lines[0].sku')                AS first_sku,
       json_extract_scalar(data, '$.status')                      AS status,
       _timestamp
FROM source
```

Paths support `$`, `.field`, `["field"]` and `[0]`. Wildcards are rejected rather than
quietly returning one of several matches. A missing path is NULL, and so is a container
under `json_extract_scalar` — that is Trino's rule, and it is what stops `{"id":42}`
landing in a column somebody casts to a number. Malformed JSON stops the pipeline: input
is a typed column, not arbitrary text.

### Ordering, when using a watermark table

Prefer a **pre-hook** that records the version and a model that pins its read to it
(`FOR VERSION AS OF`). Then the watermark is on disk before the overwrite lands and there is
no window at all. With a post-hook the watermark appears one commit later; if `ddi` looks in
between it re-streams from the previous watermark, which duplicates rows rather than dropping
them. That asymmetry is deliberate — duplicates are visible and the next rebuild erases them,
whereas a gap is silent and permanent.

`OPTIMIZE` on the target is not mistaken for a rebuild: its `Remove` actions carry
`dataChange: false`.

## Memory

```toml
[runtime]
max_memory = "6GB"      # optional; the container's own limit is used when unset
```

One number for the process, divided by the pipelines running in it — because they all start
at once, and it is that simultaneity which turns a survivable allocation into an OOM. When
`max_memory` is unset the cgroup's limit is read and three quarters of it used; when there is
no limit either, nothing is bounded, which is the right answer on a workstation.

It covers two things, and they are not the same mechanism:

- **DataFusion**, through the memory pool every session `ddi` builds — the SQL transform, the
  merge, the target scans, the upsert's grain check. Those consumers spill rather than grow.
- **The batch**, which matters more. `max_bytes_per_batch` counts *compressed parquet bytes*,
  and what the process holds is that decoded into Arrow for every file at once. Measured on a
  realistic table the gap is about 5×, so a 256 MB setting can be 1.4 GB resident — per
  pipeline, on the first batch after a cold start, when every pipeline is furthest behind and
  asking for the most.

  So a batch stops accumulating when what it has *already decoded* would fill its share. The
  ratio is measured after every batch rather than assumed, because a constant chosen here is
  a constant to get wrong later.

Measured, on 90 MiB of parquet in 8 files:

| `max_memory` | batches | peak RSS |
|---|---|---|
| unset | 1 | +413 MiB |
| 512 MiB | 4 | +119 MiB |
| 256 MiB | 8 | +89 MiB |

The floor is one commit: a commit that fits `max_bytes_per_batch` is always delivered, however
tight the budget. A budget makes batches smaller and more numerous; it never refuses one, and
it never stalls a pipeline that worked before it was set.

### What memory cannot bound

`max_memory` bounds what one pipeline holds. It is the right bound for the work a pipeline
does to *itself* and the wrong one for the work it does to a **target**, because the target
work is where pipelines stop being independent: a merge reads back a slice of the table it
writes, and the startup uniqueness check reads all of it. Neither is proportional to the
batch, so neither gets smaller when batches do — and dividing the budget more finely only
makes each pipeline spill sooner while the same number of scans run at once. Spilling sooner
spends a different budget, on local disk, which the next section is about: the two run in
opposite directions, and a tighter `max_memory` produces *more* spill rather than less.

```toml
[runtime]
max_concurrent_upsert_merges     = 4   # optional; unset means unbounded
max_concurrent_upsert_preflights = 8
```

Both are unset by default, which is the behaviour there has always been: a limit chosen here
would be a limit chosen without knowing the fleet. They are separate because they overlap in
time but not in kind — every upsert pipeline preflights once, at startup, all at the same
instant, while merges happen forever at whatever rate commits arrive. One limit covering both
would have to be set for the startup burst and would throttle steady state for the rest of
the run.

Waiting is measured, because from outside a queue and a stall look identical. Watch
`ddi_merge_queue_milliseconds_total` against `ddi_merge_milliseconds_total`: queue time rising
while merge time stays flat means the limit, not the storage, is the throughput.

`tests/memory_shape.rs` is the probe those numbers come from. It is `#[ignore]`d because it
builds multi-million-row tables, and it is in the repository because every memory incident
here was first diagnosed by correlation from outside the process, and twice that was wrong:

```bash
cargo test --profile release-lean --test memory_shape -- --ignored --nocapture --test-threads=1
ROWS=6000000 BUDGET_MB=512 cargo test --profile release-lean --test memory_shape -- --ignored --nocapture
```

## Where spilling goes, and what bounds it

```toml
[runtime]
temp_directory          = "/var/spill"   # optional; the OS temporary directory when unset
max_temp_directory_size = "8GB"          # optional; DataFusion's own 100GB when unset
```

**The cap is process-wide, and it is worth being precise about why**, because DataFusion's
own is not. DataFusion stores the limit on a `DiskManager` and checks it against that same
`DiskManager`'s counter — so out of the box it is per `RuntimeEnv`, and `ddi` builds a
`RuntimeEnv` per DataFusion *operation*: one per merge attempt, one per transform batch, one
per startup check. Eleven pipelines each honouring a hundred gigabytes is not a hundred
gigabytes. `ddi` builds one runtime at startup and derives every other from it, so they all
share one directory and one counter, and `max_temp_directory_size` means what it reads as:
bytes this process may have on local disk, full stop.

Unset is not "unbounded". Unset is DataFusion's 100 GB, which is larger than most pods'
`ephemeral-storage` limit — so unset means "bounded above the point at which the pod is
killed". `ddi` will not lower that default for you, because it cannot see your pod's limit
and an upgrade must not change behaviour for someone who edited nothing. It warns at startup
instead.

Set the cap **below** the volume's real size. It is checked after each write rather than as
an admission check, so it can be overshot by about one buffer per open spill file, and it
counts only what DataFusion wrote — it knows nothing about free space or anything else in
the pod.

In Kubernetes, where the directory points decides who is charged:

- **Unset**, or a path on the container's own filesystem, is the writable layer. The kubelet
  counts every byte of it as the pod's local ephemeral storage, and going over is an
  *eviction* — not an error, not a log line, and it takes every other pipeline in the pod
  with it. That is the one failure `ddi` cannot contain from the inside.
- A default-medium `emptyDir` is still charged to the pod unless it carries its own
  `sizeLimit`; `medium: Memory` moves the cost to the memory limit rather than removing it.
- A separate `PersistentVolumeClaim` is the only shape genuinely outside the kubelet's
  ephemeral-storage accounting.

The directory is created and probed with a real write at startup — a volume that was not
mounted stops `ddi validate`, rather than surfacing an hour later as a sort failing inside a
pipeline that has nothing to do with the mistake. A cap of zero is refused rather than
guessed at: "unbounded" and "never spill" are both plausible readings of it and they point in
opposite directions.

Watch `ddi_spill_bytes` against `ddi_spill_limit_bytes`; a ratio near one means the next merge
fails. `ddi_capacity_exhausted` says which pipeline it failed for — and a capacity failure
stops that pipeline only, waits the full backoff rather than retrying every second, and leaves
its target untouched.

## The startup uniqueness check

An upsert pipeline proves its target holds one row per key before it merges into it, because
a merge matches on the *stored* row: against a target that already holds a key twice it
updates both copies, forever, and every count and sum over that table stays wrong.

```toml
[runtime]
max_grain_check_memory = "512MB"   # optional; 512MB when unset
```

The check **writes nothing to a temporary directory, at any target size, under any
configuration**. That is a property of its shape rather than a limit it is held to: it hashes
the key column to eight bytes a row, keeps only the hashes in one congruence class of the key
space, sorts them in place and looks for two the same. Nothing registers with the memory pool
and nothing asks the disk manager for a file.

What it trades instead is *how many times it reads the target*. Eight bytes per row divided by
`max_grain_check_memory` is the number of classes, and each class is one pass over the key
column alone:

| `max_grain_check_memory` | 6 million rows | 500 million | 2 billion |
|---|---|---|---|
| 256 MB | 1 | 18 | 69 |
| **512 MB** (default) | **1** | 9 | 35 |
| 1 GB | 1 | 5 | 18 |
| 4 GB | 1 | 2 | 5 |

Almost every target is one pass, and one pass is strictly cheaper than the `GROUP BY` this
replaced — no plan, no repartition, no spill. The row count comes from the target's own log,
so the arithmetic costs no IO. A target that would need more than 256 passes is refused at
startup with the number in the message, rather than run silently for hours, and any target
taking more than one logs the count when the pipeline opens. Watch `ddi_grain_check_passes`.

The expensive answer is always "the target is fine": a broken one is answered by the first
pass that meets a duplicate.

Sixty-four bits of hash is not exact on its own — at two billion keys the birthday bound puts
about a tenth of a collision in every run — so a pass *nominates* rather than answers, and a
second pass resolves the nominees against the real key values. Reporting a collision as a
duplicate would refuse a perfectly correct table, which is the worse of the two errors this
check can make.

A pipeline that cannot pay the passes can set `upsert_grain_check = "off"`, which is an
assertion you have made and `ddi` has not verified. It warns on every start, because getting
it wrong is not recoverable by retrying.

## Storage

Tables are named by URI and the scheme picks the backend — a bare path or `file://` for
local disk, `abfss://` or `az://` for Azure. Credentials are the one thing a dbt project
cannot tell you: it knows *which* table, never how to reach it. So they are the only
functional-looking thing in `ddi`'s own config.

```toml
[storage.options]
azure_storage_account_name = "mylake"
azure_storage_account_key  = "..."
```

Both URI shapes work:

```text
abfss://container@account.dfs.core.windows.net/path/to/table
az://container/path/to/table            # account comes from the options
```

Any object-store key is accepted, so pick whichever credential the deployment has:

| Instead of an account key | Set |
|---|---|
| SAS token | `azure_storage_sas_key` |
| Bearer token | `azure_storage_token` |
| Service principal | `azure_client_id`, `azure_client_secret`, `azure_tenant_id` |
| Managed identity | nothing — it is used when no key is given |
| Local development | `azure_use_azure_cli = "true"` |

The same keys are read from the environment in upper case
(`AZURE_STORAGE_ACCOUNT_NAME`), which is usually how a container gets them, so
`[storage.options]` can be left out entirely.

Other clouds are a feature flag away — `s3` and `gcs` on the `deltalake` dependency — since
everything above [`src/storage.rs`](src/storage.rs) addresses tables by URI and never
learns which backend answered. A URI whose backend was not compiled in says so rather than
reporting a missing table.

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

## Tables other engines also write

`ddi`'s premise is that it can be pointed at a lakehouse other tools write, so it has to
read what those tools leave behind. The one that shows up in practice is precision.

The Delta protocol defines `timestamp` as **microseconds**. Trino writes **milliseconds** —
into the data files its `OPTIMIZE` rewrites, and into the `stats_parsed` of the checkpoints
it leaves behind. Nothing else in a typical estate objects: an append-only writer never
reads the data back, and Trino reads what Trino wrote. A spec-enforcing reader is the first
thing to notice, and without care it is the only thing that stops.

So `ddi` is liberal in what it accepts, along one axis only:

> **The table's Delta schema is authoritative. A physical column that differs from it in
> precision alone is coerced on read.**

- A `timestamp[ms]` data file is read as the declared `timestamp[us, tz=UTC]`. A column
  written with no timezone at all is read as UTC, because that is what Delta's
  UTC-adjusted `timestamp` means — never as local. `timestamp_ntz` keeps meaning what it
  means.
- A checkpoint whose `stats_parsed` carries types the protocol does not have no longer
  decides whether the table opens. `ddi` replays the commit log instead and says so once,
  naming the engine that wrote it:

  ```text
  WARN "abfss://…/erp_variant_article_changed" carries a checkpoint written by
       parquet-mr-trino version 480-e.5 whose stats_parsed types do not match the table
       schema; replaying the log instead. This is usually an OPTIMIZE from another engine.
  ```

  Opening is slower — every commit since version 0 is read — and the snapshot is identical.
  Nothing is lost: a checkpoint's only exclusive content is `stats_parsed`, a pre-decoded
  copy of statistics `ddi` never reads. [`src/stats.rs`](src/stats.rs) parses the `stats`
  JSON string, which the commits carry verbatim.

  Only the checkpoints that were *already there* are stepped over. A table opened this way
  stays fully writable, and the checkpoint delta-rs writes on its way through is well-typed
  and visible — so the table heals itself, and the slow open stops being needed. Where log
  retention has already removed the commits the bad checkpoint stands in for, there is
  nothing left to replay; `ddi` says exactly that rather than failing obscurely.

  A table compacted more than once has *older* checkpoints too, and the newest being
  readable says nothing about them. `ddi` never parses those, because the only questions it
  asks about an earlier version — what the schema was, which version a timestamp lands on —
  are answered from the commits alone, and it asks them without requesting the file list
  that would make delta-rs read a checkpoint. That is also why it is fast: those calls used
  to rebuild the source's entire file set once per version.

  A compaction can also land *while a merge is running*. It commits and checkpoints
  together, so an upsert that loses the race meets that checkpoint during conflict
  resolution — above the version its handle was opened at, and so read rather than skipped.
  Nothing is committed when that happens, so it is replanned against a freshly opened target
  exactly like the commit conflict it is. None of this depends on the target's own files:
  it happens with every one of them at the declared precision.

Only a **widening** between two timestamps is performed, and nothing else is newly refused
either. A file *finer* than the schema — Spark writes Delta timestamps as INT96, which
decodes as nanoseconds however the table is declared — is passed through to the coercer that
has always narrowed it. A `string` where the schema says `timestamp` is not a precision
difference at all, and fails exactly as it always has. The point of the rule is to narrow
what is refused, not to refuse more.

If you hit the checkpoint failure on a build that predates this, deleting the offending
`*.checkpoint.parquet` and `_last_checkpoint` forces the same log replay by hand, and is
safe while the JSON commits are still inside log retention. It does not help where
`OPTIMIZE` also rewrote data files — but it tells the two failures apart quickly.

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

## Live dashboards

Optional, off by default, and a *leaf* of the pipeline rather than a stage in it. After a
target commit lands, `ddi` can push a compact payload derived from that same batch to a
fan-out service — Azure Web PubSub today — so a browser gets the update over a socket it
already holds, without polling Delta and without a second real-time platform in the stack.

```text
Delta source
    |
    v
   ddi
    |
    +-----------------> Delta target
    |                     commit succeeds
    |
    +-- post-commit ---> publisher ---> Web PubSub ---> browsers
```

What to push is a dbt model, so there is no second aggregation hidden in Rust or in
frontend code:

```sql
-- models/orders_live.sql
{{ config(materialized='view') }}

select status, count(*) as orders_delta, sum(amount) as amount_delta
from {{ ref('orders_stg') }}
group by status
```

```yaml
models:
  - name: orders_live
    meta:
      ddi_publish: webpubsub
      ddi_publish_group: orders
```

**This is the one place `GROUP BY` is allowed, and the exception proves the rule.** A
transform's rows are appended to a table that outlives their batch, so a group spanning
batches would store a partial sum forever — which is why `GROUP BY` is this tool's headline
rejection. A publication's rows are a *message* describing one committed batch and are never
stored, so a partial sum is exactly what it is for. The validator takes the grain as a
parameter rather than forking, so every other rule — one source relation, no window frames,
no foreign tables — stays shared between the two.

The aggregates are narrowed further, to the ones a client can apply as a delta: `sum`,
`count`, `min`, `max`. `avg` is refused because the average of two batches is not the average
of their averages — and the refused set is asked of the query engine's own registry rather
than written out by hand, so an alias like `mean` cannot slip past it. That narrowing is also what keeps the useful duality: over one batch the
model is the delta, over the whole table it is the running total, so **the same view is the
baseline a client reloads after a gap**.

Delta stays authoritative and the realtime path cannot touch it:

- The payload is built **before** the commit, from the already-coerced batch, and sent
  **after** it. A build failure yields no payload; it never fails a batch.
- The send returns statistics, not a `Result`. "A publisher cannot fail a commit" is a
  property of the signature rather than a convention at the call site.
- At-most-once, deliberately. No retry, no queue, no outbox — the batch cannot be replayed
  once the offset has moved, and an outbox would be state in a daemon whose premise is that
  it has none.

What makes that honest is the cursor in every message. Each carries `prev_source_version` —
the previous *batch*, whether or not its own message got through — so a batch that was lost
still occupies its place in the chain and the next message says it follows one the client
never saw. That is what makes a loss detectable rather than silent. The field is a fact about
the publication sequence, not about the source table: source versions are not consecutive and
never were, because compaction and `dataChange: false` commits advance the cursor without
producing a batch.

Append-only in v1. A merge replaces the row stored under a key, so the committed batch does
not contain the value it replaced and a delta cannot be derived from it; `ddi_publish` on an
upserting model is refused.

`ddi` does not hold browser connections, mint client access tokens, or serve a negotiate
endpoint — its only HTTP surface is `/metrics`, and token-minting would mean authenticating
dashboard users, which it has no notion of. See [USING_DDI.md](USING_DDI.md) §8 for the
configuration, the payload schema, and the client contract.

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
- **Silently repairing bad data.** A value that will not convert is never nulled or dropped
  to make it fit. It is either the whole batch's problem, or it goes to a
  [data-quality table](#bad-rows-and-broken-streams) with the reason attached — and in both
  cases the target only ever holds values that survived the cast.
- **Deduplication / restatement in append mode.** If Kafka emits order v1 then a corrected
  v2, append-only silver holds both, and that is the mode's promise rather than a gap. Where
  you want one row per key, [`write_mode = "upsert"`](#upserting) does it here instead of in
  a downstream MERGE — at the cost of reading part of the target on every batch, and of the
  target no longer being an append-only table.
- **Deletion propagation.** Not even in upsert mode: a merge here inserts and updates, never
  deletes. A key that disappears upstream keeps its last known row.
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
- **Spill is local disk, and Kubernetes counts it.** Anything DataFusion cannot hold in
  memory goes to `[runtime] temp_directory`, and unset that is the container's writable layer
  — which the kubelet charges to the pod's `ephemeral-storage`. Exceeding it evicts the pod
  rather than failing a query, so there is no error to find afterwards and every pipeline in
  the pod goes down together. Set the directory and the cap in any container.

### Metrics

`ddi --metrics-addr 127.0.0.1:9100` (or `DDI_METRICS_ADDR`) serves Prometheus text on
`/metrics`. Omit the flag and no socket is opened. Every series is labelled `pipeline`.

| Metric | Type | Meaning |
|---|---|---|
| `ddi_batches_committed_total` | counter | Batches committed. |
| `ddi_rows_written_total` | counter | Rows written to targets. |
| `ddi_files_read_total` | counter | Source data files read. |
| `ddi_errors_total` | counter | Failed attempts. A failure retries with backoff; this is a rate, not a page. |
| `ddi_commits_skipped_total` | counter | Source commits consumed that produced no rows. |
| `ddi_last_source_version` | gauge | Last source version **durably committed** (the `txn` value). |
| `ddi_source_head_version` | gauge | Source head at the last poll. |
| `ddi_source_lag_versions` | gauge | Source commits not yet consumed. |
| `ddi_pipeline_up` | gauge | 1 while streaming, 0 while backing off after a failure. |
| `ddi_pipeline_config_valid` | gauge | 1 when the configuration was accepted, 0 when the pipeline was held back at load and never started. |
| `ddi_pipeline_seconds_since_progress` | gauge | Since the last completed step; -1 before the first. |
| `ddi_source_file_vacuumed` | gauge | 1 while the pipeline is stopped on a source data file the object store no longer has. |
| `ddi_capacity_exhausted` | gauge | 1 once this pipeline ran out of spill space or memory. Raised, never lowered; cleared by a step that succeeds. |
| `ddi_grain_check_passes` | gauge | Passes the last startup uniqueness check took over this target's key column. 0 in append mode. |
| `ddi_spill_bytes` | gauge | Bytes DataFusion currently holds in its temporary directory, process-wide (no `pipeline` label). |
| `ddi_spill_files` | gauge | Spill files open right now, process-wide. |
| `ddi_spill_limit_bytes` | gauge | The budget those two are measured against. Never zero: unset means DataFusion's own 100 GB. |
| `ddi_pipeline_restarts_total` | counter | Reopens after a failure. |
| `ddi_rows_rejected_total` | counter | Rows written to the data-quality table. |
| `ddi_batches_fully_rejected_total` | counter | Batches where every row was rejected. |

Upsert pipelines export five more. All stay at zero in append mode, which is the honest
reading: it never updates a row and never reads the target back.

| Metric | Type | Meaning |
|---|---|---|
| `ddi_upsert_rows_updated_total` | counter | Stored rows replaced by a newer delivery of the same key. |
| `ddi_upsert_rows_inserted_total` | counter | Rows inserted for a key the target did not hold. |
| `ddi_upsert_target_files_scanned_total` | counter | Target files a merge had to open. |
| `ddi_merges_total` | counter | Merges started. The denominator for the two below. |
| `ddi_merge_milliseconds_total` | counter | Time inside merges, permit in hand. |
| `ddi_merge_queue_milliseconds_total` | counter | Time waiting for a merge permit — rising means `max_concurrent_upsert_merges` is the throughput, not the storage. |
| `ddi_merges_in_flight` | gauge | Merges running right now, process-wide (no `pipeline` label). |
| `ddi_preflights_in_flight` | gauge | Startup uniqueness checks running right now, process-wide. |
| `ddi_upsert_window_unbounded_total` | counter | Merges that read the whole target because its statistics could not bound the window. |
| `ddi_upsert_window_clamped_total` | counter | Merges where `upsert_lookback` held the window above what completeness required. |

`ddi_upsert_window_clamped_total` is the one to alert on: each increment is a batch where a
key may have been inserted alongside an older row instead of replacing it. Watch
`ddi_upsert_target_files_scanned_total / ddi_batches_committed_total` for how well the window
is doing its job, and treat a rising `ddi_upsert_window_unbounded_total` as a sign the key
column has no usable statistics — usually because it sits past
`delta.dataSkippingNumIndexedCols`.

Lag is measured from the **cursor**, not from `ddi_last_source_version`. The two differ
whenever commits are consumed without producing a commit of our own: a run of `OPTIMIZE` on
the source advances the cursor but writes no `txn` action, so the durable offset legitimately
sits behind the head while the pipeline is fully drained. Subtracting the offset from the
head would page an operator every time bronze compacts. `ddi_last_source_version` is still
the number to look at when reasoning about *restart* behaviour — it is what a restart resumes
from.

Alert on **`ddi_pipeline_up == 0 for 10m`** for a stream that is down, on
**`ddi_source_file_vacuumed == 1`** for one that will not come back without a human (see
[A source file that is no longer there](#a-source-file-that-is-no-longer-there)), on
**`increase(ddi_batches_fully_rejected_total[15m]) > 0`** for a target that has silently
stopped growing, and on `ddi_source_lag_versions` for backlog. Use
`ddi_pipeline_seconds_since_progress` rather than lag where the failure might be in startup:
a pipeline that cannot open never reaches the code that records head and cursor, so its lag
gauge keeps the value it had while healthy.

`ddi_errors_total` is deliberately *not* the page. A failure now retries with backoff, so one
lost commit race increments it and recovers a second later; a permanently stuck stream and a
momentary blip look identical in the counter and quite different in `ddi_pipeline_up`.

## v1 limits

- **A source commit is never split.** The offset is a bare version number, which is what the
  `txn` action stores natively. An oversized commit fails loudly and names the fix rather than
  splitting silently. (`LogStreamBuilder::with_commit_splitting(true)` exists for the
  low-level API and is not used by the daemon.) Bounding source commit size upstream — e.g.
  KDI's `allowed_latency` — makes this a non-issue.
- Mid-stream schema changes surface as a batch whose schema differs from the previous one;
  the target schema is the contract and a mismatch is an error.
- **Realtime publication is append-only, at-most-once, and one payload per pipeline.** A
  merge does not say what a dashboard delta was; a message lost to a crash between commit and
  send is recovered by the client's own baseline reload rather than by an outbox; and two
  payloads describing the same batch would be indistinguishable to a client, so a second
  `ddi_publish` model for one host rejects both.

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
├── publish/         # optional post-commit fan-out; a leaf, never a stage
│   ├── jwt.rs       #   HS256 bearer token for the data plane
│   └── webpubsub.rs #   the one REST call, spoken directly
└── metrics.rs       # prometheus
```

`src/source/` is written against the API an upstream delta-rs contribution would expose
(delta-io/delta-rs#4554), so swapping to it later is a dependency change rather than a
rewrite.

## Licence

Apache-2.0 — see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE). Matching delta-rs and
kafka-delta-ingest, and free for commercial use, modification and redistribution, with a
patent grant.

Every one of the ~434 crates in the dependency tree is permissive too (MIT, Apache-2.0,
BSD, ISC, Zlib and similar). One is MPL-2.0 — `option-ext`, reached through
`deltalake-core → dirs` — whose file-level copyleft imposes nothing on a consumer that
does not modify it. There is no GPL, AGPL, SSPL or non-commercial code anywhere in the
tree.

That is a property of today's lockfile rather than a guarantee, so
[`deny.toml`](deny.toml) encodes the policy and CI enforces it: a `cargo update` that
pulls in something copyleft fails the build instead of going unnoticed.

Distributing the **binary** rather than the source carries the attribution clauses of
those dependencies with it. [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md) lists them,
and `scripts/third-party-notices.py` regenerates it from `Cargo.lock`.
