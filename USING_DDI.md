# Using `ddi`

`ddi` streams one Delta table into another continuously, exactly once, as a single
binary. It takes what it should do from your dbt project, so the transformation lives in
one place and cannot drift.

It is built to sit alongside a nightly dbt batch over the same tables. dbt owns
correctness and rebuilds the model on its schedule; `ddi` fills the gap in between and
owns latency. **dbt needs no hooks, no macros, and no knowledge that `ddi` exists.**

---

## 1. What it does, and what it refuses

`ddi` streams a model when the transformation is row-by-row: cast, rename, filter, parse
JSON, unnest, intra-row array maths. It refuses anything that needs to remember other
rows, because it could not be correct across batch boundaries:

| Model does | Streamable |
|---|---|
| cast / rename / filter | yes |
| parse a JSON payload | yes |
| unnest an array to child grain | yes |
| `array_sum` / `array_length` etc. within a row | yes |
| `GROUP BY`, aggregates | **no** |
| `JOIN`, or reading a second table | **no** |
| window functions (`OVER`) | **no** |
| `DISTINCT` | **no** |
| materialized as a view | **no** — nothing to append to |

Refusals happen when the config loads, not on the first batch, and the message says what
to change. A pipeline that cannot be correct never starts.

---

## 2. Install

```bash
git clone https://github.com/lutzkuen/ddi.git
cd ddi
cargo build --release          # binary at target/release/ddi
```

On a small machine, `cargo build --profile release-lean` trades some runtime speed for a
build that fits in about 2 GB.

---

## 3. Prepare your tables

Two rules:

1. **`ddi` never creates the target table.** dbt does, on its first run.
2. **Both tables carry a timestamp that increases as rows arrive.** Default name
   `_timestamp`. This is what lets `ddi` and dbt share a table safely — see §7.

```
orders_raw:  order_id BIGINT | data VARCHAR (JSON) | _timestamp TIMESTAMP
orders_stg:  order_id | customer_id | amount | status | _timestamp
```

---

## 4. Write the dbt model normally

Nothing here is `ddi`-specific. This is the model your batch already runs:

```sql
-- models/orders_stg.sql
{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'orders_raw') }}
),
parsed as (
    select
        order_id,
        cast(json_extract_scalar(data, '$.customer_id') as bigint) as customer_id,
        cast(json_extract_scalar(data, '$.amount')      as bigint) as amount,
        json_extract_scalar(data, '$.status')                      as status,
        _timestamp
    from source
)
select * from parsed
```

Add two `meta` keys so `ddi` knows how to resume. They live in dbt, next to the model,
because that is where the rest of the model's meaning lives:

```yaml
# models/schema.yml
models:
  - name: orders_stg
    meta:
      ddi_timestamp: _timestamp   # the default; state it only if yours differs
      ddi_key: order_id           # row identity — see §7
```

Then `dbt compile` (or `dbt run`) so `target/manifest.json` carries the compiled SQL.

### JSON functions

Your model runs in two engines — your warehouse, and `ddi` — so the SQL has to mean the
same thing in both. Trino/Starburst's JSON functions are implemented natively:

`json_extract` · `json_extract_scalar` · `json_size` · `json_array_length` ·
`json_array_contains` · `json_array_get` · `json_exists` · `json_parse` · `json_format` ·
`is_json_scalar` · `json_value` · `json_query`

DuckDB's `json_extract_string` and Spark's `get_json_object` work as aliases of
`json_extract_scalar`, so a model written for either streams unchanged.

Paths support `$`, `.field`, `["field"]` and `[0]`. Following Trino,
`json_extract_scalar` returns **NULL for an object or array** — only `json_extract`
returns those. A missing path is NULL; malformed JSON stops the pipeline, because the
input is a typed column rather than arbitrary text.

---

## 5. Configure `ddi`

`ddi`'s config holds only what dbt has no opinion about: where the manifest is, how hard
to run, and how to authenticate. Everything else is derived from the manifest on every
start, so there is no generated file to keep in sync.

```toml
# ddi.toml
manifest = "/path/to/dbt_project/target/manifest.json"

[runtime]
allowed_latency_secs = 30      # poll interval once caught up

[storage.options]
azure_storage_account_name = "mylake"
azure_storage_account_key  = "..."
```

### Where tables live

`ddi` reads locations from dbt wherever dbt records them — `location_root`, a source's
`delta_table_path`, or `meta: {ddi_location: ...}`. If your adapter names relations
without locating them, give `ddi` a template instead:

```toml
[storage]
uri_template = "abfss://lake@mylake.dfs.core.windows.net/{schema}/{name}"
```

### Azure credentials

Any object-store key works; pick whichever your deployment has:

| Credential | Set |
|---|---|
| Account key | `azure_storage_account_key` |
| SAS token | `azure_storage_sas_key` |
| Bearer token | `azure_storage_token` |
| Service principal | `azure_client_id`, `azure_client_secret`, `azure_tenant_id` |
| Managed identity | nothing — used when no key is given |
| Local dev | `azure_use_azure_cli = "true"` |

The same keys are read from the environment in upper case
(`AZURE_STORAGE_ACCOUNT_NAME`), which is usually how a container gets them, so
`[storage.options]` can be omitted entirely.

URIs take either shape:

```
abfss://container@account.dfs.core.windows.net/path/to/table
az://container/path/to/table          # account comes from the options
```

---

## 6. Run it

```bash
ddi dbt check                  # which models can be streamed, and why not
ddi validate                   # resolve config + credentials, touch no data
ddi status                     # where each pipeline would resume from
ddi once                       # run until caught up, then exit
ddi run                        # run continuously
ddi run -s orders_stg          # just one model
```

`ddi dbt check` is the one to run first:

```
streamable  orders_stg       bronze.orders_raw -> main.orders_stg
no          customer_totals  GROUP BY is not supported: this tool preserves grain ...
no          orders_enriched  depends on 2 upstream relations; ddi streams from exactly one ...

1 streamable, 2 not, of 3 model(s).
```

`ddi validate` resolves the storage backend and credentials without making a request, so
a wrong account or a missing container is caught before a daemon starts rather than on
its first batch.

### Metrics

`ddi run --metrics-addr 0.0.0.0:9100` serves Prometheus text on `/metrics`, labelled by
pipeline: `ddi_rows_written_total`, `ddi_source_lag_versions`,
`ddi_last_source_version`, `ddi_errors_total`, and others.

Alert on `ddi_source_lag_versions` for backlog and `increase(ddi_errors_total[5m])` for a
stopped pipeline. There is no dead-letter queue by design, so any error means a pipeline
has stopped and needs a human.

---

## 7. Sharing a table with the nightly batch

This is the part worth understanding, because it is where a naive setup loses rows.

`ddi` keeps its resume offset in a `txn` action inside the target's own Delta log. Those
actions **survive an overwrite**. So when dbt rebuilds the table, `ddi` would otherwise
wake up still believing it had processed through version N and resume at N+1 — and
everything it streamed while dbt was reading is wiped by the rebuild and never re-emitted:

```
00:00  dbt reads bronze as of 09:00's data
00:03  rows arrive; ddi streams them into silver
00:05  dbt OVERWRITES silver with what it read at 00:00   <- those rows are gone
00:06  ddi resumes past them                              <- and never come back
```

Silent, and it compounds nightly.

`_timestamp` closes it. After a rebuild, `ddi` notices the most recent data commit is not
its own, reads `max(_timestamp)` out of the table, and emits only rows beyond it. Rows
that arrived during the batch's run carry later timestamps by construction, so they come
back; rows the batch did write carry earlier ones, so they are not duplicated. The
schedule stops mattering, because coverage became a property of the row.

`ddi_key` resolves rows sharing *exactly* the boundary instant, which a plain `>` would
drop and a `>=` would duplicate. Set it.

**The one requirement:** the timestamp must never go backwards relative to arrival order.
A late row bearing an older timestamp is indistinguishable from one the rebuild already
wrote, and will be dropped. That suits an append-only stream; it does not suit a table
that gets backfilled.

### What else can happen to a shared table

| Event | What `ddi` does |
|---|---|
| dbt full-refresh | Rescans from the batch's high-water mark; no gaps, no duplicates |
| Rows arrive while dbt runs | Re-emitted afterwards, by timestamp |
| `OPTIMIZE` on either table | Ignored — those commits carry `dataChange: false` |
| `DELETE`/`UPDATE` upstream | Skipped, never propagated (see `change_policy`) |
| `DELETE` of old rows in the target | Left deleted |
| Target dropped and recreated | Refilled from scratch |
| Source dropped and recreated | Starts over, emitting only what is missing |

The rescan after a rebuild is bounded by the source's own file statistics — Delta records
`maxValues` per file — so a rebuild costs a read of the last commit or two, not the whole
history.

---

## 8. Operating notes

- **Decimals, not doubles.** If bronze declares prices as `double`, precision was already
  lost before `ddi` saw the row. Use `decimal(18,4)` at bronze.
- **A cast that cannot be done exactly is an error**, never a silent NULL. There is no
  dead-letter queue: bad input stops the pipeline.
- **Unnest amplification.** A row with a 10k-element array becomes 10k rows. Batches are
  bounded on input bytes *and* `max_output_rows_per_batch`.
- **`app_id` is the offset key** and must be stable forever. It is derived from the model
  name; renaming a dbt model replays that pipeline from the start.
- **Two processes with the same `app_id` on one target** is a config error, not a
  supported mode. Delta's optimistic concurrency keeps it correct, but it wastes work.
- **Deletion vectors in the source** are an explicit error, never a silent wrong result.

### Deletes and updates upstream

`ddi` is append-only. By default a `DELETE`, `UPDATE` or `MERGE` on the source stops the
pipeline rather than guessing. To carry on, set a policy per pipeline:

- `fail` (default) — stop on any change commit
- `skip_change_commits` — consume and ignore those commits
- `ignore_changes` — emit their new files, accepting that rewritten rows appear twice

---

## 9. When something goes wrong

| Message | Meaning |
|---|---|
| `GROUP BY is not supported ...` | The model aggregates; stream a staging model instead and aggregate downstream |
| `depends on 2 upstream relations` | The model joins; denormalise upstream or split it |
| `materialized as "view"` | Nothing on storage to append to |
| `no compiled_code in the manifest` | Run `dbt compile` |
| `Account must be specified` | Credentials are not reaching storage; check `[storage.options]` |
| `target ... was rewritten ... but no dedup_timestamp` | The batch rebuilt the table and `ddi` has no way to tell what it covered; set `ddi_timestamp` |
| `source ... has gone backwards` / `different id` | The source was dropped and recreated; `ddi` restarts from the beginning if `ddi_timestamp` is set |

Logs are quiet by default. `RUST_LOG=debug,delta_delta_ingest=trace` for detail.

---

## 10. Try it without any of your own infrastructure

The repository contains a runnable version of all of the above — vanilla jaffle shop
against local Delta tables, including a simulated batch rebuild and a compaction:

```bash
python -m venv .venv-dbt
.venv-dbt/bin/pip install dbt-core dbt-duckdb deltalake pyarrow pandas
cargo build
./examples/dbt/run_demo.sh
```

Every step asserts the same thing: no order missing, no order twice, JSON parsed and
cast.
