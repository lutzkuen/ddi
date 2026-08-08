# `orders_raw` → `orders_stg`, with a batch job and a stream on the same table

dbt rebuilds `orders_stg` from scratch on its own schedule. `ddi` streams the same model
into the same table continuously in between. Neither knows the other exists.

The dbt side is **vanilla**: no hooks, no macros, nothing that mentions `ddi`. Bronze is
deliberately raw — an id, an opaque JSON payload, and the instant the record arrived:

```
orders_raw:  order_id BIGINT | data VARCHAR (JSON) | _timestamp TIMESTAMP
orders_stg:  order_id | customer_id | amount | status | _timestamp
```

```bash
python -m venv .venv-dbt
.venv-dbt/bin/pip install dbt-core dbt-duckdb deltalake pyarrow pandas
cargo build
./examples/dbt/run_demo.sh
```

After every step it asserts the same three things: no order missing, no order twice, JSON
parsed and cast.

## The step that matters

```
4. orders arrive WHILE the batch is running — the window that makes this hard
  orders_raw += rows[9:13]  (4 orders)
  orders_stg   v2   rows=13   distinct=13   max_ts=00:13:00
   ...now the batch commits what it read, wiping rows 10-13 from the table
  batch rebuilt orders_stg with the 9 orders it saw
  orders_stg   v3   rows=9    distinct=9    max_ts=00:09:00
   ddi must put back exactly the ones the batch never saw:
  orders_stg   v4   rows=13   distinct=13   max_ts=00:13:00
  OK: 13 orders, each exactly once, JSON parsed and cast
```

A batch job reads bronze at one instant and commits its output later. Rows landing in
between are in neither its snapshot nor — once it overwrites — the target, even though
`ddi` had already streamed them. Nothing in the Delta log distinguishes them afterwards,
because `ddi`'s own offset survives the overwrite and points past them.

`_timestamp` is what recovers them. `ddi` reads `max(_timestamp)` out of the rebuilt table
and re-emits everything beyond it. The rows that arrived during the batch's run carry
later timestamps by construction, so they come back; the rows the batch did write carry
earlier ones, so they are not duplicated.

## Where the settings live

In dbt, next to the model, so `ddi` needs no separate configuration:

```yaml
models:
  - name: orders_stg
    meta:
      ddi_timestamp: _timestamp   # the default
      ddi_key: order_id
```

`ddi dbt convert` reads them out of the manifest. The key resolves rows sharing exactly
the watermark instant — without it, an order arriving in the same second as the batch's
newest would be assumed covered and dropped.

## JSON, in two engines

The model uses `json_extract_string`, which is DuckDB's spelling, because DuckDB is what
runs the batch here. `ddi` registers that name alongside Trino/Starburst's
`json_extract_scalar` and Spark's `get_json_object`, all with identical behaviour, so the
same model file streams unchanged whichever engine sits on the batch side.

Malformed JSON stops the pipeline rather than nulling a column — there is no dead-letter
queue by design.

## What is example-specific

`plugins/delta_write.py` exists only because dbt-duckdb can *read* Delta but not write it;
Starburst and Databricks write it natively. It shells out to `_to_delta.py` because
importing pyarrow into a process that has already loaded duckdb segfaults it — the two
bundle different Arrow ABIs. Reading Delta in-process is fine.

`orders.csv` is jaffle shop's `raw_orders` seed, vendored so the demo runs offline.

The same scenarios are asserted without dbt, deterministically, in
[`tests/hardening.rs`](../../tests/hardening.rs) — including source and target compaction,
deletes, and both tables being dropped and recreated.
