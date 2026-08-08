# dbt and `ddi` on the same table

A runnable version of the two-speed lakehouse: dbt rebuilds a model nightly and owns
correctness, `ddi` streams the same transformation continuously and owns latency, and
they share one Delta table without corrupting each other.

The dbt side is vanilla [jaffle shop](https://github.com/dbt-labs/jaffle_shop_duckdb) —
`stg_orders.sql` is the upstream file unchanged. **No hooks, no macros, nothing that
mentions `ddi`.** dbt rebuilds the whole table on every run, as a nightly batch does.

```bash
python -m venv .venv-dbt
.venv-dbt/bin/pip install dbt-core dbt-duckdb deltalake pyarrow pandas
cargo build
./examples/dbt/run_demo.sh
```

Every step asserts the same invariant: every bronze key appears in silver exactly once,
with the right values.

```
1. bronze gets its first orders, then dbt builds silver
  raw_orders   v0   rows=5    distinct=5    max=5    duplicates=0
  stg_orders   v0   rows=5    distinct=5    max=5    duplicates=0

2. ddi works out which models it can stream, from dbt's own manifest
streamable  stg_orders                   bronze.raw_orders -> main.stg_orders

3. new orders arrive; ddi streams them into dbt's table
  stg_orders   v1   rows=7    distinct=7    max=7    duplicates=0

4. the nightly dbt run — it overwrites the table ddi has been appending to
  stg_orders   v2   rows=7    distinct=7    max=7    duplicates=0

5. ddi wakes up to a rebuilt target. Every key is already there, so it emits nothing.
  WARN target was rebuilt by another writer; rescanning and skipping rows at or below
       the target's highest dedup_key  own_offset=v2+0 rescan_from=v0+0
  stg_orders   v3   rows=7    distinct=7    max=7    duplicates=0

...
  raw_orders   v3   rows=40   distinct=40   max=40   duplicates=0
  stg_orders   v7   rows=40   distinct=40   max=40   duplicates=0
  OK: 40 rows, every bronze key present exactly once, values match
```

Step 5 is the one that matters. `ddi`'s offset lives in a `txn` action in the target's
log, and those survive an overwrite — so after a rebuild it would otherwise resume from a
position describing rows that no longer exist. `dedup_key` is what makes that safe without
dbt's cooperation: `ddi` notices the most recent data commit is not its own, rescans, and
skips every row at or below the highest `order_id` the rebuild left behind.

## What is example-specific

`plugins/delta_write.py` exists only because dbt-duckdb can *read* Delta but not write it.
Starburst and Databricks write Delta natively, so nothing equivalent is needed there. It
shells out to `_to_delta.py` rather than converting in-process: importing pyarrow into a
process that has already loaded duckdb segfaults it, because duckdb bundles its own Arrow
and the two ABIs collide. Reading Delta in-process is fine — only pyarrow is the problem.

`dedup_key = "order_id"` is appended to the generated config by the script. The manifest
cannot tell you which column advances with arrival order, so it is the one thing you
supply. It must be non-decreasing in the order rows reach the source: a late row carrying
an older key is indistinguishable from one the rebuild already wrote, and gets dropped.

`raw_orders.csv` is jaffle shop's seed, vendored so the demo runs offline.
