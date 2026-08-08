#!/usr/bin/env bash
# dbt and ddi writing the same Delta table, and neither corrupting the other.
#
#   ./examples/dbt/run_demo.sh
#
# dbt is vanilla jaffle shop: no hooks, no macros, nothing that mentions ddi. It
# rebuilds silver.stg_orders from scratch every run, exactly as a nightly batch does.
# ddi streams the same transformation continuously into the same table in between.
#
# The invariant asserted after every step: every bronze key appears in silver exactly
# once, with the right values.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
export DDI_LAKE="${DDI_LAKE:-/tmp/ddi-dbt-demo}"

PY="${PY:-$ROOT/.venv-dbt/bin/python}"
DBT="${DBT:-$ROOT/.venv-dbt/bin/dbt}"
DDI="${DDI:-$ROOT/target/debug/ddi}"

for bin in "$PY" "$DBT"; do
  [ -x "$bin" ] || { echo "missing $bin — see examples/dbt/README.md for setup"; exit 1; }
done
[ -x "$DDI" ] || { echo "missing $DDI — run: cargo build"; exit 1; }

rm -rf "$DDI_LAKE"; mkdir -p "$DDI_LAKE"
export PYTHONPATH="$HERE/plugins"
export RUST_LOG="${RUST_LOG:-error}"

dbt_run() { (cd "$HERE" && "$DBT" run --profiles-dir . -q 2>&1 | grep -E 'delta_write|Error|Failure' || true); }
ddi_once() { "$DDI" once --config "$DDI_LAKE/pipelines.toml"; }
show()     { "$PY" "$HERE/check.py" show "$DDI_LAKE/$1"; }
verify()   { "$PY" "$HERE/check.py" verify; }

echo
echo "1. bronze gets its first orders, then dbt builds silver"
"$PY" "$HERE/stream.py" 0 5
dbt_run
show raw_orders; show stg_orders; verify

echo
echo "2. ddi works out which models it can stream, from dbt's own manifest"
cat > "$DDI_LAKE/ddi.toml" <<TOML
[dbt]
manifest     = "$HERE/target/manifest.json"
uri_template = "$DDI_LAKE/{name}"
TOML
"$DDI" dbt check --config "$DDI_LAKE/ddi.toml"
"$DDI" dbt convert --config "$DDI_LAKE/ddi.toml" --out "$DDI_LAKE/pipelines.toml" >/dev/null
# The one thing the manifest cannot tell us: which column advances with arrival order.
printf 'dedup_key = "order_id"\n' >> "$DDI_LAKE/pipelines.toml"
echo "  wrote $DDI_LAKE/pipelines.toml"

echo
echo "3. new orders arrive; ddi streams them into dbt's table"
"$PY" "$HERE/stream.py" 5 7
ddi_once
show raw_orders; show stg_orders; verify

echo
echo "4. the nightly dbt run — it overwrites the table ddi has been appending to"
dbt_run
show stg_orders; verify

echo
echo "5. ddi wakes up to a rebuilt target. Every key is already there, so it emits nothing."
RUST_LOG=warn,delta_delta_ingest=info ddi_once 2>&1 | grep -E 'rebuilt|committed' || true
show stg_orders; verify

echo
echo "6. the stream continues past the rebuild"
"$PY" "$HERE/stream.py" 7 20
ddi_once
show raw_orders; show stg_orders; verify

echo
echo "7. and once more around the loop"
dbt_run
ddi_once
"$PY" "$HERE/stream.py" 20 40
ddi_once
show raw_orders; show stg_orders; verify

echo
echo "done — dbt never knew ddi existed."
