#!/usr/bin/env bash
# orders_raw -> orders_stg, with a batch job and a stream writing the same table.
#
#   ./examples/dbt/run_demo.sh
#
# dbt is vanilla: no hooks, no macros, nothing that mentions ddi. It rebuilds
# orders_stg from scratch on every run, exactly as a nightly batch does. ddi streams
# the same model continuously into the same table in between.
#
# After every step: no order missing, no order twice, JSON parsed and cast.
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
stream()   { "$PY" "$HERE/stream.py" "$@"; }
show()     { "$PY" "$HERE/check.py" show "$DDI_LAKE/$1"; }
verify()   { "$PY" "$HERE/check.py" verify; }
optimize() { "$PY" -c "
from deltalake import DeltaTable
import sys
dt = DeltaTable('$DDI_LAKE/$1'); dt.optimize.compact()
print(f'  compacted $1 -> v{DeltaTable(\"$DDI_LAKE/$1\").version()}')"; }

echo
echo "1. first orders arrive; dbt builds orders_stg from the JSON payload"
stream 0 5
dbt_run
show orders_raw; show orders_stg; verify

echo
echo "2. ddi reads dbt's manifest to see what it can stream"
cat > "$DDI_LAKE/ddi.toml" <<TOML
[dbt]
manifest     = "$HERE/target/manifest.json"
uri_template = "$DDI_LAKE/{name}"
TOML
"$DDI" dbt check --config "$DDI_LAKE/ddi.toml"
"$DDI" dbt convert --config "$DDI_LAKE/ddi.toml" --out "$DDI_LAKE/pipelines.toml" >/dev/null
echo "  dedup settings taken from the model's meta: in dbt, next to the model"
grep -E 'dedup_' "$DDI_LAKE/pipelines.toml" | sed 's/^/    /'

echo
echo "3. more orders; ddi streams them"
stream 5 9
ddi_once
show orders_raw; show orders_stg; verify

echo
echo "4. orders arrive WHILE the batch is running — the window that makes this hard"
echo "   the batch reads orders_raw now, at $(show orders_raw | grep -o 'max_ts=[^ ]*')"
BATCH_SNAPSHOT_ROWS=9
stream 9 13                 # these land after the batch read, before it commits
ddi_once                    # and ddi streams them
show orders_stg
echo "   ...now the batch commits what it read, wiping rows 10-13 from the table"
"$PY" - <<PY
import os, pyarrow as pa
from deltalake import DeltaTable, write_deltalake
lake = os.environ["DDI_LAKE"]
stg = DeltaTable(f"{lake}/orders_stg").to_pandas()
snapshot = stg[stg.order_id <= $BATCH_SNAPSHOT_ROWS]
write_deltalake(f"{lake}/orders_stg", pa.Table.from_pandas(snapshot, preserve_index=False),
                mode="overwrite", schema_mode="overwrite")
print(f"  batch rebuilt orders_stg with the {len(snapshot)} orders it saw")
PY
show orders_stg
echo "   ddi must put back exactly the ones the batch never saw:"
ddi_once
show orders_stg; verify

echo
echo "5. a real dbt rebuild over the top"
dbt_run
ddi_once
show orders_stg; verify

echo
echo "6. both tables get compacted"
optimize orders_raw
optimize orders_stg
ddi_once
show orders_raw; show orders_stg; verify

echo
echo "7. the stream continues past all of it"
stream 13 40
ddi_once
show orders_raw; show orders_stg; verify

echo
echo "done — dbt never knew ddi existed."
