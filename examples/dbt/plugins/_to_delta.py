"""parquet -> Delta, run out of process. See delta_write.py for why."""
import sys
import pyarrow.parquet as pq
from deltalake import write_deltalake

parquet_path, delta_path = sys.argv[1], sys.argv[2]
table = pq.read_table(parquet_path)
write_deltalake(delta_path, table, mode="overwrite", schema_mode="overwrite")
print(f"[delta_write] {table.num_rows} rows -> {delta_path}")
