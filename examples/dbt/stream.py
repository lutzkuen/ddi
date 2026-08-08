"""Append raw orders to the bronze Delta table — stands in for the upstream stream.

    python stream.py <from> <to>      # rows [from:to) of raw_orders.csv
"""
import csv
import os
import sys

import pyarrow as pa
from deltalake import write_deltalake

LAKE = os.environ["DDI_LAKE"]
HERE = os.path.dirname(os.path.abspath(__file__))

rows = list(csv.DictReader(open(os.path.join(HERE, "raw_orders.csv"))))
lo, hi = int(sys.argv[1]), int(sys.argv[2])
chunk = rows[lo:hi]

table = pa.table(
    {
        "id": pa.array([int(r["id"]) for r in chunk], pa.int64()),
        "user_id": pa.array([int(r["user_id"]) for r in chunk], pa.int64()),
        "order_date": pa.array([r["order_date"] for r in chunk], pa.string()),
        "status": pa.array([r["status"] for r in chunk], pa.string()),
    }
)
# Append, so each call is one new Delta commit for ddi to pick up.
write_deltalake(f"{LAKE}/raw_orders", table, mode="append")
print(f"  bronze += raw_orders[{lo}:{hi}]  ({len(chunk)} rows)")
