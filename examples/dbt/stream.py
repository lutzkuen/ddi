"""Append orders to the bronze Delta table, with increasing timestamps.

    python stream.py <from> <to>

orders_raw is deliberately raw: an id, an opaque JSON payload, and the instant the
record arrived. Parsing it is the model's job.
"""
import csv
import datetime as dt
import json
import os
import sys

import pyarrow as pa
from deltalake import write_deltalake

LAKE = os.environ["DDI_LAKE"]
HERE = os.path.dirname(os.path.abspath(__file__))
EPOCH = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).replace(tzinfo=None)

rows = list(csv.DictReader(open(os.path.join(HERE, "orders.csv"))))
lo, hi = int(sys.argv[1]), int(sys.argv[2])
chunk = rows[lo:hi]

table = pa.table(
    {
        "order_id": pa.array([int(r["id"]) for r in chunk], pa.int64()),
        "data": pa.array(
            [
                json.dumps(
                    {
                        "customer_id": int(r["user_id"]),
                        "amount": int(r["id"]) * 100,
                        "status": r["status"],
                    }
                )
                for r in chunk
            ],
            pa.string(),
        ),
        # Increases with arrival order — that is the whole contract.
        "_timestamp": pa.array(
            [EPOCH + dt.timedelta(minutes=int(r["id"])) for r in chunk],
            pa.timestamp("us"),
        ),
    }
)
write_deltalake(f"{LAKE}/orders_raw", table, mode="append")
print(f"  orders_raw += rows[{lo}:{hi}]  ({len(chunk)} orders)")
