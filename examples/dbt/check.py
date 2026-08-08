"""Summarise a Delta table, or verify orders_stg against orders_raw.

    python check.py show <path>
    python check.py verify
"""
import csv
import json
import os
import sys

from deltalake import DeltaTable

LAKE = os.environ["DDI_LAKE"]
HERE = os.path.dirname(os.path.abspath(__file__))


def show(path):
    dt_ = DeltaTable(path)
    df = dt_.to_pandas()
    name = os.path.basename(path)
    if len(df) == 0:
        print(f"  {name:<12} v{dt_.version():<3} empty")
        return
    print(
        f"  {name:<12} v{dt_.version():<3} rows={len(df):<4} "
        f"distinct={df.order_id.nunique():<4} max_ts={df._timestamp.max()} "
        f"duplicates={len(df) - df.order_id.nunique()}"
    )


def verify():
    stg = DeltaTable(f"{LAKE}/orders_stg").to_pandas().sort_values("order_id")
    raw = DeltaTable(f"{LAKE}/orders_raw").to_pandas()
    src = {int(r["id"]): r for r in csv.DictReader(open(os.path.join(HERE, "orders.csv")))}

    problems = []
    dupes = len(stg) - stg.order_id.nunique()
    if dupes:
        problems.append(f"{dupes} duplicated key(s)")
    missing = sorted(set(raw.order_id) - set(stg.order_id))
    if missing:
        problems.append(f"{len(missing)} key(s) in raw but not stg: {missing[:8]}")
    extra = sorted(set(stg.order_id) - set(raw.order_id))
    if extra:
        problems.append(f"{len(extra)} key(s) in stg that raw never had: {extra[:8]}")

    # The parse actually happened, and produced the right values.
    for _, row in stg.iterrows():
        s = src[row.order_id]
        if (
            row.customer_id != int(s["user_id"])
            or row.amount != row.order_id * 100
            or row.status != s["status"]
        ):
            problems.append(f"row {row.order_id} was not parsed correctly")
            break

    if problems:
        print("  FAILED: " + "; ".join(problems))
        sys.exit(1)
    print(f"  OK: {len(stg)} orders, each exactly once, JSON parsed and cast")


if __name__ == "__main__":
    show(sys.argv[2]) if sys.argv[1] == "show" else verify()
