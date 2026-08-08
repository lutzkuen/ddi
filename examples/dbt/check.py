"""Report a Delta table, and verify silver against the raw source.

    python check.py show <path>    # one summary line
    python check.py verify         # assert silver == the raw rows, exactly once
"""
import csv
import os
import sys

from deltalake import DeltaTable

LAKE = os.environ["DDI_LAKE"]
HERE = os.path.dirname(os.path.abspath(__file__))


def show(path):
    dt = DeltaTable(path)
    df = dt.to_pandas()
    key = "order_id" if "order_id" in df.columns else "id"
    name = os.path.basename(path)
    print(
        f"  {name:<12} v{dt.version():<3} rows={len(df):<4} "
        f"distinct={df[key].nunique():<4} max={df[key].max():<4} "
        f"duplicates={len(df) - df[key].nunique()}"
    )


def verify():
    silver = DeltaTable(f"{LAKE}/stg_orders").to_pandas().sort_values("order_id")
    bronze = DeltaTable(f"{LAKE}/raw_orders").to_pandas()
    raw = {int(r["id"]): r for r in csv.DictReader(open(os.path.join(HERE, "raw_orders.csv")))}

    problems = []
    dupes = len(silver) - silver.order_id.nunique()
    if dupes:
        problems.append(f"{dupes} duplicated key(s)")

    missing = set(bronze.id) - set(silver.order_id)
    if missing:
        problems.append(f"{len(missing)} key(s) in bronze but not silver: {sorted(missing)[:10]}")

    extra = set(silver.order_id) - set(bronze.id)
    if extra:
        problems.append(f"{len(extra)} key(s) in silver that bronze never had")

    for _, row in silver.iterrows():
        src = raw[row.order_id]
        if (
            row.customer_id != int(src["user_id"])
            or row.status != src["status"]
            or str(row.order_date) != src["order_date"]
        ):
            problems.append(f"row {row.order_id} does not match the source")
            break

    if problems:
        print("  FAILED: " + "; ".join(problems))
        sys.exit(1)
    print(f"  OK: {len(silver)} rows, every bronze key present exactly once, values match")


if __name__ == "__main__":
    if sys.argv[1] == "show":
        show(sys.argv[2])
    else:
        verify()
