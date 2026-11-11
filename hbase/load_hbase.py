"""PART 1.2 - Load session time-series and product daily metrics into HBase (via Thrift/happybase).

    user_sessions          <- raw sessions_*.json (a representative subset, --max-sessions)
    product_daily_metrics  <- aggregated by Spark from the curated Parquet tables
                              (run spark/01_clean_normalize.py first)

Run hbase/schema.hbase first to get pre-split tables; otherwise they are created here unsplit.

Usage:  python hbase/load_hbase.py [--max-sessions 300000] [--skip-sessions] [--skip-metrics]
"""
import argparse
import datetime as dt
import glob
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import happybase

from common.config import HBASE_HOST, HBASE_PORT, RAW_DIR, curated, get_spark

MAX_TS = 9999999999
BATCH_SIZE = 2000

FAMILIES = {
    "user_sessions": {"s": dict(max_versions=1), "pv": dict(max_versions=1), "c": dict(max_versions=1)},
    "product_daily_metrics": {"m": dict(max_versions=1)},
}


def session_row_key(user_id, start_time, session_id):
    epoch = int(dt.datetime.fromisoformat(start_time).replace(tzinfo=dt.timezone.utc).timestamp())
    return f"{user_id}#{MAX_TS - epoch:010d}#{session_id}".encode()


def b(value):
    return b"" if value is None else str(value).encode()


def session_to_cells(s):
    cart = s["cart_contents"]
    cells = {
        b"s:session_id": b(s["session_id"]),
        b"s:start_time": b(s["start_time"]),
        b"s:end_time": b(s["end_time"]),
        b"s:duration_seconds": b(s["duration_seconds"]),
        b"s:device_type": b(s["device_profile"]["type"]),
        b"s:os": b(s["device_profile"]["os"]),
        b"s:browser": b(s["device_profile"]["browser"]),
        b"s:referrer": b(s["referrer"]),
        b"s:country": b(s["geo_data"]["country"]),
        b"s:city": b(s["geo_data"]["city"]),
        b"s:ip_address": b(s["geo_data"]["ip_address"]),
        b"s:conversion_status": b(s["conversion_status"]),
        b"s:num_page_views": b(len(s["page_views"])),
        b"s:num_products_viewed": b(len(s["viewed_products"])),
        b"s:cart_value": b(round(sum(v["quantity"] * v["price"] for v in cart.values()), 2)),
    }
    for i, pv in enumerate(s["page_views"]):
        cells[f"pv:{i:03d}".encode()] = "|".join(
            str(pv[k]) if pv[k] is not None else "" for k in
            ("timestamp", "page_type", "product_id", "category_id", "view_duration")).encode()
    for product_id, item in cart.items():
        cells[f"c:{product_id}".encode()] = f"{item['quantity']}|{item['price']}".encode()
    return cells


def ensure_tables(conn):
    existing = {t.decode() for t in conn.tables()}
    for name, families in FAMILIES.items():
        if name not in existing:
            print(f"  creating table {name} (unsplit - run hbase/schema.hbase for pre-split tables)")
            conn.create_table(name, families)


def load_sessions(conn, max_sessions):
    table = conn.table("user_sessions")
    loaded, start = 0, time.time()
    for path in sorted(glob.glob(os.path.join(RAW_DIR, "sessions_*.json")),
                       key=lambda p: int(p.rsplit("_", 1)[1].split(".")[0])):
        with open(path, encoding="utf-8") as f:
            sessions = json.load(f)
        with table.batch(batch_size=BATCH_SIZE) as batch:
            for s in sessions:
                if loaded >= max_sessions:
                    break
                batch.put(session_row_key(s["user_id"], s["start_time"], s["session_id"]), session_to_cells(s))
                loaded += 1
        print(f"  {os.path.basename(path)}: {loaded:,} sessions loaded ({loaded / (time.time() - start):,.0f} rows/s)")
        if loaded >= max_sessions:
            break
    return loaded


def load_product_metrics(conn):
    """Daily product funnel metrics computed with Spark, then written row by row to HBase."""
    from pyspark.sql import functions as F

    spark = get_spark("hbase_product_daily_metrics")
    pv = spark.read.parquet(curated("page_views")).where(F.col("page_type") == "product_detail")
    views = pv.groupBy("product_id", F.col("view_date").alias("day")).agg(
        F.count("*").alias("views"),
        F.sum("view_duration").alias("view_seconds"),
        F.countDistinct("session_id").alias("unique_sessions"))
    carts = (spark.read.parquet(curated("cart_items"))
             .groupBy("product_id", F.to_date("start_time").alias("day"))
             .agg(F.count("*").alias("cart_adds"), F.sum("quantity").alias("cart_units")))
    sales = (spark.read.parquet(curated("transaction_items"))
             .groupBy("product_id", F.to_date("txn_time").alias("day"))
             .agg(F.countDistinct("transaction_id").alias("orders"),
                  F.sum("quantity").alias("units_sold"),
                  F.round(F.sum("line_total"), 2).alias("revenue")))
    metrics = (views.join(carts, ["product_id", "day"], "full")
               .join(sales, ["product_id", "day"], "full")
               .fillna(0)
               .withColumn("day", F.date_format("day", "yyyyMMdd")))

    table = conn.table("product_daily_metrics")
    cols = ["views", "view_seconds", "unique_sessions", "cart_adds", "cart_units", "orders", "units_sold", "revenue"]
    n = 0
    with table.batch(batch_size=BATCH_SIZE) as batch:
        for row in metrics.toLocalIterator():
            batch.put(f"{row['product_id']}#{row['day']}".encode(), {f"m:{c}".encode(): b(row[c]) for c in cols})
            n += 1
    spark.stop()
    return n


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-sessions", type=int, default=300000)
    parser.add_argument("--skip-sessions", action="store_true")
    parser.add_argument("--skip-metrics", action="store_true")
    args = parser.parse_args()

    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=120000)
    ensure_tables(conn)
    start = time.time()
    if not args.skip_sessions:
        print(f"Loading up to {args.max_sessions:,} sessions into user_sessions ...")
        print(f"  -> {load_sessions(conn, args.max_sessions):,} rows")
    if not args.skip_metrics:
        print("Computing product_daily_metrics with Spark and loading ...")
        print(f"  -> {load_product_metrics(conn):,} rows")
    print(f"Done in {time.time() - start:,.1f}s")


if __name__ == "__main__":
    main()
