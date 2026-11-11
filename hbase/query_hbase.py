"""PART 1.2 - Example HBase queries (happybase / Thrift).

  1. most recent sessions of a user             -> prefix scan, newest first (reverse timestamp)
  2. a user's sessions inside a time window       -> bounded range scan computed from the row key
  3. the full click-stream of one session         -> single-row get on the 'pv' family
  4. a product's daily view-to-purchase funnel    -> range scan on product_id#yyyyMMdd

Usage:  python hbase/query_hbase.py [--user user_000042] [--product prod_00123]
"""
import argparse
import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import happybase
import pandas as pd

from common.config import HBASE_HOST, HBASE_PORT, result_path

MAX_TS = 9999999999


def rev(ts):
    return MAX_TS - int(ts.replace(tzinfo=dt.timezone.utc).timestamp())


def decode(data):
    return {k.decode().split(":", 1)[1]: v.decode() for k, v in data.items()}


def recent_sessions(table, user_id, limit=5):
    """Q1: latest sessions; only the small 's' family is read."""
    rows = table.scan(row_prefix=f"{user_id}#".encode(), columns=[b"s"], limit=limit)
    return [dict(row_key=k.decode(), **decode(d)) for k, d in rows]


def sessions_between(table, user_id, start, end):
    """Q2: newest-first keys mean the *end* of the window gives the smaller row key."""
    row_start = f"{user_id}#{rev(end):010d}".encode()
    row_stop = f"{user_id}#{rev(start):010d}~".encode()
    cols = [b"s:start_time", b"s:duration_seconds", b"s:conversion_status", b"s:cart_value", b"s:device_type"]
    return [dict(row_key=k.decode(), **decode(d)) for k, d in table.scan(row_start=row_start, row_stop=row_stop, columns=cols)]


def session_clickstream(table, row_key):
    """Q3: the page-view family of one row, ordered by the qualifier sequence."""
    data = table.row(row_key.encode(), columns=[b"pv", b"c"])
    views, cart = [], {}
    for k in sorted(data):
        fam, qual = k.decode().split(":", 1)
        val = data[k].decode().split("|")
        if fam == "pv":
            views.append(dict(seq=int(qual), timestamp=val[0], page_type=val[1], product_id=val[2] or None,
                              category_id=val[3] or None, view_duration=int(val[4])))
        else:
            cart[qual] = dict(quantity=int(val[0]), price=float(val[1]))
    return views, cart


def product_funnel(table, product_id, start_day, end_day):
    """Q4: daily metrics for a product over a date range + conversion rates."""
    rows = table.scan(row_start=f"{product_id}#{start_day}".encode(),
                      row_stop=f"{product_id}#{end_day}~".encode())
    df = pd.DataFrame([dict(day=k.decode().split("#")[1], **decode(d)) for k, d in rows])
    if df.empty:
        return df
    num_cols = [c for c in df.columns if c != "day"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    return df


def timed(label, fn, *args):
    start = time.perf_counter()
    out = fn(*args)
    print(f"\n=== {label}  ({(time.perf_counter() - start) * 1000:.1f} ms)")
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default="user_000042")
    parser.add_argument("--product", default=None)
    args = parser.parse_args()
    pd.set_option("display.width", 200)

    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=120000)
    sessions = conn.table("user_sessions")
    metrics = conn.table("product_daily_metrics")

    recent = timed(f"Q1 five most recent sessions of {args.user}", recent_sessions, sessions, args.user, 5)
    print(pd.DataFrame(recent)[["row_key", "start_time", "duration_seconds", "device_type", "referrer",
                                "num_page_views", "conversion_status", "cart_value"]].to_string(index=False))

    end = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    window = timed(f"Q2 sessions of {args.user} in the last 30 days", sessions_between, sessions, args.user,
                   end - dt.timedelta(days=30), end)
    print(pd.DataFrame(window).to_string(index=False) if window else "  (none)")

    if recent:
        views, cart = timed(f"Q3 click-stream of {recent[0]['session_id']}", session_clickstream,
                            sessions, recent[0]["row_key"])
        print(pd.DataFrame(views).to_string(index=False))
        print("  cart:", cart)

    product = args.product
    if product is None:  # default: a product this user viewed recently
        product = next((v["product_id"] for r in recent for v in session_clickstream(sessions, r["row_key"])[0]
                        if v["product_id"]), "prod_00123")
    today = dt.date.today()
    funnel = timed(f"Q4 daily funnel of {product}, last 90 days", product_funnel, metrics, product,
                   (today - dt.timedelta(days=90)).strftime("%Y%m%d"), today.strftime("%Y%m%d"))
    if not funnel.empty:
        print(funnel.head(10).to_string(index=False))
        print(f"  ... {len(funnel)} daily rows in total")
        tot = funnel[["views", "cart_adds", "orders", "units_sold", "revenue"]].sum()
        print(f"  totals: {tot.to_dict()}")
        print(f"  view->cart {tot.cart_adds / max(tot.views, 1):.1%}   view->order {tot.orders / max(tot.views, 1):.1%}")

    pd.DataFrame(recent).to_csv(result_path("hbase_recent_sessions_sample.csv"), index=False)
    funnel.to_csv(result_path("hbase_product_funnel_sample.csv"), index=False)


if __name__ == "__main__":
    main()
