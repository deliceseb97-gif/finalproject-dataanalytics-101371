"""PART 3 - Integrated analytical query: Customer Lifetime Value (CLV) estimation and segmentation.

Business question
    Which customers are worth the most over the next 12 months, how does browsing engagement
    relate to value, and which segments should marketing prioritise?

Data sources
    MongoDB  users         -> profile + geo (document store, source of truth for customers)
    MongoDB  transactions  -> per-user order history, aggregated *inside* MongoDB ($group
                              pushdown) so only ~10k summary rows cross the network
    HBase    user_sessions -> engagement (session count, duration, pages, conversion) read from
                              the small 's' family only via a full scan with column pruning
    Spark                   -> joins the three sources, computes RFM scores with window
                              functions (ntile), the CLV model and segment roll-ups

CLV model (simple, transparent, standard in retail analytics)
    observation window T   = days between first and last transaction in the data (~90)
    purchase_rate          = orders / T  (orders per day)
    expected 12m orders    = purchase_rate * 365 * retention_probability
    retention_probability  = exp(-recency_days / 30)   (a customer silent for 30 days ~ 37 %)
    CLV_12m                = expected 12m orders * avg_order_value * gross_margin

Usage:  python integration/clv_integration.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import happybase
from pymongo import MongoClient
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.config import (HBASE_HOST, HBASE_PORT, MONGO_DB, MONGO_URI, curated, get_spark,
                           result_path, to_pandas)

GROSS_MARGIN_FALLBACK = 0.25


def extract_mongo(spark, db):
    t0 = time.time()
    users = [(u["_id"], u["geo"]["country"], u["geo"]["state"], u["registration_date"])
             for u in db.users.find({}, {"geo": 1, "registration_date": 1})]
    users_df = spark.createDataFrame(users, "user_id string, country string, state string, registration_date timestamp")

    # Aggregations are pushed down to MongoDB so only one summary row per user is transferred.
    order_summary = db.transactions.aggregate([
        {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "revenue": {"$sum": "$total"},
                    "discount": {"$sum": "$discount"},
                    "first": {"$min": "$timestamp"}, "last": {"$max": "$timestamp"}}},
    ], allowDiskUse=True)
    # Revenue per user and category (item.category_id is denormalised) -> margin-weighted profit.
    category_mix = db.transactions.aggregate([
        {"$unwind": "$items"},
        {"$group": {"_id": {"user": "$user_id", "cat": "$items.category_id"}, "rev": {"$sum": "$items.subtotal"}}},
        {"$lookup": {"from": "categories", "localField": "_id.cat", "foreignField": "_id", "as": "c"}},
        {"$group": {"_id": "$_id.user",
                    "gross": {"$sum": "$rev"},
                    "profit": {"$sum": {"$multiply": ["$rev", {"$ifNull": [{"$first": "$c.avg_profit_margin"},
                                                                         GROSS_MARGIN_FALLBACK]}]}},
                    "top": {"$top": {"sortBy": {"rev": -1}, "output": "$_id.cat"}}}},
    ], allowDiskUse=True)
    mix = {d["_id"]: (d["profit"] / d["gross"] if d["gross"] else GROSS_MARGIN_FALLBACK, d["top"]) for d in category_mix}
    rows = [(d["_id"], d["orders"], float(d["revenue"]), float(d["discount"]), d["first"], d["last"],
             float(mix.get(d["_id"], (GROSS_MARGIN_FALLBACK, None))[0]), mix.get(d["_id"], (None, None))[1])
            for d in order_summary]
    orders_df = spark.createDataFrame(rows, "user_id string, orders long, revenue double, discount double, "
                                            "first_purchase timestamp, last_purchase timestamp, "
                                            "gross_margin double, top_category string")
    print(f"MongoDB: {len(users):,} users, {len(rows):,} order summaries ({time.time() - t0:.1f}s)")
    return users_df, orders_df


def extract_hbase(spark):
    """Scan only the session-summary columns; user_id is the row-key prefix."""
    t0 = time.time()
    table = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=300000).table("user_sessions")
    cols = [b"s:duration_seconds", b"s:num_page_views", b"s:num_products_viewed",
            b"s:conversion_status", b"s:cart_value", b"s:start_time"]
    rows = []
    for key, d in table.scan(columns=cols, batch_size=5000):
        rows.append((key.split(b"#", 1)[0].decode(), int(d[cols[0]]), int(d[cols[1]]), int(d[cols[2]]),
                     d[cols[3]].decode(), float(d[cols[4]]), d[cols[5]].decode()))
    df = spark.createDataFrame(rows, "user_id string, duration_seconds long, num_page_views long, "
                                     "num_products_viewed long, conversion_status string, cart_value double, "
                                     "start_time string")
    print(f"HBase: {len(rows):,} session rows ({time.time() - t0:.1f}s)")
    return df


def main():
    spark = get_spark("clv_integration")
    db = MongoClient(MONGO_URI)[MONGO_DB]

    users_df, orders_df = extract_mongo(spark, db)
    sessions_df = extract_hbase(spark)

    engagement = sessions_df.groupBy("user_id").agg(
        F.count("*").alias("sessions"),
        F.round(F.avg("duration_seconds"), 0).alias("avg_session_seconds"),
        F.round(F.avg("num_page_views"), 2).alias("avg_pages_per_session"),
        F.round(F.avg("num_products_viewed"), 2).alias("avg_products_viewed"),
        F.sum(F.when(F.col("conversion_status") == "converted", 1).otherwise(0)).alias("converted_sessions"),
        F.sum(F.when(F.col("conversion_status") == "abandoned", 1).otherwise(0)).alias("abandoned_sessions"),
        F.round(F.sum(F.when(F.col("conversion_status") == "abandoned", F.col("cart_value")).otherwise(0)), 2)
         .alias("abandoned_cart_value"),
    ).withColumn("session_conversion_rate", F.round(F.col("converted_sessions") / F.col("sessions"), 4))

    bounds = orders_df.agg(F.min("first_purchase").alias("t0"), F.max("last_purchase").alias("t1")).first()
    window_days = max((bounds.t1 - bounds.t0).days, 1)

    clv = (
        users_df.join(orders_df, "user_id", "left").join(engagement, "user_id", "left")
        .fillna(0, subset=["orders", "revenue", "discount", "sessions", "converted_sessions",
                           "abandoned_sessions", "abandoned_cart_value"])
        .withColumn("recency_days", F.datediff(F.lit(bounds.t1), F.col("last_purchase")))
        .withColumn("avg_order_value", F.round(F.col("revenue") / F.greatest(F.col("orders"), F.lit(1)), 2))
        .withColumn("purchase_rate_per_day", F.col("orders") / F.lit(window_days))
        .withColumn("retention_prob", F.exp(-F.coalesce(F.col("recency_days"), F.lit(365)) / 30.0))
        .withColumn("expected_orders_12m", F.round(F.col("purchase_rate_per_day") * 365 * F.col("retention_prob"), 2))
        .withColumn("clv_12m", F.round(F.col("expected_orders_12m") * F.col("avg_order_value")
                                       * F.coalesce(F.col("gross_margin"), F.lit(GROSS_MARGIN_FALLBACK)), 2))
        .withColumn("historic_gross_profit", F.round(F.col("revenue") * F.coalesce(F.col("gross_margin"), F.lit(GROSS_MARGIN_FALLBACK)), 2))
    )

    # RFM quintiles with window functions (5 = best).
    w = lambda col, asc=True: Window.orderBy(F.col(col).asc() if asc else F.col(col).desc())
    clv = (clv.withColumn("R", F.ntile(5).over(w("recency_days", asc=False)))
              .withColumn("F", F.ntile(5).over(w("orders")))
              .withColumn("M", F.ntile(5).over(w("revenue")))
              .withColumn("rfm_score", F.col("R") + F.col("F") + F.col("M"))
              .withColumn("segment", F.when((F.col("R") >= 4) & (F.col("F") >= 4) & (F.col("M") >= 4), "Champions")
                                      .when((F.col("F") >= 4) & (F.col("M") >= 3), "Loyal high-value")
                                      .when((F.col("R") <= 2) & (F.col("M") >= 4), "At-risk big spenders")
                                      .when(F.col("R") <= 2, "Hibernating")
                                      .when(F.col("F") <= 2, "Promising / low-frequency")
                                      .otherwise("Core"))
              .withColumn("clv_decile", F.ntile(10).over(w("clv_12m", asc=False))))
    clv.cache()

    per_user = to_pandas(clv.drop("registration_date", "first_purchase", "last_purchase"))
    per_user.to_csv(result_path("integration_clv_per_user.csv"), index=False)

    segments = to_pandas(clv.groupBy("segment").agg(
        F.count("*").alias("users"),
        F.round(F.avg("clv_12m"), 2).alias("avg_clv_12m"),
        F.round(F.sum("clv_12m"), 2).alias("total_clv_12m"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue_90d"),
        F.round(F.avg("orders"), 2).alias("avg_orders"),
        F.round(F.avg("avg_order_value"), 2).alias("avg_order_value"),
        F.round(F.avg("recency_days"), 1).alias("avg_recency_days"),
        F.round(F.avg("sessions"), 2).alias("avg_hbase_sessions"),
        F.round(F.avg("avg_session_seconds"), 0).alias("avg_session_seconds"),
        F.round(F.avg("session_conversion_rate"), 4).alias("avg_session_conversion"),
        F.round(F.sum("abandoned_cart_value"), 2).alias("abandoned_cart_value"),
    ).orderBy(F.desc("avg_clv_12m")))
    segments.to_csv(result_path("integration_clv_segments.csv"), index=False)

    deciles = to_pandas(clv.groupBy("clv_decile").agg(
        F.count("*").alias("users"), F.round(F.sum("clv_12m"), 2).alias("clv_12m"),
        F.round(F.avg("sessions"), 2).alias("avg_sessions"),
        F.round(F.avg("session_conversion_rate"), 4).alias("avg_session_conversion"),
        F.round(F.avg("avg_order_value"), 2).alias("avg_order_value")).orderBy("clv_decile"))
    deciles["share_of_clv_pct"] = (100 * deciles.clv_12m / deciles.clv_12m.sum()).round(2)
    deciles.to_csv(result_path("integration_clv_deciles.csv"), index=False)

    corr = {c: clv.stat.corr("clv_12m", c) for c in
            ["sessions", "avg_session_seconds", "avg_pages_per_session", "session_conversion_rate", "orders", "avg_order_value"]}

    print(f"\nObservation window: {window_days} days ({bounds.t0:%Y-%m-%d} -> {bounds.t1:%Y-%m-%d})")
    print("\nCLV by segment\n", segments.to_string(index=False))
    print("\nCLV by decile\n", deciles.to_string(index=False))
    print("\nPearson correlation of clv_12m with engagement / purchase features")
    for k, v in corr.items():
        print(f"  {k:<26} {v: .3f}")
    print("\nTop 10 customers by predicted 12-month CLV")
    print(per_user.sort_values("clv_12m", ascending=False).head(10)[
        ["user_id", "country", "orders", "revenue", "avg_order_value", "recency_days", "sessions",
         "session_conversion_rate", "clv_12m", "segment"]].to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
