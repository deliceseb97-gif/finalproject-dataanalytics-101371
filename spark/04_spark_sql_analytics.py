"""PART 2.2 - Spark SQL analytics.

A session counts as a purchase only when a transaction is linked to it (view purchased_sessions).
The generator's conversion_status = 'converted' flag is not used for that: it is also set when the
transaction quota was already reached and no order was created (A0 measures the gap).

Section A - complex SQL over the curated Parquet tables (registered as temp views):
    A1 daily revenue, 7-day moving average and day-over-day growth   (window functions)
    A2 category scorecard: revenue share, rank, estimated gross profit (CTEs + joins)
    A3 conversion funnel by device and referrer                        (conditional aggregation)
    A4 most-viewed vs top-selling products, view-to-purchase rate      (FULL OUTER JOIN)
    A5 traffic and conversion by weekday x hour                         (time bucketing)

Section B - "federated" SQL: samples pulled from MongoDB (users + purchase_summary) and
HBase (user_sessions summary family) into DataFrames, registered as views and joined with
Parquet data in one SQL statement. Run with --with-stores once MongoDB/HBase are loaded.

Usage:  python spark/04_spark_sql_analytics.py [--with-stores]
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import curated, get_spark, result_path, to_pandas

TABLES = ["users", "categories", "products", "sessions", "page_views", "cart_items",
          "transactions", "transaction_items"]

QUERIES = {
    "A0_dataset_profile": """
        SELECT (SELECT COUNT(*) FROM users)                                   AS users,
               (SELECT COUNT(*) FROM products)                                AS products,
               (SELECT COUNT(*) FROM products WHERE NOT is_active)            AS inactive_products,
               (SELECT COUNT(*) FROM sessions)                                AS sessions,
               (SELECT COUNT(*) FROM page_views)                              AS page_views,
               (SELECT COUNT(*) FROM transactions)                            AS transactions,
               (SELECT SUM(CASE WHEN is_session_linked THEN 0 ELSE 1 END) FROM transactions) AS orders_without_session,
               (SELECT SUM(CASE WHEN discount > 0 THEN 1 ELSE 0 END) FROM transactions)     AS discounted_orders,
               (SELECT COUNT(*) FROM sessions WHERE conversion_status = 'converted')       AS flagged_converted,
               (SELECT COUNT(*) FROM purchased_sessions)                                    AS sessions_with_order,
               (SELECT COUNT(*) FROM sessions s LEFT ANTI JOIN purchased_sessions p ON s.session_id = p.session_id
                WHERE s.conversion_status = 'converted')                                    AS converted_without_order
    """,
    "A1_daily_revenue": """
        WITH daily AS (
            SELECT txn_date,
                   COUNT(*)                       AS orders,
                   ROUND(SUM(total), 2)           AS revenue,
                   ROUND(AVG(total), 2)           AS avg_order_value,
                   ROUND(SUM(discount), 2)        AS discounts
            FROM transactions
            GROUP BY txn_date
        )
        SELECT txn_date, orders, revenue, avg_order_value, discounts,
               ROUND(AVG(revenue) OVER (ORDER BY txn_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_7d_ma,
               ROUND(100 * (revenue / LAG(revenue) OVER (ORDER BY txn_date) - 1), 2)                  AS dod_growth_pct
        FROM daily
        ORDER BY txn_date
    """,
    "A2_category_scorecard": """
        WITH cat_sales AS (
            SELECT ti.category_id,
                   SUM(ti.line_total)                 AS revenue,
                   SUM(ti.quantity)                   AS units,
                   COUNT(DISTINCT ti.transaction_id)  AS orders,
                   COUNT(DISTINCT ti.user_id)         AS buyers
            FROM transaction_items ti
            GROUP BY ti.category_id
        ),
        cat_views AS (
            SELECT p.category_id, COUNT(*) AS product_views
            FROM page_views pv JOIN products p ON pv.product_id = p.product_id
            WHERE pv.page_type = 'product_detail'
            GROUP BY p.category_id
        )
        SELECT c.category_id, c.category_name,
               ROUND(s.revenue, 2)                                      AS revenue,
               ROUND(100 * s.revenue / SUM(s.revenue) OVER (), 2)       AS revenue_share_pct,
               RANK() OVER (ORDER BY s.revenue DESC)                    AS revenue_rank,
               s.units, s.orders, s.buyers,
               ROUND(s.revenue / s.orders, 2)                           AS revenue_per_order,
               c.avg_profit_margin,
               ROUND(s.revenue * c.avg_profit_margin, 2)                AS est_gross_profit,
               v.product_views,
               ROUND(100 * s.units / v.product_views, 2)                AS units_per_100_views
        FROM cat_sales s
        JOIN categories c ON s.category_id = c.category_id
        LEFT JOIN cat_views v ON s.category_id = v.category_id
        ORDER BY revenue DESC
    """,
    "A3_funnel_by_device_referrer": """
        -- cumulative stages: each stage requires all previous ones
        WITH s AS (
            SELECT ss.*,
                   num_product_detail_views > 0                     AS st_view,
                   num_product_detail_views > 0 AND num_cart_items > 0 AS st_cart,
                   num_product_detail_views > 0 AND num_cart_items > 0 AND num_checkout_views > 0 AS st_checkout,
                   p.session_id IS NOT NULL                         AS st_purchase
            FROM sessions ss LEFT JOIN purchased_sessions p ON ss.session_id = p.session_id
        )
        SELECT device_type, referrer,
               COUNT(*)                                                       AS sessions,
               SUM(CAST(st_view AS INT))                                      AS viewed_product,
               SUM(CAST(st_cart AS INT))                                      AS added_to_cart,
               SUM(CAST(st_checkout AS INT))                                  AS reached_checkout,
               SUM(CAST(st_checkout AND st_purchase AS INT))                  AS purchased,
               SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS flagged_converted,
               ROUND(100 * AVG(CAST(st_checkout AND st_purchase AS INT)), 2)  AS conversion_rate_pct,
               ROUND(AVG(duration_seconds), 0)                                AS avg_duration_s
        FROM s
        GROUP BY ROLLUP(device_type, referrer)
        ORDER BY device_type NULLS FIRST, referrer NULLS FIRST
    """,
    "A4_viewed_vs_sold": """
        WITH views AS (
            SELECT product_id, COUNT(*) AS views, COUNT(DISTINCT session_id) AS viewing_sessions
            FROM page_views WHERE page_type = 'product_detail' AND product_id IS NOT NULL
            GROUP BY product_id
        ),
        carts AS (
            SELECT product_id, COUNT(*) AS cart_adds FROM cart_items GROUP BY product_id
        ),
        sales AS (
            SELECT product_id, SUM(quantity) AS units_sold, COUNT(DISTINCT transaction_id) AS orders,
                   SUM(line_total) AS revenue
            FROM transaction_items GROUP BY product_id
        )
        SELECT COALESCE(v.product_id, s.product_id) AS product_id, p.product_name, p.category_name,
               p.current_price, p.is_active, p.current_stock,
               COALESCE(v.views, 0) AS views, COALESCE(c.cart_adds, 0) AS cart_adds,
               COALESCE(s.orders, 0) AS orders, COALESCE(s.units_sold, 0) AS units_sold,
               ROUND(COALESCE(s.revenue, 0), 2) AS revenue,
               ROUND(100 * COALESCE(c.cart_adds, 0) / NULLIF(v.views, 0), 2) AS view_to_cart_pct,
               ROUND(100 * COALESCE(s.orders, 0) / NULLIF(v.views, 0), 2)    AS view_to_order_pct
        FROM views v
        FULL OUTER JOIN sales s ON v.product_id = s.product_id
        LEFT JOIN carts c ON COALESCE(v.product_id, s.product_id) = c.product_id
        LEFT JOIN products p ON COALESCE(v.product_id, s.product_id) = p.product_id
        ORDER BY revenue DESC
    """,
    "A5_weekday_hour": """
        SELECT date_format(s.start_time, 'E')     AS weekday,
               dayofweek(s.start_time)            AS weekday_num,
               hour(s.start_time)                 AS hour,
               COUNT(*)                           AS sessions,
               ROUND(100 * AVG(CASE WHEN p.session_id IS NOT NULL THEN 1 ELSE 0 END), 2) AS conversion_rate_pct
        FROM sessions s LEFT JOIN purchased_sessions p ON s.session_id = p.session_id
        GROUP BY 1, 2, 3
        ORDER BY weekday_num, hour
    """,
}

FEDERATED_SQL = """
    -- MongoDB (mongo_users, mongo_order_sessions) x HBase (hbase_sessions) x Parquet (transactions)
    WITH engagement AS (
        SELECT h.user_id,
               COUNT(*)                                                     AS hbase_sessions,
               ROUND(AVG(h.duration_seconds), 0)                            AS avg_session_s,
               ROUND(AVG(h.num_page_views), 1)                              AS avg_pages,
               COUNT(o.session_id)                                          AS purchase_sessions
        FROM hbase_sessions h
        LEFT JOIN mongo_order_sessions o ON h.session_id = o.session_id     -- session in HBase, order in MongoDB
        GROUP BY h.user_id
    ),
    recent_spend AS (
        SELECT user_id, ROUND(SUM(total), 2) AS spend_last_30d
        FROM transactions
        WHERE txn_time >= date_sub((SELECT MAX(txn_time) FROM transactions), 30)
        GROUP BY user_id
    )
    SELECT u.user_id, u.country, u.total_orders, u.total_spent, u.avg_order_value,
           e.hbase_sessions, e.avg_session_s, e.avg_pages, e.purchase_sessions,
           ROUND(100 * e.purchase_sessions / e.hbase_sessions, 1) AS session_conversion_pct,
           COALESCE(r.spend_last_30d, 0)                           AS spend_last_30d
    FROM mongo_users u
    JOIN engagement e ON u.user_id = e.user_id
    LEFT JOIN recent_spend r ON u.user_id = r.user_id
    ORDER BY u.total_spent DESC
"""


def register_store_views(spark, user_sample=2000):
    """Pull samples from MongoDB and HBase into Spark views (connector-free approach)."""
    import happybase
    from pymongo import MongoClient

    from common.config import HBASE_HOST, HBASE_PORT, MONGO_DB, MONGO_URI

    db = MongoClient(MONGO_URI)[MONGO_DB]
    docs = list(db.users.find({"purchase_summary": {"$exists": True}},
                              {"geo.country": 1, "purchase_summary": 1}).limit(user_sample))
    mongo_rows = [(d["_id"], d["geo"]["country"], d["purchase_summary"]["total_orders"],
                   float(d["purchase_summary"]["total_spent"]), float(d["purchase_summary"]["avg_order_value"]))
                  for d in docs]
    spark.createDataFrame(mongo_rows, "user_id string, country string, total_orders long, total_spent double, avg_order_value double") \
        .createOrReplaceTempView("mongo_users")
    order_sessions = db.transactions.distinct("session_id", {"user_id": {"$in": [r[0] for r in mongo_rows]},
                                                             "session_id": {"$ne": None}})
    spark.createDataFrame([(sid,) for sid in order_sessions], "session_id string").createOrReplaceTempView("mongo_order_sessions")

    table = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=120000).table("user_sessions")
    cols = [b"s:duration_seconds", b"s:num_page_views"]
    hbase_rows = []
    for user_id, *_ in mongo_rows:  # one prefix scan per user: exactly the HBase access pattern
        for key, data in table.scan(row_prefix=f"{user_id}#".encode(), columns=cols):
            session_id = key.decode().rsplit("#", 1)[1]  # row key = user_id#reverse_ts#session_id
            hbase_rows.append((user_id, session_id, int(data[cols[0]]), int(data[cols[1]])))
    spark.createDataFrame(hbase_rows, "user_id string, session_id string, duration_seconds long, num_page_views long") \
        .createOrReplaceTempView("hbase_sessions")
    print(f"Registered mongo_users ({len(mongo_rows):,} docs), mongo_order_sessions ({len(order_sessions):,}) "
          f"and hbase_sessions ({len(hbase_rows):,} rows)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-stores", action="store_true", help="also run the MongoDB/HBase federated query")
    args = parser.parse_args()

    spark = get_spark("04_spark_sql_analytics")
    for t in TABLES:
        spark.read.parquet(curated(t)).createOrReplaceTempView(t)
    spark.sql("SELECT DISTINCT session_id FROM transactions WHERE session_id IS NOT NULL") \
        .createOrReplaceTempView("purchased_sessions")

    for name, sql in QUERIES.items():
        df = spark.sql(sql)
        pdf = to_pandas(df)
        pdf.to_csv(result_path(f"spark_sql_{name}.csv"), index=False)
        print(f"\n=== {name} ({len(pdf):,} rows)")
        print(pdf.head(12).to_string(index=False))

    if args.with_stores:
        register_store_views(spark)
        pdf = to_pandas(spark.sql(FEDERATED_SQL))
        pdf.to_csv(result_path("spark_sql_B1_federated_mongo_hbase.csv"), index=False)
        print(f"\n=== B1_federated_mongo_hbase ({len(pdf):,} rows)")
        print(pdf.head(12).to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
