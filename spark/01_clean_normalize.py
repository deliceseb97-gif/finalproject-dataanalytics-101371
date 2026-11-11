"""PART 2.1 - Spark batch job: clean and normalise the raw JSON dataset.

Input : C:/aucadata/raw/*.json        (JSON arrays written by dataset_generator.py)
Output: C:/aucadata/curated/<table>/  (Parquet, columnar + snappy)

Curated tables
    users, categories, subcategories, products, product_price_history,
    sessions, page_views, cart_items, transactions, transaction_items

Design notes
* Explicit schemas are used instead of inference. Inference would need a full extra pass
  over ~2 GB of JSON and would turn `cart_contents` (keys are product ids) into a struct
  with thousands of fields - it is declared as a MapType instead.
* The raw files are JSON *arrays*, so they need multiLine=true, which makes every file a
  single non-splittable task. Converting once to Parquet removes that bottleneck for every
  downstream job (column pruning, predicate pushdown, splittable files).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType, MapType,
                               StringType, StructField, StructType)

from common.config import RAW_DIR, curated, get_spark

# ---------------------------------------------------------------- schemas
GEO = StructType([
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("country", StringType()),
    StructField("ip_address", StringType()),
])

USER_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("geo_data", GEO),
    StructField("registration_date", StringType()),
    StructField("last_active", StringType()),
])

CATEGORY_SCHEMA = StructType([
    StructField("category_id", StringType()),
    StructField("name", StringType()),
    StructField("subcategories", ArrayType(StructType([
        StructField("subcategory_id", StringType()),
        StructField("name", StringType()),
        StructField("profit_margin", DoubleType()),
    ]))),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("name", StringType()),
    StructField("category_id", StringType()),
    StructField("subcategory_id", StringType()),  # absent in generator output -> null
    StructField("base_price", DoubleType()),
    StructField("current_stock", LongType()),
    StructField("is_active", BooleanType()),
    StructField("price_history", ArrayType(StructType([
        StructField("price", DoubleType()),
        StructField("date", StringType()),
    ]))),
    StructField("creation_date", StringType()),
])

SESSION_SCHEMA = StructType([
    StructField("session_id", StringType()),
    StructField("user_id", StringType()),
    StructField("start_time", StringType()),
    StructField("end_time", StringType()),
    StructField("duration_seconds", LongType()),
    StructField("geo_data", GEO),
    StructField("device_profile", StructType([
        StructField("type", StringType()),
        StructField("os", StringType()),
        StructField("browser", StringType()),
    ])),
    StructField("viewed_products", ArrayType(StringType())),
    StructField("page_views", ArrayType(StructType([
        StructField("timestamp", StringType()),
        StructField("page_type", StringType()),
        StructField("product_id", StringType()),
        StructField("category_id", StringType()),
        StructField("view_duration", LongType()),
    ]))),
    StructField("cart_contents", MapType(StringType(), StructType([
        StructField("quantity", LongType()),
        StructField("price", DoubleType()),
    ]))),
    StructField("conversion_status", StringType()),
    StructField("referrer", StringType()),
])

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("session_id", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("product_id", StringType()),
        StructField("quantity", LongType()),
        StructField("unit_price", DoubleType()),
        StructField("subtotal", DoubleType()),
    ]))),
    StructField("subtotal", DoubleType()),
    StructField("discount", DoubleType()),
    StructField("total", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("status", StringType()),
])


# ---------------------------------------------------------------- helpers
def read_json(spark, pattern, schema):
    return spark.read.schema(schema).option("multiLine", "true").json(os.path.join(RAW_DIR, pattern))


def ts(col):
    """ISO-8601 string -> timestamp (handles optional microseconds)."""
    return F.to_timestamp(col)


def clean_str(col, lower=False):
    c = F.trim(col)
    c = F.lower(c) if lower else c
    return F.when(c == "", None).otherwise(c)


def write(df, name, partition_by=None):
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(curated(name))
    print(f"  wrote {name:<24} {spark.read.parquet(curated(name)).count():>10,} rows")


# ---------------------------------------------------------------- job
spark = get_spark("01_clean_normalize")
print("Cleaning raw dataset from", RAW_DIR)

# Users: flatten geo, typed timestamps, registration cohort column.
users = (
    read_json(spark, "users.json", USER_SCHEMA)
    .dropna(subset=["user_id"])
    .dropDuplicates(["user_id"])
    .select(
        "user_id",
        F.coalesce(clean_str(F.col("geo_data.city")), F.lit("unknown")).alias("city"),
        F.coalesce(F.upper(clean_str(F.col("geo_data.state"))), F.lit("NA")).alias("state"),
        F.coalesce(F.upper(clean_str(F.col("geo_data.country"))), F.lit("NA")).alias("country"),
        ts("registration_date").alias("registration_date"),
        ts("last_active").alias("last_active"),
    )
    .withColumn("registration_month", F.date_format("registration_date", "yyyy-MM"))
)
write(users, "users")

# Categories: hierarchy split into two relational tables.
categories_raw = read_json(spark, "categories.json", CATEGORY_SCHEMA).dropDuplicates(["category_id"])
subcategories = (
    categories_raw.select("category_id", F.explode("subcategories").alias("s"))
    .select("category_id", F.col("s.subcategory_id").alias("subcategory_id"),
            F.col("s.name").alias("subcategory_name"), F.col("s.profit_margin").alias("profit_margin"))
)
categories = (
    categories_raw.join(subcategories.groupBy("category_id").agg(
        F.round(F.avg("profit_margin"), 4).alias("avg_profit_margin"),
        F.count("*").alias("num_subcategories")), "category_id", "left")
    .select("category_id", F.col("name").alias("category_name"), "avg_profit_margin", "num_subcategories")
)
write(categories, "categories")
write(subcategories, "subcategories")

# Products: positive prices, non-negative stock, missing flags defaulted.
products_raw = read_json(spark, "products.json", PRODUCT_SCHEMA).dropna(subset=["product_id"]).dropDuplicates(["product_id"])
products = (
    products_raw.select(
        "product_id",
        clean_str(F.col("name")).alias("product_name"),
        "category_id",
        "subcategory_id",
        F.round("base_price", 2).alias("current_price"),
        F.greatest(F.coalesce("current_stock", F.lit(0)), F.lit(0)).alias("current_stock"),
        F.coalesce("is_active", F.lit(False)).alias("is_active"),
        ts("creation_date").alias("creation_date"),
        F.size("price_history").alias("num_price_changes"),
        F.element_at("price_history", 1)["price"].alias("initial_price"),
    )
    .where(F.col("current_price") > 0)
    .join(categories.select("category_id", "category_name"), "category_id", "left")
)
write(products, "products")
price_history = (
    products_raw.select("product_id", F.posexplode("price_history").alias("seq", "p"))
    .select("product_id", "seq", F.col("p.price").alias("price"), ts("p.date").alias("effective_date"))
)
write(price_history, "product_price_history")

# Sessions: one row per session plus exploded page views and cart items.
sessions_raw = (
    read_json(spark, "sessions_*.json", SESSION_SCHEMA)
    .dropna(subset=["session_id", "user_id"])
    .dropDuplicates(["session_id"])
)
sessions_raw.cache()

page_views = (
    sessions_raw.select("session_id", "user_id", F.posexplode("page_views").alias("seq", "pv"))
    .select(
        "session_id", "user_id", "seq",
        ts("pv.timestamp").alias("view_time"),
        clean_str(F.col("pv.page_type"), lower=True).alias("page_type"),
        "pv.product_id", "pv.category_id",
        F.greatest(F.coalesce("pv.view_duration", F.lit(0)), F.lit(0)).alias("view_duration"),
    )
    .withColumn("view_date", F.to_date("view_time"))
)
write(page_views, "page_views", partition_by="view_date")

cart_items = (
    sessions_raw.select("session_id", "user_id", ts("start_time").alias("start_time"),
                        F.explode("cart_contents").alias("product_id", "c"))
    .select("session_id", "user_id", "start_time", "product_id",
            F.col("c.quantity").alias("quantity"), F.col("c.price").alias("price"))
    .where(F.col("quantity") > 0)
)
write(cart_items, "cart_items")

page_type_count = lambda t: F.size(F.filter("page_views", lambda p: p["page_type"] == t))
sessions = (
    sessions_raw.select(
        "session_id", "user_id",
        ts("start_time").alias("start_time"),
        ts("end_time").alias("end_time"),
        "duration_seconds",
        F.coalesce(clean_str(F.col("device_profile.type"), True), F.lit("unknown")).alias("device_type"),
        F.coalesce(clean_str(F.col("device_profile.os")), F.lit("unknown")).alias("os"),
        F.coalesce(clean_str(F.col("device_profile.browser")), F.lit("unknown")).alias("browser"),
        F.coalesce(clean_str(F.col("referrer"), True), F.lit("unknown")).alias("referrer"),
        F.col("geo_data.country").alias("country"),
        F.col("geo_data.state").alias("state"),
        F.col("geo_data.city").alias("city"),
        F.col("geo_data.ip_address").alias("ip_address"),
        F.size("page_views").alias("num_page_views"),
        F.size("viewed_products").alias("num_products_viewed"),
        page_type_count("product_detail").alias("num_product_detail_views"),
        page_type_count("cart").alias("num_cart_views"),
        page_type_count("checkout").alias("num_checkout_views"),
        F.size(F.map_keys("cart_contents")).alias("num_cart_items"),
        F.aggregate(F.map_values("cart_contents"), F.lit(0.0),
                    lambda acc, c: acc + c["quantity"] * c["price"]).alias("cart_value"),
        clean_str(F.col("conversion_status"), True).alias("conversion_status"),
    )
    .withColumn("session_date", F.to_date("start_time"))
)
write(sessions, "sessions")
sessions_raw.unpersist()

# Transactions: recompute totals, flag inconsistencies, keep session linkage optional.
txn_raw = (
    read_json(spark, "transactions.json", TRANSACTION_SCHEMA)
    .dropna(subset=["transaction_id", "user_id"])
    .dropDuplicates(["transaction_id"])
)
transaction_items = (
    txn_raw.select("transaction_id", "user_id", ts("timestamp").alias("txn_time"), F.explode("items").alias("i"))
    .select("transaction_id", "user_id", "txn_time", "i.product_id",
            F.col("i.quantity").alias("quantity"), F.col("i.unit_price").alias("unit_price"),
            F.round(F.col("i.quantity") * F.col("i.unit_price"), 2).alias("line_total"))
    .where((F.col("quantity") > 0) & (F.col("unit_price") > 0))
    .join(products.select("product_id", "category_id"), "product_id", "left")
)
write(transaction_items, "transaction_items")

transactions = (
    txn_raw.select(
        "transaction_id", "session_id", "user_id",
        ts("timestamp").alias("txn_time"),
        F.round(F.coalesce("subtotal", F.lit(0.0)), 2).alias("subtotal"),
        F.round(F.coalesce("discount", F.lit(0.0)), 2).alias("discount"),
        F.round(F.coalesce("total", F.col("subtotal") - F.col("discount")), 2).alias("total"),
        F.coalesce(clean_str(F.col("payment_method"), True), F.lit("unknown")).alias("payment_method"),
        F.coalesce(clean_str(F.col("status"), True), F.lit("unknown")).alias("status"),
        F.size("items").alias("num_items"),
        F.aggregate("items", F.lit(0).cast("long"), lambda acc, i: acc + i["quantity"]).alias("units"),
    )
    .withColumn("is_session_linked", F.col("session_id").isNotNull())
    .withColumn("total_is_consistent", F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) < 0.01)
    .withColumn("txn_date", F.to_date("txn_time"))
    .withColumn("txn_month", F.date_format("txn_time", "yyyy-MM"))
)
write(transactions, "transactions")

bad = transactions.where(~F.col("total_is_consistent")).count()
print(f"Transactions with subtotal - discount != total: {bad}")
spark.stop()
print("Done.")
