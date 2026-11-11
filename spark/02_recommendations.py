"""PART 2.1 - Spark batch job: product recommendation indicators (market-basket analysis).

  A) "Users who bought X also bought Y"  - item pairs inside the same transaction
  B) "Products frequently viewed together" - product_detail pairs inside the same session

For each ordered pair (X -> Y):
    support    = sessions_or_orders_with(X and Y) / all_sessions_or_orders
    confidence = with(X and Y) / with(X)
    lift       = confidence / P(Y)          (> 1 means stronger than chance)

Outputs (C:/aucadata/curated + output/results):
    rec_bought_together, rec_viewed_together  (Parquet, all pairs above MIN_PAIR_COUNT)
    spark_rec_top_bought_together.csv, spark_rec_top_viewed_together.csv,
    spark_rec_per_product_top5.csv
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.config import curated, get_spark, result_path, to_pandas

MIN_PAIR_COUNT = 3


def pair_rules(baskets, basket_col, label):
    """baskets: (basket_id, product_id) with one row per distinct product in a basket."""
    baskets = baskets.dropDuplicates([basket_col, "product_id"]).repartition(64, basket_col).cache()
    n_baskets = baskets.select(basket_col).distinct().count()

    item_freq = baskets.groupBy("product_id").agg(F.count("*").alias("item_count"))

    # Self-join on the basket id; a < b keeps each unordered pair once (halves the shuffle).
    a, b = baskets.alias("a"), baskets.alias("b")
    pairs = (
        a.join(b, (F.col(f"a.{basket_col}") == F.col(f"b.{basket_col}")) &
               (F.col("a.product_id") < F.col("b.product_id")))
        .groupBy(F.col("a.product_id").alias("p1"), F.col("b.product_id").alias("p2"))
        .agg(F.count("*").alias("pair_count"))
        .where(F.col("pair_count") >= MIN_PAIR_COUNT)
    )
    # Expand to both directions X->Y and Y->X, then attach item frequencies (small -> broadcast).
    directed = pairs.select(F.col("p1").alias("antecedent"), F.col("p2").alias("consequent"), "pair_count") \
        .unionByName(pairs.select(F.col("p2").alias("antecedent"), F.col("p1").alias("consequent"), "pair_count"))
    freq_x = F.broadcast(item_freq.select(F.col("product_id").alias("antecedent"), F.col("item_count").alias("antecedent_count")))
    freq_y = F.broadcast(item_freq.select(F.col("product_id").alias("consequent"), F.col("item_count").alias("consequent_count")))
    rules = (
        directed.join(freq_x, "antecedent").join(freq_y, "consequent")
        .withColumn("support", F.col("pair_count") / F.lit(n_baskets))
        .withColumn("confidence", F.col("pair_count") / F.col("antecedent_count"))
        .withColumn("lift", F.col("confidence") / (F.col("consequent_count") / F.lit(n_baskets)))
        .withColumn("source", F.lit(label))
    )
    baskets.unpersist()
    print(f"[{label}] baskets={n_baskets:,} products={item_freq.count():,} rules(pair_count>={MIN_PAIR_COUNT})={rules.count():,}")
    return rules


spark = get_spark("02_recommendations")
products = spark.read.parquet(curated("products")).select("product_id", "product_name", "category_name")

# A) bought together - transaction line items
bought = pair_rules(
    spark.read.parquet(curated("transaction_items")).select("transaction_id", "product_id"),
    "transaction_id", "bought_together")
bought.write.mode("overwrite").parquet(curated("rec_bought_together"))

# B) viewed together - product detail pages in the same session
viewed = pair_rules(
    spark.read.parquet(curated("page_views"))
    .where((F.col("page_type") == "product_detail") & F.col("product_id").isNotNull())
    .select("session_id", "product_id"),
    "session_id", "viewed_together")
viewed.write.mode("overwrite").parquet(curated("rec_viewed_together"))

bought = spark.read.parquet(curated("rec_bought_together"))
viewed = spark.read.parquet(curated("rec_viewed_together"))

name_x = products.select(F.col("product_id").alias("antecedent"), F.col("product_name").alias("antecedent_name"))
name_y = products.select(F.col("product_id").alias("consequent"), F.col("product_name").alias("consequent_name"),
                         F.col("category_name").alias("consequent_category"))
cols = ["antecedent", "antecedent_name", "consequent", "consequent_name", "consequent_category",
        "pair_count", "support", "confidence", "lift"]

for rules, name in [(bought, "bought_together"), (viewed, "viewed_together")]:
    top = (rules.join(F.broadcast(name_x), "antecedent").join(F.broadcast(name_y), "consequent")
           .orderBy(F.desc("pair_count"), F.desc("lift")).select(*cols).limit(50))
    to_pandas(top).to_csv(result_path(f"spark_rec_top_{name}.csv"), index=False)
    print(f"\nTop {name} rules")
    top.show(10, truncate=28)
    rules.select(F.mean("lift").alias("mean_lift"), F.expr("percentile_approx(lift, 0.5)").alias("median_lift"),
                 F.max("lift").alias("max_lift"), F.max("pair_count").alias("max_pair_count")).show()

# Blend both signals into a top-5 "customers also bought / viewed" list per product.
blended = (
    bought.select("antecedent", "consequent", F.col("lift").alias("buy_lift"), F.col("pair_count").alias("co_bought"))
    .join(viewed.select("antecedent", "consequent", F.col("lift").alias("view_lift"), F.col("pair_count").alias("co_viewed")),
          ["antecedent", "consequent"], "full")
    .fillna(0)
    .withColumn("score", 0.7 * F.col("co_bought") + 0.3 * F.col("co_viewed"))
    .withColumn("rank", F.row_number().over(Window.partitionBy("antecedent").orderBy(F.desc("score"), F.desc("buy_lift"))))
    .where(F.col("rank") <= 5)
)
blended.write.mode("overwrite").parquet(curated("rec_per_product_top5"))
to_pandas(spark.read.parquet(curated("rec_per_product_top5")).orderBy("antecedent", "rank")) \
    .to_csv(result_path("spark_rec_per_product_top5.csv"), index=False)
print("Per-product top-5 recommendations written.")
spark.stop()
