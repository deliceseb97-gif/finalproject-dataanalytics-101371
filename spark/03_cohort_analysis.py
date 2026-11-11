"""PART 2.1 (additional) - Spark batch job: cohort analysis of user purchasing patterns.

Users are grouped by registration month; for every cohort we measure, per calendar month of
activity, how many users purchased, how much they spent and the share of the cohort that
was active (retention).

Outputs: output/results/spark_cohort_matrix.csv  (long format, one row per cohort x month)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from common.config import curated, get_spark, result_path, to_pandas

spark = get_spark("03_cohort_analysis")
users = spark.read.parquet(curated("users")).select("user_id", "registration_date", "registration_month")
txns = spark.read.parquet(curated("transactions")).select("user_id", "txn_time", "txn_month", "total")

cohort_size = users.groupBy("registration_month").agg(F.count("*").alias("cohort_users"))

cohort = (
    txns.join(users, "user_id")
    .withColumn("months_since_registration",
                F.floor(F.months_between(F.trunc("txn_time", "month"), F.trunc("registration_date", "month"))).cast("int"))
    .groupBy("registration_month", "txn_month", "months_since_registration")
    .agg(F.countDistinct("user_id").alias("active_buyers"),
         F.count("*").alias("orders"),
         F.round(F.sum("total"), 2).alias("revenue"))
    .join(cohort_size, "registration_month")
    .withColumn("retention_rate", F.round(F.col("active_buyers") / F.col("cohort_users"), 4))
    .withColumn("revenue_per_cohort_user", F.round(F.col("revenue") / F.col("cohort_users"), 2))
    .orderBy("registration_month", "txn_month")
)
pdf = to_pandas(cohort)
pdf.to_csv(result_path("spark_cohort_matrix.csv"), index=False)

print("Cohort sizes")
cohort_size.orderBy("registration_month").show()
print("Revenue per cohort user (rows = registration month, columns = purchase month)")
print(pdf.pivot(index="registration_month", columns="txn_month", values="revenue_per_cohort_user").fillna(0).to_string())
print("\nRetention rate (share of cohort that purchased in the month)")
print(pdf.pivot(index="registration_month", columns="txn_month", values="retention_rate").fillna(0).to_string())
spark.stop()
