"""PART 1.1 - MongoDB aggregation pipelines.

  1. revenue_by_category_month  - revenue analytics: revenue, units, orders and estimated
                                  gross profit per category per month ($unwind, $group,
                                  $lookup on the category hierarchy, $setWindowFields rank)
  2. user_segmentation          - purchase-frequency / RFM segmentation per user, bucketed
                                  with $bucket; the per-user summary is $merge'd back into
                                  users.purchase_summary (computed pattern)
  3. product_popularity         - top-selling vs most-viewed products (sessions page_views
                                  joined to transactions with $lookup)

Results are printed and written to output/results/mongo_*.csv.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pymongo import MongoClient

from common.config import MONGO_DB, MONGO_URI, result_path

# ------------------------------------------------------------------ pipeline 1
REVENUE_BY_CATEGORY_MONTH = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": {"category_id": "$items.category_id",
                "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}}},
        "gross_revenue": {"$sum": "$items.subtotal"},
        "units_sold": {"$sum": "$items.quantity"},
        "orders": {"$addToSet": "$_id"},
    }},
    {"$lookup": {"from": "categories", "localField": "_id.category_id",
                 "foreignField": "_id", "as": "category"}},
    {"$unwind": "$category"},
    {"$project": {
        "_id": 0,
        "category_id": "$_id.category_id",
        "category_name": "$category.name",
        "month": "$_id.month",
        "gross_revenue": {"$round": ["$gross_revenue", 2]},
        "units_sold": 1,
        "orders": {"$size": "$orders"},
        "avg_profit_margin": "$category.avg_profit_margin",
        "est_gross_profit": {"$round": [{"$multiply": ["$gross_revenue", "$category.avg_profit_margin"]}, 2]},
    }},
    {"$setWindowFields": {
        "partitionBy": "$month",
        "sortBy": {"gross_revenue": -1},
        "output": {"rank_in_month": {"$rank": {}}},
    }},
    {"$sort": {"month": 1, "rank_in_month": 1}},
]

# ------------------------------------------------------------------ pipeline 2
USER_SUMMARY_STAGES = [
    {"$group": {
        "_id": "$user_id",
        "total_orders": {"$sum": 1},
        "total_spent": {"$sum": "$total"},
        "total_discount": {"$sum": "$discount"},
        "first_purchase": {"$min": "$timestamp"},
        "last_purchase": {"$max": "$timestamp"},
        "payment_methods": {"$push": "$payment_method"},
    }},
    {"$set": {
        "total_spent": {"$round": ["$total_spent", 2]},
        "avg_order_value": {"$round": [{"$divide": ["$total_spent", "$total_orders"]}, 2]},
        "days_since_last_purchase": {"$dateDiff": {"startDate": "$last_purchase", "endDate": "$$NOW", "unit": "day"}},
        "top_payment_method": {"$first": {"$sortArray": {
            "input": {"$map": {"input": {"$setUnion": ["$payment_methods"]}, "as": "m",
                               "in": {"m": "$$m", "n": {"$size": {"$filter": {
                                   "input": "$payment_methods", "cond": {"$eq": ["$$this", "$$m"]}}}}}}},
            "sortBy": {"n": -1}}}},
    }},
    {"$set": {"top_payment_method": "$top_payment_method.m"}},
    {"$unset": "payment_methods"},
]

USER_SEGMENTATION = USER_SUMMARY_STAGES + [
    {"$bucket": {
        "groupBy": "$total_orders",
        "boundaries": [1, 20, 25, 30, 35, 1000],
        "default": "other",
        "output": {
            "users": {"$sum": 1},
            "avg_spent": {"$avg": "$total_spent"},
            "avg_order_value": {"$avg": "$avg_order_value"},
            "avg_days_since_last_purchase": {"$avg": "$days_since_last_purchase"},
            "revenue": {"$sum": "$total_spent"},
        },
    }},
    {"$set": {
        "segment": {"$switch": {"branches": [
            {"case": {"$eq": ["$_id", 1]}, "then": "Occasional (1-19 orders)"},
            {"case": {"$eq": ["$_id", 20]}, "then": "Regular (20-24)"},
            {"case": {"$eq": ["$_id", 25]}, "then": "Frequent (25-29)"},
            {"case": {"$eq": ["$_id", 30]}, "then": "Very frequent (30-34)"},
            {"case": {"$eq": ["$_id", 35]}, "then": "Champion (35+)"}],
            "default": "other"}},
        "avg_spent": {"$round": ["$avg_spent", 2]},
        "avg_order_value": {"$round": ["$avg_order_value", 2]},
        "avg_days_since_last_purchase": {"$round": ["$avg_days_since_last_purchase", 1]},
        "revenue": {"$round": ["$revenue", 2]},
    }},
]

# Computed pattern: persist each user's summary on the user document for O(1) profile reads.
MERGE_PURCHASE_SUMMARY = USER_SUMMARY_STAGES + [
    {"$project": {"purchase_summary": "$$ROOT"}},
    {"$unset": "purchase_summary._id"},
    {"$merge": {"into": "users", "on": "_id", "whenMatched": "merge", "whenNotMatched": "discard"}},
]

# ------------------------------------------------------------------ pipeline 3
PRODUCT_POPULARITY = [
    {"$unwind": "$page_views"},
    {"$match": {"page_views.page_type": "product_detail"}},
    {"$group": {"_id": "$page_views.product_id",
                "views": {"$sum": 1},
                "sessions": {"$addToSet": "$_id"},
                "avg_view_seconds": {"$avg": "$page_views.view_duration"}}},
    {"$set": {"sessions": {"$size": "$sessions"}}},
    {"$sort": {"views": -1}},
    {"$limit": 25},
    {"$lookup": {
        "from": "transactions",
        "let": {"pid": "$_id"},
        "pipeline": [
            {"$match": {"$expr": {"$in": ["$$pid", "$items.product_id"]}}},
            {"$unwind": "$items"},
            {"$match": {"$expr": {"$eq": ["$items.product_id", "$$pid"]}}},
            {"$group": {"_id": None, "units_sold": {"$sum": "$items.quantity"},
                        "revenue": {"$sum": "$items.subtotal"}, "orders": {"$sum": 1}}},
        ],
        "as": "sales",
    }},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "product"}},
    {"$project": {
        "_id": 0, "product_id": "$_id",
        "product_name": {"$first": "$product.name"},
        "category": {"$first": "$product.category.name"},
        "views": 1, "sessions": 1,
        "avg_view_seconds": {"$round": ["$avg_view_seconds", 1]},
        "units_sold": {"$ifNull": [{"$first": "$sales.units_sold"}, 0]},
        "orders": {"$ifNull": [{"$first": "$sales.orders"}, 0]},
        "revenue": {"$round": [{"$ifNull": [{"$first": "$sales.revenue"}, 0]}, 2]},
    }},
]

TOP_SELLING = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id", "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"}, "orders": {"$sum": 1}}},
    {"$sort": {"units_sold": -1}},
    {"$limit": 25},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "product"}},
    {"$project": {"_id": 0, "product_id": "$_id", "product_name": {"$first": "$product.name"},
                  "category": {"$first": "$product.category.name"}, "units_sold": 1, "orders": 1,
                  "revenue": {"$round": ["$revenue", 2]}}},
]


def run(db, coll, name, pipeline, save=True):
    start = time.time()
    rows = list(db[coll].aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(rows)
    print(f"\n=== {name}  ({len(df)} rows, {time.time() - start:.1f}s)")
    if not df.empty:
        print(df.head(15).to_string(index=False))
        if save:
            df.to_csv(result_path(f"mongo_{name}.csv"), index=False)
    return df


def main():
    db = MongoClient(MONGO_URI)[MONGO_DB]
    pd.set_option("display.width", 200)

    run(db, "transactions", "revenue_by_category_month", REVENUE_BY_CATEGORY_MONTH)
    run(db, "transactions", "user_segmentation", USER_SEGMENTATION)
    run(db, "sessions", "product_popularity_most_viewed", PRODUCT_POPULARITY)
    run(db, "transactions", "product_popularity_top_selling", TOP_SELLING)

    db.transactions.aggregate(MERGE_PURCHASE_SUMMARY, allowDiskUse=True)
    sample = db.users.find_one({"purchase_summary": {"$exists": True}})
    print("\nusers.purchase_summary written ($merge). Example document:\n", sample)


if __name__ == "__main__":
    main()
