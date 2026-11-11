"""PART 1.1 - Load the generated data into MongoDB using the document model in the report.

Collections
    categories    {_id, name, subcategories:[{subcategory_id, name, profit_margin}], avg_profit_margin}
    products      {_id, name, category:{category_id, name}, base_price, current_stock, is_active,
                   price_history:[{price, date}], creation_date}
    users         {_id, geo:{city, state, country}, registration_date, last_active,
                   purchase_summary:{...}}        <- filled in by aggregations.py ($merge)
    transactions  {_id, session_id, user_id, timestamp, items:[{product_id, category_id, quantity,
                   unit_price, subtotal}], subtotal, discount, total, payment_method, status}
    sessions      full session journey documents (page_views embedded) - a sample only;
                  the bulk session time-series lives in HBase.

Usage:  python mongodb/load_mongodb.py [--sessions-sample 100000]
"""
import argparse
import datetime as dt
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import BulkWriteError

from common.config import MONGO_DB, MONGO_URI, RAW_DIR

BATCH = 5000


def parse_dt(value):
    return dt.datetime.fromisoformat(value) if value else None


def load_json(name):
    with open(os.path.join(RAW_DIR, name), encoding="utf-8") as f:
        return json.load(f)


def insert_batched(coll, docs):
    """Unordered bulk inserts: one bad document does not abort the batch."""
    coll.drop()
    inserted, batch = 0, []
    for doc in docs:
        batch.append(doc)
        if len(batch) == BATCH:
            inserted += _flush(coll, batch)
            batch = []
    if batch:
        inserted += _flush(coll, batch)
    print(f"  {coll.name:<14} {inserted:>10,} documents")
    return inserted


def _flush(coll, batch):
    try:
        return len(coll.insert_many(batch, ordered=False).inserted_ids)
    except BulkWriteError as e:
        print(f"    warning: {len(e.details['writeErrors'])} documents rejected in batch")
        return e.details["nInserted"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sessions-sample", type=int, default=100000,
                        help="number of full session documents to store in MongoDB")
    args = parser.parse_args()

    db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)[MONGO_DB]
    db.command("ping")
    start = time.time()
    print(f"Loading {RAW_DIR} into {MONGO_URI}/{MONGO_DB}")

    # Categories: the subcategory hierarchy is small and always read with its parent -> embed.
    categories = load_json("categories.json")
    cat_names = {c["category_id"]: c["name"] for c in categories}
    insert_batched(db.categories, (
        {
            "_id": c["category_id"],
            "name": c["name"],
            "subcategories": c["subcategories"],
            "avg_profit_margin": round(sum(s["profit_margin"] for s in c["subcategories"]) / len(c["subcategories"]), 4),
        }
        for c in categories
    ))

    # Products: embed the bounded price history (1-3 points) and an extended reference to the category.
    products = load_json("products.json")
    product_cat = {p["product_id"]: p["category_id"] for p in products}
    insert_batched(db.products, (
        {
            "_id": p["product_id"],
            "name": p["name"],
            "category": {"category_id": p["category_id"], "name": cat_names.get(p["category_id"])},
            "subcategory_id": p.get("subcategory_id"),
            "base_price": p["base_price"],
            "current_stock": p["current_stock"],
            "is_active": p["is_active"],
            "price_history": [{"price": h["price"], "date": parse_dt(h["date"])} for h in p["price_history"]],
            "creation_date": parse_dt(p["creation_date"]),
        }
        for p in products
    ))
    del products

    users = load_json("users.json")
    insert_batched(db.users, (
        {
            "_id": u["user_id"],
            "geo": u["geo_data"],
            "registration_date": parse_dt(u["registration_date"]),
            "last_active": parse_dt(u["last_active"]),
        }
        for u in users
    ))
    del users

    # Transactions: line items are always read with their order -> embed; category_id is
    # denormalised into each item so revenue-by-category needs no $lookup.
    transactions = load_json("transactions.json")
    insert_batched(db.transactions, (
        {
            "_id": t["transaction_id"],
            "session_id": t["session_id"],
            "user_id": t["user_id"],
            "timestamp": parse_dt(t["timestamp"]),
            "items": [dict(i, category_id=product_cat.get(i["product_id"])) for i in t["items"]],
            "subtotal": round(t["subtotal"], 2),
            "discount": t["discount"],
            "total": t["total"],
            "payment_method": t["payment_method"],
            "status": t["status"],
        }
        for t in transactions
    ))
    del transactions

    # Sessions sample: whole journey (page views + cart) in one document.
    sessions = load_json("sessions_0.json")[: args.sessions_sample]
    insert_batched(db.sessions, (
        {
            "_id": s["session_id"],
            "user_id": s["user_id"],
            "start_time": parse_dt(s["start_time"]),
            "end_time": parse_dt(s["end_time"]),
            "duration_seconds": s["duration_seconds"],
            "geo": s["geo_data"],
            "device": s["device_profile"],
            "referrer": s["referrer"],
            "viewed_products": s["viewed_products"],
            "page_views": [dict(pv, timestamp=parse_dt(pv["timestamp"])) for pv in s["page_views"]],
            "cart": [{"product_id": k, **v} for k, v in s["cart_contents"].items()],
            "conversion_status": s["conversion_status"],
        }
        for s in sessions
    ))
    del sessions

    print("Creating indexes ...")
    db.products.create_index([("category.category_id", ASCENDING)])
    db.products.create_index([("is_active", ASCENDING), ("current_stock", ASCENDING)])
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])
    db.transactions.create_index([("session_id", ASCENDING)], sparse=True)
    db.sessions.create_index([("user_id", ASCENDING), ("start_time", DESCENDING)])
    db.sessions.create_index([("page_views.product_id", ASCENDING)])
    print(f"Done in {time.time() - start:,.1f}s")


if __name__ == "__main__":
    main()
