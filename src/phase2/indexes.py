from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database

from config.settings import MONGO_DATABASE, MONGO_URI, VALIDATED_COLLECTION

# Phase 2 Specialized Indexes Specifications
PHASE2_INDEX_SPECS: List[Dict[str, Any]] = [
    {
        "name": "idx_city_payment_status",
        "keys": [("city", ASCENDING), ("payment_status", ASCENDING)],
        "type": "Compound Index",
        "purpose": "Optimizes simultaneous filtering by city and payment status (serves orders_by_city_status query).",
        "target_collection": VALIDATED_COLLECTION,
    },
    {
        "name": "idx_total_amount_desc",
        "keys": [("total_amount", DESCENDING)],
        "type": "Single Field Index",
        "purpose": "Optimizes range queries and descending sort on order total amount (serves high_value_orders query).",
        "target_collection": VALIDATED_COLLECTION,
    },
    {
        "name": "idx_order_date",
        "keys": [("order_date", ASCENDING)],
        "type": "Single Field Index",
        "purpose": "Optimizes date range queries and chronological lookups (serves orders_by_date_range query).",
        "target_collection": VALIDATED_COLLECTION,
    },
    {
        "name": "idx_customer_order_date",
        "keys": [("customer_id", ASCENDING), ("order_date", DESCENDING)],
        "type": "Compound Index",
        "purpose": "Enables direct index lookup for customer order history sorted by most recent date first.",
        "target_collection": VALIDATED_COLLECTION,
    },
    {
        "name": "idx_updated_at",
        "keys": [("updated_at", ASCENDING)],
        "type": "Single Field Index",
        "purpose": "Powers the incremental refresh mechanism of Materialized Views by filtering delta records.",
        "target_collection": VALIDATED_COLLECTION,
    },
]


def get_db(db: Optional[Database] = None) -> Database:
    if db is not None:
        return db
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return client[MONGO_DATABASE]


def create_phase2_indexes(db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """
    Creates or ensures the existence of all Phase 2 analytical indexes idempotently.
    Returns status report of all indexes.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]
    results = []

    existing_indexes = {idx["name"] for idx in coll.list_indexes()}

    for spec in PHASE2_INDEX_SPECS:
        idx_name = spec["name"]
        keys = spec["keys"]
        already_existed = idx_name in existing_indexes

        created_name = coll.create_index(keys, name=idx_name, background=True)
        results.append({
            "name": created_name,
            "type": spec["type"],
            "keys": keys,
            "status": "EXISTING" if already_existed else "CREATED",
            "purpose": spec["purpose"],
        })

    return results


def drop_phase2_indexes(db: Optional[Database] = None) -> List[str]:
    """
    Drops only the Phase 2 specialized indexes (preserving midterm primary/unique indexes).
    Useful for before/after explain benchmarking.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]
    dropped = []

    existing = {idx["name"] for idx in coll.list_indexes()}
    for spec in PHASE2_INDEX_SPECS:
        idx_name = spec["name"]
        if idx_name in existing:
            coll.drop_index(idx_name)
            dropped.append(idx_name)

    return dropped


def list_phase2_indexes(db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """
    Lists current status of Phase 2 indexes in MongoDB.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]
    existing = {idx["name"]: idx for idx in coll.list_indexes()}

    report = []
    for spec in PHASE2_INDEX_SPECS:
        name = spec["name"]
        is_active = name in existing
        report.append({
            "name": name,
            "type": spec["type"],
            "keys": spec["keys"],
            "is_active": is_active,
            "purpose": spec["purpose"],
            "raw_info": existing.get(name),
        })

    return report


if __name__ == "__main__":
    print("Creating Phase 2 Indexes...")
    res = create_phase2_indexes()
    for item in res:
        print(f" - [{item['status']}] {item['name']} ({item['type']})")
