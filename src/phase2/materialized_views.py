from __future__ import annotations

import datetime
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import ASCENDING, UpdateOne
from pymongo.database import Database

from config.settings import VALIDATED_COLLECTION
from src.phase2.indexes import get_db
from src.phase2.queries import sanitize_doc

MV_DAILY_SALES = "daily_sales_summary"
MV_TOP_PRODUCTS = "top_products_summary"
MV_METADATA_COLL = "mv_sync_metadata"


def get_current_watermark(db: Database, view_name: str) -> Optional[str]:
    """Retrieves the last synced 'updated_at' watermark timestamp for a specific view."""
    meta = db[MV_METADATA_COLL].find_one({"view_name": view_name})
    if meta and "last_synced_updated_at" in meta:
        return meta["last_synced_updated_at"]
    return None


def set_current_watermark(
    db: Database,
    view_name: str,
    watermark: str,
    records_processed: int,
    mode: str = "INCREMENTAL",
) -> None:
    """Updates the watermark metadata document in MongoDB."""
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    db[MV_METADATA_COLL].update_one(
        {"view_name": view_name},
        {
            "$set": {
                "view_name": view_name,
                "last_synced_updated_at": watermark,
                "last_sync_time": now_iso,
                "last_records_processed": records_processed,
                "last_sync_mode": mode,
                "status": "HEALTHY",
            }
        },
        upsert=True,
    )


# ======================================================================
# Materialized View 1: daily_sales_summary
# ======================================================================

def refresh_daily_sales_summary(
    db: Database,
    force_full: bool = False,
) -> Dict[str, Any]:
    """
    Refreshes the 'daily_sales_summary' Materialized View.
    Supports smart incremental delta refresh using the updated_at watermark.
    Only affected dates are re-aggregated and upserted.
    """
    coll_val = db[VALIDATED_COLLECTION]
    coll_mv = db[MV_DAILY_SALES]

    watermark = get_current_watermark(db, MV_DAILY_SALES) if not force_full else None

    # Step 1: Detect delta records
    match_stage = {"order_date": {"$type": "string", "$gte": "2000"}}
    if watermark:
        match_stage["updated_at"] = {"$gt": watermark}

    delta_count = coll_val.count_documents(match_stage)

    if not force_full and watermark and delta_count == 0:
        return {
            "view_name": MV_DAILY_SALES,
            "mode": "INCREMENTAL",
            "status": "UP_TO_DATE",
            "records_processed": 0,
            "affected_buckets": 0,
            "watermark": watermark,
        }

    # Step 2: Determine affected dates
    if watermark and not force_full:
        # Find distinct affected dates in the delta records
        affected_dates = coll_val.distinct(
            "order_date",
            match_stage,
        )
        affected_day_prefixes = sorted({d[:10] for d in affected_dates if isinstance(d, str) and len(d) >= 10})
        aggregation_filter = {
            "order_date": {"$type": "string", "$gte": "2000"},
            "$expr": {"$in": [{"$substrCP": ["$order_date", 0, 10]}, affected_day_prefixes]},
        }
    else:
        # Full initial or forced build
        aggregation_filter = {"order_date": {"$type": "string", "$gte": "2000"}}

    # Step 3: Run targeted aggregation pipeline
    pipeline = [
        {"$match": aggregation_filter},
        {
            "$project": {
                "day_str": {"$substrCP": ["$order_date", 0, 10]},
                "total_amount": "$total_amount",
                "delivery_cost": "$delivery_cost",
                "updated_at": "$updated_at",
            }
        },
        {
            "$group": {
                "_id": "$day_str",
                "total_sales": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "total_delivery_fees": {"$sum": "$delivery_cost"},
                "max_updated_at": {"$max": "$updated_at"},
            }
        },
    ]

    agg_cursor = list(coll_val.aggregate(pipeline))
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Step 4: Upsert aggregated buckets into daily_sales_summary collection
    ops = []
    max_seen_watermark = watermark or ""

    for doc in agg_cursor:
        day_id = doc["_id"]
        doc_watermark = str(doc.get("max_updated_at") or "")
        if doc_watermark > max_seen_watermark:
            max_seen_watermark = doc_watermark

        mv_doc = {
            "_id": day_id,
            "date": day_id,
            "total_sales": round(float(doc["total_sales"]), 2),
            "order_count": int(doc["order_count"]),
            "avg_order_value": round(float(doc["avg_order_value"]), 2),
            "total_delivery_fees": round(float(doc["total_delivery_fees"]), 2),
            "last_updated": now_iso,
        }
        ops.append(UpdateOne({"_id": day_id}, {"$set": mv_doc}, upsert=True))

    if ops:
        coll_mv.bulk_write(ops, ordered=False)

    # Step 5: Update watermark
    latest_doc = coll_val.find_one(sort=[("updated_at", -1)])
    highest_updated = latest_doc.get("updated_at", now_iso) if latest_doc else now_iso
    final_watermark = str(highest_updated)

    set_current_watermark(
        db=db,
        view_name=MV_DAILY_SALES,
        watermark=final_watermark,
        records_processed=len(ops),
        mode="FULL" if force_full or not watermark else "INCREMENTAL",
    )

    return {
        "view_name": MV_DAILY_SALES,
        "mode": "FULL" if force_full or not watermark else "INCREMENTAL",
        "status": "SUCCESS",
        "records_processed": delta_count if watermark and not force_full else len(agg_cursor),
        "affected_buckets": len(ops),
        "watermark": final_watermark,
    }


# ======================================================================
# Materialized View 2: top_products_summary
# ======================================================================

def refresh_top_products_summary(
    db: Database,
    force_full: bool = False,
) -> Dict[str, Any]:
    """
    Refreshes the 'top_products_summary' Materialized View.
    Supports smart incremental delta refresh using the updated_at watermark.
    Only affected product SKUs are re-aggregated and upserted.
    """
    coll_val = db[VALIDATED_COLLECTION]
    coll_mv = db[MV_TOP_PRODUCTS]

    watermark = get_current_watermark(db, MV_TOP_PRODUCTS) if not force_full else None

    # Step 1: Detect delta records
    match_stage = {"items.sku": {"$ne": None}}
    if watermark:
        match_stage["updated_at"] = {"$gt": watermark}

    delta_count = coll_val.count_documents(match_stage)

    if not force_full and watermark and delta_count == 0:
        return {
            "view_name": MV_TOP_PRODUCTS,
            "mode": "INCREMENTAL",
            "status": "UP_TO_DATE",
            "records_processed": 0,
            "affected_buckets": 0,
            "watermark": watermark,
        }

    # Step 2: Determine affected SKUs
    incremental_mode = bool(watermark and not force_full)

    if incremental_mode:
        # Identify the SKUs touched by the delta records.
        affected_skus = coll_val.distinct("items.sku", match_stage)
        if not affected_skus:
            return {
                "view_name": MV_TOP_PRODUCTS,
                "mode": "INCREMENTAL",
                "status": "UP_TO_DATE",
                "records_processed": 0,
                "affected_buckets": 0,
                "watermark": watermark,
            }
        aggregation_filter = {"items.sku": {"$in": affected_skus}}
        item_filter = {"items.sku": {"$in": affected_skus}}
    else:
        # Full initial or forced build
        aggregation_filter = {"items.sku": {"$ne": None}}
        item_filter = {"items.sku": {"$ne": None}}

    # Step 3: Run targeted aggregation pipeline
    pipeline = [
        {"$match": aggregation_filter},
        {"$unwind": "$items"},
        {"$match": item_filter},
        {
            "$group": {
                "_id": {
                    "sku": "$items.sku",
                    "name": "$items.name",
                },
                "total_quantity": {"$sum": "$items.qty"},
                "total_revenue": {"$sum": "$items.total"},
                "order_appearances": {"$sum": 1},
                "avg_unit_price": {"$avg": "$items.unit_price"},
            }
        },
    ]

    agg_cursor = list(coll_val.aggregate(pipeline))
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Step 4: Upsert aggregated buckets into top_products_summary collection
    ops = []
    for doc in agg_cursor:
        sku = doc["_id"]["sku"]
        pname = doc["_id"]["name"]
        mv_doc = {
            "_id": sku,
            "sku": sku,
            "product_name": pname,
            "total_quantity": int(doc["total_quantity"]),
            "total_revenue": round(float(doc["total_revenue"]), 2),
            "order_appearances": int(doc["order_appearances"]),
            "avg_unit_price": round(float(doc["avg_unit_price"]), 2),
            "last_updated": now_iso,
        }
        ops.append(UpdateOne({"_id": sku}, {"$set": mv_doc}, upsert=True))

    if ops:
        coll_mv.bulk_write(ops, ordered=False)

    # Step 5: Update watermark
    latest_doc = coll_val.find_one(sort=[("updated_at", -1)])
    highest_updated = latest_doc.get("updated_at", now_iso) if latest_doc else now_iso
    final_watermark = str(highest_updated)

    set_current_watermark(
        db=db,
        view_name=MV_TOP_PRODUCTS,
        watermark=final_watermark,
        records_processed=len(ops),
        mode="FULL" if force_full or not watermark else "INCREMENTAL",
    )

    return {
        "view_name": MV_TOP_PRODUCTS,
        "mode": "FULL" if force_full or not watermark else "INCREMENTAL",
        "status": "SUCCESS",
        "records_processed": delta_count if watermark and not force_full else len(agg_cursor),
        "affected_buckets": len(ops),
        "watermark": final_watermark,
    }


# ======================================================================
# Orchestrator & View Query Methods
# ======================================================================

def refresh_materialized_views(
    view_name: str = "all",
    force_full: bool = False,
    db: Optional[Database] = None,
) -> Dict[str, Any]:
    """
    Refreshes materialized views (all or a specific view) with incremental support.
    """
    db_inst = get_db(db)
    results = {}

    if view_name in ("all", MV_DAILY_SALES):
        results[MV_DAILY_SALES] = refresh_daily_sales_summary(db_inst, force_full=force_full)

    if view_name in ("all", MV_TOP_PRODUCTS):
        results[MV_TOP_PRODUCTS] = refresh_top_products_summary(db_inst, force_full=force_full)

    return {
        "status": "SUCCESS",
        "refreshed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "views": results,
    }


def get_materialized_view(
    view_name: str,
    limit: int = 50,
    db: Optional[Database] = None,
) -> List[Dict[str, Any]]:
    """Retrieves documents directly from the materialized view collection."""
    db_inst = get_db(db)
    if view_name not in (MV_DAILY_SALES, MV_TOP_PRODUCTS):
        raise ValueError(f"Unknown view: '{view_name}'. Valid: [{MV_DAILY_SALES}, {MV_TOP_PRODUCTS}]")

    sort_field = "date" if view_name == MV_DAILY_SALES else "total_revenue"
    cursor = db_inst[view_name].find().sort(sort_field, -1).limit(int(limit))
    return [sanitize_doc(d) for d in cursor]


def get_all_mv_metadata(db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """Returns sync status and watermarks for all materialized views."""
    db_inst = get_db(db)
    cursor = db_inst[MV_METADATA_COLL].find()
    return [sanitize_doc(d) for d in cursor]


if __name__ == "__main__":
    print("Testing Materialized Views Creation and Incremental Refresh...")
    db = get_db()
    
    # Run 1: Initial Build
    print("\n--- RUN 1: Initial Build ---")
    res1 = refresh_materialized_views(db=db, force_full=True)
    for vname, vinfo in res1["views"].items():
        print(f"[{vname}] Mode: {vinfo['mode']} | Status: {vinfo['status']} | Buckets: {vinfo['affected_buckets']}")

    # Run 2: Immediate Re-run (Incremental with no new data -> Should report UP_TO_DATE)
    print("\n--- RUN 2: Incremental Re-run (No new data) ---")
    res2 = refresh_materialized_views(db=db, force_full=False)
    for vname, vinfo in res2["views"].items():
        print(f"[{vname}] Mode: {vinfo['mode']} | Status: {vinfo['status']} | Processed: {vinfo['records_processed']}")

    # Inspect Views
    print("\n--- Inspection: daily_sales_summary Sample ---")
    sample_days = get_materialized_view(MV_DAILY_SALES, limit=2, db=db)
    for s in sample_days:
        print(" ", s)

    print("\n--- Inspection: top_products_summary Sample ---")
    sample_prods = get_materialized_view(MV_TOP_PRODUCTS, limit=2, db=db)
    for p in sample_prods:
        print(" ", p)
