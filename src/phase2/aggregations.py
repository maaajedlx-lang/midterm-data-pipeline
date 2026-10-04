from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bson import ObjectId
from pymongo.database import Database

from config.settings import VALIDATED_COLLECTION
from src.phase2.indexes import get_db
from src.phase2.queries import sanitize_doc


# ======================================================================
# 5 Specialized MongoDB Aggregation Pipelines
# ======================================================================

def sales_by_city(
    limit: int = 20,
    db: Optional[Database] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """
    Aggregation 1: Sales Performance by Geographic City
    Computes total revenue, order count, average order value, and range per city.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]

    pipeline = [
        {"$match": {"city": {"$ne": None}}},
        {
            "$group": {
                "_id": "$city",
                "total_sales": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "min_order_value": {"$min": "$total_amount"},
                "max_order_value": {"$max": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_sales": {"$round": ["$total_sales", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "min_order_value": {"$round": ["$min_order_value", 2]},
                "max_order_value": {"$round": ["$max_order_value", 2]},
            }
        },
        {"$sort": {"total_sales": -1}},
        {"$limit": int(limit)},
    ]
    return [sanitize_doc(d) for d in coll.aggregate(pipeline)]


def top_selling_products(
    limit: int = 20,
    db: Optional[Database] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """
    Aggregation 2: Top Selling Products
    Unwinds the items array, aggregating unit sales and gross revenue per SKU.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]

    pipeline = [
        {"$unwind": "$items"},
        {"$match": {"items.sku": {"$ne": None}}},
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
        {
            "$project": {
                "_id": 0,
                "sku": "$_id.sku",
                "product_name": "$_id.name",
                "total_quantity": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_appearances": 1,
                "avg_unit_price": {"$round": ["$avg_unit_price", 2]},
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": int(limit)},
    ]
    return [sanitize_doc(d) for d in coll.aggregate(pipeline)]


def top_valuable_customers(
    limit: int = 20,
    db: Optional[Database] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """
    Aggregation 3: Top Value Customers (VIP Client Ranking)
    Aggregates lifetime spend, total transactions, and average spend per customer.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]

    pipeline = [
        {"$match": {"customer_id": {"$ne": None}}},
        {
            "$group": {
                "_id": {
                    "customer_id": "$customer_id",
                    "customer_name": "$customer_name",
                },
                "total_spent": {"$sum": "$total_amount"},
                "total_orders": {"$sum": 1},
                "avg_spent_per_order": {"$avg": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id.customer_id",
                "customer_name": "$_id.customer_name",
                "total_spent": {"$round": ["$total_spent", 2]},
                "total_orders": 1,
                "avg_spent_per_order": {"$round": ["$avg_spent_per_order", 2]},
            }
        },
        {"$sort": {"total_spent": -1}},
        {"$limit": int(limit)},
    ]
    return [sanitize_doc(d) for d in coll.aggregate(pipeline)]


def payment_status_distribution(
    limit: Optional[int] = None,
    db: Optional[Database] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """
    Aggregation 4: Financial Payment Status & Method Matrix
    Groups transactions by payment method and delivery status to compute revenue collection rate.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]

    pipeline = [
        {
            "$group": {
                "_id": {
                    "payment_status": "$payment_status",
                    "payment_method": "$payment_method",
                },
                "order_count": {"$sum": 1},
                "total_volume": {"$sum": "$total_amount"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "payment_status": "$_id.payment_status",
                "payment_method": "$_id.payment_method",
                "order_count": 1,
                "total_volume": {"$round": ["$total_volume", 2]},
            }
        },
        {"$sort": {"order_count": -1}},
    ]
    if limit is not None:
        pipeline.append({"$limit": int(limit)})
    return [sanitize_doc(d) for d in coll.aggregate(pipeline)]


def daily_revenue_trend(
    limit: int = 30,
    db: Optional[Database] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """
    Aggregation 5: Daily Revenue & Logistical Fees Trend
    Extracts date string prefix (YYYY-MM-DD) from order_date and computes daily velocity.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]

    pipeline = [
        {"$match": {"order_date": {"$type": "string", "$gte": "2000"}}},
        {
            "$project": {
                "day_str": {"$substrCP": ["$order_date", 0, 10]},
                "total_amount": "$total_amount",
                "delivery_cost": "$delivery_cost",
            }
        },
        {
            "$group": {
                "_id": "$day_str",
                "daily_sales": {"$sum": "$total_amount"},
                "daily_orders": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "total_delivery_fees": {"$sum": "$delivery_cost"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "daily_sales": {"$round": ["$daily_sales", 2]},
                "daily_orders": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "total_delivery_fees": {"$round": ["$total_delivery_fees", 2]},
            }
        },
        {"$sort": {"date": -1}},
        {"$limit": int(limit)},
    ]
    return [sanitize_doc(d) for d in coll.aggregate(pipeline)]


# ======================================================================
# Aggregations Registry & Dynamic Dispatcher
# ======================================================================

AGGREGATIONS_REGISTRY: Dict[str, Dict[str, Any]] = {
    "sales_by_city": {
        "function": sales_by_city,
        "description": "تقرير إجمالي المبيعات ومتوسط قيمة الطلب حسب المدينة",
        "parameters": {
            "limit": {"type": "integer", "required": False, "default": 20},
        },
    },
    "top_selling_products": {
        "function": top_selling_products,
        "description": "تقرير المنتجات الأكثر مبيعاً وتحقيقاً للإيرادات (تفكيك مصفوفة العناصر)",
        "parameters": {
            "limit": {"type": "integer", "required": False, "default": 20},
        },
    },
    "top_valuable_customers": {
        "function": top_valuable_customers,
        "description": "تقرير تصنيف كبار العملاء الأكثر إنفاقاً وطلباً",
        "parameters": {
            "limit": {"type": "integer", "required": False, "default": 20},
        },
    },
    "payment_status_distribution": {
        "function": payment_status_distribution,
        "description": "تقرير توزيع حالات الدفع وطرق السداد والمبالغ المحصلة",
        "parameters": {
            "limit": {"type": "integer", "required": False, "default": None},
        },
    },
    "daily_revenue_trend": {
        "function": daily_revenue_trend,
        "description": "تقرير الاتجاه الزمني اليومي للإيرادات ورسوم التوصيل",
        "parameters": {
            "limit": {"type": "integer", "required": False, "default": 30},
        },
    },
}


def list_available_aggregations() -> List[Dict[str, Any]]:
    """Returns metadata for all available aggregation reports."""
    summary = []
    for name, meta in AGGREGATIONS_REGISTRY.items():
        summary.append({
            "name": name,
            "description": meta["description"],
            "parameters": meta["parameters"],
        })
    return summary


def run_aggregation(name: str, **kwargs: Any) -> Dict[str, Any]:
    """Runs an aggregation report dynamically by name with provided arguments."""
    if name not in AGGREGATIONS_REGISTRY:
        raise ValueError(f"Unknown aggregation: '{name}'. Available: {list(AGGREGATIONS_REGISTRY.keys())}")

    meta = AGGREGATIONS_REGISTRY[name]
    fn = meta["function"]
    results = fn(**kwargs)

    return {
        "aggregation_name": name,
        "count": len(results),
        "results": results,
    }


if __name__ == "__main__":
    print("Testing 5 Aggregation Reports on actual MongoDB data...")
    for agg_name in AGGREGATIONS_REGISTRY:
        data = run_aggregation(agg_name, limit=3)
        print(f"\n[{agg_name}] - returned {data['count']} groups:")
        for r in data["results"][:2]:
            print("  ", r)
