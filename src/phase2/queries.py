from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

from config.settings import VALIDATED_COLLECTION
from src.phase2.indexes import get_db


def sanitize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Converts MongoDB BSON types (like ObjectId) to JSON-serializable primitives."""
    if not isinstance(doc, dict):
        return doc
    clean = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            clean[k] = str(v)
        elif isinstance(v, list):
            clean[k] = [sanitize_doc(item) if isinstance(item, dict) else item for item in v]
        elif isinstance(v, dict):
            clean[k] = sanitize_doc(v)
        else:
            clean[k] = v
    return clean


# ======================================================================
# 5 Independent Parameterized Analytical Queries
# ======================================================================

def orders_by_customer(
    customer_id: str = "عميل-1",
    limit: int = 50,
    db: Optional[Database] = None,
) -> List[Dict[str, Any]]:
    """
    Query 1: Customer Order History
    Finds orders for a specific customer, pre-sorted by date descending.
    Optimized by Compound Index: idx_customer_order_date
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]
    cursor = coll.find({"customer_id": customer_id}).sort("order_date", DESCENDING).limit(limit)
    return [sanitize_doc(doc) for doc in cursor]


def orders_by_city_status(
    city: str = "صنعاء",
    payment_status: str = "مؤكد",
    limit: int = 50,
    db: Optional[Database] = None,
) -> List[Dict[str, Any]]:
    """
    Query 2: City & Payment Status Multi-Attribute Filter
    Filters orders simultaneously by geographic city and payment confirmation status.
    Optimized by Compound Index: idx_city_payment_status
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]
    cursor = coll.find({"city": city, "payment_status": payment_status}).limit(limit)
    return [sanitize_doc(doc) for doc in cursor]


def high_value_orders(
    min_amount: float = 100000.0,
    limit: int = 50,
    db: Optional[Database] = None,
) -> List[Dict[str, Any]]:
    """
    Query 3: High Value Orders (VIP / Fraud Monitoring)
    Filters orders exceeding a financial threshold and sorts them from largest to smallest.
    Optimized by Single-Field Index: idx_total_amount_desc
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]
    cursor = coll.find({"total_amount": {"$gte": float(min_amount)}}).sort("total_amount", DESCENDING).limit(limit)
    return [sanitize_doc(doc) for doc in cursor]


def orders_by_date_range(
    start_date: str = "2025-01-01T00:00:00",
    end_date: str = "2025-01-15T23:59:59",
    limit: int = 50,
    db: Optional[Database] = None,
) -> List[Dict[str, Any]]:
    """
    Query 4: Temporal Range Cohort Filter
    Filters orders within an ISO date-time interval.
    Optimized by Single-Field Index: idx_order_date
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]
    cursor = coll.find({"order_date": {"$gte": start_date, "$lte": end_date}}).sort("order_date", ASCENDING).limit(limit)
    return [sanitize_doc(doc) for doc in cursor]


def orders_by_delivery_payment(
    delivery_type: str = "سريع",
    payment_method: str = "محفظة إلكترونية",
    limit: int = 50,
    db: Optional[Database] = None,
) -> List[Dict[str, Any]]:
    """
    Query 5: Logistical and Payment Channel Cross-Filter
    Analyzes orders fulfillment and payment method pairing (e.g. Express delivery + Electronic Wallet).
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]
    cursor = coll.find({"delivery_type": delivery_type, "payment_method": payment_method}).limit(limit)
    return [sanitize_doc(doc) for doc in cursor]


# ======================================================================
# Queries Registry & Dynamic Dispatcher
# ======================================================================

QUERIES_REGISTRY: Dict[str, Dict[str, Any]] = {
    "orders_by_customer": {
        "function": orders_by_customer,
        "description": "استعلام سجل طلبات عميل محدد مرتبة تنازلياً حسب التاريخ",
        "parameters": {
            "customer_id": {"type": "string", "required": True, "example": "عميل-1", "default": "عميل-1"},
            "limit": {"type": "integer", "required": False, "default": 50},
        },
        "target_index": "idx_customer_order_date",
    },
    "orders_by_city_status": {
        "function": orders_by_city_status,
        "description": "استعلام الطلبات حسب المدينة وحالة الدفع (مركّب)",
        "parameters": {
            "city": {"type": "string", "required": True, "example": "صنعاء", "default": "صنعاء"},
            "payment_status": {"type": "string", "required": True, "example": "مؤكد", "default": "مؤكد"},
            "limit": {"type": "integer", "required": False, "default": 50},
        },
        "target_index": "idx_city_payment_status",
    },
    "high_value_orders": {
        "function": high_value_orders,
        "description": "استعلام الطلبات ذات القيمة العالية مرتبة من الأعلى إلى الأدنى",
        "parameters": {
            "min_amount": {"type": "number", "required": False, "default": 100000.0, "example": 200000.0},
            "limit": {"type": "integer", "required": False, "default": 50},
        },
        "target_index": "idx_total_amount_desc",
    },
    "orders_by_date_range": {
        "function": orders_by_date_range,
        "description": "استعلام الطلبات ضمن فترة زمنية محددة",
        "parameters": {
            "start_date": {"type": "string", "required": True, "example": "2025-01-01T00:00:00", "default": "2025-01-01T00:00:00"},
            "end_date": {"type": "string", "required": True, "example": "2025-01-15T23:59:59", "default": "2025-01-15T23:59:59"},
            "limit": {"type": "integer", "required": False, "default": 50},
        },
        "target_index": "idx_order_date",
    },
    "orders_by_delivery_payment": {
        "function": orders_by_delivery_payment,
        "description": "استعلام الطلبات حسب نوع التوصيل وطريقة الدفع",
        "parameters": {
            "delivery_type": {"type": "string", "required": True, "example": "سريع", "default": "سريع"},
            "payment_method": {"type": "string", "required": True, "example": "محفظة إلكترونية", "default": "محفظة إلكترونية"},
            "limit": {"type": "integer", "required": False, "default": 50},
        },
        "target_index": None,
    },
}


def list_available_queries() -> List[Dict[str, Any]]:
    """Returns metadata for all available queries."""
    summary = []
    for name, meta in QUERIES_REGISTRY.items():
        summary.append({
            "name": name,
            "description": meta["description"],
            "parameters": meta["parameters"],
            "target_index": meta["target_index"],
        })
    return summary


def run_query(name: str, **kwargs: Any) -> Dict[str, Any]:
    """Runs a query dynamically by name with provided arguments."""
    if name not in QUERIES_REGISTRY:
        raise ValueError(f"Unknown query: '{name}'. Available: {list(QUERIES_REGISTRY.keys())}")
    
    meta = QUERIES_REGISTRY[name]
    fn = meta["function"]

    call_kwargs: Dict[str, Any] = {}
    for param_name, param_spec in meta.get("parameters", {}).items():
        if param_name in kwargs:
            call_kwargs[param_name] = kwargs[param_name]
        elif "default" in param_spec:
            call_kwargs[param_name] = param_spec["default"]
        elif "example" in param_spec:
            call_kwargs[param_name] = param_spec["example"]

    for k, v in kwargs.items():
        if k not in call_kwargs:
            call_kwargs[k] = v

    results = fn(**call_kwargs)
    return {
        "query_name": name,
        "count": len(results),
        "results": results,
    }
