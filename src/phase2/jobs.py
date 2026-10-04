from __future__ import annotations

import datetime
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo.database import Database

from config.settings import (
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.phase2.indexes import get_db
from src.phase2.materialized_views import refresh_materialized_views
from src.phase2.queries import sanitize_doc

JOB_LOGS_COLLECTION = "job_execution_logs"


def log_job_execution(
    db: Database,
    job_name: str,
    trigger_type: str,
    start_time: str,
    end_time: str,
    duration_seconds: float,
    status: str,
    details: Dict[str, Any],
    error_message: Optional[str] = None,
) -> str:
    """Records the job execution outcome in the job_execution_logs MongoDB collection."""
    doc = {
        "job_name": job_name,
        "trigger_type": trigger_type,  # 'SCHEDULED' or 'MANUAL'
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": round(duration_seconds, 4),
        "status": status,  # 'SUCCESS' or 'FAILED'
        "details": details,
        "error_message": error_message,
        "logged_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    res = db[JOB_LOGS_COLLECTION].insert_one(doc)
    return str(res.inserted_id)


# ======================================================================
# Job 1: Refresh Materialized Views Job
# ======================================================================

def run_refresh_mv_job(
    trigger_type: str = "MANUAL",
    db: Optional[Database] = None,
) -> Dict[str, Any]:
    """
    Job 1: Materialized Views Incremental Synchronizer
    Refreshes 'daily_sales_summary' and 'top_products_summary' incrementally.
    """
    db_inst = get_db(db)
    start_dt = datetime.datetime.now(datetime.timezone.utc)
    start_iso = start_dt.isoformat()
    status = "SUCCESS"
    error_msg = None
    details = {}

    try:
        details = refresh_materialized_views(view_name="all", force_full=False, db=db_inst)
    except Exception as exc:
        status = "FAILED"
        error_msg = f"{type(exc).__name__}: {str(exc)}\n{traceback.format_exc()}"
        details = {"error": str(exc)}

    end_dt = datetime.datetime.now(datetime.timezone.utc)
    end_iso = end_dt.isoformat()
    duration = (end_dt - start_dt).total_seconds()

    log_id = log_job_execution(
        db=db_inst,
        job_name="refresh_materialized_views_job",
        trigger_type=trigger_type,
        start_time=start_iso,
        end_time=end_iso,
        duration_seconds=duration,
        status=status,
        details=details,
        error_message=error_msg,
    )

    return {
        "log_id": log_id,
        "job_name": "refresh_materialized_views_job",
        "trigger_type": trigger_type,
        "status": status,
        "start_time": start_iso,
        "end_time": end_iso,
        "duration_seconds": duration,
        "details": details,
        "error_message": error_msg,
    }


# ======================================================================
# Job 2: Data Quality & Consistency Audit Job
# ======================================================================

def run_data_quality_audit_job(
    trigger_type: str = "MANUAL",
    db: Optional[Database] = None,
) -> Dict[str, Any]:
    """
    Job 2: Data Quality & Consistency Audit Job
    Validates cross-collection consistency (raw vs validated + quarantine),
    analyzes quarantine error frequencies, and monitors data pipeline health.
    """
    db_inst = get_db(db)
    start_dt = datetime.datetime.now(datetime.timezone.utc)
    start_iso = start_dt.isoformat()
    status = "SUCCESS"
    error_msg = None
    details = {}

    try:
        raw_count = db_inst[RAW_COLLECTION].count_documents({})
        valid_count = db_inst[VALIDATED_COLLECTION].count_documents({"quality_status": "valid"})
        corrected_count = db_inst[VALIDATED_COLLECTION].count_documents({"quality_status": "corrected"})
        validated_total = db_inst[VALIDATED_COLLECTION].count_documents({})
        quarantine_count = db_inst[QUARANTINE_COLLECTION].count_documents({})

        # Consistency equation check (for a clean single-run database):
        sum_classified = valid_count + corrected_count + quarantine_count

        # Quarantine error codes distribution
        quarantine_errors = list(
            db_inst[QUARANTINE_COLLECTION].aggregate([
                {"$unwind": "$error_codes"},
                {"$group": {"_id": "$error_codes", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10},
            ])
        )
        error_distribution = {doc["_id"]: doc["count"] for doc in quarantine_errors if "_id" in doc}

        details = {
            "audit_timestamp": start_iso,
            "raw_records_count": raw_count,
            "valid_records_count": valid_count,
            "corrected_records_count": corrected_count,
            "total_validated_count": validated_total,
            "quarantine_records_count": quarantine_count,
            "sum_classified": sum_classified,
            "is_equation_balanced": (sum_classified == raw_count) if raw_count > 0 else True,
            "quarantine_error_distribution": error_distribution,
            "system_health": "HEALTHY",
        }
    except Exception as exc:
        status = "FAILED"
        error_msg = f"{type(exc).__name__}: {str(exc)}\n{traceback.format_exc()}"
        details = {"error": str(exc)}

    end_dt = datetime.datetime.now(datetime.timezone.utc)
    end_iso = end_dt.isoformat()
    duration = (end_dt - start_dt).total_seconds()

    log_id = log_job_execution(
        db=db_inst,
        job_name="data_quality_audit_job",
        trigger_type=trigger_type,
        start_time=start_iso,
        end_time=end_iso,
        duration_seconds=duration,
        status=status,
        details=details,
        error_message=error_msg,
    )

    return {
        "log_id": log_id,
        "job_name": "data_quality_audit_job",
        "trigger_type": trigger_type,
        "status": status,
        "start_time": start_iso,
        "end_time": end_iso,
        "duration_seconds": duration,
        "details": details,
        "error_message": error_msg,
    }


# ======================================================================
# Jobs Registry & Querying Logs
# ======================================================================

JOBS_REGISTRY: Dict[str, Dict[str, Any]] = {
    "refresh_materialized_views_job": {
        "name": "refresh_materialized_views_job",
        "function": run_refresh_mv_job,
        "schedule": "Every 60 minutes (Hourly)",
        "interval_seconds": 3600,
        "description": "تحديث تزايدي ذكي للـ Materialized Views اعتماداً على الـ watermark",
    },
    "data_quality_audit_job": {
        "name": "data_quality_audit_job",
        "function": run_data_quality_audit_job,
        "schedule": "Every 120 minutes (Bi-hourly)",
        "interval_seconds": 7200,
        "description": "تدقيق جودة البيانات ومعادلة الاتساق وتوزيع أخطاء العزل عبر كافة المجموعات",
    },
}


def get_job_history(
    job_name: Optional[str] = None,
    limit: int = 20,
    db: Optional[Database] = None,
) -> List[Dict[str, Any]]:
    """Retrieves execution logs from MongoDB job_execution_logs collection."""
    db_inst = get_db(db)
    query = {"job_name": job_name} if job_name else {}
    cursor = db_inst[JOB_LOGS_COLLECTION].find(query).sort("start_time", -1).limit(int(limit))
    return [sanitize_doc(d) for d in cursor]


def run_job_by_name(name: str, trigger_type: str = "MANUAL") -> Dict[str, Any]:
    """Manually runs a registered job by name."""
    if name not in JOBS_REGISTRY:
        raise ValueError(f"Unknown job: '{name}'. Available: {list(JOBS_REGISTRY.keys())}")
    fn = JOBS_REGISTRY[name]["function"]
    return fn(trigger_type=trigger_type)


if __name__ == "__main__":
    print("Testing Manual Execution of Scheduled Jobs & Logging to MongoDB...")
    db = get_db()

    # Run Job 1 manually
    print("\n--- Running Job 1: refresh_materialized_views_job ---")
    res1 = run_job_by_name("refresh_materialized_views_job", trigger_type="MANUAL")
    print(f"Status: {res1['status']} | Duration: {res1['duration_seconds']}s | Log ID: {res1['log_id']}")

    # Run Job 2 manually
    print("\n--- Running Job 2: data_quality_audit_job ---")
    res2 = run_job_by_name("data_quality_audit_job", trigger_type="MANUAL")
    print(f"Status: {res2['status']} | Duration: {res2['duration_seconds']}s | Log ID: {res2['log_id']}")
    print(f"Audit details: {res2['details']}")

    # Check MongoDB logs
    print("\n--- Reading MongoDB Execution Logs ---")
    logs = get_job_history(limit=2, db=db)
    for log in logs:
        print(f"Log: [{log['job_name']}] Status={log['status']}, Trigger={log['trigger_type']}, Start={log['start_time']}")
