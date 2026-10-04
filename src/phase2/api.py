from __future__ import annotations

import contextlib
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from config.settings import (
    DEFAULT_INPUT_FILE,
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)
from src.elt_pipeline import execute_elt_pipeline
from src.phase2.aggregations import (
    AGGREGATIONS_REGISTRY,
    list_available_aggregations,
    run_aggregation,
)
from src.phase2.explain_runner import run_explain_benchmark
from src.phase2.indexes import create_phase2_indexes, get_db, list_phase2_indexes
from src.phase2.jobs import JOBS_REGISTRY, get_job_history
from src.phase2.materialized_views import (
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    get_all_mv_metadata,
    get_materialized_view,
    refresh_materialized_views,
)
from src.phase2.queries import QUERIES_REGISTRY, list_available_queries, run_query
from src.phase2.scheduler import GLOBAL_SCHEDULER


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Launch background job scheduler daemon
    GLOBAL_SCHEDULER.start()
    yield
    # Shutdown: Stop scheduler gracefully
    GLOBAL_SCHEDULER.stop()


app = FastAPI(
    title="Big Data Hybrid ELT & Analytics API (Phase 2)",
    description=(
        "واجهة تشغيل موحدة للمشروع النهائي لمقرر البيانات الضخمة (Phase 2). "
        "تتيح تشغيل واختبار خط الإدخال الهجين، الفهارس الذكية، الاستعلامات، التقارير التجميعية، "
        "العروض المادية بالتحديث التزايدي، والمهام المجدولة."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ======================================================================
# Request / Response Schemas
# ======================================================================

class IngestRequest(BaseModel):
    file_path: Optional[str] = Field(
        default=str(DEFAULT_INPUT_FILE),
        description="مسار ملف البيانات المدخل (افتراضياً ملف العينة المعتمد)",
    )
    engine: Optional[str] = Field(
        default=None,
        description="محرك المعالجة المطلوب (None للتوجيه الآلي، أو 'python_batch' أو 'pyspark')",
    )
    reset_db: bool = Field(
        default=False,
        description="تفريغ المجموعات قبل المعالجة (False للاختبار التراكمي/اللاتكرارية)",
    )


class RefreshMvRequest(BaseModel):
    view_name: str = Field(
        default="all",
        description="اسم العرض المادي المطلوب تحديثه: 'all' أو 'daily_sales_summary' أو 'top_products_summary'",
    )
    force_full: bool = Field(
        default=False,
        description="إجبار إعادة البناء الكامل بدلاً من التحديث التزايدي الذكي",
    )


# ======================================================================
# 1. GET /health
# ======================================================================

@app.get("/health", tags=["System Health"])
def get_health() -> Dict[str, Any]:
    """
    يعيد حالة النظام، اتصال قاعدة بيانات MongoDB، أعداد الوثائق في كافة المجموعات، وحالة المجدول.
    """
    db = get_db()
    try:
        ping = db.command("ping")
        is_mongo_ok = bool(ping.get("ok", 0) == 1)
    except Exception as exc:
        is_mongo_ok = False

    raw_count = db[RAW_COLLECTION].count_documents({}) if is_mongo_ok else 0
    val_count = db[VALIDATED_COLLECTION].count_documents({}) if is_mongo_ok else 0
    quar_count = db[QUARANTINE_COLLECTION].count_documents({}) if is_mongo_ok else 0
    daily_sales_count = db[MV_DAILY_SALES].count_documents({}) if is_mongo_ok else 0
    top_prods_count = db[MV_TOP_PRODUCTS].count_documents({}) if is_mongo_ok else 0

    return {
        "status": "UP" if is_mongo_ok else "DEGRADED",
        "mongodb": {
            "connected": is_mongo_ok,
            "database": MONGO_DATABASE,
            "collections_count": {
                "orders_raw": raw_count,
                "orders_validated": val_count,
                "quarantine_orders": quar_count,
                "daily_sales_summary": daily_sales_count,
                "top_products_summary": top_prods_count,
            },
        },
        "scheduler": {
            "running": GLOBAL_SCHEDULER.is_running(),
            "active_jobs_count": len(JOBS_REGISTRY),
        },
        "phase": "Big Data - Phase 2 (Final Project)",
    }


# ======================================================================
# 2. POST /ingest (Reuses existing Midterm Ingestion Pipeline)
# ======================================================================

@app.post("/ingest", tags=["Ingestion Gateway"])
def trigger_ingest(payload: IngestRequest) -> Dict[str, Any]:
    """
    يشغل خط معالجة البيانات النصفي (Hybrid ELT Pipeline) عبر نفس بوابة الإدخال الأصلية دون تكرار الكود.
    """
    input_path = Path(payload.file_path)
    if not input_path.is_absolute():
        input_path = PROJECT_ROOT / input_path

    if not input_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Input file not found at: {input_path}",
        )

    try:
        metrics = execute_elt_pipeline(
            input_file=input_path,
            engine_override=payload.engine,
            reset_db=payload.reset_db,
        )
        return {
            "status": "SUCCESS",
            "message": "Ingestion pipeline completed successfully via Midterm Engine.",
            "metrics": metrics,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline execution failed: {str(exc)}",
        )


# ======================================================================
# 3. POST /indexes & GET /indexes
# ======================================================================

@app.post("/indexes", tags=["Indexes Management"])
def create_indexes() -> Dict[str, Any]:
    """
    ينشئ أو يضمن وجود كافة الفهارس التحليلية المطلوبة (3+ فهارس بما فيها Compound Index) بأمان ودون تكرار.
    """
    try:
        res = create_phase2_indexes()
        return {
            "status": "SUCCESS",
            "message": f"Ensured {len(res)} specialized indexes on MongoDB.",
            "indexes": res,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create indexes: {str(exc)}",
        )


@app.get("/indexes", tags=["Indexes Management"])
def list_indexes() -> Dict[str, Any]:
    """
    يعرض الفهارس النشطة وحالتها في قاعدة البيانات.
    """
    indexes = list_phase2_indexes()
    return {
        "count": len(indexes),
        "indexes": indexes,
    }


# ======================================================================
# 4. GET /queries & GET /queries/{name}
# ======================================================================

@app.get("/queries", tags=["Queries"])
def get_queries() -> Dict[str, Any]:
    """
    يعرض قائمة الاستعلامات العملية الـ 5 المتاحة مع معايير الضبط والفهارس الخادمة لها.
    """
    return {
        "count": len(QUERIES_REGISTRY),
        "queries": list_available_queries(),
    }


@app.get("/queries/{name}", tags=["Queries"])
def execute_query(
    name: str,
    request: Request,
    limit: int = Query(50, ge=1, le=1000, description="الحد الأقصى للنتائج المرجعة"),
) -> Dict[str, Any]:
    """
    يشغل استعلاماً محدداً بالاسم مع تمرير المعاملات المطلوبة ويعيد النتيجة بصيغة JSON.
    """
    if name not in QUERIES_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Query '{name}' not found. Available: {list(QUERIES_REGISTRY.keys())}",
        )

    # Extract all query parameters dynamically
    raw_params = dict(request.query_params)
    parsed_kwargs: Dict[str, Any] = {"limit": limit}

    for k, v in raw_params.items():
        if k == "limit":
            continue
        # Convert numeric parameters if applicable
        if k in ("min_amount", "amount"):
            try:
                parsed_kwargs[k] = float(v)
            except ValueError:
                parsed_kwargs[k] = v
        else:
            parsed_kwargs[k] = v

    try:
        # Omitted query filters are inferred from the live validated dataset.
        res = run_query(name, **parsed_kwargs)
        return res
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query execution error: {str(exc)}",
        )


# ======================================================================
# 5. GET /aggregations & GET /aggregations/{name}
# ======================================================================

@app.get("/aggregations", tags=["Aggregation Reports"])
def get_aggregations() -> Dict[str, Any]:
    """
    يعرض قائمة التقارير التجميعية الـ 5 المتاحة ومواصفات كل تقرير.
    """
    return {
        "count": len(AGGREGATIONS_REGISTRY),
        "aggregations": list_available_aggregations(),
    }


@app.get("/aggregations/{name}", tags=["Aggregation Reports"])
def execute_aggregation(
    name: str,
    limit: Optional[int] = Query(20, ge=1, le=500, description="الحد الأقصى للمجموعات المرجعة"),
) -> Dict[str, Any]:
    """
    يشغل تقريراً تجميعياً محدداً بالاسم ويعيد نتائجه التحليلية الفعلية بصيغة JSON.
    """
    if name not in AGGREGATIONS_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Aggregation '{name}' not found. Available: {list(AGGREGATIONS_REGISTRY.keys())}",
        )

    try:
        kwargs: Dict[str, Any] = {}
        if limit is not None:
            kwargs["limit"] = limit
        res = run_aggregation(name, **kwargs)
        return res
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Aggregation execution failed: {str(exc)}",
        )


# ======================================================================
# 6. POST /refresh-mv & GET /materialized-views/{name}
# ======================================================================

@app.post("/refresh-mv", tags=["Materialized Views"])
def trigger_refresh_mv(payload: RefreshMvRequest = RefreshMvRequest()) -> Dict[str, Any]:
    """
    يشغل تحديث الـ Materialized Views (daily_sales_summary و top_products_summary).
    يستفيد تلقائياً من آلية التحديث التزايدي (Incremental Watermark) لتجنب إعادة بناء البيانات من البداية.
    """
    try:
        res = refresh_materialized_views(
            view_name=payload.view_name,
            force_full=payload.force_full,
        )
        return res
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Materialized view refresh failed: {str(exc)}",
        )


@app.get("/materialized-views/metadata", tags=["Materialized Views"])
def get_mv_metadata_status() -> Dict[str, Any]:
    """
    يعرض حالة وعلامات المزامنة المائية (Watermarks) للـ Materialized Views.
    """
    return {
        "metadata": get_all_mv_metadata(),
    }


@app.get("/materialized-views/{name}", tags=["Materialized Views"])
def view_materialized_view_content(
    name: str,
    limit: int = Query(50, ge=1, le=500),
) -> Dict[str, Any]:
    """
    يسترجع محتويات أحد العروض المادية مباشرة من مجموعته المخزنة.
    """
    try:
        data = get_materialized_view(name, limit=limit)
        return {
            "view_name": name,
            "count": len(data),
            "data": data,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        )


# ======================================================================
# 7. GET /jobs & POST /jobs/{name}/run
# ======================================================================

@app.get("/jobs", tags=["Scheduled Jobs"])
def get_jobs_list() -> Dict[str, Any]:
    """
    يعرض المهام المجدولة المسجلة، جداولها الزمنية، موعد التشغيل القادم، وحالة التشغيل الحي.
    """
    status_list = GLOBAL_SCHEDULER.get_status()
    recent_logs = get_job_history(limit=5)
    return {
        "scheduler_running": GLOBAL_SCHEDULER.is_running(),
        "total_jobs": len(status_list),
        "jobs": status_list,
        "recent_execution_logs": recent_logs,
    }


@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"])
def run_job_manually(name: str) -> Dict[str, Any]:
    """
    يشغل مهمة مجدولة يدوياً وفورياً، ويسجل وقت البداية، وقت النهاية، الحالة والنتيجة في MongoDB.
    """
    if name not in JOBS_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{name}' not found. Available: {list(JOBS_REGISTRY.keys())}",
        )

    try:
        result = GLOBAL_SCHEDULER.trigger_now(name)
        return {
            "status": "SUCCESS",
            "message": f"Job '{name}' executed manually.",
            "execution": result,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Job manual execution failed: {str(exc)}",
        )


# ======================================================================
# 8. GET /explain (Bonus Endpoint: Explain Before/After Benchmark)
# ======================================================================

@app.get("/explain", tags=["Indexes Management"])
def get_explain_benchmark() -> Dict[str, Any]:
    """
    يشغل فحص المقارنة لمؤشرات الأداء Explain (executionStats) قبل الفهارس وبعدها لـ 3 استعلامات.
    """
    try:
        res = run_explain_benchmark()
        return res
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Explain benchmark execution failed: {str(exc)}",
        )


# Standalone runner for testing or direct launch
if __name__ == "__main__":
    import uvicorn
    print("Starting FastAPI application on http://127.0.0.1:8000 ...")
    uvicorn.run("src.phase2.api:app", host="127.0.0.1", port=8000, reload=False)
