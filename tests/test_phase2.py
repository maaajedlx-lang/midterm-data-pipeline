from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from fastapi.testclient import TestClient

from src.phase2.aggregations import AGGREGATIONS_REGISTRY, run_aggregation
from src.phase2.api import app
from src.phase2.explain_runner import run_explain_benchmark
from src.phase2.indexes import create_phase2_indexes, get_db, list_phase2_indexes
from src.phase2.jobs import JOBS_REGISTRY, get_job_history, run_job_by_name
from src.phase2.materialized_views import (
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    get_materialized_view,
    refresh_materialized_views,
)
from src.phase2.queries import QUERIES_REGISTRY, run_query


@pytest.fixture(scope="session")
def db_conn():
    return get_db()


@pytest.fixture(scope="session")
def api_client():
    return TestClient(app)


# 1. Indexes Tests
def test_indexes_creation_and_idempotency(db_conn):
    res1 = create_phase2_indexes(db_conn)
    assert len(res1) >= 3, "Must create at least 3 indexes"
    
    # Verify compound index presence
    compound_found = any(idx["type"] == "Compound Index" for idx in res1)
    assert compound_found, "Must include at least one Compound Index"

    # Verify idempotency (second call should not fail)
    res2 = create_phase2_indexes(db_conn)
    assert len(res2) == len(res1)
    all_existing = all(idx["status"] in ("EXISTING", "CREATED") for idx in res2)
    assert all_existing


# 2. Queries Tests
@pytest.mark.parametrize("query_name", list(QUERIES_REGISTRY.keys()))
def test_all_five_queries_execution(query_name, db_conn):
    res = run_query(query_name, limit=5)
    assert res["query_name"] == query_name
    assert isinstance(res["results"], list)
    assert res["count"] >= 0


# 3. Explain ExecutionStats Tests
def test_explain_benchmark_execution(db_conn):
    benchmark = run_explain_benchmark(db_conn)
    assert benchmark["status"] == "SUCCESS"
    assert benchmark["explain_verbosity"] == "executionStats"
    assert len(benchmark["benchmarks"]) == 3, "Must benchmark 3 queries"
    
    for item in benchmark["benchmarks"]:
        assert "before" in item
        assert "after" in item
        assert "comparison" in item
        # Winning plan after index should use IXSCAN
        assert "IXSCAN" in item["after"]["winning_stage"]
        # Docs examined after index must be lower than or equal to before
        assert item["after"]["total_docs_examined"] <= item["before"]["total_docs_examined"]


# 4. Aggregations Tests
@pytest.mark.parametrize("agg_name", list(AGGREGATIONS_REGISTRY.keys()))
def test_all_five_aggregations_execution(agg_name, db_conn):
    res = run_aggregation(agg_name, limit=5)
    assert res["aggregation_name"] == agg_name
    assert isinstance(res["results"], list)
    assert res["count"] > 0, f"Aggregation {agg_name} should return results from actual data"


# 5. Materialized Views & Incremental Refresh Tests
def test_materialized_views_incremental_refresh(db_conn):
    # Full build first
    full_res = refresh_materialized_views(view_name="all", force_full=True, db=db_conn)
    assert full_res["status"] == "SUCCESS"
    assert MV_DAILY_SALES in full_res["views"]
    assert MV_TOP_PRODUCTS in full_res["views"]
    assert full_res["views"][MV_DAILY_SALES]["mode"] == "FULL"

    # Incremental refresh with no changes (should be UP_TO_DATE)
    inc_res = refresh_materialized_views(view_name="all", force_full=False, db=db_conn)
    assert inc_res["views"][MV_DAILY_SALES]["status"] == "UP_TO_DATE"
    assert inc_res["views"][MV_DAILY_SALES]["records_processed"] == 0

    # Query view data
    sales_data = get_materialized_view(MV_DAILY_SALES, limit=5, db=db_conn)
    assert len(sales_data) > 0
    assert "date" in sales_data[0]
    assert "total_sales" in sales_data[0]

    prod_data = get_materialized_view(MV_TOP_PRODUCTS, limit=5, db=db_conn)
    assert len(prod_data) > 0
    assert "sku" in prod_data[0]
    assert "total_revenue" in prod_data[0]


# 6. Scheduled Jobs Tests
@pytest.mark.parametrize("job_name", list(JOBS_REGISTRY.keys()))
def test_scheduled_jobs_manual_execution_and_logging(job_name, db_conn):
    res = run_job_by_name(job_name, trigger_type="MANUAL")
    assert res["status"] == "SUCCESS"
    assert res["job_name"] == job_name
    assert "start_time" in res
    assert "end_time" in res
    assert "duration_seconds" in res
    assert res["log_id"] is not None

    # Check that execution log was written to MongoDB
    history = get_job_history(job_name=job_name, limit=1, db=db_conn)
    assert len(history) > 0
    assert history[0]["status"] == "SUCCESS"


# 7. FastAPI Endpoints Tests
def test_api_health_endpoint(api_client):
    r = api_client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "UP"
    assert data["mongodb"]["connected"] is True


def test_api_indexes_endpoints(api_client):
    r_post = api_client.post("/indexes")
    assert r_post.status_code == 200
    assert r_post.json()["status"] == "SUCCESS"

    r_get = api_client.get("/indexes")
    assert r_get.status_code == 200
    assert r_get.json()["count"] >= 3


def test_api_queries_endpoints(api_client):
    r_list = api_client.get("/queries")
    assert r_list.status_code == 200
    assert r_list.json()["count"] == 5

    r_exec = api_client.get("/queries/orders_by_city_status?city=صنعاء&payment_status=مؤكد&limit=2")
    assert r_exec.status_code == 200
    assert r_exec.json()["query_name"] == "orders_by_city_status"


def test_api_aggregations_endpoints(api_client):
    r_list = api_client.get("/aggregations")
    assert r_list.status_code == 200
    assert r_list.json()["count"] == 5

    r_exec = api_client.get("/aggregations/sales_by_city?limit=3")
    assert r_exec.status_code == 200
    assert 0 < r_exec.json()["count"] <= 3


def test_api_materialized_views_endpoints(api_client):
    r_refresh = api_client.post("/refresh-mv", json={"view_name": "all", "force_full": False})
    assert r_refresh.status_code == 200
    assert r_refresh.json()["status"] == "SUCCESS"

    r_view = api_client.get(f"/materialized-views/{MV_DAILY_SALES}?limit=3")
    assert r_view.status_code == 200
    assert r_view.json()["count"] > 0


def test_api_jobs_endpoints(api_client):
    r_list = api_client.get("/jobs")
    assert r_list.status_code == 200
    assert r_list.json()["total_jobs"] == 2

    r_run = api_client.post("/jobs/refresh_materialized_views_job/run")
    assert r_run.status_code == 200
    assert r_run.json()["status"] == "SUCCESS"
