from __future__ import annotations

import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from src.phase2.aggregations import AGGREGATIONS_REGISTRY, run_aggregation
from src.phase2.explain_runner import run_explain_benchmark
from src.phase2.indexes import create_phase2_indexes, get_db, list_phase2_indexes
from src.phase2.jobs import JOBS_REGISTRY, get_job_history, run_job_by_name
from src.phase2.materialized_views import (
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    get_all_mv_metadata,
    get_materialized_view,
    refresh_materialized_views,
)
from src.phase2.queries import QUERIES_REGISTRY, run_query


def main():
    print("=" * 80)
    print("      🚀 دليل الإثبات الشامل للمشروع النهائي — Big Data Phase 2")
    print("=" * 80)
    db = get_db()

    # -------------------------------------------------------------
    # 1. الاستعلامات والفهارس و Explain
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("1. إنشاء الفهارس التحليلية (3+ Indexes تشمل Compound Index):")
    print("🔹 " * 20)
    indexes = create_phase2_indexes(db)
    for idx in indexes:
        print(f"  ✅ [{idx['status']}] {idx['name']} ({idx['type']})")
        print(f"     الغرض: {idx['purpose']}")

    print("\n" + "🔹 " * 20)
    print("2. تشغيل الاستعلامات الـ 5 المخصصة لبيانات المشروع الحقيقية:")
    print("🔹 " * 20)
    
    # Execute all registered queries using live-data defaults.
    query_names = list(QUERIES_REGISTRY.keys())
    assert len(query_names) >= 5, "Rubric requires at least five independent queries."
    print(f"  Found {len(query_names)} registered queries.")
    for number, query_name in enumerate(query_names, start=1):
        result = run_query(query_name, limit=2)
        assert result["query_name"] == query_name
        assert isinstance(result["results"], list)
        print(f"  [Query {number}: {query_name}] returned {result['count']} result(s)")

    print("\n" + "🔹 " * 20)
    print("3. تشغيل مقارنة Explain (executionStats) قبل وبعد الفهارس لـ 3 استعلامات:")
    print("🔹 " * 20)
    benchmarks = run_explain_benchmark(db)
    assert benchmarks["status"] == "SUCCESS"
    assert benchmarks["explain_verbosity"] == "executionStats"
    assert len(benchmarks["benchmarks"]) >= 3
    for b in benchmarks["benchmarks"]:
        print(f"\n  🔍 {b['query_name']}:")
        print(f"     قبل الفهرس: Stage={b['before']['winning_stage']} | docsExamined={b['before']['total_docs_examined']:,} | Time={b['before']['execution_time_millis']}ms")
        print(f"     بعد الفهرس : Stage={b['after']['winning_stage']} | docsExamined={b['after']['total_docs_examined']:,} | Time={b['after']['execution_time_millis']}ms")
        print(f"     🎯 الوفر المحقق: تم توفير فحص {b['comparison']['docs_examined_saved']:,} وثيقة بنسبة {b['comparison']['docs_reduction_pct']}%")

    # -------------------------------------------------------------
    # 2. التقارير التجميعية Aggregations
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("4. تشغيل التقارير التجميعية الـ 5 (Aggregation Reports):")
    print("🔹 " * 20)
    assert len(AGGREGATIONS_REGISTRY) >= 5, "Rubric requires at least five aggregation reports."
    for agg_name in AGGREGATIONS_REGISTRY:
        res = run_aggregation(agg_name, limit=2)
        assert isinstance(res["results"], list)
        print(f"\n  📊 [{agg_name}] — {AGGREGATIONS_REGISTRY[agg_name]['description']}:")
        for row in res["results"][:2]:
            print(f"     • {row}")

    # -------------------------------------------------------------
    # 3. العروض المادية Materialized Views والتحديث التزايدي
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("5. العروض المادية (Materialized Views) واختبار التحديث التزايدي الذكي:")
    print("🔹 " * 20)
    # Full build
    mv_full = refresh_materialized_views("all", force_full=True, db=db)
    assert MV_DAILY_SALES in mv_full["views"]
    assert MV_TOP_PRODUCTS in mv_full["views"]
    print(f"  [إعادة بناء كاملة]: {list(mv_full['views'].keys())} — الحالة: {mv_full['status']}")
    
    # Incremental build
    mv_inc = refresh_materialized_views("all", force_full=False, db=db)
    print(f"  [تحديث تزايدي]:")
    for vname, vmeta in mv_inc['views'].items():
        print(f"     • {vname}: Mode={vmeta['mode']}, Status={vmeta['status']}, Processed={vmeta['records_processed']}")

    # Show samples
    sales_sample = get_materialized_view(MV_DAILY_SALES, limit=1, db=db)
    prod_sample = get_materialized_view(MV_TOP_PRODUCTS, limit=1, db=db)
    print(f"  [عينة daily_sales_summary]: {sales_sample[0] if sales_sample else 'Empty'}")
    print(f"  [عينة top_products_summary]: {prod_sample[0] if prod_sample else 'Empty'}")

    # -------------------------------------------------------------
    # 4. المهام المجدولة والتشغيل اليدوي وتسجيل السجلات
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("6. المهام المجدولة (Scheduled Jobs) وتسجيل السجلات في MongoDB:")
    print("🔹 " * 20)
    assert len(JOBS_REGISTRY) >= 2, "Rubric requires at least two scheduled jobs."
    job1_res = run_job_by_name("refresh_materialized_views_job", trigger_type="MANUAL")
    print(f"  ✅ [المهمة 1]: {job1_res['job_name']} | Status: {job1_res['status']} | Duration: {job1_res['duration_seconds']}s | Log ID: {job1_res['log_id']}")

    assert job1_res["status"] == "SUCCESS"
    assert job1_res["start_time"] and job1_res["end_time"]
    job2_res = run_job_by_name("data_quality_audit_job", trigger_type="MANUAL")
    assert job2_res["status"] == "SUCCESS"
    assert job2_res["start_time"] and job2_res["end_time"]
    print(f"  ✅ [المهمة 2]: {job2_res['job_name']} | Status: {job2_res['status']} | Duration: {job2_res['duration_seconds']}s | Log ID: {job2_res['log_id']}")

    # -------------------------------------------------------------
    # ملخص اكتمال المتطلبات
    # -------------------------------------------------------------
    print("\n" + "=" * 80)
    print("✨ تم تحقيق واختبار كافة متطلبات المشروع النهائي (Big Data Phase 2) بنجاح 100%!")
    print("=" * 80)


if __name__ == "__main__":
    main()
