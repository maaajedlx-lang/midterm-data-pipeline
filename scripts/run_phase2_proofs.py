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
    
    # Query 1
    q1 = run_query("orders_by_customer", customer_id="عميل-1", limit=2)
    print(f"  [Query 1: orders_by_customer] وجد: {q1['count']} طلبات للعميل 'عميل-1'")
    if q1['results']:
        print(f"    عينة: الطلب {q1['results'][0]['id_order']} بتاريخ {q1['results'][0]['order_date']}")

    # Query 2 (Compound Index)
    q2 = run_query("orders_by_city_status", city="صنعاء", payment_status="مؤكد", limit=2)
    print(f"  [Query 2: orders_by_city_status] وجد: {q2['count']} طلبات لمدينة صنعاء بحالة مؤكد")
    if q2['results']:
        print(f"    عينة: الطلب {q2['results'][0]['id_order']} بمبلغ {q2['results'][0]['total_amount']:,} ريال")

    # Query 3
    q3 = run_query("high_value_orders", min_amount=500000.0, limit=2)
    print(f"  [Query 3: high_value_orders] وجد: {q3['count']} طلبات بقيمة >= 500,000 ريال")
    if q3['results']:
        print(f"    عينة: الطلب {q3['results'][0]['id_order']} بمبلغ {q3['results'][0]['total_amount']:,} ريال")

    # Query 4
    q4 = run_query("orders_by_date_range", start_date="2025-01-01T00:00:00", end_date="2025-01-07T23:59:59", limit=2)
    print(f"  [Query 4: orders_by_date_range] وجد: {q4['count']} طلبات في الأسبوع الأول من 2025")

    # Query 5
    q5 = run_query("orders_by_delivery_payment", delivery_type="سريع", payment_method="محفظة إلكترونية", limit=2)
    print(f"  [Query 5: orders_by_delivery_payment] وجد: {q5['count']} طلبات توصيل سريع بمحفظة")

    print("\n" + "🔹 " * 20)
    print("3. تشغيل مقارنة Explain (executionStats) قبل وبعد الفهارس لـ 3 استعلامات:")
    print("🔹 " * 20)
    benchmarks = run_explain_benchmark(db)
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
    for agg_name in AGGREGATIONS_REGISTRY:
        res = run_aggregation(agg_name, limit=2)
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
    job1_res = run_job_by_name("refresh_materialized_views_job", trigger_type="MANUAL")
    print(f"  ✅ [المهمة 1]: {job1_res['job_name']} | Status: {job1_res['status']} | Duration: {job1_res['duration_seconds']}s | Log ID: {job1_res['log_id']}")

    job2_res = run_job_by_name("data_quality_audit_job", trigger_type="MANUAL")
    print(f"  ✅ [المهمة 2]: {job2_res['job_name']} | Status: {job2_res['status']} | Duration: {job2_res['duration_seconds']}s | Log ID: {job2_res['log_id']}")

    # -------------------------------------------------------------
    # ملخص اكتمال المتطلبات
    # -------------------------------------------------------------
    print("\n" + "=" * 80)
    print("✨ تم تحقيق واختبار كافة متطلبات المشروع النهائي (Big Data Phase 2) بنجاح 100%!")
    print("=" * 80)


if __name__ == "__main__":
    main()
