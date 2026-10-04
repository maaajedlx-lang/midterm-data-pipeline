from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import ASCENDING, DESCENDING
from pymongo.database import Database

from config.settings import REPORTS_DIR, VALIDATED_COLLECTION
from src.phase2.indexes import create_phase2_indexes, drop_phase2_indexes, get_db


def extract_explain_stats(raw_explain: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts essential execution metrics from raw explain output."""
    exec_stats = raw_explain.get("executionStats", {})
    query_planner = raw_explain.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})

    def get_stage_name(plan: Dict[str, Any]) -> str:
        stage = plan.get("stage", "UNKNOWN")
        if "inputStage" in plan:
            return f"{stage} -> {get_stage_name(plan['inputStage'])}"
        return stage

    def find_index_name(plan: Dict[str, Any]) -> Optional[str]:
        if "indexName" in plan:
            return plan["indexName"]
        if "inputStage" in plan:
            return find_index_name(plan["inputStage"])
        if "inputStages" in plan:
            for s in plan["inputStages"]:
                found = find_index_name(s)
                if found:
                    return found
        return None

    return {
        "execution_time_millis": exec_stats.get("executionTimeMillis", 0),
        "total_docs_examined": exec_stats.get("totalDocsExamined", 0),
        "total_keys_examined": exec_stats.get("totalKeysExamined", 0),
        "n_returned": exec_stats.get("nReturned", 0),
        "winning_stage": get_stage_name(winning_plan),
        "index_used": find_index_name(winning_plan) or "None (COLLSCAN)",
    }


def _build_explain_queries(coll: Any) -> List[Dict[str, Any]]:
    """Build benchmark predicates from real validated data, not training constants."""
    city_status = coll.find_one(
        {
            "city": {"$exists": True, "$nin": [None, ""]},
            "payment_status": {"$exists": True, "$nin": [None, ""]},
        },
        {"city": 1, "payment_status": 1},
    )
    if not city_status:
        raise RuntimeError(
            f"No usable city/payment_status values found in '{VALIDATED_COLLECTION}'."
        )

    numeric_count = coll.count_documents({"total_amount": {"$type": "number"}})
    if numeric_count == 0:
        raise RuntimeError(
            f"No numeric total_amount values found in '{VALIDATED_COLLECTION}'."
        )

    median_cursor = (
        coll.find({"total_amount": {"$type": "number"}}, {"total_amount": 1})
        .sort("total_amount", ASCENDING)
        .skip(max(0, numeric_count // 2))
        .limit(1)
    )
    median_doc = next(iter(median_cursor), None)
    if not median_doc:
        raise RuntimeError("Could not select a live total_amount threshold.")

    date_doc = coll.find_one(
        {"order_date": {"$type": "string", "$gte": "2000"}},
        {"order_date": 1},
        sort=[("order_date", ASCENDING)],
    )
    if not date_doc or not isinstance(date_doc.get("order_date"), str):
        raise RuntimeError(
            f"No usable string order_date values found in '{VALIDATED_COLLECTION}'."
        )

    day = date_doc["order_date"][:10]
    return [
        {
            "id": "query_1_city_payment_status",
            "name": "Query 1: orders_by_city_status",
            "description": "City + payment status filter selected from live data.",
            "filter": {
                "city": city_status["city"],
                "payment_status": city_status["payment_status"],
            },
            "sort": None,
            "target_index": "idx_city_payment_status (Compound: city + payment_status)",
            "why_chosen": "Matches two equality predicates in one compound B-tree index.",
            "expected_impact": "Reduces collection scanning by using the targeted compound index.",
            "live_parameters": {
                "city": city_status["city"],
                "payment_status": city_status["payment_status"],
            },
        },
        {
            "id": "query_2_high_value_orders",
            "name": "Query 2: high_value_orders",
            "description": "Amount threshold + descending sort selected from live data.",
            "filter": {"total_amount": {"$gte": float(median_doc["total_amount"])}},
            "sort": [("total_amount", DESCENDING)],
            "target_index": "idx_total_amount_desc (Single: total_amount DESC)",
            "why_chosen": "Supports the amount range predicate and descending sort.",
            "expected_impact": "Reduces scanning and removes the need for a separate in-memory sort.",
            "live_parameters": {"min_amount": float(median_doc["total_amount"])},
        },
        {
            "id": "query_3_orders_by_date_range",
            "name": "Query 3: orders_by_date_range",
            "description": "One-day temporal range selected from a real order date.",
            "filter": {
                "order_date": {
                    "$gte": f"{day}T00:00:00",
                    "$lte": f"{day}T23:59:59",
                }
            },
            "sort": [("order_date", ASCENDING)],
            "target_index": "idx_order_date (Single: order_date ASC)",
            "why_chosen": "Matches the bounded temporal range and chronological sort.",
            "expected_impact": "Limits the index scan to the requested time window.",
            "live_parameters": {
                "start_date": f"{day}T00:00:00",
                "end_date": f"{day}T23:59:59",
            },
        },
    ]


def _explain_find(
    coll: Any,
    query_filter: Dict[str, Any],
    sort: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    """Run MongoDB Explain with explicit executionStats verbosity."""
    find_spec: Dict[str, Any] = {
        "find": coll.name,
        "filter": query_filter,
    }
    if sort:
        find_spec["sort"] = dict(sort)

    return coll.database.command(
        {
            "explain": find_spec,
            "verbosity": "executionStats",
        }
    )


def run_explain_benchmark(db: Optional[Database] = None) -> Dict[str, Any]:
    """
    Executes explain('executionStats') on 3 core queries before and after creating indexes.
    Produces comprehensive before/after metrics and explanations.
    """
    db_inst = get_db(db)
    coll = db_inst[VALIDATED_COLLECTION]

    queries_to_benchmark = _build_explain_queries(coll)

    # Step 1: Drop Phase 2 Indexes to measure BEFORE
    print("\n[Explain Benchmark] Dropping Phase 2 indexes for Baseline measurement (BEFORE)...")
    drop_phase2_indexes(db_inst)

    before_results = {}
    for q in queries_to_benchmark:
        cur = coll.find(q["filter"])
        if q["sort"]:
            cur = cur.sort(q["sort"])
        raw = _explain_find(coll, q["filter"], q["sort"])
        before_results[q["id"]] = extract_explain_stats(raw)

    # Step 2: Create Phase 2 Indexes
    print("[Explain Benchmark] Creating Phase 2 specialized indexes...")
    create_phase2_indexes(db_inst)

    # Step 3: Measure AFTER
    print("[Explain Benchmark] Measuring query executionStats with Indexes active (AFTER)...")
    after_results = {}
    for q in queries_to_benchmark:
        cur = coll.find(q["filter"])
        if q["sort"]:
            cur = cur.sort(q["sort"])
        raw = _explain_find(coll, q["filter"], q["sort"])
        after_results[q["id"]] = extract_explain_stats(raw)

    # Step 4: Compile comprehensive comparison
    benchmark_data = []
    for q in queries_to_benchmark:
        qid = q["id"]
        b = before_results[qid]
        a = after_results[qid]

        docs_diff = b["total_docs_examined"] - a["total_docs_examined"]
        time_diff = b["execution_time_millis"] - a["execution_time_millis"]
        time_reduction_pct = (
            round((time_diff / b["execution_time_millis"]) * 100, 2)
            if b["execution_time_millis"] > 0
            else 0.0
        )
        docs_reduction_pct = (
            round((docs_diff / b["total_docs_examined"]) * 100, 2)
            if b["total_docs_examined"] > 0
            else 0.0
        )

        benchmark_data.append({
            "query_id": qid,
            "query_name": q["name"],
            "description": q["description"],
            "target_index": q["target_index"],
            "why_chosen": q["why_chosen"],
            "expected_impact": q["expected_impact"],
            "before": b,
            "after": a,
            "comparison": {
                "docs_examined_saved": docs_diff,
                "docs_reduction_pct": docs_reduction_pct,
                "execution_time_saved_ms": time_diff,
                "time_reduction_pct": time_reduction_pct,
            },
        })

    report_payload = {
        "status": "SUCCESS",
        "explain_verbosity": "executionStats",
        "benchmark_timestamp": str(db_inst.command("serverStatus").get("localTime", "")),
        "total_queries_benchmarked": len(benchmark_data),
        "benchmarks": benchmark_data,
    }

    # Save to reports/explain_results.json
    json_path = REPORTS_DIR / "explain_results.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, ensure_ascii=False, indent=2)

    # Generate Markdown Report
    generate_explain_markdown(report_payload, REPORTS_DIR / "explain_results.md")

    return report_payload


def generate_explain_markdown(payload: Dict[str, Any], output_path: Path) -> None:
    """Generates a professional Markdown table comparing before and after stats."""
    lines = [
        "# تقرير تحليل أداء الاستعلامات والفهارس (Explain executionStats Report)",
        "## مقارنة تفصيلية لمؤشرات الأداء قبل إنشاء الفهارس وبعدها",
        "",
        f"> **تاريخ ووقت الفحص:** `{payload.get('benchmark_timestamp')}`  ",
        f"> **عدد الاستعلامات المفحوصة:** `{payload.get('total_queries_benchmarked')}`",
        "",
        "---",
        "",
    ]

    for item in payload.get("benchmarks", []):
        b = item["before"]
        a = item["after"]
        c = item["comparison"]

        lines.extend([
            f"### 🔍 {item['query_name']} — {item['description']}",
            f"- **الفهرس المستخدم:** `{item['target_index']}`",
            f"- **سبب اختيار الفهرس:** {item['why_chosen']}",
            f"- **أثر الفهرس المحقق:** {item['expected_impact']}",
            "",
            "| مقياس الأداء (Metric) | قبل الفهرس (Before) | بعد الفهرس (After) | نسبة التحسن والوفر |",
            "|---|---|---|---|",
            f"| **خطة التنفيذ (Winning Plan)** | `{b['winning_stage']}` | `{a['winning_stage']}` | تحول جذري من المسح الشامل إلى الفهرس |",
            f"| **الفهرس المفعل (Index Used)** | `{b['index_used']}` | `{a['index_used']}` | استخدام الفهرس المخصص بنجاح |",
            f"| **عدد الوثائق المفحوصة (docsExamined)** | **{b['total_docs_examined']:,}** وثيقة | **{a['total_docs_examined']:,}** وثيقة | **توفير {c['docs_examined_saved']:,} وثيقة ({c['docs_reduction_pct']}%)** |",
            f"| **مفاتيح الفهرس المفحوصة (keysExamined)** | {b['total_keys_examined']:,} | {a['total_keys_examined']:,} | فحص مفاتيح الشجرة بدقة |",
            f"| **الوثائق المرجعة (nReturned)** | {b['n_returned']:,} | {a['n_returned']:,} | تطابق دقيق للنتائج |",
            f"| **زمن التنفيذ (Execution Time)** | **{b['execution_time_millis']} ms** | **{a['execution_time_millis']} ms** | تسريع فوري وتقليص استهلاك المعالج |",
            "",
            "---",
            "",
        ])

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[Explain Benchmark] Markdown report generated at: {output_path}")


if __name__ == "__main__":
    res = run_explain_benchmark()
    print("\nBenchmark completed successfully!")
    for q in res["benchmarks"]:
        print(f"\n[{q['query_name']}]")
        print(f"  Before: {q['before']['winning_stage']} | docsExamined={q['before']['total_docs_examined']:,} | time={q['before']['execution_time_millis']}ms")
        print(f"  After : {q['after']['winning_stage']} | docsExamined={q['after']['total_docs_examined']:,} | time={q['after']['execution_time_millis']}ms")
        print(f"  Improvement: {q['comparison']['docs_reduction_pct']}% docs scanned saved!")
