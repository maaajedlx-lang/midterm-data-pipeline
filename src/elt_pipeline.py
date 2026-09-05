from __future__ import annotations

import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient, UpdateOne

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    RESULTS_FILE,
    SMALL_FILE_THRESHOLD_MB,
    SPARK_MASTER_URL,
    VALIDATED_COLLECTION,
)
from src.batch_loader import load_csv_raw_python_batch
from src.file_router import choose_engine
from src.mongo_setup import setup_mongodb
from src.quality_rules import process_and_classify_record
from src.spark_loader import load_csv_raw_pyspark


def execute_elt_pipeline(
    input_file: Path | str,
    engine_override: str | None = None,
    spark_master: str = SPARK_MASTER_URL,
    reset_db: bool = False,
    run_id: str | None = None,
    batch_size: int = BATCH_SIZE,
) -> Dict[str, Any]:
    """
    Executes the full hybrid ELT pipeline:
    1. MongoDB Setup & Collection Readiness
    2. Discovery & Automated File Routing
    3. Raw Ingestion Layer (orders_raw)
    4. Quality Transformation & Error Isolation
    5. Idempotent Upsert (orders_validated) & Quarantine Storage
    6. Consistency Equation Verification
    7. Metrics Collection & JSON/MD Reporting
    """
    p = Path(input_file)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")

    if not run_id:
        run_id = str(uuid.uuid4())

    pipeline_start = time.perf_counter()

    # Step 1: DB Setup
    setup_mongodb(drop_existing=reset_db)

    # Step 2: Automated File Routing
    engine, size_mb, reason = choose_engine(p, SMALL_FILE_THRESHOLD_MB)
    if engine_override:
        engine = engine_override
        reason = f"Engine overridden by user flag to '{engine_override}'."

    print("\n" + "=" * 70)
    print("HYBRID ELT PIPELINE EXECUTION")
    print("=" * 70)
    print(f"Run ID          : {run_id}")
    print(f"Input File      : {p.name} ({size_mb:.2f} MB)")
    print(f"Selected Engine : {engine}")
    print(f"Routing Reason  : {reason}")
    print("=" * 70)

    # Step 3: Raw Load Layer
    partitions = 1
    if engine == "python_batch":
        raw_metrics = load_csv_raw_python_batch(p, batch_size=batch_size, run_id=run_id)
        partitions = 1
    else:
        raw_metrics = load_csv_raw_pyspark(p, master_url=spark_master, run_id=run_id)
        partitions = raw_metrics.get("partitions", 1)

    raw_loaded_count = raw_metrics.get("raw_loaded") or raw_metrics.get("loaded_raw", 0)

    # Step 4 & 5: Transform, Quality & Idempotent Upsert
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    valid_count = 0
    corrected_count = 0
    quarantine_count = 0

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    error_case_counts: Dict[str, int] = {}

    try:
        db = client[MONGO_DATABASE]
        raw_coll = db[RAW_COLLECTION]
        val_coll = db[VALIDATED_COLLECTION]
        quar_coll = db[QUARANTINE_COLLECTION]

        print("\n[ELT] Fetching raw records from orders_raw for cleaning and validation...")
        raw_cursor = raw_coll.find({"id_run": run_id}, {"raw_record": 1, "record_raw": 1, "source_row_number": 1, "number_row_source": 1})

        upsert_operations: List[UpdateOne] = []
        quarantine_docs: List[Dict[str, Any]] = []

        # Map existing hashes in MongoDB for idempotency verification
        existing_hashes = {
            doc["id_order"]: doc.get("record_hash")
            for doc in val_coll.find({}, {"id_order": 1, "record_hash": 1})
        }

        seen_order_ids: set[str] = set()

        for doc in raw_cursor:
            raw_record = doc.get("raw_record") or doc.get("record_raw") or {}
            ord_id = (raw_record.get("order_id") or raw_record.get("id_order") or "").strip()

            is_dup = False
            if ord_id:
                if ord_id in seen_order_ids:
                    is_dup = True
                else:
                    seen_order_ids.add(ord_id)

            result = process_and_classify_record(raw_record, run_id, is_duplicate=is_dup)

            dest = result["destination"]
            record = result["record"]

            if dest == "validated":
                status = record["quality_status"]
                if status == "valid":
                    valid_count += 1
                else:
                    corrected_count += 1

                id_order = record["id_order"]
                new_hash = record.get("record_hash")
                old_hash = existing_hashes.get(id_order)

                if old_hash is None:
                    inserted_count += 1
                elif old_hash != new_hash:
                    updated_count += 1
                else:
                    unchanged_count += 1

                existing_hashes[id_order] = new_hash
                record["updated_at"] = datetime.now(timezone.utc).isoformat()

                # Idempotent Upsert query on unique business key (id_order)
                op = UpdateOne(
                    {"id_order": id_order},
                    {"$set": record},
                    upsert=True,
                )
                upsert_operations.append(op)

                if len(upsert_operations) >= 5000:
                    val_coll.bulk_write(upsert_operations, ordered=False)
                    upsert_operations.clear()

            else:  # quarantine
                quarantine_count += 1
                for code in record.get("error_codes", []):
                    error_case_counts[code] = error_case_counts.get(code, 0) + 1

                quarantine_docs.append(record)
                if len(quarantine_docs) >= 5000:
                    quar_coll.insert_many(quarantine_docs, ordered=False)
                    quarantine_docs.clear()

        # Flush remaining bulk writes
        if upsert_operations:
            val_coll.bulk_write(upsert_operations, ordered=False)
            upsert_operations.clear()

        if quarantine_docs:
            quar_coll.insert_many(quarantine_docs, ordered=False)
            quarantine_docs.clear()

        total_elapsed = time.perf_counter() - pipeline_start
        throughput = raw_loaded_count / total_elapsed if total_elapsed > 0 else 0

        # Step 6: Consistency Equation Verification
        total_classified = valid_count + corrected_count + quarantine_count
        consistency_passed = (raw_loaded_count == total_classified)
        total_validated = valid_count + corrected_count

        print("\n" + "=" * 70)
        print("ELT PIPELINE EXECUTION SUMMARY & CONSISTENCY VERIFICATION")
        print("=" * 70)
        print(f"Run ID                       : {run_id}")
        print(f"File Name                    : {p.name} ({size_mb:.2f} MB)")
        print(f"Engine Used                  : {engine}")
        print("-" * 70)
        print("1. RAW LAYER INGESTION (orders_raw):")
        print(f"   - Total Rows Read (المقروءة)        : {raw_loaded_count:,}")
        print(f"   - Raw Loaded to MongoDB (orders_raw): {raw_loaded_count:,}")
        print("-" * 70)
        print("2. QUALITY & CLASSIFICATION BREAKDOWN:")
        print(f"   - Valid Records (سليمة)             : {valid_count:,}")
        print(f"   - Corrected Records (مصححة)          : {corrected_count:,}")
        print(f"   - Quarantined Records (معزولة)       : {quarantine_count:,}")
        print("   " + "-" * 65)
        print(f"   [Consistency Rule Check / معادلة اتساق البيانات]:")
        print(f"   {valid_count:,} (Valid) + {corrected_count:,} (Corrected) + {quarantine_count:,} (Quarantine)")
        print(f"   = {total_classified:,} / {raw_loaded_count:,} (Raw Loaded)")
        print(f"   Equation Status: {'✅ PASSED (معادلة اتساق البيانات صحيحة 100%)' if consistency_passed else '❌ FAILED'}")
        if not consistency_passed:
            print(f"   ⚠️ WARNING: Inconsistency detected! Discrepancy: {raw_loaded_count - total_classified} records.")
        print("-" * 70)
        print("3. VALIDATED UPSERT BREAKDOWN (orders_validated):")
        print(f"   - Total Validated Processed         : {total_validated:,} ({valid_count:,} Valid + {corrected_count:,} Corrected)")
        print(f"   - Unique Business Key Inserted (جديدة): {inserted_count:,}")
        print(f"   - Duplicate Key Updated (محدثة)      : {updated_count:,}")
        print(f"   - Identical Unchanged (مطابقة)       : {unchanged_count:,}")
        print("   " + "-" * 65)
        print(f"   [Upsert Proof]: {inserted_count:,} (Inserted) + {updated_count:,} (Updated) + {unchanged_count:,} (Unchanged) = {total_validated:,}")
        print("-" * 70)
        print("4. PERFORMANCE & TIMING:")
        print(f"   - Total Elapsed Time                 : {total_elapsed:.3f} seconds")
        print(f"   - Pipeline Throughput                : {throughput:,.2f} records/second")
        print("=" * 70)

        summary_metrics = {
            "id_run": run_id,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "file_name": p.name,
            "file_size_mb": round(size_mb, 2),
            "used_engine": engine,
            "engine_used": engine,
            "read_rows": raw_loaded_count,
            "rows_read": raw_loaded_count,
            "loaded_raw": raw_loaded_count,
            "raw_loaded": raw_loaded_count,
            "count_valid": valid_count,
            "valid_count": valid_count,
            "count_corrected": corrected_count,
            "corrected_count": corrected_count,
            "count_quarantine": quarantine_count,
            "quarantine_count": quarantine_count,
            "count_inserted": inserted_count,
            "inserted_count": inserted_count,
            "count_updated": updated_count,
            "updated_count": updated_count,
            "count_unchanged": unchanged_count,
            "unchanged_count": unchanged_count,
            "seconds_elapsed": round(total_elapsed, 3),
            "elapsed_seconds": round(total_elapsed, 3),
            "throughput": round(throughput, 2),
            "partitions": partitions,
            "size_batch": batch_size if engine == "python_batch" else None,
            "counts_case_error": error_case_counts,
            "error_case_counts": error_case_counts,
            "consistency_check_passed": consistency_passed,
            "equation_passed": consistency_passed,
        }

        # Save metrics to reports/results.json
        save_metrics(summary_metrics)

        return summary_metrics

    finally:
        client.close()


def save_metrics(metrics: Dict[str, Any]) -> None:
    """Appends metrics to reports/results.json."""
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    all_metrics = []

    if RESULTS_FILE.exists():
        try:
            with RESULTS_FILE.open("r", encoding="utf-8") as f:
                all_metrics = json.load(f)
                if not isinstance(all_metrics, list):
                    all_metrics = [all_metrics]
        except Exception:
            all_metrics = []

    all_metrics.append(metrics)

    with RESULTS_FILE.open("w", encoding="utf-8") as f:
        json.dump(all_metrics, f, indent=2, ensure_ascii=False)

    print(f"Saved run metrics to {RESULTS_FILE}")


if __name__ == "__main__":
    sample_file = PROJECT_ROOT / "data" / "small_sample.csv"
    execute_elt_pipeline(sample_file)