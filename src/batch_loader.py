from __future__ import annotations

import csv
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
)


def create_run_id() -> str:
    """Create a unique execution ID for this pipeline run."""
    return str(uuid.uuid4())


def load_batch_to_raw(
    collection: Any,
    batch: list[dict[str, Any]],
    run_id: str,
    file_path: Path,
    start_row: int,
    batch_number: int,
) -> int:
    """
    Inserts one batch of uncleaned raw CSV records into MongoDB orders_raw collection.
    Attaches complete audit metadata without modifying raw values.
    """
    if not batch:
        return 0

    documents = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for idx, record in enumerate(batch):
        source_row = start_row + idx
        doc = {
            "id_run": run_id,
            "file_source": file_path.name,
            "number_row_source": source_row,
            "at_ingested": ingested_at,
            "engine_used": "python_batch",
            "record_raw": record,
            "source_file": file_path.name,
            "source_row_number": source_row,
            "ingested_at": ingested_at,
            "raw_record": record,
        }
        documents.append(doc)

    batch_start = time.perf_counter()

    try:
        result = collection.insert_many(documents, ordered=False)
        elapsed = time.perf_counter() - batch_start
        inserted_count = len(result.inserted_ids)
        throughput = inserted_count / elapsed if elapsed > 0 else 0

        print(
            f"[PythonBatch] Batch {batch_number:03d} | "
            f"Records: {inserted_count:,} | "
            f"Time: {elapsed:.3f}s | "
            f"Throughput: {throughput:,.2f} rec/s"
        )
        return inserted_count
    except Exception as exc:
        elapsed = time.perf_counter() - batch_start
        print(f"❌ [PythonBatch] Batch {batch_number} FAILED after {elapsed:.3f}s: {exc}")
        raise


def load_csv_raw_python_batch(
    file_path: Path | str,
    batch_size: int = BATCH_SIZE,
    run_id: str | None = None,
) -> Dict[str, Any]:
    """
    Streams CSV line-by-line and bulk-inserts uncleaned data into MongoDB orders_raw collection.
    Memory efficient: does NOT use list(reader) or load full file into RAM.
    """
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Input CSV file not found: {p}")

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    if not run_id:
        run_id = create_run_id()

    print("=" * 60)
    print("PYTHON BATCH STREAMING LOADER (RAW LOAD)")
    print("=" * 60)
    print(f"Source File : {p.name}")
    print(f"Batch Size  : {batch_size:,}")
    print(f"Run ID      : {run_id}")
    print(f"Collection  : {RAW_COLLECTION}")
    print("=" * 60)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    try:
        client.admin.command("ping")
        db = client[MONGO_DATABASE]
        raw_coll = db[RAW_COLLECTION]

        total_rows = 0
        batch_number = 0
        source_row_start = 2  # Row 1 is header

        start_time = time.perf_counter()

        with p.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            batch: list[dict[str, Any]] = []

            for row in reader:
                batch.append(dict(row))
                if len(batch) >= batch_size:
                    batch_number += 1
                    inserted = load_batch_to_raw(
                        collection=raw_coll,
                        batch=batch,
                        run_id=run_id,
                        file_path=p,
                        start_row=source_row_start,
                        batch_number=batch_number,
                    )
                    total_rows += inserted
                    source_row_start += len(batch)
                    batch.clear()

            # Insert remaining records
            if batch:
                batch_number += 1
                inserted = load_batch_to_raw(
                    collection=raw_coll,
                    batch=batch,
                    run_id=run_id,
                    file_path=p,
                    start_row=source_row_start,
                    batch_number=batch_number,
                )
                total_rows += inserted
                batch.clear()

        elapsed_seconds = time.perf_counter() - start_time
        throughput = total_rows / elapsed_seconds if elapsed_seconds > 0 else 0

        print("=" * 60)
        print("PYTHON BATCH RAW LOAD COMPLETED")
        print("=" * 60)
        print(f"Run ID          : {run_id}")
        print(f"Total Rows Raw  : {total_rows:,}")
        print(f"Total Batches   : {batch_number:,}")
        print(f"Elapsed Time    : {elapsed_seconds:.3f} seconds")
        print(f"Throughput      : {throughput:,.2f} records/second")
        print("=" * 60)

        return {
            "run_id": run_id,
            "engine_used": "python_batch",
            "file_name": p.name,
            "rows_read": total_rows,
            "raw_loaded": total_rows,
            "loaded_raw": total_rows,
            "batches": batch_number,
            "batch_size": batch_size,
            "elapsed_seconds": elapsed_seconds,
            "throughput": throughput,
        }

    finally:
        client.close()


if __name__ == "__main__":
    sample_file = PROJECT_ROOT / "data" / "small_sample.csv"
    load_csv_raw_python_batch(sample_file)