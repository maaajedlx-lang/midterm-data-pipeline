from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
    SPARK_APP_NAME,
    SPARK_MASTER_URL,
)

# Explicit schema (No inferSchema) preserving all uncleaned raw CSV strings
RAW_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("delivery_type", StringType(), True),
    StructField("delivery_cost", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("items_json", StringType(), True),
])


def create_spark_session(master_url: str = SPARK_MASTER_URL, app_name: str = SPARK_APP_NAME) -> SparkSession:
    """Creates PySpark SparkSession for Standalone Cluster or Local mode."""
    src_dir = str(PROJECT_ROOT / "src")
    root_dir = str(PROJECT_ROOT)
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    python_path = f"{root_dir};{src_dir};{os.environ.get('PYTHONPATH', '')}"
    os.environ["PYTHONPATH"] = python_path

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.local.dir", str(PROJECT_ROOT / ".spark_temp"))
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_partition_to_mongo(partition_rows, run_id: str, source_filename: str):
    """Distributed partition writer into MongoDB orders_raw collection."""
    from pymongo import MongoClient

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DATABASE", "midterm_data_pipeline")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        db = client[mongo_db]
        raw_coll = db["orders_raw"]
        batch = []
        ingested_at = datetime.now(timezone.utc).isoformat()

        for idx, row in enumerate(partition_rows):
            doc = {
                "id_run": run_id,
                "file_source": source_filename,
                "number_row_source": idx + 2,
                "at_ingested": ingested_at,
                "engine_used": "pyspark",
                "record_raw": row.asDict(),
                "source_file": source_filename,
                "source_row_number": idx + 2,
                "ingested_at": ingested_at,
                "raw_record": row.asDict(),
            }
            batch.append(doc)
            if len(batch) >= 5000:
                raw_coll.insert_many(batch, ordered=False)
                batch.clear()

        if batch:
            raw_coll.insert_many(batch, ordered=False)
            batch.clear()
    finally:
        client.close()


def load_csv_raw_pyspark(
    file_path: Path | str,
    master_url: str = SPARK_MASTER_URL,
    run_id: str | None = None,
) -> Dict[str, Any]:
    """Reads large CSV in parallel via PySpark and writes raw records to orders_raw."""
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Input file not found: {p}")

    if not run_id:
        run_id = str(uuid.uuid4())

    print("=" * 60)
    print("PYSPARK PARALLEL LOADER (RAW LOAD)")
    print("=" * 60)
    print(f"Source File : {p.name}")
    print(f"Master URL  : {master_url}")
    print(f"Run ID      : {run_id}")
    print(f"Collection  : {RAW_COLLECTION}")
    print("=" * 60)

    spark = create_spark_session(master_url)
    start_time = time.perf_counter()

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("quote", '"')
            .option("escape", '"')
            .schema(RAW_SCHEMA)
            .csv(str(p))
        )

        input_partitions = df.rdd.getNumPartitions()
        total_rows = df.count()

        print(f"[PySpark] Input Partitions : {input_partitions}")
        print(f"[PySpark] Total Rows Read  : {total_rows:,}")

        # Parallel insert per partition into orders_raw
        fn_source = p.name
        r_id = run_id
        df.rdd.foreachPartition(lambda p_rows: write_partition_to_mongo(p_rows, r_id, fn_source))

        elapsed_seconds = time.perf_counter() - start_time
        throughput = total_rows / elapsed_seconds if elapsed_seconds > 0 else 0

        print("=" * 60)
        print("PYSPARK RAW LOAD COMPLETED")
        print("=" * 60)
        print(f"Run ID           : {run_id}")
        print(f"Total Rows Raw   : {total_rows:,}")
        print(f"Input Partitions : {input_partitions}")
        print(f"Elapsed Time     : {elapsed_seconds:.3f} seconds")
        print(f"Throughput       : {throughput:,.2f} records/second")
        print("=" * 60)

        return {
            "run_id": run_id,
            "engine_used": "pyspark",
            "master_url": master_url,
            "file_name": p.name,
            "rows_read": total_rows,
            "raw_loaded": total_rows,
            "loaded_raw": total_rows,
            "partitions": input_partitions,
            "elapsed_seconds": elapsed_seconds,
            "throughput": throughput,
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    sample_file = PROJECT_ROOT / "data" / "small_sample.csv"
    load_csv_raw_pyspark(sample_file, master_url="local[*]")