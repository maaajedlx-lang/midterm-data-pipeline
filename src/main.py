from __future__ import annotations

import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DEFAULT_INPUT_FILE, HUGE_INPUT_FILE, SPARK_MASTER_URL
from src.create_small_sample import create_sample
from src.elt_pipeline import execute_elt_pipeline
from src.metrics import generate_markdown_report


def main():
    parser = argparse.ArgumentParser(
        description="Midterm Data Pipeline - Hybrid ELT Orders Processing (Python Batch / PySpark / MongoDB)"
    )
    parser.add_argument(
        "--file",
        type=str,
        default=str(DEFAULT_INPUT_FILE),
        help="Path to input CSV file (default: data/small_sample.csv)",
    )
    parser.add_argument(
        "--engine",
        type=str,
        choices=["python_batch", "pyspark"],
        default=None,
        help="Override automated File Router engine selection",
    )
    parser.add_argument(
        "--spark-master",
        type=str,
        default=SPARK_MASTER_URL,
        help="Spark Master URL for PySpark (default: local[*] or spark://...)",
    )
    parser.add_argument(
        "--create-sample",
        action="store_true",
        help="Create a small sample from the huge CSV before executing",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=100000,
        help="Number of rows for small sample creation (default: 100,000)",
    )
    parser.add_argument(
        "--reset-db",
        action="store_true",
        help="Drop existing MongoDB collections before running",
    )
    parser.add_argument(
        "--re-run-test",
        action="store_true",
        help="Run pipeline a second time to demonstrate idempotency & zero duplicate insertion",
    )

    args = parser.parse_args()
    input_path = Path(args.file)

    if not input_path.is_absolute():
        input_path = PROJECT_ROOT / input_path

    if args.create_sample:
        sample_output = PROJECT_ROOT / "data" / "small_sample.csv"
        create_sample(HUGE_INPUT_FILE, sample_output, args.sample_rows)
        input_path = sample_output

    print("\n" + "=" * 70)
    print("🚀 STARTING MIDTERM HYBRID DATA PIPELINE")
    print("=" * 70)

    # Initial ELT Execution (Run 1)
    metrics_run1 = execute_elt_pipeline(
        input_file=input_path,
        engine_override=args.engine,
        spark_master=args.spark_master,
        reset_db=args.reset_db,
    )

    # Idempotency Verification Execution (Run 2)
    if args.re_run_test:
        print("\n" + "🔄 " * 20)
        print("RUNNING IDEMPOTENCY RE-RUN TEST ON IDENTICAL DATASET")
        print("🔄 " * 20)
        metrics_run2 = execute_elt_pipeline(
            input_file=input_path,
            engine_override=args.engine,
            spark_master=args.spark_master,
            reset_db=False,  # Preserve MongoDB state to verify zero duplicated records
        )

        ins1 = metrics_run1.get("count_inserted", 0)
        ins2 = metrics_run2.get("count_inserted", 0)
        upd2 = metrics_run2.get("count_updated", 0)
        unch2 = metrics_run2.get("count_unchanged", 0)

        print("\n" + "=" * 70)
        print("IDEMPOTENCY VERIFICATION RESULTS")
        print("=" * 70)
        print(f"Run 1 Inserted : {ins1:,}")
        print(f"Run 2 Inserted : {ins2:,} (Target: 0)")
        print(f"Run 2 Updated  : {upd2:,}")
        print(f"Run 2 Unchanged: {unch2:,}")

        if ins2 == 0:
            print("✅ IDEMPOTENCY PROVEN: Zero new records inserted on re-run!")
        else:
            print(f"❌ IDEMPOTENCY FAILED: {ins2} new records were inserted on re-run!")
        print("=" * 70)

    # Generate Markdown Summary Report
    generate_markdown_report()
    print("\n✨ PIPELINE COMPLETED SUCCESSFULLY.")


if __name__ == "__main__":
    main()