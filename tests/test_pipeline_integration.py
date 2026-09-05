import sys
import tempfile
import csv
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.file_router import choose_engine, get_file_size_mb
from src.create_small_sample import create_sample
from src.quality_rules import process_and_classify_record


def test_file_router_decision():
    """Test engine decision for small vs large files against 200MB threshold."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        f.write(b"header\n" + b"x," * 1000)
        temp_file = Path(f.name)

    try:
        # For a tiny file <= 200MB, python_batch should be selected
        engine, size_mb, reason = choose_engine(temp_file, threshold_mb=200.0)
        assert engine == "python_batch"
        assert "Python Batch" in reason or "python_batch" in reason

        # If threshold is artificially set to 0.00001 MB, pyspark should be selected
        engine_large, size_mb_large, reason_large = choose_engine(temp_file, threshold_mb=0.00001)
        assert engine_large == "pyspark"
        assert "PySpark" in reason_large or "pyspark" in reason_large
    finally:
        temp_file.unlink(missing_ok=True)


def test_create_sample_preserves_header_and_rows():
    """Test create_sample extracts exact number of rows and preserves header."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as in_f:
        writer = csv.writer(in_f)
        writer.writerow(["order_id", "order_date", "total_amount"])
        for i in range(20):
            writer.writerow([f"ORD-{i}", "2025-01-01", f"{i*100}"])
        in_path = Path(in_f.name)

    out_path = in_path.parent / f"sample_{in_path.name}"

    try:
        create_sample(in_path, out_path, rows=5)
        with out_path.open("r", newline="") as f:
            reader = list(csv.reader(f))
            assert len(reader) == 6  # 1 header + 5 rows
            assert reader[0] == ["order_id", "order_date", "total_amount"]
            assert reader[1][0] == "ORD-0"
            assert reader[5][0] == "ORD-4"
    finally:
        in_path.unlink(missing_ok=True)
        out_path.unlink(missing_ok=True)


def test_consistency_equation_logic():
    """Test that all records are accounted for: Raw = Valid + Corrected + Quarantined."""
    test_records = [
        # Clean record matching target format exactly (Valid)
        {"order_id": "ORD-1", "customer_id": "C-1", "order_date": "2025-01-01T00:00:00", "currency": "YER", "total_amount": "100.0"},
        # Dirty record with non-ISO date that gets normalized (Corrected)
        {"order_id": "ORD-2", "customer_id": "C-2", "order_date": "01-01-2025", "currency": "YER", "total_amount": "200.0"},
        # Uncorrectable missing order_id (Quarantined)
        {"order_id": "", "customer_id": "C-3", "order_date": "2025-01-01T00:00:00", "currency": "YER", "total_amount": "300.0"},
    ]

    valid = 0
    corrected = 0
    quarantine = 0

    for rec in test_records:
        res = process_and_classify_record(rec, run_id="test-run")
        if res["destination"] == "validated":
            if res["record"]["quality_status"] == "valid":
                valid += 1
            else:
                corrected += 1
        else:
            quarantine += 1

    assert len(test_records) == valid + corrected + quarantine
    assert valid == 1
    assert corrected == 1
    assert quarantine == 1
