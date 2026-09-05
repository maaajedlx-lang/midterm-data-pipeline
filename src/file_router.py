from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path: Path | str) -> float:
    """Return file size in megabytes (MB)."""
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    if not p.is_file():
        raise ValueError(f"Path is not a file: {file_path}")
    size_bytes = p.stat().st_size
    return size_bytes / (1024 * 1024)


def choose_engine(file_path: Path | str, threshold_mb: float = SMALL_FILE_THRESHOLD_MB) -> Tuple[str, float, str]:
    """
    Evaluates file size and selects the processing engine automatically:
    - If file_size_mb <= threshold_mb (200 MB): 'python_batch'
    - If file_size_mb > threshold_mb: 'pyspark'
    
    Returns (engine_name, file_size_mb, reason_description).
    """
    p = Path(file_path)
    size_mb = get_file_size_mb(p)

    if size_mb <= threshold_mb:
        engine = "python_batch"
        reason = f"File size ({size_mb:.2f} MB) is <= threshold ({threshold_mb:.2f} MB). Selected Python Batch streaming engine."
    else:
        engine = "pyspark"
        reason = f"File size ({size_mb:.2f} MB) exceeds threshold ({threshold_mb:.2f} MB). Selected PySpark parallel distributed engine."

    return engine, size_mb, reason


def print_router_decision(file_path: Path | str, threshold_mb: float = SMALL_FILE_THRESHOLD_MB) -> Dict[str, str | float]:
    """Prints router output cleanly to stdout and returns metadata dict."""
    p = Path(file_path)
    engine, size_mb, reason = choose_engine(p, threshold_mb)

    print("=" * 60)
    print("FILE ROUTER ENGINE SELECTION")
    print("=" * 60)
    print(f"Input file      : {p.name}")
    print(f"Full Path       : {p}")
    print(f"File Size       : {size_mb:.2f} MB")
    print(f"Threshold       : {threshold_mb:.2f} MB")
    print(f"Selected Engine : {engine}")
    print(f"Decision Reason : {reason}")
    print("=" * 60)

    return {
        "file_name": p.name,
        "file_size_mb": round(size_mb, 2),
        "threshold_mb": threshold_mb,
        "engine_used": engine,
        "reason": reason,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="File Router for Hybrid Data Pipeline")
    parser.add_argument("file_path", type=str, nargs="?", default="data/small_sample.csv", help="Path to input CSV file")
    args = parser.parse_args()

    target_file = Path(args.file_path)
    if not target_file.is_absolute():
        target_file = PROJECT_ROOT / target_file

    print_router_decision(target_file)