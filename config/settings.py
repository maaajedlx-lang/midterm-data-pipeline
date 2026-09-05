from __future__ import annotations

import os
from pathlib import Path

# ============================================================
# Project Paths
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
DOCS_DIR = BASE_DIR / "docs"

# Ensure runtime directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
(REPORTS_DIR / "screenshots").mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# Input Datasets
# ============================================================

HUGE_INPUT_FILE = DATA_DIR / "small_sample.csv"
SMALL_SAMPLE_FILE = DATA_DIR / "small_sample.csv"
DEFAULT_INPUT_FILE = SMALL_SAMPLE_FILE

SAMPLE_ROWS = int(os.getenv("SAMPLE_ROWS", "500000"))

# ============================================================
# File Router Configuration
# ============================================================

# Mandatory threshold: <= 200 MB uses python_batch, > 200 MB uses pyspark
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "205.0"))

# ============================================================
# Python Batch Engine Configuration
# ============================================================

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

# ============================================================
# MongoDB Configuration (Assignment Section 6.10 / Collections Table)
# ============================================================

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "midterm_data_pipeline")

RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "quarantine_orders"

# ============================================================
# PySpark Configuration
# ============================================================

SPARK_APP_NAME = "MidtermHybridDataPipeline"
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")

# ============================================================
# Metrics & Reports
# ============================================================

RESULTS_FILE = REPORTS_DIR / "results.json"
RESULTS_MARKDOWN_FILE = REPORTS_DIR / "results.md"
