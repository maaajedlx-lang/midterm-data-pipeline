import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality_rules import process_and_classify_record


def test_valid_record_classification():
    raw = {
        "order_id": "ORD-1001",
        "order_date": "2025-01-31",
        "status": "مؤكد",
        "customer_id": "CUST-500",
        "customer_name": "أحمد علي",
        "customer_phone": "+967771234567",
        "customer_email": "ahmed@example.com",
        "city": "صنعاء",
        "district": "السبعين",
        "delivery_type": "سريع",
        "delivery_cost": "500",
        "payment_method": "نقداً",
        "payment_status": "مدفوع",
        "payment_amount": "5500",
        "currency": "YER",
        "total_amount": "5500",
        "items_json": '[{"sku":"ITEM-1","qty":1,"unit_price":5000}]',
    }

    res = process_and_classify_record(raw, run_id="run-test-1")
    assert res["destination"] == "validated"
    assert res["record"]["quality_status"] in ["valid", "corrected"]
    assert res["record"]["id_order"] == "ORD-1001"


def test_corrected_record_audit_trail():
    raw = {
        "order_id": "ORD-1002",
        "order_date": "31-01-2025",  # Dirty date format
        "status": "تم الدفع",       # Synonym status
        "customer_id": "CUST-501",
        "customer_email": "user@@mail..com",  # Repeated symbols
        "currency": "5000 ر.ي",      # Currency symbol text
        "total_amount": "5,000",     # Thousands comma
        "items_json": '[{"sku":"ITEM-2","qty":1,"unit_price":5000}]',
    }

    res = process_and_classify_record(raw, run_id="run-test-2")
    assert res["destination"] == "validated"
    assert res["record"]["quality_status"] == "corrected"
    assert len(res["record"]["corrections"]) > 0

    rule_codes = [c["rule_code"] for c in res["record"]["corrections"]]
    assert "EMAIL_REPEATED_SYMBOLS" in rule_codes
    assert "STATUS_SYNONYM_TRIM" in rule_codes
    assert "DATE_NORMALIZATION" in rule_codes


def test_missing_order_id_quarantine():
    raw = {
        "order_id": "",
        "customer_id": "CUST-502",
        "order_date": "2025-01-31",
        "total_amount": "1000",
    }
    res = process_and_classify_record(raw, run_id="run-test-3")
    assert res["destination"] == "quarantine"
    assert "ID_ORDER_MISSING" in res["record"]["error_codes"]


def test_missing_customer_id_quarantine():
    raw = {
        "order_id": "ORD-502",
        "customer_id": "",
        "order_date": "2025-01-31",
        "total_amount": "1000",
    }
    res = process_and_classify_record(raw, run_id="run-test-cust")
    assert res["destination"] == "quarantine"
    assert "ID_CUSTOMER_MISSING" in res["record"]["error_codes"]


def test_corrupted_json_quarantine():
    raw = {
        "order_id": "ORD-1003",
        "customer_id": "CUST-503",
        "order_date": "2025-01-31",
        "items_json": "INVALID_JSON_{{",
        "total_amount": "1000",
    }
    res = process_and_classify_record(raw, run_id="run-test-4")
    assert res["destination"] == "quarantine"
    assert "JSON_ITEMS_CORRUPTED" in res["record"]["error_codes"]


def test_empty_items_quarantine():
    raw = {
        "order_id": "ORD-EMPTY",
        "customer_id": "CUST-EMPTY",
        "order_date": "2025-01-31",
        "items_json": "[]",
        "total_amount": "1000",
    }
    res = process_and_classify_record(raw, run_id="run-test-empty")
    assert res["destination"] == "quarantine"
    assert "ITEMS_EMPTY" in res["record"]["error_codes"]


def test_impossible_date_quarantine():
    raw = {
        "order_id": "ORD-1004",
        "customer_id": "CUST-504",
        "order_date": "invalid-month-99-99",
        "total_amount": "1000",
    }
    res = process_and_classify_record(raw, run_id="run-test-5")
    assert res["destination"] == "quarantine"
    assert "DATE_IMPOSSIBLE_INVALID" in res["record"]["error_codes"]


def test_price_unknown_quarantine():
    raw = {
        "order_id": "ORD-NOPRICE",
        "customer_id": "CUST-NOPRICE",
        "order_date": "2025-01-31",
        "total_amount": "UNKNOWN_AMOUNT",
        "items_json": "",
    }
    res = process_and_classify_record(raw, run_id="run-test-noprice")
    assert res["destination"] == "quarantine"
    assert "PRICE_UNKNOWN" in res["record"]["error_codes"]


def test_negative_value_quarantine():
    raw = {
        "order_id": "ORD-NEG",
        "customer_id": "CUST-NEG",
        "order_date": "2025-01-31",
        "total_amount": "-500",
    }
    res = process_and_classify_record(raw, run_id="run-test-neg")
    assert res["destination"] == "quarantine"
    assert "VALUE_NEGATIVE_AMBIGUOUS" in res["record"]["error_codes"]


def test_duplicate_order_id_quarantine():
    raw = {
        "order_id": "ORD-DUP",
        "customer_id": "CUST-DUP",
        "order_date": "2025-01-31",
        "total_amount": "5000",
    }
    res = process_and_classify_record(raw, run_id="run-test-dup", is_duplicate=True)
    assert res["destination"] == "quarantine"
    assert "ID_ORDER_DUPLICATE" in res["record"]["error_codes"]


def test_conflicting_multiple_errors_quarantine():
    raw = {
        "order_id": "",  # Missing order ID
        "customer_id": "",  # Missing customer ID
        "order_date": "invalid_date",  # Invalid date
    }
    res = process_and_classify_record(raw, run_id="run-test-multi")
    assert res["destination"] == "quarantine"
    assert "ERRORS_CONFLICTING_MULTIPLE" in res["record"]["error_codes"]
