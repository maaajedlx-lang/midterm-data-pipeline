import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality_rules import (
    clean_text,
    normalize_date,
    normalize_email,
    normalize_phone,
    parse_numeric,
    process_and_classify_record,
)


def test_rule_1_arabic_digits():
    """Rule 1: Convert Eastern Arabic numerals (٠١٢٣٤٥٦٧٨٩) to Western digits."""
    val, s = parse_numeric("٠٠٠٥")
    assert val == 5.0

    val, s = parse_numeric("٣٢١")
    assert val == 321.0

    val, s = parse_numeric("١٢٥٠٠")
    assert val == 12500.0


def test_rule_2_currency_standardization_and_removal():
    """Rule 2: Currency standardization to YER and stripping textual currency markers."""
    val, s = parse_numeric("5000 ر.ي")
    assert val == 5000.0

    val, s = parse_numeric("ريال يمني 125000")
    assert val == 125000.0

    val, s = parse_numeric("2500 YER")
    assert val == 2500.0


def test_rule_3_thousands_separator():
    """Rule 3: Remove thousands separator commas and spaces."""
    val, s = parse_numeric("125,000.00")
    assert val == 125000.0

    val, s = parse_numeric("1,500,000")
    assert val == 1500000.0


def test_rule_4_price_in_words():
    """Rule 4: Convert known numeric words in Arabic to numbers."""
    val, s = parse_numeric("ألفان")
    assert val == 2000.0

    val, s = parse_numeric("خمسة آلاف")
    assert val == 5000.0

    val, s = parse_numeric("ثلاثة آلاف")
    assert val == 3000.0

    val, s = parse_numeric("عشرة آلاف")
    assert val == 10000.0

    val, s = parse_numeric("ألف")
    assert val == 1000.0


def test_rule_5_phone_normalization():
    """Rule 5: Phone number standardization (strip spaces, symbols, arabic digits)."""
    phone = normalize_phone("+967 77 123 4567")
    assert phone == "+967771234567"

    phone = normalize_phone("077-123-4567")
    assert phone == "0771234567"

    phone = normalize_phone("٠٧٧١٢٣٤٥٦٧")
    assert phone == "0771234567"


def test_rule_6_email_cleaning():
    """Rule 6: Email duplicate symbol cleaning."""
    email = normalize_email("user@@mail..com")
    assert email == "user@mail.com"

    email = normalize_email("john.doe@domain...com")
    assert email == "john.doe@domain.com"


def test_rule_7_date_normalization():
    """Rule 7: Date normalization into ISO standard format (YYYY-MM-DDTHH:MM:SS)."""
    iso, valid = normalize_date("31-01-2025")
    assert valid is True
    assert iso == "2025-01-31T00:00:00"

    iso, valid = normalize_date("2025/01/31 14:30:00")
    assert valid is True
    assert iso == "2025-01-31T14:30:00"

    iso, valid = normalize_date("٣١-٠١-٢٠٢٥")
    assert valid is True
    assert iso == "2025-01-31T00:00:00"

    iso, valid = normalize_date("invalid-date-string")
    assert valid is False


def test_rule_8_status_synonyms_and_trim():
    """Rule 8: Trim spaces and map status synonyms to standard dictionary."""
    raw = {
        "order_id": "ORD-R8",
        "customer_id": "CUST-R8",
        "order_date": "2025-01-15",
        "status": " تم الدفع ",
        "total_amount": "1000",
    }
    res = process_and_classify_record(raw, run_id="test-run")
    assert res["destination"] == "validated"
    assert res["record"]["payment_status"] == "مدفوع"


def test_rule_9_recalculated_total_amount():
    """Rule 9: Recalculate total amount from items and delivery cost when missing/cleanable."""
    raw = {
        "order_id": "ORD-R9",
        "customer_id": "CUST-R9",
        "order_date": "2025-01-15",
        "delivery_cost": "500",
        "total_amount": "",  # missing total
        "items_json": '[{"sku":"ITEM-1","qty":2,"unit_price":2000}]',
    }
    res = process_and_classify_record(raw, run_id="test-run")
    assert res["destination"] == "validated"
    assert res["record"]["total_amount"] == 4500.0  # (2 * 2000) + 500
