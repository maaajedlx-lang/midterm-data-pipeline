from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

# Translation tables
ARABIC_DIGITS_TRANS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
ARABIC_SEPARATORS_TRANS = str.maketrans("٫٬", ".,")

STATUS_SYNONYMS = {
    "تم الدفع": "مدفوع",
    "مدفوع": "مدفوع",
    "PAID": "مدفوع",
    "paid": "مدفوع",
    "تم التأكيد": "مؤكد",
    "مؤكد": "مؤكد",
    "CONFIRMED": "مؤكد",
    "confirmed": "مؤكد",
    "ملغى": "ملغي",
    "ملغي": "ملغي",
    "CANCELLED": "ملغي",
    "cancelled": "ملغي",
    "قيد الانتظار": "معلق",
    "معلق": "معلق",
    "PENDING": "معلق",
    "pending": "معلق",
    "قيد الشحن": "قيد الشحن",
    "SHIPPED": "قيد الشحن",
    "shipped": "قيد الشحن",
    "مرتجع": "مرتجع",
    "RETURNED": "مرتجع",
    "returned": "مرتجع",
}

WORD_TO_NUM = {
    "صفر": 0.0,
    "واحد": 1.0,
    "اثنان": 2.0,
    "اثنين": 2.0,
    "ثلاثة": 3.0,
    "اربعة": 4.0,
    "أربعة": 4.0,
    "خمسة": 5.0,
    "ستة": 6.0,
    "سبعة": 7.0,
    "ثمانية": 8.0,
    "تسعة": 9.0,
    "عشرة": 10.0,
    "ألف": 1000.0,
    "الف": 1000.0,
    "ألفان": 2000.0,
    "الفان": 2000.0,
    "ألفين": 2000.0,
    "الفين": 2000.0,
    "ثلاثة آلاف": 3000.0,
    "ثلاثة الاف": 3000.0,
    "خمسة آلاف": 5000.0,
    "خمسه آلاف": 5000.0,
    "خمسة الاف": 5000.0,
    "خمسه الاف": 5000.0,
    "عشرة آلاف": 10000.0,
    "عشرة الاف": 10000.0,
}


def clean_text(val: Any) -> str:
    """Trim whitespace or convert None to empty string."""
    if val is None:
        return ""
    return str(val).strip()


def parse_numeric(val: Any) -> Tuple[float | None, str]:
    """
    Applies:
    - Rule 1: Arabic digits (ARABIC_DIGITS)
    - Rule 2: Currency removal (CURRENCY_STANDARDIZATION)
    - Rule 3: Thousands separators removal (THOUSANDS_SEPARATOR)
    - Rule 4: Price in words conversion (WORD_TO_NUM)
    Returns (parsed_float, cleaned_string).
    """
    s = clean_text(val)
    if not s:
        return None, ""

    # Check word map directly
    if s in WORD_TO_NUM:
        num = WORD_TO_NUM[s]
        return num, str(num)

    # Rule 1: Convert Arabic numerals & decimal separators
    s_conv = s.translate(ARABIC_DIGITS_TRANS).translate(ARABIC_SEPARATORS_TRANS)

    # Rule 2: Remove currency symbols & words
    s_conv = re.sub(r"(?i)\s*(ريال\s*يمني|ريال|ر\.ي|YER|Y\.R\.?|USD|\$)\s*", "", s_conv).strip()

    # Check words again after symbol stripping
    if s_conv in WORD_TO_NUM:
        num = WORD_TO_NUM[s_conv]
        return num, str(num)

    # Rule 3: Remove thousands separator commas and spaces
    s_conv = s_conv.replace(",", "").replace(" ", "")

    if not s_conv:
        return None, ""

    try:
        num = float(s_conv)
        return num, s_conv
    except ValueError:
        return None, s


def normalize_phone(phone_str: str) -> str:
    """
    Rule 5: Phone number standardization (PHONE_CLEAN_FORMAT).
    Converts Arabic digits, strips spaces/symbols, normalizes Yemeni prefixes.
    """
    s = clean_text(phone_str).translate(ARABIC_DIGITS_TRANS)
    s = re.sub(r"[\s\-\(\)\.]+", "", s)
    return s


def normalize_email(email_str: str) -> str:
    """
    Rule 6: Email repeated symbol correction (EMAIL_REPEATED_SYMBOLS).
    e.g. user@@mail..com -> user@mail.com
    """
    s = clean_text(email_str)
    s = re.sub(r"@+", "@", s)
    s = re.sub(r"\.{2,}", ".", s)
    return s


def normalize_date(date_str: str) -> Tuple[str | None, bool]:
    """
    Rule 7: Date normalization into standard ISO format (DATE_NORMALIZATION).
    Target: YYYY-MM-DDTHH:MM:SS
    Returns (iso_date_str, is_valid).
    """
    s = clean_text(date_str).translate(ARABIC_DIGITS_TRANS)
    if not s:
        return None, False

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d/%m/%Y",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year < 2000 or dt.year > 2035:
                return None, False
            return dt.strftime("%Y-%m-%dT%H:%M:%S"), True
        except ValueError:
            continue

    return None, False


def compute_record_hash(record: Dict[str, Any]) -> str:
    """Generates deterministic SHA-256 hash of record content for idempotency comparison."""
    serialized = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def process_and_classify_record(
    raw_dict: Dict[str, Any],
    run_id: str,
    is_duplicate: bool = False,
) -> Dict[str, Any]:
    """
    Applies all 9 automated quality rules, records corrections audit trail,
    and classifies the record into Valid/Corrected (destination: 'validated')
    or Quarantined (destination: 'quarantine') with explicit error codes.
    """
    corrections: List[Dict[str, Any]] = []
    error_codes: List[str] = []
    error_details: List[str] = []

    # 1. Order ID (Stable Business Key)
    order_id = clean_text(raw_dict.get("order_id") or raw_dict.get("id_order"))
    if not order_id:
        error_codes.append("ID_ORDER_MISSING")
        error_details.append("order_id is empty or missing")
    elif is_duplicate:
        error_codes.append("ID_ORDER_DUPLICATE")
        error_details.append(f"Duplicate order_id '{order_id}' requires review")

    # 2. Customer ID
    customer_id = clean_text(raw_dict.get("customer_id"))
    if not customer_id:
        error_codes.append("ID_CUSTOMER_MISSING")
        error_details.append("customer_id is empty or missing")

    # 3. Order Date (Rule 7: DATE_NORMALIZATION)
    raw_date = clean_text(raw_dict.get("order_date"))
    iso_date, date_valid = normalize_date(raw_date)
    if not date_valid:
        error_codes.append("DATE_IMPOSSIBLE_INVALID")
        error_details.append(f"Invalid date string: '{raw_date}'")
    elif raw_date != iso_date:
        corrections.append({
            "field": "order_date",
            "original_value": raw_date,
            "corrected_value": iso_date,
            "rule_code": "DATE_NORMALIZATION",
        })

    # 4. Status (Rule 8: STATUS_SYNONYM_TRIM)
    raw_status = clean_text(raw_dict.get("status"))
    norm_status = STATUS_SYNONYMS.get(raw_status, raw_status)
    if raw_status != norm_status:
        corrections.append({
            "field": "status",
            "original_value": raw_status,
            "corrected_value": norm_status,
            "rule_code": "STATUS_SYNONYM_TRIM",
        })

    # 5. Customer Phone (Rule 1 & 5: PHONE_CLEAN_FORMAT)
    raw_phone = clean_text(raw_dict.get("customer_phone"))
    norm_phone = normalize_phone(raw_phone)
    if raw_phone != norm_phone:
        corrections.append({
            "field": "customer_phone",
            "original_value": raw_phone,
            "corrected_value": norm_phone,
            "rule_code": "PHONE_CLEAN_FORMAT",
        })

    # 6. Customer Email (Rule 6: EMAIL_REPEATED_SYMBOLS)
    raw_email = clean_text(raw_dict.get("customer_email"))
    norm_email = normalize_email(raw_email)
    if raw_email != norm_email:
        corrections.append({
            "field": "customer_email",
            "original_value": raw_email,
            "corrected_value": norm_email,
            "rule_code": "EMAIL_REPEATED_SYMBOLS",
        })

    # 7. Currency (Rule 2: CURRENCY_STANDARDIZATION)
    raw_currency = clean_text(raw_dict.get("currency"))
    norm_currency = "YER"
    if raw_currency and raw_currency.upper() not in ["YER", "RIYAL", "ريال", "ر.ي"]:
        norm_currency = raw_currency.upper()
    if raw_currency != norm_currency:
        corrections.append({
            "field": "currency",
            "original_value": raw_currency,
            "corrected_value": norm_currency,
            "rule_code": "CURRENCY_STANDARDIZATION",
        })

    # 8. Numeric amounts (Rules 1, 2, 3, 4: THOUSANDS_SEPARATOR, ARABIC_DIGITS, WORD_TO_NUM)
    raw_deliv = raw_dict.get("delivery_cost")
    deliv_val, _ = parse_numeric(raw_deliv)
    if raw_deliv is not None and str(raw_deliv).strip() != (str(deliv_val) if deliv_val is not None else ""):
        rule_name = "WORD_TO_NUM" if str(raw_deliv).strip() in WORD_TO_NUM else "THOUSANDS_SEPARATOR"
        corrections.append({
            "field": "delivery_cost",
            "original_value": raw_deliv,
            "corrected_value": deliv_val,
            "rule_code": rule_name,
        })

    raw_pay_amt = raw_dict.get("payment_amount")
    pay_amt_val, _ = parse_numeric(raw_pay_amt)
    if raw_pay_amt is not None and str(raw_pay_amt).strip() != (str(pay_amt_val) if pay_amt_val is not None else ""):
        corrections.append({
            "field": "payment_amount",
            "original_value": raw_pay_amt,
            "corrected_value": pay_amt_val,
            "rule_code": "THOUSANDS_SEPARATOR",
        })

    raw_tot_amt = raw_dict.get("total_amount")
    tot_amt_val, _ = parse_numeric(raw_tot_amt)

    # Check for negative values
    if (deliv_val is not None and deliv_val < 0) or \
       (pay_amt_val is not None and pay_amt_val < 0) or \
       (tot_amt_val is not None and tot_amt_val < 0):
        error_codes.append("VALUE_NEGATIVE_AMBIGUOUS")
        error_details.append("Negative monetary value detected")

    # 9. Items JSON & Total Audit (Rule 9: RECALCULATED_TOTAL_AMOUNT)
    items_raw = raw_dict.get("items_json")
    parsed_items = None
    if items_raw:
        try:
            if isinstance(items_raw, str):
                s_items = items_raw.strip()
                if s_items.startswith('"') and s_items.endswith('"') and '""' in s_items:
                    s_items = s_items[1:-1]
                s_items = s_items.replace('""', '"')
                parsed_items = json.loads(s_items)
            else:
                parsed_items = items_raw

            if not isinstance(parsed_items, list) or len(parsed_items) == 0:
                error_codes.append("ITEMS_EMPTY")
                error_details.append("Items list is empty")
        except Exception as exc:
            error_codes.append("JSON_ITEMS_CORRUPTED")
            error_details.append(f"JSON parse error: {exc}")

    # Recalculate total if components are available
    items_sum = 0.0
    if parsed_items and isinstance(parsed_items, list):
        for item in parsed_items:
            qty_raw = item.get("qty", 1)
            u_price_raw = item.get("unit_price", 0.0)
            qty_val, _ = parse_numeric(qty_raw)
            u_price_val, _ = parse_numeric(u_price_raw)

            q_num = qty_val if qty_val is not None else 0.0
            p_num = u_price_val if u_price_val is not None else 0.0

            if q_num < 0 or p_num < 0:
                error_codes.append("VALUE_NEGATIVE_AMBIGUOUS")
                error_details.append("Negative item quantity or price")
            items_sum += q_num * p_num

    calculated_total = items_sum + (deliv_val or 0.0)
    if tot_amt_val is None and items_sum > 0:
        tot_amt_val = calculated_total
        corrections.append({
            "field": "total_amount",
            "original_value": raw_tot_amt,
            "corrected_value": tot_amt_val,
            "rule_code": "RECALCULATED_TOTAL_AMOUNT",
        })
    elif tot_amt_val is None:
        error_codes.append("PRICE_UNKNOWN")
        error_details.append("Total amount cannot be determined")

    # Final Classification Decision
    if error_codes:
        if len(error_codes) > 1:
            error_codes.append("ERRORS_CONFLICTING_MULTIPLE")

        unique_codes = sorted(list(set(error_codes)))
        return {
            "destination": "quarantine",
            "record": {
                "id_order": order_id if order_id else None,
                "id_run": run_id,
                "error_codes": unique_codes,
                "codes_error": unique_codes,
                "error_details": error_details,
                "details_error": error_details,
                "raw_record": raw_dict,
                "record_raw": raw_dict,
                "created_at": datetime.utcnow().isoformat(),
            },
        }

    # Record is valid or corrected
    quality_status = "corrected" if corrections else "valid"

    validated_doc = {
        "id_order": order_id,
        "id_run": run_id,
        "order_date": iso_date,
        "customer_id": customer_id,
        "customer_name": clean_text(raw_dict.get("customer_name")),
        "customer_phone": norm_phone,
        "customer_email": norm_email,
        "city": clean_text(raw_dict.get("city")),
        "district": clean_text(raw_dict.get("district")),
        "delivery_type": clean_text(raw_dict.get("delivery_type")),
        "delivery_cost": deliv_val,
        "payment_method": clean_text(raw_dict.get("payment_method")),
        "payment_status": norm_status,
        "payment_amount": pay_amt_val,
        "currency": norm_currency,
        "total_amount": tot_amt_val,
        "items": parsed_items,
        "quality_status": quality_status,
        "corrections": corrections,
    }

    validated_doc["record_hash"] = compute_record_hash(validated_doc)

    return {
        "destination": "validated",
        "record": validated_doc,
    }