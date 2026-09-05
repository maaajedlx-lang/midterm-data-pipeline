from __future__ import annotations

import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    SMALL_FILE_THRESHOLD_MB,
    VALIDATED_COLLECTION,
)
from src.create_small_sample import create_sample
from src.elt_pipeline import execute_elt_pipeline
from src.file_router import choose_engine


def main():
    print("=" * 80)
    print("      📊 الدليل العملي الشامل والمباشر لتحقيق كافة متطلبات المشروع")
    print("=" * 80)

    # -------------------------------------------------------------
    # الإثبات 1: الموجه التلقائي File Router
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("1. إثبات عمل الموجه التلقائي (File Router) بالحد 200 MB:")
    print("🔹 " * 20)
    small_file = PROJECT_ROOT / "data" / "small_sample.csv"
    large_file = PROJECT_ROOT / "data" / "sample_300mb.csv"

    eng_small, sz_small, reason_small = choose_engine(small_file, SMALL_FILE_THRESHOLD_MB)
    print(f"  [ملف صغير] الملف: {small_file.name} | الحجم: {sz_small:.2f} MB | المحرك المختار: {eng_small}")
    print(f"             السبب: {reason_small}")

    eng_large, sz_large, reason_large = choose_engine(large_file, SMALL_FILE_THRESHOLD_MB)
    print(f"  [ملف كبير] الملف: {large_file.name} | الحجم: {sz_large:.2f} MB | المحرك المختار: {eng_large}")
    print(f"             السبب: {reason_large}")

    # -------------------------------------------------------------
    # الإثبات 2: تشغيل خط البيانات الكامل وتطبيق قواعد الجودة والتصنيف
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("2. إثبات تشغيل خط البيانات ELT الكامل (التشغيل الأول):")
    print("🔹 " * 20)

    run1_metrics = execute_elt_pipeline(
        input_file=small_file,
        reset_db=True,
    )

    # -------------------------------------------------------------
    # الإثبات 3: التحقق من معادلة اتساق البيانات
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("3. إثبات معادلة اتساق البيانات (Consistency Equation):")
    print("🔹 " * 20)
    raw_cnt = run1_metrics["loaded_raw"]
    val_cnt = run1_metrics["count_valid"]
    corr_cnt = run1_metrics["count_corrected"]
    quar_cnt = run1_metrics["count_quarantine"]
    total_class = val_cnt + corr_cnt + quar_cnt

    print(f"  - السجلات التي دخلت Raw أولاً  : {raw_cnt:,}")
    print(f"  - السجلات السليمة (Valid)     : {val_cnt:,}")
    print(f"  - السجلات المصححة (Corrected) : {corr_cnt:,}")
    print(f"  - السجلات المعزولة (Quarantine): {quar_cnt:,}")
    print(f"  - مجموع التصنيف               : {val_cnt:,} + {corr_cnt:,} + {quar_cnt:,} = {total_class:,}")
    print(f"  - نتيجة المطابقة              : {'✅ مطابقة 100% (لا يوجد أي سجل مفقود)' if raw_cnt == total_class else '❌ غير مطابقة'}")

    # -------------------------------------------------------------
    # الإثبات 4: إثبات عدم التكرار (Idempotency Proof)
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("4. إثبات عدم التكرار الموثوق (Idempotency Re-Run Proof):")
    print("🔹 " * 20)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]

    val_before = db[VALIDATED_COLLECTION].count_documents({})
    print(f"  [قبل إعادة التشغيل] عدد السجلات التجارية في {VALIDATED_COLLECTION}: {val_before:,}")

    print("  ... جاري إعادة تشغيل نفس الملف للمرة الثانية على نفس قاعدة البيانات ...")
    run2_metrics = execute_elt_pipeline(
        input_file=small_file,
        reset_db=False,  # لا نحذف قاعدة البيانات للتأكد من عدم التكرار!
    )

    val_after = db[VALIDATED_COLLECTION].count_documents({})
    inserted_run2 = run2_metrics["count_inserted"]
    updated_run2 = run2_metrics["count_updated"]

    print(f"\n  [بعد إعادة التشغيل]")
    print(f"  - السجلات الجديدة المضافة (Inserted): {inserted_run2:,} (المتوقع: 0)")
    print(f"  - السجلات الموجودة المحدثة (Updated) : {updated_run2:,}")
    print(f"  - إجمالي السجلات في {VALIDATED_COLLECTION} : {val_after:,} (السابق: {val_before:,})")

    if val_before == val_after and inserted_run2 == 0:
        print("  - النتيجة: ✅ تم إثبات الـ Idempotency بنجاح: لم يتضاعف أي سجل تجاري، وبقي عدد السجلات ثابتاً!")
    else:
        print("  - النتيجة: ❌ فشل اختبار Idempotency!")

    # -------------------------------------------------------------
    # الإثبات 5: عينات حية من MongoDB
    # -------------------------------------------------------------
    print("\n" + "🔹 " * 20)
    print("5. عينات حية من المجموعات الثلاث في MongoDB:")
    print("🔹 " * 20)

    # عينة من orders_raw
    sample_raw = db[RAW_COLLECTION].find_one({"id_run": run1_metrics["id_run"]})
    print("\n  أ) عينة من مجموعة البيانات الخام (orders_raw):")
    print(f"     - id_run: {sample_raw.get('id_run')}")
    print(f"     - engine_used: {sample_raw.get('engine_used')}")
    print(f"     - source_file: {sample_raw.get('source_file')}")
    print(f"     - raw_record (كما وردت): {sample_raw.get('raw_record')}")

    # عينة مصححة من orders_validated مع أثر التصحيح
    sample_corr = db[VALIDATED_COLLECTION].find_one({"quality_status": "corrected", "corrections.0": {"$exists": True}})
    print("\n  ب) عينة من سجل مصحح في (orders_validated) يوضح أثر التصحيح (Audit Trail):")
    print(f"     - id_order: {sample_corr.get('id_order')}")
    print(f"     - quality_status: {sample_corr.get('quality_status')}")
    print(f"     - customer_name: {sample_corr.get('customer_name')}")
    print(f"     - order_date: {sample_corr.get('order_date')}")
    print(f"     - total_amount: {sample_corr.get('total_amount')} {sample_corr.get('currency')}")
    print("     - مصفوفة أثر التصحيح (Corrections Array):")
    for c in sample_corr.get("corrections", []):
        print(f"       * الحقل: {c['field']} | القيمة الأصلية: '{c['original_value']}' -> القيمة المصححة: '{c['corrected_value']}' | كود القاعدة: {c['rule_code']}")

    # عينة من quarantine_orders مع رموز الخطأ
    sample_quar = db[QUARANTINE_COLLECTION].find_one()
    print("\n  ج) عينة من سجل معزول في (quarantine_orders):")
    print(f"     - id_order: {sample_quar.get('id_order')}")
    print(f"     - error_codes: {sample_quar.get('error_codes')}")
    print(f"     - error_details: {sample_quar.get('error_details')}")
    print(f"     - raw_record المحفوظ: {sample_quar.get('raw_record')}")

    # فهارس المجموعات
    print("\n  د) الفهارس المنشأة في MongoDB:")
    print(f"     - فهارس orders_validated: {[idx['name'] for idx in db[VALIDATED_COLLECTION].list_indexes()]}")
    print(f"     - فهارس quarantine_orders: {[idx['name'] for idx in db[QUARANTINE_COLLECTION].list_indexes()]}")
    print(f"     - فهارس orders_raw: {[idx['name'] for idx in db[RAW_COLLECTION].list_indexes()]}")

    client.close()

    print("\n" + "=" * 80)
    print("✨ تم اكتمال إثبات جميع المتطلبات بنجاح وبشكل عملي وموثق!")
    print("=" * 80)


if __name__ == "__main__":
    main()
