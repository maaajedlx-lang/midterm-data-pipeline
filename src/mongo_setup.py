from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import ASCENDING, MongoClient

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)

# Schema for the final validated collection (orders_validated)
VALIDATED_SCHEMA = {
    "bsonType": "object",
    "required": [
        "id_order",
        "id_run",
        "quality_status",
    ],
    "properties": {
        "id_order": {
            "bsonType": "string",
            "description": "Unique business key for the order",
        },
        "id_run": {
            "bsonType": "string",
            "description": "Unique pipeline execution identifier",
        },
        "order_date": {
            "bsonType": ["string", "null"],
        },
        "customer_id": {
            "bsonType": ["string", "null"],
        },
        "customer_name": {
            "bsonType": ["string", "null"],
        },
        "customer_phone": {
            "bsonType": ["string", "null"],
        },
        "customer_email": {
            "bsonType": ["string", "null"],
        },
        "city": {
            "bsonType": ["string", "null"],
        },
        "district": {
            "bsonType": ["string", "null"],
        },
        "delivery_type": {
            "bsonType": ["string", "null"],
        },
        "delivery_cost": {
            "bsonType": ["number", "null"],
        },
        "payment_method": {
            "bsonType": ["string", "null"],
        },
        "payment_status": {
            "bsonType": ["string", "null"],
        },
        "payment_amount": {
            "bsonType": ["number", "null"],
        },
        "currency": {
            "bsonType": ["string", "null"],
        },
        "total_amount": {
            "bsonType": ["number", "null"],
        },
        "items": {
            "bsonType": ["array", "null"],
        },
        "quality_status": {
            "enum": [
                "valid",
                "corrected",
            ],
        },
        "record_hash": {
            "bsonType": ["string", "null"],
        },
        "corrections": {
            "bsonType": "array",
            "items": {
                "bsonType": "object",
                "required": ["field", "original_value", "corrected_value", "rule_code"],
                "properties": {
                    "field": {"bsonType": "string"},
                    "original_value": {"bsonType": ["string", "null", "number", "bool"]},
                    "corrected_value": {"bsonType": ["string", "null", "number", "bool"]},
                    "rule_code": {"bsonType": "string"},
                },
            },
        },
        "updated_at": {
            "bsonType": ["date", "string", "null"],
        },
    },
}


def setup_mongodb(drop_existing: bool = False) -> None:
    """
    Sets up MongoDB collections, schema validators, and indexes.
    If drop_existing is True, existing collections are dropped first.
    """
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

    try:
        client.admin.command("ping")
        db = client[MONGO_DATABASE]

        print("=" * 60)
        print("MONGODB SETUP (ASSIGNMENT SECTION 6.10)")
        print("=" * 60)
        print(f"Database: {MONGO_DATABASE}")
        print("MongoDB connection: OK")

        if drop_existing:
            print("Resetting database (drop_existing=True)...")
            for coll in [RAW_COLLECTION, VALIDATED_COLLECTION, QUARANTINE_COLLECTION, "orders_quarantine", "validated_orders"]:
                if coll in db.list_collection_names():
                    db[coll].drop()
                    print(f"Dropped collection: {coll}")

        existing_collections = set(db.list_collection_names())

        # 1. RAW COLLECTION (orders_raw):
        # Must NOT have Schema Validator or Unique Index that prevents loading!
        if RAW_COLLECTION not in existing_collections:
            db.create_collection(RAW_COLLECTION)
            print(f"Created collection: {RAW_COLLECTION} (No Validator, No Unique Index)")
        else:
            print(f"Collection exists: {RAW_COLLECTION}")

        # 2. VALIDATED COLLECTION (orders_validated):
        # Strict Schema Validation + Unique Index on id_order + Idempotent Upsert
        if VALIDATED_COLLECTION not in existing_collections:
            db.create_collection(
                VALIDATED_COLLECTION,
                validator={"$jsonSchema": VALIDATED_SCHEMA},
                validationLevel="strict",
                validationAction="error",
            )
            print(f"Created collection: {VALIDATED_COLLECTION} (with Strict Schema Validator)")
        else:
            db.command(
                "collMod",
                VALIDATED_COLLECTION,
                validator={"$jsonSchema": VALIDATED_SCHEMA},
                validationLevel="strict",
                validationAction="error",
            )
            print(f"Updated validator for collection: {VALIDATED_COLLECTION}")

        # 3. QUARANTINE COLLECTION (quarantine_orders):
        # Must store uncorrectable records, error_codes, error_details, and raw_record
        if QUARANTINE_COLLECTION not in existing_collections:
            db.create_collection(QUARANTINE_COLLECTION)
            print(f"Created collection: {QUARANTINE_COLLECTION}")
        else:
            print(f"Collection exists: {QUARANTINE_COLLECTION}")

        # Create indexes
        validated = db[VALIDATED_COLLECTION]
        quarantine = db[QUARANTINE_COLLECTION]
        raw = db[RAW_COLLECTION]

        # orders_validated unique business key index (id_order)
        validated.create_index(
            [("id_order", ASCENDING)],
            unique=True,
            name="uq_validated_id_order",
        )
        validated.create_index(
            [("quality_status", ASCENDING)],
            name="idx_validated_quality_status",
        )
        validated.create_index(
            [("id_run", ASCENDING)],
            name="idx_validated_id_run",
        )

        # quarantine_orders indexes
        quarantine.create_index(
            [("id_order", ASCENDING)],
            name="idx_quarantine_id_order",
        )
        quarantine.create_index(
            [("error_codes", ASCENDING)],
            name="idx_quarantine_error_codes",
        )
        quarantine.create_index(
            [("codes_error", ASCENDING)],
            name="idx_quarantine_codes_error",
        )
        quarantine.create_index(
            [("id_run", ASCENDING)],
            name="idx_quarantine_id_run",
        )

        # orders_raw indexes (historical tracking per id_run)
        raw.create_index(
            [("id_run", ASCENDING)],
            name="idx_raw_id_run",
        )
        raw.create_index(
            [("source_file", ASCENDING)],
            name="idx_raw_source_file",
        )

        print("-" * 60)
        print(f"Raw Collection        : {RAW_COLLECTION} (All raw records preserved)")
        print(f"Validated Collection  : {VALIDATED_COLLECTION} (Unique id_order + Upsert)")
        print(f"Quarantine Collection : {QUARANTINE_COLLECTION} (Error codes + Raw record)")
        print("Unique Business Key   : id_order (Index: uq_validated_id_order)")
        print("Idempotent Upsert      : Configured")
        print("=" * 60)

    finally:
        client.close()


if __name__ == "__main__":
    setup_mongodb()