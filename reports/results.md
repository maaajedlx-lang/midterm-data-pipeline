# Midterm Data Pipeline Execution Report
## تقرير تشغيل خط البيانات الهجين (جامعة الرازي)

**Report Generated**: metrics.py
**Total Runs Documented**: 21

---
## Summary of Pipeline Runs

| Run ID | File Name | Size (MB) | Engine | Rows Read | Valid | Corrected | Quarantine | Inserted | Updated | Unchanged | Elapsed (s) | Throughput (rec/s) | Consistency Check |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `b5254d3d...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 29.30 | 3,412.47 | ✅ PASS |
| `d04cc08b...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 0 | 93,782 | 0 | 28.24 | 3,540.54 | ✅ PASS |
| `cc9b1ddd...` | `small_sample.csv` | 41.77 | `pyspark` | 100,000 | 68,959 | 24,836 | 6,205 | 13 | 93,782 | 0 | 42.79 | 2,337.12 | ✅ PASS |
| `f1118eec...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 40.50 | 2,469.02 | ✅ PASS |
| `88e7b52e...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 40.15 | 2,490.82 | ✅ PASS |
| `7c685031...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 0 | 93,782 | 0 | 40.44 | 2,472.72 | ✅ PASS |
| `c49d6389...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 41.29 | 2,421.77 | ✅ PASS |
| `4e5f5615...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 0 | 93,782 | 0 | 40.12 | 2,492.74 | ✅ PASS |
| `f62c49d4...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 41.29 | 2,422.07 | ✅ PASS |
| `00a2786c...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 0 | 93,782 | 0 | 40.00 | 2,499.94 | ✅ PASS |
| `6fff15e3...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 40.90 | 2,444.88 | ✅ PASS |
| `00172a74...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 0 | 93,782 | 0 | 39.91 | 2,505.68 | ✅ PASS |
| `a02ba0ba...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 40.83 | 2,449.07 | ✅ PASS |
| `e712c10e...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 41.62 | 2,402.86 | ✅ PASS |
| `3ad06b0e...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 0 | 93,782 | 0 | 39.63 | 2,523.43 | ✅ PASS |
| `21a2eb94...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 88,782 | 5,000 | 0 | 28.63 | 3,492.45 | ✅ PASS |
| `afd41441...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 0 | 93,782 | 0 | 27.45 | 3,643.37 | ✅ PASS |
| `e0d4fd2b...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 29.20 | 3,424.89 | ✅ PASS |
| `b1dab2f4...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 0 | 93,782 | 0 | 27.89 | 3,585.24 | ✅ PASS |
| `1328d721...` | `small_sample.csv` | 41.77 | `python_batch` | 100,000 | 68,932 | 24,850 | 6,218 | 93,782 | 0 | 0 | 28.77 | 3,475.72 | ✅ PASS |
| `11e4b187...` | `small_sample.csv` | 209.20 | `pyspark` | 500,000 | 344,673 | 124,199 | 31,128 | 468,872 | 0 | 0 | 155.70 | 3,211.29 | ✅ PASS |

---
## Detailed Error Taxonomy & Quarantine Breakdown

### Run 1: `b5254d3d-9012-4ac3-bdeb-bac1b007420a` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 2: `d04cc08b-6774-4b0f-8abf-7774f11d75ef` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 3: `cc9b1ddd-6c3f-4c6d-939b-032071c6a54c` (pyspark)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 687 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |


### Run 4: `f1118eec-e73e-4b95-b4a8-92f4a215cdd8` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 5: `88e7b52e-226e-4456-959e-7083353c4609` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 6: `7c685031-e702-47d6-b5d6-8af809473953` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 7: `c49d6389-8401-4962-9283-80c2fc936fb2` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 8: `4e5f5615-e451-4bec-8861-1d89fdead3a1` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 9: `f62c49d4-4be7-4faa-8cb0-369a3637e838` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 10: `00a2786c-93c5-4925-bb19-84623337443e` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 11: `6fff15e3-3e45-4455-907f-84791c493eef` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 12: `00172a74-f8af-4743-9797-7179f0e80f98` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 13: `a02ba0ba-1448-427e-b2f2-462fe9b6a7a0` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 14: `e712c10e-b662-4ed6-8ca4-8644ff78ea96` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 15: `3ad06b0e-eff4-4b95-8a1e-027e03ca4e2e` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 16: `21a2eb94-f526-4cad-8b72-f801c5c41a7e` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 17: `afd41441-cf19-468a-b54b-8e57222c5661` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 18: `e0d4fd2b-719a-457d-bf53-e3dcb0b947f7` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 19: `b1dab2f4-639f-4fb8-9030-bdd562e99aa7` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 20: `1328d721-20ca-49bc-bc31-ac6512e5911e` (python_batch)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 1,351 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 674 | Isolation category |
| `ID_CUSTOMER_MISSING` | 1,411 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 1,338 | Isolation category |
| `PRICE_UNKNOWN` | 674 | Isolation category |
| `ID_ORDER_DUPLICATE` | 672 | Isolation category |
| `ID_ORDER_MISSING` | 721 | Isolation category |
| `ITEMS_EMPTY` | 677 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 722 | Isolation category |


### Run 21: `11e4b187-f294-461b-8639-fdb63f1cf37b` (pyspark)
| Error Code | Occurrence Count | Description |
| --- | --- | --- |
| `VALUE_NEGATIVE_AMBIGUOUS` | 6,818 | Isolation category |
| `ITEMS_EMPTY` | 3,487 | Isolation category |
| `ERRORS_CONFLICTING_MULTIPLE` | 3,527 | Isolation category |
| `ID_CUSTOMER_MISSING` | 7,070 | Isolation category |
| `JSON_ITEMS_CORRUPTED` | 6,866 | Isolation category |
| `PRICE_UNKNOWN` | 3,461 | Isolation category |
| `DATE_IMPOSSIBLE_INVALID` | 3,488 | Isolation category |
| `ID_ORDER_MISSING` | 3,506 | Isolation category |
| `ID_ORDER_DUPLICATE` | 3,427 | Isolation category |


---
## Idempotency & Upsert Verification

- **Unique Business Key**: `id_order` with unique index `uq_validated_id_order` in `orders_validated`.
- **Re-run Behavior**: Re-running the pipeline on identical datasets yields `inserted_count = 0` (zero new business records inserted).
- **Consistency Equation**: Verified `run_raw_count == run_valid_count + run_corrected_count + run_quarantine_count` across all pipeline runs.