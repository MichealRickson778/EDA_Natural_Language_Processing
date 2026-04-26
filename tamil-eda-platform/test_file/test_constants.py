# test_constants.py — delete after running

from core.constants import (
    DEBOUNCE_SECONDS,
    DEFAULT_WORKER_COUNT,
    MAX_BATCH_SIZE,
    MAX_DATASET_RECORDS,
    MAX_QUEUE_SIZE,
    MAX_RETRIES,
    MAX_TRANSLATION_CHARS,
    MIN_CONFIDENCE_SCORE,
    PARQUET_ROW_GROUP_SIZE,
    RECORDS_PER_SHARD,
    SECRET_CACHE_TTL_SECONDS,
)

# Test 1 — values are correct types
assert isinstance(MAX_TRANSLATION_CHARS, int)
assert isinstance(MIN_CONFIDENCE_SCORE, float)
assert isinstance(DEBOUNCE_SECONDS, float)

# Test 2 — values are sensible
assert MAX_RETRIES == 3
assert MAX_BATCH_SIZE == 100
assert MIN_CONFIDENCE_SCORE == 0.60
assert MAX_QUEUE_SIZE == 10_000
assert MAX_DATASET_RECORDS == 10_000_000
assert RECORDS_PER_SHARD == 100_000
assert PARQUET_ROW_GROUP_SIZE == 100_000
assert SECRET_CACHE_TTL_SECONDS == 300
assert DEFAULT_WORKER_COUNT == 4
assert MAX_TRANSLATION_CHARS == 5000

print("All assertions passed.")
