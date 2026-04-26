# test_enums.py — delete after running

from core.enums import (
    ContentType,
    FileFormat,
    JobPriority,
    JobStatus,
    Language,
)

# Test 1 — str inheritance works
assert Language.TAMIL == "ta"
assert FileFormat.CSV == "csv"
assert JobStatus.PENDING == "pending"

# Test 2 — int inheritance works for priority comparison
assert JobPriority.HIGH > JobPriority.LOW
assert JobPriority.CRITICAL > JobPriority.NORMAL

# Test 3 — lookup by value works
assert Language("ta") == Language.TAMIL
assert FileFormat("pdf") == FileFormat.PDF

# Test 4 — UNKNOWN values exist where needed
assert FileFormat.UNKNOWN == "unknown"
assert Language.UNKNOWN == "unknown"
assert ContentType.UNKNOWN == "unknown"

print("All assertions passed.")
