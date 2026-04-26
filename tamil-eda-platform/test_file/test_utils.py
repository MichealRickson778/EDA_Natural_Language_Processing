# test_file/test_utils.py — delete after running

from pathlib import Path

from utils.checksum import (
    md5_of_file,
    md5_of_record,
    md5_of_text,
    sha256_of_bytes,
    sha256_of_file,
    verify_file_integrity,
)
from utils.id_generator import (
    IDGenerationError,
    generate_event_id,
    generate_job_id,
    generate_lineage_id,
    generate_request_id,
    generate_span_id,
    generate_trace_id,
    generate_version_id,
)
from utils.retry_utils import (
    RetryContext,
    RetryExhaustedError,
    calculate_backoff,
    is_retryable,
    retry_with_backoff,
)

# ── id_generator tests ────────────────────────────────────────────────────────

# Test 1 — correct prefixes
assert generate_job_id().startswith("job-")
assert generate_event_id().startswith("evt-")
assert generate_lineage_id().startswith("lin-")
assert generate_request_id().startswith("req-")

# Test 2 — trace and span IDs are correct length
assert len(generate_trace_id()) == 32
assert len(generate_span_id()) == 16

# Test 3 — IDs are unique
assert generate_job_id() != generate_job_id()
assert generate_trace_id() != generate_trace_id()

# Test 4 — version increments correctly
assert generate_version_id("v1.0") == "v1.1"
assert generate_version_id("v1.9") == "v1.10"
assert generate_version_id("v2.3") == "v2.4"

# Test 5 — invalid version format raises error
try:
    generate_version_id("1.0")
    assert False, "should have raised"
except IDGenerationError:
    pass

# ── retry_utils tests ─────────────────────────────────────────────────────────

# Test 6 — calculate_backoff increases with attempt
b1 = calculate_backoff(1, base=2.0, maximum=60.0, jitter=0.0)
b2 = calculate_backoff(2, base=2.0, maximum=60.0, jitter=0.0)
b3 = calculate_backoff(3, base=2.0, maximum=60.0, jitter=0.0)
assert b1 < b2 < b3

# Test 7 — backoff is capped at maximum
b_big = calculate_backoff(100, base=2.0, maximum=60.0, jitter=0.0)
assert b_big == 60.0

# Test 8 — is_retryable
assert is_retryable(TimeoutError()) is True
assert is_retryable(ConnectionError()) is True

class FakeHTTP503Error(Exception):
    status_code = 503
assert is_retryable(FakeHTTP503Error()) is True

class FakeHTTP401Error(Exception):
    status_code = 401
assert is_retryable(FakeHTTP401Error()) is False

# Test 9 — retry_with_backoff succeeds on second attempt
call_count = [0]
def flaky_fn(context: RetryContext) -> str:
    call_count[0] += 1
    if call_count[0] < 2:
        raise TimeoutError("first attempt fails")
    return "success"

result = retry_with_backoff(
    flaky_fn,
    max_attempts=3,
    base=0.01,
    maximum=0.1,
    retryable_exceptions=(TimeoutError,),
)
assert result == "success"
assert call_count[0] == 2

# Test 10 — RetryExhausted raised after all attempts
def always_fails(context: RetryContext) -> str:
    raise TimeoutError("always fails")

try:
    retry_with_backoff(
        always_fails,
        max_attempts=2,
        base=0.01,
        maximum=0.1,
        retryable_exceptions=(TimeoutError,),
    )
    assert False, "should have raised"
except RetryExhaustedError as e:
    assert "2" in e.message

# ── checksum tests ────────────────────────────────────────────────────────────

# Test 11 — md5_of_text is consistent
assert md5_of_text("hello") == md5_of_text("hello")

# Test 12 — normalisation works
assert md5_of_text("hello world") == md5_of_text("  hello world  ")

# Test 13 — different text = different hash
assert md5_of_text("hello") != md5_of_text("world")

# Test 14 — sha256_of_bytes
h1 = sha256_of_bytes(b"test data")
h2 = sha256_of_bytes(b"test data")
assert h1 == h2
assert len(h1) == 64

# Test 15 — md5_of_record is key-order independent
r1 = {"a": 1, "b": 2}
r2 = {"b": 2, "a": 1}
assert md5_of_record(r1) == md5_of_record(r2)

# Test 16 — file hashing and integrity verification
test_file = Path("test_file/test_data.txt")
test_file.write_text("Tamil EDA Platform test data", encoding="utf-8")

md5 = md5_of_file(test_file)
sha256 = sha256_of_file(test_file)
assert len(md5) == 32
assert len(sha256) == 64

# Test 17 — verify_file_integrity
assert verify_file_integrity(test_file, sha256) is True
assert verify_file_integrity(test_file, "wrong_hash") is False

# Cleanup
test_file.unlink()

print("All assertions passed.")
