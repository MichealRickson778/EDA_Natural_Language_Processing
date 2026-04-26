# test_settings.py — delete after running

from core.enums import Language, TranslationBackend
from core.settings import AppSettings, build_settings, load_config

# Test 1 — load_config merges correctly
raw = load_config("dev")
assert raw["platform"]["debug"] is True
assert raw["translation"]["default_backend"] == "mock"
assert raw["observability"]["log_level"] == "DEBUG"

# Test 2 — build_settings returns AppSettings
s = build_settings("dev")
assert isinstance(s, AppSettings)
assert s.env == "dev"

# Test 3 — dev values loaded correctly
assert s.platform.debug is True
assert s.translation.default_backend == TranslationBackend.MOCK
assert s.observability.log_level == "DEBUG"
assert s.database.url == "sqlite:///data/dev.db"

# Test 4 — defaults from constants applied
assert s.translation.max_chars_per_request == 5000
assert s.queue.max_size == 10000
assert s.translation.max_batch_size == 100

# Test 5 — nested access works
assert s.translation.default_target_language == Language.TAMIL
assert s.redis.url == "redis://localhost:6379/0"

# Test 6 — invalid log level rejected
try:
    from core.settings import ObservabilitySettings
    ObservabilitySettings(log_level="INVALID")
    assert False, "should have raised"
except Exception:
    pass

# Test 7 — singleton exists
from core.settings import settings

assert settings.env == "dev"

print("All assertions passed.")
