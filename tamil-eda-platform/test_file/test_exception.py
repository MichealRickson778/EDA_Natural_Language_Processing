from core.exceptions import (
    DAGError,
    IngestionError,
    PlatformError,
    SecretsError,
)

# Test 1 — basic message
e = PlatformError("something went wrong")
assert str(e) == "something went wrong"

# Test 2 — with details
e = IngestionError("CSV failed", details = {"row": 42})
assert str(e) == "CSV failed | details = {'row': 42}"
assert e.details["row"] == 42

# Test 3 — inheritance chain works
assert isinstance(IngestionError("x"), PlatformError)
assert isinstance(SecretsError("x"), PlatformError)
assert isinstance(DAGError("x"), PlatformError)

print("All assertions passed.")

