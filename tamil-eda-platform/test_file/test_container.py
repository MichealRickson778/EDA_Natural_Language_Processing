# test_container.py — delete after running

from core.dependency_container import (
    CircularDependencyError,
    DependencyContainer,
    ServiceNotFoundError,
)

# Test 1 — basic register and resolve
c = DependencyContainer()
c.register("greeting", lambda: "hello")
assert c.resolve("greeting") == "hello"

# Test 2 — singleton returns same instance
c.register("counter", lambda: [0], singleton=True)
instance1 = c.resolve("counter")
instance2 = c.resolve("counter")
assert instance1 is instance2

# Test 3 — non-singleton returns new instance each time
c.register("fresh", lambda: [0], singleton=False)
a = c.resolve("fresh")
b = c.resolve("fresh")
assert a is not b

# Test 4 — ServiceNotFoundError on unknown service
try:
    c.resolve("does_not_exist")
    assert False, "should have raised"
except ServiceNotFoundError as e:
    assert "does_not_exist" in e.message

# Test 5 — override works
c.register("backend", lambda: "real")
c.override("backend", "mock")
assert c.resolve("backend") == "mock"

# Test 6 — reset_overrides restores real service
c.reset_overrides()
assert c.resolve("backend") == "real"

# Test 7 — has() works
assert c.has("greeting") is True
assert c.has("missing") is False

# Test 8 — circular dependency detected
c2 = DependencyContainer()
c2.register("a", lambda: c2.resolve("b"))
c2.register("b", lambda: c2.resolve("a"))
try:
    c2.resolve("a")
    assert False, "should have raised"
except CircularDependencyError as e:
    assert "circular" in e.message.lower()

# Test 9 — failed factory not cached
call_count = [0]

def failing_factory():
    call_count[0] += 1
    if call_count[0] == 1:
        raise RuntimeError("first call fails")
    return "success"

c3 = DependencyContainer()
c3.register("flaky", failing_factory)
try:
    c3.resolve("flaky")
except Exception:
    pass
result = c3.resolve("flaky")
assert result == "success"
assert call_count[0] == 2

# Test 10 — shutdown calls close()
class FakeService:
    def __init__(self):
        self.closed = False
    def close(self):
        self.closed = True

c4 = DependencyContainer()
svc = FakeService()
c4.register("svc", lambda: svc)
c4.resolve("svc")
c4.shutdown()
assert svc.closed is True

print("All assertions passed.")
