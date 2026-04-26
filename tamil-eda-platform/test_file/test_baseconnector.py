from connectors.base_connector import BaseConnector


class TestConnector(BaseConnector):
    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def health_check(self) -> bool:
        return True

c = TestConnector(name="test", timeout_s=30)

assert c.is_connected() is False

assert repr(c) == "TestConnector(name='test', connected=False)"

try:
    BaseConnector()
    assert False, "Should have raised TypeError"
except TypeError:
    pass

print("All assertions are passed")
