
from abc import ABC, abstractmethod
from typing import Any

from core.exceptions import ConnectionError


class ConnectorError(ConnectionError):
    """Raised when the Connection had an issue"""

class BaseConnector(ABC):
    # define the name, timeout, connected state
    def __init__(self, name: str, timeout_s: int):
        self.name = name
        self.timeout_s = timeout_s
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        """connect to the file/API/Cloud/Database"""

    @abstractmethod
    def health_check(self) -> bool:
        """check the system health return as bool type"""

    @abstractmethod
    def disconnect(self) -> None:
        """disconnect once the work is done"""

    @abstractmethod
    def read(self, source: str, **kwargs) -> Any:
        """Read from the external system"""

    @abstractmethod
    def write(self, destination: str, data: Any, **kwargs) -> None:
        """Write from any external system"""

    @abstractmethod
    def exists(self, source: str, **kwargs) -> bool:
        """Return True if a resource exists at the given path."""

    # Non-abstract: shared behaviour all connectors get for free
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def is_connected(self) -> bool:
        return self._connected

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', connected={self._connected})"

