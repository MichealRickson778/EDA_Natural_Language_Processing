from __future__ import annotations

from collections.abc import Callable
from typing import Any

from core.exceptions import PlatformError


class ServiceNotFoundError(PlatformError):
    """Raised when resolve() is called for an unregistered service"""

class CircularDependencyError(PlatformError):
    """Raised when a circular depedency is detected during resolve"""

class DependencyContainer:
    """Central service registry for the Tamil EDA platform
    Responsibilities:

     - Register services with a factory function
        - Resolve services by name (singleton by default)
        - Allow test overrides without changing application code
        - Detect circular dependencies at resolve time
        - Clean up all services gracefully on shutdown

    Usage:
        # bootstrap.py — register once at startup
        container = DependencyContainer()
        container.register('settings', lambda: build_settings())
        container.register('db_connector', lambda: DBConnector(
            url=container.resolve('settings').database.url
        ))

        # any component — resolve when needed
        settings = container.resolve('settings')

        # tests — override before resolving
        container.override('db_connector', MockDBConnector())
        connector = container.resolve('db_connector')  # gets mock
        container.reset_overrides()
    """
    def __init__(self) -> None:

        #registerd factories - name -> callable
        self._factories: dict[str, Callable[[], Any]] = {}

        #Cached singleton instances - name -> instance
        self._singletons: dict[str, Any] = {}

        #whether each service is a singleton
        self._is_singleton: dict[str, bool] = {}

        #Test overrides - name -> pre-built instance
        self._overrides: dict[str, Any] = {}

        #trackers service currently being built - detects circular deps
        self._building: set[str] = set()

#Registration----------------------------------------------------------------

    def register(
            self,
            name: str,
            factory: Callable[[], Any],
            singleton: bool = True,
    ) -> None:
        """Register a service factory
        Args:
            name:      Unique service name. Used as the key for resolve().
            factory:   A callable (usually a lambda) that creates the service.
                       Called once for singletons, every time for non-singletons.
            singleton: If True (default), factory is called once and the
                       instance is cached. If False, a new instance is created
                       on every resolve() call.

        Example:
            container.register('settings', lambda: build_settings())
            container.register('db_connector',
                lambda: DBConnector(url=container.resolve('settings').database.url),
                singleton=True
            )
        """
        self._factories[name] = factory
        self._is_singleton[name] = singleton

#Resolution--------------------------------------------------------------

    def resolve(self, name:str) -> Any:
        """Resolve a service by name.

        Returns the service instance. For singletons, returns the cached
        instance after the first call. For non-singletons, calls the factory
        every time.

        Args:
            name: The service name used during register().

        Returns:
            The service instance.

        Raises:
            ServiceNotFoundError: If name was never registered.
            CircularDependencyError: If resolving name causes a cycle.
        """
    #Test override takes priority over everything
        if name in self._overrides:
            return self._overrides[name]

    #Check registration
        if name not in self._factories:
            registered = sorted(self._factories.keys())
            raise ServiceNotFoundError(
                f"Service not found: '{name}'",
                details={"requested":name, "registered":registered},
            )
    #Return cached singeleton if available
        if self._is_singleton[name] and name in self._singletons:
            return self._singletons[name]

    #Detect circular dependency
        if name in self._building:
            cycle = "->".join(sorted(self._building)) + f"-> {name}"
            raise CircularDependencyError(
                f"Circular dependency detected: {cycle}",
                details={"cycle": cycle}
            )

    #Build the service
        self._building.add(name)
        try:
            instance = self._factories[name]()
        except(ServiceNotFoundError, CircularDependencyError):
            raise
        except Exception as exc:
            raise PlatformError(
                f"Failed to build service '{name}'",
                details={"service": name, "error": str(exc)},
                )from exc
        finally:
            self._building.discard(name)

    #Cache if singleton
        if self._is_singleton[name]:
            self._singletons[name] = instance

        return instance

    def has(self, name: str) -> bool:
        """Return True if a service is registered under this name
        Use this to check before resolve() if you want to avoid catching
        ServiceNotFoundError.

        Example:
            if container.has('redis_connector'):
                redis = container.resolve('redis_connector')
        """
        return name in self._factories or name in self._overrides

    def override(self, name: str, instance: Any) -> None:
        """Replace a service with a pre-built instance for testing.

        The override takes effect immediately for all subsequent resolve()
        calls. Does not affect the registered factory.

        Always call reset_overrides() in test teardown to prevent
        state leaking between tests.

        Args:
            name:     The service name to override.
            instance: The pre-built instance to return from resolve().

        Example:
            container.override('translator_engine', MockTranslator())
            engine = container.resolve('translator_engine')  # gets mock
        """
        self._overrides[name] = instance

    def reset_overrides(self) -> None:
        """Remove all test overrides.

            Call this in pytest teardown fixtures to ensure each test
            starts with a clean container state.

            Example:
            @pytest.fixture(autouse=True)
            def clean_container():
                yield
                container.reset_overrides()
            """
        self._overrides.clear()

    #Shutdown------------------------------------------------------------

    def shutdown(self) -> None:
        """Gracefully shut down all registered singleton services.

        Calls .close() or .disconnect() on every singleton that has
        one of these methods. Skips stateless services silently.

        Called by lifecycle.py during graceful shutdown.

        Order: overrides first, then singletons (reverse registration order).
        """

        #shutdown overrides first
        for instance in self._overrides.values():
            self._close_service(instance)

        #shutdown singletons in reverse registration order
        for name in reversed(list(self._singletons.keys())):
            self._close_service(self._singletons[name])

        # clear all state
        self._singletons.clear()
        self._overrides.clear()
        self._building.clear()

    def _close_service(self, instance: Any) -> None:
        """Attempt to close a single service instance safely"""

        if hasattr(instance, "close"):
            try:
                instance.close()
            except Exception:
                pass # best effort - log in real implementation
        elif hasattr(instance, "disconnect"):
            try:
                instance.disconnect()
            except Exception:
                pass # best effort

    # Introspection------------------------------------------------------

    def registered_services(self) -> list[str]:
        """Return sorted list of all registered service names
        Useful for debugging and health checks
        """
        return sorted(self._factories.keys())

    def __repr__(self) -> str:
        return(
            f"DependencyContainer("
            f"registered={len(self._factories)}, "
            f"singletons={len(self._singletons)}, "
            f"overrides={len(self._overrides)})"
            )
# ── SINGLETON ─────────────────────────────────────────────────────────────────
# One container instance shared across the entire platform.
# bootstrap.py registers all services into this.
# Every component resolves from this.
container = DependencyContainer()









