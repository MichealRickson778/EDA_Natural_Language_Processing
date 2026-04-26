import logging
import threading
import time
from typing import Any

from core.exceptions import PlatformError
from core.enums import TriggerState
from trigger.base_trigger import BaseTrigger



class TriggerManagerError(PlatformError):
    """Raised when TriggerManager encounters an error"""


class TriggerManager:
    """Registers, starts, stops, and monitors all triggers."""

    def __init__(self,
                 health_check_interval_s: int,
                 max_restart_attempts: int,
                 stop_timeout_s: int):
        self._triggers: list[BaseTrigger] = []
        self._lock = threading.RLock()
        self._running = False
        self._health_loop_thread: threading.Thread | None = None
        self.health_check_interval_s = health_check_interval_s
        self.max_restart_attempts = max_restart_attempts
        self.stop_timeout_s = stop_timeout_s
        # TODO: replace with setup_logger() when bootstrap is ready
        self._logger = logging.getLogger("trigger_manager")

    # ── Registration ──────────────────────────────────────────────────────────

    def register(self, trigger: BaseTrigger) -> None:
        """Register a trigger. Must be called before start_all()."""
        with self._lock:
            # check for duplicate names
            if any(t.name == trigger.name for t in self._triggers):
                raise TriggerManagerError(
                    f"Duplicate trigger name: {trigger.name}",
                    details={"name": trigger.name}
                )
            self._triggers.append(trigger)
            self._logger.info(f"Trigger registered: {trigger.name}")

    def get_trigger(self, name: str) -> BaseTrigger | None:
        """Find a trigger by name."""
        with self._lock:
            for trigger in self._triggers:
                if trigger.name == name:
                    return trigger
        return None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start_all(self) -> None:
        """Start all registered triggers."""
        with self._lock:
            if not self._triggers:
                raise TriggerManagerError(
                    "No triggers registered before start_all(). Call register() first."
                )

        self._running = True

        # start health loop
        self._health_loop_thread = threading.Thread(
            target=self._health_loop,
            daemon=True,
            name="trigger_health_loop"
        )
        self._health_loop_thread.start()

        # start all triggers in registration order
        with self._lock:
            for trigger in self._triggers:
                try:
                    trigger.start()
                    self._logger.info(f"Trigger started: {trigger.name}")
                except Exception as exc:
                    self._logger.error(
                        f"Failed to start trigger: {trigger.name} — {exc}"
                    )

    def stop_all(self) -> None:
        """Stop all triggers in reverse registration order."""
        self._running = False

        with self._lock:
            triggers_reversed = list(reversed(self._triggers))

        for trigger in triggers_reversed:
            # stop with timeout
            thread = threading.Thread(target=trigger.stop)
            thread.start()
            thread.join(timeout=self.stop_timeout_s)
            if thread.is_alive():
                self._logger.error(
                    f"Trigger stop timed out: {trigger.name}"
                )
            else:
                self._logger.info(f"Trigger stopped: {trigger.name}")

    def restart(self, name: str) -> bool:
        """Restart a trigger by name. Returns True if successful."""
        trigger = self.get_trigger(name)
        if trigger is None:
            self._logger.error(f"Trigger not found for restart: {name}")
            return False

        if trigger._restart_count >= self.max_restart_attempts:
            trigger._state = TriggerState.FAILED
            self._logger.critical(
                f"Trigger {name} failed after {self.max_restart_attempts} restarts"
            )
            return False

        try:
            trigger._restart()
            self._logger.info(
                f"Trigger restarted: {name} "
                f"restart_count={trigger._restart_count}"
            )
            return True
        except Exception as exc:
            self._logger.error(f"Restart failed: {name} — {exc}")
            return False

    # ── Health Monitoring ─────────────────────────────────────────────────────

    def _health_loop(self) -> None:
        """Background thread — checks all triggers every HEALTH_CHECK_INTERVAL_S."""
        while self._running:
            time.sleep(self.health_check_interval_s)
            with self._lock:
                triggers_snapshot = list(self._triggers)

            for trigger in triggers_snapshot:
                try:
                    healthy = trigger.health_check()
                except Exception as exc:
                    healthy = False
                    trigger.on_error(exc)

                if not healthy:
                    self._logger.warning(
                        f"Trigger unhealthy: {trigger.name} — restarting"
                    )
                    success = self.restart(trigger.name)
                    if not success:
                        self._logger.critical(
                            f"Trigger {trigger.name} could not be restarted"
                        )

    def get_status(self) -> dict:
        """Return status of all triggers."""
        with self._lock:
            return {
                trigger.name: {
                    "state": trigger._state.value,
                    "is_running": trigger._is_running,
                    "events_emitted": trigger._events_emitted,
                    "restart_count": trigger._restart_count,
                }
                for trigger in self._triggers
            }

    def is_health_loop_alive(self) -> bool:
        """Check if health loop thread is still running."""
        if self._health_loop_thread is None:
            return False
        return self._health_loop_thread.is_alive()
