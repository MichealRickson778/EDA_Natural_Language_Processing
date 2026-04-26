from core.exceptions import PlatformError
from core.enums import TriggerState
import logging
import time
import queue

from abc import ABC, abstractmethod



class TriggerError(PlatformError):
    """Raise when we have error on trigger"""

class BaseTrigger(ABC):
    """Defines the interface every trigger must implement: start, stop, health_check. 
    Provides shared state tracking, logging, and restart logic."""

    def __init__(self, name: str, queue):
        self.name = name
        self._state = TriggerState.CREATED
        self._is_running = False
        self._events_emitted = 0
        self._restart_count = 0
        self._queue = queue
        self._logger = logging.getLogger(name)

    @abstractmethod
    def start(self) -> None:
        """Subclass must implement. Begins watching for the trigger condition. 
        Should be non-blocking or run in a background thread."""

    @abstractmethod
    def stop(self) -> None:
        """Subclass must implement. Stops the trigger cleanly. 
        Finishes emitting any in-flight event before stopping."""

    @abstractmethod
    def health_check(self) -> bool:
        """Subclass must implement. Returns True if the trigger is actively watching. 
        Returns False if it has stalled or lost its watch."""

    def emit(self, event) -> None:
        """Puts a JobEvent onto the queue. Logs the emission. 
        Increments events_emitted counter. All triggers call this."""
        if not self._is_running:
            self._logger.warning(f"Late event discarded — trigger stopped: {self.name}")
            return
        self._queue.enqueue(event)
        self._events_emitted += 1
        self._logger.info("emit successfull")
    
    def _restart(self) -> None:
        """Called by trigger_manager when health_check() returns False. 
        Calls stop(), waits, calls start(). Logs the restart."""
        self._logger.warning(f"Restarting trigger: {self.name}")
        self.stop()
        time.sleep(2)
        self.start()
        self._restart_count += 1   


    def on_error(self, error) -> None:
        """Called when a trigger encounters an internal error. Logs with CRITICAL severity. 
        Fires alerting. Subclasses can override."""
        self._state = TriggerState.ERROR
        self._is_running = False
        self._logger.critical(f"Trigger error: {self.name} — {error}")
