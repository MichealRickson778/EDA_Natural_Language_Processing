
from datetime import datetime, timezone
import queue
import time

from core.exceptions import QueueError
from events.job_events import JobEvent

class QueueFullError(QueueError):
    """raise when job queue is full """

class QueueEmptyError(QueueError):
    """raise when queue is empty"""

class QueueClosedError(QueueError):
    """raise when queue is closed"""

class JobQueue():

    def __init__(self,
                name: str,
                backend: str,
                max_size: int,
                poll_interval_s: int,
                drain_timeout_s: int):
        self.backend = backend
        self.max_size = max_size
        self.poll_interval_s = poll_interval_s
        self.drain_timeout_s = drain_timeout_s
        self._closed = False
        self._queue = queue.Queue(maxsize=max_size)
        self._name = name
    
    def enqueue(self, event, timeout = None) -> None:
        """Adds event to the tail. If queue is full and timeout is set: blocks until space is available or timeout expires, then raises QueueFullError. 
        If timeout=None: blocks indefinitely."""
        
        if self._closed is True:
            raise QueueClosedError(f"Queue is closed, closed flag set to {self._closed}")

        try:
            self._queue.put(event, block=True, timeout=timeout)
        except queue.Full:
            raise QueueFullError(
                f"queue {self._name} is full",
                details={"queue":self._name, "max_size": self.max_size}
            )

    def dequeue(self, timeout = None) -> JobEvent:
        """Removes and returns event from the head. 
        If queue is empty and timeout is set: blocks until an event arrives or timeout expires, 
        then raises QueueEmptyError."""

        if self._closed is True:
            raise QueueClosedError(f"Queue is closed, closed flag set to {self._closed}")

        try:
            event = self._queue.get(block=True, timeout=timeout)
            return event
        except queue.Empty:
            raise QueueEmptyError(
                f"queue {self._name} is empty",
                details={"queue":self._name}
            )
        
    def peek(self) -> JobEvent|None:
        """Returns next event without removing it. 
        Returns None if empty. Used by monitoring."""
        try:
            return self._queue.queue[0]
        except IndexError:
            return None

    def size(self) -> int:
        """Current number of events in the queue. Thread-safe snapshot. 
        May be stale by the time the caller uses it."""
        return self._queue.qsize()
    
    def is_empty(self) -> bool:
        """True if size() == 0. Convenience wrapper."""
        if self.size() == 0:
            return True
        return False
    
    def is_full(self) -> bool:
        """True if size() >= max_size. 
        Callers check this before enqueue to decide whether to wait.

        WARNING — TOCTOU race: never use is_full() + enqueue() as a pair.
        The state can change between the two calls.
        Instead use: enqueue(event, timeout=0) and catch QueueFullError.
        """
        if self.size() >= self.max_size:
            return True
        return False

    def drain(self, timeout_s: float = None) -> list[JobEvent]:
        """Returns all remaining events and empties the queue. Called during shutdown. 
        timeout_s is the maximum wait for in-flight dequeues to complete."""

        self._closed = True
        events = []
        deadline = time.time() + (timeout_s or self.drain_timeout_s)

        while time.time() < deadline:
            try:
                event = self.dequeue(timeout=0.1)
                events.append(event)
            except QueueEmptyError:
                break

        return events

    def close(self) -> None:
        self._closed = True



