import logging
import time
from typing import Any

from core.exceptions import PlatformError
from queue.job_queue import JobQueue, QueueClosedError
from queue.retry_queue import RetryQueue
from events.job_events import JobEvent
from events.event_factory import EventFactory
from events.job_events import FileEvent, ScheduledEvent


class QueueNotFoundError(PlatformError):
    """Raised when queue not found in registry"""

class QueueAlreadyRegisteredError(PlatformError):
    """Raised when queue name already registered"""


class QueueManager:

    def __init__(self):
        self._queues: dict[str, JobQueue] = {}
        self._retry_queue: RetryQueue | None = None
        # TODO: replace with setup_logger() when bootstrap is ready
        self._logger = logging.getLogger("queue_manager")

    def register(self, name: str, queue: Any) -> None:
        if name in self._queues:
            raise QueueAlreadyRegisteredError(
                f"Queue {name} already registered",
                details={"name": name}
            )
        self._queues[name] = queue
        if isinstance(queue, RetryQueue):
            self._retry_queue = queue
        self._logger.info(f"Queue registered: {name}")

    def get_queue(self, name: str) -> JobQueue:
        if name not in self._queues:
            raise QueueNotFoundError(
                f"Queue not registered: {name}",
                details={"name": name, "registered": list(self._queues.keys())}
            )
        return self._queues[name]

    def total_depth(self) -> int:
        # sum of size() across all queues
        return sum(q.size() for q in self._queues.values())

    def metrics_snapshot(self) -> dict:
        # return dict of {queue_name: {"depth": size, "is_full": bool, "is_empty": bool}}
        # acquire locks in alphabetical order to prevent deadlock
        snapshot = {}
        for name in sorted(self._queues.keys()):  # alphabetical order — prevents deadlock
            q = self._queues[name]
            snapshot[name] = {
                "depth": q.size(),
                "is_full": q.is_full(),
                "is_empty": q.is_empty()
            }
        return snapshot

    def health_check(self) -> bool:
        # check retry_queue thread is alive
        # return False if dead
        # return True if all healthy
        if self._retry_queue and not self._retry_queue.is_alive():
            self._logger.critical("Retry thread is dead")
            return False
        return True

    def drain_all(self, timeout_s: float = 30) -> dict:
        results = {}
        order = ["raw_event_queue", "ingestion_queue", 
             "retry_queue", "scheduled_queue", "dead_letter_queue"]
    
        for name in order:
            if name in self._queues:
                queue = self._queues[name]
                if hasattr(queue, 'drain'):
                    remaining = queue.drain(timeout_s=timeout_s)
                    results[name] = len(remaining)
                    self._logger.info(f"Queue drained: {name} remaining={len(remaining)}")
                else:
                    queue.close()
                    results[name] = 0
                    self._logger.info(f"Queue closed: {name} remaining=0")
    
        return results


    def close_all(self) -> None:
        # call close() on all queues
        for name, queue in self._queues.items():
            queue.close()
            self._logger.info(f"Queue closed: {name}")

    def get_dead_letter_events(self) -> list[JobEvent]:
        dead_letter = self._queues.get("dead_letter_queue")
        if dead_letter is None:
            return []
        # drain all events
        events = dead_letter.drain(timeout_s=1)
        # put them back
        for event in events:
            dead_letter.enqueue(event)
        return events

    def resubmit(self, event_id: str) -> bool:
        events = self.get_dead_letter_events()
        for event in events:
            if event.event_id == event_id:
                if isinstance(event, FileEvent):
                    fresh_event = EventFactory.create_file_event(
                        file_path=event.file_path,
                        file_format=event.file_format,
                        file_checksum=event.file_checksum,
                        file_size_bytes=event.file_size_bytes,
                        source_label=event.source_label,
                        encoding_hint=event.encoding_hint
                    )
                elif isinstance(event, ScheduledEvent):
                    fresh_event = EventFactory.create_scheduled_event(
                        schedule_name=event.schedule_name,
                        scheduled_for=event.scheduled_for,
                        payload=event.payload,
                        miss_policy=event.miss_policy
                        )
                ingestion_queue = self.get_queue("ingestion_queue")
                ingestion_queue.enqueue(fresh_event, priority=fresh_event.priority)
                self._logger.info(f"Event resubmitted: {event_id}")
                return True
        return False

    def validate_on_startup(self) -> None:
        if "dead_letter_queue" not in self._queues:
            raise QueueNotFoundError("dead_letter_queue must be registered first")
        for name, queue in self._queues.items():
            if hasattr(queue, 'max_size') and queue.max_size <= 0:
                raise ValueError(f"Queue {name} must have finite max_size")
        self._logger.info("QueueManager startup validation passed")




