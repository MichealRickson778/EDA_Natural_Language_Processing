import heapq
import threading
import time
import random
import logging

from queue.job_queue import JobQueue, QueueClosedError
from events.job_events import JobEvent
from events.event_factory import EventFactory
from core.exceptions import RateLimitError
from core.constants import MAX_RETRIES
from events.event_factory import RetryLimitExceededError

from core.exceptions import PlatformError

BASE_BACKOFF_S = 2.0

class RetryQueueError(PlatformError):
    """Raised when retry queue operation fails"""

class RetryQueue:

    def __init__(self, name: str, ingestion_queue: JobQueue):
        self._name = name
        self._ingestion_queue = ingestion_queue
        self._heap = []
        self._lock = threading.Lock()
        self._counter = 0
        self._closed = False
        # TODO: replace with setup_logger() when bootstrap is ready
        self._logger = logging.getLogger("retry_queue")
        self._retry_thread = threading.Thread(
            target=self._retry_loop, daemon=True
        )
        self._retry_thread.start()

    def schedule_retry(self, event: JobEvent, error: Exception) -> None:

        if event.retry_count >= MAX_RETRIES:
            raise RetryLimitExceededError(
                f"Job {event.job_id} reached MAX_RETRIES ({MAX_RETRIES})",
                details={"job_id": event.job_id, "retry_count": event.retry_count}
            )
    
        retry_event = EventFactory.create_retry_event(event)
        backoff_s = self.calculate_backoff(event.retry_count, error)
        re_queue_at = time.time() + backoff_s
    
        with self._lock:
            self._counter += 1
            heapq.heappush(self._heap, (re_queue_at, self._counter, retry_event))
    
        self._logger.info(
            f"Retry scheduled: job={event.job_id} "
            f"retry_count={retry_event.retry_count} backoff={backoff_s:.2f}s"
        )

    def calculate_backoff(self, retry_count: int, error: Exception) -> float:
        # if RateLimitError — use error.retry_after_s
        # otherwise — BASE_BACKOFF_S * (2 ** retry_count) * (0.8 + random.random() * 0.4)
        if isinstance(error, RateLimitError) and hasattr(error, 'retry_after_s'):
            return error.retry_after_s
        return BASE_BACKOFF_S * (2 ** retry_count) * (0.8 + random.random() * 0.4)

    def _retry_loop(self) -> None:
        """Background thread — checks heap every poll_interval and re-enqueues expired events"""
        while not self._closed:
            time.sleep(0.1)
            now = time.time()
            events_to_retry = []
            with self._lock:
                while self._heap and self._heap[0][0] <= now:
                    re_queue_at, seq, event = heapq.heappop(self._heap)
                    events_to_retry.append(event)
            for event in events_to_retry:
                self._requeue(event)

    def _requeue(self, event: JobEvent) -> None:
        """Move event back to ingestion_queue"""
        try:
            # enqueue to ingestion_queue with event priority
            self._ingestion_queue.enqueue(event, priority=event.priority)
            self._logger.info(f"Event re-queued: job={event.job_id}")
        except QueueClosedError:
            self._logger.warning(
            f"Enqueue after close — discarding event job={event.job_id}"
            )
        except Exception as exc:
            raise RetryQueueError(
                f"Failed to re-queue event job={event.job_id}",
                details={"job_id": event.job_id}
            ) from exc

    def size(self) -> int:
        with self._lock:
            return len(self._heap)

    def is_alive(self) -> bool:
        return self._retry_thread.is_alive()

    def close(self) -> None:
        self._closed = True
