import heapq
import threading
import time
from queue.job_queue import JobQueue, QueueEmptyError, QueueFullError, QueueClosedError
from core.enums import QueuePriority
from events.job_events import JobEvent

AGING_INTERVAL_S = 60
AGE_THRESHOLD_S = 120

class PriorityQueue(JobQueue):

    def __init__(self, name, backend, max_size, poll_interval_s, drain_timeout_s):
        super().__init__(name, backend, max_size, poll_interval_s, drain_timeout_s)
        self._heap = []
        self._lock = threading.Lock()
        self._counter = 0
        self._aging_thread = threading.Thread(target=self._aging_loop, daemon=True)
        self._aging_thread.start()

    def enqueue(self, event, priority=QueuePriority.NORMAL, timeout=None) -> None:
        if self._closed:
            raise QueueClosedError(f"Queue {self._name} is closed")
        if self.size() >= self.max_size:
            raise QueueFullError(f"Queue {self._name} is full",
                                details={"max_size": self.max_size})
        with self._lock:
            self._counter += 1
            # push 3-tuple to heap: (priority_value, sequence, event)
            heapq.heappush(self._heap, (priority.value, self._counter, time.time(), event))

    def dequeue(self, timeout=None) -> JobEvent:
        if self._closed:
            raise QueueClosedError(f"Queue {self._name} is closed")
        deadline = time.time() + (timeout or self.poll_interval_s)
        while time.time() < deadline:
            with self._lock:
                if self._heap:
                    # pop from heap — returns (priority_value, sequence, event)
                    # return just the event
                    priority_val, seq, entry_time, event = heapq.heappop(self._heap)
                    return event
            time.sleep(0.01)
        raise QueueEmptyError(f"Queue {self._name} is empty",
                             details={"queue": self._name})

    def size(self) -> int:
        with self._lock:
            return len(self._heap)

    def peek(self) -> JobEvent | None:
        with self._lock:
            if self._heap:
                # return event from top of heap without removing
                # heap[0] is the tuple — return the event part
                return self._heap[0][3]
            return None

    def _aging_loop(self) -> None:
        """Background thread — bumps priority of old LOW events"""
        while not self._closed:
            time.sleep(AGING_INTERVAL_S)
            now = time.time()
            with self._lock:
                new_heap = []
                for priority_val, seq, entry_time, event in self._heap:
                    # check if event has been waiting > AGE_THRESHOLD_S
                    # if yes — bump priority by reducing priority_val by 4
                    # add to new_heap
                    age = now - entry_time
                    if age > AGE_THRESHOLD_S:
                        priority_val = max(1, priority_val - 4)  # bump up, min is 1 (HIGH)
                    heapq.heappush(new_heap, (priority_val, seq, entry_time, event))
                self._heap = new_heap

    def drain(self, timeout_s=None) -> list[JobEvent]:
        self._closed = True
        events = []
        deadline = time.time() + (timeout_s or self.drain_timeout_s)
        while time.time() < deadline:
            with self._lock:
                if not self._heap:
                    break
                # pop all remaining events and append to events list
                priority_val, seq, entry_time, event = heapq.heappop(self._heap)
                events.append(event)
        return events
