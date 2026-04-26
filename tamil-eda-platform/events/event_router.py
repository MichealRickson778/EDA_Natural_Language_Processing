from __future__ import annotations
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from typing import Any
import logging

from core.exceptions import PlatformError
from core.enums import FileFormat, EventType
from core.constants import SUPPORTED_SCHEMA_VERSIONS
from events.job_events import JobEvent, FileEvent, ScheduledEvent


class EventRouterError(PlatformError):
    """Raised when routing fails"""


@dataclass
class HandlerInfo:
    """Stores queue name and handler class for a registered route."""
    queue_name: str
    handler: type


class EventRouter:
    """
    Routes JobEvents to the correct processing queue.
    All routing runs concurrently via ThreadPoolExecutor.
    """

    def __init__(self, max_workers: int = 4):
        self._format_handlers: dict[FileFormat, HandlerInfo] = {}
        self._schedule_handlers: dict[str, HandlerInfo] = {}
        self._queues: dict[str, Any] = {}
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._logger = logging.getLogger("event_router")

    # ── Registration ──────────────────────────────────────────────────────────

    def register_queue(self, name: str, queue: Any) -> None:
        """Register a queue by name. Called by bootstrap.py."""
        self._queues[name] = queue
        self._logger.info(f"Queue registered: {name}")

    def register_format_handler(
            self,
            file_format: FileFormat,
            queue_name: str,
            handler: type
    ) -> None:
        """Register a handler for a file format. Called by bootstrap.py."""
        self._format_handlers[file_format] = HandlerInfo(
            queue_name=queue_name,
            handler=handler
        )
        self._logger.info(f"Format handler registered: {file_format} → {queue_name}")

    def register_schedule_handler(
            self,
            schedule_name: str,
            queue_name: str,
            handler: type
    ) -> None:
        """Register a handler for a schedule name. Called by bootstrap.py."""
        self._schedule_handlers[schedule_name] = HandlerInfo(
            queue_name=queue_name,
            handler=handler
        )
        self._logger.info(f"Schedule handler registered: {schedule_name} → {queue_name}")

    # ── Routing ───────────────────────────────────────────────────────────────

    def route(self, event: JobEvent) -> None:
        """Submit event routing to the thread pool."""
        self._executor.submit(self._route, event)

    def _route(self, event: JobEvent) -> None:
        """Internal routing logic — runs in thread pool."""
        try:
            # Step 1 — check schema version
            if event.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
                self._logger.warning(
                    f"Unsupported schema version: {event.schema_version} "
                    f"for job {event.job_id} — routing to migration"
                )
                self._route_to_migration(event)
                return

            # Step 2 — route by event type
            if event.event_type == EventType.FILE:
                self._route_file_event(event)
            elif event.event_type == EventType.SCHEDULED:
                self._route_scheduled_event(event)
            else:
                self._route_to_dead_letter(event, reason="unknown_event_type")

        except Exception as e:
            self._logger.error(
                f"Routing failed for job {event.job_id}: {e}"
            )
            self._route_to_dead_letter(event, reason="routing_exception")

    def _route_file_event(self, event: FileEvent) -> None:
        """Route a FileEvent to the correct ingestion queue."""
        handler_info = self._format_handlers.get(event.file_format)

        if handler_info is None:
            self._logger.warning(
                f"No handler for format {event.file_format} "
                f"— dead-lettering job {event.job_id}"
            )
            self._route_to_dead_letter(event, reason="no_handler_for_format")
            return

        queue = self._queues.get(handler_info.queue_name)
        if queue is None:
            self._route_to_dead_letter(event, reason="queue_not_found")
            return

        queue.enqueue(event, priority=event.priority.value)
        self._logger.info(
            f"FileEvent routed: job={event.job_id} "
            f"format={event.file_format} → {handler_info.queue_name}"
        )

    def _route_scheduled_event(self, event: ScheduledEvent) -> None:
        """Route a ScheduledEvent to the correct scheduled queue."""
        handler_info = self._schedule_handlers.get(event.schedule_name)

        if handler_info is None:
            self._logger.warning(
                f"No handler for schedule {event.schedule_name} "
                f"— dead-lettering job {event.job_id}"
            )
            self._route_to_dead_letter(event, reason="no_handler_for_schedule")
            return

        queue = self._queues.get(handler_info.queue_name)
        if queue is None:
            self._route_to_dead_letter(event, reason="queue_not_found")
            return

        queue.enqueue(event, priority=event.priority.value)
        self._logger.info(
            f"ScheduledEvent routed: job={event.job_id} "
            f"schedule={event.schedule_name} → {handler_info.queue_name}"
        )

    def _route_to_dead_letter(self, event: JobEvent, reason: str) -> None:
        """Route an unroutable event to the dead letter queue."""
        queue = self._queues.get("dead_letter_queue")

        if queue is None:
            # This is the worst case — dead letter queue missing
            raise EventRouterError(
                f"dead_letter_queue not registered — event {event.job_id} lost",
                details={"job_id": event.job_id, "reason": reason}
            )

        queue.enqueue(event, priority=event.priority.value)
        self._logger.warning(
            f"Event dead-lettered: job={event.job_id} reason={reason}"
        )

    def _route_to_migration(self, event: JobEvent) -> None:
        """Route an old-schema event to the migration queue."""
        queue = self._queues.get("migration_queue")

        if queue is None:
            # No migration queue — fall back to dead letter
            self._logger.error(
                f"migration_queue not registered — routing to dead_letter instead"
            )
            self._route_to_dead_letter(event, reason="migration_queue_not_found")
            return

        queue.enqueue(event, priority=event.priority.value)
        self._logger.info(
            f"Event routed to migration: job={event.job_id} "
            f"schema_version={event.schema_version}"
        )

    # ── Startup Validation ────────────────────────────────────────────────────

    def validate_on_startup(self) -> None:
        """
        Called by bootstrap.py after all registrations.
        Ensures dead_letter_queue exists and warns about unregistered formats.
        """
        # dead_letter_queue must always exist
        if "dead_letter_queue" not in self._queues:
            raise EventRouterError(
                "dead_letter_queue must be registered before any format handlers"
            )

        # warn about FileFormat values with no registered handler
        for file_format in FileFormat:
            if file_format not in self._format_handlers:
                self._logger.warning(
                    f"No handler registered for FileFormat.{file_format.name} "
                    f"— events with this format will be dead-lettered"
                )

        self._logger.info("EventRouter startup validation passed")
