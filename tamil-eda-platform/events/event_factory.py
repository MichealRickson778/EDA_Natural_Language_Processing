from __future__ import annotations
from pathlib import Path
from datetime import datetime,timezone
from uuid import uuid4

from core.exceptions import EventCreationError
from events.job_events import JobEvent, FileEvent, ScheduledEvent
from core.enums import QueuePriority, EventType, FileFormat, MissPolicy

from core.constants import MAX_RETRIES
from pydantic import ValidationError as PydanticValidationError

class RetryLimitExceededError(EventCreationError):
    """Raise when the Retry exceed """


class EventFactory():
    """Create e event for the whole system"""

    @staticmethod
    def create_file_event(
        file_path: Path,
        file_format: FileFormat,
        file_checksum: str,
        file_size_bytes: int,
        source_label: str | None = None,
        encoding_hint: str | None = None
    ) -> FileEvent:
        try:
            return FileEvent(
                file_path=file_path,        # from parameter
                file_format=file_format,    # from parameter
                file_checksum=file_checksum, # from parameter
                file_size_bytes=file_size_bytes, # from parameter
                source_label=source_label,  # from parameter
                encoding_hint=encoding_hint, # from parameter
                priority=EventFactory._compute_priority(file_size_bytes),  # computed
                event_type=EventType.FILE,  # fixed
            # priority comes from _compute_priority()
            # event_type, job_id, event_id etc are auto-generated
            )
        except PydanticValidationError as e:
            raise EventCreationError(
                "Failed to create FileEvent",
                details={"errors": e.errors()}
            ) from e


    @staticmethod
    def create_scheduled_event(
            schedule_name: str,
            scheduled_for: datetime,
            payload: dict,
            miss_policy: MissPolicy = MissPolicy.SKIP
            ) -> ScheduledEvent:
        try:
            return ScheduledEvent(
            schedule_name = schedule_name,
            scheduled_for = scheduled_for,
            payload = payload,
            miss_policy = miss_policy,
            event_type=EventType.SCHEDULED,
            priority=QueuePriority.NORMAL,
            # priority comes from _compute_priority()
            # event_type, job_id, event_id etc are auto-generated
            )
        except PydanticValidationError as e:
            raise EventCreationError(
                "Failed to create ScheduleEvent",
                details={"errors": e.errors()}
            ) from e


    @staticmethod
    def create_retry_event(
        original_event: JobEvent
    ) -> JobEvent:
        try:
            if original_event.retry_count >= MAX_RETRIES:
                raise RetryLimitExceededError(
                    f"Job {original_event.job_id} has reached MAX_RETRIES ({MAX_RETRIES})",
                        details={"job_id": original_event.job_id, "retry_count": original_event.retry_count}
                        )
            return original_event.model_copy(
            update={
                # Generate a fresh unique event ID
                "event_id": f"evt_{uuid4().hex[:8]}",
                # Increment the count
                "retry_count": original_event.retry_count + 1,
                # Reset timestamp to now
                "emitted_at": datetime.now(timezone.utc),
                }
            )

        except RetryLimitExceededError:
            raise

        except PydanticValidationError as e:
            raise EventCreationError(
                "Failed to create ScheduleEvent",
                details={"errors": e.errors()}
            ) from e


    @staticmethod
    def _compute_priority(file_size_bytes: int) -> QueuePriority:

        if file_size_bytes > 50 * 1024 * 1024:
            return QueuePriority.LOW

        elif 1 * 1024 * 1024 <= file_size_bytes <= 50 * 1024 * 1024:
           return QueuePriority.NORMAL

        else:
            return QueuePriority.HIGH








