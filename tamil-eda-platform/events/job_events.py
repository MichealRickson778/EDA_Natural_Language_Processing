import secrets
from uuid import uuid4

from pydantic import BaseModel, Field, ConfigDict, model_validator, field_validator
from datetime import datetime, timezone, timedelta
from pathlib import Path

from core.enums import EventType, QueuePriority, FileFormat, MissPolicy
from core.constants import CURRENT_SCHEMA_VERSION, MAX_RETRIES



class JobEvent(BaseModel):
    """Fields common for all the jobs and can't be modified """

    model_config = ConfigDict(frozen=True, extra='ignore')
    job_id: str = Field(default_factory=lambda: f"job-{uuid4()}")
    event_id: str = Field(default_factory=lambda: f"evt-{uuid4().hex[:8]}")
    trace_id: str = Field(default_factory=lambda: secrets.token_hex(16))
    event_type: EventType
    priority: QueuePriority
    emitted_at: datetime = Field(default_factory=lambda:datetime.now(timezone.utc))
    schema_version: str = CURRENT_SCHEMA_VERSION
    retry_count: int = 0

    @field_validator('emitted_at')
    @classmethod
    def emitted_at_must_be_utc(cls, v):
        # 1. Check if timezone information exists (is not "naive")
        if v.tzinfo is None or v.utcoffset() != timedelta(0):
            raise ValueError(f"Datetime {v} must be in UTC (offset +00:00).")


        # 2. Check if the offset is exactly zero (UTC/GMT)
        if v > datetime.now(timezone.utc) + timedelta(seconds=5):
            raise ValueError(f"emitted_at cannot be in the future, got {v}")
        return v


    @field_validator('retry_count')
    @classmethod
    def must_be_valid_retry(cls, v):
        # Check 0 <= v <= MAX_RETRIES + 1
        limit = MAX_RETRIES + 1
        if not (0 <= v <= limit):
            raise ValueError(f"attempt must be between 0 and {limit}, got {v}")
        return v

class FileEvent(JobEvent): 
    # extra fields specific to file-triggered jobs
    event_type: EventType = EventType.FILE
    file_path: Path
    file_format: FileFormat
    file_checksum: str
    file_size_bytes: int
    source_label: str | None = None
    encoding_hint: str | None = None

    @field_validator('file_path', mode='before')
    @classmethod
    def coerce_and_warn_path(cls, v):
        if isinstance(v, str):
            import logging
            logging.getLogger("events").warning(
                f"file_path passed as str, expected Path — coercing: {v}"
            )
            return Path(v)
        return v

    @field_validator('file_path')
    @classmethod
    def must_be_absolute(cls, v: Path) -> Path:
        if not v.is_absolute():
            raise ValueError(f"file_path must be absolute, got {v}")
        return v

    @field_validator('file_size_bytes')
    @classmethod
    def must_be_positive(cls, v):
        if not v > 0:
            raise ValueError(f"file_size_bytes must be > 0, got {v}")
        return v

class ScheduledEvent(JobEvent):
    # extra fields specific to scheduled jobs
    event_type: EventType = EventType.SCHEDULED
    schedule_name: str
    scheduled_for: datetime = Field(default_factory=lambda:datetime.now(timezone.utc))
    lag_s: float = 0.0
    payload: dict
    miss_policy: MissPolicy = MissPolicy.SKIP

    @model_validator(mode='after')
    def compute_lag(self):
        object.__setattr__(self, 'lag_s', (self.emitted_at - self.scheduled_for).total_seconds())
        return self

    @field_validator('emitted_at')
    @classmethod
    def scheduled_for_must_be_utc(cls, v):
        # 1. Check if timezone information exists (is not "naive")
        if v.tzinfo is None:
            raise ValueError(f"Datetime {v} must be timezone-aware (contain tzinfo).")

        # 2. Check if the offset is exactly zero (UTC/GMT)
        if v.utcoffset() != timedelta(0):
            raise ValueError(f"Datetime {v} must be in UTC (offset +00:00).")

        return v

