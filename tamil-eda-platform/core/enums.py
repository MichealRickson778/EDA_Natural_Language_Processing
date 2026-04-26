from enum import Enum


class Language(str, Enum):
    """Supported languages in the platform
    Inherits str so values serialize cleanly to JSON and Parquet
    """
    TAMIL = "ta"
    ENGLISH = "en"
    UNKNOWN = "unknown"

class FileFormat(str, Enum):
    """Source file formats the ingestors can handle"""
    CSV = "csv"
    JSON = "json"
    PDF = "pdf"
    IMAGE = "image"
    TXT = "txt"
    UNKNOWN = "unknown"

class JobStatus(str, Enum):
    """Lifecycle states of a pipeline job"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD = "dead"

class QueuePriority(int, Enum):
    """Priority levels for queue ordering
    Inherits int so priority queue can compare values directly
    """
    LOW = 10
    NORMAL = 5
    HIGH = 1
    CRITICAL = 20

class ContentType(str, Enum):
    """Classification of text content"""
    SENTENCE = "sentence"
    PARAGRAPH = "paragraph"
    DOCUMENT = "document"
    UNKNOWN = "unknown"

class DataClassification(str, Enum):
    """Security classification assigned by data_classifier.py"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    PII = "pii"

class QueueName(str, Enum):
    """Named queued in the platform"""
    CSV = "csv_queue"
    JSON = "json_queue"
    PDF = "pdf_queue"
    IMAGE = "image_queue"
    TXT = "txt_queue"
    RETRY = "retry_queue"
    DEAD_LETTER = "dead_letter_queue"
    HUMAN_REVIEW = "human_review_queue"
    QUARANTINE = "quarantine_queue"

class TranslationBackend(str, Enum):
    """Translation API backermds the engine can use"""
    GOOGLE = "google"
    DEEPL = "deepl"
    NLLB = "nllp"
    MOCK = "mock"

class StageStatus(str, Enum):
    """Result of a single DAG stage execution"""
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"

class EventType(str, Enum):
    """Trigger for the job event"""
    FILE = "file"
    SCHEDULED = "scheduled"

class MissPolicy(str, Enum):
    SKIP = "skip"
    RUN_ONCE = "run_once"

class TriggerState(str, Enum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    ERROR = "error"
    RESTARTING = "restarting"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"
