import logging
import sys
import contextvars
import random
import json
import re
from pathlib import Path
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

# ── Context vars ──────────────────────────────────────────────────────────────
_job_id_var = contextvars.ContextVar('job_id', default=None)
_record_id_var = contextvars.ContextVar('record_id', default=None)
_stage_var = contextvars.ContextVar('stage', default='unknown')

# ── High volume stages ────────────────────────────────────────────────────────
HIGH_VOLUME_STAGES = {
    'csv_ingestor',
    'pdf_ingestor',
    'txt_ingestor',
    'preprocessor',
    'tokenizer',
    'validator'
}

STAGE_SAMPLE_RATES = {
    'csv_ingestor': 0.01,
    'pdf_ingestor': 0.01,
    'txt_ingestor': 0.01,
    'preprocessor': 0.05,
    'tokenizer':    0.05,
    'validator':    0.05,
}

# ── Default scrub patterns ────────────────────────────────────────────────────
DEFAULT_SCRUB_PATTERNS = [
    r'AIza\S+',
    r'sk-\S+',
    r'Bearer\s+\S+',
    r'password\s*[=:]\s*\S+',
    r'secret\s*[=:]\s*\S+',
]


# ── Structured JSON Formatter ─────────────────────────────────────────────────
class StructuredFormatter(logging.Formatter):
    """Formats every log line as a JSON object with fixed fields."""

    def __init__(self, fmt: str = "json", scrub_patterns: list = None):
        super().__init__()
        self.fmt = fmt
        self.scrub_patterns = scrub_patterns or DEFAULT_SCRUB_PATTERNS

    def _scrub(self, text: str) -> str:
        """Replace secret patterns with [REDACTED]."""
        for pattern in self.scrub_patterns:
            text = re.sub(pattern, '[REDACTED]', text, flags=re.IGNORECASE)
        return text

    def format(self, record: logging.LogRecord):
        # ── Sampling check for high-volume DEBUG logs ──────────────────────
        if record.levelno == logging.DEBUG:
            stage = _stage_var.get()
            if stage in HIGH_VOLUME_STAGES:
                sample_rate = STAGE_SAMPLE_RATES.get(stage, 1.0)
                if random.random() > sample_rate:
                    return None

        # ── Build message ──────────────────────────────────────────────────
        message = self._scrub(record.getMessage())

        if self.fmt == "pretty":
            return (
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} "
                f"[{record.levelname}] "
                f"[{_stage_var.get()}] "
                f"{message}"
            )

        # ── JSON format ────────────────────────────────────────────────────
        log_dict = {
            'timestamp': datetime.now(timezone.utc).isoformat() + 'Z',
            'level':     record.levelname,
            'stage':     _stage_var.get(),
            'job_id':    _job_id_var.get(),
            'record_id': _record_id_var.get(),
            'message':   message,
            'context':   getattr(record, 'context', {}),
            'trace_id':  None,
            'sampled':   record.levelno == logging.DEBUG,
        }
        return json.dumps(log_dict, ensure_ascii=False, default=str)


# ── Safe Handler ──────────────────────────────────────────────────────────────
class SafeHandler(logging.Handler):
    """Wraps any handler — exceptions never propagate to the pipeline."""

    def __init__(self, handler: logging.Handler):
        super().__init__()
        self.handler = handler

    def emit(self, record: logging.LogRecord):
        try:
            self.handler.emit(record)
        except Exception:
            self.handleError(record)

    def handleError(self, record: logging.LogRecord):
        try:
            sys.stderr.write(f"LOG HANDLER ERROR: {record.getMessage()}\n")
        except Exception:
            pass


# ── Setup Logger ──────────────────────────────────────────────────────────────
def setup_logger(
        module_name: str,
        level: str,
        fmt: str,
        console_enabled: bool,
        console_level: str,
        file_enabled: bool,
        file_path: str,
        file_max_bytes: int,
        file_backup_count: int,
        audit_enabled: bool,
        audit_path: str,
        audit_max_bytes: int,
        audit_backup_count: int,
        component_levels: dict,
        audit_components: list,
        root_level: str,
        scrub_patterns: list = None,
) -> logging.Logger:
    """
    Create and configure a structured logger for a platform module.
    All values come from the logging yaml via bootstrap.py — no hardcoded defaults.
    """
    logger = logging.getLogger(module_name)
    effective_level = component_levels.get(module_name, root_level)
    logger.setLevel(getattr(logging, effective_level.upper(), logging.INFO))

    if module_name in audit_components:
        audit_enabled = True

    # Guard against duplicate handlers
    if logger.handlers:
        return logger

    formatter = StructuredFormatter(fmt=fmt, scrub_patterns=scrub_patterns)

    # ── Console handler ────────────────────────────────────────────────────
    if console_enabled:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(getattr(logging, console_level.upper(), logging.INFO))
        stream_handler.setFormatter(formatter)
        logger.addHandler(SafeHandler(stream_handler))

    # ── File handler ───────────────────────────────────────────────────────
    if file_enabled:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            file_path,
            when='midnight',
            backupCount=file_backup_count
        )
        file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
        file_handler.setFormatter(formatter)
        logger.addHandler(SafeHandler(file_handler))

    # ── Audit handler ──────────────────────────────────────────────────────
    if audit_enabled:
        Path(audit_path).parent.mkdir(parents=True, exist_ok=True)
        audit_handler = RotatingFileHandler(
            audit_path,
            maxBytes=audit_max_bytes,
            backupCount=audit_backup_count
        )
        audit_handler.setLevel(logging.INFO)
        audit_handler.setFormatter(formatter)
        logger.addHandler(SafeHandler(audit_handler))

    return logger


# ── Set Context ───────────────────────────────────────────────────────────────
def set_context(job_id=None, record_id=None, stage=None):
    """Set context vars for the current async task or thread."""
    if job_id is not None:
        _job_id_var.set(job_id)
    if record_id is not None:
        _record_id_var.set(record_id)
    if stage is not None:
        _stage_var.set(stage)
