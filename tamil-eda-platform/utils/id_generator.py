from __future__ import annotations

import secrets
import uuid

from core.exceptions import PlatformError


class IDGenerationError(PlatformError):
    """Raised when an ID connot be generated"""

def generate_job_id() -> str:
    """Generate a unique job id
    Format: 'job-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
    Used by: event_factory, dispatcher, lineage_tracker, API
    """
    return f"job-{uuid.uuid4()}"

def generate_event_id() -> str:
    """Generate a unique event ID
    Format: 'evt-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
    Used by: event_factory — each JobEvent needs its own ID
    """
    return f"evt-{uuid.uuid4()}"

def generate_trace_id() -> str:
    """Genearte an OpenTelemetry-compliant trace ID
    Format: 32 lowercase hex characters, no hyphens
    Spec: W3C trace-context — https://www.w3.org/TR/trace-context/
    Used by: observability/tracer.py
    """
    return secrets.token_hex(16)

def generate_span_id() -> str:
    """Generate an OpenTelemetry-compliant span ID
    Format: 16 lowercase hex characters, no hyphens
    Used by: observability/tracer.py — each span inside a trace
    """
    return secrets.token_hex(8)

def generate_lineage_id() -> str:
    """Generate a unique lineage event ID
    Format: 'lin-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
    Used by: lineage/lineage_tracker.py
    """
    return f"lin-{uuid.uuid4()}"


def generate_request_id() -> str:
    """Generate a unique API request ID
    Format: 'req-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
    Used by: api/routes.py — returned in X-Request-ID response header
    """
    return f"req-{uuid.uuid4()}"

def generate_version_id(current: str) -> str:
    """Generate the next dataset version ID.
    Increments the minor version number.
    Examples:
        'v1.0' → 'v1.1'
        'v1.9' → 'v1.10'
        'v2.3' → 'v2.4'

    Args:
        current: Current version string in format 'vMAJOR.MINOR'

    Returns:
        Next version string

    Raises:
        IDGenerationError: If current version format is invalid
        """
    if not current.startswith("v") or "." not in current:
        raise IDGenerationError(
            f"Invalid version format: '{current}'",
            details={"current": current, "expected": "vMAJOR.MINOR"},
        )
    parts = current[1:].split(".")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        raise IDGenerationError(
            f"Invalid version format: '{current}'",
            details={"current": current, "expected": "vMAJOR.MINOR"},
        )
    major = int(parts[0])
    minor = int(parts[1])
    return f"v{major}.{minor + 1}"
