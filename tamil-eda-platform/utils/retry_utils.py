from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from core.constants import MAX_RETRIES, RETRY_BACKOFF_BASE, RETRY_BACKOFF_MAX
from core.exceptions import PlatformError


class RetryExhaustedError(PlatformError):
    """
    Raised when all retry attempts have failed.
    Catch this in the calling layer and convert to a domain error.
    Example:
        except RetryExhausted as e:
            raise BackendUnavailableError('All backends failed') from e
    """

@dataclass
class RetryContext:
    """Passed to retried functions so they can log attempt info"""
    attempt_number: int
    max_attempts: int
    last_error: Exception | None = None
    total_elapsed_s: float = 0.0

def calculate_backoff(
        attempt: int,
        base: float = RETRY_BACKOFF_BASE,
        maximum: float = RETRY_BACKOFF_MAX,
        jitter: float = 0.2,
    ) -> float:
        """
    Calculate sleep duration for a given attempt number
        Formula: min(base^attempt, maximum) * (1 ± jitter)

    Args:
        attempt:  Attempt number starting at 1
        base:     Base seconds — doubles each attempt
        maximum:  Cap — never sleep longer than this
        jitter:   Random factor ±jitter (0.2 = ±20%)

    Returns:
        Seconds to sleep before next attempt

    Examples:
        attempt=1 → ~2s (1.6 to 2.4s with 20% jitter)
        attempt=2 → ~4s (3.2 to 4.8s)
        attempt=3 → ~8s (6.4 to 9.6s)
        """
        raw = min(base ** attempt, maximum)
        jitter_range = raw * jitter
        return raw + random.uniform(-jitter_range, jitter_range)

def is_retryable(exc: Exception) -> bool:
    """Return True if this exception type warrents a retry
    Retryable:     503, 429, TimeoutError, ConnectionResetError
    Not retryable: 401, 400, 404, DiskFullError — retrying cannot fix these

    Args:
        exc: The exception to check

    Returns:
        True if the operation should be retried
    """
     # Always retru transient network issues
    if isinstance(exc, (TimeoutError, ConnectionError, ConnectionResetError)):
        return True

     #check HTTP status codes stored on the exception
    status = getattr(exc, "status_code", None)
    if status is not None:
          #retryable HTTP codes - server side issues that may resolve
        return status in {429, 500, 502, 503, 504}

     #Platform errors with explicit retry flag
    retry_flag = getattr(exc, "retryable", None)
    if retry_flag is not None:
        return bool(retry_flag)

     #Default - do not retry unknown exceptions
    return False

def retry_with_backoff(
        fn:Callable[...,Any],
        *args: Any,
        max_attempts: int = MAX_RETRIES,
        base: float = RETRY_BACKOFF_BASE,
        maximum: float = RETRY_BACKOFF_MAX,
        jitter: float = 0.2,
        retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
        **kwargs: Any,
        ) -> Any:
    """Retry a function with exponential backoff and jitter.

    Args:
        fn:                   Function to retry
        *args:                Positional arguments for fn
        max_attempts:         Maximum number of attempts
        base:                 Backoff base seconds
        maximum:              Maximum backoff seconds
        jitter:               Random jitter factor
        retryable_exceptions: Only retry these exception types
        **kwargs:             Keyword arguments for fn

    Returns:
        Return value of fn on success

    Raises:
        RetryExhausted: When all attempts fail

    Example:
        result = retry_with_backoff(
            translate_text,
            text="Hello",
            max_attempts=3,
            retryable_exceptions=(APIConnectorError, TimeoutError),
        )
    """
    start = time.monotonic()
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        ctx = RetryContext(
              attempt_number=attempt,
              max_attempts=max_attempts,
              last_error=last_error,
              total_elapsed_s=time.monotonic() - start,
         )
        try:
            return fn(*args, context=ctx, **kwargs)
        except retryable_exceptions as exc:
            last_error = exc

            #check if this specific error is worth retrying
            if not is_retryable(exc):
                raise RetryExhaustedError(
                    f"Non-retryable error on attempt {attempt}",
                    details={
                        "function": fn.__name__,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )from exc

            # last attempt - give up
            if attempt == max_attempts:
                break

            wait = calculate_backoff(attempt, base, maximum, jitter)
            time.sleep(wait)

    raise RetryExhaustedError(
        f"All {max_attempts} attempts failed",
        details={
            "function": fn.__name__,
            "attempts": max_attempts,
            "elapsed_s": round(time.monotonic() - start, 2),
            "final_error": str(last_error),
        }
    )from last_error


def retry(
        max_attempts: int = MAX_RETRIES,
        base: float = RETRY_BACKOFF_BASE,
        maximum: float =RETRY_BACKOFF_MAX,
        jitter: float = 0.2,
        retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """Decorator version of retry_with_backoff
    Example:
        @retry(max_attempts=3, retryable_exceptions=(APIError, TimeoutError))
        def call_translate_api(text: str, context: RetryContext) -> str:
            ...
    """
    def decorator(fn: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return retry_with_backoff(
                fn,
                *args,
                max_attempts=max_attempts,
                base=base,
                maximum=maximum,
                jitter=jitter,
                retryable_exceptions=retryable_exceptions,
                **kwargs,
            )
        wrapper.__name__ = fn.__name__
        wrapper.__doc__ = fn.__doc__
        return wrapper
    return decorator





