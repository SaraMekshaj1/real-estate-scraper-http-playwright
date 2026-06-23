from __future__ import annotations
import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Type

logger = logging.getLogger(__name__)

# ── Exceptions ────────────────────────────────────────────────────────────────
class RetryExhaustedError(RuntimeError):
    """All retry attempts were consumed without success."""

class CircuitOpenError(RuntimeError):
    """Request blocked — circuit breaker is in OPEN state."""


# ── Retry-After parsing ───────────────────────────────────────────────────────

def parse_retry_after(header_value: str | None) -> float | None:
    """Parse ``Retry-After`` header.  Returns seconds to wait, or None."""
    if not header_value:
        return None
    try:
        return float(header_value)
    except ValueError:
        # RFC 7231 HTTP-date format
        import email.utils
        try:
            ts = email.utils.parsedate_to_datetime(header_value).timestamp()
            return max(0.0, ts - time.time())
        except Exception:
            return None


# ── Retry policy ─────────────────────────────────────────────────────────────

@dataclass
class RetryPolicy:
    max_attempts: int = 4
    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter: bool = True
    retryable_exc: tuple[Type[Exception], ...] = field(
        default_factory=lambda: (
            ConnectionError,
            TimeoutError,
            OSError,
        )
    )
    retryable_status: frozenset[int] = field(
        default_factory=lambda: frozenset({429, 500, 502, 503, 504})
    )
    non_retryable_status: frozenset[int] = field(
        default_factory=lambda: frozenset({400, 401, 403, 404, 405, 410})
    )

    def compute_delay(self, attempt: int, retry_after: float | None = None) -> float:
        """Return how many seconds to wait before *attempt* (0-indexed)."""
        if retry_after is not None:
            return retry_after

        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        if self.jitter:
            delay = random.uniform(0, delay)
        return delay

    def should_retry_status(self, status: int) -> bool:
        if status in self.non_retryable_status:
            return False
        return status in self.retryable_status

    def should_retry_exc(self, exc: Exception) -> bool:
        return isinstance(exc, self.retryable_exc)


# ── Circuit breaker ───────────────────────────────────────────────────────────

class CircuitState(Enum):
    CLOSED   = auto()   # Normal operation
    OPEN     = auto()   # Blocking requests after too many failures
    HALF_OPEN = auto()  # Probing — one test request allowed


class CircuitBreaker:
    """
    Classic three-state circuit breaker.

    WHY: Without a circuit breaker, a downed target site causes every
         worker to exhaust all retries on every URL, burning through
         rate-limit budget and causing workers to pile up.  The breaker
         detects the pattern early and trips, preserving resources.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_secs: float = 30.0,
        name: str = "default",
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_secs     = recovery_secs
        self.name              = name

        self._state            = CircuitState.CLOSED
        self._failure_count    = 0
        self._last_failure_ts  = 0.0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._last_failure_ts >= self.recovery_secs:
                logger.info("[CircuitBreaker:%s] OPEN → HALF_OPEN (probing)", self.name)
                self._state = CircuitState.HALF_OPEN
        return self._state

    def record_success(self) -> None:
        self._failure_count = 0
        if self._state != CircuitState.CLOSED:
            logger.info("[CircuitBreaker:%s] → CLOSED", self.name)
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_ts = time.monotonic()
        if self._failure_count >= self.failure_threshold:
            if self._state != CircuitState.OPEN:
                logger.warning(
                    "[CircuitBreaker:%s] → OPEN after %d failures",
                    self.name, self._failure_count,
                )
            self._state = CircuitState.OPEN

    def allow_request(self) -> bool:
        s = self.state
        if s == CircuitState.CLOSED:
            return True
        if s == CircuitState.HALF_OPEN:
            return True  # let one probe through
        return False  # OPEN


# ── Sync retry decorator ──────────────────────────────────────────────────────

def with_retry(
    policy: RetryPolicy,
    circuit_breaker: CircuitBreaker | None = None,
) -> Callable:
    """Decorator factory that wraps a sync callable with retry + CB logic."""

    def decorator(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            if circuit_breaker and not circuit_breaker.allow_request():
                raise CircuitOpenError(
                    f"Circuit breaker '{circuit_breaker.name}' is OPEN"
                )

            last_exc: Exception | None = None
            for attempt in range(policy.max_attempts):
                try:
                    result = fn(*args, **kwargs)
                    if circuit_breaker:
                        circuit_breaker.record_success()
                    return result
                except Exception as exc:
                    last_exc = exc
                    retry_after = getattr(exc, "retry_after", None)

                    if not policy.should_retry_exc(exc):
                        if circuit_breaker:
                            circuit_breaker.record_failure()
                        raise

                    if attempt < policy.max_attempts - 1:
                        delay = policy.compute_delay(attempt, retry_after)
                        logger.warning(
                            "Attempt %d/%d failed for %s — retrying in %.2fs | %s",
                            attempt + 1, policy.max_attempts,
                            fn.__name__, delay, exc,
                        )
                        time.sleep(delay)
                    else:
                        if circuit_breaker:
                            circuit_breaker.record_failure()

            raise RetryExhaustedError(
                f"All {policy.max_attempts} attempts failed"
            ) from last_exc

        return wrapper
    return decorator


# ── Async retry decorator ─────────────────────────────────────────────────────

def with_async_retry(
    policy: RetryPolicy,
    circuit_breaker: CircuitBreaker | None = None,
) -> Callable:
    """Decorator factory for async callables."""

    def decorator(fn: Callable) -> Callable:
        async def wrapper(*args, **kwargs):
            if circuit_breaker and not circuit_breaker.allow_request():
                raise CircuitOpenError(
                    f"Circuit breaker '{circuit_breaker.name}' is OPEN"
                )

            last_exc: Exception | None = None
            for attempt in range(policy.max_attempts):
                try:
                    result = await fn(*args, **kwargs)
                    if circuit_breaker:
                        circuit_breaker.record_success()
                    return result
                except Exception as exc:
                    last_exc = exc
                    retry_after = getattr(exc, "retry_after", None)

                    if not policy.should_retry_exc(exc):
                        if circuit_breaker:
                            circuit_breaker.record_failure()
                        raise

                    if attempt < policy.max_attempts - 1:
                        delay = policy.compute_delay(attempt, retry_after)
                        logger.warning(
                            "Async attempt %d/%d failed — retrying in %.2fs | %s",
                            attempt + 1, policy.max_attempts, delay, exc,
                        )
                        await asyncio.sleep(delay)
                    else:
                        if circuit_breaker:
                            circuit_breaker.record_failure()

            raise RetryExhaustedError(
                f"All {policy.max_attempts} attempts failed"
            ) from last_exc

        return wrapper
    return decorator