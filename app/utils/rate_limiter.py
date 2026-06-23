from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger("scraper")


class AsyncRateLimiter:
    """
    Token-bucket async rate limiter shared across all ScraperWorker instances.

    ALGORITHM (Token Bucket):
      - Bucket starts full (capacity = burst_size tokens).
      - Tokens refill continuously at `rate` tokens/second.
      - Each HTTP request consumes one token.
      - If no token is available, the caller awaits until one refills.

    CONCURRENCY:
      - All workers share one instance (injected via DI).
      - An asyncio.Lock serialises token consumption so concurrent workers
        don't race on the internal state — only the *acquire* check is
        serialised; the actual HTTP call proceeds concurrently afterward.
      - P99 latency impact is negligible: at 5 req/s the average wait is
        200 ms per slot, which is far less than a real HTTP round-trip.

    CONFIGURATION (from ScraperSettings):
      requests_per_second : float  — steady-state throughput cap.
      burst_size          : int    — max tokens held at once (defaults to
                                     requests_per_second, i.e. 1 second of
                                     burst).  Set higher to absorb short
                                     bursts without throttling.

    USAGE:
      async with rate_limiter:
          html = await client.get_async(url)

    TESTABILITY:
      Pass a custom `clock` callable (default: time.monotonic) so unit
      tests can control time without sleeping.
    """

    def __init__(
        self,
        rate: float,
        burst_size: int | None = None,
        clock=time.monotonic,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")

        self._rate      = rate                           # tokens / second
        self._capacity  = float(burst_size or max(1, int(rate)))
        self._tokens    = self._capacity                 # start full
        self._last_refill = clock()
        self._clock     = clock
        self._lock      = asyncio.Lock()

        logger.info(
            "AsyncRateLimiter created — rate=%.2f req/s  burst_capacity=%.0f",
            self._rate,
            self._capacity,
        )

    # ── Public context-manager API ────────────────────────────────────────

    async def __aenter__(self) -> "AsyncRateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, *_) -> None:
        pass  # nothing to release — token was already consumed on entry

    # ── Core acquire ──────────────────────────────────────────────────────

    async def acquire(self) -> None:
        """
        Block until a token is available, then consume it.

        All concurrent callers serialise here — only the inter-request
        sleep is serialised; as soon as a caller gets its token it releases
        the lock and the next caller can enter.
        """
        async with self._lock:
            self._refill()

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                logger.debug(
                    "RateLimiter: token acquired (%.2f remaining)", self._tokens
                )
                return

            # Not enough tokens — compute how long to wait.
            deficit = 1.0 - self._tokens
            wait    = deficit / self._rate

            logger.debug(
                "RateLimiter: throttling %.3fs (tokens=%.4f, rate=%.2f/s)",
                wait, self._tokens, self._rate,
            )
            await asyncio.sleep(wait)

            # After sleeping, refill again and consume.
            self._refill()
            self._tokens -= 1.0

    # ── Internal helpers ──────────────────────────────────────────────────

    def _refill(self) -> None:
        """Add tokens proportional to elapsed time since last refill."""
        now     = self._clock()
        elapsed = now - self._last_refill
        gained  = elapsed * self._rate
        self._tokens     = min(self._capacity, self._tokens + gained)
        self._last_refill = now

    # ── Introspection (for tests / metrics) ───────────────────────────────

    @property
    def available_tokens(self) -> float:
        """Current token count (approximate — not lock-protected)."""
        return self._tokens

    @property
    def rate(self) -> float:
        return self._rate

    def __repr__(self) -> str:
        return (
            f"AsyncRateLimiter(rate={self._rate}/s, "
            f"capacity={self._capacity}, "
            f"tokens≈{self._tokens:.2f})"
        )