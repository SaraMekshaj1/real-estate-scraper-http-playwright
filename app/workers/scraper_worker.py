from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from app.abstractions.base_client import BaseAsyncHTTPClient
from app.abstractions.base_parser import BaseParser
from app.config.settings import ScraperSettings
from app.models.property_model import Property
from app.normalizers.property_normalizer import PropertyNormalizer
from app.validators.property_validator import PropertyValidator
from app.storage.failed_url_store import FailedUrlStore, is_retryable
from app.utils.rate_limiter import AsyncRateLimiter          # ← NEW
from monitoring.logger import Metrics

logger = logging.getLogger("scraper")

_POISON_PILL = None


@dataclass
class ScrapeResult:
    url: str
    property: Property | None = None
    error: str | None = None
    duration_secs: float = 0.0


@dataclass
class WorkerStats:
    worker_id: int
    scraped: int = 0
    errors: int = 0
    retryable_errors: int = 0
    total_duration: float = 0.0


class ScraperWorker:
    def __init__(
        self,
        worker_id: int,
        client: BaseAsyncHTTPClient,
        parser: BaseParser,
        url_queue: asyncio.Queue,
        result_queue: asyncio.Queue,
        settings: ScraperSettings,
        metrics: Metrics,
        failed_url_store: FailedUrlStore | None = None,
        rate_limiter: AsyncRateLimiter | None = None,        # ← NEW
    ) -> None:
        self._id           = worker_id
        self._client       = client
        self._parser       = parser
        self._url_q        = url_queue
        self._result_q     = result_queue
        self._settings     = settings
        self._metrics      = metrics
        self._failed_store = failed_url_store
        self._rate_limiter = rate_limiter                    # ← NEW — may be None
        self.stats         = WorkerStats(worker_id=worker_id)

    async def run(self) -> None:
        log = logging.LoggerAdapter(logger, {"worker": self._id})
        log.info("Worker started")

        try:
            while True:
                item = await self._url_q.get()

                if item is _POISON_PILL:
                    log.info("Worker received stop signal")
                    self._url_q.task_done()
                    break

                url, referer = item if isinstance(item, tuple) else (item, None)

                try:
                    result = await asyncio.wait_for(
                        self._process(url, referer, log),
                        timeout=self._settings.per_url_timeout_secs,
                    )
                except asyncio.TimeoutError:
                    log.error("Worker timeout processing %s", url)
                    error_msg = f"timeout after {self._settings.per_url_timeout_secs}s"
                    await self._record_if_retryable(url, error_msg, log)
                    result = ScrapeResult(url=url, error=error_msg)

                await self._result_q.put(result)
                self._url_q.task_done()

        finally:
            await self._result_q.put(_POISON_PILL)
            log.info(
                "Worker done — scraped=%d errors=%d retryable=%d",
                self.stats.scraped,
                self.stats.errors,
                self.stats.retryable_errors,
            )

    # ── Internal helpers ──────────────────────────────────────────────────

    async def _process(self, url: str, referer: str | None, log) -> ScrapeResult:
        t0 = time.perf_counter()
        try:
            # ── Acquire a rate-limiter token BEFORE the HTTP call ─────────
            #
            # All concurrent workers share one AsyncRateLimiter instance.
            # The token bucket ensures no more than `requests_per_second`
            # HTTP calls leave the process per second, regardless of how
            # many workers are running.
            #
            # The limiter is optional: when None (e.g. in unit tests that
            # inject a NullHTTPClient), no throttling is applied.
            if self._rate_limiter is not None:
                await self._rate_limiter.acquire()

            html = await self._client.get_async(url, referer=referer)
            raw  = self._parser.parse(html, url=url)
            prop = PropertyNormalizer.normalize(raw)

            result = PropertyValidator.validate(prop)
            if not result.is_valid:
                log.warning("Validation warnings for %s: %s", url, result.errors)

            self.stats.scraped += 1
            self._metrics.inc("worker.scraped")
            duration = time.perf_counter() - t0
            self.stats.total_duration += duration
            self._metrics.inc("worker.total_duration_secs", duration)
            return ScrapeResult(url=url, property=prop, duration_secs=duration)

        except Exception as exc:
            self.stats.errors += 1
            self._metrics.inc("worker.errors")
            error_msg = str(exc)
            log.warning("Failed %s | %s", url, error_msg)
            await self._record_if_retryable(url, error_msg, log)
            return ScrapeResult(url=url, error=error_msg)

    async def _record_if_retryable(
        self, url: str, error: str, log: logging.LoggerAdapter
    ) -> None:
        if self._failed_store is None:
            return
        if is_retryable(error):
            self.stats.retryable_errors += 1
            self._metrics.inc("worker.retryable_errors")
            await self._failed_store.record(url, error)
            log.info("Queued for retry: %s (%s)", url, error)
        else:
            log.debug("Non-retryable failure, skipping store: %s (%s)", url, error)