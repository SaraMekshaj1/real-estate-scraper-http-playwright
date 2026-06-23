from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

from app.abstractions.base_client import BaseAsyncHTTPClient
from app.abstractions.base_parser import BaseParser
from app.config.settings import ScraperSettings
from app.crawler.property_crawler import PropertiesLinksCrawler
from app.models.property_model import Property
from app.pagination.next_page import NextPage
from app.orchestration.async_scraper_orchestrator import AsyncScraperOrchestrator
from app.storage.failed_url_store import FailedUrlStore
from app.utils.rate_limiter import AsyncRateLimiter          # ← NEW
from monitoring.logger import Metrics

logger = logging.getLogger("scraper")


class ScrapeService:
    def __init__(
        self,
        client:           BaseAsyncHTTPClient,
        parser:           BaseParser,
        settings:         ScraperSettings,
        metrics:          Metrics,
        failed_url_store: FailedUrlStore   | None = None,
        rate_limiter:     AsyncRateLimiter | None = None,    # ← NEW
    ) -> None:
        self._client       = client
        self._parser       = parser
        self._settings     = settings
        self._metrics      = metrics
        self._crawler      = PropertiesLinksCrawler()
        self._next_page    = NextPage()
        self._failed_store = failed_url_store
        self._rate_limiter = rate_limiter                    # ← NEW

        self._last_orchestrator: AsyncScraperOrchestrator | None = None

    def _make_orchestrator(self) -> AsyncScraperOrchestrator:
        self._last_orchestrator = AsyncScraperOrchestrator(
            client           = self._client,
            parser           = self._parser,
            crawler          = self._crawler,
            next_page        = self._next_page,
            settings         = self._settings,
            metrics          = self._metrics,
            failed_url_store = self._failed_store,
            rate_limiter     = self._rate_limiter,           # ← NEW
        )
        return self._last_orchestrator

    @property
    def pages_exhausted(self) -> bool:
        if self._last_orchestrator is None:
            return False
        return self._last_orchestrator.pages_exhausted

    # ── Streaming path ────────────────────────────────────────────────────

    async def stream(
        self, stop_event: asyncio.Event | None = None
    ) -> AsyncIterator[Property]:
        orchestrator = self._make_orchestrator()
        if stop_event is None:
            stop_event = asyncio.Event()
        count = 0
        try:
            async for prop in orchestrator.stream(stop_event=stop_event):
                count += 1
                yield prop
        finally:
            stop_event.set()
            self._metrics.gauge("scrape.total_fetched", count)
            logger.info("ScrapeService: streamed %d properties", count)
            self._log_failed_url_summary()

    # ── Legacy path ───────────────────────────────────────────────────────

    async def run(self) -> list[Property]:
        orchestrator = self._make_orchestrator()
        properties = await orchestrator.run()
        self._metrics.gauge("scrape.total_fetched", len(properties))
        logger.info("ScrapeService: fetched %d properties", len(properties))
        self._log_failed_url_summary()
        return properties

    # ── Internals ─────────────────────────────────────────────────────────

    def _log_failed_url_summary(self) -> None:
        if self._failed_store is None:
            return

        all_entries      = self._failed_store.load()
        failed_urls      = len(all_entries)
        remaining_failed = len(
            self._failed_store.pending(self._settings.max_retry_attempts)
        )

        logger.info(
            "ScrapeService: failed_urls=%d remaining_failed=%d "
            "(max_retry_attempts=%d)",
            failed_urls,
            remaining_failed,
            self._settings.max_retry_attempts,
        )
        self._metrics.gauge("scrape.failed_urls",      failed_urls)
        self._metrics.gauge("scrape.remaining_failed", remaining_failed)