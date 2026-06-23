from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator

from app.abstractions.base_client import BaseAsyncHTTPClient
from app.abstractions.base_parser import BaseParser
from app.config.settings import ScraperSettings
from app.models.property_model import Property
from app.storage.failed_url_store import FailedUrlStore
from app.utils.rate_limiter import AsyncRateLimiter          # ← NEW
from monitoring.logger import Metrics
from app.workers.page_producer import PageProducer
from app.workers.scraper_worker import ScraperWorker, ScrapeResult

logger = logging.getLogger("scraper")
_POISON_PILL = None


class AsyncScraperOrchestrator:
    def __init__(
        self,
        client:           BaseAsyncHTTPClient,
        parser:           BaseParser,
        crawler,
        next_page,
        settings:         ScraperSettings,
        metrics:          Metrics,
        failed_url_store: FailedUrlStore   | None = None,
        rate_limiter:     AsyncRateLimiter | None = None,    # ← NEW
    ) -> None:
        self._client       = client
        self._parser       = parser
        self._crawler      = crawler
        self._next_page    = next_page
        self._settings     = settings
        self._metrics      = metrics
        self._failed_store = failed_url_store
        self._rate_limiter = rate_limiter                    # ← NEW

        self.pages_exhausted: bool = False

    async def run(self) -> list[Property]:
        return [prop async for prop in self.stream()]

    async def stream(self, stop_event: asyncio.Event | None = None) -> AsyncIterator[Property]:
        """
        Yield Property objects as soon as workers produce them.

        Rate limiting:
          The shared AsyncRateLimiter is passed to each ScraperWorker.
          Workers call `async with self._rate_limiter` immediately before
          every HTTP request, serialising token consumption globally across
          all concurrent workers.  The limiter is optional (None disables
          throttling), which keeps tests free of timing dependencies.
        """
        self.pages_exhausted = False

        if self._settings.warmup_enabled and hasattr(self._client, "warmup"):
            await self._client.warmup(self._settings.base_url)

        if stop_event is None:
            stop_event = asyncio.Event()

        url_queue:    asyncio.Queue = asyncio.Queue(maxsize=self._settings.queue_max_size)
        result_queue: asyncio.Queue = asyncio.Queue()

        producer = PageProducer(
            client     = self._client,
            crawler    = self._crawler,
            next_page  = self._next_page,
            settings   = self._settings,
            url_queue  = url_queue,
            metrics    = self._metrics,
            stop_event = stop_event,
        )

        workers = [
            ScraperWorker(
                worker_id        = i,
                client           = self._client,
                parser           = self._parser,
                url_queue        = url_queue,
                result_queue     = result_queue,
                settings         = self._settings,
                metrics          = self._metrics,
                failed_url_store = self._failed_store,
                rate_limiter     = self._rate_limiter,       # ← NEW
            )
            for i in range(self._settings.worker_count)
        ]

        logger.info("Starting %d async workers (streaming mode)", self._settings.worker_count)
        t_start = time.perf_counter()

        producer_result_holder: dict = {}

        produce_task = asyncio.create_task(self._produce_then_signal(
            producer, workers, url_queue, stop_event, producer_result_holder
        ))
        worker_tasks = [asyncio.create_task(w.run()) for w in workers]

        workers_done  = 0
        total_yielded = 0

        try:
            while workers_done < len(workers):
                result: ScrapeResult | None = await result_queue.get()

                if result is _POISON_PILL:
                    workers_done += 1
                    logger.debug(
                        "Worker sentinel received (%d/%d done)",
                        workers_done, len(workers),
                    )
                    continue

                if result.property:
                    total_yielded += 1
                    yield result.property

        finally:
            if not stop_event.is_set():
                stop_event.set()

            for t in [produce_task, *worker_tasks]:
                t.cancel()

            await asyncio.gather(produce_task, *worker_tasks, return_exceptions=True)

            self.pages_exhausted = producer_result_holder.get("pages_exhausted", False)
            logger.info(
                "Scrape phase done in %.2fs — %d properties yielded  pages_exhausted=%s",
                time.perf_counter() - t_start,
                total_yielded,
                self.pages_exhausted,
            )
            self._metrics.gauge("scrape_duration_secs", time.perf_counter() - t_start)
            self._metrics.gauge("scrape.total_fetched", total_yielded)

    @staticmethod
    async def _produce_then_signal(
        producer:               PageProducer,
        workers:                list[ScraperWorker],
        url_queue:              asyncio.Queue,
        stop_event:             asyncio.Event,
        producer_result_holder: dict,
    ) -> int:
        total_enqueued = 0
        try:
            result = await producer.produce()
            total_enqueued = result.total_enqueued
            producer_result_holder["pages_exhausted"] = result.pages_exhausted
        except asyncio.CancelledError:
            producer_result_holder["pages_exhausted"] = False
            logger.info("Producer task cancelled — stop signal was set.")
        finally:
            for _ in workers:
                try:
                    url_queue.put_nowait(_POISON_PILL)
                except asyncio.QueueFull:
                    await url_queue.put(_POISON_PILL)
            logger.info(
                "Producer done — %d URLs enqueued, poison pills sent",
                total_enqueued,
            )
        return total_enqueued