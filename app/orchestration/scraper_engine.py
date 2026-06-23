from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from app.abstractions.base_client import BaseHTTPClient
from app.abstractions.base_exporter import BaseExporter
from app.abstractions.base_parser import BaseParser
from app.abstractions.base_storage import BaseStorage
from app.config.settings import ScraperSettings
from app.mappers.property_mapper import PropertyMapper
from app.models.property_model import Property
from app.models.run_outcome import RunOutcome
from app.services.deduplication_service import DeduplicationService
from app.services.export_service import ExportService
from app.services.scrape_service import ScrapeService
from app.storage.failed_url_store import FailedUrlStore
from app.utils.rate_limiter import AsyncRateLimiter
from monitoring.logger import Metrics, timed

logger = logging.getLogger("scraper")

_DEFAULT_BATCH_SIZE = 50


class ScraperEngine:
    """
    Thin pipeline coordinator.

    Run-state decisions are delegated entirely to DeduplicationService.
    The engine observes what happened (outcome + error count) and reports
    it; the service decides what it means for the next run.
    """

    def __init__(
        self,
        client:           BaseHTTPClient,
        parser:           BaseParser,
        exporter:         BaseExporter,
        storage:          BaseStorage,
        settings:         ScraperSettings,
        metrics:          Metrics,
        failed_url_store: FailedUrlStore   | None = None,
        rate_limiter:     AsyncRateLimiter | None = None,
    ) -> None:
        self._client   = client
        self._settings = settings
        self._metrics  = metrics
        self._exporter = exporter

        self._batch_size: int = getattr(settings, "batch_size", _DEFAULT_BATCH_SIZE)

        self._scrape_svc = ScrapeService(
            client           = client,
            parser           = parser,
            settings         = settings,
            metrics          = metrics,
            failed_url_store = failed_url_store,
            rate_limiter     = rate_limiter,
        )
        self._dedup = DeduplicationService(
            storage = storage,
            metrics = metrics,
        )
        self._export_svc = ExportService(
            exporter = exporter,
            dedup    = self._dedup,
            metrics  = metrics,
        )

    # ── Public entry point ────────────────────────────────────────────────

    def run(self) -> None:
        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        start = time.perf_counter()
        logger.info("=" * 60)
        logger.info(
            "Scraper starting | env=%s  workers=%d  max_pages=%s  batch_size=%d  "
            "rate=%.1f req/s",
            self._settings.app_env,
            self._settings.worker_count,
            self._settings.max_pages,
            self._batch_size,
            self._settings.requests_per_second,
        )

        total_exported = 0

        try:
            self._exporter.open()
            try:
                total_exported = await self._stream_and_process()
            finally:
                self._exporter.close()
        finally:
            await self._client.aclose()

        elapsed = time.perf_counter() - start
        self._log_summary(total_exported, elapsed)

    # ── Core streaming loop ───────────────────────────────────────────────

    async def _stream_and_process(self) -> int:
        buffer:        list[Property] = []
        total_exported = 0
        batch_num      = 0
        total_scraped  = 0
        total_skipped  = 0

        last_id = self._dedup.get_last_exported_id()

        raw_run_flag = self._dedup.previous_run_incomplete()
        logger.info(
            "Storage check — run_in_progress raw value: %r → previous_run_incomplete()=%s",
            self._dedup._storage.load("run_in_progress"),
            raw_run_flag,
        )

        is_resume = raw_run_flag
        run_mode  = "resume" if is_resume else "daily"

        logger.info("Run mode: %s (last_exported_id=%s)", run_mode, last_id)

        self._dedup.clear_pages_exhausted()
        self._dedup.begin_run()

        EARLY_STOP_THRESHOLD = 24
        consecutive_known    = 0
        stream_exhausted     = False
        early_stop_triggered = False          # ← NEW

        stop_event = asyncio.Event()

        async for prop in self._scrape_svc.stream(stop_event=stop_event):
            total_scraped += 1

            pid = prop.property_id
            if pid and not self._dedup.is_new_id(pid):
                total_skipped     += 1
                consecutive_known += 1
                logger.debug(
                    "Known ID %s — consecutive_known=%d  run_mode=%s",
                    pid, consecutive_known, run_mode,
                )
                if run_mode == "daily" and consecutive_known >= EARLY_STOP_THRESHOLD:
                    logger.info("Early stop triggered.")
                    early_stop_triggered = True   # ← NEW
                    stop_event.set()
                    break
                continue

            consecutive_known = 0
            buffer.append(prop)

            if len(buffer) >= self._batch_size:
                batch_num += 1
                written = await self._process_batch(buffer, batch_num)
                total_exported += written
                buffer.clear()
        else:
            stream_exhausted = True

        if buffer:
            batch_num += 1
            written = await self._process_batch(buffer, batch_num)
            total_exported += written

        logger.info(
            "Stream complete — scraped=%d  skipped(dedup)=%d  exported=%d  batches=%d",
            total_scraped, total_skipped, total_exported, batch_num,
        )

        pages_exhausted      = self._scrape_svc.pages_exhausted
        scrape_error_count   = int(                               # ← NEW
            self._metrics.snapshot().get("scrape.errors", 0)
        )

        logger.info(
            "Completion check — stream_exhausted=%s  pages_exhausted=%s  "
            "early_stop=%s  scrape_errors=%d",
            stream_exhausted, pages_exhausted,
            early_stop_triggered, scrape_error_count,
        )

        # ── Determine outcome and record it ──────────────────────────────
        #
        # COMPLETED:    producer visited every page naturally (no stop_event,
        #               no max_pages clip) AND the for/else branch fired.
        #
        # EARLY_STOP:   daily run hit the consecutive-known threshold; the
        #               assumption is we've reached historical territory and
        #               any new listings tomorrow will appear at page 1.
        #               Only safe to treat as "done" when there were no
        #               worker errors — otherwise we may have gaps.
        #
        # INTERRUPTED:  everything else — rate-limit kill, crash recovery,
        #               max_pages clip, network drop, resume run that didn't
        #               finish.  Leave run_in_progress=True so next run
        #               enters resume mode.

        if stream_exhausted and pages_exhausted:
            outcome = RunOutcome.COMPLETED
            self._dedup.mark_pages_exhausted()
            logger.info(
                "Run completed cleanly — all pages scraped. "
                "end_run(COMPLETED) called; next run will be daily."
            )

        elif early_stop_triggered:
            outcome = RunOutcome.EARLY_STOP
            # end_run() decides whether errors make this a resume or daily —
            # see DeduplicationService.end_run() for the exact rule.
            logger.info(
                "Run ended via early-stop. "
                "end_run(EARLY_STOP, errors=%d) called.",
                scrape_error_count,
            )

        else:
            outcome = RunOutcome.INTERRUPTED
            logger.info(
                "Run interrupted (rate-limit / network / max_pages). "
                "end_run(INTERRUPTED) called; next run will be resume."
            )

        self._dedup.end_run(outcome, scrape_error_count)
        return total_exported

    async def _process_batch(
        self, batch: list[Property], batch_num: int
    ) -> int:
        logger.info("Batch %d — processing %d properties", batch_num, len(batch))
        export_rows = [PropertyMapper.to_export_dict(p) for p in batch]

        with timed(logger, f"export_batch_{batch_num}", self._metrics,
                   "export_duration_secs"):
            written = self._export_svc.export_batch(export_rows)

        self._metrics.gauge(
            "export.total_rows_written",
            self._metrics.snapshot().get("export.total_rows_written", 0) + written,
        )
        logger.info("Batch %d done — %d rows written", batch_num, written)
        return written

    # ── Summary ───────────────────────────────────────────────────────────

    def _log_summary(self, total_exported: int, elapsed: float) -> None:
        logger.info("=" * 60)
        logger.info("Pipeline complete")
        logger.info("  Rows exported  : %d", total_exported)
        logger.info("  Total time     : %.2fs", elapsed)
        logger.info("  Metrics        : %s", self._metrics.snapshot())
        logger.info("=" * 60)