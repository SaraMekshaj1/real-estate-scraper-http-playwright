"""
retry_failed_urls.py
====================
Standalone script that re-scrapes only the property detail URLs that were
recorded as retryable failures during a previous crawl.

USAGE:
    python retry_failed_urls.py
    python retry_failed_urls.py --max-attempts 3   # override per-run cap
    python retry_failed_urls.py --failed-path output/failed_urls.json

WHAT IT DOES:
    1. Loads failed_urls.json via FailedUrlStore.
    2. Skips URLs whose attempt count already exceeds max_retry_attempts.
    3. Feeds the remaining URLs directly into the worker queue — no
       PageProducer, no pagination, no listing-page crawling.
    4. Passes results through DeduplicationService → ExportService, exactly
       as the normal crawl does.
    5. On success:   removes the URL from the store.
    6. On retryable failure: increments attempts in the store, keeps the URL.
    7. On non-retryable failure: removes the URL (no point retrying a 404).
    8. Logs a final summary: retried / succeeded / failed / skipped counts.

DESIGN CONSTRAINTS HONOURED:
    - Parser, exporter, and deduplication code are untouched.
    - DeduplicationService continues to gate exports — a URL that succeeded
      in the main crawl between runs is silently skipped here too.
    - Uses the same ScraperWorker and AsyncScraperOrchestrator machinery,
      so circuit-breaker, retry policy, and rate limiting all apply.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from typing import TYPE_CHECKING
from app.config.settings import get_settings, ScraperSettings
from app.container_f.container import Container
from app.storage.failed_url_store import FailedUrlStore, is_retryable
from app.workers.scraper_worker import ScraperWorker, ScrapeResult
from monitoring.logger import setup_logger



if TYPE_CHECKING:
    pass

logger = logging.getLogger("scraper")

_POISON_PILL = None


async def _run_retry(settings: ScraperSettings, failed_store: FailedUrlStore) -> None:
    """Core retry logic — isolated so it is easily unit-testable."""

    pending = failed_store.pending(settings.max_retry_attempts)
    skipped = len(failed_store.load()) - len(pending)

    if not pending:
        logger.info(
            "retry_failed_urls: nothing to retry "
            "(0 pending, %d exceeded max_retry_attempts=%d)",
            skipped,
            settings.max_retry_attempts,
        )
        return

    logger.info(
        "retry_failed_urls: %d URL(s) to retry  |  %d skipped (attempts > %d)",
        len(pending),
        skipped,
        settings.max_retry_attempts,
    )

    # ── Build lightweight container (client + parser + exporter + storage) ──
    container = Container.build(settings=settings)
    container.failed_url_store = failed_store

    # ── Open exporter once for the whole retry run ────────────────────────
    container.exporter.open()

    try:
        # ── Wire up queues ────────────────────────────────────────────────
        url_queue:    asyncio.Queue = asyncio.Queue()
        result_queue: asyncio.Queue = asyncio.Queue()

        for entry in pending:
            await url_queue.put(entry["url"])

        worker_count = settings.worker_count
        for _ in range(worker_count):
            await url_queue.put(_POISON_PILL)

        # ── Create workers ────────────────────────────────────────────────
        workers = [
            ScraperWorker(
                worker_id        = i,
                client           = container.client,
                parser           = container.parser,
                url_queue        = url_queue,
                result_queue     = result_queue,
                settings         = settings,
                metrics          = container.metrics,
                failed_url_store = None,
            )
            for i in range(worker_count)
        ]

        worker_tasks = [asyncio.create_task(w.run()) for w in workers]

        # ── Collect results ───────────────────────────────────────────────
        succeeded        = 0
        failed_retryable = 0
        failed_permanent = 0
        exported         = 0
        workers_done     = 0

        entry_by_url = {e["url"]: e for e in pending}

        while workers_done < worker_count:
            result: ScrapeResult | None = await result_queue.get()

            if result is _POISON_PILL:
                workers_done += 1
                continue

            url = result.url

            if result.property:
                # ── Success path ──────────────────────────────────────────
                succeeded += 1
                await failed_store.remove(url)

                prop = result.property
                pid  = prop.property_id

                if container.engine._dedup.is_new_id(pid or ""):
                    try:
                        await _export_one(container, prop)
                        exported += 1
                    except Exception as exc:
                        logger.warning("Export failed for %s: %s", url, exc)
                else:
                    logger.debug("Dedup: already exported %s — skipping", url)

            else:
                # ── Failure path ──────────────────────────────────────────
                error = result.error or "unknown"

                if is_retryable(error):
                    failed_retryable += 1
                    entry = entry_by_url.get(url)
                    if entry:
                        entry["attempts"] += 1
                    await failed_store.save()
                    logger.warning(
                        "Retry failed (retryable) — kept in store: %s | %s", url, error
                    )
                else:
                    failed_permanent += 1
                    await failed_store.remove(url)
                    logger.info(
                        "Retry failed (non-retryable) — removed from store: %s | %s",
                        url,
                        error,
                    )

        await asyncio.gather(*worker_tasks, return_exceptions=True)

    finally:
        # Always flush and close, even if an exception was raised mid-run.
        container.exporter.close()
        await container.client.aclose()  # not close() — that's the sync wrapper


    remaining = len(failed_store.pending(settings.max_retry_attempts))

    logger.info(
        "retry_failed_urls complete — "
        "retried=%d  succeeded=%d  exported=%d  "
        "failed_retryable=%d  failed_permanent=%d  "
        "remaining_in_store=%d",
        len(pending),
        succeeded,
        exported,
        failed_retryable,
        failed_permanent,
        remaining,
    )


async def _export_one(container: Container, prop) -> None:
    """
    Serialize a single property and write it via the exporter.

    Assumes the exporter is already open (opened once in _run_retry).
    Converts the property model to a plain dict before handing off to
    write_row(), which is what CSVExporter / JSONExporter / CompositeExporter
    all expect.
    """
    pid = prop.property_id
    if pid:
        container.engine._dedup.mark_exported(pid)

    row = prop.model_dump() if hasattr(prop, "model_dump") else vars(prop)
    await asyncio.to_thread(container.exporter.write_row, row)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Re-scrape URLs that failed during the last crawl."
    )
    p.add_argument(
        "--failed-path",
        default=None,
        help="Path to failed_urls.json (default: settings.failed_urls_path)",
    )
    p.add_argument(
        "--max-attempts",
        type=int,
        default=None,
        help="Override settings.max_retry_attempts for this run.",
    )
    return p.parse_args()


def main() -> None:
    args     = _parse_args()
    settings = get_settings()


    setup_logger(
        name       = "scraper",
        level      = settings.log_level,
        log_format = settings.log_format,
        log_file   = settings.log_file,
    )

    if args.max_attempts is not None:
        settings = settings.model_copy(update={"max_retry_attempts": args.max_attempts})

    path         = args.failed_path or settings.failed_urls_path
    failed_store = FailedUrlStore(path=path)

    # Keep ProactorEventLoop (required by Playwright on Windows).
    # Run the loop manually so we can shut it down gracefully before
    # garbage collection, which eliminates the pipe-cleanup tracebacks.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run_retry(settings, failed_store))
    finally:
        try:
            # Cancel all lingering tasks (Playwright connection tasks, etc.)
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                for task in pending:
                    task.cancel()
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()


if __name__ == "__main__":
    main()