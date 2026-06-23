from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass
from app.abstractions.base_client import BaseAsyncHTTPClient
from app.config.settings import ScraperSettings
from monitoring.logger import Metrics
logger = logging.getLogger("scraper")

@dataclass
class ProducerResult:
    """
    Returned by PageProducer.produce() so callers can distinguish between
    a producer that reached the natural end of pagination vs one that was
    stopped early (stop_event, max_pages, fetch error).

    pages_exhausted=True  → producer saw get_next_page() return None
                            (i.e. every listing page was visited).
    pages_exhausted=False → producer exited for any other reason; the
                            site may have more pages we haven't seen.
    """
    total_enqueued: int
    pages_exhausted: bool


class PageProducer:
    """
    Crawls paginated listing pages and enqueues property URLs.

    Supports:
      - bounded scraping via max_pages
      - unlimited scraping when max_pages=None
      - graceful stop on fetch failure
      - early stop via stop_event (set externally by the engine when
        consecutive known IDs threshold is reached)
    """
    def __init__(
        self,
        client:     BaseAsyncHTTPClient,
        crawler,
        next_page,
        settings:   ScraperSettings,
        url_queue:  asyncio.Queue,
        metrics:    Metrics,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self._client     = client
        self._crawler    = crawler
        self._next_page  = next_page
        self._settings   = settings
        self._queue      = url_queue
        self._metrics    = metrics
        self._stop_event = stop_event or asyncio.Event()  # never None internally

    async def produce(self) -> ProducerResult:
        url             = self._settings.base_url
        page            = 0
        total           = 0
        pages_exhausted = False  # becomes True only when get_next_page returns None

        while url:
            # ── Early-stop signal (e.g. consecutive known IDs) ──────────
            if self._stop_event.is_set():
                logger.info(
                    "Producer: stop signal received before page %d — "
                    "halting pagination.",
                    page + 1,
                )
                break

            # ── Optional page limit ──────────────────────────────────────
            if (
                self._settings.max_pages is not None
                and page >= self._settings.max_pages
            ):
                logger.warning(
                    "Producer: max_pages=%d reached — stopping crawl.",
                    self._settings.max_pages,
                )
                break

            page += 1
            listing_url = url

            logger.info("Producer: fetching page %d — %s", page, url)

            try:
                html = await self._client.get_async(url)
            except asyncio.CancelledError:
                # Task was cancelled during the HTTP call — exit cleanly.
                logger.info("Producer: cancelled while fetching page %d.", page)
                raise
            except Exception as exc:
                logger.error("Producer: failed page %d | %s", page, exc)
                self._metrics.inc("producer.page_errors")
                break

            # ── Extract property links ───────────────────────────────────
            links = self._crawler.crawl_property_links(html, base_url=url)
            logger.info("Producer: found %d links on page %d", len(links), page)

            # ── Enqueue URLs ─────────────────────────────────────────────
            for link in links:
                # Check stop_event mid-enqueue too — avoids flooding the
                # queue with URLs from a page that arrived just after the
                # early-stop threshold was hit.
                if self._stop_event.is_set():
                    logger.info(
                        "Producer: stop signal set mid-enqueue on page %d "
                        "— discarding remaining %d links.",
                        page,
                        len(links) - links.index(link),
                    )
                    break

                referer = listing_url if self._settings.referer_enabled else None
                await self._queue.put((link, referer))
                total += 1
                self._metrics.inc("producer.urls_enqueued")

            # ── Discover next page ───────────────────────────────────────
            next_url = self._next_page.get_next_page(html, url)
            if not next_url:
                logger.info("Producer: no more pages found — crawl complete.")
                pages_exhausted = True  # ← natural end of pagination

            url = next_url
            self._metrics.inc("producer.pages_scraped")

        logger.info(
            "Producer done — %d URLs enqueued across %d pages (pages_exhausted=%s)",
            total, page, pages_exhausted,
        )
        return ProducerResult(total_enqueued=total, pages_exhausted=pages_exhausted)