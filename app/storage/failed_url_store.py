from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

logger = logging.getLogger("scraper")


class FailedUrlEntry(TypedDict):
    url: str
    error: str
    attempts: int
    timestamp: str


# Errors whose substrings mark a failure as retryable.
# Kept as a module-level tuple so ScraperWorker can import it too.
RETRYABLE_FRAGMENTS: tuple[str, ...] = (
    # Timeouts
    "timeout",
    "timed out",
    # Circuit breaker
    "circuit breaker",
    "circuit open",
    # Connection problems
    "connection reset",
    "connectionreset",
    "connection refused",
    "connectionrefused",
    "connection error",
    # HTTP transient errors
    "503",
    "429",
    "too many requests",
    "service unavailable",
    # Playwright / browser
    "browser has been closed",
    "browser appears dead",
    "target closed",
    "page crashed",
    # Generic network
    "network",
    "eof occurred",
    "broken pipe",
    "ssl",
    "read timeout",
)

# Errors whose substrings mark a failure as NON-retryable (checked first).
NON_RETRYABLE_FRAGMENTS: tuple[str, ...] = (
    "404",
    "not found",
    "validation",
    "parseerror",
    "parse error",
    "attributeerror",
    "keyerror",
    "valueerror",
)


def is_retryable(error: str) -> bool:
    """
    Return True if *error* looks like a transient failure worth retrying.

    Non-retryable patterns are checked first so that e.g. a "404 not found"
    string never matches the generic "network" fragment.
    """
    low = error.lower()
    for fragment in NON_RETRYABLE_FRAGMENTS:
        if fragment in low:
            return False
    for fragment in RETRYABLE_FRAGMENTS:
        if fragment in low:
            return True
    return False


class FailedUrlStore:
    """
    Persistent, asyncio-safe store for property detail-page URLs that failed
    with a retryable error during a crawl.

    FILE FORMAT:
        output/failed_urls.json
        [
          { "url": "...", "error": "...", "attempts": 1, "timestamp": "..." },
          ...
        ]

    CONCURRENCY:
        An asyncio.Lock serialises all mutations so concurrent workers sharing
        one store instance never produce a torn write.  This is sufficient for
        single-process asyncio scrapers; for multi-process use Redis instead.

    LIFECYCLE:
        1. Instantiate once and share across all ScraperWorker instances.
        2. Workers call record() on retryable failures.
        3. After the crawl, retry_failed_urls.py loads the store, re-scrapes,
           calls remove() on each success, and increments attempts on each
           new failure before calling save().
    """

    def __init__(self, path: str = "output/failed_urls.json") -> None:
        self._path = Path(path)
        self._lock = asyncio.Lock()
        # url → entry; dict keeps dedup and O(1) lookup cheap.
        self._data: dict[str, FailedUrlEntry] = {}
        self._load_sync()

    # ── Internal helpers ──────────────────────────────────────────────────

    def _load_sync(self) -> None:
        """Synchronous load called once at construction (no event loop needed)."""
        if not self._path.exists():
            return
        try:
            raw: list[FailedUrlEntry] = json.loads(
                self._path.read_text(encoding="utf-8")
            )
            self._data = {entry["url"]: entry for entry in raw}
            logger.info(
                "FailedUrlStore: loaded %d entries from %s",
                len(self._data),
                self._path,
            )
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning(
                "FailedUrlStore: corrupt file at %s (%s) — starting fresh",
                self._path,
                exc,
            )
            self._data = {}

    def _flush(self) -> None:
        """Write current state to disk. Must be called while holding self._lock."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(list(self._data.values()), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # ── Public API ────────────────────────────────────────────────────────

    async def record(self, url: str, error: str) -> None:
        """
        Record a retryable failure for *url*.

        If the URL is already present (e.g. it failed on a previous run and
        was not retried yet), increment its attempts counter and update the
        error and timestamp.  This means a URL that keeps failing across
        multiple crawls accumulates its attempt count correctly.
        """
        async with self._lock:
            existing = self._data.get(url)
            if existing:
                existing["attempts"] += 1
                existing["error"] = error
                existing["timestamp"] = _now()
            else:
                self._data[url] = FailedUrlEntry(
                    url=url,
                    error=error,
                    attempts=1,
                    timestamp=_now(),
                )
            self._flush()
            logger.debug("FailedUrlStore: recorded failure for %s (%s)", url, error)

    async def remove(self, url: str) -> None:
        """Remove *url* after a successful retry."""
        async with self._lock:
            if url in self._data:
                del self._data[url]
                self._flush()

    async def save(self) -> None:
        """Explicit flush — useful after a batch of in-memory mutations."""
        async with self._lock:
            self._flush()

    async def clear(self) -> None:
        """Wipe all entries and persist."""
        async with self._lock:
            self._data.clear()
            self._flush()

    def load(self) -> list[FailedUrlEntry]:
        """
        Return a snapshot of all current entries (no lock needed — reading a
        dict is atomic in CPython, and this is only called from a single
        coroutine at a time in practice).
        """
        return list(self._data.values())

    def pending(self, max_attempts: int) -> list[FailedUrlEntry]:
        """
        Return entries that have not yet exceeded *max_attempts*.
        Use this to build the URL list for a retry run.
        """
        return [e for e in self._data.values() if e["attempts"] <= max_attempts]

    def __len__(self) -> int:
        return len(self._data)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()