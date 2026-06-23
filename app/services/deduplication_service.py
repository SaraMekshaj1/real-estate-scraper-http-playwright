from __future__ import annotations
import logging
from app.abstractions.base_storage import BaseStorage
from app.models.property_model import Property
from monitoring.logger import Metrics
logger = logging.getLogger("scraper")

# Key namespace — centralised here so nothing else needs to know the format.
_EXPORTED_KEY = "exported:{}"


class DeduplicationService:
    """
    Tracks which property IDs have already been exported.

    TWO-STAGE DEDUP:
      Stage 1 — ID-only check (is_new_id):
        Called by the scraper *before* fetching the full property page.
        Costs only a storage lookup — avoids an HTTP request entirely for
        properties we've already processed.  The ScrapeService / Producer
        should call this when it can extract an ID from a listing URL.

      Stage 2 — Object-level filter (filter_new):
        Called after scraping when full Property objects are available.
        Catches anything that slipped through (e.g. IDs not embeddable in
        URLs) and provides the final authoritative de-dup gate.

    DESIGN NOTE (SRP):
      This service owns *only* the exported-state bookkeeping.  It has no
      knowledge of HTTP, translation, or export format — those boundaries
      are intentional.
    """

    def __init__(self, storage: BaseStorage, metrics: Metrics) -> None:
        self._storage = storage
        self._metrics = metrics

    # ── Stage 1: ID-only pre-check ────────────────────────────────────────

    def is_new_id(self, property_id: str) -> bool:
        """
        Return True if *property_id* has NOT been exported yet.

        Intended to be called before issuing a full HTTP request for the
        property detail page.  When a listing URL encodes the ID (e.g.
        /properties/12345), the producer can call this to skip the fetch
        entirely, saving bandwidth and quota.

        O(1) for all storage backends (SQLite index, Redis GET, dict key).
        """
        already_done = self._storage.exists(_EXPORTED_KEY.format(property_id))
        if already_done:
            self._metrics.inc("dedup.skipped_early")
        return not already_done

    def filter_new_ids(self, property_ids: list[str]) -> list[str]:
        """
        Batch variant of is_new_id — returns only IDs not yet exported.

        Use when you have a list of IDs from a listing page and want to
        discard known ones before fetching any detail pages.
        """
        fresh   = [pid for pid in property_ids if self.is_new_id(pid)]
        skipped = len(property_ids) - len(fresh)
        if skipped:
            logger.info(
                "DeduplicationService: early-skipped %d/%d IDs (already exported)",
                skipped, len(property_ids),
            )
        self._metrics.inc("dedup.skipped_early_batch", skipped)
        return fresh

    # ── Stage 2: Object-level filter ─────────────────────────────────────

    def filter_new(self, properties: list[Property]) -> list[Property]:
        """
        Return only properties that have not been exported before.

        This is the final authoritative gate.  It runs after scraping so it
        also catches properties whose IDs could not be checked pre-fetch.
        """
        fresh:   list[Property] = []
        skipped: int            = 0

        for prop in properties:
            pid = prop.property_id
            if pid and self._storage.exists(_EXPORTED_KEY.format(pid)):
                skipped += 1
                continue
            fresh.append(prop)

        if skipped:
            logger.info(
                "DeduplicationService: skipped %d already-exported properties",
                skipped,
            )

        self._metrics.inc("dedup.skipped", skipped)
        self._metrics.gauge("dedup.fresh", len(fresh))
        return fresh

    # ── Checkpoint ────────────────────────────────────────────────────────

    def mark_exported(self, property_id: str) -> None:
        """
        Mark a single property_id as exported.

        Called by ExportService after each successful write so the two
        services stay decoupled — neither owns the other's state.
        """
        self._storage.save(_EXPORTED_KEY.format(property_id), True)

    def get_last_exported_id(self) -> str | None:
        """Return the property_id of the most recently exported property."""
        return self._storage.load("last_exported_id")

    def set_last_exported_id(self, pid: str) -> None:
        self._storage.save("last_exported_id", pid)

    # ── Run-completion tracking ──────────────────────────────────────────

    _RUN_STATE_KEY = "run_in_progress"

    def begin_run(self) -> None:
        """
        Call once at the very start of a run, before streaming begins.
        If the process crashes before end_run() is called, this flag is
        left True, so the *next* run knows the previous one didn't finish.
        """
        self._storage.save(self._RUN_STATE_KEY, True)

    def end_run(self) -> None:
        """
        Call once the stream has been fully exhausted by natural
        completion — including a deliberate early-stop, since that's
        still a clean finish, not a crash. Never call this from a
        finally/except block that also runs on failure.
        """
        self._storage.save(self._RUN_STATE_KEY, False)

    def previous_run_incomplete(self) -> bool:
        """
        True if the previous run crashed, was killed, or never ran
        before. Used by the engine to decide whether the early-stop
        optimisation is safe.
        """
        return self._storage.load(self._RUN_STATE_KEY) is not False

    # ── Error-aware run tracking ──────────────────────────────────────────
    #
    # WHY THIS EXISTS:
    #   A "clean finish" (stream_exhausted=True, end_run() called) only
    #   means the producer reached the end of the page list without being
    #   killed. It says NOTHING about whether every property along the way
    #   was actually scraped successfully. If the circuit breaker tripped,
    #   or any other transient failure occurred, those properties were
    #   never exported — but the run still "completed cleanly" by the
    #   crash/no-crash definition above. Trusting that signal alone for
    #   next-run early-stop eligibility means a clean-but-error-riddled run
    #   silently authorises skipping the very gaps it left behind.
    #
    #   This tracks error counts as a second, independent signal. The
    #   engine's run_mode decision must check BOTH previous_run_incomplete()
    #   and previous_run_had_errors() — either one being true disables
    #   early-stop on the next run.

    _LAST_RUN_ERRORS_KEY = "last_run_had_errors"

    def mark_run_errors(self, error_count: int) -> None:
        """
        Call once at the end of a run (whether it finished cleanly or via
        early-stop) with the total worker-level scrape error count from
        that run. Any non-zero count means some properties were never
        exported despite the run "completing."
        """
        self._storage.save(self._LAST_RUN_ERRORS_KEY, error_count > 0)

    def previous_run_had_errors(self) -> bool:
        """
        True if the previous run logged any worker-level scrape errors,
        even if it finished without crashing. Forces the next run to
        skip the early-stop shortcut so it can re-attempt the gaps left
        behind, since those property IDs were never marked exported and
        is_new_id() will correctly treat them as new once reached.
        """
        return bool(self._storage.load(self._LAST_RUN_ERRORS_KEY))

    # ── Pages-exhausted tracking ──────────────────────────────────────────
    #
    # WHY THIS EXISTS:
    #   end_run() used to be called whenever the stream finished naturally
    #   (stream_exhausted=True). But "natural finish" includes the case
    #   where the async-for loop simply ran out of items because the
    #   producer stopped early due to rate-limiting, a network error, or
    #   the stop_event being set. In all those cases the for/else branch
    #   sets stream_exhausted=True even though we never saw the last page.
    #
    #   This flag is set ONLY by the producer itself, at the exact moment
    #   it finds no next_url. It is the only reliable signal that every
    #   listing page was actually visited. end_run() is now gated on BOTH
    #   stream_exhausted AND pages_exhausted — so a rate-limit stop or any
    #   other incomplete run always leaves run_in_progress=True and the
    #   next execution correctly enters resume mode.

    _PAGES_EXHAUSTED_KEY = "pages_exhausted"

    def mark_pages_exhausted(self) -> None:
        """
        Call only when the producer confirmed it reached the very last
        listing page (get_next_page returned None naturally, not via
        stop_event or max_pages).
        """
        self._storage.save(self._PAGES_EXHAUSTED_KEY, True)

    def clear_pages_exhausted(self) -> None:
        """Reset at the start of each run so stale True values don't carry over."""
        self._storage.save(self._PAGES_EXHAUSTED_KEY, False)

    def previous_run_exhausted_pages(self) -> bool:
        """
        True only if the previous run's producer reached the last page.
        Used together with previous_run_incomplete() to decide whether
        end_run() should be called at the end of the current run.
        """
        return bool(self._storage.load(self._PAGES_EXHAUSTED_KEY))