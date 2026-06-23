from __future__ import annotations
import logging
from app.abstractions.base_storage import BaseStorage
from app.models.property_model import Property
from app.models.run_outcome import RunOutcome
from monitoring.logger import Metrics

logger = logging.getLogger("scraper")

_EXPORTED_KEY = "exported:{}"


class DeduplicationService:
    """
    Tracks which property IDs have already been exported.

    TWO-STAGE DEDUP:
      Stage 1 — ID-only check (is_new_id):
        Called by the scraper *before* fetching the full property page.

      Stage 2 — Object-level filter (filter_new):
        Called after scraping when full Property objects are available.

    RUN-STATE:
      The service owns all run-state keys and the logic that maps a
      RunOutcome → next-run behaviour. ScraperEngine just reports the
      outcome; it never touches raw state keys directly.
    """

    def __init__(self, storage: BaseStorage, metrics: Metrics) -> None:
        self._storage = storage
        self._metrics = metrics

    # ── Stage 1: ID-only pre-check ────────────────────────────────────────

    def is_new_id(self, property_id: str) -> bool:
        already_done = self._storage.exists(_EXPORTED_KEY.format(property_id))
        if already_done:
            self._metrics.inc("dedup.skipped_early")
        return not already_done

    def filter_new_ids(self, property_ids: list[str]) -> list[str]:
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
        self._storage.save(_EXPORTED_KEY.format(property_id), True)

    def get_last_exported_id(self) -> str | None:
        return self._storage.load("last_exported_id")

    def set_last_exported_id(self, pid: str) -> None:
        self._storage.save("last_exported_id", pid)

    # ── Run-state keys (private; engine never touches these directly) ─────

    _RUN_STATE_KEY       = "run_in_progress"
    _LAST_RUN_ERRORS_KEY = "last_run_had_errors"
    _PAGES_EXHAUSTED_KEY = "pages_exhausted"
    _LAST_OUTCOME_KEY    = "last_run_outcome"

    # ── Run lifecycle ─────────────────────────────────────────────────────

    def begin_run(self) -> None:
        """
        Call once at the very start of a run, before streaming begins.
        Resets transient per-run flags so stale values never carry over.
        """
        self._storage.save(self._RUN_STATE_KEY,       True)
        self._storage.save(self._PAGES_EXHAUSTED_KEY, False)
        # last_run_had_errors intentionally NOT reset here — it is written
        # only by end_run() so it always reflects the previous completed run.

    def end_run(self, outcome: RunOutcome, scrape_error_count: int = 0) -> None:
        """
        Call once per run to record its outcome and decide next-run mode.

        outcome             — what kind of finish this was
        scrape_error_count  — total worker-level errors during the run
                              (from metrics or a direct counter; 0 is fine
                              if you have no error tracking yet)

        Callers (ScraperEngine) should never call this from a
        finally/except block that also runs on unhandled failure —
        only call it when you know the run finished intentionally.
        """
        had_errors = scrape_error_count > 0

        self._storage.save(self._LAST_RUN_ERRORS_KEY, had_errors)
        self._storage.save(self._LAST_OUTCOME_KEY,    outcome.name)

        # Decide whether the *next* run should start fresh or resume.
        #
        # COMPLETED + no errors  → fresh daily run next time
        # EARLY_STOP + no errors → fresh daily run next time (new listings
        #                          will be at page 1, above the known IDs)
        # EARLY_STOP + errors    → resume, because we may have gaps
        # INTERRUPTED            → always resume
        if outcome in (RunOutcome.COMPLETED, RunOutcome.EARLY_STOP) and not had_errors:
            self._storage.save(self._RUN_STATE_KEY, False)
            logger.info(
                "end_run: outcome=%s errors=%d → next run will be DAILY",
                outcome.name, scrape_error_count,
            )
        else:
            # Leave run_in_progress=True so next run enters resume mode.
            logger.info(
                "end_run: outcome=%s errors=%d → next run will be RESUME",
                outcome.name, scrape_error_count,
            )

    def previous_run_incomplete(self) -> bool:
        """
        True if the previous run should be resumed.
        False if the next run should start fresh from page 1.
        """
        return self._storage.load(self._RUN_STATE_KEY) is not False

    # ── Error-aware run tracking (kept for backward compat, now internal) ─

    def previous_run_had_errors(self) -> bool:
        return bool(self._storage.load(self._LAST_RUN_ERRORS_KEY))

    # ── Pages-exhausted tracking ──────────────────────────────────────────

    def mark_pages_exhausted(self) -> None:
        self._storage.save(self._PAGES_EXHAUSTED_KEY, True)

    def clear_pages_exhausted(self) -> None:
        self._storage.save(self._PAGES_EXHAUSTED_KEY, False)

    def previous_run_exhausted_pages(self) -> bool:
        return bool(self._storage.load(self._PAGES_EXHAUSTED_KEY))