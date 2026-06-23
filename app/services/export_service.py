from __future__ import annotations

import logging
from typing import Any

from app.abstractions.base_exporter import BaseExporter
from app.services.deduplication_service import DeduplicationService
from monitoring.logger import Metrics

logger = logging.getLogger("scraper")


class ExportService:
    """
    Writes property rows to the configured exporter and checkpoints each one.

    INCREMENTAL BATCHING:
      export_batch() is the primary entry point for the new pipeline. It
      writes one batch and checkpoints it immediately, so progress is
      preserved after every batch. The exporter is kept open across calls
      (open/close is the engine's responsibility) to avoid re-opening the
      file or DB connection on every batch.

      export() (legacy) wraps export_batch() for callers that still hand
      over all rows at once — fully backward-compatible.

    CHECKPOINT ORDERING:
      We always checkpoint after a successful write, never before.
      If the write throws, no rows are marked exported, so they will be
      retried on the next run. Durability first, dedup second.

    BACKWARD COMPATIBILITY:
      BaseExporter.write_batch() defaults to looping write_row(), so any
      exporter that has not overridden write_batch() still works correctly.
    """

    def __init__(
        self,
        exporter: BaseExporter,
        dedup: DeduplicationService,
        metrics: Metrics,
    ) -> None:
        self._exporter = exporter
        self._dedup = dedup
        self._metrics = metrics

    # ── Incremental path (new pipeline) ──────────────────────────────────

    def export_batch(self, rows: list[dict[str, Any]]) -> int:
        """
        Write *rows* to the already-open exporter and checkpoint each one.

        CALLER CONTRACT:
          - The exporter must already be open (engine called open() once
            before the batch loop starts).
          - The engine calls close() after the loop (or on error).
          - This lets the exporter buffer/flush on its own schedule without
            re-opening for every batch.

        Returns the number of rows successfully written and checkpointed.
        """
        if not rows:
            return 0

        # Write — if this throws, no checkpoints are updated.
        self._exporter.write_batch(rows)

        # Checkpoint after a confirmed write.
        count = 0
        for row in rows:
            pid = row.get("property_id")
            if pid:
                self._dedup.mark_exported(pid)
            count += 1

        # Store the most recently exported property ID.
        # This allows the engine to explicitly determine whether
        # the next run is a resume or a normal daily run.
        if rows:
            last_pid = rows[-1].get("property_id")
            if last_pid:
                self._dedup.set_last_exported_id(last_pid)

        logger.info(
            "ExportService: batch — wrote %d rows (last_exported_id=%s)",
            count,
            last_pid,
        )

        self._metrics.inc("export.rows_written", count)
        return count

    # ── Legacy path (backward-compatible) ────────────────────────────────

    def export(self, rows: list[dict[str, Any]]) -> int:
        """
        Write all rows in one shot (opens and closes the exporter).

        Kept for callers that were written against the old single-call API.
        Internally delegates to export_batch() so the logic stays in one
        place.
        """
        if not rows:
            logger.info("ExportService: nothing to export")
            return 0

        with self._exporter:
            count = self.export_batch(rows)

        logger.info("ExportService: wrote %d rows total", count)
        self._metrics.gauge("export.rows_written", count)
        return count