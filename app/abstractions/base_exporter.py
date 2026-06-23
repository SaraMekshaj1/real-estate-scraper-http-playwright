from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any


class BaseExporter(ABC):
    """
    Contract for all data exporters (CSV, JSON, DB, S3 …).

    BATCHING DESIGN (ISP / OCP):
      - write_batch() is non-abstract so existing exporters need zero changes
        (Open/Closed Principle — extend without modifying consumers).
      - Subclasses that can do better than N × write_row() should override it
        (e.g. PostgresExporter uses a single multi-row INSERT + one commit).
      - The default implementation guarantees correctness for free; performance
        optimisation is opt-in.
    """

    @abstractmethod
    def open(self) -> None:
        """Prepare the output sink (open file, acquire DB connection …)."""

    @abstractmethod
    def write_row(self, row: dict[str, Any]) -> None:
        """Persist a single property row.  Must be idempotent on duplicate
        ``property_id`` values (see deduplication notes in StorageService)."""

    def write_batch(self, rows: list[dict[str, Any]]) -> None:
        """
        Persist multiple rows in one call.

        DEFAULT: iterates write_row() — correct for every exporter,
        regardless of whether it overrides this method.

        OVERRIDE to gain:
          • DB exporters  — one transaction + one commit instead of N commits.
          • File exporters — one flush at the end instead of N flushes.
          • Network sinks  — one HTTP request / batch API call.

        Subclasses that override must honour the same idempotency guarantee
        as write_row().
        """
        for row in rows:
            self.write_row(row)

    @abstractmethod
    def close(self) -> None:
        """Flush buffers and release resources."""

    # ── Async variants (optional override) ───────────────────────────────

    async def write_row_async(self, row: dict[str, Any]) -> None:
        """Default: run sync write_row in the event-loop's thread executor."""
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.write_row, row)

    async def write_batch_async(self, rows: list[dict[str, Any]]) -> None:
        """Default: run sync write_batch in the event-loop's thread executor."""
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.write_batch, rows)

    # ── Context-manager helpers ───────────────────────────────────────────

    def __enter__(self) -> "BaseExporter":
        self.open()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    async def __aenter__(self) -> "BaseExporter":
        self.open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        self.close()