from __future__ import annotations
import csv
import json
import logging
import os
from pathlib import Path
from typing import Any
from app.abstractions.base_exporter import BaseExporter

logger = logging.getLogger(__name__)

FIELDNAMES = [
    "property_id", "url", "title", "price", "price_currency",
    "location", "description", "images",
    "total_area", "internal_area", "number_of_bedrooms", "floor",
    "status", "type", "furnished", "mortgage", "elevator",
    "number_of_toilets", "characteristics",
]

# How many rows to buffer before flushing file-based exporters.
# Tunable via env var; 0 means flush only on close() (max throughput).
_FILE_FLUSH_EVERY = int(os.getenv("EXPORTER_FLUSH_EVERY", "50"))


# ── CSV Exporter ──────────────────────────────────────────────────────────────

class CSVExporter(BaseExporter):
    """
    Writes rows to a UTF-8-BOM CSV file (Excel-friendly).

    FLUSH STRATEGY:
      Per-row flush (fsync on every write_row) is expensive when writing
      thousands of rows.  We flush every _FILE_FLUSH_EVERY rows and always
      on close(), giving a good balance between durability and throughput.
      write_batch() resets the counter so a full batch gets exactly one flush.
    """

    def __init__(self, path: str = "output/results.csv") -> None:
        self._path             = path
        self._file             = None
        self._writer           = None
        self._rows_since_flush = 0

    def open(self) -> None:
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)

        file_exists = Path(self._path).exists() and Path(self._path).stat().st_size > 0
        has_header  = False
        if file_exists:
            with open(self._path, "r", encoding="utf-8-sig") as f:
                first_line = f.readline().strip()
                has_header = (
                    first_line == ",".join(f'"{col}"' for col in FIELDNAMES)
                    or first_line == ",".join(FIELDNAMES)
                )
        mode = "a" if file_exists else "w"

        self._file = open(self._path, mode, newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(
            self._file,
            fieldnames   = FIELDNAMES,
            extrasaction = "ignore",
            quoting      = csv.QUOTE_ALL,
        )

        if not has_header:
            self._writer.writeheader()
            self._file.flush()

        self._rows_since_flush = 0
        logger.info(
            "CSVExporter opened (%s): %s",
            "append" if file_exists else "new",
            self._path,
        )

    def write_row(self, row: dict[str, Any]) -> None:
        if not self._writer:
            raise RuntimeError("Call open() first")
        normalised = {k: (row.get(k) if row.get(k) is not None else "") for k in FIELDNAMES}
        self._writer.writerow(normalised)

        self._rows_since_flush += 1
        if _FILE_FLUSH_EVERY and self._rows_since_flush >= _FILE_FLUSH_EVERY:
            self._file.flush()
            self._rows_since_flush = 0

    def write_batch(self, rows: list[dict[str, Any]]) -> None:
        """Write all rows then flush once."""
        if not self._writer:
            raise RuntimeError("Call open() first")
        for row in rows:
            normalised = {k: (row.get(k) if row.get(k) is not None else "") for k in FIELDNAMES}
            self._writer.writerow(normalised)
        self._file.flush()
        self._rows_since_flush = 0

    def close(self) -> None:
        if self._file:
            self._file.flush()
            self._file.close()
            self._file   = None
            self._writer = None
            logger.info("CSVExporter closed: %s", self._path)


# ── JSON Exporter ─────────────────────────────────────────────────────────────

class JSONExporter(BaseExporter):
    """
    Writes a JSON-Lines file (one JSON object per line).

    WHY JSON-Lines over a JSON array:
      - Streamable — can be read line-by-line without loading the whole file.
      - Appendable — safe to add rows after crashes (no trailing comma issue).
      - Directly ingestible by Spark, BigQuery, and most log aggregators.

    FLUSH STRATEGY: identical to CSVExporter — periodic + on close/batch.
    """

    def __init__(self, path: str = "output/results.jsonl") -> None:
        self._path             = path
        self._file             = None
        self._rows_since_flush = 0

    def open(self) -> None:
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)

        file_exists = Path(self._path).exists() and Path(self._path).stat().st_size > 0
        mode        = "a" if file_exists else "w"

        self._file = open(self._path, mode, encoding="utf-8")
        self._rows_since_flush = 0
        logger.info(
            "JSONExporter opened (%s): %s",
            "append" if file_exists else "new",
            self._path,
        )

    def write_row(self, row: dict[str, Any]) -> None:
        if not self._file:
            raise RuntimeError("Call open() first")
        self._file.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

        self._rows_since_flush += 1
        if _FILE_FLUSH_EVERY and self._rows_since_flush >= _FILE_FLUSH_EVERY:
            self._file.flush()
            self._rows_since_flush = 0

    def write_batch(self, rows: list[dict[str, Any]]) -> None:
        """Write all rows as a contiguous block, then flush once."""
        if not self._file:
            raise RuntimeError("Call open() first")
        self._file.write(
            "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows) + "\n"
        )
        self._file.flush()
        self._rows_since_flush = 0

    def close(self) -> None:
        if self._file:
            self._file.flush()
            self._file.close()
            self._file = None
            logger.info("JSONExporter closed: %s", self._path)


# ── PostgreSQL Exporter ───────────────────────────────────────────────────────

class PostgresExporter(BaseExporter):
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS properties (
        property_id        TEXT PRIMARY KEY,
        url                TEXT,
        title              TEXT,
        price              NUMERIC,
        price_currency     TEXT,
        location           TEXT,
        description        TEXT,
        images             TEXT,
        total_area         TEXT,
        internal_area      TEXT,
        number_of_bedrooms TEXT,
        floor              TEXT,
        status             TEXT,
        type               TEXT,
        furnished          TEXT,
        mortgage           TEXT,
        elevator           TEXT,
        number_of_toilets  TEXT,
        characteristics    TEXT,
        scraped_at         TIMESTAMPTZ DEFAULT NOW()
    );
    """

    def __init__(self, dsn: str) -> None:
        self._dsn  = dsn
        self._conn = None

    def open(self) -> None:
        try:
            import psycopg2
            self._conn = psycopg2.connect(self._dsn)
            with self._conn.cursor() as cur:
                cur.execute(self.CREATE_TABLE_SQL)
            self._conn.commit()
            logger.info("PostgresExporter connected")
        except ImportError:
            raise RuntimeError(
                "psycopg2-binary is required for PostgresExporter. "
                "Install it with: pip install psycopg2-binary"
            )

    def write_row(self, row: dict[str, Any]) -> None:
        """
        Single-row upsert. Commits immediately so the row is durable.
        For bulk writes, prefer write_batch() — it defers the commit until
        all rows are staged, reducing round-trips to the DB by N-1.
        """
        if not self._conn:
            raise RuntimeError("Call open() first")
        self._upsert_rows([row])
        self._conn.commit()

    def write_batch(self, rows: list[dict[str, Any]]) -> None:
        """
        Bulk upsert in a single transaction.

        WHY THIS IS FASTER than N × write_row():
          1. One BEGIN / COMMIT instead of N — eliminates N-1 round-trips.
          2. psycopg2 executemany() batches parameter binding server-side.
          3. The DB can optimise the whole set as one plan (index updates,
             WAL writes, lock acquisition) rather than N independent plans.
        """
        if not self._conn:
            raise RuntimeError("Call open() first")
        if not rows:
            return

        cols = [c for c in FIELDNAMES if any(r.get(c) is not None for r in rows)]

        placeholders = ", ".join(["%s"] * len(cols))
        updates      = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "property_id")
        sql = (
            f"INSERT INTO properties ({', '.join(cols)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT (property_id) DO UPDATE SET {updates};"
        )

        values = [[row.get(c) for c in cols] for row in rows]

        with self._conn.cursor() as cur:
            cur.executemany(sql, values)
        self._conn.commit()
        logger.debug("PostgresExporter: upserted %d rows in one transaction", len(rows))

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("PostgresExporter disconnected")


# ── Composite Exporter ────────────────────────────────────────────────────────

class CompositeExporter(BaseExporter):
    """
    Fans out write_row() / write_batch() to all child exporters.

    DESIGN PATTERN: Composite — the engine calls one exporter; the
    composite delegates to all children transparently.  Each child benefits
    from its own batching optimisation (e.g. Postgres gets a single
    transaction; CSV gets a single flush).
    """

    def __init__(self, exporters: list[BaseExporter]) -> None:
        self._exporters = exporters

    def open(self) -> None:
        for exp in self._exporters:
            exp.open()

    def write_row(self, row: dict[str, Any]) -> None:
        for exp in self._exporters:
            try:
                exp.write_row(row)
            except Exception as exc:
                logger.error("Exporter %s failed on write_row: %s", type(exp).__name__, exc)

    def write_batch(self, rows: list[dict[str, Any]]) -> None:
        """Delegate to each child's write_batch so every exporter can optimise."""
        for exp in self._exporters:
            try:
                exp.write_batch(rows)
            except Exception as exc:
                logger.error("Exporter %s failed on write_batch: %s", type(exp).__name__, exc)

    def close(self) -> None:
        for exp in self._exporters:
            try:
                exp.close()
            except Exception as exc:
                logger.error("Error closing exporter %s: %s", type(exp).__name__, exc)


# ── Exporter factory ──────────────────────────────────────────────────────────

def build_exporter(settings) -> BaseExporter:
    from app.config.settings import ScraperSettings
    s: ScraperSettings = settings

    active: list[BaseExporter] = []

    for name in s.exporter_list:
        if name == "csv":
            active.append(CSVExporter(s.csv_output_path))
        elif name == "json":
            path = s.csv_output_path.replace(".csv", ".jsonl")
            active.append(JSONExporter(path))
        elif name == "postgres":
            if not s.postgres_dsn:
                raise ValueError("POSTGRES_DSN must be set when postgres exporter is active")
            active.append(PostgresExporter(s.postgres_dsn))
        else:
            raise ValueError(f"Unknown exporter: {name!r}")

    if not active:
        raise ValueError("No exporters configured")

    return CompositeExporter(active) if len(active) > 1 else active[0]