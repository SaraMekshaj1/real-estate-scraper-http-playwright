from __future__ import annotations
import json
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator

class JSONFormatter(logging.Formatter):
    """Serialises each LogRecord as a single JSON line."""

    def __init__(self, extra_fields: dict[str, Any] | None = None) -> None:
        super().__init__()
        self._extra = extra_fields or {}

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts":       datetime.now(timezone.utc).isoformat(),
            "level":    record.levelname,
            "logger":   record.name,
            "msg":      record.getMessage(),
            "file":     f"{record.filename}:{record.lineno}",
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)

        # Merge any extra fields attached via logger.info("…", extra={…})
        for k, v in record.__dict__.items():
            if k not in logging.LogRecord.__dict__ and not k.startswith("_"):
                payload[k] = v

        payload.update(self._extra)
        return json.dumps(payload, default=str)


# ── Metrics (lightweight, in-process) ────────────────────────────────────────

class Metrics:
    """
    Simple in-process counter/gauge store.

    WHY: A full Prometheus client is overkill for a single-process
         scraper.  This gives us the same semantics with zero deps.
         In a production deployment, swap this for
         `prometheus_client.Counter` / `Gauge` — the call sites are
         identical.
    """

    def __init__(self) -> None:
        self._counters: dict[str, float] = {}
        self._gauges:   dict[str, float] = {}

    def inc(self, name: str, value: float = 1.0, labels: dict | None = None) -> None:
        key = self._key(name, labels)
        self._counters[key] = self._counters.get(key, 0.0) + value

    def gauge(self, name: str, value: float, labels: dict | None = None) -> None:
        key = self._key(name, labels)
        self._gauges[key] = value

    def get(self, name: str, labels: dict | None = None) -> float:
        return self._counters.get(self._key(name, labels), 0.0)

    def snapshot(self) -> dict[str, Any]:
        return {"counters": dict(self._counters), "gauges": dict(self._gauges)}

    @staticmethod
    def _key(name: str, labels: dict | None) -> str:
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


# ── Timing context manager ────────────────────────────────────────────────────

@contextmanager
def timed(
    logger: logging.Logger,
    operation: str,
    metrics: Metrics | None = None,
    metric_name: str | None = None,
) -> Generator[None, None, None]:
    """Log and (optionally) record the wall-clock duration of a block.

    Usage::

        with timed(logger, "parse_property", metrics, "parse_duration_secs"):
            result = parser.parse(html)
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.debug("⏱  %s completed in %.3fs", operation, elapsed)
        if metrics and metric_name:
            metrics.inc(metric_name, elapsed)


# ── Logger factory ────────────────────────────────────────────────────────────

def setup_logger(
    name: str = "scraper",
    level: str = "INFO",
    log_format: str = "json",
    log_file: str | None = "scraper1.log",
    extra_fields: dict[str, Any] | None = None,
) -> logging.Logger:
    """
    Build and return a configured logger.

    Args:
        name:         Logger name (use __name__ in modules).
        level:        Log level string (DEBUG / INFO / WARNING / ERROR).
        log_format:   'json' for production, 'text' for local dev.
        log_file:     Path to log file; None = console only.
        extra_fields: Arbitrary fields embedded in every JSON record
                      (e.g. {"job_id": "abc123", "worker": "w1"}).
    """
    log = logging.getLogger(name)
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    log.propagate = False

    if log.handlers:
        return log  # already configured

    if log_format == "json":
        formatter: logging.Formatter = JSONFormatter(extra_fields)
    else:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    log.addHandler(console)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        log.addHandler(fh)

    return log


# ── Module-level singletons (override in main.py via setup_logger) ────────────
metrics = Metrics()