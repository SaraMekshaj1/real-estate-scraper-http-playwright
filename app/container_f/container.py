from __future__ import annotations

from dataclasses import dataclass

from app.abstractions.base_client import BaseHTTPClient
from app.abstractions.base_exporter import BaseExporter
from app.abstractions.base_parser import BaseParser
from app.abstractions.base_storage import BaseStorage
from app.config.settings import ScraperSettings, get_settings
from app.orchestration.scraper_engine import ScraperEngine
from app.exporters_f.exporters import build_exporter
from app.storage.checkpoint_storage import build_storage, JsonStorage
from app.storage.failed_url_store import FailedUrlStore
from app.client.playwright_http_client import HTTPClient
from app.parsers.property_parser import PropertyParser
from app.utils.retry_policy import CircuitBreaker, RetryPolicy
from app.utils.rate_limiter import AsyncRateLimiter          # ← NEW
from monitoring.logger import Metrics, metrics as global_metrics, setup_logger
from monitoring import logger


@dataclass
class Container:
    settings:         ScraperSettings
    client:           BaseHTTPClient
    parser:           BaseParser
    exporter:         BaseExporter
    storage:          BaseStorage
    metrics:          Metrics
    engine:           ScraperEngine
    failed_url_store: FailedUrlStore
    rate_limiter:     AsyncRateLimiter                       # ← NEW

    # ── Production wiring ─────────────────────────────────────────────────

    @classmethod
    def build(cls, settings: ScraperSettings | None = None) -> "Container":
        s = settings or get_settings()
        logger = setup_logger(
            name       = "scraper",
            level      = s.log_level,
            log_format = s.log_format,
            log_file   = s.log_file,
        )
        m = global_metrics

        retry_policy = RetryPolicy(
            max_attempts = s.retry_max_attempts,
            base_delay   = s.retry_base_delay_secs,
            max_delay    = s.retry_max_delay_secs,
            jitter       = s.retry_jitter,
        )
        circuit_breaker = CircuitBreaker(
            failure_threshold = s.circuit_breaker_failure_threshold,
            recovery_secs     = s.circuit_breaker_recovery_secs,
        )

        # One shared limiter — all workers funnel through this single instance.
        rate_limiter = AsyncRateLimiter(                     # ← NEW
            rate       = s.requests_per_second,
            burst_size = s.rate_limiter_burst_size,
        )

        client = HTTPClient(
            settings        = s,
            retry_policy    = retry_policy,
            circuit_breaker = circuit_breaker,
        )
        parser           = PropertyParser()
        exporter         = build_exporter(s)
        storage          = build_storage(s.storage_backend, s)
        failed_url_store = FailedUrlStore(path=s.failed_urls_path)

        engine = ScraperEngine(
            client           = client,
            parser           = parser,
            exporter         = exporter,
            storage          = storage,
            settings         = s,
            metrics          = m,
            failed_url_store = failed_url_store,
            rate_limiter     = rate_limiter,                 # ← NEW
        )

        return cls(
            settings         = s,
            client           = client,
            parser           = parser,
            exporter         = exporter,
            storage          = storage,
            metrics          = m,
            engine           = engine,
            failed_url_store = failed_url_store,
            rate_limiter     = rate_limiter,                 # ← NEW
        )

    # ── Test wiring ───────────────────────────────────────────────────────

    @classmethod
    def for_testing(
        cls,
        client:           BaseHTTPClient   | None = None,
        parser:           BaseParser       | None = None,
        exporter:         BaseExporter     | None = None,
        storage:          BaseStorage      | None = None,
        settings:         ScraperSettings  | None = None,
        failed_url_store: FailedUrlStore   | None = None,
        rate_limiter:     AsyncRateLimiter | None = None,    # ← NEW
    ) -> "Container":
        # Import fakes here so production code never takes a hard dep on them.
        from tests.fakes import NullHTTPClient, InMemoryExporter

        s = settings or ScraperSettings(
            app_env      = "development",
            max_pages    = 1,
            worker_count = 1,
            log_format   = "text",
            log_file     = None,
        )
        m = Metrics()

        _client       = client           or NullHTTPClient()
        _parser       = parser           or PropertyParser()
        _exporter     = exporter         or InMemoryExporter()
        _storage      = storage          or JsonStorage(":memory:")
        _failed_store = failed_url_store or FailedUrlStore(path=":memory:")
        # High-rate limiter in tests so they never block on timing.
        _rate_limiter = rate_limiter or AsyncRateLimiter(    # ← NEW
            rate       = 10_000.0,
            burst_size = 10_000,
        )

        engine = ScraperEngine(
            client           = _client,
            parser           = _parser,
            exporter         = _exporter,
            storage          = _storage,
            settings         = s,
            metrics          = m,
            failed_url_store = _failed_store,
            rate_limiter     = _rate_limiter,                # ← NEW
        )
        return cls(
            settings         = s,
            client           = _client,
            parser           = _parser,
            exporter         = _exporter,
            storage          = _storage,
            metrics          = m,
            engine           = engine,
            failed_url_store = _failed_store,
            rate_limiter     = _rate_limiter,                # ← NEW
        )