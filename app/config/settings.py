from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ScraperSettings(BaseSettings):
    batch_size: int = 50
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Concurrency ───────────────────────────────────────────────────────
    worker_count: int = Field(
        default=3,
        ge=1,
        le=50,
        description="Number of async scraper workers running in parallel.",
    )
    queue_max_size: int = Field(default=0)

    per_url_timeout_secs: float = Field(
        default=120.0,
        gt=0,
        description=(
            "Hard deadline per URL fetch+parse including all retries. "
            "Worker skips the URL on TimeoutError instead of hanging. "
            "Should be > (page_goto_timeout=30s × retry_max_attempts=4)."
        ),
    )
    result_queue_watchdog_secs: float = Field(
        default=300.0,
        gt=0,
        description=(
            "How long the orchestrator waits on result_queue.get() before "
            "checking whether all worker tasks are already done. Safety net "
            "against a lost sentinel causing a permanent hang."
        ),
    )
    playwright_wait_until: Literal["domcontentloaded", "load", "networkidle"] = Field(
        default="domcontentloaded",
        description=(
            "Playwright page.goto wait_until strategy. "
            "'networkidle' can block indefinitely on pages with persistent "
            "connections. 'domcontentloaded' is safe for server-rendered pages."
        ),
    )

    # ── Target ────────────────────────────────────────────────────────────
    base_url: str = Field(
        default="https://www.century21albania.com/en/properties",
        description="Landing page URL for the property listing.",
    )
    max_pages: int | None = Field(default=None, ge=1)

    # ── HTTP client ───────────────────────────────────────────────────────
    http_timeout_secs: float = Field(default=20.0, gt=0)
    http_max_connections: int = Field(default=5, ge=1)
    http_max_keepalive: int = Field(default=3, ge=1)

    # ── Retry / circuit-breaker ───────────────────────────────────────────
    retry_max_attempts: int = Field(default=4, ge=1, le=10)
    retry_base_delay_secs: float = Field(default=2.0, gt=0)
    retry_max_delay_secs: float = Field(default=120.0, gt=0)
    retry_jitter: bool = Field(default=True)
    circuit_breaker_failure_threshold: int = Field(default=100)
    circuit_breaker_recovery_secs: float = Field(default=60.0)

    # ── Rate limiting ─────────────────────────────────────────────────────
    request_delay_min_secs: float = Field(default=3.0, ge=0)
    request_delay_max_secs: float = Field(default=6.0, ge=0)
    requests_per_second: float = Field(
        default=0.3,
        gt=0,
        description=(
            "Global async rate limit shared across all workers. "
            "At default 0.5 req/s with min_delay=2s this matches the existing "
            "per-request delay budget. Raise carefully on sites without WAF."
        ),
    )
    rate_limiter_burst_size: int = Field(
        default=1,
        ge=1,
        description=(
            "Token-bucket burst capacity. Controls how many requests can fire "
            "back-to-back before throttling kicks in. Keep low (1-3) for "
            "polite scraping; raise only for high-throughput internal targets."
        ),
    )

    # ── Anti-blocking ─────────────────────────────────────────────────────
    warmup_enabled: bool = Field(
        default=True,
        description="Visit homepage before scraping to acquire session cookies.",
    )
    referer_enabled: bool = Field(
        default=True,
        description="Pass the listing page URL as Referer when fetching detail pages.",
    )

    # ── Storage / output ──────────────────────────────────────────────────
    output_dir: str = Field(default="output")
    csv_output_path: str = Field(default="output/results.csv")
    checkpoint_path: str = Field(default="output/checkpoints.json")
    storage_backend: Literal["json", "sqlite", "redis"] = Field(default="json")
    redis_url: str | None = Field(default=None)

    # ── Failed URL retry store ────────────────────────────────────────────
    failed_urls_path: str = Field(
        default="output/failed_urls.json",
        description=(
            "Path to the JSON file that persists retryable failed detail-page "
            "URLs across runs.  Consumed by retry_failed_urls.py."
        ),
    )
    max_retry_attempts: int = Field(
        default=5,
        ge=1,
        description=(
            "URLs whose attempt count exceeds this value are dropped from the "
            "retry queue and assumed permanently unreachable."
        ),
    )

    # ── Raw HTML store ────────────────────────────────────────────────────
    raw_store_backend: Literal["null", "file", "s3"] = Field(
        default="null",
        description=(
            "Where to persist raw fetched HTML. "
            "'null' = disabled (default, zero overhead). "
            "'file' = gzip files on local disk (single machine). "
            "'s3'   = AWS S3 bucket (distributed / cloud)."
        ),
    )
    raw_pages_dir: str = Field(
        default="output/raw_pages",
        description="Local directory for FileRawStore .html.gz files.",
    )
    s3_raw_bucket: str | None = Field(
        default=None,
        description="S3 bucket name. Required when raw_store_backend='s3'.",
    )
    s3_raw_prefix: str = Field(
        default="raw_pages",
        description="S3 key prefix for raw HTML objects.",
    )

    # ── Exporters ─────────────────────────────────────────────────────────
    exporters: str = Field(default="csv")
    postgres_dsn: str | None = Field(default=None)

    # ── Proxy / session rotation ──────────────────────────────────────────
    proxy_list: str | None = Field(default=None)
    rotate_user_agents: bool = Field(default=True)

    # ── Observability ─────────────────────────────────────────────────────
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    log_format: Literal["json", "text"] = Field(default="text")
    log_file: str | None = Field(default="scraper.log")
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9090)

    # ── Environment ───────────────────────────────────────────────────────
    app_env: Literal["development", "staging", "production"] = Field(default="development")

    @field_validator("exporters")
    @classmethod
    def validate_exporters(cls, v: str) -> str:
        allowed = {"csv", "json", "postgres"}
        requested = {e.strip() for e in v.split(",")}
        unknown = requested - allowed
        if unknown:
            raise ValueError(f"Unknown exporters: {unknown}. Allowed: {allowed}")
        return v

    @field_validator("raw_store_backend")
    @classmethod
    def validate_raw_store(cls, v: str, info) -> str:
        if v == "s3":
            # Actual bucket presence check is deferred to container.py build time.
            pass
        return v

    @property
    def exporter_list(self) -> list[str]:
        return [e.strip() for e in self.exporters.split(",")]

    @property
    def proxy_urls(self) -> list[str]:
        if not self.proxy_list:
            return []
        return [p.strip() for p in self.proxy_list.split(",") if p.strip()]

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"


@lru_cache(maxsize=1)
def get_settings() -> ScraperSettings:
    return ScraperSettings()