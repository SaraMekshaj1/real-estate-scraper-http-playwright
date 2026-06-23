from __future__ import annotations
import asyncio
import logging
import random
import time
from typing import Any
from app.abstractions.base_client import BaseHTTPClient
from app.config.settings import ScraperSettings
from app.utils.retry_policy import CircuitBreaker, CircuitOpenError, RetryPolicy
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1280, "height": 800},
    {"width": 1366, "height": 768},
]

_BROWSER_DEAD_SIGNALS = (
    "Target page, context or browser has been closed",
    "Browser has been closed",
    "Connection closed",
    "Target closed",
)


class HTTPClient(BaseHTTPClient):
    def __init__(
        self,
        settings:        ScraperSettings,
        retry_policy:    RetryPolicy    | None = None,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self._settings        = settings
        self._retry_policy    = retry_policy    or RetryPolicy()
        self._circuit_breaker = circuit_breaker or CircuitBreaker()
        self._delay_multiplier: float = 1.0
        self._warmed_up: bool = False

        self._playwright = None
        self._browser    = None
        self._context    = None

    # ── Playwright setup ──────────────────────────────────────────────────

    async def _ensure_browser(self) -> None:
        if self._context is not None:
            return
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )

        ua = (
            random.choice(_USER_AGENTS)
            if self._settings.rotate_user_agents
            else _USER_AGENTS[0]
        )

        self._context = await self._browser.new_context(
            user_agent=ua,
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="Europe/Tirane",
            java_script_enabled=True,
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
            },
        )

        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3] });
        """)

        logger.info("Playwright browser launched (headless Chromium)")

    # ── Browser restart ───────────────────────────────────────────────────

    async def _restart_browser(self) -> None:
        """Tear down the dead Playwright instance and re-launch cleanly."""
        logger.info("Restarting Playwright browser...")

        context, browser, playwright = self._context, self._browser, self._playwright
        self._context    = None
        self._browser    = None
        self._playwright = None
        self._warmed_up  = False

        for name, resource, closer in (
            ("context",    context,    lambda obj: obj.close()),
            ("browser",    browser,    lambda obj: obj.close()),
            ("playwright", playwright, lambda obj: obj.stop()),
        ):
            if resource is None:
                continue
            try:
                await closer(resource)
            except Exception as exc:
                logger.debug("Cleanup of %s during restart (non-fatal): %s", name, exc)

        await self._ensure_browser()
        logger.info("Playwright browser restarted successfully")

    # ── Adaptive delay ────────────────────────────────────────────────────

    async def _adaptive_delay(self) -> None:
        base = random.gauss(
            mu=(
                self._settings.request_delay_min_secs
                + self._settings.request_delay_max_secs
            ) / 2,
            sigma=(
                self._settings.request_delay_max_secs
                - self._settings.request_delay_min_secs
            ) / 4,
        )
        delay = max(self._settings.request_delay_min_secs, base) * self._delay_multiplier
        logger.debug("Delay: %.2fs (multiplier=%.1fx)", delay, self._delay_multiplier)
        await asyncio.sleep(delay)

    def _on_success(self) -> None:
        self._circuit_breaker.record_success()
        if self._delay_multiplier > 1.0:
            self._delay_multiplier = max(1.0, self._delay_multiplier * 0.9)

    def _on_rate_limited(self) -> None:
        self._circuit_breaker.record_failure()
        self._delay_multiplier = min(20.0, self._delay_multiplier * 2.0)
        logger.warning("Rate limited — delay multiplier now %.1fx", self._delay_multiplier)

    def _on_error(self) -> None:
        self._circuit_breaker.record_failure()

    # ── Session warmup ────────────────────────────────────────────────────

    async def warmup(self, base_url: str) -> None:
        if self._warmed_up:
            return

        await self._ensure_browser()

        from urllib.parse import urlparse
        homepage = f"{urlparse(base_url).scheme}://{urlparse(base_url).netloc}/"

        logger.info("Session warmup: visiting %s", homepage)
        page = await self._context.new_page()
        try:
            await page.goto(homepage, wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(random.uniform(1.5, 3.0))
            logger.info("Warmup complete")
        except Exception as exc:
            logger.warning("Warmup failed (non-fatal): %s", exc)
        finally:
            self._warmed_up = True
            try:
                await page.close()
            except Exception as exc:
                logger.debug("Warmup page close error (non-fatal): %s", exc)

    # ── Core fetch ────────────────────────────────────────────────────────

    async def get_async(self, url: str, referer: str | None = None, **kwargs) -> str:
        max_attempts = self._settings.retry_max_attempts
        last_exc: Exception | None = None

        for attempt in range(1, max_attempts + 1):

            await self._ensure_browser()

            if not self._circuit_breaker.allow_request():
                raise CircuitOpenError(
                    f"Circuit breaker is OPEN — aborting request to {url}"
                )

            await self._adaptive_delay()

            page = None
            try:
                page = await self._context.new_page()

                if referer:
                    await page.set_extra_http_headers({"Referer": referer})

                response = await page.goto(
                    url,
                    wait_until="networkidle",
                    timeout=30_000,
                )

                if response is None:
                    raise RuntimeError(f"No response for {url}")

                status = response.status

                if status in (429, 503):
                    self._on_rate_limited()
                    retry_after = self._retry_policy.compute_delay(
                        attempt - 1,
                        retry_after=float(response.headers.get("retry-after", 10)),
                    )
                    logger.warning(
                        "HTTP %d — sleeping %.0fs before retry (attempt %d/%d)",
                        status, retry_after, attempt, max_attempts,
                    )
                    await page.close()
                    page = None
                    await asyncio.sleep(retry_after)
                    continue

                if status >= 400:
                    self._on_error()
                    raise RuntimeError(f"HTTP {status} for {url}")

                html = await page.content()

                self._on_success()
                return html

            except asyncio.CancelledError:
                # ── Task cancelled (e.g. early-stop shutdown) ─────────────
                # Re-raise immediately — do NOT retry, do NOT log as an error.
                # The finally block below will still close the page cleanly.
                logger.debug("get_async cancelled for %s — propagating.", url)
                raise

            except (CircuitOpenError):
                # CircuitOpenError: always re-raise, never retry.
                raise

            except Exception as exc:
                last_exc = exc
                exc_msg  = str(exc)

                # ── Detect browser/context death by message ────────────────
                if any(sig in exc_msg for sig in _BROWSER_DEAD_SIGNALS):
                    logger.warning(
                        "Browser appears dead (attempt %d/%d): %s — "
                        "restarting Playwright...",
                        attempt, max_attempts, exc_msg,
                    )
                    await self._restart_browser()
                    await asyncio.sleep(3.0)
                    continue

                self._on_error()
                logger.warning(
                    "Attempt %d/%d failed for %s: %s", attempt, max_attempts, url, exc
                )
                if attempt < max_attempts:
                    delay = self._retry_policy.compute_delay(attempt - 1)
                    await asyncio.sleep(delay)

            finally:
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass

        raise RuntimeError(
            f"All {max_attempts} attempts failed for {url}"
        ) from last_exc

    # ── Sync wrapper ──────────────────────────────────────────────────────

    def get(self, url: str, **kwargs) -> str:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is None:
            return asyncio.run(self.get_async(url, **kwargs))

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, self.get_async(url, **kwargs))
            return future.result()

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def close(self) -> None:
        if not any((self._context, self._browser, self._playwright)):
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is None:
            asyncio.run(self.aclose())
        else:
            loop.create_task(self.aclose())
            logger.debug("Playwright async close scheduled on running loop")

    async def aclose(self) -> None:
        context, browser, playwright = self._context, self._browser, self._playwright
        self._context    = None
        self._browser    = None
        self._playwright = None
        self._warmed_up  = False

        for name, resource, closer in (
            ("context",    context,    lambda obj: obj.close()),
            ("browser",    browser,    lambda obj: obj.close()),
            ("playwright", playwright, lambda obj: obj.stop()),
        ):
            if resource is None:
                continue
            try:
                await closer(resource)
            except Exception as exc:
                logger.debug(
                    "Playwright %s cleanup skipped: %s", name, exc, exc_info=True
                )

        logger.info("Playwright browser closed")