from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any                                                                                                           

class BaseSyncHTTPClient(ABC):
    @abstractmethod
    def get(self, url: str, **kwargs: Any) -> str:
        """Fetch *url* and return the response body as text.
        Must raise :class:`RuntimeError` after all retries are exhausted.
        """

    @abstractmethod
    def close(self) -> None:
        """Release any underlying connection pool or session."""

    def __enter__(self) -> "BaseSyncHTTPClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class BaseAsyncHTTPClient(ABC):
   # Async-only HTTP contract.
   #Used by :class:`~app.workers.async_workers.ScraperWorker`.

    @abstractmethod
    async def get_async(self, url: str, **kwargs: Any) -> str:
        """Async fetch — returns response body as text."""

    @abstractmethod
    async def aclose(self) -> None:
        """Async teardown."""

    async def __aenter__(self) -> "BaseAsyncHTTPClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()


class BaseHTTPClient(BaseSyncHTTPClient, BaseAsyncHTTPClient):
    """
    Combined sync + async contract.

    Use this for production clients (e.g. :class:`~app.client.playwright_http_client.HTTPClient`)
    that need to satisfy both sync call-sites and the async worker pipeline.

    Code that only needs async should type-hint :class:`BaseAsyncHTTPClient`.
    Code that only needs sync should type-hint :class:`BaseSyncHTTPClient`.
    This keeps ISP satisfied: callers depend only on the slice they use.
    """