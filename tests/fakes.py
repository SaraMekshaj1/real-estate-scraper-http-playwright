from __future__ import annotations
"""
Test doubles for use in unit and integration tests.

Import from here, never from production modules.  Keeping fakes out of
app/ ensures that no test-only code ever ships in the production bundle.
"""
from app.abstractions.base_client import BaseHTTPClient
from app.abstractions.base_exporter import BaseExporter


class NullHTTPClient(BaseHTTPClient):
    """
    HTTP client that returns empty HTML without touching the network.
    Safe to use in any test that does not care about page content.
    """

    def get(self, url: str, **kwargs) -> str:
        return "<html></html>"

    async def get_async(self, url: str, **kwargs) -> str:
        return "<html></html>"

    def close(self) -> None:
        pass

    async def aclose(self) -> None:
        pass


class InMemoryExporter(BaseExporter):
    """
    Exporter that collects rows in a plain list instead of writing to disk.
    Inspect ``exporter.rows`` after the pipeline runs to assert on output.
    """

    def __init__(self) -> None:
        self.rows: list[dict] = []

    def open(self) -> None:
        pass

    def write_row(self, row: dict) -> None:
        self.rows.append(dict(row))

    def close(self) -> None:
        pass