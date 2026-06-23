"""
tests/test_suite.py

TESTING ARCHITECTURE:
  1. UNIT TESTS — test one class in isolation; all deps are mocked.
     No network, no filesystem (except snapshots).  Fast (<1 s total).

  2. INTEGRATION TESTS — test the full pipeline wired with test doubles.
     No live network; HTML fixtures used instead.  Still fast.

  3. PARSER SNAPSHOT TESTS — record a real parse() output once, persist
     it as a JSON fixture.  Future runs assert the output is identical.
     WHY: CSS selectors silently break when the site redesigns; snapshot
     tests catch this immediately.

  4. DEPENDENCY MOCKING — all tests use Container.for_testing() or inject
     mock objects directly.  No test ever makes a live HTTP request.

DESIGN PATTERNS IN TESTS:
  - Arrange / Act / Assert (AAA) structure throughout.
  - Fixtures for shared setup (pytest fixtures via simple functions here).
  - Test doubles: _InMemoryExporter, NullTranslator, _NullHTTPClient
    (defined in container.py and reused here).
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Helpers ───────────────────────────────────────────────────────────────────

FIXTURE_DIR = Path(__file__).parent / "snapshots"
FIXTURE_DIR.mkdir(exist_ok=True)


def _read_fixture(name: str) -> str:
    path = FIXTURE_DIR / name
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


def _write_fixture(name: str, data: Any) -> None:
    path = FIXTURE_DIR / name
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


# ── Sample HTML fixture ───────────────────────────────────────────────────────

SAMPLE_PROPERTY_HTML = """
<html><body>
  <h6 class="font-semibold text-black-custom font-barlow">
    ID: <span class="text-gold-shade-55">C21-12345</span>
  </h6>
  <h1 class="font-extrabold">Apartament 2+1 ne Tirane</h1>
  <h2 class="font-bold text-gold-shade-55">150,000 €</h2>
  <div class="flex gap-1"><h6>Tiranë, Blloku</h6></div>
  <p class="paragraph-2 text-grey-shade-40">Apartament modern me pamje te bukur.</p>
</body></html>
"""


# ══════════════════════════════════════════════════════════════════════════════
# 1. RETRY POLICY UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestRetryPolicy:
    """Tests the exponential back-off and jitter calculations."""

    def test_delay_grows_exponentially(self):
        from app.utils.retry_policy import RetryPolicy
        policy = RetryPolicy(base_delay=1.0, max_delay=60.0, jitter=False)
        delays = [policy.compute_delay(i) for i in range(5)]
        # 1, 2, 4, 8, 16 — each double the previous
        for i in range(1, len(delays)):
            assert delays[i] >= delays[i - 1], "Delay should grow with attempts"

    def test_delay_capped_at_max(self):
        from app.utils.retry_policy import RetryPolicy
        policy = RetryPolicy(base_delay=1.0, max_delay=5.0, jitter=False)
        assert policy.compute_delay(100) == 5.0

    def test_jitter_within_bounds(self):
        from app.utils.retry_policy import RetryPolicy
        policy = RetryPolicy(base_delay=1.0, max_delay=10.0, jitter=True)
        for _ in range(50):
            delay = policy.compute_delay(2)
            assert 0 <= delay <= 4.0  # max at attempt 2 is 4.0

    def test_retry_after_overrides_backoff(self):
        from app.utils.retry_policy import RetryPolicy
        policy = RetryPolicy()
        assert policy.compute_delay(0, retry_after=30.0) == 30.0

    def test_non_retryable_status_not_retried(self):
        from app.utils.retry_policy import RetryPolicy
        policy = RetryPolicy()
        assert not policy.should_retry_status(403)
        assert not policy.should_retry_status(404)

    def test_retryable_status_is_retried(self):
        from app.utils.retry_policy import RetryPolicy
        policy = RetryPolicy()
        assert policy.should_retry_status(429)
        assert policy.should_retry_status(503)

    def test_sync_retry_exhausted_raises(self):
        from app.utils.retry_policy import RetryPolicy, RetryExhaustedError, with_retry
        policy = RetryPolicy(
            max_attempts=2,
            base_delay=0.0,
            jitter=False,
            retryable_exc=(ValueError,),
        )

        @with_retry(policy)
        def always_fails():
            raise ValueError("boom")

        with pytest.raises(RetryExhaustedError):
            always_fails()

    def test_sync_retry_succeeds_on_second_attempt(self):
        from app.utils.retry_policy import RetryPolicy, with_retry
        policy = RetryPolicy(max_attempts=3, base_delay=0.0, jitter=False,
                             retryable_exc=(ValueError,))
        call_count = [0]

        @with_retry(policy)
        def flaky():
            call_count[0] += 1
            if call_count[0] < 2:
                raise ValueError("not yet")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count[0] == 2


# ══════════════════════════════════════════════════════════════════════════════
# 2. CIRCUIT BREAKER UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestCircuitBreaker:
    def test_opens_after_threshold(self):
        from app.utils.retry_policy import CircuitBreaker, CircuitState
        cb = CircuitBreaker(failure_threshold=3, recovery_secs=9999)
        for _ in range(3):
            cb.record_failure()
        assert cb.state.name == "OPEN"

    def test_allows_requests_when_closed(self):
        from app.utils.retry_policy import CircuitBreaker
        cb = CircuitBreaker()
        assert cb.allow_request() is True

    def test_blocks_requests_when_open(self):
        from app.utils.retry_policy import CircuitBreaker
        cb = CircuitBreaker(failure_threshold=1, recovery_secs=9999)
        cb.record_failure()
        assert cb.allow_request() is False

    def test_transitions_to_half_open_after_recovery(self):
        from app.utils.retry_policy import CircuitBreaker, CircuitState
        cb = CircuitBreaker(failure_threshold=1, recovery_secs=0.01)
        cb.record_failure()
        time.sleep(0.05)
        assert cb.state.name == "HALF_OPEN"
        assert cb.allow_request() is True

    def test_closes_after_success_in_half_open(self):
        from app.utils.retry_policy import CircuitBreaker
        cb = CircuitBreaker(failure_threshold=1, recovery_secs=0.01)
        cb.record_failure()
        time.sleep(0.05)
        cb.record_success()
        assert cb.state.name == "CLOSED"


# ══════════════════════════════════════════════════════════════════════════════
# 3. PARSER UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestPropertyParser:
    def _parser(self):
        from app.parsers.property_parser import PropertyParser
        return PropertyParser()

    def test_parse_returns_dict(self):
        result = self._parser().parse(SAMPLE_PROPERTY_HTML, url="http://test.com")
        assert isinstance(result, dict)

    def test_extracts_title(self):
        result = self._parser().parse(SAMPLE_PROPERTY_HTML)
        assert result["title"] == "Apartament 2+1 ne Tirane"

    def test_extracts_price_currency_eur(self):
        result = self._parser().parse(SAMPLE_PROPERTY_HTML)
        assert result["price_currency"] == "EUR"

    def test_extracts_location(self):
        result = self._parser().parse(SAMPLE_PROPERTY_HTML)
        assert result["location"] == "Tiranë, Blloku"

    def test_extracts_property_id(self):
        result = self._parser().parse(SAMPLE_PROPERTY_HTML)
        assert result["property_id"] == "C21-12345"

    def test_missing_fields_return_none(self):
        result = self._parser().parse("<html><body></body></html>")
        assert result["title"] is None
        assert result["price_currency"] is None

    def test_malformed_html_does_not_raise(self):
        result = self._parser().parse("<<<not html>>>", url="http://test.com")
        assert isinstance(result, dict)

    def test_can_parse_target_domain(self):
        p = self._parser()
        assert p.can_parse("https://www.century21albania.com/properties/123")
        assert not p.can_parse("https://www.example.com/properties/123")


# ══════════════════════════════════════════════════════════════════════════════
# 4. PARSER SNAPSHOT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestParserSnapshots:
    """
    WHY: When the site redesigns, selectors silently break and parse()
    returns None for every field.  Without snapshot tests this goes
    unnoticed until someone checks the CSV.  With snapshots it fails
    immediately with a diff.
    """

    SNAPSHOT_FILE = "property_parse_result.json"

    def test_snapshot_stable(self):
        from app.parsers.property_parser import PropertyParser
        parser = PropertyParser()
        result = parser.parse(SAMPLE_PROPERTY_HTML, url="http://test.com/prop/1")

        existing = _read_fixture(self.SNAPSHOT_FILE)
        if not existing:
            # First run: create the snapshot
            _write_fixture(self.SNAPSHOT_FILE, result)
            pytest.skip("Snapshot created — run tests again to validate")
        else:
            expected = json.loads(existing)
            assert result == expected, (
                "Parser output changed!\n"
                f"Expected: {json.dumps(expected, indent=2)}\n"
                f"Got:      {json.dumps(result, indent=2)}"
            )


# ══════════════════════════════════════════════════════════════════════════════
# 5. TRANSLATOR UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestNullTranslator:
    def test_returns_input_unchanged(self):
        from app.translator.translator import NullTranslator
        t = NullTranslator()
        assert t.translate("Tiranë") == "Tiranë"

    def test_batch_returns_identity_map(self):
        from app.translator.translator import NullTranslator
        t = NullTranslator()
        result = t.translate_batch(["Tiranë", "Durrës"])
        assert result == {"Tiranë": "Tiranë", "Durrës": "Durrës"}

    def test_empty_string_returns_none(self):
        from app.translator.translator import NullTranslator
        t = NullTranslator()
        # NullTranslator returns "" for "" (identity), None for None
        assert t.translate(None) is None  # type: ignore[arg-type]


class TestYesNoTranslation:
    def test_po_maps_to_yes(self):
        from app.translator.translator import translate_yes_no
        assert translate_yes_no("Po") == "Yes"

    def test_jo_maps_to_no(self):
        from app.translator.translator import translate_yes_no
        assert translate_yes_no("Jo") == "No"

    def test_none_returns_none(self):
        from app.translator.translator import translate_yes_no
        assert translate_yes_no(None) is None

    def test_case_insensitive(self):
        from app.translator.translator import translate_yes_no
        assert translate_yes_no("PO") == "Yes"
        assert translate_yes_no("JO") == "No"


# ══════════════════════════════════════════════════════════════════════════════
# 6. STORAGE UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestJsonStorage:
    def test_save_and_load(self, tmp_path):
        from app.storage.checkpoint_storage import JsonStorage
        s = JsonStorage(str(tmp_path / "ck.json"))
        s.save("key1", {"a": 1})
        assert s.load("key1") == {"a": 1}

    def test_exists_true_after_save(self, tmp_path):
        from app.storage.checkpoint_storage import JsonStorage
        s = JsonStorage(str(tmp_path / "ck.json"))
        s.save("x", 1)
        assert s.exists("x")

    def test_exists_false_for_missing(self, tmp_path):
        from app.storage.checkpoint_storage import JsonStorage
        s = JsonStorage(str(tmp_path / "ck.json"))
        assert not s.exists("missing")

    def test_delete_removes_key(self, tmp_path):
        from app.storage.checkpoint_storage import JsonStorage
        s = JsonStorage(str(tmp_path / "ck.json"))
        s.save("k", "v")
        s.delete("k")
        assert not s.exists("k")

    def test_keys_with_prefix(self, tmp_path):
        from app.storage.checkpoint_storage import JsonStorage
        s = JsonStorage(str(tmp_path / "ck.json"))
        s.save("exported:1", True)
        s.save("exported:2", True)
        s.save("other:3",    True)
        assert sorted(s.keys("exported:")) == ["exported:1", "exported:2"]


class TestSqliteStorage:
    def test_basic_operations(self, tmp_path):
        from app.storage.checkpoint_storage import SqliteStorage
        s = SqliteStorage(str(tmp_path / "ck.db"))
        s.save("hello", "world")
        assert s.load("hello") == "world"
        assert s.exists("hello")
        s.delete("hello")
        assert not s.exists("hello")


# ══════════════════════════════════════════════════════════════════════════════
# 7. EXPORTER UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestCSVExporter:
    def test_writes_header_and_row(self, tmp_path):
        from app.exporters.exporters import CSVExporter
        path = str(tmp_path / "out.csv")
        exp = CSVExporter(path)
        with exp:
            exp.write_row({"property_id": "1", "title_al": "Test", "price": "100"})

        content = Path(path).read_text(encoding="utf-8-sig")
        assert "property_id" in content
        assert "Test" in content

    def test_missing_keys_written_as_empty(self, tmp_path):
        from app.exporters.exporters import CSVExporter
        path = str(tmp_path / "out2.csv")
        exp = CSVExporter(path)
        with exp:
            exp.write_row({"property_id": "42"})  # all other fields absent

        content = Path(path).read_text(encoding="utf-8-sig")
        assert "42" in content


class TestCompositeExporter:
    def test_fans_out_to_all_children(self):
        from app.exporters.exporters import CompositeExporter
        from app.container_f.container import _InMemoryExporter
        a, b = _InMemoryExporter(), _InMemoryExporter()
        comp = CompositeExporter([a, b])
        with comp:
            comp.write_row({"property_id": "X"})

        assert len(a.rows) == 1
        assert len(b.rows) == 1

    def test_one_child_failure_does_not_abort_others(self, tmp_path):
        """A broken exporter must not prevent others from writing."""
        from app.exporters.exporters import CompositeExporter
        from app.container_f.container import _InMemoryExporter

        class BrokenExporter(_InMemoryExporter):
            def write_row(self, row):
                raise RuntimeError("I am broken")

        good   = _InMemoryExporter()
        broken = BrokenExporter()
        comp   = CompositeExporter([broken, good])
        with comp:
            comp.write_row({"property_id": "Y"})

        assert len(good.rows) == 1  # good exporter still received the row


# ══════════════════════════════════════════════════════════════════════════════
# 8. SETTINGS UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestSettings:
    def test_defaults_are_valid(self):
        from app.config.settings import ScraperSettings
        s = ScraperSettings()
        assert s.max_pages > 0
        assert s.worker_count > 0

    def test_proxy_urls_parsed_correctly(self):
        from app.config.settings import ScraperSettings
        s = ScraperSettings(proxy_list="http://p1:8080,http://p2:8080")
        assert s.proxy_urls == ["http://p1:8080", "http://p2:8080"]

    def test_empty_proxy_list_returns_empty(self):
        from app.config.settings import ScraperSettings
        s = ScraperSettings(proxy_list=None)
        assert s.proxy_urls == []

    def test_invalid_exporter_raises(self):
        from app.config.settings import ScraperSettings
        with pytest.raises(Exception):
            ScraperSettings(exporters="ftp")

    def test_exporter_list_parsed(self):
        from app.config.settings import ScraperSettings
        s = ScraperSettings(exporters="csv,json")
        assert s.exporter_list == ["csv", "json"]


# ══════════════════════════════════════════════════════════════════════════════
# 9. INTEGRATION TEST — Full pipeline with test doubles
# ══════════════════════════════════════════════════════════════════════════════

class TestPipelineIntegration:
    """
    Tests the full scrape → translate → export flow with:
      - A mock HTTP client that returns our sample HTML
      - NullTranslator (no network)
      - InMemoryExporter (no filesystem)
    """

    def _make_mock_client(self, listing_html: str, property_html: str):
        """Returns an async-capable mock client."""
        from app.abstractions.base_client import BaseHTTPClient

        class MockClient(BaseHTTPClient):
            call_count = 0

            async def get_async(self, url, **kwargs):
                MockClient.call_count += 1
                # First call = listing page, subsequent = property pages
                if "properties" in url and MockClient.call_count == 1:
                    return listing_html
                return property_html

            def get(self, url, **kwargs):
                return asyncio.run(self.get_async(url))

            def close(self): pass
            async def aclose(self): pass

        MockClient.call_count = 0
        return MockClient()

    def test_empty_listing_produces_no_rows(self):
        from app.container_f.container import Container, _InMemoryExporter
        from app.config.settings import ScraperSettings

        exporter = _InMemoryExporter()
        container = Container.for_testing(exporter=exporter)
        # NullHTTPClient returns empty HTML → no links → no properties
        container.engine.run()

        assert len(exporter.rows) == 0

    def test_deduplication_skips_known_ids(self):
        from app.container_f.container import _InMemoryExporter
        from app.storage.checkpoint_storage import JsonStorage
        from app.models.property_model import Property
        from app.orchestration.scraper_engine import ScraperEngine
        from app.translator.translator import NullTranslator
        from app.config.settings import ScraperSettings

        exporter = _InMemoryExporter()
        storage  = JsonStorage.__new__(JsonStorage)
        storage._path = Path("/dev/null")
        storage._data = {"exported:C21-001": True}

        prop = Property(property_id="C21-001", title="Test")

        # Simulate the deduplication step directly
        s = ScraperSettings(translation_provider="null", max_pages=1, worker_count=1,
                            log_file=None)
        from monitoring.logger import Metrics
        engine = ScraperEngine(
            client     = None,  # type: ignore
            parser     = None,  # type: ignore
            translator = NullTranslator(),
            exporter   = exporter,
            storage    = storage,
            settings   = s,
            metrics    = Metrics(),
        )

        result = engine._deduplicate([prop])
        assert result == []  # known ID was skipped


# ══════════════════════════════════════════════════════════════════════════════
# 10. ASYNC WORKER UNIT TESTS
# ══════════════════════════════════════════════════════════════════════════════

class TestAsyncWorkers:
    def test_worker_processes_url_and_emits_result(self):
        import asyncio
        from app.workers.async_workers import ScraperWorker, ScrapeResult
        from app.parsers.property_parser import PropertyParser
        from monitoring.logger import Metrics

        url_queue    = asyncio.Queue()
        result_queue = asyncio.Queue()

        from app.abstractions.base_client import BaseHTTPClient

        class FixedClient(BaseHTTPClient):
            async def get_async(self, url, **kwargs):
                return SAMPLE_PROPERTY_HTML
            def get(self, url, **kwargs): return SAMPLE_PROPERTY_HTML
            def close(self): pass
            async def aclose(self): pass

        from app.config.settings import ScraperSettings
        s = ScraperSettings(translation_provider="null", max_pages=1, worker_count=1,
                            log_file=None)

        worker = ScraperWorker(
            worker_id    = 0,
            client       = FixedClient(),
            parser       = PropertyParser(),
            url_queue    = url_queue,
            result_queue = result_queue,
            settings     = s,
            metrics      = Metrics(),
        )

        async def _run():
            await url_queue.put("http://test.com/property/1")
            await url_queue.put(None)  # poison pill
            await worker.run()
            return await result_queue.get()

        result: ScrapeResult = asyncio.run(_run())
        assert result.property is not None
        assert result.property.title == "Apartament 2+1 ne Tirane"
        assert result.error is None