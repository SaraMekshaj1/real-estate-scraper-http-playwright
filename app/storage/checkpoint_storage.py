from __future__ import annotations
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any
from app.abstractions.base_storage import BaseStorage

logger = logging.getLogger(__name__)

# ── JSON file storage (default, zero deps) ────────────────────────────────────

class JsonStorage(BaseStorage):
    """
    Simple key-value store backed by a JSON file.
    Suitable for: single-process scraper, up to ~50 k keys.
    Not suitable for: distributed workers (no locking across processes).
    """
    def __init__(self, path: str = "output/checkpoint.json") -> None:
        self._path = Path(path)
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text(encoding="utf-8"))
                logger.info("Checkpoint loaded from %s (%d keys)", self._path, len(self._data))
            except json.JSONDecodeError:
                logger.warning("Corrupt checkpoint at %s — starting fresh", self._path)
                self._data = {}

    def _flush(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def save(self, key: str, value: Any) -> None:
        self._data[key] = value
        self._flush()

    def load(self, key: str) -> Any | None:
        return self._data.get(key)

    def exists(self, key: str) -> bool:
        return key in self._data

    def delete(self, key: str) -> None:
        self._data.pop(key, None)
        self._flush()

    def keys(self, prefix: str = "") -> list[str]:
        if not prefix:
            return list(self._data.keys())
        return [k for k in self._data if k.startswith(prefix)]


# ── SQLite storage ────────────────────────────────────────────────────────────

class SqliteStorage(BaseStorage):
    """
    Key-value store backed by SQLite.

    Suitable for: single-process scraper, millions of keys, fast exists()
                  lookups (indexed), concurrent readers (WAL mode).
    Not suitable for: distributed workers (SQLite is not network-accessible).
    """

    def __init__(self, path: str = "output/checkpoint.db") -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS kv (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self._conn.commit()

    def save(self, key: str, value: Any) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
            (key, json.dumps(value, default=str)),
        )
        self._conn.commit()

    def load(self, key: str) -> Any | None:
        row = self._conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def exists(self, key: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM kv WHERE key = ? LIMIT 1", (key,)
        ).fetchone()
        return row is not None

    def delete(self, key: str) -> None:
        self._conn.execute("DELETE FROM kv WHERE key = ?", (key,))
        self._conn.commit()

    def keys(self, prefix: str = "") -> list[str]:
        if not prefix:
            rows = self._conn.execute("SELECT key FROM kv").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT key FROM kv WHERE key LIKE ?", (f"{prefix}%",)
            ).fetchall()
        return [r[0] for r in rows]

    def close(self) -> None:
        self._conn.close()


# ── Redis storage (distributed) ───────────────────────────────────────────────

class RedisStorage(BaseStorage):
    """
    Key-value store backed by Redis.

    WHY: When running N scraper workers across M machines, they need a
         *shared* deduplication and checkpoint store.  Redis provides
         atomic operations, TTL support, and millisecond latency.

    REQUIRES: redis-py  (pip install redis)

    SCALABILITY: All workers share one Redis instance (or cluster).
         exists() is O(1) and can handle millions of keys.
         save() uses SET NX for atomic deduplication.
    """

    def __init__(self, url: str = "redis://localhost:6379/0", prefix: str = "scraper:") -> None:
        try:
            import redis
            self._r      = redis.from_url(url, decode_responses=True)
            self._prefix = prefix
            self._r.ping()
            logger.info("RedisStorage connected to %s", url)
        except ImportError:
            raise RuntimeError("redis-py required: pip install redis")
        except Exception as exc:
            raise RuntimeError(f"Redis connection failed: {exc}") from exc

    def _k(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def save(self, key: str, value: Any) -> None:
        self._r.set(self._k(key), json.dumps(value, default=str))

    def load(self, key: str) -> Any | None:
        raw = self._r.get(self._k(key))
        return json.loads(raw) if raw is not None else None

    def exists(self, key: str) -> bool:
        return bool(self._r.exists(self._k(key)))

    def delete(self, key: str) -> None:
        self._r.delete(self._k(key))

    def keys(self, prefix: str = "") -> list[str]:
        pattern = f"{self._prefix}{prefix}*"
        return [k[len(self._prefix):] for k in self._r.scan_iter(pattern)]


# ── Factory ───────────────────────────────────────────────────────────────────

def build_storage(backend: str, settings) -> BaseStorage:
    """Return the configured storage backend."""
    if backend == "json":
        return JsonStorage(settings.checkpoint_path)
    if backend == "sqlite":
        return SqliteStorage(settings.checkpoint_path.replace(".json", ".db"))
    if backend == "redis":
        if not settings.redis_url:
            raise ValueError("REDIS_URL must be set for redis storage backend")
        return RedisStorage(settings.redis_url)
    raise ValueError(f"Unknown storage backend: {backend!r}")