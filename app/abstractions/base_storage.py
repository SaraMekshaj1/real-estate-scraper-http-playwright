from __future__ import annotations
from abc import ABC,abstractmethod
from typing import Any

class BaseStorage(ABC):
    """Key-value / record-store contract for checkpoints and deduplication."""

    @abstractmethod
    def save(self, key: str, value: Any) -> None:
        """Persist *value* under *key*.  Overwrites existing entries."""

    @abstractmethod
    def load(self, key: str) -> Any | None:
        """Return the value for *key*, or None if absent."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if *key* has been saved."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove *key* from the store."""

    @abstractmethod
    def keys(self, prefix: str = "") -> list[str]:
        """List all keys, optionally filtered by prefix."""