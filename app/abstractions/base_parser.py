from __future__ import annotations
from abc  import ABC,abstractmethod

class BaseParser(ABC):
    """Contract for all HTML/JSON property parsers."""

    @abstractmethod
    def parse(self, html: str, url: str | None = None) -> dict:
        """Extract raw field values from a property page.

        Returns a plain dict with string values; no cleaning or
        business-logic — that belongs in the Normalizer.
        """

    @abstractmethod
    def can_parse(self, url: str) -> bool:
        """Return True if this parser understands the given URL/domain.

        Used by :class:`~app.parser.parser_factory.ParserFactory` to
        route pages to the correct implementation.
        """