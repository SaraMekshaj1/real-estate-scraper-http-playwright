from __future__ import annotations
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from app.utils.url_utils import canonicalize_url_safe

logger = logging.getLogger(__name__)


class PropertiesLinksCrawler:

    def crawl_property_links(self, html: str, base_url: str = "") -> list[str]:
        soup  = BeautifulSoup(html, "lxml")
        seen:  set[str] = set()
        links: list[str] = []

        for a in soup.select("a.h-full"):
            href = a.get("href")
            if not href:
                continue
            canonical = canonicalize_url_safe(urljoin(base_url, href))
            if canonical not in seen:
                seen.add(canonical)
                links.append(canonical)

        if not links:
            logger.warning(
                "crawl_property_links: no links found on %s — "
                "selector 'a.h-full' may be stale or the page structure has changed",
                base_url,
            )
        return links