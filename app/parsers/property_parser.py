from __future__ import annotations
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from app.abstractions.base_parser import BaseParser

# ── Selector registry ──────────────────────────────────────────────────────
# WHY: CSS selectors change when a site redesigns. Centralising them means
#      one line changes instead of hunting through methods. In a multi-site
#      setup, each parser subclass defines its own _SELECTORS.

_SELECTORS: dict[str, str] = {
    "property_id_span": "h6.font-semibold.text-black-custom.font-barlow span.text-gold-shade-55",
    "title":            "h1.font-extrabold",
    "price":            "h2.font-bold.text-gold-shade-55",
    "location":         "div.flex.gap-1 h6",
    "description":      "p.paragraph-2.text-grey-shade-40",
    "images":           "div.grid.grid-cols-4.gap-2 a",
}

_TOP_LABEL_MAP: dict[str, str] = {
    "gross area":     "total_area",
    "interior area":  "internal_area",
    "bedrooms":       "number_of_bedrooms",
    "floor":          "floor",
    "status":         "status",
    "type":           "type",
}

_TARGET_DOMAIN = "century21albania.com"


class PropertyParser(BaseParser):
    """
    Extracts raw field values from a century21albania.com/en property page.

    RESILIENCE: Every extraction method returns None on any failure.
    A corrupt or restructured page produces a partially-filled dict
    (which the validator will flag) rather than an exception that would
    abort the worker.
    """

    def can_parse(self, url: str) -> bool:
        try:
            return _TARGET_DOMAIN in urlparse(url).netloc
        except Exception:
            return False

    def parse(self, html: str, url: str | None = None) -> dict:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            return self._empty(url)

        top_grid = self._extract_top_grid(soup)
        info     = self._extract_info_container(soup)

        return {
            "property_id":        self._safe(self._extract_property_id, soup),
            "url":                url,
            "title":              self._safe(self._extract_title, soup),
            "price_raw":          self._safe(self._extract_price_raw, soup),
            "price_currency":     self._safe(self._extract_price_currency, soup),
            "location":           self._safe(self._extract_location, soup),
            "description":        self._safe(self._extract_description, soup),
            "images":             self._safe(self._extract_images, soup),
            "total_area":         top_grid.get("total_area"),
            "internal_area":      top_grid.get("internal_area"),
            "number_of_bedrooms": top_grid.get("number_of_bedrooms"),
            "floor":              top_grid.get("floor"),
            "statusi":            top_grid.get("status"),
            "lloji":              top_grid.get("type"),
            "mobilimi":           top_grid.get("mobilimi") or info.get("mobilimi"),
            "ka_hipoteke":        top_grid.get("ka_hipoteke") or info.get("ka_hipoteke"),
            "ashensor":           info.get("ashensor"),
            "number_of_toilets":  info.get("number_of_toilets"),
            "karakteristikat":    self._safe(self._extract_amenities, soup),
        }

    # ── Resilience wrapper ────────────────────────────────────────────────

    @staticmethod
    def _safe(fn, *args):
        """Call fn(*args), returning None on any exception."""
        try:
            return fn(*args)
        except Exception:
            return None

    @staticmethod
    def _empty(url: str | None) -> dict:
        return {k: None for k in (
            "property_id", "url", "title", "price_raw", "price_currency",
            "location", "description", "images", "total_area",
            "internal_area", "number_of_bedrooms", "floor", "statusi",
            "lloji", "mobilimi", "ka_hipoteke", "ashensor",
            "number_of_toilets", "karakteristikat",
        )} | {"url": url}

    # ── Field extractors ──────────────────────────────────────────────────

    @staticmethod
    def _extract_property_id(soup) -> str | None:
        tag = soup.select_one(_SELECTORS["property_id_span"])
        if tag:
            return tag.get_text(strip=True)
        meta = soup.find("meta", {"name": "crm-id"})
        return meta["content"].strip() if meta else None

    @staticmethod
    def _extract_title(soup) -> str | None:
        tag = soup.select_one(_SELECTORS["title"])
        return tag.get_text(strip=True) if tag else None

    @staticmethod
    def _extract_price_raw(soup) -> str | None:
        tag = soup.select_one(_SELECTORS["price"])
        return tag.get_text(strip=True) if tag else None

    @staticmethod
    def _extract_price_currency(soup) -> str | None:
        tag = soup.select_one(_SELECTORS["price"])
        if not tag:
            return None
        raw = tag.get_text(strip=True)
        if "€" in raw:           return "EUR"
        if "$" in raw:           return "USD"
        if "£" in raw:           return "GBP"
        if "ALL" in raw.upper(): return "ALL"
        return None

    @staticmethod
    def _extract_location(soup) -> str | None:
        tag = soup.select_one(_SELECTORS["location"])
        return tag.get_text(strip=True) if tag else None

    @staticmethod
    def _extract_images(soup) -> str | None:
        tags  = soup.select(_SELECTORS["images"])
        hrefs = [a.get("href") for a in tags if a.get("href")]
        return ",".join(hrefs) if hrefs else None

    @staticmethod
    def _extract_description(soup) -> str | None:
        tag = soup.select_one(_SELECTORS["description"])
        return tag.get_text(strip=True) if tag else None

    @staticmethod
    def _extract_top_grid(soup) -> dict:
        result: dict = {}
        grid = soup.find(
            "div",
            class_=lambda c: c and "xl:grid-cols-5" in c and "font-oakes" in c,
        )
        if not grid:
            return result

        for card in grid.find_all("div", recursive=False):
            p = card.select_one("p")
            if not p:
                continue
            span = p.find("span")
            if span:
                label_raw = p.contents[0] if p.contents else ""
                label = str(label_raw).replace("\xa0", "").strip().lower().rstrip(":")
                value = span.get_text(strip=True)
            else:
                full_text = p.get_text(strip=True)
                label = full_text.lower()
                value = full_text

            matched = False
            for map_label, field_name in _TOP_LABEL_MAP.items():
                if map_label in label:
                    result[field_name] = value
                    matched = True
                    break

            if not matched:
                norm = label.lower()
                if "furnished" in norm and "not" not in norm:
                    result["mobilimi"] = value
                elif "not furnished" in norm:
                    result["mobilimi"] = value
                elif "mortgage" in norm or "certificate" in norm:
                    result["ka_hipoteke"] = value

        return result

    @staticmethod
    def _extract_info_container(soup) -> dict:
        result: dict = {}
        container = soup.find(
            "div",
            class_=lambda c: c and "lg:grid-cols-3" in c and "font-oakes" in c,
        )
        if not container:
            return result

        for card in container.find_all("div", recursive=False):
            p = card.select_one("p") or card.find("p")
            if not p:
                continue
            text = p.get_text(strip=True)
            if not text:
                continue

            if ":" in text:
                label, value = text.split(":", 1)
                norm  = label.strip().lower()
                value = value.strip()
                if "elevator"  in norm: result["ashensor"]          = value
                elif "bath"    in norm: result["number_of_toilets"] = value
                elif "furnish" in norm: result.setdefault("mobilimi",    value)
                elif "mortgage" in norm or "certificate" in norm:
                    result.setdefault("ka_hipoteke", value)
            else:
                norm = text.lower()
                if "furnished" in norm:
                    result.setdefault("mobilimi", text)

        return result

    @staticmethod
    def _extract_amenities(soup) -> str | None:
        container = soup.find(
            "div",
            class_=lambda c: c and "md:grid-cols-5" in c and "grid-cols-3" in c,
        )
        if not container:
            return None
        items = [
            p.get_text(strip=True)
            for item in container.find_all("div", recursive=False)
            if (p := item.select_one("p")) and p.get_text(strip=True)
        ]
        return ", ".join(items) if items else None