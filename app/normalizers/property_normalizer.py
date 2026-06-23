from __future__ import annotations
import re
from app.models.property_model import Property

_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F9FF"
    "\U00002600-\U000026FF"
    "\U00002700-\U000027BF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\uFE00-\uFEFF"
    "]+",
    flags=re.UNICODE,
)

# All values must be lowercase — _status() compares with .lower().
# Do NOT add mixed-case variants; they will never match.
_INVALID_STATUS_VALUES = {
    "listimet e fundit",
    "te rekomanduara",
    "featured",
    "recent listings",
}


class PropertyNormalizer:
    """
    Converts a raw parsed dict into a typed Property dataclass.

    The website provides native English content; all fields are stored
    directly with no translation step.
    """

    @staticmethod
    def normalize(raw: dict) -> Property:
        return Property(
            property_id        = raw.get("property_id"),
            url                = raw.get("url"),
            title              = PropertyNormalizer._clean(raw.get("title")),
            price              = PropertyNormalizer._parse_price(raw.get("price_raw")),
            price_currency     = raw.get("price_currency"),
            location           = PropertyNormalizer._clean(raw.get("location")),
            description        = PropertyNormalizer._clean(raw.get("description")),
            images             = raw.get("images"),
            total_area         = PropertyNormalizer._number(raw.get("total_area")),
            internal_area      = PropertyNormalizer._number(raw.get("internal_area")),
            number_of_bedrooms = PropertyNormalizer._number(raw.get("number_of_bedrooms")),
            floor              = PropertyNormalizer._number(raw.get("floor")),
            status             = PropertyNormalizer._status(raw.get("statusi")),
            type               = PropertyNormalizer._clean(raw.get("lloji")),
            furnished          = PropertyNormalizer._clean(raw.get("mobilimi")),
            mortgage           = PropertyNormalizer._yes_no(raw.get("ka_hipoteke")),
            elevator           = PropertyNormalizer._yes_no(raw.get("ashensor")),
            number_of_toilets  = PropertyNormalizer._number(raw.get("number_of_toilets")),
            characteristics    = PropertyNormalizer._clean(raw.get("karakteristikat")),
        )

    @staticmethod
    def _clean(text: str | None) -> str | None:
        if not text:
            return None
        text = _EMOJI_PATTERN.sub("", text)
        text = re.sub(r"\s+", " ", text).strip()
        text = text.strip('\'""\u201c\u201d\u2018\u2019')
        return text or None

    @staticmethod
    def _number(text: str | None) -> str | None:
        if not text:
            return None
        match = re.search(r"\d+(?:[.,]\d+)?", text)
        return match.group().replace(",", ".") if match else None

    @staticmethod
    def _parse_price(raw: str | None) -> float | None:
        if not raw:
            return None
        cleaned = re.sub(r"[^\d.,]", "", raw)
        if not cleaned:
            return None

        if "," in cleaned and "." in cleaned:
            # Both separators present — determine order by position.
            # e.g. "1.250,50" → dot before comma → dot=thousands, comma=decimal
            # e.g. "1,250.50" → comma before dot → comma=thousands, dot=decimal
            if cleaned.index(".") < cleaned.index(","):
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")

        elif "," in cleaned:
            # Only comma: check digits after it.
            # 3 digits after comma → thousands separator ("92,000" → 92000)
            # 1-2 digits after comma → decimal point ("92,5" → 92.5)
            after_comma = cleaned.split(",")[-1]
            if len(after_comma) == 3:
                cleaned = cleaned.replace(",", "")
            else:
                cleaned = cleaned.replace(",", ".")

        elif "." in cleaned:
            # Only dot: same heuristic.
            # 3 digits after dot → thousands separator ("92.000" → 92000)
            # 1-2 digits after dot → decimal point ("92.50" → 92.5)
            after_dot = cleaned.split(".")[-1]
            if len(after_dot) == 3:
                cleaned = cleaned.replace(".", "")
            # else: already a valid float string, leave as-is

        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None

    @staticmethod
    def _status(value: str | None) -> str | None:
        cleaned = PropertyNormalizer._clean(value)
        if not cleaned:
            return None
        return None if cleaned.lower() in _INVALID_STATUS_VALUES else cleaned

    @staticmethod
    def _yes_no(value: str | None) -> str | None:
        """Normalise yes/no raw strings to 'Yes' / 'No'."""
        if not value:
            return None
        v = value.strip()
        if ":" in v:
            v = v.split(":", 1)[1].strip()
        lower = v.lower()
        if lower in ("po", "yes"):
            return "Yes"
        if lower in ("jo", "no"):
            return "No"
        return v