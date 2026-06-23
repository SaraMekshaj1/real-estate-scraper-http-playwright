from __future__ import annotations
import json
from app.exporters_f.exporters import FIELDNAMES
from app.models.property_model import Property


class PropertyMapper:
    """
    Maps a normalized Property dataclass to a flat export dict.

    FIELDNAMES (from exporters.py) is the single source of truth for which
    fields get exported and in what order. Adding a field to the model only
    requires updating FIELDNAMES — this mapper adapts automatically.

    images is serialized to a JSON string here so every exporter (CSV, JSON-
    Lines, Postgres) receives a uniform scalar value rather than a raw list.
    """

    @staticmethod
    def to_export_dict(prop: Property) -> dict:
        d = prop.to_dict()
        result = {k: d.get(k) for k in FIELDNAMES}
        if d.get("images") is not None:
            result["images"] = json.dumps(d["images"], ensure_ascii=False)
        return result