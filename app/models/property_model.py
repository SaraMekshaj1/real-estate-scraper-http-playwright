from __future__ import annotations
from dataclasses import dataclass, asdict


@dataclass
class Property:
    property_id:        str | None   = None
    url:                str | None   = None
    title:              str | None   = None
    price:              float | None = None
    price_currency:     str | None   = None
    location:           str | None   = None
    description:        str | None   = None
    images:             list[str] | None = None
    total_area:         str | None   = None
    internal_area:      str | None   = None
    number_of_bedrooms: str | None   = None
    floor:              str | None   = None
    status:             str | None   = None
    type:               str | None   = None
    furnished:          str | None   = None
    mortgage:           str | None   = None
    elevator:           str | None   = None
    number_of_toilets:  str | None   = None
    characteristics:    str | None   = None

    def to_dict(self) -> dict:
        return asdict(self)