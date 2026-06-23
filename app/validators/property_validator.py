"""validators/property_validator.py"""
from __future__ import annotations
from dataclasses import dataclass, field
from app.models.property_model import Property

@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str] = field(default_factory=list)

class PropertyValidator:
    REQUIRED = ("property_id", "url", "title")

    @staticmethod
    def validate(prop: Property) -> ValidationResult:
        errors = [
            f"Missing required field: {f}"
            for f in PropertyValidator.REQUIRED
            if not getattr(prop, f, None)
        ]
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)