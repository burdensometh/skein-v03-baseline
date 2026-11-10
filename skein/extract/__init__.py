"""Rule-driven extraction of items and links from parsed HTML."""

from skein.extract.coerce import CoercionError, coerce, parse_number
from skein.extract.extractor import ExtractedItem, ExtractionResult, Extractor
from skein.extract.links import DiscoveredLink, DomainScope, LinkExtractor
from skein.extract.rules import ExtractionPlan, FieldRule, ItemRule, LinkRule

__all__ = [
    "CoercionError",
    "DiscoveredLink",
    "DomainScope",
    "ExtractedItem",
    "ExtractionPlan",
    "ExtractionResult",
    "Extractor",
    "FieldRule",
    "ItemRule",
    "LinkExtractor",
    "LinkRule",
    "coerce",
    "parse_number",
]
