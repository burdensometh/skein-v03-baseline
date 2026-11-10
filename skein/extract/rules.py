"""Compiled extraction rules (built from configuration by :mod:`skein.config`)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

FIELD_TYPES = ("string", "int", "float", "number", "bool", "date", "datetime", "url")
FIELD_SOURCES = ("text", "own_text", "html", "outer_html", "attr")
TRANSFORMS = ("strip", "lower", "upper", "collapse", "title")


@dataclass(frozen=True)
class FieldRule:
    """How to obtain one field of an item.

    ``selector`` is evaluated relative to the item scope; ``None`` means the
    scope element itself. The raw string is taken from ``source`` (``attr``
    requires ``attr``), passed through ``transforms``, then ``regex`` (first
    named group ``value``, else group 1, else the whole match), then coerced
    to ``type``. With ``multiple`` every matching node yields a value; a
    ``join`` separator then turns the list into a single string.
    """

    name: str
    selector: str | None = None
    source: str = "text"
    attr: str | None = None
    regex: re.Pattern[str] | None = None
    transforms: tuple[str, ...] = ("strip",)
    type: str = "string"
    formats: tuple[str, ...] = ()
    multiple: bool = False
    join: str | None = None
    required: bool = False
    default: Any = None


@dataclass(frozen=True)
class ItemRule:
    """Produces items of one type from pages whose URL matches ``url_pattern``.

    Each element matched by ``scope`` yields one item (the whole document
    when ``scope`` is ``None``). The item key joins the values of
    ``key_fields``; without key fields the page URL (plus the scope index when
    a page yields several items) is used.

    A *partial* rule (``partial = True``) yields fragments of an item that
    other rules of the same name complete on other pages: its fields are
    merged field by field instead of replacing the stored record, it never
    tombstones items itself, and (when ``authority = True``) its sightings
    across a whole run decide which items are retired when the run ends.
    """

    name: str
    fields: tuple[FieldRule, ...]
    url_pattern: re.Pattern[str] | None = None
    scope: str | None = None
    key_fields: tuple[str, ...] = ()
    partial: bool = False
    authority: bool = False

    def applies_to(self, url: str) -> bool:
        return self.url_pattern is None or self.url_pattern.search(url) is not None


@dataclass(frozen=True)
class LinkRule:
    """Which links to follow from a page.

    A link qualifies when it matches at least one ``allow`` pattern (or
    ``allow`` is empty) and no ``deny`` pattern. ``max_depth`` caps the depth
    of requests created by this rule; ``priority`` is added to them.
    ``kind`` may be ``"sitemap"`` to treat targets as sitemaps.
    """

    allow: tuple[re.Pattern[str], ...] = ()
    deny: tuple[re.Pattern[str], ...] = ()
    selector: str = "a[href], area[href]"
    attr: str = "href"
    max_depth: int | None = None
    priority: int = 0
    kind: str = "page"
    source_pattern: re.Pattern[str] | None = None

    def accepts(self, url: str) -> bool:
        if self.allow and not any(p.search(url) for p in self.allow):
            return False
        return not any(p.search(url) for p in self.deny)


@dataclass(frozen=True)
class ExtractionPlan:
    """All rules of one spider."""

    items: tuple[ItemRule, ...] = ()
    links: tuple[LinkRule, ...] = field(default_factory=lambda: (LinkRule(),))
