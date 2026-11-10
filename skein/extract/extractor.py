"""Applying item rules to a parsed document."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from skein.extract.coerce import CoercionError, apply_regex, apply_transforms, coerce
from skein.extract.rules import FieldRule, ItemRule
from skein.html.dom import Document, Element


def canonical_json(data: Any) -> str:
    """Stable JSON form (sorted keys, no insignificant whitespace) of item data."""
    return json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def content_hash(canonical: str) -> str:
    """SHA-256 of a canonical JSON document, as stored with every item version."""
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class ExtractedItem:
    """One structured record pulled from a page.

    A partial extraction (``partial = True``) is a fragment of an item that
    another page completes: ``field_names`` names exactly the fields this
    fragment owns, so the store can merge it without touching fields owned by
    other rules. ``authority`` marks the fragment whose sightings decide
    deletions for this item type.
    """

    item_type: str
    key: str
    data: dict[str, Any]
    url: str
    partial: bool = False
    authority: bool = False
    field_names: tuple[str, ...] = ()

    def data_json(self) -> str:
        """Canonical JSON used for storage; its hash detects content changes."""
        return canonical_json(self.data)

    def content_hash(self) -> str:
        return content_hash(self.data_json())


@dataclass
class ExtractionResult:
    items: list[ExtractedItem] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    dropped: int = 0


class _MissingRequired(Exception):
    """Raised internally when a required field has no value."""


def _raw_value(node: Element, rule: FieldRule) -> str | None:
    if rule.source == "text":
        return node.text()
    if rule.source == "own_text":
        return node.own_text()
    if rule.source == "html":
        return node.inner_html()
    if rule.source == "outer_html":
        return node.outer_html()
    return node.get(rule.attr or "")


class Extractor:
    """Turns documents into :class:`ExtractedItem` objects according to item rules."""

    def __init__(self, rules: tuple[ItemRule, ...] | list[ItemRule]) -> None:
        self.rules = tuple(rules)

    def extract(self, document: Document, url: str) -> ExtractionResult:
        result = ExtractionResult()
        for rule in self.rules:
            if not rule.applies_to(url):
                continue
            scopes: list[Element] = document.select(rule.scope) if rule.scope else [document]
            for index, scope in enumerate(scopes):
                errors: list[str] = []
                try:
                    data = self._extract_fields(rule, scope, url, errors)
                except _MissingRequired as exc:
                    result.dropped += 1
                    result.errors.append(f"{rule.name}[{index}] on {url}: {exc}")
                    continue
                key = self._item_key(rule, data, url, index, len(scopes))
                result.errors.extend(f"{rule.name}[{index}] on {url}: {e}" for e in errors)
                if key is None:
                    result.dropped += 1
                    result.errors.append(f"{rule.name}[{index}] on {url}: key fields missing")
                    continue
                if rule.partial:
                    # Only fields the fragment actually carries take part in the
                    # merge; a selector that matched nothing must not wipe fields
                    # owned by the other page's rule. Key fields are always kept:
                    # they identify the record even when absent from field_names.
                    present = tuple(
                        name for name in data
                        if name in rule.key_fields or (data[name] is not None and data[name] != [])
                    )
                    result.items.append(ExtractedItem(
                        rule.name, key, data, url, partial=True,
                        authority=rule.authority, field_names=present))
                else:
                    result.items.append(ExtractedItem(rule.name, key, data, url))
        return result

    def _extract_fields(self, rule: ItemRule, scope: Element, url: str, errors: list[str]) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for field_rule in rule.fields:
            value = self._extract_field(field_rule, scope, url, errors)
            if value is None or value == []:
                if field_rule.required:
                    raise _MissingRequired(f"required field {field_rule.name!r} is missing")
                if field_rule.default is not None:
                    value = field_rule.default
                elif field_rule.multiple and field_rule.join is None:
                    value = []
                else:
                    value = None
            data[field_rule.name] = value
        return data

    def _extract_field(self, rule: FieldRule, scope: Element, url: str, errors: list[str]) -> Any:
        nodes = scope.select(rule.selector) if rule.selector else [scope]
        if not rule.multiple:
            nodes = nodes[:1]
        values: list[Any] = []
        for node in nodes:
            raw = _raw_value(node, rule)
            if raw is None:
                continue
            text = apply_regex(apply_transforms(raw, rule.transforms), rule.regex)
            if text is None:
                continue
            try:
                values.append(coerce(text, rule.type, base_url=url, formats=rule.formats))
            except CoercionError as exc:
                errors.append(f"field {rule.name!r}: {exc}")
        if rule.multiple:
            if rule.join is not None:
                return rule.join.join(str(v) for v in values) if values else None
            return values
        return values[0] if values else None

    @staticmethod
    def _item_key(rule: ItemRule, data: dict[str, Any], url: str, index: int, total: int) -> str | None:
        if not rule.key_fields:
            return url if total == 1 else f"{url}#{index}"
        parts: list[str] = []
        for name in rule.key_fields:
            value = data.get(name)
            if value is None or value == "" or value == []:
                return None
            parts.append("|".join(map(str, value)) if isinstance(value, list) else str(value))
        return "/".join(parts)
