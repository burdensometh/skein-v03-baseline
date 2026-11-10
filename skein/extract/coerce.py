"""String transforms, regex post-processing and type coercion for extracted values."""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import date, datetime, timezone
from typing import Any

from skein.urls.parse import URLError, resolve


class CoercionError(ValueError):
    """A raw string could not be converted to the requested field type."""


_NUMBER_RE = re.compile(r"[-+]?\d[\d.,\s'_]*\d|[-+]?\d")
_TRUE = frozenset({"true", "yes", "y", "1", "on", "t", "in stock", "available"})
_FALSE = frozenset({"false", "no", "n", "0", "off", "f", "out of stock", "sold out", "unavailable"})
DEFAULT_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d.%m.%Y",
    "%d/%m/%Y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%d %B %Y",
    "%d %b %Y",
    "%Y年%m月%d日",
)
DEFAULT_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M%z",
    "%Y-%m-%d %H:%M",
    "%a, %d %b %Y %H:%M:%S %z",
    "%a, %d %b %Y %H:%M:%S GMT",
)


def apply_transforms(value: str, transforms: tuple[str, ...]) -> str:
    for name in transforms:
        if name == "strip":
            value = value.strip()
        elif name == "lower":
            value = value.lower()
        elif name == "upper":
            value = value.upper()
        elif name == "collapse":
            value = " ".join(value.split())
        elif name == "title":
            value = value.title()
        else:
            raise ValueError(f"unknown transform {name!r}")
    return value


def apply_regex(value: str, pattern: re.Pattern[str] | None) -> str | None:
    """Narrow ``value`` with ``pattern``; ``None`` when the pattern does not match.

    The captured value is the named group ``value`` if present, otherwise the
    first group, otherwise the whole match.
    """
    if pattern is None:
        return value
    match = pattern.search(value)
    if match is None:
        return None
    if "value" in pattern.groupindex:
        return match.group("value")
    if pattern.groups:
        return match.group(1)
    return match.group(0)


def parse_number(text: str) -> int | float:
    """Lenient number parsing: ``"$1,234.50"`` -> 1234.5, ``"1.234,50 EUR"`` -> 1234.5.

    When both ``.`` and ``,`` occur, the right-most one is the decimal mark. A
    lone comma followed by exactly three digits is a thousands separator;
    a lone dot is always a decimal mark.
    """
    match = _NUMBER_RE.search(text)
    if match is None:
        raise CoercionError(f"no number in {text!r}")
    raw = re.sub(r"[\s'_]", "", match.group(0))
    negative = raw.startswith("-")
    raw = raw.lstrip("+-")
    has_dot, has_comma = "." in raw, "," in raw
    if has_dot and has_comma:
        decimal = "." if raw.rfind(".") > raw.rfind(",") else ","
        thousands = "," if decimal == "." else "."
        raw = raw.replace(thousands, "").replace(decimal, ".")
    elif has_dot or has_comma:
        sep = "." if has_dot else ","
        head, _, tail = raw.rpartition(sep)
        if raw.count(sep) > 1 or (len(tail) == 3 and head and sep == ","):
            raw = raw.replace(sep, "")
        else:
            raw = raw.replace(sep, ".")
    try:
        value: int | float = float(raw) if "." in raw else int(raw)
    except ValueError as exc:
        raise CoercionError(f"invalid number {text!r}") from exc
    return -value if negative else value


def _parse_int(text: str) -> int:
    value = parse_number(text)
    if isinstance(value, float):
        if not value.is_integer():
            raise CoercionError(f"{text!r} is not an integer")
        return int(value)
    return value


def _parse_float(text: str) -> float:
    cleaned = text.strip().replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return float(parse_number(text))


def _parse_bool(text: str) -> bool:
    lowered = " ".join(text.lower().split())
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False
    raise CoercionError(f"cannot interpret {text!r} as a boolean")


def _parse_date(text: str, formats: tuple[str, ...], with_time: bool) -> str:
    cleaned = " ".join(text.split())
    if formats:
        candidates = formats
    elif with_time:
        candidates = DEFAULT_DATETIME_FORMATS + DEFAULT_DATE_FORMATS
    else:
        candidates = DEFAULT_DATE_FORMATS + DEFAULT_DATETIME_FORMATS
    for fmt in candidates:
        try:
            parsed = datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
        if not with_time:
            return date(parsed.year, parsed.month, parsed.day).isoformat()
        if parsed.tzinfo is None and fmt.endswith("GMT"):
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.isoformat()
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CoercionError(f"{text!r} does not match any date format") from exc
    return parsed.date().isoformat() if not with_time else parsed.isoformat()


def coerce(value: str, type_name: str, *, base_url: str | None = None, formats: tuple[str, ...] = ()) -> Any:
    """Convert a string to the configured field type."""
    if type_name == "string":
        return value
    if value.strip() == "":
        raise CoercionError(f"empty value cannot be converted to {type_name}")
    if type_name == "int":
        return _parse_int(value)
    if type_name == "float":
        return _parse_float(value)
    if type_name == "number":
        return parse_number(value)
    if type_name == "bool":
        return _parse_bool(value)
    if type_name == "date":
        return _parse_date(value, formats, with_time=False)
    if type_name == "datetime":
        return _parse_date(value, formats, with_time=True)
    if type_name == "url":
        if base_url is None:
            return value.strip()
        try:
            return str(replace(resolve(base_url, value.strip()), fragment=None))
        except URLError as exc:
            raise CoercionError(f"invalid URL {value!r}: {exc}") from exc
    raise CoercionError(f"unknown field type {type_name!r}")
