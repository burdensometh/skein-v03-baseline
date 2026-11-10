"""Freshness lifetimes from response headers (RFC 9111 section 4.2).

A server that says a representation stays fresh for a day is telling a
crawler not to bother revisiting sooner. The lifetime is taken from
``Cache-Control: max-age`` (minus ``Age``), else ``Expires`` relative to
``Date``. ``no-store``/``no-cache`` and invalid ``Expires`` values mean a
lifetime of zero. The heuristic freshness RFC 9111 allows caches to derive
from ``Last-Modified`` is deliberately not used: for a crawler it would
postpone noticing changes on pages that merely happen to be old.
"""

from __future__ import annotations

from skein.net.headers import Headers
from skein.net.httpdate import parse_http_date


def parse_cache_control(values: list[str]) -> dict[str, str | None]:
    """Directives of all ``Cache-Control`` fields, lower-cased; later duplicates are ignored."""
    directives: dict[str, str | None] = {}
    for value in values:
        for part in value.split(","):
            name, sep, argument = part.strip().partition("=")
            name = name.strip().lower()
            if not name or name in directives:
                continue
            directives[name] = argument.strip().strip('"') if sep else None
    return directives


def _seconds(value: str | None) -> float | None:
    if value is None or not value.isdigit():
        return None
    return float(int(value))


def freshness_lifetime(headers: Headers, now: float) -> float | None:
    """Remaining freshness in seconds, or ``None`` when the response says nothing about it."""
    directives = parse_cache_control(headers.get_all("Cache-Control"))
    if "no-store" in directives or "no-cache" in directives:
        return 0.0
    age = _seconds(headers.get("Age")) or 0.0
    max_age = _seconds(directives.get("max-age"))
    if max_age is not None:
        return max(0.0, max_age - age)
    date = parse_http_date(headers.get("Date"), reference=now)
    reference = date if date is not None else now
    expires_header = headers.get("Expires")
    if expires_header is not None:
        expires = parse_http_date(expires_header, reference=now)
        return 0.0 if expires is None else max(0.0, expires - reference - age)
    return None
