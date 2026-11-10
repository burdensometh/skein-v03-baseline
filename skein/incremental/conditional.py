"""Validators (ETag, Last-Modified) and conditional request headers (RFC 9110 section 13)."""

from __future__ import annotations

import re
from dataclasses import dataclass

from skein.net.headers import Headers
from skein.net.httpdate import format_http_date, parse_http_date

_ETAG_RE = re.compile(r'^(W/)?"[\x21\x23-\x7e\x80-\xff]*"$')


@dataclass(frozen=True)
class Validators:
    """Cache validators of a response as they will be sent back on the next request."""

    etag: str | None = None
    last_modified: str | None = None

    @property
    def empty(self) -> bool:
        return self.etag is None and self.last_modified is None


def normalize_etag(value: str | None) -> str | None:
    """A syntactically valid entity tag, or ``None``.

    Servers sometimes send unquoted tags; those are quoted rather than
    dropped, since they still identify a representation for that server.
    """
    if value is None:
        return None
    tag = value.strip()
    if not tag:
        return None
    if _ETAG_RE.match(tag):
        return tag
    if '"' in tag or any(ord(c) < 0x21 for c in tag):
        return None
    return f'"{tag}"'


def validators_from(headers: Headers, now: float) -> Validators:
    """Extract validators from response headers.

    A ``Last-Modified`` that cannot be parsed, or lies in the future
    (compared with the response ``Date`` or, failing that, ``now``), is not
    a usable validator and is ignored, as RFC 9110 section 8.8.2.1 asks.
    The value is normalised to IMF-fixdate form.
    """
    etag = normalize_etag(headers.get("ETag"))
    last_modified = None
    moment = parse_http_date(headers.get("Last-Modified"), reference=now)
    if moment is not None:
        date = parse_http_date(headers.get("Date"), reference=now)
        if moment <= (date if date is not None else now):
            last_modified = format_http_date(moment)
    return Validators(etag, last_modified)


def conditional_headers(validators: Validators) -> dict[str, str]:
    """Request headers that make the server answer 304 when the stored representation is current."""
    headers: dict[str, str] = {}
    if validators.etag is not None:
        headers["If-None-Match"] = validators.etag
    if validators.last_modified is not None:
        headers["If-Modified-Since"] = validators.last_modified
    return headers
