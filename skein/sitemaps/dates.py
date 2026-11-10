"""W3C Datetime parsing for sitemap ``<lastmod>`` values."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

_W3C_RE = re.compile(
    r"^(?P<year>\d{4})"
    r"(?:-(?P<month>\d{2})"
    r"(?:-(?P<day>\d{2})"
    r"(?:[T ](?P<hour>\d{2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:[.,](?P<fraction>\d+))?)?"
    r"(?P<tz>Z|[+-]\d{2}:?\d{2})?"
    r")?)?)?$"
)


def parse_w3c_datetime(text: str | None) -> float | None:
    """Parse a W3C Datetime (any of its six profiles) into a Unix timestamp.

    Missing components default to the start of the period; a missing zone
    designator means UTC. Returns ``None`` for unparseable input instead of
    raising, because sitemaps in the wild contain plenty of garbage dates.
    """
    if not text:
        return None
    match = _W3C_RE.match(text.strip())
    if match is None:
        return None
    parts = match.groupdict()
    try:
        moment = datetime(
            int(parts["year"]),
            int(parts["month"] or 1),
            int(parts["day"] or 1),
            int(parts["hour"] or 0),
            int(parts["minute"] or 0),
            int(parts["second"] or 0),
            int((parts["fraction"] or "0")[:6].ljust(6, "0")),
            tzinfo=_zone(parts["tz"]),
        )
    except ValueError:
        return None
    return moment.timestamp()


def _zone(designator: str | None) -> timezone:
    if not designator or designator == "Z":
        return timezone.utc
    sign = -1 if designator[0] == "-" else 1
    digits = designator[1:].replace(":", "")
    offset = timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
    return timezone(sign * offset)
