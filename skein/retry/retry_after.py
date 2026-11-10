"""``Retry-After`` header values (RFC 9110 section 10.2.3)."""

from __future__ import annotations

import re

from skein.net.httpdate import parse_http_date

_DELTA_RE = re.compile(r"^\d+$")


def parse_retry_after(value: str | None, now: float, server_date: str | None = None) -> float | None:
    """Seconds to wait according to a ``Retry-After`` value, relative to wall time ``now``.

    The header is either ``delay-seconds`` (a non-negative integer) or an
    HTTP-date. A date is measured against the response's ``Date`` header
    when that is present and valid, so a server whose clock is off still
    gets the pause it asked for; otherwise against ``now``. A date in the
    past means "retry now" (``0.0``). Anything else, including negative or
    fractional numbers, is not a valid value and yields ``None`` so that the
    caller falls back to its own back-off.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if _DELTA_RE.match(text):
        return float(int(text))
    moment = parse_http_date(text, reference=now)
    if moment is None:
        return None
    reference = parse_http_date(server_date, reference=now) if server_date else None
    return max(0.0, moment - (reference if reference is not None else now))
