"""HTTP-date parsing and formatting (RFC 9110 section 5.6.7).

Recipients must accept all three historical formats::

    Sun, 06 Nov 1994 08:49:37 GMT    ; IMF-fixdate (the only one senders may use)
    Sunday, 06-Nov-94 08:49:37 GMT   ; obsolete RFC 850 format
    Sun Nov  6 08:49:37 1994         ; ANSI C asctime() format

Two-digit RFC 850 years are interpreted relative to a reference time: a
year that would lie more than 50 years in the future means the most recent
past year with the same last two digits.
"""

from __future__ import annotations

import calendar
import re
import time

_MONTHS = {name: index for index, name in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), start=1)}
_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTH_NAMES = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

_TIME = r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
_IMF_RE = re.compile(
    r"^[A-Za-z]{3},\s+(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]{3})\s+(?P<year>\d{4})\s+" + _TIME + r"\s+(?:GMT|UTC|\+0000)$"
)
_RFC850_RE = re.compile(
    r"^[A-Za-z]+,\s+(?P<day>\d{1,2})-(?P<month>[A-Za-z]{3})-(?P<year>\d{2}|\d{4})\s+" + _TIME + r"\s+GMT$"
)
_ASCTIME_RE = re.compile(
    r"^[A-Za-z]{3}\s+(?P<month>[A-Za-z]{3})\s+(?P<day>\d{1,2})\s+" + _TIME + r"\s+(?P<year>\d{4})$"
)


def parse_http_date(text: str | None, *, reference: float | None = None) -> float | None:
    """Return the Unix timestamp of an HTTP-date, or ``None`` if ``text`` is not one.

    ``reference`` (default: now) only matters for two-digit RFC 850 years.
    Out-of-range fields (``Feb 30``, hour 25) are rejected rather than
    normalised, because a garbled ``Retry-After`` or ``Last-Modified`` should
    be ignored instead of being turned into a plausible-looking time.
    """
    if not text:
        return None
    value = text.strip()
    for pattern in (_IMF_RE, _RFC850_RE, _ASCTIME_RE):
        match = pattern.match(value)
        if match is not None:
            break
    else:
        return None
    month = _MONTHS.get(match.group("month").lower())
    if month is None:
        return None
    year = int(match.group("year"))
    if len(match.group("year")) == 2:
        year = _expand_two_digit_year(year, time.time() if reference is None else reference)
    day, hour = int(match.group("day")), int(match.group("hour"))
    minute, second = int(match.group("minute")), int(match.group("second"))
    if not (1 <= day <= calendar.monthrange(year, month)[1] and hour < 24 and minute < 60 and second <= 60):
        return None
    return float(calendar.timegm((year, month, day, hour, minute, min(second, 59), 0, 0, 0)))


def _expand_two_digit_year(two_digits: int, reference: float) -> int:
    current = time.gmtime(reference).tm_year
    year = current - current % 100 + two_digits
    if year > current + 50:
        year -= 100
    return year


def format_http_date(epoch: float) -> str:
    """Render ``epoch`` as an IMF-fixdate (``Sun, 06 Nov 1994 08:49:37 GMT``)."""
    moment = time.gmtime(int(epoch))
    return (
        f"{_DAYS[moment.tm_wday]}, {moment.tm_mday:02d} {_MONTH_NAMES[moment.tm_mon - 1]} "
        f"{moment.tm_year:04d} {moment.tm_hour:02d}:{moment.tm_min:02d}:{moment.tm_sec:02d} GMT"
    )
