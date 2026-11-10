"""Deciding whether a failed fetch is worth retrying.

A *transient* failure (timeouts, connection resets, 429, most 5xx) may well
succeed later; a *permanent* one (404, 410, oversized bodies, redirect loops)
will not, and retrying it only burdens the site. Some transient failures are
also *host-level*: they say the whole host is overloaded or unreachable, so
every request to it should pause, not just the one URL.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from skein.net.errors import (
    BodyTooLarge,
    ConnectError,
    ConnectionClosed,
    DecodingError,
    HttpError,
    InvalidURL,
    ProtocolError,
    RedirectError,
    TimeoutError,
)
from skein.net.headers import Headers
from skein.retry.retry_after import parse_retry_after


class ErrorClass(str, Enum):
    """Retry classification stored with every failed fetch."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    ROBOTS = "robots"


@dataclass(frozen=True)
class Failure:
    """A classified failure of one fetch attempt."""

    error_class: ErrorClass
    reason: str
    host_level: bool = False
    retry_after: float | None = None
    status: int | None = None

    @property
    def transient(self) -> bool:
        return self.error_class is ErrorClass.TRANSIENT


TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504, 507, 509, 520, 521, 522, 523, 524})
HOST_LEVEL_STATUSES = frozenset({429, 502, 503, 504, 509, 521, 522, 523})
RETRY_AFTER_STATUSES = frozenset({429, 503})

_TRANSIENT_ERRORS: tuple[type[HttpError], ...] = (ConnectError, TimeoutError, ConnectionClosed, ProtocolError)
_PERMANENT_ERRORS: tuple[type[HttpError], ...] = (BodyTooLarge, DecodingError, RedirectError, InvalidURL)
_HOST_LEVEL_TIMEOUT_PHASES = frozenset({"connect"})


def classify_status(status: int, headers: Headers, now: float) -> Failure | None:
    """Classify an HTTP status; ``None`` for statuses that are not failures (1xx-3xx)."""
    if status < 400:
        return None
    reason = f"HTTP {status}"
    if status in TRANSIENT_STATUSES or (status >= 500 and status not in (501, 505)):
        retry_after = None
        if status in RETRY_AFTER_STATUSES:
            retry_after = parse_retry_after(headers.get("Retry-After"), now, headers.get("Date"))
        return Failure(ErrorClass.TRANSIENT, reason, status in HOST_LEVEL_STATUSES, retry_after, status)
    return Failure(ErrorClass.PERMANENT, reason, False, None, status)


def classify_exception(exc: HttpError) -> Failure:
    """Classify a client exception; unknown :class:`HttpError` subclasses count as transient."""
    reason = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, _PERMANENT_ERRORS):
        return Failure(ErrorClass.PERMANENT, reason)
    if isinstance(exc, TimeoutError):
        return Failure(ErrorClass.TRANSIENT, reason, exc.phase in _HOST_LEVEL_TIMEOUT_PHASES)
    if isinstance(exc, ConnectError):
        return Failure(ErrorClass.TRANSIENT, reason, True)
    return Failure(ErrorClass.TRANSIENT, reason)


_ERRORS_BY_NAME = {cls.__name__: ErrorClass.TRANSIENT for cls in _TRANSIENT_ERRORS}
_ERRORS_BY_NAME.update({cls.__name__: ErrorClass.PERMANENT for cls in _PERMANENT_ERRORS})
_ERRORS_BY_NAME.update({name: ErrorClass.PERMANENT for name in ("TooManyRedirects", "RedirectLoop", "SitemapError")})


def classify_recorded(status: int | None, error: str | None) -> ErrorClass | None:
    """Classify a stored fetch record from its status and error text.

    Used when upgrading databases written before failures were classified:
    the error text starts with the exception class name (``TimeoutError: ...``)
    or is a robots.txt denial.
    """
    if error is None and status is not None and status < 400:
        return None
    if error is not None and error.startswith("robots.txt"):
        return ErrorClass.ROBOTS
    if status is not None and status >= 400:
        failure = classify_status(status, Headers(), 0.0)
        return failure.error_class if failure is not None else None
    if error is None:
        return None
    name = error.split(":", 1)[0].strip()
    return _ERRORS_BY_NAME.get(name, ErrorClass.TRANSIENT if "will retry" in error else ErrorClass.PERMANENT)
