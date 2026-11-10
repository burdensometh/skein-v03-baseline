"""Exception hierarchy for the HTTP client."""

from __future__ import annotations


class HttpError(Exception):
    """Base class for every failure raised by :mod:`skein.net`."""

    def __init__(self, message: str, url: str | None = None) -> None:
        super().__init__(message)
        self.url = url

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base} ({self.url})" if self.url else base


class ConnectError(HttpError):
    """The TCP/TLS connection could not be established."""


class ConnectionClosed(HttpError):
    """The peer closed or reset the connection before the response was complete.

    ``before_response`` is true when not a single byte of the response head
    had arrived, which makes replaying an idempotent request safe.
    """

    def __init__(self, message: str, url: str | None = None, before_response: bool = False) -> None:
        super().__init__(message, url)
        self.before_response = before_response


class TimeoutError(HttpError):  # noqa: A001 - deliberately scoped to skein.net
    """A connect, read or total deadline elapsed."""

    def __init__(self, message: str, url: str | None = None, phase: str = "read") -> None:
        super().__init__(message, url)
        self.phase = phase


class ProtocolError(HttpError):
    """The server sent bytes that do not form a valid HTTP/1.x response."""


class BodyTooLarge(HttpError):
    """The response body (on the wire or after decoding) exceeded the configured limit."""

    def __init__(self, limit: int, url: str | None = None) -> None:
        super().__init__(f"response body exceeds {limit} bytes", url)
        self.limit = limit


class DecodingError(HttpError):
    """A ``Content-Encoding`` could not be decoded."""


class RedirectError(HttpError):
    """Base class for redirect handling failures."""


class TooManyRedirects(RedirectError):
    """The redirect chain was longer than ``max_redirects``."""


class RedirectLoop(RedirectError):
    """A redirect pointed back to a URL already visited in the same chain."""


class InvalidURL(HttpError):
    """The URL cannot be fetched (unsupported scheme, missing host, ...)."""
