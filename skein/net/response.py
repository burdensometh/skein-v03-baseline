"""The :class:`Response` value returned by :class:`skein.net.client.HttpClient`."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property

from skein.net.charset import Detected, decode_body
from skein.net.headers import Headers, parse_header_params

REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


@dataclass(frozen=True)
class RedirectHop:
    """One followed redirect: the URL requested and the status it answered with."""

    url: str
    status: int
    location: str


@dataclass
class Response:
    """A complete HTTP response with its body already content-decoded.

    ``payload`` is the entity body as transferred (transfer coding removed,
    content coding still applied) and ``request_headers`` the header fields
    sent for the final request; both are what a WARC archive records.
    """

    url: str
    status: int
    reason: str
    headers: Headers
    body: bytes
    http_version: str = "HTTP/1.1"
    request_url: str = ""
    method: str = "GET"
    history: list[RedirectHop] = field(default_factory=list)
    elapsed: float = 0.0
    wire_bytes: int = 0
    trailers: Headers = field(default_factory=Headers)
    payload: bytes = b""
    request_headers: list[tuple[str, str]] = field(default_factory=list)
    remote_address: str | None = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    @property
    def is_redirect(self) -> bool:
        return self.status in REDIRECT_STATUSES and "Location" in self.headers

    @property
    def location(self) -> str | None:
        return self.headers.get("Location")

    @property
    def content_type(self) -> str:
        """Lower-cased media type without parameters (``""`` when absent)."""
        raw = self.headers.get("Content-Type")
        return parse_header_params(raw)[0] if raw else ""

    @property
    def is_html(self) -> bool:
        media = self.content_type
        if media in ("text/html", "application/xhtml+xml"):
            return True
        if media:
            return False
        head = self.body[:512].lstrip().lower()
        return head.startswith((b"<!doctype html", b"<html")) or b"<body" in head

    @cached_property
    def _decoded(self) -> tuple[str, Detected]:
        return decode_body(self.body, self.headers.get("Content-Type"))

    @property
    def text(self) -> str:
        """Body decoded with the detected charset (BOM > header > meta > UTF-8 > cp1252)."""
        return self._decoded[0]

    @property
    def charset(self) -> Detected:
        return self._decoded[1]

    def __repr__(self) -> str:
        return f"<Response {self.status} {self.url} {len(self.body)}B>"
