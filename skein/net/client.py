"""Asynchronous HTTP/1.1 client built directly on asyncio streams."""

from __future__ import annotations

import asyncio
import logging
import ssl
from dataclasses import dataclass, field, replace
from typing import Callable, Mapping

from skein import __version__
from skein.clock import Clock, SystemClock
from skein.net.content import SUPPORTED_CODINGS, decode_content
from skein.net.errors import (
    ConnectError,
    ConnectionClosed,
    HttpError,
    InvalidURL,
    RedirectError,
    RedirectLoop,
    TimeoutError,
    TooManyRedirects,
)
from skein.net.headers import Headers
from skein.net.http11 import BodyResult, Connection, ResponseHead, serialize_request
from skein.net.pool import ConnectionPool
from skein.net.response import REDIRECT_STATUSES, RedirectHop, Response
from skein.urls.canonical import normalize_host
from skein.urls.parse import URL, URLError, parse_url, resolve

log = logging.getLogger(__name__)

DEFAULT_USER_AGENT = f"skein/{__version__} (+https://skein.invalid/bot)"
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"})
FETCHABLE_SCHEMES = frozenset({"http", "https"})
_CREDENTIAL_HEADERS = ("authorization", "cookie", "proxy-authorization")

RedirectFilter = Callable[[str], bool]


@dataclass
class ClientConfig:
    """Tunables for :class:`HttpClient`. Times are in seconds, sizes in bytes."""

    user_agent: str = DEFAULT_USER_AGENT
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    total_timeout: float = 60.0
    max_body_bytes: int = 10 * 1024 * 1024
    max_redirects: int = 10
    max_connections_per_origin: int = 6
    max_idle_per_origin: int = 4
    idle_timeout: float = 30.0
    verify_tls: bool = True
    default_headers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("connect_timeout", "read_timeout", "total_timeout"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_body_bytes <= 0:
            raise ValueError("max_body_bytes must be positive")
        if self.max_redirects < 0:
            raise ValueError("max_redirects must be >= 0")


@dataclass
class _Exchange:
    head: ResponseHead
    body: BodyResult
    peer: str | None = None


class HttpClient:
    """Fetches URLs over HTTP/1.1 with pooling, decoding, redirects and limits.

    The client is safe to share between tasks of one event loop. Use it as an
    async context manager, or call :meth:`close` when done.
    """

    def __init__(self, config: ClientConfig | None = None, clock: Clock | None = None) -> None:
        self.config = config or ClientConfig()
        self.clock = clock or SystemClock()
        context: ssl.SSLContext | None = None
        if not self.config.verify_tls:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        self.pool = ConnectionPool(
            self.clock,
            max_per_origin=self.config.max_connections_per_origin,
            max_idle_per_origin=self.config.max_idle_per_origin,
            idle_timeout=self.config.idle_timeout,
            connect_timeout=self.config.connect_timeout,
            ssl_context=context,
        )
        self.closed = False

    async def __aenter__(self) -> "HttpClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Close pooled connections; later requests raise :class:`ConnectError`."""
        if self.closed:
            return
        self.closed = True
        await self.pool.close()

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | Headers | None = None,
        body: bytes | None = None,
        follow_redirects: bool = True,
        redirect_filter: RedirectFilter | None = None,
        max_body_bytes: int | None = None,
        total_timeout: float | None = None,
    ) -> Response:
        """Perform a request and return the final :class:`Response`.

        ``redirect_filter`` is consulted with each absolute redirect target;
        returning ``False`` stops following and returns the 3xx response.
        Raises a subclass of :class:`~skein.net.errors.HttpError` on failure;
        non-2xx statuses are *not* errors.
        """
        if self.closed:
            raise ConnectError("client is closed", url)
        method = method.upper()
        deadline = total_timeout or self.config.total_timeout
        started = self.clock.monotonic()
        try:
            response = await asyncio.wait_for(
                self._follow(method, url, headers, body, follow_redirects, redirect_filter,
                             max_body_bytes or self.config.max_body_bytes),
                deadline,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"request exceeded total timeout of {deadline}s", url, phase="total") from exc
        response.elapsed = self.clock.monotonic() - started
        return response

    def _target(self, url: str) -> URL:
        try:
            parsed = parse_url(url)
        except URLError as exc:
            raise InvalidURL(str(exc), url) from exc
        if parsed.scheme not in FETCHABLE_SCHEMES:
            raise InvalidURL(f"unsupported scheme {parsed.scheme!r}", url)
        if not parsed.host:
            raise InvalidURL("URL has no host", url)
        try:
            host = normalize_host(parsed.host)
        except URLError as exc:
            raise InvalidURL(str(exc), url) from exc
        return replace(parsed, host=host, fragment=None, userinfo=None)

    def _build_headers(self, target: URL, extra: Mapping[str, str] | Headers | None, body: bytes | None) -> Headers:
        headers = Headers()
        headers.add("Host", target.host_header)
        headers.add("User-Agent", self.config.user_agent)
        headers.add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        headers.add("Accept-Encoding", ", ".join(SUPPORTED_CODINGS))
        headers.add("Connection", "keep-alive")
        for name, value in self.config.default_headers.items():
            headers.set(name, value)
        if extra is not None:
            for name, value in extra.items():
                headers.set(name, value)
        if body is not None:
            headers.set("Content-Length", str(len(body)))
        return headers

    async def _follow(
        self,
        method: str,
        url: str,
        extra_headers: Mapping[str, str] | Headers | None,
        body: bytes | None,
        follow_redirects: bool,
        redirect_filter: RedirectFilter | None,
        limit: int,
    ) -> Response:
        history: list[RedirectHop] = []
        target = self._target(url)
        visited = {(method, str(target))}
        request_url = str(target)
        extra = Headers(extra_headers.items()) if extra_headers is not None else Headers()
        while True:
            headers = self._build_headers(target, extra, body)
            exchange = await self._exchange(method, target, headers, body, limit)
            response = self._make_response(method, target, exchange, limit)
            response.request_headers = headers.items()
            response.request_url = request_url
            response.history = list(history)
            if not (follow_redirects and response.is_redirect):
                return response
            location = response.location or ""
            try:
                next_target = self._target(str(resolve(target, location)))
            except (InvalidURL, URLError) as exc:
                raise RedirectError(f"cannot follow redirect to {location!r}: {exc}", str(target)) from exc
            if redirect_filter is not None and not redirect_filter(str(next_target)):
                return response
            history.append(RedirectHop(str(target), response.status, location))
            if len(history) > self.config.max_redirects:
                raise TooManyRedirects(f"more than {self.config.max_redirects} redirects", request_url)
            if response.status == 303 and method != "HEAD" or (
                response.status in (301, 302) and method == "POST"
            ):
                method, body = "GET", None
                for name in ("Content-Type", "Content-Length"):
                    extra.remove(name)
            if next_target.origin != target.origin:
                for name in _CREDENTIAL_HEADERS:
                    extra.remove(name)
            key = (method, str(next_target))
            if key in visited:
                chain = " -> ".join([hop.url for hop in history] + [str(next_target)])
                raise RedirectLoop(f"redirect loop: {chain}", request_url)
            visited.add(key)
            log.debug("redirect %s %s -> %s", response.status, target, next_target)
            target = next_target

    async def _exchange(
        self, method: str, target: URL, headers: Headers, body: bytes | None, limit: int
    ) -> _Exchange:
        conn, reused = await self.pool.acquire(target)
        try:
            return await self._round_trip(conn, method, target, headers, body, limit)
        except ConnectionClosed as exc:
            # A keep-alive connection the server already dropped fails before any
            # response byte arrives; that request was never processed and can be
            # replayed once on a fresh connection.
            if not (reused and exc.before_response and method in IDEMPOTENT_METHODS):
                raise
            log.debug("stale keep-alive connection to %s, retrying", target.origin)
        conn, _ = await self.pool.acquire(target, fresh=True)
        return await self._round_trip(conn, method, target, headers, body, limit)

    async def _round_trip(
        self, conn: Connection, method: str, target: URL, headers: Headers, body: bytes | None, limit: int
    ) -> _Exchange:
        reusable = False
        got_head = False
        try:
            payload = serialize_request(method, target.request_target, headers, body)
            await conn.send(payload, self.config.read_timeout)
            head = await conn.read_head(self.config.read_timeout)
            got_head = True
            body_result = await conn.read_body(head, method, limit, self.config.read_timeout)
            reusable = body_result.reusable
            return _Exchange(head, body_result, conn.peer_address)
        except ConnectionClosed as exc:
            exc.url = str(target)
            exc.before_response = not got_head
            conn.abort()
            raise
        except HttpError as exc:
            exc.url = exc.url or str(target)
            conn.abort()
            raise
        except asyncio.CancelledError:
            conn.abort()
            raise
        finally:
            self.pool.release(conn, reusable=reusable)

    def _make_response(self, method: str, target: URL, exchange: _Exchange, limit: int) -> Response:
        head = exchange.head
        codings = head.headers.tokens("Content-Encoding")
        data = exchange.body.data
        if codings and head.status not in REDIRECT_STATUSES:
            try:
                data = decode_content(data, codings, limit)
            except HttpError as exc:
                exc.url = str(target)
                raise
        return Response(
            url=str(target),
            status=head.status,
            reason=head.reason,
            headers=head.headers,
            body=data,
            http_version=head.version,
            method=method,
            wire_bytes=head.raw_size + exchange.body.wire_bytes,
            trailers=exchange.body.trailers,
            payload=exchange.body.data,
            remote_address=exchange.peer,
        )
