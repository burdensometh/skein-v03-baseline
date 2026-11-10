"""HTTP/1.1 wire protocol over asyncio streams.

This module knows how to serialise a request, parse a response head and read
a response body framed by ``Content-Length``, ``Transfer-Encoding: chunked``
or connection close (RFC 9112). It does not interpret status codes, follow
redirects or decode content codings; see :mod:`skein.net.client`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from skein.clock import Clock
from skein.net.errors import BodyTooLarge, ConnectionClosed, ProtocolError, TimeoutError
from skein.net.headers import Headers

MAX_LINE = 16 * 1024
MAX_HEAD = 64 * 1024
MAX_HEADERS = 256
READ_CHUNK = 64 * 1024
NO_BODY_STATUSES = frozenset({204, 304})


@dataclass
class ResponseHead:
    """Status line and header fields of a response."""

    version: str
    status: int
    reason: str
    headers: Headers
    raw_size: int

    @property
    def keep_alive(self) -> bool:
        """Whether the server allows the connection to be reused after this response."""
        tokens = self.headers.tokens("Connection")
        if "close" in tokens:
            return False
        if self.version == "HTTP/1.0":
            return "keep-alive" in tokens
        return True


@dataclass
class BodyResult:
    """A fully read response body (still content-encoded) and framing facts."""

    data: bytes
    wire_bytes: int
    reusable: bool
    trailers: Headers


def serialize_request(method: str, target: str, headers: Headers, body: bytes | None) -> bytes:
    """Render a request head (and body) as bytes ready for the socket."""
    if not method.isalpha() or not method.isupper():
        raise ValueError(f"invalid HTTP method {method!r}")
    if any(c in target for c in " \r\n"):
        raise ValueError(f"invalid request target {target!r}")
    lines = [f"{method} {target} HTTP/1.1"]
    lines.extend(f"{name}: {value}" for name, value in headers.items())
    head = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
    return head + body if body else head


class Connection:
    """One TCP (optionally TLS) connection to an origin."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        origin: str,
        clock: Clock,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.origin = origin
        self._clock = clock
        self.created_at = clock.monotonic()
        self.last_used = self.created_at
        self.requests_sent = 0
        self.closed = False

    @property
    def idle_for(self) -> float:
        return self._clock.monotonic() - self.last_used

    @property
    def peer_address(self) -> str | None:
        """IP address of the server end of the socket (``None`` if unknown)."""
        peer = self.writer.get_extra_info("peername")
        return str(peer[0]) if isinstance(peer, tuple) and peer else None

    def looks_alive(self) -> bool:
        """Cheap liveness check: the socket is open and no EOF has been seen."""
        if self.closed or self.writer.is_closing():
            return False
        return not self.reader.at_eof()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.writer.close()

    def abort(self) -> None:
        """Drop the connection without a graceful shutdown (used after errors)."""
        self.closed = True
        transport = self.writer.transport
        if transport is not None:
            transport.abort()

    async def send(self, payload: bytes, timeout: float) -> None:
        self.requests_sent += 1
        self.last_used = self._clock.monotonic()
        try:
            self.writer.write(payload)
            await asyncio.wait_for(self.writer.drain(), timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("timed out sending request", phase="write") from exc
        except (ConnectionError, OSError) as exc:
            raise ConnectionClosed(f"connection lost while sending: {exc}") from exc

    async def _readline(self, timeout: float) -> bytes:
        try:
            line = await asyncio.wait_for(self.reader.readuntil(b"\n"), timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("timed out waiting for response data") from exc
        except asyncio.IncompleteReadError as exc:
            if exc.partial:
                raise ConnectionClosed("connection closed in the middle of a line") from exc
            raise ConnectionClosed("connection closed before response") from exc
        except asyncio.LimitOverrunError as exc:
            raise ProtocolError("response line too long") from exc
        except (ConnectionError, OSError) as exc:
            raise ConnectionClosed(f"connection error: {exc}") from exc
        if len(line) > MAX_LINE:
            raise ProtocolError("response line too long")
        return line

    async def _read(self, size: int, timeout: float, exact: bool) -> bytes:
        try:
            if exact:
                return await asyncio.wait_for(self.reader.readexactly(size), timeout)
            return await asyncio.wait_for(self.reader.read(size), timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("timed out reading response body") from exc
        except asyncio.IncompleteReadError as exc:
            raise ConnectionClosed(
                f"connection closed after {len(exc.partial)} of {size} expected bytes"
            ) from exc
        except (ConnectionError, OSError) as exc:
            raise ConnectionClosed(f"connection error: {exc}") from exc

    async def read_head(self, timeout: float) -> ResponseHead:
        """Read a final (non-1xx) response head, skipping interim responses."""
        while True:
            head = await self._read_one_head(timeout)
            if 100 <= head.status < 200 and head.status != 101:
                continue
            self.last_used = self._clock.monotonic()
            return head

    async def _read_one_head(self, timeout: float) -> ResponseHead:
        status_line = await self._readline(timeout)
        size = len(status_line)
        # Tolerate stray blank lines left over from a sloppy previous response.
        blank_budget = 4
        while status_line in (b"\r\n", b"\n") and blank_budget:
            status_line = await self._readline(timeout)
            size += len(status_line)
            blank_budget -= 1
        version, status, reason = parse_status_line(status_line)
        headers = Headers()
        last_name: str | None = None
        while True:
            line = await self._readline(timeout)
            size += len(line)
            if size > MAX_HEAD:
                raise ProtocolError("response head too large")
            if line in (b"\r\n", b"\n"):
                break
            text = line.decode("latin-1").rstrip("\r\n")
            if text[:1] in (" ", "\t"):
                if last_name is None:
                    raise ProtocolError("header continuation without a field")
                folded = headers.get_all(last_name)[-1] + " " + text.strip()
                items = headers.items()
                items[-1] = (last_name, folded)
                headers = Headers(items)
                continue
            name, sep, value = text.partition(":")
            name = name.strip()
            if not sep or not name or " " in name:
                raise ProtocolError(f"malformed header line {text[:80]!r}")
            headers.add(name, value.strip())
            last_name = name
            if len(headers) > MAX_HEADERS:
                raise ProtocolError("too many header fields")
        return ResponseHead(version, status, reason, headers, size)

    async def read_body(
        self, head: ResponseHead, method: str, limit: int, timeout: float
    ) -> BodyResult:
        """Read the body that follows ``head`` according to RFC 9112 section 6.3."""
        if method == "HEAD" or head.status in NO_BODY_STATUSES or 100 <= head.status < 200:
            return BodyResult(b"", 0, head.keep_alive, Headers())
        transfer = head.headers.tokens("Transfer-Encoding")
        if transfer:
            if transfer[-1] != "chunked":
                data = await self._read_until_close(limit, timeout)
                return BodyResult(data, len(data), False, Headers())
            data, wire, trailers = await self._read_chunked(limit, timeout)
            return BodyResult(data, wire, head.keep_alive, trailers)
        length = content_length(head.headers)
        if length is not None:
            if length > limit:
                raise BodyTooLarge(limit)
            data = await self._read(length, timeout, exact=True) if length else b""
            return BodyResult(data, length, head.keep_alive, Headers())
        data = await self._read_until_close(limit, timeout)
        return BodyResult(data, len(data), False, Headers())

    async def _read_until_close(self, limit: int, timeout: float) -> bytes:
        buf = bytearray()
        while True:
            chunk = await self._read(READ_CHUNK, timeout, exact=False)
            if not chunk:
                return bytes(buf)
            buf.extend(chunk)
            if len(buf) > limit:
                raise BodyTooLarge(limit)

    async def _read_chunked(self, limit: int, timeout: float) -> tuple[bytes, int, Headers]:
        buf = bytearray()
        wire = 0
        while True:
            line = await self._readline(timeout)
            wire += len(line)
            size_text = line.split(b";", 1)[0].strip()
            try:
                size = int(size_text, 16)
            except ValueError as exc:
                raise ProtocolError(f"invalid chunk size {size_text[:20]!r}") from exc
            if size < 0:
                raise ProtocolError("negative chunk size")
            if size == 0:
                break
            if len(buf) + size > limit:
                raise BodyTooLarge(limit)
            buf.extend(await self._read(size, timeout, exact=True))
            terminator = await self._readline(timeout)
            wire += size + len(terminator)
            if terminator.strip():
                raise ProtocolError("chunk data not followed by CRLF")
        trailers = Headers()
        while True:
            line = await self._readline(timeout)
            wire += len(line)
            if line in (b"\r\n", b"\n"):
                break
            name, sep, value = line.decode("latin-1").partition(":")
            if sep and name.strip():
                trailers.add(name.strip(), value.strip())
        return bytes(buf), wire, trailers


def parse_status_line(line: bytes) -> tuple[str, int, str]:
    """Parse ``HTTP/1.1 200 OK`` into its parts."""
    text = line.decode("latin-1").rstrip("\r\n")
    parts = text.split(" ", 2)
    if len(parts) < 2 or not parts[0].startswith("HTTP/"):
        raise ProtocolError(f"malformed status line {text[:80]!r}")
    version = parts[0]
    if version not in ("HTTP/1.0", "HTTP/1.1"):
        raise ProtocolError(f"unsupported HTTP version {version!r}")
    code = parts[1]
    if len(code) != 3 or not code.isdigit():
        raise ProtocolError(f"malformed status code {code!r}")
    return version, int(code), parts[2] if len(parts) > 2 else ""


def content_length(headers: Headers) -> int | None:
    """Validated ``Content-Length``; conflicting duplicate values are an error."""
    values = {v.strip() for raw in headers.get_all("Content-Length") for v in raw.split(",")}
    if not values:
        return None
    if len(values) > 1:
        raise ProtocolError(f"conflicting Content-Length values {sorted(values)}")
    value = values.pop()
    if not value.isdigit():
        raise ProtocolError(f"invalid Content-Length {value!r}")
    return int(value)
