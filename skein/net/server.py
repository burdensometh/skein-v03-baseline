"""A minimal HTTP/1.1 server for small read-only endpoints (metrics, archive replay).

One request per connection: the request head is read (with a timeout),
``respond(method, target)`` computes the reply and the connection is
closed after it is written. Request bodies are not read; the endpoints
served this way only answer ``GET`` and ``HEAD``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from typing import Callable

log = logging.getLogger(__name__)

READ_TIMEOUT = 5.0
MAX_HEAD = 16 * 1024


@dataclass
class Reply:
    status: int
    reason: str
    body: bytes = b""
    headers: list[tuple[str, str]] = field(default_factory=lambda: [("Content-Type", "text/plain; charset=utf-8")])

    @classmethod
    def text(cls, status: int, reason: str, message: str) -> "Reply":
        return cls(status, reason, message.encode("utf-8"))

    def encode(self, head_only: bool) -> bytes:
        lines = [f"HTTP/1.1 {self.status} {self.reason}", *(f"{n}: {v}" for n, v in self.headers),
                 f"Content-Length: {len(self.body)}", "Connection: close"]
        head = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1", "replace")
        return head if head_only else head + self.body


Responder = Callable[[str, str], Reply]


class SimpleServer:
    """Serves ``respond`` on ``host:port``; port 0 picks a free port (see :attr:`port`)."""

    def __init__(self, respond: Responder, *, host: str = "127.0.0.1", port: int = 0) -> None:
        self.respond = respond
        self.host = host
        self.requested_port = port
        self.port = 0
        self.requests = 0
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.requested_port, limit=MAX_HEAD)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), READ_TIMEOUT)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError, ConnectionError):
            writer.close()
            return
        parts = head.split(b"\r\n", 1)[0].decode("latin-1").split()
        method, target = (parts[0], parts[1]) if len(parts) >= 2 else ("", "/")
        self.requests += 1
        try:
            reply = self.respond(method, target)
        except Exception:  # noqa: BLE001 - a broken endpoint answers 500 instead of dropping the connection
            log.exception("error answering %s %s", method, target)
            reply = Reply.text(500, "Internal Server Error", "internal error\n")
        writer.write(reply.encode(head_only=method == "HEAD"))
        with contextlib.suppress(ConnectionError):
            await writer.drain()
        writer.close()
