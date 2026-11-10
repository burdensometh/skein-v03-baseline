"""Keep-alive connection pool keyed by origin."""

from __future__ import annotations

import asyncio
import ssl
from dataclasses import dataclass, field

from skein.clock import Clock
from skein.net.errors import ConnectError, TimeoutError
from skein.net.http11 import MAX_HEAD, Connection
from skein.urls.parse import URL


@dataclass
class PoolStats:
    """Counters describing pool behaviour, mainly for tests and run summaries."""

    connections_opened: int = 0
    connections_reused: int = 0
    connections_discarded: int = 0
    per_origin_opened: dict[str, int] = field(default_factory=dict)


@dataclass
class _OriginSlot:
    limit: asyncio.Semaphore
    idle: list[Connection] = field(default_factory=list)


class ConnectionPool:
    """Hands out connections per origin and keeps idle ones for reuse.

    ``max_per_origin`` caps simultaneously checked-out connections to one
    origin; callers beyond the cap wait. Idle connections older than
    ``idle_timeout`` or that have seen EOF are discarded on checkout.
    """

    def __init__(
        self,
        clock: Clock,
        *,
        max_per_origin: int = 6,
        max_idle_per_origin: int = 4,
        idle_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        if max_per_origin < 1:
            raise ValueError("max_per_origin must be >= 1")
        self._clock = clock
        self.max_per_origin = max_per_origin
        self.max_idle_per_origin = max_idle_per_origin
        self.idle_timeout = idle_timeout
        self.connect_timeout = connect_timeout
        self._ssl_context = ssl_context
        self._slots: dict[str, _OriginSlot] = {}
        self._closed = False
        self.stats = PoolStats()

    def _slot(self, origin: str) -> _OriginSlot:
        slot = self._slots.get(origin)
        if slot is None:
            slot = _OriginSlot(asyncio.Semaphore(self.max_per_origin))
            self._slots[origin] = slot
        return slot

    def idle_count(self, origin: str | None = None) -> int:
        if origin is not None:
            slot = self._slots.get(origin)
            return len(slot.idle) if slot else 0
        return sum(len(s.idle) for s in self._slots.values())

    async def acquire(self, url: URL, *, fresh: bool = False) -> tuple[Connection, bool]:
        """Check out a connection to ``url``'s origin.

        Returns ``(connection, reused)``. With ``fresh=True`` idle connections
        are skipped, which callers use to retry after a stale keep-alive.
        """
        if self._closed:
            raise ConnectError("connection pool is closed", str(url))
        origin = url.origin
        slot = self._slot(origin)
        await slot.limit.acquire()
        try:
            if not fresh:
                while slot.idle:
                    conn = slot.idle.pop()
                    if conn.looks_alive() and conn.idle_for <= self.idle_timeout:
                        self.stats.connections_reused += 1
                        return conn, True
                    conn.close()
                    self.stats.connections_discarded += 1
            conn = await self._open(url, origin)
            return conn, False
        except BaseException:
            slot.limit.release()
            raise

    def release(self, conn: Connection, *, reusable: bool) -> None:
        """Return a checked-out connection; it is kept only if ``reusable``."""
        slot = self._slots.get(conn.origin)
        if slot is None:  # pragma: no cover - acquire always creates the slot
            conn.close()
            return
        slot.limit.release()
        if (
            reusable
            and not self._closed
            and conn.looks_alive()
            and len(slot.idle) < self.max_idle_per_origin
        ):
            slot.idle.append(conn)
        else:
            conn.close()

    async def _open(self, url: URL, origin: str) -> Connection:
        host = url.host or ""
        port = url.effective_port
        if port is None:
            raise ConnectError(f"no port known for scheme {url.scheme!r}", str(url))
        context: ssl.SSLContext | None = None
        if url.scheme == "https":
            context = self._ssl_context or ssl.create_default_context()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host, port, ssl=context, server_hostname=host if context else None,
                    limit=MAX_HEAD,
                ),
                self.connect_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"connect to {host}:{port} timed out", str(url), phase="connect") from exc
        except (OSError, ssl.SSLError) as exc:
            raise ConnectError(f"cannot connect to {host}:{port}: {exc}", str(url)) from exc
        self.stats.connections_opened += 1
        self.stats.per_origin_opened[origin] = self.stats.per_origin_opened.get(origin, 0) + 1
        return Connection(reader, writer, origin, self._clock)

    async def close(self) -> None:
        """Close all idle connections and refuse further checkouts."""
        self._closed = True
        writers = []
        for slot in self._slots.values():
            for conn in slot.idle:
                conn.close()
                writers.append(conn.writer)
            slot.idle.clear()
        for writer in writers:
            try:
                await asyncio.wait_for(writer.wait_closed(), 1.0)
            except (asyncio.TimeoutError, OSError, ConnectionError):
                continue
