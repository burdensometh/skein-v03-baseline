"""Fetching and caching robots.txt per origin with RFC 9309 status semantics."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from skein.clock import Clock
from skein.incremental.conditional import Validators, conditional_headers, validators_from
from skein.net.client import HttpClient
from skein.net.errors import BodyTooLarge, HttpError, TooManyRedirects
from skein.net.response import Response
from skein.retry.retry_after import parse_retry_after
from skein.robots.parser import MAX_ROBOTS_BYTES, Decision, RobotsTxt, parse_robots
from skein.urls.parse import URL, parse_url

log = logging.getLogger(__name__)

ROBOTS_MAX_REDIRECTS = 5


@dataclass
class RobotsEntry:
    """A cached robots.txt outcome for one origin.

    ``state`` is one of ``"fetched"`` (2xx, parsed), ``"unavailable"`` (4xx,
    everything allowed), ``"unreachable"`` (5xx, 429 or network failure,
    everything disallowed) or ``"stale"`` (a previous good copy kept because
    the refresh was unreachable).
    """

    origin: str
    robots: RobotsTxt
    state: str
    status: int | None
    fetched_at: float
    expires_at: float
    error: str | None = None
    body: bytes | None = None
    etag: str | None = None
    last_modified: str | None = None

    def fresh(self, now: float) -> bool:
        return now < self.expires_at

    def to_record(self) -> dict[str, Any]:
        return {"origin": self.origin, "state": self.state, "status": self.status, "body": self.body,
                "fetched_at": self.fetched_at, "expires_at": self.expires_at, "etag": self.etag,
                "last_modified": self.last_modified, "error": self.error}

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "RobotsEntry":
        """Rebuild an entry from :meth:`to_record` output; rules are re-parsed from the stored body."""
        state = record["state"]
        if state in ("fetched", "stale") and record["body"] is not None:
            robots = parse_robots(bytes(record["body"]))
        elif state == "unavailable":
            robots = RobotsTxt.allow_all(f"robots.txt unavailable ({record['error']}); allowing all")
        else:
            robots = RobotsTxt.disallow_all(f"robots.txt unreachable ({record['error']}); disallowing all")
        return cls(record["origin"], robots, state, record["status"], record["fetched_at"], record["expires_at"],
                   record["error"], record["body"], record["etag"], record["last_modified"])


class RobotsPersistence(Protocol):
    """Where :class:`RobotsCache` keeps entries across processes (see :class:`skein.store.robots.RobotsStore`)."""

    def load(self, origin: str) -> dict[str, Any] | None:
        """The stored record for ``origin``, if any."""

    def save(self, record: dict[str, Any]) -> None:
        """Insert or replace the record of ``record["origin"]``."""


class RobotsCache:
    """Per-origin robots.txt cache.

    Successful fetches and 4xx answers live for ``ttl`` seconds; unreachable
    outcomes are retried after ``error_ttl``. When a refresh is unreachable but
    a good copy younger than ``stale_ttl`` exists, the old rules keep applying.
    Concurrent lookups for the same origin share one fetch. With a
    ``persistence`` backend entries survive restarts, and refreshing a
    fetched copy is a conditional request: a 304 extends its lifetime
    without downloading or re-parsing it. An ``observer`` sees every
    robots.txt response received (the crawler archives them).
    """

    def __init__(
        self,
        client: HttpClient,
        clock: Clock,
        user_agent: str,
        *,
        ttl: float = 24 * 3600.0,
        error_ttl: float = 600.0,
        stale_ttl: float = 7 * 24 * 3600.0,
        persistence: RobotsPersistence | None = None,
    ) -> None:
        self._client = client
        self._persistence = persistence
        self._clock = clock
        self.user_agent = user_agent
        self.ttl = ttl
        self.error_ttl = error_ttl
        self.stale_ttl = stale_ttl
        self._entries: dict[str, RobotsEntry] = {}
        self._inflight: dict[str, asyncio.Future[RobotsEntry]] = {}
        self.observer: Callable[[str, Response], None] | None = None
        self.fetches = 0
        self.revalidated = 0

    def _known(self, origin: str) -> RobotsEntry | None:
        """The in-memory entry, else the persisted one.

        Another process sharing the database may store an entry at any
        time, so the persistence is consulted again as long as nothing
        fresh is known here.
        """
        entry = self._entries.get(origin)
        if self._persistence is not None and (entry is None or not entry.fresh(self._clock.time())):
            record = self._persistence.load(origin)
            if record is not None:
                stored = RobotsEntry.from_record(record)
                if entry is None or stored.fetched_at > entry.fetched_at:
                    self._entries[origin] = entry = stored
        return entry

    def cached(self, url: str | URL) -> RobotsEntry | None:
        """The fresh cache entry for ``url``'s origin, if any (never fetches)."""
        parsed = parse_url(url) if isinstance(url, str) else url
        entry = self._known(parsed.origin)
        if entry is not None and entry.fresh(self._clock.time()):
            return entry
        return None

    def decide_cached(self, url: str | URL) -> Decision | None:
        """Access decision from the cache alone; ``None`` when a fetch is needed."""
        entry = self.cached(url)
        return entry.robots.decide(url, self.user_agent) if entry else None

    async def get(self, url: str | URL) -> RobotsEntry:
        """Return the entry for ``url``'s origin, fetching robots.txt when needed."""
        parsed = parse_url(url) if isinstance(url, str) else url
        origin = parsed.origin
        entry = self.cached(parsed)
        if entry is not None:
            return entry
        pending = self._inflight.get(origin)
        if pending is not None:
            return await asyncio.shield(pending)
        future: asyncio.Future[RobotsEntry] = asyncio.get_running_loop().create_future()
        self._inflight[origin] = future
        try:
            entry = await self._fetch(parsed)
            self._entries[origin] = entry
            if self._persistence is not None:
                self._persistence.save(entry.to_record())
            future.set_result(entry)
            return entry
        except BaseException as exc:
            future.set_exception(exc)
            future.exception()
            raise
        finally:
            del self._inflight[origin]

    def store(self, url: str | URL, robots: RobotsTxt, status: int = 200) -> RobotsEntry:
        """Install rules for an origin directly (used for preloaded or test rules)."""
        origin = (parse_url(url) if isinstance(url, str) else url).origin
        now = self._clock.time()
        entry = RobotsEntry(origin, robots, "fetched", status, now, now + self.ttl)
        self._entries[origin] = entry
        return entry

    async def _fetch(self, url: URL) -> RobotsEntry:
        origin = url.origin
        robots_url = str(URL(url.scheme, None, url.host, url.port, "/robots.txt", None, None))
        now = self._clock.time()
        self.fetches += 1
        previous = self._known(origin)
        headers = {"Accept": "text/plain,*/*;q=0.5"}
        if previous is not None and previous.state in ("fetched", "stale") and previous.body is not None:
            headers.update(conditional_headers(Validators(previous.etag, previous.last_modified)))
        try:
            response = await self._client.request(
                "GET",
                robots_url,
                headers=headers,
                max_body_bytes=MAX_ROBOTS_BYTES * 4,
                redirect_filter=_redirect_counter(ROBOTS_MAX_REDIRECTS),
            )
        except TooManyRedirects as exc:
            return self._unavailable(origin, None, now, f"too many redirects: {exc}")
        except BodyTooLarge as exc:
            return self._unreachable(origin, None, now, str(exc))
        except HttpError as exc:
            log.info("robots.txt for %s unreachable: %s", origin, exc)
            return self._unreachable(origin, None, now, f"{type(exc).__name__}: {exc}")
        if self.observer is not None:
            self.observer(origin, response)
        status = response.status
        validators = validators_from(response.headers, now)
        if status == 304 and previous is not None and previous.body is not None:
            self.revalidated += 1
            return RobotsEntry(origin, previous.robots, "fetched", status, now, now + self.ttl, None, previous.body,
                               validators.etag or previous.etag, validators.last_modified or previous.last_modified)
        if 200 <= status < 300:
            robots = parse_robots(response.body)
            log.debug("robots.txt for %s: %d groups", origin, len(robots.groups))
            return RobotsEntry(origin, robots, "fetched", status, now, now + self.ttl, None, response.body,
                               validators.etag, validators.last_modified)
        if 300 <= status < 400:
            return self._unavailable(origin, status, now, "redirect limit reached")
        if status == 429 or status >= 500:
            entry = self._unreachable(origin, status, now, f"HTTP {status}")
            wait = parse_retry_after(response.headers.get("Retry-After"), now, response.headers.get("Date"))
            if wait is not None:
                # Do not ask again before the server said it is willing to answer.
                entry.expires_at = max(entry.expires_at, now + wait)
            return entry
        return self._unavailable(origin, status, now, f"HTTP {status}")

    def _unavailable(self, origin: str, status: int | None, now: float, why: str) -> RobotsEntry:
        robots = RobotsTxt.allow_all(f"robots.txt unavailable ({why}); allowing all")
        return RobotsEntry(origin, robots, "unavailable", status, now, now + self.ttl, why)

    def _unreachable(self, origin: str, status: int | None, now: float, why: str) -> RobotsEntry:
        previous = self._entries.get(origin)
        if previous is not None and previous.state in ("fetched", "stale") and now - previous.fetched_at < self.stale_ttl:
            return RobotsEntry(origin, previous.robots, "stale", status, previous.fetched_at,
                               now + self.error_ttl, why, previous.body, previous.etag, previous.last_modified)
        robots = RobotsTxt.disallow_all(f"robots.txt unreachable ({why}); disallowing all")
        return RobotsEntry(origin, robots, "unreachable", status, now, now + self.error_ttl, why)


def _redirect_counter(limit: int):  # noqa: ANN202 - returns a closure
    """Redirect filter that stops following after ``limit`` hops."""
    hops = 0

    def allow(_: str) -> bool:
        nonlocal hops
        hops += 1
        return hops <= limit

    return allow
