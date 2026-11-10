"""Per-host politeness shared by every process working on one database.

Host state lives in SQLite so that several ``skein worker`` processes
respect the same spacing, ``Crawl-delay``, back-off and concurrency limit:

* ``host_state`` -- one row per origin: the robots.txt ``Crawl-delay``,
  when the next request may start (``next_allowed``), back-off after
  host-level failures (``backoff_until``, ``failures``), request counts;
* ``host_slots`` -- one row per concurrent request slot of an origin. A
  process takes a slot for each request it sends and gives it back
  afterwards. Slots are leased like frontier rows: a slot whose owner died
  is free again once its lease expires (or when the owner is reaped).

All times are wall-clock epoch seconds from the injected clock, the only
time scale processes share. Acquiring a slot moves ``next_allowed`` to
``start + delay``; releasing it with the actual send time moves it to at
least ``sent + delay``, so the spacing holds between actual sends even if a
request waited in a queue after its slot was taken.
"""

from __future__ import annotations

import math
import random
import sqlite3
from dataclasses import dataclass
from typing import Any

from skein.clock import Clock
from skein.politeness.policy import PolitenessPolicy
from skein.retry.backoff import BackoffPolicy
from skein.store.database import Transaction


@dataclass(frozen=True)
class HostSlot:
    """A request slot held by ``owner`` for one request to ``host``; ``start`` is the scheduled send time."""

    host: str
    slot: int
    owner: str
    start: float


@dataclass
class HostState:
    """One ``host_state`` row."""

    host_key: str
    crawl_delay: float | None = None
    next_allowed: float = 0.0
    backoff_until: float = 0.0
    quarantine_until: float = 0.0
    failures: int = 0
    backoffs: int = 0
    requests: int = 0
    last_start: float | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "HostState":
        return cls(row["host_key"], row["crawl_delay"], row["next_allowed"], row["backoff_until"],
                   row["quarantine_until"], row["failures"], row["backoffs"], row["requests"], row["last_start"])


class HostScheduler:
    """Decides when each host may receive its next request, across processes.

    Host-level failures (overload statuses, connection failures) pause the
    whole host with exponential back-off and jitter; the pause is never
    shorter than a server-supplied ``Retry-After``.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        policy: PolitenessPolicy,
        clock: Clock,
        *,
        owner: str = "local",
        backoff: BackoffPolicy | None = None,
        rng: random.Random | None = None,
        slot_ttl: float = 60.0,
    ) -> None:
        self.db = db
        self.policy = policy
        self.owner = owner
        self.backoff = backoff or BackoffPolicy()
        self.rng = rng or random.Random()
        self.slot_ttl = slot_ttl
        self._clock = clock

    # -- state -----------------------------------------------------------------

    def state(self, host: str) -> HostState:
        row = self.db.execute("SELECT * FROM host_state WHERE host_key = ?", (host,)).fetchone()
        return HostState.from_row(row) if row is not None else HostState(host)

    def _update(self, host: str, **values: Any) -> None:
        values["updated_at"] = self._clock.time()
        names = list(values)
        self.db.execute(
            f"""INSERT INTO host_state (host_key, {', '.join(names)}) VALUES (?, {', '.join('?' * len(names))})
                ON CONFLICT (host_key) DO UPDATE SET {', '.join(f'{n} = excluded.{n}' for n in names)}""",
            [host, *values.values()],
        )

    def delay_for(self, host: str) -> float:
        """Effective start-to-start spacing for ``host``."""
        return self.policy.delay(self.state(host).crawl_delay)

    def set_crawl_delay(self, host: str, delay: float | None) -> None:
        """Record the robots.txt ``Crawl-delay`` for ``host`` (``None`` clears it).

        The spacing after the most recent request is re-evaluated, so the
        request that fetched robots.txt is already followed by the new delay.
        """
        with Transaction(self.db):
            state = self.state(host)
            next_allowed = state.next_allowed
            if state.last_start is not None:
                next_allowed = max(next_allowed, state.last_start + self.policy.delay(delay))
            self._update(host, crawl_delay=delay, next_allowed=next_allowed)

    # -- slots -----------------------------------------------------------------

    def busy_slots(self, host: str) -> int:
        """Slots of ``host`` held under an unexpired lease, by any process."""
        row = self.db.execute(
            "SELECT COUNT(*) FROM host_slots WHERE host_key = ? AND owner IS NOT NULL AND lease_expires > ?",
            (host, self._clock.time()),
        ).fetchone()
        return int(row[0])

    def ready_at(self, host: str) -> float:
        """Wall time at which ``host`` may be contacted; ``inf`` while all its slots are taken."""
        if self.busy_slots(host) >= self.policy.per_host_concurrency:
            return math.inf
        state = self.state(host)
        return max(state.next_allowed, state.backoff_until)

    def readiness(self, hosts: list[str]) -> dict[str, float]:
        """:meth:`ready_at` of many hosts with two queries instead of two per host."""
        now = self._clock.time()
        ready = {host: 0.0 for host in hosts}
        for start in range(0, len(hosts), 500):
            chunk = hosts[start : start + 500]
            marks = ", ".join("?" * len(chunk))
            for host, next_allowed, backoff_until in self.db.execute(
                    f"SELECT host_key, next_allowed, backoff_until FROM host_state WHERE host_key IN ({marks})", chunk):
                ready[host] = max(next_allowed, backoff_until)
            for host, busy in self.db.execute(
                    f"""SELECT host_key, COUNT(*) FROM host_slots WHERE host_key IN ({marks})
                        AND owner IS NOT NULL AND lease_expires > ? GROUP BY host_key""", [*chunk, now]):
                if busy >= self.policy.per_host_concurrency:
                    ready[host] = math.inf
        return ready

    def is_ready(self, host: str) -> bool:
        return self.ready_at(host) <= self._clock.time()

    def acquire(self, host: str) -> HostSlot | None:
        """Take a free slot of ``host`` if the host is ready now; ``None`` otherwise."""
        now = self._clock.time()
        with Transaction(self.db):
            state = self.state(host)
            if max(state.next_allowed, state.backoff_until) > now:
                return None
            taken = {row[0]: row for row in self.db.execute(
                "SELECT slot, owner, lease_expires FROM host_slots WHERE host_key = ?", (host,))}
            free = None
            for index in range(self.policy.per_host_concurrency):
                row = taken.get(index)
                if row is None or row[1] is None or row[2] <= now:
                    free = index
                    break
            if free is None:
                return None
            start = max(now, state.next_allowed)
            self.db.execute(
                """INSERT INTO host_slots (host_key, slot, owner, lease_expires, acquired_at) VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT (host_key, slot) DO UPDATE SET owner = excluded.owner,
                       lease_expires = excluded.lease_expires, acquired_at = excluded.acquired_at""",
                (host, free, self.owner, now + self.slot_ttl, now),
            )
            self._update(host, requests=state.requests + 1, last_start=start,
                         next_allowed=start + self.policy.delay(state.crawl_delay))
        return HostSlot(host, free, self.owner, start)

    def release(self, slot: HostSlot, sent_at: float | None = None) -> bool:
        """Give ``slot`` back; ``sent_at`` (the actual send time) re-anchors the spacing.

        Returns ``False`` if the slot had already been taken over (its lease
        expired and another process reclaimed it).
        """
        with Transaction(self.db):
            cursor = self.db.execute(
                """UPDATE host_slots SET owner = NULL, lease_expires = NULL
                   WHERE host_key = ? AND slot = ? AND owner = ?""",
                (slot.host, slot.slot, slot.owner),
            )
            if sent_at is not None:
                state = self.state(slot.host)
                spaced = sent_at + self.policy.delay(state.crawl_delay)
                if spaced > state.next_allowed:
                    self._update(slot.host, next_allowed=spaced, last_start=max(sent_at, state.last_start or 0.0))
        return cursor.rowcount == 1

    def renew(self) -> int:
        """Extend the leases of every slot this owner holds; returns how many."""
        with Transaction(self.db):
            cursor = self.db.execute(
                "UPDATE host_slots SET lease_expires = ? WHERE owner = ?",
                (self._clock.time() + self.slot_ttl, self.owner),
            )
        return cursor.rowcount

    def release_owner(self, owner: str | None = None) -> int:
        """Free every slot held by ``owner`` (default: this scheduler's owner), e.g. after it died."""
        with Transaction(self.db):
            cursor = self.db.execute("UPDATE host_slots SET owner = NULL, lease_expires = NULL WHERE owner = ?",
                                     (owner or self.owner,))
        return cursor.rowcount

    # -- failures ----------------------------------------------------------------

    def back_off(self, host: str, seconds: float) -> None:
        """Keep ``host`` idle for at least ``seconds`` from now."""
        with Transaction(self.db):
            state = self.state(host)
            self._update(host, backoff_until=max(state.backoff_until, self._clock.time() + seconds))

    def host_failure(self, host: str, retry_after: float | None = None) -> float:
        """Register a host-level failure; returns the pause imposed on the host.

        The ``n``-th consecutive failure pauses the host for
        ``backoff.delay(n - 1)`` seconds (exponential with jitter from the
        scheduler's RNG), or for ``retry_after`` if the server asked for
        longer.
        """
        with Transaction(self.db):
            state = self.state(host)
            delay = self.backoff.delay(state.failures, self.rng)
            if retry_after is not None:
                delay = max(delay, retry_after)
            self._update(host, failures=state.failures + 1, backoffs=state.backoffs + 1,
                         backoff_until=max(state.backoff_until, self._clock.time() + delay))
        return delay

    def host_success(self, host: str) -> bool:
        """A request to ``host`` succeeded: reset its failure streak. Returns whether there was one."""
        state = self.state(host)
        if state.failures == 0:
            return False
        with Transaction(self.db):
            self._update(host, failures=0)
        return True

    def snapshot(self, hosts: list[str] | None = None) -> dict[str, dict[str, float | int | None]]:
        """Per-host counters for run summaries (all known hosts, or ``hosts``)."""
        rows = self.db.execute("SELECT * FROM host_state ORDER BY host_key").fetchall()
        wanted = set(hosts) if hosts is not None else None
        out: dict[str, dict[str, float | int | None]] = {}
        for row in rows:
            state = HostState.from_row(row)
            if wanted is not None and state.host_key not in wanted:
                continue
            out[state.host_key] = {
                "requests": state.requests,
                "active": self.busy_slots(state.host_key),
                "crawl_delay": state.crawl_delay,
                "delay": self.policy.delay(state.crawl_delay),
                "failures": state.failures,
                "backoffs": state.backoffs,
            }
        return out
