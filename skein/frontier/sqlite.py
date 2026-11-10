"""The persistent crawl frontier: pending requests, priorities, leases and the seen set in SQLite.

Every URL a spider ever discovered has one row in ``frontier``. Its
``state`` moves ``pending -> active -> done | failed``; transient failures
go back to ``pending`` with a ``not_before`` time, requests a server asked
us to leave alone for long become ``deferred``. Rows outlive runs: the
next run's planner turns handled rows back into pending ones when they are
due.

An ``active`` row is *leased* by one worker: ``lease_owner`` names it,
``lease_expires`` says until when the lease holds without renewal and
``lease_attempts`` counts how often the row was claimed since it was last
settled. Settling a row (done, failed, retry, defer, requeue) is a
compare-and-set on the owner, so a worker whose lease expired and was
reclaimed by another cannot overwrite the new owner's state; its results
are still recorded (delivery is at least once, writes are idempotent).
Rows whose lease expired are returned to the queue by :meth:`reclaim`;
rows that keep losing their lease (a page that crashes every worker) are
failed after ``max_lease_attempts`` claims.

Ordering within a host is priority (high first), then urgency (re-queued
documents that change often or are long overdue first), depth (shallow
first) and discovery order. Politeness is enforced at claim time through
the cross-process :class:`~skein.politeness.scheduler.HostScheduler`.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Iterable

from skein.clock import Clock
from skein.frontier.request import CrawlRequest, RequestKind
from skein.politeness.scheduler import HostScheduler, HostSlot
from skein.store.database import Transaction
from skein.store.links import orphan_condition
from skein.store.schema_workers import LOCAL_WORKER

_HANDLED = "('done', 'failed', 'deferred')"
NEW_URL_URGENCY = 1.5
# Laplace-smoothed share of revisits that found a change, plus how overdue the
# document is relative to its interval (capped at 1); never-fetched URLs rank
# above every revisit.
_URGENCY = f"""COALESCE((SELECT (d.change_count + 1.0) / (d.check_count + 2.0)
                                + MIN(1.0, MAX(0.0, :now - COALESCE(d.next_visit, :now))
                                           / MAX(COALESCE(d.revisit_interval, 1.0), 1.0))
                         FROM documents d WHERE d.spider = frontier.spider AND d.url = frontier.url
                           AND d.last_success IS NOT NULL), {NEW_URL_URGENCY})"""
_CLEAR_LEASE = "lease_owner = NULL, lease_expires = NULL"


class AddOutcome(str, Enum):
    """Why :meth:`Frontier.add` did or did not accept a request."""

    ADDED = "added"
    DUPLICATE = "duplicate"
    TOO_DEEP = "too_deep"


@dataclass
class FrontierStats:
    """Counters exposed in crawl summaries (for the current process)."""

    added: int = 0
    duplicates: int = 0
    too_deep: int = 0
    requeued: int = 0
    rescheduled: int = 0
    recovered: int = 0
    claimed: int = 0
    reclaimed: int = 0
    poisoned: int = 0
    renewed: int = 0
    leases_lost: int = 0
    by_depth: dict[int, int] = field(default_factory=dict)


@dataclass
class Claim:
    """A leased request; ``slot`` is the host slot taken for it (``None`` if it will not be fetched)."""

    request: CrawlRequest
    slot: HostSlot | None
    lease_attempts: int


@dataclass
class ClaimBatch:
    """What one :meth:`Frontier.claim` call produced.

    ``claims`` are leased requests holding a host slot; ``screened`` were
    leased without a slot because ``screen`` rejected them (they are
    settled without fetching). ``next_ready`` is the earliest wall time at
    which more work could become claimable (``inf``: wait for a change).
    """

    claims: list[Claim] = field(default_factory=list)
    screened: list[Claim] = field(default_factory=list)
    next_ready: float = math.inf
    budget_exhausted: bool = False


@dataclass(frozen=True)
class ReclaimReport:
    requeued: list[str]
    poisoned: list[str]


class Frontier:
    """Queue of one spider's requests stored in the ``frontier`` table, worked on by ``owner``.

    The frontier shares the store's connection: its writes join an open
    transaction, which lets the writer commit a page's results and the
    links found on it atomically.
    """

    def __init__(
        self,
        db: sqlite3.Connection,
        spider: str,
        clock: Clock,
        *,
        max_depth: int | None = None,
        owner: str = LOCAL_WORKER,
        lease_ttl: float = 60.0,
        max_lease_attempts: int = 5,
    ) -> None:
        if lease_ttl <= 0:
            raise ValueError("lease_ttl must be positive")
        if max_lease_attempts < 1:
            raise ValueError("max_lease_attempts must be >= 1")
        self.db = db
        self.spider = spider
        self.clock = clock
        self.max_depth = max_depth
        self.owner = owner
        self.lease_ttl = lease_ttl
        self.max_lease_attempts = max_lease_attempts
        self.run_id: int | None = None
        self.stats = FrontierStats()

    # -- inspection ----------------------------------------------------------

    def __len__(self) -> int:
        """Pending requests (all processes)."""
        row = self.db.execute("SELECT COUNT(*) FROM frontier WHERE spider = ? AND state = 'pending'", (self.spider,))
        return int(row.fetchone()[0])

    def is_empty(self) -> bool:
        return self.db.execute(
            "SELECT 1 FROM frontier WHERE spider = ? AND state = 'pending' LIMIT 1", (self.spider,)).fetchone() is None

    def is_drained(self) -> bool:
        """No pending request and none in flight in any process: the run has nothing left to do."""
        return self.db.execute(
            "SELECT 1 FROM frontier WHERE spider = ? AND state IN ('pending', 'active') LIMIT 1",
            (self.spider,)).fetchone() is None

    def pending_hosts(self) -> dict[str, float]:
        """Hosts with pending requests and the earliest ``not_before`` among them.

        A recursive skip-scan over the ``(spider, state, host_key, not_before)``
        index: one index seek per host instead of reading every pending row,
        so claiming stays cheap with hundreds of thousands of queued URLs.
        """
        rows = self.db.execute(
            """WITH RECURSIVE hosts(host) AS (
                   SELECT MIN(host_key) FROM frontier WHERE spider = :spider AND state = 'pending'
                   UNION ALL
                   SELECT (SELECT MIN(host_key) FROM frontier
                           WHERE spider = :spider AND state = 'pending' AND host_key > hosts.host)
                   FROM hosts WHERE hosts.host IS NOT NULL)
               SELECT host, (SELECT MIN(not_before) FROM frontier
                             WHERE spider = :spider AND state = 'pending' AND host_key = hosts.host)
               FROM hosts WHERE host IS NOT NULL""",
            {"spider": self.spider},
        )
        return {row[0]: float(row[1]) for row in rows}

    def hosts(self) -> list[str]:
        return sorted(self.pending_hosts())

    def hosts_seen(self) -> list[str]:
        """Every origin this spider's frontier has a URL of."""
        rows = self.db.execute("SELECT DISTINCT host_key FROM frontier WHERE spider = ? ORDER BY 1", (self.spider,))
        return [row[0] for row in rows]

    def held(self) -> int:
        """Rows this owner currently leases."""
        row = self.db.execute(
            "SELECT COUNT(*) FROM frontier WHERE spider = ? AND state = 'active' AND lease_owner = ?",
            (self.spider, self.owner))
        return int(row.fetchone()[0])

    def state_of(self, url: str) -> str | None:
        row = self.db.execute("SELECT state FROM frontier WHERE spider = ? AND url = ?", (self.spider, url)).fetchone()
        return row[0] if row is not None else None

    def run_id_of(self, url: str) -> int | None:
        """The run that last queued or settled ``url``."""
        row = self.db.execute("SELECT run_id FROM frontier WHERE spider = ? AND url = ?", (self.spider, url)).fetchone()
        return row[0] if row is not None else None

    def seen(self, url: str) -> bool:
        return self.state_of(url) is not None

    def counts(self) -> dict[str, int]:
        rows = self.db.execute("SELECT state, COUNT(*) FROM frontier WHERE spider = ? GROUP BY state", (self.spider,))
        return {row[0]: row[1] for row in rows}

    def unfinished(self, seed: str | None = None) -> int:
        """Pending, in-flight and deferred requests, optionally only those under one seed."""
        query = "SELECT COUNT(*) FROM frontier WHERE spider = ? AND state IN ('pending', 'active', 'deferred')"
        params: list[object] = [self.spider]
        if seed is not None:
            query += " AND seed = ?"
            params.append(seed)
        return int(self.db.execute(query, params).fetchone()[0])

    def listing(
        self, *, states: tuple[str, ...] = (), host: str | None = None, limit: int = 100
    ) -> list[dict[str, object]]:
        """Frontier rows for inspection, in claim order within each state."""
        clauses = ["spider = ?"]
        params: list[object] = [self.spider]
        if states:
            clauses.append(f"state IN ({', '.join('?' * len(states))})")
            params.extend(states)
        if host is not None:
            clauses.append("host_key = ?")
            params.append(host)
        rows = self.db.execute(
            f"""SELECT url, state, host_key, depth, priority, urgency, kind, attempts, not_before, seed, updated_at,
                       lease_owner, lease_expires, lease_attempts
                FROM frontier WHERE {' AND '.join(clauses)}
                ORDER BY state, host_key, priority DESC, urgency DESC, depth, seq LIMIT ?""",
            [*params, limit],
        )
        return [dict(row) for row in rows]

    def release(self, states: tuple[str, ...], *, host: str | None = None) -> int:
        """Make failed or deferred rows pending again, so the next run retries them right away."""
        allowed = {"failed", "deferred", "done"}
        if not states or not set(states) <= allowed:
            raise ValueError(f"can only release rows in states {sorted(allowed)}")
        clauses = ["spider = ?", f"state IN ({', '.join('?' * len(states))})"]
        params: list[object] = [self.spider, *states]
        if host is not None:
            clauses.append("host_key = ?")
            params.append(host)
        with Transaction(self.db):
            cursor = self.db.execute(
                f"""UPDATE frontier SET state = 'pending', attempts = 0, not_before = 0, updated_at = ?,
                        lease_attempts = 0
                    WHERE {' AND '.join(clauses)}""",
                [self.clock.time(), *params],
            )
        return cursor.rowcount

    # -- leases ----------------------------------------------------------------

    def recover(self) -> int:
        """Return requests this owner left ``active`` (an earlier incarnation of it crashed) to the queue."""
        with Transaction(self.db):
            cursor = self.db.execute(
                f"""UPDATE frontier SET state = 'pending', {_CLEAR_LEASE}, updated_at = ?
                    WHERE spider = ? AND state = 'active' AND lease_owner = ?""",
                (self.clock.time(), self.spider, self.owner),
            )
        self.stats.recovered += cursor.rowcount
        return cursor.rowcount

    def renew(self) -> int:
        """Extend every lease this owner holds by ``lease_ttl`` from now (the heartbeat)."""
        with Transaction(self.db):
            cursor = self.db.execute(
                "UPDATE frontier SET lease_expires = ? WHERE spider = ? AND state = 'active' AND lease_owner = ?",
                (self.clock.time() + self.lease_ttl, self.spider, self.owner),
            )
        self.stats.renewed += cursor.rowcount
        return cursor.rowcount

    def next_foreign_expiry(self) -> float | None:
        """When the first lease held by another owner expires (``None`` if there is none)."""
        row = self.db.execute(
            """SELECT MIN(lease_expires) FROM frontier
               WHERE spider = ? AND state = 'active' AND lease_owner IS NOT NULL AND lease_owner <> ?""",
            (self.spider, self.owner),
        ).fetchone()
        return float(row[0]) if row[0] is not None else None

    def reclaim(self, dead_owners: Iterable[str] = ()) -> ReclaimReport:
        """Put back rows whose lease expired, whose owner is known dead, or that have no owner.

        Rows claimed ``max_lease_attempts`` times without being settled are
        failed instead: whatever they do keeps killing workers.
        """
        now = self.clock.time()
        dead = sorted(set(dead_owners) - {self.owner})
        marks = ", ".join("?" * len(dead))
        condition = (f"spider = ? AND state = 'active' AND (lease_owner IS NULL OR "
                     f"(lease_owner <> ? AND lease_expires <= ?)"
                     + (f" OR lease_owner IN ({marks})" if dead else "") + ")")
        params = [self.spider, self.owner, now, *dead]
        with Transaction(self.db):
            rows = self.db.execute(f"SELECT url, lease_attempts FROM frontier WHERE {condition}", params).fetchall()
            poisoned = [row[0] for row in rows if row[1] >= self.max_lease_attempts]
            requeued = [row[0] for row in rows if row[1] < self.max_lease_attempts]
            self.db.executemany(
                f"UPDATE frontier SET state = 'pending', {_CLEAR_LEASE}, updated_at = ? WHERE spider = ? AND url = ?",
                [(now, self.spider, url) for url in requeued])
            self.db.executemany(
                f"""UPDATE frontier SET state = 'failed', {_CLEAR_LEASE}, lease_attempts = 0, updated_at = ?
                    WHERE spider = ? AND url = ?""",
                [(now, self.spider, url) for url in poisoned])
        self.stats.reclaimed += len(requeued)
        self.stats.poisoned += len(poisoned)
        return ReclaimReport(requeued, poisoned)

    # -- run lifecycle -----------------------------------------------------

    def reschedule(
        self,
        *,
        due_before: float | None = None,
        seed: str | None = None,
        kind: RequestKind | None = None,
        skip_orphans: bool = False,
        count_only: bool = False,
    ) -> int:
        """Turn handled rows back into pending ones for a new run.

        Without ``due_before`` every handled row qualifies (a full run);
        otherwise only rows whose document is due by its revisit schedule
        and rows never fetched. Deferred rows only qualify once the wait the
        server asked for is over, whatever the mode. ``seed``
        restricts the change to one crawl root, ``kind`` to pages or sitemaps;
        ``skip_orphans`` leaves URLs alone that nothing links to any more.
        Re-queued rows get an urgency so that the documents most likely to
        have changed are fetched first. With ``count_only`` nothing changes;
        the number of rows that would be re-queued is returned.
        """
        now = self.clock.time()
        clauses = ["f.spider = :spider", f"f.state IN {_HANDLED}", "(f.state <> 'deferred' OR f.not_before <= :now)"]
        params: dict[str, object] = {"spider": self.spider, "now": now, "seed": seed,
                                     "kind": kind.value if kind else None, "due": due_before}
        if seed is not None:
            clauses.append("f.seed = :seed")
        if kind is not None:
            clauses.append("f.kind = :kind")
        if skip_orphans:
            clauses.append(f"NOT ({orphan_condition('f')})")
        if due_before is not None:
            clauses.append(
                """(f.state = 'deferred'
                    OR COALESCE((SELECT d.next_visit FROM documents d
                                 WHERE d.spider = f.spider AND d.url = f.url), 0) <= :due)"""
            )
        where = " AND ".join(clauses)
        if count_only:
            return int(self.db.execute(f"SELECT COUNT(*) FROM frontier f WHERE {where}", params).fetchone()[0])
        with Transaction(self.db):
            cursor = self.db.execute(
                f"""UPDATE frontier SET state = 'pending', attempts = 0, not_before = 0, updated_at = :now,
                        lease_attempts = 0, urgency = {_URGENCY}
                    WHERE rowid IN (SELECT f.rowid FROM frontier f WHERE {where})""",
                params,
            )
        self.stats.rescheduled += cursor.rowcount
        return cursor.rowcount

    # -- quarantine --------------------------------------------------------

    def block_host(self, host: str, until: float) -> int:
        """Defer every pending request of ``host`` until wall time ``until``, including ones added later.

        The quarantine is recorded in ``host_state`` and so applies to every
        process. Returns the number of requests deferred now.
        """
        with Transaction(self.db):
            self.db.execute(
                """INSERT INTO host_state (host_key, quarantine_until, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT (host_key) DO UPDATE SET
                       quarantine_until = MAX(host_state.quarantine_until, excluded.quarantine_until),
                       updated_at = excluded.updated_at""",
                (host, until, self.clock.time()),
            )
            until = self.blocked_until(host) or until
            cursor = self.db.execute(
                """UPDATE frontier SET state = 'deferred', not_before = ?, updated_at = ?
                   WHERE spider = ? AND host_key = ? AND state = 'pending'""",
                (until, self.clock.time(), self.spider, host),
            )
        return cursor.rowcount

    def blocked_until(self, host: str) -> float | None:
        row = self.db.execute("SELECT quarantine_until FROM host_state WHERE host_key = ?", (host,)).fetchone()
        if row is None or row[0] <= self.clock.time():
            return None
        return float(row[0])

    # -- adding --------------------------------------------------------------

    def add(self, request: CrawlRequest) -> AddOutcome:
        """Enqueue ``request`` unless its URL is already known or too deep."""
        if self.max_depth is not None and request.depth > self.max_depth:
            self.stats.too_deep += 1
            return AddOutcome.TOO_DEEP
        now = self.clock.time()
        blocked = self.blocked_until(request.host_key)
        with Transaction(self.db):
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO frontier (spider, url, host_key, depth, priority, kind, referrer, seed,
                       state, attempts, not_before, urgency, sitemap_lastmod, changefreq, discovered_at, updated_at,
                       run_id, seq)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           (SELECT COALESCE(MAX(seq), 0) + 1 FROM frontier WHERE spider = ?))""",
                (self.spider, request.url, request.host_key, request.depth, request.priority, request.kind.value,
                 request.referrer, request.seed, "pending" if blocked is None else "deferred", request.attempts,
                 blocked or 0, NEW_URL_URGENCY, request.sitemap_lastmod, request.changefreq, now, now, self.run_id,
                 self.spider),
            )
        if cursor.rowcount == 0:
            self.stats.duplicates += 1
            return AddOutcome.DUPLICATE
        self.stats.added += 1
        self.stats.by_depth[request.depth] = self.stats.by_depth.get(request.depth, 0) + 1
        return AddOutcome.ADDED

    def add_many(self, requests: Iterable[CrawlRequest]) -> int:
        """Add several requests in one transaction; returns how many were new."""
        added = 0
        with Transaction(self.db):
            for request in requests:
                if self.add(request) is AddOutcome.ADDED:
                    added += 1
        return added

    def promote(self, request: CrawlRequest) -> bool:
        """Make a known URL pending again now (a sitemap says it changed), or add it.

        Rows already handled in the current run, pending or in flight are
        left alone, so a URL is fetched at most once per run this way; so
        are deferred rows whose server-requested wait is not over.
        """
        if self.add(request) is AddOutcome.ADDED:
            return True
        if self.blocked_until(request.host_key) is not None:
            return False
        now = self.clock.time()
        with Transaction(self.db):
            cursor = self.db.execute(
                f"""UPDATE frontier SET state = 'pending', attempts = 0, not_before = 0, updated_at = ?,
                        sitemap_lastmod = COALESCE(?, sitemap_lastmod), changefreq = COALESCE(?, changefreq),
                        priority = MAX(priority, ?), urgency = {NEW_URL_URGENCY}, lease_attempts = 0
                    WHERE spider = ? AND url = ? AND state IN {_HANDLED}
                      AND (run_id IS NULL OR run_id <> ?)
                      AND NOT (state = 'deferred' AND not_before > ?)""",
                (now, request.sitemap_lastmod, request.changefreq, request.priority, self.spider, request.url,
                 self.run_id if self.run_id is not None else -1, now),
            )
        if cursor.rowcount:
            self.stats.rescheduled += 1
            return True
        return False

    def mark_seen(self, url: str, host_key: str, *, depth: int = 0) -> bool:
        """Remember ``url`` as handled without queueing it (redirect targets fetched inline)."""
        now = self.clock.time()
        with Transaction(self.db):
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO frontier (spider, url, host_key, depth, kind, state, discovered_at,
                       updated_at, run_id, seq)
                   VALUES (?, ?, ?, ?, 'page', 'done', ?, ?, ?,
                           (SELECT COALESCE(MAX(seq), 0) + 1 FROM frontier WHERE spider = ?))""",
                (self.spider, url, host_key, depth, now, now, self.run_id, self.spider),
            )
        return cursor.rowcount == 1

    # -- claiming --------------------------------------------------------------

    def charged(self) -> int:
        """Pages charged against the current run's page budget (by every process).

        The count is kept on the run row and bumped in the claiming
        transaction, so checking the budget costs one lookup however many
        pages were charged.
        """
        row = self.db.execute("SELECT pages_charged FROM runs WHERE id = ?", (self.run_id,)).fetchone()
        return int(row[0]) if row is not None else 0

    def in_flight(self) -> int:
        """Requests leased under a live lease, by every process."""
        row = self.db.execute(
            "SELECT COUNT(*) FROM frontier WHERE spider = ? AND state = 'active' AND lease_expires > ?",
            (self.spider, self.clock.time()))
        return int(row.fetchone()[0])

    def claim(
        self,
        scheduler: HostScheduler,
        *,
        limit: int = 1,
        max_pages: int | None = None,
        max_in_flight: int | None = None,
        screen: Callable[[CrawlRequest], bool] | None = None,
    ) -> ClaimBatch:
        """Lease up to ``limit`` of the best pending requests among hosts that are ready now.

        Each claimed request holds a host slot from ``scheduler``. Pages
        are charged against ``max_pages`` once per run, when first claimed;
        when the budget is used up nothing more is claimed and
        ``budget_exhausted`` is set. ``max_in_flight`` caps the requests
        leased by all processes together (the spider's global concurrency).
        Requests for which ``screen`` returns ``True`` (e.g. denied by
        cached robots rules) are leased without a slot and returned in
        ``screened``.
        """
        batch = ClaimBatch()
        now = self.clock.time()
        with Transaction(self.db):
            if max_in_flight is not None:
                limit = min(limit, max_in_flight - self.in_flight())
                if limit <= 0:
                    return batch
            due = self.pending_hosts()
            charged = self.charged() if max_pages is not None else 0
            while len(batch.claims) < limit and due:
                readiness = scheduler.readiness(list(due))
                best: sqlite3.Row | None = None
                for host, not_before in list(due.items()):
                    ready = max(readiness[host], not_before)
                    if ready > now:
                        batch.next_ready = min(batch.next_ready, ready)
                        continue
                    row = self.db.execute(
                        """SELECT * FROM frontier WHERE spider = ? AND state = 'pending' AND host_key = ?
                           AND not_before <= ? ORDER BY priority DESC, urgency DESC, depth, seq LIMIT 1""",
                        (self.spider, host, now),
                    ).fetchone()
                    if row is None:
                        del due[host]
                        continue
                    if best is None or _order(row) < _order(best):
                        best = row
                if best is None:
                    break
                request = _request_from_row(best)
                if screen is not None and screen(request):
                    batch.screened.append(self._lease(best, request, None, now))
                    continue
                charge = request.kind is RequestKind.PAGE and best["charged_run"] != self.run_id
                if charge and max_pages is not None and charged >= max_pages:
                    batch.budget_exhausted = True
                    break
                slot = scheduler.acquire(request.host_key)
                if slot is None:
                    due.pop(request.host_key, None)
                    continue
                batch.claims.append(self._lease(best, request, slot, now, charge=charge))
                charged += int(charge)
        return batch

    def _lease(self, row: sqlite3.Row, request: CrawlRequest, slot: HostSlot | None, now: float, *,
               charge: bool = False) -> Claim:
        self.db.execute(
            """UPDATE frontier SET state = 'active', lease_owner = ?, lease_expires = ?,
                   lease_attempts = lease_attempts + 1, run_id = ?, updated_at = ?,
                   charged_run = CASE WHEN ? THEN ? ELSE charged_run END
               WHERE spider = ? AND url = ? AND state = 'pending'""",
            (self.owner, now + self.lease_ttl, self.run_id, now, int(charge), self.run_id, self.spider, row["url"]),
        )
        if charge:
            self.db.execute("UPDATE runs SET pages_charged = pages_charged + 1 WHERE id = ?", (self.run_id,))
        self.stats.claimed += 1
        return Claim(request, slot, int(row["lease_attempts"]) + 1)

    # -- settling ----------------------------------------------------------------

    def _settle(self, request: CrawlRequest, state: str, not_before: float = 0.0) -> bool:
        """Compare-and-set the row of a request this owner leased; ``False`` if the lease was lost."""
        blocked = self.blocked_until(request.host_key)
        if state == "pending" and blocked is not None:
            state, not_before = "deferred", max(not_before, blocked)
        with Transaction(self.db):
            cursor = self.db.execute(
                f"""UPDATE frontier SET state = ?, attempts = ?, not_before = ?, run_id = ?, updated_at = ?,
                        {_CLEAR_LEASE}, lease_attempts = 0
                    WHERE spider = ? AND url = ? AND state = 'active' AND lease_owner = ?""",
                (state, request.attempts, not_before, self.run_id, self.clock.time(), self.spider, request.url,
                 self.owner),
            )
        if cursor.rowcount == 0:
            self.stats.leases_lost += 1
            return False
        return True

    def requeue(self, request: CrawlRequest) -> bool:
        """Put a claimed request straight back (robots.txt had to be fetched first)."""
        self.stats.requeued += 1
        return self._settle(request, "pending")

    def retry_later(self, request: CrawlRequest, delay: float) -> bool:
        """Queue a claimed request again, not before ``delay`` seconds from now."""
        self.stats.requeued += 1
        return self._settle(request, "pending", self.clock.time() + delay)

    def complete(self, request: CrawlRequest, *, failed: bool = False) -> bool:
        return self._settle(request, "failed" if failed else "done")

    def defer(self, request: CrawlRequest, delay: float) -> bool:
        """Leave a request for a later run that starts at least ``delay`` seconds from now."""
        return self._settle(request, "deferred", self.clock.time() + delay)


def _order(row: sqlite3.Row) -> tuple[float, float, int, int]:
    return (-row["priority"], -row["urgency"], row["depth"], row["seq"])


def _request_from_row(row: sqlite3.Row) -> CrawlRequest:
    return CrawlRequest(
        row["url"],
        depth=row["depth"],
        priority=row["priority"],
        kind=RequestKind(row["kind"]),
        referrer=row["referrer"],
        attempts=row["attempts"],
        sitemap_lastmod=row["sitemap_lastmod"],
        seed=row["seed"],
        changefreq=row["changefreq"],
    )
