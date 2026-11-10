"""Lease upkeep for one worker: heartbeats, reaping dead peers and reclaiming their work.

``beat`` renews this worker's frontier leases and host slots and its row
in ``workers``. ``reap`` finds peers that died (their process is gone, or
their heartbeat went stale), frees their host slots and returns their
leased requests to the queue; requests that have now been leased
``max_lease_attempts`` times without being settled are failed with a fetch
event saying so. In a cooperative crawl :meth:`Coordination.run` does both
on a timer and notices when the run was closed by someone else.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Callable

from skein.clock import Clock
from skein.config.runtime import WorkerSettings
from skein.frontier.sqlite import Frontier
from skein.politeness.scheduler import HostScheduler
from skein.retry.classify import ErrorClass
from skein.store.database import is_busy
from skein.store.documents import FetchEvent
from skein.store.sqlite import DEFAULT_REVISIT, Store
from skein.workers.liveness import worker_alive

log = logging.getLogger(__name__)

STALE_FACTOR = 3.0


@dataclass
class ReapReport:
    dead: list[str] = field(default_factory=list)
    requeued: int = 0
    poisoned: list[str] = field(default_factory=list)


@dataclass
class CoordinationStats:
    beats: int = 0
    reaps: int = 0
    workers_reaped: int = 0
    reclaimed: int = 0
    poisoned: int = 0
    busy: int = 0


class Coordination:
    """Heartbeats and reaping for ``worker_id`` working on ``spider``."""

    def __init__(
        self,
        store: Store,
        frontier: Frontier,
        scheduler: HostScheduler,
        clock: Clock,
        *,
        spider: str,
        worker_id: str,
        settings: WorkerSettings,
    ) -> None:
        self.store = store
        self.frontier = frontier
        self.scheduler = scheduler
        self.clock = clock
        self.spider = spider
        self.worker_id = worker_id
        self.settings = settings
        self.stats = CoordinationStats()
        self._last_beat = clock.monotonic()

    @property
    def stale_after(self) -> float:
        return self.settings.lease_ttl * STALE_FACTOR

    def beat(self) -> None:
        """Renew this worker's leases, host slots and registry heartbeat."""
        self.frontier.renew()
        self.scheduler.renew()
        with self.store.transaction():
            self.store.workers.heartbeat(self.worker_id, self.clock.time())
        self.stats.beats += 1
        self._last_beat = self.clock.monotonic()

    def maybe_beat(self) -> bool:
        """Beat if ``heartbeat_interval`` has passed since the last beat (for crawls without a timer task)."""
        if self.clock.monotonic() - self._last_beat < self.settings.heartbeat_interval:
            return False
        self.beat()
        return True

    def dead_peers(self) -> list[str]:
        now = self.clock.time()
        return [record.id for record in self.store.workers.list(spider=self.spider, states=("running",))
                if record.id != self.worker_id and not worker_alive(record, now, self.stale_after)]

    def reap(self) -> ReapReport:
        """Mark dead peers, free their host slots and reclaim expired or orphaned leases."""
        report = ReapReport(dead=self.dead_peers())
        now = self.clock.time()
        for worker in report.dead:
            with self.store.transaction():
                self.store.workers.finish(worker, "dead", now, None)
            freed = self.scheduler.release_owner(worker)
            log.warning("%s: worker %s is dead; %d host slot(s) freed", self.spider, worker, freed)
        reclaimed = self.frontier.reclaim(dead_owners=report.dead)
        report.requeued = len(reclaimed.requeued)
        report.poisoned = reclaimed.poisoned
        if reclaimed.poisoned:
            self._record_poisoned(reclaimed.poisoned)
        self.stats.reaps += 1
        self.stats.workers_reaped += len(report.dead)
        self.stats.reclaimed += report.requeued
        self.stats.poisoned += len(report.poisoned)
        if report.requeued:
            log.info("%s: %d expired lease(s) reclaimed", self.spider, report.requeued)
        return report

    def _record_poisoned(self, urls: list[str]) -> None:
        now = self.clock.time()
        reason = f"abandoned: leased {self.settings.max_lease_attempts} times without being settled"
        with self.store.transaction():
            for url in urls:
                row = self.store.db.execute("SELECT kind, depth, run_id FROM frontier WHERE spider = ? AND url = ?",
                                            (self.spider, url)).fetchone()
                event = FetchEvent(self.spider, url, now, kind=row["kind"], depth=row["depth"], error=reason,
                                   error_class=ErrorClass.PERMANENT.value, worker_id=self.worker_id)
                self.store.documents.apply(event, row["run_id"], DEFAULT_REVISIT)
        log.warning("%s: %d request(s) failed after repeatedly losing their lease", self.spider, len(urls))

    async def run(self, run_id: int, on_run_closed: Callable[[], None]) -> None:
        """Beat every ``heartbeat_interval`` and reap every ``reclaim_interval`` until cancelled."""
        settings = self.settings
        next_reap = self.clock.monotonic()
        while True:
            await self.clock.sleep(min(settings.heartbeat_interval, settings.reclaim_interval))
            try:
                self.beat()
                if self.clock.monotonic() >= next_reap:
                    self.reap()
                    next_reap = self.clock.monotonic() + settings.reclaim_interval
            except sqlite3.OperationalError as exc:
                if not is_busy(exc):
                    raise
                self.stats.busy += 1
                log.info("%s: database busy; heartbeat postponed", self.spider)
                continue
            run = self.store.runs.get(run_id)
            if run is None or run["status"] != "running":
                log.info("%s: run %d is %s; stopping", self.spider, run_id, run["status"] if run else "gone")
                on_run_closed()
                return

