"""The dispatcher: claims frontier work for the fetch stage and decides when the run is over.

It leases requests (with host slots) as long as the fetch queue has room,
so backpressure from later stages stops it from claiming more than the
pipeline can hold. Requests that cached robots rules forbid are leased
without a slot and handed straight to the writer. When nothing is
claimable it sleeps until the earliest moment something could be -- a
host's politeness delay ending, a retry becoming due -- or until the
writer commits (new links, settled requests) or a fetcher frees a host
slot. With other processes on the same run (``cooperative``) it also
wakes every ``poll_interval`` to see their changes.

The run is finished for this worker when the frontier holds no pending
and no leased request (in any process) and nothing is in flight here.
"""

from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
from typing import Any

from skein.frontier.request import CrawlRequest
from skein.frontier.sqlite import Claim
from skein.pipeline.context import CrawlContext
from skein.pipeline.queues import StageQueue
from skein.pipeline.work import Job, Outcome, Settle
from skein.retry.classify import ErrorClass
from skein.store.database import is_busy

log = logging.getLogger(__name__)


class Dispatcher:
    """Feeds ``jobs`` from the frontier; ``writes`` receives requests settled without fetching."""

    def __init__(
        self,
        ctx: CrawlContext,
        jobs: StageQueue[Job],
        writes: StageQueue[Outcome],
        *,
        max_runtime: float | None = None,
        cooperative: bool = False,
        poll_interval: float = 0.5,
    ) -> None:
        self.ctx = ctx
        self.jobs = jobs
        self.writes = writes
        self.max_runtime = max_runtime
        self.cooperative = cooperative
        self.poll_interval = poll_interval
        self.in_flight = 0
        self.stopping = False
        self.finished = False
        self.budget_exhausted = False
        self.claims = 0
        self.idle_waits = 0
        self.busy_waits = 0
        self._wake = asyncio.Event()
        self._started = ctx.clock.monotonic()

    def notify(self) -> None:
        """Something changed (a commit, a freed host slot): look for claimable work again."""
        self._wake.set()

    def settled(self, count: int) -> None:
        self.in_flight -= count
        self._wake.set()

    def stop(self) -> None:
        """Claim nothing more; requests in flight still finish."""
        self.stopping = True
        self._wake.set()

    def _deadline(self) -> float | None:
        return None if self.max_runtime is None else self._started + self.max_runtime

    def _denied(self, request: CrawlRequest) -> bool:
        if not self.ctx.spider.obey_robots:
            return False
        decision = self.ctx.robots.decide_cached(request.url)
        return decision is not None and not decision.allowed

    def _denied_outcome(self, claim: Claim) -> Outcome:
        request = claim.request
        decision = self.ctx.robots.decide_cached(request.url)
        reason = decision.reason if decision is not None else "disallowed"
        event = self.ctx.new_event(request, error=f"robots.txt: {reason}", error_class=ErrorClass.ROBOTS.value)
        return Outcome(request, {"robots_denied": 1}, Settle.FAILED, event=event)

    async def run(self) -> None:
        ctx = self.ctx
        while True:
            deadline = self._deadline()
            if deadline is not None and ctx.clock.monotonic() >= deadline and not self.stopping:
                log.info("%s: runtime budget of %.0fs used up; stopping", ctx.spider.name, self.max_runtime)
                self.stop()
            if self.stopping or self.finished:
                return
            try:
                batch = ctx.frontier.claim(ctx.scheduler, limit=max(1, self.jobs.free),
                                           max_pages=ctx.spider.max_pages,
                                           max_in_flight=ctx.spider.global_concurrency, screen=self._denied)
            except sqlite3.OperationalError as exc:
                if not is_busy(exc):
                    raise
                # Another process held the database for longer than the busy timeout.
                self.busy_waits += 1
                await asyncio.sleep(self.poll_interval)
                continue
            for claim in batch.screened:
                self.in_flight += 1
                await self.writes.put(self._denied_outcome(claim))
            for claim in batch.claims:
                self.in_flight += 1
                self.claims += 1
                await self.jobs.put(Job(claim))
            if batch.budget_exhausted:
                ctx.checkpoint.record("stats", delta={"budget_exhausted": True})
                self.budget_exhausted = True
                self.finished = True
                return
            if batch.claims or batch.screened:
                continue
            next_ready = batch.next_ready
            if self.in_flight == 0:
                if ctx.frontier.reclaim().requeued:
                    continue
                if ctx.frontier.is_drained():
                    self.finished = True
                    return
                # Only other workers' leases remain: wake when the first of them would expire.
                expiry = ctx.frontier.next_foreign_expiry()
                if expiry is not None:
                    next_ready = min(next_ready, expiry)
            await self._idle(next_ready)

    async def _idle(self, next_ready_wall: float) -> None:
        """Wait for a notification or until ``next_ready_wall``, the runtime limit or the next poll."""
        clock = self.ctx.clock
        self.idle_waits += 1
        self._wake.clear()
        deadlines = []
        if math.isfinite(next_ready_wall):
            deadlines.append(clock.monotonic() + max(0.0, next_ready_wall - clock.time()))
        runtime_end = self._deadline()
        if runtime_end is not None:
            deadlines.append(runtime_end)
        if self.cooperative:
            deadlines.append(clock.monotonic() + self.poll_interval)
        waiter = asyncio.ensure_future(self._wake.wait())
        tasks: set[asyncio.Future[Any]] = {waiter}
        if deadlines:
            tasks.add(asyncio.ensure_future(clock.sleep_until(min(deadlines))))
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
