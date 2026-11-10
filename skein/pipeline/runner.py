"""Assembling the stages: dispatcher -> fetch -> parse -> write, connected by bounded queues.

::

    Dispatcher --jobs--> FetchStage x concurrency --parses--> ParseStage x parse_tasks --writes--> BatchWriter
         |                     |                                                            ^
         +---- robots-denied --+------------ failures, robots.txt re-queues ----------------+

Shutdown cascades through the queues: when the dispatcher stops claiming
(run finished, page budget used up, stop requested, runtime limit), the
job queue is closed; when every fetcher has drained it, the parse queue is
closed; when every parser is done, the write queue is closed and the
writer commits what is left. Nothing in flight is lost on an orderly
stop. If any stage raises, all stages are cancelled and the error
propagates (leases left behind are reclaimed by the next process).
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Awaitable, Callable

from skein.pipeline.context import CrawlContext
from skein.pipeline.dispatch import Dispatcher
from skein.pipeline.fetching import FetchStage
from skein.pipeline.parsing import ParseStage
from skein.pipeline.queues import StageQueue
from skein.pipeline.work import Fetched, Job, Outcome
from skein.pipeline.writing import BatchWriter


class Pipeline:
    """One worker's staged crawl over a shared :class:`CrawlContext`."""

    def __init__(
        self,
        ctx: CrawlContext,
        *,
        max_runtime: float | None = None,
        cooperative: bool = False,
        on_commit: Callable[[list[Outcome]], None] | None = None,
        background: list[Callable[[], Awaitable[None]]] | None = None,
    ) -> None:
        settings = ctx.runtime.pipeline
        self.ctx = ctx
        self.jobs: StageQueue[Job] = StageQueue("fetch", settings.fetch_queue, consumers=ctx.spider.concurrency)
        self.parses: StageQueue[Fetched] = StageQueue("parse", settings.parse_queue, consumers=settings.parse_tasks)
        self.writes: StageQueue[Outcome] = StageQueue("write", settings.write_queue, consumers=1)
        self.dispatcher = Dispatcher(ctx, self.jobs, self.writes, max_runtime=max_runtime, cooperative=cooperative,
                                     poll_interval=ctx.runtime.workers.poll_interval)
        self.fetch = FetchStage(ctx, self.parses, self.writes, on_slot_free=self.dispatcher.notify)
        self._executor = ThreadPoolExecutor(settings.parse_threads, thread_name_prefix="skein-parse") \
            if settings.parse_threads else None
        self.parse = ParseStage(ctx, self.writes, self._executor)
        self.writer = BatchWriter(ctx, self._committed, batch_size=settings.write_batch)
        self._on_commit = on_commit
        self._background = background or []

    def stop(self) -> None:
        self.dispatcher.stop()

    def _committed(self, batch: list[Outcome]) -> None:
        self.dispatcher.settled(len(batch))
        if self._on_commit is not None:
            self._on_commit(batch)

    async def _dispatch(self) -> None:
        try:
            await self.dispatcher.run()
        finally:
            await self.jobs.close()

    async def _fetchers(self) -> None:
        workers = [asyncio.create_task(self.fetch.run(self.jobs), name=f"skein-fetch-{n}")
                   for n in range(self.jobs.consumers)]
        try:
            await asyncio.gather(*workers)
        finally:
            for task in workers:
                task.cancel()
        await self.parses.close()

    async def _parsers(self) -> None:
        workers = [asyncio.create_task(self.parse.run(self.parses), name=f"skein-parse-{n}")
                   for n in range(self.parses.consumers)]
        try:
            await asyncio.gather(*workers)
        finally:
            for task in workers:
                task.cancel()
        await self.writes.close()

    async def run(self) -> None:
        stages = [
            asyncio.create_task(self._dispatch(), name="skein-dispatch"),
            asyncio.create_task(self._fetchers(), name="skein-fetchers"),
            asyncio.create_task(self._parsers(), name="skein-parsers"),
            asyncio.create_task(self.writer.run(self.writes), name="skein-writer"),
        ]
        helpers = [asyncio.create_task(factory(), name="skein-background") for factory in self._background]
        try:
            await asyncio.gather(*stages)
        except BaseException:
            for task in stages:
                task.cancel()
            await asyncio.gather(*stages, return_exceptions=True)
            raise
        finally:
            for task in helpers:
                task.cancel()
            await asyncio.gather(*helpers, return_exceptions=True)
            if self._executor is not None:
                self._executor.shutdown(wait=True)

    def stage_stats(self) -> dict[str, Any]:
        """Queue and stage counters for summaries and metrics."""
        return {
            "queues": {queue.name: queue.stats().to_dict() for queue in (self.jobs, self.parses, self.writes)},
            "dispatch": {"claims": self.dispatcher.claims, "idle_waits": self.dispatcher.idle_waits,
                         "busy_waits": self.dispatcher.busy_waits, "in_flight": self.dispatcher.in_flight},
            "fetch": {"processed": self.fetch.processed, "busy": self.fetch.busy},
            "parse": {"processed": self.parse.processed, "busy": self.parse.busy},
            "write": {"outcomes": self.writer.outcomes, "batches": self.writer.batch_sizes.count,
                      "fallbacks": self.writer.fallbacks, "busy_retries": self.writer.busy_retries,
                      "batch_size": self.writer.batch_sizes.to_dict(),
                      "commit_seconds": self.writer.commit_seconds.to_dict()},
        }
