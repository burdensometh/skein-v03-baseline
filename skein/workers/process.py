"""Worker processes: ``skein worker`` joining a run, and a supervisor starting several of them.

A worker process opens the database, waits (up to ``wait_for_run``
seconds) for a running run of its spider, works on it cooperatively until
the frontier is drained or the run is closed, and exits. The
:class:`Supervisor` used by ``skein crawl --workers N`` starts N such
processes, reaps them as they exit (so peers can tell a killed worker from
a live one), optionally restarts ones that died, and stops them all on
request.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from skein.clock import Clock, SystemClock
from skein.config.model import ProjectConfig
from skein.crawler.engine import CrawlResult, Crawler
from skein.store.sqlite import Store
from skein.store.workers import WorkerRegistry
from skein.workers.liveness import new_worker_id

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent


async def run_worker(
    config: ProjectConfig,
    *,
    worker_id: str | None = None,
    wait_for_run: float = 0.0,
    max_runtime: float | None = None,
    clock: Clock | None = None,
    stop: asyncio.Event | None = None,
    follow: bool = False,
) -> list[CrawlResult]:
    """Join running runs of ``config``'s spider and work on them; returns one result per run joined.

    Without ``follow`` the worker leaves after its first run (or with an
    ``idle`` result if no run appeared within ``wait_for_run`` seconds).
    With ``follow`` it keeps waiting for the next run until ``stop`` is set.
    """
    clock = clock or SystemClock()
    spider = config.spiders[0]
    identity = worker_id or new_worker_id()
    deadline = clock.monotonic() + wait_for_run
    results: list[CrawlResult] = []
    with Store(config.database, synchronous=config.checkpoint.sqlite_synchronous) as store:
        while stop is None or not stop.is_set():
            crawler = Crawler(spider, store, http=config.http, robots_ttl=config.robots_ttl, clock=clock,
                              checkpoint_dir=config.checkpoint_root(), checkpoint=config.checkpoint,
                              max_runtime=max_runtime, runtime=config.runtime, role="worker", worker_id=identity,
                              cooperative=True)
            watcher = asyncio.ensure_future(stop.wait()) if stop is not None else None
            if watcher is not None:
                watcher.add_done_callback(lambda _: crawler.stop())
            try:
                result = await crawler.run()
            finally:
                if watcher is not None:
                    watcher.cancel()
            if result.status != "idle":
                results.append(result)
                if not follow:
                    break
                deadline = clock.monotonic() + wait_for_run
            elif not follow and clock.monotonic() >= deadline:
                results.append(result)
                break
            await clock.sleep(config.runtime.workers.poll_interval)
    return results


@dataclass
class Child:
    """One supervised worker process."""

    worker_id: str
    process: subprocess.Popen[bytes]
    current_id: str = ""
    restarts: int = 0
    exit_codes: list[int] = field(default_factory=list)
    reaped: bool = False


class Supervisor:
    """Starts ``count`` worker processes for ``spider`` of the configuration file ``config_path``."""

    def __init__(
        self,
        config_path: Path,
        spider: str,
        count: int,
        *,
        prefix: str | None = None,
        extra_args: list[str] | None = None,
        restart_limit: int = 0,
        wait_for_run: float = 30.0,
    ) -> None:
        if count < 1:
            raise ValueError("count must be >= 1")
        self.config_path = Path(config_path)
        self.spider = spider
        self.count = count
        self.prefix = prefix or f"{new_worker_id()}-w"
        self.extra_args = list(extra_args or [])
        self.restart_limit = restart_limit
        self.wait_for_run = wait_for_run
        self.children: list[Child] = []

    def _command(self, worker_id: str) -> list[str]:
        """The worker command line; ``{worker}`` in extra arguments becomes the worker id."""
        extra = [argument.replace("{worker}", worker_id) for argument in self.extra_args]
        return [sys.executable, "-m", "skein", "-q", "worker", str(self.config_path), "-s", self.spider,
                "--id", worker_id, "--wait-for-run", str(self.wait_for_run), "--stats", "json", *extra]

    def _spawn(self, worker_id: str) -> subprocess.Popen[bytes]:
        environment = dict(os.environ)
        environment["PYTHONPATH"] = os.pathsep.join(filter(None, [str(ROOT), environment.get("PYTHONPATH")]))
        return subprocess.Popen(self._command(worker_id), stdout=subprocess.DEVNULL, env=environment)

    def start(self) -> None:
        for number in range(1, self.count + 1):
            worker_id = f"{self.prefix}{number}"
            self.children.append(Child(worker_id, self._spawn(worker_id), worker_id))
        log.info("started %d worker process(es): %s", self.count,
                 ", ".join(f"{c.worker_id} (pid {c.process.pid})" for c in self.children))

    def poll(self) -> list[Child]:
        """Reap exited children (restarting crashed ones within the limit); returns those still running."""
        running = []
        for child in self.children:
            code = child.process.poll()
            if code is None:
                running.append(child)
                continue
            if child.reaped:
                continue
            child.reaped = True
            child.exit_codes.append(code)
            if code != 0:
                log.warning("worker %s exited with status %d", child.worker_id, code)
            if code < 0 and child.restarts < self.restart_limit:
                child.restarts += 1
                child.current_id = f"{child.worker_id}r{child.restarts}"
                child.process = self._spawn(child.current_id)
                child.reaped = False
                running.append(child)
        return running

    def watchdog(self, registry: WorkerRegistry, now: float, stale_after: float) -> list[str]:
        """SIGKILL children that are still running but stopped renewing their heartbeat (hung).

        Their leases then return to the queue through the usual reaping, and
        :meth:`poll` restarts them if the restart limit allows. Returns the
        ids of the children killed.
        """
        killed = []
        for child in self.children:
            if child.process.poll() is not None:
                continue
            record = registry.get(child.current_id)
            if record is None or record.state != "running" or now - record.heartbeat_at <= stale_after:
                continue
            log.warning("worker %s (pid %d) missed its heartbeats for %.0fs; killing it", record.id,
                        child.process.pid, now - record.heartbeat_at)
            child.process.kill()
            killed.append(record.id)
        return killed

    async def wait(self, interval: float = 0.1, check: Callable[[], object] | None = None) -> None:
        """Until every child has exited (for good); ``check`` runs between polls (e.g. the watchdog)."""
        while self.poll():
            if check is not None:
                check()
            await asyncio.sleep(interval)

    def terminate(self, grace: float = 10.0) -> None:
        """SIGTERM every child (they finish their requests in flight), then SIGKILL stragglers."""
        for child in self.children:
            if child.process.poll() is None:
                child.process.send_signal(signal.SIGTERM)
        for child in self.children:
            try:
                child.process.wait(grace)
            except subprocess.TimeoutExpired:
                child.process.kill()
                child.process.wait(grace)
        self.poll()

    @property
    def exit_codes(self) -> dict[str, list[int]]:
        return {child.worker_id: list(child.exit_codes) for child in self.children}
