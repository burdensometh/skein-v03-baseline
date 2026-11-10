"""Running one worker's share of a crawl: session lifecycle around the staged pipeline.

A crawl run is shared by every process working on it. The *local* crawler
(``skein crawl``) plans new runs, resumes interrupted ones from its
checkpoint and works on them; *workers* (``skein worker``) join the run in
progress. Each process owns a checkpoint (snapshot plus journal) holding
its share of the run statistics; everything else -- frontier and leases,
host politeness, page budget charges, sitemap high-water marks -- is in
SQLite and shared.

The run is finalised by the last process to leave it: when the frontier is
drained (or the page budget used up) and no other worker of the run is
still alive, the statistics of every worker are merged (those of workers
that died are rebuilt from their checkpoints), watermarks are advanced
and the run is closed.
"""

from __future__ import annotations

import logging
import os
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from skein.checkpoint.session import Checkpoint, CheckpointSettings, LegacyState, SessionState
from skein.clock import Clock, SystemClock
from skein.config.model import HttpSettings, SpiderConfig
from skein.config.runtime import RuntimeSettings
from skein.dedup.seen import SeenUrls
from skein.dedup.simhash import SimHasher
from skein.dedup.simindex import NearDuplicateIndex
from skein.extract.extractor import Extractor
from skein.frontier.sqlite import Frontier
from skein.incremental.modes import RunMode
from skein.incremental.planner import RunPlan, complete_watermarks, crawl_roots, plan_run, resolve_mode
from skein.metrics.collect import add_crawl_stats, add_pipeline_stats
from skein.checkpoint.snapshot import atomic_write
from skein.metrics.exposition import merge
from skein.metrics.prometheus import Registry
from skein.metrics.server import MetricsServer
from skein.metrics.stats import CrawlStats
from skein.net.client import DEFAULT_USER_AGENT, HttpClient
from skein.pipeline.context import CrawlContext
from skein.pipeline.runner import Pipeline
from skein.pipeline.work import Outcome
from skein.politeness.scheduler import HostScheduler
from skein.robots.cache import RobotsCache
from skein.store.rekey import CanonicalRulesError, rekey_spider
from skein.store.schema_workers import local_worker_id
from skein.store.sqlite import Store
from skein.warc.writer import SealReport, WarcFile, WarcWriter, seal
from skein.workers.coordination import Coordination
from skein.workers.liveness import HOSTNAME, worker_alive

log = logging.getLogger(__name__)

RESUMABLE_STATUSES = ("running", "stopped", "failed")
ROLES = ("local", "worker")


@dataclass
class CrawlResult:
    """What a finished (or stopped) crawl reports back.

    ``stats`` is the run summary when this process closed the run, else
    this worker's own summary (``finalized`` tells which).
    """

    spider: str
    run_id: int
    status: str
    stats: dict[str, Any]
    hosts: dict[str, Any] = field(default_factory=dict)
    mode: str = RunMode.FULL.value
    resumed: bool = False
    worker_id: str = ""
    finalized: bool = False


class SpiderBusyError(RuntimeError):
    """Another live process is already crawling this spider as its local crawler."""


class Crawler:
    """Runs one spider in this process, as its local crawler or as an additional worker.

    ``cooperative`` means other processes may work on the same run: leases
    are renewed by a timer task, dead peers are reaped and the dispatcher
    polls for their changes. Without it (single-process crawls, tests with
    a fake clock) leases are renewed after commits.
    """

    def __init__(
        self,
        spider: SpiderConfig,
        store: Store,
        *,
        http: HttpSettings | None = None,
        client: HttpClient | None = None,
        clock: Clock | None = None,
        robots_ttl: float = 86400.0,
        checkpoint_dir: Path | None = None,
        checkpoint: CheckpointSettings | None = None,
        mode: RunMode | None = None,
        resume: bool = True,
        rng: random.Random | None = None,
        max_runtime: float | None = None,
        runtime: RuntimeSettings | None = None,
        role: str = "local",
        worker_id: str | None = None,
        cooperative: bool = False,
    ) -> None:
        if store.path == ":memory:" and checkpoint_dir is None:
            raise ValueError("an in-memory store needs an explicit checkpoint_dir")
        if role not in ROLES:
            raise ValueError(f"role must be one of {ROLES}")
        if role == "worker" and worker_id is None:
            raise ValueError("a worker needs a worker_id")
        self.spider = spider
        self.store = store
        self.role = role
        self.worker_id = worker_id or local_worker_id(spider.name)
        self.clock = clock or SystemClock()
        self.runtime = runtime or RuntimeSettings()
        self.cooperative = cooperative
        self._owns_client = client is None
        if client is None:
            settings = http or HttpSettings(user_agent=DEFAULT_USER_AGENT)
            client = HttpClient(settings.client_config(), self.clock)
        self.client = client
        self.rng = rng or random.Random(spider.retry_seed)
        leases = self.runtime.workers
        self.frontier = Frontier(store.db, spider.name, self.clock, max_depth=spider.max_depth, owner=self.worker_id,
                                 lease_ttl=leases.lease_ttl, max_lease_attempts=leases.max_lease_attempts)
        self.scheduler = HostScheduler(store.db, spider.politeness, self.clock, owner=self.worker_id,
                                       backoff=spider.retry.backoff, rng=self.rng, slot_ttl=leases.lease_ttl)
        self.robots = RobotsCache(client, self.clock, client.config.user_agent, ttl=robots_ttl,
                                  persistence=store.robots)
        root = checkpoint_dir or Path(f"{store.path}.checkpoints")
        directory = root / spider.name if role == "local" else root / spider.name / "workers" / self.worker_id
        self.checkpoint = Checkpoint(directory, self.clock, checkpoint)
        self.coordination = Coordination(store, self.frontier, self.scheduler, self.clock, spider=spider.name,
                                         worker_id=self.worker_id, settings=leases)
        self.requested_mode = mode or spider.incremental.mode
        self.resume_allowed = resume
        self.max_runtime = max_runtime
        self.roots = crawl_roots(
            tuple(spider.canonicalizer.key(u) for u in spider.seeds),
            tuple(spider.canonicalizer.key(u) for u in spider.sitemaps),
        )
        dedup = self.runtime.dedup
        self.ctx = CrawlContext(
            spider=spider, store=store, clock=self.clock, client=client, frontier=self.frontier,
            scheduler=self.scheduler, robots=self.robots, extractor=Extractor(spider.plan.items),
            links=spider.link_extractor(), checkpoint=self.checkpoint, runtime=self.runtime,
            worker_id=self.worker_id, rng=self.rng,
            near=NearDuplicateIndex(store.db, max_distance=dedup.max_distance) if dedup.near_duplicates else None,
            simhasher=SimHasher(dedup.shingle_size, dedup.min_words) if dedup.near_duplicates else None,
        )
        self.plan: RunPlan | None = None
        self.pipeline: Pipeline | None = None
        self.resumed = False
        self.rekeyed: dict[str, Any] | None = None
        self._stop_requested = False
        self._started_mono = 0.0
        self._metrics_written = 0.0
        self._last_commit = 0.0
        self.metrics_server: MetricsServer | None = None

    # -- public API ----------------------------------------------------------

    @property
    def session(self) -> SessionState | None:
        return self.ctx.session

    @property
    def run_id(self) -> int:
        return self.ctx.run_id

    @property
    def stats(self) -> CrawlStats:
        return self.session.stats if self.session is not None else CrawlStats()

    def stop(self) -> None:
        """Finish the requests in flight and exit; the run stays resumable."""
        self._stop_requested = True
        if self.pipeline is not None:
            self.pipeline.stop()

    async def run(self) -> CrawlResult:
        self._started_mono = self._last_commit = self.clock.monotonic()
        try:
            session = self._open_session()
        except BaseException:
            if self._owns_client:
                await self.client.close()
            raise
        if session is None:
            if self._owns_client:
                await self.client.close()
            return CrawlResult(self.spider.name, 0, "idle", {}, worker_id=self.worker_id)
        background = []
        if self.cooperative:
            background.append(lambda: self.coordination.run(session.run_id, self.stop))
        self.pipeline = Pipeline(self.ctx, max_runtime=self.max_runtime, cooperative=self.cooperative,
                                 on_commit=self._after_commit, background=background)
        if self._stop_requested:
            self.pipeline.stop()
        status = "failed"
        finalized: dict[str, Any] | None = None
        port = self.runtime.metrics.port
        try:
            if port is not None:
                self.metrics_server = MetricsServer(lambda: self.metrics().render(), port=port, health=self.health)
                await self.metrics_server.start()
            await self.pipeline.run()
            dispatcher = self.pipeline.dispatcher
            if dispatcher.budget_exhausted:
                status = "budget_exhausted"
            elif dispatcher.stopping:
                status = "stopped"
            else:
                status = "finished"
        finally:
            summary, finalized = self._close_session(status)
            if self.metrics_server is not None:
                await self.metrics_server.stop()
            if self._owns_client:
                await self.client.close()
        return CrawlResult(self.spider.name, session.run_id, status, finalized or summary,
                           self.scheduler.snapshot(self.frontier.hosts_seen()), session.mode, self.resumed,
                           self.worker_id, finalized is not None)

    def metrics(self) -> Registry:
        """This worker's statistics and pipeline state as Prometheus metric families."""
        registry = Registry()
        labels = {"spider": self.spider.name, "worker": self.worker_id}
        registry.gauge("worker_info", "Constant 1, labelled with the worker's role.",
                       ("spider", "worker", "role")).set(1, role=self.role, **labels)
        add_crawl_stats(registry, self.stats, **labels)
        if self.pipeline is not None:
            add_pipeline_stats(registry, self.pipeline, **labels)
        frontier = self.frontier.stats
        lease_events = registry.counter("lease_events_total", "Lease operations by this worker.",
                                        ("spider", "worker", "event"))
        for event, value in (("claimed", frontier.claimed), ("renewed", frontier.renewed),
                             ("reclaimed", frontier.reclaimed + self.coordination.stats.reclaimed),
                             ("poisoned", frontier.poisoned + self.coordination.stats.poisoned),
                             ("lost", frontier.leases_lost)):
            lease_events.set(value, event=event, **labels)
        return registry

    def health(self) -> dict[str, Any]:
        """Liveness report: healthy while the pipeline runs and commits keep coming (or nothing is in flight)."""
        pipeline = self.pipeline
        idle_for = self.clock.monotonic() - self._last_commit
        in_flight = pipeline.dispatcher.in_flight if pipeline is not None else 0
        stalled = in_flight > 0 and idle_for > self.runtime.workers.lease_ttl
        return {"worker": self.worker_id, "role": self.role, "run_id": self.run_id,
                "running": pipeline is not None and not pipeline.dispatcher.finished,
                "in_flight": in_flight, "seconds_since_commit": round(idle_for, 3),
                "healthy": pipeline is not None and not stalled}

    def _write_metrics(self, *, force: bool = False) -> None:
        """Rewrite the metrics textfile when due.

        Worker processes started by ``crawl --workers`` write ``<textfile>.<worker id>``;
        the local crawler folds those siblings into its own file, so the
        textfile collector sees one exposition for the whole run.
        """
        target = self.runtime.metrics.textfile
        if target is None:
            return
        now = self.clock.monotonic()
        if not force and now - self._metrics_written < self.runtime.metrics.interval:
            return
        self._metrics_written = now
        own = self.metrics().render()
        siblings = sorted(target.parent.glob(target.name + ".*")) if self.role == "local" else []
        texts = [own]
        for sibling in siblings:
            try:
                texts.append(sibling.read_text(encoding="utf-8"))
            except OSError:
                continue
        text = merge(texts, drop_labels=()) if len(texts) > 1 else own
        atomic_write(target, text.encode("utf-8"))

    def summary(self) -> dict[str, Any]:
        """This worker's statistics plus frontier, connection, pipeline and checkpoint details."""
        data = self.stats.summary(self.clock.monotonic() - self._started_mono)
        frontier = self.frontier.stats
        data["frontier"] = {
            "pending": len(self.frontier),
            "states": self.frontier.counts(),
            "added": frontier.added,
            "duplicates": frontier.duplicates,
            "too_deep": frontier.too_deep,
            "requeued": frontier.requeued,
            "rescheduled": frontier.rescheduled,
            "recovered": frontier.recovered,
            "claimed": frontier.claimed,
            "reclaimed": frontier.reclaimed + self.coordination.stats.reclaimed,
            "poisoned": frontier.poisoned + self.coordination.stats.poisoned,
            "leases_lost": frontier.leases_lost,
            "by_depth": {str(k): v for k, v in sorted(frontier.by_depth.items())},
        }
        pool = self.client.pool.stats
        data["connections"] = {"opened": pool.connections_opened, "reused": pool.connections_reused}
        session = self.session
        data["run"] = {
            "mode": session.mode if session else None,
            "resumes": session.resumes if session else 0,
            "plan": self.plan.to_dict() if self.plan else None,
            "recovery": self.checkpoint.recovery.to_dict() if self.checkpoint.recovery else None,
            "snapshots": self.checkpoint.snapshots,
            "journal_records": self.checkpoint.journal.records_written,
            "rekeyed": self.rekeyed,
        }
        data["worker"] = {"id": self.worker_id, "role": self.role, "cooperative": self.cooperative,
                          "coordination": vars(self.coordination.stats).copy()}
        if self.pipeline is not None:
            data["pipeline"] = self.pipeline.stage_stats()
        if self.ctx.seen is not None:
            data["seen_filter"] = vars(self.ctx.seen.stats).copy()
        if self.ctx.archive is not None:
            data["archive_files"] = list(self.ctx.archive.files_written)
        return data

    # -- session lifecycle -----------------------------------------------------

    def _open_session(self) -> SessionState | None:
        self._check_canonical_rules()
        self.coordination.reap()
        if self.role == "local":
            state = self._open_local()
        else:
            state = self._join_run()
            if state is None:
                return None
        self.ctx.session = state
        self._open_dedup_and_archive()
        with self.store.transaction():
            self.store.workers.register(
                self.worker_id, self.spider.name, state.run_id, hostname=HOSTNAME, pid=os.getpid(), role=self.role,
                checkpoint=str(self.checkpoint.directory), now=self.clock.time())
        return state

    def _check_canonical_rules(self) -> None:
        """Rekey stored URLs when the spider's canonicalisation settings changed (only with no peers alive)."""
        canonical = self.spider.canonicalizer
        if not self.store.canonical.needs_rekey(self.spider.name, canonical):
            if self.store.canonical.get(self.spider.name) is None:
                with self.store.transaction():
                    self.store.canonical.record(self.spider.name, canonical, self.clock.time())
            return
        if self.role == "worker":
            raise CanonicalRulesError(f"spider {self.spider.name}: stored URL keys use other canonicalisation "
                                      "settings; run `skein crawl` (or `skein db rekey`) first")
        live = [record.id for record in self.store.workers.list(spider=self.spider.name, states=("running",))
                if worker_alive(record, self.clock.time(), self.coordination.stale_after)]
        if live:
            raise CanonicalRulesError(f"spider {self.spider.name}: cannot rekey URLs while workers "
                                      f"{', '.join(live)} are running")
        with self.store.transaction():
            report = rekey_spider(self.store.db, self.spider.name, canonical, self.clock.time())
        self.rekeyed = report.to_dict()
        bloom = self.runtime.bloom_path(self.store.path, self.spider.name)
        if bloom is not None and report.renamed:
            # The filter still holds the old spellings; rebuild it from the rekeyed frontier.
            bloom.unlink(missing_ok=True)

    def _open_local(self) -> SessionState:
        mine = self.store.workers.get(self.worker_id)
        if mine is not None and worker_alive(mine, self.clock.time(), self.coordination.stale_after) \
                and mine.pid != os.getpid():
            raise SpiderBusyError(f"spider {self.spider.name} is already being crawled by pid {mine.pid}")
        state = self._recover_session() if self.resume_allowed else None
        self._settle_orphaned_runs(keep=state.run_id if state is not None else None)
        self.frontier.recover()
        self.scheduler.release_owner()
        if state is not None:
            self.resumed = True
            self.frontier.run_id = state.run_id
            with self.store.transaction():
                self.store.runs.resume(state.run_id)
            if state.legacy:
                self._import_legacy(state.run_id, state.legacy)
                state.legacy = LegacyState()
            self.checkpoint.resume(state)
            log.info("%s: resuming run %d (%s)", self.spider.name, state.run_id,
                     self.checkpoint.recovery.to_dict() if self.checkpoint.recovery else "no journal")
            return state
        now = self.clock.time()
        mode = resolve_mode(self.requested_mode, self.store, self.spider.name, self.roots)
        with self.store.transaction():
            run_id = self.store.runs.begin(self.spider.name, now, mode.value)
        self.frontier.run_id = run_id
        self.plan = plan_run(self.frontier, self.store, self.spider.name, self.roots, mode, run_id, now)
        state = SessionState(self.spider.name, run_id, mode.value, now, worker_id=self.worker_id)
        self.checkpoint.begin(state)
        return state

    def _join_run(self) -> SessionState | None:
        runs = [run for run in self.store.runs.open_runs(self.spider.name) if run["status"] == "running"]
        if not runs:
            return None
        run = runs[-1]
        self.frontier.run_id = run["id"]
        state = SessionState(self.spider.name, run["id"], run["mode"], run["started_at"], worker_id=self.worker_id)
        self.checkpoint.begin(state)
        log.info("%s: worker %s joined run %d", self.spider.name, self.worker_id, run["id"])
        return state

    def _import_legacy(self, run_id: int, legacy: LegacyState) -> None:
        """Move what a 0.2 checkpoint kept (budget charges, host back-off, lastmod marks) into the database."""
        with self.store.transaction():
            self.store.db.executemany("UPDATE frontier SET charged_run = ? WHERE spider = ? AND url = ?",
                                      [(run_id, self.spider.name, url) for url in sorted(legacy.charged)])
            self.store.db.execute(
                "UPDATE runs SET pages_charged = (SELECT COUNT(*) FROM frontier WHERE spider = ? AND charged_run = ?) "
                "WHERE id = ?", (self.spider.name, run_id, run_id))
            for host, data in legacy.hosts.items():
                if data.get("crawl_delay") is not None:
                    self.scheduler.set_crawl_delay(host, float(data["crawl_delay"]))
                backoff = float(data.get("backoff_until") or 0.0)
                if backoff > self.clock.time():
                    self.scheduler.back_off(host, backoff - self.clock.time())
                if data.get("blocked_until"):
                    self.frontier.block_host(host, float(data["blocked_until"]))
            for root, value in legacy.lastmod_high.items():
                self.store.watermarks.note_lastmod(self.spider.name, root, run_id, value)
        log.info("%s: imported 0.2 checkpoint state (%d charge(s), %d host(s))", self.spider.name,
                 len(legacy.charged), len(legacy.hosts))

    def _recover_session(self) -> SessionState | None:
        state = self.checkpoint.load(confirm=self.store.documents.journaled)
        if state is None:
            return None
        run = self.store.runs.get(state.run_id)
        if state.spider != self.spider.name or run is None or run["status"] not in RESUMABLE_STATUSES:
            log.warning("%s: discarding checkpoint of run %d (no resumable run)", self.spider.name, state.run_id)
            self.checkpoint.discard()
            self.checkpoint.recovery = None
            return None
        state.worker_id = self.worker_id
        return state

    def _settle_orphaned_runs(self, keep: int | None) -> None:
        """Runs left open without a usable checkpoint can no longer be resumed."""
        with self.store.transaction():
            for run in self.store.runs.open_runs(self.spider.name):
                if run["id"] == keep:
                    continue
                status = "abandoned" if self.resume_allowed is False or run["status"] == "stopped" else "interrupted"
                self.store.runs.finish(run["id"], run["finished_at"] or self.clock.time(), status, None)
        if not self.resume_allowed:
            self.checkpoint.discard()

    def _open_dedup_and_archive(self) -> None:
        dedup = self.runtime.dedup
        if dedup.bloom:
            seen = SeenUrls(self.store.db, self.spider.name, self.runtime.bloom_path(self.store.path, self.spider.name),
                            capacity=dedup.bloom_capacity, error_rate=dedup.bloom_error_rate)
            seen.open()
            self.ctx.seen = seen
        archive = self.runtime.archive
        if archive.enabled:
            self._seal_abandoned_warcs()
            self.ctx.archive = WarcWriter(
                self.runtime.archive_directory(self.store.path), self.clock, prefix=archive.prefix,
                worker=self.worker_id, max_size=archive.max_size, compresslevel=archive.compresslevel,
                info={"isPartOf": self.spider.name, "operator-run": str(self.run_id)},
                on_open=self._warc_opened, on_close=self._warc_closed)

    def _seal_abandoned_warcs(self) -> None:
        """Close the WARC files an earlier, killed incarnation of this worker left open."""
        directory = self.runtime.archive_directory(self.store.path)
        rows = self.store.db.execute(
            "SELECT name FROM warc_files WHERE worker_id = ? AND closed_at IS NULL", (self.worker_id,)).fetchall()
        for (name,) in rows:
            path = directory / name
            report = seal(path) if path.exists() else SealReport(0, 0, 0)
            with self.store.transaction():
                self.store.db.execute("UPDATE warc_files SET closed_at = ?, records = ?, bytes = ? WHERE name = ?",
                                      (self.clock.time(), report.records, report.size, name))
            log.warning("%s: sealed WARC file %s left open by a crash (%d record(s), %d torn byte(s) cut)",
                        self.spider.name, name, report.records, report.torn_bytes)

    def _warc_opened(self, file: WarcFile) -> None:
        directory = str(self.runtime.archive_directory(self.store.path))
        with self.store.transaction():
            self.store.db.execute(
                "INSERT OR REPLACE INTO warc_files (name, directory, worker_id, opened_at) VALUES (?, ?, ?, ?)",
                (file.name, directory, self.worker_id, file.opened_at))

    def _warc_closed(self, file: WarcFile) -> None:
        with self.store.transaction():
            self.store.db.execute("UPDATE warc_files SET closed_at = ?, records = ?, bytes = ? WHERE name = ?",
                                  (self.clock.time(), file.records, file.size, file.name))

    def _after_commit(self, batch: list[Outcome]) -> None:
        self._last_commit = self.clock.monotonic()
        if not self.cooperative:
            self.coordination.maybe_beat()
        self._write_metrics()
        if self.checkpoint.maybe_snapshot() and self.ctx.seen is not None:
            self.ctx.seen.save()

    def _close_session(self, status: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
        session = self.session
        assert session is not None
        if self.ctx.archive is not None:
            self.ctx.archive.close()
        if self.ctx.seen is not None:
            self.ctx.seen.save()
        self._write_metrics(force=True)
        summary = self.summary()
        now = self.clock.time()
        final_state = {"finished": "finished", "budget_exhausted": "finished", "stopped": "stopped"}.get(status, "stopped")
        finalized = None
        if status in ("finished", "budget_exhausted"):
            with self.store.transaction():
                self.store.workers.finish(self.worker_id, final_state, now, session.stats.to_state())
                finalized = self._finalize_run(status, summary)
            if self.role == "worker" or finalized is not None:
                self.checkpoint.discard(remove_directory=self.role == "worker")
            else:
                self.checkpoint.snapshot()
                self.checkpoint.close()
            return summary, finalized
        with self.store.transaction():
            if self.role == "local":
                self.store.runs.finish(session.run_id, now, status, summary)
                self.store.workers.finish(self.worker_id, final_state, now, None)
            else:
                self.store.workers.finish(self.worker_id, final_state, now, session.stats.to_state())
        if self.role == "local":
            self.checkpoint.snapshot()
            self.checkpoint.close()
        else:
            self.checkpoint.discard(remove_directory=True)
        return summary, None

    def _finalize_run(self, status: str, summary: dict[str, Any]) -> dict[str, Any] | None:
        """Close the run if this is the last live process on it; returns the run summary if so."""
        session = self.session
        assert session is not None
        run = self.store.runs.get(session.run_id)
        if run is None or run["status"] != "running":
            return None
        if status == "finished" and not self.frontier.is_drained():
            return None
        now = self.clock.time()
        merged = CrawlStats()
        merged.merge(session.stats)
        workers = []
        for record in self.store.workers.list(run_id=session.run_id):
            if record.id == self.worker_id:
                continue
            if worker_alive(record, now, self.coordination.stale_after):
                log.info("%s: worker %s is still running; it will close run %d", self.spider.name, record.id,
                         session.run_id)
                return None
            stats = record.stats if record.stats is not None else self._recover_worker_stats(record.id,
                                                                                             record.checkpoint)
            if stats is not None:
                merged.merge(CrawlStats.from_state(stats))
            workers.append(record.id)
        result = merged.summary(self.clock.monotonic() - self._started_mono)
        for key in ("frontier", "connections", "run", "pipeline", "seen_filter", "archive_files"):
            if key in summary:
                result[key] = summary[key]
        result["workers"] = [self.worker_id, *workers]
        completed = complete_watermarks(self.frontier, self.store, self.spider.name, self.roots, run["started_at"],
                                        session.run_id)
        result["run"] = dict(result.get("run") or {}, watermarks_completed=len(completed))
        self.store.runs.finish(session.run_id, now, status, result)
        return result

    def _recover_worker_stats(self, worker_id: str, directory: str | None) -> dict[str, Any] | None:
        """Rebuild a dead worker's statistics from its checkpoint, then remove the checkpoint."""
        if not directory:
            return None
        checkpoint = Checkpoint(Path(directory), self.clock)
        try:
            state = checkpoint.load(confirm=self.store.documents.journaled)
        except Exception:  # noqa: BLE001 - a damaged checkpoint only costs that worker's statistics
            log.warning("%s: checkpoint of dead worker %s is unusable", self.spider.name, worker_id, exc_info=True)
            return None
        if state is None:
            return None
        stats = state.stats.to_state()
        self.store.workers.finish(worker_id, "dead", self.clock.time(), stats)
        checkpoint.discard(remove_directory=True)
        return stats

