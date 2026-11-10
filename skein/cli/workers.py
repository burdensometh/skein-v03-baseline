"""Commands for cooperating processes: ``worker`` and ``workers list|reap``."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import replace
from pathlib import Path
from typing import TextIO

from skein.cli.crawling import add_runtime_overrides, graceful_interrupt, print_summary, runtime_overrides
from skein.cli.data import add_database_source, database_of
from skein.clock import SystemClock, isoformat
from skein.config import ProjectConfig, load_config
from skein.config.runtime import WorkerSettings
from skein.crawler.engine import CrawlResult
from skein.frontier.sqlite import Frontier
from skein.politeness.policy import PolitenessPolicy
from skein.politeness.scheduler import HostScheduler
from skein.store.sqlite import Store
from skein.workers.coordination import Coordination
from skein.workers.liveness import new_worker_id, worker_alive
from skein.workers.process import run_worker

EXIT_OK = 0
EXIT_FAILURE = 1
REAPER_ID = "cli-reaper"


def add_worker_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("config", help="path to the JSON configuration")
    parser.add_argument("-s", "--spider", help="spider name (required when the config defines several)")
    parser.add_argument("--id", dest="worker_id", help="worker id (default: <host>-<pid>-<random>)")
    parser.add_argument("--db", help="override the database path from the config")
    parser.add_argument("--checkpoint-dir", help="override the checkpoint directory")
    parser.add_argument("--wait-for-run", type=float, default=0.0, metavar="S",
                        help="wait up to S seconds for a run to join (default: exit at once if there is none)")
    parser.add_argument("--max-runtime", type=float, help="leave the run after this many seconds")
    parser.add_argument("--follow", action="store_true",
                        help="after a run, wait for the next one (until SIGINT/SIGTERM)")
    parser.add_argument("--stats", choices=("text", "json"), default="text", help="summary format on stdout")
    add_runtime_overrides(parser)


def _worker_config(args: argparse.Namespace) -> ProjectConfig:
    config = load_config(args.config)
    spider = config.spider(args.spider)
    changes: dict[str, object] = {"spiders": (spider,), "runtime": runtime_overrides(config, args)}
    if args.db is not None:
        changes["database"] = Path(args.db)
    if args.checkpoint_dir is not None:
        changes["checkpoint_dir"] = Path(args.checkpoint_dir)
    return replace(config, **changes)  # type: ignore[arg-type]


async def _work(config: ProjectConfig, args: argparse.Namespace) -> list[CrawlResult]:
    stop = asyncio.Event()
    with graceful_interrupt(stop.set):
        return await run_worker(config, worker_id=args.worker_id or new_worker_id(), wait_for_run=args.wait_for_run,
                                max_runtime=args.max_runtime, stop=stop, follow=args.follow)


def cmd_worker(args: argparse.Namespace, out: TextIO) -> int:
    config = _worker_config(args)
    results = asyncio.run(_work(config, args))
    for result in results:
        print_summary(result, args.stats, out)
    healthy = ("finished", "budget_exhausted", "stopped", "idle")
    return EXIT_OK if all(result.status in healthy for result in results) else EXIT_FAILURE


def _worker_rows(store: Store, spider: str | None, settings: WorkerSettings) -> list[dict[str, object]]:
    now = SystemClock().time()
    held = store.workers.leases_held()
    rows = []
    for record in store.workers.list(spider=spider):
        alive = worker_alive(record, now, settings.lease_ttl * 3)
        rows.append({**record.to_dict(), "alive": alive, "leases": held.get(record.id, 0),
                     "heartbeat_age": round(now - record.heartbeat_at, 1),
                     "pages_stored": (record.stats or {}).get("pages_stored")})
    return rows


def cmd_workers(args: argparse.Namespace, out: TextIO) -> int:
    settings = load_config(args.config).runtime.workers if args.config else WorkerSettings()
    with Store(database_of(args)) as store:
        if args.action == "reap":
            spiders = [args.spider] if args.spider else sorted({r.spider for r in store.workers.list()})
            clock = SystemClock()
            for spider in spiders:
                frontier = Frontier(store.db, spider, clock, owner=REAPER_ID, lease_ttl=settings.lease_ttl,
                                    max_lease_attempts=settings.max_lease_attempts)
                scheduler = HostScheduler(store.db, PolitenessPolicy(), clock, owner=REAPER_ID)
                report = Coordination(store, frontier, scheduler, clock, spider=spider, worker_id=REAPER_ID,
                                      settings=settings).reap()
                out.write(f"spider {spider}: {len(report.dead)} dead worker(s) {' '.join(report.dead) or '-'}, "
                          f"{report.requeued} lease(s) reclaimed, {len(report.poisoned)} request(s) failed\n")
            return EXIT_OK
        rows = _worker_rows(store, args.spider, settings)
    if args.json:
        json.dump(rows, out, indent=2, sort_keys=True)
        out.write("\n")
        return EXIT_OK
    if not rows:
        out.write("no workers registered\n")
        return EXIT_OK
    out.write(f"{'worker':<34} {'spider':<12} {'run':>4} {'role':<7} {'state':<9} {'alive':<5} {'leases':>6} "
              f"{'beat':>8} {'pages':>6}  host/pid\n")
    for row in rows:
        out.write(f"{row['id']!s:<34} {row['spider']!s:<12} {row['run_id'] or '-'!s:>4} {row['role']!s:<7} "
                  f"{row['state']!s:<9} {'yes' if row['alive'] else 'no':<5} {row['leases']!s:>6} "
                  f"{row['heartbeat_age']!s:>7}s {row['pages_stored'] if row['pages_stored'] is not None else '-'!s:>6}"
                  f"  {row['hostname']}/{row['pid']}  started {isoformat(row['started_at'])}\n")  # type: ignore[arg-type]
    return EXIT_OK


def add_workers_parser(commands: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    worker = commands.add_parser("worker", help="join a running crawl of a spider as an additional process")
    add_worker_arguments(worker)
    workers = commands.add_parser("workers", help="list registered workers, or reap dead ones")
    workers.add_argument("action", choices=("list", "reap"))
    add_database_source(workers)
    workers.add_argument("-s", "--spider", help="only this spider")
    workers.add_argument("--json", action="store_true")

