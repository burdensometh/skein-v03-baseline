"""Commands that talk to sites: ``crawl``, ``extract``, ``robots`` and ``validate``."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import json
import signal
from pathlib import Path
from typing import Callable, Iterator, TextIO

from skein.checkpoint.journal import FsyncPolicy
from skein.config import HttpSettings, ProjectConfig, load_config
from skein.config.runtime import RuntimeSettings
from skein.crawler.engine import CrawlResult, Crawler
from skein.extract.extractor import Extractor
from skein.html.builder import parse_html
from skein.clock import SystemClock, isoformat
from skein.frontier.sqlite import Frontier
from skein.incremental.modes import RunMode
from skein.incremental.planner import crawl_roots, preview_run
from skein.net.charset import decode_body
from skein.net.client import DEFAULT_USER_AGENT, HttpClient
from skein.net.errors import HttpError
from skein.robots.cache import RobotsCache
from skein.store.sqlite import Store
from skein.urls.parse import parse_url
from skein.workers.process import Supervisor

EXIT_OK = 0
EXIT_FAILURE = 1


def add_crawl_arguments(crawl: argparse.ArgumentParser) -> None:
    crawl.add_argument("config", help="path to the JSON configuration")
    crawl.add_argument("-s", "--spider", help="spider name (required when the config defines several)")
    crawl.add_argument("--db", help="override the database path from the config")
    crawl.add_argument("--mode", choices=[m.value for m in RunMode], help="override the spider's run mode")
    crawl.add_argument("--fresh", action="store_true",
                       help="do not resume an interrupted run; start a new one")
    crawl.add_argument("--checkpoint-dir", help="override the checkpoint directory")
    crawl.add_argument("--fsync", choices=[p.value for p in FsyncPolicy], help="override the journal fsync policy")
    crawl.add_argument("--max-pages", type=int, help="override max_pages")
    crawl.add_argument("--max-depth", type=int, help="override max_depth")
    crawl.add_argument("--min-delay", type=float, help="override politeness.min_delay (seconds)")
    crawl.add_argument("--concurrency", type=int, help="override concurrency")
    crawl.add_argument("--max-runtime", type=float,
                       help="stop (resumably) after this many seconds; the next crawl continues the run")
    crawl.add_argument("--stats", choices=("text", "json"), default="text", help="summary format on stdout")
    crawl.add_argument("--workers", type=int, default=0, metavar="N",
                       help="also start N `skein worker` processes that share the run")
    crawl.add_argument("--restart-workers", type=int, default=0, metavar="N",
                       help="restart a worker that was killed, up to N times")
    add_runtime_overrides(crawl)


def add_runtime_overrides(parser: argparse.ArgumentParser) -> None:
    """Options shared by ``crawl`` and ``worker`` that override the 0.3 runtime groups."""
    parser.add_argument("--warc", nargs="?", const="", metavar="DIR",
                        help="write WARC files (to DIR, default: next to the database)")
    parser.add_argument("--metrics-file", help="rewrite this Prometheus textfile while crawling")
    parser.add_argument("--metrics-port", type=int, help="serve /metrics on this port (0: any free port)")
    parser.add_argument("--lease-ttl", type=float, help="override workers.lease_ttl (seconds)")


def runtime_overrides(config: ProjectConfig, args: argparse.Namespace) -> RuntimeSettings:
    runtime = config.runtime
    if args.warc is not None:
        directory = Path(args.warc) if args.warc else runtime.archive.directory
        runtime = dataclasses.replace(runtime, archive=dataclasses.replace(runtime.archive, enabled=True,
                                                                           directory=directory))
    if args.metrics_file is not None or args.metrics_port is not None:
        metrics = runtime.metrics
        if args.metrics_file is not None:
            metrics = dataclasses.replace(metrics, textfile=Path(args.metrics_file))
        if args.metrics_port is not None:
            metrics = dataclasses.replace(metrics, port=args.metrics_port)
        runtime = dataclasses.replace(runtime, metrics=metrics)
    if args.lease_ttl is not None:
        workers = runtime.workers
        heartbeat = min(workers.heartbeat_interval, args.lease_ttl / 3)
        runtime = dataclasses.replace(runtime, workers=dataclasses.replace(
            workers, lease_ttl=args.lease_ttl, heartbeat_interval=heartbeat,
            reclaim_interval=min(workers.reclaim_interval, heartbeat)))
    return runtime


def apply_overrides(config: ProjectConfig, args: argparse.Namespace) -> ProjectConfig:
    spider = config.spider(args.spider)
    changes: dict[str, object] = {}
    if args.max_pages is not None:
        changes["max_pages"] = args.max_pages
    if args.max_depth is not None:
        changes["max_depth"] = args.max_depth
    if args.concurrency is not None:
        changes["concurrency"] = args.concurrency
    if args.min_delay is not None:
        changes["politeness"] = dataclasses.replace(spider.politeness, min_delay=args.min_delay)
    if args.mode is not None:
        changes["incremental"] = dataclasses.replace(spider.incremental, mode=RunMode(args.mode))
    spider = dataclasses.replace(spider, **changes)
    project: dict[str, object] = {"spiders": (spider,), "runtime": runtime_overrides(config, args)}
    if args.db is not None:
        project["database"] = Path(args.db)
    if args.checkpoint_dir is not None:
        project["checkpoint_dir"] = Path(args.checkpoint_dir)
    if args.fsync is not None:
        project["checkpoint"] = dataclasses.replace(config.checkpoint, fsync=FsyncPolicy(args.fsync))
    return dataclasses.replace(config, **project)  # type: ignore[arg-type]


@contextlib.contextmanager
def graceful_interrupt(stop: Callable[[], None]) -> Iterator[None]:
    """Turn SIGINT/SIGTERM into a graceful ``stop()`` while the crawl runs."""
    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, stop)
        loop.add_signal_handler(signal.SIGTERM, stop)
        installed = True
    except (NotImplementedError, RuntimeError, ValueError):
        # Not the main thread, or a platform without loop signal handlers.
        installed = False
    try:
        yield
    finally:
        if installed:
            loop.remove_signal_handler(signal.SIGINT)
            loop.remove_signal_handler(signal.SIGTERM)


async def run_crawl(
    config: ProjectConfig,
    *,
    resume: bool,
    max_runtime: float | None = None,
    supervisor: Supervisor | None = None,
) -> CrawlResult:
    """Run the local crawler; with a ``supervisor`` its worker processes share the run.

    The local crawler opens (or resumes) the run first, so the workers find
    it. When the local crawler is done, the workers are waited for; the
    last process to leave closes the run, and the result then carries the
    run's merged statistics.
    """
    spider = config.spiders[0]
    with Store(config.database, synchronous=config.checkpoint.sqlite_synchronous) as store:
        crawler = Crawler(
            spider, store, http=config.http, robots_ttl=config.robots_ttl,
            checkpoint_dir=config.checkpoint_root(), checkpoint=config.checkpoint, resume=resume,
            max_runtime=max_runtime, runtime=config.runtime, cooperative=True,
        )

        def stop() -> None:
            crawler.stop()
            if supervisor is not None:
                for child in supervisor.children:
                    if child.process.poll() is None:
                        child.process.send_signal(signal.SIGTERM)

        with graceful_interrupt(stop):
            if supervisor is None:
                return await crawler.run()
            supervisor.start()
            stale_after = config.runtime.workers.lease_ttl * 3

            def watchdog() -> None:
                supervisor.watchdog(store.workers, SystemClock().time(), stale_after)

            try:
                result = await crawler.run()
                await supervisor.wait(check=watchdog)
            except BaseException:
                supervisor.terminate()
                raise
        if not result.finalized and result.run_id:
            run = store.runs.get(result.run_id)
            if run is not None and run["status"] != "running" and run["stats"]:
                result = dataclasses.replace(result, status=run["status"], stats=run["stats"], finalized=True)
        return result


def print_summary(result: CrawlResult, fmt: str, out: TextIO) -> None:
    if fmt == "json":
        json.dump({"spider": result.spider, "run_id": result.run_id, "status": result.status, "mode": result.mode,
                   "resumed": result.resumed, "stats": result.stats, "hosts": result.hosts,
                   "worker": result.worker_id, "finalized": result.finalized},
                  out, indent=2, sort_keys=True)
        out.write("\n")
        return
    stats = result.stats
    if not stats:
        out.write(f"spider {result.spider}: worker {result.worker_id} found no running run\n")
        return
    items, documents = stats["items"], stats["documents"]
    resumed = " (resumed)" if result.resumed else ""
    lines = [
        f"spider {result.spider}: run {result.run_id} {result.status}{resumed}, {result.mode} mode, "
        f"{stats['duration']}s",
        f"  requests {stats['requests']}  responses {stats['responses']}  retries {stats['retries']}  "
        f"deferred {stats['deferred']}  host back-offs {stats['host_backoffs']}",
        f"  status   {' '.join(f'{k}={v}' for k, v in stats['status_counts'].items()) or '-'}",
        f"  errors   {' '.join(f'{k}={v}' for k, v in stats['errors'].items()) or '-'}",
        f"  incremental conditional={stats['conditional_requests']} not_modified={stats['not_modified']} "
        f"sitemap skipped={stats['sitemap_skipped']} promoted={stats['sitemap_promoted']}",
        f"  documents new={documents['new']} changed={documents['changed']} unchanged={documents['unchanged']}",
        f"  items    new={items['new']} updated={items['updated']} unchanged={items['unchanged']} "
        f"touched={items['touched']} dropped={items['dropped']}",
        f"  links    found={stats['links_found']} enqueued={stats['links_enqueued']}  "
        f"robots denied={stats['robots_denied']}",
        f"  bytes    {stats['bytes_received']} received, {stats['bytes_decoded']} decoded",
        f"  dedup    near-duplicates={stats['dedup']['near_duplicates']} "
        f"links filtered={stats['dedup']['links_filtered']}  warc records={stats['archive']['records']} "
        f"revisits={stats['archive']['revisits']}",
    ]
    if result.stats.get("workers"):
        lines.append(f"  workers  {' '.join(result.stats['workers'])}")
    out.write("\n".join(lines) + "\n")


def cmd_crawl(args: argparse.Namespace, out: TextIO) -> int:
    config = apply_overrides(load_config(args.config), args)
    supervisor = None
    if args.workers:
        passthrough = []
        for option in ("db", "checkpoint_dir", "metrics_file"):
            value = getattr(args, option)
            if value is not None:
                flag = "--" + option.replace("_", "-")
                passthrough += [flag, value if option != "metrics_file" else f"{value}.{{worker}}"]
        if args.warc is not None:
            passthrough += ["--warc", args.warc] if args.warc else ["--warc"]
        if args.lease_ttl is not None:
            passthrough += ["--lease-ttl", str(args.lease_ttl)]
        supervisor = Supervisor(Path(args.config), config.spiders[0].name, args.workers, extra_args=passthrough,
                                restart_limit=args.restart_workers)
    result = asyncio.run(run_crawl(config, resume=not args.fresh, max_runtime=args.max_runtime,
                                   supervisor=supervisor))
    print_summary(result, args.stats, out)
    return EXIT_OK if result.status in ("finished", "budget_exhausted", "stopped") else EXIT_FAILURE


def cmd_validate(args: argparse.Namespace, out: TextIO) -> int:
    config = load_config(args.config)
    out.write(f"{args.config}: OK (version {config.version}, database {config.database})\n")
    for notice in config.deprecations:
        out.write(f"  deprecated: {notice}\n")
    for spider in config.spiders:
        out.write(
            f"  spider {spider.name}: {len(spider.seeds)} seed(s), {len(spider.sitemaps)} sitemap(s), "
            f"{len(spider.plan.items)} item rule(s), {len(spider.plan.links)} link rule(s), "
            f"min_delay={spider.politeness.min_delay}s, concurrency={spider.concurrency}, "
            f"mode={spider.incremental.mode.value}\n"
        )
    return EXIT_OK


async def _fetch_page(config: ProjectConfig, url: str) -> tuple[str, str, int | None, str | None]:
    """Fetch ``url`` once; returns ``(final_url, html, status, charset)``."""
    async with HttpClient(config.http.client_config()) as client:
        response = await client.request("GET", url)
    return response.url, response.text, response.status, response.charset.encoding


def cmd_extract(args: argparse.Namespace, out: TextIO) -> int:
    config = load_config(args.config)
    spider = config.spider(args.spider)
    url = spider.canonicalizer.key(args.url)
    status: int | None = None
    charset: str | None = None
    if args.file is not None:
        with open(args.file, "rb") as handle:
            html, detected = decode_body(handle.read())
        charset = detected.encoding
    else:
        try:
            url, html, status, charset = asyncio.run(_fetch_page(config, url))
        except HttpError as exc:
            raise ValueError(f"cannot fetch {args.url}: {exc}") from exc
    document = parse_html(html)
    result = Extractor(spider.plan.items).extract(document, url)
    def item_report(i: object) -> dict[str, object]:
        report = {"type": i.item_type, "key": i.key, "data": i.data, "hash": i.content_hash()}
        if i.partial:
            report["partial"] = True
            report["authority"] = i.authority
            report["fields"] = list(i.field_names)
        return report

    report: dict[str, object] = {
        "url": url,
        "status": status,
        "charset": charset,
        "title": document.title,
        "fingerprint": spider.incremental.fingerprint.of_document(document),
        "items": [item_report(i) for i in result.items],
        "dropped": result.dropped,
        "errors": result.errors,
    }
    if not args.no_links:
        report["links"] = [link.url for link in spider.link_extractor().extract(document, url, 0)]
    json.dump(report, out, indent=2, ensure_ascii=False)
    out.write("\n")
    return EXIT_OK


async def _robots_report(settings: HttpSettings, user_agent: str, urls: list[str]) -> list[str]:
    lines: list[str] = []
    async with HttpClient(settings.client_config()) as client:
        cache = RobotsCache(client, client.clock, user_agent)
        described: set[str] = set()
        for url in urls:
            entry = await cache.get(url)
            if entry.origin not in described:
                described.add(entry.origin)
                group = entry.robots.group_for(user_agent)
                delay = "-" if group.crawl_delay is None else f"{group.crawl_delay:g}s"
                status = "-" if entry.status is None else str(entry.status)
                lines.append(f"{entry.origin}: robots.txt {entry.state} (HTTP {status}), "
                             f"group {','.join(group.agents)}, crawl-delay {delay}")
                lines.extend(f"  sitemap {sitemap}" for sitemap in entry.robots.sitemaps)
            decision = entry.robots.decide(url, user_agent)
            lines.append(f"  {'ALLOW' if decision.allowed else 'DENY '} {url}  ({decision.reason})")
    return lines


def cmd_robots(args: argparse.Namespace, out: TextIO) -> int:
    settings = load_config(args.config).http if args.config else HttpSettings(user_agent=DEFAULT_USER_AGENT)
    if args.user_agent:
        settings = dataclasses.replace(settings, user_agent=args.user_agent)
    for url in args.urls:
        parsed = parse_url(url)
        if parsed.scheme not in ("http", "https") or not parsed.host:
            raise ValueError(f"not an absolute http(s) URL: {url!r}")
    lines = asyncio.run(_robots_report(settings, settings.user_agent, args.urls))
    out.write("\n".join(lines) + "\n")
    return EXIT_OK


def cmd_plan(args: argparse.Namespace, out: TextIO) -> int:
    """Show what the next ``crawl`` would do, without fetching or changing anything."""
    config = load_config(args.config)
    spider = config.spider(args.spider)
    database = Path(args.db) if args.db else config.database
    if not database.exists():
        out.write(f"spider {spider.name}: no database yet; the first run will be a full crawl\n")
        return EXIT_OK
    roots = crawl_roots(tuple(spider.canonicalizer.key(u) for u in spider.seeds),
                        tuple(spider.canonicalizer.key(u) for u in spider.sitemaps))
    with Store(database) as store:
        clock = SystemClock()
        open_runs = [run["id"] for run in store.runs.open_runs(spider.name)]
        frontier = Frontier(store.db, spider.name, clock, max_depth=spider.max_depth)
        mode = RunMode(args.mode) if args.mode else spider.incremental.mode
        preview = preview_run(frontier, store, spider.name, roots, mode, clock.time())
    preview["resumable_runs"] = open_runs
    if args.json:
        json.dump(preview, out, indent=2, sort_keys=True)
        out.write("\n")
        return EXIT_OK
    if open_runs:
        out.write(f"spider {spider.name}: run {open_runs[-1]} is unfinished and will be resumed "
                  f"(use crawl --fresh to start over)\n")
    out.write(f"spider {spider.name}: next run is {preview['mode']}\n")
    out.write(f"  re-queued      {preview['requeued']} known URL(s) plus {len(roots)} root(s)\n")
    out.write(f"  still queued   {preview['already_pending']}\n")
    out.write(f"  deferred       {preview['deferred_waiting']} until {isoformat(preview['deferred_until']) or '-'}\n")  # type: ignore[arg-type]
    out.write(f"  orphans        {preview['orphans']} skipped\n")
    out.write(f"  next due       {isoformat(preview['next_due']) or '-'}\n")  # type: ignore[arg-type]
    return EXIT_OK
