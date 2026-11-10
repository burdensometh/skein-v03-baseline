"""``skein dedup``, ``skein metrics`` and ``skein db rekey``."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, TextIO

from skein.cli.data import _only_spider, add_database_source, database_of
from skein.clock import SystemClock
from skein.config import load_config
from skein.config.runtime import RuntimeSettings
from skein.dedup.bloomfile import BloomFileError, load
from skein.dedup.seen import SeenUrls
from skein.dedup.simindex import NearDuplicateIndex
from skein.metrics.collect import add_crawl_stats, add_database_state
from skein.checkpoint.snapshot import atomic_write
from skein.metrics.exposition import merge
from skein.metrics.prometheus import Registry
from skein.metrics.stats import CrawlStats
from skein.store.sqlite import Store

EXIT_OK = 0


def _runtime(args: argparse.Namespace) -> RuntimeSettings:
    return load_config(args.config).runtime if getattr(args, "config", None) else RuntimeSettings()


def _bloom_report(store: Store, spider: str, runtime: RuntimeSettings, rebuild: bool,
                  probes: list[str]) -> dict[str, Any]:
    path = runtime.bloom_path(store.path, spider)
    report: dict[str, Any] = {"spider": spider, "path": str(path) if path else None}
    frontier = int(store.db.execute("SELECT COUNT(*) FROM frontier WHERE spider = ?", (spider,)).fetchone()[0])
    report["frontier_urls"] = frontier
    if path is not None and path.exists():
        try:
            header, bloom = load(path)
            report.update(bits=header.bits, hashes=header.hashes, count=header.count, capacity=header.capacity,
                          error_rate=header.error_rate, fill=round(bloom.bits_set() / bloom.bits, 6),
                          estimated_false_positive_rate=bloom.false_positive_rate(),
                          file_bytes=path.stat().st_size)
        except BloomFileError as exc:
            report["error"] = str(exc)
    if rebuild or probes:
        dedup = runtime.dedup
        seen = SeenUrls(store.db, spider, path, capacity=dedup.bloom_capacity, error_rate=dedup.bloom_error_rate)
        if rebuild and path is not None:
            path.unlink(missing_ok=True)
        report["opened"] = seen.open()
        if rebuild:
            seen.save()
        report["probes"] = {url: url in seen.bloom for url in probes}
    return report


def cmd_dedup(args: argparse.Namespace, out: TextIO) -> int:
    runtime = _runtime(args)
    with Store(database_of(args)) as store:
        spider = _only_spider(store, args.spider)
        if args.action == "bloom":
            report = _bloom_report(store, spider, runtime, args.rebuild, args.check or [])
            if args.json:
                json.dump(report, out, indent=2, sort_keys=True)
                out.write("\n")
                return EXIT_OK
            for key, value in report.items():
                out.write(f"{key:<32} {value}\n")
            return EXIT_OK
        index = NearDuplicateIndex(store.db, max_distance=args.max_distance if args.max_distance is not None
                                   else runtime.dedup.max_distance)
        if args.rebuild:
            with store.transaction():
                linked = index.relink(spider)
            out.write(f"spider {spider}: {linked} near-duplicate page(s) within distance {index.max_distance}\n")
        clusters = index.clusters(spider)
        distances = {row[0]: row[1] for row in store.db.execute(
            "SELECT url, near_distance FROM documents WHERE spider = ? AND near_duplicate_of IS NOT NULL",
            (spider,))}
    if args.json:
        json.dump([{"urls": cluster, "distances": {u: distances.get(u) for u in cluster}} for cluster in clusters],
                  out, indent=2)
        out.write("\n")
        return EXIT_OK
    if not clusters:
        out.write(f"spider {spider}: no near-duplicate pages recorded\n")
    for number, cluster in enumerate(clusters, start=1):
        out.write(f"cluster {number}: {len(cluster)} page(s)\n")
        for url in cluster:
            distance = distances.get(url)
            out.write(f"  {url}{'' if distance is None else f'  (distance {distance})'}\n")
    return EXIT_OK


def database_metrics(store: Store) -> Registry:
    """Shared database state plus the statistics of open runs' finished workers."""
    registry = Registry()
    add_database_state(registry, store.db, SystemClock().time())
    for record in store.workers.list(states=("finished", "stopped", "dead")):
        if record.stats is None or record.run_id is None:
            continue
        run = store.runs.get(record.run_id)
        if run is None or run["status"] != "running":
            continue
        add_crawl_stats(registry, CrawlStats.from_state(record.stats), spider=record.spider, worker=record.id)
    return registry


def cmd_metrics(args: argparse.Namespace, out: TextIO) -> int:
    if args.merge:
        texts = [Path(path).read_text(encoding="utf-8") for path in args.merge]
        text = merge(texts, drop_labels=tuple(args.drop_label or ("worker",)))
    else:
        if args.db is None and args.config is None:
            raise ValueError("give --db or --config (or --merge FILE...)")
        with Store(database_of(args)) as store:
            text = database_metrics(store).render()
    if args.output:
        atomic_write(Path(args.output), text.encode("utf-8"))
        out.write(f"wrote {text.count('# TYPE')} metric famil(ies) to {args.output}\n")
    else:
        out.write(text)
    return EXIT_OK


def add_maintenance_parsers(commands: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    dedup = commands.add_parser("dedup", help="inspect the seen-URL Bloom filter or near-duplicate clusters")
    dedup.add_argument("action", choices=("bloom", "near"))
    add_database_source(dedup)
    dedup.add_argument("-s", "--spider", help="spider name (default: the only spider in the database)")
    dedup.add_argument("--rebuild", action="store_true",
                       help="bloom: rebuild the filter from the frontier; near: relink pages from stored SimHashes")
    dedup.add_argument("--max-distance", type=int, help="near: Hamming distance limit (0-3) for --rebuild")
    dedup.add_argument("--check", action="append", metavar="URL", help="bloom: test whether URL is in the filter")
    dedup.add_argument("--json", action="store_true")
    metrics = commands.add_parser("metrics", help="Prometheus exposition of the database's crawl state")
    source = metrics.add_mutually_exclusive_group()
    source.add_argument("--db", help="database file")
    source.add_argument("--config", help="take the database path from this configuration")
    source.add_argument("--merge", nargs="+", metavar="FILE", help="merge worker textfiles instead")
    metrics.add_argument("--drop-label", action="append", help="with --merge: label to sum over (default: worker)")
    metrics.add_argument("-o", "--output", help="write a textfile (atomically) instead of printing")
