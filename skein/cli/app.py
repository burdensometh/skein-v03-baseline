"""Argument parsing and dispatch for the ``skein`` command line."""

from __future__ import annotations

import argparse
import logging
import sys
import warnings
from typing import Sequence, TextIO

from skein import __version__
from skein.checkpoint.journal import JournalError
from skein.checkpoint.session import CheckpointError
from skein.checkpoint.snapshot import SnapshotError
from skein.cli.crawling import add_crawl_arguments, cmd_crawl, cmd_extract, cmd_plan, cmd_robots, cmd_validate
from skein.incremental.modes import RunMode
from skein.cli.data import (
    EXPORT_KINDS,
    add_database_source,
    cmd_db,
    cmd_export,
    cmd_frontier,
    cmd_history,
    cmd_checkpoint,
    cmd_runs,
    cmd_status,
    cmd_upgrade_config,
)
from skein.cli.archive import add_warc_parser, cmd_warc
from skein.cli.maintenance import add_maintenance_parsers, cmd_dedup, cmd_metrics
from skein.cli.workers import add_workers_parser, cmd_worker, cmd_workers
from skein.config import ConfigDeprecationWarning, ConfigError
from skein.crawler.engine import SpiderBusyError
from skein.store.rekey import CanonicalRulesError
from skein.warc.record import WarcFormatError
from skein.export.writers import FORMATS
from skein.store.database import StoreError

log = logging.getLogger("skein")

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="skein", description="Polite, incremental web harvesting.")
    parser.add_argument("--version", action="version", version=f"skein {__version__}")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="more logging (-vv for debug)")
    parser.add_argument("-q", "--quiet", action="store_true", help="only log errors")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    crawl = commands.add_parser("crawl", help="run (or resume) a spider defined in a configuration file")
    add_crawl_arguments(crawl)
    add_workers_parser(commands)

    export = commands.add_parser("export", help="export items, item versions, changes, fetch events or documents")
    add_database_source(export)
    export.add_argument("-s", "--spider", help="spider name (default: the only spider in the database)")
    export.add_argument("-f", "--format", choices=FORMATS, default="jsonl")
    export.add_argument("-o", "--output", help="output file (default: stdout)")
    export.add_argument("--what", choices=EXPORT_KINDS, default="items", help="what to export (default: items)")
    export.add_argument("-t", "--type", dest="item_type", help="only items of this type")
    export.add_argument("--since", help="only records seen/observed/fetched at or after this ISO 8601 time")
    export.add_argument("--changed-since", help="only items/documents whose content changed at or after this time")
    export.add_argument("--fields", help="comma-separated data columns for CSV")
    export.add_argument("--cursor", help="with --what changes: only changes after this named cursor, then advance it")
    export.add_argument("--as-of", help="with --what items: the item versions valid at this ISO 8601 time")

    validate = commands.add_parser("validate", help="check a configuration file and print its spiders")
    validate.add_argument("config")

    extract = commands.add_parser("extract", help="apply a spider's rules to one page and print the result")
    extract.add_argument("config")
    extract.add_argument("url", help="page URL (fetched unless --file is given)")
    extract.add_argument("-s", "--spider", help="spider name")
    extract.add_argument("--file", help="read HTML from this file instead of fetching")
    extract.add_argument("--no-links", action="store_true", help="omit discovered links from the output")

    robots = commands.add_parser("robots", help="show how robots.txt treats some URLs")
    robots.add_argument("urls", nargs="+", help="absolute URLs to check")
    robots.add_argument("--config", help="take user agent and HTTP settings from this configuration")
    robots.add_argument("--user-agent", help="user agent to evaluate (overrides the configuration)")

    runs = commands.add_parser("runs", help="list crawl runs recorded in a database")
    add_database_source(runs)
    runs.add_argument("-s", "--spider", help="only runs of this spider")
    runs.add_argument("--json", action="store_true", help="print full run records as JSON")
    runs.add_argument("--show", type=int, metavar="RUN_ID", help="details of one run: statuses, hosts, errors")
    runs.add_argument("--stop", type=int, metavar="RUN_ID",
                      help="stop a running run: every process working on it exits after its requests in flight")

    plan = commands.add_parser("plan", help="show what the next crawl would queue, without crawling")
    plan.add_argument("config")
    plan.add_argument("-s", "--spider", help="spider name")
    plan.add_argument("--db", help="override the database path from the config")
    plan.add_argument("--mode", choices=[m.value for m in RunMode], help="preview this run mode")
    plan.add_argument("--json", action="store_true")

    status = commands.add_parser("status", help="frontier, revisit schedule, watermarks and checkpoint per spider")
    add_database_source(status)
    status.add_argument("-s", "--spider", help="only this spider")
    status.add_argument("--checkpoint-dir", help="checkpoint directory (default: next to the database)")
    status.add_argument("--json", action="store_true")

    db = commands.add_parser("db", help="inspect, migrate, prune or rekey the database")
    db.add_argument("action", choices=("status", "migrate", "prune", "rekey", "merges"))
    add_database_source(db)
    db.add_argument("--no-backup", action="store_true", help="migrate without writing a backup copy first")
    db.add_argument("--dry-run", action="store_true",
                    help="migrate a copy in memory, or rekey inside a rolled-back transaction, and report")
    db.add_argument("--older-than", type=float, default=90.0, help="prune: age in days (default 90)")
    db.add_argument("-s", "--spider", help="prune/rekey: only this spider")
    db.add_argument("--vacuum", action="store_true", help="prune: reclaim disk space afterwards")
    db.add_argument("--check", action="store_true", help="status: also run SQLite integrity and foreign key checks")
    db.add_argument("--limit", type=int, default=1000, help="merges: at most this many renames")

    history = commands.add_parser("history", help="all versions of one item with field-level changes")
    add_database_source(history)
    history.add_argument("item_type", help="item type (rule name)")
    history.add_argument("key", help="item key")
    history.add_argument("-s", "--spider", help="spider name (default: the only spider in the database)")
    history.add_argument("--json", action="store_true")

    frontier = commands.add_parser("frontier", help="list queued URLs or queue failed/deferred ones again")
    frontier.add_argument("action", choices=("list", "release"))
    add_database_source(frontier)
    frontier.add_argument("-s", "--spider", help="spider name (default: the only spider in the database)")
    frontier.add_argument("--state", action="append",
                          choices=("pending", "active", "done", "failed", "deferred"),
                          help="filter by state (repeatable); release defaults to failed")
    frontier.add_argument("--host", help="only this origin, e.g. https://shop.example:443")
    frontier.add_argument("--limit", type=int, default=100)
    frontier.add_argument("--json", action="store_true")
    frontier.add_argument("--leased-by", metavar="WORKER",
                          help="release: take back every request WORKER leases and mark it dead")

    checkpoint = commands.add_parser("checkpoint", help="verify, dump or discard a spider's crash-recovery state")
    checkpoint.add_argument("action", choices=("verify", "dump", "discard"))
    add_database_source(checkpoint)
    checkpoint.add_argument("-s", "--spider", help="spider name (default: the only spider in the database)")
    checkpoint.add_argument("--checkpoint-dir", help="checkpoint directory (default: next to the database)")
    checkpoint.add_argument("--worker", help="act on this worker's checkpoint instead of the local crawler's")

    add_warc_parser(commands)
    add_maintenance_parsers(commands)

    upgrade = commands.add_parser("upgrade-config", help="rewrite a version-1 configuration as version 2")
    upgrade.add_argument("config")
    upgrade.add_argument("-o", "--output", help="write here instead of stdout")
    return parser


def configure_logging(verbose: int, quiet: bool) -> None:
    level = logging.ERROR if quiet else (logging.WARNING, logging.INFO, logging.DEBUG)[min(verbose, 2)]
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", stream=sys.stderr)
    logging.captureWarnings(True)


def main(argv: Sequence[str] | None = None, out: TextIO | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose, args.quiet)
    stream = out if out is not None else sys.stdout
    handlers = {
        "crawl": cmd_crawl,
        "export": cmd_export,
        "validate": cmd_validate,
        "extract": cmd_extract,
        "robots": cmd_robots,
        "runs": cmd_runs,
        "status": cmd_status,
        "db": cmd_db,
        "history": cmd_history,
        "frontier": cmd_frontier,
        "upgrade-config": cmd_upgrade_config,
        "checkpoint": cmd_checkpoint,
        "plan": cmd_plan,
        "worker": cmd_worker,
        "workers": cmd_workers,
        "warc": cmd_warc,
        "dedup": cmd_dedup,
        "metrics": cmd_metrics,
    }
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("default" if not args.quiet else "ignore", ConfigDeprecationWarning)
            return handlers[args.command](args, stream)
    except ConfigError as exc:
        print(f"skein: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (KeyError, ValueError, StoreError, CheckpointError, SnapshotError, JournalError, CanonicalRulesError,
            SpiderBusyError, WarcFormatError) as exc:
        message = exc.args[0] if isinstance(exc, KeyError) and exc.args else exc
        print(f"skein: {message}", file=sys.stderr)
        return EXIT_FAILURE
