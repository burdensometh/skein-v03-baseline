"""Commands that read or maintain a database: ``export``, ``runs``, ``status`` and ``db``."""

from __future__ import annotations

import argparse
import contextlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO

from skein.checkpoint.journal import JournalError
from skein.checkpoint.session import Checkpoint, CheckpointError
from skein.checkpoint.snapshot import SnapshotError, read_snapshot
from skein.clock import SystemClock, isoformat
from skein.config import load_config, parse_config
from skein.config.compat import upgraded_document
from skein.export.writers import (
    DOCUMENT_COLUMNS,
    EVENT_COLUMNS,
    ITEM_COLUMNS,
    VERSION_COLUMNS,
    document_record,
    event_record,
    item_record,
    version_record,
    write_csv,
    write_jsonl,
)
from skein.frontier.sqlite import Frontier
from skein.store.database import StoreError
from skein.store.history import change_feed, item_history
from skein.store.migrations import dry_run, inspect_database
from skein.store.rekey import rekey_spider
from skein.store.retention import prune_events
from skein.store.schema import MIGRATIONS
from skein.store.sqlite import Store

EXIT_OK = 0
EXPORT_KINDS = ("items", "versions", "changes", "events", "documents")
CHANGE_COLUMNS = ("_type", "_key", "_version", "_change", "_observed_at", "_url", "_fields")


def add_database_source(parser: argparse.ArgumentParser) -> None:
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--db", help="database file")
    source.add_argument("--config", help="take the database path from this configuration")


def database_of(args: argparse.Namespace, *, must_exist: bool = True) -> Path:
    database = Path(args.db) if args.db is not None else load_config(args.config).database
    if must_exist and not database.exists():
        raise StoreError(f"database {database} does not exist")
    return database


def parse_since(text: str | None, option: str = "--since") -> float | None:
    if text is None:
        return None
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{option}: not an ISO 8601 timestamp: {text!r}") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def _only_spider(store: Store, requested: str | None) -> str:
    if requested is not None:
        return requested
    known = store.spiders()
    if len(known) != 1:
        raise ValueError(f"database holds {len(known)} spiders ({', '.join(known) or 'none'}); use --spider")
    return known[0]


def cmd_export(args: argparse.Namespace, out: TextIO) -> int:
    database = database_of(args)
    since = parse_since(args.since)
    changed_since = parse_since(args.changed_since, "--changed-since")
    if args.cursor and args.what != "changes":
        raise ValueError("--cursor only applies to --what changes")
    positions: list[int] = []
    with Store(database) as store:
        spider = _only_spider(store, args.spider)
        records: Iterable[dict[str, Any]]
        if args.as_of is not None and args.what != "items":
            raise ValueError("--as-of only applies to --what items")
        if args.what == "items" and args.as_of is not None:
            moment = parse_since(args.as_of, "--as-of")
            assert moment is not None
            records = (version_record(v) for v in store.items.as_of(spider, moment, item_type=args.item_type))
            leading = VERSION_COLUMNS
        elif args.what == "items":
            records = (item_record(i) for i in store.items.iter(
                spider, item_type=args.item_type, since=since, changed_since=changed_since))
            leading = ITEM_COLUMNS
        elif args.what == "versions":
            records = (version_record(v) for v in store.items.versions(spider, item_type=args.item_type, since=since))
            leading = VERSION_COLUMNS
        elif args.what == "changes":
            after = store.cursors.get(args.cursor, spider) if args.cursor else None
            records = change_records(store, spider, since, args.item_type, after, positions)
            leading = CHANGE_COLUMNS
        elif args.what == "events":
            records = (event_record(e) for e in store.documents.events(spider, since=since))
            leading = EVENT_COLUMNS
        else:
            records = (document_record(d) for d in store.documents.iter_documents(spider, changed_since=changed_since))
            leading = DOCUMENT_COLUMNS
        with contextlib.ExitStack() as stack:
            target = out
            if args.output is not None:
                target = stack.enter_context(open(args.output, "w", encoding="utf-8", newline=""))
            if args.format == "jsonl":
                write_jsonl(records, target)
            else:
                fields = [f.strip() for f in args.fields.split(",")] if args.fields else None
                write_csv(records, target, leading=leading, fields=fields)
        if args.cursor and positions:
            # Only advanced once every record was written out.
            store.cursors.advance(args.cursor, spider, max(positions), SystemClock().time())
    return EXIT_OK


def _run_details(store: Store, run_id: int, out: TextIO) -> None:
    run = store.runs.get(run_id)
    if run is None:
        raise ValueError(f"no run with id {run_id}")
    stats = run["stats"] or {}
    out.write(f"run {run['id']} of spider {run['spider']}: {run['status']}, {run['mode']} mode, "
              f"{run['resumes']} resume(s)\n")
    out.write(f"  started {isoformat(run['started_at'])}, finished {isoformat(run['finished_at']) or '-'}\n")
    rows = store.db.execute(
        """SELECT COALESCE(CAST(status AS TEXT), '-') AS s, COALESCE(error_class, '-') AS c, COUNT(*) AS n,
                  SUM(conditional) AS cond, SUM(changed) AS changed
           FROM fetch_events WHERE run_id = ? GROUP BY s, c ORDER BY n DESC""",
        (run_id,),
    ).fetchall()
    for row in rows:
        out.write(f"  status {row['s']:>4} {row['c']:<10} {row['n']:>6} fetch(es), {row['cond'] or 0} conditional, "
                  f"{row['changed'] or 0} changed\n")
    for host, counters in sorted(stats.get("per_host", {}).items()):
        detail = " ".join(f"{k}={v}" for k, v in sorted(counters.items()))
        out.write(f"  host {host} {detail}\n")
    for error in store.db.execute(
        """SELECT error, COUNT(*) AS n FROM fetch_events WHERE run_id = ? AND error IS NOT NULL
           GROUP BY error ORDER BY n DESC, error LIMIT 5""",
        (run_id,),
    ):
        out.write(f"  error  {error['n']:>4}x {error['error']}\n")
    fetched = dict(store.db.execute(
        "SELECT worker_id, COUNT(*) FROM fetch_events WHERE run_id = ? GROUP BY worker_id", (run_id,)).fetchall())
    for record in store.workers.list(run_id=run_id):
        pages = (record.stats or {}).get("pages_stored", "-")
        out.write(f"  worker {record.id} ({record.role}, {record.state}, pid {record.pid} on {record.hostname}): "
                  f"{fetched.get(record.id, 0)} fetch event(s), {pages} page(s) in its statistics\n")


def cmd_runs(args: argparse.Namespace, out: TextIO) -> int:
    with Store(database_of(args)) as store:
        if args.stop is not None:
            with store.transaction():
                cursor = store.db.execute(
                    "UPDATE runs SET status = 'stopped', finished_at = ? WHERE id = ? AND status = 'running'",
                    (SystemClock().time(), args.stop))
            if cursor.rowcount != 1:
                raise ValueError(f"run {args.stop} is not running")
            out.write(f"run {args.stop} stopped; its workers finish their requests in flight and exit\n")
            return EXIT_OK
        if args.show is not None:
            _run_details(store, args.show, out)
            return EXIT_OK
        runs = store.runs.list(args.spider)
    if args.json:
        json.dump(runs, out, indent=2, sort_keys=True)
        out.write("\n")
        return EXIT_OK
    out.write(f"{'id':>4}  {'spider':<16} {'status':<17} {'mode':<12} {'started':<20} {'secs':>8} "
              f"{'reqs':>6} {'new':>5} {'upd':>5} {'304':>5}\n")
    for run in runs:
        stats = run["stats"] or {}
        items = stats.get("items", {})
        duration = (run["finished_at"] - run["started_at"]) if run["finished_at"] else None
        out.write(
            f"{run['id']:>4}  {run['spider']:<16} {run['status']:<17} {run['mode']:<12} "
            f"{isoformat(run['started_at']):<20} {'-' if duration is None else f'{duration:.1f}':>8} "
            f"{stats.get('requests', '-'):>6} {items.get('new', '-'):>5} {items.get('updated', '-'):>5} "
            f"{stats.get('not_modified', '-'):>5}\n"
        )
    return EXIT_OK


def _checkpoint_report(directory: Path) -> dict[str, Any]:
    checkpoint = Checkpoint(directory, SystemClock())
    if not checkpoint.exists():
        return {"present": False}
    try:
        state = checkpoint.load(repair=False)
    except (CheckpointError, SnapshotError, JournalError) as exc:
        return {"present": True, "error": str(exc)}
    assert state is not None and checkpoint.recovery is not None
    return {"present": True, "run_id": state.run_id, "mode": state.mode, "seq": state.seq,
            "worker": state.worker_id, "requests": state.stats.requests, **checkpoint.recovery.to_dict()}


def spider_status(store: Store, spider: str, checkpoint_root: Path, now: float) -> dict[str, Any]:
    db = store.db
    rules = store.canonical.get(spider)
    states = {row[0]: row[1] for row in db.execute(
        "SELECT state, COUNT(*) FROM frontier WHERE spider = ? GROUP BY state", (spider,))}
    documents = db.execute(
        """SELECT COUNT(*), SUM(CASE WHEN next_visit <= ? THEN 1 ELSE 0 END), MIN(next_visit),
                  SUM(change_count), SUM(check_count)
           FROM documents WHERE spider = ?""",
        (now, spider),
    ).fetchone()
    return {
        "spider": spider,
        "statuses": store.documents.status_counts(spider),
        "frontier": states,
        "documents": {"total": documents[0], "due": documents[1] or 0, "next_visit": isoformat(documents[2]),
                      "changes": documents[3] or 0, "checks": documents[4] or 0},
        "items": {"current": store.items.count(spider), "versions": store.items.count_versions(spider)},
        "orphans": len(store.links.orphans(spider)),
        "watermarks": [
            {"seed": m.seed, "kind": m.kind, "last_run_id": m.last_run_id,
             "last_completed": isoformat(m.last_completed), "lastmod_high": isoformat(m.lastmod_high)}
            for m in store.watermarks.all(spider)
        ],
        "open_runs": [run["id"] for run in store.runs.open_runs(spider)],
        "checkpoint": _checkpoint_report(checkpoint_root / spider),
        "worker_checkpoints": sorted(path.name for path in (checkpoint_root / spider / "workers").glob("*")
                                     if path.is_dir()),
        "workers": {state: count for state, count in db.execute(
            "SELECT state, COUNT(*) FROM workers WHERE spider = ? GROUP BY state", (spider,))},
        "leases": {owner: count for owner, count in db.execute(
            "SELECT lease_owner, COUNT(*) FROM frontier WHERE spider = ? AND state = 'active' GROUP BY lease_owner",
            (spider,))},
        "near_duplicates": int(db.execute(
            "SELECT COUNT(*) FROM documents WHERE spider = ? AND near_duplicate_of IS NOT NULL",
            (spider,)).fetchone()[0]),
        "canonical": {"version": rules.version, "signature": rules.signature} if rules is not None else None,
    }


def cmd_status(args: argparse.Namespace, out: TextIO) -> int:
    database = database_of(args)
    root = Path(args.checkpoint_dir) if args.checkpoint_dir else (
        load_config(args.config).checkpoint_root() if args.config else Path(f"{database}.checkpoints"))
    with Store(database) as store:
        spiders = [args.spider] if args.spider else store.spiders()
        now = SystemClock().time()
        reports = [spider_status(store, name, root, now) for name in spiders]
        robots = store.robots.origins()
        hosts = [dict(row) for row in store.db.execute(
            """SELECT h.*, (SELECT COUNT(*) FROM host_slots s WHERE s.host_key = h.host_key AND s.owner IS NOT NULL
                             AND s.lease_expires > ?) AS busy
               FROM host_state h ORDER BY h.host_key""", (now,))]
    if args.json:
        json.dump({"spiders": reports, "robots": robots, "hosts": hosts}, out, indent=2, sort_keys=True)
        out.write("\n")
        return EXIT_OK
    for report in reports:
        docs, items, cp = report["documents"], report["items"], report["checkpoint"]
        frontier = " ".join(f"{k}={v}" for k, v in sorted(report["frontier"].items())) or "-"
        out.write(f"spider {report['spider']}\n")
        out.write(f"  frontier   {frontier}\n")
        statuses = " ".join(f"{k}={v}" for k, v in report["statuses"].items()) or "-"
        out.write(f"  fetches    {statuses}\n")
        out.write(f"  documents  {docs['total']} known, {docs['due']} due, next visit {docs['next_visit'] or '-'}, "
                  f"{docs['changes']} change(s) in {docs['checks']} check(s)\n")
        out.write(f"  items      {items['current']} current, {items['versions']} version(s)\n")
        out.write(f"  orphans    {report['orphans']} URL(s) no longer linked from anywhere\n")
        workers = " ".join(f"{k}={v}" for k, v in sorted(report["workers"].items())) or "-"
        leases = " ".join(f"{k}={v}" for k, v in sorted(report["leases"].items(), key=str)) or "-"
        out.write(f"  workers    {workers}; leases {leases}\n")
        out.write(f"  near-dups  {report['near_duplicates']} document(s)\n")
        for mark in report["watermarks"]:
            out.write(f"  watermark  {mark['seed']} completed {mark['last_completed'] or '-'} "
                      f"lastmod {mark['lastmod_high'] or '-'}\n")
        if cp.get("present"):
            detail = cp.get("error") or (f"run {cp['run_id']} at seq {cp['seq']}, {cp['replayed']} journal "
                                         f"record(s) after snapshot, {cp['torn_bytes']} torn byte(s)")
            out.write(f"  checkpoint {detail}\n")
        else:
            out.write("  checkpoint none\n")
        if report["worker_checkpoints"]:
            out.write(f"  worker checkpoints {' '.join(report['worker_checkpoints'])}\n")
    for host in hosts:
        pause = max(0.0, host["backoff_until"] - now)
        out.write(f"host {host['host_key']}: {host['requests']} request(s), crawl-delay "
                  f"{host['crawl_delay'] if host['crawl_delay'] is not None else '-'}, {host['busy']} slot(s) busy, "
                  f"failure streak {host['failures']}, back-off {pause:.0f}s"
                  f"{', quarantined' if host['quarantine_until'] > now else ''}\n")
    for entry in robots:
        validator = "etag" if entry["etag"] else ("last-modified" if entry["last_modified"] else "no validators")
        out.write(f"robots.txt {entry['origin']}: {entry['state']} (HTTP {entry['status'] or '-'}), "
                  f"expires {isoformat(entry['expires_at'])}, {validator}\n")
    return EXIT_OK


def cmd_db(args: argparse.Namespace, out: TextIO) -> int:
    if args.action == "rekey":
        return cmd_rekey(args, out)
    if args.action == "merges":
        return _merges(args, out)
    database = database_of(args, must_exist=args.action != "migrate" or args.dry_run)
    if args.action == "migrate" and args.dry_run:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
            applied, tables = dry_run(db, MIGRATIONS)
        out.write(f"{database}: {len(applied)} migration(s) would be applied\n")
        for migration in applied:
            out.write(f"  {migration.version} {migration.name}: {migration.description}\n")
        for table, count in tables.items():
            out.write(f"  table {table:<20} {count:>8} row(s) afterwards\n")
        return EXIT_OK
    if args.action == "prune":
        cutoff = SystemClock().time() - args.older_than * 86400.0
        with Store(database) as store:
            report = prune_events(store.db, args.spider, cutoff, vacuum=args.vacuum)
        out.write(f"{database}: deleted {report.events_deleted} fetch event(s), {report.events_kept} kept"
                  f"{', vacuumed' if report.vacuumed else ''}\n")
        return EXIT_OK
    if args.action == "migrate":
        before = inspect_database(database, MIGRATIONS).version if database.exists() else 0
        with Store(database, backup=not args.no_backup) as store:
            applied = store.applied
            backup = store.backup_path
        out.write(f"{database}: schema version {before} -> {before + len(applied)}\n")
        for migration in applied:
            out.write(f"  applied {migration.version} {migration.name}: {migration.description}\n")
        if backup is not None:
            out.write(f"  backup written to {backup}\n")
        return EXIT_OK
    status = inspect_database(database, MIGRATIONS)
    out.write(f"{status.path}: schema version {status.version} (latest {status.latest})\n")
    if args.check:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
            problems = [row[0] for row in db.execute("PRAGMA integrity_check") if row[0] != "ok"]
            problems += [f"foreign key violation in {row[0]} (row {row[1]})"
                         for row in db.execute("PRAGMA foreign_key_check")]
        for problem in problems:
            out.write(f"  problem: {problem}\n")
        if problems:
            raise StoreError(f"{database}: {len(problems)} integrity problem(s)")
        out.write("  integrity: ok\n")
    for entry in status.history:
        when = isoformat(entry.applied_at) or "before migration tracking"
        out.write(f"  {entry.version} {entry.name:<16} {when}\n")
    for migration in status.pending:
        out.write(f"  pending {migration.version} {migration.name}: {migration.description}\n")
    for table, count in status.tables.items():
        out.write(f"  table {table:<20} {count:>8} row(s)\n")
    return EXIT_OK



def cmd_rekey(args: argparse.Namespace, out: TextIO) -> int:
    """Rewrite stored URL keys with the configured canonicalisation rules (``db rekey``)."""
    if not args.config:
        raise ValueError("db rekey needs --config (the canonicalisation settings come from the spiders)")
    config = load_config(args.config)
    database = Path(args.db) if args.db else config.database
    spiders = [config.spider(args.spider)] if args.spider else list(config.spiders)
    with Store(database) as store:
        for spider in spiders:
            store.canonical.needs_rekey(spider.name, spider.canonicalizer)
            store.db.execute("BEGIN IMMEDIATE")
            try:
                report = rekey_spider(store.db, spider.name, spider.canonicalizer, SystemClock().time())
            finally:
                store.db.execute("ROLLBACK" if args.dry_run else "COMMIT")
            verb = "would rename" if args.dry_run else "renamed"
            out.write(f"spider {spider.name}: {verb} {report.renamed} of {report.examined} URL(s); merged "
                      f"{report.frontier_merged} frontier row(s), {report.documents_merged} document(s), "
                      f"{report.links_merged} link(s); rewrote {report.events_rewritten} fetch event(s) and "
                      f"{report.items_rewritten} item version(s) [{report.signature}]\n")
    return EXIT_OK


def _merges(args: argparse.Namespace, out: TextIO) -> int:
    """The URL renames (and merges) canonicalisation rule changes caused."""
    with Store(database_of(args)) as store:
        spider = _only_spider(store, args.spider)
        rows = store.canonical.merges(spider, limit=args.limit)
        rules = store.canonical.get(spider)
    out.write(f"spider {spider}: keys from canonical rules "
              f"{rules.version if rules else '-'} ({rules.signature if rules else 'unrecorded'})\n")
    for row in rows:
        marker = "merged " if row["collided"] else "renamed"
        out.write(f"  {marker} {row['old_url']} -> {row['new_url']}  ({isoformat(row['merged_at'])})\n")
    return EXIT_OK


def cmd_history(args: argparse.Namespace, out: TextIO) -> int:
    with Store(database_of(args)) as store:
        spider = _only_spider(store, args.spider)
        changes = item_history(store.items, spider, args.item_type, args.key)
    if not changes:
        raise ValueError(f"no item {args.item_type}/{args.key} in spider {spider}")
    if args.json:
        json.dump([{"version": c.version, "kind": c.kind.value, "observed_at": isoformat(c.observed_at),
                    "url": c.url, "run_id": c.run_id, "fields": [f.to_dict() for f in c.fields]} for c in changes],
                  out, indent=2, ensure_ascii=False)
        out.write("\n")
        return EXIT_OK
    for change in changes:
        out.write(f"v{change.version} {change.kind.value:<8} {isoformat(change.observed_at)} run {change.run_id} "
                  f"{change.url}\n")
        for diff in change.fields:
            if diff.kind == "changed":
                out.write(f"    {diff.path}: {json.dumps(diff.before, ensure_ascii=False)} -> "
                          f"{json.dumps(diff.after, ensure_ascii=False)}\n")
            else:
                value = diff.after if diff.kind == "added" else diff.before
                out.write(f"    {diff.path} {diff.kind}: {json.dumps(value, ensure_ascii=False)}\n")
    return EXIT_OK


def change_records(
    store: Store, spider: str, since: float | None, item_type: str | None, after: int | None = None,
    positions: list[int] | None = None,
) -> Iterable[dict[str, Any]]:
    for change in change_feed(store.items, spider, since=since, item_type=item_type, after=after):
        if positions is not None:
            positions.append(change.position)
        yield {
            "_type": change.item_type, "_key": change.key, "_version": change.version, "_change": change.kind.value,
            "_observed_at": isoformat(change.observed_at), "_url": change.url,
            "_fields": sorted({diff.path.split(".")[0] for diff in change.fields}),
            "data": None if change.kind.value == "deleted" else change.data,
        }


def cmd_frontier(args: argparse.Namespace, out: TextIO) -> int:
    states = tuple(args.state or ())
    with Store(database_of(args)) as store:
        spider = _only_spider(store, args.spider)
        frontier = Frontier(store.db, spider, SystemClock())
        if args.action == "release" and args.leased_by:
            frontier.owner = "cli"
            report = frontier.reclaim(dead_owners=[args.leased_by])
            with store.transaction():
                store.workers.finish(args.leased_by, "dead", SystemClock().time(), None)
            out.write(f"{len(report.requeued)} request(s) leased by {args.leased_by} queued again, "
                      f"{len(report.poisoned)} failed after too many lost leases\n")
            return EXIT_OK
        if args.action == "release":
            released = frontier.release(states or ("failed",), host=args.host)
            out.write(f"{released} request(s) of spider {spider} queued again\n")
            return EXIT_OK
        rows = frontier.listing(states=states, host=args.host, limit=args.limit)
    if args.json:
        json.dump(rows, out, indent=2)
        out.write("\n")
        return EXIT_OK
    for row in rows:
        wait = f" not before {isoformat(float(row['not_before']))}" if row["not_before"] else ""  # type: ignore[arg-type]
        lease = (f" leased by {row['lease_owner']} until {isoformat(float(row['lease_expires']))}"  # type: ignore[arg-type]
                 if row["lease_owner"] and row["lease_expires"] else "")
        out.write(f"{row['state']:<9} {row['kind']:<8} d={row['depth']} p={row['priority']} "
                  f"a={row['attempts']} {row['url']}{wait}{lease}\n")
    return EXIT_OK


def cmd_upgrade_config(args: argparse.Namespace, out: TextIO) -> int:
    source = Path(args.config)
    try:
        data = json.loads(source.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"cannot read {source}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"{source} does not hold a JSON object")
    upgraded, notices = upgraded_document(data)
    parse_config(upgraded, base_dir=source.resolve().parent, source=str(source))
    text = json.dumps(upgraded, indent=2, ensure_ascii=False) + "\n"
    if args.output is not None:
        Path(args.output).write_text(text, encoding="utf-8")
        out.write(f"wrote {args.output} ({len(notices)} change(s))\n")
    else:
        out.write(text)
    return EXIT_OK


def _checkpoint_for(args: argparse.Namespace, store: Store) -> tuple[str, Checkpoint]:
    """The local crawler's checkpoint of the spider, or with ``--worker`` that worker's."""
    spider = _only_spider(store, args.spider)
    if args.checkpoint_dir:
        root = Path(args.checkpoint_dir)
    elif args.config:
        root = load_config(args.config).checkpoint_root()
    else:
        root = Path(f"{store.path}.checkpoints")
    if args.worker:
        record = store.workers.get(args.worker)
        if record is not None and record.checkpoint:
            return spider, Checkpoint(Path(record.checkpoint), SystemClock())
        return spider, Checkpoint(root / spider / "workers" / args.worker, SystemClock())
    return spider, Checkpoint(root / spider, SystemClock())


def cmd_checkpoint(args: argparse.Namespace, out: TextIO) -> int:
    with Store(database_of(args)) as store:
        spider, checkpoint = _checkpoint_for(args, store)
        if args.action == "discard" and args.worker:
            checkpoint.discard(remove_directory=True)
            out.write(f"checkpoint of worker {args.worker} discarded\n")
            return EXIT_OK
        if args.action == "discard":
            for run in store.runs.open_runs(spider):
                store.runs.finish(run["id"], run["finished_at"] or SystemClock().time(), "abandoned", None)
            checkpoint.discard()
            out.write(f"checkpoint of spider {spider} discarded; open runs abandoned\n")
            return EXIT_OK
        if not checkpoint.exists():
            out.write(f"spider {spider}: no checkpoint\n")
            return EXIT_OK
        if args.action == "verify":
            replay = checkpoint.journal.replay(repair=False)
            for segment in replay.segments:
                status = segment.reason or "ok"
                out.write(f"{segment.path.name}: {segment.records} record(s), {segment.valid_bytes} valid byte(s), "
                          f"{segment.torn_bytes} torn byte(s) ({status})\n")
            state = checkpoint.load(confirm=store.documents.journaled, repair=False)
            assert state is not None and checkpoint.recovery is not None
            out.write(f"snapshot at seq {checkpoint.recovery.snapshot_seq}; state after replay at seq {state.seq}, "
                      f"{checkpoint.recovery.unconfirmed} unconfirmed record(s)\n")
            return EXIT_OK
        snapshot_seq = int((read_snapshot(checkpoint.snapshot_path) or {}).get("seq", 0))
        for record in checkpoint.journal.replay(after_seq=snapshot_seq, repair=False).records:
            out.write(json.dumps(record, sort_keys=True) + "\n")
    return EXIT_OK
