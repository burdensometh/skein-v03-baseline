"""Recomputing canonical URL keys and merging the rows that collapse onto one key.

When canonicalisation rules change (rules version 1 -> 2, or a spider's
query settings change), URLs stored under their old keys must move to the
new ones. Several old keys can map to the same new key -- ``/p?utm_source=a``
and ``/p`` both become ``/p`` -- and the rows of such a group are merged:

* **fetch history** (``fetch_events``) is never merged or dropped: every
  attempt keeps its row, its ``url`` is rewritten to the new key, so the
  merged document's history is the union of the members' histories.
* **frontier**: one row survives. Outstanding work wins over handled work
  (``pending``/``active`` before ``done``, ``deferred``, ``failed``); the
  smallest depth, highest priority and urgency, earliest discovery and the
  referrer and seed of the first-discovered member are kept; leases are
  dropped (``active`` becomes ``pending``).
* **documents**: the member with the most recent successful fetch provides
  the content state (status, validators, fingerprint, payload digest);
  fetch/check/change counters and the observed span are summed so the
  revisit estimate keeps all evidence; the earliest ``next_visit`` and
  shortest interval win.
* **items** are identified by their item key, not by URL, so they never
  collide; their ``url`` column is rewritten.
* **links** with the same (source, target) after rewriting are merged:
  earliest first sighting, latest last sighting, live if any member is.
* **watermarks** of merged seeds keep the latest start and, conservatively,
  the oldest completion (none if any member never completed).
* **SimHashes**: the most recently computed value is kept and its bands are
  rebuilt.

Every renamed URL is logged in ``canonical_merges``; ``canonical_rules``
records which rules (version and settings signature) produced each
spider's keys.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterable

from skein.dedup.simhash import band_values, to_unsigned
from skein.store.database import execute_script
from skein.urls.canonical import Canonicalizer
from skein.urls.parse import URLError

log = logging.getLogger(__name__)

RULES_TABLES = """
CREATE TABLE canonical_rules (
    spider     TEXT    PRIMARY KEY,
    version    INTEGER NOT NULL,
    signature  TEXT    NOT NULL,
    updated_at REAL    NOT NULL
);
CREATE TABLE canonical_merges (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    spider    TEXT    NOT NULL,
    old_url   TEXT    NOT NULL,
    new_url   TEXT    NOT NULL,
    collided  INTEGER NOT NULL,
    signature TEXT    NOT NULL,
    merged_at REAL    NOT NULL
);
CREATE INDEX canonical_merges_by_new ON canonical_merges(spider, new_url)
"""

_URL_SOURCES = (
    "SELECT url FROM frontier WHERE spider = :spider",
    "SELECT url FROM documents WHERE spider = :spider",
    "SELECT DISTINCT url FROM fetch_events WHERE spider = :spider",
    "SELECT DISTINCT url FROM item_history WHERE spider = :spider",
    "SELECT DISTINCT source FROM links WHERE spider = :spider",
    "SELECT DISTINCT target FROM links WHERE spider = :spider",
    "SELECT seed FROM watermarks WHERE spider = :spider",
    "SELECT url FROM simhashes WHERE spider = :spider",
)
_STATE_RANK = {"active": 0, "pending": 0, "done": 1, "deferred": 2, "failed": 3}


class CanonicalRulesError(RuntimeError):
    """Stored keys were produced by newer rules than the configuration asks for."""


@dataclass
class RekeyReport:
    """What one spider's rekeying changed."""

    spider: str
    signature: str
    examined: int = 0
    renamed: int = 0
    unparsable: int = 0
    frontier_merged: int = 0
    documents_merged: int = 0
    links_merged: int = 0
    watermarks_merged: int = 0
    events_rewritten: int = 0
    items_rewritten: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StoredRules:
    version: int
    signature: str
    updated_at: float


class CanonicalStore:
    """Repository for ``canonical_rules`` and ``canonical_merges``."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def get(self, spider: str) -> StoredRules | None:
        row = self.db.execute(
            "SELECT version, signature, updated_at FROM canonical_rules WHERE spider = ?", (spider,)
        ).fetchone()
        return StoredRules(int(row[0]), str(row[1]), float(row[2])) if row is not None else None

    def record(self, spider: str, canonicalizer: Canonicalizer, now: float) -> None:
        self.db.execute(
            """INSERT INTO canonical_rules (spider, version, signature, updated_at) VALUES (?, ?, ?, ?)
               ON CONFLICT (spider) DO UPDATE SET version = excluded.version, signature = excluded.signature,
                   updated_at = excluded.updated_at""",
            (spider, canonicalizer.rules, canonicalizer.signature(), now),
        )

    def merges(self, spider: str, *, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self.db.execute(
            """SELECT old_url, new_url, collided, signature, merged_at FROM canonical_merges
               WHERE spider = ? ORDER BY id LIMIT ?""",
            (spider, limit),
        )
        return [dict(row) for row in rows]

    def needs_rekey(self, spider: str, canonicalizer: Canonicalizer) -> bool:
        """Whether stored keys were produced by other settings; raises if they need newer rules than these."""
        stored = self.get(spider)
        if stored is None or stored.signature == canonicalizer.signature():
            return False
        if canonicalizer.rules < stored.version:
            raise CanonicalRulesError(
                f"spider {spider}: stored URL keys use canonical rules version {stored.version}; "
                f"the configuration asks for version {canonicalizer.rules}, which cannot restore them"
            )
        return True


def known_spiders(db: sqlite3.Connection) -> list[str]:
    rows = db.execute(
        """SELECT spider FROM frontier UNION SELECT spider FROM documents UNION SELECT spider FROM fetch_events
           UNION SELECT spider FROM item_history UNION SELECT spider FROM runs ORDER BY 1"""
    )
    return [row[0] for row in rows]


def rekey_spider(db: sqlite3.Connection, spider: str, canonicalizer: Canonicalizer, now: float) -> RekeyReport:
    """Rewrite every stored URL of ``spider`` to ``canonicalizer``'s keys, merging collisions.

    Runs inside the caller's transaction; the spider's rules record is
    updated at the end.
    """
    report = RekeyReport(spider, canonicalizer.signature())
    urls: set[str] = set()
    for query in _URL_SOURCES:
        urls.update(row[0] for row in db.execute(query, {"spider": spider}) if row[0] is not None)
    report.examined = len(urls)
    mapping: dict[str, str] = {}
    for url in urls:
        try:
            new = canonicalizer.key(url)
        except URLError:
            report.unparsable += 1
            continue
        if new != url:
            mapping[url] = new
    report.renamed = len(mapping)
    if mapping:
        _apply_mapping(db, spider, mapping, report, now)
    CanonicalStore(db).record(spider, canonicalizer, now)
    if mapping:
        log.info("%s: rekeyed %d URL(s); merged %d frontier row(s), %d document(s)", spider, report.renamed,
                 report.frontier_merged, report.documents_merged)
    return report


def _apply_mapping(db: sqlite3.Connection, spider: str, mapping: dict[str, str], report: RekeyReport,
                   now: float) -> None:
    db.execute("DROP TABLE IF EXISTS temp.rekey_map")
    db.execute("CREATE TEMP TABLE rekey_map (old TEXT PRIMARY KEY, new TEXT NOT NULL)")
    db.executemany("INSERT INTO temp.rekey_map (old, new) VALUES (?, ?)", sorted(mapping.items()))
    try:
        collided = _collisions(db, spider, mapping)
        report.frontier_merged = _merge_keyed(db, "frontier", "url", spider, mapping, _merge_frontier)
        report.documents_merged = _merge_keyed(db, "documents", "url", spider, mapping, _merge_documents)
        report.watermarks_merged = _merge_keyed(db, "watermarks", "seed", spider, mapping, _merge_watermarks)
        _merge_keyed(db, "simhashes", "url", spider, mapping, _merge_simhashes)
        _rebuild_bands(db, spider, set(mapping.values()) | set(mapping))
        report.links_merged = _merge_links(db, spider, mapping)
        report.events_rewritten = _rewrite(db, "fetch_events", "url", spider)
        report.items_rewritten = _rewrite(db, "item_history", "url", spider)
        for table, column in (("frontier", "referrer"), ("frontier", "seed"), ("documents", "near_duplicate_of")):
            _rewrite(db, table, column, spider)
        db.executemany(
            """INSERT INTO canonical_merges (spider, old_url, new_url, collided, signature, merged_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [(spider, old, new, int(new in collided), report.signature, now) for old, new in sorted(mapping.items())],
        )
    finally:
        db.execute("DROP TABLE temp.rekey_map")


def _collisions(db: sqlite3.Connection, spider: str, mapping: dict[str, str]) -> set[str]:
    """New keys that more than one stored URL (old or already-canonical) maps onto."""
    members: dict[str, set[str]] = defaultdict(set)
    for old, new in mapping.items():
        members[new].add(old)
    existing = {row[0] for row in db.execute(
        "SELECT url FROM frontier WHERE spider = ? AND url IN (SELECT new FROM temp.rekey_map) "
        "UNION SELECT url FROM documents WHERE spider = ? AND url IN (SELECT new FROM temp.rekey_map)",
        (spider, spider))}
    return {new for new, olds in members.items() if len(olds) > 1 or new in existing}


def _rows_for(db: sqlite3.Connection, table: str, column: str, spider: str) -> list[dict[str, Any]]:
    rows = db.execute(
        f"""SELECT * FROM {table} WHERE spider = ? AND ({column} IN (SELECT old FROM temp.rekey_map)
                                                  OR {column} IN (SELECT new FROM temp.rekey_map))""",
        (spider,),
    )
    return [dict(row) for row in rows]


def _merge_keyed(
    db: sqlite3.Connection,
    table: str,
    column: str,
    spider: str,
    mapping: dict[str, str],
    combine: Callable[[list[dict[str, Any]]], dict[str, Any]],
) -> int:
    """Move rows keyed by (spider, ``column``) to their new keys; returns rows absorbed by merging."""
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _rows_for(db, table, column, spider):
        groups[mapping.get(row[column], row[column])].append(row)
    absorbed = 0
    for new, members in groups.items():
        if len(members) == 1 and members[0][column] == new:
            continue
        merged = combine(members) if len(members) > 1 else dict(members[0])
        merged[column] = new
        db.executemany(f"DELETE FROM {table} WHERE spider = ? AND {column} = ?",
                       [(spider, member[column]) for member in members])
        names = list(merged)
        db.execute(f"INSERT INTO {table} ({', '.join(names)}) VALUES ({', '.join('?' * len(names))})",
                   [merged[name] for name in names])
        absorbed += len(members) - 1
    return absorbed


def _min(values: Iterable[Any]) -> Any:
    present = [v for v in values if v is not None]
    return min(present) if present else None


def _max(values: Iterable[Any]) -> Any:
    present = [v for v in values if v is not None]
    return max(present) if present else None


def _merge_frontier(rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = min(rows, key=lambda r: r["seq"])
    winner = min(rows, key=lambda r: (_STATE_RANK.get(r["state"], 9), -(r["updated_at"] or 0)))
    state = "pending" if winner["state"] == "active" else winner["state"]
    merged = dict(first)
    merged.update(
        state=state,
        attempts=0 if state == "pending" else winner["attempts"],
        not_before=winner["not_before"] if state != "pending" else 0,
        depth=min(r["depth"] for r in rows),
        priority=max(r["priority"] for r in rows),
        urgency=max(r["urgency"] for r in rows),
        sitemap_lastmod=_max(r["sitemap_lastmod"] for r in rows),
        changefreq=winner["changefreq"] or first["changefreq"],
        discovered_at=min(r["discovered_at"] for r in rows),
        updated_at=max(r["updated_at"] for r in rows),
        run_id=_max(r["run_id"] for r in rows),
        charged_run=_max(r["charged_run"] for r in rows),
        lease_owner=None,
        lease_expires=None,
        lease_attempts=0,
    )
    return merged


def _merge_documents(rows: list[dict[str, Any]]) -> dict[str, Any]:
    winner = max(rows, key=lambda r: (r["last_success"] or -1.0, r["last_fetched"] or -1.0, r["last_event_id"] or -1))
    merged = dict(winner)
    merged.update(
        first_fetched=min(r["first_fetched"] for r in rows),
        last_fetched=_max(r["last_fetched"] for r in rows),
        last_success=_max(r["last_success"] for r in rows),
        last_changed=_max(r["last_changed"] for r in rows),
        fetch_count=sum(r["fetch_count"] for r in rows),
        check_count=sum(r["check_count"] for r in rows),
        change_count=sum(r["change_count"] for r in rows),
        observed_span=sum(r["observed_span"] for r in rows),
        next_visit=_min(r["next_visit"] for r in rows),
        revisit_interval=_min(r["revisit_interval"] for r in rows),
        sitemap_lastmod=_max(r["sitemap_lastmod"] for r in rows),
        last_event_id=_max(r["last_event_id"] for r in rows),
    )
    return merged


def _merge_watermarks(rows: list[dict[str, Any]]) -> dict[str, Any]:
    winner = max(rows, key=lambda r: r["last_started"] or -1.0)
    completions = [r["last_completed"] for r in rows]
    lastmods = [r["lastmod_high"] for r in rows]
    merged = dict(winner)
    merged.update(
        last_started=_max(r["last_started"] for r in rows),
        last_run_id=_max(r["last_run_id"] for r in rows),
        last_completed=None if None in completions else min(completions),
        lastmod_high=None if None in lastmods else min(lastmods),
        run_lastmod_high=_max(r["run_lastmod_high"] for r in rows),
        run_lastmod_run=_max(r["run_lastmod_run"] for r in rows),
    )
    return merged


def _merge_simhashes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return dict(max(rows, key=lambda r: r["computed_at"]))


def _rebuild_bands(db: sqlite3.Connection, spider: str, urls: set[str]) -> None:
    db.executemany("DELETE FROM simhash_bands WHERE spider = ? AND url = ?", [(spider, url) for url in urls])
    rows = db.execute(
        "SELECT url, simhash FROM simhashes WHERE spider = ? AND url IN (SELECT new FROM temp.rekey_map)", (spider,)
    ).fetchall()
    db.executemany(
        "INSERT OR IGNORE INTO simhash_bands (spider, band, value, url) VALUES (?, ?, ?, ?)",
        [(spider, band, value, row[0]) for row in rows
         for band, value in enumerate(band_values(to_unsigned(row[1])))],
    )


def _merge_links(db: sqlite3.Connection, spider: str, mapping: dict[str, str]) -> int:
    rows = [dict(r) for r in db.execute(
        """SELECT * FROM links WHERE spider = ? AND (source IN (SELECT old FROM temp.rekey_map)
                                                OR target IN (SELECT old FROM temp.rekey_map)
                                                OR source IN (SELECT new FROM temp.rekey_map)
                                                OR target IN (SELECT new FROM temp.rekey_map))""",
        (spider,),
    )]
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(mapping.get(row["source"], row["source"]), mapping.get(row["target"], row["target"]))].append(row)
    absorbed = 0
    for (source, target), members in groups.items():
        if len(members) == 1 and (members[0]["source"], members[0]["target"]) == (source, target):
            continue
        removals = [m["removed_at"] for m in members]
        db.executemany("DELETE FROM links WHERE spider = ? AND source = ? AND target = ?",
                       [(spider, m["source"], m["target"]) for m in members])
        db.execute(
            "INSERT INTO links (spider, source, target, first_seen, last_seen, removed_at) VALUES (?, ?, ?, ?, ?, ?)",
            (spider, source, target, min(m["first_seen"] for m in members), max(m["last_seen"] for m in members),
             None if None in removals else max(removals)),
        )
        absorbed += len(members) - 1
    return absorbed


def _rewrite(db: sqlite3.Connection, table: str, column: str, spider: str) -> int:
    cursor = db.execute(
        f"""UPDATE {table} SET {column} = (SELECT m.new FROM temp.rekey_map m WHERE m.old = {table}.{column})
            WHERE spider = ? AND {column} IN (SELECT old FROM temp.rekey_map)""",
        (spider,),
    )
    return cursor.rowcount


def adopt_strict_rules(db: sqlite3.Connection) -> None:
    """Schema migration: rekey every spider with the built-in version-2 rules and merge duplicates."""
    execute_script(db, RULES_TABLES)
    canonicalizer = Canonicalizer.strict()
    now = time.time()
    for spider in known_spiders(db):
        rekey_spider(db, spider, canonicalizer, now)
