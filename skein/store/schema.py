"""skein's schema history, one :class:`Migration` per released schema change.

Version 1 is the schema written by skein 0.1 (``runs``, ``pages``,
``items``). Versions 2-7 (skein 0.2) split the page log into fetch events
and current document state, version items by content hash and add the
persistent crawl frontier with per-seed watermarks. Versions 8-11 (skein
0.3) add worker leases and cross-process politeness, turn item history into
an SCD type 2 table, add near-duplicate and WARC bookkeeping and rekey
stored URLs with canonicalisation rules version 2. A brand-new database runs all of them in
order, so there is exactly one way to arrive at the current schema.
"""

from __future__ import annotations

import json
import sqlite3

from skein.extract.extractor import canonical_json, content_hash
from skein.retry.classify import classify_recorded
from skein.store.database import execute_script
from skein.store.migrations import Migration
from skein.store.rekey import adopt_strict_rules
from skein.store.schema_archive import ARCHIVE
from skein.store.schema_history import to_item_history
from skein.store.schema_sightings import add_item_sightings
from skein.store.schema_workers import add_leases
from skein.urls.parse import URLError, parse_url

BASELINE = """
CREATE TABLE runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    spider        TEXT    NOT NULL,
    started_at    REAL    NOT NULL,
    finished_at   REAL,
    status        TEXT    NOT NULL DEFAULT 'running',
    stats         TEXT
);
CREATE TABLE pages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER REFERENCES runs(id),
    spider          TEXT    NOT NULL,
    url             TEXT    NOT NULL,
    final_url       TEXT,
    kind            TEXT    NOT NULL DEFAULT 'page',
    depth           INTEGER NOT NULL DEFAULT 0,
    status          INTEGER,
    content_type    TEXT,
    charset         TEXT,
    content_length  INTEGER,
    fetched_at      REAL    NOT NULL,
    elapsed         REAL,
    redirects       TEXT,
    error           TEXT,
    items_found     INTEGER NOT NULL DEFAULT 0,
    links_found     INTEGER NOT NULL DEFAULT 0,
    sitemap_lastmod REAL
);
CREATE INDEX pages_by_url ON pages(spider, url);
CREATE INDEX pages_by_run ON pages(run_id);
CREATE TABLE items (
    spider      TEXT NOT NULL,
    item_type   TEXT NOT NULL,
    item_key    TEXT NOT NULL,
    url         TEXT NOT NULL,
    data        TEXT NOT NULL,
    first_seen  REAL NOT NULL,
    last_seen   REAL NOT NULL,
    PRIMARY KEY (spider, item_type, item_key)
);
CREATE INDEX items_by_last_seen ON items(spider, last_seen)
"""

FETCH_EVENTS = """
CREATE TABLE fetch_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER REFERENCES runs(id),
    spider          TEXT    NOT NULL,
    url             TEXT    NOT NULL,
    final_url       TEXT,
    kind            TEXT    NOT NULL DEFAULT 'page',
    depth           INTEGER NOT NULL DEFAULT 0,
    status          INTEGER,
    content_type    TEXT,
    charset         TEXT,
    content_length  INTEGER,
    fetched_at      REAL    NOT NULL,
    elapsed         REAL,
    redirects       TEXT,
    error           TEXT,
    error_class     TEXT,
    attempt         INTEGER NOT NULL DEFAULT 0,
    conditional     INTEGER NOT NULL DEFAULT 0,
    etag            TEXT,
    last_modified   TEXT,
    fingerprint     TEXT,
    changed         INTEGER,
    freshness       REAL,
    items_found     INTEGER NOT NULL DEFAULT 0,
    links_found     INTEGER NOT NULL DEFAULT 0,
    sitemap_lastmod REAL
);
CREATE INDEX fetch_events_by_url ON fetch_events(spider, url);
CREATE INDEX fetch_events_by_run ON fetch_events(run_id);
CREATE INDEX fetch_events_by_time ON fetch_events(spider, fetched_at);
CREATE TABLE documents (
    spider           TEXT    NOT NULL,
    url              TEXT    NOT NULL,
    final_url        TEXT,
    kind             TEXT    NOT NULL DEFAULT 'page',
    status           INTEGER,
    content_type     TEXT,
    charset          TEXT,
    etag             TEXT,
    last_modified    TEXT,
    fingerprint      TEXT,
    first_fetched    REAL    NOT NULL,
    last_fetched     REAL,
    last_success     REAL,
    last_changed     REAL,
    fetch_count      INTEGER NOT NULL DEFAULT 0,
    check_count      INTEGER NOT NULL DEFAULT 0,
    change_count     INTEGER NOT NULL DEFAULT 0,
    unchanged_streak INTEGER NOT NULL DEFAULT 0,
    observed_span    REAL    NOT NULL DEFAULT 0,
    revisit_interval REAL,
    next_visit       REAL,
    sitemap_lastmod  REAL,
    changefreq       TEXT,
    last_error       TEXT,
    error_class      TEXT,
    last_event_id    INTEGER REFERENCES fetch_events(id),
    PRIMARY KEY (spider, url)
);
CREATE INDEX documents_by_next_visit ON documents(spider, next_visit)
"""

PAGE_COLUMNS = (
    "id, run_id, spider, url, final_url, kind, depth, status, content_type, charset, content_length, "
    "fetched_at, elapsed, redirects, error, items_found, links_found, sitemap_lastmod"
)


def split_pages(db: sqlite3.Connection) -> None:
    """Replace ``pages`` by ``fetch_events`` (every attempt) and ``documents`` (latest state per URL)."""
    execute_script(db, FETCH_EVENTS)
    db.execute(f"INSERT INTO fetch_events ({PAGE_COLUMNS}) SELECT {PAGE_COLUMNS} FROM pages")
    rows = db.execute("SELECT id, status, error FROM fetch_events WHERE error IS NOT NULL OR status >= 400").fetchall()
    for row in rows:
        error_class = classify_recorded(row["status"], row["error"])
        db.execute(
            "UPDATE fetch_events SET error_class = ? WHERE id = ?",
            (error_class.value if error_class is not None else None, row["id"]),
        )
    db.execute(
        """INSERT INTO documents (spider, url, final_url, kind, status, content_type, charset,
               first_fetched, last_fetched, last_success, last_changed, fetch_count,
               sitemap_lastmod, last_error, error_class, last_event_id)
           SELECT e.spider, e.url, e.final_url, e.kind, e.status, e.content_type, e.charset,
                  agg.first_fetched, agg.last_fetched, agg.last_success, agg.first_success, agg.fetches,
                  e.sitemap_lastmod, e.error, e.error_class, e.id
           FROM fetch_events e
           JOIN (SELECT spider, url,
                        MIN(fetched_at) AS first_fetched,
                        MAX(CASE WHEN status IS NOT NULL THEN fetched_at END) AS last_fetched,
                        MAX(CASE WHEN status BETWEEN 200 AND 299 THEN fetched_at END) AS last_success,
                        MIN(CASE WHEN status BETWEEN 200 AND 299 THEN fetched_at END) AS first_success,
                        SUM(CASE WHEN status IS NOT NULL OR error_class <> 'robots' THEN 1 ELSE 0 END) AS fetches,
                        MAX(id) AS last_id
                 FROM fetch_events GROUP BY spider, url) agg
             ON e.id = agg.last_id"""
    )
    # The latest successful response is the document's content state, even if
    # a later attempt failed.
    db.execute(
        """UPDATE documents SET
               status = (SELECT e.status FROM fetch_events e WHERE e.spider = documents.spider
                         AND e.url = documents.url AND e.status BETWEEN 200 AND 299 ORDER BY e.id DESC LIMIT 1),
               content_type = (SELECT e.content_type FROM fetch_events e WHERE e.spider = documents.spider
                         AND e.url = documents.url AND e.status BETWEEN 200 AND 299 ORDER BY e.id DESC LIMIT 1)
           WHERE last_success IS NOT NULL AND (status IS NULL OR status NOT BETWEEN 200 AND 299)"""
    )
    db.execute("DROP TABLE pages")


def add_item_versions(db: sqlite3.Connection) -> None:
    """Hash item content, number versions and keep every version in ``item_versions``."""
    db.execute("ALTER TABLE items ADD COLUMN content_hash TEXT")
    db.execute("ALTER TABLE items ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
    db.execute("ALTER TABLE items ADD COLUMN changed_at REAL")
    db.execute("ALTER TABLE items ADD COLUMN deleted_at REAL")
    db.execute(
        """CREATE TABLE item_versions (
               spider         TEXT    NOT NULL,
               item_type      TEXT    NOT NULL,
               item_key       TEXT    NOT NULL,
               version        INTEGER NOT NULL,
               content_hash   TEXT    NOT NULL,
               data           TEXT    NOT NULL,
               url            TEXT    NOT NULL,
               observed_at    REAL    NOT NULL,
               deleted        INTEGER NOT NULL DEFAULT 0,
               run_id         INTEGER REFERENCES runs(id),
               fetch_event_id INTEGER REFERENCES fetch_events(id),
               PRIMARY KEY (spider, item_type, item_key, version)
           )"""
    )
    db.execute("CREATE INDEX item_versions_by_time ON item_versions(spider, observed_at)")
    db.execute("CREATE INDEX items_by_url ON items(spider, url)")
    for row in db.execute("SELECT rowid, data, first_seen FROM items").fetchall():
        canonical = canonical_json(json.loads(row["data"]))
        db.execute(
            "UPDATE items SET data = ?, content_hash = ?, changed_at = ? WHERE rowid = ?",
            (canonical, content_hash(canonical), row["first_seen"], row["rowid"]),
        )
    db.execute(
        """INSERT INTO item_versions (spider, item_type, item_key, version, content_hash, data, url, observed_at)
           SELECT spider, item_type, item_key, version, content_hash, data, url, first_seen FROM items"""
    )


CRAWL_STATE = """
CREATE TABLE frontier (
    spider          TEXT    NOT NULL,
    url             TEXT    NOT NULL,
    host_key        TEXT    NOT NULL,
    depth           INTEGER NOT NULL DEFAULT 0,
    priority        INTEGER NOT NULL DEFAULT 0,
    kind            TEXT    NOT NULL DEFAULT 'page',
    referrer        TEXT,
    seed            TEXT,
    state           TEXT    NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'active', 'done', 'failed', 'deferred')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    not_before      REAL    NOT NULL DEFAULT 0,
    urgency         REAL    NOT NULL DEFAULT 0,
    sitemap_lastmod REAL,
    changefreq      TEXT,
    discovered_at   REAL    NOT NULL,
    updated_at      REAL    NOT NULL,
    run_id          INTEGER,
    seq             INTEGER NOT NULL,
    PRIMARY KEY (spider, url)
);
CREATE INDEX frontier_queue ON frontier(spider, state, host_key, priority DESC, urgency DESC, depth, seq);
CREATE INDEX frontier_due ON frontier(spider, state, host_key, not_before);
CREATE INDEX frontier_by_seed ON frontier(spider, seed, state);
CREATE TABLE watermarks (
    spider         TEXT NOT NULL,
    seed           TEXT NOT NULL,
    kind           TEXT NOT NULL DEFAULT 'page',
    last_run_id    INTEGER,
    last_started   REAL,
    last_completed REAL,
    lastmod_high   REAL,
    PRIMARY KEY (spider, seed)
);
ALTER TABLE runs ADD COLUMN mode TEXT NOT NULL DEFAULT 'full';
ALTER TABLE runs ADD COLUMN resumes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE fetch_events ADD COLUMN journal_seq INTEGER;
CREATE INDEX fetch_events_by_journal ON fetch_events(run_id, journal_seq)
"""


def add_crawl_state(db: sqlite3.Connection) -> None:
    """Persistent frontier, watermarks and journal links; every known document becomes a handled frontier entry."""
    execute_script(db, CRAWL_STATE)
    rows = db.execute(
        """SELECT d.spider, d.url, d.kind, d.first_fetched, d.last_fetched, d.error_class,
                  e.depth, e.run_id
           FROM documents d LEFT JOIN fetch_events e ON e.id = d.last_event_id
           ORDER BY d.first_fetched, d.url"""
    ).fetchall()
    for seq, row in enumerate(rows, start=1):
        try:
            host_key = parse_url(row["url"]).origin
        except URLError:
            continue
        state = "failed" if row["error_class"] in ("permanent", "robots") else "done"
        db.execute(
            """INSERT INTO frontier (spider, url, host_key, depth, kind, state, discovered_at, updated_at, run_id, seq)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (row["spider"], row["url"], host_key, row["depth"] or 0, row["kind"], state,
             row["first_fetched"], row["last_fetched"] or row["first_fetched"], row["run_id"], seq),
        )


ROBOTS_CACHE = """
CREATE TABLE robots_cache (
    origin        TEXT PRIMARY KEY,
    state         TEXT NOT NULL,
    status        INTEGER,
    body          BLOB,
    fetched_at    REAL NOT NULL,
    expires_at    REAL NOT NULL,
    etag          TEXT,
    last_modified TEXT,
    error         TEXT
)
"""


LINK_GRAPH = """
CREATE TABLE links (
    spider     TEXT NOT NULL,
    source     TEXT NOT NULL,
    target     TEXT NOT NULL,
    first_seen REAL NOT NULL,
    last_seen  REAL NOT NULL,
    removed_at REAL,
    PRIMARY KEY (spider, source, target)
);
CREATE INDEX links_by_target ON links(spider, target, removed_at)
"""

EXPORT_CURSORS = """
CREATE TABLE export_cursors (
    name       TEXT    NOT NULL,
    spider     TEXT    NOT NULL,
    position   INTEGER NOT NULL DEFAULT 0,
    updated_at REAL    NOT NULL,
    PRIMARY KEY (name, spider)
)
"""


MIGRATIONS: tuple[Migration, ...] = (
    Migration(1, "baseline", BASELINE, "runs, pages and items as written by skein 0.1"),
    Migration(2, "split_pages", split_pages,
              "pages become fetch_events plus documents; ETag, Last-Modified and fingerprint columns"),
    Migration(3, "item_versions", add_item_versions, "item content hashes, version numbers and history"),
    Migration(4, "crawl_state", add_crawl_state,
              "persistent frontier, per-seed watermarks, run modes, journal sequence of fetch events"),
    Migration(5, "robots_cache", ROBOTS_CACHE, "robots.txt entries with validators shared across runs"),
    Migration(6, "link_graph", LINK_GRAPH, "current outlinks per page, for orphan detection"),
    Migration(7, "export_cursors", EXPORT_CURSORS, "named positions in the item change feed"),
    Migration(8, "worker_leases", add_leases,
              "frontier leases, cross-process host politeness slots, worker registry, per-worker journals"),
    Migration(9, "item_history", to_item_history,
              "items and item_versions become one SCD type 2 table with validity intervals"),
    Migration(10, "dedup_archive", ARCHIVE, "SimHash band index, payload digests and WARC record locations"),
    Migration(11, "canonical_rules_v2", adopt_strict_rules,
              "URL keys recomputed with canonical rules version 2; colliding rows merged"),
    Migration(12, "item_sightings", add_item_sightings,
              "run-scoped sightings of authority fragment rules for deferred deletion reconciliation"),
)
LATEST_VERSION = MIGRATIONS[-1].version
