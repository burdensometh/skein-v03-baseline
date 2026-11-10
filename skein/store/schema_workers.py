"""Schema for cooperating worker processes: frontier leases, host politeness slots, worker registry.

Several ``skein worker`` processes share one database. A worker owns a
frontier row while it holds a *lease* on it (owner, expiry, number of
times the row was leased); politeness across processes lives in
``host_state`` (spacing, back-off, quarantine per origin) and
``host_slots`` (one row per concurrent request a host may receive).
Fetch events remember which worker's journal recorded them, because every
worker keeps its own checkpoint.
"""

from __future__ import annotations

import sqlite3
import uuid

from skein.store.database import execute_script

LOCAL_WORKER = "local"


def local_worker_id(spider: str) -> str:
    """Worker id of the in-process crawler of ``spider`` (``skein crawl`` without helper processes)."""
    return f"{LOCAL_WORKER}-{spider}"


LEASES = """
ALTER TABLE frontier ADD COLUMN lease_owner TEXT;
ALTER TABLE frontier ADD COLUMN lease_expires REAL;
ALTER TABLE frontier ADD COLUMN lease_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE frontier ADD COLUMN charged_run INTEGER;
CREATE INDEX frontier_leases ON frontier(spider, state, lease_expires);
CREATE INDEX frontier_by_owner ON frontier(lease_owner) WHERE lease_owner IS NOT NULL;
CREATE INDEX frontier_charged ON frontier(spider, charged_run);
CREATE INDEX frontier_by_seq ON frontier(spider, seq);
CREATE TABLE workers (
    id           TEXT    PRIMARY KEY,
    spider       TEXT    NOT NULL,
    run_id       INTEGER,
    hostname     TEXT    NOT NULL,
    pid          INTEGER NOT NULL,
    role         TEXT    NOT NULL DEFAULT 'worker',
    state        TEXT    NOT NULL DEFAULT 'running'
                 CHECK (state IN ('running', 'stopped', 'finished', 'dead')),
    started_at   REAL    NOT NULL,
    heartbeat_at REAL    NOT NULL,
    stopped_at   REAL,
    checkpoint   TEXT,
    stats        TEXT
);
CREATE INDEX workers_by_spider ON workers(spider, state);
CREATE TABLE host_state (
    host_key         TEXT    PRIMARY KEY,
    crawl_delay      REAL,
    next_allowed     REAL    NOT NULL DEFAULT 0,
    backoff_until    REAL    NOT NULL DEFAULT 0,
    quarantine_until REAL    NOT NULL DEFAULT 0,
    failures         INTEGER NOT NULL DEFAULT 0,
    backoffs         INTEGER NOT NULL DEFAULT 0,
    requests         INTEGER NOT NULL DEFAULT 0,
    last_start       REAL,
    updated_at       REAL    NOT NULL DEFAULT 0
);
CREATE TABLE host_slots (
    host_key      TEXT    NOT NULL,
    slot          INTEGER NOT NULL,
    owner         TEXT,
    lease_expires REAL,
    acquired_at   REAL,
    PRIMARY KEY (host_key, slot)
);
CREATE INDEX host_slots_by_owner ON host_slots(owner) WHERE owner IS NOT NULL;
ALTER TABLE fetch_events ADD COLUMN worker_id TEXT;
DROP INDEX fetch_events_by_journal;
CREATE INDEX fetch_events_by_journal ON fetch_events(run_id, worker_id, journal_seq);
ALTER TABLE runs ADD COLUMN pages_charged INTEGER NOT NULL DEFAULT 0;
ALTER TABLE watermarks ADD COLUMN run_lastmod_high REAL;
ALTER TABLE watermarks ADD COLUMN run_lastmod_run INTEGER;
CREATE TABLE skein_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""


def add_leases(db: sqlite3.Connection) -> None:
    """Lease columns, host politeness tables and the worker registry.

    Rows a crashed 0.2 process left ``active`` keep that state without an
    owner; lease reclaim treats them as expired. Journaled events of 0.2
    belong to the single in-process crawler, which is the spider's local
    worker now, so an interrupted 0.2 run can still be resumed.
    """
    execute_script(db, LEASES)
    db.execute("UPDATE fetch_events SET worker_id = ? || '-' || spider WHERE journal_seq IS NOT NULL",
               (LOCAL_WORKER,))
    db.execute("INSERT INTO skein_meta (key, value) VALUES ('database_id', ?)", (uuid.uuid4().hex,))


def database_id(db: sqlite3.Connection) -> str:
    """The random identity written when the worker schema was created (files derived from the DB carry it)."""
    row = db.execute("SELECT value FROM skein_meta WHERE key = 'database_id'").fetchone()
    if row is None:
        raise LookupError("database has no identity (schema too old)")
    return str(row[0])
