"""Fetch events (one row per attempt) and current document state (one row per URL)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field, fields, replace
from typing import Any, Iterator

from skein.incremental.revisit import RevisitPolicy

EVENT_COLUMNS = (
    "run_id", "spider", "url", "final_url", "kind", "depth", "status", "content_type", "charset",
    "content_length", "fetched_at", "elapsed", "redirects", "error", "error_class", "attempt",
    "conditional", "etag", "last_modified", "fingerprint", "changed", "freshness", "items_found", "links_found",
    "sitemap_lastmod", "journal_seq", "worker_id", "payload_digest", "warc_file", "warc_offset", "warc_length",
    "warc_revisit",
)


@dataclass
class FetchEvent:
    """Outcome of one fetch attempt, successful or not.

    ``changed`` is filled in by :meth:`DocumentStore.apply`: ``True``/``False``
    when the response could be compared with the stored state, ``None``
    otherwise (first fetch, failures, documents stored without fingerprint).
    ``warc_record_id``/``warc_date`` identify the WARC response record that
    archived this response's payload (not stored on the event; they become
    the document's reference for later revisit records).
    """

    spider: str
    url: str
    fetched_at: float
    kind: str = "page"
    depth: int = 0
    final_url: str | None = None
    status: int | None = None
    content_type: str | None = None
    charset: str | None = None
    content_length: int | None = None
    elapsed: float | None = None
    redirects: list[str] | None = None
    error: str | None = None
    error_class: str | None = None
    attempt: int = 0
    conditional: bool = False
    etag: str | None = None
    last_modified: str | None = None
    fingerprint: str | None = None
    changed: bool | None = None
    freshness: float | None = None
    items_found: int = 0
    links_found: int = 0
    sitemap_lastmod: float | None = None
    changefreq: str | None = None
    journal_seq: int | None = None
    worker_id: str | None = None
    payload_digest: str | None = None
    warc_file: str | None = None
    warc_offset: int | None = None
    warc_length: int | None = None
    warc_revisit: bool = False
    warc_record_id: str | None = None
    warc_date: str | None = None

    @property
    def not_modified(self) -> bool:
        return self.status == 304

    @property
    def has_content(self) -> bool:
        """A 2xx response whose body was processed."""
        return self.status is not None and 200 <= self.status < 300


@dataclass
class DocumentState:
    """The latest known state of one URL and the statistics behind its revisit schedule."""

    spider: str
    url: str
    first_fetched: float
    final_url: str | None = None
    kind: str = "page"
    status: int | None = None
    content_type: str | None = None
    charset: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    fingerprint: str | None = None
    last_fetched: float | None = None
    last_success: float | None = None
    last_changed: float | None = None
    fetch_count: int = 0
    check_count: int = 0
    change_count: int = 0
    unchanged_streak: int = 0
    observed_span: float = 0.0
    revisit_interval: float | None = None
    next_visit: float | None = None
    sitemap_lastmod: float | None = None
    changefreq: str | None = None
    last_error: str | None = None
    error_class: str | None = None
    last_event_id: int | None = None
    near_duplicate_of: str | None = None
    near_distance: int | None = None
    payload_digest: str | None = None
    warc_record_id: str | None = None
    warc_date: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "DocumentState":
        names = {f.name for f in fields(cls)}
        return cls(**{key: row[key] for key in row.keys() if key in names})

    def as_dict(self) -> dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}


@dataclass(frozen=True)
class DocumentChange:
    """How one fetch event changed the document state."""

    event_id: int
    new: bool
    changed: bool | None
    next_visit: float | None
    interval: float | None
    state: DocumentState | None = field(default=None, repr=False, compare=False)


class DocumentStore:
    """Repository for ``fetch_events`` and ``documents``; callers provide the transaction."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def get(self, spider: str, url: str) -> DocumentState | None:
        row = self.db.execute("SELECT * FROM documents WHERE spider = ? AND url = ?", (spider, url)).fetchone()
        return DocumentState.from_row(row) if row is not None else None

    def apply(self, event: FetchEvent, run_id: int | None, revisit: RevisitPolicy) -> DocumentChange:
        """Insert ``event`` and fold it into the URL's document state."""
        previous = self.get(event.spider, event.url)
        event.changed = _compare(previous, event)
        event_id = self._insert_event(event, run_id)
        state = _next_state(previous, event, event_id, revisit)
        self._save(state)
        new = event.has_content and (previous is None or previous.last_success is None)
        return DocumentChange(event_id, new, event.changed, state.next_visit, state.revisit_interval, state)

    def _insert_event(self, event: FetchEvent, run_id: int | None) -> int:
        values = {
            "run_id": run_id,
            "redirects": json.dumps(event.redirects) if event.redirects else None,
            "conditional": int(event.conditional),
            "warc_revisit": int(event.warc_revisit),
            "changed": None if event.changed is None else int(event.changed),
        }
        row = [values[c] if c in values else getattr(event, c) for c in EVENT_COLUMNS]
        cursor = self.db.execute(
            f"INSERT INTO fetch_events ({', '.join(EVENT_COLUMNS)}) VALUES ({', '.join('?' * len(EVENT_COLUMNS))})",
            row,
        )
        return int(cursor.lastrowid)

    def _save(self, state: DocumentState) -> None:
        data = state.as_dict()
        columns = list(data)
        updates = ", ".join(f"{c} = excluded.{c}" for c in columns if c not in ("spider", "url"))
        self.db.execute(
            f"INSERT INTO documents ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))}) "
            f"ON CONFLICT (spider, url) DO UPDATE SET {updates}",
            [data[c] for c in columns],
        )

    def iter_documents(self, spider: str, *, changed_since: float | None = None) -> Iterator[DocumentState]:
        query = "SELECT * FROM documents WHERE spider = ?"
        params: list[Any] = [spider]
        if changed_since is not None:
            query += " AND last_changed >= ?"
            params.append(changed_since)
        for row in self.db.execute(query + " ORDER BY url", params):
            yield DocumentState.from_row(row)

    def events(
        self,
        spider: str,
        *,
        url: str | None = None,
        run_id: int | None = None,
        since: float | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Fetch events of ``spider`` in insertion order, optionally for one URL, run or time window."""
        clauses = ["spider = ?"]
        params: list[Any] = [spider]
        for column, op, value in (("url", "=", url), ("run_id", "=", run_id), ("fetched_at", ">=", since)):
            if value is not None:
                clauses.append(f"{column} {op} ?")
                params.append(value)
        for row in self.db.execute(f"SELECT * FROM fetch_events WHERE {' AND '.join(clauses)} ORDER BY id", params):
            record = dict(row)
            record["redirects"] = json.loads(record["redirects"]) if record["redirects"] else []
            record["conditional"] = bool(record["conditional"])
            record["changed"] = None if record["changed"] is None else bool(record["changed"])
            yield record

    def journaled(self, run_id: int, worker_id: str, journal_seq: int) -> bool:
        """Whether the fetch events journaled by ``worker_id`` as record ``journal_seq`` were committed."""
        row = self.db.execute(
            "SELECT 1 FROM fetch_events WHERE run_id = ? AND worker_id = ? AND journal_seq = ? LIMIT 1",
            (run_id, worker_id, journal_seq),
        ).fetchone()
        return row is not None

    def archived_payload(self, spider: str, digest: str) -> DocumentState | None:
        """A document whose last archived response had payload ``digest`` (any URL)."""
        row = self.db.execute(
            """SELECT * FROM documents WHERE spider = ? AND payload_digest = ? AND warc_record_id IS NOT NULL
               ORDER BY last_success DESC LIMIT 1""",
            (spider, digest),
        ).fetchone()
        return DocumentState.from_row(row) if row is not None else None

    def mark_near_duplicate(self, spider: str, url: str, original: str | None, distance: int | None) -> None:
        self.db.execute(
            "UPDATE documents SET near_duplicate_of = ?, near_distance = ? WHERE spider = ? AND url = ?",
            (original, distance, spider, url),
        )

    def status_counts(self, spider: str) -> dict[str, int]:
        rows = self.db.execute(
            "SELECT COALESCE(CAST(status AS TEXT), 'error') AS s, COUNT(*) FROM fetch_events "
            "WHERE spider = ? GROUP BY s ORDER BY s",
            (spider,),
        ).fetchall()
        return {row[0]: row[1] for row in rows}


def _compare(previous: DocumentState | None, event: FetchEvent) -> bool | None:
    if previous is None or previous.last_success is None:
        return None
    if event.not_modified:
        return False
    if not event.has_content or event.fingerprint is None or previous.fingerprint is None:
        return None
    return event.fingerprint != previous.fingerprint


def _next_state(
    previous: DocumentState | None, event: FetchEvent, event_id: int, revisit: RevisitPolicy
) -> DocumentState:
    now = event.fetched_at
    state = replace(previous) if previous is not None else DocumentState(event.spider, event.url, first_fetched=now)
    state.last_event_id = event_id
    state.kind = event.kind
    if event.sitemap_lastmod is not None:
        state.sitemap_lastmod = event.sitemap_lastmod
    if event.changefreq is not None:
        state.changefreq = event.changefreq
    if event.error_class == "robots":
        state.last_error, state.error_class = event.error, event.error_class
        state.next_visit = now + revisit.max_interval
        return state
    state.fetch_count += 1
    state.last_fetched = now
    if not (event.has_content or event.not_modified):
        # Failures and deferred redirects leave the content state alone.
        state.last_error, state.error_class = event.error, event.error_class
        if event.status is not None:
            state.status = event.status
            state.final_url = event.final_url or state.final_url
        if event.error_class is None:
            interval = state.revisit_interval or revisit.first_interval(state.changefreq)
        else:
            interval = revisit.failure_interval(event.error_class == "transient", state.revisit_interval)
        state.next_visit = now + interval
        return state
    if event.changed is not None and previous is not None and previous.last_success is not None:
        state.check_count += 1
        state.observed_span += max(0.0, now - previous.last_success)
        if event.changed:
            state.change_count += 1
            state.unchanged_streak = 0
            state.last_changed = now
        else:
            state.unchanged_streak += 1
    elif previous is None or previous.last_success is None:
        state.last_changed = now
    state.last_success = now
    state.last_error = state.error_class = None
    if event.has_content:
        state.status = event.status
        state.final_url = event.final_url
        state.content_type = event.content_type
        state.charset = event.charset
        state.fingerprint = event.fingerprint
        state.etag = event.etag
        state.last_modified = event.last_modified
        state.payload_digest = event.payload_digest or state.payload_digest
        if event.warc_record_id is not None:
            state.warc_record_id, state.warc_date = event.warc_record_id, event.warc_date
    else:
        # A 304 may carry refreshed validators (RFC 9110 15.4.5).
        state.etag = event.etag or state.etag
        state.last_modified = event.last_modified or state.last_modified
    state.revisit_interval = revisit.next_interval(
        checks=state.check_count,
        changes=state.change_count,
        observed_span=state.observed_span,
        previous=state.revisit_interval,
        changefreq=state.changefreq,
        freshness=event.freshness,
    )
    state.next_visit = now + state.revisit_interval
    return state
