"""Pruning old fetch events while keeping everything the current state refers to."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from skein.store.database import Transaction


@dataclass(frozen=True)
class PruneReport:
    events_deleted: int
    events_kept: int
    vacuumed: bool


def prune_events(db: sqlite3.Connection, spider: str | None, before: float, *, vacuum: bool = False) -> PruneReport:
    """Delete fetch events older than ``before``.

    Events still referenced are kept: the latest event of every document
    (``documents.last_event_id``) and every event that produced an item
    version (``item_history.fetch_event_id``). So are events of runs that
    have not finished, which a resumed run may still need to recognise.
    """
    scope = "AND e.spider = ?" if spider is not None else ""
    params: list[object] = [before]
    if spider is not None:
        params.append(spider)
    with Transaction(db):
        cursor = db.execute(
            f"""DELETE FROM fetch_events WHERE id IN (
                    SELECT e.id FROM fetch_events e
                    WHERE e.fetched_at < ? {scope}
                      AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.last_event_id = e.id)
                      AND NOT EXISTS (SELECT 1 FROM item_history v WHERE v.fetch_event_id = e.id)
                      AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.id = e.run_id
                                      AND r.status IN ('running', 'stopped', 'failed')))""",
            params,
        )
        deleted = cursor.rowcount
        kept = int(db.execute("SELECT COUNT(*) FROM fetch_events").fetchone()[0])
    if vacuum:
        db.execute("VACUUM")
    return PruneReport(deleted, kept, vacuum)
