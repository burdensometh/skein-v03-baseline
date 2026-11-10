"""The link graph: which page (or sitemap) currently points at which URL.

Incremental runs use it to stop revisiting *orphans*: URLs that were
reached through links in the past, but whose every inbound link has since
disappeared from the pages that carried it. URLs never recorded as link
targets (seeds, URLs known from databases older than the graph) are never
considered orphans.
"""

from __future__ import annotations

import sqlite3
from typing import Iterable

ORPHAN_CONDITION = """EXISTS (SELECT 1 FROM links l WHERE l.spider = {alias}.spider AND l.target = {alias}.url)
    AND NOT EXISTS (SELECT 1 FROM links l WHERE l.spider = {alias}.spider AND l.target = {alias}.url
                    AND l.removed_at IS NULL)"""


def orphan_condition(alias: str) -> str:
    """SQL condition true for orphaned rows of a table aliased ``alias`` with ``spider`` and ``url`` columns."""
    return ORPHAN_CONDITION.format(alias=alias)


class LinkStore:
    """Repository for ``links``; callers provide the transaction."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def replace_outlinks(self, spider: str, source: str, targets: Iterable[str], now: float) -> tuple[int, int]:
        """Record the complete set of links found on ``source``; returns ``(current, removed)`` counts.

        Links no longer present are marked removed rather than deleted, so
        that "was linked once" stays distinguishable from "never linked".
        """
        current = sorted(set(targets))
        for target in current:
            self.db.execute(
                """INSERT INTO links (spider, source, target, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT (spider, source, target) DO UPDATE SET last_seen = excluded.last_seen,
                       removed_at = NULL""",
                (spider, source, target, now, now),
            )
        marks = ", ".join("?" * len(current))
        exclusion = f"AND target NOT IN ({marks})" if current else ""
        cursor = self.db.execute(
            f"""UPDATE links SET removed_at = ? WHERE spider = ? AND source = ? AND removed_at IS NULL {exclusion}""",
            (now, spider, source, *current),
        )
        return len(current), cursor.rowcount

    def inbound(self, spider: str, target: str, *, include_removed: bool = False) -> list[str]:
        query = "SELECT source FROM links WHERE spider = ? AND target = ?"
        if not include_removed:
            query += " AND removed_at IS NULL"
        return [row[0] for row in self.db.execute(query + " ORDER BY source", (spider, target))]

    def orphans(self, spider: str) -> list[str]:
        rows = self.db.execute(
            f"SELECT f.url FROM frontier f WHERE f.spider = ? AND {orphan_condition('f')} ORDER BY f.url", (spider,)
        )
        return [row[0] for row in rows]
