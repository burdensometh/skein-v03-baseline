"""Named export cursors: how far a downstream consumer has read the item change feed."""

from __future__ import annotations

import sqlite3


class CursorStore:
    """Positions are ``item_versions`` row ids, which only grow."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def get(self, name: str, spider: str) -> int:
        row = self.db.execute(
            "SELECT position FROM export_cursors WHERE name = ? AND spider = ?", (name, spider)
        ).fetchone()
        return int(row[0]) if row is not None else 0

    def advance(self, name: str, spider: str, position: int, now: float) -> None:
        """Move the cursor forward (never backwards) after the consumer received everything up to ``position``."""
        self.db.execute(
            """INSERT INTO export_cursors (name, spider, position, updated_at) VALUES (?, ?, ?, ?)
               ON CONFLICT (name, spider) DO UPDATE SET position = MAX(position, excluded.position),
                   updated_at = excluded.updated_at""",
            (name, spider, position, now),
        )

    def all(self) -> list[dict[str, object]]:
        return [dict(row) for row in self.db.execute("SELECT * FROM export_cursors ORDER BY spider, name")]
