"""Persisted robots.txt cache entries (one row per origin)."""

from __future__ import annotations

import sqlite3
from typing import Any

COLUMNS = ("origin", "state", "status", "body", "fetched_at", "expires_at", "etag", "last_modified", "error")


class RobotsStore:
    """SQLite backend for :class:`skein.robots.cache.RobotsCache`."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def load(self, origin: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM robots_cache WHERE origin = ?", (origin,)).fetchone()
        return {column: row[column] for column in COLUMNS} if row is not None else None

    def save(self, record: dict[str, Any]) -> None:
        values = [record[column] for column in COLUMNS]
        self.db.execute(
            f"INSERT OR REPLACE INTO robots_cache ({', '.join(COLUMNS)}) VALUES ({', '.join('?' * len(COLUMNS))})",
            values,
        )

    def origins(self) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT origin, state, status, fetched_at, expires_at, etag, last_modified FROM robots_cache ORDER BY origin"
        )
        return [dict(row) for row in rows]
