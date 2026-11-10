"""The ``runs`` table: one row per crawl run, resumed runs keep their row."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

RUN_STATUSES = ("running", "finished", "budget_exhausted", "stopped", "failed", "interrupted", "abandoned")


class RunLog:
    """Creates, resumes and closes run records."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def begin(self, spider: str, started_at: float, mode: str) -> int:
        cursor = self.db.execute(
            "INSERT INTO runs (spider, started_at, mode) VALUES (?, ?, ?)", (spider, started_at, mode)
        )
        return int(cursor.lastrowid)

    def resume(self, run_id: int) -> None:
        """Re-open an interrupted or stopped run."""
        cursor = self.db.execute(
            "UPDATE runs SET status = 'running', finished_at = NULL, resumes = resumes + 1 WHERE id = ?", (run_id,)
        )
        if cursor.rowcount != 1:
            raise KeyError(f"no run with id {run_id}")

    def finish(self, run_id: int, finished_at: float, status: str, stats: dict[str, Any] | None) -> None:
        if status not in RUN_STATUSES:
            raise ValueError(f"unknown run status {status!r}")
        self.db.execute(
            "UPDATE runs SET finished_at = ?, status = ?, stats = COALESCE(?, stats) WHERE id = ?",
            (finished_at, status, json.dumps(stats, sort_keys=True) if stats is not None else None, run_id),
        )

    def get(self, run_id: int) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return _decode(row) if row is not None else None

    def list(self, spider: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM runs" + (" WHERE spider = ?" if spider else "") + " ORDER BY id"
        return [_decode(row) for row in self.db.execute(query, (spider,) if spider else ())]

    def open_runs(self, spider: str) -> list[dict[str, Any]]:
        """Runs of ``spider`` that never reached a final status (the process died) or were stopped."""
        rows = self.db.execute(
            "SELECT * FROM runs WHERE spider = ? AND status IN ('running', 'stopped') ORDER BY id", (spider,)
        )
        return [_decode(row) for row in rows]


def _decode(row: sqlite3.Row) -> dict[str, Any]:
    record = dict(row)
    record["stats"] = json.loads(record["stats"]) if record["stats"] else None
    return record
