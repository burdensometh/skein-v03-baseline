"""The ``workers`` table: every process that claims frontier work registers here.

A worker row records where the process runs (host name and pid, so that
peers on the same machine can tell a dead process from a slow one), which
run it works for, when it last renewed its leases (``heartbeat_at``),
where its checkpoint lives and, once it stops, its final run statistics.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

WORKER_STATES = ("running", "stopped", "finished", "dead")


@dataclass(frozen=True)
class WorkerRecord:
    id: str
    spider: str
    run_id: int | None
    hostname: str
    pid: int
    role: str
    state: str
    started_at: float
    heartbeat_at: float
    stopped_at: float | None
    checkpoint: str | None
    stats: dict[str, Any] | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "WorkerRecord":
        return cls(row["id"], row["spider"], row["run_id"], row["hostname"], row["pid"], row["role"], row["state"],
                   row["started_at"], row["heartbeat_at"], row["stopped_at"], row["checkpoint"],
                   json.loads(row["stats"]) if row["stats"] else None)

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "spider": self.spider, "run_id": self.run_id, "hostname": self.hostname,
                "pid": self.pid, "role": self.role, "state": self.state, "started_at": self.started_at,
                "heartbeat_at": self.heartbeat_at, "stopped_at": self.stopped_at, "checkpoint": self.checkpoint}


class WorkerRegistry:
    """Repository for ``workers``; each method is its own small transaction unless one is open."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def register(
        self, worker_id: str, spider: str, run_id: int | None, *, hostname: str, pid: int, role: str,
        checkpoint: str | None, now: float,
    ) -> None:
        """Insert or take over the row of ``worker_id`` (a restarted worker keeps its id)."""
        self.db.execute(
            """INSERT INTO workers (id, spider, run_id, hostname, pid, role, state, started_at, heartbeat_at,
                                    checkpoint)
               VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?, ?)
               ON CONFLICT (id) DO UPDATE SET spider = excluded.spider, run_id = excluded.run_id,
                   hostname = excluded.hostname, pid = excluded.pid, role = excluded.role, state = 'running',
                   started_at = excluded.started_at, heartbeat_at = excluded.heartbeat_at, stopped_at = NULL,
                   checkpoint = excluded.checkpoint, stats = NULL""",
            (worker_id, spider, run_id, hostname, pid, role, now, now, checkpoint),
        )

    def heartbeat(self, worker_id: str, now: float) -> bool:
        cursor = self.db.execute(
            "UPDATE workers SET heartbeat_at = ? WHERE id = ? AND state = 'running'", (now, worker_id))
        return cursor.rowcount == 1

    def finish(self, worker_id: str, state: str, now: float, stats: dict[str, Any] | None) -> None:
        if state not in WORKER_STATES:
            raise ValueError(f"unknown worker state {state!r}")
        self.db.execute(
            "UPDATE workers SET state = ?, stopped_at = ?, stats = COALESCE(?, stats) WHERE id = ?",
            (state, now, json.dumps(stats, sort_keys=True) if stats is not None else None, worker_id),
        )

    def get(self, worker_id: str) -> WorkerRecord | None:
        row = self.db.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()
        return WorkerRecord.from_row(row) if row is not None else None

    def list(self, *, spider: str | None = None, run_id: int | None = None,
             states: tuple[str, ...] = ()) -> list[WorkerRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if spider is not None:
            clauses.append("spider = ?")
            params.append(spider)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if states:
            clauses.append(f"state IN ({', '.join('?' * len(states))})")
            params.extend(states)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.execute(f"SELECT * FROM workers {where} ORDER BY started_at, id", params)
        return [WorkerRecord.from_row(row) for row in rows]

    def leases_held(self) -> dict[str, int]:
        """Active frontier rows per lease owner."""
        rows = self.db.execute(
            "SELECT lease_owner, COUNT(*) FROM frontier WHERE state = 'active' AND lease_owner IS NOT NULL "
            "GROUP BY lease_owner")
        return {row[0]: row[1] for row in rows}
