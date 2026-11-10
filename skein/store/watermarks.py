"""Per-seed watermarks: how far each crawl root has been completely processed."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class Watermark:
    """State of one crawl root (a seed page or a sitemap).

    ``last_completed`` is the start time of the most recent run that
    processed everything reachable from the root; ``lastmod_high`` is the
    newest sitemap ``<lastmod>`` handled below a sitemap root.
    """

    spider: str
    seed: str
    kind: str
    last_run_id: int | None
    last_started: float | None
    last_completed: float | None
    lastmod_high: float | None


class WatermarkStore:
    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def get(self, spider: str, seed: str) -> Watermark | None:
        row = self.db.execute("SELECT * FROM watermarks WHERE spider = ? AND seed = ?", (spider, seed)).fetchone()
        return _from_row(row) if row is not None else None

    def all(self, spider: str) -> list[Watermark]:
        rows = self.db.execute("SELECT * FROM watermarks WHERE spider = ? ORDER BY seed", (spider,))
        return [_from_row(row) for row in rows]

    def started(self, spider: str, seed: str, kind: str, run_id: int, started_at: float) -> None:
        """Note that ``run_id`` began working on ``seed`` (completion is recorded separately)."""
        self.db.execute(
            """INSERT INTO watermarks (spider, seed, kind, last_run_id, last_started) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT (spider, seed) DO UPDATE SET kind = excluded.kind,
                   last_run_id = excluded.last_run_id, last_started = excluded.last_started""",
            (spider, seed, kind, run_id, started_at),
        )

    def note_lastmod(self, spider: str, seed: str, run_id: int, lastmod: float) -> None:
        """Raise the newest sitemap ``lastmod`` that run ``run_id`` handled below ``seed``.

        Kept per run in the table (not in a process) because every worker of
        the run contributes; :meth:`completed` folds it into the watermark.
        """
        self.db.execute(
            """UPDATE watermarks SET
                   run_lastmod_high = CASE WHEN run_lastmod_run = :run
                                           THEN MAX(COALESCE(run_lastmod_high, :value), :value) ELSE :value END,
                   run_lastmod_run = :run
               WHERE spider = :spider AND seed = :seed""",
            {"run": run_id, "value": lastmod, "spider": spider, "seed": seed},
        )

    def run_lastmod(self, spider: str, seed: str, run_id: int) -> float | None:
        row = self.db.execute(
            "SELECT run_lastmod_high FROM watermarks WHERE spider = ? AND seed = ? AND run_lastmod_run = ?",
            (spider, seed, run_id),
        ).fetchone()
        return row[0] if row is not None else None

    def completed(self, spider: str, seed: str, run_started: float, lastmod_high: float | None) -> None:
        """Advance the watermark of ``seed``; ``lastmod_high`` never moves backwards."""
        self.db.execute(
            """UPDATE watermarks SET last_completed = MAX(COALESCE(last_completed, 0), ?),
                   lastmod_high = CASE WHEN ? IS NULL THEN lastmod_high
                                       ELSE MAX(COALESCE(lastmod_high, ?), ?) END
               WHERE spider = ? AND seed = ?""",
            (run_started, lastmod_high, lastmod_high, lastmod_high, spider, seed),
        )


def _from_row(row: sqlite3.Row) -> Watermark:
    return Watermark(
        row["spider"], row["seed"], row["kind"], row["last_run_id"], row["last_started"],
        row["last_completed"], row["lastmod_high"],
    )
