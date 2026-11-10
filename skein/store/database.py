"""Opening SQLite connections and running transactions."""

from __future__ import annotations

import sqlite3
from pathlib import Path


class StoreError(RuntimeError):
    """The database cannot be used (newer schema, corrupt file, failed migration, ...)."""


SYNCHRONOUS_LEVELS = ("normal", "full")


def connect(path: str | Path, *, synchronous: str = "normal") -> sqlite3.Connection:
    """Open ``path`` in autocommit mode with WAL journaling and skein's pragmas.

    Transactions are always explicit (:class:`Transaction`), so the
    connection uses ``isolation_level=None``. ``synchronous="full"`` makes
    every commit durable against power loss (``normal`` in WAL mode only
    guarantees consistency; the last commits may be lost). Raises
    :class:`StoreError` if the file is not a usable SQLite database.
    """
    if synchronous not in SYNCHRONOUS_LEVELS:
        raise ValueError(f"synchronous must be one of {SYNCHRONOUS_LEVELS}")
    location = str(path)
    if location != ":memory:":
        Path(location).parent.mkdir(parents=True, exist_ok=True)
    try:
        db = sqlite3.connect(location, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(f"PRAGMA synchronous={synchronous.upper()}")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA busy_timeout=5000")
        db.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    except sqlite3.DatabaseError as exc:
        raise StoreError(f"cannot open database {location}: {exc}") from exc
    return db


def is_busy(exc: BaseException) -> bool:
    """Whether ``exc`` is SQLite giving up on a lock another process holds (worth retrying later)."""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message


class Transaction:
    """``BEGIN IMMEDIATE`` ... ``COMMIT``/``ROLLBACK`` around a block.

    Nested use joins the outer transaction, so repository methods can be
    composed into one atomic unit by wrapping them in an outer block.
    """

    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db
        self._owner = False

    def __enter__(self) -> sqlite3.Connection:
        if not self._db.in_transaction:
            self._db.execute("BEGIN IMMEDIATE")
            self._owner = True
        return self._db

    def __exit__(self, exc_type: object, *_: object) -> None:
        if not self._owner:
            return
        self._owner = False
        self._db.execute("ROLLBACK" if exc_type is not None else "COMMIT")


def statements(script: str) -> list[str]:
    """Split a DDL script on ``;`` (the scripts used here contain no string literals with ``;``)."""
    return [s.strip() for s in script.split(";") if s.strip()]


def execute_script(db: sqlite3.Connection, script: str) -> None:
    """Run each statement of ``script`` inside the caller's transaction.

    ``sqlite3.Connection.executescript`` would commit first, which must not
    happen in the middle of a migration.
    """
    for statement in statements(script):
        db.execute(statement)


def table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    return row is not None
