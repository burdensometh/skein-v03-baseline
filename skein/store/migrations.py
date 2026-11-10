"""A versioned schema migration framework for SQLite.

The schema version lives in ``PRAGMA user_version``; every applied
migration is also recorded in the ``schema_migrations`` table with its
name, time and duration. Each migration runs in its own ``BEGIN IMMEDIATE``
transaction with foreign-key enforcement suspended (table rebuilds need
that) and a ``foreign_key_check`` before commit, so a failing migration
leaves the database exactly at the previous version. Concurrent openers are
safe: the version is re-read after the write lock is taken.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from skein.store.database import StoreError, execute_script, table_exists

HISTORY_TABLE = "schema_migrations"


class MigrationError(StoreError):
    """A migration failed or the database schema is newer than this skein understands."""


@dataclass(frozen=True)
class Migration:
    """One schema step from ``version - 1`` to ``version``.

    ``apply`` is either a DDL script or a function that receives the
    connection (for steps that move data).
    """

    version: int
    name: str
    apply: str | Callable[[sqlite3.Connection], None]
    description: str = ""

    def run(self, db: sqlite3.Connection) -> None:
        if isinstance(self.apply, str):
            execute_script(db, self.apply)
        else:
            self.apply(db)


@dataclass(frozen=True)
class AppliedMigration:
    version: int
    name: str
    applied_at: float | None
    duration: float | None


@dataclass(frozen=True)
class SchemaStatus:
    """What :func:`inspect_database` reports without changing anything."""

    path: str
    version: int
    latest: int
    pending: tuple[Migration, ...]
    history: tuple[AppliedMigration, ...]
    tables: dict[str, int]


def check_sequence(migrations: tuple[Migration, ...]) -> None:
    """Versions must be exactly ``1..n`` in order and names unique."""
    versions = [m.version for m in migrations]
    if versions != list(range(1, len(migrations) + 1)):
        raise ValueError(f"migration versions must be 1..{len(migrations)} in order, got {versions}")
    names = [m.name for m in migrations]
    if len(set(names)) != len(names):
        raise ValueError("migration names must be unique")


class Migrator:
    """Brings one connection's schema up to date."""

    def __init__(
        self,
        db: sqlite3.Connection,
        migrations: tuple[Migration, ...],
        *,
        now: Callable[[], float] = time.time,
    ) -> None:
        check_sequence(migrations)
        self.db = db
        self.migrations = migrations
        self._now = now
        self.last_backup: Path | None = None

    @property
    def latest(self) -> int:
        return len(self.migrations)

    def current_version(self) -> int:
        return int(self.db.execute("PRAGMA user_version").fetchone()[0])

    def pending(self) -> tuple[Migration, ...]:
        version = self.current_version()
        return tuple(m for m in self.migrations if m.version > version)

    def history(self) -> tuple[AppliedMigration, ...]:
        if not table_exists(self.db, HISTORY_TABLE):
            return ()
        rows = self.db.execute(
            f"SELECT version, name, applied_at, duration FROM {HISTORY_TABLE} ORDER BY version"
        ).fetchall()
        return tuple(AppliedMigration(r[0], r[1], r[2], r[3]) for r in rows)

    def migrate(self, *, target: int | None = None, backup_path: Path | None = None) -> list[Migration]:
        """Apply pending migrations up to ``target`` (default: latest); returns those applied.

        With ``backup_path`` an online backup of the database is written
        first, but only if the database already holds a schema and something
        is pending.
        """
        version = self.current_version()
        if version > self.latest:
            raise MigrationError(
                f"database has schema version {version}; this skein supports up to {self.latest}"
            )
        goal = self.latest if target is None else target
        if not 0 <= goal <= self.latest:
            raise MigrationError(f"no such schema version {goal}")
        if goal < version:
            raise MigrationError(f"cannot downgrade schema from version {version} to {goal}")
        todo = [m for m in self.migrations if version < m.version <= goal]
        if todo and version > 0 and backup_path is not None:
            self.last_backup = backup_database(self.db, backup_path)
        applied = []
        for migration in todo:
            if self._apply(migration):
                applied.append(migration)
        return applied

    def _apply(self, migration: Migration) -> bool:
        db = self.db
        if db.in_transaction:
            raise MigrationError("cannot migrate inside an open transaction")
        db.execute("PRAGMA foreign_keys=OFF")
        # Migrations address columns by name, whatever the caller's connection uses.
        row_factory, db.row_factory = db.row_factory, sqlite3.Row
        started = time.perf_counter()
        try:
            db.execute("BEGIN IMMEDIATE")
            try:
                if self.current_version() >= migration.version:
                    db.execute("ROLLBACK")
                    return False
                self._ensure_history(migration.version)
                migration.run(db)
                violations = db.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    raise MigrationError(f"{len(violations)} foreign key violation(s), first in table {violations[0][0]}")
                db.execute(
                    f"INSERT INTO {HISTORY_TABLE} (version, name, applied_at, duration) VALUES (?, ?, ?, ?)",
                    (migration.version, migration.name, self._now(), time.perf_counter() - started),
                )
                db.execute(f"PRAGMA user_version={migration.version}")
                db.execute("COMMIT")
            except BaseException:
                if db.in_transaction:
                    db.execute("ROLLBACK")
                raise
        except MigrationError:
            raise
        except sqlite3.Error as exc:
            raise MigrationError(f"migration {migration.version} ({migration.name}) failed: {exc}") from exc
        finally:
            db.row_factory = row_factory
            db.execute("PRAGMA foreign_keys=ON")
        return True

    def _ensure_history(self, version: int) -> None:
        """Create the history table; databases older than it get their earlier versions recorded as pre-existing."""
        if table_exists(self.db, HISTORY_TABLE):
            return
        self.db.execute(
            f"""CREATE TABLE {HISTORY_TABLE} (
                version    INTEGER PRIMARY KEY,
                name       TEXT NOT NULL,
                applied_at REAL,
                duration   REAL
            )"""
        )
        for earlier in self.migrations[: version - 1]:
            self.db.execute(
                f"INSERT INTO {HISTORY_TABLE} (version, name, applied_at, duration) VALUES (?, ?, NULL, NULL)",
                (earlier.version, earlier.name),
            )


def dry_run(db: sqlite3.Connection, migrations: tuple[Migration, ...]) -> tuple[list[Migration], dict[str, int]]:
    """Apply pending migrations to an in-memory copy of ``db``; returns them and the resulting table sizes.

    The real database is only read, so this shows exactly what
    :meth:`Migrator.migrate` would do, including failures, without risk.
    """
    copy = sqlite3.connect(":memory:")
    try:
        db.backup(copy)
        applied = Migrator(copy, migrations).migrate()
        names = [r[0] for r in copy.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        return applied, {name: int(copy.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]) for name in names}
    finally:
        copy.close()


def unused_path(path: Path) -> Path:
    """``path`` itself, or ``path.1``, ``path.2``, ... whichever does not exist yet."""
    if not path.exists():
        return path
    counter = 1
    while path.with_name(f"{path.name}.{counter}").exists():
        counter += 1
    return path.with_name(f"{path.name}.{counter}")


def backup_database(db: sqlite3.Connection, destination: Path) -> Path:
    """Write a consistent copy of ``db`` to ``destination`` (never overwriting an older backup)."""
    target = unused_path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    copy = sqlite3.connect(str(target))
    try:
        db.backup(copy)
    finally:
        copy.close()
    return target


def inspect_database(path: str | Path, migrations: tuple[Migration, ...]) -> SchemaStatus:
    """Read schema version, history and table sizes without migrating."""
    location = Path(path)
    if not location.exists():
        raise StoreError(f"database {location} does not exist")
    db = sqlite3.connect(f"file:{location}?mode=ro", uri=True)
    try:
        migrator = Migrator(db, migrations)
        names = [r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        tables = {name: int(db.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]) for name in names}
        return SchemaStatus(
            str(location), migrator.current_version(), migrator.latest,
            migrator.pending() if migrator.current_version() <= migrator.latest else (),
            migrator.history(), tables,
        )
    except sqlite3.DatabaseError as exc:
        raise StoreError(f"cannot read database {location}: {exc}") from exc
    finally:
        db.close()
