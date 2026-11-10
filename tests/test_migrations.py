"""The migration framework, and upgrading a real database written by skein 0.1.

``tests/data/v01_shop.db`` was produced by the v0.1 code (two crawl runs of
the fixture shop, including a retried 503, 404s and a robots.txt denial).
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from skein.cli import main
from skein.clock import FakeClock
from skein.store import LATEST_VERSION, MIGRATIONS, Migration, MigrationError, Migrator, Store, inspect_database
from skein.store.migrations import backup_database, check_sequence, dry_run

V01_DB = Path(__file__).parent / "data" / "v01_shop.db"


@pytest.fixture
def v01(tmp_path: Path) -> Path:
    target = tmp_path / "legacy.db"
    shutil.copyfile(V01_DB, target)
    return target


def raw_rows(path: Path, query: str) -> list[tuple]:
    db = sqlite3.connect(path)
    try:
        return db.execute(query).fetchall()
    finally:
        db.close()


def test_fixture_is_a_v01_database() -> None:
    assert raw_rows(V01_DB, "PRAGMA user_version") == [(1,)]
    tables = {r[0] for r in raw_rows(V01_DB, "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"runs", "pages", "items"} <= tables and "fetch_events" not in tables


def test_v01_database_is_upgraded_on_open(v01: Path) -> None:
    pages_before = raw_rows(v01, "SELECT url, status, error, fetched_at FROM pages ORDER BY id")
    items_before = raw_rows(v01, "SELECT item_key, data, first_seen, last_seen FROM items ORDER BY item_key")
    with Store(v01) as store:
        assert [m.version for m in store.applied] == list(range(2, LATEST_VERSION + 1))
        assert store.backup_path == v01.with_name("legacy.db.schema-v1.bak")
        events = list(store.documents.events("shop"))
        assert [(e["url"], e["status"], e["error"], e["fetched_at"]) for e in events] == pages_before
        classes = {e["error"]: e["error_class"] for e in events if e["error"]}
        assert classes["HTTP 404"] == "permanent"
        assert classes["HTTP 503 (will retry)"] == "transient"
        assert next(c for err, c in classes.items() if err.startswith("robots.txt")) == "robots"

        documents = {d.url.rsplit("/", 1)[1] or "/": d for d in store.documents.iter_documents("shop")}
        assert len(documents) == len({row[0] for row in pages_before})
        flaky = documents["x?fail=1"]
        assert flaky.status == 200 and flaky.fetch_count == 3 and flaky.last_error is None
        assert documents["404"].status == 404 and documents["404"].error_class == "permanent"
        assert documents["404"].last_success is None
        secret = documents["secret"]
        assert secret.error_class == "robots" and secret.fetch_count == 0
        assert documents["1"].etag is None and documents["1"].fingerprint is None

        items = {i.key: i for i in store.items.iter("shop")}
        assert [(k, items[k].first_seen, items[k].last_seen) for k in sorted(items)] == [
            (key, first, last) for key, _, first, last in items_before]
        assert all(i.version == 1 and len(i.content_hash) == 64 for i in items.values())
        assert store.items.count_versions("shop") == len(items)
        assert {k: json.loads(data) for k, data, _, _ in items_before} == {k: i.data for k, i in items.items()}

        frontier = dict(store.db.execute("SELECT state, COUNT(*) FROM frontier GROUP BY state").fetchall())
        # Two 404s and the robots.txt denial are settled as failed; everything else is done.
        assert frontier == {"done": len(documents) - 3, "failed": 3}
        runs = store.runs.list("shop")
        assert [(r["status"], r["mode"], r["resumes"]) for r in runs] == [("finished", "full", 0)] * 2
    assert raw_rows(v01.with_name("legacy.db.schema-v1.bak"), "PRAGMA user_version") == [(1,)]
    tables = {r[0] for r in raw_rows(v01, "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "pages" not in tables


def test_migrated_database_keeps_working_with_incremental_semantics(v01: Path) -> None:
    from skein.extract import ExtractedItem
    from skein.store import FetchEvent

    with Store(v01) as store:
        (item,) = [i for i in store.items.iter("shop") if i.key == "SKU-0001"]
        same = ExtractedItem("product", item.key, dict(item.data), item.url)
        write = store.record_fetch(FetchEvent("shop", item.url, 1_800_000_000.0, status=200, fingerprint="f"), [same])
        assert write.items.unchanged == 1, "content hashes computed during migration match new hashes"
        changed = ExtractedItem("product", item.key, {**item.data, "price": 1.0}, item.url)
        write = store.record_fetch(FetchEvent("shop", item.url, 1_800_000_100.0, status=200, fingerprint="g"),
                                   [changed])
        assert write.items.updated == 1
        assert [v.version for v in store.items.versions("shop", key="SKU-0001")] == [1, 2]
        state = store.documents.get("shop", item.url)
        assert state is not None and state.check_count == 1 and state.change_count == 1


def test_migration_history_and_reopen(v01: Path) -> None:
    Store(v01).close()
    status = inspect_database(v01, MIGRATIONS)
    assert status.version == status.latest == LATEST_VERSION and status.pending == ()
    assert status.history[0].name == "baseline" and status.history[0].applied_at is None
    assert all(entry.applied_at is not None for entry in status.history[1:])
    with Store(v01) as store:
        assert store.applied == [] and store.backup_path is None


def test_dry_run_leaves_the_database_alone(v01: Path) -> None:
    db = sqlite3.connect(f"file:{v01}?mode=ro", uri=True)
    try:
        applied, tables = dry_run(db, MIGRATIONS)
    finally:
        db.close()
    assert [m.name for m in applied] == [m.name for m in MIGRATIONS[1:]]
    assert tables["fetch_events"] == 35 and "pages" not in tables and tables["item_history"] == 6
    assert raw_rows(v01, "PRAGMA user_version") == [(1,)]


def test_cli_db_status_and_migrate(v01: Path, capsys: pytest.CaptureFixture[str]) -> None:
    import io

    out = io.StringIO()
    assert main(["db", "status", "--db", str(v01)], out=out) == 0
    text = out.getvalue()
    assert "schema version 1 (latest" in text and "pending 2 split_pages" in text and "table pages" in text
    out = io.StringIO()
    assert main(["db", "migrate", "--dry-run", "--db", str(v01)], out=out) == 0
    assert f"{LATEST_VERSION - 1} migration(s) would be applied" in out.getvalue()
    out = io.StringIO()
    assert main(["db", "migrate", "--db", str(v01)], out=out) == 0
    assert f"schema version 1 -> {LATEST_VERSION}" in out.getvalue() and "backup written" in out.getvalue()
    out = io.StringIO()
    assert main(["db", "migrate", "--db", str(v01), "--no-backup"], out=out) == 0
    assert f"schema version {LATEST_VERSION} -> {LATEST_VERSION}" in out.getvalue()


def _toy(versions: list[tuple[int, str, str]]) -> tuple[Migration, ...]:
    return tuple(Migration(v, name, lambda db, sql=sql: db.execute(sql)) for v, name, sql in versions)


def test_failed_migration_rolls_back_to_previous_version(tmp_path: Path) -> None:
    db = sqlite3.connect(tmp_path / "toy.db", isolation_level=None)
    migrations = _toy([(1, "one", "CREATE TABLE a (x)"), (2, "two", "CREATE TABLE b (y)"),
                       (3, "broken", "INSERT INTO missing VALUES (1)")])
    migrator = Migrator(db, migrations, now=FakeClock().time)
    with pytest.raises(MigrationError, match="migration 3 \\(broken\\) failed"):
        migrator.migrate()
    assert migrator.current_version() == 2
    assert [h.name for h in migrator.history()] == ["one", "two"]
    assert db.execute("PRAGMA foreign_keys").fetchone() == (1,), "enforcement restored after failure"


def test_migrate_to_target_and_refuse_downgrade(tmp_path: Path) -> None:
    db = sqlite3.connect(tmp_path / "toy.db", isolation_level=None)
    migrations = _toy([(1, "one", "CREATE TABLE a (x)"), (2, "two", "CREATE TABLE b (y)")])
    migrator = Migrator(db, migrations)
    assert [m.name for m in migrator.migrate(target=1)] == ["one"]
    assert [m.name for m in migrator.pending()] == ["two"]
    assert [m.name for m in migrator.migrate()] == ["two"]
    with pytest.raises(MigrationError, match="cannot downgrade"):
        migrator.migrate(target=1)
    with pytest.raises(MigrationError, match="no such schema version"):
        migrator.migrate(target=5)
    db.execute("BEGIN")
    db.execute("PRAGMA user_version=1")
    with pytest.raises(MigrationError, match="inside an open transaction"):
        Migrator(db, migrations).migrate()


def test_concurrent_openers_apply_each_migration_once(tmp_path: Path) -> None:
    path = tmp_path / "race.db"
    first = sqlite3.connect(path, isolation_level=None)
    second = sqlite3.connect(path, isolation_level=None)
    migrations = _toy([(1, "one", "CREATE TABLE a (x)")])
    late = Migrator(second, migrations)
    assert late.pending() == migrations
    Migrator(first, migrations).migrate()
    assert late.migrate() == [], "the version is re-checked under the write lock"


def test_foreign_key_violations_abort_a_migration(tmp_path: Path) -> None:
    db = sqlite3.connect(tmp_path / "fk.db", isolation_level=None)
    migrations = _toy([
        (1, "parents", "CREATE TABLE parent (id INTEGER PRIMARY KEY)"),
        (2, "children", "CREATE TABLE child (p INTEGER REFERENCES parent(id))"),
        (3, "orphan", "INSERT INTO child VALUES (42)"),
    ])
    with pytest.raises(MigrationError, match="foreign key violation"):
        Migrator(db, migrations).migrate()
    assert db.execute("SELECT COUNT(*) FROM child").fetchone() == (0,)


def test_sequence_checks_and_backups(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="1..2"):
        check_sequence(_toy([(1, "a", "SELECT 1"), (3, "b", "SELECT 1")]))
    with pytest.raises(ValueError, match="unique"):
        check_sequence(_toy([(1, "a", "SELECT 1"), (2, "a", "SELECT 1")]))
    db = sqlite3.connect(tmp_path / "src.db")
    db.execute("CREATE TABLE t (x)")
    db.commit()
    first = backup_database(db, tmp_path / "copy.bak")
    second = backup_database(db, tmp_path / "copy.bak")
    assert first.name == "copy.bak" and second.name == "copy.bak.1", "older backups are never overwritten"


def test_newer_database_is_reported_by_inspect(tmp_path: Path) -> None:
    path = tmp_path / "future.db"
    db = sqlite3.connect(path)
    db.execute("PRAGMA user_version=99")
    db.close()
    status = inspect_database(path, MIGRATIONS)
    assert status.version == 99 and status.pending == ()
