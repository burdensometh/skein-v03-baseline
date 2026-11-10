"""SQLite storage semantics (fetch events, documents, item versions) and JSONL/CSV export."""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from pathlib import Path

import pytest

from skein.export import (
    EVENT_COLUMNS,
    ITEM_COLUMNS,
    VERSION_COLUMNS,
    document_record,
    event_record,
    item_record,
    version_record,
    write_csv,
    write_jsonl,
)
from skein.extract import ExtractedItem
from skein.incremental import RevisitPolicy
from skein.store import LATEST_VERSION, FetchEvent, Store, StoreError


def item(key: str, **data: object) -> ExtractedItem:
    return ExtractedItem("product", key, dict(data), f"http://shop.test/{key}")


def page(url: str, at: float, **kw: object) -> FetchEvent:
    kw.setdefault("status", 200)
    return FetchEvent(spider="shop", url=url, fetched_at=at, **kw)  # type: ignore[arg-type]


@pytest.fixture
def store(tmp_path: Path):  # noqa: ANN201
    with Store(tmp_path / "db" / "skein.db") as db:
        yield db


def test_schema_and_wal(store: Store, tmp_path: Path) -> None:
    raw = sqlite3.connect(tmp_path / "db" / "skein.db")
    assert raw.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert raw.execute("PRAGMA user_version").fetchone()[0] == LATEST_VERSION
    tables = {r[0] for r in raw.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"runs", "fetch_events", "documents", "item_history", "frontier", "watermarks", "workers", "host_state",
            "host_slots", "simhash_bands", "canonical_rules", "schema_migrations"} <= tables
    assert "item_versions" not in tables
    views = {r[0] for r in raw.execute("SELECT name FROM sqlite_master WHERE type='view'")}
    assert views == {"items"}
    assert "pages" not in tables
    assert store.applied and store.backup_path is None, "a new database needs no backup"


def test_item_versions_only_on_content_change(store: Store) -> None:
    first = store.record_fetch(page("http://shop.test/a", 100.0), [item("a", price=1), item("b", price=2)])
    assert (first.items.new, first.items.updated, first.items.unchanged) == (2, 0, 0)
    second = store.record_fetch(page("http://shop.test/a", 200.0), [item("a", price=1), item("b", price=3)])
    assert (second.items.new, second.items.updated, second.items.unchanged) == (0, 1, 1)
    third = store.record_fetch(page("http://shop.test/a", 300.0), [item("b", price=3)])
    assert third.items.unchanged == 1
    stored = {i.key: i for i in store.items.iter("shop")}
    assert stored["a"].first_seen == 100.0 and stored["a"].last_seen == 200.0 and stored["a"].version == 1
    assert stored["b"].data == {"price": 3} and stored["b"].version == 2 and stored["b"].changed_at == 200.0
    history = list(store.items.versions("shop", key="b"))
    assert [(v.version, v.data, v.observed_at) for v in history] == [(1, {"price": 2}, 100.0), (2, {"price": 3}, 200.0)]
    assert history[0].content_hash != history[1].content_hash
    assert history[1].fetch_event_id is not None
    assert store.items.count("shop") == 2 and store.items.count_versions("shop") == 3


def test_key_order_does_not_create_versions(store: Store) -> None:
    store.record_fetch(page("http://x/", 1.0), [ExtractedItem("product", "k", {"a": 1, "b": [1, 2]}, "http://x/")])
    write = store.record_fetch(page("http://x/", 2.0),
                               [ExtractedItem("product", "k", {"b": [1, 2], "a": 1}, "http://x/")])
    assert write.items.unchanged == 1 and store.items.count_versions("shop") == 1


def test_item_key_scoped_by_type_and_spider(store: Store) -> None:
    store.record_fetch(page("http://x/", 1.0), [item("k", v=1), ExtractedItem("review", "k", {"v": 2}, "http://x/")])
    store.record_fetch(FetchEvent("other", "http://x/", 1.0, status=200), [item("k", v=3)])
    assert [(i.item_type, i.data["v"]) for i in store.items.iter("shop")] == [("product", 1), ("review", 2)]
    assert [i.data for i in store.items.iter("shop", item_type="review")] == [{"v": 2}]
    assert store.spiders() == ["other", "shop"]


def test_since_and_changed_since_filters(store: Store) -> None:
    store.record_fetch(page("http://x/1", 10.0), [item("old", n=1)])
    store.record_fetch(page("http://x/2", 20.0), [item("new", n=1)])
    store.record_fetch(page("http://x/1", 30.0), [item("old", n=1)])
    assert [i.key for i in store.items.iter("shop", since=15.0)] == ["new", "old"]
    assert [i.key for i in store.items.iter("shop", changed_since=15.0)] == ["new"]
    assert [v.key for v in store.items.versions("shop", since=15.0)] == ["new"]


def test_document_state_tracks_validators_and_changes(store: Store) -> None:
    revisit = RevisitPolicy(initial_interval=100.0, min_interval=10.0, max_interval=1000.0)
    first = store.record_fetch(page("http://x/", 0.0, etag='"a"', fingerprint="f1"), revisit=revisit)
    assert first.document.new and first.document.changed is None and first.document.interval == 100.0
    unchanged = store.record_fetch(page("http://x/", 100.0, status=304, conditional=True), revisit=revisit)
    assert unchanged.document.changed is False and not unchanged.document.new
    changed = store.record_fetch(page("http://x/", 300.0, etag='"b"', fingerprint="f2"), revisit=revisit)
    assert changed.document.changed is True
    failed = store.record_fetch(page("http://x/", 310.0, status=503, error="HTTP 503", error_class="transient"),
                                revisit=revisit)
    assert failed.document.changed is None
    state = store.documents.get("shop", "http://x/")
    assert state is not None
    assert (state.etag, state.fingerprint, state.status) == ('"b"', "f2", 503)
    assert (state.check_count, state.change_count, state.fetch_count) == (2, 1, 4)
    assert state.last_success == 300.0 and state.last_changed == 300.0 and state.observed_span == 300.0
    assert state.error_class == "transient" and state.next_visit == 310.0 + revisit.min_interval
    events = list(store.documents.events("shop", url="http://x/"))
    assert [e["changed"] for e in events] == [None, False, True, None]
    assert events[1]["conditional"] is True


def test_not_modified_touches_items_without_versions(store: Store) -> None:
    store.record_fetch(page("http://x/p", 1.0, fingerprint="f"), [item("p", price=1)])
    write = store.record_fetch(page("http://x/p", 5.0, status=304))
    assert write.items.touched == 0, "items are keyed by their own URL"
    store.record_fetch(page("http://shop.test/p", 6.0, fingerprint="g"), [item("p", price=1)])
    write = store.record_fetch(page("http://shop.test/p", 9.0, status=304))
    assert write.items.touched == 1
    (stored,) = store.items.iter("shop")
    assert stored.last_seen == 9.0 and stored.version == 1


def test_runs_lifecycle(store: Store) -> None:
    run = store.runs.begin("shop", 5.0, "incremental")
    store.record_fetch(page("http://x/1", 6.0, redirects=["http://x/0"], depth=2), run_id=run)
    store.record_fetch(FetchEvent("shop", "http://x/2", 7.0, error="TimeoutError: slow", error_class="transient"),
                       run_id=run)
    assert [r["id"] for r in store.runs.open_runs("shop")] == [run]
    store.runs.finish(run, 8.0, "stopped", {"requests": 2})
    store.runs.resume(run)
    store.runs.finish(run, 9.0, "finished", None)
    events = list(store.documents.events("shop", run_id=run))
    assert [e["url"] for e in events] == ["http://x/1", "http://x/2"]
    assert events[0]["redirects"] == ["http://x/0"] and events[0]["depth"] == 2
    assert events[1]["status"] is None and events[1]["error_class"] == "transient"
    assert store.documents.status_counts("shop") == {"200": 1, "error": 1}
    (record,) = store.runs.list("shop")
    assert record["status"] == "finished" and record["stats"] == {"requests": 2}
    assert record["mode"] == "incremental" and record["resumes"] == 1
    with pytest.raises(ValueError):
        store.runs.finish(run, 9.0, "exploded", None)
    with pytest.raises(KeyError):
        store.runs.resume(999)


def test_failed_transaction_rolls_back_event_and_items(store: Store) -> None:
    class Exploding(ExtractedItem):
        def data_json(self) -> str:
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        store.record_fetch(page("http://x/1", 1.0), [item("ok"), Exploding("product", "bad", {}, "http://x")])
    assert list(store.documents.events("shop")) == []
    assert store.documents.get("shop", "http://x/1") is None
    assert store.items.count("shop") == 0


def test_synchronous_level(tmp_path: Path) -> None:
    with Store(tmp_path / "full.db", synchronous="full") as db:
        assert db.db.execute("PRAGMA synchronous").fetchone()[0] == 2
    with Store(tmp_path / "normal.db") as db:
        assert db.db.execute("PRAGMA synchronous").fetchone()[0] == 1
    with pytest.raises(ValueError):
        Store(tmp_path / "x.db", synchronous="off")


def test_reopen_keeps_data(tmp_path: Path) -> None:
    path = tmp_path / "keep.db"
    with Store(path) as db:
        db.record_fetch(page("http://x/", 1.0), [item("a", n=1)])
    with Store(path) as db:
        assert [i.key for i in db.items.iter("shop")] == ["a"]
        assert db.applied == []


def test_newer_schema_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "future.db"
    raw = sqlite3.connect(path)
    raw.execute("PRAGMA user_version=99")
    raw.close()
    with pytest.raises(StoreError, match="schema version 99"):
        Store(path)


def test_corrupt_file_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.db"
    path.write_bytes(b"this is not a database" * 100)
    with pytest.raises(StoreError, match="cannot open database"):
        Store(path)


def _items(store: Store) -> list[dict]:
    store.record_fetch(
        page("http://x/", 1_700_000_000.0),
        [
            item("a", title="Ä, \"quoted\"", tags=["x", "y"], dims={"w": 1, "h": 2}, ok=True),
            item("b", title="second", price=3.5, nested=[{"k": 1}]),
        ],
    )
    return [item_record(i) for i in store.items.iter("shop")]


def test_jsonl_export(store: Store) -> None:
    out = io.StringIO()
    assert write_jsonl(_items(store), out) == 2
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    assert lines[0]["_key"] == "a" and lines[0]["_first_seen"] == "2023-11-14T22:13:20Z"
    assert lines[0]["title"] == 'Ä, "quoted"' and lines[0]["tags"] == ["x", "y"]
    assert lines[1]["price"] == 3.5 and lines[1]["_version"] == 1


def test_csv_export_flattens(store: Store) -> None:
    out = io.StringIO()
    assert write_csv(_items(store), out, leading=ITEM_COLUMNS) == 2
    rows = list(csv.DictReader(io.StringIO(out.getvalue())))
    assert list(rows[0])[:6] == ["_type", "_key", "_url", "_first_seen", "_last_seen", "_version"]
    assert rows[0]["dims.w"] == "1" and rows[0]["tags"] == "x | y" and rows[0]["ok"] == "true"
    assert rows[0]["title"] == 'Ä, "quoted"'
    assert rows[1]["nested"] == '[{"k": 1}]' and rows[1]["dims.w"] == ""


def test_csv_export_selected_fields(store: Store) -> None:
    out = io.StringIO()
    write_csv(_items(store), out, leading=ITEM_COLUMNS, fields=["title"], list_separator=";")
    header = out.getvalue().splitlines()[0]
    assert header == "_type,_key,_url,_first_seen,_last_seen,_version,_changed_at,title"


def test_version_export(store: Store) -> None:
    store.record_fetch(page("http://x/", 1_700_000_000.0), [item("a", price=1)])
    store.record_fetch(page("http://x/", 1_700_000_100.0), [item("a", price=2)])
    out = io.StringIO()
    write_csv((version_record(v) for v in store.items.versions("shop")), out, leading=VERSION_COLUMNS)
    rows = list(csv.DictReader(io.StringIO(out.getvalue())))
    assert [(r["_version"], r["price"], r["_observed_at"]) for r in rows] == [
        ("1", "1", "2023-11-14T22:13:20Z"), ("2", "2", "2023-11-14T22:15:00Z")]


def test_event_and_document_export(store: Store) -> None:
    run = store.runs.begin("shop", 1.0, "full")
    store.record_fetch(page("http://x/1", 1_700_000_000.0, redirects=["http://x/0"], sitemap_lastmod=0.0,
                            etag='"e"'), run_id=run)
    store.record_fetch(FetchEvent("shop", "http://x/2", 1_700_000_001.0, error="boom", error_class="permanent"),
                       run_id=run)
    records = [event_record(e) for e in store.documents.events("shop", since=1_700_000_000.5)]
    assert [r["url"] for r in records] == ["http://x/2"]
    out = io.StringIO()
    write_csv((event_record(e) for e in store.documents.events("shop")), out, leading=EVENT_COLUMNS)
    rows = list(csv.DictReader(io.StringIO(out.getvalue())))
    assert rows[0]["redirects"] == "http://x/0" and rows[0]["fetched_at"] == "2023-11-14T22:13:20Z"
    assert rows[0]["sitemap_lastmod"] == "1970-01-01T00:00:00Z" and rows[0]["etag"] == '"e"'
    assert rows[1]["status"] == "" and rows[1]["error"] == "boom" and rows[1]["error_class"] == "permanent"
    documents = [document_record(d) for d in store.documents.iter_documents("shop")]
    assert [d["url"] for d in documents] == ["http://x/1", "http://x/2"]
    assert documents[0]["last_success"] == "2023-11-14T22:13:20Z" and documents[1]["last_success"] is None
