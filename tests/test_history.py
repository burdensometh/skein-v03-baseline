"""Item history: field diffs, the change feed with tombstones, export cursors and event retention."""

from __future__ import annotations

from pathlib import Path

import pytest

from skein.extract import ExtractedItem
from skein.store import ChangeKind, FetchEvent, Store, change_feed, diff_values, item_history, prune_events


def item(key: str, url: str = "http://x/p", **data: object) -> ExtractedItem:
    return ExtractedItem("product", key, dict(data), url)


@pytest.fixture
def store(tmp_path: Path):  # noqa: ANN201
    with Store(tmp_path / "history.db") as db:
        yield db


def fetch(store: Store, at: float, items: list[ExtractedItem], url: str = "http://x/p",
          run_id: int | None = None):  # noqa: ANN201
    return store.record_fetch(FetchEvent("shop", url, at, status=200, fingerprint=f"f{at}"), items,
                              run_id=run_id, complete_types=("product",), page_url=url)


def test_diff_values_nested_and_lists() -> None:
    before = {"price": 1, "dims": {"w": 1, "h": 2}, "tags": ["a"], "gone": True}
    after = {"price": 2, "dims": {"w": 1, "h": 3, "d": 4}, "tags": ["a", "b"], "new": None}
    assert [(c.path, c.kind, c.before, c.after) for c in diff_values(before, after)] == [
        ("dims.d", "added", None, 4),
        ("dims.h", "changed", 2, 3),
        ("gone", "removed", True, None),
        ("new", "added", None, None),
        ("price", "changed", 1, 2),
        ("tags", "changed", ["a"], ["a", "b"]),
    ]
    assert diff_values({"a": 1}, {"a": 1}) == []
    assert [c.kind for c in diff_values({"a": 1}, {"a": 1.0})] == ["changed"], "types matter (1 vs 1.0)"
    assert diff_values(1, 2)[0].path == "$"


def test_item_history_with_tombstone_and_restore(store: Store) -> None:
    fetch(store, 1.0, [item("a", price=1), item("b", price=5)])
    fetch(store, 2.0, [item("a", price=2), item("b", price=5)])
    write = fetch(store, 3.0, [item("b", price=5)])
    assert write.items.deleted == 1
    fetch(store, 4.0, [item("a", price=2), item("b", price=5)])
    history = item_history(store.items, "shop", "product", "a")
    assert [(c.version, c.kind) for c in history] == [
        (1, ChangeKind.CREATED), (2, ChangeKind.UPDATED), (3, ChangeKind.DELETED), (4, ChangeKind.RESTORED)]
    assert [(f.path, f.before, f.after) for f in history[1].fields] == [("price", 1, 2)]
    assert history[2].fields == [] and history[3].fields == []
    assert [v.deleted for v in store.items.versions("shop", key="a")] == [False, False, True, False]
    assert store.items.count("shop") == 2


def test_retire_only_touches_complete_types_on_the_same_page(store: Store) -> None:
    store.record_fetch(FetchEvent("shop", "http://x/list", 1.0, status=200),
                       [item("a", url="http://x/list"), ExtractedItem("review", "r", {}, "http://x/list")])
    store.record_fetch(FetchEvent("shop", "http://x/list", 2.0, status=200), [],
                       complete_types=("review",), page_url="http://x/list")
    assert [(i.item_type, i.deleted_at) for i in store.items.iter("shop", include_deleted=True)] == [
        ("product", None), ("review", 2.0)]
    store.record_fetch(FetchEvent("shop", "http://x/other", 3.0, status=200), [],
                       complete_types=("product",), page_url="http://x/other")
    assert store.items.count("shop") == 1, "items of other pages are untouched"
    write = store.record_fetch(FetchEvent("shop", "http://x/list", 4.0, status=304), [])
    assert write.items.touched == 1, "tombstoned items are not touched by a 304"


def test_change_feed_windows_and_positions(store: Store) -> None:
    fetch(store, 10.0, [item("a", price=1), item("b", price=1)])
    fetch(store, 20.0, [item("a", price=2), item("b", price=1)])
    fetch(store, 30.0, [item("b", price=1)])
    feed = list(change_feed(store.items, "shop"))
    assert [(c.key, c.kind.value) for c in feed] == [("a", "created"), ("b", "created"), ("a", "updated"),
                                                     ("a", "deleted")]
    assert [c.position for c in feed] == sorted(c.position for c in feed)
    late = list(change_feed(store.items, "shop", since=15.0))
    assert [(c.key, c.kind.value) for c in late] == [("a", "updated"), ("a", "deleted")]
    assert [(f.path, f.before, f.after) for f in late[0].fields] == [("price", 1, 2)], "diffed against v1"
    after = list(change_feed(store.items, "shop", after=feed[2].position))
    assert [(c.key, c.version) for c in after] == [("a", 3)]


def test_export_cursors_only_move_forward(store: Store) -> None:
    assert store.cursors.get("warehouse", "shop") == 0
    store.cursors.advance("warehouse", "shop", 10, 1.0)
    store.cursors.advance("warehouse", "shop", 4, 2.0)
    assert store.cursors.get("warehouse", "shop") == 10
    assert store.cursors.get("warehouse", "other") == 0
    assert [(c["name"], c["position"], c["updated_at"]) for c in store.cursors.all()] == [("warehouse", 10, 2.0)]


def test_prune_keeps_referenced_events(store: Store) -> None:
    run = store.runs.begin("shop", 0.0, "full")
    fetch(store, 1.0, [item("a", price=1)], run_id=run)
    fetch(store, 2.0, [item("a", price=1)], run_id=run)
    fetch(store, 3.0, [item("a", price=1)], run_id=run)
    store.record_fetch(FetchEvent("shop", "http://x/gone", 1.5, error="boom", error_class="permanent"), run_id=run)
    store.record_fetch(FetchEvent("shop", "http://x/gone", 2.5, error="boom", error_class="permanent"), run_id=run)
    report = prune_events(store.db, "shop", before=100.0)
    assert report.events_deleted == 0, "events of an unfinished run are kept"
    store.runs.finish(run, 4.0, "finished", None)
    report = prune_events(store.db, "shop", before=100.0, vacuum=True)
    kept = [(e["url"].rsplit("/", 1)[1], e["fetched_at"]) for e in store.documents.events("shop")]
    assert kept == [("p", 1.0), ("p", 3.0), ("gone", 2.5)], "the item version source and latest events stay"
    assert report.events_deleted == 2 and report.events_kept == 3 and report.vacuumed
    assert prune_events(store.db, None, before=0.5).events_deleted == 0
