"""Item history as SCD type 2, written in batches."""

from __future__ import annotations

from pathlib import Path

import pytest

from skein.extract.extractor import ExtractedItem
from skein.store import Store
from skein.store.history import change_feed, item_history
from skein.store.items import PageItems


def item(key: str, page: str = "http://x/list", **data: object) -> ExtractedItem:
    return ExtractedItem("thing", key, {"key": key, **data}, page)


def page(at: float, items: list[ExtractedItem], url: str = "http://x/list", *, complete: bool = True,
         not_modified: bool = False) -> PageItems:
    return PageItems("s", url, at, items, ("thing",) if complete else (), not_modified)


@pytest.fixture
def store(tmp_path: Path):  # noqa: ANN201
    with Store(tmp_path / "items.db") as opened:
        yield opened


def rows(store: Store) -> list[tuple]:
    return [tuple(r) for r in store.db.execute(
        "SELECT item_key, version, valid_from, valid_to, is_current, deleted, first_seen, last_seen "
        "FROM item_history ORDER BY item_key, version")]


def test_versions_get_validity_intervals(store: Store) -> None:
    with store.transaction():
        store.items.write([page(10.0, [item("a", price=1)])])
        store.items.write([page(20.0, [item("a", price=1)])])
        store.items.write([page(30.0, [item("a", price=2)])])
        store.items.write([page(40.0, [])])
        store.items.write([page(50.0, [item("a", price=2)])])
    assert rows(store) == [
        ("a", 1, 10.0, 30.0, 0, 0, 10.0, 20.0),
        ("a", 2, 30.0, 40.0, 0, 0, 10.0, 30.0),
        ("a", 3, 40.0, 50.0, 0, 1, 10.0, 30.0),
        ("a", 4, 50.0, None, 1, 0, 10.0, 50.0),
    ]
    assert [c.kind.value for c in item_history(store.items, "s", "thing", "a")] == [
        "created", "updated", "deleted", "restored"]
    assert [v.data["price"] for v in store.items.as_of("s", 35.0)] == [2]
    assert list(store.items.as_of("s", 45.0)) == [], "a tombstone is not a valid version"
    assert list(store.items.as_of("s", 5.0)) == []
    (current,) = store.items.iter("s")
    assert (current.version, current.first_seen, current.changed_at) == (4, 10.0, 50.0)


def test_one_batch_equals_the_same_pages_written_one_by_one(store: Store, tmp_path: Path) -> None:
    pages = [
        page(1.0, [item("a", price=1), item("b", price=1)]),
        page(2.0, [item("a", price=2)]),
        page(3.0, [item("b", price=5, page="http://x/other")], url="http://x/other", complete=False),
        page(4.0, [], not_modified=True),
        page(5.0, [item("a", price=2), item("c")]),
    ]
    with store.transaction():
        results = store.items.write(pages)
    assert [(r.new, r.updated, r.unchanged, r.touched, r.deleted) for r in results] == [
        (2, 0, 0, 0, 0), (0, 1, 0, 0, 1), (0, 1, 0, 0, 0), (0, 0, 0, 1, 0), (1, 0, 1, 0, 0)]
    with Store(tmp_path / "single.db") as single:
        for one in pages:
            with single.transaction():
                single.items.write([one])
        assert rows(single) == rows(store)
    assert store.items.count("s") == 3 and store.items.count_versions("s") == 6


def test_unique_current_row_is_enforced(store: Store) -> None:
    with store.transaction():
        store.items.write([page(1.0, [item("a")])])
    with pytest.raises(Exception, match="UNIQUE"):
        store.db.execute(
            """INSERT INTO item_history (spider, item_type, item_key, version, content_hash, data, url, valid_from,
                                         first_seen, last_seen) VALUES ('s', 'thing', 'a', 9, 'h', '{}', 'u', 1, 1, 1)""")
    with pytest.raises(Exception, match="CHECK"):
        store.db.execute("UPDATE item_history SET is_current = 0")


def test_change_feed_positions_follow_row_ids(store: Store) -> None:
    with store.transaction():
        store.items.write([page(1.0, [item("a", price=1)]), page(2.0, [item("a", price=2)])])
        store.items.write([page(3.0, [item("b")], url="http://x/b", complete=False)])
    feed = list(change_feed(store.items, "s"))
    assert [(c.key, c.version, c.kind.value) for c in feed] == [("a", 1, "created"), ("a", 2, "updated"),
                                                               ("b", 1, "created")]
    assert [c.position for c in feed] == sorted(c.position for c in feed)
    later = list(change_feed(store.items, "s", after=feed[0].position))
    assert [c.fields[0].path for c in later[:1]] == ["price"], "diffed against the version before the window"
    view = store.db.execute("SELECT item_key, version, changed_at, deleted_at FROM items ORDER BY 1").fetchall()
    assert [tuple(v) for v in view] == [("a", 2, 2.0, None), ("b", 1, 3.0, None)]
