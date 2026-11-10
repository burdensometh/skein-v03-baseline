"""Partial fragment rules: one item assembled from several pages.

A listing page owns status/date/membership, detail pages own the body.
Fragments merge field by field, only a changed merged record makes a new
version, and deletions are reconciled from the whole run's sightings rather
than from a single page.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from skein.extract.extractor import ExtractedItem
from skein.store import ChangeKind, Store, change_feed
from skein.store.items import PageItems

LIST = "http://x/notice/list"
DETAIL = "http://x/notice/detail/1"


def fragment(key: str, url: str, at: float, *, authority: bool = False, **fields: object) -> ExtractedItem:
    return ExtractedItem(
        "notice", key, {"doc_no": key, **fields}, url,
        partial=True, authority=authority, field_names=("doc_no", *fields),
    )


def page(at: float, items: list[ExtractedItem], url: str = LIST, *, run_id: int = 1,
         not_modified: bool = False) -> PageItems:
    # Partial rules never appear in complete_types: no per-page tombstones.
    return PageItems("s", url, at, items, (), not_modified=not_modified, run_id=run_id)


@pytest.fixture
def store(tmp_path: Path):  # noqa: ANN201
    with Store(tmp_path / "fragments.db") as opened:
        for run_id in (1, 2, 3):
            opened.db.execute("INSERT INTO runs (id, spider, started_at, status) VALUES (?, 's', ?, 'running')",
                              (run_id, float(run_id)))
        yield opened


def versions(store: Store, key: str = "N-1") -> list[tuple]:
    return [tuple(r) for r in store.db.execute(
        "SELECT version, is_current, deleted FROM item_history "
        "WHERE spider = 's' AND item_key = ? ORDER BY version", (key,))]


def test_fragments_merge_into_one_record(store: Store) -> None:
    with store.transaction():
        store.items.write([page(10.0, [fragment("N-1", LIST, 10.0, authority=True, status="open", updated="2026-11-01")])])
        store.items.write([page(20.0, [fragment("N-1", DETAIL, 20.0, body="text", agency="A", attachments=[])])])
    (current,) = store.items.iter("s")
    assert current.key == "N-1" and current.version == 2
    assert current.data == {
        "doc_no": "N-1", "status": "open", "updated": "2026-11-01",
        "body": "text", "agency": "A", "attachments": [],
    }
    assert current.url == LIST, "the first fragment names the record's URL; later fragments never overwrite it"


def test_unchanged_fragments_add_no_versions(store: Store) -> None:
    with store.transaction():
        store.items.write([page(10.0, [fragment("N-1", DETAIL, 10.0, body="text")], url=DETAIL)])
        store.items.write([page(20.0, [fragment("N-1", LIST, 20.0, authority=True, status="open")])])
        # The listing is recrawled with identical fields: no new version.
        store.items.write([page(30.0, [fragment("N-1", LIST, 30.0, authority=True, status="open")])])
        # The detail page returns 304: touched, no version.
        store.items.write([page(40.0, [], url=DETAIL, not_modified=True)])
    assert versions(store) == [(1, 0, 0), (2, 1, 0)]
    (current,) = store.items.iter("s")
    assert current.last_seen == 40.0 and current.url == DETAIL


def test_listing_field_change_makes_one_version_detail_change_makes_one(store: Store) -> None:
    with store.transaction():
        store.items.write([page(10.0, [fragment("N-1", LIST, 10.0, authority=True, status="open", updated="d1")])])
        store.items.write([page(20.0, [fragment("N-1", DETAIL, 20.0, body="v1")])])
        # Status changes on the listing only.
        store.items.write([page(30.0, [fragment("N-1", LIST, 30.0, authority=True, status="closed", updated="d2")])])
        # Body changes on the detail only.
        store.items.write([page(40.0, [fragment("N-1", DETAIL, 40.0, body="v2")])])
    assert versions(store) == [(1, 0, 0), (2, 0, 0), (3, 0, 0), (4, 1, 0)]
    (current,) = store.items.iter("s")
    assert current.data["status"] == "closed" and current.data["body"] == "v2"
    assert current.data["updated"] == "d2"


def test_missing_field_on_one_fragment_never_wipes_other_owner(store: Store) -> None:
    with store.transaction():
        store.items.write([page(10.0, [fragment("N-1", LIST, 10.0, authority=True, status="open")])])
        store.items.write([page(20.0, [fragment("N-1", DETAIL, 20.0, body="text", agency="A")])])
        # A later listing extraction happens to carry no usable status value.
        empty = ExtractedItem("notice", "N-1", {"doc_no": "N-1", "status": None}, LIST,
                              partial=True, authority=True, field_names=())
        store.items.write([page(30.0, [empty])])
    (current,) = store.items.iter("s")
    assert current.data["status"] == "open" and current.data["body"] == "text" and current.data["agency"] == "A"
    assert versions(store) == [(1, 0, 0), (2, 1, 0)]


def test_pagination_keeps_everything_run_end_reconcile_deletes_only_the_withdrawn(store: Store) -> None:
    def listing(at: float, keys: list[str], page_url: str, *, run_id: int = 1) -> PageItems:
        return page(at, [fragment(k, page_url, at, authority=True, status="open") for k in keys],
                    url=page_url, run_id=run_id)

    with store.transaction():
        # Run 1: two listing pages, one item per page, plus their details.
        store.items.write([listing(10.0, ["N-1"], LIST + "?page=1")])
        store.items.write([listing(11.0, ["N-2"], LIST + "?page=2")])
        store.items.write([page(12.0, [fragment("N-1", DETAIL, 12.0, body="b1")], url=DETAIL)])
        store.items.reconcile_authority("s", 1, 15.0)
    assert store.items.count("s") == 2
    assert store.db.execute(
        "SELECT COUNT(*) FROM item_sightings WHERE run_id = 1").fetchone()[0] == 0, \
        "a reconciled run's sightings are cleaned up"

    with store.transaction():
        # Run 2: N-2 is pushed to page 2; nothing is withdrawn.
        store.items.write([listing(20.0, ["N-1", "N-2"], LIST + "?page=1", run_id=2)])
        store.items.reconcile_authority("s", 2, 25.0)
    assert store.items.count("s") == 2, "an item moved to a later page is not a deletion"

    with store.transaction():
        # Run 3: N-2 is gone from every listing page.
        store.items.write([listing(30.0, ["N-1"], LIST + "?page=1", run_id=3)])
        retired = store.items.reconcile_authority("s", 3, 35.0)
    assert retired == 1 and store.items.count("s") == 1
    n2 = list(store.items.versions("s", key="N-2"))
    assert n2[-1].deleted is True
    # Re-listing restores the item with its old data plus the new fragment.
    with store.transaction():
        store.items.write([listing(40.0, ["N-1", "N-2"], LIST + "?page=1", run_id=3)])
    restored = [v for v in store.items.versions("s", key="N-2")]
    assert restored[-1].deleted is False and restored[-1].data["status"] == "open"


def test_304_listing_keeps_sighted_items_fresh(store: Store) -> None:
    with store.transaction():
        store.items.write([page(10.0, [fragment("N-1", LIST, 10.0, authority=True, status="open")])])
        store.items.write([page(20.0, [fragment("N-1", DETAIL, 20.0, body="text")], url=DETAIL)])
        # The listing answers 304. Its sightings (recorded in run 1) keep N-1 fresh.
        store.items.write([page(30.0, [], not_modified=True)])
    (current,) = store.items.iter("s")
    assert current.last_seen == 30.0 and current.version == 2


def test_change_feed_sees_only_real_merged_changes(store: Store) -> None:
    """Re-crawled identical fragments and 304s must not reach downstream subscribers."""
    with store.transaction():
        store.items.write([page(10.0, [fragment("N-1", LIST, 10.0, authority=True, status="open", updated="d1")])])
        store.items.write([page(20.0, [fragment("N-1", DETAIL, 20.0, body="v1")], url=DETAIL)])
        # Both pages are recrawled unchanged: the old alternating-overwrite bug
        # would have emitted two versions here; the merge emits none.
        store.items.write([page(30.0, [fragment("N-1", LIST, 30.0, authority=True, status="open", updated="d1")])])
        store.items.write([page(40.0, [fragment("N-1", DETAIL, 40.0, body="v1")], url=DETAIL, not_modified=True)])
        # Only the listing-owned status changes.
        store.items.write([page(50.0, [fragment("N-1", LIST, 50.0, authority=True, status="closed", updated="d2")])])
    feed = list(change_feed(store.items, "s"))
    assert [(c.version, c.kind) for c in feed] == [
        (1, ChangeKind.CREATED), (2, ChangeKind.UPDATED), (3, ChangeKind.UPDATED)]
    # Version 2 only adds the detail owner's field; version 3 only the listing owner's.
    assert [f.path for f in feed[1].fields] == ["body"]
    # The third transition diffs only the listing-owned fields; the body owner is untouched.
    assert sorted(f.path for f in feed[-1].fields) == ["status", "updated"]
    assert [(f.before, f.after) for f in feed[-1].fields if f.path == "status"] == [("open", "closed")]
    assert feed[-1].data["body"] == "v1", "the detail owner's field is still present on the listing-only change"


def test_change_feed_reports_authority_withdrawal_as_deletion(store: Store) -> None:
    def listing(at: float, keys: list[str], run_id: int) -> PageItems:
        return page(at, [fragment(k, LIST, at, authority=True, status="open") for k in keys],
                    run_id=run_id)

    with store.transaction():
        store.items.write([listing(10.0, ["N-1", "N-2"], 1)])
        store.items.write([page(20.0, [fragment("N-1", DETAIL, 20.0, body="b1")], url=DETAIL)])
        store.items.reconcile_authority("s", 1, 25.0)
        store.items.write([listing(30.0, ["N-1"], 2)])
        store.items.reconcile_authority("s", 2, 35.0)
    feed = [c for c in change_feed(store.items, "s") if c.key == "N-2"]
    assert [(c.version, c.kind) for c in feed] == [(1, ChangeKind.CREATED), (2, ChangeKind.DELETED)]
    assert feed[-1].fields == [], "a tombstone carries no field diff"
