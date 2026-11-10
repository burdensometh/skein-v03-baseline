"""Upgrading a real database written by skein 0.2.

``tests/data/v02_shop.db`` was produced by the 0.2 code (a ``git worktree``
of the v0.2 commit) crawling the fixture site twice with an empty
canonicalisation policy: the ``/tracked`` page links the same products and
catalog page under tracking parameters, a ``;jsessionid`` path parameter
and a different query order, and catalog cards add ``utm_source`` to every
other product link. Product 1 changed between the runs (two versions).
"""

from __future__ import annotations

import asyncio
import random
import shutil
import sqlite3
from pathlib import Path

import pytest

from skein.clock import FakeClock
from skein.config import parse_config
from skein.crawler import Crawler
from skein.store import LATEST_VERSION, Store
from skein.store.history import change_feed
from tests.fixtures_site import FixtureSite, SiteOptions, running_site

V02_DB = Path(__file__).parent / "data" / "v02_shop.db"
OLD_ORIGIN = "http://127.0.0.1:34829"
URL_COLUMNS = {
    "frontier": ("url", "referrer", "seed", "host_key"), "documents": ("url", "final_url"),
    "fetch_events": ("url", "final_url"), "items": ("url",), "item_versions": ("url",),
    "links": ("source", "target"), "watermarks": ("seed",), "robots_cache": ("origin",),
}


@pytest.fixture
def v02(tmp_path: Path) -> Path:
    target = tmp_path / "legacy.db"
    shutil.copyfile(V02_DB, target)
    return target


def raw(path: Path, query: str) -> list[tuple]:
    db = sqlite3.connect(path)
    try:
        return db.execute(query).fetchall()
    finally:
        db.close()


def relocate(path: Path, origin: str) -> None:
    """Point every stored URL of the 0.2 database at a fixture site running on another port."""
    db = sqlite3.connect(path)
    with db:
        for table, columns in URL_COLUMNS.items():
            for column in columns:
                db.execute(f"UPDATE {table} SET {column} = replace({column}, ?, ?)", (OLD_ORIGIN, origin))
    db.close()


def test_fixture_is_a_v02_database() -> None:
    assert raw(V02_DB, "PRAGMA user_version") == [(7,)]
    tables = {r[0] for r in raw(V02_DB, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert {"frontier", "item_versions", "items", "documents"} <= tables and "item_history" not in tables
    urls = [r[0] for r in raw(V02_DB, "SELECT url FROM frontier")]
    assert any("utm_" in u for u in urls) and any("jsessionid" in u for u in urls) and len(urls) == 20


def test_upgrade_recomputes_keys_and_merges_duplicates(v02: Path) -> None:
    events_before = raw(v02, "SELECT id, status, fetched_at FROM fetch_events ORDER BY id")
    versions_before = raw(v02, "SELECT rowid, item_key, version, content_hash, data, observed_at "
                               "FROM item_versions ORDER BY rowid")
    with Store(v02) as store:
        assert [m.version for m in store.applied] == [8, 9, 10, 11, 12]
        assert store.backup_path is not None and store.backup_path.name == "legacy.db.schema-v7.bak"
        urls = {row[0].removeprefix(OLD_ORIGIN) for row in store.db.execute("SELECT url FROM frontier")}
        assert urls == {"/catalog?page=1", "/catalog?page=2", "/catalog?page=2&sort=price", "/tracked",
                        *(f"/product/{n}" for n in range(1, 8))}
        states = dict(store.db.execute("SELECT url, state FROM frontier").fetchall())
        assert {u for u, state in states.items() if state != "done"} == {OLD_ORIGIN + "/product/7"}, "a 404"
        # Fetch history: every attempt kept, re-pointed at the merged key.
        events = store.db.execute("SELECT id, status, fetched_at, url FROM fetch_events ORDER BY id").fetchall()
        assert [tuple(e)[:3] for e in events] == events_before
        assert not any("utm_" in e["url"] or "jsessionid" in e["url"] or "fbclid" in e["url"] for e in events)
        # Documents: counters of the merged variants add up to the merged history.
        for document in store.documents.iter_documents("shop"):
            attempts = store.db.execute("SELECT COUNT(*) FROM fetch_events WHERE spider = 'shop' AND url = ?",
                                        (document.url,)).fetchone()[0]
            assert document.fetch_count == attempts, document.url
        product2 = store.documents.get("shop", OLD_ORIGIN + "/product/2")
        assert product2 is not None and product2.fetch_count == 8 and product2.etag is not None
        merges = store.canonical.merges("shop")
        assert len(merges) == 9 and all(m["collided"] for m in merges)
        assert {m["new_url"].removeprefix(OLD_ORIGIN) for m in merges} == {
            "/product/1", "/product/2", "/product/4", "/product/6", "/catalog?page=2&sort=price"}
        rules = store.canonical.get("shop")
        assert rules is not None and rules.version == 2
        # Items: SCD rows keep the 0.2 version rowids (change-feed cursors stay valid).
        history = store.db.execute(
            "SELECT id, item_key, version, content_hash, data, valid_from, valid_to, is_current "
            "FROM item_history ORDER BY id").fetchall()
        assert [tuple(h)[:6] for h in history] == versions_before
        first, second = [h for h in history if h["item_key"] == "SKU-0001"]
        assert first["valid_to"] == second["valid_from"] and not first["is_current"] and second["is_current"]
        assert [c.kind.value for c in change_feed(store.items, "shop", after=6)] == ["updated"]
        assert store.items.count("shop") == 6
        assert raw(v02, "SELECT name FROM sqlite_master WHERE name = 'items'") == [("items",)], "items is a view"
        assert raw(v02, "PRAGMA user_version") == [(LATEST_VERSION,)]
        assert store.db.execute("PRAGMA foreign_key_check").fetchall() == []


def test_upgraded_database_keeps_crawling_without_reintroducing_duplicates(v02: Path, tmp_path: Path) -> None:
    clock = FakeClock(1_800_000_000.0)
    with running_site(SiteOptions(catalog_pages=2, products_per_page=3, validators="both",
                                  wall_clock=clock.time)) as site:
        site.revise(1, at=1_700_086_000.0)  # the site as the second 0.2 crawl left it
        relocate(v02, site.base_url)
        before = {r[0] for r in raw(v02, "SELECT url FROM frontier")}
        config = parse_config({"version": 2, "spiders": [{
            "name": "shop", "seeds": [site.url("/tracked"), site.url("/catalog?page=1")],
            "politeness": {"min_delay": 0.5}, "incremental": {"mode": "full"},
            "extract": {"items": [{"name": "product", "url_pattern": r"/product/\d+", "key": ["sku"],
                                   "fields": {"sku": {"selector": "#sku", "required": True}, "title": "h1",
                                              "price": {"selector": ".price span", "type": "number"}}}],
                        "links": [{"allow": [r"/catalog\?", r"/product/\d+"]}]}}]})
        result = run(site, v02, config, clock, tmp_path)
        assert result.status == "finished" and result.stats["items"]["new"] == 0
        with Store(v02) as store:
            after = {r[0] for r in store.db.execute("SELECT url FROM frontier")}
            assert len(after) == 11 and len(before) == 20
            assert store.items.count_versions("shop") == 7, "unchanged products get no new versions"
            assert result.stats["conditional_requests"] > 0, "0.2 validators are still used"
            assert result.stats["not_modified"] > 0


def run(site: FixtureSite, db: Path, config, clock: FakeClock, tmp_path: Path):  # noqa: ANN001, ANN201
    async def go():  # noqa: ANN202
        with Store(db) as store:
            return await Crawler(config.spiders[0], store, http=config.http, clock=clock, rng=random.Random(0),
                                 runtime=config.runtime, checkpoint_dir=tmp_path / "cp").run()

    assert site.port
    return asyncio.run(go())
