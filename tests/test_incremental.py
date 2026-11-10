"""Incremental recrawls against the fixture site with server-controlled content changes.

The site and the crawler share one fake clock, so ``Date``,
``Last-Modified``, sitemap ``lastmod`` values and revisit schedules all
live on the same timeline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from skein.clock import FakeClock
from skein.store import Store, change_feed, item_history
from tests.helpers import PRODUCT_ITEMS, crawl, events, stored_items

DAY = 86400.0
LINKS = [{"allow": [r"/catalog\?page=\d+$", r"/product/\d+$"]}]


def shop(site: Any, db: Path, clock: FakeClock, **spider: Any):  # noqa: ANN201
    spider.setdefault("seeds", [site.url("/catalog?page=1")])
    extract = {"items": PRODUCT_ITEMS, "links": LINKS, **spider.pop("extract", {})}
    return crawl(site, db, clock, extract=extract, **spider)


def documents(db: Path) -> dict[str, Any]:
    with Store(db) as store:
        return {d.url.split("/", 3)[3]: d for d in store.documents.iter_documents("shop")}


def run_events(db: Path, run_id: int) -> list[dict[str, Any]]:
    with Store(db) as store:
        return list(store.documents.events("shop", run_id=run_id))


def test_conditional_recrawl_detects_exactly_the_changed_product(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators="both", wall_clock=clock.time)
    db = tmp_path / "shop.db"
    first = shop(site, db, clock)
    assert first.mode == "full" and first.stats["items"]["new"] == 12
    assert first.stats["conditional_requests"] == 0
    before = stored_items(db)

    clock.advance(2 * DAY)
    site.revise(3)
    second = shop(site, db, clock)
    assert second.mode == "incremental"
    stats = second.stats
    fetched = len(run_events(db, second.run_id))
    assert stats["conditional_requests"] == fetched == stats["pages_stored"]
    assert stats["not_modified"] == fetched - 2, "only product 3 and the catalog page listing it changed"
    assert stats["documents"]["changed"] == 2 and stats["documents"]["unchanged"] == fetched - 2
    assert stats["items"] == {"new": 0, "updated": 1, "unchanged": 0, "touched": 11, "deleted": 0, "dropped": 0}
    after = stored_items(db)
    product3 = after["SKU-0003"]
    assert product3.version == 2 and product3.data["price"] == 48.49
    assert product3.changed_at > before["SKU-0003"].changed_at
    assert after["SKU-0002"].version == 1 and after["SKU-0002"].changed_at == before["SKU-0002"].changed_at
    assert after["SKU-0002"].last_seen > before["SKU-0002"].last_seen, "a 304 still counts as a sighting"
    headers = site.requests_for("/product/3")[-1].headers
    assert headers["if-none-match"] == '"p3-r0"' and "if-modified-since" in headers
    unchanged = documents(db)["product/2"]
    assert unchanged.check_count == 1 and unchanged.change_count == 0 and unchanged.etag == '"p2-r0"'


@pytest.mark.parametrize("validators, header", [("etag", "if-none-match"), ("last-modified", "if-modified-since")])
def test_each_validator_alone_produces_304s(make_site, tmp_path: Path, validators: str, header: str) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators=validators, wall_clock=clock.time)
    db = tmp_path / "shop.db"
    shop(site, db, clock, max_depth=1)
    clock.advance(2 * DAY)
    second = shop(site, db, clock, max_depth=1)
    assert second.stats["not_modified"] == second.stats["pages_stored"] > 0
    sent = site.requests_for("/product/1")[-1].headers
    other = {"if-none-match", "if-modified-since"} - {header}
    assert header in sent and not other & set(sent)
    state = documents(db)["product/1"]
    assert (state.etag is not None) is (validators == "etag")
    assert (state.last_modified is not None) is (validators == "last-modified")


def test_unconditional_when_disabled_or_no_validators(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators="both", wall_clock=clock.time)
    db = tmp_path / "shop.db"
    shop(site, db, clock, max_depth=1, incremental={"conditional_requests": False})
    clock.advance(2 * DAY)
    second = shop(site, db, clock, max_depth=1, incremental={"conditional_requests": False})
    assert second.stats["conditional_requests"] == second.stats["not_modified"] == 0
    assert second.stats["documents"]["unchanged"] == second.stats["pages_stored"], "fingerprints still match"
    assert second.stats["items"]["updated"] == 0


def test_incremental_run_only_fetches_due_documents(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators="both", wall_clock=clock.time)
    db = tmp_path / "shop.db"
    shop(site, db, clock, incremental={"revisit": {"initial_interval": DAY, "min_interval": 3600}})
    clock.advance(3600)
    early = shop(site, db, clock, incremental={"revisit": {"initial_interval": DAY, "min_interval": 3600}})
    assert early.mode == "incremental"
    assert [e["url"] for e in run_events(db, early.run_id)] == [site.url("/catalog?page=1")], "only the seed root"
    full = shop(site, db, clock, incremental={"mode": "full"})
    assert full.stats["pages_stored"] == len(documents(db))


def test_revisit_interval_adapts_to_change_frequency(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators="both", wall_clock=clock.time, catalog_pages=1, products_per_page=4)
    db = tmp_path / "shop.db"
    revisit = {"initial_interval": DAY, "min_interval": 3600, "max_interval": 30 * DAY}
    for _ in range(5):
        site.revise(1)
        shop(site, db, clock, incremental={"mode": "full", "revisit": revisit})
        clock.advance(DAY)
    docs = documents(db)
    volatile, stable = docs["product/1"], docs["product/2"]
    assert volatile.change_count == volatile.check_count == 4
    assert stable.change_count == 0 and stable.check_count == 4
    assert volatile.revisit_interval < DAY < stable.revisit_interval
    assert stable.revisit_interval == 16 * DAY, "an unchanged document doubles its interval each check"
    assert volatile.revisit_interval >= 3600
    assert volatile.next_visit < stable.next_visit
    versions = [v.version for v in Store(db).items.versions("shop", key="SKU-0001")]
    assert versions == [1, 2, 3, 4, 5]


def test_revisit_rules_override_by_url_pattern(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time)
    db = tmp_path / "shop.db"
    shop(site, db, clock, incremental={"revisit_rules": [{"pattern": r"/catalog", "initial_interval": 7200}]})
    docs = documents(db)
    assert docs["catalog?page=1"].revisit_interval == 7200
    assert docs["product/1"].revisit_interval == DAY


def test_server_freshness_is_a_lower_bound(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators="both", wall_clock=clock.time, cache_control="max-age=604800")
    db = tmp_path / "shop.db"
    shop(site, db, clock, max_depth=1)
    assert documents(db)["product/1"].revisit_interval == 7 * DAY
    shop(site, tmp_path / "ignoring.db", clock, max_depth=1,
         incremental={"revisit": {"honour_freshness": False}})
    with Store(tmp_path / "ignoring.db") as store:
        assert {d.revisit_interval for d in store.documents.iter_documents("shop")} == {DAY}


def test_sitemap_mode_fetches_only_pages_with_newer_lastmod(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time, product_lastmod="2023-01-01")
    db = tmp_path / "shop.db"
    sitemap = site.url("/sitemap.xml")
    first = shop(site, db, clock, seeds=[], sitemaps=[sitemap], extract={"links": [{"allow": ["^$"]}]})
    assert first.stats["sitemap_urls"] == 12 and first.stats["items"]["new"] == 12
    with Store(db) as store:
        (mark,) = store.watermarks.all("shop")
        assert mark.seed == sitemap and mark.last_completed == first_started(store, first.run_id)
        assert mark.lastmod_high is not None

    clock.advance(3600)
    site.revise(5)
    second = shop(site, db, clock, seeds=[], sitemaps=[sitemap], extract={"links": [{"allow": ["^$"]}]},
                  incremental={"mode": "sitemap"})
    urls = [e["url"] for e in run_events(db, second.run_id)]
    assert urls == [sitemap, site.url("/product/5")]
    assert second.stats["sitemap_promoted"] == 1 and second.stats["sitemap_skipped"] == 11
    assert stored_items(db)["SKU-0005"].version == 2
    with Store(db) as store:
        (mark,) = store.watermarks.all("shop")
        assert mark.lastmod_high == site.product_modified(5)

    clock.advance(3600)
    third = shop(site, db, clock, seeds=[], sitemaps=[sitemap], extract={"links": [{"allow": ["^$"]}]},
                 incremental={"mode": "sitemap"})
    assert [e["url"] for e in run_events(db, third.run_id)] == [sitemap], "nothing newer than the watermark"


def first_started(store: Store, run_id: int) -> float:
    run = store.runs.get(run_id)
    assert run is not None
    return float(run["started_at"])


def test_incremental_mode_promotes_sitemap_changes_before_they_are_due(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time, validators="both", product_lastmod="2023-01-01")
    db = tmp_path / "shop.db"
    sitemap = site.url("/sitemap.xml")
    shop(site, db, clock, sitemaps=[sitemap])
    clock.advance(3600)
    site.revise(7)
    second = shop(site, db, clock, sitemaps=[sitemap])
    assert second.mode == "incremental"
    fetched = {e["url"] for e in run_events(db, second.run_id)}
    assert site.url("/product/7") in fetched and site.url("/product/6") not in fetched
    assert stored_items(db)["SKU-0007"].version == 2


def test_budget_limited_run_does_not_complete_the_watermark(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time)
    db = tmp_path / "shop.db"
    first = shop(site, db, clock, max_pages=3)
    assert first.status == "budget_exhausted"
    with Store(db) as store:
        (mark,) = store.watermarks.all("shop")
        assert mark.last_completed is None and mark.last_run_id == first.run_id
    second = shop(site, db, clock)
    assert second.mode == "full", "auto mode stays full until a run completes"
    assert len(stored_items(db)) == 12
    third = shop(site, db, clock)
    assert third.mode == "incremental"


def test_removed_products_are_tombstoned_and_restored(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time)
    db = tmp_path / "shop.db"
    shop(site, db, clock)
    site.options.catalog_pages = 2
    clock.advance(DAY)
    second = shop(site, db, clock, incremental={"mode": "full"})
    assert second.stats["items"]["deleted"] == 4
    live = stored_items(db)
    assert sorted(live) == [f"SKU-{n:04d}" for n in range(1, 9)]
    with Store(db) as store:
        gone = [i for i in store.items.iter("shop", include_deleted=True) if i.deleted_at is not None]
        assert sorted(i.key for i in gone) == [f"SKU-{n:04d}" for n in range(9, 13)]
        history = item_history(store.items, "shop", "product", "SKU-0010")
        assert [c.kind.value for c in history] == ["created", "deleted"]
        assert "catalog?page=3" in [u.split("/", 3)[3] for u in store.links.orphans("shop")]

    site.options.catalog_pages = 3
    clock.advance(DAY)
    shop(site, db, clock, incremental={"mode": "full"})
    with Store(db) as store:
        history = item_history(store.items, "shop", "product", "SKU-0010")
        assert [(c.version, c.kind.value) for c in history] == [(1, "created"), (2, "deleted"), (3, "restored")]
        assert history[-1].fields == [], "restored with the same content"
        assert store.items.count("shop") == 12 and store.links.orphans("shop") == []


def test_orphans_are_not_revisited_incrementally(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time, catalog_pages=3, products_per_page=1)
    db = tmp_path / "shop.db"
    links = {"links": [{"allow": [r"/catalog\?page=\d+$"]}]}
    shop(site, db, clock, extract=links)
    site.options.catalog_pages = 2
    clock.advance(2 * DAY)
    shop(site, db, clock, extract=links)
    with Store(db) as store:
        assert [u.split("/", 3)[3] for u in store.links.orphans("shop")] == ["catalog?page=3"]
        assert store.links.inbound("shop", site.url("/catalog?page=3"), include_removed=True) == [
            site.url("/catalog?page=2")]
    clock.advance(2 * DAY)
    third = shop(site, db, clock, extract=links)
    fetched = {e["url"] for e in run_events(db, third.run_id)}
    assert site.url("/catalog?page=2") in fetched and site.url("/catalog?page=3") not in fetched


def test_fingerprints_ignore_configured_noise(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time, noisy_footer=True, catalog_pages=1)
    noisy, quiet = tmp_path / "noisy.db", tmp_path / "quiet.db"
    ignore = {"fingerprint_ignore": [r"Generated at [^|]*GMT"]}
    for _ in range(2):
        shop(site, noisy, clock, incremental={"mode": "full"})
        shop(site, quiet, clock, incremental={"mode": "full", **ignore})
        clock.advance(DAY)
    assert documents(noisy)["product/1"].change_count == 1
    assert documents(quiet)["product/1"].change_count == 0
    for db in (noisy, quiet):
        assert all(item.version == 1 for item in stored_items(db).values()), "items did not change either way"


def test_change_feed_reports_what_changed_since_a_time(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time, validators="etag")
    db = tmp_path / "shop.db"
    shop(site, db, clock)
    clock.advance(2 * DAY)
    checkpoint = clock.time()
    site.revise(2)
    site.revise(2)
    shop(site, db, clock)
    with Store(db) as store:
        (change,) = list(change_feed(store.items, "shop", since=checkpoint))
    assert (change.key, change.kind.value, change.version) == ("SKU-0002", "updated", 2)
    assert [(f.path, f.before, f.after) for f in change.fields] == [("price", 34.99, 36.99)]
    assert len(events(db, site.url("/product/2"))) == 2


def test_robots_txt_is_reused_across_runs_and_revalidated(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators="etag", wall_clock=clock.time, crawl_delay=3)
    db = tmp_path / "shop.db"
    seeds = [site.url("/about")]
    crawl(site, db, clock, seeds=seeds, max_depth=0)
    clock.advance(3600)
    second = crawl(site, db, clock, seeds=seeds, max_depth=0, incremental={"mode": "full"})
    assert second.stats["robots_fetches"] == 0 and site.paths().count("/robots.txt") == 1, "persisted and fresh"
    assert [h["crawl_delay"] for h in second.hosts.values()] == [3.0], "Crawl-delay restored from the stored copy"
    clock.advance(2 * DAY)
    third = crawl(site, db, clock, seeds=seeds, max_depth=0, incremental={"mode": "full"})
    assert third.stats["robots_fetches"] == 1
    request = site.requests_for("/robots.txt")[-1]
    assert request.headers.get("if-none-match") == '"robots"' and site.not_modified >= 1
    with Store(db) as store:
        (entry,) = store.robots.origins()
        assert entry["status"] == 304 and entry["state"] == "fetched" and entry["etag"] == '"robots"'
        assert entry["expires_at"] == pytest.approx(clock.time() + 86400, abs=60)
