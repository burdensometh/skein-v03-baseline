"""Canonicalisation rules version 2 and the rekeying of stored URLs it implies."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from skein.clock import FakeClock
from skein.config import ConfigError, parse_config
from skein.frontier import CrawlRequest, Frontier
from skein.store import CanonicalRulesError, FetchEvent, Store, rekey_spider
from skein.store.items import PageItems
from skein.extract.extractor import ExtractedItem
from skein.urls import Canonicalizer, QueryPolicy
from skein.urls.tracking import TrackingFilter, strip_session_params
from tests.fixtures_site import FixtureSite
from tests.helpers import crawl

V2 = Canonicalizer.strict()
RAW_URLS = [
    "HTTP://Shop.Example:80/a/./b/../c?utm_source=x&b=2&a=1#frag",
    "http://shop.example/p;jsessionid=AB12?gclid=1&fbclid=2",
    "http://shop.example/p?x=1&x=0&mc_eid=9&_ga=1.2",
    "https://shop.example:443/%7Euser/?q=caf%c3%a9&utm_medium=email",
    "http://shop.example/cart;JSESSIONID=1;color=red/next;sid=9",
    "http://shop.example/search?",
]


def test_version_2_strips_tracking_and_session_ids_and_sorts_queries() -> None:
    assert V2.key("http://shop.example/p?utm_source=x&b=2&a=1") == "http://shop.example/p?a=1&b=2"
    assert V2.key("http://shop.example/p;jsessionid=AB12?gclid=1&fbclid=2") == "http://shop.example/p"
    assert V2.key("http://shop.example/p?x=1&x=0&a=3") == "http://shop.example/p?a=3&x=1&x=0", \
        "repeated names keep their relative order"
    assert V2.key("http://shop.example/cart;JSESSIONID=1;color=red/next;sid=9") == \
        "http://shop.example/cart;color=red/next"
    assert V2.key("http://shop.example/p?_hsenc=a&MC_CID=b&ref_src=c") == "http://shop.example/p"
    assert Canonicalizer().key("http://shop.example/p?utm_source=x&b=2&a=1") == \
        "http://shop.example/p?utm_source=x&b=2&a=1", "rules version 1 is unchanged"


def test_version_2_options() -> None:
    preserve = Canonicalizer.strict(query_order="preserve")
    assert preserve.key("http://s/p?b=1&a=2&utm_id=3") == "http://s/p?b=1&a=2"
    extra = Canonicalizer.strict(tracking=("ref", "aff_*"))
    assert extra.key("http://s/p?ref=x&aff_id=1&id=4") == "http://s/p?id=4"
    keep = Canonicalizer.strict(keep_only=("utm_source", "id"))
    assert keep.key("http://s/p?utm_source=a&utm_medium=b&id=1") == "http://s/p?id=1&utm_source=a", \
        "an explicit keep list wins over the tracking catalogue"
    assert TrackingFilter().is_tracking("UTM%5FSource") and not TrackingFilter().is_tracking("page")
    assert strip_session_params("/a;phpsessid=1/b") == "/a/b"
    with pytest.raises(ValueError):
        Canonicalizer.strict(query_order="random")
    with pytest.raises(ValueError):
        Canonicalizer(rules=3)


@pytest.mark.parametrize("raw", RAW_URLS)
def test_version_2_applied_to_a_version_1_key_equals_version_2_of_the_url(raw: str) -> None:
    v1 = Canonicalizer(QueryPolicy(drop_empty_values=True))
    assert V2.key(v1.key(raw)) == V2.key(raw)
    assert V2.key(V2.key(raw)) == V2.key(raw), "idempotent"


def test_signatures_identify_settings() -> None:
    assert V2.signature() == Canonicalizer.strict().signature()
    assert V2.signature().startswith("r2:") and Canonicalizer().signature().startswith("r1:")
    assert V2.signature() != Canonicalizer.strict(remove=("sid",)).signature()
    assert V2.signature() != Canonicalizer.strict(query_order="preserve").signature()


def spec(**canonical: object) -> dict[str, object]:
    return {"version": 2, "spiders": [{"name": "s", "seeds": ["http://a/"], "canonical": canonical}]}


def test_configuration_selects_the_rules_version() -> None:
    assert parse_config(spec()).spiders[0].canonicalizer.rules == 2
    legacy = parse_config(spec(rules=1, sort_query=True)).spiders[0].canonicalizer
    assert legacy.rules == 1 and legacy.key("http://a/?b=1&utm_x=2") == "http://a/?b=1&utm_x=2"
    tuned = parse_config(spec(tracking_params=["ref"], query_order="preserve")).spiders[0].canonicalizer
    assert tuned.key("http://a/?z=1&ref=2&a=3") == "http://a/?z=1&a=3"
    for bad, path in ((spec(rules=1, query_order="sort"), "query_order"),
                      (spec(rules=1, tracking_params=["x"]), "tracking_params"),
                      (spec(sort_query=True, query_order="preserve"), "sort_query"),
                      (spec(rules=3), "rules")):
        with pytest.raises(ConfigError) as info:
            parse_config(bad)
        assert any(issue.path.endswith(path) for issue in info.value.issues), info.value.issues


def seed_variants(store: Store, clock: FakeClock) -> None:
    """Rows written under rules-1 keys: three spellings of /p, two of /q, one plain URL."""
    frontier = Frontier(store.db, "s", clock)
    frontier.run_id = 1
    store.db.execute("INSERT INTO runs (id, spider, started_at, status) VALUES (1, 's', 0, 'finished')")
    for url, depth, priority in (("http://a/p?utm_source=n", 2, 0), ("http://a/p", 1, 5),
                                 ("http://a/p?fbclid=x", 3, 1), ("http://a/q?b=1&a=2", 1, 0),
                                 ("http://a/q?a=2&b=1", 1, 0), ("http://a/plain", 0, 0)):
        frontier.add(CrawlRequest(url, depth, priority, seed="http://a/plain"))
    store.db.execute("UPDATE frontier SET state = 'done' WHERE url NOT LIKE '%fbclid%'")
    store.db.execute("UPDATE frontier SET state = 'pending' WHERE url LIKE '%fbclid%'")
    for at, url in ((10.0, "http://a/p?utm_source=n"), (20.0, "http://a/p"), (30.0, "http://a/p"),
                    (15.0, "http://a/q?b=1&a=2"), (25.0, "http://a/q?a=2&b=1")):
        store.record_fetch(FetchEvent("s", url, at, status=200, fingerprint=f"f{at}", etag=f'"{at}"'), run_id=1)
    store.items.write([PageItems("s", "http://a/p?utm_source=n", 10.0,
                                 [ExtractedItem("thing", "k1", {"v": 1}, "http://a/p?utm_source=n")], ("thing",))])
    store.links.replace_outlinks("s", "http://a/plain", ["http://a/p?utm_source=n", "http://a/p"], 5.0)
    store.watermarks.started("s", "http://a/plain", "page", 1, 0.0)


def test_rekey_merges_rows_with_documented_semantics(tmp_path: Path) -> None:
    clock = FakeClock()
    with Store(tmp_path / "r.db") as store:
        seed_variants(store, clock)
        with store.transaction():
            report = rekey_spider(store.db, "s", V2, clock.time())
        assert (report.renamed, report.frontier_merged, report.documents_merged) == (3, 3, 2)
        assert report.events_rewritten == 2 and report.items_rewritten == 1 and report.links_merged == 1
        rows = {r["url"]: dict(r) for r in store.db.execute("SELECT * FROM frontier")}
        assert set(rows) == {"http://a/p", "http://a/q?a=2&b=1", "http://a/plain"}
        merged = rows["http://a/p"]
        assert merged["state"] == "pending", "outstanding work wins"
        assert (merged["depth"], merged["priority"]) == (1, 5), "shallowest depth, highest priority"
        assert merged["lease_owner"] is None and merged["attempts"] == 0
        document = store.documents.get("s", "http://a/p")
        assert document is not None and document.fetch_count == 3 and document.check_count == 1
        assert document.etag == '"30.0"', "content state from the latest success"
        assert document.first_fetched == 10.0 and document.last_success == 30.0
        assert [e["url"] for e in store.documents.events("s")] == ["http://a/p", "http://a/p", "http://a/p",
                                                                   "http://a/q?a=2&b=1", "http://a/q?a=2&b=1"]
        (item,) = store.items.iter("s")
        assert item.url == "http://a/p"
        assert store.links.inbound("s", "http://a/p") == ["http://a/plain"], "duplicate links merged"
        assert store.canonical.get("s").signature == V2.signature()  # type: ignore[union-attr]
        with store.transaction():
            again = rekey_spider(store.db, "s", V2, clock.time())
        assert again.renamed == 0, "rekeying is idempotent"


def test_stored_rules_cannot_be_downgraded(tmp_path: Path) -> None:
    with Store(tmp_path / "r.db") as store:
        with store.transaction():
            store.canonical.record("s", V2, 1.0)
        assert not store.canonical.needs_rekey("s", V2)
        assert store.canonical.needs_rekey("s", Canonicalizer.strict(tracking=("ref",)))
        with pytest.raises(CanonicalRulesError, match="version 2"):
            store.canonical.needs_rekey("s", Canonicalizer())


def test_crawl_rekeys_when_the_spider_settings_change(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    links = [{"allow": [r"/tracked", r"/product/\d+", r"/catalog\?"]}]
    first = crawl(site, db, seeds=[site.url("/tracked")], max_depth=1,
                  canonical={"rules": 1, "remove_params": []}, extract={"links": links})
    assert first.stats["run"]["rekeyed"] is None
    with Store(db) as store:
        before = {r[0] for r in store.db.execute("SELECT url FROM frontier")}
        assert store.canonical.get("shop").version == 1  # type: ignore[union-attr]
    assert any("utm_" in url for url in before)
    second = crawl(site, db, seeds=[site.url("/tracked")], max_depth=1, extract={"links": links})
    rekeyed = second.stats["run"]["rekeyed"]
    assert rekeyed is not None and rekeyed["renamed"] >= 5 and rekeyed["frontier_merged"] >= 4
    with Store(db) as store:
        after = {r[0] for r in store.db.execute("SELECT url FROM frontier")}
        assert not any("utm_" in u or "jsessionid" in u or "gclid" in u for u in after)
        assert len(after) < len(before)
        merges = store.canonical.merges("shop")
        assert any(m["collided"] for m in merges)
        assert all(m["new_url"] in after for m in merges if m["collided"] and "/product/1" in m["new_url"])
        assert all("utm_" not in m["new_url"] for m in merges) and json.loads(json.dumps(merges)) == merges
    with pytest.raises(CanonicalRulesError):
        crawl(site, db, seeds=[site.url("/tracked")], canonical={"rules": 1})
