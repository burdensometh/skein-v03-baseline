"""End-to-end crawls: real fixture server, real sockets, real SQLite, injected clock."""

from __future__ import annotations

import asyncio
from pathlib import Path

from skein.clock import FakeClock
from skein.config import parse_config
from skein.crawler import CrawlResult, Crawler
from skein.store import Store
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS, crawl
from tests.helpers import events as pages
from tests.helpers import stored_items as items


def test_full_crawl_of_fixture_site(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(crawl_delay=2)
    db = tmp_path / "shop.db"
    clock = FakeClock()
    result = crawl(site, db, clock, extract={"items": PRODUCT_ITEMS}, sitemaps=[site.url("/sitemap_index.xml.gz")])
    assert result.status == "finished"
    stored = items(db)
    assert len(stored) == 12
    assert stored["SKU-0002"].data == {"sku": "SKU-0002", "title": "Widget 2", "price": 34.99,
                                       "tags": ["blue", "large", "small"], "in_stock": True}
    assert stored["SKU-0004"].data["in_stock"] is False
    records = pages(db)
    urls = [p["url"] for p in records]
    assert len(urls) == len(set(urls)), "every canonical URL is fetched once"
    assert not any("utm_" in u or "#" in u for u in urls)
    assert site.url("/catalog?page=3") in urls
    kinds = {p["url"]: p["kind"] for p in records}
    assert kinds[site.url("/sitemap-pages.xml.gz")] == "sitemap"
    denied = [p for p in records if p["error"] and p["error"].startswith("robots.txt")]
    assert [p["url"] for p in denied] == [site.url("/private/secret")]
    assert site.url("/private/open") in urls
    assert site.paths().count("/robots.txt") == 1
    assert "/private/secret" not in site.paths()
    # Politeness: consecutive network fetches to the host are >= Crawl-delay apart.
    fetched = sorted(p["fetched_at"] for p in records if p["status"] is not None)
    gaps = [b - a for a, b in zip(fetched, fetched[1:])]
    assert min(gaps) >= 2.0 - 1e-9
    stats = result.stats
    assert stats["robots_denied"] == 1 and stats["sitemaps_fetched"] == 3
    assert stats["items"]["new"] == 12
    assert stats["connections"]["opened"] == 1
    lastmods = {p["url"]: p["sitemap_lastmod"] for p in records}
    assert lastmods[site.url("/product/1")] is not None


def test_recrawl_keeps_first_seen_and_updates_last_seen(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    clock = FakeClock()
    crawl(site, db, clock, extract={"items": PRODUCT_ITEMS}, max_depth=2)
    first = items(db)
    clock.advance(3600)
    second_result = crawl(site, db, clock, extract={"items": PRODUCT_ITEMS}, max_depth=2,
                          incremental={"mode": "full"})
    second = items(db)
    assert second.keys() == first.keys()
    assert second_result.stats["items"]["new"] == 0
    assert second_result.stats["items"]["unchanged"] >= len(first)
    for key, record in second.items():
        assert record.first_seen == first[key].first_seen
        assert record.last_seen > first[key].last_seen
    with Store(db) as store:
        assert [(r["status"], r["mode"]) for r in store.runs.list("shop")] == [("finished", "full")] * 2
        assert store.items.count_versions("shop") == len(first), "unchanged items get no new versions"


def test_robots_5xx_disallows_everything(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(robots_status=503)
    db = tmp_path / "shop.db"
    result = crawl(site, db, seeds=[site.url("/"), site.url("/about")])
    assert site.paths() == ["/robots.txt"]
    assert result.stats["robots_denied"] == 2
    assert all("unreachable" in p["error"] for p in pages(db))


def test_robots_404_allows_everything(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(robots_status=404)
    db = tmp_path / "shop.db"
    crawl(site, db, seeds=[site.url("/private/secret")], max_depth=0)
    assert "/private/secret" in site.paths()


def test_obey_robots_false_skips_robots(site: FixtureSite, tmp_path: Path) -> None:
    crawl(site, tmp_path / "x.db", seeds=[site.url("/private/secret")], robots={"obey": False})
    assert site.paths() == ["/private/secret"]


def test_transient_503_is_retried_until_success(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    clock = FakeClock()
    result = crawl(site, db, clock, seeds=[site.url("/flaky/one?fail=2")], max_depth=0, retry={"max_retries": 3})
    attempts = [p for p in pages(db) if "/flaky/one" in p["url"]]
    assert [p["status"] for p in attempts] == [503, 503, 200]
    assert result.stats["retries"] == 2
    assert attempts[1]["fetched_at"] - attempts[0]["fetched_at"] >= 3.0
    assert result.stats["status_counts"] == {"200": 1, "503": 2}


def test_retries_are_bounded(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    result = crawl(site, db, seeds=[site.url("/status/500")], retry={"max_retries": 2})
    assert [p["status"] for p in pages(db)] == [500, 500, 500]
    assert pages(db)[-1]["error"] == "HTTP 500"
    assert result.stats["retries"] == 2


def test_network_failures_are_recorded(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    seeds = [site.url(p) for p in ("/reset", "/slow?delay=2", "/redirect/loop/a", "/big?size=400000", "/truncated")]
    result = crawl(site, db, seeds=seeds, retry={"max_retries": 1}, concurrency=2,
                   http={"read_timeout": 0.2, "max_body_bytes": 100_000})
    errors: dict[str, list[str]] = {}
    for record in pages(db):
        errors.setdefault(record["url"], []).append(record["error"] or "")
    assert errors[site.url("/reset")][0].startswith("ConnectionClosed") and len(errors[site.url("/reset")]) == 2
    assert errors[site.url("/slow?delay=2")][-1].startswith("TimeoutError")
    assert errors[site.url("/redirect/loop/a")] == [e for e in errors[site.url("/redirect/loop/a")] if "RedirectLoop" in e]
    assert len(errors[site.url("/redirect/loop/a")]) == 1, "redirect loops are permanent"
    assert errors[site.url("/big?size=400000")][0].startswith("BodyTooLarge")
    assert len(errors[site.url("/big?size=400000")]) == 1
    assert errors[site.url("/truncated")][-1].startswith("ConnectionClosed")
    assert result.status == "finished"
    assert set(result.stats["errors"]) == {"ConnectionClosed", "TimeoutError", "RedirectLoop", "BodyTooLarge"}


def test_malformed_html_still_yields_items_and_links(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    broken_items = [{
        "name": "card", "url_pattern": "/broken$", "scope": "div.product", "key": ["sku"],
        "fields": {"sku": {"attr": "data-sku"}, "title": "a.title", "price": {"selector": ".price", "regex": r"\$([\d.]+)", "type": "float"},
                   "tags": {"selector": "ul.tags li", "multiple": True}},
    }]
    crawl(site, db, seeds=[site.url("/broken")], max_depth=1,
          extract={"items": broken_items, "links": [{"allow": ["/catalog"]}]})
    (card,) = items(db).values()
    assert card.data == {"sku": "SKU-9001", "title": "Mangled widget", "price": 5.0, "tags": ["alpha", "beta", "gamma"]}
    assert site.url("/catalog?page=2") in [p["url"] for p in pages(db)]


def test_charsets_end_to_end(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    names = ["latin1", "meta-gbk", "http-equiv-sjis", "bom-utf8", "bom-utf16"]
    text_items = [{"name": "text", "fields": {"text": "#text"}}]
    crawl(site, db, seeds=[site.url(f"/charset/{n}") for n in names], extract={"items": text_items}, max_depth=0)
    stored = {key.rsplit("/", 1)[1]: record.data["text"] for key, record in items(db).items()}
    assert stored == {"latin1": "Café crème brûlée", "meta-gbk": "商品目录", "http-equiv-sjis": "日本語のページ",
                      "bom-utf8": "Café crème brûlée", "bom-utf16": "Ünïcödé"}
    charsets = {p["url"].rsplit("/", 1)[1]: p["charset"] for p in pages(db)}
    assert charsets["meta-gbk"] == "gb18030" and charsets["bom-utf16"] == "utf-16-le"


def test_same_origin_redirect_followed_inline(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    result = crawl(site, db, seeds=[site.url("/redirect/chain/4")], max_depth=0)
    (record,) = pages(db)
    assert record["status"] == 200 and record["final_url"] == site.url("/redirect/chain/0")
    assert len(record["redirects"]) == 4
    assert result.stats["redirects_followed"] == 4


def test_cross_origin_redirect_becomes_new_request(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    origin_a, origin_b = make_site(), make_site()
    db = tmp_path / "shop.db"
    seed = origin_a.url("/redirect/to?url=" + origin_b.url("/product/3"))
    result = crawl(origin_a, db, seeds=[seed], extract={"items": PRODUCT_ITEMS}, max_depth=0,
                   allowed_domains=["127.0.0.1"])
    records = {p["url"]: p for p in pages(db)}
    assert records[seed]["status"] == 302
    assert records[origin_b.url("/product/3")]["status"] == 200
    assert "/robots.txt" in origin_b.paths()
    assert result.stats["redirects_deferred"] == 1
    assert "SKU-0003" in items(db)


def test_page_budget(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    result = crawl(site, db, max_pages=5)
    assert result.status == "budget_exhausted"
    fetched = [p for p in pages(db) if p["kind"] == "page" and p["status"] is not None]
    assert len({p["url"] for p in fetched}) == 5
    denied = [p for p in pages(db) if p["status"] is None]
    assert all(p["error"].startswith("robots.txt") for p in denied), "robots denials are not charged"


def test_depth_limit(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    result = crawl(site, db, max_depth=1)
    assert max(p["depth"] for p in pages(db)) == 1
    assert result.stats["frontier"]["too_deep"] > 0


def test_stop_request_ends_crawl(site: FixtureSite, tmp_path: Path) -> None:
    config = parse_config({"version": 2, "spiders": [{"name": "shop", "seeds": [site.url("/")],
                                                      "politeness": {"min_delay": 0.5}}]})

    async def go() -> CrawlResult:
        with Store(tmp_path / "stop.db") as store:
            crawler = Crawler(config.spiders[0], store)
            task = asyncio.create_task(crawler.run())
            await asyncio.sleep(0.3)
            crawler.stop()
            return await task

    result = asyncio.run(go())
    assert result.status == "stopped"
    assert result.stats["requests"] < 10
    assert (tmp_path / "stop.db.checkpoints" / "shop" / "snapshot.bin").exists(), "a stopped run stays resumable"


def test_throughput_without_politeness(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=40, products_per_page=5)
    db = tmp_path / "fast.db"
    result = crawl(site, db, clock=None, concurrency=16, politeness={"min_delay": 0, "per_host_concurrency": 16},
                   robots={"obey": False}, extract={"items": PRODUCT_ITEMS})
    assert result.status == "finished"
    assert result.stats["items"]["new"] == 200
    assert result.stats["requests"] >= 240


def test_x_robots_tag_header_is_honoured(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "tags.db"
    any_page = [{"name": "page", "fields": {"title": "h1"}}]
    seeds = [site.url(f"/tagged/{mode}") for mode in ("noindex", "nofollow", "other", "none", "all")]
    crawl(site, db, seeds=seeds, max_depth=1, extract={"items": any_page, "links": [{"allow": [r"/product/2$"]}]})
    stored = {key.rsplit("/", 1)[1] for key in items(db)}
    assert stored == {"nofollow", "other", "all", "2"}
    links = {p["url"].rsplit("/", 1)[1]: p["links_found"] for p in pages(db) if "/tagged/" in p["url"]}
    assert links["nofollow"] == 0 and links["none"] == 0
    assert links["noindex"] + links["other"] + links["all"] == 1, "product/2 is enqueued once"
