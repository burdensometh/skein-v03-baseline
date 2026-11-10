"""The staged pipeline: bounded queues with backpressure, batched writes, stage isolation."""

from __future__ import annotations

import asyncio
import random
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from skein.clock import FakeClock
from skein.config import parse_config
from skein.crawler import Crawler
from skein.pipeline import StageQueue
from skein.store import Store
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS, crawl, spider_spec

LINKS = [{"allow": [r"/catalog\?page=\d+$", r"/product/\d+$"]}]


def test_queue_backpressure_and_closing() -> None:
    async def scenario() -> dict[str, Any]:
        queue: StageQueue[int] = StageQueue("test", 2, consumers=2)
        received: list[int | None] = []

        async def producer() -> None:
            for n in range(6):
                await queue.put(n)
            await queue.close()

        async def consumer() -> None:
            while True:
                await asyncio.sleep(0)
                value = await queue.get()
                received.append(value)
                if value is None:
                    return

        await asyncio.gather(producer(), consumer(), consumer())
        stats = queue.stats()
        with pytest.raises(RuntimeError):
            await queue.put(99)
        return {"received": received, "stats": stats}

    outcome = asyncio.run(scenario())
    assert sorted(v for v in outcome["received"] if v is not None) == list(range(6))
    assert outcome["received"].count(None) == 2, "each consumer sees the end once"
    stats = outcome["stats"]
    assert stats.puts == 6 and stats.high_water == 2 and stats.blocked_puts > 0 and stats.depth == 0
    with pytest.raises(ValueError):
        StageQueue("bad", 0)


def test_get_nowait_and_free() -> None:
    async def scenario() -> None:
        queue: StageQueue[str] = StageQueue("q", 3)
        await queue.put("a")
        assert queue.free == 2 and len(queue) == 1
        assert queue.get_nowait() == "a"
        with pytest.raises(asyncio.QueueEmpty):
            queue.get_nowait()
        await queue.close()
        assert queue.get_nowait() is None and len(queue) == 0

    asyncio.run(scenario())


def run_crawl(site: FixtureSite, db: Path, project: dict[str, Any], patch: Any = None, **spider: Any) -> Any:
    config = parse_config({"version": 2, **project, "spiders": [spider_spec(
        site, seeds=[site.url("/catalog?page=1")], politeness={"min_delay": 0, "per_host_concurrency": 8},
        concurrency=8, extract={"items": PRODUCT_ITEMS, "links": LINKS}, **spider)]})

    async def go() -> Any:
        with Store(db) as store:
            crawler = Crawler(config.spiders[0], store, http=config.http, clock=FakeClock(), rng=random.Random(0),
                              runtime=config.runtime)
            if patch is not None:
                patch(crawler)
            return await crawler.run()

    return asyncio.run(go())


def test_writer_batches_commits_under_load(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=6, products_per_page=8)
    batched = run_crawl(site, tmp_path / "batched.db", {"pipeline": {"write_batch": 64}})
    single = run_crawl(site, tmp_path / "single.db", {"pipeline": {"write_batch": 1}})
    for result in (batched, single):
        assert result.status == "finished" and result.stats["items"]["new"] == 48
    write = batched.stats["pipeline"]["write"]
    assert write["outcomes"] == batched.stats["pipeline"]["dispatch"]["claims"], "one outcome per claim"
    assert batched.stats["write_batches"] < write["outcomes"], "several outcomes share a transaction"
    assert batched.stats["write_batches"] == write["batches"]
    assert single.stats["write_batches"] == single.stats["pipeline"]["write"]["outcomes"]
    with Store(tmp_path / "batched.db") as left, Store(tmp_path / "single.db") as right:
        assert {i.key: i.data for i in left.items.iter("shop")} == {i.key: i.data for i in right.items.iter("shop")}


def test_tiny_queues_exert_backpressure(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=4, products_per_page=8)
    result = run_crawl(site, tmp_path / "tight.db",
                       {"pipeline": {"fetch_queue": 1, "parse_queue": 1, "write_queue": 1, "parse_tasks": 1}})
    queues = result.stats["pipeline"]["queues"]
    assert result.status == "finished" and result.stats["items"]["new"] == 32
    assert all(queue["high_water"] <= 1 for queue in queues.values())
    assert sum(queue["blocked_puts"] for queue in queues.values()) > 0, "producers had to wait for room"


def test_parsing_in_threads_gives_the_same_result(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=2, products_per_page=6)
    threaded = run_crawl(site, tmp_path / "threads.db", {"pipeline": {"parse_threads": 2, "parse_tasks": 4}})
    assert threaded.status == "finished" and threaded.stats["items"]["new"] == 12


def test_a_failing_outcome_is_isolated_from_its_batch(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=2, products_per_page=6)
    broken = site.url("/product/3")

    def patch(crawler: Crawler) -> None:
        original = crawler.store.documents.apply

        def apply(event, run_id, revisit):  # noqa: ANN001, ANN202
            if event.url == broken and event.status == 200:
                raise RuntimeError("disk full (simulated)")
            return original(event, run_id, revisit)

        crawler.store.documents.apply = apply  # type: ignore[method-assign]

    result = run_crawl(site, tmp_path / "isolated.db", {"pipeline": {"write_batch": 64}}, patch=patch)
    assert result.status == "finished" and result.stats["errors"] == {"internal": 1}
    assert result.stats["items"]["new"] == 11
    with Store(tmp_path / "isolated.db") as store:
        states = dict(store.db.execute("SELECT url, state FROM frontier").fetchall())
        assert states[broken] == "failed" and list(states.values()).count("done") == len(states) - 1
        (event,) = store.documents.events("shop", url=broken)
        assert event["error"] == "internal error in write stage"


def test_page_budget_is_charged_once_per_page(site: FixtureSite, tmp_path: Path) -> None:
    result = crawl(site, tmp_path / "budget.db", extract={"items": PRODUCT_ITEMS}, max_pages=5,
                   politeness={"min_delay": 0})
    assert result.status == "budget_exhausted" and result.stats["budget_exhausted"]
    with Store(tmp_path / "budget.db") as store:
        assert store.db.execute("SELECT COUNT(*) FROM frontier WHERE charged_run = 1").fetchone()[0] == 5
        assert store.db.execute("SELECT pages_charged FROM runs WHERE id = 1").fetchone()[0] == 5
        assert store.db.execute("SELECT COUNT(*) FROM frontier WHERE state = 'active'").fetchone()[0] == 0


def test_lock_contention_retries_the_whole_batch(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=2, products_per_page=4)
    failures = {"left": 2}

    def patch(crawler: Crawler) -> None:
        writer_commit = crawler.store.transaction

        def transaction():  # noqa: ANN202
            if failures["left"] and crawler.pipeline is not None and crawler.pipeline.writer.outcomes > 3:
                failures["left"] -= 1
                raise sqlite3.OperationalError("database is locked")
            return writer_commit()

        crawler.store.transaction = transaction  # type: ignore[method-assign]

    result = run_crawl(site, tmp_path / "busy.db", {"pipeline": {"write_batch": 4}}, patch=patch)
    assert result.status == "finished" and result.stats["errors"] == {}
    assert result.stats["pipeline"]["write"]["busy_retries"] == 2 and result.stats["items"]["new"] == 8


def test_several_hosts_are_crawled_side_by_side(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    partner = make_site(catalog_pages=1, products_per_page=3)
    shop = make_site(catalog_pages=1, products_per_page=3, extra_links=(partner.url("/catalog?page=1"),))
    result = crawl(shop, tmp_path / "hosts.db", clock, allowed_domains=["127.0.0.1"], max_depth=3,
                   politeness={"min_delay": 2.0}, concurrency=4,
                   extract={"items": PRODUCT_ITEMS, "links": [{"allow": [r"/catalog\?page=1$", r"/product/\d+$",
                                                                         r"/$"]}]})
    assert result.status == "finished" and set(result.hosts) == {shop.base_url, partner.base_url}
    assert all(host["requests"] == 7 for host in result.hosts.values()), \
        "robots.txt, home, catalog and four product URLs per host"
    with Store(tmp_path / "hosts.db") as store:
        hosts = {row[0]: row[1] for row in store.db.execute(
            "SELECT host_key, COUNT(*) FROM frontier GROUP BY host_key")}
        assert len(hosts) == 2 and store.items.count("shop") == 3, "both shops sell the same SKUs"
    elapsed = clock.monotonic()
    assert elapsed < 2.0 * 12, "two hosts at 2 s spacing each take about half the time of one after the other"
    assert len(shop.requests) == len(partner.requests) == 7


def test_global_concurrency_caps_requests_in_flight_across_processes(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=3, products_per_page=6, latency=0.01)
    observed: list[int] = []

    def patch(crawler: Crawler) -> None:
        original = crawler.client.request

        async def request(*args: Any, **kwargs: Any) -> Any:
            observed.append(crawler.frontier.in_flight())
            return await original(*args, **kwargs)

        crawler.client.request = request  # type: ignore[method-assign]

    capped = run_crawl(site, tmp_path / "capped.db", {}, patch=patch, global_concurrency=2)
    assert capped.status == "finished" and capped.stats["items"]["new"] == 18
    assert max(observed) <= 2
    observed.clear()
    run_crawl(site, tmp_path / "free.db", {}, patch=patch)
    assert max(observed) > 2, "without the cap all eight fetchers are busy"
