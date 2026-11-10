"""Prometheus text exposition: rendering, parsing, merging, and metrics from crawls and the database."""

from __future__ import annotations

import asyncio
import io
import math
import random
from pathlib import Path

import pytest

from skein.cli import main
from skein.clock import FakeClock
from skein.config import parse_config
from skein.crawler import Crawler
from skein.metrics import CrawlStats, Histogram, Registry
from skein.metrics.collect import add_crawl_stats
from skein.metrics.exposition import ExpositionError, merge, parse
from skein.metrics.prometheus import format_value, write_textfile
from skein.metrics.server import MetricsServer
from skein.store import Store
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS, crawl, spider_spec


def test_rendering_follows_the_text_format(tmp_path: Path) -> None:
    registry = Registry()
    requests = registry.counter("requests_total", "Requests\nsent \\ so far.", ("host",))
    requests.inc(host='a"b')
    requests.inc(2, host='a"b')
    registry.gauge("depth", "Queue depth.").set(3.5)
    latency = registry.histogram("latency_seconds", "Latency.", (0.1, 1.0))
    for value in (0.05, 0.5, 5.0):
        latency.observe(value)
    registry.gauge("unused", "No samples, not rendered.")
    text = registry.render()
    assert text.splitlines() == [
        "# HELP skein_requests_total Requests\\nsent \\\\ so far.",
        "# TYPE skein_requests_total counter",
        'skein_requests_total{host="a\\"b"} 3',
        "# HELP skein_depth Queue depth.",
        "# TYPE skein_depth gauge",
        "skein_depth 3.5",
        "# HELP skein_latency_seconds Latency.",
        "# TYPE skein_latency_seconds histogram",
        'skein_latency_seconds_bucket{le="0.1"} 1',
        'skein_latency_seconds_bucket{le="1"} 2',
        'skein_latency_seconds_bucket{le="+Inf"} 3',
        "skein_latency_seconds_sum 5.55",
        "skein_latency_seconds_count 3",
    ]
    write_textfile(tmp_path / "m.prom", registry)
    assert (tmp_path / "m.prom").read_text() == text
    assert format_value(math.inf) == "+Inf" and format_value(float("nan")) == "NaN" and format_value(2.0) == "2"


def test_registry_validation() -> None:
    registry = Registry()
    with pytest.raises(ValueError, match="_total"):
        registry.counter("requests", "x")
    with pytest.raises(ValueError, match="label"):
        registry.gauge("g", "x", ("le",))
    with pytest.raises(ValueError, match="metric name"):
        registry.gauge("bad-name", "x")
    counter = registry.counter("c_total", "x", ("a",))
    with pytest.raises(ValueError, match="takes labels"):
        counter.inc(b="1")
    with pytest.raises(ValueError, match="only increase"):
        counter.inc(-1, a="x")
    with pytest.raises(TypeError):
        counter.observe(1.0, a="x")
    with pytest.raises(ValueError, match="already registered"):
        registry.gauge("c_total", "x", ("a",))
    with pytest.raises(ValueError, match="ascending"):
        registry.histogram("h", "x", (2.0, 1.0))


def test_parse_and_merge_worker_textfiles() -> None:
    texts = []
    for worker, count in (("w1", 3), ("w2", 4)):
        registry = Registry()
        registry.counter("requests_total", "Requests.", ("spider", "worker")).set(count, spider="s", worker=worker)
        histogram = Histogram((0.5,))
        for _ in range(count):
            histogram.observe(0.1)
        registry.histogram("latency_seconds", "Latency.", (0.5,), ("spider", "worker")).set_histogram(
            histogram, spider="s", worker=worker)
        texts.append(registry.render())
    families = parse(texts[0])
    assert families["skein_latency_seconds"].kind == "histogram"
    assert [s.name for s in families["skein_latency_seconds"].samples] == [
        "skein_latency_seconds_bucket", "skein_latency_seconds_bucket", "skein_latency_seconds_sum",
        "skein_latency_seconds_count"]
    merged = parse(merge(texts))
    (requests,) = merged["skein_requests_total"].samples
    assert requests.labels == {"spider": "s"} and requests.value == 7
    buckets = {s.labels["le"]: s.value for s in merged["skein_latency_seconds"].samples if "le" in s.labels}
    assert buckets == {"0.5": 7, "+Inf": 7}
    kept = parse(merge(texts, drop_labels=()))
    assert len(kept["skein_requests_total"].samples) == 2
    with pytest.raises(ExpositionError):
        parse('metric{label="unterminated} 1')
    with pytest.raises(ExpositionError):
        parse("metric not-a-number")
    with pytest.raises(ExpositionError):
        merge(["# TYPE x counter\nx 1\n", "# TYPE x gauge\nx 1\n"])


def test_crawl_statistics_and_pipeline_as_metrics(site: FixtureSite, tmp_path: Path) -> None:
    textfile = tmp_path / "crawl.prom"
    result = crawl(site, tmp_path / "shop.db", extract={"items": PRODUCT_ITEMS}, max_depth=2,
                   project={"metrics": {"textfile": str(textfile), "interval": 0.1}})
    families = parse(textfile.read_text())
    labels = {"spider": "shop", "worker": "local-shop"}
    requests = families["skein_requests_total"].samples
    assert requests == [type(requests[0])("skein_requests_total", labels, result.stats["requests"])]
    statuses = {s.labels["status"]: s.value for s in families["skein_responses_by_status_total"].samples}
    assert statuses == {k: float(v) for k, v in result.stats["status_counts"].items()}
    assert families["skein_fetch_latency_seconds"].kind == "histogram"
    stages = {s.labels["stage"] for s in families["skein_queue_capacity"].samples}
    assert stages == {"fetch", "parse", "write"}
    batches = [s for s in families["skein_write_batch_size"].samples if s.name.endswith("_count")]
    assert batches[0].value == result.stats["write_batches"]
    leases = {s.labels["event"]: s.value for s in families["skein_lease_events_total"].samples}
    claims = result.stats["pipeline"]["dispatch"]["claims"]
    assert leases["claimed"] == claims + result.stats["robots_denied"] and leases["lost"] == 0


def test_metrics_endpoint_while_crawling(site: FixtureSite, tmp_path: Path) -> None:
    config = parse_config({"version": 2, "metrics": {"port": 0},
                           "spiders": [spider_spec(site, extract={"items": PRODUCT_ITEMS}, max_depth=1)]})
    scraped: list[str] = []

    async def go() -> None:
        with Store(tmp_path / "shop.db") as store:
            crawler = Crawler(config.spiders[0], store, http=config.http, clock=FakeClock(), rng=random.Random(0),
                              runtime=config.runtime)
            original = crawler._after_commit

            def after_commit(batch):  # noqa: ANN001, ANN202
                original(batch)
                if not scraped and crawler.metrics_server is not None and crawler.metrics_server.port:
                    scraped.append("pending")

            crawler._after_commit = after_commit  # type: ignore[method-assign]
            task = asyncio.create_task(crawler.run())
            while not scraped:
                await asyncio.sleep(0.01)
            reader, writer = await asyncio.open_connection("127.0.0.1", crawler.metrics_server.port)  # type: ignore
            writer.write(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            scraped.append((await reader.read()).decode())
            writer.close()
            await task

    asyncio.run(go())
    response = scraped[-1]
    assert response.startswith("HTTP/1.1 200 OK") and "text/plain; version=0.0.4" in response
    assert "skein_worker_info{" in response and "skein_requests_total{" in response


def test_metrics_server_routes() -> None:
    async def go() -> list[bytes]:
        server = MetricsServer(lambda: "x 1\n")
        await server.start()
        answers = []
        for request in (b"GET /metrics HTTP/1.1\r\n\r\n", b"GET /other HTTP/1.1\r\n\r\n",
                        b"POST /metrics HTTP/1.1\r\n\r\n"):
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            writer.write(request)
            answers.append(await reader.read())
            writer.close()
        await server.stop()
        assert server.scrapes == 1
        return answers

    ok, missing, wrong = asyncio.run(go())
    assert ok.endswith(b"\r\n\r\nx 1\n") and b" 404 " in missing and b" 405 " in wrong


def test_database_metrics_command(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    crawl(site, db, extract={"items": PRODUCT_ITEMS}, max_depth=1)
    out = io.StringIO()
    assert main(["metrics", "--db", str(db)], out=out) == 0
    families = parse(out.getvalue())
    states = {s.labels["state"]: s.value for s in families["skein_frontier_requests"].samples}
    assert states["done"] > 0 and families["skein_runs"].samples[0].labels["status"] == "finished"
    assert families["skein_workers"].samples[0].labels == {"spider": "shop", "state": "finished"}
    first, second = tmp_path / "a.prom", tmp_path / "b.prom"
    for path, worker in ((first, "w1"), (second, "w2")):
        registry = Registry()
        stats = CrawlStats()
        stats.apply({"requests": 2, "status": 200})
        add_crawl_stats(registry, stats, spider="shop", worker=worker)
        write_textfile(path, registry)
    merged = io.StringIO()
    assert main(["metrics", "--merge", str(first), str(second)], out=merged) == 0
    assert 'skein_requests_total{spider="shop"} 4' in merged.getvalue()


def test_health_endpoint_reports_a_stalled_worker(site: FixtureSite, tmp_path: Path) -> None:
    config = parse_config({"version": 2, "spiders": [spider_spec(site, max_depth=0)]})

    async def go() -> list[bytes]:
        with Store(tmp_path / "health.db") as store:
            crawler = Crawler(config.spiders[0], store, http=config.http, clock=FakeClock(), runtime=config.runtime)
            assert crawler.health()["healthy"] is False, "no pipeline yet"
            server = MetricsServer(lambda: "", health=crawler.health)
            await server.start()
            answers = []
            try:
                await crawler.run()
                for _ in range(2):
                    reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
                    writer.write(b"GET /healthz HTTP/1.1\r\n\r\n")
                    answers.append(await reader.read())
                    writer.close()
                    crawler.pipeline.dispatcher.in_flight = 1  # type: ignore[union-attr]
                    crawler.clock.advance(3600)  # type: ignore[attr-defined]
            finally:
                await server.stop()
            return answers

    healthy, stalled = asyncio.run(go())
    assert healthy.startswith(b"HTTP/1.1 200 OK") and b'"worker": "local-shop"' in healthy
    assert stalled.startswith(b"HTTP/1.1 503") and b'"healthy": false' in stalled
    assert MetricsServer(lambda: "")._answer("GET", "/healthz").status == 404
