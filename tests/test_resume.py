"""Crash-safe resumption: a real ``skein crawl`` subprocess killed with SIGKILL, then resumed.

What must hold after any crash: every result that was committed is still
there, the resumed run is the same run, no page that was committed is
fetched again in that run, no item gets a duplicate version, and the run
statistics rebuilt from snapshot + journal agree with the database.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from skein.checkpoint import frame, write_snapshot
from skein.clock import FakeClock
from skein.cli import main
from skein.config import parse_config
from skein.config.runtime import PipelineSettings, RuntimeSettings
from skein.crawler import Crawler
from skein.frontier import CrawlRequest, Frontier
from skein.store import Store
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS, crawl

ROOT = Path(__file__).resolve().parent.parent
PRODUCTS = 40


def write_config(tmp_path: Path, site: FixtureSite) -> Path:
    spider = {
        "name": "shop",
        "seeds": [site.url("/catalog?page=1")],
        "concurrency": 2,
        "politeness": {"min_delay": 0, "per_host_concurrency": 2},
        "canonical": {"remove_params": ["utm_*"]},
        "extract": {"items": PRODUCT_ITEMS,
                    "links": [{"allow": [r"/catalog\?page=\d+$", r"/product/\d+$"]}]},
    }
    config = {"version": 2, "database": "shop.db",
              "checkpoint": {"fsync": "batch", "fsync_batch": 4, "snapshot_every": 10},
              "spiders": [spider]}
    path = tmp_path / "skein.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def committed_events(db: Path) -> int:
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=1)
    except sqlite3.OperationalError:
        return 0
    try:
        return int(conn.execute("SELECT COUNT(*) FROM fetch_events").fetchone()[0])
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def start_and_kill(config: Path, db: Path, after_events: int) -> int:
    """Run ``skein crawl`` in a subprocess and SIGKILL it once ``after_events`` fetches are committed."""
    process = subprocess.Popen(
        [sys.executable, "-m", "skein", "-q", "crawl", str(config), "--stats", "json"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError(f"crawl finished before it could be killed: {process.stderr.read()!r}")
            seen = committed_events(db)
            if seen >= after_events:
                os.kill(process.pid, signal.SIGKILL)
                process.wait(10)
                return seen
            time.sleep(0.01)
        raise AssertionError("crawl made no progress")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(10)
        process.stdout.close()
        process.stderr.close()


def resume_in_process(config: Path) -> dict[str, Any]:
    out = io.StringIO()
    assert main(["-q", "crawl", str(config), "--stats", "json"], out=out) == 0
    return json.loads(out.getvalue())


def check_consistency(db: Path) -> dict[str, Any]:
    with Store(db) as store:
        (run,) = store.runs.list("shop")
        events = list(store.documents.events("shop"))
        successes = Counter(e["url"] for e in events if e["status"] == 200)
        assert max(successes.values()) == 1, "a committed page was fetched twice in the same run"
        items = list(store.items.iter("shop"))
        assert len(items) == PRODUCTS
        assert store.items.count_versions("shop") == PRODUCTS, "no duplicated item versions"
        assert all(item.version == 1 for item in items)
        assert run["stats"]["pages_stored"] == len(events), "statistics replayed from the journal match the DB"
        assert run["stats"]["items"]["new"] == PRODUCTS
        assert dict(store.db.execute("SELECT state, COUNT(*) FROM frontier GROUP BY state").fetchall()) == {
            "done": len(successes)}
        return run


@pytest.mark.parametrize("tear_journal", [False, True], ids=["clean-kill", "torn-journal-tail"])
def test_sigkill_mid_crawl_then_resume(make_site, tmp_path: Path, tear_journal: bool) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=8, products_per_page=5, latency=0.03)
    config = write_config(tmp_path, site)
    db = tmp_path / "shop.db"
    killed_at = start_and_kill(config, db, after_events=12)
    assert killed_at >= 12
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT status FROM runs").fetchall() == [("running",)]
        states = dict(conn.execute("SELECT state, COUNT(*) FROM frontier GROUP BY state").fetchall())
    assert states.get("pending", 0) + states.get("active", 0) > 0, "the kill happened mid-run"
    checkpoint_dir = tmp_path / "shop.db.checkpoints" / "shop"
    segments = sorted(checkpoint_dir.glob("journal-*.log"))
    assert (checkpoint_dir / "snapshot.bin").exists() and segments
    if tear_journal:
        with open(segments[-1], "ab") as handle:
            handle.write(frame(b'{"seq": 999999, "type": "charge", "url": "x"}')[:-5])

    summary = resume_in_process(config)
    assert summary["status"] == "finished" and summary["resumed"] is True
    recovery = summary["stats"]["run"]["recovery"]
    assert recovery is not None and (recovery["torn_bytes"] > 0) is tear_journal
    run = check_consistency(db)
    assert run["resumes"] == 1 and run["status"] == "finished"
    assert not checkpoint_dir.exists() or not any(checkpoint_dir.iterdir()), "a finished run leaves no checkpoint"

    reference_db = tmp_path / "reference.db"
    reference = crawl(site, reference_db, None, seeds=[site.url("/catalog?page=1")], concurrency=2,
                      politeness={"min_delay": 0, "per_host_concurrency": 2},
                      extract={"items": PRODUCT_ITEMS,
                               "links": [{"allow": [r"/catalog\?page=\d+$", r"/product/\d+$"]}]})
    with Store(db) as resumed, Store(reference_db) as uninterrupted:
        assert {i.key: i.data for i in resumed.items.iter("shop")} == {
            i.key: i.data for i in uninterrupted.items.iter("shop")}
    assert reference.stats["items"]["new"] == PRODUCTS


def test_fresh_start_abandons_the_interrupted_run(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=8, products_per_page=5, latency=0.03)
    config = write_config(tmp_path, site)
    db = tmp_path / "shop.db"
    start_and_kill(config, db, after_events=8)
    out = io.StringIO()
    assert main(["-q", "crawl", str(config), "--fresh", "--stats", "json"], out=out) == 0
    summary = json.loads(out.getvalue())
    assert summary["resumed"] is False and summary["run_id"] == 2
    with Store(db) as store:
        assert [r["status"] for r in store.runs.list("shop")] == ["abandoned", "finished"]
        assert store.items.count_versions("shop") == PRODUCTS, "pages committed before the kill are not re-versioned"


# The crash tests stop at an exact page count, so every outcome gets its own batch.
ONE_BY_ONE = RuntimeSettings(pipeline=PipelineSettings(write_batch=1))


class Crash(BaseException):
    """Stands in for the process dying at an exact point."""


def test_crash_between_journal_write_and_commit(site: FixtureSite, tmp_path: Path) -> None:
    """The journal holds a record whose transaction never committed: replay must ignore it."""
    db = tmp_path / "shop.db"
    config = parse_config({"version": 2, "spiders": [{
        "name": "shop", "seeds": [site.url("/catalog?page=1")], "politeness": {"min_delay": 0},
        "extract": {"items": PRODUCT_ITEMS, "links": [{"allow": [r"/product/\d+$"]}]}}]})
    spider = config.spiders[0]

    async def crashing_run() -> None:
        with Store(db) as store:
            crawler = Crawler(spider, store, runtime=ONE_BY_ONE)
            original = crawler.checkpoint.write

            def write(kind: str, **fields: Any) -> dict[str, Any]:
                entry = original(kind, **fields)
                if kind == "batch" and crawler.stats.pages_stored == 3:
                    raise Crash()
                return entry

            crawler.checkpoint.write = write  # type: ignore[method-assign]
            with pytest.raises(Crash):
                await crawler.run()

    asyncio.run(crashing_run())
    with Store(db) as store:
        assert [r["status"] for r in store.runs.list("shop")] == ["failed"]
        assert len(list(store.documents.events("shop"))) == 3

    async def resumed_run() -> Any:
        with Store(db) as store:
            return await Crawler(spider, store).run()

    result = asyncio.run(resumed_run())
    assert result.resumed and result.status == "finished"
    # The snapshot written on failure does not contain the record; the journal still does and replay rejects it.
    assert result.stats["run"]["recovery"]["unconfirmed"] == 1
    with Store(db) as store:
        (run,) = store.runs.list("shop")
        assert run["stats"]["pages_stored"] == len(list(store.documents.events("shop")))
        assert store.items.count_versions("shop") == store.items.count("shop") == 12


def test_unconfirmed_journal_record_after_hard_kill(site: FixtureSite, tmp_path: Path) -> None:
    """Same crash point, but no orderly shutdown: the stray record is only in the journal."""
    db = tmp_path / "shop.db"
    config = parse_config({"version": 2, "checkpoint": {"fsync": "always"}, "spiders": [{
        "name": "shop", "seeds": [site.url("/catalog?page=1")], "politeness": {"min_delay": 0},
        "extract": {"items": PRODUCT_ITEMS, "links": [{"allow": [r"/product/\d+$"]}]}}]})
    spider = config.spiders[0]

    async def killed_run() -> None:
        with Store(db) as store:
            crawler = Crawler(spider, store, checkpoint=config.checkpoint, runtime=ONE_BY_ONE)
            original = crawler.checkpoint.write
            original_close = crawler._close_session

            def write(kind: str, **fields: Any) -> dict[str, Any]:
                entry = original(kind, **fields)
                if kind == "batch" and crawler.stats.pages_stored == 2:
                    raise Crash()
                return entry

            def no_orderly_shutdown(status: str) -> tuple[dict[str, Any], None]:
                crawler.checkpoint.journal.close()
                return {}, None

            crawler.checkpoint.write = write  # type: ignore[method-assign]
            crawler._close_session = no_orderly_shutdown  # type: ignore[method-assign]
            with pytest.raises(Crash):
                await crawler.run()
            assert original_close is not None

    asyncio.run(killed_run())

    async def resumed_run() -> Any:
        with Store(db) as store:
            return await Crawler(spider, store, checkpoint=config.checkpoint).run()

    result = asyncio.run(resumed_run())
    assert result.resumed and result.stats["run"]["recovery"]["unconfirmed"] == 1
    with Store(db) as store:
        (run,) = store.runs.list("shop")
        assert run["status"] == "finished" and run["resumes"] == 1
        assert run["stats"]["pages_stored"] == len(list(store.documents.events("shop")))
        assert store.items.count_versions("shop") == store.items.count("shop") == 12


def test_resumes_a_checkpoint_written_by_0_2(site: FixtureSite, tmp_path: Path) -> None:
    """A 0.2 snapshot (format 1) with budget charges, host state and lastmod marks is imported, then resumed."""
    db = tmp_path / "shop.db"
    seed = site.url("/catalog?page=1")
    config = parse_config({"version": 2, "spiders": [{
        "name": "shop", "seeds": [seed], "politeness": {"min_delay": 0}, "max_pages": 6,
        "extract": {"items": PRODUCT_ITEMS, "links": [{"allow": [r"/product/\d+$"]}]}}]})
    spider = config.spiders[0]
    with Store(db) as store:
        with store.transaction():
            run_id = store.runs.begin("shop", 1_700_000_000.0, "full")
            store.watermarks.started("shop", seed, "page", run_id, 1_700_000_000.0)
        frontier = Frontier(store.db, "shop", FakeClock())
        frontier.run_id = run_id
        frontier.add(CrawlRequest(seed, seed=seed))
        frontier.add(CrawlRequest(site.url("/product/1"), 1, seed=seed))
    legacy = {"format": 1, "spider": "shop", "run_id": run_id, "mode": "full", "started_at": 1_700_000_000.0,
              "seq": 0, "resumes": 0, "charged": [site.url("/product/1")],
              "hosts": {site.base_url: {"crawl_delay": None, "failures": 0, "backoff_until": 0.0,
                                         "next_allowed": 0.0}},
              "robots_sitemaps": [], "lastmod_high": {seed: 1_650_000_000.0}, "stats": {"requests": 3}}
    checkpoint_dir = tmp_path / "shop.db.checkpoints" / "shop"
    checkpoint_dir.mkdir(parents=True)
    write_snapshot(checkpoint_dir / "snapshot.bin", legacy)

    async def resume() -> Any:
        with Store(db) as store:
            return await Crawler(spider, store, clock=FakeClock()).run()

    result = asyncio.run(resume())
    assert result.resumed and result.status == "budget_exhausted" and result.stats["requests"] > 3
    with Store(db) as store:
        (run,) = store.runs.list("shop")
        assert run["pages_charged"] == 6, "the charge made under 0.2 counts against the budget"
        assert store.db.execute("SELECT charged_run FROM frontier WHERE url = ?",
                                (site.url("/product/1"),)).fetchone()[0] == run_id
        assert store.watermarks.run_lastmod("shop", seed, run_id) == 1_650_000_000.0, \
            "the 0.2 sitemap high-water mark now lives on the watermark row"
