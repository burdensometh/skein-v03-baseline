"""Several real OS processes crawling one run: leases, cross-process politeness, a SIGKILLed worker."""

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

from skein.cli import main
from skein.config import load_config
from skein.crawler import Crawler
from skein.incremental.modes import RunMode
from skein.workers.process import run_worker
from skein.store import Store
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS

ROOT = Path(__file__).resolve().parent.parent
LINKS = [{"allow": [r"/catalog\?page=\d+$", r"/product/\d+$"]}]


def write_config(tmp_path: Path, site: FixtureSite, **project: Any) -> Path:
    config = {
        "version": 2, "database": "shop.db",
        "workers": {"lease_ttl": 3, "heartbeat_interval": 0.5, "reclaim_interval": 0.5, "poll_interval": 0.1},
        "checkpoint": {"fsync": "batch", "snapshot_every": 20},
        **project,
        "spiders": [{"name": "shop", "seeds": [site.url("/catalog?page=1")], "concurrency": 2,
                     "politeness": {"min_delay": 0, "per_host_concurrency": 4},
                     "extract": {"items": PRODUCT_ITEMS, "links": LINKS}}],
    }
    path = tmp_path / "skein.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def query(db: Path, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=2)
    except sqlite3.OperationalError:
        return []
    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def find_victim(db: Path, process: subprocess.Popen[bytes], deadline: float) -> tuple[str, int]:
    """A helper worker that holds leases and has committed results."""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("the crawl finished before a worker could be killed")
        rows = query(db, """SELECT w.id, w.pid, (SELECT COUNT(*) FROM frontier f WHERE f.state = 'active'
                                                  AND f.lease_owner = w.id),
                                   (SELECT COUNT(*) FROM fetch_events e WHERE e.worker_id = w.id)
                            FROM workers w WHERE w.role = 'worker' AND w.state = 'running'""")
        for worker_id, pid, leases, events in rows:
            if leases > 0 and events >= 3:
                return worker_id, pid
        time.sleep(0.02)
    raise AssertionError("no worker took part in the crawl")


def test_crawl_with_worker_processes_survives_a_sigkilled_worker(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=20, products_per_page=10, latency=0.08)
    config = write_config(tmp_path, site)
    db = tmp_path / "shop.db"
    process = subprocess.Popen(
        [sys.executable, "-m", "skein", "-q", "crawl", str(config), "--workers", "2", "--stats", "json"],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        victim, pid = find_victim(db, process, time.monotonic() + 60)
        held = query(db, "SELECT COUNT(*) FROM frontier WHERE lease_owner = ? AND state = 'active'", (victim,))
        os.kill(pid, signal.SIGKILL)
        stdout, stderr = process.communicate(timeout=120)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert process.returncode == 0, stderr.decode()
    summary = json.loads(stdout)
    assert held[0][0] > 0
    assert summary["status"] == "finished" and summary["finalized"] is True
    with Store(db) as store:
        (run,) = store.runs.list("shop")
        assert run["status"] == "finished"
        workers = {record.id: record for record in store.workers.list(spider="shop")}
        assert workers[victim].state == "dead" and workers[victim].stats is not None, \
            "a killed worker's statistics are rebuilt from its checkpoint"
        assert len(workers) == 3 and sorted(run["stats"]["workers"]) == sorted(workers)
        events = list(store.documents.events("shop"))
        assert run["stats"]["pages_stored"] == len(events), "merged statistics match the database"
        assert len({e["worker_id"] for e in events}) == 3, "all three processes fetched pages"
        successes = Counter(e["url"] for e in events if e["status"] == 200)
        assert len(successes) == 220 and max(successes.values()) <= 2, "at-least-once, never more than redelivery"
        assert store.items.count("shop") == 200 and store.items.count_versions("shop") == 200, \
            "redelivered pages add no item versions"
        assert dict(store.db.execute("SELECT state, COUNT(*) FROM frontier GROUP BY state").fetchall()) == {
            "done": 220}
        assert store.db.execute("SELECT COUNT(*) FROM host_slots WHERE owner IS NOT NULL").fetchone()[0] == 0
    assert not (tmp_path / "shop.db.checkpoints" / "shop" / "workers" / victim).exists()


def test_cross_process_politeness_holds_with_workers(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=3, products_per_page=8, latency=0.02)
    spider_politeness = {"min_delay": 0.15, "per_host_concurrency": 1}
    config_path = write_config(tmp_path, site)
    config = json.loads(config_path.read_text())
    config["spiders"][0]["politeness"] = spider_politeness
    config_path.write_text(json.dumps(config))
    out = io.StringIO()
    assert main(["-q", "crawl", str(config_path), "--workers", "1", "--stats", "json"], out=out) == 0
    summary = json.loads(out.getvalue())
    assert summary["status"] == "finished"
    assert len(site.requests) >= 28 and "local-shop" in summary["stats"]["workers"]
    with Store(tmp_path / "shop.db") as store:
        times = sorted(row[0] for row in store.db.execute("SELECT fetched_at FROM fetch_events"))
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert min(gaps) >= 0.15 - 0.03, "one host slot and min_delay are shared by both processes"


def test_worker_without_a_running_run_exits_idle(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    out = io.StringIO()
    assert main(["-q", "worker", str(config), "--id", "lonely", "--stats", "json"], out=out) == 0
    result = json.loads(out.getvalue())
    assert result["status"] == "idle" and result["worker"] == "lonely"
    listing = io.StringIO()
    assert main(["workers", "list", "--db", str(tmp_path / "shop.db")], out=listing) == 0
    assert "no workers registered" in listing.getvalue()


def test_following_worker_joins_successive_runs_until_stopped(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(catalog_pages=3, products_per_page=6, latency=0.02)
    config = load_config(write_config(tmp_path, site))

    async def scenario() -> list[Any]:
        stop = asyncio.Event()
        worker = asyncio.create_task(run_worker(config, worker_id="follower", wait_for_run=0.0, stop=stop,
                                                follow=True))
        for mode in ("full", "full"):
            with Store(config.database) as store:
                crawler = Crawler(config.spiders[0], store, http=config.http, runtime=config.runtime,
                                  checkpoint_dir=config.checkpoint_root(), mode=RunMode(mode))
                result = await crawler.run()
                assert result.status == "finished"
            for _ in range(200):
                await asyncio.sleep(0.02)
                if worker.done():
                    break
                with Store(config.database) as store:
                    record = store.workers.get("follower")
                if record is not None and record.state == "finished" and record.run_id == result.run_id:
                    break
        stop.set()
        return await asyncio.wait_for(worker, 30)

    results = asyncio.run(scenario())
    assert [r.status for r in results] == ["finished"] * len(results) and 1 <= len(results) <= 2
    with Store(config.database) as store:
        assert [run["status"] for run in store.runs.list("shop")] == ["finished", "finished"]
        assert {e["worker_id"] for e in store.documents.events("shop")} >= {"local-shop"}
