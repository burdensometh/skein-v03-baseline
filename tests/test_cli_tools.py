"""Command line tools added for 0.3: warc, dedup, workers, db rekey/merges, export --as-of."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from skein.cli import main
from skein.clock import FakeClock
from skein.store import Store
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS, crawl


def run(*argv: str) -> tuple[int, str]:
    out = io.StringIO()
    code = main(list(argv), out=out)
    return code, out.getvalue()


@pytest.fixture
def archived(make_site, tmp_path: Path) -> tuple[FixtureSite, Path]:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(validators="both", wall_clock=clock.time)
    db = tmp_path / "shop.db"
    crawl(site, db, clock, extract={"items": PRODUCT_ITEMS}, max_depth=1, project={"warc": {"enabled": True}})
    clock.advance(86400 * 3)
    site.revise(1)
    crawl(site, db, clock, extract={"items": PRODUCT_ITEMS}, max_depth=1, project={"warc": {"enabled": True}},
          incremental={"mode": "full"})
    return site, db


def test_warc_commands(archived: tuple[FixtureSite, Path], tmp_path: Path) -> None:
    site, db = archived
    archive = tmp_path / "shop.db.warc"
    code, listing = run("warc", "list", str(archive))
    assert code == 0 and " warcinfo " in listing and " revisit " in listing
    code, rows = run("warc", "list", "--json", str(archive))
    first = json.loads(rows.splitlines()[0])
    assert first["type"] == "warcinfo" and first["offset"] == 0
    code, verified = run("warc", "verify", str(archive))
    assert code == 0 and "record(s) checked" in verified
    code, message = run("warc", "index", str(archive))
    assert code == 0 and (archive / "index.cdx").exists() and "capture(s)" in message
    code, cdxj = run("warc", "index", "--cdxj", "-o", "-", str(archive))
    assert code == 0 and all(line.split(" ", 2)[2].startswith("{") for line in cdxj.splitlines())
    response = next(json.loads(r) for r in rows.splitlines() if json.loads(r)["type"] == "response")
    target = tmp_path / "payload.bin"
    code, _ = run("warc", "extract", str(archive / response["file"]), "--offset", str(response["offset"]),
                  "-o", str(target))
    assert code == 0 and target.stat().st_size > 0
    code, headers = run("warc", "extract", str(archive / response["file"]), "--offset", str(response["offset"]),
                        "--part", "headers")
    assert "WARC-Type: response" in headers
    code, replayed = run("warc", "replay", site.url("/product/1"), "--db", str(db))
    assert code == 0 and replayed.startswith("HTTP 200") and "Widget 1" in replayed and "$23.49" in replayed
    code, older = run("warc", "replay", site.url("/product/1"), "--dir", str(archive), "--at", "2023")
    assert "$22.49" in older, "the earlier capture shows the old price"
    assert run("warc", "replay", site.url("/nowhere"), "--dir", str(archive))[0] == 1
    assert run("warc", "serve", "--dir", str(archive), "--port", "0", "--duration", "0.05")[0] == 0
    broken = tmp_path / "broken.warc.gz"
    broken.write_bytes((archive / response["file"]).read_bytes()[:-7])
    code, report = run("warc", "verify", str(broken))
    assert code == 1 and "truncated tail" in report


def test_dedup_commands(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "news.db"
    crawl(site, db, seeds=[site.url("/articles")], canonical={"keep_params": ["edition"]})
    code, report = run("dedup", "bloom", "--db", str(db), "--json", "--check", site.url("/article/1"))
    data = json.loads(report)
    assert code == 0 and data["frontier_urls"] == 13 and data["probes"] == {site.url("/article/1"): True}
    assert data["fill"] > 0 and data["estimated_false_positive_rate"] < 1e-6
    code, rebuilt = run("dedup", "bloom", "--db", str(db), "--rebuild")
    assert code == 0 and "opened" in rebuilt and "missing" in rebuilt
    code, clusters = run("dedup", "near", "--db", str(db))
    assert code == 0 and clusters.count("cluster ") == 4 and "distance" in clusters
    code, as_json = run("dedup", "near", "--db", str(db), "--json")
    assert len(json.loads(as_json)) == 4


def test_workers_db_and_export_commands(site: FixtureSite, tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    crawl(site, db, seeds=[site.url("/tracked")], max_depth=1, canonical={"rules": 1, "remove_params": []})
    code, listing = run("workers", "list", "--db", str(db))
    assert code == 0 and "local-shop" in listing and "finished" in listing
    code, rows = run("workers", "list", "--db", str(db), "--json")
    assert json.loads(rows)[0]["alive"] is False
    assert "0 dead worker(s)" in run("workers", "reap", "--db", str(db))[1]
    config = tmp_path / "skein.json"
    config.write_text(json.dumps({"version": 2, "database": "shop.db", "spiders": [
        {"name": "shop", "seeds": [site.url("/tracked")]}]}))
    code, preview = run("db", "rekey", "--config", str(config), "--dry-run")
    assert code == 0 and "would rename" in preview
    with Store(db) as store:
        assert any("utm_" in r[0] for r in store.db.execute("SELECT url FROM frontier")), "dry run changed nothing"
    code, done = run("db", "rekey", "--config", str(config))
    assert code == 0 and "renamed" in done
    code, merges = run("db", "merges", "--db", str(db))
    assert code == 0 and "merged " in merges and "keys from canonical rules 2" in merges
    assert run("db", "rekey", "--db", str(db))[0] == 1, "rekey needs the configuration"
    code, status = run("status", "--db", str(db))
    assert "workers    finished=1" in status and "near-dups" in status
    code, frontier = run("frontier", "list", "--db", str(db), "--limit", "3")
    assert code == 0 and frontier.count("\n") == 3


def test_export_items_as_of_a_moment(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock(1_700_000_000.0)
    site = make_site(wall_clock=clock.time)
    db = tmp_path / "shop.db"
    crawl(site, db, clock, seeds=[site.url("/product/1")], max_depth=0, extract={"items": PRODUCT_ITEMS})
    clock.advance(86400)
    site.revise(1)
    crawl(site, db, clock, seeds=[site.url("/product/1")], max_depth=0, extract={"items": PRODUCT_ITEMS},
          incremental={"mode": "full"})
    code, then = run("export", "--db", str(db), "--as-of", "2023-11-14T23:00:00Z")
    (old,) = [json.loads(line) for line in then.splitlines()]
    assert code == 0 and old["price"] == 22.49 and old["_current"] is False and old["_valid_to"]
    code, now = run("export", "--db", str(db), "--what", "versions", "-f", "csv")
    assert now.splitlines()[0].startswith("_type,_key,_version,_url,_observed_at,_valid_to,_current")
    assert run("export", "--db", str(db), "--what", "events", "--as-of", "2023-11-14T23:00:00Z")[0] == 1


def test_run_stop_query_and_relink_commands(archived: tuple[FixtureSite, Path], tmp_path: Path) -> None:
    site, db = archived
    with Store(db) as store:
        with store.transaction():
            run_id = store.runs.begin("shop", 1.0, "full")
    code, message = run("runs", "--db", str(db), "--stop", str(run_id))
    assert code == 0 and "stopped" in message
    assert run("runs", "--db", str(db), "--stop", str(run_id))[0] == 1, "only a running run can be stopped"
    code, details = run("runs", "--db", str(db), "--show", "2")
    assert "worker local-shop (local, finished" in details
    code, captures = run("warc", "query", site.url("/"), "--match", "host", "--db", str(db))
    assert code == 0 and len(captures.splitlines()) >= 8
    code, products = run("warc", "query", site.url("/product/"), "--match", "prefix", "--status", "200",
                         "--db", str(db), "--cdxj")
    assert code == 0 and all('"status": "200"' in line for line in products.splitlines())
    assert run("warc", "query", "http://elsewhere.test/", "--db", str(db))[0] == 1
    code, relinked = run("dedup", "near", "--db", str(db), "--rebuild", "--max-distance", "0")
    assert code == 0 and "within distance 0" in relinked
