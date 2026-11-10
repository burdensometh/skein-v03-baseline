"""The ``skein`` command line, driven in-process against the fixture site."""

from __future__ import annotations

import csv
import io
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from skein.cli import main
from tests.fixtures_site import FixtureSite

ROOT = Path(__file__).resolve().parent.parent


def write_config(tmp_path: Path, site: FixtureSite, **spider: Any) -> Path:
    spec: dict[str, Any] = {
        "name": "shop",
        "seeds": [site.url("/catalog?page=1")],
        "concurrency": 2,
        "politeness": {"min_delay": 0, "max_crawl_delay": 0},
        "canonical": {"remove_params": ["utm_*"]},
        "extract": {
            "links": [{"allow": [r"/catalog\?page=\d+$", r"/product/\d+$"]}],
            "items": [
                {
                    "name": "product",
                    "url_pattern": r"/product/\d+$",
                    "key": ["sku"],
                    "fields": {
                        "sku": "#sku",
                        "title": "h1",
                        "price": {"selector": ".price span", "type": "number"},
                        "tags": {"selector": "li.tag", "multiple": True},
                    },
                }
            ],
        },
    }
    spec.update(spider)
    path = tmp_path / "skein.json"
    path.write_text(json.dumps({"version": 2, "database": "data/skein.db", "spiders": [spec]}), encoding="utf-8")
    return path


def run_cli(*argv: str) -> tuple[int, str]:
    out = io.StringIO()
    code = main(list(argv), out=out)
    return code, out.getvalue()


def test_crawl_then_export_jsonl_and_csv(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    code, output = run_cli("-q", "crawl", str(config), "--stats", "json")
    assert code == 0
    summary = json.loads(output)
    assert summary["status"] == "finished"
    assert summary["stats"]["items"]["new"] == 12
    assert (tmp_path / "data" / "skein.db").exists()

    code, output = run_cli("-q", "export", "--config", str(config))
    assert code == 0
    records = [json.loads(line) for line in output.splitlines()]
    assert len(records) == 12
    assert records[0]["_key"] == "SKU-0001" and records[0]["price"] == 22.49

    target = tmp_path / "out.csv"
    code, _ = run_cli("-q", "export", "--db", str(tmp_path / "data" / "skein.db"), "-f", "csv", "-o", str(target))
    assert code == 0
    rows = list(csv.DictReader(target.open(encoding="utf-8")))
    assert len(rows) == 12
    assert rows[1]["tags"] == "blue | large | small"

    code, output = run_cli("-q", "export", "--config", str(config), "--what", "events", "-f", "csv")
    assert code == 0
    fetch_rows = list(csv.DictReader(io.StringIO(output)))
    assert len(fetch_rows) == summary["stats"]["pages_stored"]
    assert {r["status"] for r in fetch_rows} == {"200"}

    code, output = run_cli("-q", "export", "--config", str(config), "--what", "documents")
    documents = [json.loads(line) for line in output.splitlines()]
    assert len(documents) == len(fetch_rows) and all(d["fingerprint"].startswith("v1:") for d in documents)

    code, output = run_cli("-q", "export", "--config", str(config), "--what", "versions", "-f", "csv")
    assert len(list(csv.DictReader(io.StringIO(output)))) == 12

    code, output = run_cli("runs", "--config", str(config))
    assert code == 0
    assert "finished" in output.splitlines()[1] and "full" in output.splitlines()[1]


def test_crawl_text_summary_and_overrides(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    code, output = run_cli("-q", "crawl", str(config), "--max-pages", "3", "--db", str(tmp_path / "other.db"))
    assert code == 0
    assert output.startswith("spider shop: run 1 budget_exhausted")
    assert "requests" in output and "items" in output
    assert (tmp_path / "other.db").exists() and not (tmp_path / "data").exists()


def test_export_since_and_type_filters(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    run_cli("-q", "crawl", str(config))
    code, output = run_cli("-q", "export", "--config", str(config), "--since", "2999-01-01T00:00:00Z")
    assert code == 0 and output == ""
    code, output = run_cli("-q", "export", "--config", str(config), "-t", "nothing")
    assert code == 0 and output == ""
    code, output = run_cli("-q", "export", "--config", str(config), "--changed-since", "2000-01-01")
    assert code == 0 and len(output.splitlines()) == 12


def test_validate_command(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    code, output = run_cli("validate", str(config))
    assert code == 0
    assert "spider shop: 1 seed(s), 0 sitemap(s), 1 item rule(s), 1 link rule(s)" in output
    assert "mode=auto" in output and "deprecated" not in output


def test_invalid_config_exit_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"version": 2, "spiders": [{"name": "x", "seeds": ["nope"]}]}), encoding="utf-8")
    code, _ = run_cli("crawl", str(bad))
    assert code == 2
    assert "spiders[0].seeds[0]" in capsys.readouterr().err


def test_export_errors(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    code, _ = run_cli("export", "--db", str(tmp_path / "missing.db"))
    assert code == 1
    assert "does not exist" in capsys.readouterr().err
    code, _ = run_cli("export", "--db", str(tmp_path / "x.db"), "--since", "yesterday")
    assert code == 1


def test_unknown_spider(site: FixtureSite, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = write_config(tmp_path, site)
    code, _ = run_cli("crawl", str(config), "--spider", "ghost")
    assert code == 1
    assert "no spider named 'ghost'" in capsys.readouterr().err


def test_extract_from_live_page_and_file(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    code, output = run_cli("extract", str(config), site.url("/product/5"))
    assert code == 0
    report = json.loads(output)
    assert report["status"] == 200
    (extracted,) = report["items"]
    assert extracted["key"] == "SKU-0005" and len(extracted["hash"]) == 64
    assert extracted["data"] == {"sku": "SKU-0005", "title": "Widget 5", "price": 72.49, "tags": ["sale", "red", "green"]}
    assert report["fingerprint"].startswith("v1:")
    assert site.url("/product/6") in report["links"]

    page = tmp_path / "page.html"
    page.write_bytes('<meta charset="gbk"><h1>商品</h1><span id="sku">S-1</span>'.encode("gbk"))
    code, output = run_cli("extract", str(config), site.url("/product/99"), "--file", str(page), "--no-links")
    report = json.loads(output)
    assert report["charset"] == "gb18030"
    assert report["items"][0]["data"]["title"] == "商品"
    assert "links" not in report


def test_module_entry_point(tmp_path: Path) -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "skein", "--version"], cwd=ROOT, capture_output=True, text=True, timeout=30
    )
    assert completed.returncode == 0
    assert completed.stdout.strip().startswith("skein 0.3")


def test_robots_command(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(crawl_delay=4)
    code, output = run_cli("robots", site.url("/"), site.url("/private/x"), site.url("/private/open"))
    assert code == 0
    lines = output.splitlines()
    assert lines[0] == f"http://127.0.0.1:{site.port}: robots.txt fetched (HTTP 200), group *, crawl-delay 4s"
    assert lines[1] == f"  sitemap {site.url('/sitemap_index.xml.gz')}"
    assert lines[2].startswith("  ALLOW") and lines[3].startswith("  DENY ") and "Disallow: /private/" in lines[3]
    assert lines[4].startswith("  ALLOW") and "Allow: /private/open" in lines[4]
    code, output = run_cli("robots", "--user-agent", "otherbot/1.0", site.url("/"))
    assert "group otherbot" in output and "DENY" in output
    assert site.paths().count("/robots.txt") == 2


def test_robots_command_rejects_relative_url(capsys: pytest.CaptureFixture[str]) -> None:
    code, _ = run_cli("robots", "/just/a/path")
    assert code == 1
    assert "not an absolute http(s) URL" in capsys.readouterr().err


def test_example_config_is_valid() -> None:
    code, output = run_cli("validate", str(ROOT / "examples" / "fixture-shop.json"))
    assert code == 0
    assert "spider shop: 1 seed(s), 1 sitemap(s), 1 item rule(s), 2 link rule(s)" in output
