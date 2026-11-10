"""Operational commands: status, frontier, checkpoint, history, change export with cursors, config upgrade."""

from __future__ import annotations

import io
import json
import warnings
from pathlib import Path
from typing import Any

import pytest

from skein.cli import main
from skein.config import load_config
from tests.fixtures_site import FixtureSite

ROOT = Path(__file__).resolve().parent.parent


def run_cli(*argv: str) -> tuple[int, str]:
    out = io.StringIO()
    code = main(list(argv), out=out)
    return code, out.getvalue()


def write_config(tmp_path: Path, site: FixtureSite, **spider: Any) -> Path:
    spec: dict[str, Any] = {
        "name": "shop",
        "seeds": [site.url("/catalog?page=1"), site.url("/status/404")],
        "politeness": {"min_delay": 0},
        "retry": {"max_retries": 0},
        "extract": {
            "links": [{"allow": [r"/catalog\?page=\d+$", r"/product/\d+$"]}],
            "items": [{"name": "product", "url_pattern": r"/product/\d+$", "key": ["sku"],
                       "fields": {"sku": "#sku", "price": {"selector": ".price span", "type": "number"}}}],
        },
        **spider,
    }
    path = tmp_path / "skein.json"
    path.write_text(json.dumps({"version": 2, "database": "shop.db", "spiders": [spec]}), encoding="utf-8")
    return path


def test_status_and_frontier_commands(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    assert run_cli("-q", "crawl", str(config))[0] == 0
    code, output = run_cli("status", "--config", str(config))
    assert code == 0
    assert "spider shop" in output and "frontier   done=15 failed=1" in output
    assert "items      12 current, 12 version(s)" in output and "checkpoint none" in output
    assert f"robots.txt {site.base_url}: fetched (HTTP 200)" in output and "fetches    200=15 404=1" in output
    code, output = run_cli("status", "--config", str(config), "--json")
    (report,) = json.loads(output)["spiders"]
    assert report["documents"]["total"] == 16 and len(report["watermarks"]) == 2
    assert all(mark["last_completed"] for mark in report["watermarks"])

    code, output = run_cli("frontier", "list", "--config", str(config), "--state", "failed")
    assert code == 0 and output.startswith("failed") and site.url("/status/404") in output
    code, output = run_cli("frontier", "list", "--config", str(config), "--json", "--limit", "3")
    assert len(json.loads(output)) == 3
    code, output = run_cli("frontier", "release", "--config", str(config))
    assert output == "1 request(s) of spider shop queued again\n"
    code, output = run_cli("frontier", "list", "--config", str(config), "--state", "pending")
    assert output.count("\n") == 1


def test_checkpoint_commands_on_a_stopped_run(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    code, output = run_cli("-q", "crawl", str(config), "--max-runtime", "0", "--stats", "json")
    summary = json.loads(output)
    assert code == 0 and summary["status"] == "stopped" and summary["stats"]["requests"] == 0
    code, output = run_cli("checkpoint", "verify", "--config", str(config))
    assert code == 0 and "journal-" in output and "(ok)" in output and "0 unconfirmed record(s)" in output
    code, output = run_cli("status", "--config", str(config))
    assert "checkpoint run 1 at seq" in output
    code, output = run_cli("checkpoint", "dump", "--config", str(config))
    assert code == 0
    code, output = run_cli("-q", "crawl", str(config), "--stats", "json")
    resumed = json.loads(output)
    assert resumed["resumed"] is True and resumed["run_id"] == 1 and resumed["status"] == "finished"
    code, output = run_cli("checkpoint", "verify", "--config", str(config))
    assert output == "spider shop: no checkpoint\n"


def test_checkpoint_discard_abandons_the_run(site: FixtureSite, tmp_path: Path) -> None:
    config = write_config(tmp_path, site)
    run_cli("-q", "crawl", str(config), "--max-runtime", "0")
    code, output = run_cli("checkpoint", "discard", "--config", str(config))
    assert code == 0 and "discarded" in output
    code, output = run_cli("runs", "--config", str(config), "--json")
    assert [r["status"] for r in json.loads(output)] == ["abandoned"]
    code, output = run_cli("-q", "crawl", str(config), "--stats", "json")
    assert json.loads(output)["resumed"] is False and json.loads(output)["run_id"] == 2


def test_history_and_change_export_with_cursor(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(validators="etag")
    config = write_config(tmp_path, site, incremental={"mode": "full"})
    run_cli("-q", "crawl", str(config))
    code, output = run_cli("-q", "export", "--config", str(config), "--what", "changes", "--cursor", "dw")
    first = [json.loads(line) for line in output.splitlines()]
    assert code == 0 and len(first) == 12 and {r["_change"] for r in first} == {"created"}
    code, output = run_cli("-q", "export", "--config", str(config), "--what", "changes", "--cursor", "dw")
    assert output == "", "the cursor moved past everything already delivered"
    site.revise(4)
    run_cli("-q", "crawl", str(config))
    code, output = run_cli("-q", "export", "--config", str(config), "--what", "changes", "--cursor", "dw",
                           "-f", "csv")
    lines = output.splitlines()
    assert lines[0].startswith("_type,_key,_version,_change") and len(lines) == 2
    assert "SKU-0004,2,updated" in lines[1] and "price" in lines[1]
    code, output = run_cli("history", "--config", str(config), "product", "SKU-0004")
    assert code == 0
    assert output.splitlines()[0].startswith("v1 created") and output.splitlines()[-1].strip() == "price: 59.99 -> 60.99"
    code, output = run_cli("history", "--config", str(config), "product", "SKU-0004", "--json")
    assert [entry["kind"] for entry in json.loads(output)] == ["created", "updated"]
    code, _ = run_cli("history", "--config", str(config), "product", "missing")
    assert code == 1
    code, _ = run_cli("export", "--config", str(config), "--what", "items", "--cursor", "dw")
    assert code == 1


def test_upgrade_config_writes_an_equivalent_v2_file(tmp_path: Path) -> None:
    source = ROOT / "tests" / "data" / "v01_config.json"
    target = tmp_path / "v2.json"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        code, output = run_cli("upgrade-config", str(source), "-o", str(target))
        old = load_config(source)
    assert code == 0 and "change(s)" in output
    data = json.loads(target.read_text(encoding="utf-8"))
    assert data["version"] == 2 and "min_delay" not in data["spiders"][0]
    assert data["spiders"][0]["politeness"]["min_delay"] == 0.2
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        new = load_config(target)
    old_spider, new_spider = old.spider(), new.spider()
    for name in ("politeness", "retry", "canonicalizer", "max_depth", "seeds", "sitemaps", "deny_extensions"):
        assert getattr(old_spider, name) == getattr(new_spider, name), name
    assert [i.name for i in old_spider.plan.items] == [i.name for i in new_spider.plan.items]
    code, output = run_cli("upgrade-config", str(target))
    assert json.loads(output) == data, "version 2 passes through unchanged"


def test_validate_reports_deprecations(tmp_path: Path) -> None:
    source = ROOT / "tests" / "data" / "v01_config.json"
    with pytest.warns(DeprecationWarning):
        code, output = run_cli("validate", str(source))
    assert code == 0 and "(version 1," in output
    assert "deprecated: spiders[0].min_delay is deprecated; use spiders[0].politeness.min_delay" in output


def test_plan_preview_runs_show_and_integrity_check(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    site = make_site(validators="etag")
    config = write_config(tmp_path, site)
    code, output = run_cli("plan", str(config))
    assert code == 0 and "no database yet" in output
    run_cli("-q", "crawl", str(config))
    code, output = run_cli("plan", str(config))
    assert code == 0 and "next run is incremental" in output and "re-queued      0 known URL(s)" in output
    code, output = run_cli("plan", str(config), "--mode", "full", "--json")
    preview = json.loads(output)
    assert preview["mode"] == "full" and preview["requeued"] == 16 and preview["resumable_runs"] == []
    assert preview["next_due"] is not None and len(preview["roots"]) == 2

    code, output = run_cli("runs", "--config", str(config), "--show", "1")
    assert code == 0 and output.startswith("run 1 of spider shop: finished, full mode, 0 resume(s)")
    assert "status  200 -" in output and "status  404 permanent" in output
    assert f"host {site.base_url}" in output and "requests=" in output
    assert "error     1x HTTP 404" in output
    assert run_cli("runs", "--config", str(config), "--show", "9")[0] == 1

    code, output = run_cli("db", "status", "--config", str(config), "--check")
    assert code == 0 and "integrity: ok" in output


def test_per_host_statistics_in_the_run_summary(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    first, second = make_site(), make_site()
    config = write_config(tmp_path, first, seeds=[first.url("/about"), second.url("/status/503")],
                          allowed_domains=["127.0.0.1"], retry={"max_retries": 1, "backoff_base": 0, "jitter": 0})
    code, output = run_cli("-q", "crawl", str(config), "--stats", "json")
    per_host = json.loads(output)["stats"]["per_host"]
    assert per_host[first.base_url] == {"pages_stored": 1, "requests": 2, "responses": 1,
                                        "bytes_received": per_host[first.base_url]["bytes_received"]}
    busy = per_host[second.base_url]
    assert busy["requests"] == 3 and busy["retries"] == 1 and busy["pages_stored"] == 2
