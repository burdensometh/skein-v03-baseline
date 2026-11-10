"""Version-2 configuration: grouped keys, defaults, validation and typed settings."""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pytest

from skein.checkpoint import FsyncPolicy
from skein.config import CONFIG_VERSION, ConfigError, load_config, parse_config
from skein.incremental import RunMode

MINIMAL = {"version": 2, "spiders": [{"name": "shop", "seeds": ["https://shop.example/"]}]}


def issues_of(data: object) -> list[str]:
    with pytest.raises(ConfigError) as info:
        parse_config(data)
    return [str(issue) for issue in info.value.issues]


def test_minimal_config_defaults_without_warnings() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        config = parse_config(MINIMAL)
    spider = config.spider()
    assert config.version == CONFIG_VERSION == 2 and config.deprecations == ()
    assert config.database == Path("skein.db") and config.robots_ttl == 86400.0
    assert config.checkpoint_root() == Path("skein.db.checkpoints")
    assert config.checkpoint.fsync is FsyncPolicy.BATCH and config.checkpoint.snapshot_every == 500
    assert spider.allowed_domains == ("shop.example",)
    assert spider.politeness.min_delay == 1.0 and spider.politeness.respect_crawl_delay
    assert spider.retry.max_retries == 2 and spider.retry.backoff.factor == 2.0 and spider.retry.backoff.jitter == 0.25
    assert spider.retry.respect_retry_after and spider.retry.max_retry_after == 3600.0
    assert spider.retry_seed is None
    assert spider.incremental.mode is RunMode.AUTO and spider.incremental.conditional_requests
    assert spider.incremental.revisit.initial_interval == 86400.0
    assert spider.obey_robots and spider.obey_nofollow and spider.obey_noindex
    assert len(spider.plan.links) == 1 and spider.plan.links[0].selector == "a[href], area[href]"


def test_grouped_spider_is_built() -> None:
    data = {
        "version": 2,
        "project": "demo",
        "user_agent": "demo-bot/1.0",
        "robots": {"ttl": 600},
        "checkpoint": {"directory": "state", "fsync": "always", "snapshot_every": 50, "snapshot_interval": 5,
                       "sqlite_synchronous": "full"},
        "http": {"read_timeout": 5, "headers": {"X-Token": "abc"}},
        "spiders": [
            {
                "name": "shop",
                "seeds": ["https://shop.example/"],
                "sitemaps": ["https://shop.example/sitemap.xml"],
                "max_pages": 100,
                "robots": {"obey": False, "use_sitemaps": True},
                "politeness": {"min_delay": 0.5, "max_crawl_delay": 9, "per_host_concurrency": 2,
                               "respect_crawl_delay": False},
                "retry": {"max_retries": 4, "backoff_base": 1, "backoff_factor": 3, "backoff_max": 60,
                          "jitter": 0, "respect_retry_after": False, "max_retry_after": 120, "seed": 42},
                "canonical": {"sort_query": True, "remove_params": ["utm_*"]},
                "extract": {
                    "obey_nofollow": False,
                    "deny_extensions": [".PDF", "zip"],
                    "links": [{"allow": ["/catalog"], "priority": 3}],
                    "items": [{"name": "product", "key": ["sku"],
                               "fields": {"sku": "#sku", "price": {"selector": ".price", "type": "number"}}}],
                },
                "incremental": {"mode": "incremental", "conditional_requests": False,
                                "fingerprint_ignore": ["Generated at .*"],
                                "revisit": {"initial_interval": 7200, "min_interval": 600, "max_interval": 86400,
                                            "target_probability": 0.3, "retry_interval": 900}},
            }
        ],
    }
    config = parse_config(data, base_dir=Path("/srv/skein"))
    spider = config.spider("shop")
    assert config.robots_ttl == 600 and config.checkpoint_root() == Path("/srv/skein/state")
    assert config.checkpoint.fsync is FsyncPolicy.ALWAYS and config.checkpoint.snapshot_every == 50
    assert config.checkpoint.sqlite_synchronous == "full"
    assert not spider.obey_robots and spider.use_robots_sitemaps and not spider.obey_nofollow
    assert (spider.politeness.min_delay, spider.politeness.max_crawl_delay, spider.politeness.per_host_concurrency) == (
        0.5, 9.0, 2)
    assert not spider.politeness.respect_crawl_delay
    backoff = spider.retry.backoff
    assert (spider.retry.max_retries, backoff.base, backoff.factor, backoff.max_delay, backoff.jitter) == (
        4, 1.0, 3.0, 60.0, 0.0)
    assert not spider.retry.respect_retry_after and spider.retry.max_retry_after == 120.0
    assert spider.retry_seed == 42
    assert spider.canonicalizer.key("https://shop.example/?utm_a=1&b=2&a=1") == "https://shop.example/?a=1&b=2"
    assert spider.deny_extensions == frozenset({"pdf", "zip"})
    assert spider.plan.links[0].priority == 3 and spider.plan.items[0].key_fields == ("sku",)
    incremental = spider.incremental
    assert incremental.mode is RunMode.INCREMENTAL and not incremental.conditional_requests
    assert incremental.revisit.min_interval == 600 and incremental.revisit.retry_interval == 900
    assert incremental.revisit.target_probability == 0.3
    assert incremental.fingerprint.ignore[0].pattern == "Generated at .*"


def test_flat_v1_keys_are_unknown_in_v2() -> None:
    data = json.loads(json.dumps(MINIMAL))
    data["spiders"][0]["min_delay"] = 3
    data["spiders"][0]["politeness"] = {"min_dely": 3}
    data["robots_ttl"] = 5
    assert issues_of(data) == [
        "robots_ttl: unknown key; did you mean 'robots'?",
        "spiders[0].min_delay: unknown key",
        "spiders[0].politeness.min_dely: unknown key; did you mean 'min_delay'?",
    ]


def test_grouped_problems_have_grouped_paths() -> None:
    data = {
        "version": 2,
        "checkpoint": {"fsync": "sometimes"},
        "spiders": [{
            "name": "a", "seeds": ["http://a/"],
            "retry": {"backoff_base": 10, "backoff_max": 5, "jitter": 1.5},
            "incremental": {"mode": "partial", "revisit": {"min_interval": 100, "initial_interval": 50}},
            "extract": {"items": [{"name": "p", "fields": {"a": {"selector": "div["}}}]},
        }],
    }
    problems = issues_of(data)
    joined = "\n".join(problems)
    assert "checkpoint.fsync: expected one of ['always', 'batch', 'never'], got 'sometimes'" in joined
    assert "spiders[0].retry.jitter: must be <= 1, got 1.5" in joined
    assert "spiders[0].incremental.mode: expected one of ['full', 'incremental', 'sitemap', 'auto']" in joined
    assert "spiders[0].incremental.revisit.initial_interval: must lie between min_interval and max_interval" in joined
    assert "spiders[0].extract.items[0].fields.a.selector: invalid CSS selector" in joined
    assert issues_of({"version": 2, "spiders": [{"name": "a", "seeds": ["http://a/"],
                                                 "retry": {"backoff_base": 10, "backoff_max": 5}}]}) == [
        "spiders[0].retry.backoff_max: must be >= backoff_base"]


def test_load_from_file_resolves_paths(tmp_path: Path) -> None:
    path = tmp_path / "conf" / "skein.json"
    path.parent.mkdir()
    data = dict(MINIMAL, database="data/out.db")
    path.write_text(json.dumps(data), encoding="utf-8")
    config = load_config(path)
    assert config.database == path.parent.resolve() / "data" / "out.db"
    assert config.checkpoint_root() == path.parent.resolve() / "data" / "out.db.checkpoints"
    assert config.source == str(path)


def test_environment_expansion_in_groups() -> None:
    data = {"version": 2, "checkpoint": {"directory": "${STATE:-/var/lib/skein}"},
            "spiders": [{"name": "a", "seeds": ["https://${HOST}/"], "politeness": {"min_delay": 1}}]}
    config = parse_config(data, env={"HOST": "shop.example"})
    assert config.checkpoint_root() == Path("/var/lib/skein")
    assert config.spider().seeds == ("https://shop.example/",)
