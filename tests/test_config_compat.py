"""Version-1 configuration files: loaded through the v2 schema with deprecation warnings.

Validation errors must still name the keys the user wrote (``spiders[0].min_delay``),
not their v2 locations (``spiders[0].politeness.min_delay``).
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pytest

from skein.config import ConfigDeprecationWarning, ConfigError, load_config, parse_config
from skein.config.compat import PathMap, upgrade_v1

pytestmark = pytest.mark.filterwarnings("ignore::skein.config.ConfigDeprecationWarning")

MINIMAL = {"version": 1, "spiders": [{"name": "shop", "seeds": ["https://shop.example/"]}]}


def issues_of(data: object) -> list[str]:
    with pytest.raises(ConfigError) as info:
        parse_config(data)
    return [str(issue) for issue in info.value.issues]


def test_minimal_v1_config_defaults() -> None:
    config = parse_config(MINIMAL)
    spider = config.spider()
    assert config.version == 1
    assert spider.retry.max_retries == 2 and spider.retry.backoff.base == 5.0
    assert spider.retry.backoff.factor == 1.0 and spider.retry.backoff.jitter == 0.0, "v1 retried at a fixed delay"
    assert config.database == Path("skein.db")
    assert spider.allowed_domains == ("shop.example",)
    assert spider.politeness.min_delay == 1.0
    assert spider.concurrency == 4
    assert spider.obey_robots and spider.obey_nofollow
    assert len(spider.plan.links) == 1 and spider.plan.links[0].selector == "a[href], area[href]"
    assert "pdf" in spider.deny_extensions and spider.obey_noindex
    assert config.http.client_config().max_redirects == 10


def test_full_v1_spider_is_built() -> None:
    data = {
        "version": 1,
        "project": "demo",
        "user_agent": "demo-bot/1.0",
        "http": {"read_timeout": 5, "headers": {"X-Token": "abc"}, "max_connections_per_host": 2},
        "spiders": [
            {
                "name": "shop",
                "seeds": ["https://shop.example/"],
                "sitemaps": ["https://shop.example/sitemap.xml"],
                "allowed_domains": ["shop.example", "cdn.example"],
                "max_depth": 3,
                "max_pages": 100,
                "min_delay": 0.5,
                "sort_query": True,
                "remove_params": ["utm_*"],
                "deny_extensions": [".PDF", "zip"],
                "links": [{"allow": ["/catalog"], "deny": ["\\.pdf$"], "max_depth": 2, "priority": 3, "from": "/"}],
                "items": [
                    {
                        "name": "product",
                        "url_pattern": "/product/\\d+",
                        "key": ["sku"],
                        "fields": {
                            "sku": "#sku",
                            "price": {"selector": ".price", "type": "number"},
                            "image": {"selector": "img", "attr": "src", "type": "url"},
                            "tags": {"selector": "li", "multiple": True, "transforms": ["strip", "lower"]},
                            "date": {"selector": "time", "type": "date", "formats": ["%d.%m.%Y"]},
                        },
                    }
                ],
            }
        ],
    }
    config = parse_config(data)
    spider = config.spider("shop")
    assert config.http.user_agent == "demo-bot/1.0"
    assert config.http.client_config().default_headers == {"X-Token": "abc"}
    assert spider.canonicalizer.key("https://shop.example/?utm_a=1&b=2&a=1") == "https://shop.example/?a=1&b=2"
    (item,) = spider.plan.items
    fields = {f.name: f for f in item.fields}
    assert fields["sku"].selector == "#sku" and fields["sku"].source == "text"
    assert fields["image"].source == "attr" and fields["image"].type == "url"
    assert fields["tags"].multiple and fields["tags"].transforms == ("strip", "lower")
    assert fields["date"].formats == ("%d.%m.%Y",)
    assert spider.deny_extensions == frozenset({"pdf", "zip"})
    assert spider.link_extractor().deny_extensions == frozenset({"pdf", "zip"})
    link = spider.plan.links[0]
    assert link.accepts("https://shop.example/catalog/1") and not link.accepts("https://shop.example/catalog/a.pdf")
    assert link.max_depth == 2 and link.priority == 3 and link.source_pattern is not None
    assert spider.politeness.min_delay == 0.5 and spider.max_pages == 100


def test_unknown_keys_get_suggestions() -> None:
    data = json.loads(json.dumps(MINIMAL))
    data["spiders"][0]["max_dept"] = 3
    data["htttp"] = {}
    assert issues_of(data) == [
        "htttp: unknown key; did you mean 'http'?",
        "spiders[0].max_dept: unknown key; did you mean 'max_depth'?",
    ]


def test_all_problems_reported_with_paths() -> None:
    data = {
        "version": 1,
        "spiders": [
            {
                "name": "9bad",
                "seeds": ["ftp://x/", "not a url"],
                "concurrency": 0,
                "min_delay": "1",
                "obey_robots": "yes",
                "items": [{"name": "p", "fields": {"a": {"selector": "div[", "type": "integr"}, "b": {"regex": "("}}}],
                "links": [{"allow": ["[unclosed"]}],
            }
        ],
    }
    problems = issues_of(data)
    joined = "\n".join(problems)
    assert "spiders[0].name: '9bad' does not match" in joined
    assert "spiders[0].seeds[0]: expected an absolute http(s) URL" in joined
    assert "spiders[0].concurrency: must be >= 1, got 0" in joined
    assert "spiders[0].min_delay: expected a number, got string" in joined
    assert "spiders[0].obey_robots: expected true or false, got string" in joined
    assert "spiders[0].items[0].fields.a.selector: invalid CSS selector" in joined
    assert "spiders[0].items[0].fields.a.type: expected one of" in joined and "did you mean 'int'" in joined
    assert "spiders[0].items[0].fields.b.regex: invalid regular expression" in joined
    assert "spiders[0].links[0].allow[0]: invalid regular expression" in joined


def test_cross_field_checks() -> None:
    data = {
        "version": 1,
        "spiders": [
            {
                "name": "a",
                "items": [
                    {"name": "p", "key": ["sku"],
                     "fields": {"t": {"source": "attr"}, "d": {"formats": ["%Y"]}, "j": {"join": ","}}}
                ],
            }
        ],
    }
    assert issues_of(data) == [
        "spiders[0].items[0].fields.t.attr: is required when source is 'attr'",
        "spiders[0].items[0].fields.d.formats: only applies to date and datetime fields",
        "spiders[0].items[0].fields.j.join: only applies to fields with multiple: true",
        "spiders[0].items[0].key: key field 'sku' is not defined in fields",
        "spiders[0].seeds: a spider needs at least one seed or sitemap URL",
    ]


def test_version_handling() -> None:
    assert issues_of({"spiders": []}) == ["version: is required (this skein reads version 2)"]
    assert issues_of({"version": 3, "spiders": []}) == [
        "version: unsupported configuration version 3; expected 1 or 2"]
    assert issues_of({"version": True, "spiders": []}) == [
        "version: unsupported configuration version True; expected 1 or 2"]
    assert issues_of([1, 2]) == ["<root>: expected an object, got array"]


def test_duplicate_spiders_and_selection() -> None:
    data = {"version": 1, "spiders": [MINIMAL["spiders"][0], MINIMAL["spiders"][0]]}
    assert issues_of(data) == ["spiders: duplicate spider name(s): shop"]
    two = {"version": 1, "spiders": [MINIMAL["spiders"][0], {"name": "b", "seeds": ["http://b.example/"]}]}
    config = parse_config(two)
    assert config.spider("b").name == "b"
    with pytest.raises(KeyError, match="several spiders"):
        config.spider()
    with pytest.raises(KeyError, match="no spider named 'c'"):
        config.spider("c")


def test_load_from_file_resolves_database(tmp_path: Path) -> None:
    path = tmp_path / "conf" / "skein.json"
    path.parent.mkdir()
    data = dict(MINIMAL, database="data/out.db")
    path.write_text(json.dumps(data), encoding="utf-8")
    config = load_config(path)
    assert config.database == path.parent.resolve() / "data" / "out.db"
    assert config.source == str(path)


def test_load_errors(tmp_path: Path) -> None:
    broken = tmp_path / "broken.json"
    broken.write_text('{"version": 1,\n "spiders": [}', encoding="utf-8")
    with pytest.raises(ConfigError, match="invalid JSON at line 2"):
        load_config(broken)
    with pytest.raises(ConfigError, match="cannot read file"):
        load_config(tmp_path / "missing.json")


def test_error_message_format() -> None:
    with pytest.raises(ConfigError) as info:
        parse_config({"version": 1, "spiders": [{"name": "a", "seeds": [], "concurrency": -1}]}, source="x.json")
    text = str(info.value)
    assert text.startswith("invalid configuration in x.json (1 problem):")
    assert "  - spiders[0].concurrency: must be >= 1, got -1" in text


def test_environment_expansion(tmp_path: Path) -> None:
    data = {
        "version": 1,
        "database": "${SKEIN_DB:-default.db}",
        "http": {"headers": {"Authorization": "Bearer ${API_TOKEN}", "X-Price": "$$5"}},
        "spiders": [{"name": "a", "seeds": ["https://${SHOP_HOST}/"]}],
    }
    config = parse_config(data, env={"API_TOKEN": "s3cret", "SHOP_HOST": "shop.example"})
    assert config.http.headers == {"Authorization": "Bearer s3cret", "X-Price": "$5"}
    assert config.database == Path("default.db")
    assert config.spider().seeds == ("https://shop.example/",)
    assert config.spider().allowed_domains == ("shop.example",)
    path = tmp_path / "env.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ConfigError) as info:
        load_config(path, env={"SKEIN_DB": "x.db"})
    assert [str(i) for i in info.value.issues] == [
        "http.headers.Authorization: environment variable API_TOKEN is not set",
        "spiders[0].seeds[0]: environment variable SHOP_HOST is not set",
    ]


def test_v1_load_warns_once_per_moved_key(tmp_path: Path) -> None:
    data = {"version": 1, "robots_ttl": 60, "spiders": [
        {"name": "a", "seeds": ["http://a.example/"], "min_delay": 2, "retry_delay": 9, "items": []}]}
    path = tmp_path / "old.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        config = load_config(path)
    messages = [str(w.message) for w in caught if issubclass(w.category, ConfigDeprecationWarning)]
    assert len(messages) == 5
    assert all(m.startswith(f"{path}: ") for m in messages)
    assert any("configuration version 1 is deprecated" in m for m in messages)
    assert any("spiders[0].retry_delay is deprecated; use spiders[0].retry.backoff_base" in m for m in messages)
    assert any("robots_ttl is deprecated; use robots.ttl" in m for m in messages)
    assert config.deprecations and len(config.deprecations) == 5
    assert config.robots_ttl == 60 and config.spider().retry.backoff.base == 9.0
    assert config.spider().politeness.min_delay == 2.0


def test_upgrade_v1_does_not_modify_input_and_keeps_unknown_keys() -> None:
    data = {"version": 1, "spiders": [{"name": "a", "seeds": ["http://a/"], "min_delay": 1, "mystery": 1}]}
    upgraded, paths, notices = upgrade_v1(data)
    assert data["spiders"][0]["min_delay"] == 1, "input untouched"
    spider = upgraded["spiders"][0]
    assert upgraded["version"] == 2 and spider["politeness"] == {"min_delay": 1} and spider["mystery"] == 1
    assert paths.rewrite("spiders[0].politeness.min_delay") == "spiders[0].min_delay"
    assert paths.rewrite("spiders[0].mystery") == "spiders[0].mystery"
    assert len(notices) == 2


def test_v1_large_retry_delay_raises_the_backoff_cap() -> None:
    data = {"version": 1, "spiders": [{"name": "a", "seeds": ["http://a/"], "retry_delay": 900}]}
    assert parse_config(data).spider().retry.backoff.max_delay == 900.0


def test_v1_group_clash_is_reported_at_the_group() -> None:
    data = {"version": 1, "spiders": [{"name": "a", "seeds": ["http://a/"], "politeness": 3, "min_delay": 1}]}
    problems = issues_of(data)
    assert "spiders[0].politeness: expected an object, got integer" in problems
    assert "spiders[0].min_delay: unknown key" in problems


def test_path_map_prefers_the_longest_match() -> None:
    paths = PathMap()
    paths.add("spiders[0].extract.items", "spiders[0].items")
    paths.add("spiders[0].extract", "spiders[0].extract_group")
    assert paths.rewrite("spiders[0].extract.items[1].fields.a") == "spiders[0].items[1].fields.a"
    assert paths.rewrite("spiders[0].extract.links") == "spiders[0].extract_group.links"
    assert paths.rewrite("spiders[0].extraction") == "spiders[0].extraction"
