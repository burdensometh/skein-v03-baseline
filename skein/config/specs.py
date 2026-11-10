"""The version-2 configuration schema.

Settings that v1 kept flat on each spider are grouped by concern:
``robots``, ``politeness``, ``retry``, ``canonical``, ``extract`` and
``incremental``; project-wide ``robots`` and ``checkpoint`` groups sit
next to ``http``. Since 0.3 the project also has ``pipeline``,
``workers``, ``dedup``, ``warc`` and ``metrics`` groups, and ``canonical``
selects the canonicalisation rules version (default 2).
"""

from __future__ import annotations

from typing import Any

from skein.checkpoint.journal import FsyncPolicy
from skein.config.runtime import NEAR_DUPLICATE_ACTIONS
from skein.config.schema import Anything, Bool, Field, HttpUrl, ListOf, MapOf, Number, Obj, RegexStr, SelectorStr, Str
from skein.extract.rules import FIELD_SOURCES, FIELD_TYPES, TRANSFORMS
from skein.incremental.modes import RunMode
from skein.net.client import DEFAULT_USER_AGENT

CONFIG_VERSION = 2
NAME_PATTERN = r"[A-Za-z][A-Za-z0-9_\-]*"


def _field_checks(field: dict[str, Any]) -> list[tuple[str, str]]:
    problems = []
    if field["source"] == "attr" and not field["attr"]:
        problems.append(("attr", "is required when source is 'attr'"))
    if field["join"] is not None and not field["multiple"]:
        problems.append(("join", "only applies to fields with multiple: true"))
    if field["formats"] and field["type"] not in ("date", "datetime"):
        problems.append(("formats", "only applies to date and datetime fields"))
    return problems


def _item_checks(item: dict[str, Any]) -> list[tuple[str, str]]:
    return [
        ("key", f"key field {name!r} is not defined in fields")
        for name in item["key"]
        if name not in item["fields"]
    ]


def _spider_checks(spider: dict[str, Any]) -> list[tuple[str, str]]:
    if not spider["seeds"] and not spider["sitemaps"]:
        return [("seeds", "a spider needs at least one seed or sitemap URL")]
    return []


def _retry_checks(retry: dict[str, Any]) -> list[tuple[str, str]]:
    if retry["backoff_max"] < retry["backoff_base"]:
        return [("backoff_max", "must be >= backoff_base")]
    return []


def _revisit_checks(revisit: dict[str, Any]) -> list[tuple[str, str]]:
    if not revisit["min_interval"] <= revisit["initial_interval"] <= revisit["max_interval"]:
        return [("initial_interval", "must lie between min_interval and max_interval")]
    return []


FIELD_SPEC = Obj(
    {
        "selector": Field(SelectorStr(non_empty=True), None),
        "source": Field(Str(choices=FIELD_SOURCES), None),
        "attr": Field(Str(non_empty=True), None),
        "regex": Field(RegexStr(), None),
        "transforms": Field(ListOf(Str(choices=TRANSFORMS)), ["strip"]),
        "type": Field(Str(choices=FIELD_TYPES), "string"),
        "formats": Field(ListOf(Str(non_empty=True)), []),
        "multiple": Field(Bool(), False),
        "join": Field(Str(), None),
        "required": Field(Bool(), False),
        "default": Field(Anything(), None),
    },
    shorthand="selector",
    checks=(_field_checks,),
)

ITEM_SPEC = Obj(
    {
        "name": Field(Str(pattern=NAME_PATTERN)),
        "url_pattern": Field(RegexStr(), None),
        "scope": Field(SelectorStr(non_empty=True), None),
        "key": Field(ListOf(Str(non_empty=True), unique=True), []),
        "fields": Field(MapOf(FIELD_SPEC, key_pattern=r"[A-Za-z_][A-Za-z0-9_]*", min_items=1)),
    },
    checks=(_item_checks,),
)

LINK_SPEC = Obj(
    {
        "allow": Field(ListOf(RegexStr()), []),
        "deny": Field(ListOf(RegexStr()), []),
        "selector": Field(SelectorStr(non_empty=True), "a[href], area[href]"),
        "attr": Field(Str(non_empty=True), "href"),
        "max_depth": Field(Number(integer=True, minimum=0, nullable=True), None),
        "priority": Field(Number(integer=True, minimum=-1000, maximum=1000), 0),
        "kind": Field(Str(choices=("page", "sitemap")), "page"),
        "from": Field(RegexStr(), None),
    }
)

SPIDER_ROBOTS_SPEC = Obj({"obey": Field(Bool(), True), "use_sitemaps": Field(Bool(), False)})

POLITENESS_SPEC = Obj(
    {
        "min_delay": Field(Number(minimum=0), 1.0),
        "max_crawl_delay": Field(Number(minimum=0), 60.0),
        "per_host_concurrency": Field(Number(integer=True, minimum=1, maximum=64), 1),
        "respect_crawl_delay": Field(Bool(), True),
    }
)

RETRY_SPEC = Obj(
    {
        "max_retries": Field(Number(integer=True, minimum=0, maximum=20), 2),
        "backoff_base": Field(Number(minimum=0), 5.0),
        "backoff_factor": Field(Number(minimum=1), 2.0),
        "backoff_max": Field(Number(minimum=0), 300.0),
        "jitter": Field(Number(minimum=0, maximum=1), 0.25),
        "respect_retry_after": Field(Bool(), True),
        "max_retry_after": Field(Number(minimum=0), 3600.0),
        "host_failure_limit": Field(Number(integer=True, minimum=1, nullable=True), 10),
        "quarantine": Field(Number(minimum=0), 3600.0),
        "seed": Field(Number(integer=True, nullable=True), None),
    },
    checks=(_retry_checks,),
)

def _canonical_checks(canonical: dict[str, Any]) -> list[tuple[str, str]]:
    problems = []
    if canonical["rules"] == 1:
        if canonical["query_order"] is not None:
            problems.append(("query_order", "only applies to canonical rules version 2"))
        if canonical["tracking_params"]:
            problems.append(("tracking_params", "only applies to canonical rules version 2"))
    elif canonical["sort_query"] and canonical["query_order"] == "preserve":
        problems.append(("sort_query", "contradicts query_order 'preserve'"))
    return problems


CANONICAL_SPEC = Obj(
    {
        "rules": Field(Number(integer=True, minimum=1, maximum=2), 2),
        "sort_query": Field(Bool(), False),
        "query_order": Field(Str(choices=("sort", "preserve"), nullable=True), None),
        "remove_params": Field(ListOf(Str(non_empty=True)), []),
        "keep_params": Field(ListOf(Str(non_empty=True), nullable=True), None),
        "tracking_params": Field(ListOf(Str(non_empty=True)), []),
        "drop_empty_params": Field(Bool(), False),
        "keep_fragment": Field(Bool(), False),
    },
    checks=(_canonical_checks,),
)

EXTRACT_SPEC = Obj(
    {
        "items": Field(ListOf(ITEM_SPEC), []),
        "links": Field(ListOf(LINK_SPEC), [{}]),
        "obey_nofollow": Field(Bool(), True),
        "obey_noindex": Field(Bool(), True),
        "deny_extensions": Field(ListOf(Str(pattern=r"\.?[A-Za-z0-9]+")), None),
    }
)

REVISIT_SPEC = Obj(
    {
        "initial_interval": Field(Number(minimum=1), 86400.0),
        "min_interval": Field(Number(minimum=1), 3600.0),
        "max_interval": Field(Number(minimum=1), 30 * 86400.0),
        "target_probability": Field(Number(minimum=0.01, maximum=0.99), 0.5),
        "retry_interval": Field(Number(minimum=1, nullable=True), None),
        "honour_freshness": Field(Bool(), True),
    },
    checks=(_revisit_checks,),
)

def _revisit_rule_checks(rule: dict[str, Any]) -> list[tuple[str, str]]:
    low, initial, high = rule["min_interval"], rule["initial_interval"], rule["max_interval"]
    if None not in (low, high) and low > high:
        return [("min_interval", "must not exceed max_interval")]
    if initial is not None and ((low is not None and initial < low) or (high is not None and initial > high)):
        return [("initial_interval", "must lie between min_interval and max_interval")]
    return []


REVISIT_RULE_SPEC = Obj(
    {
        "pattern": Field(RegexStr()),
        "initial_interval": Field(Number(minimum=1, nullable=True), None),
        "min_interval": Field(Number(minimum=1, nullable=True), None),
        "max_interval": Field(Number(minimum=1, nullable=True), None),
        "target_probability": Field(Number(minimum=0.01, maximum=0.99, nullable=True), None),
    },
    checks=(_revisit_rule_checks,),
)

INCREMENTAL_SPEC = Obj(
    {
        "mode": Field(Str(choices=tuple(m.value for m in RunMode)), RunMode.AUTO.value),
        "conditional_requests": Field(Bool(), True),
        "use_sitemap_lastmod": Field(Bool(), True),
        "fingerprint_ignore": Field(ListOf(RegexStr()), []),
        "revisit": Field(REVISIT_SPEC, {}),
        "revisit_rules": Field(ListOf(REVISIT_RULE_SPEC), []),
    }
)

SPIDER_SPEC = Obj(
    {
        "name": Field(Str(pattern=NAME_PATTERN)),
        "seeds": Field(ListOf(HttpUrl(), unique=True), []),
        "sitemaps": Field(ListOf(HttpUrl(), unique=True), []),
        "allowed_domains": Field(ListOf(Str(non_empty=True)), None),
        "max_depth": Field(Number(integer=True, minimum=0, nullable=True), None),
        "max_pages": Field(Number(integer=True, minimum=1, nullable=True), None),
        "concurrency": Field(Number(integer=True, minimum=1, maximum=1024), 4),
        "global_concurrency": Field(Number(integer=True, minimum=1, maximum=100_000, nullable=True), None),
        "robots": Field(SPIDER_ROBOTS_SPEC, {}),
        "politeness": Field(POLITENESS_SPEC, {}),
        "retry": Field(RETRY_SPEC, {}),
        "canonical": Field(CANONICAL_SPEC, {}),
        "extract": Field(EXTRACT_SPEC, {}),
        "incremental": Field(INCREMENTAL_SPEC, {}),
    },
    checks=(_spider_checks,),
)

HTTP_SPEC = Obj(
    {
        "connect_timeout": Field(Number(minimum=0.001), 10.0),
        "read_timeout": Field(Number(minimum=0.001), 30.0),
        "total_timeout": Field(Number(minimum=0.001), 60.0),
        "max_body_bytes": Field(Number(integer=True, minimum=1024), 10 * 1024 * 1024),
        "max_redirects": Field(Number(integer=True, minimum=0, maximum=50), 10),
        "max_connections_per_host": Field(Number(integer=True, minimum=1, maximum=256), 6),
        "verify_tls": Field(Bool(), True),
        "headers": Field(MapOf(Str(), key_pattern=r"[!#$%&'*+.^_`|~0-9A-Za-z\-]+"), {}),
    }
)

PROJECT_ROBOTS_SPEC = Obj({"ttl": Field(Number(minimum=0), 86400.0)})

CHECKPOINT_SPEC = Obj(
    {
        "directory": Field(Str(non_empty=True), None),
        "fsync": Field(Str(choices=tuple(p.value for p in FsyncPolicy)), FsyncPolicy.BATCH.value),
        "fsync_batch": Field(Number(integer=True, minimum=1), 64),
        "fsync_interval": Field(Number(minimum=0), 1.0),
        "snapshot_every": Field(Number(integer=True, minimum=1), 500),
        "snapshot_interval": Field(Number(minimum=0), 30.0),
        "sqlite_synchronous": Field(Str(choices=("normal", "full")), "normal"),
    }
)

PIPELINE_SPEC = Obj(
    {
        "fetch_queue": Field(Number(integer=True, minimum=1, maximum=10_000), 32),
        "parse_queue": Field(Number(integer=True, minimum=1, maximum=10_000), 64),
        "write_queue": Field(Number(integer=True, minimum=1, maximum=100_000), 128),
        "write_batch": Field(Number(integer=True, minimum=1, maximum=10_000), 64),
        "parse_tasks": Field(Number(integer=True, minimum=1, maximum=256), 2),
        "parse_threads": Field(Number(integer=True, minimum=0, maximum=64), 0),
    }
)


def _worker_checks(workers: dict[str, Any]) -> list[tuple[str, str]]:
    if workers["heartbeat_interval"] >= workers["lease_ttl"]:
        return [("heartbeat_interval", "must be shorter than lease_ttl")]
    return []


WORKERS_SPEC = Obj(
    {
        "lease_ttl": Field(Number(minimum=0.1), 60.0),
        "heartbeat_interval": Field(Number(minimum=0.05), 10.0),
        "max_lease_attempts": Field(Number(integer=True, minimum=1, maximum=100), 5),
        "reclaim_interval": Field(Number(minimum=0.05), 5.0),
        "poll_interval": Field(Number(minimum=0.01), 0.5),
    },
    checks=(_worker_checks,),
)

BLOOM_SPEC = Obj(
    {
        "enabled": Field(Bool(), True),
        "directory": Field(Str(non_empty=True), None),
        "capacity": Field(Number(integer=True, minimum=1000), 200_000),
        "error_rate": Field(Number(minimum=1e-12, maximum=0.5), 1e-6),
        "verify_every": Field(Number(integer=True, minimum=0), 64),
    }
)

NEAR_SPEC = Obj(
    {
        "enabled": Field(Bool(), True),
        "max_distance": Field(Number(integer=True, minimum=0, maximum=3), 3),
        "shingle_size": Field(Number(integer=True, minimum=1, maximum=10), 3),
        "min_words": Field(Number(integer=True, minimum=1), 8),
        "action": Field(Str(choices=NEAR_DUPLICATE_ACTIONS), "record"),
    }
)

DEDUP_SPEC = Obj({"bloom": Field(BLOOM_SPEC, {}), "near_duplicates": Field(NEAR_SPEC, {})})

WARC_SPEC = Obj(
    {
        "enabled": Field(Bool(), False),
        "directory": Field(Str(non_empty=True), None),
        "prefix": Field(Str(pattern=r"[A-Za-z0-9][A-Za-z0-9_.\-]*"), "skein"),
        "max_size": Field(Number(integer=True, minimum=1024), 100 * 1024 * 1024),
        "metadata": Field(Bool(), True),
        "revisits": Field(Bool(), True),
        "compresslevel": Field(Number(integer=True, minimum=1, maximum=9), 6),
    }
)

METRICS_SPEC = Obj(
    {
        "textfile": Field(Str(non_empty=True), None),
        "interval": Field(Number(minimum=0.1), 15.0),
        "port": Field(Number(integer=True, minimum=0, maximum=65535, nullable=True), None),
    }
)

PROJECT_SPEC = Obj(
    {
        "version": Field(Number(integer=True)),
        "project": Field(Str(non_empty=True), "skein"),
        "database": Field(Str(non_empty=True), "skein.db"),
        "user_agent": Field(Str(non_empty=True), DEFAULT_USER_AGENT),
        "http": Field(HTTP_SPEC, {}),
        "robots": Field(PROJECT_ROBOTS_SPEC, {}),
        "checkpoint": Field(CHECKPOINT_SPEC, {}),
        "pipeline": Field(PIPELINE_SPEC, {}),
        "workers": Field(WORKERS_SPEC, {}),
        "dedup": Field(DEDUP_SPEC, {}),
        "warc": Field(WARC_SPEC, {}),
        "metrics": Field(METRICS_SPEC, {}),
        "spiders": Field(ListOf(SPIDER_SPEC, min_items=1)),
    }
)
