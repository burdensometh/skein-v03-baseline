"""Turning validated version-2 dictionaries into typed settings objects."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from skein.checkpoint.journal import FsyncPolicy
from skein.checkpoint.session import CheckpointSettings
from skein.config.model import HttpSettings, ProjectConfig, SpiderConfig
from skein.config.runtime import (
    ArchiveSettings,
    DedupSettings,
    MetricsSettings,
    PipelineSettings,
    RuntimeSettings,
    WorkerSettings,
)
from skein.extract.links import DEFAULT_DENY_EXTENSIONS
from skein.extract.rules import ExtractionPlan, FieldRule, ItemRule, LinkRule
from skein.incremental.fingerprint import Fingerprinter
from skein.incremental.modes import IncrementalSettings, RevisitRule, RunMode
from skein.incremental.revisit import RevisitPolicy
from skein.politeness.policy import PolitenessPolicy
from skein.retry.backoff import BackoffPolicy
from skein.retry.policy import RetryPolicy
from skein.urls.canonical import Canonicalizer, QueryPolicy
from skein.urls.parse import parse_url


def build_field(name: str, raw: dict[str, Any]) -> FieldRule:
    source = raw["source"] or ("attr" if raw["attr"] else "text")
    return FieldRule(
        name=name,
        selector=raw["selector"],
        source=source,
        attr=raw["attr"],
        regex=re.compile(raw["regex"]) if raw["regex"] else None,
        transforms=tuple(raw["transforms"]),
        type=raw["type"],
        formats=tuple(raw["formats"]),
        multiple=raw["multiple"],
        join=raw["join"],
        required=raw["required"],
        default=raw["default"],
    )


def build_item(raw: dict[str, Any]) -> ItemRule:
    return ItemRule(
        name=raw["name"],
        fields=tuple(build_field(name, spec) for name, spec in raw["fields"].items()),
        url_pattern=re.compile(raw["url_pattern"]) if raw["url_pattern"] else None,
        scope=raw["scope"],
        key_fields=tuple(raw["key"]),
        partial=raw["partial"],
        authority=raw["authority"],
    )


def build_link(raw: dict[str, Any]) -> LinkRule:
    return LinkRule(
        allow=tuple(re.compile(p) for p in raw["allow"]),
        deny=tuple(re.compile(p) for p in raw["deny"]),
        selector=raw["selector"],
        attr=raw["attr"],
        max_depth=raw["max_depth"],
        priority=raw["priority"],
        kind=raw["kind"],
        source_pattern=re.compile(raw["from"]) if raw["from"] else None,
    )


def default_domains(raw: dict[str, Any]) -> tuple[str, ...]:
    hosts: list[str] = []
    for url in [*raw["seeds"], *raw["sitemaps"]]:
        host = (parse_url(url).host or "").lower()
        if host and host not in hosts:
            hosts.append(host)
    return tuple(hosts)


def build_retry(raw: dict[str, Any]) -> RetryPolicy:
    return RetryPolicy(
        max_retries=raw["max_retries"],
        backoff=BackoffPolicy(
            base=raw["backoff_base"], factor=raw["backoff_factor"], max_delay=raw["backoff_max"], jitter=raw["jitter"]
        ),
        respect_retry_after=raw["respect_retry_after"],
        max_retry_after=raw["max_retry_after"],
        host_failure_limit=raw["host_failure_limit"],
        quarantine=raw["quarantine"],
    )


def build_incremental(raw: dict[str, Any]) -> IncrementalSettings:
    revisit = raw["revisit"]
    default = RevisitPolicy(
        initial_interval=revisit["initial_interval"],
        min_interval=revisit["min_interval"],
        max_interval=revisit["max_interval"],
        target_probability=revisit["target_probability"],
        retry_interval=revisit["retry_interval"],
        honour_freshness=revisit["honour_freshness"],
    )
    rules = []
    for rule in raw["revisit_rules"]:
        overrides = {k: v for k, v in rule.items() if k != "pattern" and v is not None}
        rules.append(RevisitRule(re.compile(rule["pattern"]), default.derive(**overrides)))
    return IncrementalSettings(
        mode=RunMode(raw["mode"]),
        conditional_requests=raw["conditional_requests"],
        use_sitemap_lastmod=raw["use_sitemap_lastmod"],
        revisit=default,
        revisit_rules=tuple(rules),
        fingerprint=Fingerprinter(tuple(re.compile(p) for p in raw["fingerprint_ignore"])),
    )


def build_canonicalizer(raw: dict[str, Any]) -> Canonicalizer:
    """Rules version 1 keeps 0.1/0.2 keys exactly; version 2 adds tracking and session-id removal and sorting."""
    keep_params = raw["keep_params"]
    keep_only = tuple(keep_params) if keep_params is not None else None
    if raw["rules"] == 1:
        return Canonicalizer(
            query=QueryPolicy(sort=raw["sort_query"], remove=tuple(raw["remove_params"]), keep_only=keep_only,
                              drop_empty_values=raw["drop_empty_params"]),
            keep_fragment=raw["keep_fragment"],
        )
    order = raw["query_order"] or "sort"
    return Canonicalizer.strict(remove=tuple(raw["remove_params"]), keep_only=keep_only,
                                  drop_empty_values=raw["drop_empty_params"], tracking=tuple(raw["tracking_params"]),
                                  query_order=order, keep_fragment=raw["keep_fragment"])


def build_runtime(raw: dict[str, Any], base_dir: Path | None) -> RuntimeSettings:
    pipeline, workers, dedup, warc, metrics = (raw["pipeline"], raw["workers"], raw["dedup"], raw["warc"],
                                               raw["metrics"])
    bloom, near = dedup["bloom"], dedup["near_duplicates"]
    return RuntimeSettings(
        pipeline=PipelineSettings(**pipeline),
        workers=WorkerSettings(**workers),
        dedup=DedupSettings(
            bloom=bloom["enabled"],
            bloom_directory=_resolve(bloom["directory"], base_dir) if bloom["directory"] else None,
            bloom_capacity=bloom["capacity"],
            bloom_error_rate=bloom["error_rate"],
            bloom_verify_every=bloom["verify_every"],
            near_duplicates=near["enabled"],
            max_distance=near["max_distance"],
            shingle_size=near["shingle_size"],
            min_words=near["min_words"],
            near_action=near["action"],
        ),
        archive=ArchiveSettings(
            enabled=warc["enabled"],
            directory=_resolve(warc["directory"], base_dir) if warc["directory"] else None,
            prefix=warc["prefix"],
            max_size=warc["max_size"],
            metadata=warc["metadata"],
            revisits=warc["revisits"],
            compresslevel=warc["compresslevel"],
        ),
        metrics=MetricsSettings(
            textfile=_resolve(metrics["textfile"], base_dir) if metrics["textfile"] else None,
            interval=metrics["interval"],
            port=metrics["port"],
        ),
    )


def build_spider(raw: dict[str, Any]) -> SpiderConfig:
    domains = raw["allowed_domains"]
    canonical, extract = raw["canonical"], raw["extract"]
    politeness, robots = raw["politeness"], raw["robots"]
    return SpiderConfig(
        name=raw["name"],
        seeds=tuple(raw["seeds"]),
        sitemaps=tuple(raw["sitemaps"]),
        use_robots_sitemaps=robots["use_sitemaps"],
        allowed_domains=tuple(domains) if domains is not None else default_domains(raw),
        max_depth=raw["max_depth"],
        max_pages=raw["max_pages"],
        obey_robots=robots["obey"],
        obey_nofollow=extract["obey_nofollow"],
        obey_noindex=extract["obey_noindex"],
        deny_extensions=(
            frozenset(e.lstrip(".").lower() for e in extract["deny_extensions"])
            if extract["deny_extensions"] is not None
            else DEFAULT_DENY_EXTENSIONS
        ),
        concurrency=raw["concurrency"],
        global_concurrency=raw["global_concurrency"],
        politeness=PolitenessPolicy(
            min_delay=politeness["min_delay"],
            max_crawl_delay=politeness["max_crawl_delay"],
            per_host_concurrency=politeness["per_host_concurrency"],
            respect_crawl_delay=politeness["respect_crawl_delay"],
        ),
        retry=build_retry(raw["retry"]),
        retry_seed=raw["retry"]["seed"],
        canonicalizer=build_canonicalizer(canonical),
        plan=ExtractionPlan(
            items=tuple(build_item(item) for item in extract["items"]),
            links=tuple(build_link(link) for link in extract["links"]),
        ),
        incremental=build_incremental(raw["incremental"]),
    )


def _resolve(path: str, base_dir: Path | None) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute() and base_dir is not None:
        resolved = base_dir / resolved
    return resolved


def build_project(
    raw: dict[str, Any], *, version: int, base_dir: Path | None, source: str | None, deprecations: tuple[str, ...]
) -> ProjectConfig:
    http_raw = raw["http"]
    checkpoint = raw["checkpoint"]
    http = HttpSettings(
        user_agent=raw["user_agent"],
        connect_timeout=http_raw["connect_timeout"],
        read_timeout=http_raw["read_timeout"],
        total_timeout=http_raw["total_timeout"],
        max_body_bytes=http_raw["max_body_bytes"],
        max_redirects=http_raw["max_redirects"],
        max_connections_per_host=http_raw["max_connections_per_host"],
        verify_tls=http_raw["verify_tls"],
        headers=dict(http_raw["headers"]),
    )
    return ProjectConfig(
        version=version,
        project=raw["project"],
        database=_resolve(raw["database"], base_dir),
        http=http,
        robots_ttl=raw["robots"]["ttl"],
        spiders=tuple(build_spider(s) for s in raw["spiders"]),
        checkpoint=CheckpointSettings(
            fsync=FsyncPolicy(checkpoint["fsync"]),
            fsync_batch=checkpoint["fsync_batch"],
            fsync_interval=checkpoint["fsync_interval"],
            snapshot_every=checkpoint["snapshot_every"],
            snapshot_interval=checkpoint["snapshot_interval"],
            sqlite_synchronous=checkpoint["sqlite_synchronous"],
        ),
        checkpoint_dir=_resolve(checkpoint["directory"], base_dir) if checkpoint["directory"] else None,
        runtime=build_runtime(raw, base_dir),
        source=source,
        deprecations=deprecations,
    )
