"""Typed configuration objects produced by the loader."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from skein.checkpoint.session import CheckpointSettings
from skein.config.runtime import RuntimeSettings
from skein.extract.links import DEFAULT_DENY_EXTENSIONS, DomainScope, LinkExtractor
from skein.extract.rules import ExtractionPlan
from skein.incremental.modes import IncrementalSettings
from skein.net.client import ClientConfig
from skein.politeness.policy import PolitenessPolicy
from skein.retry.policy import RetryPolicy
from skein.urls.canonical import Canonicalizer


@dataclass(frozen=True)
class HttpSettings:
    """Network settings shared by all spiders of a project."""

    user_agent: str
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    total_timeout: float = 60.0
    max_body_bytes: int = 10 * 1024 * 1024
    max_redirects: int = 10
    max_connections_per_host: int = 6
    verify_tls: bool = True
    headers: dict[str, str] = field(default_factory=dict)

    def client_config(self) -> ClientConfig:
        return ClientConfig(
            user_agent=self.user_agent,
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
            total_timeout=self.total_timeout,
            max_body_bytes=self.max_body_bytes,
            max_redirects=self.max_redirects,
            max_connections_per_origin=self.max_connections_per_host,
            verify_tls=self.verify_tls,
            default_headers=dict(self.headers),
        )


@dataclass(frozen=True)
class SpiderConfig:
    """Everything one spider needs to crawl, extract and recrawl incrementally."""

    name: str
    seeds: tuple[str, ...]
    sitemaps: tuple[str, ...] = ()
    use_robots_sitemaps: bool = False
    allowed_domains: tuple[str, ...] = ()
    max_depth: int | None = None
    max_pages: int | None = None
    obey_robots: bool = True
    obey_nofollow: bool = True
    obey_noindex: bool = True
    deny_extensions: frozenset[str] = DEFAULT_DENY_EXTENSIONS
    concurrency: int = 4
    global_concurrency: int | None = None
    politeness: PolitenessPolicy = field(default_factory=PolitenessPolicy)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    retry_seed: int | None = None
    canonicalizer: Canonicalizer = field(default_factory=Canonicalizer)
    plan: ExtractionPlan = field(default_factory=ExtractionPlan)
    incremental: IncrementalSettings = field(default_factory=IncrementalSettings)

    def link_extractor(self) -> LinkExtractor:
        """A :class:`LinkExtractor` configured with this spider's rules, scope and filters."""
        return LinkExtractor(
            self.plan.links,
            self.canonicalizer,
            DomainScope(self.allowed_domains),
            obey_nofollow=self.obey_nofollow,
            deny_extensions=self.deny_extensions,
        )


@dataclass(frozen=True)
class ProjectConfig:
    """A loaded and validated configuration file.

    ``version`` is the version the file was written in; ``deprecations``
    lists what the compatibility loader had to translate.
    """

    version: int
    project: str
    database: Path
    http: HttpSettings
    robots_ttl: float
    spiders: tuple[SpiderConfig, ...]
    checkpoint: CheckpointSettings = field(default_factory=CheckpointSettings)
    checkpoint_dir: Path | None = None
    runtime: RuntimeSettings = field(default_factory=RuntimeSettings)
    source: str | None = None
    deprecations: tuple[str, ...] = ()

    def spider(self, name: str | None = None) -> SpiderConfig:
        """Return the spider called ``name``; with ``None`` the only spider."""
        if name is None:
            if len(self.spiders) != 1:
                names = ", ".join(s.name for s in self.spiders)
                raise KeyError(f"configuration defines several spiders ({names}); choose one")
            return self.spiders[0]
        for spider in self.spiders:
            if spider.name == name:
                return spider
        raise KeyError(f"no spider named {name!r}; known: {', '.join(s.name for s in self.spiders)}")

    def checkpoint_root(self) -> Path:
        """Directory holding one checkpoint subdirectory per spider."""
        if self.checkpoint_dir is not None:
            return self.checkpoint_dir
        return self.database.with_name(self.database.name + ".checkpoints")
