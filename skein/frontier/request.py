"""The unit of work flowing through the frontier."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from skein.urls.parse import parse_url


class RequestKind(str, Enum):
    """What the crawler should do with a fetched request."""

    PAGE = "page"
    SITEMAP = "sitemap"


@dataclass
class CrawlRequest:
    """A URL scheduled for fetching.

    ``url`` must already be canonical; it doubles as the de-duplication key.
    ``host_key`` is the origin used for politeness and robots decisions.
    ``seed`` is the crawl root (seed page or sitemap URL) through which the
    URL was first reached; per-seed watermarks are kept for these roots.
    """

    url: str
    depth: int = 0
    priority: int = 0
    kind: RequestKind = RequestKind.PAGE
    referrer: str | None = None
    attempts: int = 0
    sitemap_lastmod: float | None = None
    seed: str | None = None
    changefreq: str | None = None
    host_key: str = field(init=False)

    def __post_init__(self) -> None:
        self.host_key = parse_url(self.url).origin

    def child(self, url: str, *, priority: int = 0, kind: RequestKind = RequestKind.PAGE) -> "CrawlRequest":
        """A request discovered on this one, one level deeper and under the same seed."""
        return CrawlRequest(url, self.depth + 1, priority, kind, referrer=self.url, seed=self.seed)
