"""Politeness settings shared by all hosts of a crawl."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PolitenessPolicy:
    """``min_delay`` is the minimum start-to-start spacing for requests to one host.

    A robots.txt ``Crawl-delay`` raises it, but never above
    ``max_crawl_delay`` so that a hostile value cannot stall the crawl.
    ``per_host_concurrency`` is the number of requests one host may have
    in flight, counted over every process sharing the database.
    """

    min_delay: float = 1.0
    max_crawl_delay: float = 60.0
    per_host_concurrency: int = 1
    respect_crawl_delay: bool = True

    def __post_init__(self) -> None:
        if self.min_delay < 0:
            raise ValueError("min_delay must be >= 0")
        if self.max_crawl_delay < 0:
            raise ValueError("max_crawl_delay must be >= 0")
        if self.per_host_concurrency < 1:
            raise ValueError("per_host_concurrency must be >= 1")

    def delay(self, crawl_delay: float | None) -> float:
        """Effective spacing given a host's robots.txt ``Crawl-delay`` (``None`` if it has none)."""
        delay = self.min_delay
        if self.respect_crawl_delay and crawl_delay is not None:
            delay = max(delay, min(crawl_delay, self.max_crawl_delay))
        return delay
