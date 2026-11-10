"""Run statistics collected by the crawler.

Statistics are updated only through :meth:`CrawlStats.apply` with small
*delta* dictionaries. The crawler journals the same deltas, so replaying a
journal after a crash rebuilds exactly the counters the live process had.
Every worker process keeps its own :class:`CrawlStats`; a run's statistics
are the :meth:`CrawlStats.merge` of all its workers'.
"""

from __future__ import annotations

import bisect
from collections import Counter
from dataclasses import dataclass, field, fields
from typing import Any

DEFAULT_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
PER_HOST_COUNTERS = frozenset({"requests", "responses", "bytes_received", "retries", "not_modified", "pages_stored"})


class Histogram:
    """Cumulative-bucket histogram (Prometheus style ``le`` buckets)."""

    def __init__(self, buckets: tuple[float, ...] = DEFAULT_LATENCY_BUCKETS) -> None:
        if list(buckets) != sorted(buckets):
            raise ValueError("histogram buckets must be sorted")
        self.buckets = tuple(buckets)
        self._counts = [0] * (len(buckets) + 1)
        self.total = 0.0
        self.count = 0

    def observe(self, value: float) -> None:
        self._counts[bisect.bisect_left(self.buckets, value)] += 1
        self.total += value
        self.count += 1

    def cumulative(self) -> list[tuple[str, int]]:
        """``(le, count)`` pairs including the ``+Inf`` bucket."""
        out: list[tuple[str, int]] = []
        running = 0
        for bound, count in zip(self.buckets, self._counts):
            running += count
            out.append((f"{bound:g}", running))
        out.append(("+Inf", running + self._counts[-1]))
        return out

    def quantile(self, q: float) -> float | None:
        """Upper bucket bound below which a fraction ``q`` of observations fall."""
        if self.count == 0:
            return None
        target = q * self.count
        running = 0
        for bound, count in zip(self.buckets, self._counts):
            running += count
            if running >= target:
                return bound
        return float("inf")

    def merge(self, other: "Histogram") -> None:
        """Add ``other``'s observations (same buckets required)."""
        if other.buckets != self.buckets:
            raise ValueError("cannot merge histograms with different buckets")
        self._counts = [a + b for a, b in zip(self._counts, other._counts)]
        self.total += other.total
        self.count += other.count

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "sum": round(self.total, 6),
            "p50": self.quantile(0.5),
            "p95": self.quantile(0.95),
            "buckets": dict(self.cumulative()),
        }

    def to_state(self) -> dict[str, Any]:
        return {"buckets": list(self.buckets), "counts": list(self._counts), "total": self.total}

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> "Histogram":
        histogram = cls(tuple(state["buckets"]))
        counts = [int(c) for c in state["counts"]]
        if len(counts) != len(histogram._counts):
            raise ValueError("histogram state does not match its buckets")
        histogram._counts = counts
        histogram.total = float(state["total"])
        histogram.count = sum(counts)
        return histogram


@dataclass
class CrawlStats:
    """Counters for one crawl run; :meth:`summary` gives a JSON-friendly view."""

    requests: int = 0
    responses: int = 0
    status_counts: Counter = field(default_factory=Counter)
    errors: Counter = field(default_factory=Counter)
    retries: int = 0
    retry_after_honoured: int = 0
    deferred: int = 0
    gave_up: int = 0
    host_backoffs: int = 0
    hosts_quarantined: int = 0
    bytes_received: int = 0
    bytes_decoded: int = 0
    pages_stored: int = 0
    non_html: int = 0
    conditional_requests: int = 0
    not_modified: int = 0
    documents_new: int = 0
    documents_changed: int = 0
    documents_unchanged: int = 0
    items_new: int = 0
    items_updated: int = 0
    items_unchanged: int = 0
    items_touched: int = 0
    items_deleted: int = 0
    items_dropped: int = 0
    extraction_errors: int = 0
    links_found: int = 0
    links_enqueued: int = 0
    robots_fetches: int = 0
    robots_denied: int = 0
    sitemaps_fetched: int = 0
    sitemap_urls: int = 0
    sitemap_skipped: int = 0
    sitemap_promoted: int = 0
    redirects_followed: int = 0
    redirects_deferred: int = 0
    near_duplicates: int = 0
    links_filtered: int = 0
    warc_records: int = 0
    warc_revisits: int = 0
    warc_cross_revisits: int = 0
    bloom_false_positives: int = 0
    warc_bytes: int = 0
    leases_lost: int = 0
    write_batches: int = 0
    budget_exhausted: bool = False
    latency: Histogram = field(default_factory=Histogram)
    per_host: dict[str, Counter] = field(default_factory=dict)

    @classmethod
    def counter_names(cls) -> frozenset[str]:
        return frozenset(f.name for f in fields(cls) if f.type in ("int", int))

    def apply(self, delta: dict[str, Any]) -> None:
        """Add a delta: integer counters by name plus the special keys
        ``status`` (one response with that status), ``error`` (one error of
        that kind), ``latency`` (one observation in seconds),
        ``budget_exhausted`` and ``host``. With ``host``, the counters in
        :data:`PER_HOST_COUNTERS` and errors are also added to that host's
        own tally.
        """
        names = self.counter_names()
        host = delta.get("host")
        if host is not None:
            tally = self.per_host.setdefault(str(host), Counter())
            for key in PER_HOST_COUNTERS & delta.keys():
                tally[key] += int(delta[key])
            if "error" in delta:
                tally["errors"] += 1
        for key, value in delta.items():
            if key == "host":
                continue
            if key == "status":
                self.status_counts[str(value)] += 1
            elif key == "error":
                self.errors[str(value)] += 1
            elif key == "latency":
                self.latency.observe(float(value))
            elif key == "budget_exhausted":
                self.budget_exhausted = self.budget_exhausted or bool(value)
            elif key in names:
                setattr(self, key, getattr(self, key) + int(value))
            else:
                raise KeyError(f"unknown statistics counter {key!r}")

    def merge(self, other: "CrawlStats") -> None:
        """Add another worker's statistics to these."""
        for name in self.counter_names():
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.status_counts.update(other.status_counts)
        self.errors.update(other.errors)
        self.budget_exhausted = self.budget_exhausted or other.budget_exhausted
        self.latency.merge(other.latency)
        for host, counts in other.per_host.items():
            self.per_host.setdefault(host, Counter()).update(counts)

    def to_state(self) -> dict[str, Any]:
        state: dict[str, Any] = {name: getattr(self, name) for name in sorted(self.counter_names())}
        state["status_counts"] = dict(self.status_counts)
        state["errors"] = dict(self.errors)
        state["budget_exhausted"] = self.budget_exhausted
        state["latency"] = self.latency.to_state()
        state["per_host"] = {host: dict(counts) for host, counts in self.per_host.items()}
        return state

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> "CrawlStats":
        stats = cls()
        for name in cls.counter_names():
            setattr(stats, name, int(state.get(name, 0)))
        stats.status_counts = Counter({str(k): int(v) for k, v in state.get("status_counts", {}).items()})
        stats.errors = Counter({str(k): int(v) for k, v in state.get("errors", {}).items()})
        stats.budget_exhausted = bool(state.get("budget_exhausted", False))
        if "latency" in state:
            stats.latency = Histogram.from_state(state["latency"])
        stats.per_host = {host: Counter({k: int(v) for k, v in counts.items()})
                          for host, counts in state.get("per_host", {}).items()}
        return stats

    def summary(self, duration: float) -> dict[str, Any]:
        return {
            "requests": self.requests,
            "responses": self.responses,
            "status_counts": dict(sorted(self.status_counts.items())),
            "errors": dict(sorted(self.errors.items())),
            "retries": self.retries,
            "retry_after_honoured": self.retry_after_honoured,
            "deferred": self.deferred,
            "gave_up": self.gave_up,
            "host_backoffs": self.host_backoffs,
            "hosts_quarantined": self.hosts_quarantined,
            "bytes_received": self.bytes_received,
            "bytes_decoded": self.bytes_decoded,
            "pages_stored": self.pages_stored,
            "non_html": self.non_html,
            "conditional_requests": self.conditional_requests,
            "not_modified": self.not_modified,
            "documents": {
                "new": self.documents_new,
                "changed": self.documents_changed,
                "unchanged": self.documents_unchanged,
            },
            "items": {
                "new": self.items_new,
                "updated": self.items_updated,
                "unchanged": self.items_unchanged,
                "touched": self.items_touched,
                "deleted": self.items_deleted,
                "dropped": self.items_dropped,
            },
            "extraction_errors": self.extraction_errors,
            "links_found": self.links_found,
            "links_enqueued": self.links_enqueued,
            "robots_fetches": self.robots_fetches,
            "robots_denied": self.robots_denied,
            "sitemaps_fetched": self.sitemaps_fetched,
            "sitemap_urls": self.sitemap_urls,
            "sitemap_skipped": self.sitemap_skipped,
            "sitemap_promoted": self.sitemap_promoted,
            "redirects_followed": self.redirects_followed,
            "redirects_deferred": self.redirects_deferred,
            "dedup": {"near_duplicates": self.near_duplicates, "links_filtered": self.links_filtered,
                      "bloom_false_positives": self.bloom_false_positives},
            "archive": {"records": self.warc_records, "revisits": self.warc_revisits,
                        "cross_url_revisits": self.warc_cross_revisits, "bytes": self.warc_bytes},
            "leases_lost": self.leases_lost,
            "write_batches": self.write_batches,
            "budget_exhausted": self.budget_exhausted,
            "duration": round(max(0.0, duration), 3),
            "requests_per_second": round(self.requests / duration, 2) if duration > 0 else None,
            "latency": self.latency.to_dict(),
            "per_host": {host: dict(sorted(counts.items())) for host, counts in sorted(self.per_host.items())},
        }
