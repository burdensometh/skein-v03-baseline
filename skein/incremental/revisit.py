"""Adaptive revisit intervals estimated from a document's change history.

Each successful revisit of a document is a *check* that either detected a
change (different fingerprint, or a 200 after validators no longer match)
or did not (304, identical fingerprint). With ``n`` checks, ``x`` detected
changes and a mean check interval ``I``, the change rate is estimated with
Cho & Garcia-Molina's bias-reduced estimator for incomplete histories::

    rate = -ln((n - x + 0.5) / (n + 0.5)) / I

which stays finite when every check saw a change. The next interval is the
time after which the document has changed with probability
``target_probability``: ``-ln(1 - p) / rate``. To keep a single
observation from swinging the schedule wildly, an interval may at most grow
by ``max_growth`` or shrink by ``max_shrink`` per visit, and always stays
within ``[min_interval, max_interval]``. A freshness lifetime announced by
the server (see :mod:`skein.incremental.freshness`) is a lower bound when
``honour_freshness`` is set.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace

CHANGEFREQ_SECONDS = {
    "hourly": 3600.0,
    "daily": 86400.0,
    "weekly": 7 * 86400.0,
    "monthly": 30 * 86400.0,
    "yearly": 365 * 86400.0,
}


@dataclass(frozen=True)
class RevisitPolicy:
    """Bounds and tuning of the revisit schedule (all times in seconds)."""

    initial_interval: float = 86400.0
    min_interval: float = 3600.0
    max_interval: float = 30 * 86400.0
    target_probability: float = 0.5
    max_growth: float = 2.0
    max_shrink: float = 4.0
    retry_interval: float | None = None
    honour_freshness: bool = True

    def __post_init__(self) -> None:
        if not 0 < self.min_interval <= self.initial_interval <= self.max_interval:
            raise ValueError("revisit intervals must satisfy 0 < min <= initial <= max")
        if not 0 < self.target_probability < 1:
            raise ValueError("target_probability must be between 0 and 1")
        if self.max_growth < 1 or self.max_shrink < 1:
            raise ValueError("max_growth and max_shrink must be >= 1")

    def derive(self, **overrides: float) -> "RevisitPolicy":
        """A copy with some bounds replaced; unspecified bounds are adjusted as needed to stay consistent."""
        low = overrides.get("min_interval", self.min_interval)
        high = overrides.get("max_interval", self.max_interval)
        initial = overrides.get("initial_interval", self.initial_interval)
        if "initial_interval" not in overrides:
            initial = min(high, max(low, initial))
        elif "min_interval" not in overrides and initial < low:
            low = initial
        elif "max_interval" not in overrides and initial > high:
            high = initial
        changes = {**overrides, "min_interval": low, "max_interval": high, "initial_interval": initial}
        return replace(self, **changes)

    def clamp(self, seconds: float) -> float:
        return min(self.max_interval, max(self.min_interval, seconds))

    def hint(self, changefreq: str | None) -> float | None:
        """Interval suggested by a sitemap ``<changefreq>`` (``None`` when absent or unknown)."""
        if changefreq is None:
            return None
        if changefreq == "always":
            return self.min_interval
        if changefreq == "never":
            return self.max_interval
        seconds = CHANGEFREQ_SECONDS.get(changefreq)
        return self.clamp(seconds) if seconds is not None else None

    def first_interval(self, changefreq: str | None = None) -> float:
        hinted = self.hint(changefreq)
        return hinted if hinted is not None else self.initial_interval

    def estimate_rate(self, checks: int, changes: int, observed_span: float) -> float | None:
        """Changes per second, or ``None`` without usable history."""
        if checks <= 0 or observed_span <= 0:
            return None
        changes = min(max(changes, 0), checks)
        mean_interval = observed_span / checks
        ratio = (checks - changes + 0.5) / (checks + 0.5)
        return -math.log(ratio) / mean_interval

    def next_interval(
        self,
        *,
        checks: int,
        changes: int,
        observed_span: float,
        previous: float | None,
        changefreq: str | None = None,
        freshness: float | None = None,
    ) -> float:
        """Interval until the next visit after a successful fetch."""
        if previous is None:
            interval = self.first_interval(changefreq)
        else:
            rate = self.estimate_rate(checks, changes, observed_span)
            if rate is None:
                interval = self.clamp(previous)
            else:
                wanted = math.inf if rate == 0 else -math.log(1.0 - self.target_probability) / rate
                interval = self.clamp(min(previous * self.max_growth, max(previous / self.max_shrink, wanted)))
        if self.honour_freshness and freshness is not None:
            interval = max(interval, min(freshness, self.max_interval))
        return interval

    def failure_interval(self, transient: bool, previous: float | None) -> float:
        """Interval after a failed fetch: soon for transient failures, rarely for permanent ones."""
        if transient:
            return self.retry_interval if self.retry_interval is not None else self.min_interval
        return self.max_interval if previous is None else self.clamp(max(previous, self.max_interval / 4))
