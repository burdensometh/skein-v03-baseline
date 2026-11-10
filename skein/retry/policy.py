"""Per-request retry decisions."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum

from skein.retry.backoff import BackoffPolicy
from skein.retry.classify import Failure


class RetryAction(str, Enum):
    """What to do with a request after a failed attempt."""

    RETRY = "retry"
    DEFER = "defer"
    GIVE_UP = "give_up"


@dataclass(frozen=True)
class RetryDecision:
    action: RetryAction
    delay: float = 0.0
    reason: str = ""


@dataclass(frozen=True)
class RetryPolicy:
    """How often and how patiently a failed request is retried within a run.

    ``max_retries`` bounds the extra attempts for transient failures. A
    server-supplied ``Retry-After`` is honoured (it is a lower bound for the
    back-off) unless it exceeds ``max_retry_after``: such a request is
    *deferred* to a later run instead of keeping this run waiting. After
    ``host_failure_limit`` consecutive host-level failures the whole host
    is quarantined: its queued requests are deferred by ``quarantine``
    seconds.
    """

    max_retries: int = 2
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    respect_retry_after: bool = True
    max_retry_after: float = 3600.0
    host_failure_limit: int | None = 10
    quarantine: float = 3600.0

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.max_retry_after < 0:
            raise ValueError("max_retry_after must be >= 0")
        if self.host_failure_limit is not None and self.host_failure_limit < 1:
            raise ValueError("host_failure_limit must be >= 1")

    def quarantines(self, consecutive_failures: int) -> bool:
        """Whether a host with this many consecutive host-level failures is shut out for the run."""
        return self.host_failure_limit is not None and consecutive_failures >= self.host_failure_limit

    def decide(self, failure: Failure, attempts: int, rng: random.Random) -> RetryDecision:
        """Decide after the ``attempts``-th retry (0 = the first attempt) failed with ``failure``."""
        if not failure.transient:
            return RetryDecision(RetryAction.GIVE_UP, reason=f"permanent failure: {failure.reason}")
        retry_after = failure.retry_after if self.respect_retry_after else None
        if retry_after is not None and retry_after > self.max_retry_after:
            return RetryDecision(
                RetryAction.DEFER, retry_after,
                f"Retry-After {retry_after:g}s exceeds {self.max_retry_after:g}s; deferred to a later run",
            )
        if attempts >= self.max_retries:
            return RetryDecision(RetryAction.GIVE_UP, reason=f"gave up after {attempts + 1} attempt(s)")
        delay = self.backoff.delay(attempts, rng)
        if retry_after is not None:
            delay = max(delay, retry_after)
        return RetryDecision(RetryAction.RETRY, delay, f"retry {attempts + 1}/{self.max_retries} in {delay:.2f}s")
