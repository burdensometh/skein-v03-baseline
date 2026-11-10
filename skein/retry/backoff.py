"""Exponential back-off with proportional jitter."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class BackoffPolicy:
    """``base * factor ** attempt`` seconds, capped at ``max_delay``, plus jitter.

    Jitter is proportional and only ever lengthens the wait: the delay for
    attempt ``n`` lies in ``[d, d * (1 + jitter)]`` (still capped), where ``d``
    is the deterministic exponential delay. Callers pass a
    :class:`random.Random` so that tests can seed it and get exact values.
    """

    base: float = 5.0
    factor: float = 2.0
    max_delay: float = 300.0
    jitter: float = 0.25

    def __post_init__(self) -> None:
        if self.base < 0:
            raise ValueError("back-off base must be >= 0")
        if self.factor < 1:
            raise ValueError("back-off factor must be >= 1")
        if self.max_delay < self.base:
            raise ValueError("back-off max_delay must be >= base")
        if not 0 <= self.jitter <= 1:
            raise ValueError("back-off jitter must be between 0 and 1")

    def ceiling(self, attempt: int) -> float:
        """The un-jittered delay for the ``attempt``-th consecutive failure (0-based)."""
        if attempt < 0:
            raise ValueError("attempt must be >= 0")
        # Multiply step by step so that huge attempt numbers cannot overflow.
        delay = self.base
        for _ in range(attempt):
            delay *= self.factor
            if delay >= self.max_delay:
                return self.max_delay
        return min(delay, self.max_delay)

    def delay(self, attempt: int, rng: random.Random) -> float:
        ceiling = self.ceiling(attempt)
        if self.jitter == 0 or ceiling == 0:
            return ceiling
        return min(self.max_delay, ceiling * (1.0 + self.jitter * rng.random()))
