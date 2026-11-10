"""Injectable time sources.

Every component that needs "now" or has to wait receives a :class:`Clock`.
Production code uses :class:`SystemClock`; tests use :class:`FakeClock`, whose
``sleep`` advances virtual time instantly so politeness delays cost nothing.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Source of wall-clock time, monotonic time and asynchronous waiting."""

    def time(self) -> float:
        """Seconds since the Unix epoch (wall clock)."""

    def monotonic(self) -> float:
        """Seconds on a clock that never goes backwards."""

    async def sleep(self, seconds: float) -> None:
        """Suspend the calling task for ``seconds``."""

    async def sleep_until(self, deadline: float) -> None:
        """Suspend the calling task until ``monotonic()`` reaches ``deadline``."""


class SystemClock:
    """Clock backed by the operating system and the running event loop."""

    def time(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))

    async def sleep_until(self, deadline: float) -> None:
        await asyncio.sleep(max(0.0, deadline - time.monotonic()))


class FakeClock:
    """Deterministic clock for tests.

    ``sleep`` and ``sleep_until`` move virtual time forward immediately and
    yield to the event loop once, so tests never wait for real. Wall time and
    monotonic time advance together.
    """

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._wall_offset = start
        self._mono = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self._wall_offset + self._mono

    def monotonic(self) -> float:
        return self._mono

    def advance(self, seconds: float) -> None:
        """Move time forward by ``seconds`` (negative values are rejected)."""
        if seconds < 0:
            raise ValueError("FakeClock cannot move backwards")
        self._mono += seconds

    def set_time(self, wall: float) -> None:
        """Jump the wall clock to ``wall`` without touching monotonic order."""
        if wall < self.time():
            raise ValueError("FakeClock cannot move backwards")
        self._mono += wall - self.time()

    async def sleep(self, seconds: float) -> None:
        seconds = max(0.0, seconds)
        self.sleeps.append(seconds)
        self._mono += seconds
        await asyncio.sleep(0)

    async def sleep_until(self, deadline: float) -> None:
        """Jump to ``deadline`` unless time has already passed it.

        Several tasks waiting for the same deadline therefore advance time
        once, not once each, which keeps politeness timings exact in tests.
        """
        if deadline > self._mono:
            self.sleeps.append(deadline - self._mono)
            self._mono = deadline
        await asyncio.sleep(0)


def isoformat(epoch: float | None) -> str | None:
    """Render an epoch timestamp as an ISO 8601 UTC string (``None`` passes through)."""
    if epoch is None:
        return None
    moment = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return moment.isoformat(timespec="seconds").replace("+00:00", "Z")
