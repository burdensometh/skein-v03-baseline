"""Bounded queues between pipeline stages, with backpressure accounting and orderly closing.

Every item's time in the queue is recorded in :attr:`StageQueue.waits`.
A producer that finds the queue full waits until a consumer makes room --
that is the backpressure: a slow writer stalls the parsers, stalled
parsers stall the fetchers, and the dispatcher stops claiming new work
(and with it host slots and leases) until the pipeline drains. Every wait
is counted, so the metrics show which stage is the bottleneck.

Closing a queue enqueues one end marker per consumer behind the remaining
items; each consumer finishes the work in front of it and then receives
``None``.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Generic, TypeVar

from skein.metrics.stats import Histogram

T = TypeVar("T")
_END = object()
WAIT_BUCKETS = (0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0)


@dataclass
class QueueStats:
    name: str
    capacity: int
    depth: int
    high_water: int
    puts: int
    blocked_puts: int
    blocked_seconds: float

    def to_dict(self) -> dict[str, object]:
        return {"capacity": self.capacity, "depth": self.depth, "high_water": self.high_water, "puts": self.puts,
                "blocked_puts": self.blocked_puts, "blocked_seconds": round(self.blocked_seconds, 6)}


class StageQueue(Generic[T]):
    """An :class:`asyncio.Queue` of at most ``capacity`` items feeding ``consumers`` tasks."""

    def __init__(self, name: str, capacity: int, consumers: int = 1) -> None:
        if capacity < 1 or consumers < 1:
            raise ValueError("queue capacity and consumer count must be >= 1")
        self.name = name
        self.capacity = capacity
        self.consumers = consumers
        self._queue: asyncio.Queue[object] = asyncio.Queue(capacity)
        self._closed = False
        self._ends = 0
        self.high_water = 0
        self.puts = 0
        self.blocked_puts = 0
        self.blocked_seconds = 0.0
        self.waits = Histogram(WAIT_BUCKETS)

    def __len__(self) -> int:
        """Items waiting, not counting end markers."""
        return self._queue.qsize() - self._ends

    @property
    def free(self) -> int:
        return max(0, self.capacity - self._queue.qsize())

    async def put(self, item: T) -> None:
        if self._closed:
            raise RuntimeError(f"queue {self.name} is closed")
        entry = (time.perf_counter(), item)
        if self._queue.full():
            self.blocked_puts += 1
            started = time.perf_counter()
            await self._queue.put(entry)
            self.blocked_seconds += time.perf_counter() - started
        else:
            self._queue.put_nowait(entry)
        self.puts += 1
        self.high_water = max(self.high_water, self._queue.qsize())

    async def get(self) -> T | None:
        """The next item, or ``None`` once the queue is closed and drained (for this consumer)."""
        return self._unwrap(await self._queue.get())

    def _unwrap(self, entry: object) -> T | None:
        if entry is _END:
            self._ends -= 1
            return None
        queued_at, item = entry  # type: ignore[misc]
        self.waits.observe(time.perf_counter() - queued_at)
        return item  # type: ignore[no-any-return]

    def get_nowait(self) -> T | None:
        """The next item if one is waiting; raises :class:`asyncio.QueueEmpty` otherwise."""
        return self._unwrap(self._queue.get_nowait())

    async def close(self) -> None:
        """Signal end of input: every consumer gets ``None`` after the queued items."""
        if self._closed:
            return
        self._closed = True
        for _ in range(self.consumers):
            self._ends += 1
            await self._queue.put(_END)

    def stats(self) -> QueueStats:
        return QueueStats(self.name, self.capacity, len(self), self.high_water, self.puts, self.blocked_puts,
                          self.blocked_seconds)
