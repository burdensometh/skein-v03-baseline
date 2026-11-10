"""The persisted filter of URLs a spider has already queued.

Link extraction produces mostly URLs the frontier already holds. The
parse stage asks this filter first and drops links it reports as seen, so
they never cost a database round trip in the writer. A Bloom filter has
no false negatives, so a URL is only dropped if it was really added -- or,
with the configured (tiny) false-positive probability, by mistake; the
filter is sized so that this stays below ``error_rate``.

The filter file belongs to one database (its header carries the
database's identity) and one spider. It is rebuilt from the frontier when
it is missing, damaged, written for another database, or too small for
the frontier it has to describe. Several worker processes share the file:
:meth:`SeenUrls.save` takes an exclusive ``flock`` on a lock file, ORs the
bits of the file currently on disk into its own and replaces the file
atomically, so no worker's additions are lost.
"""

from __future__ import annotations

import contextlib
import fcntl
import logging
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from skein.dedup.bloom import BloomFilter
from skein.dedup.bloomfile import BloomFileError, load, save
from skein.store.schema_workers import database_id

log = logging.getLogger(__name__)

GROWTH_HEADROOM = 1.25


@dataclass
class SeenStats:
    loaded_from: str = "new"
    probes: int = 0
    maybe_seen: int = 0
    added: int = 0
    saves: int = 0


@contextlib.contextmanager
def file_lock(path: Path) -> Iterator[None]:
    """Hold an exclusive advisory lock on ``path`` (created if needed)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class SeenUrls:
    """Bloom filter of one spider's frontier URLs, optionally backed by ``path``."""

    def __init__(
        self,
        db: sqlite3.Connection,
        spider: str,
        path: Path | None,
        *,
        capacity: int = 200_000,
        error_rate: float = 1e-6,
    ) -> None:
        self.db = db
        self.spider = spider
        self.path = path
        self.capacity = capacity
        self.error_rate = error_rate
        self.stats = SeenStats()
        self.identity = bytes.fromhex(database_id(db))
        self.bloom = BloomFilter.for_capacity(capacity, error_rate)
        self._dirty = False

    @property
    def lock_path(self) -> Path:
        assert self.path is not None
        return self.path.with_name(self.path.name + ".lock")

    def open(self) -> str:
        """Load the filter file or rebuild it from the frontier; returns how it was obtained."""
        known = int(self.db.execute("SELECT COUNT(*) FROM frontier WHERE spider = ?", (self.spider,)).fetchone()[0])
        reason = "new" if self.path is None else self._try_load(known)
        if reason != "loaded":
            self.capacity = max(self.capacity, int(known * 2))
            self.bloom = BloomFilter.for_capacity(self.capacity, self.error_rate)
            rows = self.db.execute("SELECT url FROM frontier WHERE spider = ?", (self.spider,))
            self.bloom.update(row[0] for row in rows)
            self._dirty = True
            if self.path is not None:
                log.info("%s: rebuilt seen-URL filter from %d frontier URL(s) (%s)", self.spider, known, reason)
        self.stats.loaded_from = reason
        return reason

    def _try_load(self, known: int) -> str:
        assert self.path is not None
        try:
            header, bloom = load(self.path)
        except BloomFileError as exc:
            if self.path.exists():
                log.warning("%s: seen-URL filter %s unusable: %s", self.spider, self.path, exc)
                return f"corrupt: {exc}"
            return "missing"
        if header.database_id != self.identity:
            return "foreign database"
        if known * GROWTH_HEADROOM > header.capacity:
            return "grown"
        self.bloom = bloom
        self.capacity = header.capacity
        self.error_rate = header.error_rate
        return "loaded"

    def might_contain(self, url: str) -> bool:
        self.stats.probes += 1
        if url in self.bloom:
            self.stats.maybe_seen += 1
            return True
        return False

    def add(self, urls: Iterable[str]) -> int:
        added = self.bloom.update(urls)
        if added:
            self.stats.added += added
            self._dirty = True
        return added

    def save(self) -> bool:
        """Merge with the file on disk and replace it atomically; returns whether anything was written."""
        if self.path is None or not self._dirty:
            return False
        with file_lock(self.lock_path):
            try:
                header, on_disk = load(self.path)
            except BloomFileError:
                on_disk = None
            else:
                if header.database_id != self.identity or not on_disk.compatible(self.bloom):
                    on_disk = None
            if on_disk is not None:
                self.bloom.union_update(on_disk)
            save(self.path, self.bloom, capacity=self.capacity, error_rate=self.error_rate,
                 database_id=self.identity)
        self._dirty = False
        self.stats.saves += 1
        return True
