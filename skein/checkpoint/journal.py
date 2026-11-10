"""An append-only journal of JSON records split into segment files.

Segment ``journal-<first seq>.log`` starts with a 12-byte header (magic and
the sequence number of its first record) followed by framed records. Every
record carries a strictly increasing ``seq``. A new segment is started each
time the journal is opened and after each snapshot, so a torn record can
only ever sit at the end of a segment that is no longer written to; replay
truncates it away.

Durability is configurable: ``always`` fsyncs after every record, ``batch``
after ``batch_records`` records or ``batch_interval`` seconds (and on
close), ``never`` leaves flushing to the operating system. A killed
process loses nothing under any policy (the data is in the page cache); the
policy only matters for power loss.
"""

from __future__ import annotations

import json
import os
import re
import struct
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, BinaryIO, Callable

from skein.checkpoint.framing import frame, scan
from skein.checkpoint.snapshot import fsync_directory

SEGMENT_MAGIC = b"SKJ1"
SEGMENT_HEADER = struct.Struct("<4sQ")
_SEGMENT_RE = re.compile(r"^journal-(\d{16})\.log$")


class FsyncPolicy(str, Enum):
    ALWAYS = "always"
    BATCH = "batch"
    NEVER = "never"


class JournalError(RuntimeError):
    """The journal cannot be read or written."""


class JournalCorruptError(JournalError):
    """A record in the middle of a segment is damaged; replaying past it would lose ordering."""


@dataclass
class SegmentReport:
    path: Path
    first_seq: int
    records: int
    valid_bytes: int
    torn_bytes: int = 0
    reason: str | None = None


@dataclass
class Replay:
    """Records read back from the journal, oldest first."""

    records: list[dict[str, Any]] = field(default_factory=list)
    segments: list[SegmentReport] = field(default_factory=list)
    skipped: int = 0

    @property
    def torn_bytes(self) -> int:
        return sum(s.torn_bytes for s in self.segments)


def segment_name(first_seq: int) -> str:
    return f"journal-{first_seq:016d}.log"


class Journal:
    """Writer and reader of one journal directory."""

    def __init__(
        self,
        directory: Path,
        *,
        fsync: FsyncPolicy = FsyncPolicy.BATCH,
        batch_records: int = 64,
        batch_interval: float = 1.0,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        if batch_records < 1:
            raise ValueError("batch_records must be >= 1")
        self.directory = Path(directory)
        self.fsync = FsyncPolicy(fsync)
        self.batch_records = batch_records
        self.batch_interval = batch_interval
        self._now = now
        self._file: BinaryIO | None = None
        self._segment: Path | None = None
        self._unsynced = 0
        self._last_sync = now()
        self._last_seq = 0
        self.records_written = 0
        self.syncs = 0

    # -- reading -----------------------------------------------------------

    def segments(self) -> list[tuple[int, Path]]:
        if not self.directory.is_dir():
            return []
        found = []
        for path in self.directory.iterdir():
            match = _SEGMENT_RE.match(path.name)
            if match:
                found.append((int(match.group(1)), path))
        return sorted(found)

    def replay(self, after_seq: int = 0, *, repair: bool = True) -> Replay:
        """Read every record with ``seq > after_seq``.

        A torn final record of a segment is dropped (and, with ``repair``,
        cut off the file). Damage followed by further records raises
        :class:`JournalCorruptError`, as do sequence numbers that go
        backwards.
        """
        replay = Replay()
        previous = 0
        for first_seq, path in self.segments():
            report, payloads = self._read_segment(path, first_seq, repair)
            replay.segments.append(report)
            for payload in payloads:
                try:
                    record = json.loads(payload)
                    seq = int(record["seq"])
                except (ValueError, KeyError, TypeError) as exc:
                    raise JournalCorruptError(f"{path}: undecodable record: {exc}") from exc
                if seq <= previous:
                    raise JournalCorruptError(f"{path}: sequence number {seq} after {previous}")
                previous = seq
                if seq <= after_seq:
                    replay.skipped += 1
                    continue
                replay.records.append(record)
        return replay

    def _read_segment(self, path: Path, first_seq: int, repair: bool) -> tuple[SegmentReport, list[bytes]]:
        data = path.read_bytes()
        if len(data) < SEGMENT_HEADER.size:
            # Crashed while creating the segment: nothing in it was ever acknowledged.
            if repair:
                path.unlink(missing_ok=True)
            return SegmentReport(path, first_seq, 0, 0, len(data), "incomplete segment header"), []
        magic, header_seq = SEGMENT_HEADER.unpack_from(data, 0)
        if magic != SEGMENT_MAGIC or header_seq != first_seq:
            raise JournalCorruptError(f"{path}: bad segment header")
        result = scan(data, SEGMENT_HEADER.size)
        if result.corrupt:
            raise JournalCorruptError(f"{path}: {result.reason}")
        report = SegmentReport(path, first_seq, len(result.payloads), result.valid_end, len(data) - result.valid_end,
                               result.reason)
        if result.torn and repair:
            with open(path, "r+b") as handle:
                handle.truncate(result.valid_end)
                handle.flush()
                os.fsync(handle.fileno())
        return report, result.payloads

    # -- writing -------------------------------------------------------------

    def open(self, next_seq: int) -> None:
        """Start a new segment whose first record will have sequence number ``next_seq``."""
        self.close()
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / segment_name(next_seq)
        if path.exists():
            # A segment that never received a record (opened, then superseded by
            # a snapshot at the same position) can be replaced safely.
            report, payloads = self._read_segment(path, next_seq, repair=False)
            if payloads or report.torn_bytes:
                raise JournalError(f"segment {path} already exists and holds records")
            path.unlink()
        handle = open(path, "xb", buffering=0)
        handle.write(SEGMENT_HEADER.pack(SEGMENT_MAGIC, next_seq))
        os.fsync(handle.fileno())
        fsync_directory(self.directory)
        self._file, self._segment = handle, path
        self._last_seq = next_seq - 1
        self._unsynced = 0
        self._last_sync = self._now()

    def append(self, record: dict[str, Any]) -> None:
        if self._file is None:
            raise JournalError("journal is not open")
        seq = record.get("seq")
        if not isinstance(seq, int) or seq <= self._last_seq:
            raise JournalError(f"record sequence {seq!r} must be greater than {self._last_seq}")
        payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self._file.write(frame(payload))
        self._last_seq = seq
        self._unsynced += 1
        self.records_written += 1
        if self.fsync is FsyncPolicy.ALWAYS:
            self.sync()
        elif self.fsync is FsyncPolicy.BATCH and (
            self._unsynced >= self.batch_records or self._now() - self._last_sync >= self.batch_interval
        ):
            self.sync()

    def sync(self) -> None:
        if self._file is None or self._unsynced == 0:
            return
        os.fsync(self._file.fileno())
        self.syncs += 1
        self._unsynced = 0
        self._last_sync = self._now()

    def close(self) -> None:
        if self._file is None:
            return
        if self.fsync is not FsyncPolicy.NEVER:
            self.sync()
        self._file.close()
        self._file = None

    def prune(self, upto_seq: int) -> int:
        """Delete segments that only hold records with ``seq <= upto_seq``; returns the number removed."""
        segments = self.segments()
        removed = 0
        for (first, path), (next_first, _) in zip(segments, segments[1:]):
            if next_first - 1 <= upto_seq and path != self._segment:
                path.unlink(missing_ok=True)
                removed += 1
        return removed

    def remove_all(self) -> None:
        self.close()
        for _, path in self.segments():
            path.unlink(missing_ok=True)
