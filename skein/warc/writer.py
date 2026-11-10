"""Writing WARC files: one gzip member per record, size-based rollover, a warcinfo record per file.

Each record is compressed as its own gzip member, so any record can be
read by seeking to its offset and decompressing one member. A file is
closed and a new one started once it has grown past ``max_size`` bytes;
rollover only happens between exchanges, so a request, its response and
metadata always share a file. File names are
``<prefix>-<UTC timestamp>-<serial>-<worker>.warc.gz`` and every file
starts with a ``warcinfo`` record describing the crawl.

The writer also keeps the index line of every response and revisit
record it writes and leaves them next to the file as ``<file>.cdx`` when
the file is closed, so indexing a finished archive does not have to
re-read it.

The bytes of an exchange are appended to a WARC file inside the writer's
caller's database transaction, but a file on disk cannot take part in a
SQLite rollback. The writer therefore mirrors the transaction boundary:
:meth:`begin` opens a rollback window, :meth:`commit` closes it once the
database commit landed, and :meth:`rollback` cuts every gzip member the
abandoned transaction had appended back off the file. A record only
becomes durable together with the fetch event that points at it, which is
what keeps the archive and the database consistent when several worker
processes contend for the database.
"""

from __future__ import annotations

import gzip
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, BinaryIO

from skein import __version__
from skein.clock import Clock
from skein.checkpoint.snapshot import atomic_write
from skein.warc.cdx import CDX_HEADER, CdxLine, cdx_line, sidecar_path
from skein.warc.reader import WarcReader
from skein.warc.record import WarcRecord, make_record, warc_date

SOFTWARE = f"skein/{__version__}"


@dataclass(frozen=True)
class RecordLocation:
    """Where a record landed: file name (relative to the archive directory), member offset and length."""

    filename: str
    offset: int
    length: int
    record_id: str
    date: str


@dataclass
class WarcFile:
    name: str
    opened_at: float
    size: int = 0
    records: int = 0


@dataclass
class _PendingRegion:
    """A byte range appended to one WARC file by a transaction that has not committed yet."""

    name: str
    start: int
    end: int


class WarcWriter:
    """Appends records to rolling ``.warc.gz`` files in ``directory``.

    ``on_open``/``on_close`` are called with the :class:`WarcFile` when a
    file is started or finished (the crawler records files in the
    database). The writer is used from one thread; records are flushed to
    the operating system after every :meth:`write`.

    Every :meth:`write` belongs to the transaction window opened by
    :meth:`begin`; a window without an explicit :meth:`begin` commits on
    its own (the standalone, single-process usage). :meth:`rollback`
    truncates the uncommitted members, so a rolled-back database
    transaction leaves no orphan records behind.
    """

    def __init__(
        self,
        directory: Path,
        clock: Clock,
        *,
        prefix: str = "skein",
        worker: str = "local",
        max_size: int = 100 * 1024 * 1024,
        compresslevel: int = 6,
        info: dict[str, str] | None = None,
        on_open: Callable[[WarcFile], None] | None = None,
        on_close: Callable[[WarcFile], None] | None = None,
    ) -> None:
        if max_size < 1024:
            raise ValueError("max_size must be at least 1024 bytes")
        self.directory = Path(directory)
        self.clock = clock
        self.prefix = prefix
        self.worker = "".join(c if c.isalnum() or c in "-_." else "_" for c in worker)
        self.max_size = max_size
        self.compresslevel = compresslevel
        self.info = dict(info or {})
        self.on_open = on_open
        self.on_close = on_close
        self.current: WarcFile | None = None
        self._handle: BinaryIO | None = None
        self._serial = 0
        self._index: list[CdxLine] = []
        self.files_written: list[str] = []
        self._in_transaction = False
        self._pending: list[_PendingRegion] = []
        self._closed_in_transaction: list[WarcFile] = []

    def _filename(self) -> str:
        stamp = warc_date(self.clock.time())[:19].replace("-", "").replace(":", "").replace("T", "")
        while True:
            self._serial += 1
            name = f"{self.prefix}-{stamp}-{self._serial:05d}-{self.worker}.warc.gz"
            if not (self.directory / name).exists():
                return name

    def _open(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        name = self._filename()
        self._handle = open(self.directory / name, "ab")
        self.current = WarcFile(name, self.clock.time())
        self.files_written.append(name)
        if self.on_open is not None:
            self.on_open(self.current)
        fields = {"software": SOFTWARE, "format": "WARC File Format 1.1",
                  "conformsTo": "http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/",
                  "hostname": socket.gethostname(), **self.info}
        body = "".join(f"{key}: {value}\r\n" for key, value in fields.items()).encode("utf-8")
        info = make_record("warcinfo", body, date=warc_date(self.clock.time()), content_type="application/warc-fields",
                           extra=[("WARC-Filename", name)])
        self._append(info)

    def _append(self, record: WarcRecord) -> RecordLocation:
        assert self._handle is not None and self.current is not None
        member = gzip.compress(record.serialize(), compresslevel=self.compresslevel, mtime=0)
        offset = self.current.size
        self._handle.write(member)
        self.current.size += len(member)
        self.current.records += 1
        line = cdx_line(record, offset, len(member), self.current.name)
        if line is not None:
            self._index.append(line)
        if self._in_transaction:
            self._remember_pending(self.current.name, offset, self.current.size)
        return RecordLocation(self.current.name, offset, len(member), record.record_id, record.date)

    def _remember_pending(self, name: str, start: int, end: int) -> None:
        if self._pending and self._pending[-1].name == name and self._pending[-1].end == start:
            self._pending[-1].end = end
        else:
            self._pending.append(_PendingRegion(name, start, end))

    def write(self, records: list[WarcRecord]) -> list[RecordLocation]:
        """Append ``records`` (one exchange) to the current file, rolling over first if it is full."""
        implicit = not self._in_transaction
        if implicit:
            self.begin()
        try:
            if self.current is not None and self.current.size >= self.max_size:
                self.close()
            if self.current is None:
                self._open()
            locations = [self._append(record) for record in records]
            assert self._handle is not None
            self._handle.flush()
        except BaseException:
            if implicit:
                self.rollback()
            raise
        if implicit:
            self.commit()
        return locations

    def begin(self) -> None:
        """Open a rollback window covering every :meth:`write` until :meth:`commit` or :meth:`rollback`."""
        if self._in_transaction:
            raise RuntimeError("a WARC transaction is already open")
        self._in_transaction = True
        self._pending = []
        self._closed_in_transaction = []

    def commit(self) -> None:
        """Make the appended bytes permanent after the database transaction that references them committed."""
        if not self._in_transaction:
            return
        self.sync()
        closed = self._closed_in_transaction
        self._clear_transaction_state()
        for file in closed:
            if self.on_close is not None:
                self.on_close(file)

    def rollback(self) -> None:
        """Cut off every gzip member appended since :meth:`begin`; the database transaction must roll back too."""
        if not self._in_transaction:
            return
        try:
            if not self._pending:
                return
            created_names = {region.name for region in self._pending if region.start == 0}
            reopen_name: str | None = None
            for name in dict.fromkeys(region.name for region in self._pending):
                path = self.directory / name
                if name in created_names:
                    # The warcinfo went in with the failed transaction: nothing is worth keeping.
                    path.unlink(missing_ok=True)
                    sidecar_path(path).unlink(missing_ok=True)
                    self.files_written = [written for written in self.files_written if written != name]
                    continue
                restore_size = min(region.start for region in self._pending if region.name == name)
                with open(path, "r+b") as handle:
                    handle.truncate(restore_size)
                    handle.flush()
                    os.fsync(handle.fileno())
                sidecar_path(path).unlink(missing_ok=True)
            # Which file, if any, should be the active one again?
            active = self.current if self.current is not None and self.current.name not in created_names else None
            if active is not None:
                active.size = min(region.start for region in self._pending if region.name == active.name)
                active.records = self._records_on_disk(active.name)
                reopen_name = active.name
            elif self._closed_in_transaction:
                # A rollover inside the failed transaction closed a previously
                # committed file; that file becomes the active one again.
                reopen_name = self._closed_in_transaction[0].name
            # Always drop and reopen the handle: truncating a file a
            # BufferedWriter has cached bytes for leaves its stream position
            # unreliable, whereas O_APPEND resumes at the true end of file.
            if self._handle is not None:
                self._handle.close()
                self._handle = None
            self.current = None
            self._index = []
            if reopen_name is not None:
                self._reopen(WarcFile(reopen_name, self.clock.time()))
        finally:
            self._clear_transaction_state()

    def _records_on_disk(self, name: str) -> int:
        return sum(1 for _ in WarcReader(self.directory / name))

    def _reopen(self, file: WarcFile) -> None:
        path = self.directory / file.name
        self._handle = open(path, "ab")
        self.current = file
        self.current.size = path.stat().st_size
        self.current.records = self._records_on_disk(file.name)
        self._index = [cdx_line(entry.record, entry.offset, entry.length, file.name)
                       for entry in WarcReader(path)]
        self._index = [line for line in self._index if line is not None]

    def _clear_transaction_state(self) -> None:
        self._in_transaction = False
        self._pending = []
        self._closed_in_transaction = []

    def sync(self) -> None:
        """Flush and fsync the open file (called before the database commit that references it)."""
        if self._handle is not None:
            self._handle.flush()
            os.fsync(self._handle.fileno())

    def close(self) -> None:
        if self._handle is None or self.current is None:
            return
        self.sync()
        self._handle.close()
        finished = self.current
        index = CDX_HEADER + "\n" + "".join(line.to_line() + "\n" for line in sorted(self._index))
        atomic_write(sidecar_path(self.directory / finished.name), index.encode("utf-8"))
        self._index = []
        self._handle = None
        self.current = None
        if self._in_transaction:
            # The on_close database UPDATE must land with the commit, not before it.
            self._closed_in_transaction.append(finished)
        elif self.on_close is not None:
            self.on_close(finished)


@dataclass(frozen=True)
class SealReport:
    """What sealing a WARC file left behind by a crashed writer did."""

    records: int
    size: int
    torn_bytes: int


def seal(path: Path) -> SealReport:
    """Make a file a killed writer left open whole again.

    A torn final gzip member is cut off (the records before it are intact,
    each being its own member) and the sidecar CDX index the writer would
    have left on close is written.
    """
    reader = WarcReader(path)
    lines: list[CdxLine] = []
    records = 0
    end = 0
    for entry in reader:
        records += 1
        end = entry.offset + entry.length
        line = cdx_line(entry.record, entry.offset, entry.length, path.name)
        if line is not None:
            lines.append(line)
    torn = 0
    if reader.truncated is not None:
        torn = path.stat().st_size - end
        with open(path, "r+b") as handle:
            handle.truncate(end)
            handle.flush()
            os.fsync(handle.fileno())
    index = CDX_HEADER + "\n" + "".join(line.to_line() + "\n" for line in sorted(lines))
    atomic_write(sidecar_path(path), index.encode("utf-8"))
    return SealReport(records, end, torn)
