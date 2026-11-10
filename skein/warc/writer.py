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


class WarcWriter:
    """Appends records to rolling ``.warc.gz`` files in ``directory``.

    ``on_open``/``on_close`` are called with the :class:`WarcFile` when a
    file is started or finished (the crawler records files in the
    database). The writer is used from one thread; records are flushed to
    the operating system after every :meth:`write`.
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
        return RecordLocation(self.current.name, offset, len(member), record.record_id, record.date)

    def write(self, records: list[WarcRecord]) -> list[RecordLocation]:
        """Append ``records`` (one exchange) to the current file, rolling over first if it is full."""
        if self.current is not None and self.current.size >= self.max_size:
            self.close()
        if self.current is None:
            self._open()
        locations = [self._append(record) for record in records]
        assert self._handle is not None
        self._handle.flush()
        return locations

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
        if self.on_close is not None:
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
