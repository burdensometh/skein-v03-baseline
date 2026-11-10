"""Reading WARC files: sequential iteration, random access by offset, damage tolerance.

Gzipped files are read member by member; each record's offset and
compressed length are reported, which is what a CDX index stores.
Uncompressed ``.warc`` files are read record by record. A final member (or
record) cut short -- the writer was killed mid-write -- ends iteration and
is reported in :attr:`WarcReader.truncated` instead of raising; damage
before the end raises :class:`~skein.warc.record.WarcFormatError`.
"""

from __future__ import annotations

import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator

from skein.warc.record import WarcFormatError, WarcRecord, parse_record

CHUNK = 64 * 1024
GZIP_MAGIC = b"\x1f\x8b"
MAX_RECORD_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True)
class Truncation:
    """An incomplete final member/record: where it starts and how many bytes are there."""

    offset: int
    available: int
    reason: str


@dataclass(frozen=True)
class Entry:
    offset: int
    length: int
    record: WarcRecord


def _inflate_member(handle: BinaryIO, pending: bytes) -> tuple[bytes, int, bytes, bool]:
    """Decompress one gzip member starting with ``pending``.

    Returns ``(data, consumed_bytes, leftover, complete)``.
    """
    inflater = zlib.decompressobj(zlib.MAX_WBITS | 16)
    pieces: list[bytes] = []
    consumed = 0
    produced = 0
    data = pending
    while True:
        try:
            out = inflater.decompress(data)
        except zlib.error as exc:
            raise WarcFormatError(f"corrupt gzip member: {exc}") from exc
        produced += len(out)
        if produced > MAX_RECORD_BYTES:
            raise WarcFormatError("record exceeds the size limit")
        pieces.append(out)
        if inflater.eof:
            leftover = inflater.unused_data
            consumed += len(data) - len(leftover)
            return b"".join(pieces), consumed, leftover, True
        consumed += len(data)
        data = handle.read(CHUNK)
        if not data:
            return b"".join(pieces), consumed, b"", False


class WarcReader:
    """Iterates over the records of one WARC file (gzip per record, or uncompressed)."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.truncated: Truncation | None = None

    def __iter__(self) -> Iterator[Entry]:
        with open(self.path, "rb") as handle:
            magic = handle.read(2)
            handle.seek(0)
            if magic == GZIP_MAGIC:
                yield from self._gzip_entries(handle)
            else:
                yield from self._plain_entries(handle)

    def _gzip_entries(self, handle: BinaryIO) -> Iterator[Entry]:
        offset = 0
        pending = b""
        while True:
            if not pending:
                pending = handle.read(CHUNK)
                if not pending:
                    return
            if not pending.startswith(GZIP_MAGIC[: len(pending)]):
                raise WarcFormatError(f"{self.path}: no gzip member at offset {offset}")
            data, consumed, pending, complete = _inflate_member(handle, pending)
            if not complete:
                self.truncated = Truncation(offset, consumed, "gzip member ends early")
                return
            try:
                record, _ = parse_record(data, 0)
            except WarcFormatError as exc:
                raise WarcFormatError(f"{self.path} at offset {offset}: {exc}") from exc
            yield Entry(offset, consumed, record)
            offset += consumed

    def _plain_entries(self, handle: BinaryIO) -> Iterator[Entry]:
        data = handle.read()
        position = 0
        while position < len(data):
            if not data[position:].strip(b"\r\n"):
                return
            try:
                record, end = parse_record(data, position)
            except WarcFormatError as exc:
                if "truncated" in str(exc) or "not terminated" in str(exc):
                    self.truncated = Truncation(position, len(data) - position, str(exc))
                    return
                raise WarcFormatError(f"{self.path} at offset {position}: {exc}") from exc
            yield Entry(position, end - position, record)
            position = end


def read_record(path: Path, offset: int) -> WarcRecord:
    """The record whose gzip member (or plain record) starts at ``offset``."""
    with open(path, "rb") as handle:
        handle.seek(offset)
        head = handle.read(CHUNK)
        if head.startswith(GZIP_MAGIC):
            data, _, _, complete = _inflate_member(handle, head)
            if not complete:
                raise WarcFormatError(f"{path}: gzip member at offset {offset} is truncated")
            return parse_record(data, 0)[0]
        rest = head + handle.read()
    return parse_record(rest, 0)[0]
