"""Length- and CRC-framed binary records, shared by the journal and snapshots.

Each record is ``<u32 payload length><u32 CRC-32 of payload><payload>``
(little endian). Records are only ever appended, so a crash can leave at
most one incomplete record, and only at the very end. :func:`scan` tells
that *torn tail* (tolerated, the record was never acknowledged) apart from
damage in the middle of the data (reported as corruption).
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass, field

RECORD_HEADER = struct.Struct("<II")
MAX_RECORD_BYTES = 64 * 1024 * 1024


def frame(payload: bytes) -> bytes:
    if not payload:
        raise ValueError("records must not be empty")
    if len(payload) > MAX_RECORD_BYTES:
        raise ValueError(f"record of {len(payload)} bytes exceeds {MAX_RECORD_BYTES}")
    return RECORD_HEADER.pack(len(payload), zlib.crc32(payload)) + payload


@dataclass
class ScanResult:
    """Outcome of reading framed records from a buffer.

    ``valid_end`` is the offset just past the last intact record. When
    ``torn`` is set, the bytes from there on are an incomplete final record;
    when ``corrupt`` is set they contain a damaged record followed by more
    data. ``reason`` describes either case.
    """

    payloads: list[bytes] = field(default_factory=list)
    valid_end: int = 0
    torn: bool = False
    corrupt: bool = False
    reason: str | None = None


def scan(data: bytes, start: int = 0) -> ScanResult:
    """Read consecutive records from ``data[start:]``."""
    result = ScanResult(valid_end=start)
    offset = start
    size = len(data)
    while offset < size:
        remaining = size - offset
        if remaining < RECORD_HEADER.size:
            return _stop(result, offset, torn=True, reason=f"truncated record header ({remaining} bytes)")
        length, crc = RECORD_HEADER.unpack_from(data, offset)
        if length == 0:
            # Zero-filled blocks appear when a file was extended but never written.
            zero_tail = not any(data[offset:])
            return _stop(result, offset, torn=zero_tail, reason="zero-filled tail" if zero_tail else "empty record")
        end = offset + RECORD_HEADER.size + length
        if length > MAX_RECORD_BYTES or end > size:
            return _stop(result, offset, torn=True, reason=f"truncated record ({length} bytes announced, "
                                                           f"{remaining - RECORD_HEADER.size} present)")
        payload = data[offset + RECORD_HEADER.size : end]
        if zlib.crc32(payload) != crc:
            at_tail = end == size
            return _stop(result, offset, torn=at_tail, reason=f"checksum mismatch at offset {offset}")
        result.payloads.append(payload)
        offset = end
        result.valid_end = offset
    return result


def _stop(result: ScanResult, offset: int, *, torn: bool, reason: str) -> ScanResult:
    result.valid_end = offset
    result.torn = torn
    result.corrupt = not torn
    result.reason = reason
    return result
