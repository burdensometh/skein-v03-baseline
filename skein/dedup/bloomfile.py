"""The on-disk Bloom filter format.

A file is a fixed 72-byte header followed by the filter's bit array::

    offset size  field
         0    4  magic "SKBF"
         4    2  format version (1)
         6    2  hash scheme (1 = BLAKE2b-128 double hashing)
         8    4  k, hashes per item
        12    8  m, bits in the filter (multiple of 8)
        20    8  count, insertions recorded
        28    8  capacity the filter was sized for
        36    8  target false-positive rate (IEEE double)
        44   16  identity of the database the filter describes
        60    4  CRC-32 of the bit array
        64    4  reserved (zero)
        68    4  CRC-32 of bytes 0..67

All integers are little-endian. Files are replaced atomically (temporary
file, fsync, rename, directory fsync), so a reader sees either the old or
the new filter. Any mismatch -- magic, version, header or payload
checksum, payload length -- raises :class:`BloomFileError`.
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

from skein.checkpoint.snapshot import atomic_write
from skein.dedup.bloom import HASH_SCHEMES, BloomFilter

MAGIC = b"SKBF"
FORMAT_VERSION = 1
_HEADER = struct.Struct("<4sHHIQQQd16sII")
_HEADER_CRC = struct.Struct("<I")
HEADER_SIZE = _HEADER.size + _HEADER_CRC.size


class BloomFileError(ValueError):
    """A Bloom filter file is missing, damaged or of an unsupported format."""


@dataclass(frozen=True)
class BloomHeader:
    version: int
    scheme: int
    hashes: int
    bits: int
    count: int
    capacity: int
    error_rate: float
    database_id: bytes
    payload_crc: int


def encode(bloom: BloomFilter, *, capacity: int, error_rate: float, database_id: bytes) -> bytes:
    if len(database_id) != 16:
        raise ValueError("database_id must be 16 bytes")
    payload = bytes(bloom.data)
    header = _HEADER.pack(MAGIC, FORMAT_VERSION, bloom.scheme, bloom.hashes, bloom.bits, bloom.count, capacity,
                          error_rate, database_id, zlib.crc32(payload), 0)
    return header + _HEADER_CRC.pack(zlib.crc32(header)) + payload


def read_header(raw: bytes) -> BloomHeader:
    if len(raw) < HEADER_SIZE:
        raise BloomFileError(f"file too short for a header ({len(raw)} bytes)")
    fields = _HEADER.unpack_from(raw)
    (stored_crc,) = _HEADER_CRC.unpack_from(raw, _HEADER.size)
    magic, version, scheme, hashes, bits, count, capacity, error_rate, database_id, payload_crc, _ = fields
    if magic != MAGIC:
        raise BloomFileError(f"not a skein Bloom filter (magic {magic!r})")
    if zlib.crc32(raw[: _HEADER.size]) != stored_crc:
        raise BloomFileError("header checksum mismatch")
    if version != FORMAT_VERSION:
        raise BloomFileError(f"unsupported Bloom filter format version {version}")
    if scheme not in HASH_SCHEMES:
        raise BloomFileError(f"unknown hash scheme {scheme}")
    if bits % 8 or bits == 0 or hashes == 0:
        raise BloomFileError(f"invalid filter geometry m={bits} k={hashes}")
    return BloomHeader(version, scheme, hashes, bits, count, capacity, error_rate, database_id, payload_crc)


def decode(raw: bytes) -> tuple[BloomHeader, BloomFilter]:
    header = read_header(raw)
    payload = raw[HEADER_SIZE:]
    if len(payload) != header.bits // 8:
        raise BloomFileError(f"payload is {len(payload)} bytes, header promises {header.bits // 8}")
    if zlib.crc32(payload) != header.payload_crc:
        raise BloomFileError("payload checksum mismatch")
    return header, BloomFilter(header.bits, header.hashes, data=payload, count=header.count, scheme=header.scheme)


def load(path: Path) -> tuple[BloomHeader, BloomFilter]:
    try:
        raw = Path(path).read_bytes()
    except FileNotFoundError as exc:
        raise BloomFileError(f"{path} does not exist") from exc
    except OSError as exc:
        raise BloomFileError(f"cannot read {path}: {exc}") from exc
    return decode(raw)


def save(path: Path, bloom: BloomFilter, *, capacity: int, error_rate: float, database_id: bytes) -> None:
    atomic_write(Path(path), encode(bloom, capacity=capacity, error_rate=error_rate, database_id=database_id))
