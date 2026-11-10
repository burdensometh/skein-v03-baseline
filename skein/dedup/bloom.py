"""A Bloom filter over strings with Kirsch-Mitzenmacher double hashing.

``k`` bit positions per item are derived from one 128-bit BLAKE2b digest
split into two 64-bit halves ``h1``, ``h2``: position ``i`` is
``(h1 + i * h2) mod m``. Membership tests have no false negatives; the
false-positive rate after ``n`` insertions is about
``(1 - e^(-k n / m))^k``.
"""

from __future__ import annotations

import hashlib
import math
from typing import Iterable, Iterator

HASH_SCHEME_BLAKE2B_DOUBLE = 1
HASH_SCHEMES = {HASH_SCHEME_BLAKE2B_DOUBLE: "blake2b-128 double hashing"}


def optimal_parameters(capacity: int, error_rate: float) -> tuple[int, int]:
    """Bit count ``m`` and hash count ``k`` for ``capacity`` items at ``error_rate`` false positives."""
    if capacity < 1:
        raise ValueError("capacity must be >= 1")
    if not 0.0 < error_rate < 1.0:
        raise ValueError("error_rate must be between 0 and 1")
    bits = math.ceil(-capacity * math.log(error_rate) / (math.log(2) ** 2))
    bits = max(64, (bits + 7) // 8 * 8)
    hashes = max(1, round(bits / capacity * math.log(2)))
    return bits, hashes


class BloomFilter:
    """Fixed-size Bloom filter; ``count`` is the number of insertions that set at least one new bit."""

    def __init__(self, bits: int, hashes: int, *, data: bytes | bytearray | None = None, count: int = 0,
                 scheme: int = HASH_SCHEME_BLAKE2B_DOUBLE) -> None:
        if bits < 8 or bits % 8:
            raise ValueError("bit count must be a positive multiple of 8")
        if hashes < 1:
            raise ValueError("hash count must be >= 1")
        if scheme not in HASH_SCHEMES:
            raise ValueError(f"unknown hash scheme {scheme}")
        self.bits = bits
        self.hashes = hashes
        self.scheme = scheme
        self.count = count
        if data is None:
            self.data = bytearray(bits // 8)
        else:
            if len(data) != bits // 8:
                raise ValueError(f"expected {bits // 8} bytes of filter data, got {len(data)}")
            self.data = bytearray(data)

    @classmethod
    def for_capacity(cls, capacity: int, error_rate: float) -> "BloomFilter":
        bits, hashes = optimal_parameters(capacity, error_rate)
        return cls(bits, hashes)

    def _positions(self, item: str) -> Iterator[int]:
        digest = hashlib.blake2b(item.encode("utf-8"), digest_size=16).digest()
        first = int.from_bytes(digest[:8], "little")
        second = int.from_bytes(digest[8:], "little") | 1
        for index in range(self.hashes):
            yield (first + index * second) % self.bits

    def add(self, item: str) -> bool:
        """Insert ``item``; returns ``True`` if it was certainly not present before."""
        fresh = False
        data = self.data
        for position in self._positions(item):
            byte, mask = position >> 3, 1 << (position & 7)
            if not data[byte] & mask:
                data[byte] |= mask
                fresh = True
        if fresh:
            self.count += 1
        return fresh

    def update(self, items: Iterable[str]) -> int:
        return sum(1 for item in items if self.add(item))

    def __contains__(self, item: object) -> bool:
        if not isinstance(item, str):
            return False
        data = self.data
        return all(data[p >> 3] & (1 << (p & 7)) for p in self._positions(item))

    def compatible(self, other: "BloomFilter") -> bool:
        return (self.bits, self.hashes, self.scheme) == (other.bits, other.hashes, other.scheme)

    def union_update(self, other: "BloomFilter") -> None:
        """OR ``other``'s bits into this filter (both must have identical parameters)."""
        if not self.compatible(other):
            raise ValueError("cannot merge Bloom filters with different parameters")
        merged = int.from_bytes(self.data, "little") | int.from_bytes(other.data, "little")
        self.data = bytearray(merged.to_bytes(len(self.data), "little"))
        self.count = self.estimated_count()

    def bits_set(self) -> int:
        return int.from_bytes(self.data, "little").bit_count()

    def estimated_count(self) -> int:
        """Item count estimated from the fill (Swamidass & Baldi), used after merging filters."""
        filled = self.bits_set()
        if filled >= self.bits:
            return self.count
        return round(-self.bits / self.hashes * math.log(1.0 - filled / self.bits))

    def false_positive_rate(self) -> float:
        """Current probability that an absent item tests as present."""
        return (self.bits_set() / self.bits) ** self.hashes
