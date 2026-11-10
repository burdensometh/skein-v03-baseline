"""SimHash fingerprints of page text for near-duplicate detection (Charikar, 2002).

A page's text is cut into overlapping word shingles; each shingle is hashed
to 64 bits and votes +1/-1 per bit position, weighted by how often it
occurs. The sign of each position's total gives one bit of the SimHash.
Pages whose shingle sets mostly overlap get hashes that differ in few
bits, so the Hamming distance between two SimHashes estimates how
different the texts are. Hashes are split into :data:`BANDS` bands of
:data:`BAND_BITS` bits for indexing: hashes within distance
``BANDS - 1`` necessarily agree on at least one whole band.
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from dataclasses import dataclass
from typing import Mapping

SIMHASH_BITS = 64
BANDS = 4
BAND_BITS = SIMHASH_BITS // BANDS
MAX_INDEXED_DISTANCE = BANDS - 1
_MASK = (1 << SIMHASH_BITS) - 1
_BAND_MASK = (1 << BAND_BITS) - 1
_WORD_RE = re.compile(r"\w+", re.UNICODE)


def feature_hash(feature: str) -> int:
    """A stable 64-bit hash of one feature (independent of ``PYTHONHASHSEED``)."""
    return int.from_bytes(hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest(), "big")


def shingles(text: str, size: int = 3) -> Counter[str]:
    """Word ``size``-grams of the lower-cased text, with multiplicities.

    Texts shorter than ``size`` words yield their words as single features.
    """
    words = [w.lower() for w in _WORD_RE.findall(text)]
    if len(words) < size:
        return Counter(words)
    return Counter(" ".join(words[i : i + size]) for i in range(len(words) - size + 1))


def simhash(features: Mapping[str, int]) -> int:
    """The 64-bit SimHash of weighted ``features`` (0 for no features)."""
    totals = [0] * SIMHASH_BITS
    for feature, weight in features.items():
        value = feature_hash(feature)
        for bit in range(SIMHASH_BITS):
            if value >> bit & 1:
                totals[bit] += weight
            else:
                totals[bit] -= weight
    result = 0
    for bit, total in enumerate(totals):
        if total > 0:
            result |= 1 << bit
    return result


def hamming(a: int, b: int) -> int:
    return ((a ^ b) & _MASK).bit_count()


def band_values(value: int) -> tuple[int, ...]:
    """The :data:`BANDS` band values of a 64-bit hash, lowest bits first."""
    return tuple((value >> (band * BAND_BITS)) & _BAND_MASK for band in range(BANDS))


def to_signed(value: int) -> int:
    """Map an unsigned 64-bit value onto SQLite's signed INTEGER range."""
    return value - (1 << SIMHASH_BITS) if value >= 1 << (SIMHASH_BITS - 1) else value


def to_unsigned(value: int) -> int:
    return value & _MASK


@dataclass(frozen=True)
class SimHasher:
    """Computes SimHashes of page text; texts with fewer than ``min_words`` words get none.

    Very short pages (error stubs, redirects with a body) would otherwise
    all look alike.
    """

    shingle_size: int = 3
    min_words: int = 8

    def __post_init__(self) -> None:
        if self.shingle_size < 1:
            raise ValueError("shingle_size must be >= 1")

    def of_text(self, text: str) -> int | None:
        if len(_WORD_RE.findall(text)) < self.min_words:
            return None
        return simhash(shingles(text, self.shingle_size))
