"""De-duplication: a persisted Bloom filter of seen URLs and SimHash near-duplicate detection."""

from skein.dedup.bloom import BloomFilter, optimal_parameters
from skein.dedup.bloomfile import BloomFileError, BloomHeader
from skein.dedup.seen import SeenUrls
from skein.dedup.simhash import SimHasher, hamming, simhash
from skein.dedup.simindex import NearDuplicateIndex, NearMatch

__all__ = [
    "BloomFileError",
    "BloomFilter",
    "BloomHeader",
    "NearDuplicateIndex",
    "NearMatch",
    "SeenUrls",
    "SimHasher",
    "hamming",
    "optimal_parameters",
    "simhash",
]
