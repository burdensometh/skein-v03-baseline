"""Incremental crawling: run modes, conditional requests, fingerprints, revisit scheduling, watermarks."""

from skein.incremental.conditional import Validators, conditional_headers, normalize_etag, validators_from
from skein.incremental.fingerprint import Fingerprinter
from skein.incremental.modes import IncrementalSettings, RevisitRule, RunMode
from skein.incremental.revisit import RevisitPolicy

__all__ = [
    "Fingerprinter",
    "IncrementalSettings",
    "RevisitPolicy",
    "RevisitRule",
    "RunMode",
    "Validators",
    "conditional_headers",
    "normalize_etag",
    "validators_from",
]
