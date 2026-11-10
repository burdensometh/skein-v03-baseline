"""Run modes and the incremental-crawl settings of a spider."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum

from skein.incremental.fingerprint import Fingerprinter
from skein.incremental.revisit import RevisitPolicy


class RunMode(str, Enum):
    """Which known URLs a run revisits.

    ``full`` revisits every URL the spider knows; ``incremental`` only
    those due by their revisit schedule, plus sitemap entries whose
    ``lastmod`` is newer than what was stored; ``sitemap`` re-reads the
    sitemaps and fetches only pages they list as new or changed (plus seed
    pages never fetched), ignoring revisit schedules; ``auto`` is ``full``
    for a spider without a completed run and ``incremental`` afterwards.
    """

    FULL = "full"
    INCREMENTAL = "incremental"
    SITEMAP = "sitemap"
    AUTO = "auto"


@dataclass(frozen=True)
class RevisitRule:
    """A revisit policy for URLs matching ``pattern`` (searched, not anchored)."""

    pattern: re.Pattern[str]
    policy: RevisitPolicy


@dataclass(frozen=True)
class IncrementalSettings:
    mode: RunMode = RunMode.AUTO
    conditional_requests: bool = True
    use_sitemap_lastmod: bool = True
    revisit: RevisitPolicy = field(default_factory=RevisitPolicy)
    revisit_rules: tuple[RevisitRule, ...] = ()
    fingerprint: Fingerprinter = field(default_factory=Fingerprinter)

    def revisit_for(self, url: str) -> RevisitPolicy:
        """The policy of the first rule whose pattern matches ``url``, else the spider default."""
        for rule in self.revisit_rules:
            if rule.pattern.search(url):
                return rule.policy
        return self.revisit
