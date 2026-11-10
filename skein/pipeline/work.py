"""What flows between the pipeline stages.

``Job`` (dispatcher -> fetch stage): a leased frontier request with its
host slot. ``Fetched`` (fetch -> parse stage): the response and the fetch
event describing it. ``Outcome`` (fetch or parse -> writer): everything the
writer commits for one request in the batch transaction -- the fetch event,
items, discovered links, the frontier state change and the statistics
delta for the journal.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from skein.extract.extractor import ExtractedItem
from skein.frontier.request import CrawlRequest
from skein.frontier.sqlite import Claim
from skein.net.response import Response
from skein.store.documents import FetchEvent
from skein.warc.http import Exchange

Tally = dict[str, Any]


def bump(tally: Tally, key: str, amount: int = 1) -> None:
    tally[key] = tally.get(key, 0) + amount


class Settle(str, Enum):
    """The frontier state change an outcome makes (applied with the lease compare-and-set)."""

    DONE = "done"
    FAILED = "failed"
    RETRY = "retry"
    DEFER = "defer"
    REQUEUE = "requeue"


@dataclass
class Job:
    """A claimed request on its way to the fetch stage."""

    claim: Claim

    @property
    def request(self) -> CrawlRequest:
        return self.claim.request


@dataclass
class Fetched:
    """A response that needs parsing; ``event`` already carries status, validators and timings."""

    request: CrawlRequest
    response: Response
    event: FetchEvent
    tally: Tally
    exchange: Exchange | None = None
    extra_children: list[CrawlRequest] = field(default_factory=list)
    robots_origin: str | None = None


@dataclass
class Outcome:
    """One request's results, committed by the writer in a batch transaction.

    ``sitemap_entries`` are URLs listed in a sitemap: the writer decides
    whether to add, promote or skip each (that needs the database).
    ``robots_origin`` names an origin whose robots.txt sitemaps are queued
    with this outcome (recorded in the worker's journal). ``archive_only``
    holds exchanges archived without a fetch event (robots.txt responses).
    """

    request: CrawlRequest
    tally: Tally
    settle: Settle = Settle.DONE
    delay: float = 0.0
    event: FetchEvent | None = None
    items: list[ExtractedItem] = field(default_factory=list)
    children: list[CrawlRequest] = field(default_factory=list)
    sitemap_entries: list[CrawlRequest] = field(default_factory=list)
    outlinks: list[str] | None = None
    complete_types: tuple[str, ...] = ()
    page_url: str | None = None
    lastmod: float | None = None
    root: str | None = None
    simhash: int | None = None
    exchange: Exchange | None = None
    robots_origin: str | None = None
    archive_only: list[Exchange] = field(default_factory=list)
