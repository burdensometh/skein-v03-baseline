"""The components one worker's pipeline stages share."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

from skein.checkpoint.session import Checkpoint, SessionState
from skein.clock import Clock
from skein.config.model import SpiderConfig
from skein.config.runtime import RuntimeSettings
from skein.dedup.seen import SeenUrls
from skein.dedup.simhash import SimHasher
from skein.dedup.simindex import NearDuplicateIndex
from skein.extract.extractor import Extractor
from skein.extract.links import LinkExtractor
from skein.frontier.request import CrawlRequest
from skein.frontier.sqlite import Frontier
from skein.net.client import HttpClient
from skein.politeness.scheduler import HostScheduler
from skein.robots.cache import RobotsCache
from skein.store.documents import FetchEvent
from skein.store.sqlite import Store
from skein.warc.writer import WarcWriter


@dataclass
class CrawlContext:
    """Everything a stage needs: configuration, shared state stores and this worker's identity.

    All database access happens on the event loop thread (stages run as
    tasks of one loop; only CPU-bound parsing may move to a thread pool and
    it never touches the database).
    """

    spider: SpiderConfig
    store: Store
    clock: Clock
    client: HttpClient
    frontier: Frontier
    scheduler: HostScheduler
    robots: RobotsCache
    extractor: Extractor
    links: LinkExtractor
    checkpoint: Checkpoint
    runtime: RuntimeSettings
    worker_id: str
    rng: random.Random
    seen: SeenUrls | None = None
    near: NearDuplicateIndex | None = None
    simhasher: SimHasher | None = None
    archive: WarcWriter | None = None
    session: SessionState | None = field(default=None, repr=False)

    @property
    def user_agent(self) -> str:
        return self.client.config.user_agent

    @property
    def run_id(self) -> int:
        return self.session.run_id if self.session is not None else 0

    def new_event(self, request: CrawlRequest, **values: Any) -> FetchEvent:
        """A fetch event for ``request`` stamped with the current time and this worker."""
        return FetchEvent(
            spider=self.spider.name,
            url=request.url,
            fetched_at=self.clock.time(),
            kind=request.kind.value,
            depth=request.depth,
            attempt=request.attempts,
            sitemap_lastmod=request.sitemap_lastmod,
            changefreq=request.changefreq,
            worker_id=self.worker_id,
            **values,
        )
