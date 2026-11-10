"""The parse stage: from a response to items, links, sitemap entries and a SimHash.

HTML parsing, extraction and link discovery are CPU-bound; with
``pipeline.parse_threads`` they run in a thread pool (they touch no shared
state), everything else here runs on the event loop. Links that the
seen-URL Bloom filter already knows are dropped here, before they cost the
writer a frontier insert.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from concurrent.futures import Executor

from skein.pipeline.analysis import PageAnalysis, analyse_page, analyse_sitemap
from skein.frontier.request import CrawlRequest, RequestKind
from skein.incremental.planner import SITEMAP_PRIORITY
from skein.pipeline.context import CrawlContext
from skein.pipeline.fetching import internal_failure
from skein.pipeline.queues import StageQueue
from skein.pipeline.work import Fetched, Outcome, Settle, bump
from skein.retry.classify import ErrorClass
from skein.urls.parse import URLError, resolve

log = logging.getLogger(__name__)


class ParseStage:
    """Turns :class:`Fetched` responses into :class:`Outcome` records for the writer."""

    def __init__(self, ctx: CrawlContext, written: StageQueue[Outcome], executor: Executor | None = None) -> None:
        self.ctx = ctx
        self.written = written
        self.executor = executor
        self.busy = 0
        self.processed = 0
        self._positives = 0

    async def run(self, inbox: StageQueue[Fetched]) -> None:
        while True:
            fetched = await inbox.get()
            if fetched is None:
                return
            self.busy += 1
            try:
                outcome = await self.process(fetched)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one bad page must not stop the crawl
                log.exception("unexpected error while parsing %s", fetched.request.url)
                outcome = internal_failure(self.ctx, fetched.request, "parse")
            finally:
                self.busy -= 1
            self.processed += 1
            outcome.exchange = outcome.exchange or fetched.exchange
            if fetched.robots_origin is not None:
                outcome.children.extend(fetched.extra_children)
                outcome.robots_origin = fetched.robots_origin
            await self.written.put(outcome)

    async def process(self, fetched: Fetched) -> Outcome:
        response = fetched.response
        if response.is_redirect:
            return self._redirect(fetched)
        if response.status == 304:
            bump(fetched.tally, "not_modified")
            return Outcome(fetched.request, fetched.tally, event=fetched.event)
        if fetched.request.kind is RequestKind.SITEMAP:
            return self._sitemap(fetched)
        return await self._page(fetched)

    def _redirect(self, fetched: Fetched) -> Outcome:
        """Queue an off-origin (or robots-unknown) redirect target as its own request."""
        ctx, request, response = self.ctx, fetched.request, fetched.response
        children = []
        try:
            target = ctx.spider.canonicalizer.key(resolve(response.url, response.location or ""))
        except URLError:
            target = None
        if target is not None and ctx.links.scope.allows(target):
            children.append(CrawlRequest(target, request.depth, request.priority, request.kind,
                                         referrer=request.url, seed=request.seed))
            if not ctx.frontier.seen(target):
                bump(fetched.tally, "redirects_deferred")
        return Outcome(request, fetched.tally, event=fetched.event, children=children)

    def _sitemap(self, fetched: Fetched) -> Outcome:
        ctx, request, response, event, tally = (self.ctx, fetched.request, fetched.response, fetched.event,
                                                fetched.tally)
        if not response.ok:
            event.error = f"sitemap HTTP {response.status}"
            return Outcome(request, tally, event=event)
        analysis = analyse_sitemap(response, ctx.spider.incremental.fingerprint)
        event.fingerprint = analysis.fingerprint
        if analysis.sitemap is None:
            tally["error"] = "SitemapError"
            event.error, event.error_class = analysis.error, ErrorClass.PERMANENT.value
            return Outcome(request, tally, Settle.FAILED, event=event)
        bump(tally, "sitemaps_fetched")
        outcome = Outcome(request, tally, event=event, outlinks=[], root=request.seed or request.url)
        sitemap = analysis.sitemap
        for entry in sitemap.entries:
            try:
                url = ctx.spider.canonicalizer.key(entry.loc)
            except URLError:
                continue
            if not ctx.links.scope.allows(url):
                continue
            outcome.outlinks.append(url)  # type: ignore[union-attr]
            if sitemap.is_index:
                child = CrawlRequest(url, request.depth, SITEMAP_PRIORITY, RequestKind.SITEMAP, referrer=request.url,
                                     sitemap_lastmod=entry.lastmod, seed=request.seed)
            else:
                priority = round((entry.priority or 0.5) * 10)
                child = CrawlRequest(url, request.depth, priority, RequestKind.PAGE, referrer=request.url,
                                     sitemap_lastmod=entry.lastmod, seed=request.seed, changefreq=entry.changefreq)
                bump(tally, "sitemap_urls")
            if entry.lastmod is not None:
                outcome.lastmod = max(outcome.lastmod or entry.lastmod, entry.lastmod)
            outcome.sitemap_entries.append(child)
        return outcome

    async def _analyse(self, fetched: Fetched, final_url: str) -> PageAnalysis:
        ctx = self.ctx
        work = functools.partial(
            analyse_page, fetched.response, final_url, fetched.request.depth,
            extractor=ctx.extractor, links=ctx.links, fingerprinter=ctx.spider.incremental.fingerprint,
            user_agent=ctx.user_agent, obey_noindex=ctx.spider.obey_noindex, simhasher=ctx.simhasher,
        )
        if self.executor is None:
            return work()
        return await asyncio.get_running_loop().run_in_executor(self.executor, work)

    async def _page(self, fetched: Fetched) -> Outcome:
        ctx, request, response, event, tally = (self.ctx, fetched.request, fetched.response, fetched.event,
                                                fetched.tally)
        final_url = self._canonical_final(request, response.url, bool(response.history))
        if not response.ok:
            event.error = f"HTTP {response.status}"
            return Outcome(request, tally, event=event)
        analysis = await self._analyse(fetched, final_url)
        event.fingerprint = analysis.fingerprint
        if not analysis.html:
            bump(tally, "non_html")
        bump(tally, "items_dropped", analysis.dropped)
        bump(tally, "extraction_errors", analysis.extraction_errors)
        bump(tally, "links_found", len(analysis.links))
        event.items_found = len(analysis.items)
        children = []
        for link in analysis.links:
            if ctx.seen is not None and ctx.seen.might_contain(link.url):
                if not self._false_positive(link.url):
                    bump(tally, "links_filtered")
                    continue
                bump(tally, "bloom_false_positives")
            if link.kind == "sitemap":
                children.append(request.child(link.url, priority=link.priority + SITEMAP_PRIORITY,
                                              kind=RequestKind.SITEMAP))
            else:
                children.append(request.child(link.url, priority=link.priority))
        return Outcome(request, tally, event=event, items=analysis.items, children=children,
                       complete_types=analysis.item_types, page_url=final_url,
                       outlinks=[link.url for link in analysis.links], simhash=analysis.simhash)

    def _false_positive(self, url: str) -> bool:
        """Check every ``verify_every``-th filter hit against the frontier; a miss is a false positive.

        The URL is then let through (and counted), so the filter's error rate
        is measured on real traffic and its mistakes cost nothing for the
        URLs that are checked.
        """
        every = self.ctx.runtime.dedup.bloom_verify_every
        if not every:
            return False
        self._positives += 1
        if self._positives % every:
            return False
        return not self.ctx.frontier.seen(url)

    def _canonical_final(self, request: CrawlRequest, url: str, redirected: bool) -> str:
        if not redirected:
            return request.url
        try:
            return self.ctx.spider.canonicalizer.key(url)
        except URLError:
            return request.url
