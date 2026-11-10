"""The write stage: outcomes committed in batched SQLite transactions.

The writer takes whatever outcomes are waiting (up to ``write_batch``) and
commits them in one transaction: fetch events, document states, SimHash
index rows, WARC record locations, discovered links, the link graph, the
item history of every page (one batched :meth:`~skein.store.items.ItemStore.write`)
and the frontier state changes. Under load batches fill up and the cost
of a commit is shared; when the pipeline is quiet every outcome is
committed as soon as it arrives.

Inside the same transaction one ``batch`` record with the statistics
deltas of all outcomes is appended to this worker's journal, and its
sequence number is stored on the batch's fetch events: after a crash,
replay applies the record only if the transaction committed. Should a
batch fail, its outcomes are retried one by one so that a single bad
outcome cannot hold up the others; one that fails on its own is recorded
as an internal error.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from typing import Callable

from skein.frontier.sqlite import AddOutcome
from skein.incremental.modes import RunMode
from skein.incremental.planner import SitemapDecision, sitemap_decision
from skein.metrics.stats import Histogram
from skein.pipeline.context import CrawlContext
from skein.pipeline.fetching import internal_failure
from skein.pipeline.queues import StageQueue
from skein.pipeline.work import Outcome, Settle, Tally, bump
from skein.store.database import is_busy
from skein.store.documents import FetchEvent
from skein.store.items import ItemWrite, PageItems
from skein.store.sqlite import page_items
from skein.warc.http import RevisitOf, exchange_records, revisit_profile
from skein.warc.record import sha1_digest
from skein.warc.writer import SOFTWARE

log = logging.getLogger(__name__)

PROGRESS_EVERY = 100
BATCH_BUCKETS = (1, 2, 4, 8, 16, 32, 64, 128, 256)
COMMIT_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
MAX_METADATA_OUTLINKS = 500
BUSY_RETRIES = 8


class BatchWriter:
    """Consumes outcomes and commits them in batches; ``on_committed(n)`` follows every commit."""

    def __init__(self, ctx: CrawlContext, on_committed: Callable[[list[Outcome]], None], *, batch_size: int) -> None:
        self.ctx = ctx
        self.on_committed = on_committed
        self.batch_size = batch_size
        self.batch_sizes = Histogram(BATCH_BUCKETS)
        self.commit_seconds = Histogram(COMMIT_BUCKETS)
        self.outcomes = 0
        self.fallbacks = 0
        self.busy_retries = 0

    async def run(self, inbox: StageQueue[Outcome]) -> None:
        closing = False
        while not closing:
            first = await inbox.get()
            if first is None:
                return
            batch = [first]
            while len(batch) < self.batch_size:
                try:
                    item = inbox.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is None:
                    closing = True
                    break
                batch.append(item)
            await self._commit_patiently(batch)
            self.on_committed(batch)
            # Give the other stages a turn between commits.
            await asyncio.sleep(0)

    async def _commit_patiently(self, batch: list[Outcome]) -> None:
        """Commit, waiting out lock contention with other processes instead of failing the batch."""
        for attempt in range(BUSY_RETRIES):
            try:
                self.commit(batch)
                return
            except sqlite3.OperationalError as exc:
                if not is_busy(exc) or attempt == BUSY_RETRIES - 1:
                    raise
                self.busy_retries += 1
                log.info("%s: database busy, retrying a batch of %d", self.ctx.spider.name, len(batch))
                await asyncio.sleep(min(2.0, 0.05 * 2 ** attempt))

    def commit(self, batch: list[Outcome]) -> None:
        """Commit ``batch``; on failure retry its outcomes one at a time.

        Lock contention (another process holding the database) is not a
        failure of the outcomes: it propagates so that the batch is retried
        as a whole.
        """
        try:
            self._commit(batch)
            return
        except Exception as exc:  # noqa: BLE001 - isolate the outcome that breaks the batch
            if is_busy(exc):
                raise
            if len(batch) == 1:
                outcome = batch[0]
                log.exception("cannot record %s; marking it failed", outcome.request.url)
                self._commit([internal_failure(self.ctx, outcome.request, "write")])
                return
            log.warning("batch of %d outcome(s) failed; committing them one by one", len(batch), exc_info=True)
        self.fallbacks += 1
        for outcome in batch:
            self.commit([outcome])

    def _commit(self, batch: list[Outcome]) -> None:
        ctx = self.ctx
        started = time.perf_counter()
        pages: list[PageItems] = []
        owners: list[Tally] = []
        known: list[str] = []
        saved = [(dict(outcome.tally), list(outcome.exchange.metadata) if outcome.exchange else None)
                 for outcome in batch]
        archive = ctx.archive
        if archive is not None:
            archive.begin()
        try:
            with ctx.store.transaction():
                seq = ctx.checkpoint.next_seq
                for outcome in batch:
                    page = self._apply(outcome, seq, known)
                    if page is not None:
                        pages.append(page)
                        owners.append(outcome.tally)
                for tally, written in zip(owners, ctx.store.items.write(pages)):
                    _tally_items(tally, written)
                if archive is not None:
                    archive.sync()
                deltas = [outcome.tally for outcome in batch] + [{"write_batches": 1}]
                confirm = any(outcome.event is not None for outcome in batch)
                entry = ctx.checkpoint.write("batch", deltas=deltas, confirm=confirm)
        except BaseException:
            # The database transaction is rolling back; cut the records it
            # appended to WARC files so the archive cannot outlive its fetch
            # events (the batch is retried afterwards and re-archived once).
            if archive is not None:
                archive.rollback()
            for outcome, (tally, metadata) in zip(batch, saved):
                outcome.tally = tally
                if outcome.exchange is not None and metadata is not None:
                    outcome.exchange.metadata = metadata
                if outcome.event is not None:
                    outcome.event.journal_seq = None
            raise
        if archive is not None:
            archive.commit()
        ctx.checkpoint.apply(entry)
        for outcome in batch:
            if outcome.robots_origin is not None:
                ctx.checkpoint.record("robots_sitemaps", origin=outcome.robots_origin)
        if ctx.seen is not None:
            ctx.seen.add(known)
        self.outcomes += len(batch)
        self.batch_sizes.observe(len(batch))
        self.commit_seconds.observe(time.perf_counter() - started)
        stats = ctx.checkpoint.state.stats if ctx.checkpoint.state is not None else None
        if stats is not None and stats.pages_stored and stats.pages_stored % PROGRESS_EVERY < len(batch):
            log.info("%s: %d records, %d items new, %d errors", ctx.spider.name, stats.pages_stored,
                     stats.items_new, sum(stats.errors.values()))

    # -- one outcome ----------------------------------------------------------

    def _apply(self, outcome: Outcome, seq: int, known: list[str]) -> PageItems | None:
        """Write one outcome's rows (inside the batch transaction); returns its item write, if any."""
        ctx = self.ctx
        event = outcome.event
        tally = outcome.tally
        if ctx.archive is not None:
            for capture in outcome.archive_only:
                locations = ctx.archive.write(exchange_records(capture, revisit=None, software=SOFTWARE))
                bump(tally, "warc_records", len(locations))
                bump(tally, "warc_bytes", sum(location.length for location in locations))
        if event is None:
            self._enqueue(outcome, known)
            self._settle(outcome)
            return None
        near = self._near_duplicate(outcome, event)
        action = ctx.runtime.dedup.near_action if near is not None else "record"
        if action in ("skip_links", "skip"):
            outcome.children, outcome.sitemap_entries = [], []
        event.links_found = self._enqueue(outcome, known)
        if outcome.page_url is not None and outcome.page_url != outcome.request.url:
            ctx.frontier.mark_seen(outcome.page_url, outcome.request.host_key, depth=outcome.request.depth)
        if outcome.outlinks is not None:
            ctx.store.links.replace_outlinks(ctx.spider.name, event.url, outcome.outlinks, event.fetched_at)
        if outcome.exchange is not None and ctx.archive is not None:
            self._archive(outcome, event)
        event.journal_seq = seq
        change = ctx.store.documents.apply(event, ctx.run_id, ctx.spider.incremental.revisit_for(event.url))
        bump(tally, "pages_stored")
        if change.new:
            bump(tally, "documents_new")
        elif change.changed is True:
            bump(tally, "documents_changed")
        elif change.changed is False:
            bump(tally, "documents_unchanged")
        if outcome.simhash is not None:
            ctx.store.documents.mark_near_duplicate(ctx.spider.name, event.url, near[0] if near else None,
                                                    near[1] if near else None)
        if outcome.lastmod is not None and outcome.root is not None:
            ctx.store.watermarks.note_lastmod(ctx.spider.name, outcome.root, ctx.run_id, outcome.lastmod)
        self._settle(outcome)
        items = [] if action == "skip" else outcome.items
        complete = () if action == "skip" else outcome.complete_types
        return page_items(event, change.event_id, items, run_id=ctx.run_id, complete_types=complete,
                          page_url=outcome.page_url)

    def _near_duplicate(self, outcome: Outcome, event: FetchEvent) -> tuple[str, int] | None:
        ctx = self.ctx
        if outcome.simhash is None or ctx.near is None:
            return None
        match = ctx.near.nearest(ctx.spider.name, event.url, outcome.simhash)
        ctx.near.store(ctx.spider.name, event.url, outcome.simhash, event.fetched_at)
        if match is None:
            return None
        bump(outcome.tally, "near_duplicates")
        return match.url, match.distance

    def _enqueue(self, outcome: Outcome, known: list[str]) -> int:
        """Add discovered links and decide on sitemap entries; returns how many requests were queued."""
        ctx = self.ctx
        tally = outcome.tally
        enqueued = 0
        for child in outcome.children:
            result = ctx.frontier.add(child)
            if result is not AddOutcome.TOO_DEEP:
                known.append(child.url)
            if result is AddOutcome.ADDED:
                enqueued += 1
        mode = RunMode(ctx.session.mode) if ctx.session is not None else RunMode.FULL
        watermark = None
        if outcome.sitemap_entries and outcome.root is not None:
            watermark = ctx.store.watermarks.get(ctx.spider.name, outcome.root)
        for child in outcome.sitemap_entries:
            if not ctx.spider.incremental.use_sitemap_lastmod:
                decision = SitemapDecision.ADD
            else:
                decision = sitemap_decision(ctx.frontier, ctx.store, ctx.spider.name, child, watermark, mode)
            if decision is SitemapDecision.ADD:
                if ctx.frontier.add(child) is AddOutcome.ADDED:
                    enqueued += 1
                    known.append(child.url)
            elif decision is SitemapDecision.PROMOTE:
                if ctx.frontier.promote(child):
                    enqueued += 1
                    bump(tally, "sitemap_promoted")
            elif decision is SitemapDecision.SKIP:
                bump(tally, "sitemap_skipped")
        bump(tally, "links_enqueued", enqueued)
        return enqueued

    def _settle(self, outcome: Outcome) -> None:
        frontier = self.ctx.frontier
        request = outcome.request
        if outcome.settle is Settle.DONE:
            kept = frontier.complete(request)
        elif outcome.settle is Settle.FAILED:
            kept = frontier.complete(request, failed=True)
        elif outcome.settle is Settle.RETRY:
            kept = frontier.retry_later(request, outcome.delay)
        elif outcome.settle is Settle.DEFER:
            kept = frontier.defer(request, outcome.delay)
        else:
            kept = frontier.requeue(request)
        if not kept:
            # Another worker reclaimed the lease; it owns the row now. Our results
            # are recorded all the same (at-least-once delivery).
            bump(outcome.tally, "leases_lost")
            log.info("%s: lease on %s was lost before it was settled", self.ctx.spider.name, request.url)

    def _archive(self, outcome: Outcome, event: FetchEvent) -> None:
        """Write the exchange's WARC records (a revisit when nothing new was transferred)."""
        ctx = self.ctx
        exchange = outcome.exchange
        assert exchange is not None and ctx.archive is not None
        settings = ctx.runtime.archive
        digest = sha1_digest(exchange.payload)
        event.payload_digest = digest
        previous = ctx.store.documents.get(ctx.spider.name, event.url)
        revisit = None
        if settings.revisits:
            profile = revisit_profile(exchange.status, digest, previous.payload_digest if previous else None)
            if profile is None and 200 <= exchange.status < 300 and exchange.payload:
                # The same payload archived under another URL (mirrors, aliases, tracking variants).
                other = ctx.store.documents.archived_payload(ctx.spider.name, digest)
                if other is not None:
                    previous, profile = other, revisit_profile(exchange.status, digest, digest)
                    bump(outcome.tally, "warc_cross_revisits")
            if profile is not None:
                revisit = RevisitOf(profile, previous.warc_record_id if previous else None,
                                    (previous.final_url or previous.url) if previous else exchange.url,
                                    previous.warc_date if previous else None)
        if settings.metadata:
            exchange.metadata.extend(("outlink", url) for url in (outcome.outlinks or [])[:MAX_METADATA_OUTLINKS])
            if outcome.simhash is not None:
                exchange.metadata.append(("simhash", f"{outcome.simhash:016x}"))
        else:
            exchange.metadata = []
        locations = ctx.archive.write(exchange_records(exchange, revisit=revisit, software=SOFTWARE))
        main = locations[1]
        event.warc_file, event.warc_offset, event.warc_length = main.filename, main.offset, main.length
        if revisit is None and 200 <= exchange.status < 300:
            event.warc_record_id, event.warc_date = main.record_id, main.date
        bump(outcome.tally, "warc_records", len(locations))
        bump(outcome.tally, "warc_bytes", sum(location.length for location in locations))
        if revisit is not None:
            event.warc_revisit = True
            bump(outcome.tally, "warc_revisits")


def _tally_items(tally: Tally, written: ItemWrite) -> None:
    for name in ("new", "updated", "unchanged", "touched", "deleted"):
        value = getattr(written, name)
        if value:
            bump(tally, f"items_{name}", value)
