"""The fetch stage: robots.txt, conditional requests, the HTTP exchange and failure handling.

Each of the spider's ``concurrency`` fetch tasks takes a :class:`Job` (a
leased request holding a host slot), sends at most one HTTP request for it
and gives the host slot back -- with the actual send time, so per-host
spacing holds between real sends. Responses go on to the parse stage;
failures are classified here (retry with back-off, defer, give up) and go
straight to the writer, as do requests that first had to fetch the
origin's robots.txt (they are re-queued and wait for their politeness
turn like any other request).
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from typing import Callable

from skein.frontier.request import CrawlRequest, RequestKind
from skein.incremental.conditional import Validators, conditional_headers, validators_from
from skein.incremental.freshness import freshness_lifetime
from skein.incremental.planner import SITEMAP_PRIORITY
from skein.net.errors import HttpError
from skein.net.response import Response
from skein.pipeline.context import CrawlContext
from skein.pipeline.queues import StageQueue
from skein.pipeline.work import Fetched, Job, Outcome, Settle, Tally, bump
from skein.politeness.scheduler import HostSlot
from skein.retry.classify import ErrorClass, Failure, classify_exception, classify_status
from skein.retry.policy import RetryAction
from skein.robots.cache import RobotsEntry
from skein.store.database import is_busy
from skein.store.documents import FetchEvent
from skein.urls.parse import URLError, parse_url
from skein.warc.http import Exchange

log = logging.getLogger(__name__)


def capture(response: Response, fetched_at: float, metadata: list[tuple[str, str]] | None = None) -> Exchange:
    """What the archive keeps of a response: request and response heads plus the transferred payload."""
    return Exchange(
        url=response.url,
        fetched_at=fetched_at,
        method=response.method,
        request_target=parse_url(response.url).request_target,
        request_headers=list(response.request_headers),
        http_version=response.http_version,
        status=response.status,
        reason=response.reason,
        response_headers=response.headers.items(),
        payload=response.payload,
        ip_address=response.remote_address,
        metadata=metadata or [],
    )


def internal_failure(ctx: CrawlContext, request: CrawlRequest, stage: str) -> Outcome:
    """The outcome of a request whose processing raised unexpectedly: it fails without retry."""
    return Outcome(request, {"error": "internal"}, Settle.FAILED,
                   event=ctx.new_event(request, error=f"internal error in {stage} stage",
                                       error_class=ErrorClass.PERMANENT.value))


class FetchStage:
    """Turns jobs into :class:`Fetched` responses (to ``parsed``) or final outcomes (to ``written``)."""

    def __init__(
        self,
        ctx: CrawlContext,
        parsed: StageQueue[Fetched],
        written: StageQueue[Outcome],
        on_slot_free: Callable[[], None],
    ) -> None:
        self.ctx = ctx
        self.parsed = parsed
        self.written = written
        self.on_slot_free = on_slot_free
        self._robots_applied: set[str] = set()
        self._robots_captures: dict[str, Exchange] = {}
        if ctx.archive is not None:
            ctx.robots.observer = self._capture_robots
        self.busy = 0
        self.processed = 0

    def _capture_robots(self, origin: str, response: Response) -> None:
        """Keep a robots.txt response for the archive (it has no fetch event of its own)."""
        self._robots_captures[origin] = capture(response, self.ctx.clock.time())

    async def run(self, inbox: StageQueue[Job]) -> None:
        while True:
            job = await inbox.get()
            if job is None:
                return
            self.busy += 1
            try:
                result = await self._process(job)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - one bad request must not stop the crawl
                log.exception("unexpected error while fetching %s", job.request.url)
                result = internal_failure(self.ctx, job.request, "fetch")
            finally:
                self.busy -= 1
            self.processed += 1
            if isinstance(result, Fetched):
                await self.parsed.put(result)
            else:
                await self.written.put(result)

    async def _process(self, job: Job) -> Fetched | Outcome:
        ctx = self.ctx
        request = job.request
        slot = job.claim.slot
        sent_at: float | None = None
        extra: list[CrawlRequest] = []
        robots_origin: str | None = None
        try:
            if ctx.spider.obey_robots:
                entry = ctx.robots.cached(request.url)
                if entry is None:
                    # First contact with an origin: the robots.txt fetch uses this
                    # request's slot; the page goes back to the frontier.
                    sent_at = ctx.clock.time()
                    entry = await ctx.robots.get(request.url)
                    robots_origin, extra = self._apply_robots(request, entry)
                    capture = self._robots_captures.pop(entry.origin, None)
                    return Outcome(request, {"robots_fetches": 1, "requests": 1, "host": request.host_key},
                                   Settle.REQUEUE, children=extra, robots_origin=robots_origin,
                                   archive_only=[capture] if capture is not None else [])
                if entry.origin not in self._robots_applied:
                    robots_origin, extra = self._apply_robots(request, entry)
            sent_at = ctx.clock.time()
            result = await self._fetch(request)
        finally:
            if slot is not None:
                self._release(slot, sent_at)
        if isinstance(result, Fetched):
            result.extra_children, result.robots_origin = extra, robots_origin
        elif robots_origin is not None:
            result.children.extend(extra)
            result.robots_origin = robots_origin
        return result

    def _release(self, slot: HostSlot, sent_at: float | None) -> None:
        """Give the host slot back; if the database stays locked, its lease simply runs out."""
        try:
            self.ctx.scheduler.release(slot, sent_at)
        except sqlite3.OperationalError as exc:
            if not is_busy(exc):
                raise
            log.warning("%s: could not release a slot of %s (database busy); it expires on its own",
                        self.ctx.spider.name, slot.host)
        self.on_slot_free()

    def _apply_robots(self, request: CrawlRequest, entry: RobotsEntry) -> tuple[str | None, list[CrawlRequest]]:
        """Pass the origin's Crawl-delay to the scheduler; robots.txt sitemaps to queue (once per run)."""
        ctx = self.ctx
        self._robots_applied.add(entry.origin)
        delay = entry.robots.group_for(ctx.user_agent).crawl_delay
        if delay != ctx.scheduler.state(request.host_key).crawl_delay:
            ctx.scheduler.set_crawl_delay(request.host_key, delay)
        if entry.state == "unreachable":
            log.warning("robots.txt for %s unreachable (%s); host is disallowed", entry.origin, entry.error)
        session = ctx.session
        assert session is not None
        if not ctx.spider.use_robots_sitemaps or entry.origin in session.robots_sitemaps:
            return None, []
        found = []
        for raw in entry.robots.sitemaps:
            try:
                url = ctx.spider.canonicalizer.key(raw)
            except URLError:
                continue
            if ctx.links.scope.allows(url):
                found.append(CrawlRequest(url, 0, SITEMAP_PRIORITY, RequestKind.SITEMAP, seed=request.seed))
        return entry.origin, found

    def _redirect_filter(self, request: CrawlRequest) -> Callable[[str], bool]:
        origin = parse_url(request.url).origin
        ctx = self.ctx

        def follow(target: str) -> bool:
            if parse_url(target).origin != origin:
                return False
            if not ctx.spider.obey_robots:
                return True
            decision = ctx.robots.decide_cached(target)
            return decision is not None and decision.allowed

        return follow

    def _stored_validators(self, request: CrawlRequest) -> Validators:
        """Validators of the stored copy; none when there is no stored content to fall back on after a 304."""
        if not self.ctx.spider.incremental.conditional_requests:
            return Validators()
        document = self.ctx.store.documents.get(self.ctx.spider.name, request.url)
        if document is None or document.last_success is None or document.fingerprint is None:
            return Validators()
        return Validators(document.etag, document.last_modified)

    async def _fetch(self, request: CrawlRequest) -> Fetched | Outcome:
        ctx = self.ctx
        tally: Tally = {"requests": 1, "host": request.host_key}
        headers = conditional_headers(self._stored_validators(request))
        if headers:
            bump(tally, "conditional_requests")
        started = ctx.clock.monotonic()
        try:
            response = await ctx.client.request(
                "GET", request.url, headers=headers, redirect_filter=self._redirect_filter(request)
            )
        except HttpError as exc:
            tally["error"] = type(exc).__name__
            event = ctx.new_event(request, elapsed=ctx.clock.monotonic() - started, conditional=bool(headers))
            return self._fail(request, classify_exception(exc), event, tally)
        tally.update(status=response.status, latency=response.elapsed)
        bump(tally, "responses")
        bump(tally, "bytes_received", response.wire_bytes)
        bump(tally, "bytes_decoded", len(response.body))
        if response.history:
            bump(tally, "redirects_followed", len(response.history))
        event = self._response_event(request, response, conditional=bool(headers))
        exchange = None
        if ctx.archive is not None:
            metadata = [("fetchTimeMs", f"{(response.elapsed or 0.0) * 1000:.0f}")]
            if request.referrer:
                metadata.append(("via", request.referrer))
            metadata.extend(("redirectedFrom", hop.url) for hop in response.history)
            exchange = capture(response, event.fetched_at, metadata)
        failure = classify_status(response.status, response.headers, ctx.clock.time())
        if failure is None and response.status == 304 and not headers:
            failure = Failure(ErrorClass.PERMANENT, "HTTP 304 without a conditional request", status=304)
        if failure is not None:
            outcome = self._fail(request, failure, event, tally)
            outcome.exchange = exchange
            return outcome
        ctx.scheduler.host_success(request.host_key)
        return Fetched(request, response, event, tally, exchange)

    def _response_event(self, request: CrawlRequest, response: Response, *, conditional: bool) -> FetchEvent:
        ctx = self.ctx
        event = ctx.new_event(
            request,
            final_url=response.url,
            status=response.status,
            content_type=response.headers.get("Content-Type"),
            content_length=len(response.body),
            elapsed=response.elapsed,
            redirects=[hop.url for hop in response.history] or None,
            conditional=conditional,
        )
        now = ctx.clock.time()
        validators = validators_from(response.headers, now)
        event.etag, event.last_modified = validators.etag, validators.last_modified
        event.freshness = freshness_lifetime(response.headers, now)
        if response.ok and (response.is_html or response.content_type.startswith("text/")):
            event.charset = response.charset.encoding
        return event

    def _fail(self, request: CrawlRequest, failure: Failure, event: FetchEvent, tally: Tally) -> Outcome:
        """Classify a failed attempt into retry, defer or give up (host-level failures pause the host)."""
        ctx = self.ctx
        retry = ctx.spider.retry
        if failure.host_level:
            pause = ctx.scheduler.host_failure(request.host_key, failure.retry_after)
            bump(tally, "host_backoffs")
            failures = ctx.scheduler.state(request.host_key).failures
            if retry.quarantines(failures) and ctx.frontier.blocked_until(request.host_key) is None:
                deferred = ctx.frontier.block_host(request.host_key, ctx.clock.time() + retry.quarantine)
                bump(tally, "hosts_quarantined")
                log.warning("%s: %s failed %d times in a row; %d queued request(s) deferred for %.0fs",
                            ctx.spider.name, request.host_key, failures, deferred, retry.quarantine)
            log.info("%s: backing off %s for %.1fs (%s)", ctx.spider.name, request.host_key, pause, failure.reason)
        decision = retry.decide(failure, request.attempts, ctx.rng)
        event.error_class = failure.error_class.value
        outcome = Outcome(request, tally, event=event)
        if failure.status in (404, 410) and request.kind is RequestKind.PAGE:
            # The page is gone, and with it every item it used to carry.
            outcome.complete_types = tuple(r.name for r in ctx.extractor.rules if r.applies_to(request.url))
        if decision.action is RetryAction.RETRY:
            event.error = f"{failure.reason} (will retry)"
            request.attempts += 1
            bump(tally, "retries")
            if failure.retry_after is not None and retry.respect_retry_after:
                bump(tally, "retry_after_honoured")
            outcome.settle, outcome.delay = Settle.RETRY, decision.delay
        elif decision.action is RetryAction.DEFER:
            event.error = f"{failure.reason} (deferred)"
            bump(tally, "deferred")
            outcome.settle, outcome.delay = Settle.DEFER, decision.delay
        else:
            event.error = failure.reason
            if failure.transient:
                bump(tally, "gave_up")
            log.info("giving up on %s: %s", request.url, decision.reason)
            outcome.settle = Settle.FAILED
        return outcome
