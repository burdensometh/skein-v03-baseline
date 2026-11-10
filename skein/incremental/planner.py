"""Preparing the frontier for a run and maintaining per-seed watermarks."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum

from skein.frontier.request import CrawlRequest, RequestKind
from skein.frontier.sqlite import AddOutcome, Frontier
from skein.incremental.modes import RunMode
from skein.store.sqlite import Store
from skein.store.watermarks import Watermark

log = logging.getLogger(__name__)

SITEMAP_PRIORITY = 100


@dataclass
class RunPlan:
    """What :func:`plan_run` scheduled."""

    mode: RunMode
    roots: list[CrawlRequest] = field(default_factory=list)
    rescheduled: int = 0
    added: int = 0

    def to_dict(self) -> dict[str, object]:
        return {"mode": self.mode.value, "roots": len(self.roots), "rescheduled": self.rescheduled,
                "added": self.added}


def crawl_roots(seeds: tuple[str, ...], sitemaps: tuple[str, ...]) -> list[CrawlRequest]:
    """Canonical seed and sitemap requests; each is its own watermark root."""
    roots = [CrawlRequest(url, 0, SITEMAP_PRIORITY, RequestKind.SITEMAP, seed=url) for url in sitemaps]
    roots += [CrawlRequest(url, 0, 0, RequestKind.PAGE, seed=url) for url in seeds]
    return roots


def resolve_mode(requested: RunMode, store: Store, spider: str, roots: list[CrawlRequest]) -> RunMode:
    """Turn ``auto`` into ``full`` (some root never completed) or ``incremental``."""
    if requested is not RunMode.AUTO:
        return requested
    for root in roots:
        mark = store.watermarks.get(spider, root.url)
        if mark is None or mark.last_completed is None:
            return RunMode.FULL
    return RunMode.INCREMENTAL


def plan_run(
    frontier: Frontier,
    store: Store,
    spider: str,
    roots: list[CrawlRequest],
    mode: RunMode,
    run_id: int,
    now: float,
) -> RunPlan:
    """Queue this run's work.

    Roots are visited every run (that is how new and changed URLs are
    found), except seed pages in sitemap mode, which only rely on the
    sitemaps. A full run re-queues every known URL, an incremental run the
    ones whose revisit time has come and that something still links to, a
    sitemap run every known sitemap. Watermarks note that this run started
    working on each root.
    """
    plan = RunPlan(mode)
    with store.transaction():
        if mode is RunMode.SITEMAP:
            plan.rescheduled = frontier.reschedule(kind=RequestKind.SITEMAP)
        elif mode is RunMode.FULL:
            plan.rescheduled = frontier.reschedule()
        else:
            plan.rescheduled = frontier.reschedule(due_before=now, skip_orphans=True)
        for root in roots:
            if frontier.add(root) is AddOutcome.ADDED:
                plan.added += 1
            elif (root.kind is RequestKind.SITEMAP or mode is not RunMode.SITEMAP) and frontier.promote(root):
                plan.rescheduled += 1
            store.watermarks.started(spider, root.url, root.kind.value, run_id, now)
            plan.roots.append(root)
    log.info("%s: %s run, %d URL(s) re-queued, %d new root(s)", spider, mode.value, plan.rescheduled, plan.added)
    return plan


class SitemapDecision(str, Enum):
    """``KNOWN``: already queued or handled by this run; ``SKIP``: left to its revisit schedule."""

    ADD = "add"
    PROMOTE = "promote"
    SKIP = "skip"
    KNOWN = "known"


def sitemap_decision(
    frontier: Frontier,
    store: Store,
    spider: str,
    request: CrawlRequest,
    watermark: Watermark | None,
    mode: RunMode,
) -> SitemapDecision:
    """How an incremental run treats a URL listed in a sitemap.

    Unknown URLs are added. A known URL is fetched now only if its
    ``lastmod`` is newer than the root's watermark (everything up to the
    watermark was processed by a complete run) and newer than the last
    successful fetch of the document. Otherwise it is left to its revisit
    schedule. In a full run every known URL is queued already.
    """
    state = frontier.state_of(request.url)
    if state is None:
        return SitemapDecision.ADD
    if mode is RunMode.FULL or state in ("pending", "active") or frontier.run_id_of(request.url) == frontier.run_id:
        return SitemapDecision.KNOWN
    if request.sitemap_lastmod is None:
        return SitemapDecision.SKIP
    if watermark is not None and watermark.lastmod_high is not None and request.sitemap_lastmod <= watermark.lastmod_high:
        return SitemapDecision.SKIP
    document = store.documents.get(spider, request.url)
    if document is not None and document.last_success is not None and request.sitemap_lastmod <= document.last_success:
        return SitemapDecision.SKIP
    return SitemapDecision.PROMOTE


def complete_watermarks(
    frontier: Frontier,
    store: Store,
    spider: str,
    roots: list[CrawlRequest],
    run_started: float,
    run_id: int,
) -> list[str]:
    """Advance the watermark of every root with no pending work left; returns those roots.

    The sitemap ``lastmod`` high-water mark is the one the run's workers
    recorded for the root while processing its sitemaps.
    """
    completed = []
    with store.transaction():
        for root in roots:
            if frontier.unfinished(root.url) == 0:
                lastmod = store.watermarks.run_lastmod(spider, root.url, run_id)
                store.watermarks.completed(spider, root.url, run_started, lastmod)
                completed.append(root.url)
    return completed


def preview_run(
    frontier: Frontier, store: Store, spider: str, roots: list[CrawlRequest], requested: RunMode, now: float
) -> dict[str, object]:
    """What the next run would queue, computed read-only (nothing in the database changes)."""
    mode = resolve_mode(requested, store, spider, roots)
    if mode is RunMode.SITEMAP:
        due = frontier.reschedule(kind=RequestKind.SITEMAP, count_only=True)
    elif mode is RunMode.FULL:
        due = frontier.reschedule(count_only=True)
    else:
        due = frontier.reschedule(due_before=now, skip_orphans=True, count_only=True)
    db = store.db
    waiting = db.execute(
        "SELECT COUNT(*), MIN(not_before) FROM frontier WHERE spider = ? AND state = 'deferred' AND not_before > ?",
        (spider, now),
    ).fetchone()
    next_due = db.execute(
        """SELECT MIN(d.next_visit) FROM documents d JOIN frontier f ON f.spider = d.spider AND f.url = d.url
           WHERE d.spider = ? AND d.next_visit > ? AND f.state IN ('done', 'failed')""",
        (spider, now),
    ).fetchone()[0]
    return {
        "mode": mode.value,
        "requeued": due,
        "already_pending": frontier.unfinished() - (waiting[0] or 0),
        "deferred_waiting": waiting[0] or 0,
        "deferred_until": waiting[1],
        "orphans": len(store.links.orphans(spider)),
        "roots": [root.url for root in roots],
        "next_due": next_due,
        "watermarks": {mark.seed: mark.last_completed for mark in store.watermarks.all(spider)},
    }
