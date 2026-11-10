"""The SQLite-backed frontier: ordering, de-duplication, politeness at dequeue, persistence."""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from skein.clock import FakeClock
from skein.frontier import AddOutcome, CrawlRequest, Frontier, RequestKind
from skein.politeness import HostScheduler, PolitenessPolicy
from skein.store import FetchEvent, Store


def req(url: str, depth: int = 0, priority: int = 0, **kw: object) -> CrawlRequest:
    return CrawlRequest(url, depth=depth, priority=priority, **kw)  # type: ignore[arg-type]


def open_scheduler(store: Store, clock: FakeClock, owner: str = "local", **policy: object) -> HostScheduler:
    settings = {"min_delay": 0.0, "per_host_concurrency": 64, **policy}
    return HostScheduler(store.db, PolitenessPolicy(**settings), clock, owner=owner)  # type: ignore[arg-type]


def take(frontier: Frontier, scheduler: HostScheduler | None = None) -> CrawlRequest | None:
    """Claim the next request (releasing its host slot right away, as after an instant fetch)."""
    scheduler = scheduler or open_scheduler_for(frontier)
    batch = frontier.claim(scheduler)
    if not batch.claims:
        return None
    claim = batch.claims[0]
    assert claim.slot is not None
    scheduler.release(claim.slot)
    return claim.request


def open_scheduler_for(frontier: Frontier) -> HostScheduler:
    return HostScheduler(frontier.db, PolitenessPolicy(min_delay=0.0, per_host_concurrency=64), frontier.clock,
                         owner=frontier.owner)


@pytest.fixture
def store(tmp_path: Path):  # noqa: ANN201
    with Store(tmp_path / "frontier.db") as db:
        yield db


def drain(frontier: Frontier) -> list[str]:
    order = []
    while True:
        request = take(frontier)
        if request is None:
            return order
        order.append(request.url.rsplit("/", 1)[1])
        frontier.complete(request)


def test_request_host_key_seed_and_child() -> None:
    parent = CrawlRequest("https://Example.com:443/a", depth=2, seed="https://example.com/")
    assert parent.host_key == "https://example.com:443"
    child = parent.child("https://example.com/b", priority=3, kind=RequestKind.SITEMAP)
    assert (child.depth, child.priority, child.kind, child.referrer) == (3, 3, RequestKind.SITEMAP, parent.url)
    assert child.seed == "https://example.com/"


def test_dedup_and_depth_limit(store: Store) -> None:
    frontier = Frontier(store.db, "s", FakeClock(), max_depth=1)
    assert frontier.add(req("http://a/1")) is AddOutcome.ADDED
    assert frontier.add(req("http://a/1", priority=9)) is AddOutcome.DUPLICATE
    assert frontier.add(req("http://a/2", depth=2)) is AddOutcome.TOO_DEEP
    assert len(frontier) == 1 and frontier.seen("http://a/1") and not frontier.seen("http://a/2")
    assert frontier.stats.duplicates == 1 and frontier.stats.too_deep == 1
    other_spider = Frontier(store.db, "t", FakeClock())
    assert other_spider.add(req("http://a/1")) is AddOutcome.ADDED, "spiders have separate frontiers"


def test_priority_then_depth_then_fifo(store: Store) -> None:
    frontier = Frontier(store.db, "s", FakeClock())
    frontier.add(req("http://a/low", priority=0))
    frontier.add(req("http://a/deep", depth=3, priority=5))
    frontier.add(req("http://a/shallow", depth=1, priority=5))
    frontier.add(req("http://a/second-low", priority=0))
    assert drain(frontier) == ["shallow", "deep", "low", "second-low"]
    assert frontier.counts() == {"done": 4}


def test_claim_skips_hosts_that_are_not_ready(store: Store) -> None:
    clock = FakeClock()
    frontier = Frontier(store.db, "s", clock)
    scheduler = open_scheduler(store, clock)
    frontier.add(req("http://busy/1", priority=10))
    frontier.add(req("http://idle/1"))
    scheduler.back_off("http://busy:80", 5.0)
    clock.advance(1.0)
    request = take(frontier, scheduler)
    assert request is not None and request.url == "http://idle/1"
    batch = frontier.claim(scheduler)
    assert not batch.claims and batch.next_ready == clock.time() + 4.0
    clock.advance(4.0)
    request = take(frontier, scheduler)
    assert request is not None and request.url == "http://busy/1"
    assert frontier.claim(scheduler).next_ready == math.inf


def test_retry_later_respects_not_before(store: Store) -> None:
    clock = FakeClock()
    frontier = Frontier(store.db, "s", clock)
    scheduler = open_scheduler(store, clock)
    frontier.add(req("http://a/1"))
    request = take(frontier, scheduler)
    assert request is not None
    request.attempts = 1
    frontier.retry_later(request, 30.0)
    assert len(frontier) == 1
    assert frontier.claim(scheduler).next_ready == pytest.approx(clock.time() + 30.0)
    clock.advance(30.0)
    again = take(frontier, scheduler)
    assert again is not None and again.attempts == 1


def test_state_survives_reopen_and_active_rows_are_recovered(tmp_path: Path) -> None:
    path = tmp_path / "persist.db"
    clock = FakeClock()
    with Store(path) as store:
        frontier = Frontier(store.db, "s", clock)
        frontier.add_many([req(f"http://a/{n}", seed="http://a/0") for n in range(4)])
        taken = take(frontier)
        assert taken is not None
        frontier.complete(taken)
        in_flight = take(frontier)
        assert in_flight is not None
    with Store(path) as store:
        frontier = Frontier(store.db, "s", clock)
        assert frontier.counts() == {"done": 1, "active": 1, "pending": 2}
        assert len(frontier) == 2
        assert frontier.recover() == 1 and len(frontier) == 3
        assert frontier.unfinished("http://a/0") == 3
        assert drain(frontier) == ["1", "2", "3"]
        assert frontier.add(req("http://a/0")) is AddOutcome.DUPLICATE, "the seen set is persistent"


def test_reschedule_full_and_due(store: Store) -> None:
    clock = FakeClock()
    frontier = Frontier(store.db, "s", clock)
    frontier.add_many([req("http://a/fresh"), req("http://a/stale"), req("http://a/never")])
    for _ in range(2):
        request = take(frontier)
        assert request is not None
        store.record_fetch(FetchEvent("s", request.url, clock.time(), status=200, fingerprint="x"))
        frontier.complete(request)
    store.db.execute("UPDATE documents SET next_visit = ? WHERE url = 'http://a/stale'", (clock.time() - 1,))
    store.db.execute("UPDATE documents SET next_visit = ? WHERE url = 'http://a/fresh'", (clock.time() + 99,))
    assert frontier.reschedule(due_before=clock.time()) == 1, "only the overdue document"
    assert sorted(drain(frontier)) == ["never", "stale"]
    assert frontier.reschedule() == 3, "a full run re-queues everything handled"


def test_promote_once_per_run_and_defer(store: Store) -> None:
    clock = FakeClock()
    frontier = Frontier(store.db, "s", clock)
    frontier.run_id = 1
    frontier.add(req("http://a/x"))
    request = take(frontier)
    assert request is not None
    frontier.complete(request)
    assert not frontier.promote(req("http://a/x", sitemap_lastmod=5.0)), "already handled in this run"
    frontier.run_id = 2
    assert frontier.promote(req("http://a/x", sitemap_lastmod=5.0, priority=7))
    request = take(frontier)
    assert request is not None and request.priority == 7 and request.sitemap_lastmod == 5.0
    frontier.defer(request, 3600)
    assert frontier.counts() == {"deferred": 1} and frontier.is_empty()
    assert frontier.reschedule(due_before=clock.time()) == 0, "deferral not over yet"
    clock.advance(3600)
    assert frontier.reschedule(due_before=clock.time()) == 1


def test_mark_seen_and_requeue(store: Store) -> None:
    frontier = Frontier(store.db, "s", FakeClock())
    assert frontier.mark_seen("http://a/final", "http://a:80")
    assert not frontier.mark_seen("http://a/final", "http://a:80")
    assert frontier.add(req("http://a/final")) is AddOutcome.DUPLICATE
    frontier.add(req("http://a/1"))
    request = take(frontier)
    assert request is not None
    frontier.requeue(request)
    assert frontier.stats.requeued == 1 and frontier.hosts() == ["http://a:80"]


def test_rescheduled_documents_are_ordered_by_urgency(store: Store) -> None:
    clock = FakeClock()
    frontier = Frontier(store.db, "s", clock)
    urls = ["http://a/calm", "http://a/volatile", "http://a/overdue"]
    frontier.add_many([req(u) for u in urls])
    for _ in urls:
        request = take(frontier)
        assert request is not None
        frontier.complete(request)
    store.db.execute("""INSERT INTO documents (spider, url, first_fetched, last_success, check_count, change_count,
                                               revisit_interval, next_visit)
                        VALUES ('s', 'http://a/calm', 0, 1, 8, 0, 100, ?),
                               ('s', 'http://a/volatile', 0, 1, 8, 8, 100, ?),
                               ('s', 'http://a/overdue', 0, 1, 8, 0, 100, ?)""",
                     (clock.time(), clock.time(), clock.time() - 100))
    frontier.add(req("http://a/brand-new"))
    assert frontier.reschedule(count_only=True) == 3
    assert frontier.counts() == {"done": 3, "pending": 1}, "count_only changes nothing"
    assert frontier.reschedule() == 3
    assert drain(frontier) == ["brand-new", "overdue", "volatile", "calm"]
    urgency = {row["url"].rsplit("/", 1)[1]: row["urgency"] for row in frontier.listing()}
    assert urgency["calm"] == pytest.approx(0.1) and urgency["volatile"] == pytest.approx(0.9)
    assert urgency["overdue"] == pytest.approx(1.1) and urgency["brand-new"] == 1.5


def test_blocked_host_defers_new_and_settled_requests(store: Store) -> None:
    clock = FakeClock()
    frontier = Frontier(store.db, "s", clock)
    frontier.add_many([req("http://a/1"), req("http://a/2"), req("http://b/1")])
    scheduler = open_scheduler(store, clock)
    scheduler.back_off("http://b:80", 99.0)
    in_flight = take(frontier, scheduler)
    assert in_flight is not None and in_flight.url == "http://a/1"
    assert frontier.block_host("http://a:80", clock.time() + 600) == 1
    frontier.add(req("http://a/3"))
    frontier.retry_later(in_flight, 5)
    assert frontier.hosts() == ["http://b:80"]
    states = {row["url"]: (row["state"], row["not_before"]) for row in frontier.listing()}
    assert {u for u, (s, _) in states.items() if s == "deferred"} == {"http://a/1", "http://a/2", "http://a/3"}
    assert all(nb == clock.time() + 600 for s, nb in states.values() if s == "deferred")
    clock.advance(601)
    assert frontier.blocked_until("http://a:80") is None
