"""Failure classification, Retry-After, per-request retries and per-host back-off."""

from __future__ import annotations

import random
from pathlib import Path

import pytest

from skein.clock import FakeClock
from skein.net.errors import (
    BodyTooLarge,
    ConnectError,
    ConnectionClosed,
    DecodingError,
    ProtocolError,
    RedirectLoop,
    TimeoutError,
    TooManyRedirects,
)
from skein.net.headers import Headers
from skein.retry import (
    BackoffPolicy,
    ErrorClass,
    Failure,
    RetryAction,
    RetryPolicy,
    classify_exception,
    classify_recorded,
    classify_status,
    parse_retry_after,
)
from skein.store import Store
from tests.helpers import crawl, events, key

NOW = 1_700_000_000.0


def test_retry_after_forms() -> None:
    assert parse_retry_after("120", NOW) == 120.0
    assert parse_retry_after(" 0 ", NOW) == 0.0
    assert parse_retry_after("Tue, 14 Nov 2023 22:15:20 GMT", NOW) == 120.0
    assert parse_retry_after("Tuesday, 14-Nov-23 22:15:20 GMT", NOW) == 120.0
    assert parse_retry_after("Tue Nov 14 22:15:20 2023", NOW) == 120.0
    assert parse_retry_after("Mon, 13 Nov 2023 00:00:00 GMT", NOW) == 0.0, "dates in the past mean now"
    for bad in (None, "", "-5", "1.5", "soon", "Tue, 31 Feb 2023 00:00:00 GMT"):
        assert parse_retry_after(bad, NOW) is None


def test_retry_after_date_is_relative_to_the_server_clock() -> None:
    # The server's clock is an hour ahead; it still means "two minutes from now".
    server_date = "Tue, 14 Nov 2023 23:13:20 GMT"
    assert parse_retry_after("Tue, 14 Nov 2023 23:15:20 GMT", NOW, server_date) == 120.0
    assert parse_retry_after("Tue, 14 Nov 2023 23:15:20 GMT", NOW, "garbage") == 3720.0
    headers = Headers([("Retry-After", "Tue, 14 Nov 2023 23:15:20 GMT"), ("Date", server_date)])
    failure = classify_status(429, headers, NOW)
    assert failure is not None and failure.retry_after == 120.0


@pytest.mark.parametrize("status, error_class, host_level", [
    (408, ErrorClass.TRANSIENT, False),
    (429, ErrorClass.TRANSIENT, True),
    (500, ErrorClass.TRANSIENT, False),
    (502, ErrorClass.TRANSIENT, True),
    (503, ErrorClass.TRANSIENT, True),
    (504, ErrorClass.TRANSIENT, True),
    (599, ErrorClass.TRANSIENT, False),
    (400, ErrorClass.PERMANENT, False),
    (401, ErrorClass.PERMANENT, False),
    (404, ErrorClass.PERMANENT, False),
    (410, ErrorClass.PERMANENT, False),
    (501, ErrorClass.PERMANENT, False),
    (505, ErrorClass.PERMANENT, False),
])
def test_status_classification(status: int, error_class: ErrorClass, host_level: bool) -> None:
    failure = classify_status(status, Headers(), NOW)
    assert failure is not None
    assert (failure.error_class, failure.host_level, failure.status) == (error_class, host_level, status)


def test_success_and_redirect_statuses_are_not_failures() -> None:
    assert all(classify_status(code, Headers(), NOW) is None for code in (200, 204, 301, 304))
    assert classify_status(500, Headers([("Retry-After", "5")]), NOW).retry_after is None, "only 429/503"  # type: ignore[union-attr]


def test_exception_classification() -> None:
    transient = [ConnectionClosed("reset"), ProtocolError("junk"), TimeoutError("slow", phase="read")]
    permanent = [BodyTooLarge(10), DecodingError("bad gzip"), RedirectLoop("loop"), TooManyRedirects("many")]
    assert all(classify_exception(e).transient for e in transient)
    assert not any(classify_exception(e).transient for e in permanent)
    assert classify_exception(ConnectError("refused")).host_level
    assert classify_exception(TimeoutError("connect", phase="connect")).host_level
    assert not classify_exception(TimeoutError("read", phase="read")).host_level
    assert classify_exception(ConnectionClosed("x")).reason.startswith("ConnectionClosed: x")


def test_recorded_error_classification() -> None:
    assert classify_recorded(200, None) is None
    assert classify_recorded(None, "robots.txt: Disallow: /x") is ErrorClass.ROBOTS
    assert classify_recorded(503, "HTTP 503 (will retry)") is ErrorClass.TRANSIENT
    assert classify_recorded(404, "HTTP 404") is ErrorClass.PERMANENT
    assert classify_recorded(None, "TimeoutError: slow (will retry)") is ErrorClass.TRANSIENT
    assert classify_recorded(None, "BodyTooLarge: response body exceeds 10 bytes") is ErrorClass.PERMANENT
    assert classify_recorded(None, "SitemapError: bad xml") is ErrorClass.PERMANENT


def test_retry_policy_decisions() -> None:
    policy = RetryPolicy(max_retries=2, backoff=BackoffPolicy(base=1, factor=2, max_delay=60, jitter=0),
                         max_retry_after=100)
    rng = random.Random(0)
    transient = Failure(ErrorClass.TRANSIENT, "HTTP 500")
    assert policy.decide(transient, 0, rng) == pytest.approx(policy.decide(transient, 0, rng))
    assert [policy.decide(transient, n, rng).delay for n in (0, 1)] == [1.0, 2.0]
    assert policy.decide(transient, 2, rng).action is RetryAction.GIVE_UP
    assert policy.decide(Failure(ErrorClass.PERMANENT, "HTTP 404"), 0, rng).action is RetryAction.GIVE_UP
    polite = Failure(ErrorClass.TRANSIENT, "HTTP 429", retry_after=30.0)
    decision = policy.decide(polite, 0, rng)
    assert decision.action is RetryAction.RETRY and decision.delay == 30.0
    deferred = policy.decide(Failure(ErrorClass.TRANSIENT, "HTTP 503", retry_after=7200.0), 0, rng)
    assert deferred.action is RetryAction.DEFER and deferred.delay == 7200.0
    ignoring = RetryPolicy(respect_retry_after=False, backoff=BackoffPolicy(base=1, jitter=0))
    assert ignoring.decide(polite, 0, rng).delay == 1.0
    assert RetryPolicy(host_failure_limit=3).quarantines(3) and not RetryPolicy(host_failure_limit=None).quarantines(99)
    with pytest.raises(ValueError):
        RetryPolicy(max_retries=-1)


@pytest.mark.parametrize("form", ["seconds", "date"])
def test_429_retry_after_is_honoured_end_to_end(make_site, tmp_path: Path, form: str) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time)
    db = tmp_path / "shop.db"
    url = site.url(f"/limited/a?fail=2&after=40&form={form}")
    result = crawl(site, db, clock, seeds=[url], max_depth=0, politeness={"min_delay": 0},
                   retry={"backoff_base": 1, "jitter": 0, "max_retries": 3})
    attempts = events(db, url)
    assert [e["status"] for e in attempts] == [429, 429, 200]
    gaps = [b["fetched_at"] - a["fetched_at"] for a, b in zip(attempts, attempts[1:])]
    assert all(gap >= 40.0 for gap in gaps), gaps
    assert result.stats["retry_after_honoured"] == 2 and result.stats["host_backoffs"] == 2
    assert attempts[0]["error"] == "HTTP 429 (will retry)" and attempts[0]["error_class"] == "transient"
    assert [host["failures"] for host in result.hosts.values()] == [0], "the success reset the streak"


def test_503_with_long_retry_after_is_deferred_to_a_later_run(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time)
    db = tmp_path / "shop.db"
    url = site.url("/busy/b?fail=1&after=7200")
    first = crawl(site, db, clock, seeds=[url], max_depth=0, retry={"max_retry_after": 600})
    assert first.status == "finished" and first.stats["deferred"] == 1
    assert [e["error"] for e in events(db, url)] == ["HTTP 503 (deferred)"]
    with Store(db) as store:
        assert store.db.execute("SELECT state FROM frontier WHERE url = ?", (key(url),)).fetchone()[0] == "deferred"
    clock.advance(3600)
    crawl(site, db, clock, seeds=[url], max_depth=0, retry={"max_retry_after": 600})
    assert len(events(db, url)) == 1, "not before the server's time"
    clock.advance(3601)
    crawl(site, db, clock, seeds=[url], max_depth=0, retry={"max_retry_after": 600})
    assert [e["status"] for e in events(db, url)] == [503, 200]


def test_permanent_errors_are_not_retried(site, tmp_path: Path) -> None:  # noqa: ANN001
    db = tmp_path / "shop.db"
    seeds = [site.url("/status/404"), site.url("/status/410"), site.url("/status/400"), site.url("/status/500")]
    result = crawl(site, db, seeds=seeds, max_depth=0, retry={"max_retries": 2})
    attempts: dict[str, list[str]] = {}
    for event in events(db):
        attempts.setdefault(event["url"].rsplit("/", 1)[1], []).append(event["error_class"])
    assert attempts == {"404": ["permanent"], "410": ["permanent"], "400": ["permanent"],
                        "500": ["transient"] * 3}
    assert result.stats["gave_up"] == 1 and result.stats["retries"] == 2


def test_host_backoff_uses_seeded_jitter(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time)
    retry = {"max_retries": 4, "backoff_base": 2, "backoff_factor": 2, "jitter": 0.5, "seed": 11}

    def run(name: str) -> list[float]:
        db = tmp_path / f"{name}.db"
        url = site.url(f"/busy/{name}?fail=4&after=0")
        crawl(site, db, clock, seeds=[url], max_depth=0, politeness={"min_delay": 0}, retry=retry,
              crawler={"rng": random.Random(11)})
        times = [e["fetched_at"] for e in events(db, url)]
        return [b - a for a, b in zip(times, times[1:])]

    first, second = run("x"), run("y")
    assert first == pytest.approx(second), "the same seed gives the same back-off schedule"
    ceilings = [BackoffPolicy(base=2, factor=2, jitter=0).ceiling(n) for n in range(4)]
    for gap, ceiling in zip(first, ceilings):
        assert ceiling <= gap <= ceiling * 1.5 + 0.5


def test_repeated_host_failures_quarantine_the_host(make_site, tmp_path: Path) -> None:  # noqa: ANN001
    clock = FakeClock()
    site = make_site(wall_clock=clock.time)
    db = tmp_path / "shop.db"
    seeds = [site.url(f"/busy/q{n}?fail=99&after=0") for n in range(6)]
    result = crawl(site, db, clock, seeds=seeds, max_depth=0, politeness={"min_delay": 0},
                   retry={"max_retries": 5, "backoff_base": 1, "jitter": 0, "host_failure_limit": 3,
                          "quarantine": 1800})
    assert result.status == "finished" and result.stats["hosts_quarantined"] == 1
    assert len(events(db)) == 3, "the host is left alone after the third consecutive failure"
    with Store(db) as store:
        states = dict(store.db.execute("SELECT state, COUNT(*) FROM frontier GROUP BY state").fetchall())
        assert states == {"deferred": 6}
        not_before = {row[0] for row in store.db.execute("SELECT not_before FROM frontier")}
        assert min(not_before) >= clock.time() - 60 + 1800 - 60
