"""The politeness scheduler, per-host failure back-off and the fake clock."""

from __future__ import annotations

import asyncio
import math
import random

import sqlite3
from pathlib import Path
from typing import Iterator

import pytest

from skein.clock import FakeClock, SystemClock, isoformat
from skein.politeness import HostScheduler, HostSlot, PolitenessPolicy
from skein.retry import BackoffPolicy
from skein.store import Store


@pytest.fixture
def db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    with Store(tmp_path / "hosts.db") as store:
        yield store.db


T0 = 1_700_000_000.0


def test_scheduler_min_delay_spacing(db: sqlite3.Connection) -> None:
    clock = FakeClock(T0)
    scheduler = HostScheduler(db, PolitenessPolicy(min_delay=1.5), clock)
    host = "http://a:80"
    assert scheduler.is_ready(host)
    slot = scheduler.acquire(host)
    assert slot is not None and slot.start == T0
    assert scheduler.ready_at(host) == math.inf, "the only slot is taken"
    scheduler.release(slot)
    assert scheduler.ready_at(host) == T0 + 1.5
    assert scheduler.acquire(host) is None, "not ready yet"
    clock.advance(1.0)
    assert not scheduler.is_ready(host)
    clock.advance(0.5)
    assert scheduler.is_ready(host)


def test_release_re_anchors_spacing_on_the_actual_send(db: sqlite3.Connection) -> None:
    clock = FakeClock(T0)
    scheduler = HostScheduler(db, PolitenessPolicy(min_delay=2.0), clock)
    slot = scheduler.acquire("http://a:80")
    assert slot is not None
    clock.advance(5.0)  # the request waited in a queue before it was sent
    scheduler.release(slot, sent_at=clock.time())
    assert scheduler.ready_at("http://a:80") == T0 + 7.0


def test_crawl_delay_is_capped_and_applies_retroactively(db: sqlite3.Connection) -> None:
    clock = FakeClock(T0)
    scheduler = HostScheduler(db, PolitenessPolicy(min_delay=1.0, max_crawl_delay=5.0), clock)
    host = "http://a:80"
    slot = scheduler.acquire(host)
    assert slot is not None
    scheduler.release(slot)
    scheduler.set_crawl_delay(host, 3.0)
    assert scheduler.ready_at(host) == T0 + 3.0
    scheduler.set_crawl_delay(host, 3600)
    assert scheduler.delay_for(host) == 5.0
    ignoring = HostScheduler(db, PolitenessPolicy(min_delay=1.0, respect_crawl_delay=False), clock)
    ignoring.set_crawl_delay(host, 10)
    assert ignoring.delay_for(host) == 1.0


def test_per_host_concurrency_is_shared_between_processes(db: sqlite3.Connection) -> None:
    clock = FakeClock(T0)
    policy = PolitenessPolicy(min_delay=0.0, per_host_concurrency=2)
    first = HostScheduler(db, policy, clock, owner="w1", slot_ttl=30)
    second = HostScheduler(db, policy, clock, owner="w2", slot_ttl=30)
    host = "http://a:80"
    one = first.acquire(host)
    assert one is not None and second.is_ready(host)
    two = second.acquire(host)
    assert two is not None and {one.slot, two.slot} == {0, 1}
    assert not first.is_ready(host) and first.acquire(host) is None
    assert not first.release(HostSlot(host, two.slot, "w1", two.start)), "only the owner can give a slot back"
    assert first.release(one) and first.busy_slots(host) == 1
    scheduler = first
    scheduler.back_off(host, 30)
    assert scheduler.ready_at(host) == T0 + 30
    assert scheduler.snapshot()[host]["requests"] == 2


def test_slots_of_a_dead_owner_expire_or_are_released(db: sqlite3.Connection) -> None:
    clock = FakeClock(T0)
    policy = PolitenessPolicy(min_delay=0.0)
    dead = HostScheduler(db, policy, clock, owner="dead", slot_ttl=10)
    alive = HostScheduler(db, policy, clock, owner="alive", slot_ttl=10)
    assert dead.acquire("http://a:80") is not None and dead.acquire("http://b:80") is not None
    assert alive.acquire("http://a:80") is None
    clock.advance(5)
    assert dead.renew() == 2, "a heartbeat extends both slot leases"
    clock.advance(9)
    assert alive.acquire("http://a:80") is None, "renewed lease still holds"
    clock.advance(2)
    taken = alive.acquire("http://a:80")
    assert taken is not None and taken.owner == "alive", "an expired slot is free again"
    assert alive.release_owner("dead") == 1
    assert alive.acquire("http://b:80") is not None


def test_policy_validation() -> None:
    with pytest.raises(ValueError):
        PolitenessPolicy(min_delay=-1)
    with pytest.raises(ValueError):
        PolitenessPolicy(per_host_concurrency=0)


def test_fake_clock_semantics() -> None:
    clock = FakeClock(start=1000.0)

    async def go() -> None:
        await asyncio.gather(clock.sleep_until(5.0), clock.sleep_until(5.0), clock.sleep_until(3.0))
        assert clock.monotonic() == 5.0
        await clock.sleep(2.5)

    asyncio.run(go())
    assert clock.monotonic() == 7.5
    assert clock.time() == 1007.5
    with pytest.raises(ValueError):
        clock.advance(-1)
    clock.set_time(2000.0)
    assert clock.time() == 2000.0
    assert isoformat(0) == "1970-01-01T00:00:00Z"
    assert isoformat(None) is None


def test_system_clock_is_monotonic() -> None:
    clock = SystemClock()
    first = clock.monotonic()
    asyncio.run(clock.sleep(0.001))
    assert clock.monotonic() >= first
    assert clock.time() > 1_600_000_000


def test_host_failures_back_off_exponentially_with_seeded_jitter(db: sqlite3.Connection) -> None:
    clock = FakeClock(T0)
    backoff = BackoffPolicy(base=2.0, factor=2.0, max_delay=20.0, jitter=0.5)
    scheduler = HostScheduler(db, PolitenessPolicy(min_delay=0.0), clock, backoff=backoff, rng=random.Random(7))
    host = "http://a:80"
    pauses = [scheduler.host_failure(host) for _ in range(6)]
    reference = random.Random(7)
    expected = [min(20.0, backoff.ceiling(n) * (1 + 0.5 * reference.random())) for n in range(6)]
    assert pauses == expected
    for n, pause in enumerate(pauses):
        assert backoff.ceiling(n) <= pause <= min(20.0, backoff.ceiling(n) * 1.5)
    assert pauses[-1] == 20.0, "the cap holds even with jitter"
    assert scheduler.ready_at(host) == pytest.approx(T0 + max(pauses))
    assert scheduler.snapshot()[host]["failures"] == 6 and scheduler.snapshot()[host]["backoffs"] == 6


def test_retry_after_is_a_lower_bound_and_success_resets_the_streak(db: sqlite3.Connection) -> None:
    clock = FakeClock(T0)
    scheduler = HostScheduler(db, PolitenessPolicy(min_delay=0.0), clock,
                              backoff=BackoffPolicy(base=1.0, jitter=0.0), rng=random.Random(1))
    host = "http://a:80"
    assert scheduler.host_failure(host, retry_after=30.0) == 30.0
    assert scheduler.ready_at(host) == T0 + 30.0
    assert scheduler.host_failure(host) == 2.0, "second failure doubles the base"
    assert scheduler.host_success(host) is True
    assert scheduler.host_success(host) is False
    clock.advance(40)
    assert scheduler.host_failure(host) == 1.0, "the streak starts over after a success"


def test_host_state_is_shared_by_a_later_process(tmp_path: Path) -> None:
    """Back-off and Crawl-delay live in the database, so a restarted (or second) process honours them."""
    path = tmp_path / "hosts.db"
    clock = FakeClock(T0)
    with Store(path) as store:
        first = HostScheduler(store.db, PolitenessPolicy(min_delay=1.0), clock, owner="w1",
                              backoff=BackoffPolicy(base=10, jitter=0))
        first.set_crawl_delay("http://a:80", 4.0)
        first.host_failure("http://a:80", retry_after=120.0)
    later = FakeClock(T0 + 20)
    with Store(path) as store:
        second = HostScheduler(store.db, PolitenessPolicy(min_delay=1.0), later, owner="w2",
                               backoff=BackoffPolicy(base=10, jitter=0))
        assert second.delay_for("http://a:80") == 4.0
        assert second.ready_at("http://a:80") == pytest.approx(T0 + 120)
        assert second.state("http://a:80").failures == 1
        later.advance(100)
        assert second.acquire("http://a:80") is not None
