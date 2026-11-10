"""Frontier leases, host slots and reaping of dead workers (single process, several owners)."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import time
from pathlib import Path

import pytest

from skein.clock import FakeClock
from skein.config.runtime import WorkerSettings
from skein.frontier import CrawlRequest, Frontier
from skein.politeness import HostScheduler, PolitenessPolicy
from skein.store import Store
from skein.workers import Coordination, pid_alive, worker_alive
from skein.workers.process import Child, Supervisor
from tests.fixtures_site import FixtureSite
from tests.helpers import PRODUCT_ITEMS, crawl

POLICY = PolitenessPolicy(min_delay=0.0, per_host_concurrency=8)


@pytest.fixture
def store(tmp_path: Path):  # noqa: ANN201
    with Store(tmp_path / "leases.db") as opened:
        yield opened


def owner(store: Store, clock: FakeClock, name: str, *, ttl: float = 30.0,
          attempts: int = 5) -> tuple[Frontier, HostScheduler]:
    frontier = Frontier(store.db, "s", clock, owner=name, lease_ttl=ttl, max_lease_attempts=attempts)
    return frontier, HostScheduler(store.db, POLICY, clock, owner=name, slot_ttl=ttl)


def lease_row(store: Store, url: str) -> dict[str, object]:
    return dict(store.db.execute("SELECT state, lease_owner, lease_expires, lease_attempts FROM frontier "
                                 "WHERE url = ?", (url,)).fetchone())


def test_claim_leases_and_settling_is_compare_and_set(store: Store) -> None:
    clock = FakeClock(1000.0)
    alice, alice_hosts = owner(store, clock, "alice")
    bob, bob_hosts = owner(store, clock, "bob")
    alice.add(CrawlRequest("http://a/1"))
    (claim,) = alice.claim(alice_hosts).claims
    assert claim.lease_attempts == 1 and claim.slot is not None and claim.slot.owner == "alice"
    assert lease_row(store, "http://a/1") == {"state": "active", "lease_owner": "alice",
                                              "lease_expires": 1030.0, "lease_attempts": 1}
    assert not bob.claim(bob_hosts).claims, "a leased request is invisible to others"
    clock.advance(20)
    assert alice.renew() == 1 and lease_row(store, "http://a/1")["lease_expires"] == 1050.0
    clock.advance(31)
    assert bob.reclaim().requeued == ["http://a/1"]
    (stolen,) = bob.claim(bob_hosts).claims
    assert stolen.lease_attempts == 2
    assert alice.complete(claim.request) is False and alice.stats.leases_lost == 1
    assert lease_row(store, "http://a/1")["lease_owner"] == "bob", "the new owner's lease is untouched"
    assert bob.complete(stolen.request) is True
    assert lease_row(store, "http://a/1") == {"state": "done", "lease_owner": None, "lease_expires": None,
                                              "lease_attempts": 0}
    assert alice.is_drained() and bob.held() == 0


def test_requests_that_keep_losing_their_lease_are_failed(store: Store) -> None:
    clock = FakeClock(1000.0)
    reaper, _ = owner(store, clock, "reaper", attempts=2)
    for attempt in range(2):
        crasher, hosts = owner(store, clock, f"crasher{attempt}", ttl=5)
        if attempt == 0:
            crasher.add(CrawlRequest("http://a/poison"))
        assert crasher.claim(hosts).claims
        clock.advance(6)
        report = reaper.reclaim()
        hosts.release_owner()
    assert report.poisoned == ["http://a/poison"] and not report.requeued
    assert lease_row(store, "http://a/poison")["state"] == "failed"


def test_rows_left_active_without_owner_are_reclaimed(store: Store) -> None:
    clock = FakeClock(1000.0)
    frontier, _ = owner(store, clock, "w")
    frontier.add(CrawlRequest("http://a/legacy"))
    store.db.execute("UPDATE frontier SET state = 'active'")
    assert frontier.reclaim().requeued == ["http://a/legacy"]


def exited_pid() -> int:
    process = subprocess.Popen([sys.executable, "-c", "pass"])
    process.wait()
    return process.pid


def zombie() -> subprocess.Popen[bytes]:
    process = subprocess.Popen([sys.executable, "-c", "pass"])
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        state = Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
        if state == "Z":
            break
        time.sleep(0.01)
    return process


def test_liveness_of_processes_and_workers(store: Store) -> None:
    assert pid_alive(subprocess.os.getpid()) and not pid_alive(exited_pid()) and not pid_alive(0)
    child = zombie()
    try:
        assert not pid_alive(child.pid), "an exited child nobody waited for is a zombie, not alive"
    finally:
        child.wait()
    with store.transaction():
        store.workers.register("far", "s", 1, hostname="elsewhere", pid=1, role="worker", checkpoint=None, now=100.0)
    record = store.workers.get("far")
    assert record is not None
    assert worker_alive(record, 150.0, 90.0) and not worker_alive(record, 191.0, 90.0)


def test_reaping_dead_workers_frees_their_slots_and_leases(store: Store) -> None:
    clock = FakeClock(1000.0)
    ghost, ghost_hosts = owner(store, clock, "ghost", ttl=600)
    ghost.add_many([CrawlRequest(f"http://a/{n}") for n in range(3)])
    claims = ghost.claim(ghost_hosts, limit=2).claims
    assert len(claims) == 2
    with store.transaction():
        store.workers.register("ghost", "s", 1, hostname=subprocess.os.uname().nodename, pid=exited_pid(),
                               role="worker", checkpoint=None, now=clock.time())
        store.workers.register("remote", "s", 1, hostname="elsewhere", pid=1, role="worker", checkpoint=None,
                               now=clock.time())
    survivor, survivor_hosts = owner(store, clock, "survivor")
    coordination = Coordination(store, survivor, survivor_hosts, clock, spider="s", worker_id="survivor",
                                settings=WorkerSettings(lease_ttl=600, heartbeat_interval=10))
    report = coordination.reap()
    assert report.dead == ["ghost"] and report.requeued == 2, "no need to wait for the 600 s lease"
    assert store.workers.get("ghost").state == "dead"  # type: ignore[union-attr]
    assert store.workers.get("remote").state == "running"  # type: ignore[union-attr]
    assert survivor_hosts.busy_slots("http://a:80") == 0
    clock.advance(1801)
    assert coordination.dead_peers() == ["remote"], "a remote worker is dead once its heartbeat is stale"
    coordination.beat()
    assert coordination.stats.beats == 1


def test_poisoned_requests_get_a_fetch_event(store: Store) -> None:
    clock = FakeClock(1000.0)
    crasher, hosts = owner(store, clock, "crasher", ttl=5, attempts=1)
    crasher.add(CrawlRequest("http://a/boom"))
    assert crasher.claim(hosts).claims
    clock.advance(10)
    survivor, survivor_hosts = owner(store, clock, "survivor", attempts=1)
    coordination = Coordination(store, survivor, survivor_hosts, clock, spider="s", worker_id="survivor",
                                settings=WorkerSettings(max_lease_attempts=1))
    assert coordination.reap().poisoned == ["http://a/boom"]
    (event,) = store.documents.events("s", url="http://a/boom")
    assert event["error"].startswith("abandoned") and event["error_class"] == "permanent"


def test_crawl_waits_for_a_remote_workers_lease_to_expire(site: FixtureSite, tmp_path: Path) -> None:
    clock = FakeClock()
    db = tmp_path / "shop.db"
    links = [{"allow": [r"/catalog\?page=\d+$", r"/product/\d+$"]}]
    first = crawl(site, db, clock, seeds=[site.url("/catalog?page=1")], max_pages=3,
                  extract={"items": PRODUCT_ITEMS, "links": links})
    assert first.status == "budget_exhausted"
    with Store(db) as store:
        pending = [r[0] for r in store.db.execute("SELECT url FROM frontier WHERE state = 'pending' LIMIT 2")]
        assert len(pending) == 2
        store.db.executemany("UPDATE frontier SET state = 'active', lease_owner = 'ghost', lease_expires = ?, "
                             "lease_attempts = 1 WHERE url = ?", [(clock.time() + 300, url) for url in pending])
        with store.transaction():
            store.workers.register("ghost", "shop", 1, hostname="elsewhere", pid=1, role="worker",
                                   checkpoint=None, now=clock.time())
    started = clock.time()
    second = crawl(site, db, clock, seeds=[site.url("/catalog?page=1")],
                   extract={"items": PRODUCT_ITEMS, "links": links}, incremental={"mode": "incremental"})
    assert second.status == "finished"
    assert second.stats["frontier"]["reclaimed"] == 2 and clock.time() >= started + 300
    with Store(db) as store:
        assert store.db.execute("SELECT COUNT(*) FROM frontier WHERE state <> 'done'").fetchone()[0] == 0
        assert store.items.count("shop") == 12


def test_watchdog_kills_a_worker_whose_heartbeat_stopped(store: Store, tmp_path: Path) -> None:
    supervisor = Supervisor(tmp_path / "unused.json", "s", 1)
    hung = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    supervisor.children.append(Child("w-hung", hung, "w-hung"))
    with store.transaction():
        store.workers.register("w-hung", "s", 1, hostname="here", pid=hung.pid, role="worker", checkpoint=None,
                               now=1000.0)
    try:
        assert supervisor.watchdog(store.workers, 1100.0, stale_after=180.0) == []
        assert supervisor.watchdog(store.workers, 1200.0, stale_after=180.0) == ["w-hung"]
        assert hung.wait(10) == -9
        assert supervisor.poll() == [] and supervisor.exit_codes == {"w-hung": [-9]}
    finally:
        if hung.poll() is None:
            hung.kill()
            hung.wait()


def test_coordination_notices_a_stopped_run(store: Store) -> None:
    clock = FakeClock(1000.0)
    frontier, hosts = owner(store, clock, "w")
    with store.transaction():
        store.db.execute("INSERT INTO runs (id, spider, started_at, status) VALUES (1, 's', 0, 'running')")
        store.workers.register("w", "s", 1, hostname="h", pid=1, role="worker", checkpoint=None, now=clock.time())
    coordination = Coordination(store, frontier, hosts, clock, spider="s", worker_id="w",
                                settings=WorkerSettings(lease_ttl=30, heartbeat_interval=5, reclaim_interval=5))
    stopped: list[bool] = []

    async def go() -> None:
        task = asyncio.ensure_future(coordination.run(1, lambda: stopped.append(True)))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not task.done() and coordination.stats.beats >= 1
        store.db.execute("UPDATE runs SET status = 'stopped'")
        await asyncio.wait_for(task, 5)

    asyncio.run(go())
    assert stopped == [True] and store.workers.get("w").heartbeat_at > 1000.0  # type: ignore[union-attr]
