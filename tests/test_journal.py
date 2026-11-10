"""Crash-consistent files: record framing, the segmented journal, atomic snapshots, session state."""

from __future__ import annotations

import json
import os
import struct
import zlib
from pathlib import Path

import pytest

from skein.checkpoint import (
    Checkpoint,
    CheckpointError,
    CheckpointSettings,
    FsyncPolicy,
    Journal,
    JournalCorruptError,
    JournalError,
    SessionState,
    SnapshotError,
    atomic_write,
    frame,
    read_snapshot,
    scan,
    write_snapshot,
)
from skein.clock import FakeClock


def records(n: int, start: int = 1) -> list[dict]:
    return [{"seq": seq, "type": "stats", "delta": {"requests": 1}} for seq in range(start, start + n)]


def test_frame_layout_and_scan() -> None:
    data = frame(b"hello") + frame(b"world")
    assert struct.unpack_from("<II", data, 0) == (5, zlib.crc32(b"hello"))
    result = scan(data)
    assert result.payloads == [b"hello", b"world"] and result.valid_end == len(data)
    assert not result.torn and not result.corrupt and result.reason is None
    with pytest.raises(ValueError):
        frame(b"")


@pytest.mark.parametrize("cut", [1, 4, 7, 8, 9, 12])
def test_scan_tolerates_any_truncation_of_the_last_record(cut: int) -> None:
    good = frame(b"first record")
    data = good + frame(b"second record")[:cut]
    result = scan(data)
    assert result.payloads == [b"first record"]
    assert result.torn and not result.corrupt and result.valid_end == len(good)


def test_scan_distinguishes_torn_tail_from_corruption() -> None:
    first, second = frame(b"aaaa"), frame(b"bbbb")
    flipped_tail = first + second[:-1] + bytes([second[-1] ^ 0xFF])
    assert scan(flipped_tail).torn, "a bad checksum on the final record is a torn write"
    flipped_middle = first[:-1] + bytes([first[-1] ^ 0xFF]) + second
    result = scan(flipped_middle)
    assert result.corrupt and result.payloads == [] and "checksum mismatch" in (result.reason or "")
    zero_tail = first + b"\x00" * 64
    assert scan(zero_tail).torn and scan(zero_tail).reason == "zero-filled tail"


def test_journal_roundtrip_across_segments(tmp_path: Path) -> None:
    journal = Journal(tmp_path, fsync=FsyncPolicy.ALWAYS)
    journal.open(1)
    for record in records(3):
        journal.append(record)
    journal.open(4)
    for record in records(2, start=4):
        journal.append(record)
    journal.close()
    assert journal.syncs == 5 and journal.records_written == 5
    assert [first for first, _ in journal.segments()] == [1, 4]
    replay = journal.replay()
    assert [r["seq"] for r in replay.records] == [1, 2, 3, 4, 5]
    assert [r["seq"] for r in journal.replay(after_seq=3).records] == [4, 5]
    assert journal.replay(after_seq=3).skipped == 3


def test_journal_rejects_out_of_order_and_closed_appends(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    with pytest.raises(JournalError, match="not open"):
        journal.append({"seq": 1})
    journal.open(10)
    journal.append({"seq": 10})
    with pytest.raises(JournalError, match="must be greater"):
        journal.append({"seq": 10})
    journal.close()


def test_batch_and_never_fsync_policies(tmp_path: Path) -> None:
    clock = FakeClock()
    batch = Journal(tmp_path / "batch", fsync=FsyncPolicy.BATCH, batch_records=3, batch_interval=10.0,
                    now=clock.monotonic)
    batch.open(1)
    for record in records(7):
        batch.append(record)
    assert batch.syncs == 2, "after records 3 and 6"
    clock.advance(11)
    batch.append({"seq": 8})
    assert batch.syncs == 3, "the interval forces a sync"
    batch.close()
    never = Journal(tmp_path / "never", fsync=FsyncPolicy.NEVER)
    never.open(1)
    for record in records(5):
        never.append(record)
    never.close()
    assert never.syncs == 0 and len(never.replay().records) == 5


def test_torn_final_record_is_dropped_and_truncated(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    journal.open(1)
    for record in records(3):
        journal.append(record)
    journal.close()
    (_, segment) = journal.segments()[0]
    intact = segment.stat().st_size
    with open(segment, "ab") as handle:
        handle.write(frame(json.dumps({"seq": 4}).encode())[:-3])
    replay = journal.replay(repair=False)
    assert [r["seq"] for r in replay.records] == [1, 2, 3] and replay.torn_bytes > 0
    assert segment.stat().st_size > intact, "repair=False leaves the file alone"
    replay = journal.replay()
    assert replay.torn_bytes > 0 and segment.stat().st_size == intact
    assert journal.replay().torn_bytes == 0


def test_corruption_in_the_middle_is_an_error(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    journal.open(1)
    for record in records(3):
        journal.append(record)
    journal.close()
    (_, segment) = journal.segments()[0]
    data = bytearray(segment.read_bytes())
    data[20] ^= 0xFF
    segment.write_bytes(bytes(data))
    with pytest.raises(JournalCorruptError):
        journal.replay()


def test_incomplete_segment_header_is_discarded(tmp_path: Path) -> None:
    (tmp_path / "journal-0000000000000001.log").write_bytes(b"SKJ")
    journal = Journal(tmp_path)
    replay = journal.replay()
    assert replay.records == [] and replay.segments[0].reason == "incomplete segment header"
    assert journal.segments() == []


def test_prune_keeps_the_live_segment(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    for first in (1, 4, 7):
        journal.open(first)
        for record in records(3, start=first):
            journal.append(record)
    assert journal.prune(6) == 2
    assert [first for first, _ in journal.segments()] == [7]
    assert journal.prune(100) == 0, "the segment being written is never pruned"


def test_atomic_write_and_snapshot_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "snap" / "snapshot.bin"
    write_snapshot(path, {"seq": 1, "a": [1, 2]})
    write_snapshot(path, {"seq": 2, "a": [3]})
    assert read_snapshot(path) == {"seq": 2, "a": [3]}
    assert sorted(p.name for p in path.parent.iterdir()) == ["snapshot.bin"], "no temporary files remain"
    assert read_snapshot(tmp_path / "missing.bin") is None
    atomic_write(tmp_path / "plain.txt", b"x" * 100_000)
    assert (tmp_path / "plain.txt").read_bytes() == b"x" * 100_000


def test_damaged_snapshots_are_refused(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.bin"
    write_snapshot(path, {"seq": 1})
    data = path.read_bytes()
    path.write_bytes(data[:-2])
    with pytest.raises(SnapshotError, match="damaged"):
        read_snapshot(path)
    path.write_bytes(b"NOPE" + data[4:])
    with pytest.raises(SnapshotError, match="not a skein snapshot"):
        read_snapshot(path)


def make_state() -> SessionState:
    return SessionState("shop", run_id=7, mode="full", started_at=1000.0)


def test_session_state_applies_records() -> None:
    state = make_state()
    state.apply({"seq": 1, "type": "batch", "deltas": [{"requests": 1, "status": 200, "latency": 0.02},
                                                        {"requests": 1, "error": "TimeoutError"},
                                                        {"write_batches": 1}], "confirm": True})
    state.apply({"seq": 2, "type": "stats", "delta": {"robots_fetches": 1}})
    state.apply({"seq": 3, "type": "robots_sitemaps", "origin": "http://a:80"})
    state.apply({"seq": 4, "type": "resume"})
    assert state.seq == 4 and state.resumes == 1 and state.worker_id == "local-shop"
    assert state.stats.requests == 2 and dict(state.stats.status_counts) == {"200": 1}
    assert dict(state.stats.errors) == {"TimeoutError": 1} and state.stats.latency.count == 1
    assert state.stats.write_batches == 1 and state.stats.robots_fetches == 1
    assert state.robots_sitemaps == {"http://a:80"} and not state.legacy
    restored = SessionState.from_dict(json.loads(json.dumps(state.to_dict())))
    assert restored.to_dict() == state.to_dict()
    with pytest.raises(CheckpointError, match="unknown journal record type"):
        state.apply({"seq": 5, "type": "mystery"})
    with pytest.raises(KeyError):
        state.apply({"seq": 5, "type": "stats", "delta": {"nonsense": 1}})


def test_records_and_snapshots_written_by_0_2_are_collected_as_legacy_state() -> None:
    """0.2 journaled budget charges, host state and lastmod marks; 0.3 keeps them in SQLite."""
    snapshot = {"format": 1, "spider": "shop", "run_id": 3, "mode": "full", "started_at": 5.0, "seq": 2,
                "charged": ["http://a/1"], "hosts": {"http://a:80": {"crawl_delay": 2.0}},
                "robots_sitemaps": [], "lastmod_high": {"http://a/s.xml": 10.0}, "stats": {"requests": 4}}
    state = SessionState.from_dict(snapshot)
    assert state.worker_id == "local-shop" and state.stats.requests == 4
    state.apply({"seq": 3, "type": "charge", "url": "http://a/2"})
    state.apply({"seq": 4, "type": "fetch", "delta": {"pages_stored": 1}, "lastmod": {"http://a/s.xml": 30.0}})
    state.apply({"seq": 5, "type": "fetch", "delta": {"pages_stored": 1}, "lastmod": {"http://a/s.xml": 20.0}})
    state.apply({"seq": 6, "type": "host", "host": "http://a:80", "state": {"failures": 2}})
    assert state.legacy.charged == {"http://a/1", "http://a/2"}
    assert state.legacy.lastmod_high == {"http://a/s.xml": 30.0}, "the high-water mark never decreases"
    assert state.legacy.hosts == {"http://a:80": {"failures": 2}} and state.stats.pages_stored == 2
    assert SessionState.from_dict(json.loads(json.dumps(state.to_dict()))).legacy == state.legacy
    with pytest.raises(CheckpointError, match="format"):
        SessionState.from_dict({**snapshot, "format": 9})


def test_checkpoint_replays_journal_after_snapshot(tmp_path: Path) -> None:
    clock = FakeClock()
    settings = CheckpointSettings(fsync=FsyncPolicy.ALWAYS, snapshot_every=1000, snapshot_interval=1e9)
    checkpoint = Checkpoint(tmp_path, clock, settings)
    checkpoint.begin(make_state())
    for n in range(5):
        checkpoint.record("robots_sitemaps", origin=f"http://a{n}:80")
    checkpoint.close()
    loader = Checkpoint(tmp_path, clock, settings)
    state = loader.load()
    assert state is not None and state.seq == 5 and len(state.robots_sitemaps) == 5
    assert loader.recovery is not None and loader.recovery.snapshot_seq == 0 and loader.recovery.replayed == 5
    loader.resume(state)
    loader.record("robots_sitemaps", origin="http://after-restart:80")
    loader.snapshot()
    loader.close()
    again = Checkpoint(tmp_path, clock, settings)
    state = again.load()
    assert state is not None and state.resumes == 1 and len(state.robots_sitemaps) == 6
    assert again.recovery is not None and again.recovery.replayed == 0 and again.recovery.snapshot_seq == 7
    assert len(again.journal.segments()) == 1, "segments covered by the snapshot were pruned"


def test_unconfirmed_records_are_skipped_but_consume_their_sequence(tmp_path: Path) -> None:
    clock = FakeClock()
    checkpoint = Checkpoint(tmp_path, clock)
    checkpoint.begin(make_state())
    committed = checkpoint.write("batch", deltas=[{"pages_stored": 1}], confirm=True)
    checkpoint.apply(committed)
    checkpoint.write("batch", deltas=[{"pages_stored": 1}], confirm=True)  # the commit never happened
    checkpoint.record("robots_sitemaps", origin="http://a:80")
    checkpoint.close()
    loader = Checkpoint(tmp_path, clock)
    seen: list[tuple[int, str, int]] = []

    def confirm(run_id: int, worker: str, seq: int) -> bool:
        seen.append((run_id, worker, seq))
        return run_id == 7 and worker == "local-shop" and seq == committed["seq"]

    state = loader.load(confirm=confirm)
    assert state is not None and state.stats.pages_stored == 1 and state.seq == 3
    assert [s for _, _, s in seen] == [1, 2], "only confirmable records are checked"
    assert loader.recovery is not None and loader.recovery.unconfirmed == 1 and loader.recovery.replayed == 2
    loader.resume(state)
    assert loader.next_seq == 5


def test_automatic_snapshots_and_snapshot_without_new_records(tmp_path: Path) -> None:
    clock = FakeClock()
    checkpoint = Checkpoint(tmp_path, clock, CheckpointSettings(snapshot_every=3, snapshot_interval=1e9))
    checkpoint.begin(make_state())
    for n in range(7):
        checkpoint.record("robots_sitemaps", origin=f"http://a{n}:80")
        checkpoint.maybe_snapshot()
    assert checkpoint.snapshots == 3, "one at begin, then after records 3 and 6"
    checkpoint.snapshot()
    checkpoint.snapshot()
    checkpoint.close()
    state = Checkpoint(tmp_path, clock).load()
    assert state is not None and len(state.robots_sitemaps) == 7


def test_journal_without_snapshot_is_an_error_and_stale_temporaries_go(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    journal.open(1)
    journal.append({"seq": 1, "type": "resume"})
    journal.close()
    with pytest.raises(CheckpointError, match="no snapshot"):
        Checkpoint(tmp_path, FakeClock()).load()
    checkpoint = Checkpoint(tmp_path, FakeClock())
    checkpoint.discard()
    leftover = tmp_path / f".snapshot.bin.{os.getpid() + 1}.tmp"
    leftover.write_bytes(b"partial")
    assert checkpoint.load() is None and not leftover.exists()
    with pytest.raises(CheckpointError, match="no session"):
        checkpoint.record("resume")
