"""Crash-safe worker session state: a snapshot plus the journal of changes since.

Every process working on a run (the in-process crawler is the worker
``local``) keeps its own checkpoint. The :class:`SessionState` holds what
is not shared through SQLite: which run the worker works for, its share of
the run statistics and the origins whose robots.txt sitemaps it already
queued. Page budget charges, host politeness and back-off and the
per-root sitemap ``lastmod`` high-water marks moved into the database in
0.3, because several processes update them.

The state changes only by applying journal records, both live and during
replay, so a recovered state is identical to the one the crashed process
had after its last journaled change. Journals written by 0.2 (format 1
snapshots, ``fetch``/``charge``/``host`` records) still replay: what moved
to the database is collected in :attr:`SessionState.legacy` for the
crawler to import.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from skein.checkpoint.journal import FsyncPolicy, Journal, Replay
from skein.checkpoint.snapshot import read_snapshot, remove_stale_temporaries, write_snapshot
from skein.clock import Clock
from skein.metrics.stats import CrawlStats

log = logging.getLogger(__name__)

STATE_FORMAT = 2
LEGACY_FORMAT = 1
SNAPSHOT_NAME = "snapshot.bin"


class CheckpointError(RuntimeError):
    """The checkpoint directory holds state that cannot be used."""


@dataclass
class LegacyState:
    """State a 0.2 checkpoint kept that now lives in the database; imported once on resume."""

    charged: set[str] = field(default_factory=set)
    hosts: dict[str, dict[str, Any]] = field(default_factory=dict)
    lastmod_high: dict[str, float] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return bool(self.charged or self.hosts or self.lastmod_high)


@dataclass
class SessionState:
    """The replayable state of one worker's part in a crawl run."""

    spider: str
    run_id: int
    mode: str
    started_at: float
    worker_id: str = ""
    seq: int = 0
    resumes: int = 0
    robots_sitemaps: set[str] = field(default_factory=set)
    stats: CrawlStats = field(default_factory=CrawlStats)
    legacy: LegacyState = field(default_factory=LegacyState)

    def __post_init__(self) -> None:
        if not self.worker_id:
            self.worker_id = f"local-{self.spider}"

    def apply(self, record: dict[str, Any]) -> None:
        """Fold one journal record into the state."""
        kind = record.get("type")
        if kind == "batch":
            for delta in record["deltas"]:
                self.stats.apply(delta)
        elif kind == "stats":
            self.stats.apply(record["delta"])
        elif kind == "fetch":
            self.stats.apply(record["delta"])
            for root, value in record.get("lastmod", {}).items():
                previous = self.legacy.lastmod_high.get(root, float(value))
                self.legacy.lastmod_high[root] = max(float(value), previous)
        elif kind == "charge":
            self.legacy.charged.add(record["url"])
        elif kind == "host":
            self.legacy.hosts[record["host"]] = dict(record["state"])
        elif kind == "robots_sitemaps":
            self.robots_sitemaps.add(record["origin"])
        elif kind == "resume":
            self.resumes += 1
        else:
            raise CheckpointError(f"unknown journal record type {kind!r}")
        self.seq = int(record["seq"])

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": STATE_FORMAT,
            "spider": self.spider,
            "run_id": self.run_id,
            "mode": self.mode,
            "started_at": self.started_at,
            "worker_id": self.worker_id,
            "seq": self.seq,
            "resumes": self.resumes,
            "robots_sitemaps": sorted(self.robots_sitemaps),
            "stats": self.stats.to_state(),
            "legacy": {"charged": sorted(self.legacy.charged), "hosts": self.legacy.hosts,
                       "lastmod_high": self.legacy.lastmod_high},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SessionState":
        """Rebuild a state from :meth:`to_dict` output, or from a 0.2 (format 1) snapshot."""
        version = data.get("format")
        if version not in (STATE_FORMAT, LEGACY_FORMAT):
            raise CheckpointError(f"unsupported session state format {version!r}")
        legacy = data if version == LEGACY_FORMAT else data.get("legacy", {})
        try:
            return cls(
                spider=data["spider"],
                run_id=int(data["run_id"]),
                mode=data["mode"],
                started_at=float(data["started_at"]),
                worker_id=str(data.get("worker_id", f"local-{data['spider']}")),
                seq=int(data["seq"]),
                resumes=int(data.get("resumes", 0)),
                robots_sitemaps=set(data.get("robots_sitemaps", ())),
                stats=CrawlStats.from_state(data.get("stats", {})),
                legacy=LegacyState(
                    charged=set(legacy.get("charged", ())),
                    hosts={k: dict(v) for k, v in legacy.get("hosts", {}).items()},
                    lastmod_high={k: float(v) for k, v in legacy.get("lastmod_high", {}).items()},
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CheckpointError(f"invalid session state: {exc}") from exc


@dataclass(frozen=True)
class CheckpointSettings:
    """Durability and snapshot cadence of the session checkpoint."""

    fsync: FsyncPolicy = FsyncPolicy.BATCH
    fsync_batch: int = 64
    fsync_interval: float = 1.0
    snapshot_every: int = 500
    snapshot_interval: float = 30.0
    sqlite_synchronous: str = "normal"


@dataclass(frozen=True)
class Recovery:
    """What loading a checkpoint found."""

    snapshot_seq: int
    replayed: int
    unconfirmed: int
    torn_bytes: int
    segments: int

    def to_dict(self) -> dict[str, int]:
        return {"snapshot_seq": self.snapshot_seq, "replayed": self.replayed, "unconfirmed": self.unconfirmed,
                "torn_bytes": self.torn_bytes, "segments": self.segments}


class Checkpoint:
    """Owns one spider's checkpoint directory: ``snapshot.bin`` and ``journal-*.log``."""

    def __init__(self, directory: Path, clock: Clock, settings: CheckpointSettings | None = None) -> None:
        self.directory = Path(directory)
        self.clock = clock
        self.settings = settings or CheckpointSettings()
        self.journal = Journal(
            self.directory,
            fsync=self.settings.fsync,
            batch_records=self.settings.fsync_batch,
            batch_interval=self.settings.fsync_interval,
            now=clock.monotonic,
        )
        self.state: SessionState | None = None
        self.recovery: Recovery | None = None
        self.snapshots = 0
        self._written = 0
        self._since_snapshot = 0
        self._last_snapshot = clock.monotonic()

    @property
    def snapshot_path(self) -> Path:
        return self.directory / SNAPSHOT_NAME

    def exists(self) -> bool:
        return self.snapshot_path.exists() or bool(self.journal.segments())

    def load(
        self, *, confirm: Callable[[int, str, int], bool] | None = None, repair: bool = True
    ) -> SessionState | None:
        """Rebuild the last session from snapshot and journal; ``None`` if there is none.

        Records written with ``confirm=True`` belong to a database
        transaction; ``confirm(run_id, worker_id, seq)`` reports whether that
        transaction committed. Unconfirmed records (the process died between
        journaling and committing) are skipped, though their sequence
        numbers stay consumed.
        """
        if self.directory.is_dir():
            removed = remove_stale_temporaries(self.directory, SNAPSHOT_NAME)
            if removed:
                log.info("removed %d unfinished snapshot file(s) in %s", removed, self.directory)
        data = read_snapshot(self.snapshot_path)
        if data is None:
            if self.journal.segments():
                raise CheckpointError(f"{self.directory} has journal segments but no snapshot")
            return None
        state = SessionState.from_dict(data)
        replay: Replay = self.journal.replay(after_seq=state.seq, repair=repair)
        unconfirmed = 0
        for record in replay.records:
            if record.get("confirm") and confirm is not None and not confirm(
                    state.run_id, state.worker_id, int(record["seq"])):
                state.seq = int(record["seq"])
                unconfirmed += 1
                continue
            state.apply(record)
        self.recovery = Recovery(int(data["seq"]), len(replay.records) - unconfirmed, unconfirmed,
                                 replay.torn_bytes, len(replay.segments))
        if replay.torn_bytes:
            log.warning("checkpoint %s: dropped a torn journal record (%d bytes)", self.directory, replay.torn_bytes)
        return state

    def begin(self, state: SessionState) -> None:
        """Start checkpointing a new session (any previous files are replaced)."""
        self.discard()
        self.state = state
        self._written = state.seq
        self.snapshot()

    def resume(self, state: SessionState) -> None:
        """Continue a recovered session in this process."""
        self.state = state
        self._written = state.seq
        self.journal.open(self.next_seq)
        self.record("resume")

    @property
    def next_seq(self) -> int:
        """Sequence number the next journal record will get."""
        return max(self._require_state().seq, self._written) + 1

    def write(self, kind: str, **fields: Any) -> dict[str, Any]:
        """Journal a change without applying it yet (see :meth:`apply`).

        Used inside a database transaction: the record is on disk before the
        commit, and applied to the live state only once the commit succeeded.
        """
        entry = {"seq": self.next_seq, "type": kind, **fields}
        self.journal.append(entry)
        self._written = entry["seq"]
        return entry

    def apply(self, entry: dict[str, Any]) -> None:
        """Apply a record previously journaled with :meth:`write`."""
        self._require_state().apply(entry)
        self._since_snapshot += 1

    def record(self, kind: str, **fields: Any) -> None:
        """Journal a change and apply it to the live state."""
        self.apply(self.write(kind, **fields))

    def maybe_snapshot(self) -> bool:
        """Snapshot when enough records or time have accumulated; call between transactions."""
        if self._since_snapshot >= self.settings.snapshot_every or (
            self._since_snapshot and self.clock.monotonic() - self._last_snapshot >= self.settings.snapshot_interval
        ):
            self.snapshot()
            return True
        return False

    def snapshot(self) -> None:
        """Write the full state atomically, start a fresh segment and drop segments it covers."""
        state = self._require_state()
        self.journal.close()
        write_snapshot(self.snapshot_path, state.to_dict())
        self.journal.open(self.next_seq)
        self.journal.prune(state.seq)
        self.snapshots += 1
        self._since_snapshot = 0
        self._last_snapshot = self.clock.monotonic()

    def close(self) -> None:
        """Flush and close the journal, keeping everything needed to resume."""
        self.journal.close()

    def discard(self, *, remove_directory: bool = False) -> None:
        """Delete the snapshot and every journal segment (and the directory itself, if asked and empty)."""
        self.journal.remove_all()
        self.snapshot_path.unlink(missing_ok=True)
        if self.directory.is_dir():
            remove_stale_temporaries(self.directory, SNAPSHOT_NAME)
            if remove_directory and not any(self.directory.iterdir()):
                self.directory.rmdir()

    def _require_state(self) -> SessionState:
        if self.state is None:
            raise CheckpointError("no session has been started or resumed")
        return self.state
