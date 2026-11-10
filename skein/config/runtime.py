"""Settings of the 0.3 runtime: pipeline queues, worker leases, de-duplication, archiving, metrics.

They sit at project level in the configuration file (groups ``pipeline``,
``workers``, ``dedup``, ``warc`` and ``metrics``) and are bundled in
:class:`RuntimeSettings`, which the crawler receives as a whole.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

NEAR_DUPLICATE_ACTIONS = ("record", "skip_links", "skip")


@dataclass(frozen=True)
class PipelineSettings:
    """Bounded queues between the fetch, parse and write stages, and the writer's batch size.

    ``parse_threads`` > 0 runs HTML parsing and extraction in a thread pool
    so that the event loop keeps serving sockets while large pages parse.
    """

    fetch_queue: int = 32
    parse_queue: int = 64
    write_queue: int = 128
    write_batch: int = 64
    parse_tasks: int = 2
    parse_threads: int = 0

    def __post_init__(self) -> None:
        for name in ("fetch_queue", "parse_queue", "write_queue", "write_batch", "parse_tasks"):
            if getattr(self, name) < 1:
                raise ValueError(f"pipeline.{name} must be >= 1")
        if self.parse_threads < 0:
            raise ValueError("pipeline.parse_threads must be >= 0")


@dataclass(frozen=True)
class WorkerSettings:
    """Lease and heartbeat timing for cooperating processes (seconds)."""

    lease_ttl: float = 60.0
    heartbeat_interval: float = 10.0
    max_lease_attempts: int = 5
    reclaim_interval: float = 5.0
    poll_interval: float = 0.5

    def __post_init__(self) -> None:
        if self.lease_ttl <= 0 or self.heartbeat_interval <= 0 or self.poll_interval <= 0:
            raise ValueError("worker timings must be positive")
        if self.heartbeat_interval >= self.lease_ttl:
            raise ValueError("workers.heartbeat_interval must be shorter than workers.lease_ttl")
        if self.max_lease_attempts < 1:
            raise ValueError("workers.max_lease_attempts must be >= 1")


@dataclass(frozen=True)
class DedupSettings:
    """Seen-URL Bloom filter and SimHash near-duplicate detection."""

    bloom: bool = True
    bloom_directory: Path | None = None
    bloom_capacity: int = 200_000
    bloom_error_rate: float = 1e-6
    bloom_verify_every: int = 64
    near_duplicates: bool = True
    max_distance: int = 3
    shingle_size: int = 3
    min_words: int = 8
    near_action: str = "record"

    def __post_init__(self) -> None:
        if self.near_action not in NEAR_DUPLICATE_ACTIONS:
            raise ValueError(f"dedup near-duplicate action must be one of {NEAR_DUPLICATE_ACTIONS}")


@dataclass(frozen=True)
class ArchiveSettings:
    """WARC output; disabled unless ``enabled``."""

    enabled: bool = False
    directory: Path | None = None
    prefix: str = "skein"
    max_size: int = 100 * 1024 * 1024
    metadata: bool = True
    revisits: bool = True
    compresslevel: int = 6


@dataclass(frozen=True)
class MetricsSettings:
    """Prometheus exposition: a textfile rewritten every ``interval`` seconds and/or an HTTP port."""

    textfile: Path | None = None
    interval: float = 15.0
    port: int | None = None


@dataclass(frozen=True)
class RuntimeSettings:
    pipeline: PipelineSettings = field(default_factory=PipelineSettings)
    workers: WorkerSettings = field(default_factory=WorkerSettings)
    dedup: DedupSettings = field(default_factory=DedupSettings)
    archive: ArchiveSettings = field(default_factory=ArchiveSettings)
    metrics: MetricsSettings = field(default_factory=MetricsSettings)

    def bloom_path(self, database: str, spider: str) -> Path | None:
        """Where the seen-URL filter of ``spider`` lives (``None``: in memory only)."""
        if not self.dedup.bloom:
            return None
        if self.dedup.bloom_directory is not None:
            return self.dedup.bloom_directory / f"{spider}.bloom"
        if database == ":memory:":
            return None
        return Path(f"{database}.seen") / f"{spider}.bloom"

    def archive_directory(self, database: str) -> Path:
        if self.archive.directory is not None:
            return self.archive.directory
        return Path(f"{database}.warc")
