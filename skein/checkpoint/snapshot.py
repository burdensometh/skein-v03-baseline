"""Snapshot files replaced atomically: write a temporary file, fsync it, rename it over the old one."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import Any

from skein.checkpoint.framing import frame, scan

SNAPSHOT_MAGIC = b"SKS1"


class SnapshotError(RuntimeError):
    """A snapshot file exists but is damaged or not a skein snapshot."""


def fsync_directory(directory: Path) -> None:
    """Make a rename in ``directory`` durable (a no-op where directories cannot be opened)."""
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        with contextlib.suppress(OSError):
            os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path: Path, data: bytes) -> None:
    """Replace ``path`` with ``data`` so that readers see either the old or the new content.

    The temporary file lives in the same directory (``os.replace`` is only
    atomic within a file system) and is fsynced before the rename; the
    directory is fsynced afterwards so the rename itself survives a crash.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    except BaseException:
        os.close(fd)
        temporary.unlink(missing_ok=True)
        raise
    os.close(fd)
    os.replace(temporary, path)
    fsync_directory(path.parent)


def remove_stale_temporaries(directory: Path, name: str) -> int:
    """Delete temporary files a crash left behind while writing ``name``."""
    removed = 0
    for leftover in directory.glob(f".{name}.*.tmp"):
        leftover.unlink(missing_ok=True)
        removed += 1
    return removed


def write_snapshot(path: Path, state: dict[str, Any]) -> int:
    """Atomically store ``state`` as one framed JSON record; returns the file size."""
    payload = json.dumps(state, sort_keys=True, separators=(",", ":")).encode("utf-8")
    data = SNAPSHOT_MAGIC + frame(payload)
    atomic_write(path, data)
    return len(data)


def read_snapshot(path: Path) -> dict[str, Any] | None:
    """The stored state, ``None`` if there is no snapshot; :class:`SnapshotError` if it is damaged."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    if not data.startswith(SNAPSHOT_MAGIC):
        raise SnapshotError(f"{path} is not a skein snapshot")
    result = scan(data, len(SNAPSHOT_MAGIC))
    if result.reason is not None or len(result.payloads) != 1:
        raise SnapshotError(f"{path} is damaged: {result.reason or 'unexpected record count'}")
    try:
        state = json.loads(result.payloads[0])
    except ValueError as exc:
        raise SnapshotError(f"{path} holds invalid JSON: {exc}") from exc
    if not isinstance(state, dict):
        raise SnapshotError(f"{path} does not hold a state object")
    return state
