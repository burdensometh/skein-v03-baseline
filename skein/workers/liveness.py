"""Telling live workers from dead ones.

A worker on this machine is alive while its process exists (and is not a
zombie awaiting its parent) and its heartbeat is not stale. A worker on
another machine can only be judged by its heartbeat: once it is older
than the staleness limit the worker is presumed dead.
"""

from __future__ import annotations

import os
import secrets
import socket
from pathlib import Path

from skein.store.workers import WorkerRecord

HOSTNAME = socket.gethostname()


def pid_alive(pid: int) -> bool:
    """Whether process ``pid`` exists on this machine and has not exited (zombies count as dead)."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    stat = Path(f"/proc/{pid}/stat")
    try:
        fields = stat.read_text().rsplit(")", 1)[1].split()
    except (OSError, IndexError):
        return True
    return fields[0] != "Z"


def worker_alive(record: WorkerRecord, now: float, stale_after: float, hostname: str = HOSTNAME) -> bool:
    """Whether ``record`` describes a worker that is still working."""
    if record.state != "running":
        return False
    if now - record.heartbeat_at > stale_after:
        return False
    if record.hostname == hostname:
        return pid_alive(record.pid)
    return True


def new_worker_id() -> str:
    """A worker id unique across machines and restarts: ``<host>-<pid>-<random>``."""
    host = "".join(c if c.isalnum() or c in "-." else "-" for c in HOSTNAME)[:32] or "host"
    return f"{host}-{os.getpid()}-{secrets.token_hex(3)}"
