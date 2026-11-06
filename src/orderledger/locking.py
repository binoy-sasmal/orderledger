"""Exclusive per-workspace write lock.

DuckDB allows one writer process per database file. This lock turns an accidental second
writer into a clear, retryable rejection (or a bounded wait) instead of a low-level IO error
halfway through a run.

Scope of the protection (see docs/orchestration.md):
  * It covers every process that goes through `run_pipeline` (the CLI `run` and `backfill`
    commands, and therefore every Airflow task) on a filesystem that shares byte-range locks
    for the same file.
  * It does NOT stop someone opening wh.duckdb directly with another tool, and a copy of the
    workspace on another filesystem has its own lock.
"""

from __future__ import annotations

import contextlib
import os
import time
from collections.abc import Iterator
from pathlib import Path

LOCK_NAME = ".write.lock"


class WorkspaceBusy(Exception):
    """Another writer holds the workspace lock. Safe to retry later."""


def _try_lock(fd: int) -> bool:
    try:
        if os.name == "nt":
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False


def _unlock(fd: int) -> None:
    if os.name == "nt":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        with contextlib.suppress(OSError):
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)


@contextlib.contextmanager
def workspace_lock(ws: Path, wait_seconds: float = 0.0) -> Iterator[None]:
    """Hold the exclusive write lock for `ws`; raise WorkspaceBusy if not obtained in time."""
    ws.mkdir(parents=True, exist_ok=True)
    fd = os.open(ws / LOCK_NAME, os.O_RDWR | os.O_CREAT)
    deadline = time.monotonic() + max(0.0, wait_seconds)
    try:
        while not _try_lock(fd):
            if time.monotonic() >= deadline:
                raise WorkspaceBusy(
                    f"another writer holds the lock on {ws} (waited {wait_seconds:g}s); safe to retry"
                )
            time.sleep(0.2)
        try:
            yield
        finally:
            _unlock(fd)
    finally:
        os.close(fd)
