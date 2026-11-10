"""Single-writer lock for an archive (`<data_dir>/.tghoard/archive.lock`).

Commands that write to the archive (sync, watch, start, account add/remove,
migrate --apply, verify-media --fix, rebuild-*) take an exclusive, non-blocking OS lock.
A second writer exits immediately with a clear message instead of racing on the same JSON
parts and checkpoint rows. The lock is released automatically if the process dies (it's an
flock on an open file descriptor).
"""

from __future__ import annotations

import contextlib
import json
import os
import time
from collections.abc import Iterator
from pathlib import Path

try:  # POSIX
    import fcntl
except ImportError:  # pragma: no cover - Windows: no cross-process lock
    fcntl = None  # type: ignore[assignment]


class ArchiveLockedError(RuntimeError):
    def __init__(self, path: Path, holder: dict[str, object] | None) -> None:
        self.path = path
        self.holder = holder or {}
        who = (
            f"`{self.holder.get('command', '?')}` (pid {self.holder.get('pid', '?')})"
            if self.holder
            else "another process"
        )
        super().__init__(f"the archive is in use by {who}; lock file: {path}")


@contextlib.contextmanager
def archive_lock(internal_dir: Path, command: str) -> Iterator[None]:
    """Hold the archive's writer lock for the duration of the block."""
    if fcntl is None:
        yield
        return
    internal_dir.mkdir(parents=True, exist_ok=True)
    path = internal_dir / "archive.lock"
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            holder: dict[str, object] | None
            try:
                holder = json.loads(os.pread(fd, 4096, 0).decode("utf-8") or "{}")
            except (OSError, ValueError):
                holder = None
            raise ArchiveLockedError(path, holder) from None
        info = json.dumps({"pid": os.getpid(), "command": command, "since": time.time()})
        os.ftruncate(fd, 0)
        os.pwrite(fd, info.encode("utf-8"), 0)
        try:
            yield
        finally:
            os.ftruncate(fd, 0)
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
