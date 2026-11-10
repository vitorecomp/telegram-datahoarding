"""Timestamped log output for `tghoard sync` (normal + `--verbose`) and web UI progress."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.markup import escape
from rich.text import Text

from tghoard.sync.live_state import (
    LiveAccountSink,
    LiveChatSink,
    LiveDownloadSink,
    LiveSyncState,
)


def human_bytes(n: int | None) -> str:
    if n is None:
        return "?"
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} B"


def human_duration(secs: float) -> str:
    secs = max(0.0, secs)
    if secs < 60:
        return f"{secs:.1f}s"
    mins, s = divmod(int(secs), 60)
    if mins < 60:
        return f"{mins}m{s:02d}s"
    hours, m = divmod(mins, 60)
    return f"{hours}h{m:02d}m"


class SyncReporter:
    """Prints timestamped lines, optionally scoped with a prefix (account / chat).

    - `info`: always shown (stages, progress per batch, downloads).
    - `debug`: shown only with `--verbose` (each RPC, dedup reuse, swallowed errors).
    """

    def __init__(
        self,
        console: Console | None = None,
        *,
        verbose: bool = False,
        prefix: str = "",
        enabled: bool = True,
        progress: SyncProgress | None = None,
        bar: BarHandle | None = None,
    ) -> None:
        self.console = console or Console()
        self.verbose = verbose
        self.prefix = prefix
        self.enabled = enabled
        self.progress = progress
        # Progress row owned by this scope (e.g. the chat being synced); no-op by default.
        self.bar = bar or NULL_BAR

    def child(self, label: str, *, bar: BarHandle | None = None) -> SyncReporter:
        """New reporter sharing console/verbosity/progress, with `label` appended to the prefix."""
        return SyncReporter(
            self.console,
            verbose=self.verbose,
            prefix=f"{self.prefix}{escape(label)} ",
            enabled=self.enabled,
            progress=self.progress,
            bar=bar if bar is not None else self.bar,
        )

    @property
    def live(self) -> LiveSyncState | None:
        """Web UI live state (None when no web UI / tests)."""
        return self.progress.live if self.progress is not None else None

    def add_bar(
        self,
        description: str,
        *,
        total: float | None,
        unit: str = "",
        kind: str = "chat",
        key: str | None = None,
    ) -> BarHandle:
        """Progress row (terminal and/or web UI). `kind`/`key` identify it for the web UI."""
        if self.progress is None or not self.enabled:
            return NULL_BAR
        return self.progress.add(description, total=total, unit=unit, kind=kind, key=key)

    def download_bar(self, description: str, size_bytes: int | None) -> BarHandle:
        """Per-file byte bar, only for large downloads (small files would just flicker)."""
        if (
            self.progress is None
            or not self.enabled
            or size_bytes is None
            or size_bytes < DOWNLOAD_BAR_MIN_BYTES
        ):
            return NULL_BAR
        return self.progress.add_download(description, total_bytes=size_bytes)

    def _emit(self, msg: str, style: str | None = None) -> None:
        if not self.enabled:
            return
        ts = datetime.now().strftime("%H:%M:%S")
        body = f"[{style}]{msg}[/{style}]" if style else msg
        self.console.print(f"[dim]{ts}[/dim] [dim]{self.prefix}[/dim]{body}", highlight=False)
        live = self.progress.live if self.progress is not None else None
        if live is not None:
            live.add_log(f"{self.prefix}{msg}")

    def info(self, msg: str) -> None:
        self._emit(msg)

    def warn(self, msg: str) -> None:
        self._emit(msg, "yellow")

    def debug(self, msg: str) -> None:
        if self.verbose:
            self._emit(msg, "dim")


class Stopwatch:
    def __init__(self) -> None:
        self.start = time.monotonic()

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start

    def __str__(self) -> str:
        return human_duration(self.elapsed)


# ---------------------------------------------------------------------------
# Progress rows (shown in the web UI; the terminal only gets log lines)
# ---------------------------------------------------------------------------

DOWNLOAD_BAR_MIN_BYTES = 1024 * 1024  # only show per-file bars for downloads >= 1 MB


class BarHandle:
    """One logical progress row fanned out to sinks (today: the web UI live state).

    All methods are no-ops without sinks, so sync code can update bars unconditionally.
    The terminal shows only log lines; progress bars live in the web UI.
    """

    def __init__(self, *sinks: Any) -> None:
        self._sinks = [s for s in sinks if s is not None]

    def advance(self, n: float = 1) -> None:
        for s in self._sinks:
            s.advance(n)

    def update(self, **kwargs: Any) -> None:
        for s in self._sinks:
            s.update(**kwargs)

    def finish(self) -> None:
        for s in self._sinks:
            s.finish()

    def remove(self) -> None:
        for s in self._sinks:
            s.remove()


NULL_BAR = BarHandle()


class SyncProgress:
    """Progress tracking mirrored into `LiveSyncState` for the web UI (no terminal bars)."""

    def __init__(self, *, live: LiveSyncState | None = None) -> None:
        self.live = live

    def add(
        self,
        description: str,
        *,
        total: float | None,
        unit: str = "",
        kind: str = "chat",
        key: str | None = None,
    ) -> BarHandle:
        if self.live is None or key is None:
            return NULL_BAR
        if kind == "account":
            self.live.account_update(key, total=total, completed=0)
            return BarHandle(LiveAccountSink(self.live, key))
        self.live.chat_update(key, total=total, completed=0, unit=unit or "msgs")
        return BarHandle(LiveChatSink(self.live, key))

    def add_download(self, description: str, *, total_bytes: int | None) -> BarHandle:
        if self.live is None:
            return NULL_BAR
        label = Text.from_markup(description).plain
        return BarHandle(LiveDownloadSink(self.live, label, total_bytes))


NULL_REPORTER = SyncReporter(enabled=False)
