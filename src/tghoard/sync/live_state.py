"""Thread-safe live sync state shared between the asyncio sync loop and the web UI thread."""

from __future__ import annotations

import asyncio
import itertools
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from rich.text import Text

SPEED_SAMPLES = 1800  # 30 min of 1s samples

Phase = Literal["idle", "connecting", "waiting_user", "running", "done"]
ChatStatus = Literal["queued", "waiting_lock", "running", "done", "failed"]


@dataclass
class ChatLive:
    chat_key: str
    title: str
    account: str
    priority: int = 0
    status: ChatStatus = "queued"
    completed: float = 0
    total: float | None = None
    unit: str = "msgs"
    started_at: float | None = None
    finished_at: float | None = None
    messages_saved: int = 0
    media_downloaded: int = 0
    media_failed: int = 0
    error: str | None = None


@dataclass
class AccountLive:
    alias: str
    completed: float = 0
    total: float | None = None
    status: str = "running"
    started_at: float = field(default_factory=time.time)
    pace: str = ""  # auto-throttle state (pause per page, parallelism, FloodWaits)
    floods: int = 0
    dl_queued: int = 0  # media download pool: jobs waiting in the channel
    dl_active: int = 0  # ...downloading right now
    dl_limit: int = 0  # ...allowed at once (auto-throttle)
    dl_queue_max: int = 0  # channel size: backpressure kicks in when full


@dataclass
class DownloadLive:
    id: int
    label: str
    completed: float = 0
    total: float | None = None
    started_at: float = field(default_factory=time.time)


class LiveSyncState:
    """Snapshot-able sync status. All mutations take a lock; reads return plain dicts."""

    def __init__(self, *, log_size: int = 300) -> None:
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self.phase: Phase = "idle"
        self.message = ""
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.accounts: dict[str, AccountLive] = {}
        self.chats: dict[str, ChatLive] = {}
        self.downloads: dict[int, DownloadLive] = {}
        self.log: deque[dict[str, Any]] = deque(maxlen=log_size)
        self.runs = 0
        # Run counters + download-speed time series (sampled when the UI polls, <= 1/s)
        self.bytes_total = 0
        self.files_total = 0
        self.messages_total = 0
        self._samples: deque[tuple[float, int]] = deque(maxlen=SPEED_SAMPLES)
        # "Start sync" requests coming from the web UI, delivered into the asyncio loop.
        self._loop: asyncio.AbstractEventLoop | None = None
        self._start_event: asyncio.Event | None = None

    # -- loop integration ---------------------------------------------------
    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._start_event = asyncio.Event()

    @property
    def start_event(self) -> asyncio.Event:
        if self._start_event is None:
            self._start_event = asyncio.Event()
        return self._start_event

    def request_start(self) -> bool:
        """Called from the web thread: wakes a sync waiting for the user (like pressing Enter)."""
        if self._loop is None or self._start_event is None:
            return False
        try:
            self._loop.call_soon_threadsafe(self._start_event.set)
        except RuntimeError:  # loop already closed (sync shutting down)
            return False
        return True

    # -- phases -------------------------------------------------------------
    def set_phase(self, phase: Phase, message: str = "") -> None:
        with self._lock:
            self.phase = phase
            self.message = message
            if phase == "running":
                self.started_at = time.time()
                self.finished_at = None
                self.runs += 1
                self.accounts.clear()
                self.chats.clear()
                self.downloads.clear()
                self.bytes_total = self.files_total = self.messages_total = 0
                self._samples.clear()
            elif phase == "done":
                self.finished_at = time.time()

    def add_log(self, markup: str) -> None:
        plain = Text.from_markup(markup).plain if "[" in markup else markup
        with self._lock:
            self.log.append({"t": time.time(), "msg": plain})

    # -- throughput counters --------------------------------------------------
    def add_bytes(self, n: float) -> None:
        if n > 0:
            with self._lock:
                self.bytes_total += int(n)

    def add_files(self, n: int = 1) -> None:
        with self._lock:
            self.files_total += n

    def add_messages(self, n: int) -> None:
        with self._lock:
            self.messages_total += n

    def _speed_series(self, now: float) -> tuple[list[list[float]], float]:
        """Sample `bytes_total` (<= 1/s) and return ([[t, bytes/s], ...], current bytes/s)."""
        if not self._samples or now - self._samples[-1][0] >= 1.0:
            self._samples.append((now, self.bytes_total))
        pts = list(self._samples)
        series = [
            [t1, max(0.0, (b1 - b0) / (t1 - t0))]
            for (t0, b0), (t1, b1) in zip(pts, pts[1:], strict=False)
            if t1 > t0
        ]
        recent = [p for p in pts if now - p[0] <= 5.0] or pts[-1:]
        if len(recent) >= 2 and recent[-1][0] > recent[0][0]:
            current = (recent[-1][1] - recent[0][1]) / (recent[-1][0] - recent[0][0])
        else:
            current = 0.0
        return series, max(0.0, current)

    # -- accounts / chats -----------------------------------------------------
    def account_update(self, alias: str, **fields: Any) -> None:
        with self._lock:
            acc = self.accounts.setdefault(alias, AccountLive(alias=alias))
            for k, v in fields.items():
                if hasattr(acc, k):
                    setattr(acc, k, v)

    def set_queue(self, account: str, queued: list[tuple[str, str, int]]) -> None:
        """Replace the queued (not yet started) chats of `account`, in priority order."""
        with self._lock:
            keep = {
                k: c
                for k, c in self.chats.items()
                if not (c.account == account and c.status == "queued")
            }
            for chat_key, title, priority in queued:
                if chat_key in keep:
                    continue
                keep[chat_key] = ChatLive(
                    chat_key=chat_key, title=title, account=account, priority=priority
                )
            self.chats = keep

    def chat_update(self, chat_key: str, **fields: Any) -> None:
        with self._lock:
            chat = self.chats.get(chat_key)
            if chat is None:
                chat = ChatLive(
                    chat_key=chat_key,
                    title=str(fields.get("title", chat_key)),
                    account=str(fields.get("account", "")),
                )
                self.chats[chat_key] = chat
            for k, v in fields.items():
                if hasattr(chat, k):
                    setattr(chat, k, v)

    def chat_advance(self, chat_key: str, n: float) -> None:
        with self._lock:
            chat = self.chats.get(chat_key)
            if chat is not None:
                chat.completed += n

    # -- downloads ------------------------------------------------------------
    def download_start(self, label: str, total: float | None) -> int:
        with self._lock:
            dl_id = next(self._ids)
            self.downloads[dl_id] = DownloadLive(id=dl_id, label=label, total=total)
            return dl_id

    def download_update(self, dl_id: int, **fields: Any) -> None:
        with self._lock:
            dl = self.downloads.get(dl_id)
            if dl is not None:
                for k, v in fields.items():
                    if hasattr(dl, k):
                        setattr(dl, k, v)

    def download_end(self, dl_id: int) -> None:
        with self._lock:
            self.downloads.pop(dl_id, None)

    # -- read side ------------------------------------------------------------
    def snapshot(self, *, log_since: float | None = None) -> dict[str, Any]:
        with self._lock:
            chats = [asdict(c) for c in self.chats.values()]
            log = [e for e in self.log if log_since is None or e["t"] > log_since]
            now = time.time()
            speed, speed_now = self._speed_series(now)
            return {
                "bytes_total": self.bytes_total,
                "files_total": self.files_total,
                "messages_total": self.messages_total,
                "speed": speed,
                "speed_now": speed_now,
                "phase": self.phase,
                "message": self.message,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "now": now,
                "runs": self.runs,
                "accounts": [asdict(a) for a in self.accounts.values()],
                "running": [c for c in chats if c["status"] in ("running", "waiting_lock")],
                "queued": sorted(
                    (c for c in chats if c["status"] == "queued"),
                    key=lambda c: (-c["priority"], c["title"].lower()),
                ),
                "finished": [c for c in chats if c["status"] in ("done", "failed")],
                "downloads": [asdict(d) for d in self.downloads.values()],
                "log": log[-150:],
            }


# ---------------------------------------------------------------------------
# Progress sinks: a bar update fans out to the terminal (Rich) and/or live state
# ---------------------------------------------------------------------------


class LiveChatSink:
    def __init__(self, state: LiveSyncState, chat_key: str) -> None:
        self.state, self.chat_key = state, chat_key

    def advance(self, n: float) -> None:
        self.state.chat_advance(self.chat_key, n)

    def update(self, **kw: Any) -> None:
        self.state.chat_update(
            self.chat_key, **{k: v for k, v in kw.items() if k in ("completed", "total", "unit")}
        )

    def finish(self) -> None:
        pass

    def remove(self) -> None:
        pass  # chat status (done/failed) is set explicitly by the orchestrator


class LiveAccountSink:
    def __init__(self, state: LiveSyncState, alias: str) -> None:
        self.state, self.alias = state, alias

    def advance(self, n: float) -> None:
        with self.state._lock:
            acc = self.state.accounts.get(self.alias)
            if acc is not None:
                acc.completed += n

    def update(self, **kw: Any) -> None:
        self.state.account_update(
            self.alias, **{k: v for k, v in kw.items() if k in ("completed", "total")}
        )

    def finish(self) -> None:
        pass

    def remove(self) -> None:
        pass


class LiveDownloadSink:
    """Per-file progress; byte deltas also feed the download-speed counter."""

    def __init__(self, state: LiveSyncState, label: str, total: float | None) -> None:
        self.state = state
        self.dl_id = state.download_start(label, total)
        self._seen = 0.0

    def advance(self, n: float) -> None:
        with self.state._lock:
            dl = self.state.downloads.get(self.dl_id)
            if dl is not None:
                dl.completed += n

    def update(self, **kw: Any) -> None:
        if "completed" in kw and kw["completed"] is not None:
            done = float(kw["completed"])
            self.state.add_bytes(done - self._seen)
            self._seen = max(self._seen, done)
        self.state.download_update(
            self.dl_id, **{k: v for k, v in kw.items() if k in ("completed", "total")}
        )

    def finish(self) -> None:
        pass

    def remove(self) -> None:
        self.state.download_end(self.dl_id)


async def wait_for_user(start_event: asyncio.Event) -> Literal["enter", "web", "quit"]:
    """Wait for Enter on stdin *or* the web UI "start" button, whichever comes first.

    Returns "quit" on EOF (Ctrl+D) or when the user types `q`.
    """
    import sys

    loop = asyncio.get_running_loop()
    start_event.clear()
    line_fut: asyncio.Future[str] = loop.create_future()

    def _on_stdin() -> None:
        line = sys.stdin.readline()
        if not line_fut.done():
            wants_quit = not line or line.strip().lower() in ("q", "quit", "sair", "exit")
            line_fut.set_result("quit" if wants_quit else "enter")

    fd = sys.stdin.fileno()
    use_reader = True
    try:
        loop.add_reader(fd, _on_stdin)
    except (NotImplementedError, ValueError, OSError):
        use_reader = False  # e.g. Windows Proactor loop: fall back to a thread
    stdin_task: asyncio.Future[Any] = (
        line_fut if use_reader else asyncio.ensure_future(asyncio.to_thread(sys.stdin.readline))
    )
    web_task = asyncio.ensure_future(start_event.wait())
    try:
        done, _ = await asyncio.wait({stdin_task, web_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        if use_reader:
            loop.remove_reader(fd)
        web_task.cancel()
    if web_task in done and not stdin_task.done():
        return "web"
    result = stdin_task.result()
    if use_reader:
        return result  # type: ignore[no-any-return]
    return "quit" if not result else "enter"
