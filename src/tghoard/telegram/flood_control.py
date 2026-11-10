"""Per-account auto-throttle driven by FloodWaits (AIMD: back off fast, recover slowly).

Signals: Telethon silently sleeps FloodWaits shorter than `flood_sleep_threshold` (60s) and
only logs them (`'Sleeping for %ds (%s) on %s flood wait'`). Each account's client is created
with its own `base_logger` (`tghoard.tg.<account_id>`), so a logging handler can attribute
every sleep to the right account. Longer FloodWaits raise and are reported via `record()`.

Knobs adjusted (bounded by the user's settings):
- `history_wait`: pause between 100-message history pages. Floor = TGHOARD_HISTORY_WAIT.
- `concurrency`: parallel media downloads of the account (its download pool). Ceiling =
  TGHOARD_CONCURRENCY. API requests don't follow it: `RateLimiter` keeps a fixed number of
  API slots and API FloodWaits raise `history_wait` instead.

Rules:
- FloodWait on a file request     -> concurrency x cut_ratio (geometric progression, default
  (GetFile, GetCdnFile, ...)         1/2: 32 -> 16 -> 8 -> 4 -> 2 -> 1; min 1), at most once
                                     per CUT_COOLDOWN
- FloodWait on any other request  -> history_wait = max(2 * wait, 0.25s), capped at 5s
- every RECOVER_AFTER s w/o flood -> history_wait halves toward the floor and concurrency
                                     grows linearly (+1, capped at the ceiling)
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from typing import Any

from rich.markup import escape

HISTORY_REQUESTS = ("GetHistoryRequest", "SearchRequest", "GetRepliesRequest")
# Only FloodWaits on *file transfer* requests cut download parallelism; any other request
# (GetMessages, GetHistory, metadata...) backs off the API pause instead.
DOWNLOAD_REQUESTS = frozenset(
    {
        "GetFileRequest",
        "GetCdnFileRequest",
        "GetFileHashesRequest",
        "GetCdnFileHashesRequest",
        "ReuploadCdnFileRequest",
    }
)
CUT_COOLDOWN = 3.0  # one burst hitting N parallel downloads = one cut, not N


def is_download_request(name: str) -> bool:
    return name in DOWNLOAD_REQUESTS or name.startswith("download")


MAX_HISTORY_WAIT = 5.0
MIN_BACKOFF_WAIT = 0.25
RECOVER_AFTER = 20.0  # TGHOARD_THROTTLE_RECOVER_SECS
CUT_RATIO = 0.5  # TGHOARD_THROTTLE_CUT_RATIO


PAGE_SIZE = 100  # Telethon fetches history 100 messages per request

LOGGER_PREFIX = "tghoard.tg"


class FloodController:
    def __init__(
        self,
        *,
        base_wait: float = 0.0,
        max_concurrency: int = 3,
        enabled: bool = True,
        on_change: Callable[[str], None] | None = None,
        recover_after: float = RECOVER_AFTER,
        cut_ratio: float = CUT_RATIO,
        cut_cooldown: float = CUT_COOLDOWN,
    ) -> None:
        self.floor = max(0.0, base_wait)
        self.max_concurrency = max(1, max_concurrency)
        self.enabled = enabled
        self.history_wait = self.floor
        self.concurrency = self.max_concurrency
        self.floods = 0
        self.flood_seconds = 0.0
        self.recover_after = recover_after
        self.cut_ratio = min(0.95, max(0.05, cut_ratio))
        self.cut_cooldown = max(0.0, cut_cooldown)
        self._last_cut = float("-inf")
        # Independent quiet-period timers: history-page floods must not hold back the
        # recovery of download parallelism (and vice versa).
        now = time.monotonic()
        self._last_hist = now
        self._last_dl = now
        self._on_change = on_change
        self._listeners: list[Callable[[int], None]] = []  # concurrency subscribers

    # -- subscriptions --------------------------------------------------------------
    def on_concurrency(self, callback: Callable[[int], None]) -> None:
        self._listeners.append(callback)

    def describe(self) -> str:
        return (
            f"pause {self.history_wait:.2f}s/page · simultaneous downloads "
            f"{self.concurrency}/{self.max_concurrency} (--concurrency) · "
            f"{self.floods} FloodWait(s)"
        )

    def _changed(self, why: str) -> None:
        for cb in self._listeners:
            cb(self.concurrency)
        if self._on_change is not None:
            self._on_change(f"{why} → {self.describe()}")

    # -- signals --------------------------------------------------------------------
    def record(self, seconds: float, request_name: str) -> None:
        """A FloodWait happened (auto-slept by Telethon or raised)."""
        self.floods += 1
        self.flood_seconds += max(0.0, seconds)
        if not self.enabled:
            return
        now = time.monotonic()
        if not is_download_request(request_name):
            self._last_hist = now  # postpones only the API/history-pause recovery
            new_wait = min(MAX_HISTORY_WAIT, max(self.history_wait * 2, MIN_BACKOFF_WAIT))
            if new_wait != self.history_wait:
                self.history_wait = new_wait
                self._changed(f"🐢 FloodWait of {seconds:.0f}s on {escape(request_name)}")
        else:
            self._last_dl = now  # postpones only the download-parallelism recovery
            if now - self._last_cut < self.cut_cooldown:
                return  # same burst (several parallel downloads hit it): already cut
            self._last_cut = now
            new_conc = max(1, int(self.concurrency * self.cut_ratio))  # geometric cut
            if new_conc != self.concurrency:
                self.concurrency = new_conc
                self._changed(f"🐢 FloodWait of {seconds:.0f}s on {escape(request_name)}")

    def maybe_recover(self) -> None:
        """Step back toward full speed after a quiet period (cheap; call often).

        The history pause and download parallelism recover independently, each after its
        own quiet period.
        """
        if not self.enabled:
            return
        now = time.monotonic()
        changed: list[str] = []
        if now - self._last_hist >= self.recover_after:
            self._last_hist = now
            halved = self.history_wait / 2
            # Snap to the floor once below the smallest back-off step (no slow creep to 0)
            new_wait = halved if halved >= max(self.floor + 0.01, MIN_BACKOFF_WAIT) else self.floor
            if new_wait != self.history_wait:
                self.history_wait = new_wait
                changed.append("pause")
        if now - self._last_dl >= self.recover_after:
            self._last_dl = now
            new_conc = min(self.max_concurrency, self.concurrency + 1)  # linear +1
            if new_conc != self.concurrency:
                self.concurrency = new_conc
                changed.append("downloads")
        if changed:
            self._changed(f"🐇 no FloodWait for {self.recover_after:.0f}s ({', '.join(changed)})")

    # -- pacing ---------------------------------------------------------------------
    async def page_pause(self) -> None:
        self.maybe_recover()
        if self.history_wait > 0:
            await asyncio.sleep(self.history_wait)


async def paced[T](
    iterator: AsyncIterator[T], controller: FloodController | None
) -> AsyncIterator[T]:
    """Yield from a Telethon history iterator, pausing between 100-message pages."""
    count = 0
    async for item in iterator:
        yield item
        count += 1
        if controller is not None and count % PAGE_SIZE == 0:
            await controller.page_pause()


class DynamicSlots:
    """FIFO-fair semaphore whose limit can change at runtime (driven by `FloodController`).

    Waiters are served strictly in arrival order, so no job can starve while newer jobs
    keep taking the slots (important when the limit is cut down to 1).
    """

    def __init__(self, limit: int) -> None:
        self.limit = max(1, limit)
        self.active = 0
        self._waiters: deque[asyncio.Future[None]] = deque()

    def set_limit(self, limit: int) -> None:
        self.limit = max(1, limit)
        self._wake()

    def _wake(self) -> None:
        while self._waiters and self.active < self.limit:
            fut = self._waiters.popleft()
            if not fut.done():
                self.active += 1  # slot handed over to this waiter
                fut.set_result(None)

    async def __aenter__(self) -> DynamicSlots:
        if not self._waiters and self.active < self.limit:
            self.active += 1
            return self
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                self.active -= 1  # slot was granted just as we were cancelled: give it back
                self._wake()
            else:
                try:
                    self._waiters.remove(fut)
                except ValueError:
                    pass
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        self.active -= 1
        self._wake()


# ---------------------------------------------------------------------------
# Registry + Telethon log hook
# ---------------------------------------------------------------------------

_CONTROLLERS: dict[int, FloodController] = {}


def register(account_id: int, controller: FloodController) -> None:
    _CONTROLLERS[int(account_id)] = controller
    install_log_hook()


def unregister(account_id: int) -> None:
    _CONTROLLERS.pop(int(account_id), None)


def client_logger_name(account_id: int) -> str:
    """`base_logger` to pass to `TelegramClient` so its flood sleeps reach the controller."""
    return f"{LOGGER_PREFIX}.{int(account_id)}"


class _TelethonFloodHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = str(record.msg)
            if "flood wait" not in msg or not record.args:
                return
            parts = record.name.split(".")
            account_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
            controller = _CONTROLLERS.get(account_id) if account_id is not None else None
            if controller is None:
                return
            # args = (" early" | "", seconds, timedelta, request_class_name)
            args: Any = record.args
            controller.record(float(args[1]), str(args[3]))
        except Exception:
            pass  # never let throttling break Telethon


_hook_installed = False


def install_log_hook() -> None:
    global _hook_installed
    if _hook_installed:
        return
    logger = logging.getLogger(LOGGER_PREFIX)
    logger.setLevel(logging.INFO)  # Telethon logs flood sleeps at INFO
    logger.propagate = False  # keep Telethon's INFO chatter out of the terminal...
    logger.addHandler(_TelethonFloodHandler())
    warnings = logging.StreamHandler()  # ...but still show its warnings/errors, as before
    warnings.setLevel(logging.WARNING)
    warnings.setFormatter(logging.Formatter("[telethon] %(levelname)s: %(message)s"))
    logger.addHandler(warnings)
    _hook_installed = True
