"""Concurrency limiter, jitter, and automatic FloodWaitError handling."""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from rich.console import Console
from rich.markup import escape
from telethon import errors as tg_errors

from tghoard.telegram.flood_control import DynamicSlots

T = TypeVar("T")

# Transient Telegram/network-side errors: safe to retry with backoff.
# `ServerError` covers RpcMcgetFailError, RpcCallFailError, InterdcCall*Error (HTTP 500).
TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    tg_errors.ServerError,
    tg_errors.TimedOutError,
    ConnectionError,
    asyncio.TimeoutError,
)

# Progressive backoff (seconds) for transient failures beyond Telethon's internal retry.
TRANSIENT_BACKOFF: tuple[float, ...] = (15.0, 30.0, 60.0, 120.0, 240.0)


def _warn(reporter: Any, console: Console, msg: str) -> None:
    if reporter is not None:
        reporter.warn(msg)
    else:
        console.print(f"[yellow]{msg}[/yellow]")


def is_transient_error(exc: BaseException) -> bool:
    """True for temporary errors (Telegram 500s, timeouts, dropped connections)."""
    return isinstance(exc, TRANSIENT_ERRORS)


DROPPED_CONNECTION_MSG = "request cancelled by Telethon (connection dropped)"


def is_spurious_cancellation() -> bool:
    """True when a `CancelledError` arrived although nobody cancelled the current task.

    Telethon cancels the futures of in-flight requests when its connection drops, so the
    awaiting coroutine sees `CancelledError` with `current_task().cancelling() == 0`. That is
    a transient network failure, not a request to stop. Real cancellation (Ctrl+C, shutdown,
    a TaskGroup or the download pool cancelling the task) always has `cancelling() > 0` and
    must propagate.
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() == 0


def dropped_connection_error() -> ConnectionError:
    return ConnectionError(DROPPED_CONNECTION_MSG)


def flood_wait_seconds(exc: BaseException) -> float | None:
    """Seconds to wait required by a FloodWait (or None if it is not a FloodWait)."""
    if isinstance(exc, tg_errors.FloodWaitError):
        return max(1, int(getattr(exc, "seconds", 1))) + random.uniform(0.5, 2.0)
    return None


async def retry_transient[R](
    fn: Callable[[], Awaitable[R]],
    *,
    description: str,
    console: Console,
    backoff: tuple[float, ...] = TRANSIENT_BACKOFF,
    reporter: Any = None,
    on_flood: Callable[[float, str], None] | None = None,
) -> R:
    """Retry the whole `fn` on FloodWait/transient errors with progressive backoff.

    Meant for idempotent, checkpointed units of work (e.g. syncing one chat), where a
    retry resumes from the last saved checkpoint. A `CancelledError` caused by Telethon
    dropping its connection (see `is_spurious_cancellation`) counts as a transient
    `ConnectionError`; real cancellation propagates untouched.
    """
    attempt = 0
    while True:
        try:
            return await fn()
        except asyncio.CancelledError:
            if not is_spurious_cancellation():
                raise  # really being cancelled (Ctrl+C / shutdown): never swallow it
            exc: Exception = dropped_connection_error()  # Telethon dropped the connection
        except Exception as caught:
            exc = caught
        wait = flood_wait_seconds(exc)
        if (
            wait is not None
            and on_flood is not None
            and not getattr(exc, "_tghoard_recorded", False)
        ):
            request = type(getattr(exc, "request", None)).__name__
            on_flood(float(getattr(exc, "seconds", wait)), request)
            exc._tghoard_recorded = True  # type: ignore[attr-defined]
        if wait is None and is_transient_error(exc):
            wait = backoff[attempt] if attempt < len(backoff) else None
            if wait is not None:
                wait += random.uniform(0.0, wait * 0.2)
        if wait is None or attempt >= len(backoff):
            raise exc
        attempt += 1
        _warn(
            reporter,
            console,
            f"⚠️  {type(exc).__name__} in {escape(description)}: {escape(str(exc))}. "
            f"Retrying in {wait:.0f}s ({attempt}/{len(backoff)})...",
        )
        await asyncio.sleep(wait)


class RateLimiter:
    """Wraps MTProto RPCs and media downloads with concurrency limits and FloodWait retry."""

    def __init__(
        self,
        *,
        concurrency: int = 3,
        min_delay: float = 0.05,
        max_delay: float = 0.20,
        console: Console | None = None,
        max_retries: int = 5,
        reporter: Any = None,
        controller: Any = None,
    ) -> None:
        # Fixed ceiling of simultaneous API requests. Deliberately NOT subscribed to the
        # controller: its `concurrency` knob is the *download* parallelism, cut only by
        # FloodWaits on file requests and applied to the download pool. FloodWaits on API
        # requests back off the pause between history pages instead (and the call that hit
        # one waits it out below), so a burst of GetFile floods never starves history,
        # metadata and get_messages calls.
        self.slots = DynamicSlots(max(1, concurrency))
        self.controller = controller  # only ticked (`maybe_recover`) and fed FloodWaits
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.console = console or Console()
        self.max_retries = max_retries
        # Optional (SyncReporter): with `--verbose`, logs every RPC and its duration.
        self.reporter = reporter

    async def call(
        self, fn: Callable[[], Awaitable[T]], *, description: str = "RPC", use_slots: bool = True
    ) -> T:
        """Execute async callable respecting semaphore, jitter, and FloodWaitError.

        Transient failures (500s, timeouts, dropped connections — including the
        `CancelledError` Telethon raises when it cancels in-flight requests on a disconnect)
        are retried with backoff; after `max_retries` the last one is raised (a dropped
        connection as `ConnectionError`). Real cancellation always propagates.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                if self.controller is not None:
                    self.controller.maybe_recover()
                # Jitter *before* taking a slot: never sleep while holding one.
                if self.max_delay > 0:
                    await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
                async with self.slots if use_slots else contextlib.nullcontext():
                    if self.reporter is None or not self.reporter.verbose:
                        return await fn()
                    t0 = time.monotonic()
                    result = await fn()
                    self.reporter.debug(f"↳ {escape(description)} ({time.monotonic() - t0:.2f}s)")
                    return result
            except tg_errors.FloodWaitError as exc:
                wait_secs = max(1, int(getattr(exc, "seconds", 1))) + random.uniform(0.5, 2.0)
                if self.controller is not None:
                    # classify by the real Telegram request (falls back to our description)
                    req = getattr(exc, "request", None)
                    name = type(req).__name__ if req is not None else description
                    self.controller.record(float(getattr(exc, "seconds", 1)), name)
                    exc._tghoard_recorded = True  # type: ignore[attr-defined]
                _warn(
                    self.reporter,
                    self.console,
                    f"⏳ FloodWait in {escape(description)}: waiting "
                    f"{wait_secs:.1f}s (attempt {attempt}/{self.max_retries})...",
                )
                if attempt > self.max_retries:
                    raise
                await asyncio.sleep(wait_secs)
            except asyncio.CancelledError:
                if not is_spurious_cancellation():
                    raise  # really being cancelled: never swallow it
                if attempt > self.max_retries:
                    raise dropped_connection_error() from None
                await self._transient_pause(attempt, "connection dropped", description)
            except TRANSIENT_ERRORS as exc:
                if attempt > self.max_retries:
                    raise
                await self._transient_pause(attempt, type(exc).__name__, description)

    async def _transient_pause(self, attempt: int, what: str, description: str) -> None:
        wait_secs = TRANSIENT_BACKOFF[min(attempt - 1, len(TRANSIENT_BACKOFF) - 1)]
        _warn(
            self.reporter,
            self.console,
            f"⚠️  {what} in {escape(description)}: waiting "
            f"{wait_secs:.0f}s (attempt {attempt}/{self.max_retries})...",
        )
        await asyncio.sleep(wait_secs)
