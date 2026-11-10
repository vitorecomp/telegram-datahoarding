"""Per-account media download pool: long-lived workers consuming a bounded channel.

    reader (sync_chat_messages) ──submit()──▶ [ bounded asyncio.Queue ] ──▶ N workers ──▶ Telegram
                                  ◀── Future ──────────────────────────────────────────┘

- Workers are created once per account and reused for every chat (no task per file).
- The channel is bounded: when it is full, `submit()` waits, so the message reader can't
  run arbitrarily ahead of the downloads (backpressure, steady memory).
- How many workers may download at once follows the auto-throttle (`set_limit`), between 1
  and the number of workers (`TGHOARD_CONCURRENCY`).
- Jobs carry a `group` (the chat). `cancel_group()` drops that chat's queued jobs (also the
  ones a worker already took but that still wait for a slot) and interrupts its running
  ones without killing the workers, which keep serving other chats.

Everything runs on the asyncio event loop (Telethon is async): "workers" are coroutines,
not OS threads.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from tghoard.telegram.flood_control import DynamicSlots


@dataclass
class _Job:
    factory: Callable[[], Awaitable[Any]]
    group: str
    future: asyncio.Future[Any]
    cancel_requested: bool = False


@dataclass
class PoolStats:
    queued: int = 0
    active: int = 0
    limit: int = 0
    workers: int = 0
    done: int = 0
    failed: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


class MediaDownloadPool:
    def __init__(
        self,
        workers: int,
        *,
        queue_size: int | None = None,
        queue_factor: int = 4,
        on_stats: Callable[[PoolStats], None] | None = None,
        name: str = "media",
    ) -> None:
        self.workers = max(1, workers)
        self._slots = DynamicSlots(self.workers)
        # Bounded channel: when full, submit() waits (backpressure on the message reader)
        self.queue_size = queue_size or max(16, self.workers * queue_factor)
        self._queue: asyncio.Queue[_Job] = asyncio.Queue(maxsize=self.queue_size)
        self._tasks: list[asyncio.Task[None]] = []
        self._running: dict[int, _Job] = {}
        # Job each worker has taken off the channel, from dequeue until it finishes —
        # including while it still waits for a download slot (then it's neither queued
        # nor in `_running`, and `cancel_group` must still reach it).
        self._claimed: dict[int, _Job] = {}
        self._cancel_tokens: set[int] = set()
        self._closing = False
        self._on_stats = on_stats
        self._name = name
        self.stats = PoolStats(limit=self.workers, workers=self.workers)
        self.stats.extra["queue_size"] = self.queue_size

    # -- lifecycle ------------------------------------------------------------------
    def start(self) -> MediaDownloadPool:
        if not self._tasks:
            self._tasks = [
                asyncio.create_task(self._worker(i), name=f"{self._name}-worker-{i}")
                for i in range(self.workers)
            ]
        return self

    async def close(self) -> None:
        """Stop the workers; any job not finished yet is cancelled."""
        self._closing = True
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        while not self._queue.empty():
            job = self._queue.get_nowait()
            if not job.future.done():
                job.future.cancel()
        self._emit()

    async def __aenter__(self) -> MediaDownloadPool:
        return self.start()

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # -- control --------------------------------------------------------------------
    def set_limit(self, limit: int) -> None:
        """How many workers may download at the same time (auto-throttle)."""
        self._slots.set_limit(min(self.workers, max(1, limit)))
        self.stats.limit = self._slots.limit
        self._emit()

    async def submit(
        self, factory: Callable[[], Awaitable[Any]], *, group: str = ""
    ) -> asyncio.Future[Any]:
        """Queue a job; waits while the channel is full. Returns the job's Future."""
        if self._closing:
            raise RuntimeError("pool closed")
        self.start()
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        await self._queue.put(_Job(factory=factory, group=group, future=future))
        self.stats.queued = self._queue.qsize()
        self._emit()
        return future

    def cancel_group(self, group: str) -> int:
        """Cancel every queued/claimed/running job of `group` (e.g. a chat that aborted)."""
        n = 0
        for job in list(self._queue._queue):  # type: ignore[attr-defined]
            if job.group == group and not job.future.done():
                job.cancel_requested = True
                job.future.cancel()
                n += 1
        for idx, job in list(self._claimed.items()):
            if job.group != group or job.cancel_requested:
                continue
            if self._running.get(idx) is job:
                # Downloading right now: interrupt the worker (it survives, see `_worker`)
                job.cancel_requested = True
                self._cancel_tokens.add(idx)
                self._tasks[idx].cancel()
                n += 1
            elif not job.future.done():
                # Dequeued but still waiting for a slot: cancelling its future is enough —
                # once the worker gets the slot, the `future.done()` check skips the job.
                job.cancel_requested = True
                job.future.cancel()
                n += 1
        return n

    # -- internals ------------------------------------------------------------------
    def _emit(self) -> None:
        self.stats.queued = self._queue.qsize()
        self.stats.active = len(self._running)
        if self._on_stats is not None:
            try:
                self._on_stats(self.stats)
            except Exception:
                pass

    async def _worker(self, idx: int) -> None:
        while True:
            current: _Job | None = None
            try:
                current = await self._queue.get()
                self._claimed[idx] = current
                try:
                    if current.future.done():  # cancelled while waiting in the channel
                        continue
                    async with self._slots:
                        if current.future.done():
                            continue
                        self._running[idx] = current
                        self._emit()
                        try:
                            result = await current.factory()
                        except asyncio.CancelledError:
                            task = asyncio.current_task()
                            if task is not None and task.cancelling() == 0:
                                # The JOB raised CancelledError without this worker being
                                # cancelled: Telethon cancels in-flight requests when the
                                # connection drops. That's a failed download, not a reason
                                # for the worker to die (it would never be replaced).
                                self.stats.failed += 1
                                if not current.future.done():
                                    current.future.set_exception(
                                        ConnectionError("download interrupted (connection dropped)")
                                    )
                                continue
                            raise
                        except BaseException as exc:  # noqa: BLE001 - delivered to awaiter
                            self.stats.failed += 1
                            if not current.future.done():
                                current.future.set_exception(exc)
                        else:
                            self.stats.done += 1
                            if not current.future.done():
                                current.future.set_result(result)
                        finally:
                            self._running.pop(idx, None)
                            self._emit()
                    # job finished without the group cancel reaching us (the job swallowed it
                    # or it raced): clear the token AND the pending cancellation, so the next
                    # cancel is classified correctly (real vs. connection drop)
                    if idx in self._cancel_tokens:
                        self._cancel_tokens.discard(idx)
                        me = asyncio.current_task()
                        if me is not None and me.cancelling():
                            me.uncancel()
                finally:
                    self._claimed.pop(idx, None)
                    self._queue.task_done()
            except asyncio.CancelledError:
                if current is not None and not current.future.done():
                    current.future.cancel()
                # Pool shutdown -> really stop. A `cancel_group` interrupt -> the job is
                # dropped, the cancellation cleared, and the worker keeps serving.
                if self._closing or idx not in self._cancel_tokens:
                    raise
                self._cancel_tokens.discard(idx)
                task = asyncio.current_task()
                if task is not None:
                    task.uncancel()
                self._emit()
