"""Concurrency limiter, jitter, and automatic FloodWaitError handling."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from rich.console import Console
from telethon import errors as tg_errors

T = TypeVar("T")


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
    ) -> None:
        self.semaphore = asyncio.Semaphore(max(1, concurrency))
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.console = console or Console()
        self.max_retries = max_retries

    async def call(self, fn: Callable[[], Awaitable[T]], *, description: str = "RPC") -> T:
        """Execute async callable respecting semaphore, jitter, and FloodWaitError."""
        attempt = 0
        while True:
            attempt += 1
            try:
                async with self.semaphore:
                    if self.max_delay > 0:
                        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
                    return await fn()
            except tg_errors.FloodWaitError as exc:
                wait_secs = max(1, int(getattr(exc, "seconds", 1))) + random.uniform(0.5, 2.0)
                self.console.print(
                    f"[yellow]⏳ FloodWait em {description}: aguardando "
                    f"{wait_secs:.1f}s (tentativa {attempt}/{self.max_retries})...[/yellow]"
                )
                if attempt > self.max_retries:
                    raise
                await asyncio.sleep(wait_secs)
