"""Bridge from the web UI thread to Telegram (chat avatars + server-side totals).

The web server runs in its own thread; Telethon clients belong to the sync's asyncio loop,
so every Telegram call is scheduled there with `run_coroutine_threadsafe`. Nothing here is
needed by the CLI: without a loop (standalone `tghoard web`) only cached data is served.

Caches (internal state, not part of the archive):
- `.tghoard/avatars/<chat_key>.jpg` (+ `<chat_key>.none` marker for chats without a photo)
- `.tghoard/remote_counts.json`: totals on Telegram (messages, photos, videos, audio, files)
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from typing import Any

from tghoard.storage.atomic import atomic_write_text

CHAT_KEY_RE = re.compile(r"^[a-z]+_[0-9_]+$")
NO_PHOTO_TTL_SECS = 24 * 3600
READY_WAIT_SECS = 45.0  # how long a request waits for chat registration to finish
COUNTS_TTL_SECS = 6 * 3600

# Telegram search filters summed into our 4 media categories (see media_manager classifier)
COUNT_FILTERS: dict[str, tuple[str, ...]] = {
    "photos": ("InputMessagesFilterPhotos",),
    "videos": (
        "InputMessagesFilterVideo",
        "InputMessagesFilterRoundVideo",
        "InputMessagesFilterGif",
    ),
    "audio": ("InputMessagesFilterMusic", "InputMessagesFilterVoice"),
    "files": ("InputMessagesFilterDocument",),
}


class TelegramBridge:
    def __init__(self, internal_dir: Path, *, concurrency: int = 2) -> None:
        self.avatar_dir = internal_dir / "avatars"
        self.counts_path = internal_dir / "remote_counts.json"
        self._entities: dict[str, tuple[Any, Any]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._sem: asyncio.Semaphore | None = None
        self._concurrency = concurrency
        self._inflight: dict[str, Future[Any]] = {}
        self._lock = threading.Lock()
        self._counts: dict[str, dict[str, Any]] = self._load_counts()
        # Set once all accounts' dialogs are registered. The page asks for every photo as soon
        # as it opens, usually while `sync` is still listing dialogs: wait instead of 404ing.
        self._ready = threading.Event()
        self.log: Any = None  # optional callable(str) -> web UI log (e.g. LiveSyncState.add_log)

    # -- registration (asyncio side) -------------------------------------------
    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def register(self, chat_key: str, client: Any, entity: Any) -> None:
        """Remember which client can query this chat (first account wins)."""
        self._entities.setdefault(chat_key, (client, entity))

    def mark_ready(self) -> None:
        self._ready.set()

    def _wait_registered(self, chat_key: str) -> bool:
        """True if `chat_key` can be queried, waiting for registration if it is in progress."""
        if chat_key in self._entities:
            return True
        if self._loop is None:
            return False
        self._ready.wait(timeout=READY_WAIT_SECS)
        return chat_key in self._entities

    def _log(self, msg: str) -> None:
        if self.log is not None:
            try:
                self.log(msg)
            except Exception:
                pass

    @property
    def can_fetch(self) -> bool:
        """True while connected to Telegram (during `sync`); standalone UI serves cache only."""
        return self._loop is not None

    # -- generic scheduling -------------------------------------------------------
    def _run(self, key: str, make_coro: Any, timeout: float) -> Any:
        """Run `make_coro()` once on the sync loop (dedup concurrent callers by `key`)."""
        if self._loop is None:
            return None
        with self._lock:
            fut = self._inflight.get(key)
            if fut is None:
                coro = make_coro()
                try:
                    fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
                except RuntimeError:  # the sync's event loop already stopped
                    coro.close()
                    return None
                self._inflight[key] = fut
        try:
            return fut.result(timeout=timeout)
        except Exception:
            return None
        finally:
            with self._lock:
                if self._inflight.get(key) is fut and fut.done():
                    del self._inflight[key]

    async def _limited(self, coro: Any) -> Any:
        if self._sem is None:
            self._sem = asyncio.Semaphore(self._concurrency)
        async with self._sem:
            return await coro

    # -- avatars ------------------------------------------------------------------
    def cached_avatar(self, chat_key: str) -> Path | None:
        if not CHAT_KEY_RE.match(chat_key):
            return None
        path = self.avatar_dir / f"{chat_key}.jpg"
        return path if path.exists() else None

    def _avatar_known_missing(self, chat_key: str) -> bool:
        try:
            marker = self.avatar_dir / f"{chat_key}.none"
            return time.time() - marker.stat().st_mtime < NO_PHOTO_TTL_SECS
        except FileNotFoundError:
            return False

    async def _fetch_avatar(self, chat_key: str) -> Path | None:
        client, entity = self._entities[chat_key]
        self.avatar_dir.mkdir(parents=True, exist_ok=True)
        target = self.avatar_dir / f"{chat_key}.jpg"
        tmp = self.avatar_dir / f".{chat_key}.tmp.jpg"
        try:
            result = await self._limited(
                client.download_profile_photo(entity, file=str(tmp), download_big=False)
            )
        except Exception as exc:
            # A failure is NOT "chat has no photo": don't cache it, let the next request retry.
            tmp.unlink(missing_ok=True)
            self._log(f"⚠ chat photo {chat_key} failed: {type(exc).__name__}: {exc}")
            return None
        if result and Path(result).exists() and Path(result).stat().st_size > 0:
            os.replace(result, target)
            return target
        tmp.unlink(missing_ok=True)
        # Telegram answered "no photo": remember it for a day
        atomic_write_text(self.avatar_dir / f"{chat_key}.none", "")
        return None

    def avatar(self, chat_key: str, *, timeout: float = 25.0) -> Path | None:
        """Cached avatar, or download it through the sync loop (blocks the web thread)."""
        if not CHAT_KEY_RE.match(chat_key):
            return None
        if (hit := self.cached_avatar(chat_key)) is not None:
            return hit
        if self._avatar_known_missing(chat_key) or not self._wait_registered(chat_key):
            return None
        return self._run(f"avatar:{chat_key}", lambda: self._fetch_avatar(chat_key), timeout)

    # -- totals on Telegram ---------------------------------------------------------
    def _load_counts(self) -> dict[str, dict[str, Any]]:
        try:
            return dict(json.loads(self.counts_path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            return {}

    def _save_counts(self) -> None:
        with self._lock:
            payload = json.dumps(self._counts, indent=1, sort_keys=True)
        atomic_write_text(self.counts_path, payload + "\n")

    def cached_counts(self, chat_key: str) -> dict[str, Any] | None:
        return self._counts.get(chat_key)

    def all_cached_counts(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._counts)

    async def _fetch_counts(self, chat_key: str) -> dict[str, Any] | None:
        from telethon.tl import functions
        from telethon.tl import types as tl_types

        client, entity = self._entities[chat_key]
        counts: dict[str, Any] = {}
        try:
            history = await self._limited(client.get_messages(entity, limit=0))
            counts["messages"] = int(getattr(history, "total", None) or 0)
        except Exception:
            return None
        names = [n for group in COUNT_FILTERS.values() for n in group]
        try:
            res = await self._limited(
                client(
                    functions.messages.GetSearchCountersRequest(
                        peer=entity, filters=[getattr(tl_types, n)() for n in names]
                    )
                )
            )
            by_filter = {type(c.filter).__name__: int(c.count) for c in res}
            for cat, group in COUNT_FILTERS.items():
                counts[cat] = sum(by_filter.get(n, 0) for n in group)
        except Exception:
            pass  # message total is still useful on its own
        counts["fetched_at"] = time.time()
        with self._lock:
            self._counts[chat_key] = counts
        self._save_counts()
        return counts

    def counts(self, chat_key: str, *, timeout: float = 25.0) -> dict[str, Any] | None:
        """Totals on Telegram: cached if fresh, else fetched through the sync loop."""
        if not CHAT_KEY_RE.match(chat_key):
            return None
        cached = self._counts.get(chat_key)
        fresh = cached is not None and time.time() - cached.get("fetched_at", 0) < COUNTS_TTL_SECS
        if fresh or not self._wait_registered(chat_key):
            return cached
        got = self._run(f"counts:{chat_key}", lambda: self._fetch_counts(chat_key), timeout)
        return got if got is not None else cached
