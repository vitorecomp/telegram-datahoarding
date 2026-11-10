"""Speed-related behaviour: no forced history pause, parallel media, safe month-batched backfill."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import (
    FakeChannel,
    FakeMediaPhoto,
    FakeMessage,
    FakePhoto,
    FakeTelegramClient,
    FakeUser,
    chat_paths_for,
)
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.paths import iter_all_part_files
from tghoard.sync.orchestrator import run_multi_account_sync


def _cfg(tmp_path: Path, **kw: Any) -> tuple[AppConfig, CheckpointDB, Any]:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", **kw)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    return cfg, db, db.upsert_account(account_id=111, alias="conta1")


def _sel(key: str, **kw: Any) -> dict[str, ChatSelection]:
    return {key: ChatSelection(chat_key=key, title=key, section="channels", **kw)}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("override", "auto", "expected"),
    [(None, True, 0.0), (0.5, True, 0.0), (None, False, 0.0), (0.5, False, 0.5)],
)
async def test_history_pages_have_no_forced_pause(tmp_path: Path, override, auto, expected) -> None:
    cfg, db, acc = _cfg(tmp_path)
    cfg = cfg.with_overrides(history_wait=override, auto_throttle=auto)
    ch = FakeChannel(id=1, title="C", broadcast=True)
    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"),
        dialogs=[ch],
        messages_by_entity_id={1: [FakeMessage(id=1, date=datetime(2026, 11, 9, tzinfo=UTC))]},
    )
    await run_multi_account_sync(
        account_clients=[(acc, client)], selections=_sel("channel_1"), config=cfg, db=db
    )
    # Never Telethon's 1s default. With auto-throttle the pause is applied by our controller
    # (floor = --history-wait), so Telethon's own pause is 0; without it, the fixed setting.
    assert client.wait_times and set(client.wait_times) == {expected}
    db.close()


class SlowMediaClient(FakeTelegramClient):
    def __init__(self, delay: float, **kw: Any) -> None:
        super().__init__(**kw)
        self.delay, self.active, self.max_active = delay, 0, 0

    async def download_media(self, msg_or_media: Any, file: str, progress_callback: Any = None):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self.delay)
            return await super().download_media(msg_or_media, file, progress_callback)
        finally:
            self.active -= 1


@pytest.mark.asyncio
async def test_media_in_one_chat_downloads_in_parallel(tmp_path: Path) -> None:
    cfg, db, acc = _cfg(tmp_path, concurrency=4)
    ch = FakeChannel(id=1, title="Fotos", broadcast=True)
    n, delay = 12, 0.4
    msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, 0, i, tzinfo=UTC),
            message=f"#{i}",
            media=FakeMediaPhoto(photo=FakePhoto(id=7000 + i)),
        )
        for i in range(1, n + 1)
    ]
    client = SlowMediaClient(
        delay, me=FakeUser(id=111, first_name="V"), dialogs=[ch], messages_by_entity_id={1: msgs}
    )
    t0 = time.perf_counter()
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections=_sel("channel_1", photos=True),
        config=cfg,
        db=db,
    )
    elapsed = time.perf_counter() - t0
    assert stats.media_downloaded == n
    assert client.max_active == 4  # bounded by --concurrency, not 1-by-1
    assert elapsed < n * delay * 0.6  # sequential would take >= n * delay

    # Every message saved with its media recorded, in order
    part = chat_paths_for(cfg.data_dir, "channel_1").messages_dir / "2026-11-pt000.json"
    saved = json.loads(part.read_text("utf-8"))["messages"]
    assert [m["id"] for m in saved] == list(range(1, n + 1))
    assert all(m["media"]["downloaded"] for m in saved)
    db.close()


@pytest.mark.asyncio
async def test_abort_cancels_in_flight_downloads(tmp_path: Path) -> None:
    cfg, db, acc = _cfg(tmp_path, concurrency=3)
    ch = FakeChannel(id=1, title="Fotos", broadcast=True)
    msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, 0, i, tzinfo=UTC),
            media=FakeMediaPhoto(photo=FakePhoto(id=8000 + i)),
        )
        for i in range(1, 40)
    ]
    client = SlowMediaClient(
        5.0,
        me=FakeUser(id=111, first_name="V"),
        dialogs=[ch],
        messages_by_entity_id={1: msgs},
        fail_after_iter_count=20,
    )
    stats = await run_multi_account_sync(  # the chat aborts (recorded), the run completes
        account_clients=[(acc, client)],
        selections=_sel("channel_1", photos=True),
        config=cfg,
        db=db,
    )
    assert [k for _, k, _ in stats.failed_chats] == ["channel_1"]
    await asyncio.sleep(0)
    leftovers = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert leftovers == []  # no orphan downloads still running
    assert client.active == 0
    db.close()


@pytest.mark.asyncio
async def test_dm_backfill_batches_by_month_and_resumes_exactly(tmp_path: Path) -> None:
    cfg, db, acc = _cfg(tmp_path)
    start = datetime(2026, 8, 1, tzinfo=UTC)
    msgs = [  # ~3 months, newest-first backfill path (Saved Messages)
        FakeMessage(id=i, date=start + timedelta(hours=7 * i), sender_id=111, message=f"m{i}")
        for i in range(1, 301)
    ]
    sels = {"saved_111": ChatSelection(chat_key="saved_111", title="Saved", section="saved")}

    crashing = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"),
        dialogs=[],
        messages_by_entity_id={111: msgs},
        fail_after_iter_count=150,
    )
    stats = await run_multi_account_sync(
        account_clients=[(acc, crashing)], selections=sels, config=cfg, db=db
    )
    assert [k for _, k, _ in stats.failed_chats] == ["saved_111"]  # skipped, not fatal
    cursor = db.get_chat_cursor("saved_111", 111)
    assert cursor and not cursor["backfill_completed"]
    resume_from = cursor["backfill_offset_id"]
    assert resume_from is not None and 150 < resume_from < 300  # whole months written

    healthy = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"), dialogs=[], messages_by_entity_id={111: msgs}
    )
    await run_multi_account_sync(
        account_clients=[(acc, healthy)], selections=sels, config=cfg, db=db
    )
    assert db.get_chat_cursor("saved_111", 111)["backfill_completed"]

    ids: list[int] = []
    for part in iter_all_part_files(chat_paths_for(cfg.data_dir, "saved_111")):
        ids += [m["id"] for m in json.loads(part.read_text("utf-8"))["messages"]]
    assert ids == list(range(1, 301))  # nothing lost, nothing duplicated, in order
    db.close()
