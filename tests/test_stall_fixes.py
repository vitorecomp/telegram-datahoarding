"""Regressions for a stalled pending-media drain (head-of-line blocking) and a stuck throttle."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
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
)
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.live_state import LiveSyncState
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.telegram.flood_control import DynamicSlots, FloodController


@pytest.mark.asyncio
async def test_dynamic_slots_are_fifo_fair() -> None:
    slots = DynamicSlots(1)
    order: list[int] = []
    gate = asyncio.Event()

    async def job(i: int) -> None:
        async with slots:
            order.append(i)
            if i == 0:
                await gate.wait()
            await asyncio.sleep(0)

    tasks = [asyncio.create_task(job(i)) for i in range(8)]
    await asyncio.sleep(0.01)
    gate.set()
    await asyncio.gather(*tasks)
    assert order == list(range(8))  # strictly arrival order, nobody starves


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_leak_a_slot() -> None:
    slots = DynamicSlots(1)
    async with slots:
        waiter = asyncio.create_task(slots.__aenter__())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
    assert slots.active == 0
    async with slots:  # still usable
        assert slots.active == 1


def test_history_floods_do_not_block_download_recovery() -> None:
    c = FloodController(max_concurrency=8, recover_after=0.02, cut_cooldown=0)
    for _ in range(3):
        c.record(5, "GetFileRequest")
    assert c.concurrency == 1
    t_end = time.monotonic() + 0.2
    while time.monotonic() < t_end:  # history keeps flooding the whole time...
        c.record(5, "GetHistoryRequest")
        c.maybe_recover()
        time.sleep(0.005)
    assert c.concurrency > 1  # ...yet downloads recovered on their own timer
    assert c.history_wait > 0  # while the history pause stayed backed off


class StuckFirstDownloadClient(FakeTelegramClient):
    """The first media download hangs until released; the rest are instant."""

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.release = asyncio.Event()
        self.first_seen = False

    async def download_media(self, msg_or_media: Any, file: str, progress_callback: Any = None):
        if not self.first_seen:
            self.first_seen = True
            await self.release.wait()
        return await super().download_media(msg_or_media, file, progress_callback)


@pytest.mark.asyncio
async def test_drain_records_finished_downloads_while_one_is_stuck(tmp_path: Path) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", concurrency=4)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 0, i, tzinfo=UTC),
            media=FakeMediaPhoto(photo=FakePhoto(id=600 + i)),
        )
        for i in range(1, 11)
    ]
    dialogs = [FakeChannel(id=1, title="C", broadcast=True)]

    def sel(photos: bool) -> dict[str, ChatSelection]:
        return {
            "channel_1": ChatSelection(
                chat_key="channel_1", title="C", section="channels", photos=photos
            )
        }

    first = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"), dialogs=dialogs, messages_by_entity_id={1: msgs}
    )
    await run_multi_account_sync(
        account_clients=[(acc, first)], selections=sel(False), config=cfg, db=db
    )  # photos deferred -> 10 pending
    assert len(db.list_pending_media("channel_1", ["photos"])) == 10

    stuck = StuckFirstDownloadClient(
        me=FakeUser(id=111, first_name="V"), dialogs=dialogs, messages_by_entity_id={1: msgs}
    )
    live = LiveSyncState()
    run = asyncio.create_task(
        run_multi_account_sync(
            account_clients=[(acc, stuck)], selections=sel(True), config=cfg, db=db, live=live
        )
    )
    for _ in range(200):  # wait until the 9 other downloads are done
        await asyncio.sleep(0.01)
        if (
            len(stuck.download_calls) >= 9
            and len(db.list_pending_media("channel_1", ["photos"])) <= 1
        ):
            break
    # Recorded although the first (oldest) download is still hanging
    assert len(db.list_pending_media("channel_1", ["photos"])) == 1
    assert live.snapshot()["files_total"] == 9  # dashboard speed/counters keep moving
    stuck.release.set()
    await asyncio.wait_for(run, 5)
    assert db.list_pending_media("channel_1", ["photos"]) == []
    db.close()


def test_archive_lock_allows_a_single_writer(tmp_path: Path) -> None:
    from tghoard.storage.process_lock import ArchiveLockedError, archive_lock

    with archive_lock(tmp_path, "sync"):
        with pytest.raises(ArchiveLockedError) as err:
            with archive_lock(tmp_path, "watch"):
                pass
        assert "`sync`" in str(err.value)
    with archive_lock(tmp_path, "watch"):  # released after the first writer finished
        pass


@pytest.mark.asyncio
async def test_pool_worker_survives_a_dropped_connection() -> None:
    """Telethon cancels in-flight requests on disconnect: the job sees CancelledError."""
    from tghoard.sync.media_pool import MediaDownloadPool

    async def dropped() -> None:
        raise asyncio.CancelledError  # what a cancelled Telethon request future raises

    async def ok() -> str:
        return "ok"

    async with MediaDownloadPool(1) as pool:
        bad = await pool.submit(dropped, group="c")
        with pytest.raises(ConnectionError):
            await bad
        good = await pool.submit(ok, group="c")
        assert await asyncio.wait_for(good, 2) == "ok"  # the single worker is still alive
        assert all(not t.done() for t in pool._tasks)
