"""MediaDownloadPool: fixed workers + bounded channel (backpressure, limits, group cancel)."""

from __future__ import annotations

import asyncio

import pytest

from tghoard.sync.media_pool import MediaDownloadPool


class Probe:
    def __init__(self) -> None:
        self.active = self.peak = 0
        self.order: list[int] = []

    def job(self, n: int, delay: float = 0.02, fail: bool = False):
        async def run() -> int:
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                await asyncio.sleep(delay)
                if fail:
                    raise ValueError(f"job {n} falhou")
                self.order.append(n)
                return n * 10
            finally:
                self.active -= 1

        return run


@pytest.mark.asyncio
async def test_fixed_workers_limit_results_and_errors() -> None:
    probe = Probe()
    async with MediaDownloadPool(4) as pool:
        tasks_before = len(asyncio.all_tasks())
        futs = [await pool.submit(probe.job(i), group="c") for i in range(12)]
        bad = await pool.submit(probe.job(99, fail=True), group="c")
        assert [await f for f in futs] == [i * 10 for i in range(12)]
        with pytest.raises(ValueError, match="job 99"):
            await bad
        assert probe.peak == 4  # never more than the workers
        assert len(asyncio.all_tasks()) == tasks_before  # no task per job: workers reused
        assert probe.order[:4] == [0, 1, 2, 3]  # FIFO
        assert pool.stats.done == 12 and pool.stats.failed == 1
    assert pool._tasks == []  # closed cleanly


@pytest.mark.asyncio
async def test_bounded_channel_applies_backpressure() -> None:
    probe = Probe()
    pool = MediaDownloadPool(1, queue_factor=1)  # 1 worker, channel of 16
    pool.start()
    gate = asyncio.Event()

    async def blocker() -> None:
        await gate.wait()

    await pool.submit(blocker)  # occupies the only worker
    await asyncio.sleep(0)
    for i in range(16):  # fills the channel
        await pool.submit(probe.job(i, delay=0))
    overflow = asyncio.ensure_future(pool.submit(probe.job(100, delay=0)))
    await asyncio.sleep(0.05)
    assert not overflow.done()  # reader is held back while the channel is full
    gate.set()
    fut = await asyncio.wait_for(overflow, 2)
    assert await fut == 1000
    await pool.close()


@pytest.mark.asyncio
async def test_queue_size_setting_controls_backpressure() -> None:
    pool = MediaDownloadPool(1, queue_size=20)  # TGHOARD_DOWNLOAD_QUEUE
    pool.start()
    gate = asyncio.Event()

    async def blocker() -> None:
        await gate.wait()

    await pool.submit(blocker)
    await asyncio.sleep(0)
    for _ in range(20):
        await pool.submit(blocker)
    assert pool.stats.queued == 20
    extra = asyncio.ensure_future(pool.submit(blocker))
    await asyncio.sleep(0.05)
    assert not extra.done()  # 21st waiting job: reader held back
    gate.set()
    await asyncio.wait_for(extra, 2)
    await pool.close()


@pytest.mark.asyncio
async def test_limit_follows_auto_throttle_at_runtime() -> None:
    probe = Probe()
    async with MediaDownloadPool(6) as pool:
        pool.set_limit(2)
        await asyncio.gather(*[await pool.submit(probe.job(i)) for i in range(8)])
        assert probe.peak == 2
        probe.peak = 0
        pool.set_limit(6)
        await asyncio.gather(*[await pool.submit(probe.job(i)) for i in range(12)])
        assert probe.peak == 6


@pytest.mark.asyncio
async def test_cancel_group_spares_workers_and_other_chats() -> None:
    probe = Probe()
    async with MediaDownloadPool(2) as pool:
        slow_a = [await pool.submit(probe.job(i, delay=5), group="chat_a") for i in range(2)]
        queued_a = await pool.submit(probe.job(9, delay=5), group="chat_a")
        other = await pool.submit(probe.job(7), group="chat_b")
        await asyncio.sleep(0.05)  # both workers busy with chat_a
        assert pool.cancel_group("chat_a") == 3
        for f in [*slow_a, queued_a]:
            with pytest.raises(asyncio.CancelledError):
                await f
        assert await asyncio.wait_for(other, 2) == 70  # workers survived, chat_b served
        assert probe.active == 0
        again = await pool.submit(probe.job(8), group="chat_a")
        assert await again == 80  # pool fully usable afterwards
        assert all(not t.done() for t in pool._tasks)


@pytest.mark.asyncio
async def test_sync_uses_a_fixed_pool_not_a_task_per_media(tmp_path) -> None:
    from datetime import UTC, datetime

    from tests.conftest import FakeChannel, FakeMediaPhoto, FakeMessage, FakePhoto, FakeUser
    from tests.test_performance_paths import SlowMediaClient
    from tghoard.config import AppConfig
    from tghoard.storage.chats_manifest import ChatSelection
    from tghoard.storage.checkpoint_db import CheckpointDB
    from tghoard.sync.live_state import LiveSyncState
    from tghoard.sync.orchestrator import run_multi_account_sync

    class TaskCountingClient(SlowMediaClient):
        max_tasks = 0

        async def download_media(self, msg_or_media, file, progress_callback=None):
            TaskCountingClient.max_tasks = max(self.max_tasks, len(asyncio.all_tasks()))
            return await super().download_media(msg_or_media, file, progress_callback)

    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", concurrency=4)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    ch = FakeChannel(id=1, title="Fotos", broadcast=True)
    msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, 0, i, tzinfo=UTC),
            media=FakeMediaPhoto(photo=FakePhoto(id=9000 + i)),
        )
        for i in range(1, 41)
    ]
    client = TaskCountingClient(
        0.15, me=FakeUser(id=111, first_name="V"), dialogs=[ch], messages_by_entity_id={1: msgs}
    )
    live = LiveSyncState()
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_1": ChatSelection(
                chat_key="channel_1", title="Fotos", section="channels", photos=True
            )
        },
        config=cfg,
        db=db,
        live=live,
    )
    db.close()
    assert stats.media_downloaded == 40
    assert client.max_active == 4
    # 40 media in one batch: with a task per media this would exceed 40; the pool keeps a
    # fixed set (4 workers + the sync's own tasks).
    assert TaskCountingClient.max_tasks < 20
    acct = live.snapshot()["accounts"][0]
    assert acct["dl_limit"] == 4 and acct["dl_active"] == 0 and acct["dl_queued"] == 0
