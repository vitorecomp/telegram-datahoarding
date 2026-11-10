"""Tests for parallel chat sync: real concurrency, bounded slots and media dedup under races."""

from __future__ import annotations

import asyncio
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
    chat_paths_for,
)
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.orchestrator import run_multi_account_sync


class SlowClient(FakeTelegramClient):
    """Fake client that yields control while iterating and tracks overlapping chat syncs."""

    def __init__(self, *, barrier_size: int = 1, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.active = 0
        self.max_active = 0
        self.barrier_size = barrier_size
        self._barrier = asyncio.Event()

    async def iter_messages(self, entity: Any, **kwargs: Any):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        # Hold until `barrier_size` chats are iterating at once (proves real overlap);
        # the timeout keeps the test from hanging if parallelism were broken.
        if self.active >= self.barrier_size:
            self._barrier.set()
        try:
            await asyncio.wait_for(self._barrier.wait(), timeout=2.0)
        except TimeoutError:
            pass
        try:
            async for m in super().iter_messages(entity, **kwargs):
                await asyncio.sleep(0.01)
                yield m
        finally:
            self.active -= 1

    async def download_media(self, msg_or_media: Any, file: str) -> str:
        await asyncio.sleep(0.02)  # widen the race window between chats
        return await super().download_media(msg_or_media, file)


def _setup(tmp_path: Path, parallel_chats: int, n_chats: int, shared_photo: bool = False):
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", parallel_chats=parallel_chats)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    channels = [
        FakeChannel(id=5000 + i, title=f"Canal {i}", broadcast=True) for i in range(n_chats)
    ]
    msgs: dict[int, list[FakeMessage]] = {}
    for ch in channels:
        msgs[ch.id] = [
            FakeMessage(
                id=j,
                date=datetime(2026, 11, 9, 12, 0, j, tzinfo=UTC),
                message=f"{ch.title} #{j}",
                media=FakeMediaPhoto(photo=FakePhoto(id=424242))
                if shared_photo and j == 1
                else None,
            )
            for j in range(1, 6)
        ]
    selections = {
        f"channel_{ch.id}": ChatSelection(
            chat_key=f"channel_{ch.id}",
            title=ch.title,
            section="channels",
            baixar=True,
            photos=True,
        )
        for ch in channels
    }
    client = SlowClient(
        barrier_size=parallel_chats,
        me=FakeUser(id=111, first_name="Vitor"),
        dialogs=channels,
        messages_by_entity_id=msgs,
    )
    return cfg, db, acc, client, selections


@pytest.mark.asyncio
@pytest.mark.parametrize(("parallel", "expected_max"), [(1, 1), (3, 3)])
async def test_chats_sync_concurrently_up_to_parallel_limit(
    tmp_path: Path, parallel: int, expected_max: int
) -> None:
    cfg, db, acc, client, selections = _setup(tmp_path, parallel_chats=parallel, n_chats=6)
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)], selections=selections, config=cfg, db=db
    )
    assert client.max_active == expected_max
    assert stats.chats_synced == 6
    assert stats.messages_saved == 30
    for key in selections:
        assert db.get_synced_ranges(key) == [(1, 5)]
    db.close()


@pytest.mark.asyncio
async def test_same_media_in_parallel_chats_downloads_once(tmp_path: Path) -> None:
    cfg, db, acc, client, selections = _setup(
        tmp_path, parallel_chats=4, n_chats=4, shared_photo=True
    )
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)], selections=selections, config=cfg, db=db
    )
    assert client.download_calls == [424242]
    assert stats.media_downloaded == 1
    assert stats.media_dedup_reused == 3
    for i in range(4):
        photos = list(
            chat_paths_for(cfg.data_dir, f"channel_{5000 + i}").media_dir("photos").iterdir()
        )
        assert len(photos) == 1
    db.close()
