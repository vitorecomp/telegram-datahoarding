"""Download-speed time series and run counters behind the dashboard."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.conftest import (
    FakeChannel,
    FakeDocument,
    FakeMediaDocument,
    FakeMediaPhoto,
    FakeMessage,
    FakePhoto,
    FakeTelegramClient,
    FakeUser,
)
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync import live_state
from tghoard.sync.live_state import LiveDownloadSink, LiveSyncState
from tghoard.sync.orchestrator import run_multi_account_sync


def test_speed_series_from_samples(monkeypatch) -> None:
    clock = [1000.0]
    monkeypatch.setattr(live_state.time, "time", lambda: clock[0])
    live = LiveSyncState()
    live.set_phase("running")
    for _ in range(5):  # 2 MB every second
        live.snapshot()
        live.add_bytes(2 * 1048576)
        clock[0] += 1.0
    snap = live.snapshot()
    assert [round(v / 1048576, 2) for _, v in snap["speed"]] == [2.0, 2.0, 2.0, 2.0, 2.0]
    assert round(snap["speed_now"] / 1048576, 2) == 2.0
    # polls faster than 1/s don't add samples
    n = len(live._samples)
    live.snapshot()
    assert len(live._samples) == n
    live.set_phase("running")  # a new run resets counters and history
    assert live.snapshot()["bytes_total"] == 0 and live.snapshot()["speed"] == []


def test_download_sink_counts_bytes_once() -> None:
    live = LiveSyncState()
    sink = LiveDownloadSink(live, "video", 10_000)
    for done in (2_500, 2_500, 7_000, 10_000):  # repeated/progressive callbacks
        sink.update(completed=done, total=10_000)
    sink.remove()
    assert live.bytes_total == 10_000 and live.downloads == {}


@pytest.mark.asyncio
async def test_sync_feeds_counters(tmp_path: Path) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    big = FakeDocument(id=55, mime_type="application/pdf", size=3 * 1048576)
    msgs = [
        FakeMessage(
            id=1,
            date=datetime(2026, 11, 9, tzinfo=UTC),
            media=FakeMediaPhoto(photo=FakePhoto(id=501)),
        ),  # small -> counted at finish
        FakeMessage(
            id=2,
            date=datetime(2026, 11, 9, 0, 1, tzinfo=UTC),
            media=FakeMediaDocument(document=big),
        ),  # big -> counted via progress
        FakeMessage(id=3, date=datetime(2026, 11, 9, 0, 2, tzinfo=UTC), message="texto"),
    ]
    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"),
        dialogs=[FakeChannel(id=1, title="C", broadcast=True)],
        messages_by_entity_id={1: msgs},
    )
    live = LiveSyncState()
    await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_1": ChatSelection(
                chat_key="channel_1", title="C", section="channels", photos=True, files=True
            )
        },
        config=cfg,
        db=db,
        live=live,
    )
    db.close()
    snap = live.snapshot()
    assert snap["files_total"] == 2
    assert snap["messages_total"] == 3
    photo_bytes = len(b"binary-content-501")
    assert snap["bytes_total"] == 3 * 1048576 + photo_bytes  # big file once + small file
