"""Only real downloads use the pool; web stats never block requests; quiet broken pipes."""

from __future__ import annotations

import io
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
from tghoard.storage.chats_manifest import ChatSelection, render_chats_manifest
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.media_pool import MediaDownloadPool
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.web import server as web_server
from tghoard.web.server import WebUI, _QuietHTTPServer


@pytest.mark.asyncio
async def test_only_network_downloads_go_through_the_pool(tmp_path: Path, monkeypatch) -> None:
    submitted: list[int] = []
    real_submit = MediaDownloadPool.submit

    async def counting_submit(self, factory, *, group=""):
        submitted.append(1)
        return await real_submit(self, factory, group=group)

    monkeypatch.setattr(MediaDownloadPool, "submit", counting_submit)
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    video = FakeDocument(id=900, mime_type="video/mp4", size=2048)
    msgs = [
        FakeMessage(
            id=1,
            date=datetime(2026, 11, 9, tzinfo=UTC),
            media=FakeMediaPhoto(photo=FakePhoto(id=501)),
        ),  # photos: download
        FakeMessage(
            id=2,
            date=datetime(2026, 11, 9, 0, 1, tzinfo=UTC),
            media=FakeMediaDocument(document=video),
        ),  # videos unchecked
        FakeMessage(
            id=3,
            date=datetime(2026, 11, 9, 0, 2, tzinfo=UTC),
            media=FakeMediaPhoto(photo=FakePhoto(id=501)),
        ),  # same photo: reuse
    ]
    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"),
        dialogs=[FakeChannel(id=1, title="C", broadcast=True)],
        messages_by_entity_id={1: msgs},
    )
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_1": ChatSelection(
                chat_key="channel_1", title="C", section="channels", photos=True
            )
        },
        config=cfg,
        db=db,
    )
    db.close()
    assert client.download_calls == [501]
    assert stats.media_downloaded == 1 and stats.media_deferred == 1
    # message 3 was submitted before 1 finished, so it may count as a reuse via the pool;
    # the unchecked video never is.
    assert 1 <= len(submitted) <= 2


def test_web_stats_never_block_after_first_load(tmp_path: Path, monkeypatch) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    cfg.chats_manifest_path.write_text(
        render_chats_manifest([ChatSelection(chat_key="channel_1", title="A", section="channels")]),
        "utf-8",
    )
    ui = WebUI(
        data_dir=cfg.data_dir,
        manifest_path=cfg.chats_manifest_path,
        db_path=cfg.checkpoint_db_path,
        port=0,
    )
    calls: list[bool] = []
    real = ui._compute_stats

    def slow_compute(*, scan_folders: bool = True) -> Any:
        calls.append(scan_folders)
        if scan_folders:
            time.sleep(1.0)  # counting files on a big archive on a slow disk
        return real(scan_folders=scan_folders)

    monkeypatch.setattr(ui, "_compute_stats", slow_compute)
    monkeypatch.setattr(web_server, "STATS_TTL_SECS", 0.0)
    t0 = time.perf_counter()
    first = ui.chats_payload()  # database-only stats, never waits for the file count
    assert first["media_pending"] is True
    for _ in range(3):
        ui.chats_payload()  # cached result returned while one background count runs
    assert time.perf_counter() - t0 < 0.5
    time.sleep(1.3)
    assert calls == [False, True]  # one quick load + exactly one background count
    assert ui.chats_payload()["media_pending"] is False


def test_broken_pipe_is_silent_but_real_errors_are_reported(monkeypatch) -> None:
    printed = io.StringIO()
    monkeypatch.setattr(sys, "stderr", printed)
    srv = _QuietHTTPServer.__new__(_QuietHTTPServer)
    for exc in (BrokenPipeError(), ConnectionResetError()):
        try:
            raise exc
        except OSError:
            srv.handle_error(None, ("127.0.0.1", 1))
    assert printed.getvalue() == ""
    try:
        raise ValueError("bug real")
    except ValueError:
        srv.handle_error(None, ("127.0.0.1", 1))
    assert "bug real" in printed.getvalue()
