"""Chat avatars + Telegram totals through the web UI bridge, and migration 0002 (empty dirs)."""

from __future__ import annotations

import asyncio
import json
import threading
import urllib.request
from pathlib import Path
from typing import Any

import pytest
from telethon.tl import types as tl_types
from typer.testing import CliRunner

from tghoard.cli import app
from tghoard.config import AppConfig
from tghoard.migrations import load_applied, pending_migrations
from tghoard.storage.chats_manifest import ChatSelection, render_chats_manifest
from tghoard.web.server import WebUI
from tghoard.web.telegram_bridge import TelegramBridge

JPEG = b"\xff\xd8\xff\xe0fake-jpeg-bytes"


class _Total(list):
    total = 0


class FakeTgClient:
    """Just the calls the bridge uses: profile photo, history total, search counters."""

    def __init__(self, *, has_photo: bool = True) -> None:
        self.has_photo = has_photo
        self.photo_calls = 0
        self.count_calls = 0

    async def download_profile_photo(self, entity: Any, file: str, download_big: bool = True):
        self.photo_calls += 1
        if not self.has_photo:
            return None
        Path(file).write_bytes(JPEG)
        return file

    async def get_messages(self, entity: Any, limit: int | None = None, **_: Any):
        self.count_calls += 1
        res = _Total()
        res.total = 1234
        return res

    async def __call__(self, request: Any) -> Any:
        counts = {
            "InputMessagesFilterPhotos": 300,
            "InputMessagesFilterVideo": 40,
            "InputMessagesFilterRoundVideo": 2,
            "InputMessagesFilterGif": 8,
            "InputMessagesFilterMusic": 5,
            "InputMessagesFilterVoice": 7,
            "InputMessagesFilterDocument": 90,
        }
        return [
            tl_types.messages.SearchCounter(filter=f, count=counts[type(f).__name__])
            for f in request.filters
        ]


@pytest.fixture
def loop_thread():
    """A running event loop in a background thread (stands in for the sync's loop)."""
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    t.join(2)
    loop.close()


def _get(url: str) -> tuple[int, bytes, str]:
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return r.status, r.read(), r.headers.get_content_type()
    except urllib.error.HTTPError as e:
        return e.code, e.read(), ""


def test_web_ui_downloads_and_serves_chat_photos_and_totals(tmp_path: Path, loop_thread) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    cfg.chats_manifest_path.write_text(
        render_chats_manifest(
            [
                ChatSelection(chat_key="channel_7", title="Com Foto", section="channels"),
                ChatSelection(chat_key="channel_8", title="Sem Foto", section="channels"),
            ]
        ),
        "utf-8",
    )
    with_photo, without_photo = FakeTgClient(), FakeTgClient(has_photo=False)
    bridge = TelegramBridge(cfg.internal_dir)
    bridge.attach_loop(loop_thread)
    bridge.register("channel_7", with_photo, object())
    bridge.register("channel_8", without_photo, object())
    ui = WebUI(
        data_dir=cfg.data_dir,
        manifest_path=cfg.chats_manifest_path,
        db_path=cfg.checkpoint_db_path,
        bridge=bridge,
        port=0,
    )
    ui.start()
    try:
        base = ui.url.rstrip("/")
        # Avatar: downloaded from Telegram on first request, then served from cache
        status, body, ctype = _get(f"{base}/avatar/channel_7")
        assert (status, body, ctype) == (200, JPEG, "image/jpeg")
        assert (cfg.internal_dir / "avatars" / "channel_7.jpg").read_bytes() == JPEG
        _get(f"{base}/avatar/channel_7")
        assert with_photo.photo_calls == 1
        # Chat without photo -> 404 (UI shows initials) and remembered, no refetch
        assert _get(f"{base}/avatar/channel_8")[0] == 404
        assert _get(f"{base}/avatar/channel_8")[0] == 404
        assert without_photo.photo_calls == 1
        assert _get(f"{base}/avatar/../../etc/passwd")[0] == 404  # path traversal rejected

        # Totals on Telegram, summed into our categories, cached
        status, body, _ = _get(f"{base}/api/counts/channel_7")
        remote = json.loads(body)["remote"]
        assert {k: remote[k] for k in ("messages", "photos", "videos", "audio", "files")} == {
            "messages": 1234,
            "photos": 300,
            "videos": 50,
            "audio": 12,
            "files": 90,
        }
        _get(f"{base}/api/counts/channel_7")
        assert with_photo.count_calls == 1  # second call served from cache
        chats = json.loads(_get(f"{base}/api/chats")[1])
        assert chats["can_fetch"] is True
        row = next(c for c in chats["chats"] if c["chat_key"] == "channel_7")
        assert row["remote"]["photos"] == 300
    finally:
        ui.stop()

    # Standalone `tghoard web` (not connected): cached avatar/totals still available
    offline = TelegramBridge(cfg.internal_dir)
    assert offline.can_fetch is False
    assert offline.avatar("channel_7") is not None
    assert offline.counts("channel_7")["messages"] == 1234
    assert offline.counts("channel_8") is None


def test_migration_0002_removes_only_empty_folders(tmp_path: Path) -> None:
    data = tmp_path / "data"
    cfg = AppConfig(data_dir=data)
    cfg.ensure_dirs()
    (data / "metadata" / "canal-1").mkdir(parents=True)
    (data / "metadata" / "canal-1" / "info.md").write_text("# Info — c (`channel_1`)\n", "utf-8")
    (data / "photos" / "canal-1").mkdir(parents=True)
    (data / "photos" / "canal-1" / "a.jpg").write_bytes(b"x")
    for root in ("videos", "audio", "files"):
        (data / root / "canal-1").mkdir(parents=True)  # empty: created by older versions
    (data / "files" / "canal-2").mkdir()
    (data / "files" / "canal-2" / ".dl_123_.bin").write_bytes(b"p")  # hidden file -> kept

    assert [m.id for m in pending_migrations(cfg)] == ["0002"]
    assert pending_migrations(cfg, required_only=True) == []  # never blocks sync/stats

    runner = CliRunner()
    assert runner.invoke(app, ["stats", "--data-dir", str(data)]).exit_code == 0
    result = runner.invoke(app, ["migrate", "--apply", "--data-dir", str(data)])
    assert result.exit_code == 0, result.output
    assert "0002_remove_empty_folders" in load_applied(cfg)

    assert (data / "photos" / "canal-1" / "a.jpg").exists()
    assert not (data / "videos").exists() and not (data / "audio").exists()  # empty roots too
    assert not (data / "files" / "canal-1").exists()
    assert (data / "files" / "canal-2" / ".dl_123_.bin").exists()
    assert (data / "metadata" / "canal-1" / "info.md").exists()


def test_photo_requested_before_chats_are_registered_waits_and_succeeds(
    tmp_path: Path, loop_thread
) -> None:
    """Regression: the page asks for photos while `sync` is still listing dialogs."""
    bridge = TelegramBridge(tmp_path / ".tghoard")
    bridge.attach_loop(loop_thread)  # connected, but nothing registered yet
    client = FakeTgClient()

    def register_later() -> None:
        import time

        time.sleep(0.3)  # dialogs listed a moment after the request arrived
        bridge.register("channel_7", client, object())
        bridge.mark_ready()

    threading.Thread(target=register_later, daemon=True).start()
    assert bridge.avatar("channel_7") is not None  # waited instead of failing
    assert client.photo_calls == 1
    assert bridge.avatar("channel_unknown_1") is None  # ready + unknown -> no wait, None


def test_download_error_is_not_cached_as_no_photo(tmp_path: Path, loop_thread) -> None:
    logs: list[str] = []
    bridge = TelegramBridge(tmp_path / ".tghoard")
    bridge.log = logs.append
    bridge.attach_loop(loop_thread)
    flaky = FakeTgClient()
    calls = {"n": 0}
    real = flaky.download_profile_photo

    async def fail_once(entity, file, download_big=True):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("rede caiu")
        return await real(entity, file, download_big)

    flaky.download_profile_photo = fail_once  # type: ignore[method-assign]
    bridge.register("channel_9", flaky, object())
    bridge.mark_ready()
    assert bridge.avatar("channel_9") is None  # failed...
    assert not (tmp_path / ".tghoard" / "avatars" / "channel_9.none").exists()  # ...not cached
    assert any("rede caiu" in m for m in logs)  # and visible in the web UI log
    assert bridge.avatar("channel_9") is not None  # next request retries and works
