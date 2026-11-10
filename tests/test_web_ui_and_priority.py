"""Web UI API, chats.md priorities (incl. live re-prioritization), live state, wait-for-user."""

from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import FakeChannel, FakeMessage, FakeTelegramClient, FakeUser
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import (
    ChatSelection,
    load_chats_manifest,
    parse_chats_manifest,
    render_chats_manifest,
    update_chat_selections,
)
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.live_state import LiveSyncState, wait_for_user
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.web.server import WebUI

# ---------------------------------------------------------------------------
# chats.md priority
# ---------------------------------------------------------------------------


def test_priority_roundtrip_sorting_and_title_cannot_spoof_it() -> None:
    entries = [
        ChatSelection(chat_key="channel_1", title="Zeta", section="channels", priority=0),
        ChatSelection(chat_key="channel_2", title="Alpha", section="channels", priority=5),
        ChatSelection(chat_key="channel_3", title="prioridade: 99", section="channels"),
    ]
    text = render_chats_manifest(entries)
    lines = [ln for ln in text.splitlines() if ln.startswith("- [")]
    assert "| prioridade: 5 — **Alpha**" in lines[0]  # highest priority listed first
    parsed = parse_chats_manifest(text)
    assert parsed["channel_2"].priority == 5
    assert parsed["channel_3"].priority == 0  # number inside the title is ignored
    # Old files without the token still parse (priority defaults to 0)
    legacy = "- [x] baixar | [ ] photos | [ ] videos | [ ] audio | [ ] files — **X** [`group_9`]"
    assert parse_chats_manifest(legacy)["group_9"].priority == 0


def test_update_chat_selections_validates_and_clamps(tmp_path: Path) -> None:
    path = tmp_path / "chats.md"
    path.write_text(
        render_chats_manifest([ChatSelection(chat_key="channel_1", title="A", section="channels")]),
        "utf-8",
    )
    out = update_chat_selections(path, {"channel_1": {"photos": True, "priority": 5000}})
    assert out["channel_1"].photos is True and out["channel_1"].priority == 999
    assert load_chats_manifest(path)["channel_1"].priority == 999
    with pytest.raises(ValueError):
        update_chat_selections(path, {"channel_1": {"title": "hack"}})
    with pytest.raises(ValueError):
        update_chat_selections(path, {"channel_1": {"photos": "yes"}})
    update_chat_selections(path, {"unknown_5": {"photos": True}})  # ignored, no crash


# ---------------------------------------------------------------------------
# Scheduler honours priorities, re-read live from chats.md
# ---------------------------------------------------------------------------


class OrderClient(FakeTelegramClient):
    """Records the order chats start syncing; can run a hook when the first one starts."""

    def __init__(self, *, on_first: Any = None, **kw: Any) -> None:
        super().__init__(**kw)
        self.order: list[int] = []
        self.on_first = on_first

    async def get_messages(self, entity: Any, limit: int | None = None, ids=None):
        if ids is None and limit == 1:
            self.order.append(int(entity.id))
            if len(self.order) == 1 and self.on_first is not None:
                self.on_first()
        return await super().get_messages(entity, limit=limit, ids=ids)


def _setup_three_channels(tmp_path: Path, priorities: dict[int, int]):
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", parallel_chats=1)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    chans = [FakeChannel(id=i, title=f"Canal {i}", broadcast=True) for i in (1, 2, 3)]
    msgs = {
        c.id: [FakeMessage(id=1, date=datetime(2026, 11, 9, tzinfo=UTC), message="x")]
        for c in chans
    }
    manifest = cfg.chats_manifest_path
    manifest.write_text(
        render_chats_manifest(
            [
                ChatSelection(
                    chat_key=f"channel_{c.id}",
                    title=c.title,
                    section="channels",
                    priority=priorities[c.id],
                )
                for c in chans
            ]
        ),
        "utf-8",
    )
    return cfg, db, acc, chans, msgs, manifest


@pytest.mark.asyncio
async def test_sync_order_follows_priority(tmp_path: Path) -> None:
    cfg, db, acc, chans, msgs, manifest = _setup_three_channels(tmp_path, {1: 0, 2: 10, 3: 5})
    client = OrderClient(
        me=FakeUser(id=111, first_name="V"), dialogs=chans, messages_by_entity_id=msgs
    )
    await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections=load_chats_manifest(manifest),
        config=cfg,
        db=db,
        manifest_path=manifest,
    )
    assert client.order == [2, 3, 1]
    db.close()


@pytest.mark.asyncio
async def test_priority_and_checkbox_changes_apply_to_a_running_sync(tmp_path: Path) -> None:
    cfg, db, acc, chans, msgs, manifest = _setup_three_channels(tmp_path, {1: 10, 2: 5, 3: 0})

    def reprioritize_mid_sync() -> None:  # what the web UI does while chat 1 is syncing
        update_chat_selections(
            manifest, {"channel_3": {"priority": 50}, "channel_2": {"baixar": False}}
        )

    client = OrderClient(
        on_first=reprioritize_mid_sync,
        me=FakeUser(id=111, first_name="V"),
        dialogs=chans,
        messages_by_entity_id=msgs,
    )
    live = LiveSyncState()
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections=load_chats_manifest(manifest),
        config=cfg,
        db=db,
        manifest_path=manifest,
        live=live,
    )
    assert client.order == [1, 3]  # 3 jumped the queue, 2 was unchecked before starting
    assert stats.chats_synced == 2
    snap = live.snapshot()
    assert snap["phase"] == "done"
    assert {c["chat_key"]: c["status"] for c in snap["finished"]} == {
        "channel_1": "done",
        "channel_3": "done",
    }
    assert snap["accounts"][0]["completed"] == 2
    assert any("starting" in e["msg"] for e in snap["log"])
    db.close()


# ---------------------------------------------------------------------------
# Web API
# ---------------------------------------------------------------------------


@pytest.fixture
def web(tmp_path: Path):
    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    cfg.chats_manifest_path.write_text(
        render_chats_manifest(
            [ChatSelection(chat_key="channel_7", title="Canal Sete", section="channels")]
        ),
        "utf-8",
    )
    live = LiveSyncState()
    ui = WebUI(
        data_dir=cfg.data_dir,
        manifest_path=cfg.chats_manifest_path,
        db_path=cfg.checkpoint_db_path,
        live=live,
        port=0,
    )
    ui.start()
    yield ui, cfg, live
    ui.stop()


def _req(ui: WebUI, path: str, body: Any = None, headers: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(ui.url.rstrip("/") + path, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            raw = r.read()
            return r.status, (
                json.loads(raw) if r.headers.get_content_type() == "application/json" else raw
            )
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def test_web_serves_page_chats_and_status(web) -> None:
    ui, cfg, live = web
    status, page = _req(ui, "/")
    assert status == 200 and b"tghoard" in page
    status, data = _req(ui, "/api/chats")
    assert status == 200
    (row,) = data["chats"]
    assert row["chat_key"] == "channel_7" and row["priority"] == 0
    assert row["stats"]["messages"] == 0 and "photos" in row["stats"]["media_counts"]
    status, snap = _req(ui, "/api/status")
    assert status == 200 and snap["phase"] == "idle"


def test_web_updates_chats_md_only_with_csrf_header(web) -> None:
    ui, cfg, _ = web
    body = {"updates": {"channel_7": {"photos": True, "priority": 3}}}
    status, err = _req(ui, "/api/chats", body)  # no X-Tghoard header
    assert (
        status == 403 and load_chats_manifest(cfg.chats_manifest_path)["channel_7"].photos is False
    )

    status, data = _req(ui, "/api/chats", body, {"X-Tghoard": "1"})
    assert status == 200 and data["chats"]["channel_7"] == {
        "baixar": True,
        "photos": True,
        "videos": False,
        "audio": False,
        "files": False,
        "priority": 3,
    }
    saved = load_chats_manifest(cfg.chats_manifest_path)["channel_7"]
    assert saved.photos is True and saved.priority == 3

    status, err = _req(
        ui, "/api/chats", {"updates": {"channel_7": {"title": "x"}}}, {"X-Tghoard": "1"}
    )
    assert status == 400 and "not editable" in err["error"]


def test_web_rejects_foreign_host_header(web) -> None:
    ui, *_ = web
    status, _ = _req(ui, "/api/chats", headers={"Host": "evil.example.com"})
    assert status == 403


def _pipe_stdin(monkeypatch: pytest.MonkeyPatch):
    """Replace stdin with a real pipe (pytest's captured stdin has no fileno)."""
    import sys

    r, w = os.pipe()
    reader = os.fdopen(r, "r")
    monkeypatch.setattr(sys, "stdin", reader)
    return reader, w


def test_web_start_button_wakes_waiting_sync(web, monkeypatch) -> None:
    ui, _, live = web
    reader, w = _pipe_stdin(monkeypatch)  # stdin that never gets a line

    async def scenario() -> str:
        live.attach_loop(asyncio.get_running_loop())
        waiter = asyncio.create_task(wait_for_user(live.start_event))
        await asyncio.sleep(0.05)
        await asyncio.to_thread(_req, ui, "/api/sync/start", {}, {"X-Tghoard": "1"})
        return await asyncio.wait_for(waiter, 3)

    try:
        assert asyncio.run(scenario()) == "web"
    finally:
        os.close(w)
        reader.close()


@pytest.mark.parametrize(("typed", "expected"), [(b"\n", "enter"), (b"q\n", "quit"), (b"", "quit")])
def test_wait_for_user_enter_and_quit(monkeypatch, typed: bytes, expected: str) -> None:
    reader, w = _pipe_stdin(monkeypatch)
    os.write(w, typed)
    if not typed:
        os.close(w)  # EOF (Ctrl+D)
    try:
        assert asyncio.run(asyncio.wait_for(wait_for_user(asyncio.Event()), 3)) == expected
    finally:
        if typed:
            os.close(w)
        reader.close()
