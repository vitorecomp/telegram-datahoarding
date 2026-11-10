"""When you joined each channel/group: recorded in the DB, info.md and the web UI."""

from __future__ import annotations

import json
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telethon.tl import functions

from tests.conftest import (
    FakeChannel,
    FakeChat,
    FakeMessage,
    FakeTelegramClient,
    FakeUser,
    chat_paths_for,
)
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection, render_chats_manifest
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.web.server import WebUI

JOINED_CH = datetime(2021, 3, 14, 9, 30, tzinfo=UTC)
JOINED_GRP = datetime(2019, 7, 1, 18, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_join_dates_for_channels_and_basic_groups(tmp_path: Path) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    channel = FakeChannel(id=10, title="Canal", broadcast=True, date=JOINED_CH)
    not_selected = FakeChannel(
        id=11, title="Outro", megagroup=True, date=datetime(2023, 1, 2, tzinfo=UTC)
    )
    left = FakeChannel(
        id=12, title="Saí", broadcast=True, date=datetime(2015, 1, 1, tzinfo=UTC), left=True
    )
    group = FakeChat(id=30, title="Grupo Legado")
    msg = [FakeMessage(id=1, date=datetime(2026, 11, 9, tzinfo=UTC), message="oi")]
    sels = {
        "channel_10": ChatSelection(chat_key="channel_10", title="Canal", section="channels"),
        "group_30": ChatSelection(chat_key="group_30", title="Grupo Legado", section="groups"),
    }
    cfg.chats_manifest_path.write_text(render_chats_manifest(list(sels.values())), "utf-8")

    async def rpc(request: Any) -> Any:
        if isinstance(request, functions.messages.GetFullChatRequest):
            me = SimpleNamespace(user_id=111, inviter_id=500, date=JOINED_GRP)
            other = SimpleNamespace(user_id=500, inviter_id=None, date=None)
            return SimpleNamespace(
                full_chat=SimpleNamespace(
                    about="grupo antigo", participants=SimpleNamespace(participants=[other, me])
                )
            )
        raise NotImplementedError(type(request).__name__)

    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"),
        dialogs=[channel, not_selected, left, group],
        messages_by_entity_id={10: msg, 30: msg},
        rpc_handler=rpc,
    )
    await run_multi_account_sync(
        account_clients=[(acc, client)], selections=sels, config=cfg, db=db
    )

    # Channels: from the dialog entity (no extra request), even for chats not being synced
    assert db.get_memberships("channel_10")[111]["joined_at"] == JOINED_CH.isoformat()
    assert db.get_memberships("supergroup_11")[111]["joined_at"].startswith("2023-01-02")
    assert db.get_memberships("channel_12") == {}  # left: Channel.date isn't a join date
    # Basic group: from getFullChat's member list, with who invited us
    grp = db.get_memberships("group_30")[111]
    assert grp["joined_at"] == JOINED_GRP.isoformat() and grp["inviter_id"] == 500
    db.close()

    info = chat_paths_for(cfg.data_dir, "group_30").meta("info.md").read_text("utf-8")
    assert "| Entrou em |" in info
    assert f"`{JOINED_GRP.isoformat()}` (convidado por `500`)" in info

    ui = WebUI(
        data_dir=cfg.data_dir,
        manifest_path=cfg.chats_manifest_path,
        db_path=cfg.checkpoint_db_path,
        port=0,
    )
    ui.start()
    try:
        with urllib.request.urlopen(ui.url + "api/chats", timeout=5) as r:
            rows = {c["chat_key"]: c for c in json.loads(r.read())["chats"]}
    finally:
        ui.stop()
    assert rows["channel_10"]["stats"]["joined_at"] == JOINED_CH.isoformat()
    assert rows["group_30"]["stats"]["joined"] == [
        {"account": "conta1", "joined_at": JOINED_GRP.isoformat(), "inviter_id": 500}
    ]
