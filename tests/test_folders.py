"""Telegram chat folders -> folders.md / folders.json / web UI filter."""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

import pytest
from telethon.tl import functions
from telethon.tl import types as tl

from tests.conftest import FakeChannel, FakeChat, FakeTelegramClient, FakeUser
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection, render_chats_manifest
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.folders_sync import load_folders
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.web.server import WebUI


def _title(text: str) -> tl.TextWithEntities:
    return tl.TextWithEntities(text=text, entities=[])


FILTERS = tl.messages.DialogFilters(
    filters=[
        tl.DialogFilterDefault(),  # "All chats": skipped
        tl.DialogFilter(  # explicit chats, one pinned
            id=2,
            title=_title("Trabalho"),
            emoticon="💼",
            pinned_peers=[tl.InputPeerChannel(channel_id=10, access_hash=0)],
            include_peers=[tl.InputPeerUser(user_id=500, access_hash=0), tl.InputPeerSelf()],
            exclude_peers=[],
        ),
        tl.DialogFilter(  # rule based: every group, minus one excluded
            id=3,
            title=_title("Grupos"),
            pinned_peers=[],
            include_peers=[],
            exclude_peers=[tl.InputPeerChat(chat_id=30)],
            groups=True,
        ),
        tl.DialogFilterChatlist(  # shared folder (invite link)
            id=4,
            title=_title("Comunidade"),
            pinned_peers=[],
            include_peers=[tl.InputPeerChannel(channel_id=11, access_hash=0)],
        ),
    ]
)


def _setup(tmp_path: Path):
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    dialogs = [
        FakeChannel(id=10, title="Canal Notícias", broadcast=True),
        FakeChannel(id=11, title="Python BR", megagroup=True),
        FakeChannel(id=12, title="Rust BR", megagroup=True),
        FakeChat(id=30, title="Grupo Antigo"),
        FakeUser(id=500, first_name="Ana"),
    ]
    cfg.chats_manifest_path.write_text(
        render_chats_manifest(
            [ChatSelection(chat_key="channel_10", title="Canal Notícias", section="channels")]
        ),
        "utf-8",
    )

    async def rpc(request: Any) -> Any:
        if isinstance(request, functions.messages.GetDialogFiltersRequest):
            return FILTERS
        raise NotImplementedError(type(request).__name__)

    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"),
        dialogs=dialogs,
        messages_by_entity_id={},
        rpc_handler=rpc,
    )
    return cfg, db, acc, client


@pytest.mark.asyncio
async def test_sync_saves_folders_md_and_json(tmp_path: Path) -> None:
    cfg, db, acc, client = _setup(tmp_path)
    await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_10": ChatSelection(chat_key="channel_10", title="C", section="channels")
        },
        config=cfg,
        db=db,
    )
    db.close()

    folders = {f["id"]: f for f in load_folders(cfg.data_dir)["accounts"]["conta1"]["folders"]}
    assert set(folders) == {2, 3, 4}  # "All chats" default skipped
    work = folders[2]
    assert work["title"] == "Trabalho" and work["emoticon"] == "💼"
    assert work["pinned"] == ["channel_10"]
    assert work["members"] == ["channel_10", "dm_111_500", "saved_111"]
    groups = folders[3]
    assert groups["rules"] == ["grupos"]
    assert groups["members"] == ["supergroup_11", "supergroup_12"]  # group_30 excluded
    assert folders[4]["shared"] is True and folders[4]["members"] == ["supergroup_11"]

    md = (cfg.data_dir / "folders.md").read_text("utf-8")
    assert "### 💼 Trabalho (id 2)" in md
    assert "**Canal Notícias** [`channel_10`] 📌" in md
    assert "- **Regras**: grupos" in md
    assert "compartilhada" in md


@pytest.mark.asyncio
async def test_web_ui_exposes_folders_for_filtering(tmp_path: Path) -> None:
    cfg, db, acc, client = _setup(tmp_path)
    await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_10": ChatSelection(chat_key="channel_10", title="C", section="channels")
        },
        config=cfg,
        db=db,
    )
    db.close()
    ui = WebUI(
        data_dir=cfg.data_dir,
        manifest_path=cfg.chats_manifest_path,
        db_path=cfg.checkpoint_db_path,
        port=0,
    )
    ui.start()
    try:
        with urllib.request.urlopen(ui.url + "api/chats", timeout=5) as r:
            data = json.loads(r.read())
    finally:
        ui.stop()
    by_key = {f["key"]: f for f in data["folders"]}
    assert by_key["conta1:2"]["label"] == "💼 Trabalho"
    assert by_key["conta1:3"]["members"] == ["supergroup_11", "supergroup_12"]


@pytest.mark.asyncio
async def test_folder_errors_never_break_the_sync(tmp_path: Path) -> None:
    cfg, db, acc, _ = _setup(tmp_path)
    plain = FakeTelegramClient(  # no rpc handler: GetDialogFilters raises
        me=FakeUser(id=111, first_name="V"),
        dialogs=[FakeChannel(id=10, title="Canal Notícias", broadcast=True)],
        messages_by_entity_id={},
    )
    stats = await run_multi_account_sync(
        account_clients=[(acc, plain)],
        selections={
            "channel_10": ChatSelection(chat_key="channel_10", title="C", section="channels")
        },
        config=cfg,
        db=db,
    )
    db.close()
    assert stats.failed_chats == [] and stats.chats_synced == 1
    assert not (cfg.data_dir / "folders.md").exists()
