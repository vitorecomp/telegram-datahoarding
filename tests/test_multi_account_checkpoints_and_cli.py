"""Tests for multi-account dedup, checkpoint resume, offline PTS diff, rebuild, and CLI."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from tests.conftest import (
    FakeChannel,
    FakeChat,
    FakeMediaPhoto,
    FakeMessage,
    FakePhoto,
    FakeTelegramClient,
    FakeUser,
)
from tghoard.cli import app
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.sync.rebuilder import rebuild_checkpoint_from_plaintext


@pytest.mark.asyncio
async def test_multi_account_shared_supergroup_and_legacy_group_zero_redownload(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    cfg = AppConfig(data_dir=data_dir, sync_mode="standard", concurrency=2)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)

    acc1 = db.upsert_account(
        account_id=111, alias="conta1", username="vitor1", display_name="Vitor 1"
    )
    acc2 = db.upsert_account(
        account_id=222, alias="conta2", username="vitor2", display_name="Vitor 2"
    )

    shared_sg = FakeChannel(id=8888, title="Grupo Comum", megagroup=True, pts=50)
    shared_legacy = FakeChat(id=4444, title="Grupo Legado")
    dt1 = datetime(2026, 11, 9, 10, 0, tzinfo=UTC)
    dt2 = datetime(2026, 11, 9, 10, 1, tzinfo=UTC)

    # Supergroup has global message IDs (1, 2)
    sg_msgs_acc1 = [
        FakeMessage(
            id=1,
            date=dt1,
            sender_id=111,
            message="Olá do supergrupo!",
            media=FakeMediaPhoto(photo=FakePhoto(id=7001)),
        ),
        FakeMessage(id=2, date=dt2, sender_id=222, message="Segunda msg global"),
    ]
    # Account 2 sees messages 1..2 PLUS a brand-new message #3!
    sg_msgs_acc2 = [
        *sg_msgs_acc1,
        FakeMessage(
            id=3,
            date=datetime(2026, 11, 9, 10, 2, tzinfo=UTC),
            sender_id=222,
            message="Msg #3 nova",
        ),
    ]

    # Legacy basic group has DIFFERENT msg_ids per account (801 vs 19) for the SAME msg!
    legacy_acc1 = [
        FakeMessage(
            id=801,
            date=dt1,
            sender_id=111,
            message="Mensagem grupo legado",
            media=FakeMediaPhoto(photo=FakePhoto(id=7002)),
        )
    ]
    legacy_acc2 = [
        FakeMessage(
            id=19,
            date=dt1,
            sender_id=111,
            message="Mensagem grupo legado",
            media=FakeMediaPhoto(photo=FakePhoto(id=7002)),
        )
    ]

    client1 = FakeTelegramClient(
        me=FakeUser(id=111, first_name="Vitor 1", username="vitor1"),
        dialogs=[shared_sg, shared_legacy],
        messages_by_entity_id={8888: sg_msgs_acc1, 4444: legacy_acc1},
    )
    client2 = FakeTelegramClient(
        me=FakeUser(id=222, first_name="Vitor 2", username="vitor2"),
        dialogs=[shared_sg, shared_legacy],
        messages_by_entity_id={8888: sg_msgs_acc2, 4444: legacy_acc2},
    )

    selections = {
        "supergroup_8888": ChatSelection(
            chat_key="supergroup_8888",
            title="Grupo Comum",
            section="groups",
            baixar=True,
            photos=True,
        ),
        "group_4444": ChatSelection(
            chat_key="group_4444",
            title="Grupo Legado",
            section="groups",
            baixar=True,
            photos=True,
        ),
    }

    stats = await run_multi_account_sync(
        account_clients=[(acc1, client1), (acc2, client2)],
        selections=selections,
        config=cfg,
        db=db,
    )

    # Account 1 downloaded photos 7001 and 7002; Account 2 MUST do ZERO media downloads!
    assert client1.download_calls == [7001, 7002]
    assert client2.download_calls == []
    assert stats.messages_saved == 4  # sg(1,2) + legacy(1) from acc1, plus sg(3) from acc2!

    # Verify Supergroup JSON has messages [1, 2, 3] without duplicates
    sg_dir = data_dir / "supergroup_8888__grupo-comum"
    sg_json = json.loads((sg_dir / "chats" / "2026-11-09-pt000.json").read_text(encoding="utf-8"))
    assert [m["id"] for m in sg_json["messages"]] == [1, 2, 3]
    info_md = (sg_dir / "metadata" / "info.md").read_text(encoding="utf-8")
    assert "conta1" in info_md and "conta2" in info_md

    # Verify Legacy Group JSON has ONLY 1 message with both account_msg_ids mapped!
    leg_dir = data_dir / "group_4444__grupo-legado"
    leg_json = json.loads((leg_dir / "chats" / "2026-11-09-pt000.json").read_text(encoding="utf-8"))
    assert len(leg_json["messages"]) == 1
    assert leg_json["messages"][0]["account_msg_ids"] == {"111": 801, "222": 19}
    assert leg_json["messages"][0]["seen_by_accounts"] == ["111", "222"]
    db.close()


@pytest.mark.asyncio
async def test_sqlite_checkpoint_resumes_after_mid_sync_crash(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    cfg = AppConfig(data_dir=data_dir, sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")

    ch = FakeChannel(id=9999, title="Canal Gigante", broadcast=True)
    all_230_msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, i // 60, i % 60, tzinfo=UTC),
            message=f"Post #{i}",
        )
        for i in range(1, 231)
    ]
    selections = {
        "channel_9999": ChatSelection(
            chat_key="channel_9999",
            title="Canal Gigante",
            section="channels",
            baixar=True,
        )
    }

    # Run 1 crashes at message 150 (after batch 1..100 already committed!)
    crashing_client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="Vitor"),
        dialogs=[ch],
        messages_by_entity_id={9999: all_230_msgs},
        fail_after_iter_count=150,
    )
    with pytest.raises(RuntimeError, match="Simulated mid-sync interruption"):
        await run_multi_account_sync(
            account_clients=[(acc, crashing_client)],
            selections=selections,
            config=cfg,
            db=db,
        )

    # First batch (1..100) MUST already be safely checkpointed in SQLite and on disk!
    assert db.get_synced_ranges("channel_9999") == [(1, 100)]

    # Run 2 succeeds -> resumes strictly from 101..230!
    healthy_client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="Vitor"),
        dialogs=[ch],
        messages_by_entity_id={9999: all_230_msgs},
    )
    stats2 = await run_multi_account_sync(
        account_clients=[(acc, healthy_client)],
        selections=selections,
        config=cfg,
        db=db,
    )
    assert stats2.messages_saved == 130  # Only 101..230 fetched & written!
    assert db.get_synced_ranges("channel_9999") == [(1, 230)]

    # Test `rebuild-checkpoint` after wiping SQLite indexes
    db.reset_archive_indexes()
    assert db.get_synced_ranges("channel_9999") == []
    rep = rebuild_checkpoint_from_plaintext(data_dir, db)
    assert rep.messages_indexed == 230
    assert db.get_synced_ranges("channel_9999") == [(1, 230)]
    db.close()


@pytest.mark.asyncio
async def test_offline_pts_diff_marks_deleted_and_versions_edits_plus_cli_search_stats(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    cfg = AppConfig(data_dir=data_dir, sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")

    dm_peer = FakeUser(id=333, first_name="Carlos", username="carlos")
    dt = datetime(2026, 11, 9, 18, 0, tzinfo=UTC)
    dm_msgs = [
        FakeMessage(
            id=10, date=dt, sender_id=333, message="Segredo confidencial https://secret.example"
        ),
        FakeMessage(id=11, date=dt, sender_id=333, message="Texto original v1"),
    ]
    selections = {
        "dm_111_333": ChatSelection(
            chat_key="dm_111_333",
            title="Carlos",
            section="private",
            baixar=True,
        )
    }

    @dataclass
    class State:
        pts: int
        qts: int = 0
        seq: int = 1
        date: datetime = dt

    @dataclass
    class UpdateDeleteMessages:
        messages: list[int]

    @dataclass
    class UpdateEditMessage:
        message: Any

    @dataclass
    class Difference:
        other_updates: list[Any]
        state: State

    phase = {"step": 1}

    async def rpc_handler(req: Any) -> Any:
        name = type(req).__name__
        if name == "GetStateRequest":
            return State(pts=100)
        if name == "GetDifferenceRequest" and phase["step"] == 2:
            edited_msg = FakeMessage(
                id=11,
                date=dt,
                sender_id=333,
                message="Texto editado v2",
                edit_date=datetime(2026, 11, 9, 19, 0, tzinfo=UTC),
            )
            return Difference(
                other_updates=[
                    UpdateDeleteMessages(messages=[10]),
                    UpdateEditMessage(message=edited_msg),
                ],
                state=State(pts=102),
            )
        raise NotImplementedError(name)

    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="Vitor"),
        dialogs=[dm_peer],
        messages_by_entity_id={333: dm_msgs},
        rpc_handler=rpc_handler,
    )

    # Run 1: initial sync + baseline PTS=100
    await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections=selections,
        config=cfg,
        db=db,
    )

    # Run 2: offline PTS diff replays deletion of msg #10 and edit of msg #11!
    phase["step"] = 2
    stats2 = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections=selections,
        config=cfg,
        db=db,
    )
    assert stats2.deletes_preserved == 1
    assert stats2.edits_detected == 1
    db.close()

    # Verify CLI `search`, `stats`, `account list`, and `rebuild-md`!
    runner = CliRunner()

    res_del = runner.invoke(app, ["search", "confidencial", "--deleted", "-d", str(data_dir)])
    assert res_del.exit_code == 0
    assert "APAGADA" in res_del.stdout

    res_ed = runner.invoke(app, ["search", "original v1", "--edited", "-d", str(data_dir)])
    assert res_ed.exit_code == 0
    assert "EDITADA(1)" in res_ed.stdout

    res_stats = runner.invoke(app, ["stats", "-d", str(data_dir)])
    assert res_stats.exit_code == 0
    assert "Mensagens apagadas preservadas" in res_stats.stdout

    res_acc = runner.invoke(app, ["account", "list", "-d", str(data_dir)])
    assert res_acc.exit_code == 0
    assert "conta1" in res_acc.stdout

    res_rb = runner.invoke(app, ["rebuild-md", "-d", str(data_dir)])
    assert res_rb.exit_code == 0
