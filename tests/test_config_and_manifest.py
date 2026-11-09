"""Tests for `.env` configuration, `--data-dir` defaults, and `chats.md` discovery/merge."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.conftest import FakeChannel, FakeChat, FakeTelegramClient, FakeUser
from tghoard.config import load_config
from tghoard.storage.chats_manifest import (
    load_chats_manifest,
    parse_chats_manifest,
)
from tghoard.storage.checkpoint_db import AccountRecord
from tghoard.sync.discovery import run_discovery_and_write_manifest


def test_config_default_data_dir_and_empty_env(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("TG_API_ID=\nTG_API_HASH=\n", encoding="utf-8")

    cfg = load_config(env_file)
    assert cfg.tg_api_id is None
    assert cfg.tg_api_hash is None
    assert cfg.data_dir == Path("./data")
    assert cfg.max_json_kb == 1024

    overridden = cfg.with_overrides(data_dir=tmp_path / "custom_data", max_json_kb=512)
    assert overridden.data_dir == tmp_path / "custom_data"
    assert overridden.chats_manifest_path == tmp_path / "custom_data" / "chats.md"
    assert overridden.max_json_bytes == 512 * 1024


def test_tolerant_manifest_parser() -> None:
    raw_md = (
        "# Seleção de Chats — tghoard\n\n"
        "## Grupos e Supergrupos\n"
        "- [x] baixar | [] photos | [X] videos | [ ] audio | [x] files "
        "— **Dev Brasil** (`@devbrasil`) [`supergroup_987654`] *(contas: c1, c2)*\n"
        "- [] baixar | [x] photos | [] videos | [] audio | [] files "
        "— **Grupo Pausado** [`group_123`] *(contas: c1)*\n"
    )
    parsed = parse_chats_manifest(raw_md)
    dev = parsed["supergroup_987654"]
    assert dev.baixar is True
    assert dev.photos is False
    assert dev.videos is True
    assert dev.audio is False
    assert dev.files is True
    assert dev.username == "devbrasil"
    assert dev.accounts == ["c1", "c2"]

    paused = parsed["group_123"]
    assert paused.baixar is False
    assert paused.photos is True


@pytest.mark.asyncio
async def test_start_discovery_and_non_destructive_multi_account_merge(tmp_path: Path) -> None:
    manifest_path = tmp_path / "data" / "chats.md"

    acc1 = AccountRecord(
        account_id=111,
        alias="conta1",
        phone="+5511900001111",
        username="vitor1",
        display_name="Vitor 1",
        added_at="2026-11-09T10:00:00+00:00",
    )
    shared_sg = FakeChannel(id=987654, title="Dev Brasil", username="devbrasil", megagroup=True)
    maria = FakeUser(id=333, first_name="Maria", username="maria")
    bot = FakeUser(id=999, first_name="BotFather", username="BotFather", bot=True)

    client1 = FakeTelegramClient(
        me=FakeUser(id=111, first_name="Vitor 1", username="vitor1"),
        dialogs=[shared_sg, maria, bot],
        messages_by_entity_id={},
    )

    summary1 = await run_discovery_and_write_manifest(
        account_clients=[(acc1, client1)],
        manifest_path=manifest_path,
    )
    assert summary1.total_chats == 4  # saved_111 + supergroup + maria + bot
    assert summary1.added_chats == 4

    loaded = load_chats_manifest(manifest_path)
    sg_entry = loaded["supergroup_987654"]
    assert sg_entry.baixar is True
    assert sg_entry.photos is False
    assert sg_entry.videos is False
    assert sg_entry.audio is False
    assert sg_entry.files is False
    assert sg_entry.accounts == ["conta1"]

    # Simulate user manually customizing checkboxes in `data/chats.md`
    edited_text = (
        manifest_path.read_text(encoding="utf-8")
        .replace(
            "- [x] baixar | [ ] photos | [ ] videos | [ ] audio | [ ] files — **Dev Brasil**",
            "- [x] baixar | [x] photos | [ ] videos | [x] audio | [x] files — **Dev Brasil**",
        )
        .replace(
            "- [x] baixar | [ ] photos | [ ] videos | [ ] audio | [ ] files — **BotFather**",
            "- [ ] baixar | [ ] photos | [ ] videos | [ ] audio | [ ] files — **BotFather**",
        )
    )
    manifest_path.write_text(edited_text, encoding="utf-8")

    # Now Account 2 joins `start` sharing `Dev Brasil` + adding a new basic group!
    acc2 = AccountRecord(
        account_id=222,
        alias="conta2",
        phone="+5511900002222",
        username="vitor2",
        display_name="Vitor 2",
        added_at="2026-11-09T11:00:00+00:00",
    )
    new_group = FakeChat(id=4455, title="Time Engenharia")
    client2 = FakeTelegramClient(
        me=FakeUser(id=222, first_name="Vitor 2", username="vitor2"),
        dialogs=[shared_sg, new_group],
        messages_by_entity_id={},
    )

    summary2 = await run_discovery_and_write_manifest(
        account_clients=[(acc1, client1), (acc2, client2)],
        manifest_path=manifest_path,
    )
    assert summary2.total_chats == 6  # + saved_222 + group_4455 (shared_sg was consolidated!)

    reloaded = load_chats_manifest(manifest_path)
    sg_after = reloaded["supergroup_987654"]
    # User's manual checkbox edits MUST be preserved AND accounts merged!
    assert sg_after.accounts == ["conta1", "conta2"]
    assert sg_after.baixar is True
    assert sg_after.photos is True
    assert sg_after.videos is False
    assert sg_after.audio is True
    assert sg_after.files is True

    # BotFather remained unchecked!
    assert reloaded["dm_111_999"].baixar is False

    # Newly added group has default checkboxes ([x] baixar, rest unchecked)
    grp_after = reloaded["group_4455"]
    assert grp_after.baixar is True
    assert grp_after.photos is False
    assert grp_after.videos is False
    assert grp_after.audio is False
    assert grp_after.files is False
