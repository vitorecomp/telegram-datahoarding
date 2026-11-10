"""Failed media downloads never abort a chat and are retried on the next sync."""

from __future__ import annotations

import json
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
    chat_paths_for,
)
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.orchestrator import run_multi_account_sync

CHAT = "channel_321"


def _msgs() -> list[FakeMessage]:
    return [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, 0, i, tzinfo=UTC),
            message=f"#{i}",
            media=FakeMediaPhoto(photo=FakePhoto(id=9000 + i)) if i in (2, 4) else None,
        )
        for i in range(1, 7)
    ]


def _selection() -> dict[str, ChatSelection]:
    return {
        CHAT: ChatSelection(
            chat_key=CHAT,
            title="Canal Fotos",
            section="channels",
            baixar=True,
            photos=True,
            files=True,
        )
    }


def _client(msgs: list[FakeMessage], **kwargs) -> FakeTelegramClient:
    return FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"),
        dialogs=[FakeChannel(id=321, title="Canal Fotos", broadcast=True)],
        messages_by_entity_id={321: msgs},
        **kwargs,
    )


def _media_of(cfg: AppConfig, msg_id: int) -> dict:
    part = chat_paths_for(cfg.data_dir, CHAT).messages_dir / "2026-11-pt000.json"
    payload = json.loads(part.read_text(encoding="utf-8"))
    return next(m for m in payload["messages"] if m["id"] == msg_id)["media"]


@pytest.fixture
def env(tmp_path: Path):
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    yield cfg, db, acc
    db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["download_fail_ids", "download_empty_ids"])
async def test_failed_photo_keeps_chat_going_and_is_retried_next_sync(env, failure: str) -> None:
    cfg, db, acc = env
    msgs = _msgs()

    # Run 1: photo of msg #2 fails (raises / empty file) -> chat still completes
    broken = _client(msgs, **{failure: {9002}})
    stats1 = await run_multi_account_sync(
        account_clients=[(acc, broken)], selections=_selection(), config=cfg, db=db
    )
    assert stats1.failed_chats == []
    assert stats1.messages_saved == 6  # every message saved despite the broken photo
    assert db.get_synced_ranges(CHAT) == [(1, 6)]
    assert _media_of(cfg, 2)["downloaded"] is False
    assert _media_of(cfg, 2)["skip_reason"] in ("download_failed", "empty_download")
    assert _media_of(cfg, 4)["downloaded"] is True
    assert stats1.media_failed == 1
    assert stats1.media_failed_pending_total == 1
    (pending,) = db.list_pending_media(CHAT, ["photos"])
    assert pending.msg_id == 2 and pending.attempts >= 1 and pending.last_error

    # Run 2: Telegram healthy again -> only the failed photo is fetched, nothing else
    healthy = _client(msgs)
    stats2 = await run_multi_account_sync(
        account_clients=[(acc, healthy)], selections=_selection(), config=cfg, db=db
    )
    assert healthy.download_calls == [9002]
    assert stats2.messages_saved == 0
    assert stats2.retroactive_media_downloaded == 1
    assert stats2.media_failed_pending_total == 0
    media2 = _media_of(cfg, 2)
    assert media2["downloaded"] is True and "skip_reason" not in media2
    assert (cfg.data_dir / media2["path"]).is_file()
    assert db.list_pending_media(CHAT, ["photos"]) == []


@pytest.mark.asyncio
async def test_oversized_media_is_kept_in_queue_until_limit_raised(env) -> None:
    cfg, db, acc = env
    big_doc = FakeDocument(id=777, mime_type="application/zip", size=3 * 1024 * 1024)
    msgs = [
        FakeMessage(
            id=1,
            date=datetime(2026, 11, 9, 12, 0, 1, tzinfo=UTC),
            message="zip",
            media=FakeMediaDocument(document=big_doc),
        )
    ]

    small_cfg = cfg.with_overrides(max_media_mb=1)
    c1 = _client(msgs)
    await run_multi_account_sync(
        account_clients=[(acc, c1)], selections=_selection(), config=small_cfg, db=db
    )
    assert c1.download_calls == []
    assert _media_of(cfg, 1)["skip_reason"] == "exceeds_max_media_mb"

    # Still over the limit: stays queued, no RPC/download attempted
    c2 = _client(msgs)
    await run_multi_account_sync(
        account_clients=[(acc, c2)], selections=_selection(), config=small_cfg, db=db
    )
    assert c2.download_calls == []
    assert len(db.list_pending_media(CHAT, ["files"])) == 1

    # User raises TGHOARD_MAX_MEDIA_MB -> downloaded on the next sync
    c3 = _client(msgs)
    await run_multi_account_sync(
        account_clients=[(acc, c3)],
        selections=_selection(),
        config=cfg.with_overrides(max_media_mb=10),
        db=db,
    )
    assert c3.download_calls == [777]
    assert _media_of(cfg, 1)["downloaded"] is True
    assert db.list_pending_media(CHAT, ["files"]) == []


def test_existing_database_gets_new_pending_media_columns(tmp_path: Path) -> None:
    import sqlite3

    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE pending_media (chat_key TEXT NOT NULL, canonical_key TEXT NOT NULL, "
        "account_id INTEGER NOT NULL, msg_id INTEGER NOT NULL, day_str TEXT NOT NULL, "
        "category TEXT NOT NULL, tg_type TEXT NOT NULL, tg_id INTEGER NOT NULL, file_name TEXT, "
        "mime_type TEXT, size_bytes INTEGER, downloaded INTEGER NOT NULL DEFAULT 0, "
        "PRIMARY KEY (chat_key, canonical_key))"
    )
    conn.commit()
    conn.close()

    db = CheckpointDB(db_path)
    cols = {r["name"] for r in db._conn.execute("PRAGMA table_info(pending_media)")}
    assert {"attempts", "last_error"} <= cols
    assert db.count_failed_media() == 0
    db.close()
