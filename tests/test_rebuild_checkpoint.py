"""`rebuild-checkpoint`: synced ranges follow the archived ids; the media queue is rebuilt."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests.conftest import (
    FakeChannel,
    FakeMediaPhoto,
    FakeMessage,
    FakePhoto,
    FakeTelegramClient,
    FakeUser,
    chat_paths_for,
)
from tghoard.cli import app
from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow
from tghoard.storage.markdown_writer import ensure_initial_metadata_files
from tghoard.storage.media_manager import MediaManager
from tghoard.storage.paths import ChatPaths, iter_all_part_files
from tghoard.sync.messages_sync import sync_chat_messages
from tghoard.sync.rebuilder import rebuild_checkpoint_from_plaintext
from tghoard.telegram.throttler import RateLimiter

runner = CliRunner()


def _chat(data_dir: Path, folder: str, chat_key: str) -> ChatPaths:
    chat = ChatPaths(data_dir=data_dir, folder=folder)
    chat.ensure()
    ensure_initial_metadata_files(chat, chat_key=chat_key, chat_title=folder)
    return chat


def _msg(mid: int, date: str, media: dict | None = None, *, account: int | None = 111) -> dict:
    return {
        "canonical_key": f"id:{mid}",
        "id": mid,
        "date": date,
        "text": f"msg {mid}",
        "account_msg_ids": {str(account): mid} if account is not None else {},
        "media": media,
    }


def _write(chat: ChatPaths, chat_key: str, month: str, msgs: list[dict]) -> None:
    ChatPartWriter(max_json_bytes=4096).upsert_month_messages(
        chat_paths=chat, chat_key=chat_key, chat_title=chat.folder, month=month,
        incoming_messages=msgs,
    )  # fmt: skip


def _queue(db: CheckpointDB) -> list[tuple]:
    return sorted(
        tuple(r)
        for r in db._conn.execute(
            "SELECT chat_key, canonical_key, account_id, msg_id, day_str, category, tg_type, "
            "tg_id, downloaded, attempts FROM pending_media"
        )
    )


def test_ranges_cover_only_archived_ids(tmp_path: Path) -> None:
    """ids 1-5 + 90000-90005 (e.g. `--since` + an interrupted full sync) must not become
    `1..90005`: the 89,994 ids in between were never fetched."""
    data_dir = tmp_path / "data"
    ch = _chat(data_dir, "canal-1", "channel_1")
    ids = [*range(1, 6), *range(90000, 90006)]
    _write(ch, "channel_1", "2026-11", [_msg(i, "2026-11-09T10:00:00+00:00") for i in ids])
    # Supergroup: holes (deleted ids) stay out; adjacent ids merge across months/parts
    sg = _chat(data_dir, "grupo-2", "supergroup_2")
    _write(sg, "supergroup_2", "2026-10", [_msg(i, f"2026-10-0{i}T10:00:00") for i in (4, 5)])
    _write(sg, "supergroup_2", "2026-11", [_msg(i, "2026-11-01T10:00:00") for i in (6, 8, 9)])
    # DMs use per-account ids: no ranges
    dm = _chat(data_dir, "ana-111-500", "dm_111_500")
    _write(dm, "dm_111_500", "2026-11", [_msg(i, "2026-11-01T10:00:00") for i in (1, 2)])

    with CheckpointDB(data_dir / ".tghoard" / "checkpoint.db") as db:
        db.add_synced_range("channel_1", 1, 90005)  # stale state from before the rebuild
        rep = rebuild_checkpoint_from_plaintext(data_dir, db)
        assert db.get_synced_ranges("channel_1") == [(1, 5), (90000, 90005)]
        assert db.get_missing_ranges("channel_1", 1, 90010) == [(6, 89999), (90006, 90010)]
        assert db.get_synced_ranges("supergroup_2") == [(4, 6), (8, 9)]
        assert db.get_synced_ranges("dm_111_500") == []
        assert rep.synced_ranges == 4
        assert rep.messages_indexed == len(ids) + 5 + 2


def test_media_queue_is_rebuilt_from_the_json(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    ch = _chat(data_dir, "canal-1", "channel_1")
    photo = ch.media_dir("photos") / "2026-11-09_2_photo_7002.jpg"
    photo.parent.mkdir(parents=True)
    photo.write_bytes(b"jpg")
    not_downloaded = {
        "category": "photos",
        "tg_type": "photo",
        "tg_id": 7001,
        "file_name": None,
        "mime_type": "image/jpeg",
        "size_bytes": 10,
        "downloaded": False,
        "skip_reason": "category_disabled",
    }
    downloaded = {
        "category": "photos",
        "tg_type": "photo",
        "tg_id": 7002,
        "downloaded": True,
        "path": ch.media_rel("photos", photo.name),
        "sha256": "0" * 64,
        "size_bytes": 3,
    }
    video = {"category": "videos", "tg_type": "document", "tg_id": 7004, "downloaded": False}
    _write(
        ch,
        "channel_1",
        "2026-11",
        [
            _msg(1, "2026-11-09T10:00:00", not_downloaded),
            _msg(2, "2026-11-09T10:01:00", downloaded),
            _msg(3, "2026-11-09T10:02:00", {"category": "photos", "downloaded": False}),  # no id
            _msg(4, "2026-11-09T10:03:00", video),
        ],
    )
    dm = _chat(data_dir, "ana-111-500", "dm_111_500")
    dm_media = {"category": "audio", "tg_type": "document", "tg_id": 7005, "downloaded": False}
    _write(dm, "dm_111_500", "2026-10", [_msg(9, "2026-10-01T10:00:00", dm_media, account=None)])

    def row(key: str, msg_id: int, category: str, tg_type: str, tg_id: int) -> PendingMediaRow:
        return PendingMediaRow(
            chat_key="channel_1", canonical_key=key, account_id=111, msg_id=msg_id,
            day_str="2026-11", category=category, tg_type=tg_type, tg_id=tg_id, file_name=None,
            mime_type=None, size_bytes=None,
        )  # fmt: skip

    with CheckpointDB(data_dir / ".tghoard" / "checkpoint.db") as db:
        db.upsert_account(account_id=111, alias="conta1")  # fallback for the DM (no ids map)
        # Stale queue: id:2 is on disk but still queued; id:4 marked done but not on disk
        db.enqueue_pending_media(row("id:2", 2, "photos", "photo", 7002))
        db.enqueue_pending_media(row("id:4", 4, "videos", "document", 7004))
        db.mark_pending_media_downloaded("channel_1", "id:4")

        rep = rebuild_checkpoint_from_plaintext(data_dir, db)
        expected = [
            ("channel_1", "id:1", 111, 1, "2026-11", "photos", "photo", 7001, 0, 0),
            ("channel_1", "id:2", 111, 2, "2026-11", "photos", "photo", 7002, 1, 0),
            ("channel_1", "id:4", 111, 4, "2026-11", "videos", "document", 7004, 0, 0),
            ("dm_111_500", "id:9", 111, 9, "2026-10", "audio", "document", 7005, 0, 0),
        ]
        assert _queue(db) == expected
        assert rep.pending_media_queued == 3
        assert db.get_indexed_media("photo", 7002).rel_path == downloaded["path"]
        (queued,) = db.list_pending_media("channel_1", ["photos"])
        assert (queued.canonical_key, queued.mime_type, queued.size_bytes) == (
            "id:1",
            "image/jpeg",
            10,
        )

        # Idempotent: a second rebuild changes nothing
        rebuild_checkpoint_from_plaintext(data_dir, db)
        assert _queue(db) == expected


@pytest.mark.asyncio
async def test_sync_after_rebuild_fetches_the_ids_missing_from_the_archive(
    tmp_path: Path,
) -> None:
    """End to end: archive with ids 1-5 and 25-30 -> rebuild from scratch -> the next sync
    fetches 6-24, and media deferred before the rebuild is still queued."""
    data_dir = tmp_path / "data"
    db_path = data_dir / ".tghoard" / "checkpoint.db"
    me = FakeUser(id=111, first_name="Vitor")
    channel = FakeChannel(id=9999, title="Canal", broadcast=True)

    def message(i: int) -> FakeMessage:
        media = FakeMediaPhoto(photo=FakePhoto(id=7000 + i)) if i in (3, 12) else None
        return FakeMessage(id=i, date=datetime(2026, 11, 9, 12, i, tzinfo=UTC), media=media)

    selection = ChatSelection(chat_key="channel_9999", title="Canal", section="channels")

    async def sync(db: CheckpointDB, ids: list[int]) -> int:
        client = FakeTelegramClient(
            me=me, dialogs=[channel], messages_by_entity_id={9999: [message(i) for i in ids]}
        )
        res = await sync_chat_messages(
            client=client,
            entity=channel,
            account_id=111,
            selection=selection,
            data_dir=data_dir,
            db=db,
            writer=ChatPartWriter(),
            media_manager=MediaManager(data_dir=data_dir, checkpoint_db=db),
            limiter=RateLimiter(concurrency=2, min_delay=0.0, max_delay=0.0),
        )
        return res.messages_saved

    db = CheckpointDB(db_path)
    assert await sync(db, [*range(1, 6), *range(25, 31)]) == 11
    queue_before = _queue(db)
    db.close()
    for suffix in ("", "-wal", "-shm"):
        Path(f"{db_path}{suffix}").unlink(missing_ok=True)

    result = runner.invoke(app, ["rebuild-checkpoint", "--data-dir", str(data_dir)])
    assert result.exit_code == 0, result.output
    with CheckpointDB(db_path) as db:
        assert db.get_synced_ranges("channel_9999") == [(1, 5), (25, 30)]
        assert _queue(db) == queue_before  # photo of msg 3 (photos unchecked)
        assert await sync(db, list(range(1, 31))) == 19
        assert db.get_synced_ranges("channel_9999") == [(1, 30)]

    chat = chat_paths_for(data_dir, "channel_9999")
    archived = [
        m["id"]
        for part in iter_all_part_files(chat)
        for m in json.loads(part.read_text("utf-8"))["messages"]
    ]
    assert sorted(archived) == list(range(1, 31))
