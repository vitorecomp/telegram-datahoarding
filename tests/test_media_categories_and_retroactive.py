"""Tests for the 4 attachment folders (`photos`, `videos`, `audio`, `files`) and sync."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tests.conftest import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeSticker,
    DocumentAttributeVideo,
    FakeButton,
    FakeChannel,
    FakeDocument,
    FakeEntityTextUrl,
    FakeMarkup,
    FakeMediaDocument,
    FakeMediaPhoto,
    FakeMessage,
    FakePhoto,
    FakeRow,
    FakeTelegramClient,
    FakeUser,
)
from tghoard.storage.chat_writer import ChatDayWriter
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.media_manager import MediaManager
from tghoard.sync.messages_sync import sync_chat_messages
from tghoard.telegram.throttler import RateLimiter


@pytest.mark.asyncio
async def test_four_media_categories_selective_and_retroactive_sync(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    db = CheckpointDB(data_dir / ".tghoard" / "checkpoint.db")
    writer = ChatDayWriter(max_json_bytes=1024 * 1024)
    media_mgr = MediaManager(data_dir=data_dir, checkpoint_db=db)
    limiter = RateLimiter(concurrency=2, min_delay=0.0, max_delay=0.0)

    me = FakeUser(id=111, first_name="Vitor", username="vitor")
    group = FakeChannel(id=5555, title="Pesquisa IA", username="pesquisaia", megagroup=True)
    dt = datetime(2026, 11, 9, 15, 0, tzinfo=UTC)

    msgs = [
        # 1. Photo + plain URL + TextUrl + inline button URL
        FakeMessage(
            id=1,
            date=dt,
            sender_id=111,
            sender=me,
            message="Veja https://arxiv.org/abs/2611.00001 e o paper aqui",
            entities=[FakeEntityTextUrl(offset=38, length=10, url="https://huggingface.co/papers")],
            reply_markup=FakeMarkup(
                rows=[FakeRow(buttons=[FakeButton(url="https://github.com/org/repo")])]
            ),
            media=FakeMediaPhoto(photo=FakePhoto(id=9001)),
        ),
        # 2. Video (.mp4)
        FakeMessage(
            id=2,
            date=dt,
            sender_id=111,
            sender=me,
            message="Demo em vídeo",
            media=FakeMediaDocument(
                document=FakeDocument(
                    id=9002,
                    mime_type="video/mp4",
                    attributes=[DocumentAttributeVideo(), DocumentAttributeFilename("demo.mp4")],
                )
            ),
        ),
        # 3. Audio / Voice note (.ogg)
        FakeMessage(
            id=3,
            date=dt,
            sender_id=111,
            sender=me,
            message="",
            media=FakeMediaDocument(
                document=FakeDocument(
                    id=9003,
                    mime_type="audio/ogg",
                    attributes=[DocumentAttributeAudio(voice=True)],
                )
            ),
        ),
        # 4. Generic file (.pdf) -> `files/`
        FakeMessage(
            id=4,
            date=dt,
            sender_id=111,
            sender=me,
            message="Slides da apresentação",
            media=FakeMediaDocument(
                document=FakeDocument(
                    id=9004,
                    mime_type="application/pdf",
                    attributes=[DocumentAttributeFilename("slides.pdf")],
                )
            ),
        ),
        # 5. Sticker (.webp) -> `files/` (neither photo, video, nor audio!)
        FakeMessage(
            id=5,
            date=dt,
            sender_id=111,
            sender=me,
            message="",
            media=FakeMediaDocument(
                document=FakeDocument(
                    id=9005,
                    mime_type="image/webp",
                    attributes=[
                        DocumentAttributeSticker(),
                        DocumentAttributeFilename("sticker.webp"),
                    ],
                )
            ),
        ),
    ]

    # Also a second channel forwarding the exact same video `9002` to test cross-chat hardlinking!
    other_channel = FakeChannel(id=7777, title="Mirror IA", broadcast=True)
    other_msgs = [
        FakeMessage(
            id=1,
            date=dt,
            message="Repost do vídeo",
            media=FakeMediaDocument(
                document=FakeDocument(
                    id=9002,
                    mime_type="video/mp4",
                    attributes=[DocumentAttributeVideo(), DocumentAttributeFilename("demo.mp4")],
                )
            ),
        )
    ]

    client = FakeTelegramClient(
        me=me,
        dialogs=[group, other_channel],
        messages_by_entity_id={5555: msgs, 7777: other_msgs},
    )

    # Phase 1: Default checkboxes (`[x] baixar`, all 4 media boxes unchecked!)
    sel_text_only = ChatSelection(
        chat_key="supergroup_5555",
        title="Pesquisa IA",
        section="groups",
        baixar=True,
        photos=False,
        videos=False,
        audio=False,
        files=False,
    )
    res1 = await sync_chat_messages(
        client=client,
        entity=group,
        account_id=111,
        selection=sel_text_only,
        data_dir=data_dir,
        db=db,
        writer=writer,
        media_manager=media_mgr,
        limiter=limiter,
    )
    assert res1.messages_saved == 5
    assert res1.media_downloaded == 0
    assert res1.media_deferred == 5
    assert len(client.download_calls) == 0

    chat_dir = data_dir / "supergroup_5555__pesquisa-ia"
    for sub in ("chats", "photos", "videos", "audio", "files", "metadata"):
        assert (chat_dir / sub).is_dir()

    links_md = (chat_dir / "links.md").read_text(encoding="utf-8")
    assert "https://arxiv.org/abs/2611.00001" in links_md
    assert "https://huggingface.co/papers" in links_md
    assert "https://github.com/org/repo" in links_md

    # Phase 2: User checks `[x] photos` and `[x] audio` in `chats.md` and runs `sync` again!
    sel_photos_audio = ChatSelection(
        chat_key="supergroup_5555",
        title="Pesquisa IA",
        section="groups",
        baixar=True,
        photos=True,
        videos=False,
        audio=True,
        files=False,
    )
    res2 = await sync_chat_messages(
        client=client,
        entity=group,
        account_id=111,
        selection=sel_photos_audio,
        data_dir=data_dir,
        db=db,
        writer=writer,
        media_manager=media_mgr,
        limiter=limiter,
    )
    assert res2.messages_saved == 0  # Zero messages re-downloaded!
    assert res2.retroactive_media_downloaded == 2  # Only photo 9001 + audio 9003!
    assert set(client.download_calls) == {9001, 9003}
    assert len(list((chat_dir / "photos").iterdir())) == 1
    assert len(list((chat_dir / "audio").iterdir())) == 1
    assert len(list((chat_dir / "videos").iterdir())) == 0
    assert len(list((chat_dir / "files").iterdir())) == 0

    # Phase 3: Check `[x] videos` and `[x] files` -> drains video + PDF/Sticker!
    sel_all = ChatSelection(
        chat_key="supergroup_5555",
        title="Pesquisa IA",
        section="groups",
        baixar=True,
        photos=True,
        videos=True,
        audio=True,
        files=True,
    )
    res3 = await sync_chat_messages(
        client=client,
        entity=group,
        account_id=111,
        selection=sel_all,
        data_dir=data_dir,
        db=db,
        writer=writer,
        media_manager=media_mgr,
        limiter=limiter,
    )
    assert res3.retroactive_media_downloaded == 3
    assert len(list((chat_dir / "videos").iterdir())) == 1
    assert len(list((chat_dir / "files").iterdir())) == 2  # slides.pdf + sticker.webp

    part_doc = json.loads(
        (chat_dir / "chats" / "2026-11-09-pt000.json").read_text(encoding="utf-8")
    )
    for m in part_doc["messages"]:
        assert m["media"]["downloaded"] is True
        assert (chat_dir / m["media"]["path"]).exists()

    # Phase 4: Sync `other_channel` with `[x] videos` -> video 9002 hardlinked (0 downloads)!
    downloads_before = len(client.download_calls)
    sel_mirror = ChatSelection(
        chat_key="channel_7777",
        title="Mirror IA",
        section="channels",
        baixar=True,
        videos=True,
    )
    res4 = await sync_chat_messages(
        client=client,
        entity=other_channel,
        account_id=111,
        selection=sel_mirror,
        data_dir=data_dir,
        db=db,
        writer=writer,
        media_manager=media_mgr,
        limiter=limiter,
    )
    assert len(client.download_calls) == downloads_before  # 0 new network downloads!
    assert res4.media_dedup_reused == 1
    mirror_vid = next((data_dir / "channel_7777__mirror-ia" / "videos").iterdir())
    orig_vid = next((chat_dir / "videos").iterdir())
    assert mirror_vid.stat().st_ino == orig_vid.stat().st_ino  # True hardlink!
    db.close()
