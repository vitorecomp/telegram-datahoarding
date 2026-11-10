"""Media durability: partial/stale files are never trusted, temps are atomic and cleaned up."""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telethon.tl import types as tl_types

from tghoard.storage import media_manager as mm_mod
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.media_manager import ClassifiedMedia, MediaManager, classify_telethon_media
from tghoard.storage.paths import resolve_chat_paths


def _setup(tmp_path: Path, data_dir: Path | None = None) -> tuple[Path, CheckpointDB, MediaManager]:
    d = data_dir or tmp_path / "data"
    d.mkdir(parents=True, exist_ok=True)
    db = CheckpointDB(d / ".tghoard" / "checkpoint.db")
    return d, db, MediaManager(data_dir=d, checkpoint_db=db)


def _doc_media(tg_id: int, size: int, file_name: str = "a.pdf") -> Any:
    doc = SimpleNamespace(
        id=tg_id,
        mime_type="application/pdf",
        size=size,
        attributes=[tl_types.DocumentAttributeFilename(file_name=file_name)],
    )
    return SimpleNamespace(document=doc)


def _photo_media(tg_id: int) -> Any:
    return SimpleNamespace(photo=SimpleNamespace(id=tg_id, sizes=[SimpleNamespace(size=10)]))


def _writer(payload: bytes):
    async def download(tmp: Path) -> Path:
        tmp.write_bytes(payload)
        return tmp

    return download


async def _process(mm: MediaManager, cp: Any, media: Any, downloader: Any, msg_id: int = 1):
    return await mm.process_message_media(
        media=media,
        selection=ChatSelection(chat_key="channel_1", title="Chan", photos=True, files=True),
        chat_paths=cp,
        chat_key="channel_1",
        canonical_key=f"k{msg_id}",
        account_id=1,
        msg_id=msg_id,
        day_str="2026-11",
        file_day="2026-11-09",
        downloader=downloader,
    )


@pytest.mark.asyncio
async def test_interrupted_copy_is_replaced_not_reused(tmp_path: Path) -> None:
    d, db, mm = _setup(tmp_path)
    src_cp = resolve_chat_paths(d, "channel_1", "A")
    dst_cp = resolve_chat_paths(d, "channel_2", "B")
    src = src_cp.media_dir("videos") / "2026-11-09_1_document_77.mp4"
    src.parent.mkdir(parents=True)
    src.write_bytes(b"x" * 1000)
    db.register_indexed_media(
        tg_type="document",
        tg_id=77,
        category="videos",
        sha256="full",
        size_bytes=1000,
        rel_path=src.relative_to(d).as_posix(),
    )
    # an older copy fallback was killed mid-way (exFAT / SIGKILL): 10-byte partial left
    dst = dst_cp.media_dir("videos") / "2026-11-09_5_document_77.mp4"
    dst.parent.mkdir(parents=True)
    dst.write_bytes(b"x" * 10)
    cm = ClassifiedMedia(
        category="videos",
        tg_type="document",
        tg_id=77,
        file_name=None,
        mime_type="video/mp4",
        size_bytes=1000,
        default_ext=".mp4",
    )
    out = await mm._acquire_media_file(
        classified=cm, chat_paths=dst_cp, prefix_tag="2026-11-09_5", base_meta={}, downloader=None
    )
    assert out.dedup_reused
    assert dst.stat().st_size == 1000
    assert not [p for p in dst.parent.iterdir() if p.name.startswith(".dl_")]


def test_copy_fallback_goes_through_a_temp_file(tmp_path: Path, monkeypatch) -> None:
    src = tmp_path / "src.bin"
    src.write_bytes(b"y" * 4096)
    dst = tmp_path / "out" / "dst.bin"
    targets: list[str] = []

    def no_hardlinks(*_: Any) -> None:
        raise OSError(1, "Operation not permitted")  # exFAT

    real_copy2 = mm_mod.shutil.copy2

    def spy_copy2(a: Any, b: Any) -> Any:
        targets.append(Path(b).name)
        return real_copy2(a, b)

    monkeypatch.setattr(mm_mod.os, "link", no_hardlinks)
    monkeypatch.setattr(mm_mod.shutil, "copy2", spy_copy2)
    mm_mod._link_or_copy(src, dst, 4096)
    assert dst.read_bytes() == b"y" * 4096
    assert targets and targets[0].startswith(".dl_")  # never copied straight into `dst`
    assert sorted(p.name for p in dst.parent.iterdir()) == ["dst.bin"]


@pytest.mark.asyncio
async def test_truncated_indexed_file_is_downloaded_again(tmp_path: Path) -> None:
    d, db, mm = _setup(tmp_path)
    cp = resolve_chat_paths(d, "channel_1", "Chan")
    media = _doc_media(5, size=64)
    first = await _process(mm, cp, media, _writer(b"z" * 64))
    assert first.network_downloaded
    path = d / first.metadata["path"]
    sel = ChatSelection(chat_key="channel_1", title="Chan", files=True)
    assert not mm.needs_network(media, sel)

    path.write_bytes(b"")  # power loss / pulled USB drive left it empty
    assert mm.indexed_copy("document", 5) is None
    assert mm.needs_network(media, sel)
    again = await _process(mm, cp, media, _writer(b"z" * 64), msg_id=2)
    assert again.network_downloaded and not again.dedup_reused


@pytest.mark.asyncio
async def test_symlinked_category_dir_is_indexed_with_its_lexical_path(tmp_path: Path) -> None:
    other_drive = tmp_path / "bigdrive" / "photos"
    other_drive.mkdir(parents=True)
    d = tmp_path / "data"
    d.mkdir()
    (d / "photos").symlink_to(other_drive)
    d, db, mm = _setup(tmp_path, d)
    cp = resolve_chat_paths(d, "channel_1", "Chan")
    out = await _process(mm, cp, _photo_media(42), _writer(b"p" * 10))
    assert not out.failed
    indexed = db.get_indexed_media("photo", 42)
    assert indexed is not None
    assert (
        indexed.rel_path == out.metadata["path"] == f"photos/{cp.folder}/2026-11-09_1_photo_42.jpg"
    )
    assert (other_drive / cp.folder / "2026-11-09_1_photo_42.jpg").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("file_name", "ext"),
    [
        ("report./../../x", ".x"),
        ("evil.pdf\x00", ".pdf"),
        ("../../../../etc/passwd.txt", ".txt"),
        ("noext.", ".pdf"),  # falls back to the MIME map
        ("weird.%%%", ".pdf"),
        ("long.abcdefghijklmnop", ".abcdefghij"),
    ],
)
async def test_untrusted_extension_never_breaks_the_download(
    tmp_path: Path, file_name: str, ext: str
) -> None:
    d, db, mm = _setup(tmp_path)
    cp = resolve_chat_paths(d, "channel_1", "Chan")
    media = _doc_media(7, size=10, file_name=file_name)
    assert classify_telethon_media(media).default_ext == ext
    out = await _process(mm, cp, media, _writer(b"x" * 10))
    assert out.network_downloaded and not out.failed
    assert (d / out.metadata["path"]).parent == cp.media_dir("files")


@pytest.mark.asyncio
async def test_incomplete_document_download_is_queued_for_retry(tmp_path: Path) -> None:
    d, db, mm = _setup(tmp_path)
    cp = resolve_chat_paths(d, "channel_1", "Chan")
    out = await _process(mm, cp, _doc_media(9, size=1000), _writer(b"x" * 600))  # short chunk
    assert out.failed and not out.network_downloaded
    assert out.metadata["skip_reason"] == "download_failed"
    assert "600 of 1000" in (out.error or "")
    assert db.get_indexed_media("document", 9) is None
    pending = db.list_pending_media("channel_1", ["files"])
    assert [r.tg_id for r in pending] == [9]
    assert "incomplete download" in (pending[0].last_error or "")
    assert list(cp.media_dir("files").iterdir()) == []  # neither the partial nor its temp


@pytest.mark.asyncio
async def test_download_is_fsynced_before_it_is_renamed(tmp_path: Path, monkeypatch) -> None:
    d, db, mm = _setup(tmp_path)
    cp = resolve_chat_paths(d, "channel_1", "Chan")
    events: list[str] = []
    real_fsync, real_replace = os.fsync, os.replace

    def spy_fsync(fd: int) -> None:
        events.append("fsync")
        real_fsync(fd)

    def spy_replace(a: Any, b: Any) -> None:
        events.append("replace")
        real_replace(a, b)

    monkeypatch.setattr(mm_mod.os, "fsync", spy_fsync)
    monkeypatch.setattr(mm_mod.os, "replace", spy_replace)
    out = await _process(mm, cp, _photo_media(1), _writer(b"p" * 10))
    assert out.network_downloaded
    assert events[-2:] == ["fsync", "replace"]


@pytest.mark.asyncio
async def test_stale_temps_are_swept_once_but_fresh_ones_kept(tmp_path: Path) -> None:
    d, db, mm = _setup(tmp_path)
    cp = resolve_chat_paths(d, "channel_1", "Chan")
    photos = cp.media_dir("photos")
    photos.mkdir(parents=True)
    stale = photos / ".dl_123_abc.jpg"  # left by a SIGKILL an hour ago
    stale.write_bytes(b"partial")
    old = time.time() - 3600
    os.utime(stale, (old, old))
    fresh = photos / ".dl_456_def.jpg"  # recent: could still be in use, keep it
    fresh.write_bytes(b"partial")
    keep_me = photos / "2026-11-01_1_photo_1.jpg"
    keep_me.write_bytes(b"real")
    os.utime(keep_me, (old, old))

    seen_during_download: list[bool] = []

    async def download(tmp: Path) -> Path:
        seen_during_download.append(tmp.exists())
        tmp.write_bytes(b"p" * 10)
        return tmp

    out = await _process(mm, cp, _photo_media(2), download, msg_id=2)
    assert out.network_downloaded and seen_during_download == [True]
    assert not stale.exists()
    assert fresh.exists() and keep_me.exists()
    assert mm.stale_temps_removed == 1

    stale.write_bytes(b"partial")
    os.utime(stale, (old, old))
    await _process(mm, cp, _photo_media(3), _writer(b"q" * 10), msg_id=3)
    assert stale.exists()  # each folder is swept only once per run (cheap)


def test_sweep_never_touches_an_active_download(tmp_path: Path) -> None:
    active = tmp_path / ".dl_9_x.jpg"
    active.write_bytes(b"in progress")
    old = time.time() - 3600
    os.utime(active, (old, old))  # e.g. a long stall with no writes
    removed = mm_mod._sweep_stale_temps(tmp_path, frozenset({active}), 60)
    assert removed == 0 and active.exists()


@pytest.mark.asyncio
async def test_dropped_connection_inside_downloader_is_a_failed_download(tmp_path: Path) -> None:
    d, db, mm = _setup(tmp_path)
    cp = resolve_chat_paths(d, "channel_1", "Chan")

    async def dropped(tmp: Path) -> Path:
        raise asyncio.CancelledError  # Telethon cancelled the request (no retry wrapper)

    out = await _process(mm, cp, _photo_media(4), dropped)
    assert out.failed and out.metadata["skip_reason"] == "download_failed"
    assert [r.tg_id for r in db.list_pending_media("channel_1", ["photos"])] == [4]
    assert not [p for p in cp.media_dir("photos").iterdir() if p.name.startswith(".dl_")]


@pytest.mark.asyncio
async def test_real_cancellation_of_a_download_propagates(tmp_path: Path) -> None:
    d, db, mm = _setup(tmp_path)
    cp = resolve_chat_paths(d, "channel_1", "Chan")
    started = asyncio.Event()

    async def hang(tmp: Path) -> Path:
        started.set()
        await asyncio.sleep(30)
        return tmp

    task = asyncio.create_task(_process(mm, cp, _photo_media(6), hang))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert db.list_pending_media("channel_1", ["photos"]) == []  # not recorded as a failure
    assert not [p for p in cp.media_dir("photos").iterdir() if p.name.startswith(".dl_")]


@pytest.mark.asyncio
async def test_disk_full_during_download_propagates_instead_of_queueing(tmp_path: Path) -> None:
    d, db, mm = _setup(tmp_path)
    cp = resolve_chat_paths(d, "channel_1", "Chan")

    async def disk_full(tmp: Path) -> Path:
        raise OSError(28, "No space left on device", str(tmp))

    with pytest.raises(OSError, match="No space left"):
        await _process(mm, cp, _photo_media(8), disk_full)
    # not "one broken attachment": nothing queued as a failure, no temp left behind
    assert db.list_pending_media("channel_1", ["photos"]) == []
    assert not [p for p in cp.media_dir("photos").iterdir() if p.name.startswith(".dl_")]
