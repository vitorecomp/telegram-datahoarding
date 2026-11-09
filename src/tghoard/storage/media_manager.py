"""Deterministic 4-way media classifier and cross-account/cross-chat deduplicating downloader."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telethon.tl import types as tl_types

from tghoard.config import MediaCategory
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow

SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._-]+")

MIME_EXT_MAP: dict[str, str] = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/heic": ".heic",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "video/quicktime": ".mov",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/x-wav": ".wav",
    "audio/flac": ".flac",
    "application/pdf": ".pdf",
    "application/zip": ".zip",
}


@dataclass
class ClassifiedMedia:
    category: MediaCategory
    tg_type: str  # "photo" | "document"
    tg_id: int
    file_name: str | None
    mime_type: str | None
    size_bytes: int | None
    default_ext: str


@dataclass
class MediaProcessOutcome:
    metadata: dict[str, Any]
    network_downloaded: bool = False
    dedup_reused: bool = False
    queued_pending: bool = False


def _sanitize_filename(name: str) -> str:
    cleaned = SAFE_FILENAME_RE.sub("_", name).strip("._")
    return cleaned[:96] or "attachment"


def classify_telethon_media(media: Any) -> ClassifiedMedia | None:
    """Classify a Telethon `Message.media` (or duck-typed media object) into 1 of 4 buckets:
    `photos` | `videos` | `audio` | `files`.
    """
    if media is None:
        return None

    # 1. Standard Telegram Photo (MessageMediaPhoto or raw Photo)
    photo = getattr(media, "photo", None)
    if isinstance(media, tl_types.Photo):
        photo = media
    if photo is not None and getattr(photo, "id", None) is not None:
        size_bytes: int | None = None
        for sz in getattr(photo, "sizes", None) or []:
            s_val = getattr(sz, "size", None)
            if isinstance(s_val, int):
                size_bytes = max(size_bytes or 0, s_val)
            elif hasattr(sz, "sizes") and getattr(sz, "sizes", None):
                size_bytes = max(size_bytes or 0, max(sz.sizes))
        return ClassifiedMedia(
            category="photos",
            tg_type="photo",
            tg_id=int(photo.id),
            file_name=None,
            mime_type="image/jpeg",
            size_bytes=size_bytes,
            default_ext=".jpg",
        )

    # 2. Document-backed media (videos, GIFs, round notes, voice, music, files, stickers)
    doc = getattr(media, "document", None)
    if isinstance(media, tl_types.Document):
        doc = media
    if doc is None or getattr(doc, "id", None) is None:
        return None

    tg_id = int(doc.id)
    mime = str(getattr(doc, "mime_type", "") or "").lower()
    size_val = getattr(doc, "size", None)
    doc_size = int(size_val) if isinstance(size_val, int) else None
    attrs = getattr(doc, "attributes", None) or []

    file_name: str | None = None
    is_sticker = False
    is_audio = False
    is_video = False

    for attr in attrs:
        cls_name = type(attr).__name__
        if (
            isinstance(attr, tl_types.DocumentAttributeFilename)
            or cls_name == "DocumentAttributeFilename"
        ):
            file_name = getattr(attr, "file_name", None)
        elif (
            isinstance(attr, tl_types.DocumentAttributeSticker)
            or cls_name == "DocumentAttributeSticker"
        ):
            is_sticker = True
        elif (
            isinstance(attr, tl_types.DocumentAttributeAudio)
            or cls_name == "DocumentAttributeAudio"
        ):
            is_audio = True
        elif isinstance(attr, tl_types.DocumentAttributeVideo) or cls_name in (
            "DocumentAttributeVideo",
            "DocumentAttributeAnimated",
        ):
            is_video = True

    # Determine category deterministically:
    # Stickers (.webp / .tgs / .webm) go to `files` (neither photos, videos, nor audio)
    if is_sticker or mime == "application/x-tgsticker":
        category: MediaCategory = "files"
    elif is_audio or mime.startswith("audio/"):
        category = "audio"
    elif is_video or mime.startswith("video/"):
        category = "videos"
    elif mime.startswith("image/"):
        category = "photos"
    else:
        category = "files"

    ext = ""
    if file_name and "." in file_name:
        ext = "." + file_name.rsplit(".", 1)[-1].lower()[:10]
    if not ext:
        ext = MIME_EXT_MAP.get(mime, ".bin")

    return ClassifiedMedia(
        category=category,
        tg_type="document",
        tg_id=tg_id,
        file_name=file_name,
        mime_type=mime or None,
        size_bytes=doc_size,
        default_ext=ext,
    )


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def _link_or_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


class MediaManager:
    """Handles category routing (`photos/`, `videos/`, `audio/`, `files/`) and deduplication."""

    def __init__(
        self,
        *,
        data_dir: Path,
        checkpoint_db: CheckpointDB,
        max_media_bytes: int | None = 50 * 1024 * 1024,
    ) -> None:
        self.data_dir = data_dir
        self.db = checkpoint_db
        self.max_media_bytes = max_media_bytes

    def _build_target_filename(
        self,
        *,
        prefix_tag: str,
        classified: ClassifiedMedia,
    ) -> str:
        if classified.file_name:
            stem, dot, ext = classified.file_name.rpartition(".")
            if dot:
                safe_stem = _sanitize_filename(stem)[:60]
                safe_ext = "." + _sanitize_filename(ext)[:10]
                return f"{prefix_tag}_{classified.tg_id}_{safe_stem}{safe_ext}"
        return f"{prefix_tag}_{classified.tg_type}_{classified.tg_id}{classified.default_ext}"

    async def process_message_media(
        self,
        *,
        media: Any,
        selection: ChatSelection,
        chat_dir: Path,
        chat_key: str,
        canonical_key: str,
        account_id: int,
        msg_id: int,
        day_str: str,
        downloader: Callable[[Path], Awaitable[Path | str | None]],
    ) -> MediaProcessOutcome | None:
        """Classify, deduplicate or download (or defer to `pending_media`) a message attachment."""
        classified = classify_telethon_media(media)
        if classified is None:
            return None

        base_meta: dict[str, Any] = {
            "category": classified.category,
            "tg_type": classified.tg_type,
            "tg_id": classified.tg_id,
            "file_name": classified.file_name,
            "mime_type": classified.mime_type,
            "size_bytes": classified.size_bytes,
            "downloaded": False,
            "path": None,
            "sha256": None,
        }

        # Check if user enabled this category checkbox in `chats.md`
        if not selection.allows_category(classified.category):
            self.db.enqueue_pending_media(
                PendingMediaRow(
                    chat_key=chat_key,
                    canonical_key=canonical_key,
                    account_id=account_id,
                    msg_id=msg_id,
                    day_str=day_str,
                    category=classified.category,
                    tg_type=classified.tg_type,
                    tg_id=classified.tg_id,
                    file_name=classified.file_name,
                    mime_type=classified.mime_type,
                    size_bytes=classified.size_bytes,
                )
            )
            return MediaProcessOutcome(metadata=base_meta, queued_pending=True)

        return await self._acquire_media_file(
            classified=classified,
            chat_dir=chat_dir,
            prefix_tag=f"{day_str}_{msg_id}",
            base_meta=base_meta,
            downloader=downloader,
        )

    async def download_standalone_media(
        self,
        *,
        media: Any,
        selection: ChatSelection,
        chat_dir: Path,
        prefix_tag: str,
        downloader: Callable[[Path], Awaitable[Path | str | None]],
    ) -> MediaProcessOutcome | None:
        """Download Story or Profile media respecting `[x] photos` / `[x] videos` checkboxes."""
        classified = classify_telethon_media(media)
        if classified is None:
            return None
        base_meta: dict[str, Any] = {
            "category": classified.category,
            "tg_type": classified.tg_type,
            "tg_id": classified.tg_id,
            "file_name": classified.file_name,
            "mime_type": classified.mime_type,
            "size_bytes": classified.size_bytes,
            "downloaded": False,
            "path": None,
            "sha256": None,
        }
        if not selection.allows_category(classified.category):
            return MediaProcessOutcome(metadata=base_meta)

        return await self._acquire_media_file(
            classified=classified,
            chat_dir=chat_dir,
            prefix_tag=prefix_tag,
            base_meta=base_meta,
            downloader=downloader,
        )

    async def _acquire_media_file(
        self,
        *,
        classified: ClassifiedMedia,
        chat_dir: Path,
        prefix_tag: str,
        base_meta: dict[str, Any],
        downloader: Callable[[Path], Awaitable[Path | str | None]],
    ) -> MediaProcessOutcome:
        target_subdir = chat_dir / classified.category
        target_subdir.mkdir(parents=True, exist_ok=True)
        target_filename = self._build_target_filename(
            prefix_tag=prefix_tag,
            classified=classified,
        )
        dest_path = target_subdir / target_filename

        # 1. Check global cross-account / cross-chat `media_index` first!
        indexed = self.db.get_indexed_media(classified.tg_type, classified.tg_id)
        if indexed is not None:
            indexed_abs = self.data_dir / indexed.rel_path
            if indexed_abs.exists():
                if indexed_abs.parent.resolve() == target_subdir.resolve():
                    rel_in_chat = f"{classified.category}/{indexed_abs.name}"
                else:
                    _link_or_copy(indexed_abs, dest_path)
                    rel_in_chat = f"{classified.category}/{dest_path.name}"
                meta = dict(base_meta)
                meta.update(
                    {
                        "downloaded": True,
                        "path": rel_in_chat,
                        "sha256": indexed.sha256,
                        "size_bytes": indexed.size_bytes,
                    }
                )
                return MediaProcessOutcome(metadata=meta, dedup_reused=True)

        # 2. Enforce max_media_bytes guard before hitting network
        if (
            self.max_media_bytes is not None
            and classified.size_bytes is not None
            and classified.size_bytes > self.max_media_bytes
        ):
            meta = dict(base_meta)
            meta["skip_reason"] = "exceeds_max_media_mb"
            return MediaProcessOutcome(metadata=meta)

        # 3. Download into temporary file inside target_subdir, hash, and atomically move
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".dl_{classified.tg_id}_",
            suffix=classified.default_ext,
            dir=str(target_subdir),
        )
        os.close(fd)
        tmp_path = Path(tmp_name)
        try:
            dl_res = await downloader(tmp_path)
            actual_tmp = Path(dl_res) if dl_res is not None else tmp_path
            if not actual_tmp.exists() or actual_tmp.stat().st_size == 0:
                tmp_path.unlink(missing_ok=True)
                meta = dict(base_meta)
                meta["skip_reason"] = "empty_download"
                return MediaProcessOutcome(metadata=meta)

            sha256 = _sha256_file(actual_tmp)
            size_bytes = actual_tmp.stat().st_size
            os.replace(actual_tmp, dest_path)
            if actual_tmp != tmp_path:
                tmp_path.unlink(missing_ok=True)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

        rel_to_data = dest_path.resolve().relative_to(self.data_dir.resolve()).as_posix()
        self.db.register_indexed_media(
            tg_type=classified.tg_type,
            tg_id=classified.tg_id,
            category=classified.category,
            sha256=sha256,
            size_bytes=size_bytes,
            rel_path=rel_to_data,
        )
        meta = dict(base_meta)
        meta.update(
            {
                "downloaded": True,
                "path": f"{classified.category}/{dest_path.name}",
                "sha256": sha256,
                "size_bytes": size_bytes,
            }
        )
        return MediaProcessOutcome(metadata=meta, network_downloaded=True)
