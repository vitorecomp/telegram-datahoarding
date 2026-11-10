"""Deterministic 4-way media classifier and cross-account/cross-chat deduplicating downloader."""

from __future__ import annotations

import asyncio
import errno
import hashlib
import os
import re
import shutil
import stat
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telethon import errors as tg_errors
from telethon.tl import types as tl_types

from tghoard.config import MediaCategory
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB, IndexedMedia, PendingMediaRow
from tghoard.storage.paths import ChatPaths

SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9._-]+")
UNSAFE_EXT_CHARS_RE = re.compile(r"[^A-Za-z0-9]+")

# Prefix of in-progress download/copy temporaries inside a media folder. Leftovers (SIGKILL,
# power loss, pulled drive) older than STALE_TEMP_SECS are removed the first time this
# process writes into that folder.
TEMP_PREFIX = ".dl_"
STALE_TEMP_SECS = 15 * 60

# Disk full / read-only filesystem / quota: no other file could be written either, so these
# are never "one broken attachment" — they propagate and stop the sync.
FATAL_STORAGE_ERRNOS = frozenset(
    e for e in (errno.ENOSPC, errno.EROFS, getattr(errno, "EDQUOT", None)) if e is not None
)


def fatal_storage_error(exc: BaseException) -> OSError | None:
    """The disk-full/read-only `OSError` in `exc`'s cause/context chain, if any."""
    seen: list[BaseException] = []
    cur: BaseException | None = exc
    while cur is not None and all(cur is not s for s in seen):
        if isinstance(cur, OSError) and cur.errno in FATAL_STORAGE_ERRNOS:
            return cur
        seen.append(cur)
        cur = cur.__cause__ or cur.__context__
    return None


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
    # Download raised or returned nothing: message is still saved, media is re-queued in
    # `pending_media` and retried on the next sync.
    failed: bool = False
    error: str | None = None


def _sanitize_filename(name: str) -> str:
    cleaned = SAFE_FILENAME_RE.sub("_", name).strip("._")
    return cleaned[:96] or "attachment"


def _sanitize_ext(raw: str) -> str:
    """`.ext` with only ASCII letters/digits (max 10), or "" — safe as a `mkstemp` suffix."""
    cleaned = UNSAFE_EXT_CHARS_RE.sub("", raw)[:10].lower()
    return f".{cleaned}" if cleaned else ""


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
        # Untrusted (sender-chosen) name: e.g. `x./../y` or a NUL must never reach mkstemp
        ext = _sanitize_ext(file_name.rsplit(".", 1)[-1])
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


def _hash_and_sync(path: Path) -> tuple[str, int]:
    """SHA-256 and size of a finished download, flushed to disk before it's renamed/indexed.

    Without the fsync, a power loss / pulled USB drive right after `os.replace` can leave a
    zero-length or partial file that `media_index` already trusts (and hardlinks elsewhere).
    """
    hasher = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while chunk := fh.read(1024 * 1024):
            hasher.update(chunk)
            size += len(chunk)
        os.fsync(fh.fileno())
    return hasher.hexdigest(), size


def _file_has_size(path: Path, expected: int | None) -> bool:
    """True if `path` is a regular file with exactly `expected` bytes (any size if None)."""
    try:
        st = path.stat()
    except OSError:
        return False
    return stat.S_ISREG(st.st_mode) and (expected is None or st.st_size == expected)


def _link_or_copy(src: Path, dst: Path, expected_size: int | None = None) -> None:
    """Make `dst` a hardlink to (or, across filesystems / on exFAT, a copy of) `src`.

    An existing `dst` is trusted only if it has `expected_size` bytes; anything else (e.g. a
    copy interrupted by SIGKILL in an older version) is replaced. The new file is built
    under a temporary name and renamed into place, so `dst` is never seen half-written.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    if _file_has_size(dst, expected_size):
        return
    fd, tmp_name = tempfile.mkstemp(prefix=f"{TEMP_PREFIX}cp_", dir=str(dst.parent))
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        tmp.unlink()  # os.link needs a free name; mkstemp only reserved a unique one
        try:
            os.link(src, tmp)
        except OSError:
            shutil.copy2(src, tmp)
            with tmp.open("rb") as fh:
                os.fsync(fh.fileno())
        os.replace(tmp, dst)
    finally:
        tmp.unlink(missing_ok=True)  # no-op after a successful replace


def _sweep_stale_temps(directory: Path, keep: frozenset[Path], max_age: float) -> int:
    """Delete `.dl_*` leftovers older than `max_age` seconds (never the ones in `keep`)."""
    removed = 0
    cutoff = time.time() - max_age
    try:
        entries = list(os.scandir(directory))
    except OSError:
        return 0
    for entry in entries:
        if not entry.name.startswith(TEMP_PREFIX):
            continue
        path = Path(entry.path)
        if path in keep:
            continue
        try:
            if entry.is_file(follow_symlinks=False) and entry.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError:
            pass  # best effort
    return removed


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
        # One lock per media (tg_type, tg_id): chats syncing in parallel that contain the same
        # media (e.g. forwarded) download it once and the others reuse it via hardlink.
        # key -> [lock, users]: the entry lives while anyone holds or waits for the lock
        self._media_locks: dict[tuple[str, int], list[Any]] = {}
        # Temp files of downloads in progress (never swept) and folders already swept
        self._active_temps: set[Path] = set()
        self._swept_dirs: set[Path] = set()
        self.stale_temps_removed = 0

    def indexed_copy(self, tg_type: str, tg_id: int) -> IndexedMedia | None:
        """The `media_index` entry for this media if its file is really on disk, else None.

        "Really on disk" = a regular file with exactly the indexed size: an interrupted copy,
        a file truncated by a power loss or an emptied file is not reused (it gets
        downloaded again instead).
        """
        indexed = self.db.get_indexed_media(tg_type, tg_id)
        if indexed is None or not _file_has_size(
            self.data_dir / indexed.rel_path, indexed.size_bytes
        ):
            return None
        return indexed

    async def _sweep_dir_once(self, directory: Path) -> None:
        """Remove stale `.dl_*` temps the first time this run downloads into `directory`."""
        if directory in self._swept_dirs:
            return
        self._swept_dirs.add(directory)  # before the await: concurrent callers skip it
        self.stale_temps_removed += await asyncio.to_thread(
            _sweep_stale_temps, directory, frozenset(self._active_temps), STALE_TEMP_SECS
        )

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

    def needs_network(self, media: Any, selection: ChatSelection) -> bool:
        """True only if processing this media would download it from Telegram.

        False for categories not enabled in chats.md (just queued as pending), media already
        on disk via `media_index` (hardlink/reuse) and files over `max_media_bytes`: those are
        handled inline, so the download pool only carries real network downloads.
        """
        classified = classify_telethon_media(media)
        if classified is None or not selection.allows_category(classified.category):
            return False
        if self.indexed_copy(classified.tg_type, classified.tg_id) is not None:
            return False
        return not (
            self.max_media_bytes is not None
            and classified.size_bytes is not None
            and classified.size_bytes > self.max_media_bytes
        )

    async def process_message_media(
        self,
        *,
        media: Any,
        selection: ChatSelection,
        chat_paths: ChatPaths,
        chat_key: str,
        canonical_key: str,
        account_id: int,
        msg_id: int,
        day_str: str,
        file_day: str | None = None,
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

        pending_row = PendingMediaRow(
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

        # Check if user enabled this category checkbox in `chats.md`
        if not selection.allows_category(classified.category):
            self.db.enqueue_pending_media(pending_row)
            return MediaProcessOutcome(metadata=base_meta, queued_pending=True)

        try:
            outcome = await self._acquire_media_file(
                classified=classified,
                chat_paths=chat_paths,
                prefix_tag=f"{file_day or day_str}_{msg_id}",
                base_meta=base_meta,
                downloader=downloader,
            )
        except (tg_errors.UnauthorizedError, tg_errors.AuthKeyError):
            raise  # the account lost its authorization: not this attachment's fault
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is None or task.cancelling():
                raise  # really being cancelled (chat aborted, Ctrl+C): propagate
            # Telethon cancelled the request because its connection dropped (and the
            # downloader didn't turn that into ConnectionError): a failed download.
            return self._failed(pending_row, base_meta, "ConnectionError: connection dropped")
        except Exception as exc:
            if fatal_storage_error(exc) is not None:
                raise  # disk full / read-only: every other download would fail the same way
            # A single broken attachment must never abort the chat: keep the message,
            # queue the media and retry it (with a fresh file_reference) on the next sync.
            return self._failed(pending_row, base_meta, f"{type(exc).__name__}: {exc}")

        skip_reason = outcome.metadata.get("skip_reason")
        if skip_reason == "empty_download":
            self.db.record_media_failure(pending_row, skip_reason)
            outcome.failed = True
            outcome.error = skip_reason
        elif outcome.failed:  # e.g. incomplete download (size differs from the document's)
            self.db.record_media_failure(pending_row, outcome.error or "download_failed")
        elif skip_reason == "exceeds_max_media_mb":
            # Not a failure, but queued so raising TGHOARD_MAX_MEDIA_MB later fetches it.
            self.db.enqueue_pending_media(pending_row)
        return outcome

    def _failed(
        self, pending_row: PendingMediaRow, base_meta: dict[str, Any], error: str
    ) -> MediaProcessOutcome:
        """Keep the message, queue the media in `pending_media` for the next sync."""
        self.db.record_media_failure(pending_row, error)
        meta = dict(base_meta)
        meta["skip_reason"] = "download_failed"
        return MediaProcessOutcome(metadata=meta, failed=True, error=error)

    async def download_standalone_media(
        self,
        *,
        media: Any,
        selection: ChatSelection,
        chat_paths: ChatPaths,
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
            chat_paths=chat_paths,
            prefix_tag=prefix_tag,
            base_meta=base_meta,
            downloader=downloader,
        )

    async def _acquire_media_file(
        self,
        *,
        classified: ClassifiedMedia,
        chat_paths: ChatPaths,
        prefix_tag: str,
        base_meta: dict[str, Any],
        downloader: Callable[[Path], Awaitable[Path | str | None]],
    ) -> MediaProcessOutcome:
        key = (classified.tg_type, classified.tg_id)
        entry = self._media_locks.setdefault(key, [asyncio.Lock(), 0])
        entry[1] += 1  # counted before waiting, so the entry can't vanish under a waiter
        try:
            async with entry[0]:
                return await self._acquire_media_file_locked(
                    classified=classified,
                    chat_paths=chat_paths,
                    prefix_tag=prefix_tag,
                    base_meta=base_meta,
                    downloader=downloader,
                )
        finally:
            entry[1] -= 1
            if entry[1] == 0 and self._media_locks.get(key) is entry:
                del self._media_locks[key]

    async def _acquire_media_file_locked(
        self,
        *,
        classified: ClassifiedMedia,
        chat_paths: ChatPaths,
        prefix_tag: str,
        base_meta: dict[str, Any],
        downloader: Callable[[Path], Awaitable[Path | str | None]],
    ) -> MediaProcessOutcome:
        target_subdir = chat_paths.media_dir(classified.category)  # created only on download
        target_filename = self._build_target_filename(
            prefix_tag=prefix_tag,
            classified=classified,
        )
        dest_path = target_subdir / target_filename

        # 1. Check global cross-account / cross-chat `media_index` first! (only trusted
        #    when the indexed file is on disk with its full size)
        indexed = self.indexed_copy(classified.tg_type, classified.tg_id)
        if indexed is not None:
            indexed_abs = self.data_dir / indexed.rel_path
            if indexed_abs.parent.resolve() == target_subdir.resolve():
                rel_in_chat = chat_paths.media_rel(classified.category, indexed_abs.name)
            else:
                # off the event loop: on filesystems without hardlinks (exFAT) this is a
                # full file copy
                await asyncio.to_thread(_link_or_copy, indexed_abs, dest_path, indexed.size_bytes)
                rel_in_chat = chat_paths.media_rel(classified.category, dest_path.name)
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
        target_subdir.mkdir(parents=True, exist_ok=True)
        await self._sweep_dir_once(target_subdir)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f"{TEMP_PREFIX}{classified.tg_id}_",
            suffix=classified.default_ext,
            dir=str(target_subdir),
        )
        os.close(fd)
        tmp_path = Path(tmp_name)
        self._active_temps.add(tmp_path)
        try:
            dl_res = await downloader(tmp_path)
            actual_tmp = Path(dl_res) if dl_res is not None else tmp_path
            if not actual_tmp.exists() or actual_tmp.stat().st_size == 0:
                tmp_path.unlink(missing_ok=True)
                meta = dict(base_meta)
                meta["skip_reason"] = "empty_download"
                return MediaProcessOutcome(metadata=meta)

            # hash + fsync off the event loop, before the file is renamed and indexed
            sha256, size_bytes = await asyncio.to_thread(_hash_and_sync, actual_tmp)
            expected = classified.size_bytes if classified.tg_type == "document" else None
            if expected and size_bytes != expected:
                # Telethon stops at the first short chunk without checking the total: a
                # truncated file must not be indexed. Retried on the next sync.
                actual_tmp.unlink(missing_ok=True)
                tmp_path.unlink(missing_ok=True)
                error = f"incomplete download: got {size_bytes} of {expected} bytes"
                meta = dict(base_meta)
                meta["skip_reason"] = "download_failed"
                return MediaProcessOutcome(metadata=meta, failed=True, error=error)
            os.replace(actual_tmp, dest_path)
            if actual_tmp != tmp_path:
                tmp_path.unlink(missing_ok=True)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        finally:
            self._active_temps.discard(tmp_path)

        # Lexical, not `resolve()`d: a category folder may be a symlink to another drive
        rel_to_data = chat_paths.media_rel(classified.category, dest_path.name)
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
                "path": rel_to_data,
                "sha256": sha256,
                "size_bytes": size_bytes,
            }
        )
        return MediaProcessOutcome(metadata=meta, network_downloaded=True)
