"""Rebuild `checkpoint.db` or Markdown files purely from plain-text chat directories."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from tghoard.config import ALL_MEDIA_CATEGORIES
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.markdown_writer import rebuild_links_and_audit_md_from_parts
from tghoard.storage.paths import iter_all_part_files, parse_part_filename, uses_global_msg_ids

MEDIA_ID_IN_FILENAME_RE = re.compile(
    r"_(photo|document)_(\d+)\.[a-z0-9]+$|\d{4}-\d{2}-\d{2}_\d+_(\d+)_"
)


@dataclass
class RebuildReport:
    chats_scanned: int = 0
    part_files_scanned: int = 0
    messages_indexed: int = 0
    media_files_indexed: int = 0
    links_rebuilt: int = 0


def _sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def iter_archive_chat_dirs(data_dir: Path) -> list[tuple[str, Path]]:
    """Yield `(chat_key, chat_dir)` for all plain-text chat folders inside `data_dir`."""
    if not data_dir.exists():
        return []
    found: list[tuple[str, Path]] = []
    for child in sorted(data_dir.iterdir()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        if not (child / "chats").is_dir():
            continue
        chat_key = child.name.split("__", 1)[0]
        found.append((chat_key, child))
    return found


def rebuild_checkpoint_from_plaintext(
    data_dir: Path,
    db: CheckpointDB,
) -> RebuildReport:
    """Reconstruct `checkpoint.db` message locators, synced ranges, and media index from disk."""
    report = RebuildReport()
    db.reset_archive_indexes()

    for chat_key, chat_dir in iter_archive_chat_dirs(data_dir):
        report.chats_scanned += 1
        min_global_id: int | None = None
        max_global_id: int | None = None

        for part_path in iter_all_part_files(chat_dir):
            report.part_files_scanned += 1
            parsed = parse_part_filename(part_path.name)
            if parsed is None:
                continue
            day_str, part_num = parsed
            payload = json.loads(part_path.read_text(encoding="utf-8"))
            messages = payload.get("messages") or []

            locators: list[tuple[str, str, int, int | None, bool]] = []
            per_account_maps: dict[int, list[tuple[int, str]]] = {}

            for msg in messages:
                ckey = str(msg.get("canonical_key") or f"id:{msg.get('id')}")
                sender = msg.get("sender") or {}
                sender_id = int(sender["id"]) if sender.get("id") is not None else None
                is_del = bool(msg.get("is_deleted"))
                locators.append((ckey, day_str, part_num, sender_id, is_del))
                report.messages_indexed += 1

                mid = msg.get("id")
                if uses_global_msg_ids(chat_key) and isinstance(mid, int) and mid > 0:
                    min_global_id = mid if min_global_id is None else min(min_global_id, mid)
                    max_global_id = mid if max_global_id is None else max(max_global_id, mid)

                for acc_str, acc_mid in (msg.get("account_msg_ids") or {}).items():
                    if str(acc_str).isdigit():
                        per_account_maps.setdefault(int(acc_str), []).append((int(acc_mid), ckey))

                media = msg.get("media")
                if isinstance(media, dict) and media.get("downloaded") and media.get("path"):
                    m_abs = chat_dir / str(media["path"])
                    tg_type = media.get("tg_type")
                    tg_id = media.get("tg_id")
                    category = str(media.get("category") or "files")
                    if m_abs.exists() and tg_type and tg_id is not None:
                        sha = str(media.get("sha256") or _sha256_path(m_abs))
                        sz = int(media.get("size_bytes") or m_abs.stat().st_size)
                        rel_to_data = m_abs.resolve().relative_to(data_dir.resolve()).as_posix()
                        db.register_indexed_media(
                            tg_type=str(tg_type),
                            tg_id=int(tg_id),
                            category=category,
                            sha256=sha,
                            size_bytes=sz,
                            rel_path=rel_to_data,
                        )
                        report.media_files_indexed += 1

            db.record_messages_batch(
                chat_key=chat_key,
                account_id=None,
                locators=locators,
                account_mappings=[],
            )
            for acc_id, mappings in per_account_maps.items():
                db.record_messages_batch(
                    chat_key=chat_key,
                    account_id=acc_id,
                    locators=[],
                    account_mappings=mappings,
                )

        if (
            uses_global_msg_ids(chat_key)
            and min_global_id is not None
            and max_global_id is not None
        ):
            db.add_synced_range(chat_key, min_global_id, max_global_id)

        # Also index any standalone profile/story media files in subfolders
        for cat in ALL_MEDIA_CATEGORIES:
            sub = chat_dir / cat
            if not sub.exists():
                continue
            for fpath in sub.iterdir():
                if not fpath.is_file() or fpath.name.startswith("."):
                    continue
                m = MEDIA_ID_IN_FILENAME_RE.search(fpath.name)
                if m:
                    tg_type = m.group(1) or "document"
                    tg_id = int(m.group(2) or m.group(3))
                    if db.get_indexed_media(tg_type, tg_id) is None:
                        rel_to_data = fpath.resolve().relative_to(data_dir.resolve()).as_posix()
                        db.register_indexed_media(
                            tg_type=tg_type,
                            tg_id=tg_id,
                            category=cat,
                            sha256=_sha256_path(fpath),
                            size_bytes=fpath.stat().st_size,
                            rel_path=rel_to_data,
                        )
                        report.media_files_indexed += 1

    return report


def rebuild_markdown_from_plaintext(
    data_dir: Path,
    *,
    chat_filter: str | None = None,
) -> RebuildReport:
    """Regenerate `links.md` and `metadata/deleted_and_edits.md` from `chats/*-pt*.json`."""
    report = RebuildReport()
    for chat_key, chat_dir in iter_archive_chat_dirs(data_dir):
        if chat_filter and chat_filter not in (chat_key, chat_dir.name):
            continue
        report.chats_scanned += 1
        parts = iter_all_part_files(chat_dir)
        report.part_files_scanned += len(parts)
        title = chat_key
        if parts:
            first_payload = json.loads(parts[0].read_text(encoding="utf-8"))
            title = str(first_payload.get("chat_title") or chat_key)
        links_cnt, _, _ = rebuild_links_and_audit_md_from_parts(
            chat_dir,
            chat_key=chat_key,
            chat_title=title,
        )
        report.links_rebuilt += links_cnt
    return report
