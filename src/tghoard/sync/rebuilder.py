"""Rebuild `checkpoint.db` or Markdown files purely from plain-text chat directories."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from tghoard.config import ALL_MEDIA_CATEGORIES
from tghoard.storage.atomic import merge_ranges, utc_now_iso
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow
from tghoard.storage.markdown_writer import rebuild_links_and_audit_md_from_parts
from tghoard.storage.paths import (
    ChatPaths,
    iter_all_part_files,
    iter_chat_paths,
    parse_part_filename,
    uses_global_msg_ids,
)
from tghoard.utils.verify_media import _pending_row

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
    synced_ranges: int = 0
    pending_media_queued: int = 0


def _sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def iter_archive_chat_dirs(data_dir: Path) -> list[tuple[str, ChatPaths]]:
    """Yield `(chat_key, ChatPaths)` for every chat in the category-first layout.

    Identity comes from `metadata/<folder>/info.md`, so this works without `checkpoint.db`.
    """
    if not data_dir.exists():
        return []
    return iter_chat_paths(data_dir)


def _replace_synced_ranges(db: CheckpointDB, chat_key: str, ranges: list[tuple[int, int]]) -> None:
    """Store a chat's synced ranges in one transaction (bulk `add_synced_range`)."""
    now = utc_now_iso()
    conn = db._conn
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM chat_synced_ranges WHERE chat_key = ?", (chat_key,))
        conn.executemany(
            "INSERT INTO chat_synced_ranges (chat_key, start_id, end_id, updated_at) "
            "VALUES (?, ?, ?, ?)",
            [(chat_key, s, e, now) for s, e in ranges],
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _store_pending_media(
    db: CheckpointDB, chat_key: str, queued: list[PendingMediaRow], downloaded: list[str]
) -> None:
    """Make `pending_media` match one part file.

    `queued`: media with `downloaded: false` -> (re)queued, even if a stale row says it was
    downloaded (attempts/backoff of rows already queued are kept). `downloaded`: canonical
    keys of media the JSON has on disk -> any stale queue row is settled, never refetched.
    """
    if queued:
        conn = db._conn
        conn.execute("BEGIN")
        try:
            conn.executemany(
                "UPDATE pending_media SET downloaded = 0 "
                "WHERE chat_key = ? AND canonical_key = ? AND downloaded = 1",
                [(r.chat_key, r.canonical_key) for r in queued],
            )
            for row in queued:
                db.enqueue_pending_media(row)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    db.mark_pending_media_downloaded_many(chat_key, downloaded)


def rebuild_checkpoint_from_plaintext(
    data_dir: Path,
    db: CheckpointDB,
) -> RebuildReport:
    """Rebuild `checkpoint.db` from disk: locators, synced ranges, media index, media queue.

    Synced ranges (channels/supergroups) cover only the ids actually archived, merging
    adjacent ids: an archive holding ids 1-5 and 90000-90005 yields `[1, 5], [90000, 90005]`,
    so the next sync still fetches 6-89999. Small holes (deleted messages, usually) are
    re-checked cheaply by the sync with `get_messages(ids=...)`.
    """
    report = RebuildReport()
    db.reset_archive_indexes()
    accounts = db.list_accounts()
    fallback_account = accounts[0].account_id if accounts else None

    for chat_key, chat_paths in iter_archive_chat_dirs(data_dir):
        report.chats_scanned += 1
        global_ids = uses_global_msg_ids(chat_key)
        chat_ranges: list[tuple[int, int]] = []

        for part_path in iter_all_part_files(chat_paths):
            report.part_files_scanned += 1
            parsed = parse_part_filename(part_path.name)
            if parsed is None:
                continue
            day_str, part_num = parsed
            payload = json.loads(part_path.read_text(encoding="utf-8"))
            messages = payload.get("messages") or []

            locators: list[tuple[str, str, int, int | None, bool]] = []
            per_account_maps: dict[int, list[tuple[int, str]]] = {}
            part_ids: list[int] = []
            queued: list[PendingMediaRow] = []
            settled: list[str] = []

            for msg in messages:
                ckey = str(msg.get("canonical_key") or f"id:{msg.get('id')}")
                sender = msg.get("sender") or {}
                sender_id = int(sender["id"]) if sender.get("id") is not None else None
                is_del = bool(msg.get("is_deleted"))
                locators.append((ckey, day_str, part_num, sender_id, is_del))
                report.messages_indexed += 1

                mid = msg.get("id")
                if global_ids and isinstance(mid, int) and mid > 0:
                    part_ids.append(mid)

                for acc_str, acc_mid in (msg.get("account_msg_ids") or {}).items():
                    if str(acc_str).isdigit():
                        per_account_maps.setdefault(int(acc_str), []).append((int(acc_mid), ckey))

                media = msg.get("media")
                if isinstance(media, dict) and media.get("tg_id") is not None:
                    if not media.get("downloaded"):
                        row = _pending_row(chat_key, day_str, msg, media, fallback_account)
                        if row is not None:
                            queued.append(row)
                    else:
                        settled.append(ckey)
                if isinstance(media, dict) and media.get("downloaded") and media.get("path"):
                    m_abs = data_dir / str(media["path"])
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
            _store_pending_media(db, chat_key, queued, settled)
            report.pending_media_queued += len(queued)
            # merge adjacent ids only; ranges are kept per part (memory ~ number of holes)
            chat_ranges.extend(merge_ranges((i, i) for i in part_ids))

        chat_ranges = merge_ranges(chat_ranges)
        if global_ids and chat_ranges:
            _replace_synced_ranges(db, chat_key, chat_ranges)
            report.synced_ranges += len(chat_ranges)

        # Also index any standalone profile/story media files in subfolders
        for cat in ALL_MEDIA_CATEGORIES:
            sub = chat_paths.media_dir(cat)
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
    """Regenerate `links/<folder>.md` and `deleted_and_edits.md` from `messages/<folder>/*.json`."""
    report = RebuildReport()
    for chat_key, chat_paths in iter_archive_chat_dirs(data_dir):
        if chat_filter and chat_filter not in (chat_key, chat_paths.folder):
            continue
        report.chats_scanned += 1
        parts = iter_all_part_files(chat_paths)
        report.part_files_scanned += len(parts)
        title = chat_key
        if parts:
            first_payload = json.loads(parts[0].read_text(encoding="utf-8"))
            title = str(first_payload.get("chat_title") or chat_key)
        links_cnt, _, _ = rebuild_links_and_audit_md_from_parts(
            chat_paths,
            chat_key=chat_key,
            chat_title=title,
        )
        report.links_rebuilt += links_cnt
    return report
