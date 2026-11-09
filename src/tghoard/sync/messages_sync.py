"""Incremental message synchronization, batch checkpointing, and retroactive pending_media drain."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from tghoard.storage.chat_writer import ChatDayWriter, make_canonical_message_key
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.markdown_writer import (
    ensure_initial_metadata_files,
    rebuild_links_and_audit_md_from_parts,
)
from tghoard.storage.media_manager import (
    ClassifiedMedia,
    MediaManager,
    classify_telethon_media,
)
from tghoard.storage.paths import resolve_chat_dir, uses_global_msg_ids
from tghoard.telegram.entities import serialize_message_to_dict, to_utc_iso
from tghoard.telegram.throttler import RateLimiter

BATCH_SIZE = 100


@dataclass
class ChatSyncResult:
    chat_key: str
    messages_saved: int = 0
    messages_dedup_skipped: int = 0
    edits_detected: int = 0
    media_downloaded: int = 0
    media_dedup_reused: int = 0
    media_deferred: int = 0
    retroactive_media_downloaded: int = 0
    links_total: int = 0


def _day_str_from_iso(date_iso: str) -> str:
    if len(date_iso) >= 10 and date_iso[4] == "-" and date_iso[7] == "-":
        return date_iso[:10]
    return "1970-01-01"


async def _flush_message_batch(
    *,
    batch_items: list[tuple[str, int, str, int | None, dict[str, Any]]],
    chat_dir: Path,
    chat_key: str,
    chat_title: str,
    account_id: int,
    db: CheckpointDB,
    writer: ChatDayWriter,
    global_range: tuple[int, int] | None,
    cursor_offset_id: int | None,
    cursor_completed: bool | None,
    folder_rel_path: str,
) -> int:
    """Write a batch of messages to `-ptNNN.json` files and commit SQLite checkpoint."""
    if not batch_items:
        return 0

    by_day: dict[str, list[dict[str, Any]]] = {}
    for day_str, _, _, _, msg_dict in batch_items:
        by_day.setdefault(day_str, []).append(msg_dict)

    part_lookup: dict[tuple[str, str], int] = {}
    edits_count = 0
    for day_str, day_msgs in sorted(by_day.items()):
        part_map, edits = writer.upsert_day_messages(
            chat_dir=chat_dir,
            chat_key=chat_key,
            chat_title=chat_title,
            day_str=day_str,
            incoming_messages=day_msgs,
        )
        edits_count += len(edits)
        db.update_day_part_numbers(chat_key, day_str, part_map)
        for ckey, pnum in part_map.items():
            part_lookup[(day_str, ckey)] = pnum

    locators = [
        (
            ckey,
            day_str,
            part_lookup.get((day_str, ckey), 0),
            sender_id,
            bool(msg_dict.get("is_deleted")),
        )
        for day_str, _, ckey, sender_id, msg_dict in batch_items
    ]
    account_mappings = [(msg_id, ckey) for _, msg_id, ckey, _, _ in batch_items]

    db.record_messages_batch(
        chat_key=chat_key,
        account_id=account_id,
        locators=locators,
        account_mappings=account_mappings,
    )
    if global_range is not None:
        db.add_synced_range(chat_key, global_range[0], global_range[1])

    db.upsert_chat_cursor(
        chat_key=chat_key,
        account_id=account_id,
        folder_rel_path=folder_rel_path,
        backfill_offset_id=cursor_offset_id,
        backfill_completed=cursor_completed,
    )
    return edits_count


async def sync_chat_messages(
    *,
    client: Any,
    entity: Any,
    account_id: int,
    selection: ChatSelection,
    data_dir: Path,
    db: CheckpointDB,
    writer: ChatDayWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
    since_date: datetime | None = None,
) -> ChatSyncResult:
    """Sync missing message intervals for a chat + drain any newly enabled media categories."""
    chat_key = selection.chat_key
    res = ChatSyncResult(chat_key=chat_key)
    if not selection.baixar:
        return res

    chat_dir = resolve_chat_dir(data_dir, chat_key, selection.title)
    ensure_initial_metadata_files(chat_dir, chat_key=chat_key, chat_title=selection.title)
    folder_rel = chat_dir.resolve().relative_to(data_dir.resolve()).as_posix()

    # Record that this account participates in this chat
    db.upsert_chat_cursor(
        chat_key=chat_key,
        account_id=account_id,
        folder_rel_path=folder_rel,
    )

    touched_messages = False

    if uses_global_msg_ids(chat_key):
        # Supergroups & Channels: global message IDs -> compute exact missing intervals!
        top_list = await limiter.call(
            lambda: client.get_messages(entity, limit=1),
            description=f"get_top_message({chat_key})",
        )
        max_server_id = 0
        if top_list:
            first_item = top_list[0] if isinstance(top_list, (list, tuple)) else top_list
            max_server_id = int(getattr(first_item, "id", 0) or 0)

        if max_server_id > 0:
            missing_ranges = db.get_missing_ranges(chat_key, 1, max_server_id)
            if not missing_ranges:
                # 100% already synced by another account or previous run!
                synced = db.get_synced_ranges(chat_key)
                res.messages_dedup_skipped = sum((e - s + 1) for s, e in synced)

            for r_start, r_end in missing_ranges:
                batch: list[tuple[str, int, str, int | None, dict[str, Any]]] = []
                batch_ids: list[int] = []

                # Iterate oldest->newest within [r_start, r_end] so checkpoints advance cleanly!
                iter_kwargs: dict[str, Any] = {
                    "min_id": r_start - 1,
                    "max_id": r_end + 1,
                    "reverse": True,
                }
                async for msg in client.iter_messages(entity, **iter_kwargs):
                    msg_id = int(getattr(msg, "id", 0) or 0)
                    if msg_id <= 0:
                        continue
                    msg_dt = getattr(msg, "date", None)
                    if (
                        since_date is not None
                        and isinstance(msg_dt, datetime)
                        and msg_dt < since_date
                    ):
                        batch_ids.append(msg_id)
                        continue

                    item_tuple = await _process_single_message(
                        client=client,
                        msg=msg,
                        chat_key=chat_key,
                        account_id=account_id,
                        selection=selection,
                        chat_dir=chat_dir,
                        db=db,
                        media_manager=media_manager,
                        limiter=limiter,
                        res=res,
                    )
                    batch.append(item_tuple)
                    batch_ids.append(msg_id)

                    if len(batch) >= BATCH_SIZE:
                        b_min = (
                            r_start
                            if batch_ids[0] == batch[0][1] and res.messages_saved == len(batch)
                            else min(batch_ids)
                        )
                        b_max = max(batch_ids)
                        res.edits_detected += await _flush_message_batch(
                            batch_items=batch,
                            chat_dir=chat_dir,
                            chat_key=chat_key,
                            chat_title=selection.title,
                            account_id=account_id,
                            db=db,
                            writer=writer,
                            global_range=(b_min, b_max),
                            cursor_offset_id=b_max,
                            cursor_completed=False,
                            folder_rel_path=folder_rel,
                        )
                        touched_messages = True
                        r_start = b_max + 1
                        batch.clear()
                        batch_ids.clear()

                if batch:
                    res.edits_detected += await _flush_message_batch(
                        batch_items=batch,
                        chat_dir=chat_dir,
                        chat_key=chat_key,
                        chat_title=selection.title,
                        account_id=account_id,
                        db=db,
                        writer=writer,
                        global_range=(r_start, r_end),
                        cursor_offset_id=None,
                        cursor_completed=True,
                        folder_rel_path=folder_rel,
                    )
                    touched_messages = True
                elif r_start <= r_end:
                    # Range had only deleted/empty gaps on server -> mark range covered
                    db.add_synced_range(chat_key, r_start, r_end)
                    db.upsert_chat_cursor(
                        chat_key=chat_key,
                        account_id=account_id,
                        folder_rel_path=folder_rel,
                        backfill_offset_id=None,
                        backfill_completed=True,
                    )
    else:
        # Private DMs, Saved Messages, and Legacy Basic Groups (per-account message IDs)
        cursor = db.get_chat_cursor(chat_key, account_id)
        backfill_done = bool(cursor and cursor.get("backfill_completed"))
        resume_offset = (
            int(cursor["backfill_offset_id"])
            if (cursor and cursor.get("backfill_offset_id"))
            else None
        )

        row_max = db._conn.execute(
            "SELECT MAX(msg_id) AS max_id FROM account_message_map "
            "WHERE account_id = ? AND chat_key = ?",
            (account_id, chat_key),
        ).fetchone()
        max_known_id = int(row_max["max_id"]) if (row_max and row_max["max_id"] is not None) else 0

        if not backfill_done:
            # Initial historical backfill (resumable from `resume_offset`)
            iter_kwargs = {}
            if resume_offset is not None:
                iter_kwargs["offset_id"] = resume_offset
            batch = []
            last_seen_id: int | None = resume_offset

            async for msg in client.iter_messages(entity, **iter_kwargs):
                msg_id = int(getattr(msg, "id", 0) or 0)
                if msg_id <= 0:
                    continue
                last_seen_id = msg_id
                msg_dt = getattr(msg, "date", None)
                if since_date is not None and isinstance(msg_dt, datetime) and msg_dt < since_date:
                    break

                item_tuple = await _process_single_message(
                    client=client,
                    msg=msg,
                    chat_key=chat_key,
                    account_id=account_id,
                    selection=selection,
                    chat_dir=chat_dir,
                    db=db,
                    media_manager=media_manager,
                    limiter=limiter,
                    res=res,
                )
                batch.append(item_tuple)
                if len(batch) >= BATCH_SIZE:
                    res.edits_detected += await _flush_message_batch(
                        batch_items=batch,
                        chat_dir=chat_dir,
                        chat_key=chat_key,
                        chat_title=selection.title,
                        account_id=account_id,
                        db=db,
                        writer=writer,
                        global_range=None,
                        cursor_offset_id=last_seen_id,
                        cursor_completed=False,
                        folder_rel_path=folder_rel,
                    )
                    touched_messages = True
                    batch.clear()

            if batch:
                res.edits_detected += await _flush_message_batch(
                    batch_items=batch,
                    chat_dir=chat_dir,
                    chat_key=chat_key,
                    chat_title=selection.title,
                    account_id=account_id,
                    db=db,
                    writer=writer,
                    global_range=None,
                    cursor_offset_id=None,
                    cursor_completed=True,
                    folder_rel_path=folder_rel,
                )
                touched_messages = True
            else:
                db.upsert_chat_cursor(
                    chat_key=chat_key,
                    account_id=account_id,
                    folder_rel_path=folder_rel,
                    backfill_offset_id=None,
                    backfill_completed=True,
                )

        # Also fetch any incremental new messages > max_known_id when backfill was already completed
        if backfill_done and max_known_id > 0:
            batch = []
            async for msg in client.iter_messages(entity, min_id=max_known_id, reverse=True):
                msg_id = int(getattr(msg, "id", 0) or 0)
                if msg_id <= max_known_id:
                    continue
                item_tuple = await _process_single_message(
                    client=client,
                    msg=msg,
                    chat_key=chat_key,
                    account_id=account_id,
                    selection=selection,
                    chat_dir=chat_dir,
                    db=db,
                    media_manager=media_manager,
                    limiter=limiter,
                    res=res,
                )
                batch.append(item_tuple)
                if len(batch) >= BATCH_SIZE:
                    res.edits_detected += await _flush_message_batch(
                        batch_items=batch,
                        chat_dir=chat_dir,
                        chat_key=chat_key,
                        chat_title=selection.title,
                        account_id=account_id,
                        db=db,
                        writer=writer,
                        global_range=None,
                        cursor_offset_id=None,
                        cursor_completed=True,
                        folder_rel_path=folder_rel,
                    )
                    touched_messages = True
                    batch.clear()
            if batch:
                res.edits_detected += await _flush_message_batch(
                    batch_items=batch,
                    chat_dir=chat_dir,
                    chat_key=chat_key,
                    chat_title=selection.title,
                    account_id=account_id,
                    db=db,
                    writer=writer,
                    global_range=None,
                    cursor_offset_id=None,
                    cursor_completed=True,
                    folder_rel_path=folder_rel,
                )
                touched_messages = True

    # Save channel PTS if entity carries one
    entity_pts = getattr(entity, "pts", None)
    if entity_pts is not None and uses_global_msg_ids(chat_key):
        db.set_pts_state(
            account_id,
            int(entity_pts),
            scope_key=f"channel:{int(getattr(entity, 'id', 0))}",
        )

    # Drain retroactive pending_media if user recently checked [x] photos/videos/audio/files!
    retro_dl = await drain_pending_media(
        client=client,
        entity=entity,
        account_id=account_id,
        selection=selection,
        chat_dir=chat_dir,
        db=db,
        writer=writer,
        media_manager=media_manager,
        limiter=limiter,
    )
    res.retroactive_media_downloaded = retro_dl

    if touched_messages or retro_dl > 0 or not (chat_dir / "links.md").exists():
        links_total, _, _ = rebuild_links_and_audit_md_from_parts(
            chat_dir,
            chat_key=chat_key,
            chat_title=selection.title,
        )
        res.links_total = links_total

    return res


async def _process_single_message(
    *,
    client: Any,
    msg: Any,
    chat_key: str,
    account_id: int,
    selection: ChatSelection,
    chat_dir: Path,
    db: CheckpointDB,
    media_manager: MediaManager,
    limiter: RateLimiter,
    res: ChatSyncResult,
) -> tuple[str, int, str, int | None, dict[str, Any]]:
    msg_id = int(getattr(msg, "id", 0))
    date_iso = to_utc_iso(getattr(msg, "date", None)) or "1970-01-01T00:00:00+00:00"
    day_str = _day_str_from_iso(date_iso)
    text = str(getattr(msg, "message", None) or getattr(msg, "raw_text", None) or "")

    sender_id_raw = getattr(msg, "sender_id", None)
    if sender_id_raw is None and getattr(msg, "sender", None) is not None:
        sender_id_raw = getattr(msg.sender, "id", None)
    sender_id = int(sender_id_raw) if sender_id_raw is not None else None

    media_obj = getattr(msg, "media", None)
    classified = classify_telethon_media(media_obj)
    media_tg_id = classified.tg_id if classified is not None else None

    # Check if `(account_id, chat_key, msg_id)` was already mapped earlier (e.g., before an edit)
    existing_locs = db.locate_by_account_msg(account_id, msg_id, chat_key=chat_key)
    if existing_locs:
        canonical_key = existing_locs[0].canonical_key
        day_str = existing_locs[0].day_str
    else:
        canonical_key = make_canonical_message_key(
            chat_key=chat_key,
            msg_id=msg_id,
            sender_id=sender_id,
            date_iso=date_iso,
            media_tg_id=media_tg_id,
            grouped_id=getattr(msg, "grouped_id", None),
            text=text,
        )
        # Check if another account already synced this canonical_key (e.g., basic group)
        already_loc = db.locate_by_canonical(chat_key, canonical_key)
        if already_loc is not None:
            res.messages_dedup_skipped += 1
        else:
            res.messages_saved += 1

    media_meta: dict[str, Any] | None = None
    if media_obj is not None and classified is not None:
        outcome = await media_manager.process_message_media(
            media=media_obj,
            selection=selection,
            chat_dir=chat_dir,
            chat_key=chat_key,
            canonical_key=canonical_key,
            account_id=account_id,
            msg_id=msg_id,
            day_str=day_str,
            downloader=lambda target, m=msg: limiter.call(
                lambda: client.download_media(m, file=str(target)),
                description=f"download_media({chat_key}#{msg_id})",
            ),
        )
        if outcome is not None:
            media_meta = outcome.metadata
            if outcome.network_downloaded:
                res.media_downloaded += 1
            elif outcome.dedup_reused:
                res.media_dedup_reused += 1
            elif outcome.queued_pending:
                res.media_deferred += 1

    msg_dict = serialize_message_to_dict(
        msg,
        account_id=account_id,
        canonical_key=canonical_key,
        media_meta=media_meta,
    )
    return (day_str, msg_id, canonical_key, sender_id, msg_dict)


async def drain_pending_media(
    *,
    client: Any,
    entity: Any,
    account_id: int,
    selection: ChatSelection,
    chat_dir: Path,
    db: CheckpointDB,
    writer: ChatDayWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
) -> int:
    """Download media for categories newly checked in `chats.md` on previously synced messages."""
    enabled_cats = selection.enabled_categories()
    if not enabled_cats:
        return 0

    pending_rows = db.list_pending_media(selection.chat_key, enabled_cats)
    if not pending_rows:
        return 0

    downloaded_count = 0
    need_rpc_rows = []

    # Step 1: Resolve any items already in global `media_index` without touching Telegram!
    for row in pending_rows:
        indexed = db.get_indexed_media(row.tg_type, row.tg_id)
        if indexed is not None and (media_manager.data_dir / indexed.rel_path).exists():
            classified = ClassifiedMedia(
                category=row.category,
                tg_type=row.tg_type,
                tg_id=row.tg_id,
                file_name=row.file_name,
                mime_type=row.mime_type,
                size_bytes=row.size_bytes,
                default_ext="." + indexed.rel_path.rsplit(".", 1)[-1]
                if "." in indexed.rel_path
                else ".bin",
            )
            outcome = await media_manager._acquire_media_file(
                classified=classified,
                chat_dir=chat_dir,
                prefix_tag=f"{row.day_str}_{row.msg_id}",
                base_meta={"category": row.category, "tg_type": row.tg_type, "tg_id": row.tg_id},
                downloader=lambda _: None,  # type: ignore[return-value,arg-type]
            )
            if outcome.metadata.get("downloaded"):
                loc = db.locate_by_canonical(row.chat_key, row.canonical_key)
                part_num = loc.part_num if loc is not None else 0
                writer.attach_downloaded_media(
                    chat_dir=chat_dir,
                    day_str=row.day_str,
                    part_num=part_num,
                    canonical_key=row.canonical_key,
                    rel_media_path=str(outcome.metadata["path"]),
                    sha256=str(outcome.metadata["sha256"]),
                    size_bytes=int(outcome.metadata["size_bytes"]),
                )
                db.mark_pending_media_downloaded(row.chat_key, row.canonical_key)
                downloaded_count += 1
        elif row.account_id == account_id or uses_global_msg_ids(selection.chat_key):
            need_rpc_rows.append(row)

    # Step 2: Fetch remote messages in chunks of 50 to obtain fresh `file_reference` & download
    for idx in range(0, len(need_rpc_rows), 50):
        chunk = need_rpc_rows[idx : idx + 50]
        ids = [r.msg_id for r in chunk]
        fetched = await limiter.call(
            lambda msg_ids=ids: client.get_messages(entity, ids=msg_ids),
            description=f"fetch_pending_media({selection.chat_key})",
        )
        msg_by_id: dict[int, Any] = {}
        for item in fetched or []:
            if item is not None and getattr(item, "id", None) is not None:
                msg_by_id[int(item.id)] = item

        for row in chunk:
            remote_msg = msg_by_id.get(row.msg_id)
            if remote_msg is None or getattr(remote_msg, "media", None) is None:
                continue
            outcome = await media_manager.process_message_media(
                media=remote_msg.media,
                selection=selection,
                chat_dir=chat_dir,
                chat_key=row.chat_key,
                canonical_key=row.canonical_key,
                account_id=account_id,
                msg_id=row.msg_id,
                day_str=row.day_str,
                downloader=lambda target, m=remote_msg, r=row: limiter.call(
                    lambda: client.download_media(m, file=str(target)),
                    description=f"download_pending({r.chat_key}#{r.msg_id})",
                ),
            )
            if outcome is not None and outcome.metadata.get("downloaded"):
                loc = db.locate_by_canonical(row.chat_key, row.canonical_key)
                part_num = loc.part_num if loc is not None else 0
                writer.attach_downloaded_media(
                    chat_dir=chat_dir,
                    day_str=row.day_str,
                    part_num=part_num,
                    canonical_key=row.canonical_key,
                    rel_media_path=str(outcome.metadata["path"]),
                    sha256=str(outcome.metadata["sha256"]),
                    size_bytes=int(outcome.metadata["size_bytes"]),
                )
                db.mark_pending_media_downloaded(row.chat_key, row.canonical_key)
                downloaded_count += 1

    return downloaded_count
