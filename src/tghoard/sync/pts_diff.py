"""Offline MTProto PTS delta replay (`getDifference` / `getChannelDifference`) and window audit."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from telethon.tl import functions as tl_functions
from telethon.tl import types as tl_types

from tghoard.storage.chat_writer import ChatDayWriter
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.markdown_writer import rebuild_links_and_audit_md_from_parts
from tghoard.storage.paths import resolve_chat_dir
from tghoard.telegram.entities import serialize_message_to_dict, to_utc_iso
from tghoard.telegram.throttler import RateLimiter


@dataclass
class PtsDiffStats:
    deleted_marked: int = 0
    edits_recorded: int = 0
    touched_chats: set[str] = field(default_factory=set)
    too_long_channels: set[int] = field(default_factory=set)
    global_too_long: bool = False


def _apply_deleted_msg_ids(
    *,
    account_id: int,
    msg_ids: list[int],
    chat_key_hint: str | None,
    selections: dict[str, ChatSelection],
    data_dir: Path,
    db: CheckpointDB,
    writer: ChatDayWriter,
    source: str,
    stats: PtsDiffStats,
) -> None:
    for msg_id in msg_ids:
        located_list = db.locate_by_account_msg(account_id, int(msg_id), chat_key=chat_key_hint)
        for loc in located_list:
            sel = selections.get(loc.chat_key)
            if sel is not None and not sel.baixar:
                continue
            title = sel.title if sel is not None else loc.chat_key
            chat_dir = resolve_chat_dir(data_dir, loc.chat_key, title)
            updated = writer.mark_message_deleted(
                chat_dir=chat_dir,
                day_str=loc.day_str,
                part_num=loc.part_num,
                canonical_key=loc.canonical_key,
                source=source,
            )
            if updated is not None:
                db.mark_locator_deleted(loc.chat_key, loc.canonical_key)
                stats.deleted_marked += 1
                stats.touched_chats.add(loc.chat_key)


def _apply_edited_message(
    *,
    account_id: int,
    msg: Any,
    chat_key_hint: str | None,
    selections: dict[str, ChatSelection],
    data_dir: Path,
    db: CheckpointDB,
    writer: ChatDayWriter,
    stats: PtsDiffStats,
) -> None:
    msg_id = getattr(msg, "id", None)
    if msg_id is None:
        return
    located_list = db.locate_by_account_msg(account_id, int(msg_id), chat_key=chat_key_hint)
    for loc in located_list:
        sel = selections.get(loc.chat_key)
        if sel is not None and not sel.baixar:
            continue
        title = sel.title if sel is not None else loc.chat_key
        chat_dir = resolve_chat_dir(data_dir, loc.chat_key, title)
        incoming_dict = serialize_message_to_dict(
            msg,
            account_id=account_id,
            canonical_key=loc.canonical_key,
        )
        part_map, edits = writer.upsert_day_messages(
            chat_dir=chat_dir,
            chat_key=loc.chat_key,
            chat_title=title,
            day_str=loc.day_str,
            incoming_messages=[incoming_dict],
        )
        db.update_day_part_numbers(loc.chat_key, loc.day_str, part_map)
        if edits:
            stats.edits_recorded += len(edits)
            stats.touched_chats.add(loc.chat_key)


async def sync_account_pts_diff(
    *,
    client: Any,
    account_id: int,
    selections: dict[str, ChatSelection],
    channel_entities: dict[str, Any],
    data_dir: Path,
    db: CheckpointDB,
    writer: ChatDayWriter,
    limiter: RateLimiter,
) -> PtsDiffStats:
    """Replay offline `getDifference` & `getChannelDifference` to catch deletes and edits."""
    stats = PtsDiffStats()
    global_state = db.get_pts_state(account_id, "global")

    # Initialize baseline PTS if first time
    if global_state is None:
        if callable(client):
            try:
                st = await limiter.call(
                    lambda: client(tl_functions.updates.GetStateRequest()),
                    description="updates.GetState",
                )
                date_ts = int(st.date.timestamp()) if getattr(st, "date", None) else 0
                db.set_pts_state(
                    account_id,
                    int(st.pts),
                    scope_key="global",
                    qts=int(getattr(st, "qts", 0)),
                    seq=int(getattr(st, "seq", 0)),
                    date_ts=date_ts,
                )
            except Exception:
                pass
        return stats

    # 1. Global mailbox difference (private chats, Saved Messages, legacy basic groups)
    if callable(client) and global_state.pts > 0:
        pts = global_state.pts
        qts = global_state.qts
        date_ts = max(1, global_state.date_ts)
        for _ in range(25):
            diff = await limiter.call(
                lambda p=pts, d=date_ts, q=qts: client(
                    tl_functions.updates.GetDifferenceRequest(pts=p, date=d, qts=q)
                ),
                description="updates.GetDifference",
            )
            cls_name = type(diff).__name__
            if isinstance(diff, tl_types.updates.DifferenceEmpty) or cls_name == "DifferenceEmpty":
                seq = int(getattr(diff, "seq", global_state.seq))
                d_val = getattr(diff, "date", None)
                new_dts = int(d_val.timestamp()) if hasattr(d_val, "timestamp") else date_ts
                db.set_pts_state(
                    account_id, pts, scope_key="global", qts=qts, seq=seq, date_ts=new_dts
                )
                break

            if (
                isinstance(diff, tl_types.updates.DifferenceTooLong)
                or cls_name == "DifferenceTooLong"
            ):
                stats.global_too_long = True
                new_pts = int(getattr(diff, "pts", pts))
                db.set_pts_state(account_id, new_pts, scope_key="global", qts=qts, date_ts=date_ts)
                break

            for upd in getattr(diff, "other_updates", None) or []:
                upd_cls = type(upd).__name__
                if (
                    isinstance(upd, tl_types.UpdateDeleteMessages)
                    or upd_cls == "UpdateDeleteMessages"
                ):
                    _apply_deleted_msg_ids(
                        account_id=account_id,
                        msg_ids=list(getattr(upd, "messages", [])),
                        chat_key_hint=None,
                        selections=selections,
                        data_dir=data_dir,
                        db=db,
                        writer=writer,
                        source="pts_diff",
                        stats=stats,
                    )
                elif isinstance(upd, tl_types.UpdateEditMessage) or upd_cls == "UpdateEditMessage":
                    _apply_edited_message(
                        account_id=account_id,
                        msg=getattr(upd, "message", None),
                        chat_key_hint=None,
                        selections=selections,
                        data_dir=data_dir,
                        db=db,
                        writer=writer,
                        stats=stats,
                    )
                elif (
                    isinstance(upd, tl_types.UpdateChannelTooLong)
                    or upd_cls == "UpdateChannelTooLong"
                ):
                    ch_id = getattr(upd, "channel_id", None)
                    if ch_id is not None:
                        stats.too_long_channels.add(int(ch_id))

            state_obj = getattr(diff, "state", None) or getattr(diff, "intermediate_state", None)
            if state_obj is not None:
                pts = int(getattr(state_obj, "pts", pts))
                qts = int(getattr(state_obj, "qts", qts))
                seq = int(getattr(state_obj, "seq", 0))
                d_val = getattr(state_obj, "date", None)
                date_ts = int(d_val.timestamp()) if hasattr(d_val, "timestamp") else date_ts
                db.set_pts_state(
                    account_id, pts, scope_key="global", qts=qts, seq=seq, date_ts=date_ts
                )

            if isinstance(diff, tl_types.updates.Difference) or cls_name == "Difference":
                break

    # 2. Per-channel / supergroup PTS difference
    if callable(client):
        for chat_key, entity in channel_entities.items():
            sel = selections.get(chat_key)
            if sel is not None and not sel.baixar:
                continue
            raw_ch_id = int(getattr(entity, "id", 0))
            scope_key = f"channel:{raw_ch_id}"
            ch_state = db.get_pts_state(account_id, scope_key)
            if ch_state is None or ch_state.pts <= 0:
                continue

            ch_pts = ch_state.pts
            for _ in range(25):
                try:
                    ch_diff = await limiter.call(
                        lambda ent=entity, p=ch_pts: client(
                            tl_functions.updates.GetChannelDifferenceRequest(
                                channel=ent,
                                filter=tl_types.ChannelMessagesFilterEmpty(),
                                pts=p,
                                limit=100,
                            )
                        ),
                        description=f"GetChannelDifference({chat_key})",
                    )
                except Exception:
                    break

                ch_cls = type(ch_diff).__name__
                if (
                    isinstance(ch_diff, tl_types.updates.ChannelDifferenceEmpty)
                    or ch_cls == "ChannelDifferenceEmpty"
                ):
                    new_pts = int(getattr(ch_diff, "pts", ch_pts))
                    db.set_pts_state(account_id, new_pts, scope_key=scope_key)
                    break

                if (
                    isinstance(ch_diff, tl_types.updates.ChannelDifferenceTooLong)
                    or ch_cls == "ChannelDifferenceTooLong"
                ):
                    stats.too_long_channels.add(raw_ch_id)
                    dialog_obj = getattr(ch_diff, "dialog", None)
                    new_pts = int(getattr(dialog_obj, "pts", ch_pts))
                    db.set_pts_state(account_id, new_pts, scope_key=scope_key)
                    break

                for upd in getattr(ch_diff, "other_updates", None) or []:
                    upd_cls = type(upd).__name__
                    if (
                        isinstance(upd, tl_types.UpdateDeleteChannelMessages)
                        or upd_cls == "UpdateDeleteChannelMessages"
                    ):
                        _apply_deleted_msg_ids(
                            account_id=account_id,
                            msg_ids=list(getattr(upd, "messages", [])),
                            chat_key_hint=chat_key,
                            selections=selections,
                            data_dir=data_dir,
                            db=db,
                            writer=writer,
                            source="pts_diff",
                            stats=stats,
                        )
                    elif (
                        isinstance(upd, tl_types.UpdateEditChannelMessage)
                        or upd_cls == "UpdateEditChannelMessage"
                    ):
                        _apply_edited_message(
                            account_id=account_id,
                            msg=getattr(upd, "message", None),
                            chat_key_hint=chat_key,
                            selections=selections,
                            data_dir=data_dir,
                            db=db,
                            writer=writer,
                            stats=stats,
                        )

                ch_pts = int(getattr(ch_diff, "pts", ch_pts))
                db.set_pts_state(account_id, ch_pts, scope_key=scope_key)
                if getattr(ch_diff, "final", True):
                    break

    # Refresh Markdown audit files on touched chats
    for chat_key in stats.touched_chats:
        sel = selections.get(chat_key)
        title = sel.title if sel is not None else chat_key
        chat_dir = resolve_chat_dir(data_dir, chat_key, title)
        rebuild_links_and_audit_md_from_parts(chat_dir, chat_key=chat_key, chat_title=title)

    return stats


async def reconcile_recent_window(
    *,
    client: Any,
    entity: Any,
    account_id: int,
    chat_key: str,
    chat_title: str,
    data_dir: Path,
    db: CheckpointDB,
    writer: ChatDayWriter,
    limiter: RateLimiter,
    window_size: int = 200,
) -> tuple[int, int]:
    """Audit the most recent `window_size` synced messages when `DifferenceTooLong` occurs."""
    rows = db._conn.execute(
        """
        SELECT m.msg_id, l.canonical_key, l.day_str, l.part_num, l.is_deleted
        FROM account_message_map m
        JOIN message_locator l
          ON l.chat_key = m.chat_key AND l.canonical_key = m.canonical_key
        WHERE m.account_id = ? AND m.chat_key = ?
        ORDER BY m.msg_id DESC
        LIMIT ?
        """,
        (account_id, chat_key, window_size),
    ).fetchall()
    if not rows or not hasattr(client, "get_messages"):
        return 0, 0

    ids_to_check = [int(r["msg_id"]) for r in rows]
    fetched = await limiter.call(
        lambda: client.get_messages(entity, ids=ids_to_check),
        description=f"reconcile_window({chat_key})",
    )
    by_id: dict[int, Any] = {}
    for item in fetched or []:
        if item is not None and not isinstance(item, tl_types.MessageEmpty):
            mid = getattr(item, "id", None)
            if mid is not None:
                by_id[int(mid)] = item

    chat_dir = resolve_chat_dir(data_dir, chat_key, chat_title)
    deleted_cnt = 0
    edited_cnt = 0

    for r in rows:
        mid = int(r["msg_id"])
        ckey = str(r["canonical_key"])
        day_str = str(r["day_str"])
        part_num = int(r["part_num"])
        was_deleted = bool(r["is_deleted"])

        remote_msg = by_id.get(mid)
        if remote_msg is None:
            if not was_deleted:
                updated = writer.mark_message_deleted(
                    chat_dir=chat_dir,
                    day_str=day_str,
                    part_num=part_num,
                    canonical_key=ckey,
                    source="window_audit",
                )
                if updated is not None:
                    db.mark_locator_deleted(chat_key, ckey)
                    deleted_cnt += 1
        elif to_utc_iso(getattr(remote_msg, "edit_date", None)) is not None:
            inc = serialize_message_to_dict(
                remote_msg,
                account_id=account_id,
                canonical_key=ckey,
            )
            part_map, edits = writer.upsert_day_messages(
                chat_dir=chat_dir,
                chat_key=chat_key,
                chat_title=chat_title,
                day_str=day_str,
                incoming_messages=[inc],
            )
            db.update_day_part_numbers(chat_key, day_str, part_map)
            edited_cnt += len(edits)

    if deleted_cnt or edited_cnt:
        rebuild_links_and_audit_md_from_parts(chat_dir, chat_key=chat_key, chat_title=chat_title)
    return deleted_cnt, edited_cnt
