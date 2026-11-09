"""Rich metadata sync (`metadata/*.md`: info, members, topics, stories, profile, presence)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from telethon import errors as tg_errors
from telethon.tl import functions as tl_functions
from telethon.tl import types as tl_types

from tghoard.storage.atomic import utc_now_iso
from tghoard.storage.chat_writer import ChatDayWriter, make_canonical_message_key
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.markdown_writer import (
    append_markdown_log_entries,
    ensure_initial_metadata_files,
    rebuild_links_and_audit_md_from_parts,
    write_info_md,
    write_members_md,
    write_topics_md,
)
from tghoard.storage.media_manager import MediaManager
from tghoard.storage.paths import iter_all_part_files, resolve_chat_dir
from tghoard.telegram.entities import (
    classify_entity_to_chat_info,
    display_name_of_entity,
    serialize_message_to_dict,
    to_utc_iso,
)
from tghoard.telegram.throttler import RateLimiter


def _format_user_status(status: Any) -> str | None:
    if status is None:
        return None
    cls_name = type(status).__name__
    if isinstance(status, tl_types.UserStatusOnline) or cls_name == "UserStatusOnline":
        exp = to_utc_iso(getattr(status, "expires", None))
        return f"Online (expira em `{exp}`)" if exp else "Online"
    if isinstance(status, tl_types.UserStatusOffline) or cls_name == "UserStatusOffline":
        was = to_utc_iso(getattr(status, "was_online", None))
        return f"Offline (visto online em `{was}`)" if was else "Offline"
    if isinstance(status, tl_types.UserStatusRecently) or cls_name == "UserStatusRecently":
        return "Visto recentemente"
    if isinstance(status, tl_types.UserStatusLastWeek) or cls_name == "UserStatusLastWeek":
        return "Visto na última semana"
    if isinstance(status, tl_types.UserStatusLastMonth) or cls_name == "UserStatusLastMonth":
        return "Visto no último mês"
    return cls_name


def _compute_local_folder_stats(chat_dir: Path) -> dict[str, int]:
    def _count_files(sub: str) -> int:
        p = chat_dir / sub
        if not p.exists():
            return 0
        return sum(1 for f in p.iterdir() if f.is_file() and not f.name.startswith("."))

    links_count = 0
    links_path = chat_dir / "links.md"
    if links_path.exists():
        for line in links_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("- `") and " — <" in line:
                links_count += 1

    return {
        "parts": len(iter_all_part_files(chat_dir)),
        "photos": _count_files("photos"),
        "videos": _count_files("videos"),
        "audio": _count_files("audio"),
        "files": _count_files("files"),
        "links": links_count,
    }


async def sync_chat_metadata(
    *,
    client: Any,
    entity: Any,
    account_id: int,
    account_alias: str,
    selection: ChatSelection,
    data_dir: Path,
    db: CheckpointDB,
    writer: ChatDayWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
    sync_members: bool = True,
    sync_stories: bool = True,
    sync_profile_photos: bool = True,
) -> None:
    """Sync all plain-text Markdown files inside `<chat_dir>/metadata/`."""
    if not selection.baixar:
        return

    info = classify_entity_to_chat_info(entity, self_id=account_id, account_alias=account_alias)
    chat_key = selection.chat_key
    chat_dir = resolve_chat_dir(data_dir, chat_key, selection.title)
    ensure_initial_metadata_files(chat_dir, chat_key=chat_key, chat_title=selection.title)

    # 1. Fetch full entity bio / about description if supported
    bio_or_about: str | None = getattr(entity, "about", None)
    if bio_or_about is None and callable(client):
        try:
            if info.kind in ("dm", "bot", "saved"):
                full_u = await limiter.call(
                    lambda: client(tl_functions.users.GetFullUserRequest(entity)),
                    description=f"GetFullUser({chat_key})",
                )
                bio_or_about = getattr(getattr(full_u, "full_user", None), "about", None)
            elif info.kind in ("supergroup", "channel"):
                full_c = await limiter.call(
                    lambda: client(tl_functions.channels.GetFullChannelRequest(entity)),
                    description=f"GetFullChannel({chat_key})",
                )
                full_chat = getattr(full_c, "full_chat", None)
                bio_or_about = getattr(full_chat, "about", None)
                if info.participants_count is None and getattr(
                    full_chat, "participants_count", None
                ):
                    info.participants_count = int(full_chat.participants_count)
        except Exception:
            pass

    # 2. Detect profile changes -> `metadata/profile_history.md`
    phone = getattr(entity, "phone", None)
    new_profile_state = {
        "title": info.title,
        "username": info.username,
        "usernames": info.usernames,
        "bio": bio_or_about,
        "phone": str(phone) if phone else None,
    }
    diffs = db.update_profile_state_and_diff(chat_key, new_profile_state)
    if diffs:
        now = utc_now_iso()
        diff_lines = [
            f"- `{now}` — **{field_name}** alterado de `{old_val}` para `{new_val}`"
            for field_name, old_val, new_val in diffs
        ]
        append_markdown_log_entries(
            chat_dir / "metadata" / "profile_history.md",
            header_title=f"Histórico de Perfil — {selection.title} (`{chat_key}`)",
            entries=diff_lines,
        )

    # 3. Historical profile photos/videos (if `[x] photos` or `[x] videos` enabled)
    if (
        sync_profile_photos
        and (selection.photos or selection.videos)
        and hasattr(client, "iter_profile_photos")
    ):
        try:
            photo_entries: list[str] = []
            async for p_item in client.iter_profile_photos(entity, limit=25):
                p_id = getattr(p_item, "id", None)
                if p_id is None:
                    continue
                outcome = await media_manager.download_standalone_media(
                    media=p_item,
                    selection=selection,
                    chat_dir=chat_dir,
                    prefix_tag="profile",
                    downloader=lambda target, ph=p_item: limiter.call(
                        lambda: client.download_media(ph, file=str(target)),
                        description=f"download_profile_photo({chat_key})",
                    ),
                )
                if outcome is not None and outcome.metadata.get("downloaded"):
                    p_date = to_utc_iso(getattr(p_item, "date", None)) or "data-desconhecida"
                    photo_entries.append(
                        f"- Foto de perfil `{p_id}` ({p_date}): `../{outcome.metadata['path']}`"
                    )
            if photo_entries:
                append_markdown_log_entries(
                    chat_dir / "metadata" / "profile_history.md",
                    header_title=f"Histórico de Perfil — {selection.title} (`{chat_key}`)",
                    entries=photo_entries,
                )
        except Exception:
            pass

    # 4. Presence snapshot -> `metadata/presence.md`
    status_desc = _format_user_status(getattr(entity, "status", None))
    if status_desc:
        append_markdown_log_entries(
            chat_dir / "metadata" / "presence.md",
            header_title=f"Presença e Atividade — {selection.title} (`{chat_key}`)",
            entries=[f"- `{utc_now_iso()}` (via `{account_alias}`): {status_desc}"],
        )

    # 5. Group / Supergroup / Channel participants -> `metadata/members.md`
    if (
        sync_members
        and info.kind in ("group", "supergroup", "channel")
        and hasattr(client, "iter_participants")
    ):
        members_list: list[dict[str, Any]] = []
        note: str | None = None
        try:
            async for user in client.iter_participants(entity, limit=500):
                participant = getattr(user, "participant", None)
                p_cls = type(participant).__name__ if participant is not None else "member"
                if "Creator" in p_cls:
                    role = "creator"
                elif "Admin" in p_cls:
                    role = "admin"
                else:
                    role = "member"
                members_list.append(
                    {
                        "id": int(getattr(user, "id", 0)),
                        "name": display_name_of_entity(user),
                        "username": getattr(user, "username", None),
                        "role": role,
                        "is_bot": bool(getattr(user, "bot", False)),
                    }
                )
        except tg_errors.ChatAdminRequiredError:
            note = "Lista completa de participantes restrita aos administradores neste grupo/canal."
        except Exception as exc:
            note = f"Não foi possível enumerar participantes ({type(exc).__name__})."

        if members_list or note:
            write_members_md(
                chat_dir,
                chat_key=chat_key,
                chat_title=selection.title,
                members=members_list,
                note=note,
            )

    # 6. Forum topics -> `metadata/topics.md`
    if info.is_forum and callable(client):
        try:
            res_topics = await limiter.call(
                lambda: client(
                    tl_functions.channels.GetForumTopicsRequest(
                        channel=entity,
                        offset_date=None,
                        offset_id=0,
                        offset_topic=0,
                        limit=100,
                    )
                ),
                description=f"GetForumTopics({chat_key})",
            )
            topics_data = [
                {
                    "id": int(getattr(t, "id", 0)),
                    "title": getattr(t, "title", ""),
                    "closed": bool(getattr(t, "closed", False)),
                    "pinned": bool(getattr(t, "pinned", False)),
                }
                for t in getattr(res_topics, "topics", None) or []
                if hasattr(t, "title")
            ]
            write_topics_md(
                chat_dir,
                chat_key=chat_key,
                chat_title=selection.title,
                topics=topics_data,
            )
        except Exception:
            pass

    # 7. Active Stories before 24h expiry -> `metadata/stories.md`
    if sync_stories and callable(client) and info.kind in ("dm", "channel", "supergroup", "saved"):
        try:
            peer_stories_res = await limiter.call(
                lambda: client(tl_functions.stories.GetPeerStoriesRequest(peer=entity)),
                description=f"GetPeerStories({chat_key})",
            )
            stories_container = getattr(peer_stories_res, "stories", None)
            story_items = getattr(stories_container, "stories", None) or []
            story_lines: list[str] = []
            for st_item in story_items:
                st_id = getattr(st_item, "id", None)
                if st_id is None:
                    continue
                st_date = to_utc_iso(getattr(st_item, "date", None)) or ""
                st_exp = to_utc_iso(getattr(st_item, "expire_date", None)) or ""
                caption = str(getattr(st_item, "caption", "") or "").replace("\n", " ").strip()
                views_obj = getattr(st_item, "views", None)
                views_cnt = getattr(views_obj, "views_count", None)
                media_ref = "mídia não baixada"

                st_media = getattr(st_item, "media", None)
                if st_media is not None:
                    outcome = await media_manager.download_standalone_media(
                        media=st_media,
                        selection=selection,
                        chat_dir=chat_dir,
                        prefix_tag=f"story_{st_id}",
                        downloader=lambda target, sm=st_media, sid=st_id: limiter.call(
                            lambda: client.download_media(sm, file=str(target)),
                            description=f"download_story({chat_key}#{sid})",
                        ),
                    )
                    if outcome is not None and outcome.metadata.get("downloaded"):
                        media_ref = f"`../{outcome.metadata['path']}`"

                views_part = f" | views: `{views_cnt}`" if views_cnt is not None else ""
                cap_part = f" — _{caption}_" if caption else ""
                story_lines.append(
                    f"- **Story `#{st_id}`** (`{st_date}` → expira `{st_exp}`{views_part}): "
                    f"{media_ref}{cap_part}"
                )
            if story_lines:
                append_markdown_log_entries(
                    chat_dir / "metadata" / "stories.md",
                    header_title=f"Stories — {selection.title} (`{chat_key}`)",
                    entries=story_lines,
                )
        except Exception:
            pass

    # 8. 48h Admin Log (recovers deleted messages & edits where user is admin!)
    is_admin = bool(getattr(entity, "admin_rights", None) or getattr(entity, "creator", False))
    if is_admin and info.kind in ("supergroup", "channel") and hasattr(client, "iter_admin_log"):
        try:
            cursor = db.get_chat_cursor(chat_key, account_id)
            min_ev_id = (
                int(cursor["last_admin_event_id"])
                if (cursor and cursor.get("last_admin_event_id"))
                else 0
            )
            max_ev_id = min_ev_id
            ev_lines: list[str] = []
            recovered_deleted = False

            async for ev in client.iter_admin_log(entity, limit=100, min_id=min_ev_id):
                ev_id = int(getattr(ev, "id", 0))
                max_ev_id = max(max_ev_id, ev_id)
                ev_date = to_utc_iso(getattr(ev, "date", None)) or utc_now_iso()
                act = getattr(ev, "action", None)
                act_name = type(act).__name__ if act is not None else "AdminEvent"
                ev_lines.append(f"- `{ev_date}` | `event #{ev_id}` | `{act_name}`")

                # Recover deleted message from 48h server Admin Log even if sent & deleted offline!
                if act_name == "ChannelAdminLogEventActionDeleteMessage":
                    del_msg = getattr(act, "message", None)
                    if del_msg is not None and getattr(del_msg, "id", None) is not None:
                        d_mid = int(del_msg.id)
                        d_iso = to_utc_iso(getattr(del_msg, "date", None)) or ev_date
                        d_day = d_iso[:10]
                        ckey = make_canonical_message_key(
                            chat_key=chat_key,
                            msg_id=d_mid,
                            sender_id=getattr(del_msg, "sender_id", None),
                            date_iso=d_iso,
                        )
                        rec = serialize_message_to_dict(
                            del_msg,
                            account_id=account_id,
                            canonical_key=ckey,
                        )
                        rec["is_deleted"] = True
                        rec["deleted_detected_at"] = ev_date
                        rec["delete_source"] = "admin_log"
                        pmap, _ = writer.upsert_day_messages(
                            chat_dir=chat_dir,
                            chat_key=chat_key,
                            chat_title=selection.title,
                            day_str=d_day,
                            incoming_messages=[rec],
                        )
                        db.record_messages_batch(
                            chat_key=chat_key,
                            account_id=account_id,
                            locators=[(ckey, d_day, pmap.get(ckey, 0), None, True)],
                            account_mappings=[(d_mid, ckey)],
                        )
                        recovered_deleted = True

            if ev_lines:
                append_markdown_log_entries(
                    chat_dir / "metadata" / "events_and_admin_log.md",
                    header_title=f"Eventos e Admin Log — {selection.title} (`{chat_key}`)",
                    entries=ev_lines,
                )
            if max_ev_id > min_ev_id:
                folder_rel = chat_dir.resolve().relative_to(data_dir.resolve()).as_posix()
                db.upsert_chat_cursor(
                    chat_key=chat_key,
                    account_id=account_id,
                    folder_rel_path=folder_rel,
                    last_admin_event_id=max_ev_id,
                )
            if recovered_deleted:
                rebuild_links_and_audit_md_from_parts(
                    chat_dir,
                    chat_key=chat_key,
                    chat_title=selection.title,
                )
        except Exception:
            pass

    # 9. Write consolidated `metadata/info.md`
    participating = db.list_chat_accounts(chat_key)
    accounts_info = [
        {
            "account_id": acc.account_id,
            "alias": acc.alias,
            "username": acc.username,
            "updated_at": acc.last_sync_at or utc_now_iso(),
        }
        for acc in participating
    ]
    write_info_md(
        chat_dir,
        chat_key=chat_key,
        chat_title=selection.title,
        chat_type=info.kind,
        peer_id=info.peer_raw_id,
        username=info.username,
        usernames=info.usernames,
        bio_or_about=bio_or_about,
        phone=str(phone) if phone else None,
        participants_count=info.participants_count,
        accounts_info=accounts_info,
        synced_ranges=db.get_synced_ranges(chat_key),
        stats=_compute_local_folder_stats(chat_dir),
    )
