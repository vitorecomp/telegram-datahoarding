"""Rich metadata sync (`metadata/*.md`: info, members, topics, stories, profile, presence)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from rich.markup import escape
from telethon import errors as tg_errors
from telethon.tl import functions as tl_functions
from telethon.tl import types as tl_types

from tghoard.storage.atomic import utc_now_iso
from tghoard.storage.chat_writer import (
    ChatPartWriter,
    make_canonical_message_key,
    message_content_tag,
)
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
from tghoard.storage.media_manager import MediaManager, classify_telethon_media
from tghoard.storage.paths import (
    ChatPaths,
    iter_all_part_files,
    month_from_iso,
    resolve_chat_paths,
)
from tghoard.sync.reporter import NULL_REPORTER, SyncReporter
from tghoard.telegram.entities import (
    classify_entity_to_chat_info,
    display_name_of_entity,
    serialize_message_to_dict,
    to_utc_iso,
)
from tghoard.telegram.throttler import RateLimiter


def _format_user_status(status: Any) -> str | None:
    # Written to metadata/presence.md (archive content): kept in Portuguese.
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


def _md_media_link(rel_path: str) -> str:
    """Link from `metadata/<folder>/*.md` to a media file (`media.path` is data_dir-relative)."""
    return f"../../{rel_path}"


def _exc_text(exc: BaseException) -> str:
    """`Type: message` of an exception, escaped for Rich markup (may quote Telegram text)."""
    return escape(f"{type(exc).__name__}: {exc}")


def _compute_local_folder_stats(chat_paths: ChatPaths) -> dict[str, int]:
    def _count_files(sub: str) -> int:
        p = chat_paths.media_dir(sub)
        if not p.exists():
            return 0
        return sum(1 for f in p.iterdir() if f.is_file() and not f.name.startswith("."))

    links_count = 0
    links_path = chat_paths.links_path
    if links_path.exists():
        for line in links_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("- `") and " — <" in line:
                links_count += 1

    return {
        "parts": len(iter_all_part_files(chat_paths)),
        "photos": _count_files("photos"),
        "videos": _count_files("videos"),
        "audio": _count_files("audio"),
        "files": _count_files("files"),
        "links": links_count,
    }


def _recover_admin_log_deleted_message(
    *,
    del_msg: Any,
    ev_date: str,
    chat_key: str,
    account_id: int,
    chat_title: str,
    chat_paths: ChatPaths,
    db: CheckpointDB,
    writer: ChatPartWriter,
) -> bool:
    """Archive a message recovered from an Admin Log delete event, flagged `is_deleted`.

    It goes to its monthly `YYYY-MM-ptNNN.json`; if it was already archived, the existing
    record is merged (flagged deleted, an edit made before the deletion becomes a revision)
    instead of being duplicated.
    """
    if del_msg is None or getattr(del_msg, "id", None) is None:
        return False
    d_mid = int(del_msg.id)
    d_iso = to_utc_iso(getattr(del_msg, "date", None)) or ev_date
    d_month = month_from_iso(d_iso)
    sender_id_raw = getattr(del_msg, "sender_id", None)
    sender_id = int(sender_id_raw) if sender_id_raw is not None else None

    existing = db.locate_by_account_msg(account_id, d_mid, chat_key=chat_key)
    if existing:
        ckey = existing[0].canonical_key
    else:
        classified = classify_telethon_media(getattr(del_msg, "media", None))
        ckey = make_canonical_message_key(
            chat_key=chat_key,
            msg_id=d_mid,
            sender_id=sender_id,
            date_iso=d_iso,
            media_tg_id=classified.tg_id if classified is not None else None,
            grouped_id=getattr(del_msg, "grouped_id", None),
            text=str(getattr(del_msg, "message", None) or ""),
            content_tag=message_content_tag(del_msg),
        )
    rec = serialize_message_to_dict(del_msg, account_id=account_id, canonical_key=ckey)
    rec["is_deleted"] = True
    rec["deleted_detected_at"] = ev_date
    rec["delete_source"] = "admin_log"
    pmap, _ = writer.upsert_month_messages(
        chat_paths=chat_paths,
        chat_key=chat_key,
        chat_title=chat_title,
        month=d_month,
        incoming_messages=[rec],
    )
    db.update_day_part_numbers(chat_key, d_month, pmap)
    db.record_messages_batch(
        chat_key=chat_key,
        account_id=account_id,
        locators=[(ckey, d_month, pmap.get(ckey, 0), sender_id, True)],
        account_mappings=[(d_mid, ckey)],
    )
    return True


async def sync_chat_metadata(
    *,
    client: Any,
    entity: Any,
    account_id: int,
    account_alias: str,
    selection: ChatSelection,
    data_dir: Path,
    db: CheckpointDB,
    writer: ChatPartWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
    sync_members: bool = True,
    sync_stories: bool = True,
    sync_profile_photos: bool = True,
    reporter: SyncReporter = NULL_REPORTER,
) -> None:
    """Sync all plain-text Markdown files inside `<chat_dir>/metadata/`."""
    if not selection.baixar:
        return

    info = classify_entity_to_chat_info(entity, self_id=account_id, account_alias=account_alias)
    chat_key = selection.chat_key
    chat_paths = resolve_chat_paths(data_dir, chat_key, selection.title)
    ensure_initial_metadata_files(chat_paths, chat_key=chat_key, chat_title=selection.title)
    reporter.debug("🗂 syncing metadata (profile, members, topics, stories, admin log)")

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
            elif info.kind == "group":
                # Basic group: `Chat.date` is the creation date; *our* join date (and who
                # invited us) is in the member list returned by getFullChat.
                full_g = await limiter.call(
                    lambda: client(tl_functions.messages.GetFullChatRequest(chat_id=entity.id)),
                    description=f"GetFullChat({chat_key})",
                )
                full_chat = getattr(full_g, "full_chat", None)
                bio_or_about = getattr(full_chat, "about", None)
                members = getattr(getattr(full_chat, "participants", None), "participants", None)
                for part in members or []:
                    if int(getattr(part, "user_id", 0) or 0) != int(account_id):
                        continue
                    if type(part).__name__ == "ChatParticipantCreator":
                        joined, inviter, src = (
                            to_utc_iso(getattr(entity, "date", None)),
                            None,
                            "creator",
                        )
                    else:
                        joined = to_utc_iso(getattr(part, "date", None))
                        inviter, src = getattr(part, "inviter_id", None), "getFullChat"
                    db.set_memberships([(chat_key, account_id, joined, inviter, src)])
                    break
        except Exception as exc:
            reporter.debug(f"⚠ bio/description (GetFull*) unavailable: {_exc_text(exc)}")

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
            chat_paths.meta("profile_history.md"),
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
                    chat_paths=chat_paths,
                    prefix_tag="profile",
                    downloader=lambda target, ph=p_item: limiter.call(
                        lambda: client.download_media(ph, file=str(target)),
                        description=f"download_profile_photo({chat_key})",
                    ),
                )
                if outcome is not None and outcome.metadata.get("downloaded"):
                    p_date = to_utc_iso(getattr(p_item, "date", None)) or "data-desconhecida"
                    photo_entries.append(
                        f"- Foto de perfil `{p_id}` ({p_date}): "
                        f"`{_md_media_link(outcome.metadata['path'])}`"
                    )
            if photo_entries:
                append_markdown_log_entries(
                    chat_paths.meta("profile_history.md"),
                    header_title=f"Histórico de Perfil — {selection.title} (`{chat_key}`)",
                    entries=photo_entries,
                )
        except Exception as exc:
            reporter.debug(f"⚠ profile photos unavailable: {_exc_text(exc)}")

    # 4. Presence snapshot -> `metadata/presence.md`
    status_desc = _format_user_status(getattr(entity, "status", None))
    if status_desc:
        append_markdown_log_entries(
            chat_paths.meta("presence.md"),
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
        note: str | None = None  # written to members.md (archive content, Portuguese)
        log_note = ""
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
            log_note = "full member list restricted to admins"
        except Exception as exc:
            note = f"Não foi possível enumerar participantes ({type(exc).__name__})."
            log_note = f"could not list members ({type(exc).__name__})"

        reporter.debug(
            f"👥 {len(members_list)} member(s)" + (f" — {escape(log_note)}" if log_note else "")
        )
        if members_list or note:
            write_members_md(
                chat_paths,
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
                chat_paths,
                chat_key=chat_key,
                chat_title=selection.title,
                topics=topics_data,
            )
        except Exception as exc:
            reporter.debug(f"⚠ forum topics unavailable: {_exc_text(exc)}")

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
                        chat_paths=chat_paths,
                        prefix_tag=f"story_{st_id}",
                        downloader=lambda target, sm=st_media, sid=st_id: limiter.call(
                            lambda: client.download_media(sm, file=str(target)),
                            description=f"download_story({chat_key}#{sid})",
                        ),
                    )
                    if outcome is not None and outcome.metadata.get("downloaded"):
                        media_ref = f"`{_md_media_link(outcome.metadata['path'])}`"

                views_part = f" | views: `{views_cnt}`" if views_cnt is not None else ""
                cap_part = f" — _{caption}_" if caption else ""
                story_lines.append(
                    f"- **Story `#{st_id}`** (`{st_date}` → expira `{st_exp}`{views_part}): "
                    f"{media_ref}{cap_part}"
                )
            if story_lines:
                reporter.info(f"📖 {len(story_lines)} active story(ies) captured")
                append_markdown_log_entries(
                    chat_paths.meta("stories.md"),
                    header_title=f"Stories — {selection.title} (`{chat_key}`)",
                    entries=story_lines,
                )
        except Exception as exc:
            reporter.debug(f"⚠ stories unavailable: {_exc_text(exc)}")

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

            # No limit: the server keeps only 48h of events, and the cursor jumps to the
            # newest one below — a cap would skip the older events for good.
            async for ev in client.iter_admin_log(entity, limit=None, min_id=min_ev_id):
                ev_id = int(getattr(ev, "id", 0))
                max_ev_id = max(max_ev_id, ev_id)
                ev_date = to_utc_iso(getattr(ev, "date", None)) or utc_now_iso()
                act = getattr(ev, "action", None)
                act_name = type(act).__name__ if act is not None else "AdminEvent"
                ev_lines.append(f"- `{ev_date}` | `event #{ev_id}` | `{act_name}`")

                # Recover deleted message from 48h server Admin Log even if sent & deleted offline!
                if act_name == "ChannelAdminLogEventActionDeleteMessage":
                    if _recover_admin_log_deleted_message(
                        del_msg=getattr(act, "message", None),
                        ev_date=ev_date,
                        chat_key=chat_key,
                        account_id=account_id,
                        chat_title=selection.title,
                        chat_paths=chat_paths,
                        db=db,
                        writer=writer,
                    ):
                        recovered_deleted = True

            if ev_lines:
                append_markdown_log_entries(
                    chat_paths.meta("events_and_admin_log.md"),
                    header_title=f"Eventos e Admin Log — {selection.title} (`{chat_key}`)",
                    entries=ev_lines,
                )
            if max_ev_id > min_ev_id:
                folder_rel = chat_paths.folder
                db.upsert_chat_cursor(
                    chat_key=chat_key,
                    account_id=account_id,
                    folder_rel_path=folder_rel,
                    last_admin_event_id=max_ev_id,
                )
            if recovered_deleted:
                rebuild_links_and_audit_md_from_parts(
                    chat_paths,
                    chat_key=chat_key,
                    chat_title=selection.title,
                )
        except Exception as exc:
            reporter.debug(f"⚠ admin log unavailable: {_exc_text(exc)}")

    # 9. Write consolidated `metadata/info.md`
    participating = db.list_chat_accounts(chat_key)
    memberships = db.get_memberships(chat_key)
    accounts_info = [
        {
            "account_id": acc.account_id,
            "alias": acc.alias,
            "username": acc.username,
            "updated_at": acc.last_sync_at or utc_now_iso(),
            "joined_at": memberships.get(acc.account_id, {}).get("joined_at"),
            "inviter_id": memberships.get(acc.account_id, {}).get("inviter_id"),
        }
        for acc in participating
    ]
    write_info_md(
        chat_paths,
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
        stats=_compute_local_folder_stats(chat_paths),
    )
