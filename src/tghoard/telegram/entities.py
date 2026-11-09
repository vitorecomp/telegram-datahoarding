"""Helpers to normalize Telethon entities and messages into clean plain-text dicts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from telethon.tl import types as tl_types

from tghoard.storage.chats_manifest import ManifestSection
from tghoard.storage.markdown_writer import extract_links_from_text_and_entities
from tghoard.storage.paths import ChatKind, make_chat_key


@dataclass
class EntityChatInfo:
    chat_key: str
    kind: ChatKind
    section: ManifestSection
    title: str
    username: str | None
    usernames: list[str]
    peer_raw_id: int
    is_forum: bool
    is_bot: bool
    participants_count: int | None


def to_utc_iso(dt: datetime | str | None) -> str | None:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat(timespec="seconds")


def extract_extra_usernames(entity: Any) -> list[str]:
    extra: list[str] = []
    for u_obj in getattr(entity, "usernames", None) or []:
        uname = getattr(u_obj, "username", None)
        if uname:
            extra.append(str(uname))
    return extra


def display_name_of_entity(entity: Any) -> str:
    if entity is None:
        return "Desconhecido"
    title = getattr(entity, "title", None)
    if title:
        return str(title).strip()
    first = str(getattr(entity, "first_name", "") or "").strip()
    last = str(getattr(entity, "last_name", "") or "").strip()
    full = f"{first} {last}".strip()
    if full:
        return full
    uname = getattr(entity, "username", None)
    if uname:
        return f"@{uname}"
    ent_id = getattr(entity, "id", None)
    return str(ent_id) if ent_id is not None else "Desconhecido"


def classify_entity_to_chat_info(
    entity: Any,
    *,
    self_id: int,
    account_alias: str | None = None,
) -> EntityChatInfo:
    """Classify a Telethon User/Chat/Channel (or duck-typed object) into canonical metadata."""
    raw_id = int(getattr(entity, "id", 0))
    username = getattr(entity, "username", None)
    usernames = extract_extra_usernames(entity)
    if not username and usernames:
        username = usernames[0]

    is_megagroup = bool(getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False))
    is_broadcast = bool(getattr(entity, "broadcast", False))
    is_forum = bool(getattr(entity, "forum", False))
    is_bot = bool(getattr(entity, "bot", False))
    participants_count = getattr(entity, "participants_count", None)

    has_channel_flags = hasattr(entity, "megagroup") or hasattr(entity, "broadcast")
    has_group_title = getattr(entity, "title", None) is not None and not hasattr(
        entity, "first_name"
    )

    if isinstance(entity, (tl_types.Channel, tl_types.ChannelForbidden)) or has_channel_flags:
        kind: ChatKind = "supergroup" if (is_megagroup or not is_broadcast) else "channel"
        section: ManifestSection = "groups" if kind == "supergroup" else "channels"
        title = display_name_of_entity(entity)
    elif isinstance(entity, (tl_types.Chat, tl_types.ChatForbidden)) or has_group_title:
        kind = "group"
        section = "groups"
        title = display_name_of_entity(entity)
    else:
        if raw_id == self_id:
            kind = "saved"
            section = "saved"
            label = account_alias or str(self_id)
            title = f"Saved Messages ({label})"
        elif is_bot:
            kind = "bot"
            section = "bots"
            title = display_name_of_entity(entity)
        else:
            kind = "dm"
            section = "private"
            title = display_name_of_entity(entity)

    chat_key = make_chat_key(kind, raw_id, self_id)
    return EntityChatInfo(
        chat_key=chat_key,
        kind=kind,
        section=section,
        title=title,
        username=str(username) if username else None,
        usernames=usernames,
        peer_raw_id=raw_id,
        is_forum=is_forum,
        is_bot=is_bot,
        participants_count=int(participants_count) if participants_count is not None else None,
    )


def _extract_entities_and_button_urls(msg: Any) -> tuple[list[dict[str, Any]], list[str]]:
    serialized_entities: list[dict[str, Any]] = []
    extra_urls: list[str] = []

    for ent in getattr(msg, "entities", None) or []:
        item: dict[str, Any] = {
            "type": type(ent).__name__,
            "offset": int(getattr(ent, "offset", 0)),
            "length": int(getattr(ent, "length", 0)),
        }
        url_val = getattr(ent, "url", None)
        if url_val:
            item["url"] = str(url_val)
            extra_urls.append(str(url_val))
        serialized_entities.append(item)

    reply_markup = getattr(msg, "reply_markup", None)
    for row in getattr(reply_markup, "rows", None) or []:
        for btn in getattr(row, "buttons", None) or []:
            btn_url = getattr(btn, "url", None)
            if isinstance(btn_url, str) and btn_url:
                extra_urls.append(btn_url)

    media_webpage = getattr(getattr(msg, "media", None), "webpage", None)
    wp_url = getattr(media_webpage, "url", None)
    if isinstance(wp_url, str) and wp_url:
        extra_urls.append(wp_url)

    return serialized_entities, extra_urls


def _extract_topic_id(msg: Any) -> int | None:
    reply_to = getattr(msg, "reply_to", None)
    if reply_to is None:
        return None
    if getattr(reply_to, "forum_topic", False):
        top_id = getattr(reply_to, "reply_to_top_id", None)
        if top_id is not None:
            return int(top_id)
        r_id = getattr(reply_to, "reply_to_msg_id", None)
        if r_id is not None:
            return int(r_id)
    return None


def _extract_poll(msg: Any) -> dict[str, Any] | None:
    media = getattr(msg, "media", None)
    poll = getattr(media, "poll", None)
    if poll is None:
        return None
    q_obj = getattr(poll, "question", "")
    question = getattr(q_obj, "text", q_obj)
    answers: list[dict[str, Any]] = []
    for ans in getattr(poll, "answers", None) or []:
        a_txt = getattr(ans, "text", "")
        answers.append(
            {
                "text": str(getattr(a_txt, "text", a_txt)),
                "option": bytes(getattr(ans, "option", b"")).hex(),
            }
        )
    results_obj = getattr(media, "results", None)
    total_voters = getattr(results_obj, "total_voters", None)
    return {
        "id": int(getattr(poll, "id", 0)),
        "question": str(question),
        "quiz": bool(getattr(poll, "quiz", False)),
        "public_voters": bool(getattr(poll, "public_voters", False)),
        "total_voters": int(total_voters) if total_voters is not None else None,
        "answers": answers,
    }


def _extract_reactions_summary(
    msg: Any,
    detailed_users: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    reactions_obj = getattr(msg, "reactions", None)
    if reactions_obj is None and not detailed_users:
        return None
    summary: list[dict[str, Any]] = []
    for res in getattr(reactions_obj, "results", None) or []:
        r_obj = getattr(res, "reaction", None)
        emoticon = getattr(r_obj, "emoticon", None)
        doc_id = getattr(r_obj, "document_id", None)
        label = emoticon if emoticon else (f"custom:{doc_id}" if doc_id else "paid")
        summary.append(
            {
                "emoji": str(label),
                "count": int(getattr(res, "count", 0)),
            }
        )
    if not summary and not detailed_users:
        return None
    return {
        "summary": summary,
        "users": detailed_users or [],
    }


def _extract_service_action(msg: Any) -> dict[str, Any] | None:
    action = getattr(msg, "action", None)
    if action is None:
        return None
    cls_name = type(action).__name__
    info: dict[str, Any] = {"type": cls_name}
    for attr_name in ("title", "duration", "score", "Call_id"):
        val = getattr(action, attr_name.lower(), None)
        if val is not None:
            info[attr_name.lower()] = val
    reason = getattr(action, "reason", None)
    if reason is not None:
        info["reason"] = type(reason).__name__
    users = getattr(action, "users", None)
    if users:
        info["users"] = [int(u) for u in users]
    return info


def _extract_forward(msg: Any) -> dict[str, Any] | None:
    fwd = getattr(msg, "fwd_from", None)
    if fwd is None:
        return None
    from_name = getattr(fwd, "from_name", None)
    from_id_obj = getattr(fwd, "from_id", None)
    from_id: int | None = None
    if from_id_obj is not None:
        for attr in ("user_id", "channel_id", "chat_id"):
            val = getattr(from_id_obj, attr, None)
            if val is not None:
                from_id = int(val)
                break
    return {
        "from_id": from_id,
        "from_name": str(from_name) if from_name else None,
        "date": to_utc_iso(getattr(fwd, "date", None)),
        "channel_post": getattr(fwd, "channel_post", None),
    }


def serialize_message_to_dict(
    msg: Any,
    *,
    account_id: int,
    canonical_key: str,
    media_meta: dict[str, Any] | None = None,
    detailed_reactions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Convert a Telethon Message into our clean plain-text daily JSON record."""
    msg_id = int(getattr(msg, "id", 0))
    date_iso = to_utc_iso(getattr(msg, "date", None)) or ""
    edit_iso = to_utc_iso(getattr(msg, "edit_date", None))
    text = str(getattr(msg, "message", None) or getattr(msg, "raw_text", None) or "")

    sender_obj = getattr(msg, "sender", None)
    sender_id_raw = getattr(msg, "sender_id", None)
    if sender_id_raw is None and sender_obj is not None:
        sender_id_raw = getattr(sender_obj, "id", None)
    sender_id = int(sender_id_raw) if sender_id_raw is not None else None
    sender_name = (
        display_name_of_entity(sender_obj)
        if sender_obj is not None
        else (str(sender_id) if sender_id is not None else "Canal / Sistema")
    )
    sender_username = getattr(sender_obj, "username", None) if sender_obj is not None else None

    entities, extra_urls = _extract_entities_and_button_urls(msg)
    links = extract_links_from_text_and_entities(text, entities, extra_urls)

    reply_to = getattr(msg, "reply_to", None)
    reply_to_msg_id = getattr(reply_to, "reply_to_msg_id", None)
    if reply_to_msg_id is None:
        reply_to_msg_id = getattr(msg, "reply_to_msg_id", None)

    return {
        "canonical_key": canonical_key,
        "id": msg_id,
        "account_msg_ids": {str(account_id): msg_id},
        "date": date_iso,
        "topic_id": _extract_topic_id(msg),
        "sender": {
            "id": sender_id,
            "name": sender_name,
            "username": str(sender_username) if sender_username else None,
        },
        "text": text,
        "entities": entities,
        "links": links,
        "reply_to_msg_id": int(reply_to_msg_id) if reply_to_msg_id is not None else None,
        "forward_from": _extract_forward(msg),
        "media": media_meta,
        "reactions": _extract_reactions_summary(msg, detailed_reactions),
        "poll": _extract_poll(msg),
        "service_action": _extract_service_action(msg),
        "views": getattr(msg, "views", None),
        "forwards": getattr(msg, "forwards", None),
        "grouped_id": getattr(msg, "grouped_id", None),
        "noforwards": bool(getattr(msg, "noforwards", False)),
        "edit_date": edit_iso,
        "revisions": [],
        "is_deleted": False,
        "deleted_detected_at": None,
        "delete_source": None,
        "seen_by_accounts": [str(account_id)],
    }
