"""Partitioned daily JSON writer (`chats/YYYY-MM-DD-pt000.json`, `-pt001.json`, ...)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from tghoard.storage.atomic import atomic_write_json, utc_now_iso
from tghoard.storage.paths import (
    format_part_tag,
    list_day_part_files,
    part_file_path,
    uses_global_msg_ids,
)


def make_canonical_message_key(
    *,
    chat_key: str,
    msg_id: int,
    sender_id: int | None,
    date_iso: str,
    media_tg_id: int | None = None,
    grouped_id: int | None = None,
    text: str = "",
) -> str:
    """Compute cross-account canonical key for a message."""
    if uses_global_msg_ids(chat_key):
        return f"id:{msg_id}"
    discriminator = (
        f"media:{media_tg_id}" if media_tg_id is not None else f"grp:{grouped_id}|txt:{text[:128]}"
    )
    raw = f"{sender_id or 0}|{date_iso}|{discriminator}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"fp:{digest}"


def _message_sort_key(msg: dict[str, Any]) -> tuple[str, int, str]:
    return (
        str(msg.get("date") or ""),
        int(msg.get("id") or 0),
        str(msg.get("canonical_key") or ""),
    )


def _merge_message_records(
    existing: dict[str, Any],
    incoming: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Merge an incoming message into an existing record, tracking revisions on edit.

    Returns `(merged_record, new_revision_or_none)`.
    """
    merged = dict(existing)
    added_revision: dict[str, Any] | None = None

    # Merge multi-account maps
    acct_ids = dict(existing.get("account_msg_ids") or {})
    for k, v in (incoming.get("account_msg_ids") or {}).items():
        acct_ids[str(k)] = int(v)
    merged["account_msg_ids"] = acct_ids

    seen = sorted(
        set(existing.get("seen_by_accounts") or []) | set(incoming.get("seen_by_accounts") or [])
    )
    merged["seen_by_accounts"] = seen

    # Detect edit: text changed or edit_date advanced
    old_text = existing.get("text") or ""
    new_text = incoming.get("text") if incoming.get("text") is not None else old_text
    old_edit_date = existing.get("edit_date")
    new_edit_date = incoming.get("edit_date")

    text_changed = incoming.get("text") is not None and new_text != old_text
    edit_advanced = new_edit_date is not None and new_edit_date != old_edit_date and text_changed

    revisions = list(existing.get("revisions") or [])
    if text_changed or edit_advanced:
        added_revision = {
            "revision": len(revisions) + 1,
            "captured_at": utc_now_iso(),
            "edit_date": old_edit_date,
            "text": old_text,
        }
        revisions.append(added_revision)
        merged["text"] = new_text
        merged["entities"] = incoming.get("entities", existing.get("entities"))
        merged["links"] = incoming.get("links", existing.get("links"))

    if new_edit_date is not None:
        merged["edit_date"] = new_edit_date
    merged["revisions"] = revisions

    # Preserve downloaded media if existing already downloaded it!
    existing_media = existing.get("media")
    incoming_media = incoming.get("media")
    if existing_media and existing_media.get("downloaded"):
        merged["media"] = existing_media
    elif incoming_media is not None:
        merged["media"] = incoming_media

    # Update reactions / views / forwards if fresher
    if incoming.get("reactions") is not None:
        merged["reactions"] = incoming["reactions"]
    if incoming.get("views") is not None:
        merged["views"] = incoming["views"]
    if incoming.get("forwards") is not None:
        merged["forwards"] = incoming["forwards"]
    if incoming.get("poll") is not None:
        merged["poll"] = incoming["poll"]

    # Never clear deletion once detected!
    if existing.get("is_deleted") or incoming.get("is_deleted"):
        merged["is_deleted"] = True
        merged["deleted_detected_at"] = (
            existing.get("deleted_detected_at")
            or incoming.get("deleted_detected_at")
            or utc_now_iso()
        )
        merged["delete_source"] = existing.get("delete_source") or incoming.get("delete_source")

    return merged, added_revision


def _build_part_envelope(
    *,
    chat_key: str,
    chat_title: str,
    day_str: str,
    part_num: int,
    messages: list[dict[str, Any]],
) -> dict[str, Any]:
    first_id = messages[0].get("id") if messages else None
    last_id = messages[-1].get("id") if messages else None
    return {
        "chat_key": chat_key,
        "chat_title": chat_title,
        "date": day_str,
        "part": part_num,
        "part_tag": format_part_tag(part_num),
        "message_count": len(messages),
        "first_msg_id": first_id,
        "last_msg_id": last_id,
        "updated_at": utc_now_iso(),
        "messages": messages,
    }


def _estimate_json_bytes(obj: Any) -> int:
    return len(json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8"))


def pack_messages_into_parts(
    *,
    chat_key: str,
    chat_title: str,
    day_str: str,
    messages: Sequence[dict[str, Any]],
    max_json_bytes: int,
    start_part: int = 0,
) -> list[tuple[int, dict[str, Any]]]:
    """Deterministically pack sorted messages into `-ptNNN` envelopes <= `max_json_bytes`."""
    if not messages:
        return []

    base_envelope = _build_part_envelope(
        chat_key=chat_key,
        chat_title=chat_title,
        day_str=day_str,
        part_num=start_part,
        messages=[],
    )
    overhead_bytes = _estimate_json_bytes(base_envelope) + 64

    parts: list[tuple[int, dict[str, Any]]] = []
    current_msgs: list[dict[str, Any]] = []
    current_bytes = overhead_bytes
    part_idx = start_part

    for msg in messages:
        # Indent inside `messages` array adds ~4 bytes per line; conservative estimate:
        msg_serialized = json.dumps(msg, indent=2, ensure_ascii=False)
        indented_bytes = len(msg_serialized.encode("utf-8")) + msg_serialized.count("\n") * 4 + 8

        if current_msgs and (current_bytes + indented_bytes) > max_json_bytes:
            envelope = _build_part_envelope(
                chat_key=chat_key,
                chat_title=chat_title,
                day_str=day_str,
                part_num=part_idx,
                messages=current_msgs,
            )
            parts.append((part_idx, envelope))
            part_idx += 1
            current_msgs = [msg]
            current_bytes = overhead_bytes + indented_bytes
        else:
            current_msgs.append(msg)
            current_bytes += indented_bytes

    if current_msgs:
        envelope = _build_part_envelope(
            chat_key=chat_key,
            chat_title=chat_title,
            day_str=day_str,
            part_num=part_idx,
            messages=current_msgs,
        )
        parts.append((part_idx, envelope))

    return parts


class ChatDayWriter:
    """Reads, merges, splits, and updates `chats/YYYY-MM-DD-ptNNN.json` files."""

    def __init__(self, max_json_bytes: int = 1024 * 1024) -> None:
        self.max_json_bytes = max(4096, max_json_bytes)

    def upsert_day_messages(
        self,
        *,
        chat_dir: Path,
        chat_key: str,
        chat_title: str,
        day_str: str,
        incoming_messages: Sequence[dict[str, Any]],
    ) -> tuple[dict[str, int], list[tuple[dict[str, Any], dict[str, Any]]]]:
        """Merge `incoming_messages` into `YYYY-MM-DD-ptNNN.json` files.

        Returns `(part_map: {canonical_key: part_num}, edits: [(msg, revision), ...])`.
        """
        if not incoming_messages:
            return {}, []

        existing_parts = list_day_part_files(chat_dir, day_str)
        edits_detected: list[tuple[dict[str, Any], dict[str, Any]]] = []

        # Fast-path: pure chronological tail append when >=2 parts exist!
        if len(existing_parts) >= 2:
            prev_last_part_num, prev_last_path = existing_parts[-2]
            prev_payload = json.loads(prev_last_path.read_text(encoding="utf-8"))
            prev_msgs = prev_payload.get("messages") or []
            tail_part_num, tail_path = existing_parts[-1]
            tail_payload = json.loads(tail_path.read_text(encoding="utf-8"))
            tail_msgs = tail_payload.get("messages") or []

            if prev_msgs and tail_part_num == prev_last_part_num + 1:
                boundary_key = _message_sort_key(prev_msgs[-1])
                if all(_message_sort_key(m) > boundary_key for m in incoming_messages):
                    by_ckey: dict[str, dict[str, Any]] = {
                        str(m["canonical_key"]): m for m in tail_msgs
                    }
                    for inc in incoming_messages:
                        ckey = str(inc["canonical_key"])
                        if ckey in by_ckey:
                            merged_msg, rev = _merge_message_records(by_ckey[ckey], inc)
                            by_ckey[ckey] = merged_msg
                            if rev is not None:
                                edits_detected.append((merged_msg, rev))
                        else:
                            by_ckey[ckey] = dict(inc)

                    sorted_tail = sorted(by_ckey.values(), key=_message_sort_key)
                    packed = pack_messages_into_parts(
                        chat_key=chat_key,
                        chat_title=chat_title,
                        day_str=day_str,
                        messages=sorted_tail,
                        max_json_bytes=self.max_json_bytes,
                        start_part=tail_part_num,
                    )
                    part_map: dict[str, int] = {}
                    for pnum, env in packed:
                        atomic_write_json(part_file_path(chat_dir, day_str, pnum), env)
                        for m in env["messages"]:
                            part_map[str(m["canonical_key"])] = pnum
                    return part_map, edits_detected

        # General full-day merge & repack (loads all existing parts for that single day)
        all_by_ckey: dict[str, dict[str, Any]] = {}
        for _, ppath in existing_parts:
            payload = json.loads(ppath.read_text(encoding="utf-8"))
            for m in payload.get("messages") or []:
                all_by_ckey[str(m["canonical_key"])] = m

        for inc in incoming_messages:
            ckey = str(inc["canonical_key"])
            if ckey in all_by_ckey:
                merged_msg, rev = _merge_message_records(all_by_ckey[ckey], inc)
                all_by_ckey[ckey] = merged_msg
                if rev is not None:
                    edits_detected.append((merged_msg, rev))
            else:
                all_by_ckey[ckey] = dict(inc)

        sorted_all = sorted(all_by_ckey.values(), key=_message_sort_key)
        packed_all = pack_messages_into_parts(
            chat_key=chat_key,
            chat_title=chat_title,
            day_str=day_str,
            messages=sorted_all,
            max_json_bytes=self.max_json_bytes,
            start_part=0,
        )

        written_part_nums: set[int] = set()
        part_map = {}
        for pnum, env in packed_all:
            atomic_write_json(part_file_path(chat_dir, day_str, pnum), env)
            written_part_nums.add(pnum)
            for m in env["messages"]:
                part_map[str(m["canonical_key"])] = pnum

        # Clean up any leftover higher-numbered part files if day compacted
        for old_pnum, old_path in existing_parts:
            if old_pnum not in written_part_nums:
                old_path.unlink(missing_ok=True)

        return part_map, edits_detected

    def mark_message_deleted(
        self,
        *,
        chat_dir: Path,
        day_str: str,
        part_num: int,
        canonical_key: str,
        source: str,
        deleted_at: str | None = None,
    ) -> dict[str, Any] | None:
        """Mark a single message as `is_deleted=True` inside its exact `-ptNNN.json` file."""
        target = part_file_path(chat_dir, day_str, part_num)
        candidates = [target]
        if not target.exists():
            candidates = [p for _, p in list_day_part_files(chat_dir, day_str)]

        ts = deleted_at or utc_now_iso()
        for path in candidates:
            if not path.exists():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            changed = False
            matched_msg: dict[str, Any] | None = None
            for msg in payload.get("messages") or []:
                if msg.get("canonical_key") == canonical_key:
                    if not msg.get("is_deleted"):
                        msg["is_deleted"] = True
                        msg["deleted_detected_at"] = ts
                        msg["delete_source"] = source
                        changed = True
                    matched_msg = msg
                    break
            if matched_msg is not None:
                if changed:
                    payload["updated_at"] = utc_now_iso()
                    atomic_write_json(path, payload)
                return matched_msg
        return None

    def attach_downloaded_media(
        self,
        *,
        chat_dir: Path,
        day_str: str,
        part_num: int,
        canonical_key: str,
        rel_media_path: str,
        sha256: str,
        size_bytes: int,
    ) -> bool:
        """Update `media.downloaded = True` and `media.path` after retroactive media download."""
        target = part_file_path(chat_dir, day_str, part_num)
        candidates = (
            [target] if target.exists() else [p for _, p in list_day_part_files(chat_dir, day_str)]
        )
        for path in candidates:
            payload = json.loads(path.read_text(encoding="utf-8"))
            for msg in payload.get("messages") or []:
                if msg.get("canonical_key") == canonical_key:
                    media_obj = dict(msg.get("media") or {})
                    media_obj["downloaded"] = True
                    media_obj["path"] = rel_media_path
                    media_obj["sha256"] = sha256
                    media_obj["size_bytes"] = size_bytes
                    msg["media"] = media_obj
                    payload["updated_at"] = utc_now_iso()
                    atomic_write_json(path, payload)
                    return True
        return False
