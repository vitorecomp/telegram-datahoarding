"""Partitioned monthly JSON writer (`messages/<folder>/YYYY-MM-pt000.json`, `-pt001.json`, ...)."""

from __future__ import annotations

import hashlib
import json
import os
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from tghoard.storage.atomic import atomic_write_json, utc_now_iso
from tghoard.storage.paths import (
    ChatPaths,
    format_part_tag,
    list_month_part_files,
    month_from_iso,
    part_file_path,
    uses_global_msg_ids,
)

# Stable, account-independent attributes of a service action / non-file media used to tell
# apart messages that have neither text nor a downloadable attachment (see
# `message_content_tag`). Never include per-account values (`access_hash`,
# `file_reference`) or fields that change later (`duration` of a call, poll results).
_TAG_SCALAR_ATTRS = (
    "id",
    "users",
    "user_id",
    "inviter_id",
    "channel_id",
    "chat_id",
    "title",
    "message",
    "emoticon",
    "value",
    "game_id",
    "score",
    "period",
    "domain",
    "currency",
    "total_amount",
    "months",
    "phone_number",
    "first_name",
    "last_name",
    "address",
    "venue_id",
    "provider",
    "slug",
)
_TAG_NESTED_ID_ATTRS = ("photo", "call", "poll", "game", "story")


def _tag_value(val: Any) -> Any:
    """Keep plain scalars / lists of scalars; anything else (TL objects, bytes) is dropped."""
    if val is None or val == "":
        return None
    if isinstance(val, int | str):
        return val
    if isinstance(val, list | tuple) and val and all(isinstance(v, int | str) for v in val):
        return list(val)
    return None


def message_content_tag(msg: Any) -> str | None:
    """Deterministic, account-independent description of a message's action/media payload.

    Used by `make_canonical_message_key` only for messages with neither text nor a classified
    attachment (service events, polls, dice, contacts, locations, ...), which would otherwise
    collide when the same sender produces two of them within the same second.
    """
    payload: dict[str, Any] = {}
    for kind in ("action", "media"):
        obj = getattr(msg, kind, None)
        if obj is None:
            continue
        entry: dict[str, Any] = {"type": type(obj).__name__}
        for attr in _TAG_SCALAR_ATTRS:
            norm = _tag_value(getattr(obj, attr, None))
            if norm is not None:
                entry[attr] = norm
        for attr in _TAG_NESTED_ID_ATTRS:
            inner_id = getattr(getattr(obj, attr, None), "id", None)
            if isinstance(inner_id, int) and not isinstance(inner_id, bool):
                entry[f"{attr}_id"] = inner_id
        payload[kind] = entry
    if not payload:
        return None
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def make_canonical_message_key(
    *,
    chat_key: str,
    msg_id: int,
    sender_id: int | None,
    date_iso: str,
    media_tg_id: int | None = None,
    grouped_id: int | None = None,
    text: str = "",
    content_tag: str | None = None,
) -> str:
    """Compute cross-account canonical key for a message.

    Channels/supergroups share message ids across accounts (`id:<msg_id>`). Elsewhere ids are
    per account, so the key is a content fingerprint (sender + second + media id or text).
    `content_tag` (see `message_content_tag`) is only mixed in for messages with neither
    text nor media, so the keys of ordinary messages already archived never change.
    """
    if uses_global_msg_ids(chat_key):
        return f"id:{msg_id}"
    if media_tg_id is not None:
        discriminator = f"media:{media_tg_id}"
    else:
        discriminator = f"grp:{grouped_id}|txt:{text[:128]}"
        if not text and content_tag:
            discriminator += f"|tag:{content_tag}"
    raw = f"{sender_id or 0}|{date_iso}|{discriminator}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"fp:{digest}"


def _ckey(msg: dict[str, Any]) -> str:
    return str(msg.get("canonical_key") or f"id:{msg.get('id')}")


def _message_sort_key(msg: dict[str, Any]) -> tuple[str, int, str]:
    return (
        str(msg.get("date") or ""),
        int(msg.get("id") or 0),
        str(msg.get("canonical_key") or ""),
    )


def _revision_sig(rev: dict[str, Any]) -> tuple[Any, ...]:
    entities = rev.get("entities")
    return (
        rev.get("edit_date"),
        rev.get("text"),
        json.dumps(entities, sort_keys=True, default=str) if "entities" in rev else None,
    )


def _merge_message_records(
    existing: dict[str, Any],
    incoming: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Merge an incoming message into an existing record, tracking revisions on edit.

    Returns `(merged_record, new_revision_or_none)`. Never discards a previous text: an
    incoming record that carries its own `revisions` (another archived copy of the same
    message) has them unioned in.
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

    revisions = [dict(r) for r in existing.get("revisions") or []]
    known = {_revision_sig(r) for r in revisions}
    for rev in incoming.get("revisions") or []:
        sig = _revision_sig(rev)
        if sig not in known:
            known.add(sig)
            revisions.append(dict(rev))

    # Detect edit: text changed, or (on a real edit) only the entities changed, e.g. the URL
    # behind a text link
    old_text = existing.get("text") or ""
    new_text = incoming.get("text") if incoming.get("text") is not None else old_text
    old_edit_date = existing.get("edit_date")
    new_edit_date = incoming.get("edit_date")

    text_changed = incoming.get("text") is not None and new_text != old_text
    edit_advanced = new_edit_date is not None and new_edit_date != old_edit_date
    entities_changed = incoming.get("entities") is not None and incoming.get(
        "entities"
    ) != existing.get("entities")

    if text_changed or (edit_advanced and entities_changed):
        candidate: dict[str, Any] = {
            "revision": len(revisions) + 1,
            "captured_at": utc_now_iso(),
            "edit_date": old_edit_date,
            "text": old_text,
        }
        if entities_changed and not text_changed:
            candidate["entities"] = existing.get("entities")
            candidate["links"] = existing.get("links")
        if _revision_sig(candidate) not in known:
            revisions.append(candidate)
            added_revision = candidate
        merged["text"] = new_text
        merged["entities"] = incoming.get("entities", existing.get("entities"))
        merged["links"] = incoming.get("links", existing.get("links"))

    for idx, rev in enumerate(revisions, start=1):
        rev["revision"] = idx

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


def _reconcile_copies(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Merge two archived copies of the same message (left by an interrupted repack).

    Revisions only ever grow, so the copy with more of them is the newer one; it is merged
    on top of the older one, keeping every revision, deletion flag and downloaded file.
    """
    if a is b:
        return a
    ra, rb = len(a.get("revisions") or []), len(b.get("revisions") or [])
    if ra != rb:
        older, newer = (a, b) if ra < rb else (b, a)
    elif str(a.get("edit_date") or "") > str(b.get("edit_date") or ""):
        older, newer = b, a
    else:
        older, newer = a, b
    merged, _ = _merge_message_records(older, newer)
    return merged


def _month_needs_repair(parts: Sequence[tuple[int, dict[str, Any]]]) -> bool:
    """True if a month's parts overlap (duplicated keys), are out of order or are not
    numbered `pt000..ptN` without gaps.

    These only happen after an interrupted repack (or a hand edit); the next write then
    repacks the whole month.
    """
    if [pnum for pnum, _ in parts] != list(range(len(parts))):
        return True
    seen: set[str] = set()
    prev_last: tuple[str, int, str] | None = None
    for _, payload in parts:
        msgs = payload.get("messages") or []
        for msg in msgs:
            key = _ckey(msg)
            if key in seen:
                return True
            seen.add(key)
        if msgs:
            if prev_last is not None and _message_sort_key(msgs[0]) <= prev_last:
                return True
            prev_last = _message_sort_key(msgs[-1])
    return False


def _same_part_content(old: dict[str, Any], new: dict[str, Any]) -> bool:
    keys = (set(old) | set(new)) - {"updated_at"}
    return all(old.get(k) == new.get(k) for k in keys)


def _with_downloaded_media(
    msg: dict[str, Any], *, rel_media_path: str, sha256: str, size_bytes: int
) -> dict[str, Any]:
    media_obj = dict(msg.get("media") or {})
    media_obj["downloaded"] = True
    media_obj["path"] = rel_media_path
    media_obj["sha256"] = sha256
    media_obj["size_bytes"] = size_bytes
    media_obj.pop("skip_reason", None)
    updated = dict(msg)
    updated["media"] = media_obj
    return updated


def _fsync_dir(path: Path) -> None:
    """Make renames inside `path` durable (orders part writes across a power loss)."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _build_part_envelope(
    *,
    chat_key: str,
    chat_title: str,
    month: str,
    part_num: int,
    messages: list[dict[str, Any]],
) -> dict[str, Any]:
    first_id = messages[0].get("id") if messages else None
    last_id = messages[-1].get("id") if messages else None
    return {
        "chat_key": chat_key,
        "chat_title": chat_title,
        "month": month,
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
    month: str,
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
        month=month,
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
                month=month,
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
            month=month,
            part_num=part_idx,
            messages=current_msgs,
        )
        parts.append((part_idx, envelope))

    return parts


class ChatPartWriter:
    """Reads, merges, splits, and updates `messages/<folder>/YYYY-MM-ptNNN.json` files.

    Crash safety: a repack writes the parts in dependency order (a message's new part is
    durably on disk before its old part is overwritten; obsolete parts are removed last), so
    an interruption can leave a message in two parts but never in none. Such duplicates are
    merged on read and the month is repacked cleanly by its next write.
    """

    def __init__(self, max_json_bytes: int = 1024 * 1024, *, cache_parts: int = 16) -> None:
        self.max_json_bytes = max(4096, max_json_bytes)
        # Parsed part files, keyed by path and validated by (mtime_ns, size): appending a
        # batch no longer re-reads/re-parses the same 1 MB parts every 100 messages.
        self._cache: OrderedDict[Path, tuple[tuple[int, int, int], dict[str, Any]]] = OrderedDict()
        self._cache_parts = cache_parts
        # Months fully checked (no overlap left by an interrupted repack) by this process;
        # only those may take the tail-append fast path.
        self._clean_months: set[tuple[Path, str]] = set()

    # -- cached part I/O ----------------------------------------------------------------
    @staticmethod
    def _sig(path: Path) -> tuple[int, int, int]:
        st = path.stat()
        # inode too: atomic writes (os.replace) always create a new inode, so a same-size
        # rewrite within one coarse mtime tick is still detected
        return (st.st_ino, st.st_mtime_ns, st.st_size)

    def _read(self, path: Path) -> dict[str, Any]:
        sig = self._sig(path)
        hit = self._cache.get(path)
        if hit is not None and hit[0] == sig:
            self._cache.move_to_end(path)
            return hit[1]
        payload = json.loads(path.read_text(encoding="utf-8"))
        self._remember(path, sig, payload)
        return payload

    def _write(self, path: Path, payload: dict[str, Any]) -> None:
        try:
            atomic_write_json(path, payload)
        except BaseException:
            self._forget(path)  # the cached payload may hold edits that never hit the disk
            raise
        self._remember(path, self._sig(path), payload)

    def _remember(self, path: Path, sig: tuple[int, int, int], payload: dict[str, Any]) -> None:
        self._cache[path] = (sig, payload)
        self._cache.move_to_end(path)
        while len(self._cache) > self._cache_parts:
            self._cache.popitem(last=False)

    def _forget(self, path: Path) -> None:
        self._cache.pop(path, None)

    # -- month writes ---------------------------------------------------------------------
    def upsert_month_messages(
        self,
        *,
        chat_paths: ChatPaths,
        chat_key: str,
        chat_title: str,
        month: str,
        incoming_messages: Sequence[dict[str, Any]],
    ) -> tuple[dict[str, int], list[tuple[dict[str, Any], dict[str, Any]]]]:
        """Merge `incoming_messages` into `YYYY-MM-ptNNN.json` files.

        `month` is `YYYY-MM` (a full ISO date is truncated to its month). Returns
        `(part_map: {canonical_key: part_num}, edits: [(msg, revision), ...])`.
        """
        if not incoming_messages:
            return {}, []
        month = month_from_iso(month)
        existing_parts = list_month_part_files(chat_paths, month)
        month_id = (chat_paths.messages_dir, month)

        loaded: list[tuple[int, Path, dict[str, Any]]] | None = None
        needs_repair = False
        if existing_parts and month_id not in self._clean_months:
            loaded = [(pnum, path, self._read(path)) for pnum, path in existing_parts]
            needs_repair = _month_needs_repair([(pnum, payload) for pnum, _, payload in loaded])
            if not needs_repair:
                self._clean_months.add(month_id)

        try:
            if existing_parts and not needs_repair:
                result = self._try_tail_append(
                    chat_paths=chat_paths,
                    chat_key=chat_key,
                    chat_title=chat_title,
                    month=month,
                    existing_parts=existing_parts,
                    incoming_messages=incoming_messages,
                )
                if result is not None:
                    return result
            result = self._repack_month(
                chat_paths=chat_paths,
                chat_key=chat_key,
                chat_title=chat_title,
                month=month,
                existing_parts=existing_parts,
                loaded=loaded,
                incoming_messages=incoming_messages,
            )
        except BaseException:
            self._clean_months.discard(month_id)  # parts may overlap now: re-check next time
            raise
        self._clean_months.add(month_id)
        return result

    @staticmethod
    def _merge_incoming(
        by_ckey: dict[str, dict[str, Any]],
        incoming_messages: Sequence[dict[str, Any]],
    ) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        edits: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for inc in incoming_messages:
            ckey = _ckey(inc)
            if ckey in by_ckey:
                merged_msg, rev = _merge_message_records(by_ckey[ckey], inc)
                by_ckey[ckey] = merged_msg
                if rev is not None:
                    edits.append((merged_msg, rev))
            else:
                by_ckey[ckey] = dict(inc)
        return edits

    def _try_tail_append(
        self,
        *,
        chat_paths: ChatPaths,
        chat_key: str,
        chat_title: str,
        month: str,
        existing_parts: list[tuple[int, Path]],
        incoming_messages: Sequence[dict[str, Any]],
    ) -> tuple[dict[str, int], list[tuple[dict[str, Any], dict[str, Any]]]] | None:
        """Chronological tail append: only the last part is rewritten (plus new parts if it
        overflows). Applies when every incoming message sorts after the end of the part
        before the tail (or the tail is the month's only part); None otherwise."""
        tail_part_num, tail_path = existing_parts[-1]
        tail_payload = self._read(tail_path)
        on_disk: dict[int, dict[str, Any]] = {tail_part_num: tail_payload}
        if len(existing_parts) >= 2:
            prev_part_num, prev_path = existing_parts[-2]
            prev_payload = self._read(prev_path)
            prev_msgs = prev_payload.get("messages") or []
            if not prev_msgs or tail_part_num != prev_part_num + 1:
                return None
            boundary_key = _message_sort_key(prev_msgs[-1])
            if not all(_message_sort_key(m) > boundary_key for m in incoming_messages):
                return None
            # A copy from another account may carry a different id (so it sorts after the
            # boundary) while the record already sits in the previous part
            prev_keys = {_ckey(m) for m in prev_msgs}
            if any(_ckey(m) in prev_keys for m in incoming_messages):
                return None
            on_disk[prev_part_num] = prev_payload

        by_ckey: dict[str, dict[str, Any]] = {}
        for m in tail_payload.get("messages") or []:
            key = _ckey(m)
            by_ckey[key] = _reconcile_copies(by_ckey[key], m) if key in by_ckey else m
        edits = self._merge_incoming(by_ckey, incoming_messages)

        packed = pack_messages_into_parts(
            chat_key=chat_key,
            chat_title=chat_title,
            month=month,
            messages=sorted(by_ckey.values(), key=_message_sort_key),
            max_json_bytes=self.max_json_bytes,
            start_part=tail_part_num,
        )
        self._commit_month(chat_paths, month, on_disk=on_disk, packed=packed, remove=[])
        return self._part_map(packed), edits

    def _repack_month(
        self,
        *,
        chat_paths: ChatPaths,
        chat_key: str,
        chat_title: str,
        month: str,
        existing_parts: list[tuple[int, Path]],
        loaded: list[tuple[int, Path, dict[str, Any]]] | None,
        incoming_messages: Sequence[dict[str, Any]],
    ) -> tuple[dict[str, int], list[tuple[dict[str, Any], dict[str, Any]]]]:
        """General full-month merge & repack (loads all existing parts for that month)."""
        if loaded is None:
            loaded = [(pnum, path, self._read(path)) for pnum, path in existing_parts]

        all_by_ckey: dict[str, dict[str, Any]] = {}
        for _, _, payload in loaded:
            for m in payload.get("messages") or []:
                key = _ckey(m)
                # Same message in two parts (interrupted repack): merge, never pick one
                all_by_ckey[key] = (
                    _reconcile_copies(all_by_ckey[key], m) if key in all_by_ckey else m
                )
        edits = self._merge_incoming(all_by_ckey, incoming_messages)

        packed_all = pack_messages_into_parts(
            chat_key=chat_key,
            chat_title=chat_title,
            month=month,
            messages=sorted(all_by_ckey.values(), key=_message_sort_key),
            max_json_bytes=self.max_json_bytes,
            start_part=0,
        )
        new_nums = {pnum for pnum, _ in packed_all}
        remove = [(pnum, path) for pnum, path, _ in loaded if pnum not in new_nums]
        self._commit_month(
            chat_paths,
            month,
            on_disk={pnum: payload for pnum, _, payload in loaded},
            packed=packed_all,
            remove=remove,
        )
        return self._part_map(packed_all), edits

    @staticmethod
    def _part_map(packed: list[tuple[int, dict[str, Any]]]) -> dict[str, int]:
        return {_ckey(m): pnum for pnum, env in packed for m in env["messages"]}

    def _commit_month(
        self,
        chat_paths: ChatPaths,
        month: str,
        *,
        on_disk: dict[int, dict[str, Any]],
        packed: list[tuple[int, dict[str, Any]]],
        remove: list[tuple[int, Path]],
    ) -> None:
        """Write `packed` and delete `remove` so that no crash point loses a message.

        `on_disk` holds the current payload of every part about to be rewritten or removed
        (and possibly other, untouched parts). A part is only overwritten once every message
        it gives away is durably stored in its new part (or in an untouched part). Boundaries
        move monotonically, so these dependencies are acyclic for a sorted month; for an
        out-of-order month a cycle is broken by first parking the stranded records in a
        temporary spill part (removed at the end, like any obsolete part).
        """
        new_home: dict[str, int] = {}
        new_records: dict[str, dict[str, Any]] = {}
        new_keys: dict[int, set[str]] = {}
        for pnum, env in packed:
            keys = new_keys.setdefault(pnum, set())
            for m in env["messages"]:
                key = _ckey(m)
                keys.add(key)
                new_home[key] = pnum
                new_records[key] = m

        todo = {
            pnum: env
            for pnum, env in packed
            if pnum not in on_disk or not _same_part_content(on_disk[pnum], env)
        }
        remove = list(remove)
        current = {
            pnum: {_ckey(m) for m in payload.get("messages") or []}
            for pnum, payload in on_disk.items()
        }
        # Copies in parts that are not rewritten stay valid throughout (obsolete parts are
        # only deleted after every write)
        safe_elsewhere: set[str] = set()
        for pnum, keys in current.items():
            if pnum not in todo:
                safe_elsewhere |= keys

        at_risk: dict[int, set[str]] = {}
        deps: dict[int, set[int]] = {}
        for pnum in todo:
            risky = {
                k
                for k in current.get(pnum, ())
                if k not in new_keys[pnum] and k not in safe_elsewhere
            }
            at_risk[pnum] = risky
            deps[pnum] = {new_home[k] for k in risky if new_home.get(k, pnum) != pnum} & set(todo)

        messages_dir = chat_paths.messages_dir
        pending = set(todo)
        next_spill = max([*on_disk, *new_keys, -1]) + 1
        while pending:
            ready = [p for p in pending if not (deps[p] & pending)]
            # Lowest first: a fresh month is written pt000, pt001, ... so an interruption
            # leaves a gap-free prefix
            if ready:
                pnum = min(ready)
            else:
                pnum = min(pending)
                stranded = sorted(
                    (new_records[k] for k in at_risk[pnum] if new_home[k] in pending),
                    key=_message_sort_key,
                )
                spill_path = part_file_path(chat_paths, month, next_spill)
                self._write(
                    spill_path,
                    _build_part_envelope(
                        chat_key=str(todo[pnum].get("chat_key") or ""),
                        chat_title=str(todo[pnum].get("chat_title") or ""),
                        month=month,
                        part_num=next_spill,
                        messages=stranded,
                    ),
                )
                _fsync_dir(messages_dir)
                remove.append((next_spill, spill_path))
                next_spill += 1
            self._write(part_file_path(chat_paths, month, pnum), todo[pnum])
            pending.discard(pnum)
            if pending or remove:
                _fsync_dir(messages_dir)

        for _, old_path in remove:
            old_path.unlink(missing_ok=True)
            self._forget(old_path)

    # -- in-place updates of a single message ---------------------------------------------
    def _update_message(
        self,
        *,
        chat_paths: ChatPaths,
        month: str,
        part_num: int | None,
        canonical_key: str,
        update: Callable[[dict[str, Any]], dict[str, Any] | None],
    ) -> tuple[int, dict[str, Any]] | None:
        """Apply `update` to a message, trying its expected part first, then every part of
        the month (the locator's part number can be stale). Returns `(part, message)` for
        the first part holding it, or None if no part of the month has it."""

        def _apply(path: Path) -> dict[str, Any] | None:
            payload = self._read(path)
            msgs = payload.get("messages") or []
            for idx, msg in enumerate(msgs):
                if _ckey(msg) != canonical_key:
                    continue
                updated = update(msg)
                if updated is None:
                    return msg
                # copy-on-write: cached payloads share message dicts across parts
                new_msgs = list(msgs)
                new_msgs[idx] = updated
                new_payload = dict(payload)
                new_payload["messages"] = new_msgs
                new_payload["updated_at"] = utc_now_iso()
                self._write(path, new_payload)
                return updated
            return None

        month = month_from_iso(month)
        hint = part_num if isinstance(part_num, int) and part_num >= 0 else None
        if hint is not None:
            hint_path = part_file_path(chat_paths, month, hint)
            if hint_path.exists():
                found = _apply(hint_path)
                if found is not None:
                    return hint, found

        first: tuple[int, dict[str, Any]] | None = None
        for pnum, path in list_month_part_files(chat_paths, month):
            if pnum == hint:
                continue
            found = _apply(path)  # every copy (an interrupted repack may leave two)
            if found is not None and first is None:
                first = (pnum, found)
        return first

    def locate_and_mark_deleted(
        self,
        *,
        chat_paths: ChatPaths,
        month: str,
        part_num: int | None,
        canonical_key: str,
        source: str,
        deleted_at: str | None = None,
    ) -> tuple[int, dict[str, Any]] | None:
        """Flag a message `is_deleted=True` wherever it is in the month.

        Returns `(actual_part, message)` (callers fix the locator when the part differs), or
        None when no part of that month holds the message.
        """
        ts = deleted_at or utc_now_iso()

        def _flag(msg: dict[str, Any]) -> dict[str, Any] | None:
            if msg.get("is_deleted"):
                return None
            flagged = dict(msg)
            flagged["is_deleted"] = True
            flagged["deleted_detected_at"] = ts
            flagged["delete_source"] = source
            return flagged

        return self._update_message(
            chat_paths=chat_paths,
            month=month,
            part_num=part_num,
            canonical_key=canonical_key,
            update=_flag,
        )

    def mark_message_deleted(
        self,
        *,
        chat_paths: ChatPaths,
        month: str,
        part_num: int,
        canonical_key: str,
        source: str,
        deleted_at: str | None = None,
    ) -> dict[str, Any] | None:
        """Mark a single message as `is_deleted=True` (see `locate_and_mark_deleted`)."""
        hit = self.locate_and_mark_deleted(
            chat_paths=chat_paths,
            month=month,
            part_num=part_num,
            canonical_key=canonical_key,
            source=source,
            deleted_at=deleted_at,
        )
        return hit[1] if hit is not None else None

    def locate_and_attach_media(
        self,
        *,
        chat_paths: ChatPaths,
        month: str,
        part_num: int | None,
        canonical_key: str,
        rel_media_path: str,
        sha256: str,
        size_bytes: int,
    ) -> int | None:
        """Set `media.downloaded`/`media.path` after a retroactive download.

        Returns the part that holds the message, or None if no part of the month has it.
        """

        hit = self._update_message(
            chat_paths=chat_paths,
            month=month,
            part_num=part_num,
            canonical_key=canonical_key,
            update=lambda msg: _with_downloaded_media(
                msg, rel_media_path=rel_media_path, sha256=sha256, size_bytes=size_bytes
            ),
        )
        return hit[0] if hit is not None else None

    def attach_downloaded_media_batch(
        self,
        *,
        chat_paths: ChatPaths,
        month: str,
        part_num: int | None,
        media_by_key: Mapping[str, Mapping[str, Any]],
    ) -> set[str]:
        """Attach several downloaded files at once (`{canonical_key: {path, sha256,
        size_bytes}}`): each touched part is rewritten once.

        `part_num` is where the messages are expected; keys not found there are looked up in
        the other parts of the month (stale locators). Returns the keys found and updated;
        a key missing from the result is in no part of that month.
        """
        month = month_from_iso(month)

        def _apply(path: Path, keys: set[str]) -> set[str]:
            payload = self._read(path)
            msgs = payload.get("messages") or []
            new_msgs = list(msgs)
            hit: set[str] = set()
            for idx, msg in enumerate(msgs):
                key = _ckey(msg)
                if key not in keys:
                    continue
                media = media_by_key[key]
                new_msgs[idx] = _with_downloaded_media(
                    msg,
                    rel_media_path=str(media["path"]),
                    sha256=str(media["sha256"]),
                    size_bytes=int(media["size_bytes"]),
                )
                hit.add(key)
            if hit:
                new_payload = dict(payload)
                new_payload["messages"] = new_msgs
                new_payload["updated_at"] = utc_now_iso()
                self._write(path, new_payload)
            return hit

        wanted = set(media_by_key)
        found: set[str] = set()
        hint = part_num if isinstance(part_num, int) and part_num >= 0 else None
        if hint is not None and wanted:
            hint_path = part_file_path(chat_paths, month, hint)
            if hint_path.exists():
                found |= _apply(hint_path, wanted)
        remaining = wanted - found
        if remaining:
            for pnum, path in list_month_part_files(chat_paths, month):
                if pnum != hint:
                    found |= _apply(path, remaining)  # every copy of a duplicated message
        return found

    def attach_downloaded_media(
        self,
        *,
        chat_paths: ChatPaths,
        month: str,
        part_num: int,
        canonical_key: str,
        rel_media_path: str,
        sha256: str,
        size_bytes: int,
    ) -> bool:
        """Update `media.downloaded = True` and `media.path` after retroactive media download.

        False means the message is in no part of that month (nothing was written).
        """
        return (
            self.locate_and_attach_media(
                chat_paths=chat_paths,
                month=month,
                part_num=part_num,
                canonical_key=canonical_key,
                rel_media_path=rel_media_path,
                sha256=sha256,
                size_bytes=size_bytes,
            )
            is not None
        )
