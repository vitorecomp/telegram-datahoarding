"""Check the archive's media against its JSONs (`tghoard verify-media`), fully offline.

For every message with media in `messages/<folder>/*.json`:
- `downloaded: true`  -> the file at `media.path` must exist with the recorded size
                         (and, with `check_hash`, the recorded SHA-256).
- `downloaded: false` -> it must be in `pending_media` (queued to be downloaded when its
                         category is enabled); otherwise it was lost from the queue.

With `fix=True`:
- missing/corrupt files: JSON entry set to `downloaded: false` (`skip_reason`
  `missing_on_disk` / `corrupt_on_disk`), queued with `record_media_failure`, and their
  `media_index` entry removed (so dedup never links a corrupt copy). Corrupt files are left
  in place and simply overwritten by the next download.
- lost entries: re-queued. The next `tghoard sync` downloads them (for enabled categories).

Run it with `sync` stopped: it rewrites JSON files.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tghoard.storage.atomic import atomic_write_json
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow
from tghoard.storage.paths import ChatPaths, iter_all_part_files, iter_chat_paths


@dataclass
class MediaProblem:
    chat_key: str
    msg_id: int
    kind: str  # missing | size_mismatch | hash_mismatch | lost_from_queue
    path: str | None
    detail: str = ""


@dataclass
class VerifyReport:
    chats: int = 0
    media_checked: int = 0
    ok: int = 0
    not_downloaded: int = 0
    problems: list[MediaProblem] = field(default_factory=list)
    json_files_updated: int = 0
    requeued: int = 0

    def count(self, kind: str) -> int:
        return sum(1 for p in self.problems if p.kind == kind)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def _pending_row(
    chat_key: str,
    month: str,
    msg: dict[str, Any],
    media: dict[str, Any],
    fallback_account: int | None,
) -> PendingMediaRow | None:
    ids = {
        int(a): int(m) for a, m in (msg.get("account_msg_ids") or {}).items() if str(a).isdigit()
    }
    if ids:
        account_id, msg_id = next(iter(ids.items()))
    elif fallback_account is not None and isinstance(msg.get("id"), int):
        account_id, msg_id = fallback_account, int(msg["id"])
    else:
        return None
    if media.get("tg_id") is None or not media.get("category"):
        return None
    return PendingMediaRow(
        chat_key=chat_key,
        canonical_key=str(msg.get("canonical_key") or f"id:{msg.get('id')}"),
        account_id=account_id,
        msg_id=msg_id,
        day_str=month,
        category=media["category"],
        tg_type=str(media.get("tg_type") or "document"),
        tg_id=int(media["tg_id"]),
        file_name=media.get("file_name"),
        mime_type=media.get("mime_type"),
        size_bytes=media.get("size_bytes"),
    )


def verify_media(
    data_dir: Path,
    db: CheckpointDB | None,
    *,
    fix: bool = False,
    check_hash: bool = False,
    chat_filter: str | None = None,
) -> VerifyReport:
    report = VerifyReport()
    fallback_account = None
    queued: set[tuple[str, str]] = set()
    if db is not None:
        accounts = db.list_accounts()
        fallback_account = accounts[0].account_id if accounts else None
        queued = {
            (str(r["chat_key"]), str(r["canonical_key"]))
            for r in db._conn.execute(
                "SELECT chat_key, canonical_key FROM pending_media WHERE downloaded = 0"
            )
        }

    for chat_key, chat in iter_chat_paths(data_dir):
        if chat_filter and chat_filter not in (chat_key, chat.folder):
            continue
        report.chats += 1
        for part in iter_all_part_files(chat):
            payload = json.loads(part.read_text(encoding="utf-8"))
            month = str(payload.get("month") or part.name[:7])
            changed = False
            for msg in payload.get("messages") or []:
                media = msg.get("media")
                if not isinstance(media, dict) or media.get("tg_id") is None:
                    continue
                report.media_checked += 1
                ckey = str(msg.get("canonical_key") or f"id:{msg.get('id')}")
                msg_id = int(msg.get("id") or 0)

                if not media.get("downloaded"):
                    report.not_downloaded += 1
                    if (chat_key, ckey) not in queued:
                        report.problems.append(
                            MediaProblem(
                                chat_key,
                                msg_id,
                                "lost_from_queue",
                                None,
                                str(media.get("skip_reason") or ""),
                            )
                        )
                        row = _pending_row(chat_key, month, msg, media, fallback_account)
                        if fix and db is not None and row is not None:
                            db.enqueue_pending_media(row)
                            queued.add((chat_key, ckey))
                            report.requeued += 1
                    continue

                rel = str(media.get("path") or "")
                problem = _check_file(chat, rel, media, check_hash)
                if problem is None:
                    report.ok += 1
                    continue
                kind, detail = problem
                report.problems.append(MediaProblem(chat_key, msg_id, kind, rel, detail))
                if not fix:
                    continue
                media["downloaded"] = False
                media["skip_reason"] = "missing_on_disk" if kind == "missing" else "corrupt_on_disk"
                changed = True
                if db is not None:
                    db._conn.execute("DELETE FROM media_index WHERE rel_path = ?", (rel,))
                    row = _pending_row(chat_key, month, msg, media, fallback_account)
                    if row is not None:
                        db.record_media_failure(row, f"{media['skip_reason']}: {detail}")
                        queued.add((chat_key, ckey))
                        report.requeued += 1
            if changed:
                atomic_write_json(part, payload)
                report.json_files_updated += 1
    return report


def _check_file(
    chat: ChatPaths, rel: str, media: dict[str, Any], check_hash: bool
) -> tuple[str, str] | None:
    if not rel:
        return ("missing", "no path in the JSON")
    path = chat.abs_path(rel)
    if not path.is_file():
        return ("missing", "file does not exist")
    size = path.stat().st_size
    expected = media.get("size_bytes")
    if expected is not None and int(expected) != size:
        return ("size_mismatch", f"{size} B on disk, {expected} B expected")
    if check_hash and media.get("sha256") and len(str(media["sha256"])) == 64:
        actual = _sha256(path)
        if actual != media["sha256"]:
            return ("hash_mismatch", "SHA-256 differs from the recorded one")
    return None
