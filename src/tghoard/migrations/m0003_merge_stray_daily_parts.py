"""0003 — merge daily `YYYY-MM-DD-ptNNN.json` files written by older `watch`/admin-log code.

Before this fix, `tghoard watch` (live messages) and the Admin Log recovery of deleted
messages passed a full date where a month was expected, so they wrote
`messages/<folder>/YYYY-MM-DD-ptNNN.json`. No reader looks at that name (invariant 3 is
`YYYY-MM-ptNNN.json`), so those messages were invisible to search, stats, rebuild and the
web UI. This migration merges each such file into its month's parts (by `canonical_key`,
so nothing is duplicated and nothing is lost), points the checkpoint rows that still say
`YYYY-MM-DD` at the month, and only then deletes the daily file.

Resumable: merging is idempotent, and a daily file is removed only after its messages are
in the monthly parts and the database was updated. Required, but only ever needed on
archives that actually contain such files.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from rich.console import Console
from rich.markup import escape

from tghoard.config import AppConfig
from tghoard.migrations import Migration, MigrationPlan
from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.paths import ChatPaths, read_chat_key_from_metadata

STRAY_DAILY_RE = re.compile(r"^(\d{4}-\d{2})-(\d{2})-pt\d{3}\.json$")


def _stray_files(data_dir: Path) -> list[tuple[str, Path]]:
    """`(folder, path)` of every daily part under `messages/<folder>/`."""
    found: list[tuple[str, Path]] = []
    root = data_dir / "messages"
    if not root.is_dir():
        return found
    for folder in sorted(p for p in root.iterdir() if p.is_dir()):
        for f in sorted(folder.iterdir()):
            if f.is_file() and STRAY_DAILY_RE.match(f.name):
                found.append((folder.name, f))
    return found


def _is_needed(cfg: AppConfig) -> bool:
    return bool(_stray_files(cfg.data_dir))


def _plan(cfg: AppConfig) -> MigrationPlan:
    by_folder: dict[str, list[Path]] = {}
    for folder, path in _stray_files(cfg.data_dir):
        by_folder.setdefault(folder, []).append(path)
    total = sum(len(v) for v in by_folder.values())
    return MigrationPlan(
        summary=(
            f"{total} daily JSON file(s) in {len(by_folder)} chat folder(s) will be merged "
            "into their monthly parts."
        ),
        steps=[
            "Merge each messages/<folder>/YYYY-MM-DD-ptNNN.json into YYYY-MM-ptNNN.json "
            "(by canonical_key: no duplicates, revisions and deletion flags kept)",
            "Point checkpoint rows (message locator, pending media) at the month",
            "Delete each daily file only after its messages are in the monthly parts",
        ],
        table_headers=("Folder", "Daily files"),
        table_rows=[(folder, str(len(paths))) for folder, paths in sorted(by_folder.items())],
    )


def _apply(cfg: AppConfig, console: Console) -> None:
    writer = ChatPartWriter(max_json_bytes=cfg.max_json_bytes)
    db = CheckpointDB(cfg.checkpoint_db_path) if cfg.checkpoint_db_path.exists() else None
    merged_files = merged_msgs = 0
    skipped: list[str] = []
    try:
        for folder, path in _stray_files(cfg.data_dir):
            chat = ChatPaths(data_dir=cfg.data_dir, folder=folder)
            chat_key = read_chat_key_from_metadata(chat.metadata_dir)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                messages = [m for m in payload.get("messages") or [] if isinstance(m, dict)]
            except (OSError, ValueError, AttributeError) as exc:
                skipped.append(_set_aside(cfg, path, f"unreadable: {exc}"))
                continue
            if chat_key is None:
                chat_key = str(payload.get("chat_key") or "")
            if not chat_key:
                skipped.append(_set_aside(cfg, path, "unknown chat"))
                continue
            m = STRAY_DAILY_RE.match(path.name)
            assert m is not None
            month, day = m.group(1), f"{m.group(1)}-{m.group(2)}"
            part_map: dict[str, int] = {}
            if messages:
                part_map, _ = writer.upsert_month_messages(
                    chat_paths=chat,
                    chat_key=chat_key,
                    chat_title=str(payload.get("chat_title") or chat_key),
                    month=month,
                    incoming_messages=messages,
                )
            if db is not None:
                _repoint_rows(db, chat_key, day, month, part_map)
            path.unlink()  # its messages are now in the monthly parts
            merged_files += 1
            merged_msgs += len(messages)
    finally:
        if db is not None:
            db.close()
    console.print(
        f"  🗂 {merged_files} daily file(s) merged into monthly parts ({merged_msgs} message(s))"
    )
    for item in skipped:
        console.print(f"  [yellow]⚠ not merged, kept for you to inspect: {escape(item)}[/yellow]")


def _set_aside(cfg: AppConfig, path: Path, reason: str) -> str:
    """Rename (never delete) a file we can't merge, so the migration can still finish."""
    target = path.with_name(path.name + ".unmerged")
    try:
        path.rename(target)
    except OSError:
        target = path
    return f"{target.relative_to(cfg.data_dir)} ({reason})"


def _repoint_rows(
    db: CheckpointDB, chat_key: str, day: str, month: str, part_map: dict[str, int]
) -> None:
    conn = db._conn
    conn.execute("BEGIN")
    try:
        conn.executemany(
            "UPDATE message_locator SET day_str = ?, part_num = ? "
            "WHERE chat_key = ? AND canonical_key = ?",
            [(month, part, chat_key, ckey) for ckey, part in part_map.items()],
        )
        # Rows recorded with the full date that weren't in this file's part map
        conn.execute(
            "UPDATE message_locator SET day_str = ? WHERE chat_key = ? AND day_str = ?",
            (month, chat_key, day),
        )
        conn.execute(
            "UPDATE pending_media SET day_str = ? WHERE chat_key = ? AND day_str = ?",
            (month, chat_key, day),
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


MIGRATION = Migration(
    id="0003",
    name="merge_stray_daily_parts",
    description="Merge daily messages/<folder>/YYYY-MM-DD-ptNNN.json files (old `watch` bug)",
    is_needed=_is_needed,
    plan=_plan,
    apply=_apply,
    required=True,
)
