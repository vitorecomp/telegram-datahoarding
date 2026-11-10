"""0001 — per-chat folders + daily JSON  ->  category-first layout + monthly JSON.

Before (legacy):
    <data>/<chat_key>__<slug>/chats/YYYY-MM-DD-ptNNN.json
    <data>/<chat_key>__<slug>/{photos,videos,audio,files}/...
    <data>/<chat_key>__<slug>/links.md
    <data>/<chat_key>__<slug>/metadata/*.md

After:
    <data>/messages/<kebab-title>-<id>/YYYY-MM-ptNNN.json
    <data>/{photos,videos,audio,files}/<kebab-title>-<id>/...
    <data>/links/<kebab-title>-<id>.md
    <data>/metadata/<kebab-title>-<id>/*.md

Media files are *moved* (rename: no copy, hardlinks preserved). Day files are merged into
month parts (re-split by `max_json_kb`), media paths inside JSON are rewritten, and
`checkpoint.db` is updated in place (backed up first) so cursors/ranges are kept.

Crash safety — every step is idempotent and the order makes each crash point resumable:
1. `metadata/info.md` (the chat identity) moves first, so a re-run resolves the same folder.
2. Metadata, `links.md` and media are moved (never overwriting a different file). The rare
   rename on a name clash is journaled in `.tghoard/migration-0001.json` *before* the move.
3. Day files are merged into month parts (merging is idempotent: re-feeding is a no-op).
4. `checkpoint.db` is updated in one transaction, derived only from what is on disk (month
   parts are scanned for part numbers; every `media_index` row still under the old folder
   is pointed at the moved file), so it doesn't matter what an interrupted run did.
5. Only then are the day files deleted, and the old folder removed where empty.
Interrupted temp files (`.dl_*` partial downloads, `.<name>.<rand>.tmp` atomic writes) are
discarded. Anything else that can't be migrated is left in place, named in the output and
recorded in the journal, so `is_needed` turns False instead of asking for endless re-runs.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.markup import escape

from tghoard.config import AppConfig
from tghoard.migrations import Migration, MigrationPlan, backup_checkpoint_db
from tghoard.storage.atomic import atomic_write_json, atomic_write_text
from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.chats_manifest import load_chats_manifest
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.markdown_writer import ensure_initial_metadata_files
from tghoard.storage.paths import (
    MEDIA_ROOTS,
    ChatPaths,
    index_chat_folders,
    iter_all_part_files,
    make_folder_name,
    month_from_iso,
    parse_part_filename,
    read_chat_key_from_metadata,
    single_line,
)

LEGACY_PART_RE_DAY = 10  # "YYYY-MM-DD" prefix length of legacy part files
# Temp file of the atomic writer (`.<target>.<random>.tmp`), left behind by a crash.
ATOMIC_TEMP_RE = re.compile(r"^\..+\.tmp$")
JOURNAL_NAME = "migration-0001.json"
MAX_LISTED_LEFTOVERS = 20


@dataclass
class LegacyChat:
    old_dir: Path
    chat_key: str
    title: str
    folder: str  # target folder name in the new layout
    json_files: list[Path] = field(default_factory=list)
    media_counts: dict[str, int] = field(default_factory=dict)
    media_bytes: int = 0


# ---------------------------------------------------------------------------
# Journal (`.tghoard/migration-0001.json`): name-clash renames + accepted leftovers
# ---------------------------------------------------------------------------
def _journal_path(cfg: AppConfig) -> Path:
    return cfg.internal_dir / JOURNAL_NAME


def _load_journal(cfg: AppConfig) -> dict[str, dict[str, Any]]:
    """`{"renames": {"<old_dir>/<cat>/<name>": "<new name>"}, "leftovers": {...}}`."""
    try:
        raw = json.loads(_journal_path(cfg).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raw = {}
    if not isinstance(raw, dict):
        raw = {}
    out: dict[str, dict[str, Any]] = {}
    for key in ("renames", "leftovers"):
        value = raw.get(key)
        out[key] = dict(value) if isinstance(value, dict) else {}
    return out


def _save_journal(cfg: AppConfig, journal: dict[str, dict[str, Any]]) -> None:
    if not journal["renames"] and not journal["leftovers"]:
        _journal_path(cfg).unlink(missing_ok=True)
        return
    atomic_write_json(_journal_path(cfg), journal)


# ---------------------------------------------------------------------------
# Detection, scan and plan (read-only)
# ---------------------------------------------------------------------------
def _legacy_dirs(data_dir: Path) -> list[Path]:
    if not data_dir.is_dir():
        return []
    return sorted(
        child
        for child in data_dir.iterdir()
        if child.is_dir() and "__" in child.name and (child / "chats").is_dir()
    )


def _entries(old_dir: Path) -> list[Path]:
    """Every non-directory entry under `old_dir` (files, symlinks, sockets...)."""
    return [p for p in old_dir.rglob("*") if p.is_symlink() or not p.is_dir()]


def _is_needed(cfg: AppConfig) -> bool:
    """Needed while a legacy folder holds anything besides already-reported leftovers.

    A legacy folder is "done" only once a run processed it to the end (database included)
    and recorded the files it could not migrate; until then it always needs a (re-)run,
    whatever an interrupted run left behind.
    """
    dirs = _legacy_dirs(cfg.data_dir)
    if not dirs:
        return False
    accepted = _load_journal(cfg)["leftovers"]
    for old in dirs:
        if old.name not in accepted:
            return True
        remaining = {p.relative_to(old).as_posix() for p in _entries(old)}
        if not remaining or not remaining <= set(accepted[old.name]):
            return True
    return False


def _is_temp_leftover(path: Path, *, media: bool = False) -> bool:
    """Interrupted temp file of ours, never referenced anywhere: safe to discard."""
    if ATOMIC_TEMP_RE.match(path.name):
        return True
    return media and path.name.startswith(".dl_")


def _daily_json_files(old_dir: Path) -> list[Path]:
    return sorted(
        p for p in (old_dir / "chats").glob("*.json") if p.is_file() and not p.name.startswith(".")
    )


def _title_for(old_dir: Path, chat_key: str, manifest_titles: dict[str, str]) -> str:
    if chat_key in manifest_titles:
        return manifest_titles[chat_key]
    for part in _daily_json_files(old_dir):
        try:
            payload = json.loads(part.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        title = payload.get("chat_title") if isinstance(payload, dict) else None
        if title:
            return str(title)
    return old_dir.name.split("__", 1)[1] if "__" in old_dir.name else chat_key


def _scan(cfg: AppConfig) -> list[LegacyChat]:
    data_dir = cfg.data_dir
    manifest_titles = {k: v.title for k, v in load_chats_manifest(cfg.chats_manifest_path).items()}
    existing = index_chat_folders(data_dir, refresh=True)  # chats already in new layout
    taken = set(existing.values())
    chats: list[LegacyChat] = []
    for old_dir in _legacy_dirs(data_dir):
        chat_key = old_dir.name.split("__", 1)[0]
        title = _title_for(old_dir, chat_key, manifest_titles)
        folder = existing.get(chat_key)
        if folder is None:
            base = make_folder_name(chat_key, title)
            folder, n = base, 2
            while folder in taken:
                folder, n = f"{base}-{n}", n + 1
            taken.add(folder)
        lc = LegacyChat(old_dir=old_dir, chat_key=chat_key, title=title, folder=folder)
        lc.json_files = _daily_json_files(old_dir)
        for cat in MEDIA_ROOTS:
            files = [
                f for f in (old_dir / cat).glob("*") if f.is_file() and not f.name.startswith(".")
            ]
            lc.media_counts[cat] = len(files)
            lc.media_bytes += sum(f.stat().st_size for f in files)
        chats.append(lc)
    return chats


def _plan(cfg: AppConfig) -> MigrationPlan:
    chats = _scan(cfg)
    rows: list[tuple[str, ...]] = []
    for lc in chats:
        months = sorted({f.name[:7] for f in lc.json_files})
        rows.append(
            (
                lc.old_dir.name,
                lc.folder,
                f"{len(lc.json_files)} → ~{len(months)} month(s)",
                " ".join(f"{c[0]}:{lc.media_counts[c]}" for c in MEDIA_ROOTS),
                f"{lc.media_bytes / 1024 / 1024:.1f} MB",
            )
        )
    total_media = sum(sum(lc.media_counts.values()) for lc in chats)
    return MigrationPlan(
        summary=(
            f"{len(chats)} chat folder(s) in the old layout; {total_media} media file(s) "
            "will be MOVED (rename, no copy) and the daily JSONs grouped by month."
        ),
        steps=[
            "Back up .tghoard/checkpoint.db to .tghoard/backups/",
            "metadata/*.md  → metadata/<folder>/",
            "links.md       → links/<folder>.md",
            "photos|videos|audio|files/* → <category>/<folder>/ (paths in the JSON rewritten)",
            "chats/YYYY-MM-DD-ptNNN.json → messages/<folder>/YYYY-MM-ptNNN.json",
            "checkpoint.db updated in place (cursors and ranges preserved)",
            "Temp files of interrupted writes (.dl_*, .*.tmp) are discarded",
            "Old folders removed only if they end up empty (unknown files are kept and listed)",
            "Safe to interrupt: running it again resumes where it stopped",
        ],
        table_headers=("Old folder", "New folder", "JSONs", "Media (p/v/a/f)", "Size"),
        table_rows=rows,
    )


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------
def _move_file(
    src: Path, dest_dir: Path, before_rename: Callable[[str], None] | None = None
) -> Path:
    """Rename `src` into `dest_dir`, never overwriting a different file.

    On a name clash with a *different* file the source gets a `-2`, `-3`... suffix;
    `before_rename(new_name)` runs before the move so the rename can be journaled.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    if dest.exists() or dest.is_symlink():
        try:
            same = os.path.samefile(src, dest)
        except OSError:
            same = False
        if same:
            src.unlink()  # already there (hardlink of the same inode)
            return dest
        stem, suffix, n = dest.stem, dest.suffix, 2
        while dest.exists() or dest.is_symlink():
            dest = dest_dir / f"{stem}-{n}{suffix}"
            n += 1
        if before_rename is not None:
            before_rename(dest.name)
    os.replace(src, dest)
    return dest


def _ensure_identity(chat: ChatPaths, lc: LegacyChat) -> None:
    """Make `metadata/<folder>/info.md` start with `` (`<chat_key>`) `` (folder identity)."""
    if read_chat_key_from_metadata(chat.metadata_dir) == lc.chat_key:
        return
    info = chat.meta("info.md")
    header = f"# Informações — {single_line(lc.title)} (`{lc.chat_key}`)\n"
    body = info.read_text(encoding="utf-8") if info.is_file() else "\n_Sincronização pendente._\n"
    if body and not body.startswith("\n"):
        body = "\n" + body
    atomic_write_text(info, header + body)


def _move_metadata(old: Path, chat: ChatPaths, lc: LegacyChat) -> None:
    src_dir = old / "metadata"
    chat.metadata_dir.mkdir(parents=True, exist_ok=True)
    # info.md first: once it is in place, a re-run resolves this same folder by identity
    legacy_info = src_dir / "info.md"
    if legacy_info.is_file() and not chat.meta("info.md").exists():
        os.replace(legacy_info, chat.meta("info.md"))
    _ensure_identity(chat, lc)
    if not src_dir.is_dir():
        return
    for f in sorted(src_dir.iterdir()):
        if not f.is_file():
            continue  # unknown sub-folder: left in place and reported
        if _is_temp_leftover(f):
            f.unlink(missing_ok=True)  # interrupted atomic write, never the real file
        else:
            _move_file(f, chat.metadata_dir)


def _move_links(old: Path, chat: ChatPaths) -> None:
    for f in old.glob(".*"):
        if f.is_file() and _is_temp_leftover(f):
            f.unlink(missing_ok=True)
    if (old / "links.md").is_file():
        chat.links_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(old / "links.md", chat.links_path)


def _move_media(
    cfg: AppConfig, old: Path, chat: ChatPaths, journal: dict[str, dict[str, Any]]
) -> dict[str, str]:
    """Move media into `<category>/<folder>/`; return `{"<cat>/<name>": new_name}` renames."""
    renames = journal["renames"]
    for cat in MEDIA_ROOTS:
        src_dir = old / cat
        if not src_dir.is_dir():
            continue
        for f in sorted(src_dir.iterdir()):
            if f.is_dir() and not f.is_symlink():
                continue  # unknown sub-folder: left in place and reported
            if _is_temp_leftover(f, media=True):
                f.unlink(missing_ok=True)  # interrupted partial download, never referenced
                continue
            key = f"{old.name}/{cat}/{f.name}"

            def journal_rename(new_name: str, key: str = key) -> None:
                renames[key] = new_name
                _save_journal(cfg, journal)

            dest = _move_file(f, chat.media_dir(cat), before_rename=journal_rename)
            if dest.name == f.name and renames.pop(key, None) is not None:
                _save_journal(cfg, journal)  # stale entry of a rename that never happened
    prefix = f"{old.name}/"
    return {k[len(prefix) :]: v for k, v in renames.items() if k.startswith(prefix)}


def _rewrite_media_path(msg: dict[str, Any], chat: ChatPaths, renamed: dict[str, str]) -> None:
    media = msg.get("media")
    if not isinstance(media, dict) or not media.get("path"):
        return
    old = str(media["path"])
    parts = old.split("/")
    if len(parts) == 3 and parts[0] in MEDIA_ROOTS and parts[1] == chat.folder:
        return  # already new-style (resumed run)
    if len(parts) == 2 and parts[0] in MEDIA_ROOTS:
        media["path"] = chat.media_rel(parts[0], renamed.get(old, parts[1]))


def _read_daily(path: Path) -> tuple[list[dict[str, Any]] | None, str]:
    """`(messages, "")` for a valid legacy day file, `(None, reason)` otherwise."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return None, f"unreadable JSON: {exc}"
    msgs = payload.get("messages") if isinstance(payload, dict) else None
    if not isinstance(payload, dict) or not isinstance(msgs, list | None):
        return None, "not a tghoard message file"
    msgs = msgs or []
    if not all(isinstance(m, dict) and m.get("canonical_key") for m in msgs):
        return None, "messages without `canonical_key`"
    return msgs, ""


def _merge_daily_json(
    lc: LegacyChat,
    chat: ChatPaths,
    writer: ChatPartWriter,
    renamed: dict[str, str],
    problems: dict[Path, str],
) -> list[Path]:
    """Merge day files into month parts (streamed month by month). Returns the merged files."""
    merged: list[Path] = []
    current_month: str | None = None
    buffer: list[dict[str, Any]] = []

    def flush() -> None:
        if current_month is None or not buffer:
            return
        writer.upsert_month_messages(
            chat_paths=chat,
            chat_key=lc.chat_key,
            chat_title=lc.title,
            month=current_month,
            incoming_messages=buffer,
        )
        buffer.clear()

    for part in _daily_json_files(lc.old_dir):
        msgs, reason = _read_daily(part)
        if msgs is None:
            problems[part] = reason  # left in place, reported
            continue
        for msg in msgs:
            _rewrite_media_path(msg, chat, renamed)
            month = month_from_iso(str(msg.get("date") or "") or part.name[:LEGACY_PART_RE_DAY])
            if month != current_month:
                flush()
                current_month = month
            buffer.append(msg)
        merged.append(part)
    flush()
    return merged


def _update_checkpoint(
    db: CheckpointDB, lc: LegacyChat, chat: ChatPaths, renamed: dict[str, str]
) -> None:
    """Point `checkpoint.db` at the new layout (one transaction, derived from the disk only)."""
    conn = db._conn
    ck = lc.chat_key
    old_prefix = f"{lc.old_dir.name}/"
    conn.execute("BEGIN")
    try:
        # Fallback for locators whose message isn't in any part: at least the month is right
        conn.execute(
            "UPDATE message_locator SET day_str = substr(day_str, 1, 7) "
            "WHERE chat_key = ? AND length(day_str) = 10",
            (ck,),
        )
        # Exact month + part of every archived message, read from the written month parts
        for part in iter_all_part_files(chat):
            parsed = parse_part_filename(part.name)
            if parsed is None:
                continue
            month, part_num = parsed
            payload = json.loads(part.read_text(encoding="utf-8"))
            conn.executemany(
                "UPDATE message_locator SET day_str = ?, part_num = ? "
                "WHERE chat_key = ? AND canonical_key = ?",
                [
                    (month, part_num, ck, str(m["canonical_key"]))
                    for m in payload.get("messages") or []
                    if isinstance(m, dict) and m.get("canonical_key")
                ],
            )
        conn.execute(
            "UPDATE pending_media SET day_str = substr(day_str, 1, 7) "
            "WHERE chat_key = ? AND length(day_str) = 10",
            (ck,),
        )
        # Every media_index row still under the old folder -> the moved file (whichever
        # run moved it), so nothing already on disk is downloaded again.
        updates: list[tuple[str, str, int]] = []
        for row in conn.execute(
            "SELECT tg_type, tg_id, rel_path FROM media_index WHERE substr(rel_path, 1, ?) = ?",
            (len(old_prefix), old_prefix),
        ).fetchall():
            rest = str(row["rel_path"])[len(old_prefix) :].split("/")
            if len(rest) != 2 or rest[0] not in MEDIA_ROOTS:
                continue
            cat, name = rest
            new_rel = chat.media_rel(cat, renamed.get(f"{cat}/{name}", name))
            if chat.abs_path(new_rel).is_file():
                updates.append((new_rel, row["tg_type"], row["tg_id"]))
        conn.executemany(
            "UPDATE media_index SET rel_path = ? WHERE tg_type = ? AND tg_id = ?", updates
        )
        conn.execute(
            "UPDATE chat_account_cursors SET folder_rel_path = ? WHERE chat_key = ?",
            (lc.folder, ck),
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _prune_old_dir(old: Path) -> None:
    """Remove empty directories only (deepest first, `chats/` last, then the folder)."""
    dirs = [p for p in old.rglob("*") if p.is_dir() and not p.is_symlink()]
    # `chats/` is what marks a legacy folder: drop it last so a crash here never hides a
    # folder that still has other content to clean up.
    dirs.sort(key=lambda p: (-len(p.parts), p == old / "chats"))
    for sub in [*dirs, old]:
        try:
            sub.rmdir()
        except OSError:
            pass


def _migrate_chat(
    cfg: AppConfig,
    lc: LegacyChat,
    writer: ChatPartWriter,
    db: CheckpointDB | None,
    journal: dict[str, dict[str, Any]] | None = None,
) -> list[tuple[str, str]]:
    """Migrate one legacy folder. Returns `[(path_rel_to_data_dir, reason), ...]` left behind."""
    data_dir = cfg.data_dir
    journal = journal if journal is not None else _load_journal(cfg)
    chat = ChatPaths(data_dir=data_dir, folder=lc.folder)
    old = lc.old_dir
    problems: dict[Path, str] = {}

    # 1. metadata (info.md first: it carries the identity used to resolve the folder)
    _move_metadata(old, chat, lc)
    # 2. links.md
    _move_links(old, chat)
    chat.ensure()
    ensure_initial_metadata_files(chat, chat_key=lc.chat_key, chat_title=lc.title)
    # 3. media (rename; clashes journaled to rewrite the JSON paths on any later run)
    renamed = _move_media(cfg, old, chat, journal)
    # 4. daily JSON -> monthly parts (day files are kept until the database is updated)
    merged = _merge_daily_json(lc, chat, writer, renamed, problems)
    # 5. checkpoint.db in place (cursors and synced ranges untouched)
    if db is not None:
        _update_checkpoint(db, lc, chat, renamed)
    # 6. day files, now fully represented in the month parts and the database (plus temp
    #    files of interrupted writes of day files: the real file was never replaced)
    for part in merged:
        part.unlink(missing_ok=True)
    for f in (old / "chats").glob(".*"):
        if f.is_file() and _is_temp_leftover(f):
            f.unlink(missing_ok=True)
    # 7. remove the old folder tree only where empty; report anything left behind
    _prune_old_dir(old)
    if not old.exists():
        return []
    return [
        (p.relative_to(data_dir).as_posix(), problems.get(p, "unknown file"))
        for p in sorted(_entries(old))
    ]


def _apply(cfg: AppConfig, console: Console) -> None:
    chats = _scan(cfg)
    if not chats:
        console.print("  nothing to migrate.")
        return
    backup = backup_checkpoint_db(cfg, "0001")
    if backup is not None:
        console.print(f"  💾 database backup: {backup.relative_to(cfg.data_dir)}")
    writer = ChatPartWriter(max_json_bytes=cfg.max_json_bytes)
    db = CheckpointDB(cfg.checkpoint_db_path) if cfg.checkpoint_db_path.exists() else None
    journal = _load_journal(cfg)
    leftovers: list[tuple[str, str]] = []
    try:
        for i, lc in enumerate(chats, 1):
            console.print(f"  [{i}/{len(chats)}] {lc.old_dir.name} → [bold]{lc.folder}[/bold]")
            left = _migrate_chat(cfg, lc, writer, db, journal)
            leftovers += left
            # The chat is done (database included): forget its renames, and accept what
            # couldn't be migrated so `is_needed` stops asking for a re-run.
            prefix = f"{lc.old_dir.name}/"
            for key in [k for k in journal["renames"] if k.startswith(prefix)]:
                del journal["renames"][key]
            if left:
                journal["leftovers"][lc.old_dir.name] = sorted(
                    rel[len(prefix) :] for rel, _ in left
                )
            else:
                journal["leftovers"].pop(lc.old_dir.name, None)
            _save_journal(cfg, journal)
    finally:
        if db is not None:
            db.close()
    index_chat_folders(cfg.data_dir, refresh=True)
    if leftovers:
        console.print(
            f"  [yellow]⚠ {len(leftovers)} file(s) could not be migrated and were left in "
            "the old folder(s):[/yellow]"
        )
        for rel, reason in leftovers[:MAX_LISTED_LEFTOVERS]:
            console.print(f"    • {escape(rel)} [dim]({escape(reason)})[/dim]")
        if len(leftovers) > MAX_LISTED_LEFTOVERS:
            console.print(f"    … and {len(leftovers) - MAX_LISTED_LEFTOVERS} more")
        console.print(
            "  [yellow]Everything else was migrated. tghoard does not know how to migrate "
            "these files, so running `tghoard migrate --apply` again will not change them: "
            "review them and move or delete them yourself (their old folders are kept "
            "only because of them).[/yellow]"
        )


MIGRATION = Migration(
    id="0001",
    name="category_first_layout",
    description=(
        "Category-first layout (messages/, photos/, videos/, audio/, files/, links/, metadata/) "
        "with `kebab-title-<id>` folders and JSON grouped by month (YYYY-MM-ptNNN.json)"
    ),
    is_needed=_is_needed,
    plan=_plan,
    apply=_apply,
)
