"""Generator, parser, and non-destructive merger for `<data_dir>/chats.md`."""

from __future__ import annotations

import contextlib
import os
import re
import threading
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from tghoard.config import MediaCategory
from tghoard.storage.atomic import atomic_write_text
from tghoard.storage.paths import single_line

try:  # POSIX
    import fcntl
except ImportError:  # pragma: no cover - Windows: thread lock only
    fcntl = None  # type: ignore[assignment]

ManifestSection = Literal["saved", "groups", "channels", "private", "bots"]

SECTION_HEADERS: tuple[tuple[ManifestSection, str], ...] = (
    ("saved", "Saved Messages"),
    ("groups", "Grupos e Supergrupos"),
    ("channels", "Canais"),
    ("private", "Conversas Privadas"),
    ("bots", "Bots"),
)

HEADER_TO_SECTION: dict[str, ManifestSection] = {
    title.lower(): key for key, title in SECTION_HEADERS
}

CHECKBOX_TOKEN_RE = re.compile(
    r"\[\s*([xX*]?)\s*\]\s*(baixar|photos?|videos?|media|audios?|files?|arquivos?)",
    re.IGNORECASE,
)
CHAT_KEY_TOKEN_RE = re.compile(r"\[`([a-z]+_[0-9_]+)`\]")
TITLE_TOKEN_RE = re.compile(r"\*\*(.+?)\*\*")
USERNAME_TOKEN_RE = re.compile(r"\(`@([A-Za-z0-9_]{1,64})`\)")
# Telegram usernames: letters, digits and underscores only (anything else is not written)
VALID_USERNAME_RE = re.compile(r"[A-Za-z0-9_]{1,64}")
ACCOUNTS_TOKEN_RE = re.compile(r"\*\(contas:\s*([^)]+)\)\*", re.IGNORECASE)
PRIORITY_TOKEN_RE = re.compile(r"\b(?:prioridade|priority|prio)\s*[:=]\s*(-?\d+)", re.IGNORECASE)

# Fields the web UI (or any programmatic editor) may change on an existing entry.
EDITABLE_FIELDS: dict[str, type] = {
    "baixar": bool,
    "photos": bool,
    "videos": bool,
    "audio": bool,
    "files": bool,
    "priority": int,
}
PRIORITY_MIN, PRIORITY_MAX = -99, 999

# Serializes read-modify-write of chats.md between the web UI thread and the CLI (in-process);
# `_manifest_lock` adds an flock so a standalone `tghoard web` and `tghoard start` don't race.
_MANIFEST_LOCK = threading.Lock()


@contextlib.contextmanager
def _manifest_lock(manifest_path: Path) -> Iterator[None]:
    with _MANIFEST_LOCK:
        if fcntl is None:
            yield
            return
        lock_path = manifest_path.with_name(f".{manifest_path.name}.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)  # releases the flock


def _normalize_flag_name(raw_name: str) -> str:
    lowered = raw_name.lower()
    if lowered == "baixar":
        return "baixar"
    if lowered in ("photo", "photos"):
        return "photos"
    if lowered in ("video", "videos", "media"):
        return "videos"
    if lowered in ("audio", "audios"):
        return "audio"
    if lowered in ("file", "files", "arquivo", "arquivos"):
        return "files"
    return lowered


def infer_section_from_chat_key(chat_key: str, *, is_bot: bool = False) -> ManifestSection:
    if chat_key.startswith("saved_"):
        return "saved"
    if chat_key.startswith(("group_", "supergroup_")):
        return "groups"
    if chat_key.startswith("channel_"):
        return "channels"
    if is_bot:
        return "bots"
    return "private"


@dataclass
class ChatSelection:
    """Represents one chat entry inside `chats.md`."""

    chat_key: str
    title: str
    section: ManifestSection = "private"
    username: str | None = None
    accounts: list[str] = field(default_factory=list)
    baixar: bool = True
    photos: bool = False
    videos: bool = False
    audio: bool = False
    files: bool = False
    # Higher syncs first (ties: alphabetical). Editable in chats.md or the web UI.
    priority: int = 0

    def allows_category(self, category: MediaCategory) -> bool:
        return bool(getattr(self, category))

    def enabled_categories(self) -> list[MediaCategory]:
        result: list[MediaCategory] = []
        for cat in ("photos", "videos", "audio", "files"):
            if getattr(self, cat):
                result.append(cat)  # type: ignore[arg-type]
        return result


def _fmt_box(checked: bool) -> str:
    return "[x]" if checked else "[ ]"


def format_chat_line(entry: ChatSelection) -> str:
    """Format a single chat line for `chats.md`."""
    boxes = (
        f"{_fmt_box(entry.baixar)} baixar | "
        f"{_fmt_box(entry.photos)} photos | "
        f"{_fmt_box(entry.videos)} videos | "
        f"{_fmt_box(entry.audio)} audio | "
        f"{_fmt_box(entry.files)} files | "
        f"prioridade: {entry.priority}"
    )
    # The title is the only free text on the line; the parser reads everything else from
    # outside the `**…**` span, so it must not be able to close that span or break the line.
    safe_title = single_line(entry.title).replace("**", "").strip() or entry.chat_key
    username = (entry.username or "").lstrip("@")
    user_part = f" (`@{username}`)" if VALID_USERNAME_RE.fullmatch(username) else ""
    accounts_sorted = sorted(set(entry.accounts))
    accounts_part = f" *(contas: {', '.join(accounts_sorted)})*" if accounts_sorted else ""
    return f"- {boxes} — **{safe_title}**{user_part} [`{entry.chat_key}`]{accounts_part}"


def render_chats_manifest(entries: Sequence[ChatSelection]) -> str:
    """Render full `chats.md` document.

    The header text, section titles and tokens are part of the file format (kept in
    Portuguese so existing files stay byte-compatible); don't translate them.
    """
    lines: list[str] = [
        "# Seleção de Chats — tghoard",
        "",
        "Edite as caixas abaixo antes de rodar `tghoard sync`:",
        "- `[x] baixar`: sincroniza mensagens (`messages/<pasta>/YYYY-MM-ptNNN.json`), "
        "`links/<pasta>.md` e `metadata/<pasta>/*.md`",
        "- `[ ] photos`: baixa fotos (`photos/`)",
        "- `[ ] videos`: baixa vídeos, GIFs e video notes (`videos/`)",
        "- `[ ] audio`: baixa mensagens de voz e áudios/músicas (`audio/`)",
        "- `[ ] files`: baixa qualquer outro arquivo que não seja photo, video nem audio "
        "(`files/`)",
        "- `prioridade: N`: ordem do `sync` (maior primeiro; padrão `0`). Também editável na "
        "web UI, inclusive durante um sync em andamento",
        "",
    ]

    grouped: dict[ManifestSection, list[ChatSelection]] = {key: [] for key, _ in SECTION_HEADERS}
    for entry in entries:
        grouped.setdefault(entry.section, []).append(entry)

    for section_key, section_title in SECTION_HEADERS:
        bucket = grouped.get(section_key, [])
        if not bucket:
            continue
        lines.append(f"## {section_title}")
        for item in sorted(bucket, key=lambda c: (-c.priority, c.title.lower(), c.chat_key)):
            lines.append(format_chat_line(item))
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_chats_manifest(content: str) -> dict[str, ChatSelection]:
    """Parse `chats.md` tolerating `[]`, `[ ]`, `[x]`, `[X]` and spacing variations."""
    parsed: dict[str, ChatSelection] = {}
    current_section: ManifestSection | None = None

    # Split on "\n" only: `splitlines()` would also break on U+2028 & co. inside a title
    for raw_line in content.split("\n"):
        line = raw_line.strip()
        if line.startswith("## "):
            heading = line[3:].strip().lower()
            current_section = HEADER_TO_SECTION.get(heading)
            continue

        # The title (`**…**`) is free text from Telegram: every token is read from the rest
        # of the line, so a chat named "x [`channel_1`] [ ] baixar" can't fake any of them.
        title_match = TITLE_TOKEN_RE.search(line)
        title = title_match.group(1).strip() if title_match else ""
        outside = (
            f"{line[: title_match.start()]} {line[title_match.end() :]}" if title_match else line
        )
        key_match = CHAT_KEY_TOKEN_RE.search(outside)
        if not key_match:
            continue

        chat_key = key_match.group(1)
        flags: dict[str, bool] = {
            "baixar": True,
            "photos": False,
            "videos": False,
            "audio": False,
            "files": False,
        }
        for mark, raw_flag in CHECKBOX_TOKEN_RE.findall(outside):
            norm_flag = _normalize_flag_name(raw_flag)
            if norm_flag in flags:
                flags[norm_flag] = bool(mark.strip())

        title = title or chat_key
        user_match = USERNAME_TOKEN_RE.search(outside)
        username = user_match.group(1) if user_match else None

        prio_match = PRIORITY_TOKEN_RE.search(outside)
        priority = int(prio_match.group(1)) if prio_match else 0

        acc_match = ACCOUNTS_TOKEN_RE.search(outside)
        accounts = (
            [a.strip() for a in acc_match.group(1).split(",") if a.strip()] if acc_match else []
        )

        section = current_section or infer_section_from_chat_key(chat_key)
        parsed[chat_key] = ChatSelection(
            chat_key=chat_key,
            title=title,
            section=section,
            username=username,
            accounts=accounts,
            baixar=flags["baixar"],
            photos=flags["photos"],
            videos=flags["videos"],
            audio=flags["audio"],
            files=flags["files"],
            priority=max(PRIORITY_MIN, min(PRIORITY_MAX, priority)),
        )

    return parsed


def load_chats_manifest(manifest_path: Path) -> dict[str, ChatSelection]:
    """Load and parse `chats.md` from disk."""
    if not manifest_path.exists():
        return {}
    return parse_chats_manifest(manifest_path.read_text(encoding="utf-8"))


def merge_and_save_chats_manifest(
    manifest_path: Path,
    discovered: Sequence[ChatSelection],
) -> tuple[list[ChatSelection], int, int]:
    """Merge newly discovered dialogs into `chats.md` without overwriting user edits.

    Returns `(merged_entries, added_count, updated_existing_count)`.
    """
    with _manifest_lock(manifest_path):
        return _merge_and_save_locked(manifest_path, discovered)


def _merge_and_save_locked(
    manifest_path: Path,
    discovered: Sequence[ChatSelection],
) -> tuple[list[ChatSelection], int, int]:
    existing = load_chats_manifest(manifest_path)
    merged: dict[str, ChatSelection] = dict(existing)
    added = 0
    updated = 0

    for item in discovered:
        if item.chat_key in merged:
            prev = merged[item.chat_key]
            combined_accounts = sorted(set(prev.accounts) | set(item.accounts))
            merged[item.chat_key] = ChatSelection(
                chat_key=prev.chat_key,
                title=item.title or prev.title,
                section=item.section or prev.section,
                username=item.username or prev.username,
                accounts=combined_accounts,
                # Preserve user's checkbox edits!
                baixar=prev.baixar,
                photos=prev.photos,
                videos=prev.videos,
                audio=prev.audio,
                files=prev.files,
                priority=prev.priority,
            )
            updated += 1
        else:
            # Brand-new chat defaults: [x] baixar, all media unchecked
            merged[item.chat_key] = ChatSelection(
                chat_key=item.chat_key,
                title=item.title,
                section=item.section,
                username=item.username,
                accounts=sorted(set(item.accounts)),
                baixar=item.baixar,
                photos=item.photos,
                videos=item.videos,
                audio=item.audio,
                files=item.files,
            )
            added += 1

    ordered = list(merged.values())
    atomic_write_text(manifest_path, render_chats_manifest(ordered))
    return ordered, added, updated


def update_chat_selections(
    manifest_path: Path,
    updates: Mapping[str, Mapping[str, Any]],
) -> dict[str, ChatSelection]:
    """Apply field edits (`{chat_key: {"photos": True, "priority": 5}}`) to `chats.md`.

    Only `EDITABLE_FIELDS` are accepted; unknown chat keys are ignored. The file is
    re-read under a lock and rewritten atomically, so concurrent manual edits made
    before the call are preserved. Returns the full, updated manifest.
    """
    with _manifest_lock(manifest_path):
        entries = load_chats_manifest(manifest_path)
        for chat_key, fields in updates.items():
            entry = entries.get(chat_key)
            if entry is None:
                continue
            for name, value in fields.items():
                kind = EDITABLE_FIELDS.get(name)
                if kind is None:
                    raise ValueError(f"field not editable: {name}")
                if kind is bool:
                    if not isinstance(value, bool):
                        raise ValueError(f"{name} must be a boolean")
                    setattr(entry, name, value)
                else:
                    if isinstance(value, bool) or not isinstance(value, int):
                        raise ValueError(f"{name} must be an integer")
                    setattr(entry, name, max(PRIORITY_MIN, min(PRIORITY_MAX, value)))
        atomic_write_text(manifest_path, render_chats_manifest(list(entries.values())))
        return entries


class ManifestSource:
    """Live view of `chats.md`: re-parses only when the file actually changed on disk.

    Lets a running `sync` pick up priorities/checkboxes edited in the web UI (or by hand).
    """

    def __init__(self, manifest_path: Path) -> None:
        self.path = manifest_path
        self._sig: tuple[int, int, int] | None = None
        self._entries: dict[str, ChatSelection] = {}

    def current(self) -> dict[str, ChatSelection]:
        try:
            st = self.path.stat()
        except FileNotFoundError:
            return {}
        sig = (st.st_ino, st.st_mtime_ns, st.st_size)  # atomic replace => new inode
        if sig != self._sig:
            self._entries = load_chats_manifest(self.path)
            self._sig = sig
        return self._entries
