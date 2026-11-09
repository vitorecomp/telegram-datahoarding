"""Generator, parser, and non-destructive merger for `<data_dir>/chats.md`."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from tghoard.config import MediaCategory
from tghoard.storage.atomic import atomic_write_text

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
USERNAME_TOKEN_RE = re.compile(r"\(`@([^`]+)`\)")
ACCOUNTS_TOKEN_RE = re.compile(r"\*\(contas:\s*([^)]+)\)\*", re.IGNORECASE)


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
        f"{_fmt_box(entry.files)} files"
    )
    safe_title = entry.title.replace("\n", " ").replace("**", "").strip() or entry.chat_key
    user_part = f" (`@{entry.username.lstrip('@')}`)" if entry.username else ""
    accounts_sorted = sorted(set(entry.accounts))
    accounts_part = f" *(contas: {', '.join(accounts_sorted)})*" if accounts_sorted else ""
    return f"- {boxes} — **{safe_title}**{user_part} [`{entry.chat_key}`]{accounts_part}"


def render_chats_manifest(entries: Sequence[ChatSelection]) -> str:
    """Render full `chats.md` document."""
    lines: list[str] = [
        "# Seleção de Chats — tghoard",
        "",
        "Edite as caixas abaixo antes de rodar `tghoard sync`:",
        "- `[x] baixar`: sincroniza mensagens (`chats/YYYY-MM-DD-pt000.json`), "
        "`links.md` e `metadata/*.md`",
        "- `[ ] photos`: baixa fotos (`photos/`)",
        "- `[ ] videos`: baixa vídeos, GIFs e video notes (`videos/`)",
        "- `[ ] audio`: baixa mensagens de voz e áudios/músicas (`audio/`)",
        "- `[ ] files`: baixa qualquer outro arquivo que não seja photo, video nem audio "
        "(`files/`)",
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
        for item in sorted(bucket, key=lambda c: (c.title.lower(), c.chat_key)):
            lines.append(format_chat_line(item))
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_chats_manifest(content: str) -> dict[str, ChatSelection]:
    """Parse `chats.md` tolerating `[]`, `[ ]`, `[x]`, `[X]` and spacing variations."""
    parsed: dict[str, ChatSelection] = {}
    current_section: ManifestSection | None = None

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            heading = line[3:].strip().lower()
            current_section = HEADER_TO_SECTION.get(heading)
            continue

        key_match = CHAT_KEY_TOKEN_RE.search(line)
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
        for mark, raw_flag in CHECKBOX_TOKEN_RE.findall(line):
            norm_flag = _normalize_flag_name(raw_flag)
            if norm_flag in flags:
                flags[norm_flag] = bool(mark.strip())

        title_match = TITLE_TOKEN_RE.search(line)
        title = title_match.group(1).strip() if title_match else chat_key

        user_match = USERNAME_TOKEN_RE.search(line)
        username = user_match.group(1).strip() if user_match else None

        acc_match = ACCOUNTS_TOKEN_RE.search(line)
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
