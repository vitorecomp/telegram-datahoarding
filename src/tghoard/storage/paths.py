"""Canonical chat keys, directory layout, and -ptNNN.json naming helpers."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from tghoard.storage.atomic import atomic_write_text

ChatKind = Literal["saved", "supergroup", "channel", "group", "dm", "bot"]

# Line breaks (incl. U+2028/U+2029, which `str.splitlines()` honors) and C0/C1 controls
_NOT_INLINE_RE = re.compile(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]+")


def single_line(text: object) -> str:
    """Telegram-sourced text (titles, names, captions) made safe for one line of Markdown.

    Every line-oriented file we parse back (`chats.md`, `metadata/*/info.md`, the append-only
    logs) relies on one entry per line; a title with an embedded line break could otherwise
    forge extra entries or a fake identity header.
    """
    return _NOT_INLINE_RE.sub(" ", str(text)).strip()


def make_chat_key(kind: ChatKind, peer_raw_id: int, self_id: int) -> str:
    """Compute canonical cross-account chat_key."""
    abs_peer = abs(int(peer_raw_id))
    abs_self = abs(int(self_id))
    if kind == "saved" or (kind in ("dm", "bot") and abs_peer == abs_self):
        return f"saved_{abs_self}"
    if kind == "supergroup":
        return f"supergroup_{abs_peer}"
    if kind == "channel":
        return f"channel_{abs_peer}"
    if kind == "group":
        return f"group_{abs_peer}"
    lo, hi = sorted((abs_self, abs_peer))
    return f"dm_{lo}_{hi}"


def uses_global_msg_ids(chat_key: str) -> bool:
    """True when message IDs are shared across all Telegram accounts."""
    return chat_key.startswith(("channel_", "supergroup_"))


def slugify(text: str, max_len: int = 48) -> str:
    """Build a clean, filesystem-friendly slug preserving readable ASCII/Latin chars."""
    normalized = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", normalized.lower()).strip("-._")
    if not cleaned:
        cleaned = "chat"
    return cleaned[:max_len].rstrip("-._") or "chat"


def kebab_case(text: str, max_len: int = 60) -> str:
    """`"Python Brasil 🐍 — Dev"` -> `"python-brasil-dev"` (ASCII, lowercase, single hyphens)."""
    normalized = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^a-z0-9]+", "-", normalized.lower()).strip("-")
    return cleaned[:max_len].rstrip("-") or "chat"


# ---------------------------------------------------------------------------
# Category-first layout
#
#   <data_dir>/messages/<folder>/YYYY-MM-ptNNN.json
#   <data_dir>/{photos,videos,audio,files}/<folder>/...
#   <data_dir>/links/<folder>.md
#   <data_dir>/metadata/<folder>/*.md
#
# `<folder>` = `<kebab-title>-<id>` (e.g. `python-brasil-1234567890`, DMs `joao-5-7`).
# Identity lives in plain text: `metadata/<folder>/info.md` starts with "(`<chat_key>`)",
# so folders can be re-resolved (and DB rebuilt) without any internal state.
# ---------------------------------------------------------------------------

MEDIA_ROOTS: tuple[str, ...] = ("photos", "videos", "audio", "files")
LAYOUT_ROOTS: tuple[str, ...] = ("messages", *MEDIA_ROOTS, "links", "metadata")
# Anchored at the end of the line: the title comes first, so a chat named "x (`channel_1`)"
# can't claim another chat's folder.
CHAT_KEY_IN_HEADER_RE = re.compile(r"\(`([a-z]+_[0-9_]+)`\)\s*$")


def chat_id_suffix(chat_key: str) -> str:
    """`channel_123` -> `123`, `dm_5_7` -> `5-7`, `saved_9` -> `9`."""
    return "-".join(chat_key.split("_")[1:]) or chat_key


def make_folder_name(chat_key: str, title: str) -> str:
    return f"{kebab_case(title)}-{chat_id_suffix(chat_key)}"


@dataclass(frozen=True)
class ChatPaths:
    """All locations of one chat inside the category-first layout."""

    data_dir: Path
    folder: str

    @property
    def messages_dir(self) -> Path:
        return self.data_dir / "messages" / self.folder

    def media_dir(self, category: str) -> Path:
        return self.data_dir / category / self.folder

    @property
    def links_path(self) -> Path:
        return self.data_dir / "links" / f"{self.folder}.md"

    @property
    def metadata_dir(self) -> Path:
        return self.data_dir / "metadata" / self.folder

    def meta(self, filename: str) -> Path:
        return self.metadata_dir / filename

    def media_rel(self, category: str, filename: str) -> str:
        """Media path as stored in JSON/`media_index`: relative to `data_dir`, POSIX."""
        return f"{category}/{self.folder}/{filename}"

    def abs_path(self, rel_path: str) -> Path:
        return self.data_dir / rel_path

    def ensure(self) -> None:
        """Create only what identifies the chat (`metadata/<folder>/`) and its links file.

        `messages/` and the media category folders are created lazily, the first time a
        file is actually written there — no empty `photos/<folder>/` for chats whose media
        isn't being downloaded.
        """
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.links_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.links_path.exists():
            atomic_write_text(
                self.links_path, f"# Links — {self.folder}\n\n_Nenhum link capturado ainda._\n"
            )


def read_chat_key_from_metadata(metadata_dir: Path) -> str | None:
    info = metadata_dir / "info.md"
    try:
        with info.open(encoding="utf-8") as fh:
            first = fh.readline()
    except OSError:
        return None
    m = CHAT_KEY_IN_HEADER_RE.search(first)
    return m.group(1) if m else None


_FOLDER_INDEX: dict[Path, dict[str, str]] = {}


def scan_chat_folders(data_dir: Path) -> dict[str, str]:
    """`{chat_key: folder}` from `metadata/*/info.md` headers. Pure: never touches the cache."""
    index: dict[str, str] = {}
    meta_root = data_dir / "metadata"
    if meta_root.is_dir():
        for child in sorted(meta_root.iterdir()):
            if child.is_dir() and not child.name.startswith("."):
                ck = read_chat_key_from_metadata(child)
                if ck is not None:
                    index.setdefault(ck, child.name)
    return index


def index_chat_folders(data_dir: Path, *, refresh: bool = False) -> dict[str, str]:
    """Process-wide cached `scan_chat_folders` used by the sync to resolve folders.

    Other threads (web UI) must use `scan_chat_folders` instead, so they never replace the
    cache while the sync is creating a folder whose `info.md` isn't written yet.
    """
    key = data_dir.resolve()
    if refresh or key not in _FOLDER_INDEX:
        _FOLDER_INDEX[key] = scan_chat_folders(data_dir)
    return _FOLDER_INDEX[key]


def resolve_chat_paths(data_dir: Path, chat_key: str, title: str) -> ChatPaths:
    """Find the chat's existing folder (by identity, not name) or create a new one.

    Renamed chats keep their original folder. Name clashes get a numeric suffix.
    """
    index = index_chat_folders(data_dir)
    folder = index.get(chat_key)
    if folder is None:
        base = make_folder_name(chat_key, title)
        taken = set(index.values())
        folder, n = base, 2
        while folder in taken or (data_dir / "metadata" / folder).exists():
            meta = data_dir / "metadata" / folder
            if read_chat_key_from_metadata(meta) == chat_key:
                break  # our own folder, just not in this (stale) index yet
            if folder not in taken and _is_empty_dir(meta):
                break  # left without identity by a crash right after mkdir: reclaim it
            folder, n = f"{base}-{n}", n + 1
        index[chat_key] = folder
        paths = ChatPaths(data_dir=data_dir, folder=folder)
        paths.ensure()
        # Claim the folder right away: identity lives in info.md's first line
        info = paths.meta("info.md")
        if read_chat_key_from_metadata(paths.metadata_dir) is None:
            atomic_write_text(
                info,
                f"# Informações — {single_line(title)} (`{chat_key}`)\n\n"
                "_Sincronização pendente._\n",
            )
        return paths
    paths = ChatPaths(data_dir=data_dir, folder=folder)
    paths.ensure()
    return paths


def _is_empty_dir(path: Path) -> bool:
    try:
        return path.is_dir() and not any(path.iterdir())
    except OSError:
        return False


def iter_chat_paths(data_dir: Path) -> list[tuple[str, ChatPaths]]:
    """All known chats in the layout as `(chat_key, ChatPaths)`, sorted by folder."""
    index = index_chat_folders(data_dir, refresh=True)
    return sorted(
        ((ck, ChatPaths(data_dir=data_dir, folder=f)) for ck, f in index.items()),
        key=lambda item: item[1].folder,
    )


def is_legacy_layout(data_dir: Path) -> bool:
    """True if `data_dir` still has pre-0001 per-chat folders (`<chat_key>__<slug>/chats/`)."""
    if not data_dir.is_dir():
        return False
    return any(
        child.is_dir() and "__" in child.name and (child / "chats").is_dir()
        for child in data_dir.iterdir()
    )


# ---------------------------------------------------------------------------
# Monthly `YYYY-MM-ptNNN.json` partitions
# ---------------------------------------------------------------------------

PART_FILENAME_RE = re.compile(r"^(\d{4}-\d{2})-pt(\d+)\.json$")


def month_from_iso(date_iso: str | None) -> str:
    """`2026-11-09T10:00:00+00:00` (or `2026-11-09`) -> `2026-11`."""
    if date_iso and len(date_iso) >= 7 and date_iso[4] == "-":
        return date_iso[:7]
    return "1970-01"


def format_part_tag(part: int) -> str:
    """Format part number as pt000, pt001, ..., pt999, pt1000."""
    if part < 0:
        raise ValueError(f"part must be non-negative, got {part}")
    return f"pt{part:03d}"


def part_file_path(chat_paths: ChatPaths, month: str, part: int) -> Path:
    """Return `<data_dir>/messages/<folder>/YYYY-MM-ptNNN.json`."""
    return chat_paths.messages_dir / f"{month}-{format_part_tag(part)}.json"


def parse_part_filename(filename: str) -> tuple[str, int] | None:
    """Parse `YYYY-MM-ptNNN.json` -> `(month, part_num)` or None."""
    match = PART_FILENAME_RE.match(filename)
    if not match:
        return None
    return match.group(1), int(match.group(2))


def list_month_part_files(chat_paths: ChatPaths, month: str) -> list[tuple[int, Path]]:
    """Return sorted `[(part_num, path), ...]` for a given `YYYY-MM`."""
    if not chat_paths.messages_dir.exists():
        return []
    parts: list[tuple[int, Path]] = []
    for path in chat_paths.messages_dir.glob(f"{month}-pt*.json"):
        parsed = parse_part_filename(path.name)
        if parsed is not None and parsed[0] == month:
            parts.append((parsed[1], path))
    parts.sort(key=lambda item: item[0])
    return parts


def iter_all_part_files(chat_paths: ChatPaths) -> list[Path]:
    """Return all `YYYY-MM-ptNNN.json` files in strict chronological + part order."""
    if not chat_paths.messages_dir.exists():
        return []
    items: list[tuple[str, int, Path]] = []
    for path in chat_paths.messages_dir.glob("*-pt*.json"):
        parsed = parse_part_filename(path.name)
        if parsed is not None:
            items.append((parsed[0], parsed[1], path))
    items.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in items]
