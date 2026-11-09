"""Canonical chat keys, directory layout, and -ptNNN.json naming helpers."""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Literal

ChatKind = Literal["saved", "supergroup", "channel", "group", "dm", "bot"]
CHAT_SUBDIRS: tuple[str, ...] = ("chats", "photos", "videos", "audio", "files", "metadata")
PART_FILENAME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-pt(\d+)\.json$")


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


def resolve_chat_dir(data_dir: Path, chat_key: str, title: str) -> Path:
    """Resolve existing chat folder by immutable chat_key prefix or create a new one."""
    data_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{chat_key}__"
    for child in sorted(data_dir.iterdir()):
        if child.is_dir() and (child.name == chat_key or child.name.startswith(prefix)):
            ensure_chat_layout(child)
            return child

    folder = data_dir / f"{chat_key}__{slugify(title)}"
    ensure_chat_layout(folder)
    return folder


def ensure_chat_layout(chat_dir: Path) -> None:
    """Ensure mandatory chat subdirectories exist."""
    chat_dir.mkdir(parents=True, exist_ok=True)
    for sub in CHAT_SUBDIRS:
        (chat_dir / sub).mkdir(parents=True, exist_ok=True)
    links_path = chat_dir / "links.md"
    if not links_path.exists():
        links_path.write_text(
            f"# Links — {chat_dir.name}\n\n_Nenhum link capturado ainda._\n",
            encoding="utf-8",
        )


def format_part_tag(part: int) -> str:
    """Format part number as pt000, pt001, ..., pt999, pt1000."""
    if part < 0:
        raise ValueError(f"part must be non-negative, got {part}")
    return f"pt{part:03d}"


def part_file_path(chat_dir: Path, day_str: str, part: int) -> Path:
    """Return `<chat_dir>/chats/YYYY-MM-DD-ptNNN.json`."""
    return chat_dir / "chats" / f"{day_str}-{format_part_tag(part)}.json"


def parse_part_filename(filename: str) -> tuple[str, int] | None:
    """Parse `YYYY-MM-DD-ptNNN.json` -> `(day_str, part_num)` or None."""
    match = PART_FILENAME_RE.match(filename)
    if not match:
        return None
    return match.group(1), int(match.group(2))


def list_day_part_files(chat_dir: Path, day_str: str) -> list[tuple[int, Path]]:
    """Return sorted `[(part_num, path), ...]` for a given `YYYY-MM-DD`."""
    chats_dir = chat_dir / "chats"
    if not chats_dir.exists():
        return []
    parts: list[tuple[int, Path]] = []
    for path in chats_dir.glob(f"{day_str}-pt*.json"):
        parsed = parse_part_filename(path.name)
        if parsed is not None and parsed[0] == day_str:
            parts.append((parsed[1], path))
    parts.sort(key=lambda item: item[0])
    return parts


def iter_all_part_files(chat_dir: Path) -> list[Path]:
    """Return all `YYYY-MM-DD-ptNNN.json` files in strict chronological + part order."""
    chats_dir = chat_dir / "chats"
    if not chats_dir.exists():
        return []
    items: list[tuple[str, int, Path]] = []
    for path in chats_dir.glob("*-pt*.json"):
        parsed = parse_part_filename(path.name)
        if parsed is not None:
            items.append((parsed[0], parsed[1], path))
    items.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in items]
