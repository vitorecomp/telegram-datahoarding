"""Per-chat archive statistics (messages, media per category, links, sizes) for the web UI."""

from __future__ import annotations

import os
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from tghoard.storage.paths import MEDIA_ROOTS, ChatPaths, scan_chat_folders

# Anchored at the start of the line, so a chat title can't fake the count
LINKS_TOTAL_RE = re.compile(r"^- \*\*Total de links\*\*:\s*(\d+)")


@dataclass
class ChatStats:
    folder: str | None = None
    messages: int = 0
    deleted: int = 0
    first_month: str | None = None
    last_month: str | None = None
    media_counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(MEDIA_ROOTS, 0))
    media_bytes: dict[str, int] = field(default_factory=lambda: dict.fromkeys(MEDIA_ROOTS, 0))
    links: int = 0
    json_bytes: int = 0
    pending_media: int = 0
    failed_media: int = 0
    last_sync_at: str | None = None
    joined_at: str | None = None  # earliest join date among your accounts
    joined: list[dict[str, Any]] = field(default_factory=list)  # per account

    @property
    def total_bytes(self) -> int:
        return self.json_bytes + sum(self.media_bytes.values())

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["total_bytes"] = self.total_bytes
        return d


def _scan_dir(path: Path) -> tuple[int, int]:
    count = size = 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.name.startswith(".") or not entry.is_file(follow_symlinks=False):
                    continue
                count += 1
                size += entry.stat(follow_symlinks=False).st_size
    except FileNotFoundError:
        pass
    return count, size


def _links_total(path: Path) -> int:
    try:
        with path.open(encoding="utf-8") as fh:
            for _, line in zip(range(8), fh, strict=False):
                if m := LINKS_TOTAL_RE.search(line):
                    return int(m.group(1))
    except OSError:
        pass
    return 0


def open_readonly_db(db_path: Path) -> sqlite3.Connection | None:
    """Read-only connection usable from another thread (the web server's)."""
    if not db_path.exists():
        return None
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 3000")
    return conn


def compute_all_chat_stats(data_dir: Path, conn: sqlite3.Connection | None) -> dict[str, ChatStats]:
    """Stats for every chat present on disk and/or in `checkpoint.db`, keyed by chat_key."""
    stats: dict[str, ChatStats] = {}

    def get(chat_key: str) -> ChatStats:
        return stats.setdefault(chat_key, ChatStats())

    if conn is not None:
        for r in conn.execute(
            "SELECT chat_key, COUNT(*) AS n, SUM(is_deleted) AS d, "
            "MIN(day_str) AS lo, MAX(day_str) AS hi FROM message_locator GROUP BY chat_key"
        ):
            st = get(r["chat_key"])
            st.messages, st.deleted = int(r["n"]), int(r["d"] or 0)
            st.first_month = (r["lo"] or "")[:7] or None
            st.last_month = (r["hi"] or "")[:7] or None
        for r in conn.execute(
            "SELECT chat_key, COUNT(*) AS n, SUM(last_error IS NOT NULL) AS f "
            "FROM pending_media WHERE downloaded = 0 GROUP BY chat_key"
        ):
            st = get(r["chat_key"])
            st.pending_media, st.failed_media = int(r["n"]), int(r["f"] or 0)
        for r in conn.execute(
            "SELECT chat_key, MAX(updated_at) AS t FROM chat_account_cursors GROUP BY chat_key"
        ):
            get(r["chat_key"]).last_sync_at = r["t"]
        try:
            rows = conn.execute(
                "SELECT m.chat_key, m.joined_at, m.inviter_id, a.alias FROM chat_membership m "
                "LEFT JOIN accounts a ON a.account_id = m.account_id "
                "WHERE m.joined_at IS NOT NULL ORDER BY m.joined_at"
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []  # database created before the table existed (read-only connection)
        for r in rows:
            st = get(r["chat_key"])
            st.joined_at = st.joined_at or r["joined_at"]
            st.joined.append(
                {"account": r["alias"], "joined_at": r["joined_at"], "inviter_id": r["inviter_id"]}
            )

    for chat_key, folder in scan_chat_folders(data_dir).items():
        st = get(chat_key)
        st.folder = folder
        paths = ChatPaths(data_dir=data_dir, folder=folder)
        for cat in MEDIA_ROOTS:
            st.media_counts[cat], st.media_bytes[cat] = _scan_dir(paths.media_dir(cat))
        _, st.json_bytes = _scan_dir(paths.messages_dir)
        st.links = _links_total(paths.links_path)
    return stats
