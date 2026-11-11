"""Per-chat archive statistics (messages, media per category, links, sizes) for the web UI."""

from __future__ import annotations

import os
import re
import sqlite3
import threading
import time
from collections.abc import Iterable
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
    # Media of archived messages not downloaded yet (category unchecked, over the size limit
    # or failed), per category: items, their exact total size, and items of unknown size.
    queued_counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(MEDIA_ROOTS, 0))
    queued_bytes: dict[str, int] = field(default_factory=lambda: dict.fromkeys(MEDIA_ROOTS, 0))
    queued_unsized: dict[str, int] = field(default_factory=lambda: dict.fromkeys(MEDIA_ROOTS, 0))
    # ...of which above the size limit (TGHOARD_MAX_MEDIA_MB): skipped until it is raised
    oversized_counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(MEDIA_ROOTS, 0))
    oversized_bytes: dict[str, int] = field(default_factory=lambda: dict.fromkeys(MEDIA_ROOTS, 0))
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


class FolderScanCache:
    """`(file count, bytes)` per directory, re-scanned only when the directory changed.

    Counting a big archive means one `stat` per file — minutes on a USB disk with hundreds of
    thousands of media files. A directory's mtime changes whenever a file is added, renamed
    or removed in it, so unchanged directories reuse the last result; entries also expire
    after `MAX_AGE_SECS` in case a filesystem doesn't update directory mtimes reliably.
    """

    MAX_AGE_SECS = 600.0

    def __init__(self) -> None:
        self._entries: dict[str, tuple[int, float, int, int]] = {}
        self._lock = threading.Lock()

    def scan(self, path: Path) -> tuple[int, int]:
        key = str(path)
        try:
            mtime = os.stat(path).st_mtime_ns  # read before scanning: later changes are seen
        except OSError:
            with self._lock:
                self._entries.pop(key, None)
            return 0, 0
        now = time.monotonic()
        with self._lock:
            hit = self._entries.get(key)
        if hit is not None and hit[0] == mtime and now - hit[1] < self.MAX_AGE_SECS:
            return hit[2], hit[3]
        count, size = _scan_dir(path)
        with self._lock:
            self._entries[key] = (mtime, now, count, size)
        return count, size


def compute_all_chat_stats(
    data_dir: Path,
    conn: sqlite3.Connection | None,
    *,
    max_media_bytes: int | None = None,
    scan_folders: bool = True,
    folder_cache: FolderScanCache | None = None,
) -> dict[str, ChatStats]:
    """Stats for every chat present on disk and/or in `checkpoint.db`, keyed by chat_key.

    `max_media_bytes` (the sync's size limit, None = unlimited) splits out the queued media
    that is skipped for being too large. `scan_folders=False` skips counting the files on
    disk (fast: database only); `folder_cache` reuses counts of unchanged directories.
    """
    limit = max_media_bytes if max_media_bytes is not None else -1
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
            "SELECT chat_key, category, COUNT(*) AS n, SUM(last_error IS NOT NULL) AS f, "
            "SUM(size_bytes) AS b, SUM(size_bytes IS NULL) AS u, "
            "SUM(?1 >= 0 AND size_bytes > ?1) AS on_, "
            "SUM(CASE WHEN ?1 >= 0 AND size_bytes > ?1 THEN size_bytes END) AS ob "
            "FROM pending_media WHERE downloaded = 0 GROUP BY chat_key, category",
            (limit,),
        ):
            st = get(r["chat_key"])
            n = int(r["n"])
            st.pending_media += n
            st.failed_media += int(r["f"] or 0)
            cat = r["category"]
            if cat in st.queued_counts:
                st.queued_counts[cat] += n
                st.queued_bytes[cat] += int(r["b"] or 0)
                st.queued_unsized[cat] += int(r["u"] or 0)
                st.oversized_counts[cat] += int(r["on_"] or 0)
                st.oversized_bytes[cat] += int(r["ob"] or 0)
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
        st.links = _links_total(paths.links_path)
        if not scan_folders:
            continue
        scan = folder_cache.scan if folder_cache is not None else _scan_dir
        for cat in MEDIA_ROOTS:
            st.media_counts[cat], st.media_bytes[cat] = scan(paths.media_dir(cat))
        _, st.json_bytes = scan(paths.messages_dir)
    return stats


# -- space still needed ----------------------------------------------------------------
# A chat's own average is used once it has at least this many media of known size in the
# category; otherwise the archive-wide average for the category.
MIN_AVG_SAMPLE = 3


@dataclass
class ToDownload:
    """Space still needed for one media category of one chat."""

    exact_bytes: int = 0  # queued media of known size (archived messages)
    queued: int = 0  # queued items (archived messages, not downloaded)
    estimated_bytes: int | None = 0  # None: there are items to estimate but no average yet
    estimated_items: int = 0  # not archived yet (Telegram total - known) + queued of unknown size
    avg_bytes: float | None = None

    @property
    def total_bytes(self) -> int:
        return self.exact_bytes + (self.estimated_bytes or 0)

    @property
    def is_estimate(self) -> bool:
        return self.estimated_items > 0


def media_size_averages(stats: Iterable[ChatStats]) -> dict[str, float | None]:
    """Archive-wide average size per media category (downloaded + queued of known size)."""
    total = dict.fromkeys(MEDIA_ROOTS, 0)
    items = dict.fromkeys(MEDIA_ROOTS, 0)
    for st in stats:
        for cat in MEDIA_ROOTS:
            total[cat] += st.media_bytes[cat] + st.queued_bytes[cat]
            items[cat] += st.media_counts[cat] + st.queued_counts[cat] - st.queued_unsized[cat]
    return {cat: (total[cat] / items[cat] if items[cat] > 0 else None) for cat in MEDIA_ROOTS}


def estimate_to_download(
    st: ChatStats,
    remote: dict[str, Any] | None,
    averages: dict[str, float | None],
) -> dict[str, ToDownload]:
    """Space each category of this chat still needs, whether or not it is checked.

    Exact for media of archived messages (their size is in the queue). Media of history not
    archived yet (Telegram's total minus what the archive knows) — and queued items of unknown
    size — are estimated with the chat's average size for the category (or the archive-wide
    one). The web UI's chat table does the same computation (keep them in sync).
    """
    out: dict[str, ToDownload] = {}
    for cat in MEDIA_ROOTS:
        sized = st.media_counts[cat] + st.queued_counts[cat] - st.queued_unsized[cat]
        avg: float | None
        if sized >= MIN_AVG_SAMPLE:
            avg = (st.media_bytes[cat] + st.queued_bytes[cat]) / sized
        else:
            avg = averages.get(cat)
        known = st.media_counts[cat] + st.queued_counts[cat]
        total_remote = (remote or {}).get(cat)
        unarchived = max(0, int(total_remote) - known) if isinstance(total_remote, int) else 0
        est_items = unarchived + st.queued_unsized[cat]
        out[cat] = ToDownload(
            exact_bytes=st.queued_bytes[cat],
            queued=st.queued_counts[cat],
            estimated_bytes=(round(est_items * avg) if avg else (None if est_items else 0)),
            estimated_items=est_items,
            avg_bytes=avg,
        )
    return out


# -- media skipped for being over the size limit -------------------------------------------
@dataclass
class OversizedItem:
    chat_key: str
    title: str
    category: str
    msg_id: int
    month: str
    file_name: str | None
    mime_type: str | None
    size_bytes: int
    selected: bool  # chat checked for download and this category checked


@dataclass
class OversizedReport:
    limit_bytes: int | None
    items: list[OversizedItem] = field(default_factory=list)  # largest first, up to max_items
    total_items: int = 0
    total_bytes: int = 0
    # What a higher limit would download: [(limit_mb, items, bytes)], cumulative
    thresholds: list[tuple[int, int, int]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


THRESHOLDS_MB = (100, 250, 500, 1000, 2000, 4000)


def list_oversized_media(
    conn: sqlite3.Connection | None,
    selections: dict[str, Any],
    max_media_bytes: int | None,
    *,
    include_unselected: bool = False,
    max_items: int = 500,
) -> OversizedReport:
    """Queued media larger than the size limit, largest first.

    By default only media the sync would otherwise download (chat checked and its category
    checked in `chats.md`); `include_unselected` lists every oversized queued item.
    """
    report = OversizedReport(limit_bytes=max_media_bytes)
    if conn is None or max_media_bytes is None:
        return report
    steps = [mb for mb in THRESHOLDS_MB if mb * 1024 * 1024 > max_media_bytes]
    reach = dict.fromkeys(steps, (0, 0))
    rows = conn.execute(
        "SELECT chat_key, category, msg_id, day_str, file_name, mime_type, size_bytes "
        "FROM pending_media WHERE downloaded = 0 AND size_bytes > ? ORDER BY size_bytes DESC",
        (max_media_bytes,),
    )
    for r in rows:
        sel = selections.get(r["chat_key"])
        selected = bool(sel is not None and sel.baixar and getattr(sel, r["category"], False))
        if not selected and not include_unselected:
            continue
        size = int(r["size_bytes"])
        report.total_items += 1
        report.total_bytes += size
        for mb in steps:
            if size <= mb * 1024 * 1024:
                n, b = reach[mb]
                reach[mb] = (n + 1, b + size)
        if len(report.items) < max_items:
            report.items.append(
                OversizedItem(
                    chat_key=r["chat_key"],
                    title=sel.title if sel is not None else r["chat_key"],
                    category=r["category"],
                    msg_id=int(r["msg_id"]),
                    month=str(r["day_str"] or "")[:7],
                    file_name=r["file_name"],
                    mime_type=r["mime_type"],
                    size_bytes=size,
                    selected=selected,
                )
            )
    for mb in steps:  # stop at the first limit that would download everything
        report.thresholds.append((mb, *reach[mb]))
        if reach[mb][0] == report.total_items:
            break
    return report
