"""SQLite WAL checkpoint and cross-account deduplication engine."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tghoard.config import MediaCategory
from tghoard.storage.atomic import compute_missing_ranges, merge_ranges, utc_now_iso

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS accounts (
    account_id   INTEGER PRIMARY KEY,
    alias        TEXT UNIQUE,
    phone        TEXT,
    username     TEXT,
    display_name TEXT,
    added_at     TEXT NOT NULL,
    last_sync_at TEXT
);

CREATE TABLE IF NOT EXISTS account_pts_state (
    account_id   INTEGER NOT NULL,
    scope_key    TEXT NOT NULL,
    pts          INTEGER NOT NULL,
    qts          INTEGER NOT NULL DEFAULT 0,
    seq          INTEGER NOT NULL DEFAULT 0,
    date_ts      INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (account_id, scope_key)
);

CREATE TABLE IF NOT EXISTS chat_synced_ranges (
    chat_key     TEXT NOT NULL,
    start_id     INTEGER NOT NULL,
    end_id       INTEGER NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (chat_key, start_id)
);

CREATE TABLE IF NOT EXISTS chat_account_cursors (
    chat_key             TEXT NOT NULL,
    account_id           INTEGER NOT NULL,
    folder_rel_path      TEXT NOT NULL,
    backfill_offset_id   INTEGER,
    backfill_completed   INTEGER NOT NULL DEFAULT 0,
    last_story_id        INTEGER,
    last_admin_event_id  INTEGER,
    updated_at           TEXT NOT NULL,
    forward_max_id       INTEGER,
    PRIMARY KEY (chat_key, account_id)
);

CREATE TABLE IF NOT EXISTS message_locator (
    chat_key       TEXT NOT NULL,
    canonical_key  TEXT NOT NULL,
    day_str        TEXT NOT NULL,
    part_num       INTEGER NOT NULL DEFAULT 0,
    sender_id      INTEGER,
    is_deleted     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chat_key, canonical_key)
);

CREATE INDEX IF NOT EXISTS idx_locator_chat_day
    ON message_locator (chat_key, day_str, part_num);

CREATE TABLE IF NOT EXISTS account_message_map (
    account_id     INTEGER NOT NULL,
    chat_key       TEXT NOT NULL,
    msg_id         INTEGER NOT NULL,
    canonical_key  TEXT NOT NULL,
    PRIMARY KEY (account_id, chat_key, msg_id)
);

CREATE INDEX IF NOT EXISTS idx_account_global_msg
    ON account_message_map (account_id, msg_id);

CREATE TABLE IF NOT EXISTS pending_media (
    chat_key       TEXT NOT NULL,
    canonical_key  TEXT NOT NULL,
    account_id     INTEGER NOT NULL,
    msg_id         INTEGER NOT NULL,
    day_str        TEXT NOT NULL,
    category       TEXT NOT NULL,
    tg_type        TEXT NOT NULL,
    tg_id          INTEGER NOT NULL,
    file_name      TEXT,
    mime_type      TEXT,
    size_bytes     INTEGER,
    downloaded     INTEGER NOT NULL DEFAULT 0,
    attempts       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    retry_after    REAL,
    PRIMARY KEY (chat_key, canonical_key)
);

CREATE INDEX IF NOT EXISTS idx_pending_media_lookup
    ON pending_media (chat_key, category, downloaded);

CREATE TABLE IF NOT EXISTS media_index (
    tg_type        TEXT NOT NULL,
    tg_id          INTEGER NOT NULL,
    category       TEXT NOT NULL,
    sha256         TEXT NOT NULL,
    size_bytes     INTEGER NOT NULL,
    rel_path       TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    PRIMARY KEY (tg_type, tg_id)
);

CREATE TABLE IF NOT EXISTS chat_membership (
    chat_key       TEXT NOT NULL,
    account_id     INTEGER NOT NULL,
    joined_at      TEXT,
    inviter_id     INTEGER,
    source         TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (chat_key, account_id)
);

CREATE TABLE IF NOT EXISTS chat_metadata_state (
    chat_key       TEXT NOT NULL,
    account_id     INTEGER NOT NULL,
    synced_at      REAL NOT NULL,
    PRIMARY KEY (chat_key, account_id)
);

CREATE TABLE IF NOT EXISTS chat_profile_state (
    chat_key       TEXT PRIMARY KEY,
    state_json     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
"""

# Indexes on columns added by `_migrate` (created after the columns exist).
POST_MIGRATION_SQL = """
CREATE INDEX IF NOT EXISTS idx_pending_media_order
    ON pending_media (chat_key, downloaded, day_str, msg_id, canonical_key);
"""

PENDING_PAGE_SIZE = 1000

# Permanent media failures (the message or its media no longer exists on the server) are
# retried on every sync for the first few attempts, then with an exponential backoff
# (1 day, 2, 4, ... capped at 30 days). They stay in `pending_media` (`downloaded = 0`,
# `last_error` set), so `verify-media` and the stats keep showing them.
PERMANENT_FAILURE_FREE_ATTEMPTS = 3
PERMANENT_FAILURE_BASE_BACKOFF_SECS = 24 * 3600.0
PERMANENT_FAILURE_MAX_BACKOFF_SECS = 30 * 24 * 3600.0


def permanent_failure_retry_after(attempts: int, now: float) -> float | None:
    """When a permanently failing media item may be retried again (`None` = next sync)."""
    over = attempts - PERMANENT_FAILURE_FREE_ATTEMPTS
    if over <= 0:
        return None
    delay = PERMANENT_FAILURE_BASE_BACKOFF_SECS * (2 ** min(over - 1, 16))
    return now + min(PERMANENT_FAILURE_MAX_BACKOFF_SECS, delay)


@dataclass
class AccountRecord:
    account_id: int
    alias: str
    phone: str | None
    username: str | None
    display_name: str | None
    added_at: str
    last_sync_at: str | None = None


@dataclass
class PtsState:
    pts: int
    qts: int = 0
    seq: int = 0
    date_ts: int = 0


@dataclass
class LocatedMessage:
    chat_key: str
    canonical_key: str
    day_str: str
    part_num: int
    sender_id: int | None
    is_deleted: bool


@dataclass
class PendingMediaRow:
    chat_key: str
    canonical_key: str
    account_id: int
    msg_id: int
    day_str: str
    category: MediaCategory
    tg_type: str
    tg_id: int
    file_name: str | None
    mime_type: str | None
    size_bytes: int | None
    attempts: int = 0
    last_error: str | None = None


@dataclass
class IndexedMedia:
    tg_type: str
    tg_id: int
    category: str
    sha256: str
    size_bytes: int
    rel_path: str


class CheckpointDB:
    """SQLite WAL wrapper exclusively for checkpoints, ranges, and deduplication."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA_SQL)
        self._migrate()
        try:
            os.chmod(self.db_path, 0o600)
        except OSError:
            pass

    def _migrate(self) -> None:
        """Add columns introduced after the first release to existing databases."""
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(pending_media)")}
        if "attempts" not in cols:
            self._conn.execute(
                "ALTER TABLE pending_media ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0"
            )
        if "last_error" not in cols:
            self._conn.execute("ALTER TABLE pending_media ADD COLUMN last_error TEXT")
        if "retry_after" not in cols:
            self._conn.execute("ALTER TABLE pending_media ADD COLUMN retry_after REAL")
        cursor_cols = {
            r["name"] for r in self._conn.execute("PRAGMA table_info(chat_account_cursors)")
        }
        if "forward_max_id" not in cursor_cols:
            # NULL on existing rows: `sync` derives a safe starting point (see messages_sync)
            self._conn.execute("ALTER TABLE chat_account_cursors ADD COLUMN forward_max_id INTEGER")
        self._conn.executescript(POST_MIGRATION_SQL)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> CheckpointDB:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Accounts
    # ------------------------------------------------------------------
    def upsert_account(
        self,
        *,
        account_id: int,
        alias: str | None = None,
        phone: str | None = None,
        username: str | None = None,
        display_name: str | None = None,
    ) -> AccountRecord:
        now = utc_now_iso()
        existing = self.get_account(account_id)
        final_alias = alias or (existing.alias if existing else f"acc_{account_id}")
        # Ensure alias uniqueness across different account_ids
        clash = self._conn.execute(
            "SELECT account_id FROM accounts WHERE alias = ? AND account_id != ?",
            (final_alias, account_id),
        ).fetchone()
        if clash is not None:
            final_alias = f"{final_alias}_{account_id}"

        self._conn.execute(
            """
            INSERT INTO accounts (account_id, alias, phone, username, display_name, added_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                alias = excluded.alias,
                phone = COALESCE(excluded.phone, accounts.phone),
                username = COALESCE(excluded.username, accounts.username),
                display_name = COALESCE(excluded.display_name, accounts.display_name)
            """,
            (account_id, final_alias, phone, username, display_name, now),
        )
        rec = self.get_account(account_id)
        assert rec is not None
        return rec

    def touch_account_sync(self, account_id: int) -> None:
        self._conn.execute(
            "UPDATE accounts SET last_sync_at = ? WHERE account_id = ?",
            (utc_now_iso(), account_id),
        )

    def get_account(self, identifier: int | str) -> AccountRecord | None:
        if isinstance(identifier, int) or (isinstance(identifier, str) and identifier.isdigit()):
            row = self._conn.execute(
                "SELECT * FROM accounts WHERE account_id = ?",
                (int(identifier),),
            ).fetchone()
            if row is not None:
                return self._row_to_account(row)
        row = self._conn.execute(
            "SELECT * FROM accounts WHERE alias = ? OR username = ?",
            (str(identifier).lstrip("@"), str(identifier).lstrip("@")),
        ).fetchone()
        return self._row_to_account(row) if row is not None else None

    def list_accounts(self) -> list[AccountRecord]:
        rows = self._conn.execute(
            "SELECT * FROM accounts ORDER BY added_at ASC, account_id ASC"
        ).fetchall()
        return [self._row_to_account(r) for r in rows]

    def remove_account(self, identifier: int | str) -> AccountRecord | None:
        acc = self.get_account(identifier)
        if acc is None:
            return None
        self._conn.execute("BEGIN")
        try:
            self._conn.execute("DELETE FROM accounts WHERE account_id = ?", (acc.account_id,))
            self._conn.execute(
                "DELETE FROM account_pts_state WHERE account_id = ?",
                (acc.account_id,),
            )
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        return acc

    @staticmethod
    def _row_to_account(row: sqlite3.Row) -> AccountRecord:
        return AccountRecord(
            account_id=int(row["account_id"]),
            alias=str(row["alias"]),
            phone=row["phone"],
            username=row["username"],
            display_name=row["display_name"],
            added_at=str(row["added_at"]),
            last_sync_at=row["last_sync_at"],
        )

    # ------------------------------------------------------------------
    # PTS / QTS state
    # ------------------------------------------------------------------
    def get_pts_state(self, account_id: int, scope_key: str = "global") -> PtsState | None:
        row = self._conn.execute(
            "SELECT pts, qts, seq, date_ts FROM account_pts_state "
            "WHERE account_id = ? AND scope_key = ?",
            (account_id, scope_key),
        ).fetchone()
        if row is None:
            return None
        return PtsState(
            pts=int(row["pts"]),
            qts=int(row["qts"]),
            seq=int(row["seq"]),
            date_ts=int(row["date_ts"]),
        )

    def set_pts_state(
        self,
        account_id: int,
        pts: int,
        *,
        scope_key: str = "global",
        qts: int = 0,
        seq: int = 0,
        date_ts: int = 0,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO account_pts_state
                (account_id, scope_key, pts, qts, seq, date_ts, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, scope_key) DO UPDATE SET
                pts = excluded.pts,
                qts = excluded.qts,
                seq = excluded.seq,
                date_ts = CASE
                    WHEN excluded.date_ts > 0 THEN excluded.date_ts
                    ELSE account_pts_state.date_ts
                END,
                updated_at = excluded.updated_at
            """,
            (account_id, scope_key, pts, qts, seq, date_ts, utc_now_iso()),
        )

    # ------------------------------------------------------------------
    # Synced Ranges & Pagination Cursors
    # ------------------------------------------------------------------
    def get_synced_ranges(self, chat_key: str) -> list[tuple[int, int]]:
        rows = self._conn.execute(
            "SELECT start_id, end_id FROM chat_synced_ranges "
            "WHERE chat_key = ? ORDER BY start_id ASC",
            (chat_key,),
        ).fetchall()
        # Stored ranges are kept merged by `add_synced_ranges`; merging on read (linear on
        # already-sorted rows) also normalizes rows written by anything else.
        return merge_ranges((int(r["start_id"]), int(r["end_id"])) for r in rows)

    def add_synced_range(self, chat_key: str, start_id: int, end_id: int) -> None:
        self.add_synced_ranges(chat_key, [(start_id, end_id)])

    def add_synced_ranges(self, chat_key: str, ranges: Iterable[tuple[int, int]]) -> None:
        """Mark many id ranges as synced, in one transaction.

        Incremental: each range only touches the stored ranges it overlaps or touches
        (index lookups on `(chat_key, start_id)`), instead of re-reading and rewriting every
        range of the chat — closing thousands of small holes used to be quadratic.
        """
        incoming = merge_ranges(ranges)
        if not incoming:
            return
        now = utc_now_iso()
        conn = self._conn
        conn.execute("BEGIN")
        try:
            for start, end in incoming:
                new_start, new_end = start, end
                # Stored ranges are disjoint and non-adjacent, so at most one range that
                # starts before `start` can overlap/touch [start, end]: the closest one.
                left = conn.execute(
                    "SELECT start_id, end_id FROM chat_synced_ranges "
                    "WHERE chat_key = ? AND start_id < ? ORDER BY start_id DESC LIMIT 1",
                    (chat_key, start),
                ).fetchone()
                if left is not None and int(left["end_id"]) >= start - 1:
                    if int(left["end_id"]) >= end:
                        continue  # already covered
                    new_start = int(left["start_id"])
                    conn.execute(
                        "DELETE FROM chat_synced_ranges WHERE chat_key = ? AND start_id = ?",
                        (chat_key, new_start),
                    )
                # Every range starting inside [start, end + 1] overlaps or touches it
                inner = conn.execute(
                    "SELECT MAX(end_id) AS max_end FROM chat_synced_ranges "
                    "WHERE chat_key = ? AND start_id BETWEEN ? AND ?",
                    (chat_key, start, end + 1),
                ).fetchone()
                if inner is not None and inner["max_end"] is not None:
                    new_end = max(new_end, int(inner["max_end"]))
                    conn.execute(
                        "DELETE FROM chat_synced_ranges "
                        "WHERE chat_key = ? AND start_id BETWEEN ? AND ?",
                        (chat_key, start, end + 1),
                    )
                conn.execute(
                    "INSERT INTO chat_synced_ranges (chat_key, start_id, end_id, updated_at) "
                    "VALUES (?, ?, ?, ?)",
                    (chat_key, new_start, new_end, now),
                )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def get_missing_ranges(
        self,
        chat_key: str,
        min_id: int,
        max_id: int,
    ) -> list[tuple[int, int]]:
        synced = self.get_synced_ranges(chat_key)
        return compute_missing_ranges(min_id, max_id, synced)

    def get_chat_cursor(self, chat_key: str, account_id: int) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM chat_account_cursors WHERE chat_key = ? AND account_id = ?",
            (chat_key, account_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def upsert_chat_cursor(
        self,
        *,
        chat_key: str,
        account_id: int,
        folder_rel_path: str,
        backfill_offset_id: int | None = None,
        backfill_completed: bool | None = None,
        last_story_id: int | None = None,
        last_admin_event_id: int | None = None,
        forward_max_id: int | None = None,
    ) -> None:
        """Upsert a chat/account cursor; `None` arguments keep the stored value.

        `forward_max_id` (DMs, Saved Messages, basic groups) is the highest message id up to
        which `sync` itself walked the history contiguously; it only ever moves forward.
        """
        existing = self.get_chat_cursor(chat_key, account_id)
        now = utc_now_iso()
        final_offset = (
            backfill_offset_id
            if backfill_offset_id is not None or backfill_completed
            else (existing["backfill_offset_id"] if existing else None)
        )
        final_completed = (
            int(backfill_completed)
            if backfill_completed is not None
            else (int(existing["backfill_completed"]) if existing else 0)
        )
        final_story = (
            last_story_id
            if last_story_id is not None
            else (existing["last_story_id"] if existing else None)
        )
        final_admin = (
            last_admin_event_id
            if last_admin_event_id is not None
            else (existing["last_admin_event_id"] if existing else None)
        )
        old_forward = existing.get("forward_max_id") if existing else None
        final_forward = (
            max(int(old_forward or 0), int(forward_max_id))
            if forward_max_id is not None
            else old_forward
        )
        self._conn.execute(
            """
            INSERT INTO chat_account_cursors (
                chat_key, account_id, folder_rel_path, backfill_offset_id,
                backfill_completed, last_story_id, last_admin_event_id, updated_at,
                forward_max_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_key, account_id) DO UPDATE SET
                folder_rel_path = excluded.folder_rel_path,
                backfill_offset_id = excluded.backfill_offset_id,
                backfill_completed = excluded.backfill_completed,
                last_story_id = excluded.last_story_id,
                last_admin_event_id = excluded.last_admin_event_id,
                updated_at = excluded.updated_at,
                forward_max_id = excluded.forward_max_id
            """,
            (
                chat_key,
                account_id,
                folder_rel_path,
                final_offset,
                final_completed,
                final_story,
                final_admin,
                now,
                final_forward,
            ),
        )

    def get_chat_folder_rel_path(self, chat_key: str) -> str | None:
        row = self._conn.execute(
            "SELECT folder_rel_path FROM chat_account_cursors "
            "WHERE chat_key = ? ORDER BY updated_at DESC LIMIT 1",
            (chat_key,),
        ).fetchone()
        return str(row["folder_rel_path"]) if row is not None else None

    # ------------------------------------------------------------------
    # Membership (when each account joined each chat)
    # ------------------------------------------------------------------
    def set_memberships(self, rows: Iterable[tuple[str, int, str | None, int | None, str]]) -> None:
        """Upsert `(chat_key, account_id, joined_at_iso, inviter_id, source)` rows."""
        now = utc_now_iso()
        self._conn.execute("BEGIN")
        try:
            self._conn.executemany(
                """
                INSERT INTO chat_membership
                    (chat_key, account_id, joined_at, inviter_id, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_key, account_id) DO UPDATE SET
                    joined_at = excluded.joined_at,
                    inviter_id = COALESCE(excluded.inviter_id, chat_membership.inviter_id),
                    source = excluded.source,
                    updated_at = excluded.updated_at
                """,
                [(ck, acc, joined, inviter, src, now) for ck, acc, joined, inviter, src in rows],
            )
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    def metadata_synced_at(self, chat_key: str, account_id: int) -> float | None:
        """Unix time of the last metadata refresh of `chat_key` by `account_id`."""
        row = self._conn.execute(
            "SELECT synced_at FROM chat_metadata_state WHERE chat_key = ? AND account_id = ?",
            (chat_key, account_id),
        ).fetchone()
        return float(row["synced_at"]) if row is not None else None

    def mark_metadata_synced(self, chat_key: str, account_id: int, when: float) -> None:
        self._conn.execute(
            "INSERT INTO chat_metadata_state (chat_key, account_id, synced_at) VALUES (?, ?, ?) "
            "ON CONFLICT(chat_key, account_id) DO UPDATE SET synced_at = excluded.synced_at",
            (chat_key, account_id, when),
        )

    def get_memberships(self, chat_key: str) -> dict[int, dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM chat_membership WHERE chat_key = ?", (chat_key,)
        ).fetchall()
        return {int(r["account_id"]): dict(r) for r in rows}

    def list_chat_accounts(self, chat_key: str) -> list[AccountRecord]:
        rows = self._conn.execute(
            """
            SELECT a.* FROM accounts a
            JOIN chat_account_cursors c ON c.account_id = a.account_id
            WHERE c.chat_key = ?
            ORDER BY a.account_id ASC
            """,
            (chat_key,),
        ).fetchall()
        return [self._row_to_account(r) for r in rows]

    # ------------------------------------------------------------------
    # Message Locator & Account Message Mapping
    # ------------------------------------------------------------------
    def record_messages_batch(
        self,
        *,
        chat_key: str,
        account_id: int | None,
        locators: Iterable[tuple[str, str, int, int | None, bool]],
        account_mappings: Iterable[tuple[int, str]],
    ) -> None:
        """Atomically record `(canonical_key, day_str, part_num, sender_id, is_deleted)`."""
        self._conn.execute("BEGIN")
        try:
            self._conn.executemany(
                """
                INSERT INTO message_locator
                    (chat_key, canonical_key, day_str, part_num, sender_id, is_deleted)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_key, canonical_key) DO UPDATE SET
                    day_str = excluded.day_str,
                    part_num = excluded.part_num,
                    sender_id = COALESCE(excluded.sender_id, message_locator.sender_id),
                    is_deleted = MAX(message_locator.is_deleted, excluded.is_deleted)
                """,
                [
                    (chat_key, ckey, day_str, part_num, sender_id, int(is_del))
                    for ckey, day_str, part_num, sender_id, is_del in locators
                ],
            )
            if account_id is not None:
                self._conn.executemany(
                    """
                    INSERT INTO account_message_map (account_id, chat_key, msg_id, canonical_key)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(account_id, chat_key, msg_id) DO UPDATE SET
                        canonical_key = excluded.canonical_key
                    """,
                    [(account_id, chat_key, msg_id, ckey) for msg_id, ckey in account_mappings],
                )
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    def update_day_part_numbers(
        self,
        chat_key: str,
        day_str: str,
        part_map: dict[str, int],
    ) -> None:
        if not part_map:
            return
        self._conn.execute("BEGIN")
        try:
            self._conn.executemany(
                """
                UPDATE message_locator
                SET part_num = ?
                WHERE chat_key = ? AND canonical_key = ? AND day_str = ?
                """,
                [(part, chat_key, ckey, day_str) for ckey, part in part_map.items()],
            )
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    def locate_by_canonical(self, chat_key: str, canonical_key: str) -> LocatedMessage | None:
        row = self._conn.execute(
            "SELECT * FROM message_locator WHERE chat_key = ? AND canonical_key = ?",
            (chat_key, canonical_key),
        ).fetchone()
        return self._row_to_located(row) if row is not None else None

    def locate_by_account_msg(
        self,
        account_id: int,
        msg_id: int,
        chat_key: str | None = None,
    ) -> list[LocatedMessage]:
        """Resolve `(account_id, msg_id)` (with or without chat_key) to exact day/part."""
        if chat_key is not None:
            rows = self._conn.execute(
                """
                SELECT l.* FROM message_locator l
                JOIN account_message_map m
                  ON m.chat_key = l.chat_key AND m.canonical_key = l.canonical_key
                WHERE m.account_id = ? AND m.chat_key = ? AND m.msg_id = ?
                """,
                (account_id, chat_key, msg_id),
            ).fetchall()
            if not rows:
                # Global channel/supergroup fallback where canonical_key == f"id:{msg_id}"
                rows = self._conn.execute(
                    "SELECT * FROM message_locator WHERE chat_key = ? AND canonical_key = ?",
                    (chat_key, f"id:{msg_id}"),
                ).fetchall()
        else:
            # Used for global UpdateDeleteMessages where peer_id is omitted by MTProto!
            rows = self._conn.execute(
                """
                SELECT l.* FROM message_locator l
                JOIN account_message_map m
                  ON m.chat_key = l.chat_key AND m.canonical_key = l.canonical_key
                WHERE m.account_id = ? AND m.msg_id = ?
                  AND NOT l.chat_key LIKE 'channel_%'
                  AND NOT l.chat_key LIKE 'supergroup_%'
                """,
                (account_id, msg_id),
            ).fetchall()
        return [self._row_to_located(r) for r in rows]

    def is_account_msg_known(self, account_id: int, chat_key: str, msg_id: int) -> bool:
        """Whether `account_id` already archived its message `msg_id` of `chat_key`."""
        row = self._conn.execute(
            "SELECT 1 FROM account_message_map "
            "WHERE account_id = ? AND chat_key = ? AND msg_id = ?",
            (account_id, chat_key, msg_id),
        ).fetchone()
        return row is not None

    def account_maps_canonical(self, account_id: int, chat_key: str, canonical_key: str) -> bool:
        """Whether one of `account_id`'s messages in `chat_key` is mapped to `canonical_key`."""
        row = self._conn.execute(
            "SELECT 1 FROM account_message_map "
            "WHERE account_id = ? AND chat_key = ? AND canonical_key = ? LIMIT 1",
            (account_id, chat_key, canonical_key),
        ).fetchone()
        return row is not None

    def mark_locator_deleted(self, chat_key: str, canonical_key: str) -> None:
        self._conn.execute(
            "UPDATE message_locator SET is_deleted = 1 WHERE chat_key = ? AND canonical_key = ?",
            (chat_key, canonical_key),
        )

    @staticmethod
    def _row_to_located(row: sqlite3.Row) -> LocatedMessage:
        return LocatedMessage(
            chat_key=str(row["chat_key"]),
            canonical_key=str(row["canonical_key"]),
            day_str=str(row["day_str"]),
            part_num=int(row["part_num"]),
            sender_id=int(row["sender_id"]) if row["sender_id"] is not None else None,
            is_deleted=bool(row["is_deleted"]),
        )

    # ------------------------------------------------------------------
    # Pending Media & Global Media Deduplication Index
    # ------------------------------------------------------------------
    def enqueue_pending_media(self, row: PendingMediaRow) -> None:
        self._conn.execute(
            """
            INSERT INTO pending_media (
                chat_key, canonical_key, account_id, msg_id, day_str,
                category, tg_type, tg_id, file_name, mime_type, size_bytes, downloaded
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(chat_key, canonical_key) DO UPDATE SET
                account_id = excluded.account_id,
                msg_id = excluded.msg_id,
                day_str = excluded.day_str
            WHERE pending_media.downloaded = 0
            """,
            (
                row.chat_key,
                row.canonical_key,
                row.account_id,
                row.msg_id,
                row.day_str,
                row.category,
                row.tg_type,
                row.tg_id,
                row.file_name,
                row.mime_type,
                row.size_bytes,
            ),
        )

    @staticmethod
    def _row_to_pending(r: sqlite3.Row) -> PendingMediaRow:
        return PendingMediaRow(
            chat_key=str(r["chat_key"]),
            canonical_key=str(r["canonical_key"]),
            account_id=int(r["account_id"]),
            msg_id=int(r["msg_id"]),
            day_str=str(r["day_str"]),
            category=r["category"],
            tg_type=str(r["tg_type"]),
            tg_id=int(r["tg_id"]),
            file_name=r["file_name"],
            mime_type=r["mime_type"],
            size_bytes=int(r["size_bytes"]) if r["size_bytes"] is not None else None,
            attempts=int(r["attempts"] or 0),
            last_error=r["last_error"],
        )

    def list_pending_media_page(
        self,
        chat_key: str,
        categories: Iterable[MediaCategory],
        *,
        after: tuple[str, int, str] | None = None,
        limit: int = PENDING_PAGE_SIZE,
        due_at: float | None = None,
    ) -> list[PendingMediaRow]:
        """One page of a chat's pending media, ordered by `(day_str, msg_id, canonical_key)`.

        Keyset pagination: pass the last row's `(day_str, msg_id, canonical_key)` as
        `after` to get the next page. With `due_at` (unix time), rows still in a
        permanent-failure backoff (`retry_after > due_at`) are left out.
        """
        cats = list(categories)
        if not cats:
            return []
        placeholders = ",".join("?" for _ in cats)
        sql = (
            "SELECT * FROM pending_media "
            f"WHERE chat_key = ? AND downloaded = 0 AND category IN ({placeholders})"
        )
        params: list[Any] = [chat_key, *cats]
        if due_at is not None:
            sql += " AND (retry_after IS NULL OR retry_after <= ?)"
            params.append(due_at)
        if after is not None:
            sql += " AND (day_str, msg_id, canonical_key) > (?, ?, ?)"
            params.extend(after)
        sql += " ORDER BY day_str ASC, msg_id ASC, canonical_key ASC LIMIT ?"
        params.append(int(limit))
        return [self._row_to_pending(r) for r in self._conn.execute(sql, params).fetchall()]

    def iter_pending_media_pages(
        self,
        chat_key: str,
        categories: Iterable[MediaCategory],
        *,
        page_size: int = PENDING_PAGE_SIZE,
        due_at: float | None = None,
    ) -> Iterator[list[PendingMediaRow]]:
        """Lazily yield pages of pending media (one query per page, nothing held open)."""
        cats = list(categories)
        after: tuple[str, int, str] | None = None
        while True:
            page = self.list_pending_media_page(
                chat_key, cats, after=after, limit=page_size, due_at=due_at
            )
            if not page:
                return
            yield page
            if len(page) < page_size:
                return
            last = page[-1]
            after = (last.day_str, last.msg_id, last.canonical_key)

    def list_pending_media(
        self,
        chat_key: str,
        categories: Iterable[MediaCategory],
    ) -> list[PendingMediaRow]:
        """Every pending item of the chat for `categories` (including backed-off ones)."""
        return [row for page in self.iter_pending_media_pages(chat_key, categories) for row in page]

    def pending_media_summary(
        self,
        chat_key: str,
        categories: Iterable[MediaCategory],
        *,
        max_bytes: int | None,
        due_at: float,
    ) -> dict[str, int]:
        """Counts for the drain: `due` (within the size limit, not backed off), `oversized`,
        `retrying` (due items that failed before) and `backed_off`."""
        cats = list(categories)
        if not cats:
            return {"due": 0, "oversized": 0, "retrying": 0, "backed_off": 0}
        placeholders = ",".join("?" for _ in cats)
        limit = max_bytes if max_bytes is not None else -1
        row = self._conn.execute(
            f"""
            SELECT
                SUM(CASE WHEN ? >= 0 AND COALESCE(size_bytes, 0) > ? THEN 1 ELSE 0 END)
                    AS oversized,
                SUM(CASE WHEN retry_after IS NOT NULL AND retry_after > ? THEN 1 ELSE 0 END)
                    AS backed_off,
                SUM(CASE WHEN NOT (? >= 0 AND COALESCE(size_bytes, 0) > ?)
                          AND (retry_after IS NULL OR retry_after <= ?) THEN 1 ELSE 0 END)
                    AS due,
                SUM(CASE WHEN NOT (? >= 0 AND COALESCE(size_bytes, 0) > ?)
                          AND (retry_after IS NULL OR retry_after <= ?)
                          AND last_error IS NOT NULL THEN 1 ELSE 0 END)
                    AS retrying
            FROM pending_media
            WHERE chat_key = ? AND downloaded = 0 AND category IN ({placeholders})
            """,
            (limit, limit, due_at, limit, limit, due_at, limit, limit, due_at, chat_key, *cats),
        ).fetchone()
        return {k: int(row[k] or 0) for k in ("due", "oversized", "retrying", "backed_off")}

    def record_media_failure(
        self,
        row: PendingMediaRow,
        error: str,
        *,
        permanent: bool = False,
        now: float | None = None,
    ) -> None:
        """Queue (or re-queue) media whose download failed so the next sync retries it.

        Uses the same `pending_media` queue drained by every `sync`. Transient failures are
        retried on every sync. `permanent=True` (e.g. the message is gone from the server)
        is retried on every sync for the first `PERMANENT_FAILURE_FREE_ATTEMPTS` attempts,
        then with an exponential backoff (`retry_after`); the row stays visible as pending.
        """
        retry_after: float | None = None
        if permanent:
            cur = self._conn.execute(
                "SELECT attempts FROM pending_media WHERE chat_key = ? AND canonical_key = ?",
                (row.chat_key, row.canonical_key),
            ).fetchone()
            attempts = (int(cur["attempts"] or 0) if cur is not None else 0) + 1
            retry_after = permanent_failure_retry_after(
                attempts, time.time() if now is None else now
            )
        self._conn.execute(
            """
            INSERT INTO pending_media (
                chat_key, canonical_key, account_id, msg_id, day_str,
                category, tg_type, tg_id, file_name, mime_type, size_bytes,
                downloaded, attempts, last_error, retry_after
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?)
            ON CONFLICT(chat_key, canonical_key) DO UPDATE SET
                account_id = excluded.account_id,
                msg_id = excluded.msg_id,
                day_str = excluded.day_str,
                downloaded = 0,
                attempts = pending_media.attempts + 1,
                last_error = excluded.last_error,
                retry_after = excluded.retry_after
            """,
            (
                row.chat_key,
                row.canonical_key,
                row.account_id,
                row.msg_id,
                row.day_str,
                row.category,
                row.tg_type,
                row.tg_id,
                row.file_name,
                row.mime_type,
                row.size_bytes,
                error,
                retry_after,
            ),
        )

    def count_failed_media(self) -> int:
        """Media still awaiting retry after a failed download (across all chats)."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM pending_media "
            "WHERE downloaded = 0 AND last_error IS NOT NULL"
        ).fetchone()
        return int(row["n"])

    def mark_pending_media_downloaded(self, chat_key: str, canonical_key: str) -> None:
        self.mark_pending_media_downloaded_many(chat_key, [canonical_key])

    def mark_pending_media_downloaded_many(
        self, chat_key: str, canonical_keys: Iterable[str]
    ) -> None:
        """Mark many queue rows as downloaded in one transaction."""
        keys = list(canonical_keys)
        if not keys:
            return
        self._conn.execute("BEGIN")
        try:
            self._conn.executemany(
                "UPDATE pending_media SET downloaded = 1, last_error = NULL, retry_after = NULL "
                "WHERE chat_key = ? AND canonical_key = ?",
                [(chat_key, k) for k in keys],
            )
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    def get_indexed_media(self, tg_type: str, tg_id: int) -> IndexedMedia | None:
        row = self._conn.execute(
            "SELECT * FROM media_index WHERE tg_type = ? AND tg_id = ?",
            (tg_type, tg_id),
        ).fetchone()
        if row is None:
            return None
        return IndexedMedia(
            tg_type=str(row["tg_type"]),
            tg_id=int(row["tg_id"]),
            category=str(row["category"]),
            sha256=str(row["sha256"]),
            size_bytes=int(row["size_bytes"]),
            rel_path=str(row["rel_path"]),
        )

    def register_indexed_media(
        self,
        *,
        tg_type: str,
        tg_id: int,
        category: str,
        sha256: str,
        size_bytes: int,
        rel_path: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO media_index
                (tg_type, tg_id, category, sha256, size_bytes, rel_path, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tg_type, tg_id) DO UPDATE SET
                sha256 = excluded.sha256,
                size_bytes = excluded.size_bytes,
                rel_path = excluded.rel_path
            """,
            (tg_type, tg_id, category, sha256, size_bytes, rel_path, utc_now_iso()),
        )

    # ------------------------------------------------------------------
    # Profile State Tracking (for profile_history.md)
    # ------------------------------------------------------------------
    def update_profile_state_and_diff(
        self,
        chat_key: str,
        new_state: dict[str, Any],
    ) -> list[tuple[str, Any, Any]]:
        """Compare `new_state` against last snapshot; return `[(field, old, new), ...]`."""
        row = self._conn.execute(
            "SELECT state_json FROM chat_profile_state WHERE chat_key = ?",
            (chat_key,),
        ).fetchone()
        diffs: list[tuple[str, Any, Any]] = []
        if row is not None:
            old_state: dict[str, Any] = json.loads(row["state_json"])
            for field_name, new_val in new_state.items():
                old_val = old_state.get(field_name)
                if old_val != new_val and (old_val is not None or new_val is not None):
                    diffs.append((field_name, old_val, new_val))

        self._conn.execute(
            """
            INSERT INTO chat_profile_state (chat_key, state_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_key) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (chat_key, json.dumps(new_state, ensure_ascii=False), utc_now_iso()),
        )
        return diffs

    # ------------------------------------------------------------------
    # Summary Stats & Reset (for stats & rebuild-checkpoint)
    # ------------------------------------------------------------------
    def reset_archive_indexes(self) -> None:
        """Clear message/range/media indexes while preserving authenticated accounts & PTS."""
        self._conn.execute("BEGIN")
        try:
            self._conn.execute("DELETE FROM chat_synced_ranges")
            self._conn.execute("DELETE FROM message_locator")
            self._conn.execute("DELETE FROM account_message_map")
            self._conn.execute("DELETE FROM media_index")
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise

    def summary_counts(self) -> dict[str, int]:
        total_msgs = self._conn.execute("SELECT COUNT(*) FROM message_locator").fetchone()[0]
        deleted_msgs = self._conn.execute(
            "SELECT COUNT(*) FROM message_locator WHERE is_deleted = 1"
        ).fetchone()[0]
        total_chats = self._conn.execute(
            "SELECT COUNT(DISTINCT chat_key) FROM message_locator"
        ).fetchone()[0]
        shared_chats = self._conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT chat_key FROM chat_account_cursors
                GROUP BY chat_key HAVING COUNT(DISTINCT account_id) > 1
            )
            """
        ).fetchone()[0]
        indexed_media = self._conn.execute("SELECT COUNT(*) FROM media_index").fetchone()[0]
        pending_media_cnt = self._conn.execute(
            "SELECT COUNT(*) FROM pending_media WHERE downloaded = 0"
        ).fetchone()[0]
        accounts_cnt = self._conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
        return {
            "accounts": int(accounts_cnt),
            "chats_with_messages": int(total_chats),
            "shared_chats": int(shared_chats),
            "messages": int(total_msgs),
            "deleted_messages": int(deleted_msgs),
            "unique_media_files": int(indexed_media),
            "pending_media": int(pending_media_cnt),
        }
