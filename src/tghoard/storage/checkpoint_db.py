"""SQLite WAL checkpoint and cross-account deduplication engine."""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterable
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

CREATE TABLE IF NOT EXISTS chat_profile_state (
    chat_key       TEXT PRIMARY KEY,
    state_json     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
"""


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
        try:
            os.chmod(self.db_path, 0o600)
        except OSError:
            pass

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
        return [(int(r["start_id"]), int(r["end_id"])) for r in rows]

    def add_synced_range(self, chat_key: str, start_id: int, end_id: int) -> list[tuple[int, int]]:
        current = self.get_synced_ranges(chat_key)
        merged = merge_ranges([*current, (start_id, end_id)])
        now = utc_now_iso()
        self._conn.execute("BEGIN")
        try:
            self._conn.execute(
                "DELETE FROM chat_synced_ranges WHERE chat_key = ?",
                (chat_key,),
            )
            self._conn.executemany(
                "INSERT INTO chat_synced_ranges (chat_key, start_id, end_id, updated_at) "
                "VALUES (?, ?, ?, ?)",
                [(chat_key, s, e, now) for s, e in merged],
            )
            self._conn.execute("COMMIT")
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        return merged

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
    ) -> None:
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
        self._conn.execute(
            """
            INSERT INTO chat_account_cursors (
                chat_key, account_id, folder_rel_path, backfill_offset_id,
                backfill_completed, last_story_id, last_admin_event_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_key, account_id) DO UPDATE SET
                folder_rel_path = excluded.folder_rel_path,
                backfill_offset_id = excluded.backfill_offset_id,
                backfill_completed = excluded.backfill_completed,
                last_story_id = excluded.last_story_id,
                last_admin_event_id = excluded.last_admin_event_id,
                updated_at = excluded.updated_at
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
            ),
        )

    def get_chat_folder_rel_path(self, chat_key: str) -> str | None:
        row = self._conn.execute(
            "SELECT folder_rel_path FROM chat_account_cursors "
            "WHERE chat_key = ? ORDER BY updated_at DESC LIMIT 1",
            (chat_key,),
        ).fetchone()
        return str(row["folder_rel_path"]) if row is not None else None

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

    def list_pending_media(
        self,
        chat_key: str,
        categories: Iterable[MediaCategory],
    ) -> list[PendingMediaRow]:
        cats = list(categories)
        if not cats:
            return []
        placeholders = ",".join("?" for _ in cats)
        rows = self._conn.execute(
            f"""
            SELECT * FROM pending_media
            WHERE chat_key = ? AND downloaded = 0 AND category IN ({placeholders})
            ORDER BY day_str ASC, msg_id ASC
            """,
            (chat_key, *cats),
        ).fetchall()
        return [
            PendingMediaRow(
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
            )
            for r in rows
        ]

    def mark_pending_media_downloaded(self, chat_key: str, canonical_key: str) -> None:
        self._conn.execute(
            "UPDATE pending_media SET downloaded = 1 WHERE chat_key = ? AND canonical_key = ?",
            (chat_key, canonical_key),
        )

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
