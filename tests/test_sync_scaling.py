"""Scaling of the sync on big, old archives: hole closing, the pending-media drain (batched
JSON attaches, paged queue, permanent-failure backoff) and the off-loop links rebuild."""

from __future__ import annotations

import asyncio
import json
import random
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import (
    FakeChannel,
    FakeMediaPhoto,
    FakeMessage,
    FakePhoto,
    FakeTelegramClient,
    FakeUser,
)
from tghoard.storage import checkpoint_db as cdb
from tghoard.storage.atomic import atomic_write_json, merge_ranges
from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow
from tghoard.storage.media_manager import MediaManager
from tghoard.storage.paths import (
    ChatPaths,
    iter_all_part_files,
    resolve_chat_paths,
)
from tghoard.sync import messages_sync
from tghoard.sync.messages_sync import sync_chat_messages
from tghoard.telegram.throttler import RateLimiter

CHAT = "channel_1"
RETRY_AFTER_SQL = "SELECT retry_after FROM pending_media WHERE msg_id = 2"
CHANNEL = FakeChannel(id=1, title="C", broadcast=True)
START = datetime(2026, 10, 1, tzinfo=UTC)


def _sel(photos: bool) -> ChatSelection:
    return ChatSelection(chat_key=CHAT, title="C", section="channels", photos=photos)


def _photo_msgs(ids: Any, photos: Any = None) -> list[FakeMessage]:
    photos = set(ids if photos is None else photos)
    return [
        FakeMessage(
            id=i,
            date=START + timedelta(seconds=i),
            message=f"m{i}",
            media=FakeMediaPhoto(photo=FakePhoto(id=50_000 + i)) if i in photos else None,
        )
        for i in ids
    ]


class Client(FakeTelegramClient):
    def __init__(self, msgs: list[FakeMessage]) -> None:
        super().__init__(me=FakeUser(id=111), dialogs=[CHANNEL], messages_by_entity_id={1: msgs})
        self.id_lookups: list[list[int]] = []

    async def get_messages(self, entity: Any, limit: int | None = None, ids=None, **kw: Any):
        if ids is not None:
            self.id_lookups.append(list(ids))
        return await super().get_messages(entity, limit=limit, ids=ids, **kw)


@dataclass
class Env:
    data_dir: Path
    db: CheckpointDB
    writer: ChatPartWriter
    media: MediaManager

    async def sync(self, client: Any, selection: ChatSelection) -> Any:
        return await sync_chat_messages(
            client=client,
            entity=CHANNEL,
            account_id=111,
            selection=selection,
            data_dir=self.data_dir,
            db=self.db,
            writer=self.writer,
            media_manager=self.media,
            limiter=RateLimiter(concurrency=4, min_delay=0.0, max_delay=0.0),
        )

    @property
    def paths(self) -> ChatPaths:
        return resolve_chat_paths(self.data_dir, CHAT, "C")

    def media_by_id(self) -> dict[int, dict[str, Any]]:
        return {
            m["id"]: m.get("media") or {}
            for p in iter_all_part_files(self.paths)
            for m in json.loads(p.read_text("utf-8"))["messages"]
        }


def _env(tmp_path: Path, writer: ChatPartWriter | None = None) -> Env:
    data_dir = tmp_path / "data"
    db = CheckpointDB(data_dir / ".tghoard" / "checkpoint.db")
    return Env(
        data_dir, db, writer or ChatPartWriter(), MediaManager(data_dir=data_dir, checkpoint_db=db)
    )


# ---------------------------------------------------------------------------------------
# Synced ranges
# ---------------------------------------------------------------------------------------
def test_closing_20k_holes_is_fast_and_exact(tmp_path: Path) -> None:
    db = CheckpointDB(tmp_path / "c.db")
    holes = 20_000
    # archive synced by an old version: one 1-id hole per 100-message batch (repro s11)
    db.add_synced_ranges(CHAT, [(i * 100 + 1, i * 100 + 99) for i in range(holes + 1)])
    missing = db.get_missing_ranges(CHAT, 1, (holes + 1) * 100 - 1)
    assert len(missing) == holes
    t0 = time.perf_counter()
    for i in range(0, holes, 100):  # what _sync_small_gaps does: one batch per request
        db.add_synced_ranges(CHAT, missing[i : i + 100])
    elapsed = time.perf_counter() - t0
    assert db.get_synced_ranges(CHAT) == [(1, (holes + 1) * 100 - 1)]
    assert elapsed < 1.0, f"closing {holes} holes took {elapsed:.2f}s"
    db.close()


def test_incremental_ranges_match_a_full_merge(tmp_path: Path) -> None:
    db = CheckpointDB(tmp_path / "c.db")
    rnd = random.Random(1234)
    reference: list[tuple[int, int]] = []
    for step in range(600):
        batch = []
        for _ in range(rnd.choice((1, 1, 3, 10))):
            a = rnd.randint(-5, 3000)
            b = a + rnd.choice((0, 0, 1, 5, 40, 400)) * rnd.choice((1, -1))
            batch.append((a, b))  # includes reversed and non-positive ranges
        if step % 2:
            db.add_synced_ranges(CHAT, batch)
        else:
            for a, b in batch:
                db.add_synced_range(CHAT, a, b)
        reference = merge_ranges([*reference, *batch])
        assert db.get_synced_ranges(CHAT) == reference
    # stored rows themselves stay merged (disjoint, non-adjacent)
    rows = db._conn.execute(
        "SELECT start_id, end_id FROM chat_synced_ranges WHERE chat_key = ? ORDER BY start_id",
        (CHAT,),
    ).fetchall()
    assert [(r[0], r[1]) for r in rows] == reference
    db.close()


@pytest.mark.asyncio
async def test_closing_many_holes_during_sync_keeps_the_loop_responsive(tmp_path: Path) -> None:
    env = _env(tmp_path)
    n_holes = 5000
    top = (n_holes + 1) * 100
    env.db.add_synced_ranges(CHAT, [(i * 100 + 1, i * 100 + 99) for i in range(n_holes + 1)])
    client = Client(_photo_msgs([1, top], photos=[]))  # holes are deleted ids
    max_gap = 0.0
    stop = asyncio.Event()

    async def ticker() -> None:
        nonlocal max_gap
        last = time.perf_counter()
        while not stop.is_set():
            await asyncio.sleep(0)
            now = time.perf_counter()
            max_gap, last = max(max_gap, now - last), now

    tick = asyncio.create_task(ticker())
    t0 = time.perf_counter()
    await env.sync(client, _sel(False))
    elapsed = time.perf_counter() - t0
    stop.set()
    await tick
    assert env.db.get_synced_ranges(CHAT) == [(1, top)]
    assert len(client.id_lookups) == n_holes // 100
    assert max_gap < 0.25, f"event loop blocked for {max_gap:.2f}s"
    assert elapsed < 5, f"sync took {elapsed:.1f}s"
    env.db.close()


# ---------------------------------------------------------------------------------------
# Pending-media drain
# ---------------------------------------------------------------------------------------
class BatchWriter(ChatPartWriter):
    """Counts the attach calls the drain makes."""

    def __init__(self) -> None:
        super().__init__()
        self.single_calls = 0
        self.batch_calls: list[tuple[str, int, int]] = []

    def attach_downloaded_media(self, **kw: Any) -> bool:
        self.single_calls += 1
        return super().attach_downloaded_media(**kw)

    def attach_downloaded_media_batch(self, **kw: Any) -> set[str]:
        self.batch_calls.append((kw["month"], kw["part_num"], len(kw["media_by_key"])))
        return super().attach_downloaded_media_batch(**kw)


@pytest.mark.asyncio
async def test_drain_writes_each_part_once_per_batch(tmp_path: Path) -> None:
    writer = BatchWriter()
    env = _env(tmp_path, writer)
    msgs = _photo_msgs(range(1, 301))
    await env.sync(Client(msgs), _sel(False))  # photos deferred -> 300 queued
    assert len(env.db.list_pending_media(CHAT, ["photos"])) == 300

    res = await env.sync(Client(msgs), _sel(True))
    assert res.retroactive_media_downloaded == 300
    assert writer.single_calls == 0  # never one full-part rewrite per file
    assert sum(n for _, _, n in writer.batch_calls) == 300
    assert len(writer.batch_calls) <= 3, writer.batch_calls
    assert env.db.list_pending_media(CHAT, ["photos"]) == []
    media = env.media_by_id()
    assert all(media[i]["downloaded"] and media[i]["path"] for i in range(1, 301))
    env.db.close()


@pytest.mark.asyncio
async def test_drain_repairs_stale_locators(tmp_path: Path) -> None:
    env = _env(tmp_path)
    msgs = _photo_msgs(range(1, 41))
    await env.sync(Client(msgs), _sel(False))
    # locators pointing at a part that doesn't hold the messages (e.g. interrupted repack)
    env.db._conn.execute("UPDATE message_locator SET part_num = 7 WHERE chat_key = ?", (CHAT,))
    res = await env.sync(Client(msgs), _sel(True))
    assert res.retroactive_media_downloaded == 40
    assert env.db.list_pending_media(CHAT, ["photos"]) == []
    assert all(m["downloaded"] for m in env.media_by_id().values())
    parts = {
        r[0]
        for r in env.db._conn.execute(
            "SELECT part_num FROM message_locator WHERE chat_key = ?", (CHAT,)
        )
    }
    assert parts == {0}  # re-pointed to where the messages really are
    env.db.close()


@pytest.mark.asyncio
async def test_downloaded_media_missing_from_json_stays_queued(tmp_path: Path) -> None:
    env = _env(tmp_path)
    msgs = _photo_msgs(range(1, 6))
    await env.sync(Client(msgs), _sel(False))
    # the JSON lost message 3 (e.g. edited by hand): attaching its media must not succeed
    (part,) = iter_all_part_files(env.paths)
    payload = json.loads(part.read_text("utf-8"))
    payload["messages"] = [m for m in payload["messages"] if m["id"] != 3]
    atomic_write_json(part, payload)

    res = await env.sync(Client(msgs), _sel(True))
    assert res.retroactive_media_downloaded == 4
    (left,) = env.db.list_pending_media(CHAT, ["photos"])
    assert left.msg_id == 3 and left.last_error.startswith("message_not_found_in_json")
    env.db.close()


@pytest.mark.asyncio
async def test_drain_streams_the_queue_in_pages(tmp_path: Path, monkeypatch) -> None:
    env = _env(tmp_path)
    msgs = _photo_msgs(range(1, 36))
    await env.sync(Client(msgs), _sel(False))
    monkeypatch.setattr(messages_sync, "PENDING_PAGE_SIZE", 10)
    pages: list[int] = []
    real_page = CheckpointDB.list_pending_media_page

    def spy(self: CheckpointDB, *a: Any, **kw: Any) -> list[PendingMediaRow]:
        rows = real_page(self, *a, **kw)
        pages.append(len(rows))
        return rows

    def forbidden(*a: Any, **kw: Any) -> None:
        raise AssertionError("the drain must not load the whole queue")

    monkeypatch.setattr(CheckpointDB, "list_pending_media_page", spy)
    monkeypatch.setattr(CheckpointDB, "list_pending_media", forbidden)
    res = await env.sync(Client(msgs), _sel(True))
    assert res.retroactive_media_downloaded == 35
    assert pages == [10, 10, 10, 5]
    env.db.close()


def test_pending_pages_are_keyset_ordered_and_skip_backoff(tmp_path: Path) -> None:
    db = CheckpointDB(tmp_path / "c.db")
    rows = []
    for i in range(2500):
        rows.append(
            PendingMediaRow(
                chat_key=CHAT,
                canonical_key=f"k{i % 7}-{i}",  # (day_str, msg_id) repeats across keys
                account_id=111,
                msg_id=i // 3,
                day_str=f"2026-{1 + i % 12:02d}",
                category="photos",
                tg_type="photo",
                tg_id=i,
                file_name=None,
                mime_type=None,
                size_bytes=10 if i % 50 else 10**9,
            )
        )
    for r in rows:
        db.enqueue_pending_media(r)
    pages = list(db.iter_pending_media_pages(CHAT, ["photos"]))
    assert [len(p) for p in pages] == [1000, 1000, 500]
    flat = [r for p in pages for r in p]
    keys = [(r.day_str, r.msg_id, r.canonical_key) for r in flat]
    assert keys == sorted(keys) and len(set(keys)) == 2500

    now = time.time()
    db.record_media_failure(rows[1], "gone", permanent=True, now=now)  # free attempt
    for _ in range(4):
        db.record_media_failure(rows[2], "gone", permanent=True, now=now)  # backed off
    due = [
        r.canonical_key
        for p in db.iter_pending_media_pages(CHAT, ["photos"], due_at=now)
        for r in p
    ]
    assert rows[1].canonical_key in due and rows[2].canonical_key not in due
    assert len(db.list_pending_media(CHAT, ["photos"])) == 2500  # still visible
    summary = db.pending_media_summary(CHAT, ["photos"], max_bytes=10**6, due_at=now)
    assert summary == {"due": 2449, "oversized": 50, "retrying": 1, "backed_off": 1}
    db.close()


def test_permanent_failure_backoff_schedule() -> None:
    day = 24 * 3600
    assert [cdb.permanent_failure_retry_after(n, 0) for n in (1, 2, 3)] == [None] * 3
    assert cdb.permanent_failure_retry_after(4, 0) == day
    assert cdb.permanent_failure_retry_after(5, 0) == 2 * day
    assert cdb.permanent_failure_retry_after(50, 0) == 30 * day  # capped


@pytest.mark.asyncio
async def test_media_gone_from_the_server_is_retried_with_backoff(tmp_path: Path) -> None:
    env = _env(tmp_path)
    msgs = _photo_msgs(range(1, 4))
    await env.sync(Client(msgs), _sel(False))
    without_2 = [m for m in msgs if m.id != 2]  # message 2 deleted on the server

    looked_up = []
    for _ in range(5):
        client = Client(without_2)
        await env.sync(client, _sel(True))
        looked_up.append(any(2 in ids for ids in client.id_lookups))
    assert looked_up == [True, True, True, True, False]  # 3 free retries, then backoff
    (row,) = env.db.list_pending_media(CHAT, ["photos"])  # still queued and visible
    assert row.msg_id == 2 and row.attempts == 4
    assert row.last_error == "message_or_media_not_found_on_server"
    assert env.db.count_failed_media() == 1
    retry_after = env.db._conn.execute(RETRY_AFTER_SQL).fetchone()[0]
    assert retry_after > time.time() + 23 * 3600

    # a transient failure (e.g. verify-media --fix re-queue) clears the backoff
    env.db.record_media_failure(row, "missing_on_disk: file does not exist")
    assert env.db._conn.execute(RETRY_AFTER_SQL).fetchone()[0] is None
    env.db.close()


def test_old_pending_table_gets_retry_after_and_order_index(tmp_path: Path) -> None:
    import sqlite3

    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE pending_media (chat_key TEXT NOT NULL, canonical_key TEXT NOT NULL, "
        "account_id INTEGER NOT NULL, msg_id INTEGER NOT NULL, day_str TEXT NOT NULL, "
        "category TEXT NOT NULL, tg_type TEXT NOT NULL, tg_id INTEGER NOT NULL, file_name TEXT, "
        "mime_type TEXT, size_bytes INTEGER, downloaded INTEGER NOT NULL DEFAULT 0, "
        "attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, "
        "PRIMARY KEY (chat_key, canonical_key))"
    )
    conn.commit()
    conn.close()
    db = CheckpointDB(db_path)
    cols = {r["name"] for r in db._conn.execute("PRAGMA table_info(pending_media)")}
    assert "retry_after" in cols
    plan = db._conn.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM pending_media WHERE chat_key = ? AND downloaded = 0 "
        "AND (day_str, msg_id, canonical_key) > (?, ?, ?) "
        "ORDER BY day_str, msg_id, canonical_key LIMIT 10",
        (CHAT, "2026-01", 1, "x"),
    ).fetchall()
    assert "idx_pending_media_order" in str([tuple(r) for r in plan])
    db.close()


# ---------------------------------------------------------------------------------------
# links.md / deleted_and_edits.md rebuild
# ---------------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_links_rebuild_runs_off_the_event_loop(tmp_path: Path, monkeypatch) -> None:
    env = _env(tmp_path)
    real = messages_sync.rebuild_links_and_audit_md_from_parts
    threads: list[bool] = []

    def spy(*a: Any, **kw: Any) -> Any:
        threads.append(threading.current_thread() is threading.main_thread())
        return real(*a, **kw)

    monkeypatch.setattr(messages_sync, "rebuild_links_and_audit_md_from_parts", spy)
    msgs = _photo_msgs(range(1, 6), photos=[])
    msgs[0].message = "see https://example.com/x"
    res = await env.sync(Client(msgs), _sel(False))
    assert threads == [False]  # ran in a worker thread
    assert res.links_total == 1 and "https://example.com/x" in env.paths.links_path.read_text()
    env.db.close()


@pytest.mark.asyncio
async def test_links_rebuild_is_redone_when_parts_change_meanwhile(
    tmp_path: Path, monkeypatch
) -> None:
    env = _env(tmp_path)
    real = messages_sync.rebuild_links_and_audit_md_from_parts
    calls = 0

    def concurrent_writer(chat_paths: ChatPaths, **kw: Any) -> Any:
        nonlocal calls
        calls += 1
        out = real(chat_paths, **kw)
        if calls == 1:  # e.g. another account's PTS replay edits a part right now
            (part,) = iter_all_part_files(chat_paths)
            payload = json.loads(part.read_text("utf-8"))
            payload["messages"][0]["links"] = ["https://late.example/"]
            atomic_write_json(part, payload)
        return out

    monkeypatch.setattr(messages_sync, "rebuild_links_and_audit_md_from_parts", concurrent_writer)
    res = await env.sync(Client(_photo_msgs(range(1, 4), photos=[])), _sel(False))
    assert calls == 2
    assert res.links_total == 1 and "https://late.example/" in env.paths.links_path.read_text()
    env.db.close()
