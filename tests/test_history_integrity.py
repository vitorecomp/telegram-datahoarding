"""History integrity: early-ending iterators never leave holes marked as synced, and the
DM/basic-group forward cursor can't be advanced by out-of-sequence writers (`watch`)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import FakeChannel, FakeChat, FakeMessage, FakeTelegramClient, FakeUser
from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.media_manager import MediaManager
from tghoard.storage.paths import iter_all_part_files, resolve_chat_paths
from tghoard.sync import messages_sync
from tghoard.sync.messages_sync import TAIL_CHECK_MAX_IDS, sync_chat_messages
from tghoard.telegram.throttler import RateLimiter

START = datetime(2026, 9, 1, tzinfo=UTC)
ME = FakeUser(id=111, first_name="V")
CHANNEL = FakeChannel(id=1, title="C", broadcast=True)
CHANNEL_SEL = ChatSelection(chat_key="channel_1", title="C", section="channels")
SAVED_SEL = ChatSelection(chat_key="saved_111", title="Saved", section="saved")


def _msgs(ids: Any, *, minutes: int = 1) -> list[FakeMessage]:
    return [
        FakeMessage(
            id=i, date=START + timedelta(minutes=minutes * i), sender_id=111, message=f"m{i}"
        )
        for i in ids
    ]


@dataclass
class Env:
    data_dir: Path
    db: CheckpointDB
    writer: ChatPartWriter
    media: MediaManager

    async def sync(
        self, client: Any, entity: Any, selection: ChatSelection, account_id: int = 111
    ) -> Any:
        return await sync_chat_messages(
            client=client,
            entity=entity,
            account_id=account_id,
            selection=selection,
            data_dir=self.data_dir,
            db=self.db,
            writer=self.writer,
            media_manager=self.media,
            limiter=RateLimiter(concurrency=2, min_delay=0.0, max_delay=0.0),
        )

    def archived_ids(self, selection: ChatSelection) -> list[int]:
        paths = resolve_chat_paths(self.data_dir, selection.chat_key, selection.title)
        ids = [
            m["id"]
            for p in iter_all_part_files(paths)
            for m in json.loads(p.read_text("utf-8"))["messages"]
        ]
        return sorted(ids)


@pytest.fixture
def env(tmp_path: Path):
    data_dir = tmp_path / "data"
    db = CheckpointDB(data_dir / ".tghoard" / "checkpoint.db")
    yield Env(data_dir, db, ChatPartWriter(), MediaManager(data_dir=data_dir, checkpoint_db=db))
    db.close()


class Client(FakeTelegramClient):
    """Fake client recording requests; `stop_after(call_no, kwargs)` -> id after which the
    history iterator silently ends (like Telethon on an empty/odd page), or None."""

    def __init__(self, msgs: list[FakeMessage], entity: Any = CHANNEL, stop_after: Any = None):
        super().__init__(me=ME, dialogs=[entity], messages_by_entity_id={entity.id: msgs})
        self.stop_after = stop_after
        self.id_lookups: list[list[int]] = []
        self.probes: list[dict[str, Any]] = []
        self.history_calls: list[dict[str, Any]] = []

    async def get_messages(self, entity: Any, limit: int | None = None, ids=None, **kw: Any):
        if ids is not None:
            self.id_lookups.append(list(ids))
        elif kw:
            self.probes.append(kw)
        return await super().get_messages(entity, limit=limit, ids=ids, **kw)

    async def iter_messages(self, entity: Any, **kw: Any):
        self.history_calls.append(kw)
        cutoff = self.stop_after(len(self.history_calls), kw) if self.stop_after else None
        async for m in super().iter_messages(entity, **kw):
            reverse = kw.get("reverse", False)
            if cutoff is not None and (m.id > cutoff if reverse else m.id < cutoff):
                return  # silent early end
            yield m


# ---------------------------------------------------------------------------------------
# Channels / supergroups
# ---------------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_channel_iterator_ending_early_leaves_no_false_coverage(env: Env) -> None:
    msgs = _msgs(range(1, 301))
    # the iterator always ends after id 150 (reviewer repro s10)
    client = Client(msgs, stop_after=lambda n, kw: 150)
    await env.sync(client, CHANNEL, CHANNEL_SEL)
    # the unexplained tail (151-300) was checked by id right away, not assumed empty
    assert env.archived_ids(CHANNEL_SEL) == list(range(1, 301))
    assert env.db.get_synced_ranges("channel_1") == [(1, 300)]


@pytest.mark.asyncio
async def test_channel_transient_early_end_resumes_paging(env: Env) -> None:
    msgs = _msgs(range(1, 1001))
    # only the first history request ends early (e.g. duplicated ids on a bad connection)
    client = Client(msgs, stop_after=lambda n, kw: 300 if n == 1 else None)
    await env.sync(client, CHANNEL, CHANNEL_SEL)
    assert env.archived_ids(CHANNEL_SEL) == list(range(1, 1001))
    assert env.db.get_synced_ranges("channel_1") == [(1, 1000)]
    assert len(client.history_calls) == 2 and client.history_calls[1]["min_id"] == 300
    assert client.id_lookups == []  # paging resumed; no id checks needed


@pytest.mark.asyncio
async def test_channel_permanent_stall_on_a_big_tail_is_walked_by_id(env: Env) -> None:
    n = 2 * TAIL_CHECK_MAX_IDS + 500
    msgs = _msgs(range(1, n + 1))
    # deterministic stall: paging never gets past id 150 (e.g. an all-MessageEmpty page)
    client = Client(msgs, stop_after=lambda c, kw: 150)
    await env.sync(client, CHANNEL, CHANNEL_SEL)
    assert env.archived_ids(CHANNEL_SEL) == list(range(1, n + 1))
    assert env.db.get_synced_ranges("channel_1") == [(1, n)]
    assert client.probes  # the big tail was probed before walking it by id


@pytest.mark.asyncio
async def test_interior_range_ending_in_deleted_ids_is_closed_by_id(env: Env) -> None:
    env.db.add_synced_ranges("channel_1", [(1, 100), (1201, 1300)])
    ids = [i for i in range(1, 1301) if not 1101 <= i <= 1200]  # 1101-1200 deleted
    client = Client(_msgs(ids))
    await env.sync(client, CHANNEL, CHANNEL_SEL)
    assert env.db.get_synced_ranges("channel_1") == [(1, 1300)]
    assert client.id_lookups == [list(range(1101, 1201))]  # one request for the tail
    assert env.archived_ids(CHANNEL_SEL) == list(range(101, 1101))


@pytest.mark.asyncio
async def test_big_empty_range_is_confirmed_by_a_probe(env: Env) -> None:
    env.db.add_synced_ranges("channel_1", [(1, 100), (5001, 5100)])
    client = Client(_msgs([*range(1, 101), *range(5001, 5101)]))  # 101-5000 deleted
    await env.sync(client, CHANNEL, CHANNEL_SEL)
    assert env.db.get_synced_ranges("channel_1") == [(1, 5100)]
    assert client.probes == [{"max_id": 5001, "min_id": 100}]
    assert client.id_lookups == []


@pytest.mark.asyncio
async def test_deleted_ids_inside_the_walked_range_are_still_covered(env: Env) -> None:
    ids = [i for i in range(1, 321) if i not in (101, 102, 103, 201)]
    client = Client(_msgs(ids))
    await env.sync(client, CHANNEL, CHANNEL_SEL)
    assert env.db.get_synced_ranges("channel_1") == [(1, 320)]
    assert client.id_lookups == [] and client.probes == []


# ---------------------------------------------------------------------------------------
# DMs / Saved Messages / basic groups
# ---------------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_dm_backfill_completion_is_confirmed(env: Env) -> None:
    msgs = _msgs(range(1, 301), minutes=600)
    # newest -> oldest; the first request ends after reaching id 151
    client = Client(msgs, entity=ME, stop_after=lambda n, kw: 151 if n == 1 else None)
    await env.sync(client, ME, SAVED_SEL)
    assert env.archived_ids(SAVED_SEL) == list(range(1, 301))
    cursor = env.db.get_chat_cursor("saved_111", 111)
    assert cursor["backfill_completed"] and cursor["forward_max_id"] == 300
    assert client.probes == [{"max_id": 151}, {"max_id": 1}]


@pytest.mark.asyncio
async def test_dm_backfill_stall_stays_incomplete_and_resumes(env: Env) -> None:
    msgs = _msgs(range(1, 301), minutes=600)
    stalled = Client(msgs, entity=ME, stop_after=lambda n, kw: 151)
    await env.sync(stalled, ME, SAVED_SEL)
    cursor = env.db.get_chat_cursor("saved_111", 111)
    assert not cursor["backfill_completed"]  # never assumed complete
    assert cursor["backfill_offset_id"] == 151

    await env.sync(Client(msgs, entity=ME), ME, SAVED_SEL)
    assert env.archived_ids(SAVED_SEL) == list(range(1, 301))
    assert env.db.get_chat_cursor("saved_111", 111)["backfill_completed"]


def _watch_saves(env: Env, msg_id: int) -> None:
    """What `tghoard watch` does for one live message: JSON + locator, no cursor."""
    paths = resolve_chat_paths(env.data_dir, "saved_111", "Saved")
    ckey = f"watch:{msg_id}"
    month = (START + timedelta(minutes=600 * msg_id)).strftime("%Y-%m")
    pmap, _ = env.writer.upsert_month_messages(
        chat_paths=paths,
        chat_key="saved_111",
        chat_title="Saved",
        month=month,
        incoming_messages=[
            {
                "id": msg_id,
                "canonical_key": ckey,
                "date": (START + timedelta(minutes=600 * msg_id)).isoformat(),
                "text": f"m{msg_id}",
            }
        ],
    )
    env.db.record_messages_batch(
        chat_key="saved_111",
        account_id=111,
        locators=[(ckey, month, pmap[ckey], 111, False)],
        account_mappings=[(msg_id, ckey)],
    )


@pytest.mark.asyncio
async def test_watch_recording_a_newer_message_does_not_hide_missed_ones(env: Env) -> None:
    msgs = _msgs(range(1, 21), minutes=600)
    await env.sync(Client(msgs[:10], entity=ME), ME, SAVED_SEL)
    _watch_saves(env, 20)  # 11-19 were missed while `watch` was disconnected (repro s6)

    client = Client(msgs, entity=ME)
    await env.sync(client, ME, SAVED_SEL)
    assert env.archived_ids(SAVED_SEL) == list(range(1, 21))  # 11-19 recovered, 20 once
    assert env.db.get_chat_cursor("saved_111", 111)["forward_max_id"] == 20
    assert client.history_calls[-1]["min_id"] == 10  # the sync-owned cursor, not MAX(id)


@pytest.mark.asyncio
async def test_legacy_db_without_forward_cursor_rescans_once(env: Env) -> None:
    msgs = _msgs(range(1, 21), minutes=600)
    await env.sync(Client(msgs[:10], entity=ME), ME, SAVED_SEL)
    _watch_saves(env, 20)
    # database from before `forward_max_id` existed
    env.db._conn.execute("UPDATE chat_account_cursors SET forward_max_id = NULL")

    rescan = Client(msgs, entity=ME)
    await env.sync(rescan, ME, SAVED_SEL)
    assert rescan.history_calls[-1]["min_id"] == 0  # whole history, known ids skipped
    assert env.archived_ids(SAVED_SEL) == list(range(1, 21))
    assert env.db.get_chat_cursor("saved_111", 111)["forward_max_id"] == 20

    after = Client(msgs, entity=ME)
    await env.sync(after, ME, SAVED_SEL)
    assert after.history_calls[-1]["min_id"] == 20  # one-time only


@pytest.mark.asyncio
async def test_legacy_db_with_backfill_in_progress_starts_at_its_offset(env: Env) -> None:
    msgs = _msgs(range(1, 301), minutes=600)
    stalled = Client(msgs, entity=ME, stop_after=lambda n, kw: 151)
    await env.sync(stalled, ME, SAVED_SEL)
    env.db._conn.execute("UPDATE chat_account_cursors SET forward_max_id = NULL")

    client = Client(msgs, entity=ME)
    await env.sync(client, ME, SAVED_SEL)
    forward = [kw for kw in client.history_calls if kw.get("reverse")]
    assert forward and forward[0]["min_id"] == 151  # [151, top] was walked by the backfill
    assert env.archived_ids(SAVED_SEL) == list(range(1, 301))


def test_old_cursor_table_gets_the_forward_column(tmp_path: Path) -> None:
    import sqlite3

    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE chat_account_cursors (chat_key TEXT NOT NULL, account_id INTEGER NOT NULL, "
        "folder_rel_path TEXT NOT NULL, backfill_offset_id INTEGER, "
        "backfill_completed INTEGER NOT NULL DEFAULT 0, last_story_id INTEGER, "
        "last_admin_event_id INTEGER, updated_at TEXT NOT NULL, "
        "PRIMARY KEY (chat_key, account_id))"
    )
    conn.execute(
        "INSERT INTO chat_account_cursors VALUES ('saved_111', 111, 'saved', NULL, 1, NULL, "
        "NULL, 'x')"
    )
    conn.commit()
    conn.close()

    db = CheckpointDB(db_path)
    cursor = db.get_chat_cursor("saved_111", 111)
    assert "forward_max_id" in cursor and cursor["forward_max_id"] is None
    db.upsert_chat_cursor(
        chat_key="saved_111", account_id=111, folder_rel_path="saved", forward_max_id=50
    )
    db.upsert_chat_cursor(
        chat_key="saved_111", account_id=111, folder_rel_path="saved", forward_max_id=40
    )
    assert db.get_chat_cursor("saved_111", 111)["forward_max_id"] == 50  # never moves back
    db.close()


# ---------------------------------------------------------------------------------------
# Service messages (no text, no media) in the same second
# ---------------------------------------------------------------------------------------
GROUP = FakeChat(id=42, title="G")
GROUP_SEL = ChatSelection(chat_key="group_42", title="G", section="groups")


@dataclass
class MessageActionChatAddUser:
    users: list[int]


@dataclass
class FakeServiceMessage(FakeMessage):
    action: Any = None


def _group_msgs(with_second_service: bool = True) -> list[FakeMessage]:
    same_second = START + timedelta(hours=5)
    msgs: list[FakeMessage] = [
        FakeMessage(id=4, date=START, sender_id=111, message="hello"),
        FakeServiceMessage(
            id=5, date=same_second, sender_id=111, action=MessageActionChatAddUser([201])
        ),
    ]
    if with_second_service:
        msgs.append(
            FakeServiceMessage(
                id=6, date=same_second, sender_id=111, action=MessageActionChatAddUser([202])
            )
        )
    return msgs


def _group_records(env: Env) -> list[dict[str, Any]]:
    paths = resolve_chat_paths(env.data_dir, "group_42", "G")
    return [m for p in iter_all_part_files(paths) for m in json.loads(p.read_text())["messages"]]


@pytest.mark.asyncio
async def test_two_service_messages_in_the_same_second_are_both_kept(env: Env) -> None:
    await env.sync(Client(_group_msgs(), entity=GROUP), GROUP, GROUP_SEL)
    records = _group_records(env)
    assert sorted(m["id"] for m in records) == [4, 5, 6]
    assert len({m["canonical_key"] for m in records}) == 3


@pytest.mark.asyncio
async def test_service_message_archived_before_content_tags_is_not_duplicated(
    env: Env, monkeypatch
) -> None:
    # account 222 archived the group with the old keys (no content tag)
    with monkeypatch.context() as m:
        m.setattr(messages_sync, "message_content_tag", lambda msg: None)
        await env.sync(Client(_group_msgs(False), entity=GROUP), GROUP, GROUP_SEL, account_id=222)
    # a second local account syncs the same group with the new code: same messages, no copies
    await env.sync(Client(_group_msgs(False), entity=GROUP), GROUP, GROUP_SEL)
    records = _group_records(env)
    assert sorted(m["id"] for m in records) == [4, 5]
    assert all({str(a) for a in m["seen_by_accounts"]} == {"111", "222"} for m in records)


@pytest.mark.asyncio
async def test_legacy_key_of_another_own_message_is_not_reused(env: Env, monkeypatch) -> None:
    with monkeypatch.context() as m:  # message 5 archived with the old key
        m.setattr(messages_sync, "message_content_tag", lambda msg: None)
        await env.sync(Client(_group_msgs(False), entity=GROUP), GROUP, GROUP_SEL)
    env.db._conn.execute("UPDATE chat_account_cursors SET forward_max_id = 5")
    # message 6 arrives later: its old key equals message 5's, which this account owns
    await env.sync(Client(_group_msgs(), entity=GROUP), GROUP, GROUP_SEL)
    records = _group_records(env)
    assert sorted(m["id"] for m in records) == [4, 5, 6]
