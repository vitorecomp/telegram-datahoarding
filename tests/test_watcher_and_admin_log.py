"""`tghoard watch`, Admin Log recovery and PTS-diff deletes write the monthly layout safely.

Regressions for: live/admin-log messages written to a per-*day* `YYYY-MM-DD-pt000.json` that
every later message overwrote; deletes dropped when the locator part number is stale; Rich
markup in chat titles aborting a multi-id delete event.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

import pytest
from rich.console import Console

from tests.conftest import FakeChannel, FakeMediaPhoto, FakeMessage, FakePhoto, FakeUser
from tghoard.config import AppConfig
from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB
from tghoard.storage.media_manager import MediaManager
from tghoard.storage.paths import (
    ChatPaths,
    iter_all_part_files,
    list_month_part_files,
    resolve_chat_paths,
)
from tghoard.sync import pts_diff
from tghoard.sync.metadata_sync import sync_chat_metadata
from tghoard.sync.reporter import SyncReporter
from tghoard.sync.watcher import (
    LiveWatchContext,
    handle_message_deleted,
    handle_message_edited,
    handle_new_message,
)
from tghoard.telegram.throttler import RateLimiter

ME = 111
TITLE = "Live [/x] [bold]chat"  # Rich-markup look-alikes must not break printing
CHAT_KEY = "supergroup_777"


def _day(minute: int, day: int = 9) -> datetime:
    return datetime(2026, 11, day, 10, minute, 0, tzinfo=UTC)


class _Event:
    def __init__(
        self,
        chat: Any,
        *,
        message: Any = None,
        deleted_ids: list[int] | None = None,
        chat_id: int | None = None,
    ) -> None:
        self._chat = chat
        self.message = message
        self.deleted_ids = deleted_ids or []
        self.chat_id = chat_id

    async def get_chat(self) -> Any:
        return self._chat


def _ctx(tmp_path: Path, *, chat_key: str = CHAT_KEY, title: str = TITLE) -> LiveWatchContext:
    cfg = AppConfig(data_dir=tmp_path / "data")
    db = CheckpointDB(cfg.data_dir / ".tghoard" / "checkpoint.db")
    account = AccountRecord(
        account_id=ME, alias="me", phone=None, username=None, display_name=None, added_at="x"
    )
    return LiveWatchContext(
        client=None,
        account=account,
        selections={chat_key: ChatSelection(chat_key=chat_key, title=title)},
        config=cfg,
        db=db,
        writer=ChatPartWriter(max_json_bytes=cfg.max_json_bytes),
        media_manager=MediaManager(data_dir=cfg.data_dir, checkpoint_db=db),
        limiter=RateLimiter(concurrency=1, min_delay=0.0, max_delay=0.0),
        console=Console(file=StringIO(), width=200),
    )


def _chat_paths(ctx: LiveWatchContext, chat_key: str = CHAT_KEY) -> ChatPaths:
    return resolve_chat_paths(ctx.config.data_dir, chat_key, ctx.selections[chat_key].title)


def _messages(cp: ChatPaths) -> list[dict[str, Any]]:
    return [
        m
        for p in iter_all_part_files(cp)
        for m in json.loads(p.read_text(encoding="utf-8"))["messages"]
    ]


def _output(ctx: LiveWatchContext) -> str:
    return ctx.console.file.getvalue()  # type: ignore[attr-defined]


GROUP = FakeChannel(id=777, title=TITLE, megagroup=True)


def test_live_messages_of_the_same_day_all_survive_in_the_monthly_part(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    msgs = [FakeMessage(id=i, date=_day(i), message=f"live {i}", sender_id=5) for i in (1, 2, 3)]
    photo = FakeMessage(
        id=4, date=_day(4), sender_id=5, media=FakeMediaPhoto(photo=FakePhoto(id=9001))
    )

    async def run() -> None:
        for m in [*msgs, photo]:
            await handle_new_message(ctx, _Event(GROUP, message=m))
        await handle_new_message(ctx, _Event(GROUP, message=msgs[0]))  # re-delivered

    asyncio.run(run())

    cp = _chat_paths(ctx)
    assert [p.name for p in cp.messages_dir.iterdir()] == ["2026-11-pt000.json"]
    assert [m["id"] for m in _messages(cp)] == [1, 2, 3, 4]
    for mid in (1, 2, 3, 4):
        (loc,) = ctx.db.locate_by_account_msg(ME, mid, chat_key=CHAT_KEY)
        assert (loc.day_str, loc.part_num) == ("2026-11", 0)
    # photos unchecked -> queued for later, keyed by month like `sync` does
    (row,) = ctx.db.list_pending_media(CHAT_KEY, ["photos"])
    assert row.day_str == "2026-11" and row.msg_id == 4
    assert "saved" in _output(ctx) and "[/x]" in _output(ctx)


def test_live_dm_service_messages_in_the_same_second_are_both_kept(tmp_path: Path) -> None:
    peer = FakeUser(id=500, first_name="Ana")
    dm_key = "dm_111_500"
    ctx = _ctx(tmp_path, chat_key=dm_key, title="Ana")

    class MessageActionChatAddUser:
        def __init__(self, users: list[int]) -> None:
            self.users = users

    def svc(mid: int, user: int) -> FakeMessage:
        m = FakeMessage(id=mid, date=_day(0), sender_id=500)
        m.action = MessageActionChatAddUser([user])  # type: ignore[attr-defined]
        return m

    asyncio.run(handle_new_message(ctx, _Event(peer, message=svc(101, 1))))
    asyncio.run(handle_new_message(ctx, _Event(peer, message=svc(102, 2))))
    stored = _messages(_chat_paths(ctx, dm_key))
    assert sorted(m["service_action"]["users"] for m in stored) == [[1], [2]]
    assert len({m["canonical_key"] for m in stored}) == 2

    # Re-processing without checkpoint state (content-derived key) adds nothing
    ctx.db.close()
    ctx2 = _ctx(tmp_path / "fresh-db", chat_key=dm_key, title="Ana")
    ctx2.config = ctx.config
    ctx2.writer = ctx.writer
    asyncio.run(handle_new_message(ctx2, _Event(peer, message=svc(101, 1))))
    assert len(_messages(_chat_paths(ctx, dm_key))) == 2


def _archive_with_parts(ctx: LiveWatchContext, n: int = 12) -> dict[int, int]:
    """Archive `n` big messages through the watcher with tiny parts; return id -> part."""
    ctx.writer = ChatPartWriter(max_json_bytes=4096)
    for i in range(1, n + 1):
        msg = FakeMessage(id=i, date=_day(i), message=f"msg {i} " + "z" * 1500, sender_id=5)
        asyncio.run(handle_new_message(ctx, _Event(GROUP, message=msg)))
    return {
        i: ctx.db.locate_by_account_msg(ME, i, chat_key=CHAT_KEY)[0].part_num
        for i in range(1, n + 1)
    }


def test_multi_id_delete_with_markup_title_and_a_failing_id_marks_the_rest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = _ctx(tmp_path)
    parts = _archive_with_parts(ctx)
    assert len(set(parts.values())) >= 3

    # Stale locator: message 2 claims to be in the last part
    stale = max(parts.values())
    ctx.db.update_day_part_numbers(CHAT_KEY, "2026-11", {"id:2": stale})
    real_mark = ctx.writer.locate_and_mark_deleted

    def flaky(**kwargs: Any) -> Any:
        if kwargs["canonical_key"] == "id:5":
            raise OSError("disk hiccup [/red]")
        return real_mark(**kwargs)

    monkeypatch.setattr(ctx.writer, "locate_and_mark_deleted", flaky)
    event = _Event(GROUP, deleted_ids=[2, 5, 7, 9], chat_id=-100777)
    asyncio.run(handle_message_deleted(ctx, event))

    by_id = {m["id"]: m for m in _messages(_chat_paths(ctx))}
    assert {i for i, m in by_id.items() if m["is_deleted"]} == {2, 7, 9}
    assert len(by_id) == 12
    # locator healed to the real part
    (loc,) = ctx.db.locate_by_account_msg(ME, 2, chat_key=CHAT_KEY)
    assert loc.part_num == parts[2] and loc.is_deleted
    out = _output(ctx)
    assert "could not record deletion of msg #5" in out and "disk hiccup [/red]" in out
    assert out.count("deleted remotely") == 3


def test_delete_whose_record_is_missing_is_reported(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _archive_with_parts(ctx, n=3)
    ctx.db.record_messages_batch(
        chat_key=CHAT_KEY,
        account_id=ME,
        locators=[("id:50", "2026-11", 0, None, False)],
        account_mappings=[(50, "id:50")],
    )
    asyncio.run(handle_message_deleted(ctx, _Event(GROUP, deleted_ids=[50], chat_id=-100777)))
    assert "was not found" in _output(ctx)


def test_live_edit_with_legacy_day_locator_versions_and_heals_it(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _archive_with_parts(ctx, n=6)
    # Locator written by the old watcher: full date as day_str and a wrong part
    ctx.db.record_messages_batch(
        chat_key=CHAT_KEY,
        account_id=ME,
        locators=[("id:3", "2026-11-09", 7, 5, False)],
        account_mappings=[(3, "id:3")],
    )
    edited = FakeMessage(
        id=3, date=_day(3), message="msg 3 fixed", sender_id=5, edit_date=_day(30, day=10)
    )
    asyncio.run(handle_message_edited(ctx, _Event(GROUP, message=edited)))
    (rec,) = [m for m in _messages(_chat_paths(ctx)) if m["id"] == 3]
    assert rec["text"] == "msg 3 fixed" and rec["revisions"][0]["text"].startswith("msg 3 z")
    (loc,) = ctx.db.locate_by_account_msg(ME, 3, chat_key=CHAT_KEY)
    assert loc.day_str == "2026-11"
    assert any(
        m["id"] == 3
        for _, p in list_month_part_files(_chat_paths(ctx), "2026-11")
        if p.name.endswith(f"pt{loc.part_num:03d}.json")
        for m in json.loads(p.read_text(encoding="utf-8"))["messages"]
    )
    assert "versioned" in _output(ctx)


# -- Admin Log recovery ----------------------------------------------------------------------


class ChannelAdminLogEventActionDeleteMessage:
    def __init__(self, message: Any) -> None:
        self.message = message


class _AdminEvent:
    def __init__(self, ev_id: int, message: Any) -> None:
        self.id = ev_id
        self.date = _day(59, day=10)
        self.action = ChannelAdminLogEventActionDeleteMessage(message)


class _AdminClient:
    def __init__(self, events: list[_AdminEvent]) -> None:
        self._events = events

    async def iter_admin_log(self, entity: Any, limit: int = 100, min_id: int = 0):
        for ev in self._events:
            if ev.id > min_id:
                yield ev


def test_admin_log_delete_flags_the_archived_record_and_recovers_unarchived_ones(
    tmp_path: Path,
) -> None:
    ctx = _ctx(tmp_path)
    _archive_with_parts(ctx, n=3)
    admin_group = FakeChannel(id=777, title=TITLE, megagroup=True, admin_rights=object())
    deleted_known = FakeMessage(id=2, date=_day(2), message="msg 2 edited before delete")
    deleted_unknown = FakeMessage(id=40, date=_day(40), message="never synced", sender_id=6)
    client = _AdminClient([_AdminEvent(1, deleted_known), _AdminEvent(2, deleted_unknown)])

    asyncio.run(
        sync_chat_metadata(
            client=client,
            entity=admin_group,
            account_id=ME,
            account_alias="me",
            selection=ctx.selections[CHAT_KEY],
            data_dir=ctx.config.data_dir,
            db=ctx.db,
            writer=ctx.writer,
            media_manager=ctx.media_manager,
            limiter=ctx.limiter,
            sync_members=False,
            sync_stories=False,
            sync_profile_photos=False,
        )
    )
    cp = _chat_paths(ctx)
    assert all(p.name.startswith("2026-11-pt") for p in iter_all_part_files(cp))
    assert sorted(p.name for p in cp.messages_dir.iterdir()) == [
        p.name for p in iter_all_part_files(cp)
    ]
    msgs = _messages(cp)
    assert [m["id"] for m in msgs] == [1, 2, 3, 40]
    by_id = {m["id"]: m for m in msgs}
    assert by_id[2]["is_deleted"] and by_id[2]["delete_source"] == "admin_log"
    assert by_id[2]["text"] == "msg 2 edited before delete"
    assert by_id[2]["revisions"][0]["text"].startswith("msg 2 z")
    assert by_id[40]["is_deleted"] and not by_id[1]["is_deleted"]
    for mid in (2, 40):
        (loc,) = ctx.db.locate_by_account_msg(ME, mid, chat_key=CHAT_KEY)
        assert loc.day_str == "2026-11" and loc.is_deleted

    # Replaying the same events (cursor reset) never duplicates
    ctx.db.upsert_chat_cursor(
        chat_key=CHAT_KEY, account_id=ME, folder_rel_path=cp.folder, last_admin_event_id=0
    )
    asyncio.run(
        sync_chat_metadata(
            client=client,
            entity=admin_group,
            account_id=ME,
            account_alias="me",
            selection=ctx.selections[CHAT_KEY],
            data_dir=ctx.config.data_dir,
            db=ctx.db,
            writer=ctx.writer,
            media_manager=ctx.media_manager,
            limiter=ctx.limiter,
            sync_members=False,
            sync_stories=False,
            sync_profile_photos=False,
        )
    )
    assert [m["id"] for m in _messages(cp)] == [1, 2, 3, 40]


# -- PTS diff --------------------------------------------------------------------------------


def test_pts_diff_delete_with_stale_part_is_applied_and_missing_one_reported(
    tmp_path: Path,
) -> None:
    ctx = _ctx(tmp_path)
    parts = _archive_with_parts(ctx)
    stale = max(parts.values())
    ctx.db.update_day_part_numbers(CHAT_KEY, "2026-11", {"id:1": stale})
    ctx.db.record_messages_batch(
        chat_key=CHAT_KEY,
        account_id=ME,
        locators=[("id:99", "2026-11", 0, None, False)],
        account_mappings=[(99, "id:99")],
    )
    out = StringIO()
    reporter = SyncReporter(Console(file=out, width=200))
    stats = pts_diff.PtsDiffStats()
    pts_diff._apply_deleted_msg_ids(
        account_id=ME,
        msg_ids=[1, 99],
        chat_key_hint=CHAT_KEY,
        selections=ctx.selections,
        data_dir=ctx.config.data_dir,
        db=ctx.db,
        writer=ctx.writer,
        source="pts_diff",
        stats=stats,
        reporter=reporter,
    )
    assert stats.deleted_marked == 1
    assert stats.delete_targets_missing == [f"{CHAT_KEY}#99"]
    assert "msg #99" in out.getvalue() and "[/x]" in out.getvalue()
    (rec,) = [m for m in _messages(_chat_paths(ctx)) if m["id"] == 1]
    assert rec["is_deleted"] and rec["delete_source"] == "pts_diff"
    (loc,) = ctx.db.locate_by_account_msg(ME, 1, chat_key=CHAT_KEY)
    assert loc.part_num == parts[1] and loc.is_deleted
