"""Auto-throttle: FloodWaits (incl. Telethon's silent sleeps) adapt pause and parallelism."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from telethon.client.users import _fmt_flood
from telethon.tl import functions

from tests.conftest import FakeChannel, FakeMessage, FakeTelegramClient, FakeUser
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.live_state import LiveSyncState
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.telegram import flood_control
from tghoard.telegram.flood_control import DynamicSlots, FloodController


def _history_req() -> Any:
    return functions.messages.GetHistoryRequest(
        peer="me",
        offset_id=0,
        offset_date=None,
        add_offset=0,
        limit=100,
        max_id=0,
        min_id=0,
        hash=0,
    )


def test_aimd_backoff_and_recovery() -> None:
    changes: list[str] = []
    c = FloodController(
        base_wait=0.0,
        max_concurrency=4,
        on_change=changes.append,
        recover_after=0.05,
        cut_cooldown=0,
    )
    seen: list[int] = []
    c.on_concurrency(seen.append)

    c.record(7, "GetHistoryRequest")
    assert c.history_wait == 0.25  # from 0 -> minimum backoff
    c.record(7, "GetHistoryRequest")
    assert c.history_wait == 0.5  # doubles
    c.record(3, "GetFileRequest")
    assert c.concurrency == 2 and seen[-1] == 2  # non-history flood halves parallelism
    c.record(3, "GetFileRequest")
    c.record(3, "GetFileRequest")
    assert c.concurrency == 1  # never below 1
    assert c.floods == 5 and any("🐢" in m for m in changes)

    c.maybe_recover()  # too soon: nothing changes
    assert (c.history_wait, c.concurrency) == (0.5, 1)
    time.sleep(0.06)
    c.maybe_recover()
    assert (c.history_wait, c.concurrency) == (0.25, 2)  # halves wait, +1 parallel
    for _ in range(4):
        time.sleep(0.06)
        c.maybe_recover()
    assert (c.history_wait, c.concurrency) == (0.0, 4)  # back to floor / ceiling
    assert any("🐇" in m for m in changes)


def test_floor_ceiling_cap_and_disabled() -> None:
    c = FloodController(base_wait=0.3, max_concurrency=3, cut_cooldown=0)
    for _ in range(20):
        c.record(30, "GetHistoryRequest")
    assert c.history_wait == 5.0  # capped
    off = FloodController(base_wait=0.3, max_concurrency=3, enabled=False)
    off.record(30, "GetHistoryRequest")
    off.record(30, "GetFileRequest")
    assert (off.history_wait, off.concurrency, off.floods) == (0.3, 3, 2)  # counted only


def test_telethon_silent_flood_sleep_reaches_the_right_account() -> None:
    a, b = FloodController(max_concurrency=4), FloodController(max_concurrency=4)
    flood_control.register(111, a)
    flood_control.register(222, b)
    try:
        # Exactly what Telethon logs (users.py) on the per-account base_logger
        log = logging.getLogger(f"{flood_control.client_logger_name(111)}.client.users")
        log.info(*_fmt_flood(12, _history_req()))
        log.info(*_fmt_flood(4, _history_req(), early=True))
        assert a.floods == 2 and a.history_wait == 0.5
        assert b.floods == 0  # other account untouched
        logging.getLogger("tghoard.tg.222.client.users").info("unrelated %s", "message")
        assert b.floods == 0
    finally:
        flood_control.unregister(111)
        flood_control.unregister(222)


@pytest.mark.asyncio
async def test_dynamic_slots_follow_the_limit() -> None:
    slots = DynamicSlots(4)
    active = peak = 0

    async def work() -> None:
        nonlocal active, peak
        async with slots:
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.02)
            active -= 1

    await asyncio.gather(*(work() for _ in range(12)))
    assert peak == 4
    slots.set_limit(1)
    peak = 0
    await asyncio.gather(*(work() for _ in range(5)))
    assert peak == 1
    slots.set_limit(3)
    peak = 0
    await asyncio.gather(*(work() for _ in range(9)))
    assert peak == 3


class FloodingClient(FakeTelegramClient):
    """Behaves like Telethon hitting FloodWaits: logs the silent sleep on the account logger."""

    async def iter_messages(self, entity: Any, **kwargs: Any):
        log = logging.getLogger(f"{flood_control.client_logger_name(111)}.client.users")
        n = 0
        async for m in super().iter_messages(entity, **kwargs):
            n += 1
            if n in (50, 150):
                log.info(*_fmt_flood(2, _history_req()))
            yield m


@pytest.mark.asyncio
async def test_sync_slows_down_on_flood_waits_and_reports_it(tmp_path: Path, monkeypatch) -> None:
    pauses: list[float] = []

    async def fake_page_pause(self: FloodController) -> None:
        pauses.append(self.history_wait)

    monkeypatch.setattr(FloodController, "page_pause", fake_page_pause)
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    ch = FakeChannel(id=1, title="Grande", broadcast=True)
    msgs = [
        FakeMessage(id=i, date=datetime(2026, 11, 9, 12, i // 60 % 60, i % 60, tzinfo=UTC))
        for i in range(1, 301)
    ]
    client = FloodingClient(
        me=FakeUser(id=111, first_name="V"), dialogs=[ch], messages_by_entity_id={1: msgs}
    )
    live = LiveSyncState()
    await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_1": ChatSelection(chat_key="channel_1", title="Grande", section="channels")
        },
        config=cfg,
        db=db,
        live=live,
    )
    db.close()
    # After the 1st flood (msg 50) the pause between pages grows, after the 2nd it doubles
    assert pauses == [0.25, 0.5, 0.5]
    acct = live.snapshot()["accounts"][0]
    assert acct["floods"] == 2 and "0.50s/page" in acct["pace"]
    assert any("FloodWait of 2s on GetHistoryRequest" in e["msg"] for e in live.snapshot()["log"])


def test_cut_is_geometric_and_recovery_is_linear() -> None:
    c = FloodController(max_concurrency=5, recover_after=0.01, cut_cooldown=0)
    cuts = []
    for _ in range(4):
        c.record(5, "GetFileRequest")
        cuts.append(c.concurrency)
    assert cuts == [2, 1, 1, 1]  # PG with ratio 1/2 (5 -> 2 -> 1), never below 1
    climb = []
    for _ in range(6):
        time.sleep(0.015)
        c.maybe_recover()
        climb.append(c.concurrency)
    assert climb == [2, 3, 4, 5, 5, 5]  # +1 per quiet period, capped at the ceiling
    gentle = FloodController(max_concurrency=40, cut_ratio=0.75, cut_cooldown=0)
    gentle.record(5, "GetFileRequest")
    gentle.record(5, "GetFileRequest")
    assert gentle.concurrency == 22  # 40 * 0.75 = 30 -> 30 * 0.75 = 22


def test_only_file_requests_cut_downloads_and_bursts_count_once() -> None:
    c = FloodController(max_concurrency=16)
    for name in ("GetMessagesRequest", "GetFullChannelRequest", "GetHistoryRequest"):
        c.record(5, name)
    assert c.concurrency == 16  # API floods never touch download parallelism...
    assert c.history_wait > 0  # ...they back off the API pause instead
    for _ in range(8):  # one burst hitting 8 parallel downloads
        c.record(5, "GetFileRequest")
    assert c.concurrency == 8  # one cut, not 16 -> 1
    assert FloodController(max_concurrency=16).describe().startswith("pause 0.00s/page")
