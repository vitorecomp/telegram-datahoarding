"""Failure isolation: one chat's/account's error must not take the rest of the sync down."""

from __future__ import annotations

import asyncio
import errno
import io
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from rich.console import Console
from telethon import errors as tg_errors

from tests.conftest import (
    FakeChannel,
    FakeMediaPhoto,
    FakeMessage,
    FakePhoto,
    FakeTelegramClient,
    FakeUser,
)
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync import orchestrator
from tghoard.sync.media_pool import MediaDownloadPool
from tghoard.sync.orchestrator import (
    ArchiveStorageError,
    OrchestratorStats,
    run_multi_account_sync,
)
from tghoard.telegram import throttler
from tghoard.telegram.flood_control import FloodController
from tghoard.telegram.throttler import RateLimiter, retry_transient

_real_sleep = asyncio.sleep


@pytest.fixture
def fast_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """No real backoff waits, but still yield to the loop (other accounts keep running)."""
    slept: list[float] = []

    async def fake_sleep(secs: float, *a: Any) -> None:
        slept.append(secs)
        await _real_sleep(0)

    monkeypatch.setattr(throttler.asyncio, "sleep", fake_sleep)
    return slept


def _msgs(n: int) -> list[FakeMessage]:
    return [
        FakeMessage(
            id=i, date=datetime(2026, 11, 9, 12, i // 60, i % 60, tzinfo=UTC), message=f"m{i}"
        )
        for i in range(1, n + 1)
    ]


def _sel(chat_key: str, title: str, **kw: Any) -> ChatSelection:
    return ChatSelection(chat_key=chat_key, title=title, section="channels", baixar=True, **kw)


class SlowClient(FakeTelegramClient):
    """Yields between messages, so a second account is mid-sync when the first one fails."""

    async def iter_messages(self, *a: Any, **k: Any):
        async for m in super().iter_messages(*a, **k):
            await _real_sleep(0.002)
            yield m


def _two_accounts(tmp_path: Path, exc_factory: Any):
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", parallel_chats=1)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    a1 = db.upsert_account(account_id=111, alias="a1")
    a2 = db.upsert_account(account_id=222, alias="a2")
    c1 = FakeTelegramClient(
        me=FakeUser(id=111),
        dialogs=[
            FakeChannel(id=1001, title="AAA bad", broadcast=True),
            FakeChannel(id=1002, title="ZZZ good", broadcast=True),
        ],
        messages_by_entity_id={1001: _msgs(10), 1002: _msgs(10)},
        fail_after_iter_count=0,
        fail_exc_factory=exc_factory,
        fail_entity_ids={1001},
    )
    c2 = SlowClient(
        me=FakeUser(id=222),
        dialogs=[FakeChannel(id=2001, title="other", broadcast=True)],
        messages_by_entity_id={2001: _msgs(50)},
    )
    sels = {
        "channel_1001": _sel("channel_1001", "AAA bad"),
        "channel_1002": _sel("channel_1002", "ZZZ good"),
        "channel_2001": _sel("channel_2001", "other"),
    }
    return cfg, db, [(a1, c1), (a2, c2)], sels


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exc_factory", "expected"),
    [
        (lambda: ConnectionError("conn reset"), "ConnectionError: conn reset"),
        (lambda: OSError(errno.EIO, "Input/output error"), "OSError: [Errno 5]"),
        (lambda: json.JSONDecodeError("Expecting value", "", 0), "JSONDecodeError"),
        (lambda: sqlite3.OperationalError("disk I/O error"), "OperationalError: disk I/O"),
        (lambda: KeyError("boom"), "KeyError"),
        # what awaiting a request future Telethon cancelled on disconnect raises
        (lambda: asyncio.CancelledError(), "ConnectionError: request cancelled by Telethon"),
    ],
)
async def test_non_rpc_error_skips_only_that_chat(
    tmp_path: Path, fast_sleep: list[float], exc_factory: Any, expected: str
) -> None:
    cfg, db, clients, sels = _two_accounts(tmp_path, exc_factory)
    stats = await run_multi_account_sync(
        account_clients=clients,
        selections=sels,
        config=cfg,
        db=db,
        console=Console(file=io.StringIO()),
    )
    assert [(a, k) for a, k, _ in stats.failed_chats] == [("a1", "channel_1001")]
    assert stats.failed_chats[0][2].startswith(expected)
    assert stats.failed_accounts == []
    assert db.get_synced_ranges("channel_1002") == [(1, 10)]  # same account, next chat
    assert db.get_synced_ranges("channel_2001") == [(1, 50)]  # other account, untouched
    db.close()


@pytest.mark.asyncio
async def test_disk_full_stops_the_whole_run(tmp_path: Path, fast_sleep: list[float]) -> None:
    cfg, db, clients, sels = _two_accounts(
        tmp_path, lambda: OSError(errno.ENOSPC, "No space left on device")
    )
    buf = io.StringIO()
    with pytest.raises(ArchiveStorageError, match="No space left"):
        await run_multi_account_sync(
            account_clients=clients,
            selections=sels,
            config=cfg,
            db=db,
            console=Console(file=buf, width=300),
        )
    assert "Sync stopped: the archive can't be written" in buf.getvalue()
    leftovers = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert leftovers == []
    db.close()


@pytest.mark.asyncio
async def test_storage_failure_classification(tmp_path: Path) -> None:
    probe_ok = tmp_path
    gone = tmp_path / "unplugged-drive" / ".tghoard"
    full = sqlite3.OperationalError("database or disk is full")
    full.sqlite_errorcode = 13  # type: ignore[attr-defined]
    readonly = sqlite3.OperationalError("attempt to write a readonly database")
    readonly.sqlite_errorcode = 8 | (1 << 8)  # type: ignore[attr-defined]  # extended code
    wrapped = RuntimeError("write failed")
    wrapped.__cause__ = OSError(errno.EROFS, "Read-only file system")

    assert await orchestrator._storage_failure(full, probe_ok)
    assert await orchestrator._storage_failure(readonly, probe_ok)
    assert await orchestrator._storage_failure(wrapped, probe_ok)
    # EIO on one file while the archive is still writable: only that chat is skipped...
    assert await orchestrator._storage_failure(OSError(errno.EIO, "x"), probe_ok) is None
    # ...but if even a tiny write next to checkpoint.db fails, the drive is gone
    assert await orchestrator._storage_failure(OSError(errno.EIO, "x"), gone)
    # network-side OSErrors and non-I/O errors never count as storage failures
    assert await orchestrator._storage_failure(ConnectionError("reset"), gone) is None
    assert await orchestrator._storage_failure(ValueError("bad json"), gone) is None


@pytest.mark.asyncio
async def test_auth_error_stops_only_that_account(tmp_path: Path, fast_sleep: list[float]) -> None:
    cfg, db, clients, sels = _two_accounts(
        tmp_path, lambda: tg_errors.AuthKeyUnregisteredError(request=None)
    )
    buf = io.StringIO()
    stats = await run_multi_account_sync(
        account_clients=clients,
        selections=sels,
        config=cfg,
        db=db,
        console=Console(file=buf, width=300),
    )
    assert [a for a, _ in stats.failed_accounts] == ["a1"]
    assert "AuthKeyUnregisteredError" in stats.failed_accounts[0][1]
    assert stats.failed_chats == []
    assert db.get_synced_ranges("channel_1002") == []  # a1 stopped: its next chat never ran
    assert db.get_synced_ranges("channel_2001") == [(1, 50)]  # a2 finished regardless
    assert "account `a1` stopped" in buf.getvalue()
    db.close()


@pytest.mark.asyncio
async def test_auth_error_on_the_only_account_is_raised(
    tmp_path: Path, fast_sleep: list[float]
) -> None:
    cfg, db, clients, sels = _two_accounts(
        tmp_path, lambda: tg_errors.AuthKeyUnregisteredError(request=None)
    )
    with pytest.raises(tg_errors.AuthKeyUnregisteredError):
        await run_multi_account_sync(
            account_clients=clients[:1],
            selections=sels,
            config=cfg,
            db=db,
            console=Console(file=io.StringIO()),
        )
    db.close()


@pytest.mark.asyncio
async def test_real_cancellation_is_never_swallowed(tmp_path: Path) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="a1")

    class Hanging(FakeTelegramClient):
        started = asyncio.Event()

        async def iter_messages(self, *a: Any, **k: Any):
            async for m in super().iter_messages(*a, **k):
                self.started.set()
                await asyncio.sleep(30)
                yield m

    client = Hanging(
        me=FakeUser(id=111),
        dialogs=[FakeChannel(id=1, title="C", broadcast=True)],
        messages_by_entity_id={1: _msgs(5)},
    )
    run = asyncio.create_task(
        run_multi_account_sync(
            account_clients=[(acc, client)],
            selections={"channel_1": _sel("channel_1", "C")},
            config=cfg,
            db=db,
            console=Console(file=io.StringIO()),
        )
    )
    await asyncio.wait_for(client.started.wait(), 5)
    run.cancel()  # Ctrl+C / shutdown
    with pytest.raises(asyncio.CancelledError):
        await run
    assert run.cancelled()
    db.close()


@pytest.mark.asyncio
async def test_dropped_download_goes_to_pending_media_not_chat_abort(
    tmp_path: Path, fast_sleep: list[float]
) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", concurrency=3)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="a1")

    class DropClient(FakeTelegramClient):
        async def download_media(self, m: Any, file: str, progress_callback: Any = None):
            if int(m.media.photo.id) == 9015:
                self.download_calls.append(9015)
                raise asyncio.CancelledError  # connection keeps dropping for this file
            return await super().download_media(m, file, progress_callback)

    msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, 0, i, tzinfo=UTC),
            media=FakeMediaPhoto(photo=FakePhoto(id=9000 + i)),
        )
        for i in range(1, 31)
    ]
    client = DropClient(
        me=FakeUser(id=111),
        dialogs=[FakeChannel(id=1, title="Fotos", broadcast=True)],
        messages_by_entity_id={1: msgs},
    )
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={"channel_1": _sel("channel_1", "Fotos", photos=True)},
        config=cfg,
        db=db,
        console=Console(file=io.StringIO()),
    )
    assert stats.failed_chats == []
    assert db.get_synced_ranges("channel_1") == [(1, 30)]
    assert stats.media_downloaded == 29
    pending = db.list_pending_media("channel_1", ["photos"])
    assert [r.msg_id for r in pending] == [15]
    assert "ConnectionError" in (pending[0].last_error or "")
    # retried inside the limiter (with backoff) instead of restarting the whole chat
    assert client.download_calls.count(9015) > 1
    db.close()


# --- throttler -------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_transient_treats_telethon_cancel_as_connection_error(
    fast_sleep: list[float],
) -> None:
    calls = 0

    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise asyncio.CancelledError  # request future cancelled on disconnect
        return "ok"

    assert (
        await retry_transient(flaky, description="x", console=Console(file=io.StringIO())) == "ok"
    )
    assert calls == 3

    async def always() -> None:
        raise asyncio.CancelledError

    with pytest.raises(ConnectionError, match="connection dropped"):
        await retry_transient(
            always, description="x", console=Console(file=io.StringIO()), backoff=(1.0, 1.0)
        )


@pytest.mark.asyncio
async def test_retry_transient_and_limiter_let_real_cancellation_through() -> None:
    started = asyncio.Event()

    async def hang() -> None:
        started.set()
        await asyncio.sleep(30)

    limiter = RateLimiter(concurrency=1, min_delay=0, max_delay=0)
    for runner in (
        lambda: retry_transient(hang, description="x", console=Console(file=io.StringIO())),
        lambda: limiter.call(hang),
    ):
        started.clear()
        task = asyncio.create_task(runner())
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
    assert limiter.slots.active == 0


@pytest.mark.asyncio
async def test_limiter_retries_dropped_request_then_raises_connection_error(
    fast_sleep: list[float],
) -> None:
    calls = 0

    async def dropped() -> None:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError

    limiter = RateLimiter(
        concurrency=1, min_delay=0, max_delay=0, max_retries=2, console=Console(file=io.StringIO())
    )
    with pytest.raises(ConnectionError):
        await limiter.call(dropped, description="download_media([x]#1)")
    assert calls == 3
    assert limiter.slots.active == 0


@pytest.mark.asyncio
async def test_api_slots_do_not_follow_download_cuts() -> None:
    ctl = FloodController(max_concurrency=8)
    limiter = RateLimiter(concurrency=8, controller=ctl)
    ctl.record(10, "GetFileRequest")  # file FloodWait: only download parallelism is cut
    assert ctl.concurrency == 4
    assert limiter.slots.limit == 8
    ctl.record(10, "GetHistoryRequest")  # API FloodWait: the pause grows instead
    assert ctl.history_wait > 0
    assert limiter.slots.limit == 8


@pytest.mark.asyncio
async def test_jitter_is_not_slept_while_holding_a_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    limiter = RateLimiter(concurrency=1, min_delay=0.01, max_delay=0.01)
    active_during_sleep: list[int] = []

    async def spy_sleep(secs: float, *a: Any) -> None:
        active_during_sleep.append(limiter.slots.active)

    monkeypatch.setattr(throttler.asyncio, "sleep", spy_sleep)

    async def rpc() -> str:
        return "ok"

    assert await limiter.call(rpc) == "ok"
    assert active_during_sleep == [0]


# --- media pool --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_group_reaches_jobs_waiting_for_a_slot() -> None:
    ran: list[str] = []

    def job(name: str, secs: float):
        async def run() -> str:
            ran.append(name)
            await asyncio.sleep(secs)
            return name

        return run

    async with MediaDownloadPool(4) as pool:
        pool.set_limit(1)  # auto-throttle cut parallelism to 1
        big = await pool.submit(job("B", 0.1), group="B")
        others = [await pool.submit(job(f"A{i}", 0.01), group="A") for i in range(5)]
        await asyncio.sleep(0.01)  # idle workers dequeue A0..A2 and wait for the slot
        assert pool.cancel_group("A") == 5
        assert all(f.cancelled() for f in others)
        assert await asyncio.wait_for(big, 2) == "B"
        await asyncio.sleep(0.05)
        assert ran == ["B"]  # nothing of the aborted chat ran
        assert all(not t.done() for t in pool._tasks)  # workers still alive
        assert pool._slots.active == 0


# --- markup -----------------------------------------------------------------------------


def test_summary_escapes_aliases_and_error_text() -> None:
    stats = OrchestratorStats(
        failed_chats=[("vitor", "channel_1", "ValueError: [/bold] broken [red]")],
        failed_accounts=[("[bold]x", "UnauthorizedError: [/]")],
    )
    buf = io.StringIO()
    orchestrator._print_summary_table(Console(file=buf, width=300), stats)  # no MarkupError
    out = buf.getvalue()
    assert "[vitor] `channel_1` — ValueError: [/bold] broken [red]" in out
    assert "[[bold]x] — UnauthorizedError: [/]" in out


@pytest.mark.asyncio
async def test_disk_full_during_a_download_stops_the_run(
    tmp_path: Path, fast_sleep: list[float]
) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", concurrency=2)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="a1")

    class FullDisk(FakeTelegramClient):
        async def download_media(self, m: Any, file: str, progress_callback: Any = None):
            self.download_calls.append(int(m.media.photo.id))
            raise OSError(errno.ENOSPC, "No space left on device", file)

    msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, i // 60, i % 60, tzinfo=UTC),
            media=FakeMediaPhoto(photo=FakePhoto(id=7000 + i)),
        )
        for i in range(1, 300)
    ]
    client = FullDisk(
        me=FakeUser(id=111),
        dialogs=[FakeChannel(id=1, title="Fotos", broadcast=True)],
        messages_by_entity_id={1: msgs},
    )
    with pytest.raises(ArchiveStorageError):
        await run_multi_account_sync(
            account_clients=[(acc, client)],
            selections={"channel_1": _sel("channel_1", "Fotos", photos=True)},
            config=cfg,
            db=db,
            console=Console(file=io.StringIO()),
        )
    assert len(client.download_calls) < 50  # didn't churn through the whole queue
    assert db.count_failed_media() == 0  # nothing wrongly marked as a broken attachment
    db.close()
