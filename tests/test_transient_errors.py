"""Tests for resilience against transient Telegram errors (HTTP 500, FloodWait) during sync."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from telethon import errors as tg_errors

from tests.conftest import FakeChannel, FakeMessage, FakeTelegramClient, FakeUser
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.telegram import throttler


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []

    async def fake_sleep(secs: float) -> None:
        slept.append(secs)

    monkeypatch.setattr(throttler.asyncio, "sleep", fake_sleep)
    return slept


def _channel_msgs(n: int) -> list[FakeMessage]:
    return [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, i // 60, i % 60, tzinfo=UTC),
            message=f"Post #{i}",
        )
        for i in range(1, n + 1)
    ]


def _selection(chat_key: str, title: str) -> ChatSelection:
    return ChatSelection(chat_key=chat_key, title=title, section="channels", baixar=True)


def _rpc_mcget_fail() -> Exception:
    return tg_errors.RpcMcgetFailError(request=None)


@pytest.mark.asyncio
async def test_transient_server_error_retries_and_resumes_from_checkpoint(
    tmp_path: Path, _no_real_sleep: list[float]
) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    ch = FakeChannel(id=9999, title="Canal", broadcast=True)

    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="Vitor"),
        dialogs=[ch],
        messages_by_entity_id={9999: _channel_msgs(230)},
        fail_after_iter_count=150,
        fail_exc_factory=_rpc_mcget_fail,
        fail_times=1,
    )
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={"channel_9999": _selection("channel_9999", "Canal")},
        config=cfg,
        db=db,
    )

    assert client.failures_raised == 1
    assert _no_real_sleep, "deveria ter aguardado backoff antes de repetir"
    assert stats.failed_chats == []
    assert db.get_synced_ranges("channel_9999") == [(1, 230)]
    # Retry resumes after the committed batch: only 101..230 counted on the successful attempt
    assert stats.messages_saved == 130
    db.close()


@pytest.mark.asyncio
async def test_persistent_error_skips_chat_and_continues_with_others(tmp_path: Path) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    bad = FakeChannel(id=1001, title="Canal Ruim", broadcast=True)
    good = FakeChannel(id=1002, title="Canal Bom", broadcast=True)

    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="Vitor"),
        dialogs=[bad, good],
        messages_by_entity_id={1001: _channel_msgs(10), 1002: _channel_msgs(10)},
        fail_after_iter_count=0,
        fail_exc_factory=_rpc_mcget_fail,
        fail_entity_ids={1001},
    )
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_1001": _selection("channel_1001", "Canal Ruim"),
            "channel_1002": _selection("channel_1002", "Canal Bom"),
        },
        config=cfg,
        db=db,
    )

    assert client.failures_raised == 1 + len(throttler.TRANSIENT_BACKOFF)
    assert [k for _, k, _ in stats.failed_chats] == ["channel_1001"]
    assert db.get_synced_ranges("channel_1001") == []
    assert db.get_synced_ranges("channel_1002") == [(1, 10)]
    db.close()
