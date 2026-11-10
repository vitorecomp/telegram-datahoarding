"""Tests for `tghoard sync` progress output (normal vs `--verbose`)."""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from rich.console import Console

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
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.sync.reporter import human_bytes, human_duration


async def _run(tmp_path: Path, verbose: bool) -> str:
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    ch = FakeChannel(id=77, title="Canal [Teste]", broadcast=True)
    msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, i // 60, i % 60, tzinfo=UTC),
            message=f"#{i}",
            media=FakeMediaPhoto(photo=FakePhoto(id=5000 + i)) if i in (10, 20) else None,
        )
        for i in range(1, 151)
    ]
    sel = ChatSelection(
        chat_key="channel_77", title="Canal [Teste]", section="channels", baixar=True, photos=True
    )
    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"), dialogs=[ch], messages_by_entity_id={77: msgs}
    )
    buf = io.StringIO()
    await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={"channel_77": sel},
        config=cfg,
        db=db,
        console=Console(file=buf, width=300, force_terminal=False),
        verbose=verbose,
    )
    db.close()
    return buf.getvalue()


@pytest.mark.asyncio
async def test_default_output_shows_stages_progress_and_downloads(tmp_path: Path) -> None:
    out = await _run(tmp_path, verbose=False)
    assert "📋 1 dialog(s) in the account; 1 marked for download" in out
    assert "⏪ PTS diff" in out
    assert "Canal [Teste] · ▶ starting [1/1] `channel_77`" in out  # brackets not eaten
    assert "~150 ids missing in 1 range(s): 1–150" in out
    assert "💾 batch saved: 100 msgs (ids 1–100)" in out
    assert "💾 batch saved: 50 msgs (ids 101–150)" in out
    assert out.count("⬇ photos/") == 2
    assert "✓ [1/1] done in" in out
    assert "🏁 sync finished in" in out
    # verbose-only details hidden
    assert "↳ " not in out
    assert "🗂 syncing metadata" not in out


@pytest.mark.asyncio
async def test_verbose_output_adds_rpc_and_metadata_details(tmp_path: Path) -> None:
    out = await _run(tmp_path, verbose=True)
    assert "(verbose mode)" in out
    assert "↳ get_top_message(channel_77)" in out
    assert "↳ download_media(channel_77#10)" in out
    assert "→ fetching range 1–150" in out
    assert "🗂 syncing metadata" in out
    assert "🔗 links.md regenerated" in out


def test_human_formatters() -> None:
    assert human_bytes(512) == "512 B"
    assert human_bytes(1536) == "1.5 KB"
    assert human_bytes(5 * 1024 * 1024) == "5.0 MB"
    assert human_bytes(None) == "?"
    assert human_duration(3.21) == "3.2s"
    assert human_duration(125) == "2m05s"
    assert human_duration(3 * 3600 + 7 * 60) == "3h07m"


@pytest.mark.asyncio
async def test_since_never_checkpoints_skipped_history(tmp_path: Path) -> None:
    """Regression: the 'empty range' branch passed `reporter=` to `db.upsert_chat_cursor`."""
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    ch = FakeChannel(id=88, title="Canal Antigo", broadcast=True)
    old_msgs = [
        FakeMessage(id=i, date=datetime(2020, 1, 1, 12, 0, i, tzinfo=UTC), message=f"#{i}")
        for i in range(1, 6)
    ]
    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"), dialogs=[ch], messages_by_entity_id={88: old_msgs}
    )
    buf = io.StringIO()
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_88": ChatSelection(
                chat_key="channel_88", title="Canal Antigo", section="channels", baixar=True
            )
        },
        config=cfg,
        db=db,
        since_date=datetime(2026, 1, 1, tzinfo=UTC),  # every message is older -> nothing saved
        console=Console(file=buf, width=300, force_terminal=False),
        verbose=True,
    )
    assert stats.failed_chats == []
    assert stats.messages_saved == 0
    # older than --since: not saved AND not checkpointed, so a later full sync fetches them
    assert db.get_synced_ranges("channel_88") == []
    db.close()


@pytest.mark.asyncio
async def test_progress_goes_to_web_ui_not_terminal(
    tmp_path: Path,
) -> None:
    from tests.conftest import FakeDocument, FakeMediaDocument

    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    ch = FakeChannel(id=99, title="Canal Vídeos", broadcast=True)
    big = FakeDocument(id=4242, mime_type="application/pdf", size=5 * 1024 * 1024)
    msgs = [
        FakeMessage(
            id=i,
            date=datetime(2026, 11, 9, 12, 0, i, tzinfo=UTC),
            message=f"#{i}",
            media=FakeMediaDocument(document=big) if i == 3 else None,
        )
        for i in range(1, 11)
    ]
    client = FakeTelegramClient(
        me=FakeUser(id=111, first_name="V"), dialogs=[ch], messages_by_entity_id={99: msgs}
    )
    from tghoard.sync.live_state import LiveSyncState

    class SpyLive(LiveSyncState):
        downloads_seen: list[str] = []

        def download_start(self, label, total):
            self.downloads_seen.append(label)
            return super().download_start(label, total)

    live = SpyLive()
    buf = io.StringIO()
    stats = await run_multi_account_sync(
        account_clients=[(acc, client)],
        selections={
            "channel_99": ChatSelection(
                chat_key="channel_99",
                title="Canal Vídeos",
                section="channels",
                baixar=True,
                files=True,
            )
        },
        config=cfg,
        db=db,
        console=Console(file=buf, width=200, force_terminal=True),  # even on a real TTY
        live=live,
    )
    out = buf.getvalue()
    assert stats.messages_saved == 10
    assert client.progress_callbacks == [4242]  # >= 1 MB download reported byte progress
    assert len(live.downloads_seen) == 1 and "#3" in live.downloads_seen[0]
    snap = live.snapshot()
    assert snap["accounts"][0]["completed"] == 1 and snap["downloads"] == []  # finished
    assert "ETA" not in out and "━" not in out.split("Sync summary")[0]  # no terminal bars
    assert "💾 batch saved" in out  # log lines still printed
    db.close()


@pytest.mark.asyncio
async def test_non_terminal_output_is_plain_log_lines(tmp_path: Path) -> None:
    out = await _run(tmp_path, verbose=False)  # non-terminal console
    assert "ETA" not in out  # bars' ETA column absent: plain log lines only


@pytest.mark.asyncio
async def test_since_then_full_sync_fetches_older_history(tmp_path: Path) -> None:
    """--since once, then a normal sync: older messages must still be archived."""
    from tests.conftest import chat_paths_for
    from tghoard.storage.paths import iter_all_part_files

    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard")
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    msgs = [
        FakeMessage(id=i, date=datetime(2025, 12, 20 + i, tzinfo=UTC), message=f"#{i}")
        for i in range(1, 9)
    ]  # ids 1-8: Dec 21 .. Dec 28
    chats = {
        "channel_77": ChatSelection(chat_key="channel_77", title="C", section="channels"),
        "saved_111": ChatSelection(chat_key="saved_111", title="S", section="saved"),
    }

    def client():
        return FakeTelegramClient(
            me=FakeUser(id=111, first_name="V"),
            dialogs=[FakeChannel(id=77, title="C", broadcast=True)],
            messages_by_entity_id={77: msgs, 111: msgs},
        )

    await run_multi_account_sync(
        account_clients=[(acc, client())],
        selections=chats,
        config=cfg,
        db=db,
        since_date=datetime(2025, 12, 24, 12, tzinfo=UTC),
    )
    assert db.get_synced_ranges("channel_77") == [(5, 8)]  # only what was saved
    assert not db.get_chat_cursor("saved_111", 111)["backfill_completed"]

    await run_multi_account_sync(
        account_clients=[(acc, client())], selections=chats, config=cfg, db=db
    )  # no --since: older history comes in
    assert db.get_synced_ranges("channel_77") == [(1, 8)]
    for key in ("channel_77", "saved_111"):
        ids = sorted(
            m["id"]
            for p in iter_all_part_files(chat_paths_for(cfg.data_dir, key))
            for m in json.loads(p.read_text("utf-8"))["messages"]
        )
        assert ids == list(range(1, 9)), key
    assert db.get_chat_cursor("saved_111", 111)["backfill_completed"]
    db.close()
