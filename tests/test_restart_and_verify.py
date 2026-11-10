"""Fast restarts (no checkpoint holes, bulk hole check, metadata throttle) + verify-media."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from tests.conftest import (
    FakeChannel,
    FakeMediaPhoto,
    FakeMessage,
    FakePhoto,
    FakeTelegramClient,
    FakeUser,
    chat_paths_for,
)
from tghoard.cli import app
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection, render_chats_manifest
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.paths import iter_all_part_files
from tghoard.sync import orchestrator
from tghoard.sync.orchestrator import run_multi_account_sync

START = datetime(2026, 10, 1, tzinfo=UTC)


def _msgs(ids, photos: set[int] | None = None) -> list[FakeMessage]:
    photos = photos or set()
    return [
        FakeMessage(
            id=i,
            date=START + timedelta(minutes=i),
            message=f"m{i}",
            media=FakeMediaPhoto(photo=FakePhoto(id=70000 + i)) if i in photos else None,
        )
        for i in ids
    ]


class CountingClient(FakeTelegramClient):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.id_lookups: list[list[int]] = []
        self.history_ranges: list[tuple[int, int]] = []

    async def get_messages(self, entity: Any, limit: int | None = None, ids=None):
        if ids is not None:
            self.id_lookups.append(list(ids))
        return await super().get_messages(entity, limit=limit, ids=ids)

    async def iter_messages(self, entity: Any, min_id: int = 0, max_id: int = 0, **kw: Any):
        self.history_ranges.append((min_id + 1, max_id - 1))
        async for m in super().iter_messages(entity, min_id=min_id, max_id=max_id, **kw):
            yield m


def _setup(tmp_path: Path, **cfg_kw: Any):
    cfg = AppConfig(data_dir=tmp_path / "data", sync_mode="standard", **cfg_kw)
    cfg.ensure_dirs()
    db = CheckpointDB(cfg.checkpoint_db_path)
    acc = db.upsert_account(account_id=111, alias="conta1")
    sels = {
        "channel_9": ChatSelection(
            chat_key="channel_9", title="Canal", section="channels", photos=True
        )
    }
    cfg.chats_manifest_path.write_text(render_chats_manifest(list(sels.values())), "utf-8")
    return cfg, db, acc, sels


def _client(msgs, cls=CountingClient, **kw):
    return cls(
        me=FakeUser(id=111, first_name="V"),
        dialogs=[FakeChannel(id=9, title="Canal", broadcast=True)],
        messages_by_entity_id={9: msgs},
        **kw,
    )


@pytest.mark.asyncio
async def test_deleted_ids_at_batch_boundaries_leave_no_holes(tmp_path: Path) -> None:
    cfg, db, acc, sels = _setup(tmp_path)
    # ids 101-103 and 201 deleted: they fall exactly on 100-message batch boundaries
    ids = [i for i in range(1, 321) if i not in (101, 102, 103, 201)]
    await run_multi_account_sync(
        account_clients=[(acc, _client(_msgs(ids)))], selections=sels, config=cfg, db=db
    )
    assert db.get_synced_ranges("channel_9") == [(1, 320)]  # one contiguous range
    db.close()


@pytest.mark.asyncio
async def test_existing_holes_are_checked_in_bulk_once(tmp_path: Path) -> None:
    cfg, db, acc, sels = _setup(tmp_path)
    # Archive state left by the old bug: lots of tiny holes between synced ranges
    for s, e in [(1, 100), (103, 200), (205, 300), (302, 400)]:
        db.add_synced_range("channel_9", s, e)
    ids = [i for i in range(1, 401) if i not in (102, 201, 202, 203, 204)]  # 101 & 301 exist
    client = _client(_msgs(ids))
    await run_multi_account_sync(
        account_clients=[(acc, client)], selections=sels, config=cfg, db=db
    )
    assert client.id_lookups == [[101, 102, 201, 202, 203, 204, 301]]  # 1 request, not 3
    assert client.history_ranges == []  # no per-hole history requests
    assert db.get_synced_ranges("channel_9") == [(1, 400)]
    part = chat_paths_for(cfg.data_dir, "channel_9").messages_dir / "2026-10-pt000.json"
    saved = {m["id"] for m in json.loads(part.read_text("utf-8"))["messages"]}
    assert {101, 301} <= saved  # real messages inside holes were archived

    again = _client(_msgs(ids))  # next restart: nothing left to check
    await run_multi_account_sync(account_clients=[(acc, again)], selections=sels, config=cfg, db=db)
    assert again.id_lookups == [] and again.history_ranges == []
    db.close()


@pytest.mark.asyncio
async def test_metadata_refreshed_at_most_every_n_hours(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []
    real = orchestrator.sync_chat_metadata

    async def counting(**kw: Any) -> None:
        calls.append(kw["selection"].chat_key)
        await real(**kw)

    monkeypatch.setattr(orchestrator, "sync_chat_metadata", counting)
    cfg, db, acc, sels = _setup(tmp_path)
    msgs = _msgs(range(1, 6))
    for _ in range(2):
        await run_multi_account_sync(
            account_clients=[(acc, _client(msgs))], selections=sels, config=cfg, db=db
        )
    assert calls == ["channel_9"]  # second run (restart) skipped metadata
    await run_multi_account_sync(
        account_clients=[(acc, _client(msgs))],
        selections=sels,
        config=cfg.with_overrides(metadata_every_hours=0),
        db=db,
    )
    assert calls == ["channel_9", "channel_9"]  # --refresh-metadata / 0 = always
    db.close()


@pytest.mark.asyncio
async def test_verify_media_detects_and_requeues_missing_files(tmp_path: Path) -> None:
    cfg, db, acc, sels = _setup(tmp_path)
    msgs = _msgs(range(1, 11), photos={2, 4, 6})
    await run_multi_account_sync(
        account_clients=[(acc, _client(msgs))], selections=sels, config=cfg, db=db
    )
    db.close()
    chat = chat_paths_for(cfg.data_dir, "channel_9")
    files = sorted(chat.media_dir("photos").iterdir())
    assert len(files) == 3
    files[0].unlink()  # deleted by accident
    files[1].write_bytes(b"x")  # truncated / corrupt
    runner = CliRunner()
    d = str(cfg.data_dir)

    report = runner.invoke(app, ["verify-media", "-d", d], env={"COLUMNS": "200"})
    assert report.exit_code == 1  # problems found, nothing changed
    assert "Missing on disk" in report.output and "[missing]" in report.output
    assert "[size_mismatch]" in report.output

    fixed = runner.invoke(app, ["verify-media", "-d", d, "--fix"], env={"COLUMNS": "200"})
    assert fixed.exit_code == 0, fixed.output
    payload = json.loads(next(iter(iter_all_part_files(chat))).read_text("utf-8"))
    reasons = sorted(
        m["media"].get("skip_reason") or "ok" for m in payload["messages"] if m["media"]
    )
    assert reasons == ["corrupt_on_disk", "missing_on_disk", "ok"]

    # next sync downloads exactly the two broken files again
    db = CheckpointDB(cfg.checkpoint_db_path)
    healthy = _client(msgs)
    await run_multi_account_sync(
        account_clients=[(acc, healthy)], selections=sels, config=cfg, db=db
    )
    db.close()
    assert len(healthy.download_calls) == 2  # only the missing + the corrupt one
    assert set(healthy.download_calls) <= {70002, 70004, 70006}
    clean = runner.invoke(app, ["verify-media", "-d", d, "--hash"], env={"COLUMNS": "200"})
    assert clean.exit_code == 0 and "All recorded media is on disk" in clean.output


@pytest.mark.asyncio
async def test_verify_media_requeues_media_lost_from_the_queue(tmp_path: Path) -> None:
    cfg, db, acc, sels = _setup(tmp_path)
    sels["channel_9"].photos = False  # photos deferred -> queued in pending_media
    await run_multi_account_sync(
        account_clients=[(acc, _client(_msgs(range(1, 6), photos={3})))],
        selections=sels,
        config=cfg,
        db=db,
    )
    db._conn.execute("DELETE FROM pending_media")  # queue lost (e.g. old DB)
    db.close()
    runner = CliRunner()
    out = runner.invoke(
        app, ["verify-media", "-d", str(cfg.data_dir), "--fix"], env={"COLUMNS": "200"}
    )
    assert out.exit_code == 0 and "[lost_from_queue]" in out.output
    db = CheckpointDB(cfg.checkpoint_db_path)
    (row,) = db.list_pending_media("channel_9", ["photos"])
    assert row.tg_id == 70003 and row.day_str == "2026-10"
    db.close()
