"""Migration 0003: daily parts written by the old `watch`/admin-log code are merged into months."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from tghoard.cli import app
from tghoard.config import AppConfig
from tghoard.migrations import pending_migrations
from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.checkpoint_db import CheckpointDB
from tghoard.storage.markdown_writer import ensure_initial_metadata_files
from tghoard.storage.paths import ChatPaths, iter_all_part_files


def _msg(i: int, **extra) -> dict:
    return {
        "id": i,
        "canonical_key": f"id:{i}",
        "date": f"2026-11-09T10:00:{i:02d}+00:00",
        "text": f"m{i}",
        **extra,
    }


def _archive(tmp_path: Path) -> tuple[AppConfig, ChatPaths]:
    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    chat = ChatPaths(data_dir=cfg.data_dir, folder="canal-1")
    chat.ensure()
    ensure_initial_metadata_files(chat, chat_key="channel_1", chat_title="Canal")
    ChatPartWriter().upsert_month_messages(
        chat_paths=chat,
        chat_key="channel_1",
        chat_title="Canal",
        month="2026-11",
        incoming_messages=[_msg(1)],
    )
    daily = {
        "chat_key": "channel_1",
        "chat_title": "Canal",
        "month": "2026-11-09",
        "part": 0,
        "messages": [_msg(1, is_deleted=True), _msg(2), _msg(3)],
    }
    (chat.messages_dir / "2026-11-09-pt000.json").write_text(json.dumps(daily), "utf-8")
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        db.record_messages_batch(
            chat_key="channel_1",
            account_id=None,
            locators=[("id:1", "2026-11", 0, None, False)]
            + [(f"id:{i}", "2026-11-09", 0, None, False) for i in (2, 3)],
            account_mappings=[],
        )
    return cfg, chat


def test_stray_daily_parts_are_merged_and_block_until_then(tmp_path: Path) -> None:
    cfg, chat = _archive(tmp_path)
    assert [m.key for m in pending_migrations(cfg, required_only=True)] == [
        "0003_merge_stray_daily_parts"
    ]
    runner = CliRunner()
    blocked = runner.invoke(app, ["stats", "-d", str(cfg.data_dir)])
    assert blocked.exit_code == 2

    out = runner.invoke(app, ["migrate", "--apply", "-d", str(cfg.data_dir)])
    assert out.exit_code == 0, out.output
    assert not pending_migrations(cfg, required_only=True)
    assert not (chat.messages_dir / "2026-11-09-pt000.json").exists()

    msgs = [
        m for p in iter_all_part_files(chat) for m in json.loads(p.read_text("utf-8"))["messages"]
    ]
    assert sorted(m["id"] for m in msgs) == [1, 2, 3]  # merged, not duplicated
    assert next(m for m in msgs if m["id"] == 1).get("is_deleted") is True  # flag kept
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        rows = db._conn.execute(
            "SELECT DISTINCT day_str FROM message_locator WHERE chat_key = 'channel_1'"
        ).fetchall()
    assert [r[0] for r in rows] == ["2026-11"]
    assert runner.invoke(app, ["stats", "-d", str(cfg.data_dir)]).exit_code == 0


def test_unreadable_daily_part_is_set_aside_not_deleted(tmp_path: Path) -> None:
    cfg, chat = _archive(tmp_path)
    (chat.messages_dir / "2026-11-10-pt000.json").write_text("{not json", "utf-8")
    out = CliRunner().invoke(app, ["migrate", "--apply", "-d", str(cfg.data_dir)])
    assert out.exit_code == 0, out.output
    assert (chat.messages_dir / "2026-11-10-pt000.json.unmerged").read_text() == "{not json"
    assert not pending_migrations(cfg, required_only=True)
