"""Space still needed per chat/category, and media skipped by the size limit (web UI + CLI)."""

from __future__ import annotations

import time
from pathlib import Path

from typer.testing import CliRunner

from tests.test_web_ui_and_priority import _req
from tghoard.cli import app
from tghoard.config import AppConfig
from tghoard.storage.atomic import atomic_write_text
from tghoard.storage.chat_stats import (
    compute_all_chat_stats,
    estimate_to_download,
    list_oversized_media,
    media_size_averages,
    open_readonly_db,
)
from tghoard.storage.chats_manifest import ChatSelection, load_chats_manifest, render_chats_manifest
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow
from tghoard.storage.markdown_writer import ensure_initial_metadata_files
from tghoard.storage.paths import resolve_chat_paths
from tghoard.web.server import WebUI

MB = 1024 * 1024


def _row(chat: str, i: int, cat: str, size: int | None) -> PendingMediaRow:
    return PendingMediaRow(
        chat_key=chat,
        canonical_key=f"id:{i}",
        account_id=1,
        msg_id=i,
        day_str="2026-10",
        category=cat,  # type: ignore[arg-type]
        tg_type="document",
        tg_id=10_000 + i,
        file_name=f"clip_{i}.mp4" if cat == "videos" else None,
        mime_type="video/mp4" if cat == "videos" else "image/jpeg",
        size_bytes=size,
    )


def _archive(tmp_path: Path) -> AppConfig:
    """channel_1: videos checked, photos not; 2 videos on disk, 3 queued (one of 80 MB)."""
    cfg = AppConfig(data_dir=tmp_path / "data", max_media_mb=50)
    cfg.ensure_dirs()
    chat = resolve_chat_paths(cfg.data_dir, "channel_1", "Canal")
    ensure_initial_metadata_files(chat, chat_key="channel_1", chat_title="Canal")
    for i in (1, 2):
        p = chat.media_dir("videos") / f"v{i}.mp4"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "wb") as fh:
            fh.truncate(10 * MB)
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        db.enqueue_pending_media(_row("channel_1", 3, "videos", 20 * MB))
        db.enqueue_pending_media(_row("channel_1", 4, "videos", 80 * MB))
        db.enqueue_pending_media(_row("channel_1", 5, "videos", None))  # size unknown
        db.enqueue_pending_media(_row("channel_1", 6, "photos", 2 * MB))
        db.enqueue_pending_media(_row("channel_1", 7, "photos", 300 * MB))  # unchecked
    atomic_write_text(
        cfg.chats_manifest_path,
        render_chats_manifest(
            [ChatSelection(chat_key="channel_1", title="Canal", section="channels", videos=True)]
        ),
    )
    # Totals on Telegram (web UI cache): 10 videos in the channel, 2 + 3 known
    atomic_write_text(
        cfg.internal_dir / "remote_counts.json",
        '{"channel_1": {"messages": 50, "photos": 2, "videos": 10, "audio": 0, "files": 0,'
        ' "fetched_at": 9999999999}}',
    )
    return cfg


def test_estimate_is_exact_for_queue_and_estimated_for_unarchived(tmp_path: Path) -> None:
    cfg = _archive(tmp_path)
    conn = open_readonly_db(cfg.checkpoint_db_path)
    stats = compute_all_chat_stats(cfg.data_dir, conn, max_media_bytes=cfg.max_media_bytes)
    conn.close()
    st = stats["channel_1"]
    assert st.queued_counts["videos"] == 3 and st.queued_bytes["videos"] == 100 * MB
    assert st.queued_unsized["videos"] == 1
    assert st.oversized_counts == {"photos": 1, "videos": 1, "audio": 0, "files": 0}
    assert st.pending_media == 5

    remote = {"videos": 10, "photos": 2}
    todo = estimate_to_download(st, remote, media_size_averages(stats.values()))
    v = todo["videos"]
    # avg from the 4 videos of known size: (2×10 + 20 + 80) / 4 = 30 MB
    assert v.avg_bytes == 30 * MB
    # 10 on Telegram - 5 known = 5 not archived, + 1 queued of unknown size
    assert v.exact_bytes == 100 * MB and v.estimated_items == 6
    assert v.estimated_bytes == 6 * 30 * MB and v.is_estimate
    assert todo["photos"].exact_bytes == 302 * MB and not todo["photos"].is_estimate
    assert todo["audio"].total_bytes == 0


def test_oversized_list_respects_selection_and_suggests_limits(tmp_path: Path) -> None:
    cfg = _archive(tmp_path)
    conn = open_readonly_db(cfg.checkpoint_db_path)
    entries = load_chats_manifest(cfg.chats_manifest_path)
    selected = list_oversized_media(conn, entries, cfg.max_media_bytes)
    everything = list_oversized_media(conn, entries, cfg.max_media_bytes, include_unselected=True)
    unlimited = list_oversized_media(conn, entries, None)
    conn.close()
    assert [(i.msg_id, i.selected) for i in selected.items] == [(4, True)]  # photos unchecked
    assert selected.total_bytes == 80 * MB
    assert dict((mb, n) for mb, n, _ in selected.thresholds)[100] == 1  # 100 MB would get it
    assert [i.msg_id for i in everything.items] == [7, 4]  # largest first
    assert unlimited.items == [] and unlimited.limit_bytes is None


def test_web_payload_and_oversized_endpoint(tmp_path: Path) -> None:
    cfg = _archive(tmp_path)
    ui = WebUI(
        data_dir=cfg.data_dir,
        manifest_path=cfg.chats_manifest_path,
        db_path=cfg.checkpoint_db_path,
        port=0,
        max_media_bytes=cfg.max_media_bytes,
    )
    ui.start()
    try:
        status, data = _req(ui, "/api/chats")
        assert status == 200
        for _ in range(50):  # files on disk are counted in the background after the 1st load
            if not data["media_pending"]:
                break
            time.sleep(0.1)
            status, data = _req(ui, "/api/chats")
        assert data["media_pending"] is False
        assert data["max_media_bytes"] == 50 * MB
        assert data["oversized"] == {"items": 1, "bytes": 80 * MB}  # checked categories only
        assert data["media_avg"]["videos"] == 30 * MB
        (row,) = data["chats"]
        assert row["stats"]["queued_bytes"]["videos"] == 100 * MB
        status, rep = _req(ui, "/api/oversized")
        assert status == 200 and [i["msg_id"] for i in rep["items"]] == [4]
        status, rep = _req(ui, "/api/oversized?all=1")
        assert [i["msg_id"] for i in rep["items"]] == [7, 4]
        assert rep["items"][0]["selected"] is False
    finally:
        ui.stop()


def test_cli_stats_reports(tmp_path: Path) -> None:
    cfg = _archive(tmp_path)
    runner = CliRunner()
    env = {"COLUMNS": "200", "TGHOARD_MAX_MEDIA_MB": "50"}
    out = runner.invoke(app, ["stats", "-d", str(cfg.data_dir), "--to-download"], env=env)
    assert out.exit_code == 0, out.output
    assert "Canal" in out.output and "~280.0 MB" in out.output  # 100 MB exact + 6 × 30 MB
    out = runner.invoke(app, ["stats", "-d", str(cfg.data_dir), "--too-large"], env=env)
    assert out.exit_code == 0, out.output
    assert "clip_4.mp4" in out.output and "80.0 MB" in out.output
    assert "a limit of 100 MB → 1 file(s)" in out.output
