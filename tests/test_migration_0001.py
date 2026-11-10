"""Migration 0001: legacy per-chat folders + daily JSON -> category-first + monthly JSON."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests.conftest import chat_paths_for
from tghoard.cli import app
from tghoard.config import AppConfig
from tghoard.migrations import load_applied, pending_migrations
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow
from tghoard.storage.paths import iter_all_part_files

runner = CliRunner()


def _msg(
    mid: int, date: str, media_path: str | None = None, tg_id: int = 0, text: str | None = None
) -> dict:
    media = None
    if media_path:
        media = {
            "category": media_path.split("/")[0],
            "tg_type": "photo",
            "tg_id": tg_id,
            "downloaded": True,
            "path": media_path,
            "sha256": "x",
            "size_bytes": 3,
        }
    return {
        "canonical_key": f"id:{mid}",
        "id": mid,
        "account_msg_ids": {"111": mid},
        "date": date,
        "sender": {"id": 1, "name": "A"},
        "seen_by_accounts": ["111"],
        "text": text if text is not None else f"msg {mid}",
        "links": [],
        "media": media,
        "revisions": [],
        "is_deleted": False,
    }


def _write_legacy_day(chat_dir: Path, key: str, title: str, day: str, part: int, msgs) -> None:
    (chat_dir / "chats").mkdir(parents=True, exist_ok=True)
    env = {
        "chat_key": key,
        "chat_title": title,
        "date": day,
        "part": part,
        "part_tag": f"pt{part:03d}",
        "messages": msgs,
    }
    (chat_dir / "chats" / f"{day}-pt{part:03d}.json").write_text(json.dumps(env), "utf-8")


def _build_legacy_archive(data_dir: Path) -> AppConfig:
    cfg = AppConfig(data_dir=data_dir, sync_mode="standard")
    cfg.ensure_dirs()
    (data_dir / "chats.md").write_text(
        "## Canais\n"
        "- [x] baixar | [x] photos | [ ] videos | [ ] audio | [ ] files — "
        "**Canal Notícias BR** [`channel_42`]\n"
        "## Grupos e Supergrupos\n"
        "- [x] baixar | [x] photos | [ ] videos | [ ] audio | [ ] files — "
        "**Dev Group** [`supergroup_77`]\n",
        "utf-8",
    )
    ch = data_dir / "channel_42__canal-noticias-br"
    for sub in ("photos", "videos", "audio", "files", "metadata"):
        (ch / sub).mkdir(parents=True)
    (ch / "photos" / "2026-10-30_1_photo_900.jpg").write_bytes(b"abc")
    (ch / "photos" / ".dl_123_.jpg").write_bytes(b"partial")
    (ch / "links.md").write_text("# Links — x\n\n- **Total de links**: 0\n", "utf-8")
    (ch / "metadata" / "info.md").write_text("# Informações — Canal (`channel_42`)\n", "utf-8")
    _write_legacy_day(
        ch,
        "channel_42",
        "Canal Notícias BR",
        "2026-10-30",
        0,
        [_msg(1, "2026-10-30T10:00:00+00:00", "photos/2026-10-30_1_photo_900.jpg", 900)],
    )
    _write_legacy_day(
        ch,
        "channel_42",
        "Canal Notícias BR",
        "2026-11-01",
        0,
        [_msg(2, "2026-11-01T10:00:00+00:00")],
    )
    _write_legacy_day(
        ch,
        "channel_42",
        "Canal Notícias BR",
        "2026-11-02",
        0,
        [_msg(3, "2026-11-02T10:00:00+00:00")],
    )
    _write_legacy_day(
        ch,
        "channel_42",
        "Canal Notícias BR",
        "2026-11-02",
        1,
        [_msg(4, "2026-11-02T11:00:00+00:00")],
    )

    # Second chat shares the same photo through a hardlink (multi-chat dedup)
    sg = data_dir / "supergroup_77__dev-group"
    (sg / "photos").mkdir(parents=True)
    os.link(
        ch / "photos" / "2026-10-30_1_photo_900.jpg", sg / "photos" / "2026-10-30_5_photo_900.jpg"
    )
    _write_legacy_day(
        sg,
        "supergroup_77",
        "Dev Group",
        "2026-10-30",
        0,
        [_msg(5, "2026-10-30T12:00:00+00:00", "photos/2026-10-30_5_photo_900.jpg", 900)],
    )

    with CheckpointDB(cfg.checkpoint_db_path) as db:
        db.record_messages_batch(
            chat_key="channel_42",
            account_id=111,
            locators=[
                ("id:1", "2026-10-30", 0, 1, False),
                ("id:2", "2026-11-01", 0, 1, False),
                ("id:3", "2026-11-02", 0, 1, False),
                ("id:4", "2026-11-02", 1, 1, False),
            ],
            account_mappings=[(1, "id:1"), (2, "id:2"), (3, "id:3"), (4, "id:4")],
        )
        db.add_synced_range("channel_42", 1, 10)
        db.upsert_chat_cursor(
            chat_key="channel_42",
            account_id=111,
            folder_rel_path="channel_42__canal-noticias-br",
            backfill_completed=True,
        )
        db.register_indexed_media(
            tg_type="photo",
            tg_id=900,
            category="photos",
            sha256="x",
            size_bytes=3,
            rel_path="channel_42__canal-noticias-br/photos/2026-10-30_1_photo_900.jpg",
        )
        db.enqueue_pending_media(
            PendingMediaRow(
                chat_key="channel_42",
                canonical_key="id:2",
                account_id=111,
                msg_id=2,
                day_str="2026-11-01",
                category="videos",
                tg_type="document",
                tg_id=901,
                file_name=None,
                mime_type="video/mp4",
                size_bytes=10,
            )
        )
    return cfg


def test_sync_refuses_legacy_layout_until_migrated(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    _build_legacy_archive(data_dir)
    result = runner.invoke(app, ["stats", "--data-dir", str(data_dir)])
    assert result.exit_code == 2
    assert "tghoard migrate" in result.output


def test_dry_run_changes_nothing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    cfg = _build_legacy_archive(data_dir)
    before = sorted(str(p.relative_to(data_dir)) for p in data_dir.rglob("*"))
    result = runner.invoke(app, ["migrate", "--data-dir", str(data_dir)], env={"COLUMNS": "250"})
    assert result.exit_code == 0, result.output
    assert "canal-noticias-br-42" in result.output
    assert "Preview mode" in result.output
    assert sorted(str(p.relative_to(data_dir)) for p in data_dir.rglob("*")) == before
    assert [m.id for m in pending_migrations(cfg)] == ["0001"]


def test_apply_moves_everything_merges_months_and_updates_db(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    cfg = _build_legacy_archive(data_dir)
    original_inode = (
        (data_dir / "channel_42__canal-noticias-br/photos/2026-10-30_1_photo_900.jpg").stat().st_ino
    )

    result = runner.invoke(app, ["migrate", "--apply", "--data-dir", str(data_dir)])
    assert result.exit_code == 0, result.output
    assert "0001_category_first_layout" in load_applied(cfg)

    # Old folders are gone; new category-first layout exists
    assert not (data_dir / "channel_42__canal-noticias-br").exists()
    assert not (data_dir / "supergroup_77__dev-group").exists()
    ch = chat_paths_for(data_dir, "channel_42")
    assert ch.folder == "canal-noticias-br-42"
    assert (
        ch.links_path == data_dir / "links" / "canal-noticias-br-42.md" and ch.links_path.exists()
    )

    # Media moved (not copied): same inode, hardlink between the two chats preserved
    moved = ch.media_dir("photos") / "2026-10-30_1_photo_900.jpg"
    assert moved.stat().st_ino == original_inode
    sg = chat_paths_for(data_dir, "supergroup_77")
    assert (sg.media_dir("photos") / "2026-10-30_5_photo_900.jpg").stat().st_ino == original_inode
    assert not list(ch.media_dir("photos").glob(".dl_*"))  # partial download dropped

    # Daily files merged into months, keeping the part logic; media paths rewritten
    assert [p.name for p in iter_all_part_files(ch)] == ["2026-10-pt000.json", "2026-11-pt000.json"]
    nov = json.loads((ch.messages_dir / "2026-11-pt000.json").read_text("utf-8"))
    assert nov["month"] == "2026-11" and [m["id"] for m in nov["messages"]] == [2, 3, 4]
    octo = json.loads((ch.messages_dir / "2026-10-pt000.json").read_text("utf-8"))
    assert (
        octo["messages"][0]["media"]["path"]
        == "photos/canal-noticias-br-42/2026-10-30_1_photo_900.jpg"
    )
    assert (data_dir / octo["messages"][0]["media"]["path"]).is_file()

    # checkpoint.db updated in place (and backed up first)
    assert list((cfg.internal_dir / "backups").glob("checkpoint-*-before-0001.db"))
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        loc = db.locate_by_canonical("channel_42", "id:4")
        assert loc is not None and (loc.day_str, loc.part_num) == ("2026-11", 0)
        assert db.get_synced_ranges("channel_42") == [(1, 10)]  # ranges preserved
        cursor = db.get_chat_cursor("channel_42", 111)
        assert cursor["backfill_completed"] and cursor["folder_rel_path"] == "canal-noticias-br-42"
        indexed = db.get_indexed_media("photo", 900)
        assert indexed.rel_path == "photos/canal-noticias-br-42/2026-10-30_1_photo_900.jpg"
        (pending,) = db.list_pending_media("channel_42", ["videos"])
        assert pending.day_str == "2026-11"

    # Commands work again; a second run is a no-op
    assert runner.invoke(app, ["stats", "--data-dir", str(data_dir)]).exit_code == 0
    again = runner.invoke(app, ["migrate", "--apply", "--data-dir", str(data_dir)])
    assert again.exit_code == 0 and "already on the current layout" in again.output


def test_apply_is_resumable_after_interruption(tmp_path: Path, monkeypatch) -> None:
    from tghoard.migrations import m0001_category_first_layout as m1

    data_dir = tmp_path / "data"
    cfg = _build_legacy_archive(data_dir)
    real = m1._migrate_chat
    calls = {"n": 0}

    def crash_on_second(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise KeyboardInterrupt
        return real(*args, **kwargs)

    monkeypatch.setattr(m1, "_migrate_chat", crash_on_second)
    result = runner.invoke(app, ["migrate", "--apply", "--data-dir", str(data_dir)])
    assert result.exit_code != 0
    assert "0001_category_first_layout" not in load_applied(cfg)

    monkeypatch.setattr(m1, "_migrate_chat", real)
    result = runner.invoke(app, ["migrate", "--apply", "--data-dir", str(data_dir)])
    assert result.exit_code == 0, result.output
    assert not m1._legacy_dirs(data_dir)
    sg = chat_paths_for(data_dir, "supergroup_77")
    assert [p.name for p in iter_all_part_files(sg)] == ["2026-10-pt000.json"]


# ---------------------------------------------------------------------------
# Crash recovery: every interruption point resumes to the same end state
# ---------------------------------------------------------------------------
BIG = "x" * 1500  # with 4 KB parts: 2 big messages per part -> multi-part months


class _Crash(Exception):
    """Simulated kill of the process."""


def _build_crash_archive(data_dir: Path) -> AppConfig:
    """Legacy archive touching every step: several metadata/media files, a hardlink across
    chats, a November split into several parts, `media_index` rows and a queued media."""
    _build_legacy_archive(data_dir)
    ch = data_dir / "channel_42__canal-noticias-br"
    (ch / "metadata" / "presence.md").write_text("# Presença — Canal (`channel_42`)\n", "utf-8")
    (ch / "metadata" / "profile_history.md").write_text("# Histórico (`channel_42`)\n", "utf-8")
    (ch / "photos" / "2026-11-03_6_photo_906.jpg").write_bytes(b"p906")
    (ch / "videos" / "2026-11-04_8_photo_908.mp4").write_bytes(b"v908")
    days = {
        "2026-11-03": [(6, "photos/2026-11-03_6_photo_906.jpg", 906), (7, None, 0)],
        "2026-11-04": [(8, "videos/2026-11-04_8_photo_908.mp4", 908), (9, None, 0)],
    }
    locators = []
    for day, items in days.items():
        msgs = [_msg(mid, f"{day}T10:0{mid}:00+00:00", path, tg, BIG) for mid, path, tg in items]
        _write_legacy_day(ch, "channel_42", "Canal Notícias BR", day, 0, msgs)
        locators += [(f"id:{mid}", day, 0, 1, False) for mid, _, _ in items]
    cfg = AppConfig(data_dir=data_dir, sync_mode="standard", max_json_kb=4)
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        db.record_messages_batch(
            chat_key="channel_42",
            account_id=111,
            locators=locators,
            account_mappings=[(mid, f"id:{mid}") for mid in (6, 7, 8, 9)],
        )
        db.record_messages_batch(
            chat_key="supergroup_77",
            account_id=111,
            locators=[("id:5", "2026-10-30", 0, 1, False)],
            account_mappings=[(5, "id:5")],
        )
        for tg_id, rel in (
            (906, "channel_42__canal-noticias-br/photos/2026-11-03_6_photo_906.jpg"),
            (908, "channel_42__canal-noticias-br/videos/2026-11-04_8_photo_908.mp4"),
        ):
            db.register_indexed_media(
                tg_type="photo", tg_id=tg_id, category="x", sha256="x", size_bytes=4, rel_path=rel
            )
    return cfg


def _snapshot(cfg: AppConfig) -> dict:
    """Everything the migration produces (archive files + checkpoint.db), minus timestamps."""
    data_dir = cfg.data_dir
    files: dict[str, object] = {}
    for p in sorted(data_dir.rglob("*")):
        rel = p.relative_to(data_dir).as_posix()
        if rel.startswith(".tghoard"):
            continue
        if p.is_dir():
            files[rel + "/"] = None
        elif p.suffix == ".json":
            payload = json.loads(p.read_text("utf-8"))
            payload.pop("updated_at", None)
            files[rel] = payload
        else:
            files[rel] = (p.read_bytes(), p.stat().st_nlink)
    queries = {
        "locator": "SELECT chat_key, canonical_key, day_str, part_num, sender_id, is_deleted "
        "FROM message_locator",
        "account_map": "SELECT account_id, chat_key, msg_id, canonical_key "
        "FROM account_message_map",
        "media_index": "SELECT tg_type, tg_id, category, rel_path FROM media_index",
        "pending": "SELECT chat_key, canonical_key, day_str, category, downloaded "
        "FROM pending_media",
        "cursors": "SELECT chat_key, account_id, folder_rel_path, backfill_completed "
        "FROM chat_account_cursors",
        "ranges": "SELECT chat_key, start_id, end_id FROM chat_synced_ranges",
    }
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        tables = {k: sorted(tuple(r) for r in db._conn.execute(q)) for k, q in queries.items()}
    return {"files": files, "db": tables}


def _apply_quiet(cfg: AppConfig) -> str:
    from rich.console import Console

    from tghoard.migrations import m0001_category_first_layout as m1

    out = io.StringIO()
    m1._apply(cfg, Console(file=out, width=300))
    return out.getvalue()


@pytest.fixture(scope="module")
def reference_snapshot(tmp_path_factory: pytest.TempPathFactory) -> dict:
    """End state of an uninterrupted migration of `_build_crash_archive`."""
    from tghoard.migrations import m0001_category_first_layout as m1

    cfg = _build_crash_archive(tmp_path_factory.mktemp("ref") / "data")
    _apply_quiet(cfg)
    assert not m1._is_needed(cfg)
    return _snapshot(cfg)


def _crash_on_call(monkeypatch, owner, name: str, n: int, *, after: bool) -> dict:
    """Make the `n`-th call of `owner.name` raise `_Crash` (before or after running it)."""
    real = getattr(owner, name)
    state = {"calls": 0, "crashed": False}

    def wrapper(*args, **kwargs):
        state["calls"] += 1
        hit = state["calls"] == n
        if hit and not after:
            state["crashed"] = True
            raise _Crash(name)
        result = real(*args, **kwargs)
        if hit:
            state["crashed"] = True
            raise _Crash(name)
        return result

    monkeypatch.setattr(owner, name, wrapper)
    return state


def _crash_on_day_file_unlink(monkeypatch, n: int) -> dict:
    """Kill the process right after the `n`-th legacy day file is deleted."""
    real = Path.unlink
    state = {"calls": 0, "crashed": False}

    def unlink(self, missing_ok=False):
        real(self, missing_ok=missing_ok)
        if self.parent.name == "chats" and self.suffix == ".json":
            state["calls"] += 1
            if state["calls"] == n:
                state["crashed"] = True
                raise _Crash("unlink")

    monkeypatch.setattr(Path, "unlink", unlink)
    return state


# (target, attribute, n-th call, crash after running it?)
#  _move_file calls: 1-2 channel metadata, 3-5 channel media, 6 supergroup media
CRASH_POINTS = [
    *[("module", "_move_file", n, True) for n in range(1, 7)],
    ("module", "_move_media", 1, False),  # metadata + links done, no media moved
    ("module", "_merge_daily_json", 1, False),  # all media moved, DB not updated (bug 1c)
    ("writer", "upsert_month_messages", 1, True),  # October written, November not
    ("writer", "_write", 2, True),  # November: first part on disk, the others not
    ("writer", "_write", 3, True),  # November: two parts on disk
    ("module", "parse_part_filename", 2, False),  # mid DB transaction (rolled back)
    ("module", "_update_checkpoint", 1, False),  # months written, DB untouched
    ("module", "_update_checkpoint", 1, True),  # DB updated, day files still there
    ("unlink", "day-file", 2, True),  # some day files deleted, others not
    ("module", "_prune_old_dir", 1, False),  # day files gone, folders not pruned
    ("module", "_prune_old_dir", 1, True),  # pruned, chat not journaled
    ("module", "_migrate_chat", 2, False),  # first chat done, second untouched
]


@pytest.mark.parametrize(("target", "attr", "n", "after"), CRASH_POINTS)
def test_crash_at_any_point_resumes_to_the_uninterrupted_result(
    tmp_path: Path, monkeypatch, reference_snapshot: dict, target: str, attr: str, n: int, after
) -> None:
    from tghoard.migrations import m0001_category_first_layout as m1

    cfg = _build_crash_archive(tmp_path / "data")
    with monkeypatch.context() as mp:
        if target == "unlink":
            state = _crash_on_day_file_unlink(mp, n)
        else:
            owner = m1 if target == "module" else m1.ChatPartWriter
            state = _crash_on_call(mp, owner, attr, n, after=after)
        with pytest.raises(_Crash):
            _apply_quiet(cfg)
    assert state["crashed"]
    assert m1._is_needed(cfg)  # an interrupted run never looks finished

    _apply_quiet(cfg)
    assert not m1._is_needed(cfg)
    assert not (cfg.internal_dir / m1.JOURNAL_NAME).exists()
    assert _snapshot(cfg) == reference_snapshot


def test_reference_end_state_is_correct(reference_snapshot: dict) -> None:
    """Sanity of the reference itself: split months, exact parts, rewritten paths."""
    files, db = reference_snapshot["files"], reference_snapshot["db"]
    nov_parts = sorted(k for k in files if k.startswith("messages/canal-noticias-br-42/2026-11"))
    assert len(nov_parts) >= 3  # 4 KB parts
    where = {}
    for rel in nov_parts:
        for m in files[rel]["messages"]:
            where[f"id:{m['id']}"] = ("2026-11", files[rel]["part"])
    locs = {(ck, key): (day, part) for ck, key, day, part, _, _ in db["locator"]}
    for key, expected in where.items():
        assert locs[("channel_42", key)] == expected
    assert {r[1]: r[3] for r in db["media_index"]} == {
        900: "photos/canal-noticias-br-42/2026-10-30_1_photo_900.jpg",
        906: "photos/canal-noticias-br-42/2026-11-03_6_photo_906.jpg",
        908: "videos/canal-noticias-br-42/2026-11-04_8_photo_908.mp4",
    }
    assert not any(k.startswith(("channel_42__", "supergroup_77__")) for k in files)


def test_crash_between_day_file_delete_and_db_update_is_recovered(tmp_path: Path, monkeypatch):
    """State left by the old ordering (day files deleted, DB never updated): the part numbers
    come from the month parts on disk, not from day files that no longer exist."""
    from tghoard.migrations import m0001_category_first_layout as m1

    cfg = _build_crash_archive(tmp_path / "data")
    with monkeypatch.context() as mp:
        _crash_on_call(mp, m1, "_update_checkpoint", 1, after=False)
        with pytest.raises(_Crash):
            _apply_quiet(cfg)
    old_dir = cfg.data_dir / "channel_42__canal-noticias-br"
    for day_file in old_dir.glob("chats/*.json"):
        day_file.unlink()
    assert m1._is_needed(cfg)

    _apply_quiet(cfg)
    assert not old_dir.exists()
    ch = chat_paths_for(cfg.data_dir, "channel_42")
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        for part in iter_all_part_files(ch):
            payload = json.loads(part.read_text("utf-8"))
            for m in payload["messages"]:
                loc = db.locate_by_canonical("channel_42", m["canonical_key"])
                assert (loc.day_str, loc.part_num) == (payload["month"], payload["part"])
        assert max(db.locate_by_canonical("channel_42", f"id:{i}").part_num for i in (6, 9)) > 0
        assert db.get_indexed_media("photo", 906).rel_path == (
            "photos/canal-noticias-br-42/2026-11-03_6_photo_906.jpg"
        )


def test_repeated_crashes_still_converge(tmp_path: Path, monkeypatch, reference_snapshot) -> None:
    from tghoard.migrations import m0001_category_first_layout as m1

    cfg = _build_crash_archive(tmp_path / "data")
    for owner, attr, n, after in (
        (m1, "_move_file", 4, True),
        (m1.ChatPartWriter, "_write", 1, True),
        (m1, "_prune_old_dir", 1, False),
    ):
        with monkeypatch.context() as mp:
            _crash_on_call(mp, owner, attr, n, after=after)
            with pytest.raises(_Crash):
                _apply_quiet(cfg)
    _apply_quiet(cfg)
    assert _snapshot(cfg) == reference_snapshot


def test_name_clash_rename_survives_a_crash(tmp_path: Path, monkeypatch) -> None:
    """A media file renamed on a name clash (`-2`) keeps its JSON/media_index paths right even
    when the run that renamed it was killed before rewriting the JSON."""
    from tghoard.migrations import m0001_category_first_layout as m1

    cfg = _build_crash_archive(tmp_path / "data")
    with monkeypatch.context() as mp:
        _crash_on_call(mp, m1, "_move_media", 1, after=False)
        with pytest.raises(_Crash):
            _apply_quiet(cfg)
    clash_dir = cfg.data_dir / "photos" / "canal-noticias-br-42"
    clash_dir.mkdir(parents=True)
    (clash_dir / "2026-10-30_1_photo_900.jpg").write_bytes(b"someone else's file")
    with monkeypatch.context() as mp:
        _crash_on_call(mp, m1, "_move_file", 1, after=True)  # photo 900, renamed to -2
        with pytest.raises(_Crash):
            _apply_quiet(cfg)
    journal = json.loads((cfg.internal_dir / m1.JOURNAL_NAME).read_text("utf-8"))
    assert list(journal["renames"].values()) == ["2026-10-30_1_photo_900-2.jpg"]

    _apply_quiet(cfg)
    assert not m1._is_needed(cfg)
    assert (clash_dir / "2026-10-30_1_photo_900.jpg").read_bytes() == b"someone else's file"
    assert (clash_dir / "2026-10-30_1_photo_900-2.jpg").read_bytes() == b"abc"
    ch = chat_paths_for(cfg.data_dir, "channel_42")
    octo = json.loads((ch.messages_dir / "2026-10-pt000.json").read_text("utf-8"))
    assert octo["messages"][0]["media"]["path"] == (
        "photos/canal-noticias-br-42/2026-10-30_1_photo_900-2.jpg"
    )
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        assert db.get_indexed_media("photo", 900).rel_path == (
            "photos/canal-noticias-br-42/2026-10-30_1_photo_900-2.jpg"
        )
    assert not (cfg.internal_dir / m1.JOURNAL_NAME).exists()


def test_stray_temp_files_do_not_block_the_migration(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    cfg = _build_legacy_archive(data_dir)
    ch = data_dir / "channel_42__canal-noticias-br"
    (ch / "chats" / ".2026-11-02-pt000.json.k2j3h4ab.tmp").write_text("{", "utf-8")
    (ch / "metadata" / ".info.md.zz9yy8xx.tmp").write_text("half", "utf-8")
    (ch / ".links.md.q1w2e3r4.tmp").write_text("half", "utf-8")
    (ch / "videos" / ".dl_901_abc.mp4").write_bytes(b"partial")

    result = runner.invoke(app, ["migrate", "--apply", "--data-dir", str(data_dir)])
    assert result.exit_code == 0, result.output
    assert "0001_category_first_layout" in load_applied(cfg)
    assert not ch.exists()
    leftovers = [p for p in data_dir.rglob("*.tmp")] + list(data_dir.rglob(".dl_*"))
    assert leftovers == []
    assert runner.invoke(app, ["stats", "--data-dir", str(data_dir)]).exit_code == 0


def test_unknown_leftovers_are_named_and_do_not_loop(tmp_path: Path) -> None:
    from tghoard.migrations import m0001_category_first_layout as m1

    data_dir = tmp_path / "data"
    cfg = _build_legacy_archive(data_dir)
    ch = data_dir / "channel_42__canal-noticias-br"
    (ch / "chats" / "notes.txt").write_text("mine", "utf-8")
    (ch / "chats" / "2026-11-05-pt000.json").write_text('{"messages": [', "utf-8")  # truncated
    (ch / "photos" / "sub").mkdir()
    (ch / "photos" / "sub" / "x.jpg").write_bytes(b"x")

    result = runner.invoke(
        app, ["migrate", "--apply", "--data-dir", str(data_dir)], env={"COLUMNS": "300"}
    )
    assert result.exit_code == 0, result.output
    out = " ".join(result.output.split())
    for name in ("chats/notes.txt", "chats/2026-11-05-pt000.json", "photos/sub/x.jpg"):
        assert f"channel_42__canal-noticias-br/{name}" in out
    assert "unreadable JSON" in out
    assert "again will not change them" in out
    assert "run it again" not in out
    assert "0001_category_first_layout" in load_applied(cfg)
    # Nothing unknown was deleted; everything else was migrated
    assert sorted(p.relative_to(ch).as_posix() for p in m1._entries(ch)) == [
        "chats/2026-11-05-pt000.json",
        "chats/notes.txt",
        "photos/sub/x.jpg",
    ]
    assert not m1._is_needed(cfg)
    assert [p.name for p in iter_all_part_files(chat_paths_for(data_dir, "channel_42"))] == [
        "2026-10-pt000.json",
        "2026-11-pt000.json",
    ]
    assert runner.invoke(app, ["stats", "--data-dir", str(data_dir)]).exit_code == 0
    again = runner.invoke(app, ["migrate", "--apply", "--data-dir", str(data_dir)])
    assert again.exit_code == 0 and "already on the current layout" in again.output

    # A new file showing up next to the accepted leftovers makes it needed again
    _write_legacy_day(ch, "channel_42", "Canal", "2026-11-06", 0, [_msg(9, "2026-11-06T1:00")])
    assert m1._is_needed(cfg)
