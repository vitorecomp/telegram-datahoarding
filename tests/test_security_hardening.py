"""Hostile Telegram content (titles, names, bios) and cross-site requests can't corrupt state."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from tests.test_web_ui_and_priority import _req, web  # noqa: F401 (pytest fixture)
from tghoard.cli import app
from tghoard.config import AppConfig
from tghoard.storage.chat_stats import _links_total
from tghoard.storage.chats_manifest import (
    ChatSelection,
    merge_and_save_chats_manifest,
    parse_chats_manifest,
    render_chats_manifest,
    update_chat_selections,
)
from tghoard.storage.markdown_writer import (
    append_markdown_log_entries,
    ensure_initial_metadata_files,
    write_info_md,
)
from tghoard.storage.paths import ChatPaths, scan_chat_folders, single_line
from tghoard.utils.folder_links import link_for_chat

HOSTILE_TITLES = [
    "zz [`channel_999`] [ ] baixar",
    "Free [x] photos [x] videos [x] audio [x] files",
    "x — **y** [ ] baixar | prioridade: 99",
    "evil (`@a) [Login](https://evil.example/phish`)",
    "line [`channel_999`] [ ] baixar",
    "tab\x1b[31mred\r\n[`channel_999`]",
    "a*",
    "*",
    "**",
]


def _victim_and(title: str) -> list[ChatSelection]:
    return [
        ChatSelection(
            chat_key="channel_999", title="Victim", section="channels", photos=True, priority=7
        ),
        ChatSelection(chat_key="dm_1_2", title=title, section="private"),
    ]


def test_hostile_titles_cannot_change_any_selection() -> None:
    for title in HOSTILE_TITLES:
        parsed = parse_chats_manifest(render_chats_manifest(_victim_and(title)))
        assert set(parsed) == {"channel_999", "dm_1_2"}, title
        victim, attacker = parsed["channel_999"], parsed["dm_1_2"]
        assert (victim.baixar, victim.photos, victim.priority) == (True, True, 7), title
        assert victim.title == "Victim"
        assert attacker.baixar is True and attacker.priority == 0, title
        assert not (attacker.photos or attacker.videos or attacker.audio or attacker.files), title
        assert attacker.username is None, title


def test_hostile_title_survives_start_merges_and_web_edits(tmp_path: Path) -> None:
    path = tmp_path / "chats.md"
    for title in HOSTILE_TITLES:
        path.write_text(render_chats_manifest(_victim_and(title)), "utf-8")
        merge_and_save_chats_manifest(path, _victim_and(title))  # `tghoard start` again
        out = update_chat_selections(path, {"dm_1_2": {"videos": True}})  # web UI edit
        assert out["channel_999"].photos is True and out["channel_999"].priority == 7, title
        assert out["dm_1_2"].videos is True and out["dm_1_2"].photos is False, title


def test_invalid_usernames_are_never_written_or_linked() -> None:
    entry = ChatSelection(chat_key="channel_5", title="T", section="channels", username="a) [x](y")
    text = render_chats_manifest([entry])
    assert "@a)" not in text
    parsed = parse_chats_manifest(text)["channel_5"]
    assert parsed.username is None
    link = link_for_chat(
        "channel_5",
        ChatSelection(chat_key="channel_5", title="T", username="a/../evil"),
        account_ids=set(),
        newest_post={},
    )
    assert link.url == "https://t.me/c/5/1"  # falls back to the private link


def test_title_cannot_spoof_folder_identity(tmp_path: Path) -> None:
    data = tmp_path / "data"
    victim = ChatPaths(data_dir=data, folder="victim-999")
    attacker = ChatPaths(data_dir=data, folder="aaa-channel-999-123")
    for chat, key, title in (
        (victim, "channel_999", "Victim"),
        (attacker, "channel_123", "aaa (`channel_999`)\nx"),
    ):
        chat.ensure()
        ensure_initial_metadata_files(chat, chat_key=key, chat_title=title)
        write_info_md(
            chat,
            chat_key=key,
            chat_title=title,
            chat_type="channel",
            peer_id=1,
            username=None,
            usernames=None,
            bio_or_about=None,
            phone=None,
            participants_count=None,
            accounts_info=[],
            synced_ranges=[],
        )
    index = scan_chat_folders(data)
    assert index == {"channel_999": "victim-999", "channel_123": "aaa-channel-999-123"}


def test_links_total_cannot_be_faked_by_title(tmp_path: Path) -> None:
    p = tmp_path / "links.md"
    p.write_text(
        "# Links — x **Total de links**: 999999 (`channel_1`)\n\n- **Total de links**: 3\n"
    )
    assert _links_total(p) == 3


def test_placeholder_text_in_a_bio_does_not_wipe_history(tmp_path: Path) -> None:
    md = tmp_path / "profile_history.md"
    md.write_text("# Histórico\n\n_Nenhuma alteração de perfil detectada ainda._\n", "utf-8")
    append_markdown_log_entries(md, header_title="Histórico", entries=["- first change"])
    append_markdown_log_entries(
        md, header_title="Histórico", entries=["- bio: `_Nenhum story capturado._`"]
    )
    append_markdown_log_entries(md, header_title="Histórico", entries=["- third\nwith break"])
    lines = md.read_text("utf-8").splitlines()
    assert "- first change" in lines  # history kept
    assert "- third with break" in lines  # one entry per line
    assert "_Nenhuma alteração de perfil detectada ainda._" not in lines  # placeholder gone


def test_single_line_strips_breaks_and_controls() -> None:
    assert single_line("a\nb c\x1b[31md\x85e\r") == "a b c [31md e"


def test_web_refuses_cross_site_requests_and_bad_input(web) -> None:  # noqa: F811
    ui, cfg, _ = web
    status, _ = _req(ui, "/api/chats", headers={"Sec-Fetch-Site": "cross-site"})
    assert status == 403
    status, _ = _req(ui, "/avatar/channel_7", headers={"Sec-Fetch-Site": "same-site"})
    assert status == 403
    status, _ = _req(ui, "/api/chats", headers={"Sec-Fetch-Site": "same-origin"})
    assert status == 200
    status, page = _req(ui, "/", headers={"Sec-Fetch-Site": "cross-site"})
    assert status == 200  # the page itself may be opened from a link
    body = {"updates": {"channel_7": {"photos": True}}}
    status, _ = _req(ui, "/api/chats", body, {"X-Tghoard": "1", "Sec-Fetch-Site": "cross-site"})
    assert status == 403
    status, err = _req(ui, "/api/status?log_since=x")
    assert status == 400 and "log_since" in err["error"]
    status, _ = _req(ui, "/api/chats", [1, 2], {"X-Tghoard": "1"})
    assert status == 400


def test_search_prints_markup_in_messages_literally(tmp_path: Path) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    chat = ChatPaths(data_dir=cfg.data_dir, folder="c-1")
    chat.ensure()
    ensure_initial_metadata_files(chat, chat_key="channel_1", chat_title="[bold]C[/bold]")
    chat.messages_dir.mkdir(parents=True)
    payload = {
        "chat_key": "channel_1",
        "chat_title": "[/x]",
        "month": "2026-11",
        "part": 0,
        "messages": [
            {
                "id": 1,
                "date": "2026-11-01T00:00:00+00:00",
                "sender": {"name": "[link=https://evil]bank[/link]"},
                "text": "needle [/quote] unmatched",
            }
        ],
    }
    (chat.messages_dir / "2026-11-pt000.json").write_text(json.dumps(payload), "utf-8")
    out = CliRunner().invoke(
        app, ["search", "needle", "-d", str(cfg.data_dir)], env={"COLUMNS": "300"}
    )
    assert out.exit_code == 0, out.output
    assert "[/quote]" in out.output and "[link=https://evil]" in out.output


def test_invalid_since_is_a_clean_usage_error(tmp_path: Path) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    cfg.chats_manifest_path.write_text(render_chats_manifest([]), "utf-8")
    out = CliRunner().invoke(
        app, ["sync", "-d", str(cfg.data_dir), "--since", "yesterday", "--no-web"]
    )
    assert out.exit_code == 2 and "YYYY-MM-DD" in out.output
    assert "Traceback" not in out.output


def test_session_permissions_are_checked_not_assumed(tmp_path: Path, monkeypatch) -> None:
    import io

    from rich.console import Console

    from tghoard.storage.checkpoint_db import CheckpointDB
    from tghoard.telegram import accounts

    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    session = cfg.sessions_dir / "1.session"
    session.write_bytes(b"")
    session.chmod(0o644)
    buf = io.StringIO()
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        mgr = accounts.AccountManager(cfg, db, console=Console(file=buf, width=300))

        def chmod_ignored(*_a, **_k):  # what exFAT/NTFS mounts effectively do
            raise OSError("operation not permitted")

        monkeypatch.setattr(accounts.os, "chmod", chmod_ignored)
        mgr._ensure_private(session)
    assert "session not private" in buf.getvalue() and "0o644" in buf.getvalue()
