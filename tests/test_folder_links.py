"""`tghoard folder-links`: links for every chat in each Telegram folder (offline)."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from typer.testing import CliRunner

from tghoard.cli import app
from tghoard.config import AppConfig
from tghoard.storage.chats_manifest import ChatSelection, render_chats_manifest
from tghoard.storage.checkpoint_db import CheckpointDB

runner = CliRunner()


def _archive(tmp_path: Path) -> AppConfig:
    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    cfg.chats_manifest_path.write_text(
        render_chats_manifest(
            [
                ChatSelection(
                    chat_key="channel_10",
                    title="Notícias [BR]",
                    section="channels",
                    username="noticiasbr",
                ),
                ChatSelection(chat_key="supergroup_11", title="Privado Dev", section="groups"),
                ChatSelection(chat_key="group_30", title="Grupo Legado", section="groups"),
                ChatSelection(chat_key="dm_111_500", title="Ana", section="private"),
                ChatSelection(
                    chat_key="dm_111_600", title="Bruno", section="private", username="bruno"
                ),
            ]
        ),
        "utf-8",
    )
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        db.upsert_account(account_id=111, alias="conta1")
        db.add_synced_range("supergroup_11", 1, 4321)
    (cfg.internal_dir / "folders.json").write_text(
        json.dumps(
            {
                "accounts": {
                    "conta1": {
                        "updated_at": "x",
                        "folders": [
                            {
                                "id": 2,
                                "title": "Trabalho",
                                "emoticon": "💼",
                                "members": [
                                    "channel_10",
                                    "supergroup_11",
                                    "group_30",
                                    "dm_111_500",
                                    "dm_111_600",
                                ],
                            },
                            {"id": 3, "title": "Vazia", "emoticon": None, "members": []},
                        ],
                    }
                },
                "titles": {},
            }
        ),
        "utf-8",
    )
    return cfg


def test_markdown_links_per_folder(tmp_path: Path) -> None:
    cfg = _archive(tmp_path)
    result = runner.invoke(app, ["folder-links", "--data-dir", str(cfg.data_dir)])
    assert result.exit_code == 0, result.output
    md = (cfg.data_dir / "folder_links.md").read_text("utf-8")
    assert "### 💼 Trabalho (5)" in md
    assert "- [Notícias (BR)](https://t.me/noticiasbr) — public" in md  # [] escaped
    assert "- [Privado Dev](https://t.me/c/11/4321) — private (members only)" in md
    assert "- Grupo Legado — no link; private basic group" in md
    assert "- [Ana](tg://user?id=500) — opens in the app" in md  # peer, not our account id
    assert "- [Bruno](https://t.me/bruno) — public" in md
    assert "### 📁 Vazia (0)" in md


def test_txt_csv_filters_and_stdout(tmp_path: Path) -> None:
    cfg = _archive(tmp_path)
    d = str(cfg.data_dir)
    out = runner.invoke(app, ["folder-links", "-d", d, "-f", "txt", "--only-channels", "-o", "-"])
    assert out.exit_code == 0
    assert out.output.splitlines() == [
        "# conta1 / Trabalho",
        "https://t.me/noticiasbr",
        "https://t.me/c/11/4321",
    ]
    pub = runner.invoke(app, ["folder-links", "-d", d, "-f", "txt", "--only-public", "-o", "-"])
    assert "t.me/c/" not in pub.output and "https://t.me/bruno" in pub.output

    target = tmp_path / "links.csv"
    res = runner.invoke(
        app, ["folder-links", "-d", d, "-f", "csv", "--folder", "trab", "-o", str(target)]
    )
    assert res.exit_code == 0
    rows = list(csv.DictReader(io.StringIO(target.read_text("utf-8"))))
    assert {r["chat_key"]: r["visibility"] for r in rows}["supergroup_11"] == "members"
    assert len(rows) == 5


def test_without_folders_explains_what_to_do(tmp_path: Path) -> None:
    cfg = AppConfig(data_dir=tmp_path / "data")
    cfg.ensure_dirs()
    res = runner.invoke(app, ["folder-links", "-d", str(cfg.data_dir)])
    assert res.exit_code == 1 and "tghoard sync" in res.output
