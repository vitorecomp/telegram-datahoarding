"""Tests for monthly `YYYY-MM-ptNNN.json` partitioning, folder naming, edits, and deletions."""

from __future__ import annotations

import json
from pathlib import Path

from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.paths import iter_all_part_files, resolve_chat_paths


def _make_msg(msg_id: int, date_iso: str, text: str) -> dict:
    return {
        "canonical_key": f"id:{msg_id}",
        "id": msg_id,
        "account_msg_ids": {"111": msg_id},
        "date": date_iso,
        "topic_id": None,
        "sender": {"id": 500, "name": "Alice", "username": "alice"},
        "text": text,
        "entities": [],
        "links": [],
        "reply_to_msg_id": None,
        "forward_from": None,
        "media": None,
        "reactions": None,
        "poll": None,
        "service_action": None,
        "views": None,
        "forwards": None,
        "grouped_id": None,
        "noforwards": False,
        "edit_date": None,
        "revisions": [],
        "is_deleted": False,
        "deleted_detected_at": None,
        "delete_source": None,
        "seen_by_accounts": ["111"],
    }


def test_small_month_uses_pt000_and_large_month_splits_sequentially(tmp_path: Path) -> None:
    chat = resolve_chat_paths(tmp_path, "supergroup_1000", "Dev Chat")
    assert chat.folder == "dev-chat-1000"
    assert chat.messages_dir == tmp_path / "messages" / "dev-chat-1000"
    # Small max_json_bytes (6 KB) to trigger deterministic multi-part splitting
    writer = ChatPartWriter(max_json_bytes=6 * 1024)

    # 1. Small month (only 2 short messages) -> MUST still be named `2026-10-pt000.json`
    small_msgs = [
        _make_msg(1, "2026-10-08T10:00:00+00:00", "Bom dia!"),
        _make_msg(2, "2026-10-20T10:05:00+00:00", "Tudo certo?"),
    ]
    pmap_small, _ = writer.upsert_month_messages(
        chat_paths=chat,
        chat_key="supergroup_1000",
        chat_title="Dev Chat",
        month="2026-10",
        incoming_messages=small_msgs,
    )
    assert pmap_small == {"id:1": 0, "id:2": 0}
    assert (chat.messages_dir / "2026-10-pt000.json").exists()
    assert not (chat.messages_dir / "2026-10.json").exists()

    # 2. Busy month (30 messages over several days) -> `-pt000.json`, `-pt001.json`, ...
    busy_msgs = [
        _make_msg(
            100 + i,
            f"2026-11-{1 + i // 10:02d}T12:{i // 60:02d}:{i % 60:02d}+00:00",
            f"Mensagem longa #{100 + i} — " + ("conteudo " * 25),
        )
        for i in range(30)
    ]
    pmap_busy, _ = writer.upsert_month_messages(
        chat_paths=chat,
        chat_key="supergroup_1000",
        chat_title="Dev Chat",
        month="2026-11",
        incoming_messages=busy_msgs,
    )

    part_names = [p.name for p in iter_all_part_files(chat)]
    assert part_names[0] == "2026-10-pt000.json"
    assert "2026-11-pt000.json" in part_names
    assert "2026-11-pt001.json" in part_names
    assert len([n for n in part_names if n.startswith("2026-11-pt")]) >= 3

    # Simple `sorted(glob("*.json"))` iterates strictly in order with 0 loss
    iterated_ids: list[int] = []
    for fpath in sorted(chat.messages_dir.glob("*.json")):
        doc = json.loads(fpath.read_text(encoding="utf-8"))
        assert fpath.name.endswith(f"-{doc['part_tag']}.json")
        assert fpath.name.startswith(doc["month"])
        for m in doc["messages"]:
            iterated_ids.append(m["id"])

    assert iterated_ids == [1, 2, *[100 + i for i in range(30)]]

    # 3. Mark a message in part >= 1 as deleted and verify it stays intact with `is_deleted=True`
    target_ckey = "id:120"
    target_part = pmap_busy[target_ckey]
    assert target_part >= 1
    deleted_obj = writer.mark_message_deleted(
        chat_paths=chat,
        month="2026-11",
        part_num=target_part,
        canonical_key=target_ckey,
        source="pts_diff",
    )
    assert deleted_obj is not None
    assert deleted_obj["is_deleted"] is True
    assert "Mensagem longa #120" in deleted_obj["text"]

    # 4. Edit message #105 and verify previous text is pushed into `revisions[]`
    edited_version = _make_msg(
        105,
        "2026-11-01T12:00:05+00:00",
        "Texto corrigido na edição v2!",
    )
    edited_version["edit_date"] = "2026-11-09T13:00:00+00:00"
    _, edits = writer.upsert_month_messages(
        chat_paths=chat,
        chat_key="supergroup_1000",
        chat_title="Dev Chat",
        month="2026-11",
        incoming_messages=[edited_version],
    )
    assert len(edits) == 1
    updated_msg, rev_entry = edits[0]
    assert updated_msg["text"] == "Texto corrigido na edição v2!"
    assert rev_entry["revision"] == 1
    assert "Mensagem longa #105" in rev_entry["text"]


def test_folder_names_are_kebab_case_with_id_and_survive_renames(tmp_path: Path) -> None:
    from tghoard.storage.markdown_writer import ensure_initial_metadata_files
    from tghoard.storage.paths import index_chat_folders, kebab_case

    assert kebab_case("Python Brasil 🐍 — Dev!") == "python-brasil-dev"
    assert kebab_case("Ação & Reação") == "acao-reacao"
    assert kebab_case("🔥🔥") == "chat"

    dm = resolve_chat_paths(tmp_path, "dm_5_7", "João Silva")
    assert dm.folder == "joao-silva-5-7"
    ensure_initial_metadata_files(dm, chat_key="dm_5_7", chat_title="João Silva")

    ch = resolve_chat_paths(tmp_path, "channel_42", "Notícias")
    ensure_initial_metadata_files(ch, chat_key="channel_42", chat_title="Notícias")
    # Only the identity (metadata) and links file exist up front; no empty media folders
    assert (tmp_path / "metadata" / "noticias-42" / "info.md").is_file()
    assert (tmp_path / "links" / "noticias-42.md").is_file()
    for root in ("messages", "photos", "videos", "audio", "files"):
        assert not (tmp_path / root / "noticias-42").exists()

    # Fresh process (cold index) + renamed chat -> same folder, resolved via metadata/info.md
    index_chat_folders(tmp_path, refresh=True)
    renamed = resolve_chat_paths(tmp_path, "channel_42", "Notícias Urgentes")
    assert renamed.folder == "noticias-42"

    # Same kebab title + clashing id suffix for a different chat -> unique folder
    clash = resolve_chat_paths(tmp_path, "group_42", "Notícias")
    assert clash.folder == "noticias-42-2"
