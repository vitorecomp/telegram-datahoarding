"""Tests for uniform `-pt000`, `-pt001`, ... JSON partitioning, edits, and deletions."""

from __future__ import annotations

import json
from pathlib import Path

from tghoard.storage.chat_writer import ChatDayWriter
from tghoard.storage.paths import iter_all_part_files, resolve_chat_dir


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


def test_small_day_uses_pt000_and_large_day_splits_sequentially(tmp_path: Path) -> None:
    chat_dir = resolve_chat_dir(tmp_path, "supergroup_1000", "Dev Chat")
    # Small max_json_bytes (6 KB) to trigger deterministic multi-part splitting
    writer = ChatDayWriter(max_json_bytes=6 * 1024)

    # 1. Small day (only 2 short messages) -> MUST still be named `2026-11-08-pt000.json`
    small_msgs = [
        _make_msg(1, "2026-11-08T10:00:00+00:00", "Bom dia!"),
        _make_msg(2, "2026-11-08T10:05:00+00:00", "Tudo certo?"),
    ]
    pmap_small, _ = writer.upsert_day_messages(
        chat_dir=chat_dir,
        chat_key="supergroup_1000",
        chat_title="Dev Chat",
        day_str="2026-11-08",
        incoming_messages=small_msgs,
    )
    assert pmap_small == {"id:1": 0, "id:2": 0}
    assert (chat_dir / "chats" / "2026-11-08-pt000.json").exists()
    assert not (chat_dir / "chats" / "2026-11-08.json").exists()

    # 2. Busy day (30 messages) -> splits across `-pt000.json`, `-pt001.json`, `-pt002.json`...
    busy_msgs = [
        _make_msg(
            100 + i,
            f"2026-11-09T12:{i // 60:02d}:{i % 60:02d}+00:00",
            f"Mensagem longa #{100 + i} — " + ("conteudo " * 25),
        )
        for i in range(30)
    ]
    pmap_busy, _ = writer.upsert_day_messages(
        chat_dir=chat_dir,
        chat_key="supergroup_1000",
        chat_title="Dev Chat",
        day_str="2026-11-09",
        incoming_messages=busy_msgs,
    )

    all_parts = iter_all_part_files(chat_dir)
    part_names = [p.name for p in all_parts]
    assert part_names[0] == "2026-11-08-pt000.json"
    assert "2026-11-09-pt000.json" in part_names
    assert "2026-11-09-pt001.json" in part_names
    assert len([n for n in part_names if n.startswith("2026-11-09-pt")]) >= 3

    # Verify simple `sorted(chats_dir.glob("*.json"))` iterates strictly in order with 0 loss!
    iterated_ids: list[int] = []
    for fpath in sorted((chat_dir / "chats").glob("*.json")):
        doc = json.loads(fpath.read_text(encoding="utf-8"))
        assert fpath.name.endswith(f"-{doc['part_tag']}.json")
        for m in doc["messages"]:
            iterated_ids.append(m["id"])

    assert iterated_ids == [1, 2, *[100 + i for i in range(30)]]

    # 3. Mark a message in part >= 1 as deleted and verify it stays intact with `is_deleted=True`
    target_ckey = "id:120"
    target_part = pmap_busy[target_ckey]
    assert target_part >= 1
    deleted_obj = writer.mark_message_deleted(
        chat_dir=chat_dir,
        day_str="2026-11-09",
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
        "2026-11-09T12:00:05+00:00",
        "Texto corrigido na edição v2!",
    )
    edited_version["edit_date"] = "2026-11-09T13:00:00+00:00"
    _, edits = writer.upsert_day_messages(
        chat_dir=chat_dir,
        chat_key="supergroup_1000",
        chat_title="Dev Chat",
        day_str="2026-11-09",
        incoming_messages=[edited_version],
    )
    assert len(edits) == 1
    updated_msg, rev_entry = edits[0]
    assert updated_msg["text"] == "Texto corrigido na edição v2!"
    assert rev_entry["revision"] == 1
    assert "Mensagem longa #105" in rev_entry["text"]
