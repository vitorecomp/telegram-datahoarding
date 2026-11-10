"""Crash safety and correctness of `ChatPartWriter` (monthly `YYYY-MM-ptNNN.json` parts).

- an interrupted repack never loses an archived message (duplicates are allowed);
- duplicates left behind are merged on read and the next write converges to a clean month;
- deletes/media attaches find a message whose locator part number is stale;
- fingerprint keys tell apart service messages from the same sender in the same second;
- entity-only edits (e.g. the URL behind a text link) are versioned.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import tghoard.storage.chat_writer as cw
from tghoard.storage.chat_writer import (
    ChatPartWriter,
    make_canonical_message_key,
    message_content_tag,
)
from tghoard.storage.paths import ChatPaths, list_month_part_files

MONTH = "2026-11"
SMALL = 4096  # minimum max_json_bytes: ~2 messages of 1.2 KB per part


def _msg(i: int, *, text: str | None = None, minute_offset: int = 0) -> dict[str, Any]:
    minutes = 60 + i + minute_offset
    return {
        "canonical_key": f"fp:{i:05d}",
        "id": i,
        "account_msg_ids": {"7": i},
        "date": f"2026-11-09T{minutes // 60:02d}:{minutes % 60:02d}:00+00:00",
        "sender": {"id": 1, "name": "A", "username": None},
        "text": text if text is not None else f"m{i} " + "x" * 1200,
        "entities": [],
        "links": [],
        "media": None,
        "edit_date": None,
        "revisions": [],
        "is_deleted": False,
        "deleted_detected_at": None,
        "delete_source": None,
        "seen_by_accounts": ["7"],
    }


def _paths(root: Path) -> ChatPaths:
    return ChatPaths(data_dir=root, folder="dm-1-2")


def _upsert(writer: ChatPartWriter, cp: ChatPaths, msgs: list[dict[str, Any]], month: str = MONTH):
    return writer.upsert_month_messages(
        chat_paths=cp, chat_key="dm_1_2", chat_title="D", month=month, incoming_messages=msgs
    )


def _parts(cp: ChatPaths, month: str = MONTH) -> list[tuple[int, list[dict[str, Any]]]]:
    return [
        (pnum, json.loads(path.read_text(encoding="utf-8"))["messages"])
        for pnum, path in list_month_part_files(cp, month)
    ]


def _all_ids(cp: ChatPaths) -> list[int]:
    return [m["id"] for _, msgs in _parts(cp) for m in msgs]


def _assert_clean(cp: ChatPaths, expected_ids: set[int]) -> None:
    parts = _parts(cp)
    assert [p for p, _ in parts] == list(range(len(parts))), "parts must be pt000..ptN"
    ids = [m["id"] for _, msgs in parts for m in msgs]
    assert len(ids) == len(set(ids)), "no duplicates once a write completed"
    assert set(ids) == expected_ids
    dates = [(m["date"], m["id"]) for _, msgs in parts for m in msgs]
    assert dates == sorted(dates), "month is globally sorted across parts"
    leftovers = [p.name for p in cp.messages_dir.iterdir() if p.name.endswith(".tmp")]
    assert not leftovers


class _Crash(BaseException):
    """Simulated kill (BaseException, like KeyboardInterrupt / SystemExit)."""


def _crash_on_write(monkeypatch: pytest.MonkeyPatch, n: int) -> dict[str, int]:
    real = cw.atomic_write_json
    calls = {"n": 0}

    def flaky(path: Path, payload: Any) -> None:
        calls["n"] += 1
        if calls["n"] == n:
            raise _Crash(f"killed before write #{n}")
        real(path, payload)

    monkeypatch.setattr(cw, "atomic_write_json", flaky)
    return calls


# Each scenario: (setup batches written cleanly, the batch that gets interrupted[, max bytes
# of the writer that writes that batch])
SCENARIOS: dict[str, tuple[Any, ...]] = {
    # DM / Saved / basic-group backfill goes newest -> oldest: front-inserts on every flush
    "front_insert": ([[_msg(i) for i in range(100, 130)]], [_msg(i) for i in range(90, 100)]),
    # front-insert into a month that already has several parts, small batch
    "front_insert_small": ([[_msg(i) for i in range(100, 116)]], [_msg(95)]),
    # chronological tail append that overflows into several new parts (fast path)
    "tail_overflow": ([[_msg(i) for i in range(100, 105)]], [_msg(i) for i in range(105, 125)]),
    # brand-new month written as several parts at once
    "fresh_month": ([], [_msg(i) for i in range(100, 112)]),
    # edits all over the month: every part grows (revisions keep the old text)
    "edit_all": (
        [[_msg(i) for i in range(100, 116)]],
        [_msg(i, text=f"short {i}") for i in range(100, 116)],
    ),
    # max_json_kb raised: the month is repacked into fewer parts, obsolete ones removed last
    "fewer_parts": ([[_msg(i) for i in range(100, 116)]], [_msg(95)], 16 * 1024),
    # insert in the middle of a multi-part month
    "middle_insert": (
        [[_msg(i, minute_offset=i % 2 * 100) for i in range(100, 120, 2)]],
        [_msg(i, minute_offset=0) for i in range(101, 111, 2)],
    ),
}


def _setup(root: Path, batches: list[list[dict[str, Any]]]) -> ChatPaths:
    shutil.rmtree(root, ignore_errors=True)
    cp = _paths(root)
    writer = ChatPartWriter(max_json_bytes=SMALL)
    for batch in batches:
        _upsert(writer, cp, batch)
    return cp


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
@pytest.mark.parametrize("retry_in_same_process", [False, True])
def test_interrupted_repack_never_loses_a_message_and_next_write_converges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: str, retry_in_same_process: bool
) -> None:
    setup_batches, batch, *rest = SCENARIOS[scenario]
    max_bytes = rest[0] if rest else SMALL
    expected = {m["id"] for b in setup_batches for m in b} | {m["id"] for m in batch}

    # Reference: the same writes, uninterrupted
    ref = _setup(tmp_path / "ref", setup_batches)
    _upsert(ChatPartWriter(max_json_bytes=max_bytes), ref, batch)
    _assert_clean(ref, expected)
    reference_layout = [[m["id"] for m in msgs] for _, msgs in _parts(ref)]

    crash_points = 0
    for n in range(1, 50):
        cp = _setup(tmp_path / f"run{n}", setup_batches)
        before = set(_all_ids(cp))
        writer = ChatPartWriter(max_json_bytes=max_bytes)
        _crash_on_write(monkeypatch, n)
        try:
            _upsert(writer, cp, batch)
            crashed = False
        except _Crash:
            crashed = True
        monkeypatch.undo()
        if not crashed:
            break
        crash_points += 1

        after = _all_ids(cp)
        missing = before - set(after)
        assert not missing, f"crash before write #{n} lost archived messages {sorted(missing)}"

        # Restart (fresh writer) or retry in-process: the batch is re-processed (its
        # checkpoint was never committed) and the month converges to the clean layout
        retry_writer = writer if retry_in_same_process else ChatPartWriter(max_json_bytes=max_bytes)
        _upsert(retry_writer, cp, batch)
        _assert_clean(cp, expected)
        assert [[m["id"] for m in msgs] for _, msgs in _parts(cp)] == reference_layout

    assert crash_points >= 1, "scenario must involve at least one interruptible write"


def test_crash_left_duplicates_are_cleaned_by_an_unrelated_tail_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_batches, batch = SCENARIOS["front_insert"]
    cp = _setup(tmp_path / "d", setup_batches)
    _crash_on_write(monkeypatch, 2)
    with pytest.raises(_Crash):
        _upsert(ChatPartWriter(max_json_bytes=SMALL), cp, batch)
    monkeypatch.undo()
    ids = _all_ids(cp)
    assert len(ids) != len(set(ids)), "this crash point leaves a transient duplicate"

    # A fresh process appends a newer message (would take the tail fast path): the
    # month is checked once, found overlapping, and repacked
    _upsert(ChatPartWriter(max_json_bytes=SMALL), cp, [_msg(500)])
    _assert_clean(cp, set(ids) | {500})


def test_interrupted_obsolete_part_removal_leaves_duplicates_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_batches, batch, max_bytes = SCENARIOS["fewer_parts"]
    cp = _setup(tmp_path / "s", setup_batches)
    n_before = len(list_month_part_files(cp, MONTH))
    real_unlink = Path.unlink

    def no_unlink(self: Path, missing_ok: bool = False) -> None:
        if self.name.startswith(MONTH):
            raise _Crash("killed during cleanup")
        real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", no_unlink)
    with pytest.raises(_Crash):
        _upsert(ChatPartWriter(max_json_bytes=max_bytes), cp, batch)
    monkeypatch.undo()
    assert len(list_month_part_files(cp, MONTH)) == n_before  # obsolete parts still there
    assert set(_all_ids(cp)) == {95, *range(100, 116)}

    _upsert(ChatPartWriter(max_json_bytes=max_bytes), cp, [_msg(116)])
    _assert_clean(cp, {95, *range(100, 117)})
    assert len(list_month_part_files(cp, MONTH)) < n_before


def test_revisions_survive_duplicate_merge_without_being_duplicated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_batches, batch = SCENARIOS["edit_all"]
    cp = _setup(tmp_path / "e", setup_batches)
    _crash_on_write(monkeypatch, 3)
    with pytest.raises(_Crash):
        _upsert(ChatPartWriter(max_json_bytes=SMALL), cp, batch)
    monkeypatch.undo()
    _upsert(ChatPartWriter(max_json_bytes=SMALL), cp, [_msg(116, text="short 116")])
    _assert_clean(cp, set(range(100, 117)))
    edited = [m for _, msgs in _parts(cp) for m in msgs if m["id"] < 116]
    edited_by_crashed_run = [m for m in edited if m["text"].startswith("short")]
    assert edited_by_crashed_run, "the interrupted run had written some edited parts"
    for m in edited_by_crashed_run:
        assert [r["text"][:4] for r in m["revisions"]] == [f"m{m['id']}"[:4]]


def _write_raw_part(cp: ChatPaths, pnum: int, msgs: list[dict[str, Any]]) -> None:
    cp.messages_dir.mkdir(parents=True, exist_ok=True)
    payload = cw._build_part_envelope(
        chat_key="dm_1_2", chat_title="D", month=MONTH, part_num=pnum, messages=msgs
    )
    (cp.messages_dir / f"{MONTH}-pt{pnum:03d}.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def test_out_of_order_month_breaks_the_write_cycle_without_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """pt000=[late], pt001=[early]: each part must receive the other's message (a cycle)."""
    big = "y" * 2500  # one message per 4 KB part
    early, late = _msg(1, text=big), _msg(2, text=big)
    for n in range(1, 10):
        cp = _paths(tmp_path / f"c{n}")
        _write_raw_part(cp, 0, [late])
        _write_raw_part(cp, 1, [early])
        _crash_on_write(monkeypatch, n)
        try:
            _upsert(ChatPartWriter(max_json_bytes=SMALL), cp, [_msg(3, text=big)])
            crashed = False
        except _Crash:
            crashed = True
        monkeypatch.undo()
        assert {1, 2} <= set(_all_ids(cp)), f"crash before write #{n} lost a message"
        if not crashed:
            _assert_clean(cp, {1, 2, 3})
            assert n > 2, "the cycle needs a spill write before the two swapped parts"
            break
        _upsert(ChatPartWriter(max_json_bytes=SMALL), cp, [_msg(3, text=big)])
        _assert_clean(cp, {1, 2, 3})


def test_duplicate_copies_are_merged_not_one_picked(tmp_path: Path) -> None:
    cp = _paths(tmp_path)
    old = _msg(10, text="original")
    old["media"] = {"tg_id": 5, "downloaded": True, "path": "photos/dm-1-2/a.jpg"}
    new = _msg(10, text="edited")
    new["edit_date"] = "2026-11-09T12:00:00+00:00"
    new["revisions"] = [
        {"revision": 1, "captured_at": "x", "edit_date": None, "text": "original"},
    ]
    new["is_deleted"] = True
    new["deleted_detected_at"] = "2026-11-10T00:00:00+00:00"
    new["delete_source"] = "pts_diff"
    new["media"] = {"tg_id": 5, "downloaded": False, "path": None}
    # The newer copy sits in the *lower* part, the older one in the higher part
    _write_raw_part(cp, 0, [_msg(9, text="a"), new])
    _write_raw_part(cp, 1, [old, _msg(11, text="b")])

    _upsert(ChatPartWriter(), cp, [_msg(12, text="c")])
    _assert_clean(cp, {9, 10, 11, 12})
    (rec,) = [m for _, msgs in _parts(cp) for m in msgs if m["id"] == 10]
    assert rec["text"] == "edited"
    assert [r["text"] for r in rec["revisions"]] == ["original"]
    assert rec["is_deleted"] is True and rec["delete_source"] == "pts_diff"
    assert rec["media"]["downloaded"] is True


def test_full_date_month_is_normalized_and_every_message_of_the_day_survives(
    tmp_path: Path,
) -> None:
    """Old `watch` passed `YYYY-MM-DD` as the month: each message overwrote the last."""
    cp = _paths(tmp_path)
    writer = ChatPartWriter()
    for i in (1, 2, 3):
        _upsert(writer, cp, [_msg(i, text=f"live {i}")], month="2026-11-09")
    assert sorted(p.name for p in cp.messages_dir.iterdir()) == ["2026-11-pt000.json"]
    assert _all_ids(cp) == [1, 2, 3]
    doc = json.loads((cp.messages_dir / "2026-11-pt000.json").read_text(encoding="utf-8"))
    assert doc["month"] == "2026-11"


# -- stale locator part numbers -------------------------------------------------------------


def _multi_part_month(tmp_path: Path) -> tuple[ChatPaths, ChatPartWriter, dict[str, int]]:
    cp = _paths(tmp_path)
    writer = ChatPartWriter(max_json_bytes=SMALL)
    pmap, _ = _upsert(writer, cp, [_msg(i) for i in range(100, 112)])
    assert len(set(pmap.values())) >= 3
    return cp, writer, pmap


def test_delete_with_stale_part_scans_the_month_and_reports_the_real_part(
    tmp_path: Path,
) -> None:
    cp, writer, pmap = _multi_part_month(tmp_path)
    target = "fp:00110"
    real = pmap[target]
    stale = 0 if real != 0 else 1
    assert (cp.messages_dir / f"{MONTH}-pt{stale:03d}.json").exists()

    hit = writer.locate_and_mark_deleted(
        chat_paths=cp, month=MONTH, part_num=stale, canonical_key=target, source="pts_diff"
    )
    assert hit is not None and hit[0] == real and hit[1]["is_deleted"] is True
    on_disk = [m for p, msgs in _parts(cp) for m in msgs if m["canonical_key"] == target]
    assert len(on_disk) == 1 and on_disk[0]["is_deleted"] is True
    # compat wrapper, full-date month (legacy locator) and a missing part file
    assert writer.mark_message_deleted(
        chat_paths=cp, month="2026-11-09", part_num=99, canonical_key=target, source="x"
    )["is_deleted"]


def test_attach_with_stale_part_finds_the_message_and_false_means_not_found(
    tmp_path: Path,
) -> None:
    cp, writer, pmap = _multi_part_month(tmp_path)
    target = "fp:00101"
    stale = max(pmap.values())
    assert pmap[target] != stale
    kwargs = {"rel_media_path": "photos/dm-1-2/a.jpg", "sha256": "0", "size_bytes": 1}

    assert (
        writer.locate_and_attach_media(
            chat_paths=cp, month=MONTH, part_num=stale, canonical_key=target, **kwargs
        )
        == pmap[target]
    )
    (rec,) = [m for _, msgs in _parts(cp) for m in msgs if m["canonical_key"] == target]
    assert rec["media"]["downloaded"] is True and rec["media"]["path"] == kwargs["rel_media_path"]

    snapshot = {p.name: p.read_bytes() for p in cp.messages_dir.iterdir()}
    assert (
        writer.attach_downloaded_media(
            chat_paths=cp, month=MONTH, part_num=0, canonical_key="fp:nope", **kwargs
        )
        is False
    )
    assert (
        writer.locate_and_mark_deleted(
            chat_paths=cp, month=MONTH, part_num=0, canonical_key="fp:nope", source="x"
        )
        is None
    )
    assert {p.name: p.read_bytes() for p in cp.messages_dir.iterdir()} == snapshot


def test_batch_attach_rewrites_each_part_once_and_reports_found_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cp, writer, pmap = _multi_part_month(tmp_path)
    hinted = max(pmap.values())
    in_hint = [k for k, p in pmap.items() if p == hinted]
    elsewhere = "fp:00100"  # stale: its locator says `hinted`, it is in pt000
    assert pmap[elsewhere] != hinted
    media = {"path": "photos/dm-1-2/x.jpg", "sha256": "ab", "size_bytes": 3}
    request = {k: media for k in [*in_hint, elsewhere, "fp:missing"]}

    writes: list[str] = []
    real = cw.atomic_write_json

    def counting(path: Path, payload: Any) -> None:
        writes.append(path.name)
        real(path, payload)

    monkeypatch.setattr(cw, "atomic_write_json", counting)
    found = writer.attach_downloaded_media_batch(
        chat_paths=cp, month=MONTH, part_num=hinted, media_by_key=request
    )
    assert found == {*in_hint, elsewhere}
    assert sorted(writes) == sorted({f"{MONTH}-pt{hinted:03d}.json", f"{MONTH}-pt000.json"})
    done = {
        m["canonical_key"]
        for _, msgs in _parts(cp)
        for m in msgs
        if (m.get("media") or {}).get("downloaded")
    }
    assert done == found


# -- canonical keys (item: fingerprint collisions) ------------------------------------------


def _legacy_key(sender: int | None, date: str, disc: str) -> str:
    raw = f"{sender or 0}|{date}|{disc}"
    return "fp:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


DATE = "2026-11-09T10:00:00+00:00"


def test_keys_of_ordinary_messages_are_unchanged() -> None:
    """Already-archived keys (JSON + checkpoint.db) must keep matching on re-sync."""
    k: Callable[..., str] = make_canonical_message_key
    assert k(chat_key="dm_1_2", msg_id=5, sender_id=42, date_iso=DATE, text="hello") == (
        _legacy_key(42, DATE, "grp:None|txt:hello")
    )
    assert k(
        chat_key="group_9", msg_id=5, sender_id=42, date_iso=DATE, text="hi", content_tag="t"
    ) == _legacy_key(42, DATE, "grp:None|txt:hi")
    assert k(
        chat_key="saved_1", msg_id=5, sender_id=1, date_iso=DATE, media_tg_id=77, content_tag="t"
    ) == _legacy_key(1, DATE, "media:77")
    assert k(chat_key="dm_1_2", msg_id=5, sender_id=42, date_iso=DATE) == (
        _legacy_key(42, DATE, "grp:None|txt:")
    )
    assert k(chat_key="supergroup_3", msg_id=5, sender_id=4, date_iso=DATE, content_tag="t") == (
        "id:5"
    )


class MessageActionChatAddUser:
    def __init__(self, users: list[int]) -> None:
        self.users = users


class MessageActionGroupCall:
    def __init__(self, call_id: int, duration: int | None) -> None:
        self.call = type("InputGroupCall", (), {"id": call_id, "access_hash": 999})()
        self.duration = duration


class _Photo:
    def __init__(self, photo_id: int, access_hash: int, file_reference: bytes) -> None:
        self.id = photo_id
        self.access_hash = access_hash
        self.file_reference = file_reference


class MessageActionChatEditPhoto:
    def __init__(self, photo_id: int, access_hash: int, file_reference: bytes) -> None:
        self.photo = _Photo(photo_id, access_hash, file_reference)


class MessageMediaDice:
    def __init__(self, value: int, emoticon: str = "🎲") -> None:
        self.value = value
        self.emoticon = emoticon


class _Svc:
    def __init__(self, msg_id: int, action: Any = None, media: Any = None) -> None:
        self.id = msg_id
        self.action = action
        self.media = media


def _svc_key(msg: Any, chat_key: str = "group_5") -> str:
    return make_canonical_message_key(
        chat_key=chat_key,
        msg_id=msg.id,
        sender_id=42,
        date_iso=DATE,
        text="",
        content_tag=message_content_tag(msg),
    )


def test_service_messages_in_the_same_second_get_distinct_keys(tmp_path: Path) -> None:
    add1, add2 = _Svc(101, MessageActionChatAddUser([1])), _Svc(102, MessageActionChatAddUser([2]))
    assert _svc_key(add1) != _svc_key(add2)
    # the same event seen by a second account (its own msg id): same key -> deduplicated
    assert _svc_key(add1) == _svc_key(_Svc(9001, MessageActionChatAddUser([1])))
    # two dice rolls in one second
    assert _svc_key(_Svc(1, media=MessageMediaDice(3))) != _svc_key(
        _Svc(2, media=MessageMediaDice(5))
    )

    cp = ChatPaths(data_dir=tmp_path, folder="g-5")
    w = ChatPartWriter()
    recs = []
    for m, users in ((add1, [1]), (add2, [2])):
        rec = _msg(m.id, text="")
        rec["canonical_key"] = _svc_key(m)
        rec["service_action"] = {"type": "MessageActionChatAddUser", "users": users}
        recs.append(rec)
    w.upsert_month_messages(
        chat_paths=cp, chat_key="group_5", chat_title="G", month=MONTH, incoming_messages=recs
    )
    stored = [m for _, msgs in _parts(cp) for m in msgs]
    assert sorted(m["service_action"]["users"][0] for m in stored) == [1, 2]


def test_content_tag_ignores_per_account_and_volatile_fields() -> None:
    running = _Svc(1, MessageActionGroupCall(call_id=55, duration=None))
    ended = _Svc(1, MessageActionGroupCall(call_id=55, duration=3600))
    assert message_content_tag(running) == message_content_tag(ended)
    a = _Svc(1, MessageActionChatEditPhoto(7, access_hash=1, file_reference=b"\x01"))
    b = _Svc(1, MessageActionChatEditPhoto(7, access_hash=2, file_reference=b"\x02"))
    assert message_content_tag(a) == message_content_tag(b)
    assert '"photo_id":7' in (message_content_tag(a) or "")
    assert message_content_tag(_Svc(1)) is None


# -- entity-only edits ------------------------------------------------------------------------


def test_entity_only_edit_is_versioned_and_updates_links(tmp_path: Path) -> None:
    cp = _paths(tmp_path)
    w = ChatPartWriter()
    first = _msg(1, text="see docs")
    first["entities"] = [{"type": "MessageEntityTextUrl", "offset": 4, "length": 4, "url": "a"}]
    first["links"] = ["https://old.example"]
    _upsert(w, cp, [first])

    # Same text, URL changed, real edit (edit_date set)
    edited = _msg(1, text="see docs")
    edited["entities"] = [{"type": "MessageEntityTextUrl", "offset": 4, "length": 4, "url": "b"}]
    edited["links"] = ["https://new.example"]
    edited["edit_date"] = "2026-11-09T11:00:00+00:00"
    _, edits = _upsert(w, cp, [edited])
    assert len(edits) == 1
    (rec,) = [m for _, msgs in _parts(cp) for m in msgs]
    assert rec["links"] == ["https://new.example"]
    (rev,) = rec["revisions"]
    assert rev["text"] == "see docs" and rev["links"] == ["https://old.example"]
    assert rev["entities"][0]["url"] == "a"

    # Re-delivery of the same edit is idempotent
    _, edits = _upsert(w, cp, [dict(edited)])
    assert edits == []
    (rec,) = [m for _, msgs in _parts(cp) for m in msgs]
    assert len(rec["revisions"]) == 1

    # Entities differing without an edit (e.g. older serialization) never fake a revision
    resync = _msg(1, text="see docs")
    resync["entities"] = []
    resync["edit_date"] = "2026-11-09T11:00:00+00:00"
    _, edits = _upsert(w, cp, [resync])
    assert edits == []
