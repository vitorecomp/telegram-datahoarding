"""Download queue is FIFO; expired file references are refreshed, and not retried by default."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from telethon import errors as tg_errors

from tests.test_sync_scaling import CHANNEL, CHAT, Client, _env, _photo_msgs, _sel
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow
from tghoard.sync.media_pool import MediaDownloadPool
from tghoard.sync.messages_sync import _make_downloader, sync_chat_messages
from tghoard.sync.reporter import NULL_REPORTER
from tghoard.telegram.throttler import RateLimiter

EXPIRED = "FileReferenceExpiredError: The file reference has expired"


def _row(i: int) -> PendingMediaRow:
    return PendingMediaRow(
        chat_key=CHAT,
        canonical_key=f"id:{i}",
        account_id=111,
        msg_id=i,
        day_str="2026-10",
        category="photos",
        tg_type="photo",
        tg_id=50_000 + i,
        file_name=None,
        mime_type=None,
        size_bytes=100,
    )


def test_queue_is_fifo_and_a_failed_item_goes_to_the_back(tmp_path: Path) -> None:
    db = CheckpointDB(tmp_path / "c.db")
    for i in (5, 1, 4, 2, 3):  # queued in this order (not id order)
        db.enqueue_pending_media(_row(i))
    order = [r.msg_id for p in db.iter_pending_media_pages(CHAT, ["photos"]) for r in p]
    assert order == [5, 1, 4, 2, 3]
    db.record_media_failure(_row(5), "TimeoutError: slow")
    db.record_media_failure(_row(5), "TimeoutError: slow again")
    rows = [r for p in db.iter_pending_media_pages(CHAT, ["photos"]) for r in p]
    assert [r.msg_id for r in rows] == [1, 4, 2, 3, 5]  # retried last, not first
    assert rows[-1].attempts == 2 and rows[-1].last_error == "TimeoutError: slow again"
    db.close()


def test_expired_references_are_skipped_unless_asked(tmp_path: Path) -> None:
    db = CheckpointDB(tmp_path / "c.db")
    for i in (1, 2, 3):
        db.enqueue_pending_media(_row(i))
    db.record_media_failure(_row(1), EXPIRED)
    db.record_media_failure(_row(2), "TimeoutError: slow")
    default = [
        r.msg_id
        for p in db.iter_pending_media_pages(CHAT, ["photos"], skip_expired=True)
        for r in p
    ]
    everything = [r.msg_id for p in db.iter_pending_media_pages(CHAT, ["photos"]) for r in p]
    assert default == [3, 2] and everything == [3, 1, 2]
    summary = db.pending_media_summary(
        CHAT, ["photos"], max_bytes=None, due_at=0.0, skip_expired=True
    )
    assert summary["expired"] == 1 and summary["due"] == 2 and summary["retrying"] == 1
    assert db.pending_media_summary(CHAT, ["photos"], max_bytes=None, due_at=0.0)["due"] == 3
    assert db.count_expired_media() == 1
    db.close()


class _Msg:
    def __init__(self, ref: str) -> None:
        self.id, self.media, self.ref = 7, object(), ref


class _ExpiringClient:
    def __init__(self) -> None:
        self.refetches: list[Any] = []
        self.downloads: list[str] = []

    async def download_media(self, msg: _Msg, file: str, **_: Any) -> str:
        self.downloads.append(msg.ref)
        if msg.ref == "stale":
            raise tg_errors.FileReferenceExpiredError(request=None)
        Path(file).write_bytes(b"ok")
        return file

    async def get_messages(self, entity: Any, ids: int) -> _Msg:
        self.refetches.append((entity, ids))
        return _Msg("fresh")


@pytest.mark.asyncio
async def test_downloader_refreshes_an_expired_reference_once(tmp_path: Path) -> None:
    client = _ExpiringClient()
    download = _make_downloader(
        client=client,
        limiter=RateLimiter(concurrency=2, min_delay=0.0, max_delay=0.0),
        reporter=NULL_REPORTER,
        msg=_Msg("stale"),
        size_bytes=None,
        label="x",
        description="d",
        entity="the-channel",
    )
    assert await download(tmp_path / "f.jpg") == str(tmp_path / "f.jpg")
    assert client.downloads == ["stale", "fresh"]
    assert client.refetches == [("the-channel", 7)]


class _RefetchClient(Client):
    async def get_messages(self, entity: Any, limit: int | None = None, ids=None, **kw: Any):
        if isinstance(ids, int):
            return (await super().get_messages(entity, ids=[ids]))[0]
        return await super().get_messages(entity, limit=limit, ids=ids, **kw)


@pytest.mark.asyncio
async def test_drain_skips_expired_by_default_and_retries_them_with_the_flag(
    tmp_path: Path,
) -> None:
    env = _env(tmp_path)
    msgs = _photo_msgs(range(1, 4))
    await env.sync(_RefetchClient(msgs), _sel(False))  # photos queued, not downloaded
    (row,) = [r for r in env.db.list_pending_media(CHAT, ["photos"]) if r.msg_id == 2]
    env.db.record_media_failure(row, EXPIRED)

    plain = _RefetchClient(msgs)
    await env.sync(plain, _sel(True))
    assert sorted(plain.download_calls) == [50_001, 50_003]  # #2 is not retried by default
    assert env.db.count_expired_media() == 1

    retry = _RefetchClient(msgs)
    res = await sync_chat_messages(
        client=retry,
        entity=CHANNEL,
        account_id=111,
        selection=_sel(True),
        data_dir=env.data_dir,
        db=env.db,
        writer=env.writer,
        media_manager=env.media,
        limiter=RateLimiter(concurrency=4, min_delay=0.0, max_delay=0.0),
        retry_expired=True,
    )
    assert retry.download_calls == [50_002] and res.retroactive_media_downloaded == 1
    assert env.db.count_expired_media() == 0
    env.db.close()


@pytest.mark.asyncio
async def test_pool_wait_backlog_lets_producers_fetch_just_in_time() -> None:
    gate = asyncio.Event()

    async def job() -> None:
        await gate.wait()

    async with MediaDownloadPool(1, queue_size=50) as pool:
        for _ in range(6):
            await pool.submit(job)
        waiter = asyncio.create_task(pool.wait_backlog(2, poll=0.01))
        await asyncio.sleep(0.05)
        assert not waiter.done()  # 5 still queued behind the running one
        gate.set()
        await asyncio.wait_for(waiter, 1.0)
