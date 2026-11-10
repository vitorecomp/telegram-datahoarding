"""Incremental message synchronization, batch checkpointing, and retroactive pending_media drain."""

from __future__ import annotations

import asyncio
import functools
import json
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.markup import escape

from tghoard.storage.chat_writer import (
    ChatPartWriter,
    make_canonical_message_key,
    message_content_tag,
)
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import CheckpointDB, PendingMediaRow
from tghoard.storage.markdown_writer import (
    ensure_initial_metadata_files,
    rebuild_links_and_audit_md_from_parts,
)
from tghoard.storage.media_manager import (
    ClassifiedMedia,
    MediaManager,
    classify_telethon_media,
    fatal_storage_error,
)
from tghoard.storage.paths import (
    ChatPaths,
    iter_all_part_files,
    list_month_part_files,
    month_from_iso,
    part_file_path,
    resolve_chat_paths,
    uses_global_msg_ids,
)
from tghoard.sync.media_pool import MediaDownloadPool
from tghoard.sync.reporter import (
    DOWNLOAD_BAR_MIN_BYTES,
    NULL_BAR,
    NULL_REPORTER,
    SyncReporter,
    human_bytes,
)
from tghoard.telegram.entities import serialize_message_to_dict, to_utc_iso
from tghoard.telegram.flood_control import FloodController, paced
from tghoard.telegram.throttler import RateLimiter

BATCH_SIZE = 100
# DM/Saved backfill walks newest -> oldest, so each batch *prepends* to a month and forces
# a full repack of that month. Flush at month boundaries (or every N messages) instead of
# every 100, so most months are written once.
BACKFILL_BATCH_SIZE = 1000
# Missing ranges up to this size are checked by id (get_messages(ids=...)) in bulk instead of
# one history request each; they are usually deleted messages.
GAP_FETCH_MAX = 100
GAP_IDS_PER_REQUEST = 100
# Telethon's history iterator ends silently on an empty page, an all-`MessageEmpty` page or
# duplicated/out-of-order ids. When it stops before the end of a range, the unexplained tail
# is never assumed empty: up to this many ids are checked by id right away (larger tails
# are confirmed with a second, independent request first).
TAIL_CHECK_MAX_IDS = 1000
# DM/basic-group forward walk: persist the forward cursor at least every N walked messages.
FORWARD_CURSOR_EVERY = 1000
PENDING_FETCH_CHUNK = 100  # messages re-fetched per request to refresh file references
PENDING_PAGE_SIZE = 1000  # queue rows read per query (the queue is streamed, never loaded)
# Pending-media drain: finished downloads are written to their JSON part in groups (one
# rewrite per part per flush, instead of one full-part rewrite per file).
MEDIA_FLUSH_MAX_ITEMS = 500
MEDIA_FLUSH_MAX_SECS = 5.0
MEDIA_FLUSH_IDLE_SECS = 0.5  # nothing finished for this long: flush what is buffered
LINKS_REBUILD_ATTEMPTS = 3


class PendingItem:
    """A processed message whose media may still be downloading in the background.

    Index access mirrors the old tuple `(month, msg_id, canonical_key, sender_id, msg_dict)`
    for the first four fields; `resolve()` waits for the media and returns the full tuple.
    """

    __slots__ = ("day_str", "msg_id", "canonical_key", "sender_id", "_dict", "_task")

    def __init__(
        self,
        day_str: str,
        msg_id: int,
        canonical_key: str,
        sender_id: int | None,
        msg_dict: dict[str, Any] | None = None,
        task: Awaitable[dict[str, Any]] | None = None,
    ) -> None:
        self.day_str, self.msg_id = day_str, msg_id
        self.canonical_key, self.sender_id = canonical_key, sender_id
        self._dict, self._task = msg_dict, task

    def __getitem__(self, idx: int) -> Any:
        return (self.day_str, self.msg_id, self.canonical_key, self.sender_id)[idx]

    async def resolve(self) -> tuple[str, int, str, int | None, dict[str, Any]]:
        if self._dict is None:
            assert self._task is not None
            try:
                self._dict = await self._task
            except asyncio.CancelledError:
                me = asyncio.current_task()
                if me is not None and me.cancelling():
                    raise  # we really are being cancelled
                # The download future was cancelled by someone else (pool shutdown, group
                # cancel): surface it as an error so the chat fails/retries visibly instead of
                # its worker ending silently.
                raise ConnectionError("download cancelled before finishing") from None
        return (self.day_str, self.msg_id, self.canonical_key, self.sender_id, self._dict)


@dataclass
class ChatSyncResult:
    chat_key: str
    messages_saved: int = 0
    messages_dedup_skipped: int = 0
    edits_detected: int = 0
    media_downloaded: int = 0
    media_dedup_reused: int = 0
    media_deferred: int = 0
    retroactive_media_downloaded: int = 0
    media_failed: int = 0
    links_total: int = 0


def _make_downloader(
    *,
    client: Any,
    limiter: RateLimiter,
    reporter: SyncReporter,
    msg: Any,
    size_bytes: int | None,
    label: str,
    description: str,
) -> Any:
    """Downloader callable for MediaManager, with a live byte bar for large files."""

    async def _download(target: Path) -> Any:
        bar = reporter.download_bar(label, size_bytes)
        kwargs: dict[str, Any] = {}
        if bar is not NULL_BAR:
            kwargs["progress_callback"] = lambda cur, tot: bar.update(completed=cur, total=tot)
        try:
            # Concurrency is governed by the MediaDownloadPool, not the API limiter's slots
            # (so history/API calls never queue behind long downloads).
            return await limiter.call(
                lambda: client.download_media(msg, file=str(target), **kwargs),
                description=description,
                use_slots=False,
            )
        finally:
            bar.remove()

    return _download


async def _flush_message_batch(
    *,
    batch_items: list[Any],
    chat_paths: ChatPaths,
    chat_key: str,
    chat_title: str,
    account_id: int,
    db: CheckpointDB,
    writer: ChatPartWriter,
    global_range: tuple[int, int] | None,
    cursor_offset_id: int | None,
    cursor_completed: bool | None,
    folder_rel_path: str,
    reporter: SyncReporter = NULL_REPORTER,
    res: ChatSyncResult | None = None,
    forward_max_id: int | None = None,
) -> int:
    """Write a batch of messages to `-ptNNN.json` files and commit SQLite checkpoint.

    Waits first for the batch's media downloads (started in parallel while reading), so a
    checkpoint is only committed once its messages *and* media are settled. Order: JSON
    parts -> locators -> synced range -> cursor.
    """
    if not batch_items:
        return 0
    batch_items = [await it.resolve() if isinstance(it, PendingItem) else it for it in batch_items]
    if reporter.live is not None:
        reporter.live.add_messages(len(batch_items))
    days = sorted({item[0] for item in batch_items})
    ids = [item[1] for item in batch_items]

    by_day: dict[str, list[dict[str, Any]]] = {}
    for day_str, _, _, _, msg_dict in batch_items:
        by_day.setdefault(day_str, []).append(msg_dict)

    part_lookup: dict[tuple[str, str], int] = {}
    edits_count = 0
    for day_str, day_msgs in sorted(by_day.items()):
        part_map, edits = writer.upsert_month_messages(
            chat_paths=chat_paths,
            chat_key=chat_key,
            chat_title=chat_title,
            month=day_str,
            incoming_messages=day_msgs,
        )
        edits_count += len(edits)
        db.update_day_part_numbers(chat_key, day_str, part_map)
        for ckey, pnum in part_map.items():
            part_lookup[(day_str, ckey)] = pnum

    locators = [
        (
            ckey,
            day_str,
            part_lookup.get((day_str, ckey), 0),
            sender_id,
            bool(msg_dict.get("is_deleted")),
        )
        for day_str, _, ckey, sender_id, msg_dict in batch_items
    ]
    account_mappings = [(msg_id, ckey) for _, msg_id, ckey, _, _ in batch_items]

    db.record_messages_batch(
        chat_key=chat_key,
        account_id=account_id,
        locators=locators,
        account_mappings=account_mappings,
    )
    if global_range is not None:
        db.add_synced_range(chat_key, global_range[0], global_range[1])

    db.upsert_chat_cursor(
        chat_key=chat_key,
        account_id=account_id,
        folder_rel_path=folder_rel_path,
        backfill_offset_id=cursor_offset_id,
        backfill_completed=cursor_completed,
        forward_max_id=forward_max_id,
    )
    day_span = days[0] if len(days) == 1 else f"{days[0]} → {days[-1]}"
    totals = (
        f" | this run: {res.messages_saved} new, {res.messages_dedup_skipped} dedup, "
        f"{res.media_downloaded} media"
        if res is not None
        else ""
    )
    edits_part = f", {edits_count} edits" if edits_count else ""
    reporter.info(
        f"💾 batch saved: {len(batch_items)} msgs (ids {min(ids)}–{max(ids)}{edits_part}) "
        f"| {day_span}{totals}"
    )
    return edits_count


@dataclass
class _ChatCtx:
    """Everything one chat's sync needs (keeps the helpers' signatures short)."""

    client: Any
    entity: Any
    account_id: int
    selection: ChatSelection
    chat_paths: ChatPaths
    db: CheckpointDB
    writer: ChatPartWriter
    media_manager: MediaManager
    limiter: RateLimiter
    res: ChatSyncResult
    reporter: SyncReporter
    media_pool: MediaDownloadPool
    since_date: datetime | None
    page_wait: float
    pacer: FloodController | None
    touched: bool = field(default=False)  # any message written in this run

    @property
    def chat_key(self) -> str:
        return self.selection.chat_key

    def history(self, **iter_kwargs: Any) -> AsyncIterator[Any]:
        return paced(
            self.client.iter_messages(self.entity, wait_time=self.page_wait, **iter_kwargs),
            self.pacer,
        )

    async def call(self, fn: Callable[[], Awaitable[Any]], description: str) -> Any:
        return await self.limiter.call(fn, description=f"{description}({self.chat_key})")

    async def process(self, msg: Any) -> PendingItem:
        return await _process_single_message(
            client=self.client,
            msg=msg,
            chat_key=self.chat_key,
            account_id=self.account_id,
            selection=self.selection,
            chat_paths=self.chat_paths,
            db=self.db,
            media_manager=self.media_manager,
            limiter=self.limiter,
            res=self.res,
            reporter=self.reporter,
            pool=self.media_pool,
        )

    async def flush(
        self,
        batch: list[Any],
        *,
        global_range: tuple[int, int] | None = None,
        cursor_offset_id: int | None = None,
        cursor_completed: bool | None = None,
        forward_max_id: int | None = None,
    ) -> None:
        if not batch:
            return
        self.res.edits_detected += await _flush_message_batch(
            batch_items=batch,
            chat_paths=self.chat_paths,
            chat_key=self.chat_key,
            chat_title=self.selection.title,
            account_id=self.account_id,
            db=self.db,
            writer=self.writer,
            global_range=global_range,
            cursor_offset_id=cursor_offset_id,
            cursor_completed=cursor_completed,
            folder_rel_path=self.chat_paths.folder,
            reporter=self.reporter,
            res=self.res,
            forward_max_id=forward_max_id,
        )
        self.touched = True

    def save_cursor(self, **kwargs: Any) -> None:
        self.db.upsert_chat_cursor(
            chat_key=self.chat_key,
            account_id=self.account_id,
            folder_rel_path=self.chat_paths.folder,
            **kwargs,
        )


def _first_msg_id(result: Any) -> int:
    """Id of the first message of a `get_messages` result (0 when there is none)."""
    if result is None:
        return 0
    first = result
    if isinstance(result, (list, tuple)):
        first = next((m for m in result if m is not None), None)
    return int(getattr(first, "id", 0) or 0) if first is not None else 0


def _iter_id_chunks(gaps: Iterable[tuple[int, int]], size: int) -> Iterator[list[int]]:
    """Ids of `gaps` (sorted ranges) in chunks of `size`, without materializing them all."""
    chunk: list[int] = []
    for start, end in gaps:
        nxt = start
        while nxt <= end:
            take = min(end, nxt + size - len(chunk) - 1)
            chunk.extend(range(nxt, take + 1))
            nxt = take + 1
            if len(chunk) >= size:
                yield chunk
                chunk = []
    if chunk:
        yield chunk


def _id_runs(ids: Iterable[int]) -> list[tuple[int, int]]:
    """Contiguous runs `[(start, end), ...]` of ascending ids."""
    runs: list[tuple[int, int]] = []
    for i in ids:
        if runs and i == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], i)
        else:
            runs.append((i, i))
    return runs


async def sync_chat_messages(
    *, media_pool: MediaDownloadPool | None = None, **kwargs: Any
) -> ChatSyncResult:
    """Sync missing message intervals for a chat + drain newly enabled media categories.

    Media downloads go to the account's `MediaDownloadPool` (fixed workers + bounded channel)
    while messages keep being read. If the chat fails, its queued/running downloads are
    cancelled; the pool's workers keep serving other chats. Without a pool (direct callers),
    a private one is created for this chat.
    """
    own_pool = media_pool is None
    pool = media_pool or MediaDownloadPool(kwargs["limiter"].slots.limit).start()
    chat_key = kwargs["selection"].chat_key
    try:
        return await _sync_chat_messages_impl(media_pool=pool, **kwargs)
    except BaseException:
        pool.cancel_group(chat_key)
        raise
    finally:
        if own_pool:
            await pool.close()


async def _sync_chat_messages_impl(
    *,
    client: Any,
    entity: Any,
    account_id: int,
    selection: ChatSelection,
    data_dir: Path,
    db: CheckpointDB,
    writer: ChatPartWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
    since_date: datetime | None = None,
    reporter: SyncReporter = NULL_REPORTER,
    history_wait: float = 0.0,
    pacer: FloodController | None = None,
    media_pool: MediaDownloadPool,
) -> ChatSyncResult:
    chat_key = selection.chat_key
    res = ChatSyncResult(chat_key=chat_key)
    if not selection.baixar:
        return res

    chat_paths = resolve_chat_paths(data_dir, chat_key, selection.title)
    ensure_initial_metadata_files(chat_paths, chat_key=chat_key, chat_title=selection.title)

    # Record that this account participates in this chat
    db.upsert_chat_cursor(
        chat_key=chat_key,
        account_id=account_id,
        folder_rel_path=chat_paths.folder,
    )

    ctx = _ChatCtx(
        client=client,
        entity=entity,
        account_id=account_id,
        selection=selection,
        chat_paths=chat_paths,
        db=db,
        writer=writer,
        media_manager=media_manager,
        limiter=limiter,
        res=res,
        reporter=reporter,
        media_pool=media_pool,
        since_date=since_date,
        # With an auto-throttle `pacer`, pauses between history pages are adaptive (FloodWait
        # driven) and Telethon's own fixed pause is disabled; otherwise use the fixed setting.
        page_wait=0.0 if pacer is not None else history_wait,
        pacer=pacer,
    )
    if uses_global_msg_ids(chat_key):
        await _sync_global_id_chat(ctx)
    else:
        await _sync_per_account_chat(ctx)

    # Save channel PTS if entity carries one
    entity_pts = getattr(entity, "pts", None)
    if entity_pts is not None and uses_global_msg_ids(chat_key):
        db.set_pts_state(
            account_id,
            int(entity_pts),
            scope_key=f"channel:{int(getattr(entity, 'id', 0))}",
        )

    # Drain retroactive pending_media if user recently checked [x] photos/videos/audio/files!
    retro_dl = await drain_pending_media(
        client=client,
        entity=entity,
        account_id=account_id,
        selection=selection,
        chat_paths=chat_paths,
        db=db,
        writer=writer,
        media_manager=media_manager,
        limiter=limiter,
        reporter=reporter,
        media_pool=media_pool,
    )
    res.retroactive_media_downloaded = retro_dl

    if ctx.touched or retro_dl > 0 or not (chat_paths.links_path).exists():
        links_total = await _rebuild_links_off_loop(
            chat_paths, chat_key=chat_key, chat_title=selection.title, reporter=reporter
        )
        if links_total is not None:
            res.links_total = links_total
            reporter.debug(f"🔗 links.md regenerated ({links_total} links)")

    return res


async def _rebuild_links_off_loop(
    chat_paths: ChatPaths, *, chat_key: str, chat_title: str, reporter: SyncReporter
) -> int | None:
    """Regenerate `links/<folder>.md` + `deleted_and_edits.md` in a worker thread.

    The rebuild re-parses every part of the chat (seconds to minutes on a big channel on a
    slow disk), so it must not freeze the event loop. The caller holds the chat's lock, so
    no other sync of this chat writes its parts meanwhile; writers outside that lock (e.g.
    another account's PTS replay) are detected by comparing the parts' signatures before
    and after, and the rebuild is redone. Failures only leave the derived `.md` files
    stale (they are regenerated on the next change), so they never fail the chat.
    """

    def _snapshot() -> tuple[tuple[str, int, int, int], ...]:
        sig: list[tuple[str, int, int, int]] = []
        for part in iter_all_part_files(chat_paths):
            try:
                st = part.stat()
            except FileNotFoundError:
                continue
            sig.append((part.name, st.st_ino, st.st_mtime_ns, st.st_size))
        return tuple(sig)

    def _rebuild() -> int:
        links_total, _, _ = rebuild_links_and_audit_md_from_parts(
            chat_paths, chat_key=chat_key, chat_title=chat_title
        )
        return links_total

    def _work() -> tuple[int | None, bool]:
        before = _snapshot()
        try:
            total = _rebuild()
        except FileNotFoundError:
            return None, False  # a part was replaced/compacted mid-scan: redo it
        return total, _snapshot() == before

    try:
        for _ in range(LINKS_REBUILD_ATTEMPTS):
            total, stable = await asyncio.to_thread(_work)
            if stable:
                return total
        # Parts keep changing under us (rare): one last pass on the loop, where nothing
        # else can interleave.
        return _rebuild()
    except Exception as exc:  # noqa: BLE001 - derived files: never fail the chat for them
        if fatal_storage_error(exc) is not None:
            raise  # disk full / read-only: the whole archive must stop, not just this file
        reporter.warn(f"⚠ could not regenerate links/deleted_and_edits .md files: {exc}")
        return None


# ----------------------------------------------------------------------------------------
# Channels & supergroups: global message ids -> exact missing ranges
# ----------------------------------------------------------------------------------------
async def _sync_global_id_chat(ctx: _ChatCtx) -> None:
    db, reporter, chat_key = ctx.db, ctx.reporter, ctx.chat_key
    top_list = await ctx.call(
        lambda: ctx.client.get_messages(ctx.entity, limit=1), "get_top_message"
    )
    max_server_id = _first_msg_id(top_list)
    if max_server_id <= 0:
        reporter.info("✔ chat has no visible messages on the server")
        return

    missing_ranges = db.get_missing_ranges(chat_key, 1, max_server_id)
    missing_total = sum(e - s + 1 for s, e in missing_ranges)
    reporter.bar.update(total=missing_total, unit="ids")
    if not missing_ranges:
        reporter.info(f"✔ already 100% synced up to id={max_server_id} (nothing to download)")
        # 100% already synced by another account or previous run!
        synced = db.get_synced_ranges(chat_key)
        ctx.res.messages_dedup_skipped = sum((e - s + 1) for s, e in synced)
        return
    preview = ", ".join(f"{a}–{b}" for a, b in missing_ranges[:3])
    more = f" (+{len(missing_ranges) - 3})" if len(missing_ranges) > 3 else ""
    reporter.info(
        f"🔎 latest msg id={max_server_id}; ~{missing_total} ids missing in "
        f"{len(missing_ranges)} range(s): {preview}{more}"
    )

    # Interior holes (synced ids on both sides) are almost always deleted messages: check
    # small ones by id in bulk. The head (first sync) and the tail (new messages) keep the
    # normal history paging.
    def _is_small_hole(r: tuple[int, int]) -> bool:
        return r[1] - r[0] + 1 <= GAP_FETCH_MAX and r[0] > 1 and r[1] < max_server_id

    small = [r for r in missing_ranges if _is_small_hole(r)]
    large = [r for r in missing_ranges if not _is_small_hole(r)]
    ids_done_before = 0
    if small:
        await _sync_small_gaps(ctx, small)
        ids_done_before += sum(e - s + 1 for s, e in small)
        reporter.bar.update(completed=ids_done_before)
    for r_start, r_end in large:
        ids_done_before += await _sync_history_range(ctx, r_start, r_end, ids_done_before)
        reporter.bar.update(completed=ids_done_before)


async def _sync_history_range(ctx: _ChatCtx, r_start: int, r_end: int, ids_done_before: int) -> int:
    """Page `[r_start, r_end]` oldest -> newest, checkpointing each batch.

    Each batch checkpoints `[start of range, highest id seen]`: the iterator walked every id
    up to there, so the ids missing in between are deleted messages. What lies after the
    highest id seen is only checkpointed once confirmed (the iterator may stop early).
    Returns the number of ids of the range (for the progress bar).
    """
    reporter, since = ctx.reporter, ctx.since_date
    range_start0 = r_start
    reporter.debug(f"→ fetching range {r_start}–{r_end}")
    # with --since, the range starts at the first kept message: older ids are NOT
    # checkpointed, so a later sync without --since still fetches them
    first_seen = since is None
    while r_start <= r_end:
        batch: list[Any] = []
        batch_ids: list[int] = []
        progressed = False
        iter_kwargs: dict[str, Any] = {"min_id": r_start - 1, "max_id": r_end + 1, "reverse": True}
        if since is not None:
            iter_kwargs["offset_date"] = since
        async for msg in ctx.history(**iter_kwargs):
            msg_id = int(getattr(msg, "id", 0) or 0)
            if msg_id < r_start or msg_id > r_end:
                continue  # outside the requested window (defensive)
            if not first_seen:
                first_seen = True
                r_start = range_start0 = msg_id  # range starts at the 1st kept message
            # Progress in ids covered (deleted ids leave gaps, so count ids, not msgs)
            reporter.bar.update(completed=ids_done_before + msg_id - range_start0 + 1)
            msg_dt = getattr(msg, "date", None)
            if since is not None and isinstance(msg_dt, datetime) and msg_dt < since:
                continue  # defensive: never checkpointed

            batch.append(await ctx.process(msg))
            batch_ids.append(msg_id)
            progressed = True
            if len(batch) >= BATCH_SIZE:
                # Starting at min(batch_ids) instead of r_start left a permanent hole per batch
                b_max = max(batch_ids)
                await ctx.flush(
                    batch,
                    global_range=(r_start, b_max),
                    cursor_offset_id=b_max,
                    cursor_completed=False,
                )
                r_start = b_max + 1
                batch, batch_ids = [], []
        if batch:
            b_max = max(batch_ids)
            await ctx.flush(
                batch, global_range=(r_start, b_max), cursor_offset_id=b_max, cursor_completed=False
            )
            r_start = b_max + 1
        if r_start > r_end or not first_seen:
            break
        r_start = await _resolve_history_tail(ctx, r_start, r_end, progressed)

    if r_start > r_end:
        ctx.save_cursor(backfill_offset_id=None, backfill_completed=True)
    return r_end - range_start0 + 1


async def _resolve_history_tail(ctx: _ChatCtx, start: int, end: int, progressed: bool) -> int:
    """The history iterator stopped before `end`: deal with the unexplained `[start, end]`.

    Returns where to page the history from next (`end + 1` = done with the range). Every
    path either finishes the range or moves forward, so the caller's loop terminates.
    """
    reporter, chat_key = ctx.reporter, ctx.chat_key
    size = end - start + 1
    if progressed and size > GAP_FETCH_MAX:
        reporter.debug(
            f"↻ history paging stopped at id {start - 1} (range ends at {end}); resuming"
        )
        return start
    if size <= TAIL_CHECK_MAX_IDS:
        # cheap and definitive: ids that exist are archived, the others are covered
        await _sync_small_gaps(ctx, [(start, end)], label="trailing id range(s) to verify")
        return end + 1
    # Large tail and paging made no progress: ask from the other side (newest message
    # below end + 1) whether anything is left in it.
    probe = await ctx.call(
        lambda: ctx.client.get_messages(ctx.entity, limit=1, max_id=end + 1, min_id=start - 1),
        "probe_history_tail",
    )
    newest = _first_msg_id(probe)
    if newest < start or newest > end:
        # two independent requests agree: nothing is left in the tail
        ctx.db.add_synced_range(chat_key, start, end)
        reporter.debug(f"∅ ids {start}–{end} empty/deleted on the server (confirmed)")
        return end + 1
    # Messages exist further on but paging can't reach them: check the next window by id
    # (always progresses), then page again from there.
    window_end = min(end, start + TAIL_CHECK_MAX_IDS - 1)
    reporter.warn(
        f"⚠ history paging stopped at id {start - 1} although id {newest} exists; "
        f"checking ids {start}–{window_end} directly"
    )
    await _sync_small_gaps(ctx, [(start, window_end)], label="id range(s) past a history stall")
    return window_end + 1


async def _sync_small_gaps(
    ctx: _ChatCtx, gaps: list[tuple[int, int]], *, label: str = "small gap(s)"
) -> int:
    """Resolve many small missing ranges with `get_messages(ids=...)`, 100 ids per request.

    Small holes are usually deleted messages; checking them one history request each made
    every restart slow. Ids that don't exist come back as None and are marked as covered —
    one batched range update per request (thousands of holes used to take minutes of
    blocking SQLite work). Returns the number of messages found.
    """
    reporter, chat_key, since = ctx.reporter, ctx.chat_key, ctx.since_date
    total_ids = sum(e - s + 1 for s, e in gaps)
    reporter.info(
        f"🧩 checking {len(gaps)} {label} ({total_ids} ids) in batches of {GAP_IDS_PER_REQUEST}"
    )
    found = 0
    for chunk in _iter_id_chunks(gaps, GAP_IDS_PER_REQUEST):
        fetched = await ctx.call(
            lambda c=chunk: ctx.client.get_messages(ctx.entity, ids=c), "get_messages_gaps"
        )
        wanted = set(chunk)
        msgs = sorted(
            (m for m in (fetched or []) if m is not None and int(getattr(m, "id", 0)) in wanted),
            key=lambda m: int(m.id),
        )
        batch: list[Any] = []
        skipped: set[int] = set()
        for msg in msgs:
            msg_dt = getattr(msg, "date", None)
            if since is not None and isinstance(msg_dt, datetime) and msg_dt < since:
                skipped.add(int(msg.id))  # older than --since: not saved, not checkpointed
                continue
            batch.append(await ctx.process(msg))
        if batch:
            found += len(batch)
            await ctx.flush(batch)
        # checkpoint every contiguous run of checked ids in this chunk (found or not), except
        # ids skipped because of --since — after their messages were written
        ctx.db.add_synced_ranges(chat_key, _id_runs(i for i in chunk if i not in skipped))
    return found


# ----------------------------------------------------------------------------------------
# DMs, Saved Messages and basic groups: per-account message ids
# ----------------------------------------------------------------------------------------
async def _sync_per_account_chat(ctx: _ChatCtx) -> None:
    cursor = ctx.db.get_chat_cursor(ctx.chat_key, ctx.account_id) or {}
    backfill_done = bool(cursor.get("backfill_completed"))
    resume_offset = int(cursor["backfill_offset_id"]) if cursor.get("backfill_offset_id") else None
    # Decided before the backfill: a fresh backfill sets the forward cursor itself
    forward_from, forward_missing = _forward_start(cursor, backfill_done, resume_offset)
    if not backfill_done:
        await _backfill_history(ctx, resume_offset)
    if forward_from is not None:
        await _fetch_new_messages(ctx, forward_from, forward_missing)


def _forward_start(
    cursor: dict[str, Any], backfill_done: bool, resume_offset: int | None
) -> tuple[int | None, bool]:
    """Where the forward ("new messages") walk starts: `(min_id or None, cursor_missing)`.

    The forward cursor (`forward_max_id`) is owned by `sync`: it only advances over ids that
    sync walked contiguously, so out-of-sequence writers (e.g. `tghoard watch` saving one
    live message) can't make it skip messages. Databases from before the cursor existed
    derive a safe start instead of `MAX(msg_id)` (which watch could have advanced):
    - backfill in progress: the backfill walked `[resume_offset, top]` already, so start at
      `resume_offset`;
    - backfill completed: walk the whole history once (messages already archived are
      skipped without rewriting anything; only pages are fetched).
    A fresh backfill (nothing archived yet) sets the cursor itself: no forward walk.
    """
    fwd = cursor.get("forward_max_id")
    if fwd is not None:
        return int(fwd), False
    if backfill_done:
        return 0, True
    if resume_offset is not None:
        return resume_offset, True
    return None, False


async def _backfill_history(ctx: _ChatCtx, resume_offset: int | None) -> None:
    """Initial history backfill, newest -> oldest, resumable from `resume_offset`.

    Only marked completed once confirmed: Telethon's iterator also ends on odd pages, so a
    `get_messages(limit=1, max_id=oldest)` must find nothing older first.
    """
    reporter, since = ctx.reporter, ctx.since_date
    if resume_offset is not None:
        reporter.info(f"🔎 resuming history backfill from id {resume_offset} (newest to oldest)")
    else:
        reporter.info("🔎 initial history backfill (newest to oldest)")
    fresh = resume_offset is None
    # newest id of a fresh backfill: the forward walk takes over above it
    top_seen: int | None = None
    oldest = resume_offset
    while True:
        batch: list[Any] = []
        progressed = stopped_by_since = False
        iter_kwargs: dict[str, Any] = {"offset_id": oldest} if oldest is not None else {}
        async for msg in ctx.history(**iter_kwargs):
            msg_id = int(getattr(msg, "id", 0) or 0)
            if msg_id <= 0 or (oldest is not None and msg_id >= oldest):
                continue  # only ever older ids (defensive)
            reporter.bar.advance(1)
            msg_dt = getattr(msg, "date", None)
            if since is not None and isinstance(msg_dt, datetime) and msg_dt < since:
                stopped_by_since = True
                break
            item = await ctx.process(msg)
            if fresh and top_seen is None:
                top_seen = msg_id
            # Newest -> oldest: flush when the month changes (or the batch gets large), so
            # each month is repacked once instead of every 100 messages. Resume point is
            # the oldest message already written (`offset_id` returns older ones).
            if batch and (item[0] != batch[-1][0] or len(batch) >= BACKFILL_BATCH_SIZE):
                await ctx.flush(
                    batch,
                    cursor_offset_id=batch[-1][1],
                    cursor_completed=False,
                    forward_max_id=top_seen,
                )
                batch = []
            batch.append(item)
            oldest = msg_id
            progressed = True
        if batch:
            await ctx.flush(
                batch,
                cursor_offset_id=batch[-1][1],
                cursor_completed=False,
                forward_max_id=top_seen,
            )
        if stopped_by_since:
            return  # resume older history later from the oldest saved message

        def _probe(before: int | None = oldest) -> Awaitable[Any]:
            if before is None:
                return ctx.client.get_messages(ctx.entity, limit=1)
            return ctx.client.get_messages(ctx.entity, limit=1, max_id=before)

        older = _first_msg_id(await ctx.call(_probe, "confirm_backfill_end"))
        if older <= 0 or (oldest is not None and older >= oldest):
            ctx.save_cursor(backfill_offset_id=None, backfill_completed=True)
            return
        if not progressed:
            reporter.warn(
                f"⚠ history paging stopped at id {oldest} although older messages exist "
                f"(e.g. id {older}); the backfill resumes there on the next sync"
            )
            return
        reporter.debug(f"↻ history paging stopped early at id {oldest}; resuming the backfill")


async def _fetch_new_messages(ctx: _ChatCtx, forward_from: int, cursor_missing: bool) -> None:
    """Walk ids above the forward cursor (oldest -> newest) and archive unknown messages.

    Runs also while the older-history backfill is incomplete (e.g. always using --since).
    Messages this account already archived (e.g. saved live by `tghoard watch`) are skipped.
    The cursor advances to the highest id walked, and only after the messages below it were
    written — an iterator that stops early simply resumes from there next time.
    """
    reporter = ctx.reporter
    if cursor_missing:
        reporter.info(
            f"🔎 looking for messages after id {forward_from} not archived yet "
            "(one-time check: earlier versions kept no forward cursor)"
        )
    else:
        reporter.info(f"🔎 fetching new messages after id {forward_from}")
    batch: list[Any] = []
    walked = forward_from
    since_save = 0

    async def _save() -> None:
        if batch:
            await ctx.flush(batch, forward_max_id=walked)
        else:
            ctx.save_cursor(forward_max_id=walked)

    async for msg in ctx.history(min_id=forward_from, reverse=True):
        msg_id = int(getattr(msg, "id", 0) or 0)
        if msg_id <= walked:
            continue  # ascending walk (also drops duplicates)
        reporter.bar.advance(1)
        walked = msg_id
        since_save += 1
        if not ctx.db.is_account_msg_known(ctx.account_id, ctx.chat_key, msg_id):
            batch.append(await ctx.process(msg))
        if len(batch) >= BATCH_SIZE or since_save >= FORWARD_CURSOR_EVERY:
            await _save()
            batch = []
            since_save = 0
    await _save()  # also records the cursor when nothing new was found


async def _process_single_message(
    *,
    client: Any,
    msg: Any,
    chat_key: str,
    account_id: int,
    selection: ChatSelection,
    chat_paths: ChatPaths,
    db: CheckpointDB,
    media_manager: MediaManager,
    limiter: RateLimiter,
    res: ChatSyncResult,
    reporter: SyncReporter = NULL_REPORTER,
    pool: MediaDownloadPool,
) -> PendingItem:
    msg_id = int(getattr(msg, "id", 0))
    date_iso = to_utc_iso(getattr(msg, "date", None)) or "1970-01-01T00:00:00+00:00"
    # JSON is partitioned by month; media file names keep the full day.
    day_str = month_from_iso(date_iso)
    file_day = date_iso[:10]
    text = str(getattr(msg, "message", None) or getattr(msg, "raw_text", None) or "")

    sender_id_raw = getattr(msg, "sender_id", None)
    if sender_id_raw is None and getattr(msg, "sender", None) is not None:
        sender_id_raw = getattr(msg.sender, "id", None)
    sender_id = int(sender_id_raw) if sender_id_raw is not None else None

    media_obj = getattr(msg, "media", None)
    classified = classify_telethon_media(media_obj)
    media_tg_id = classified.tg_id if classified is not None else None

    # Check if `(account_id, chat_key, msg_id)` was already mapped earlier (e.g., before an edit)
    existing_locs = db.locate_by_account_msg(account_id, msg_id, chat_key=chat_key)
    if existing_locs:
        canonical_key = existing_locs[0].canonical_key
        day_str = existing_locs[0].day_str
    else:
        key_args: dict[str, Any] = {
            "chat_key": chat_key,
            "msg_id": msg_id,
            "sender_id": sender_id,
            "date_iso": date_iso,
            "media_tg_id": media_tg_id,
            "grouped_id": getattr(msg, "grouped_id", None),
            "text": text,
        }
        # Messages with neither text nor media (service events, polls, ...) also mix in
        # their content, so two of them from one sender in the same second don't collide.
        content_tag = message_content_tag(msg)
        canonical_key = make_canonical_message_key(**key_args, content_tag=content_tag)
        # Check if another account already synced this canonical_key (e.g., basic group)
        already_loc = db.locate_by_canonical(chat_key, canonical_key)
        if already_loc is None and content_tag is not None:
            # Archived by another account before content tags existed: reuse that key
            # (no duplicate) unless it already belongs to another message of this account.
            legacy_key = make_canonical_message_key(**key_args)
            if legacy_key != canonical_key:
                legacy_loc = db.locate_by_canonical(chat_key, legacy_key)
                if legacy_loc is not None and not db.account_maps_canonical(
                    account_id, chat_key, legacy_key
                ):
                    canonical_key, already_loc = legacy_key, legacy_loc
        if already_loc is not None:
            res.messages_dedup_skipped += 1
        else:
            res.messages_saved += 1

    if media_obj is None or classified is None:
        msg_dict = serialize_message_to_dict(
            msg, account_id=account_id, canonical_key=canonical_key, media_meta=None
        )
        return PendingItem(day_str, msg_id, canonical_key, sender_id, msg_dict=msg_dict)

    async def _media_then_serialize() -> dict[str, Any]:
        outcome = await media_manager.process_message_media(
            media=media_obj,
            selection=selection,
            chat_paths=chat_paths,
            chat_key=chat_key,
            canonical_key=canonical_key,
            account_id=account_id,
            msg_id=msg_id,
            day_str=day_str,
            file_day=file_day,
            downloader=_make_downloader(
                client=client,
                limiter=limiter,
                reporter=reporter,
                msg=msg,
                size_bytes=classified.size_bytes,
                label=f"{escape(selection.title[:24])} #{msg_id} ({classified.category})",
                description=f"download_media({chat_key}#{msg_id})",
            ),
        )
        media_meta: dict[str, Any] | None = None
        if outcome is not None:
            media_meta = outcome.metadata
            _report_media(reporter, outcome, classified, msg_id)
            if outcome.network_downloaded:
                res.media_downloaded += 1
            elif outcome.dedup_reused:
                res.media_dedup_reused += 1
            elif outcome.queued_pending:
                res.media_deferred += 1
            elif outcome.failed:
                res.media_failed += 1
        return serialize_message_to_dict(
            msg, account_id=account_id, canonical_key=canonical_key, media_meta=media_meta
        )

    # No network needed (category unchecked -> just queued, already on disk -> hardlink, too
    # big -> skipped): do it inline, it's instant. Only real downloads use the pool, so its
    # queue and workers (and the speed graph) reflect actual Telegram traffic.
    if not media_manager.needs_network(media_obj, selection):
        msg_dict = await _media_then_serialize()
        return PendingItem(day_str, msg_id, canonical_key, sender_id, msg_dict=msg_dict)

    # Hand the download to the account's worker pool and keep reading messages (waits only
    # if the pool's channel is full). The batch flush awaits the returned future.
    future = await pool.submit(_media_then_serialize, group=chat_key)
    return PendingItem(day_str, msg_id, canonical_key, sender_id, task=future)


def _report_media(
    reporter: SyncReporter, outcome: Any, classified: ClassifiedMedia, msg_id: int
) -> None:
    meta = outcome.metadata or {}
    size = human_bytes(meta.get("size_bytes") or classified.size_bytes)
    if outcome.failed:
        reporter.warn(
            f"✗ download failed: {classified.category} ({size}) — msg #{msg_id}: "
            f"{escape(str(outcome.error))} → will retry next sync"
        )
    elif outcome.network_downloaded:
        reporter.info(f"⬇ {meta.get('path')} ({size}) — msg #{msg_id}")
        live = reporter.live
        if live is not None:
            live.add_files(1)
            # Large files report bytes progressively (download bar); count small ones here
            if (classified.size_bytes or 0) < DOWNLOAD_BAR_MIN_BYTES:
                live.add_bytes(int(meta.get("size_bytes") or classified.size_bytes or 0))
    elif outcome.dedup_reused:
        reporter.debug(f"♻ {meta.get('path')} reused (already downloaded) — msg #{msg_id}")
    elif outcome.queued_pending:
        reporter.debug(
            f"⏸ {classified.category} deferred (checkbox unchecked) — msg #{msg_id} ({size})"
        )
    elif meta.get("skip_reason"):
        reporter.info(
            f"⏭ {classified.category} skipped ({meta['skip_reason']}, {size}) — msg #{msg_id}; "
            "stays queued in case the limit is raised"
        )


def _part_signatures(chat_paths: ChatPaths, month: str) -> dict[Path, tuple[int, int, int]]:
    sigs: dict[Path, tuple[int, int, int]] = {}
    for _, path in list_month_part_files(chat_paths, month_from_iso(month)):
        try:
            st = path.stat()
        except FileNotFoundError:
            continue
        sigs[path] = (st.st_ino, st.st_mtime_ns, st.st_size)
    return sigs


def _attach_media_batch(
    *,
    writer: ChatPartWriter,
    db: CheckpointDB,
    chat_paths: ChatPaths,
    chat_key: str,
    month: str,
    part_num: int,
    media_by_key: dict[str, dict[str, Any]],
) -> set[str]:
    """Attach downloaded media to messages expected in `month`/`part_num` (one rewrite per
    touched part); returns the keys found. A key missing from the result is in no part of
    the month.

    The writer falls back to the month's other parts when the locator's part is stale;
    those parts get rewritten (new inode), which is how the messages found there are
    detected and their locators re-pointed — at no cost in the common case.
    """
    before = _part_signatures(chat_paths, month)
    found = set(
        writer.attach_downloaded_media_batch(
            chat_paths=chat_paths, month=month, part_num=part_num, media_by_key=media_by_key
        )
    )
    hint_path = part_file_path(chat_paths, month_from_iso(month), part_num)
    repaired: dict[str, int] = {}
    for pnum, path in list_month_part_files(chat_paths, month_from_iso(month)):
        if path == hint_path or not found - repaired.keys():
            continue
        try:
            st = path.stat()
        except FileNotFoundError:
            continue
        if before.get(path) == (st.st_ino, st.st_mtime_ns, st.st_size):
            continue  # untouched: none of our messages is there
        payload = json.loads(path.read_text(encoding="utf-8"))
        for msg in payload.get("messages") or []:
            key = str(msg.get("canonical_key") or "")
            if key in found and key not in repaired:
                repaired[key] = pnum
    if repaired:
        db.update_day_part_numbers(chat_key, month, repaired)
    return found


class _MediaAttacher:
    """Buffers finished downloads and records them part by part.

    Rewriting a whole (up to 1 MB) JSON part once per downloaded file blocked the event loop
    for hours on big queues. Finished items are grouped by `(month, part)` and each flush
    writes every touched part once, then marks its rows downloaded in one transaction.
    Order per part: JSON written first, then the queue — a crash in between only re-attaches
    the file on the next sync (it is already on disk and in `media_index`).
    """

    def __init__(
        self,
        *,
        db: CheckpointDB,
        writer: ChatPartWriter,
        chat_paths: ChatPaths,
        chat_key: str,
        reporter: SyncReporter,
    ) -> None:
        self.db, self.writer, self.chat_paths = db, writer, chat_paths
        self.chat_key, self.reporter = chat_key, reporter
        self._buf: dict[tuple[str, int], dict[str, tuple[PendingMediaRow, dict[str, Any]]]] = {}
        self._count = 0
        self._first_at: float | None = None
        self.attached = 0
        self.not_found = 0

    @property
    def pending(self) -> int:
        return self._count

    def add(self, row: PendingMediaRow, meta: dict[str, Any]) -> None:
        loc = self.db.locate_by_canonical(row.chat_key, row.canonical_key)
        month = loc.day_str if loc is not None else row.day_str
        part_num = loc.part_num if loc is not None else 0
        slot = self._buf.setdefault((month, part_num), {})
        if row.canonical_key not in slot:
            self._count += 1
        slot[row.canonical_key] = (
            row,
            {
                "path": str(meta["path"]),
                "sha256": str(meta["sha256"]),
                "size_bytes": int(meta["size_bytes"]),
            },
        )
        if self._first_at is None:
            self._first_at = time.monotonic()

    def maybe_flush(self) -> None:
        if self._count >= MEDIA_FLUSH_MAX_ITEMS or (
            self._first_at is not None and time.monotonic() - self._first_at >= MEDIA_FLUSH_MAX_SECS
        ):
            self.flush()

    def flush(self) -> None:
        buf, self._buf, self._count, self._first_at = self._buf, {}, 0, None
        for (month, part_num), items in sorted(buf.items()):
            found = _attach_media_batch(
                writer=self.writer,
                db=self.db,
                chat_paths=self.chat_paths,
                chat_key=self.chat_key,
                month=month,
                part_num=part_num,
                media_by_key={ckey: media for ckey, (_, media) in items.items()},
            )
            self.db.mark_pending_media_downloaded_many(
                self.chat_key, [ckey for ckey in items if ckey in found]
            )
            self.attached += sum(1 for ckey in items if ckey in found)
            for ckey, (row, media) in items.items():
                if ckey in found:
                    continue
                # Never mark the queue done while the JSON says "not downloaded": keep it
                # queued (the file is on disk/indexed, so a retry costs no network).
                self.not_found += 1
                self.db.record_media_failure(
                    row, f"message_not_found_in_json: {month} (downloaded to {media['path']})"
                )
                self.reporter.warn(
                    f"✗ msg #{row.msg_id}: downloaded {media['path']} but the message is not "
                    f"in messages/{self.chat_paths.folder}/{month}-*.json; kept queued"
                )


async def drain_pending_media(
    *,
    client: Any,
    entity: Any,
    account_id: int,
    selection: ChatSelection,
    chat_paths: ChatPaths,
    db: CheckpointDB,
    writer: ChatPartWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
    reporter: SyncReporter = NULL_REPORTER,
    media_pool: MediaDownloadPool,
) -> int:
    """Download media for categories newly checked in `chats.md` on previously synced messages.

    A streaming pipeline over the chat's queue, read in pages of 1000 rows (never the whole
    queue in memory): items already in `media_index` are linked locally; the rest are
    re-fetched 100 at a time (fresh `file_reference`) and handed to the account's download
    pool right away (its bounded channel paces the fetching). Finished files are recorded in
    batches (`_MediaAttacher`), also when interrupted.
    """
    enabled_cats = selection.enabled_categories()
    if not enabled_cats:
        return 0
    chat_key = selection.chat_key
    now = time.time()
    # Oversized items stay queued (cheap check, no RPC) until TGHOARD_MAX_MEDIA_MB is raised.
    max_bytes = media_manager.max_media_bytes
    summary = db.pending_media_summary(chat_key, enabled_cats, max_bytes=max_bytes, due_at=now)
    if summary["oversized"]:
        reporter.debug(f"⏭ {summary['oversized']} queued media file(s) above max_media_mb (kept)")
    if summary["backed_off"]:
        reporter.debug(
            f"⏸ {summary['backed_off']} queued media file(s) missing on the server: "
            "retried later (backoff)"
        )
    if not summary["due"]:
        return 0
    retrying = summary["retrying"]
    reporter.bar.update(total=summary["due"], completed=0, unit="pending media")
    reporter.info(
        f"📥 {summary['due']} pending media file(s) for {', '.join(sorted(enabled_cats))}"
        + (f" ({retrying} retry(ies) of earlier failures)" if retrying else "")
    )

    attacher = _MediaAttacher(
        db=db, writer=writer, chat_paths=chat_paths, chat_key=chat_key, reporter=reporter
    )
    failed = 0
    # Rows are independent: record each download as soon as it finishes (any order). An
    # in-order queue let one slow/starved job block recording of everything behind it.
    outstanding: set[asyncio.Future[Any]] = set()
    ready: deque[tuple[PendingMediaRow, Any, asyncio.Future[Any]]] = deque()

    def _settle(row: PendingMediaRow, remote_msg: Any, outcome: Any) -> None:
        nonlocal failed
        reporter.bar.advance(1)
        if outcome is None:
            # the message's media is no longer something we can download (edited?)
            db.record_media_failure(row, "media_not_downloadable_on_server", permanent=True)
            failed += 1
            return
        failed += int(outcome.failed)
        cls_row = classify_telethon_media(remote_msg.media)
        if cls_row is not None:
            _report_media(reporter, outcome, cls_row, row.msg_id)
        if outcome.metadata.get("downloaded"):
            attacher.add(row, outcome.metadata)

    def _settle_finished(*, only_successful: bool = False) -> None:
        """Record every download that has finished so far (completion order)."""
        while ready:
            row, remote_msg, fut = ready.popleft()
            if fut.cancelled():
                continue  # stays pending; retried next sync
            if only_successful and fut.exception() is not None:
                continue
            _settle(row, remote_msg, fut.result())

    def _track(row: PendingMediaRow, remote_msg: Any, fut: asyncio.Future[Any]) -> None:
        outstanding.add(fut)

        def _done(f: asyncio.Future[Any]) -> None:
            outstanding.discard(f)
            ready.append((row, remote_msg, f))

        fut.add_done_callback(_done)

    async def _from_index(row: PendingMediaRow) -> bool:
        """Resolve an item already downloaded elsewhere (`media_index`) without Telegram."""
        # only a complete copy (regular file with the indexed size); a truncated one is
        # downloaded again
        indexed = media_manager.indexed_copy(row.tg_type, row.tg_id)
        if indexed is None:
            return False
        classified = ClassifiedMedia(
            category=row.category,
            tg_type=row.tg_type,
            tg_id=row.tg_id,
            file_name=row.file_name,
            mime_type=row.mime_type,
            size_bytes=row.size_bytes,
            default_ext="." + indexed.rel_path.rsplit(".", 1)[-1]
            if "." in indexed.rel_path
            else ".bin",
        )
        outcome = await media_manager._acquire_media_file(
            classified=classified,
            chat_paths=chat_paths,
            prefix_tag=f"{row.day_str}_{row.msg_id}",
            base_meta={"category": row.category, "tg_type": row.tg_type, "tg_id": row.tg_id},
            downloader=lambda _: None,  # type: ignore[return-value,arg-type]
        )
        if not outcome.metadata.get("downloaded"):
            return False
        reporter.bar.advance(1)
        attacher.add(row, outcome.metadata)
        return True

    async def _fetch_and_submit(rows: list[PendingMediaRow]) -> None:
        nonlocal failed
        ids = [r.msg_id for r in rows]
        fetched = await limiter.call(
            lambda: client.get_messages(entity, ids=ids),
            description=f"fetch_pending_media({chat_key})",
        )
        msg_by_id: dict[int, Any] = {}
        for item in fetched or []:
            if item is not None and getattr(item, "id", None) is not None:
                msg_by_id[int(item.id)] = item

        for row in rows:
            remote_msg = msg_by_id.get(row.msg_id)
            if remote_msg is None or getattr(remote_msg, "media", None) is None:
                reporter.bar.advance(1)
                # Message (or its media) is gone from the server; keep the record, retry
                # with a backoff (see checkpoint_db.permanent_failure_retry_after).
                db.record_media_failure(row, "message_or_media_not_found_on_server", permanent=True)
                failed += 1
                reporter.debug(f"✗ msg #{row.msg_id} has no media on the server (deleted?)")
                continue
            job = functools.partial(
                media_manager.process_message_media,
                media=remote_msg.media,
                selection=selection,
                chat_paths=chat_paths,
                chat_key=row.chat_key,
                canonical_key=row.canonical_key,
                account_id=account_id,
                msg_id=row.msg_id,
                day_str=row.day_str,
                file_day=(to_utc_iso(getattr(remote_msg, "date", None)) or "")[:10] or None,
                downloader=_make_downloader(
                    client=client,
                    limiter=limiter,
                    reporter=reporter,
                    msg=remote_msg,
                    size_bytes=row.size_bytes,
                    label=f"{escape(selection.title[:24])} #{row.msg_id} ({row.category})",
                    description=f"download_pending({row.chat_key}#{row.msg_id})",
                ),
            )
            # waits only while the pool's channel is full (backpressure)
            fut = await media_pool.submit(job, group=chat_key)
            _track(row, remote_msg, fut)
            _settle_finished()
            attacher.maybe_flush()

    can_fetch_all = uses_global_msg_ids(chat_key)
    to_fetch: list[PendingMediaRow] = []
    try:
        for page in db.iter_pending_media_pages(
            chat_key, enabled_cats, page_size=PENDING_PAGE_SIZE, due_at=now
        ):
            for row in page:
                if max_bytes is not None and (row.size_bytes or 0) > max_bytes:
                    continue
                if await _from_index(row):
                    continue
                if not (can_fetch_all or row.account_id == account_id):
                    reporter.bar.advance(1)  # another account's message id: theirs to fetch
                    continue
                to_fetch.append(row)
                if len(to_fetch) >= PENDING_FETCH_CHUNK:
                    chunk, to_fetch = to_fetch, []
                    await _fetch_and_submit(chunk)
            _settle_finished()
            attacher.maybe_flush()
            await asyncio.sleep(0)  # never hold the loop for a whole page
        if to_fetch:
            await _fetch_and_submit(to_fetch)
        while outstanding:
            done, _ = await asyncio.wait(
                set(outstanding),
                timeout=MEDIA_FLUSH_IDLE_SECS if attacher.pending else None,
                return_when=asyncio.FIRST_COMPLETED,
            )
            await asyncio.sleep(0)  # let done-callbacks run
            _settle_finished()
            if done:
                attacher.maybe_flush()
            else:
                attacher.flush()  # nothing finished for a while: persist what is done
        await asyncio.sleep(0)
        _settle_finished()
        attacher.flush()
    finally:
        # interrupted? still record the files that finished (no re-download next time),
        # without letting a recording error hide the original exception
        try:
            _settle_finished(only_successful=True)
            attacher.flush()
        except Exception as exc:  # noqa: BLE001
            reporter.warn(f"⚠ could not record finished downloads: {exc}")
    downloaded_count = attacher.attached
    failed += attacher.not_found

    if downloaded_count or failed:
        reporter.info(f"📥 media queue: {downloaded_count} done, {failed} still failing")
    return downloaded_count
