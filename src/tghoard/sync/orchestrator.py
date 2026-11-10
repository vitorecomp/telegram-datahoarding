"""Multi-account sync orchestrator driven by `<data_dir>/chats.md`."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.markup import escape
from rich.table import Table
from telethon import errors as tg_errors

from tghoard.config import AppConfig
from tghoard.storage.chat_writer import ChatPartWriter
from tghoard.storage.chats_manifest import ChatSelection, ManifestSource
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB
from tghoard.storage.media_manager import MediaManager, fatal_storage_error
from tghoard.sync.folders_sync import sync_folders_for_account
from tghoard.sync.live_state import LiveSyncState
from tghoard.sync.media_pool import MediaDownloadPool, PoolStats
from tghoard.sync.messages_sync import sync_chat_messages
from tghoard.sync.metadata_sync import sync_chat_metadata
from tghoard.sync.pts_diff import PtsDiffStats, reconcile_recent_window, sync_account_pts_diff
from tghoard.sync.reporter import Stopwatch, SyncProgress, SyncReporter
from tghoard.telegram import flood_control
from tghoard.telegram.entities import channel_join_date, classify_entity_to_chat_info
from tghoard.telegram.flood_control import FloodController
from tghoard.telegram.takeout import open_sync_session
from tghoard.telegram.throttler import (
    RateLimiter,
    dropped_connection_error,
    is_spurious_cancellation,
    retry_transient,
)

# The account lost its authorization (session revoked, key unregistered, account banned):
# none of its other chats can work, so that account stops — the other accounts go on.
ACCOUNT_FATAL_ERRORS: tuple[type[BaseException], ...] = (
    tg_errors.UnauthorizedError,
    tg_errors.AuthKeyError,
    tg_errors.AuthKeyNotFound,
)
_SQLITE_READONLY, _SQLITE_FULL = 8, 13  # primary result codes (low byte of extended codes)


class ArchiveStorageError(RuntimeError):
    """The archive can't be written (disk full, read-only, drive gone): the run stops.

    Raised by `run_multi_account_sync` after cancelling every account: continuing would
    only fail every remaining chat. Progress already checkpointed is kept.
    """


@dataclass
class OrchestratorStats:
    accounts_synced: int = 0
    chats_synced: int = 0
    messages_saved: int = 0
    messages_dedup_skipped: int = 0
    edits_detected: int = 0
    deletes_preserved: int = 0
    media_downloaded: int = 0
    media_dedup_reused: int = 0
    media_deferred: int = 0
    retroactive_media_downloaded: int = 0
    media_failed: int = 0
    media_failed_pending_total: int = 0
    failed_chats: list[tuple[str, str, str]] = field(default_factory=list)
    # (alias, reason): accounts that stopped early (e.g. revoked session); the others went on
    failed_accounts: list[tuple[str, str]] = field(default_factory=list)


async def run_multi_account_sync(
    *,
    account_clients: list[tuple[AccountRecord, Any]],
    selections: dict[str, ChatSelection],
    config: AppConfig,
    db: CheckpointDB,
    chat_filter: str | None = None,
    since_date: datetime | None = None,
    sync_members: bool = True,
    sync_stories: bool = True,
    sync_profile_photos: bool = True,
    console: Console | None = None,
    verbose: bool = False,
    manifest_path: Path | None = None,
    live: LiveSyncState | None = None,
    on_entity: Callable[[str, Any, Any], None] | None = None,
) -> OrchestratorStats:
    """Execute full incremental sync across accounts respecting `<data_dir>/chats.md`.

    Accounts run in parallel (each with its own `RateLimiter`, since Telegram's FloodWait
    limits are per account) and, inside each account, up to `parallel_chats` chats
    sync simultaneously. A lock per `chat_key` ensures two accounts never
    sync the same shared chat at the same time (preserving deduplication).

    Each account's queue is ordered by `prioridade` (highest first). With `manifest_path`,
    `chats.md` is re-read whenever a chat starts: priorities and checkboxes changed in the web
    UI (or by hand) during the sync apply to the chats that have not started yet.
    `on_entity(chat_key, client, entity)` is called for every dialog (e.g. web UI avatars).
    """
    out = console or Console()
    totals = OrchestratorStats()
    clock = Stopwatch()

    manifest = ManifestSource(manifest_path) if manifest_path is not None else None
    enabled_selections = filter_enabled(
        manifest.current() if manifest is not None else selections, chat_filter
    )
    if not enabled_selections:
        out.print("[yellow]No chat marked `[x] baixar` matches the filter.[/yellow]")
        return totals

    ctx = _SyncContext(
        config=config,
        db=db,
        out=out,
        totals=totals,
        enabled_selections=enabled_selections,
        writer=ChatPartWriter(max_json_bytes=config.max_json_bytes),
        media_manager=MediaManager(
            data_dir=config.data_dir,
            checkpoint_db=db,
            max_media_bytes=config.max_media_bytes,
        ),
        since_date=since_date,
        sync_members=sync_members,
        sync_stories=sync_stories,
        sync_profile_photos=sync_profile_photos,
        multi_account=len(account_clients) > 1,
        reporter=SyncReporter(out, verbose=verbose, progress=SyncProgress(live=live)),
        static_selections=selections,
        manifest=manifest,
        chat_filter=chat_filter,
        live=live,
        on_entity=on_entity,
    )
    ctx.reporter.info(
        f"🚀 sync started: {len(account_clients)} account(s), {len(enabled_selections)} chat(s) "
        f"marked \\[x] baixar, up to {config.parallel_chats} chat(s) in parallel per account"
        + (" (verbose mode)" if verbose else "")
    )

    if live is not None:
        live.set_phase("running", "Syncing")
    try:
        # Accounts are isolated: one failing (e.g. revoked session) doesn't cancel the others
        await _run_all(
            [
                partial(_sync_account_isolated, ctx, account, client)
                for account, client in account_clients
            ]
        )
    except ArchiveStorageError as exc:
        out.print(
            f"\n[bold red]✗ Sync stopped: the archive can't be written ({escape(str(exc))}). "
            "Free up space / check the drive and run `tghoard sync` again — progress already "
            "saved stays in the checkpoint.[/bold red]"
        )
        raise
    finally:
        for account, _ in account_clients:  # also when an account failed
            flood_control.unregister(account.account_id)
        if live is not None:
            live.set_phase("done", f"Sync finished in {clock}")

    totals.chats_synced = len(ctx.synced_chat_keys)
    totals.media_failed_pending_total = db.count_failed_media()
    ctx.reporter.info(f"🏁 sync finished in {clock}")
    _print_summary_table(out, totals)
    if ctx.account_errors and len(ctx.account_errors) == len(account_clients):
        # Nothing could run at all: surface the error itself (as before account isolation)
        raise ctx.account_errors[0]
    return totals


@dataclass
class _SyncContext:
    """Shared state (single-thread asyncio) between concurrent accounts and chats."""

    config: AppConfig
    db: CheckpointDB
    out: Console
    totals: OrchestratorStats
    enabled_selections: dict[str, ChatSelection]
    writer: ChatPartWriter
    media_manager: MediaManager
    since_date: datetime | None
    sync_members: bool
    sync_stories: bool
    sync_profile_photos: bool
    multi_account: bool
    reporter: SyncReporter
    static_selections: dict[str, ChatSelection] = field(default_factory=dict)
    manifest: ManifestSource | None = None
    chat_filter: str | None = None
    live: LiveSyncState | None = None
    on_entity: Callable[[str, Any, Any], None] | None = None
    synced_chat_keys: set[str] = field(default_factory=set)
    chat_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
    account_errors: list[BaseException] = field(default_factory=list)

    def chat_lock(self, chat_key: str) -> asyncio.Lock:
        return self.chat_locks.setdefault(chat_key, asyncio.Lock())

    def current_enabled(self) -> dict[str, ChatSelection]:
        """Enabled chats right now (re-reads `chats.md` if it changed since last call)."""
        if self.manifest is None:
            return self.enabled_selections
        self.enabled_selections = filter_enabled(self.manifest.current(), self.chat_filter)
        return self.enabled_selections


def filter_enabled(
    selections: dict[str, ChatSelection], chat_filter: str | None
) -> dict[str, ChatSelection]:
    return {
        k: v
        for k, v in selections.items()
        if v.baixar and (chat_filter is None or chat_filter in (k, v.title, v.username or ""))
    }


async def _run_all(jobs: list[Callable[[], Awaitable[Any]]]) -> None:
    """Run jobs concurrently; if one fails, cancel the others and propagate its error.

    With several errors, the most severe wins: KeyboardInterrupt/SystemExit, then
    `ArchiveStorageError`, then the first one.
    """
    try:
        async with asyncio.TaskGroup() as tg:
            for job in jobs:
                tg.create_task(job())
    except BaseExceptionGroup as eg:
        leaves = list(_leaf_exceptions(eg))

        def severity(exc: BaseException) -> int:
            if not isinstance(exc, Exception):
                return 0
            return 1 if isinstance(exc, ArchiveStorageError) else 2

        raise min(leaves, key=severity) from None


def _leaf_exceptions(exc: BaseException) -> Any:
    if isinstance(exc, BaseExceptionGroup):
        for sub in exc.exceptions:
            yield from _leaf_exceptions(sub)
    else:
        yield exc


def _exception_chain(exc: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    cur: BaseException | None = exc
    while cur is not None and all(cur is not seen for seen in chain):
        chain.append(cur)
        cur = cur.__cause__ or cur.__context__
    return chain


def _probe_archive_writable(probe_dir: Path) -> str | None:
    """Create, fsync and delete a tiny file in `probe_dir`: None if that works, else why."""
    try:
        fd, name = tempfile.mkstemp(prefix=".write-probe-", dir=str(probe_dir))
        try:
            os.write(fd, b"tghoard")
            os.fsync(fd)
        finally:
            os.close(fd)
            os.unlink(name)
    except OSError as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


async def _storage_failure(exc: BaseException, probe_dir: Path) -> str | None:
    """Why `exc` means the archive storage is unusable for *every* chat (None = it doesn't).

    Disk full / read-only / quota (also as reported by SQLite) are fatal right away. Any
    other OSError / SQLite error (EIO, a vanished drive...) triggers a write probe next to
    `checkpoint.db`: if even that fails, the storage is gone; if it works, the error was
    local to that chat (a corrupt part, a bad sector) and only that chat is skipped.
    """
    if (fatal := fatal_storage_error(exc)) is not None:
        where = f" ({fatal.filename})" if fatal.filename else ""
        return f"{os.strerror(fatal.errno or 0)}{where}"
    chain = _exception_chain(exc)
    for err in chain:
        if isinstance(err, sqlite3.Error):
            code = (getattr(err, "sqlite_errorcode", None) or 0) & 0xFF
            if code in (_SQLITE_READONLY, _SQLITE_FULL):
                return f"checkpoint database: {err}"
    if any(
        isinstance(err, (OSError, sqlite3.Error))
        and not isinstance(err, (ConnectionError, TimeoutError))
        for err in chain
    ):
        problem = await asyncio.to_thread(_probe_archive_writable, probe_dir)
        if problem is not None:
            return f"write test failed: {problem}"
    return None


def _describe_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


async def _sync_account_isolated(
    ctx: _SyncContext, account: AccountRecord, raw_client: Any
) -> None:
    """One account's sync; if it fails, record it and let the other accounts go on.

    Only an `ArchiveStorageError` (and real cancellation: Ctrl+C / shutdown) stops the run.
    """
    try:
        await _sync_account(ctx, account, raw_client)
    except asyncio.CancelledError:
        if not is_spurious_cancellation():
            raise
        # Telethon cancelled a request on a dropped connection outside any retry; left as
        # CancelledError the TaskGroup would silently drop this account.
        _record_account_failure(ctx, account, dropped_connection_error())
    except ArchiveStorageError:
        raise
    except Exception as exc:
        storage = await _storage_failure(exc, ctx.config.checkpoint_db_path.parent)
        if storage is not None:
            raise ArchiveStorageError(storage) from exc
        _record_account_failure(ctx, account, exc)


def _record_account_failure(ctx: _SyncContext, account: AccountRecord, exc: BaseException) -> None:
    reason = _describe_error(exc)
    ctx.account_errors.append(exc)
    ctx.totals.failed_accounts.append((account.alias, reason))
    if ctx.live is not None:
        ctx.live.account_update(account.alias, status="failed")
        ctx.live.set_queue(account.alias, [])  # its queued chats won't start in this run
    hint = (
        " Its session is no longer authorized: run `tghoard account add` to log in again."
        if isinstance(exc, ACCOUNT_FATAL_ERRORS)
        else " It will resume on the next `tghoard sync`."
    )
    ctx.out.print(
        f"[bold red]✗ account `{escape(account.alias)}` stopped: {escape(reason)}.{hint}[/bold red]"
    )


async def _sync_account(ctx: _SyncContext, account: AccountRecord, raw_client: Any) -> None:
    out = ctx.out
    config = ctx.config
    clock = Stopwatch()
    rep = ctx.reporter.child(f"[{account.alias}]") if ctx.multi_account else ctx.reporter

    def _pace_changed(msg: str) -> None:
        rep.warn(msg) if msg.startswith("🐢") else rep.info(msg)
        if ctx.live is not None:
            ctx.live.account_update(account.alias, pace=_pace(), floods=throttle.floods)

    def _pace() -> str:
        return f"{config.parallel_chats} chats in parallel (-j) · {throttle.describe()}"

    # Auto-throttle for this account: FloodWaits (also the ones Telethon sleeps silently)
    # slow history paging / reduce parallelism; quiet periods restore speed.
    throttle = FloodController(
        base_wait=config.history_wait,
        max_concurrency=config.concurrency,
        enabled=config.auto_throttle,
        on_change=_pace_changed,
        recover_after=config.throttle_recover_secs,
        cut_ratio=config.throttle_cut_ratio,
    )
    flood_control.register(account.account_id, throttle)
    if ctx.live is not None:
        ctx.live.account_update(account.alias, pace=_pace(), floods=0)
    limiter = RateLimiter(
        concurrency=config.concurrency, console=out, reporter=rep, controller=throttle
    )
    out.print(
        f"\n[bold cyan]▶ Syncing account `{escape(account.alias)}` "
        f"(id={account.account_id}, mode={config.sync_mode}, "
        f"parallel chats={config.parallel_chats})[/bold cyan]"
    )
    ctx.totals.accounts_synced += 1

    # Every dialog of this account (not only enabled ones): chats enabled in chats.md
    # *during* the sync can still be picked up, and the web UI gets avatars for all.
    all_entities: dict[str, Any] = {}

    # Saved Messages (`me`)
    if hasattr(raw_client, "get_me"):
        me_ent = await raw_client.get_me()
        if me_ent is not None:
            all_entities[f"saved_{account.account_id}"] = me_ent

    rep.info("📋 listing the account's dialogs...")

    async def _discover_dialogs() -> list[Any]:
        return [d async for d in raw_client.iter_dialogs()]

    dialogs = await retry_transient(
        _discover_dialogs,
        description=f"iter_dialogs({account.alias})",
        console=out,
        reporter=rep,
    )
    kinds: dict[str, str] = {}
    for dialog in dialogs:
        ent = getattr(dialog, "entity", None)
        if ent is None:
            continue
        info = classify_entity_to_chat_info(
            ent,
            self_id=account.account_id,
            account_alias=account.alias,
        )
        all_entities[info.chat_key] = ent
        kinds[info.chat_key] = info.kind
    if ctx.on_entity is not None:
        for chat_key, ent in all_entities.items():
            ctx.on_entity(chat_key, raw_client, ent)

    # When this account joined each channel/supergroup: free (already in the dialog entities)
    joins = [
        (chat_key, account.account_id, joined, None, "channel.date")
        for chat_key, ent in all_entities.items()
        if (joined := channel_join_date(ent)) is not None
    ]
    if joins:
        ctx.db.set_memberships(joins)

    # Telegram chat folders -> folders.md (+ .tghoard/folders.json for the web UI)
    try:
        folders = await retry_transient(
            partial(sync_folders_for_account, raw_client, account, dialogs, config.data_dir),
            description=f"Telegram folders ({account.alias})",
            console=out,
            reporter=rep,
        )
        rep.info(f"📁 {len(folders)} Telegram folder(s) saved to folders.md")
    except ACCOUNT_FATAL_ERRORS:
        raise
    except Exception as exc:
        rep.warn(f"⚠ could not save the Telegram folders: {escape(_describe_error(exc))}")

    enabled_now = ctx.current_enabled()
    entities_by_key = {k: e for k, e in all_entities.items() if k in enabled_now}
    channel_entities = {
        k: e for k, e in entities_by_key.items() if kinds.get(k) in ("channel", "supergroup")
    }
    rep.info(
        f"📋 {len(dialogs)} dialog(s) in the account; {len(entities_by_key)} marked for download "
        f"({len(channel_entities)} channel(s)/supergroup(s))"
    )

    # Step 1: Offline PTS delta replay (edits & deletions since last sync)
    rep.info("⏪ applying edits/deletions since the last sync (PTS diff)...")
    pts_clock = Stopwatch()
    try:
        pts_stats = await sync_account_pts_diff(
            client=raw_client,
            account_id=account.account_id,
            selections=enabled_now,
            channel_entities=channel_entities,
            data_dir=config.data_dir,
            db=ctx.db,
            writer=ctx.writer,
            limiter=limiter,
            reporter=rep,
        )
    except ACCOUNT_FATAL_ERRORS:
        raise
    except Exception as exc:
        storage = await _storage_failure(exc, config.checkpoint_db_path.parent)
        if storage is not None:
            raise ArchiveStorageError(storage) from exc
        # Best-effort step: its saved pts only advances once applied, so the next sync
        # replays what was missed here. Messages and media can still be synced now.
        rep.warn(
            f"⚠ PTS diff failed ({escape(_describe_error(exc))}): edits/deletions made while "
            "offline will be applied on the next sync"
        )
        pts_stats = PtsDiffStats()
    ctx.totals.deletes_preserved += pts_stats.deleted_marked
    ctx.totals.edits_detected += pts_stats.edits_recorded
    too_long = (
        " | update history expired: auditing recent window"
        if pts_stats.global_too_long or pts_stats.too_long_channels
        else ""
    )
    missing = (
        f" | {len(pts_stats.delete_targets_missing)} deleted message(s) not in the archive"
        if pts_stats.delete_targets_missing
        else ""
    )
    rep.info(
        f"⏪ PTS diff in {pts_clock}: {pts_stats.deleted_marked} deletion(s) and "
        f"{pts_stats.edits_recorded} edit(s) applied to "
        f"{len(pts_stats.touched_chats)} chat(s){too_long}{missing}"
    )

    # One media download pool per account: fixed workers + bounded channel, shared by all
    # of this account's chats; its active-worker limit follows the auto-throttle.
    def _pool_stats(st: PoolStats) -> None:
        if ctx.live is not None:
            ctx.live.account_update(
                account.alias,
                dl_queued=st.queued,
                dl_active=st.active,
                dl_limit=st.limit,
                dl_queue_max=int(st.extra.get("queue_size", 0)),
            )

    media_pool = MediaDownloadPool(
        config.concurrency,
        queue_size=config.download_queue,
        on_stats=_pool_stats,
        name=account.alias,
    )
    media_pool.set_limit(throttle.concurrency)
    throttle.on_concurrency(media_pool.set_limit)
    async with media_pool:
        # Step 2: Sync messages + media inside Takeout (or auto fallback) session
        async with open_sync_session(
            raw_client,
            mode=config.sync_mode,
            max_media_bytes=config.max_media_bytes,
            console=out,
        ) as (sync_client, is_takeout):
            mode_badge = "Takeout" if is_takeout else "Standard"
            rep.info(
                f"Active session: [bold]{mode_badge}[/bold] | "
                f"Enabled chats visible to this account: [bold]{len(entities_by_key)}[/bold]"
            )

            account_bar = rep.add_bar(
                f"[bold cyan]{escape(account.alias)}[/bold cyan] chats",
                total=len(entities_by_key),
                unit="chats",
                kind="account",
                key=account.alias,
            )
            done: set[str] = set()
            running: set[str] = set()
            counters = {"started": 0, "done": 0, "total": len(entities_by_key)}

            def _pick() -> tuple[str, ChatSelection] | None:
                """Highest-priority chat not started yet, from the *current* chats.md."""
                enabled = ctx.current_enabled()
                queue = [
                    k for k in all_entities if k in enabled and k not in done and k not in running
                ]
                queue.sort(key=lambda k: (-enabled[k].priority, enabled[k].title.lower(), k))
                counters["total"] = len(done) + len(running) + len(queue)
                account_bar.update(total=counters["total"])
                if ctx.live is not None:
                    ctx.live.set_queue(
                        account.alias, [(k, enabled[k].title, enabled[k].priority) for k in queue]
                    )
                if not queue:
                    return None
                # Prefer chats no other account is syncing right now (shared chats wait their turn)
                free = [k for k in queue if not ctx.chat_lock(k).locked()]
                chosen = (free or queue)[0]
                return chosen, enabled[chosen]

            async def _worker() -> None:
                try:
                    await _worker_loop()
                except asyncio.CancelledError:
                    me = asyncio.current_task()
                    if me is not None and me.cancelling() == 0:
                        # A CancelledError we didn't ask for would make TaskGroup treat this
                        # worker as "cancelled" and silently skip the rest of its chats.
                        raise RuntimeError("chat worker cancelled unexpectedly") from None
                    raise

            async def _worker_loop() -> None:
                while (picked := _pick()) is not None:
                    chat_key, sel = picked
                    running.add(chat_key)
                    try:
                        await _chat_job(chat_key, all_entities[chat_key], sel)
                    finally:
                        running.discard(chat_key)
                        done.add(chat_key)
                        account_bar.advance(1)

            async def _chat_job(chat_key: str, ent: Any, sel: ChatSelection) -> None:
                title = sel.title if len(sel.title) <= 28 else sel.title[:27] + "…"
                chat_rep = rep.child(f"{title} ·")
                live_fields = {
                    "title": sel.title,
                    "account": account.alias,
                    "priority": sel.priority,
                }
                lock = ctx.chat_lock(chat_key)
                if lock.locked():
                    chat_rep.info("⏳ waiting for another account to finish this shared chat...")
                    if ctx.live is not None:
                        ctx.live.chat_update(chat_key, status="waiting_lock", **live_fields)
                async with lock:
                    bar = rep.add_bar(f"  {escape(title)}", total=None, unit="msgs", key=chat_key)
                    if ctx.live is not None:
                        ctx.live.chat_update(
                            chat_key, status="running", started_at=time.time(), **live_fields
                        )
                    try:
                        await _chat_body(sel, chat_key, ent, rep.child(f"{title} ·", bar=bar))
                    finally:
                        bar.remove()

            async def _chat_body(
                sel: ChatSelection, chat_key: str, ent: Any, chat_rep: SyncReporter
            ) -> None:
                ctx.synced_chat_keys.add(chat_key)
                counters["started"] += 1
                media_cats = ", ".join(sorted(sel.enabled_categories())) or "none"
                chat_rep.info(
                    f"▶ starting [{counters['started']}/{counters['total']}] `{escape(chat_key)}` "
                    f"(priority {sel.priority}, media: {media_cats})"
                )
                chat_clock = Stopwatch()
                try:
                    chat_res = await retry_transient(
                        partial(
                            _sync_one_chat,
                            raw_client=raw_client,
                            sync_client=sync_client,
                            entity=ent,
                            chat_key=chat_key,
                            selection=sel,
                            account=account,
                            pts_stats=pts_stats,
                            config=config,
                            db=ctx.db,
                            writer=ctx.writer,
                            media_manager=ctx.media_manager,
                            limiter=limiter,
                            totals=ctx.totals,
                            since_date=ctx.since_date,
                            sync_members=ctx.sync_members,
                            sync_stories=ctx.sync_stories,
                            sync_profile_photos=ctx.sync_profile_photos,
                            reporter=chat_rep,
                            pacer=throttle if config.auto_throttle else None,
                            media_pool=media_pool,
                        ),
                        description=f"sync of chat `{chat_key}`",
                        console=out,
                        reporter=chat_rep,
                        on_flood=throttle.record,
                    )
                except asyncio.CancelledError:
                    # Real cancellation only (retry_transient turns Telethon's dropped-request
                    # cancellations into ConnectionError): Ctrl+C, or this account stopping.
                    if ctx.live is not None:
                        ctx.live.chat_update(
                            chat_key,
                            status="failed",
                            error="interrupted (resumes on the next sync)",
                            finished_at=time.time(),
                        )
                    raise
                except ACCOUNT_FATAL_ERRORS:
                    raise  # this account can't continue (handled by _sync_account_isolated)
                except Exception as exc:
                    storage = await _storage_failure(exc, config.checkpoint_db_path.parent)
                    if storage is not None:
                        raise ArchiveStorageError(storage) from exc
                    # Persistent failure in this chat (retries exhausted, corrupt part, I/O
                    # error on one file, ...): progress already saved stays in the
                    # checkpoint; move on to the other chats and retry it next sync.
                    reason = _describe_error(exc)
                    ctx.totals.failed_chats.append((account.alias, chat_key, reason))
                    counters["done"] += 1
                    if ctx.live is not None:
                        ctx.live.chat_update(
                            chat_key, status="failed", error=reason, finished_at=time.time()
                        )
                    chat_rep.info(
                        f"[red]✗ [{counters['done']}/{counters['total']}] skipped after "
                        f"{chat_clock} ({escape(reason)}). It will resume on the next "
                        "`tghoard sync`.[/red]"
                    )
                    return

                counters["done"] += 1
                if ctx.live is not None:
                    ctx.live.chat_update(
                        chat_key,
                        status="done",
                        finished_at=time.time(),
                        messages_saved=chat_res.messages_saved,
                        media_downloaded=chat_res.media_downloaded
                        + chat_res.retroactive_media_downloaded,
                        media_failed=chat_res.media_failed,
                    )
                chat_rep.info(
                    f"[green]✓ [{counters['done']}/{counters['total']}][/green] "
                    f"done in {chat_clock}: "
                    f"+{chat_res.messages_saved} msgs "
                    f"(dedup={chat_res.messages_dedup_skipped}, "
                    f"media={chat_res.media_downloaded + chat_res.retroactive_media_downloaded}, "
                    f"reused={chat_res.media_dedup_reused}, deferred={chat_res.media_deferred}, "
                    f"edits={chat_res.edits_detected}, failed={chat_res.media_failed})"
                )

            await _run_all([_worker for _ in range(max(1, config.parallel_chats))])

    ctx.db.touch_account_sync(account.account_id)
    flood_note = (
        f" ({throttle.floods} FloodWait(s), {throttle.flood_seconds:.0f}s waiting)"
        if throttle.floods
        else ""
    )
    rep.info(f"✅ account `{escape(account.alias)}` done in {clock}{flood_note}")


async def _sync_one_chat(
    *,
    raw_client: Any,
    sync_client: Any,
    entity: Any,
    chat_key: str,
    selection: ChatSelection,
    account: AccountRecord,
    pts_stats: Any,
    config: AppConfig,
    db: CheckpointDB,
    writer: ChatPartWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
    totals: OrchestratorStats,
    since_date: datetime | None,
    sync_members: bool,
    sync_stories: bool,
    sync_profile_photos: bool,
    reporter: SyncReporter,
    pacer: FloodController | None = None,
    media_pool: MediaDownloadPool | None = None,
) -> Any:
    """Full, idempotent sync of one chat (PTS audit + messages + metadata).

    Safe to repeat: everything is checkpointed in `checkpoint.db`, so a retry resumes
    from the last saved batch.
    """
    # If PTS reported DifferenceTooLong, audit recent window first
    w_del = w_ed = 0
    raw_id = int(getattr(entity, "id", 0))
    if pts_stats.global_too_long or raw_id in pts_stats.too_long_channels:
        reporter.info(f"🩺 auditing the last {config.audit_window} messages (PTS expired)")
        w_del, w_ed = await reconcile_recent_window(
            client=sync_client,
            entity=entity,
            account_id=account.account_id,
            chat_key=chat_key,
            chat_title=selection.title,
            data_dir=config.data_dir,
            db=db,
            writer=writer,
            limiter=limiter,
            window_size=config.audit_window,
            reporter=reporter,
        )

    chat_res = await sync_chat_messages(
        client=sync_client,
        entity=entity,
        account_id=account.account_id,
        selection=selection,
        data_dir=config.data_dir,
        db=db,
        writer=writer,
        media_manager=media_manager,
        limiter=limiter,
        since_date=since_date,
        reporter=reporter,
        history_wait=config.history_wait,
        pacer=pacer,
        media_pool=media_pool,
    )

    # Metadata changes slowly and costs several requests per chat: refresh it at most every
    # `metadata_every_hours` (restarts then go straight to messages and media).
    last_meta = db.metadata_synced_at(chat_key, account.account_id)
    max_age = config.metadata_every_hours * 3600
    if max_age and last_meta is not None and time.time() - last_meta < max_age:
        hours = (time.time() - last_meta) / 3600
        reporter.debug(
            f"🗂 metadata refreshed {hours:.1f}h ago — skipping (TGHOARD_METADATA_EVERY_HOURS)"
        )
    else:
        await sync_chat_metadata(
            client=raw_client,
            entity=entity,
            account_id=account.account_id,
            account_alias=account.alias,
            selection=selection,
            data_dir=config.data_dir,
            db=db,
            writer=writer,
            media_manager=media_manager,
            limiter=limiter,
            sync_members=sync_members,
            sync_stories=sync_stories,
            sync_profile_photos=sync_profile_photos,
            reporter=reporter,
        )
        db.mark_metadata_synced(chat_key, account.account_id, time.time())

    # Only count after the chat finishes (retries do not double the totals).
    totals.deletes_preserved += w_del
    totals.edits_detected += w_ed
    totals.messages_saved += chat_res.messages_saved
    totals.messages_dedup_skipped += chat_res.messages_dedup_skipped
    totals.edits_detected += chat_res.edits_detected
    totals.media_downloaded += chat_res.media_downloaded
    totals.media_dedup_reused += chat_res.media_dedup_reused
    totals.media_deferred += chat_res.media_deferred
    totals.retroactive_media_downloaded += chat_res.retroactive_media_downloaded
    totals.media_failed += chat_res.media_failed
    return chat_res


def _print_summary_table(console: Console, stats: OrchestratorStats) -> None:
    table = Table(title="Sync summary (tghoard)")
    table.add_column("Metric", style="cyan")
    table.add_column("Total", justify="right", style="bold")
    table.add_row("Accounts processed", str(stats.accounts_synced))
    table.add_row("Chats synced", str(stats.chats_synced))
    table.add_row("New messages saved", str(stats.messages_saved))
    table.add_row("Messages skipped (multi-account dedup)", str(stats.messages_dedup_skipped))
    table.add_row("Edits versioned", str(stats.edits_detected))
    table.add_row("Deleted messages preserved", str(stats.deletes_preserved))
    table.add_row("Attachments downloaded from the network", str(stats.media_downloaded))
    table.add_row("Retroactive attachments downloaded", str(stats.retroactive_media_downloaded))
    table.add_row("Attachments reused without re-download", str(stats.media_dedup_reused))
    table.add_row("Attachments deferred (checkbox unchecked)", str(stats.media_deferred))
    table.add_row("Attachments failed in this run", str(stats.media_failed))
    table.add_row("Attachments awaiting retry (total)", str(stats.media_failed_pending_total))
    table.add_row("Failed chats (resumed next sync)", str(len(stats.failed_chats)))
    if stats.failed_accounts:
        table.add_row("Accounts stopped early", str(len(stats.failed_accounts)))
    console.print()
    console.print(table)
    # Aliases, chat keys and exception texts are user/Telegram-derived: never Rich markup
    if stats.failed_accounts:
        console.print("\n[red]Accounts stopped in this run:[/red]")
        for alias, reason in stats.failed_accounts:
            console.print(f"  • \\[{escape(alias)}] — {escape(reason)}")
    if stats.failed_chats:
        console.print("\n[red]Chats skipped in this run:[/red]")
        for alias, chat_key, reason in stats.failed_chats:
            console.print(f"  • \\[{escape(alias)}] `{escape(chat_key)}` — {escape(reason)}")
