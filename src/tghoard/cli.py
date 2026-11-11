"""CLI entrypoint for `tghoard` (`start`, `sync`, `account`, `watch`, `search`, `stats`)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import threading
import webbrowser
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

from tghoard.config import ALL_MEDIA_CATEGORIES, AppConfig, SyncMode, load_config
from tghoard.migrations import (
    discover_migrations,
    load_applied,
    pending_migrations,
    record_applied,
)
from tghoard.storage.atomic import atomic_write_text
from tghoard.storage.chat_stats import (
    compute_all_chat_stats,
    estimate_to_download,
    list_oversized_media,
    media_size_averages,
    open_readonly_db,
)
from tghoard.storage.chats_manifest import load_chats_manifest
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB
from tghoard.storage.paths import iter_all_part_files, single_line
from tghoard.storage.process_lock import ArchiveLockedError, archive_lock
from tghoard.sync.discovery import run_discovery_and_write_manifest
from tghoard.sync.live_state import LiveSyncState, wait_for_user
from tghoard.sync.orchestrator import (
    ArchiveStorageError,
    filter_enabled,
    run_multi_account_sync,
)
from tghoard.sync.rebuilder import (
    iter_archive_chat_dirs,
    rebuild_checkpoint_from_plaintext,
    rebuild_markdown_from_plaintext,
)
from tghoard.sync.reporter import human_bytes
from tghoard.sync.watcher import run_live_watcher
from tghoard.telegram.accounts import AccountManager
from tghoard.telegram.entities import classify_entity_to_chat_info
from tghoard.web.server import WebUI
from tghoard.web.telegram_bridge import TelegramBridge

app = typer.Typer(
    name="tghoard",
    help="Multi-account plain-text Telegram archiver with SQLite checkpoints.",
    no_args_is_help=True,
)
account_app = typer.Typer(
    name="account",
    help="Manage authenticated Telegram accounts.",
    no_args_is_help=True,
)
app.add_typer(account_app, name="account")

console = Console()

DataDirOption = Annotated[
    Path | None,
    typer.Option(
        "--data-dir",
        "-d",
        help="Output directory for the archive (default: TGHOARD_DATA_DIR or ./data).",
    ),
]


def _resolve_runtime_config(
    *,
    data_dir: Path | None = None,
    max_json_kb: int | None = None,
    max_media_mb: int | None = None,
    concurrency: int | None = None,
    parallel_chats: int | None = None,
    sync_mode: SyncMode | None = None,
    history_wait: float | None = None,
    auto_throttle: bool | None = None,
    metadata_every_hours: float | None = None,
):
    cfg = load_config(".env").with_overrides(
        data_dir=data_dir,
        max_json_kb=max_json_kb,
        max_media_mb=max_media_mb,
        concurrency=concurrency,
        parallel_chats=parallel_chats,
        sync_mode=sync_mode,
        history_wait=history_wait,
        auto_throttle=auto_throttle,
        metadata_every_hours=metadata_every_hours,
    )
    cfg.ensure_dirs()
    return cfg


@contextlib.contextmanager
def _writer_lock(cfg: AppConfig, command: str):
    """Only one process may write to an archive at a time (see storage/process_lock.py)."""
    try:
        with archive_lock(cfg.internal_dir, command):
            yield
    except ArchiveLockedError as exc:
        console.print(f"[red]✗ {escape(str(exc))}[/red]")
        console.print("Stop the other tghoard process (or wait for it to finish) and try again.")
        raise typer.Exit(code=3) from None


def _require_current_layout(cfg: AppConfig) -> None:
    """Refuse to touch an archive that still needs `tghoard migrate` (avoids mixed layouts)."""
    pending = pending_migrations(cfg, required_only=True)
    if not pending:
        return
    console.print(f"[red]The archive at `{cfg.data_dir}` must be migrated before continuing:[/red]")
    for mig in pending:
        console.print(f"  • [bold]{mig.key}[/bold]: {mig.description}")
    console.print(
        "See the plan with [bold]tghoard migrate[/bold] and apply it with "
        "[bold]tghoard migrate --apply[/bold]."
    )
    raise typer.Exit(code=2)


def _select_accounts(
    db: CheckpointDB,
    account_filter: str | None,
) -> list[AccountRecord]:
    if account_filter:
        rec = db.get_account(account_filter)
        if rec is None:
            raise typer.BadParameter(f"Account `{account_filter}` not found.")
        return [rec]
    return db.list_accounts()


# ----------------------------------------------------------------------
# 1. `tghoard start`
# ----------------------------------------------------------------------
@app.command("start")
def cmd_start(
    data_dir: DataDirOption = None,
    add_account: Annotated[
        bool,
        typer.Option(
            "--add-account",
            help="Authenticate a new Telegram account before updating chats.md.",
        ),
    ] = False,
    alias: Annotated[
        str | None,
        typer.Option("--alias", help="Alias for the newly authenticated account."),
    ] = None,
    chats_file: Annotated[
        Path | None,
        typer.Option("--chats-file", help="Alternative path to the chats.md file."),
    ] = None,
) -> None:
    """Authenticate account(s) and create/update `<data_dir>/chats.md` with default checkboxes."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    manifest_path = chats_file or cfg.chats_manifest_path

    async def _run() -> None:
        with CheckpointDB(cfg.checkpoint_db_path) as db:
            mgr = AccountManager(cfg, db, console=console)
            accounts = db.list_accounts()

            if not accounts or add_account:
                await mgr.interactive_add_account(alias=alias)
                accounts = db.list_accounts()

            connected: list[tuple[AccountRecord, object]] = []
            try:
                for acc in accounts:
                    client = await mgr.connect_account(acc)
                    connected.append((acc, client))

                summary = await run_discovery_and_write_manifest(
                    account_clients=connected,
                    manifest_path=manifest_path,
                    console=console,
                    data_dir=cfg.data_dir,
                )
            finally:
                for _, client in connected:
                    await client.disconnect()  # type: ignore[attr-defined]

            console.print(
                f"\n[bold green]✓ Manifest updated at:[/bold green] `{summary.manifest_path}`\n"
                f"  • Chats listed: [bold]{summary.total_chats}[/bold] "
                f"(new: {summary.added_chats}, kept: {summary.updated_chats})\n"
                f"  • Next step: edit the checkboxes in `{summary.manifest_path}` and run "
                "`tghoard sync`."
            )

    with _writer_lock(cfg, "start"):
        asyncio.run(_run())


# ----------------------------------------------------------------------
# 2. `tghoard sync`
# ----------------------------------------------------------------------
@app.command("sync")
def cmd_sync(
    data_dir: DataDirOption = None,
    account: Annotated[
        str | None,
        typer.Option("--account", "-a", help="Sync only one specific account (alias or ID)."),
    ] = None,
    chat: Annotated[
        str | None,
        typer.Option("--chat", "-c", help="Sync only one specific chat (`chat_key` or title)."),
    ] = None,
    mode: Annotated[
        SyncMode | None,
        typer.Option("--mode", help="Session mode: auto | takeout | standard."),
    ] = None,
    max_json_kb: Annotated[
        int | None,
        typer.Option("--max-json-kb", help="Max size (KB) per YYYY-MM-ptNNN.json part."),
    ] = None,
    max_media_mb: Annotated[
        int | None,
        typer.Option("--max-media-mb", help="Max size per attachment in MB (0 = no limit)."),
    ] = None,
    concurrency: Annotated[
        int | None,
        typer.Option("--concurrency", help="Max concurrency of calls/downloads."),
    ] = None,
    parallel_chats: Annotated[
        int | None,
        typer.Option(
            "--parallel-chats",
            "-j",
            min=1,
            max=16,
            help="How many chats to sync simultaneously per account (default 4).",
        ),
    ] = None,
    history_wait: Annotated[
        float | None,
        typer.Option(
            "--history-wait",
            min=0,
            max=10,
            help="Pause (s) between pages of 100 messages (default 0; Telethon would use 1s). "
            "Increase it if you get many FloodWaits.",
        ),
    ] = None,
    refresh_metadata: Annotated[
        bool,
        typer.Option(
            "--refresh-metadata",
            help="Refresh the metadata of all chats now (ignores TGHOARD_METADATA_EVERY_HOURS).",
        ),
    ] = False,
    no_auto_throttle: Annotated[
        bool,
        typer.Option(
            "--no-auto-throttle",
            help="Disable automatic tuning (pause/parallelism) based on FloodWaits.",
        ),
    ] = False,
    since: Annotated[
        str | None,
        typer.Option("--since", help="Earliest date (YYYY-MM-DD) for historical messages."),
    ] = None,
    skip_members: Annotated[
        bool,
        typer.Option("--skip-members", help="Skip syncing the member list."),
    ] = False,
    skip_stories: Annotated[
        bool,
        typer.Option("--skip-stories", help="Skip capturing active Stories."),
    ] = False,
    skip_profile_photos: Annotated[
        bool,
        typer.Option("--skip-profile-photos", help="Skip profile photo history."),
    ] = False,
    chats_file: Annotated[
        Path | None,
        typer.Option("--chats-file", help="Alternative path to the chats.md file."),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Detailed log: every RPC and its duration, reused/deferred media, "
            "metadata steps and ignored errors.",
        ),
    ] = False,
    no_web: Annotated[
        bool,
        typer.Option(
            "--no-web", help="Do not start the web UI (the CLI works the same without it)."
        ),
    ] = False,
    web_port: Annotated[
        int | None,
        typer.Option("--web-port", help="Web UI port (default: TGHOARD_WEB_PORT or 8765)."),
    ] = None,
    no_browser: Annotated[
        bool,
        # Kept so existing scripts don't break: `sync` only prints the web UI link now
        typer.Option("--no-browser", hidden=True, help="No-op (the browser is never opened)."),
    ] = False,
) -> None:
    """Sync messages, links, metadata and media according to `<data_dir>/chats.md`.

    In an interactive terminal it also opens a local web UI to manage `chats.md`
    (checkboxes and priorities, even during the sync) and follow the progress. When
    there is nothing to sync (no chat selected, or the sync finished), it waits for you
    to press Enter (or click "Start sync" in the web UI) to run again; `q` quits.
    Outside a terminal (cron, pipe) it runs once and exits.
    """
    cfg = _resolve_runtime_config(
        data_dir=data_dir,
        max_json_kb=max_json_kb,
        max_media_mb=max_media_mb,
        concurrency=concurrency,
        parallel_chats=parallel_chats,
        sync_mode=mode,
        history_wait=history_wait,
        auto_throttle=False if no_auto_throttle else None,
        metadata_every_hours=0 if refresh_metadata else None,
    )
    _require_current_layout(cfg)
    manifest_path = chats_file or cfg.chats_manifest_path
    if not manifest_path.exists():
        console.print(f"[red]File `{manifest_path}` not found. Run `tghoard start` first.[/red]")
        raise typer.Exit(code=1)

    since_dt: datetime | None = None
    if since:
        try:
            since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            console.print(f"[red]Invalid --since `{escape(since)}`: use YYYY-MM-DD.[/red]")
            raise typer.Exit(code=2) from None

    interactive = sys.stdin.isatty()
    live = LiveSyncState()
    bridge = TelegramBridge(cfg.internal_dir)
    web: WebUI | None = None

    def _start_web() -> WebUI:
        ui = WebUI(
            data_dir=cfg.data_dir,
            manifest_path=manifest_path,
            db_path=cfg.checkpoint_db_path,
            live=live,
            bridge=bridge,
            port=web_port if web_port is not None else cfg.web_port,
            max_media_bytes=cfg.max_media_bytes,
        )
        url = ui.start()
        # Just the link (no browser tab is opened): click it or paste it in the browser
        console.print(
            f"🌐 Web UI: [bold][link={url}]{url}[/link][/bold] "
            "[dim](manages chats.md; use --no-web to disable)[/dim]"
        )
        return ui

    async def _register_entities(connected: list[tuple[AccountRecord, Any]]) -> None:
        """Let the web UI show chat photos/totals before and while syncing (best effort)."""
        registered = 0
        try:
            for acc, client in connected:
                try:
                    me = await client.get_me()
                    if me is not None:
                        bridge.register(f"saved_{acc.account_id}", client, me)
                        registered += 1
                    async for dialog in client.iter_dialogs():
                        ent = getattr(dialog, "entity", None)
                        if ent is not None:
                            info = classify_entity_to_chat_info(
                                ent, self_id=acc.account_id, account_alias=acc.alias
                            )
                            bridge.register(info.chat_key, client, ent)
                            registered += 1
                except Exception as exc:
                    live.add_log(
                        f"⚠ web UI: could not list the chats of `{acc.alias}` "
                        f"for photos/totals: {type(exc).__name__}: {exc}"
                    )
        finally:
            bridge.mark_ready()  # never leave web requests waiting forever
            live.add_log(f"🖼 web UI: {registered} chat(s) ready for photos and totals")

    async def _run() -> int:
        """Returns the number of chats that failed in the last sync run."""
        failed = 0
        loop = asyncio.get_running_loop()
        live.attach_loop(loop)
        bridge.attach_loop(loop)
        bridge.log = live.add_log
        live.set_phase("connecting", "Connecting Telegram accounts")
        with CheckpointDB(cfg.checkpoint_db_path) as db:
            accounts = _select_accounts(db, account)
            if not accounts:
                console.print(
                    "[red]No authenticated account found. Run `tghoard start` first.[/red]"
                )
                raise typer.Exit(code=1)

            mgr = AccountManager(cfg, db, console=console)
            connected: list[tuple[AccountRecord, object]] = []
            try:
                for acc in accounts:
                    client = await mgr.connect_account(acc)
                    connected.append((acc, client))
                entities_task = (
                    asyncio.create_task(_register_entities(connected)) if web is not None else None
                )

                while True:
                    selections = load_chats_manifest(manifest_path)
                    has_job = bool(filter_enabled(selections, chat))
                    if has_job:
                        stats = await run_multi_account_sync(
                            account_clients=connected,
                            selections=selections,
                            config=cfg,
                            db=db,
                            chat_filter=chat,
                            since_date=since_dt,
                            sync_members=not skip_members,
                            sync_stories=not skip_stories,
                            sync_profile_photos=not skip_profile_photos,
                            console=console,
                            verbose=verbose,
                            manifest_path=manifest_path,
                            live=live,
                            on_entity=bridge.register,
                        )
                        failed = len(stats.failed_chats) + len(stats.failed_accounts)
                    if not interactive:
                        if not has_job:
                            console.print(
                                "[yellow]No chat marked `[x] baixar` matches the filter.[/yellow]"
                            )
                        break

                    where = f" in the web UI ({web.url})" if web is not None else ""
                    if has_job:
                        live.set_phase("done", "Sync finished — nothing left in the queue")
                        console.print(
                            f"\n[green]✓ Nothing left in the queue.[/green] Adjust chats.md{where} "
                            "and press [bold]Enter[/bold] to sync again "
                            "([bold]q[/bold] + Enter or Ctrl+C to quit)."
                        )
                    else:
                        live.set_phase("waiting_user", "No chat selected for download")
                        console.print(
                            f"\n[yellow]No chat marked `[x] baixar`.[/yellow] "
                            f"Select chats{where} or edit `{manifest_path}` and press "
                            "[bold]Enter[/bold] ([bold]q[/bold] + Enter or Ctrl+C to quit)."
                        )
                    answer = await wait_for_user(live.start_event)
                    if answer == "quit":
                        break
                    if answer == "web":
                        console.print("[cyan]▶ Sync requested from the web UI[/cyan]")
                if entities_task is not None:
                    entities_task.cancel()
            finally:
                for _, client in connected:
                    await client.disconnect()  # type: ignore[attr-defined]
        return failed

    failed_chats = 0
    try:
        # Lock first: a second `sync` must exit before opening a web UI of its own
        with _writer_lock(cfg, "sync"):
            if not no_web:
                web = _start_web()
            failed_chats = asyncio.run(_run())
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted. Progress already saved stays in the checkpoint.[/dim]")
        raise typer.Exit(code=130) from None
    except ArchiveStorageError:
        # Disk full / read-only / drive gone: the orchestrator already printed why
        raise typer.Exit(code=1) from None
    finally:
        if web is not None:
            web.stop()
    if failed_chats:
        # Cron/scripts can tell a partial run apart (the failed chats resume next sync)
        raise typer.Exit(code=1)


# ----------------------------------------------------------------------
# 3. `tghoard account ...`
# ----------------------------------------------------------------------
@account_app.command("list")
def cmd_account_list(data_dir: DataDirOption = None) -> None:
    """List all Telegram accounts registered in `<data_dir>`."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        accounts = db.list_accounts()

    if not accounts:
        console.print("[yellow]No accounts registered in this directory.[/yellow]")
        return

    table = Table(title=f"Telegram accounts ({cfg.data_dir})")
    table.add_column("Account ID", style="cyan")
    table.add_column("Alias", style="bold")
    table.add_column("Name")
    table.add_column("Username")
    table.add_column("Last sync")
    for acc in accounts:
        table.add_row(
            str(acc.account_id),
            escape(acc.alias),
            escape(single_line(acc.display_name or "-")),
            escape(f"@{acc.username}") if acc.username else "-",
            acc.last_sync_at or "Never",
        )
    console.print(table)


@account_app.command("add")
def cmd_account_add(
    data_dir: DataDirOption = None,
    alias: Annotated[str | None, typer.Option("--alias", help="Account alias.")] = None,
) -> None:
    """Authenticate and add a new Telegram account to the data directory."""
    cfg = _resolve_runtime_config(data_dir=data_dir)

    async def _run() -> None:
        with CheckpointDB(cfg.checkpoint_db_path) as db:
            mgr = AccountManager(cfg, db, console=console)
            await mgr.interactive_add_account(alias=alias)

    with _writer_lock(cfg, "account add"):
        asyncio.run(_run())


@account_app.command("remove")
def cmd_account_remove(
    identifier: Annotated[str, typer.Argument(help="Account alias, username or numeric ID.")],
    data_dir: DataDirOption = None,
) -> None:
    """Remove an account's session, keeping all plain-text data already downloaded."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    # A running sync/watch keeps the session open: never delete it underneath them
    with _writer_lock(cfg, "account remove"), CheckpointDB(cfg.checkpoint_db_path) as db:
        mgr = AccountManager(cfg, db, console=console)
        removed = mgr.remove_account(identifier)

    if removed is None:
        console.print(f"[red]Account `{identifier}` not found.[/red]")
        raise typer.Exit(code=1)
    console.print(
        f"[green]✓ Session of account `{escape(removed.alias)}` ({removed.account_id}) removed. "
        "Archived data is left intact.[/green]"
    )


# ----------------------------------------------------------------------
# 4. `tghoard watch`
# ----------------------------------------------------------------------
@app.command("watch")
def cmd_watch(
    data_dir: DataDirOption = None,
    account: Annotated[
        str | None,
        typer.Option("--account", "-a", help="Listen on one specific account only."),
    ] = None,
    chats_file: Annotated[
        Path | None,
        typer.Option("--chats-file", help="Alternative path to the chats.md file."),
    ] = None,
) -> None:
    """Listen to live events (edits, instant deletions, online status and typing)."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    _require_current_layout(cfg)
    manifest_path = chats_file or cfg.chats_manifest_path
    if not manifest_path.exists():
        console.print(f"[red]File `{manifest_path}` not found. Run `tghoard start`.[/red]")
        raise typer.Exit(code=1)
    selections = load_chats_manifest(manifest_path)

    async def _run() -> None:
        with CheckpointDB(cfg.checkpoint_db_path) as db:
            accounts = _select_accounts(db, account)
            if not accounts:
                console.print("[red]No accounts registered.[/red]")
                raise typer.Exit(code=1)
            mgr = AccountManager(cfg, db, console=console)
            connected: list[tuple[AccountRecord, object]] = []
            try:
                for acc in accounts:
                    client = await mgr.connect_account(acc)
                    connected.append((acc, client))
                await run_live_watcher(
                    account_clients=connected,
                    selections=selections,
                    config=cfg,
                    db=db,
                    console=console,
                )
            finally:
                for _, client in connected:
                    await client.disconnect()  # type: ignore[attr-defined]

    try:
        with _writer_lock(cfg, "watch"):
            asyncio.run(_run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Listening stopped by the user.[/yellow]")


# ----------------------------------------------------------------------
# 5. `tghoard search` & `tghoard stats`
# ----------------------------------------------------------------------
@app.command("search")
def cmd_search(
    query: Annotated[str, typer.Argument(help="Search term (case-insensitive).")],
    data_dir: DataDirOption = None,
    chat: Annotated[
        str | None, typer.Option("--chat", "-c", help="Filter by folder/chat_key.")
    ] = None,
    deleted: Annotated[bool, typer.Option("--deleted", help="Only deleted messages.")] = False,
    edited: Annotated[bool, typer.Option("--edited", help="Only edited messages.")] = False,
    links: Annotated[bool, typer.Option("--links", help="Also search the extracted URLs.")] = False,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Max number of results.")] = 50,
) -> None:
    """Quick search over the plain-text files `messages/<chat>/*-ptNNN.json`."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    _require_current_layout(cfg)
    q_lower = query.lower()
    matches = 0

    for chat_key, chat_paths in iter_archive_chat_dirs(cfg.data_dir):
        if chat and chat.lower() not in (chat_key + " " + chat_paths.folder).lower():
            continue
        for part_file in iter_all_part_files(chat_paths):
            payload = json.loads(part_file.read_text(encoding="utf-8"))
            title = payload.get("chat_title") or chat_key
            for msg in payload.get("messages") or []:
                if deleted and not msg.get("is_deleted"):
                    continue
                if edited and not msg.get("revisions"):
                    continue

                haystacks = [str(msg.get("text") or "")]
                for rev in msg.get("revisions") or []:
                    haystacks.append(str(rev.get("text") or ""))
                if links:
                    haystacks.extend(str(u) for u in (msg.get("links") or []))

                if any(q_lower in h.lower() for h in haystacks):
                    matches += 1
                    badges: list[str] = []
                    if msg.get("is_deleted"):
                        badges.append("[red]DELETED[/red]")
                    if msg.get("revisions"):
                        badges.append(f"[yellow]EDITED({len(msg['revisions'])})[/yellow]")
                    badge_str = (" " + " ".join(badges)) if badges else ""
                    # Archived text is Telegram content: no Rich markup, no control chars
                    sender = escape(single_line((msg.get("sender") or {}).get("name") or "?"))
                    snippet = escape(single_line(msg.get("text") or "")[:180])
                    console.print(
                        f"[cyan]{escape(single_line(title))}[/cyan] (`{part_file.name}`) "
                        f"#{msg.get('id')} {escape('[' + str(msg.get('date')) + ']')} "
                        f"[bold]{sender}[/bold]{badge_str}: {snippet}"
                    )
                    if matches >= limit:
                        console.print(f"[dim]Limit of {limit} results reached.[/dim]")
                        return

    if matches == 0:
        console.print("[yellow]No results found.[/yellow]")


@app.command("stats")
def cmd_stats(
    data_dir: DataDirOption = None,
    to_download: Annotated[
        bool,
        typer.Option(
            "--to-download",
            help="Space each chat still needs per media category (exact for archived "
            "messages, ~estimated for history not archived yet).",
        ),
    ] = False,
    too_large: Annotated[
        bool,
        typer.Option(
            "--too-large",
            help="List queued media not downloaded because it exceeds TGHOARD_MAX_MEDIA_MB.",
        ),
    ] = False,
    include_all: Annotated[
        bool,
        typer.Option("--all", help="With --too-large/--to-download: include unchecked chats."),
    ] = False,
    show: Annotated[int, typer.Option("--show", help="How many rows to list.")] = 30,
) -> None:
    """Show full statistics of the plain-text archive and the checkpoint database."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    _require_current_layout(cfg)
    if to_download or too_large:
        _print_media_reports(cfg, to_download, too_large, include_all, show)
        return
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        db_counts = db.summary_counts()

    folder_counts = dict.fromkeys(ALL_MEDIA_CATEGORIES, 0)
    part_files_count = 0
    chat_dirs = iter_archive_chat_dirs(cfg.data_dir)
    for _, cdir in chat_dirs:
        part_files_count += len(iter_all_part_files(cdir))
        for cat in ALL_MEDIA_CATEGORIES:
            sub = cdir.media_dir(cat)
            if sub.exists():
                folder_counts[cat] += sum(
                    1 for f in sub.iterdir() if f.is_file() and not f.name.startswith(".")
                )

    table = Table(title=f"Archive statistics ({cfg.data_dir})")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right", style="bold")
    table.add_row("Registered accounts", str(db_counts["accounts"]))
    table.add_row("Chat folders on disk", str(len(chat_dirs)))
    table.add_row("Chats shared by 2+ accounts", str(db_counts["shared_chats"]))
    table.add_row("Partitioned JSON files (*-ptNNN.json)", str(part_files_count))
    table.add_row("Indexed messages", str(db_counts["messages"]))
    table.add_row("Preserved deleted messages", str(db_counts["deleted_messages"]))
    table.add_row("Photos in photos/", str(folder_counts["photos"]))
    table.add_row("Videos in videos/", str(folder_counts["videos"]))
    table.add_row("Audio in audio/", str(folder_counts["audio"]))
    table.add_row("Other files in files/", str(folder_counts["files"]))
    table.add_row("Unique media in the global index", str(db_counts["unique_media_files"]))
    table.add_row("Pending media (checkbox unchecked)", str(db_counts["pending_media"]))
    console.print(table)
    console.print(
        "[dim]More: `stats --to-download` (space still needed per chat) and "
        "`stats --too-large` (media skipped by the size limit).[/dim]"
    )


def _print_media_reports(
    cfg: AppConfig, to_download: bool, too_large: bool, include_all: bool, show: int
) -> None:
    """`stats --to-download` / `stats --too-large` (offline: checkpoint.db + cached totals)."""
    entries = load_chats_manifest(cfg.chats_manifest_path)
    conn = open_readonly_db(cfg.checkpoint_db_path)
    try:
        if to_download:
            stats = compute_all_chat_stats(cfg.data_dir, conn, max_media_bytes=cfg.max_media_bytes)
            remote = TelegramBridge(cfg.internal_dir).all_cached_counts()
            averages = media_size_averages(stats.values())
            rows: list[tuple[int, list[str]]] = []
            for key, e in entries.items():
                if not (e.baixar or include_all):
                    continue
                st = stats.get(key)
                if st is None:
                    continue
                todo = estimate_to_download(st, remote.get(key), averages)
                cells, selected, estimated = [], 0, False
                for cat in ALL_MEDIA_CATEGORIES:
                    t = todo[cat]
                    on = e.baixar and getattr(e, cat)
                    if on:
                        selected += t.total_bytes
                        estimated = estimated or t.is_estimate
                    text = _fmt_todo(t)
                    cells.append(text if on or not text else f"[dim]{text}[/dim]")
                if selected or include_all:
                    total = ("~" if estimated else "") + human_bytes(selected)
                    rows.append((selected, [escape(single_line(e.title)), *cells, total]))
            table = Table(title="Space still needed (checked categories; dim = unchecked)")
            table.add_column("Chat", overflow="fold")
            for cat in ALL_MEDIA_CATEGORIES:
                table.add_column(cat.capitalize(), justify="right")
            table.add_column("Checked total", justify="right", style="bold")
            for _, cells in sorted(rows, key=lambda r: -r[0])[:show]:
                table.add_row(*cells)
            console.print(table)
            console.print(
                "[dim]Exact for media of archived messages; ~ = estimate for history not archived "
                "yet (total on Telegram × average size). Telegram totals come from the web UI's "
                "cache (.tghoard/remote_counts.json) — chats never opened in the web UI during "
                "a sync count only what is archived.[/dim]"
            )
        if too_large:
            rep = list_oversized_media(
                conn, entries, cfg.max_media_bytes, include_unselected=include_all, max_items=show
            )
            if rep.limit_bytes is None:
                console.print("TGHOARD_MAX_MEDIA_MB=0: no size limit, nothing is skipped for size.")
                return
            table = Table(title=f"Not downloaded: larger than {human_bytes(rep.limit_bytes)}")
            for col, just in (
                ("Chat", "left"),
                ("Category", "left"),
                ("File", "left"),
                ("Month", "right"),
                ("Message", "right"),
                ("Size", "right"),
            ):
                table.add_column(col, justify=just, overflow="fold")  # type: ignore[arg-type]
            for it in rep.items:
                style = None if it.selected else "dim"
                table.add_row(
                    escape(single_line(it.title)),
                    it.category,
                    escape(single_line(it.file_name or it.mime_type or "—")),
                    it.month or "—",
                    f"#{it.msg_id}",
                    human_bytes(it.size_bytes),
                    style=style,
                )
            console.print(table)
            console.print(
                f"{rep.total_items} file(s), {human_bytes(rep.total_bytes)} in total. Raise "
                "TGHOARD_MAX_MEDIA_MB (or run `sync --max-media-mb N`) to download them."
            )
            for mb, n, b in rep.thresholds:
                if n:
                    console.print(f"  a limit of {mb} MB → {n} file(s), {human_bytes(b)}")
    finally:
        if conn is not None:
            conn.close()


def _fmt_todo(t: Any) -> str:
    if not t.total_bytes and not t.estimated_items:
        return ""
    if t.estimated_bytes is None:
        return f"{human_bytes(t.exact_bytes)} + ?" if t.exact_bytes else "?"
    return ("~" if t.is_estimate else "") + human_bytes(t.total_bytes)


# ----------------------------------------------------------------------
# 6. Maintenance: `rebuild-checkpoint` & `rebuild-md`
# ----------------------------------------------------------------------
@app.command("rebuild-checkpoint")
def cmd_rebuild_checkpoint(data_dir: DataDirOption = None) -> None:
    """Rebuild `<data_dir>/.tghoard/checkpoint.db` by scanning only the plain-text folders."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    _require_current_layout(cfg)
    with _writer_lock(cfg, "rebuild-checkpoint"), CheckpointDB(cfg.checkpoint_db_path) as db:
        rep = rebuild_checkpoint_from_plaintext(cfg.data_dir, db)
    console.print(
        f"[green]✓ Checkpoint rebuilt successfully![/green] "
        f"Chats: {rep.chats_scanned} | JSON parts: {rep.part_files_scanned} | "
        f"Messages: {rep.messages_indexed} | Synced ranges: {rep.synced_ranges} | "
        f"Media: {rep.media_files_indexed} | Re-queued media: {rep.pending_media_queued}"
    )


@app.command("rebuild-md")
def cmd_rebuild_md(
    data_dir: DataDirOption = None,
    chat: Annotated[
        str | None, typer.Option("--chat", "-c", help="Filter a specific chat.")
    ] = None,
) -> None:
    """Regenerate `links/<folder>.md` and `deleted_and_edits.md` from the `*-ptNNN.json` files."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    _require_current_layout(cfg)
    with _writer_lock(cfg, "rebuild-md"):
        rep = rebuild_markdown_from_plaintext(cfg.data_dir, chat_filter=chat)
    console.print(
        f"[green]✓ Markdown reports updated![/green] "
        f"Chats: {rep.chats_scanned} | Parts read: {rep.part_files_scanned} | "
        f"Links extracted: {rep.links_rebuilt}"
    )


# ----------------------------------------------------------------------
# 7. Web UI standalone (`tghoard web`)
# ----------------------------------------------------------------------
@app.command("web")
def cmd_web(
    data_dir: DataDirOption = None,
    port: Annotated[
        int | None, typer.Option("--port", help="Port (default: TGHOARD_WEB_PORT or 8765).")
    ] = None,
    no_browser: Annotated[
        bool, typer.Option("--no-browser", help="Do not open the browser automatically.")
    ] = False,
) -> None:
    """Open only the web UI (edit chats.md and view statistics), without connecting to Telegram."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    _require_current_layout(cfg)
    if not cfg.chats_manifest_path.exists():
        console.print("[red]chats.md not found. Run `tghoard start` first.[/red]")
        raise typer.Exit(code=1)
    web = WebUI(
        data_dir=cfg.data_dir,
        manifest_path=cfg.chats_manifest_path,
        db_path=cfg.checkpoint_db_path,
        bridge=TelegramBridge(cfg.internal_dir),  # cache only (not connected)
        port=port if port is not None else cfg.web_port,
        max_media_bytes=cfg.max_media_bytes,
    )
    url = web.start()
    console.print(f"🌐 Web UI: [bold][link={url}]{url}[/link][/bold] — Ctrl+C to quit")
    if sys.stdin.isatty() and not no_browser:
        webbrowser.open(url)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        web.stop()


# ----------------------------------------------------------------------
# 8. Utilities
# ----------------------------------------------------------------------
@app.command("folder-links")
def cmd_folder_links(
    data_dir: DataDirOption = None,
    fmt: Annotated[
        str, typer.Option("--format", "-f", help="md (default), txt (one URL per line) or csv.")
    ] = "md",
    output: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Output file (default: <data_dir>/folder_links.<ext>; - = stdout).",
        ),
    ] = None,
    account: Annotated[
        str | None, typer.Option("--account", "-a", help="Only this account (alias).")
    ] = None,
    folder: Annotated[
        str | None, typer.Option("--folder", help="Only folders whose name contains this text.")
    ] = None,
    only_channels: Annotated[
        bool, typer.Option("--only-channels", help="Only channels and supergroups.")
    ] = False,
    only_public: Annotated[
        bool,
        typer.Option("--only-public", help="Only public links (t.me/<username>)."),
    ] = False,
) -> None:
    """Generate links to every chat in each Telegram folder (offline, from the archive)."""
    from tghoard.utils.folder_links import RENDERERS, build_folder_links

    cfg = _resolve_runtime_config(data_dir=data_dir)
    _require_current_layout(cfg)
    if fmt not in RENDERERS:
        console.print(f"[red]Invalid format `{fmt}` (use md, txt or csv).[/red]")
        raise typer.Exit(code=2)
    folders = build_folder_links(cfg.data_dir, only_channels=only_channels, only_public=only_public)
    if not folders:
        console.print(
            "[yellow]No folders found. Run `tghoard sync` (or `tghoard start`) to "
            "fetch your Telegram folders.[/yellow]"
        )
        raise typer.Exit(code=1)
    if account:
        folders = [f for f in folders if f.account == account]
    if folder:
        folders = [f for f in folders if folder.lower() in f.title.lower()]

    text = RENDERERS[fmt](folders)
    if output is not None and str(output) == "-":
        sys.stdout.write(text)
        return
    target = output or cfg.data_dir / f"folder_links.{fmt}"
    atomic_write_text(target, text)
    n_links = sum(1 for f in folders for link in f.links if link.url)
    console.print(f"[green]✓ {n_links} link(s) from {len(folders)} folder(s) → {target}[/green]")


@app.command("verify-media")
def cmd_verify_media(
    data_dir: DataDirOption = None,
    fix: Annotated[
        bool,
        typer.Option(
            "--fix",
            help="Mark missing/corrupted files in the JSON and queue them for the next "
            "sync to download again (without it, only reports).",
        ),
    ] = False,
    check_hash: Annotated[
        bool, typer.Option("--hash", help="Also check SHA-256 (slow on large archives).")
    ] = False,
    chat: Annotated[
        str | None, typer.Option("--chat", "-c", help="Only this chat (chat_key or folder).")
    ] = None,
    show: Annotated[int, typer.Option("--show", help="How many problems to list.")] = 30,
) -> None:
    """Check that every media file the JSONs say was downloaded exists on disk (offline)."""
    from tghoard.utils.verify_media import verify_media

    cfg = _resolve_runtime_config(data_dir=data_dir)
    _require_current_layout(cfg)
    db = CheckpointDB(cfg.checkpoint_db_path) if cfg.checkpoint_db_path.exists() else None
    try:
        with console.status("Checking media…"):
            with _writer_lock(cfg, "verify-media --fix") if fix else contextlib.nullcontext():
                rep = verify_media(
                    cfg.data_dir, db, fix=fix, check_hash=check_hash, chat_filter=chat
                )
    finally:
        if db is not None:
            db.close()

    table = Table(title=f"Media verification ({cfg.data_dir})")
    table.add_column("Metric", style="cyan")
    table.add_column("Total", justify="right", style="bold")
    table.add_row("Chats checked", str(rep.chats))
    table.add_row("Media in the JSONs", str(rep.media_checked))
    table.add_row("OK on disk", f"[green]{rep.ok}[/green]")
    table.add_row(
        "Not downloaded yet (queued)", str(rep.not_downloaded - rep.count("lost_from_queue"))
    )
    table.add_row("Missing on disk", f"[red]{rep.count('missing')}[/red]")
    table.add_row("Size mismatch", f"[red]{rep.count('size_mismatch')}[/red]")
    if check_hash:
        table.add_row("SHA-256 mismatch", f"[red]{rep.count('hash_mismatch')}[/red]")
    table.add_row(
        "Not downloaded and not queued", f"[yellow]{rep.count('lost_from_queue')}[/yellow]"
    )
    console.print(table)
    for p in rep.problems[:show]:
        where = escape(f" `{p.path}`") if p.path else ""
        console.print(
            f"  • {escape(f'[{p.kind}]')} {p.chat_key} #{p.msg_id}{where} {escape(p.detail)}"
        )
    if len(rep.problems) > show:
        console.print(f"  … and {len(rep.problems) - show} more (use --show N)")
    if not rep.problems:
        console.print("[green]✓ All recorded media is on disk.[/green]")
    elif fix:
        console.print(
            f"[green]✓ {rep.requeued} media file(s) queued and "
            f"{rep.json_files_updated} JSON(s) updated.[/green] The next "
            "[bold]tghoard sync[/bold] downloads them again (for the categories checked in "
            "chats.md)."
        )
    else:
        cmd = "./tghoard.sh verify-media" + (f" --chat {chat}" if chat else "") + " --fix"
        console.print(
            "[yellow]Nothing was changed.[/yellow] To queue the problems for download, "
            f"stop the sync and run:\n  [bold]{escape(cmd)}[/bold]"
        )
        raise typer.Exit(code=1)


# ----------------------------------------------------------------------
# 9. Data migrations (`src/tghoard/migrations/`)
# ----------------------------------------------------------------------
@app.command("migrate")
def cmd_migrate(
    data_dir: DataDirOption = None,
    apply: Annotated[
        bool,
        typer.Option("--apply", help="Run the migrations (without it, only shows the plan)."),
    ] = False,
) -> None:
    """Migrate the archive to the current layout. Without `--apply`, only shows the plan."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    applied = load_applied(cfg)
    pending = pending_migrations(cfg)

    table = Table(title=f"Migrations ({cfg.data_dir})")
    table.add_column("ID", style="cyan")
    table.add_column("Description")
    table.add_column("State", justify="right")
    for mig in discover_migrations():
        if mig.key in applied:
            state = f"[green]applied {applied[mig.key]}[/green]"
        elif mig in pending:
            state = "[yellow]pending[/yellow]"
        else:
            state = "[dim]not needed[/dim]"
        table.add_row(mig.key, escape(mig.description), state)
    console.print(table)

    if not pending:
        console.print("[green]✓ Archive is already on the current layout.[/green]")
        return

    for mig in pending:
        plan = mig.plan(cfg)
        console.print(f"\n[bold cyan]▶ {mig.key}[/bold cyan] — {escape(plan.summary)}")
        for step in plan.steps:
            console.print(f"  • {escape(step)}")
        if plan.table_rows:
            ptable = Table()
            for header in plan.table_headers:
                ptable.add_column(header)
            for row in plan.table_rows:
                ptable.add_row(*(escape(c) for c in row))
            console.print(ptable)

    if not apply:
        console.print(
            "\n[yellow]Preview mode: nothing was changed.[/yellow] "
            "Run [bold]tghoard migrate --apply[/bold] to execute it."
        )
        return

    for mig in discover_migrations():
        if mig.key in load_applied(cfg) or not mig.is_needed(cfg):
            continue
        console.print(f"\n[bold]Applying {mig.key}...[/bold]")
        with _writer_lock(cfg, "migrate --apply"):
            mig.apply(cfg, console)
        if mig.is_needed(cfg):
            console.print(
                f"[red]✗ {mig.key} did not finish (see warnings above); "
                "run it again to resume.[/red]"
            )
            raise typer.Exit(code=1)
        record_applied(cfg, mig)
        console.print(f"[green]✓ {mig.key} applied.[/green]")


if __name__ == "__main__":
    app()
