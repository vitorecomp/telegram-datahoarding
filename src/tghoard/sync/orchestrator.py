"""Multi-account sync orchestrator driven by `<data_dir>/chats.md`."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.table import Table

from tghoard.config import AppConfig
from tghoard.storage.chat_writer import ChatDayWriter
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB
from tghoard.storage.media_manager import MediaManager
from tghoard.sync.messages_sync import sync_chat_messages
from tghoard.sync.metadata_sync import sync_chat_metadata
from tghoard.sync.pts_diff import reconcile_recent_window, sync_account_pts_diff
from tghoard.telegram.entities import classify_entity_to_chat_info
from tghoard.telegram.takeout import open_sync_session
from tghoard.telegram.throttler import RateLimiter


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
) -> OrchestratorStats:
    """Execute full incremental sync across accounts respecting `<data_dir>/chats.md`."""
    out = console or Console()
    totals = OrchestratorStats()
    writer = ChatDayWriter(max_json_bytes=config.max_json_bytes)
    media_manager = MediaManager(
        data_dir=config.data_dir,
        checkpoint_db=db,
        max_media_bytes=config.max_media_bytes,
    )
    limiter = RateLimiter(concurrency=config.concurrency, console=out)

    enabled_selections = {
        k: v
        for k, v in selections.items()
        if v.baixar and (chat_filter is None or chat_filter in (k, v.title, v.username or ""))
    }
    if not enabled_selections:
        out.print("[yellow]Nenhum chat marcado com `[x] baixar` corresponde ao filtro.[/yellow]")
        return totals

    synced_chat_keys: set[str] = set()

    for account, raw_client in account_clients:
        out.print(
            f"\n[bold cyan]▶ Sincronizando conta `{account.alias}` "
            f"(id={account.account_id}, modo={config.sync_mode})[/bold cyan]"
        )
        totals.accounts_synced += 1

        # Discover entity map for this account
        entities_by_key: dict[str, Any] = {}
        channel_entities: dict[str, Any] = {}

        # Saved Messages (`me`)
        saved_key = f"saved_{account.account_id}"
        if saved_key in enabled_selections and hasattr(raw_client, "get_me"):
            me_ent = await raw_client.get_me()
            if me_ent is not None:
                entities_by_key[saved_key] = me_ent

        async for dialog in raw_client.iter_dialogs():
            ent = getattr(dialog, "entity", None)
            if ent is None:
                continue
            info = classify_entity_to_chat_info(
                ent,
                self_id=account.account_id,
                account_alias=account.alias,
            )
            if info.chat_key in enabled_selections:
                entities_by_key[info.chat_key] = ent
                if info.kind in ("channel", "supergroup"):
                    channel_entities[info.chat_key] = ent

        # Step 1: Offline PTS delta replay (edits & deletions since last sync)
        pts_stats = await sync_account_pts_diff(
            client=raw_client,
            account_id=account.account_id,
            selections=enabled_selections,
            channel_entities=channel_entities,
            data_dir=config.data_dir,
            db=db,
            writer=writer,
            limiter=limiter,
        )
        totals.deletes_preserved += pts_stats.deleted_marked
        totals.edits_detected += pts_stats.edits_recorded

        # Step 2: Sync messages + media inside Takeout (or auto fallback) session
        async with open_sync_session(
            raw_client,
            mode=config.sync_mode,
            max_media_bytes=config.max_media_bytes,
            console=out,
        ) as (sync_client, is_takeout):
            mode_badge = "Takeout" if is_takeout else "Standard"
            out.print(
                f"  Sessão ativa: [bold]{mode_badge}[/bold] | "
                f"Chats habilitados visíveis nesta conta: [bold]{len(entities_by_key)}[/bold]"
            )

            for chat_key, ent in entities_by_key.items():
                sel = enabled_selections[chat_key]
                synced_chat_keys.add(chat_key)

                # If PTS reported DifferenceTooLong, audit recent window first
                raw_id = int(getattr(ent, "id", 0))
                if pts_stats.global_too_long or raw_id in pts_stats.too_long_channels:
                    w_del, w_ed = await reconcile_recent_window(
                        client=sync_client,
                        entity=ent,
                        account_id=account.account_id,
                        chat_key=chat_key,
                        chat_title=sel.title,
                        data_dir=config.data_dir,
                        db=db,
                        writer=writer,
                        limiter=limiter,
                        window_size=config.audit_window,
                    )
                    totals.deletes_preserved += w_del
                    totals.edits_detected += w_ed

                chat_res = await sync_chat_messages(
                    client=sync_client,
                    entity=ent,
                    account_id=account.account_id,
                    selection=sel,
                    data_dir=config.data_dir,
                    db=db,
                    writer=writer,
                    media_manager=media_manager,
                    limiter=limiter,
                    since_date=since_date,
                )
                totals.messages_saved += chat_res.messages_saved
                totals.messages_dedup_skipped += chat_res.messages_dedup_skipped
                totals.edits_detected += chat_res.edits_detected
                totals.media_downloaded += chat_res.media_downloaded
                totals.media_dedup_reused += chat_res.media_dedup_reused
                totals.media_deferred += chat_res.media_deferred
                totals.retroactive_media_downloaded += chat_res.retroactive_media_downloaded

                await sync_chat_metadata(
                    client=raw_client,
                    entity=ent,
                    account_id=account.account_id,
                    account_alias=account.alias,
                    selection=sel,
                    data_dir=config.data_dir,
                    db=db,
                    writer=writer,
                    media_manager=media_manager,
                    limiter=limiter,
                    sync_members=sync_members,
                    sync_stories=sync_stories,
                    sync_profile_photos=sync_profile_photos,
                )

                out.print(
                    f"  [green]✓[/green] [bold]{sel.title}[/bold] (`{chat_key}`): "
                    f"+{chat_res.messages_saved} msgs "
                    f"(dedup={chat_res.messages_dedup_skipped}, "
                    f"mídias={chat_res.media_downloaded + chat_res.retroactive_media_downloaded}, "
                    f"reuso={chat_res.media_dedup_reused})"
                )

        db.touch_account_sync(account.account_id)

    totals.chats_synced = len(synced_chat_keys)
    _print_summary_table(out, totals)
    return totals


def _print_summary_table(console: Console, stats: OrchestratorStats) -> None:
    table = Table(title="Resumo da Sincronização (tghoard)")
    table.add_column("Métrica", style="cyan")
    table.add_column("Total", justify="right", style="bold")
    table.add_row("Contas processadas", str(stats.accounts_synced))
    table.add_row("Chats sincronizados", str(stats.chats_synced))
    table.add_row("Mensagens novas gravadas", str(stats.messages_saved))
    table.add_row("Mensagens puladas (dedup multi-conta)", str(stats.messages_dedup_skipped))
    table.add_row("Edições versionadas", str(stats.edits_detected))
    table.add_row("Mensagens apagadas preservadas", str(stats.deletes_preserved))
    table.add_row("Anexos baixados da rede", str(stats.media_downloaded))
    table.add_row("Anexos retroativos baixados", str(stats.retroactive_media_downloaded))
    table.add_row("Anexos reutilizados sem re-download", str(stats.media_dedup_reused))
    table.add_row("Anexos adiados (checkbox desmarcado)", str(stats.media_deferred))
    console.print()
    console.print(table)
