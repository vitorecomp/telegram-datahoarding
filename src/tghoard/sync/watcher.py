"""Optional live listener (`tghoard watch`) for transient updates (typing, presence, deletes)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from rich.console import Console
from rich.markup import escape
from telethon import events

from tghoard.config import AppConfig
from tghoard.storage.atomic import utc_now_iso
from tghoard.storage.chat_writer import (
    ChatPartWriter,
    make_canonical_message_key,
    message_content_tag,
)
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB, LocatedMessage
from tghoard.storage.markdown_writer import (
    append_markdown_log_entries,
    rebuild_links_and_audit_md_from_parts,
)
from tghoard.storage.media_manager import MediaManager, classify_telethon_media
from tghoard.storage.paths import (
    ChatPaths,
    month_from_iso,
    resolve_chat_paths,
    uses_global_msg_ids,
)
from tghoard.telegram.entities import (
    classify_entity_to_chat_info,
    serialize_message_to_dict,
    to_utc_iso,
)
from tghoard.telegram.throttler import RateLimiter


@dataclass
class LiveWatchContext:
    """Everything the live handlers of one account need."""

    client: Any
    account: AccountRecord
    selections: dict[str, ChatSelection]
    config: AppConfig
    db: CheckpointDB
    writer: ChatPartWriter
    media_manager: MediaManager
    limiter: RateLimiter
    console: Console


def _say(console: Console, markup: str) -> None:
    """Print a status line; a console/markup problem must never abort archive work."""
    try:
        console.print(markup)
    except Exception:
        try:
            console.print(markup, markup=False)
        except Exception:
            pass


def _say_error(console: Console, text: str) -> None:
    """Print plain `text` (may quote Telegram content / exception messages) in red."""
    _say(console, f"[red]{escape(text)}[/red]")


def _tag(title: str) -> str:
    """`[Chat title]` with Telegram-controlled text escaped for Rich markup."""
    return escape(f"[{title}]")


def _heal_locator(
    db: CheckpointDB,
    *,
    chat_key: str,
    loc: LocatedMessage,
    month: str,
    part_num: int,
    is_deleted: bool,
) -> None:
    """Point a locator at the month/part where its message actually is."""
    if loc.day_str == month and loc.part_num == part_num:
        return
    db.record_messages_batch(
        chat_key=chat_key,
        account_id=None,
        locators=[(loc.canonical_key, month, part_num, loc.sender_id, is_deleted)],
        account_mappings=[],
    )


async def handle_new_message(ctx: LiveWatchContext, event: Any) -> None:
    chat = await event.get_chat()
    if chat is None:
        return
    account = ctx.account
    info = classify_entity_to_chat_info(
        chat,
        self_id=account.account_id,
        account_alias=account.alias,
    )
    sel = ctx.selections.get(info.chat_key)
    if sel is None or not sel.baixar:
        return

    msg = event.message
    msg_id = int(msg.id)
    date_iso = to_utc_iso(getattr(msg, "date", None)) or utc_now_iso()
    # JSON is partitioned by month; media file names keep the full day (as in `sync`).
    month = month_from_iso(date_iso)
    file_day = date_iso[:10]
    text = str(getattr(msg, "message", None) or getattr(msg, "raw_text", None) or "")
    sender_id_raw = getattr(msg, "sender_id", None)
    if sender_id_raw is None and getattr(msg, "sender", None) is not None:
        sender_id_raw = getattr(msg.sender, "id", None)
    sender_id = int(sender_id_raw) if sender_id_raw is not None else None

    media_obj = getattr(msg, "media", None)
    classified = classify_telethon_media(media_obj)
    existing = ctx.db.locate_by_account_msg(account.account_id, msg_id, chat_key=info.chat_key)
    if existing:  # re-delivered (reconnect catch-up): keep the key already archived
        ckey = existing[0].canonical_key
    else:
        ckey = make_canonical_message_key(
            chat_key=info.chat_key,
            msg_id=msg_id,
            sender_id=sender_id,
            date_iso=date_iso,
            media_tg_id=classified.tg_id if classified else None,
            grouped_id=getattr(msg, "grouped_id", None),
            text=text,
            content_tag=message_content_tag(msg),
        )
    chat_paths = resolve_chat_paths(ctx.config.data_dir, info.chat_key, sel.title)
    media_meta = None
    if media_obj is not None and classified is not None:
        outcome = await ctx.media_manager.process_message_media(
            media=media_obj,
            selection=sel,
            chat_paths=chat_paths,
            chat_key=info.chat_key,
            canonical_key=ckey,
            account_id=account.account_id,
            msg_id=msg_id,
            day_str=month,
            file_day=file_day,
            downloader=lambda target: ctx.limiter.call(
                lambda: ctx.client.download_media(msg, file=str(target)),
                description=f"watch_media({info.chat_key}#{msg_id})",
            ),
        )
        if outcome is not None:
            media_meta = outcome.metadata

    msg_dict = serialize_message_to_dict(
        msg,
        account_id=account.account_id,
        canonical_key=ckey,
        media_meta=media_meta,
    )
    pmap, _ = ctx.writer.upsert_month_messages(
        chat_paths=chat_paths,
        chat_key=info.chat_key,
        chat_title=sel.title,
        month=month,
        incoming_messages=[msg_dict],
    )
    # Other messages of the month may have moved to another part
    ctx.db.update_day_part_numbers(info.chat_key, month, pmap)
    ctx.db.record_messages_batch(
        chat_key=info.chat_key,
        account_id=account.account_id,
        locators=[(ckey, month, pmap.get(ckey, 0), sender_id, False)],
        account_mappings=[(msg_id, ckey)],
    )
    if uses_global_msg_ids(info.chat_key):
        ctx.db.add_synced_range(info.chat_key, msg_id, msg_id)
    if msg_dict.get("links"):
        rebuild_links_and_audit_md_from_parts(
            chat_paths,
            chat_key=info.chat_key,
            chat_title=sel.title,
        )
    _say(ctx.console, f"[green]+ {_tag(sel.title)}[/green] msg #{msg_id} saved.")


async def handle_message_edited(ctx: LiveWatchContext, event: Any) -> None:
    chat = await event.get_chat()
    if chat is None:
        return
    account = ctx.account
    info = classify_entity_to_chat_info(
        chat, self_id=account.account_id, account_alias=account.alias
    )
    sel = ctx.selections.get(info.chat_key)
    if sel is None or not sel.baixar:
        return
    msg = event.message
    locs = ctx.db.locate_by_account_msg(account.account_id, int(msg.id), chat_key=info.chat_key)
    if not locs:
        return
    loc = locs[0]
    month = month_from_iso(loc.day_str)
    chat_paths = resolve_chat_paths(ctx.config.data_dir, info.chat_key, sel.title)
    inc = serialize_message_to_dict(
        msg,
        account_id=account.account_id,
        canonical_key=loc.canonical_key,
    )
    pmap, edits = ctx.writer.upsert_month_messages(
        chat_paths=chat_paths,
        chat_key=info.chat_key,
        chat_title=sel.title,
        month=month,
        incoming_messages=[inc],
    )
    ctx.db.update_day_part_numbers(info.chat_key, month, pmap)
    if loc.canonical_key in pmap:
        _heal_locator(
            ctx.db,
            chat_key=info.chat_key,
            loc=loc,
            month=month,
            part_num=pmap[loc.canonical_key],
            is_deleted=loc.is_deleted,
        )
    if edits:
        rebuild_links_and_audit_md_from_parts(
            chat_paths,
            chat_key=info.chat_key,
            chat_title=sel.title,
        )
        _say(ctx.console, f"[yellow]✏️ {_tag(sel.title)}[/yellow] edit of msg #{msg.id} versioned.")


def _apply_live_delete(
    ctx: LiveWatchContext,
    del_id: int,
    chat_key_hint: str | None,
    touched: dict[str, tuple[ChatPaths, str]],
) -> None:
    for loc in ctx.db.locate_by_account_msg(ctx.account.account_id, del_id, chat_key=chat_key_hint):
        sel = ctx.selections.get(loc.chat_key)
        if sel is not None and not sel.baixar:
            continue
        title = sel.title if sel is not None else loc.chat_key
        chat_paths = resolve_chat_paths(ctx.config.data_dir, loc.chat_key, title)
        month = month_from_iso(loc.day_str)
        hit = ctx.writer.locate_and_mark_deleted(
            chat_paths=chat_paths,
            month=month,
            part_num=loc.part_num,
            canonical_key=loc.canonical_key,
            source="live_watch",
        )
        if hit is None:
            _say(
                ctx.console,
                f"[yellow]⚠ {_tag(title)}[/yellow] msg #{del_id} was deleted remotely, but its "
                f"archived record was not found in {escape(chat_paths.folder)}/{month}-pt*.json "
                "(deletion not recorded).",
            )
            continue
        part_num, _ = hit
        ctx.db.mark_locator_deleted(loc.chat_key, loc.canonical_key)
        _heal_locator(
            ctx.db, chat_key=loc.chat_key, loc=loc, month=month, part_num=part_num, is_deleted=True
        )
        touched[loc.chat_key] = (chat_paths, title)
        _say(
            ctx.console,
            f"[red]🗑️ {_tag(title)}[/red] msg #{del_id} deleted remotely (preserved locally!).",
        )


async def handle_message_deleted(ctx: LiveWatchContext, event: Any) -> None:
    chat_key_hint: str | None = None
    if getattr(event, "chat_id", None) is not None:
        try:
            chat = await event.get_chat()
        except Exception as exc:
            _say_error(
                ctx.console,
                f"✗ deletion event: could not resolve chat {event.chat_id}: "
                f"{type(exc).__name__}: {exc}",
            )
            return
        if chat is not None:
            info = classify_entity_to_chat_info(chat, self_id=ctx.account.account_id)
            chat_key_hint = info.chat_key

    touched: dict[str, tuple[ChatPaths, str]] = {}
    # One failing id must never stop the others from being recorded
    for del_id in getattr(event, "deleted_ids", None) or []:
        try:
            _apply_live_delete(ctx, int(del_id), chat_key_hint, touched)
        except Exception as exc:
            _say_error(
                ctx.console,
                f"✗ could not record deletion of msg #{del_id}: {type(exc).__name__}: {exc}",
            )
    for chat_key, (chat_paths, title) in touched.items():
        try:
            rebuild_links_and_audit_md_from_parts(chat_paths, chat_key=chat_key, chat_title=title)
        except Exception as exc:
            _say_error(
                ctx.console,
                f"✗ could not refresh links/audit of [{title}]: {type(exc).__name__}: {exc}",
            )


async def handle_user_update(ctx: LiveWatchContext, event: Any) -> None:
    user_id = getattr(event, "user_id", None)
    if user_id is None:
        return
    lo, hi = sorted((ctx.account.account_id, int(user_id)))
    dm_key = f"dm_{lo}_{hi}"
    sel = ctx.selections.get(dm_key)
    if sel is None or not sel.baixar:
        return
    chat_paths = resolve_chat_paths(ctx.config.data_dir, dm_key, sel.title)
    now = utc_now_iso()
    desc_parts: list[str] = []
    if getattr(event, "online", None) is True:
        desc_parts.append("ficou Online")
    elif getattr(event, "online", None) is False:
        desc_parts.append("ficou Offline")
    if getattr(event, "typing", False):
        desc_parts.append("digitando...")
    elif getattr(event, "recording", False):
        desc_parts.append("gravando áudio...")
    elif getattr(event, "uploading", False):
        desc_parts.append("enviando mídia...")

    # presence.md is archive content: its entries stay in Portuguese (data format)
    if desc_parts:
        entry = f"- `{now}` (live via `{ctx.account.alias}`): {', '.join(desc_parts)}"
        append_markdown_log_entries(
            chat_paths.meta("presence.md"),
            header_title=f"Presença e Atividade — {sel.title} (`{dm_key}`)",
            entries=[entry],
        )


def attach_live_handlers(
    *,
    client: Any,
    account: AccountRecord,
    selections: dict[str, ChatSelection],
    config: AppConfig,
    db: CheckpointDB,
    writer: ChatPartWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
    console: Console,
) -> None:
    """Register live Telethon event handlers filtered by `<data_dir>/chats.md`."""
    ctx = LiveWatchContext(
        client=client,
        account=account,
        selections=selections,
        config=config,
        db=db,
        writer=writer,
        media_manager=media_manager,
        limiter=limiter,
        console=console,
    )

    @client.on(events.NewMessage())
    async def _on_new_message(event: Any) -> None:
        await handle_new_message(ctx, event)

    @client.on(events.MessageEdited())
    async def _on_edited(event: Any) -> None:
        await handle_message_edited(ctx, event)

    @client.on(events.MessageDeleted())
    async def _on_deleted(event: Any) -> None:
        await handle_message_deleted(ctx, event)

    @client.on(events.UserUpdate())
    async def _on_user_update(event: Any) -> None:
        await handle_user_update(ctx, event)


async def run_live_watcher(
    *,
    account_clients: list[tuple[AccountRecord, Any]],
    selections: dict[str, ChatSelection],
    config: AppConfig,
    db: CheckpointDB,
    console: Console | None = None,
) -> None:
    out = console or Console()
    writer = ChatPartWriter(max_json_bytes=config.max_json_bytes)
    media_manager = MediaManager(
        data_dir=config.data_dir,
        checkpoint_db=db,
        max_media_bytes=config.max_media_bytes,
    )
    limiter = RateLimiter(concurrency=config.concurrency, console=out)

    for account, client in account_clients:
        attach_live_handlers(
            client=client,
            account=account,
            selections=selections,
            config=config,
            db=db,
            writer=writer,
            media_manager=media_manager,
            limiter=limiter,
            console=out,
        )

    out.print(
        f"[bold green]📡 Listening to live events on {len(account_clients)} account(s)... "
        "(press Ctrl+C to stop)[/bold green]"
    )
    await asyncio.gather(*(client.run_until_disconnected() for _, client in account_clients))
