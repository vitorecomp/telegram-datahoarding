"""Optional live listener (`tghoard watch`) for transient updates (typing, presence, deletes)."""

from __future__ import annotations

import asyncio
from typing import Any

from rich.console import Console
from telethon import events

from tghoard.config import AppConfig
from tghoard.storage.atomic import utc_now_iso
from tghoard.storage.chat_writer import ChatDayWriter, make_canonical_message_key
from tghoard.storage.chats_manifest import ChatSelection
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB
from tghoard.storage.markdown_writer import (
    append_markdown_log_entries,
    rebuild_links_and_audit_md_from_parts,
)
from tghoard.storage.media_manager import MediaManager, classify_telethon_media
from tghoard.storage.paths import resolve_chat_dir, uses_global_msg_ids
from tghoard.telegram.entities import (
    classify_entity_to_chat_info,
    serialize_message_to_dict,
    to_utc_iso,
)
from tghoard.telegram.throttler import RateLimiter


def attach_live_handlers(
    *,
    client: Any,
    account: AccountRecord,
    selections: dict[str, ChatSelection],
    config: AppConfig,
    db: CheckpointDB,
    writer: ChatDayWriter,
    media_manager: MediaManager,
    limiter: RateLimiter,
    console: Console,
) -> None:
    """Register live Telethon event handlers filtered by `<data_dir>/chats.md`."""

    @client.on(events.NewMessage())
    async def _on_new_message(event: Any) -> None:
        chat = await event.get_chat()
        if chat is None:
            return
        info = classify_entity_to_chat_info(
            chat,
            self_id=account.account_id,
            account_alias=account.alias,
        )
        sel = selections.get(info.chat_key)
        if sel is None or not sel.baixar:
            return

        msg = event.message
        msg_id = int(msg.id)
        date_iso = to_utc_iso(msg.date) or utc_now_iso()
        day_str = date_iso[:10]
        classified = classify_telethon_media(msg.media)
        ckey = make_canonical_message_key(
            chat_key=info.chat_key,
            msg_id=msg_id,
            sender_id=msg.sender_id,
            date_iso=date_iso,
            media_tg_id=classified.tg_id if classified else None,
            grouped_id=msg.grouped_id,
            text=msg.raw_text or "",
        )
        chat_dir = resolve_chat_dir(config.data_dir, info.chat_key, sel.title)
        media_meta = None
        if msg.media is not None and classified is not None:
            outcome = await media_manager.process_message_media(
                media=msg.media,
                selection=sel,
                chat_dir=chat_dir,
                chat_key=info.chat_key,
                canonical_key=ckey,
                account_id=account.account_id,
                msg_id=msg_id,
                day_str=day_str,
                downloader=lambda target: limiter.call(
                    lambda: client.download_media(msg, file=str(target)),
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
        pmap, _ = writer.upsert_day_messages(
            chat_dir=chat_dir,
            chat_key=info.chat_key,
            chat_title=sel.title,
            day_str=day_str,
            incoming_messages=[msg_dict],
        )
        db.record_messages_batch(
            chat_key=info.chat_key,
            account_id=account.account_id,
            locators=[(ckey, day_str, pmap.get(ckey, 0), msg.sender_id, False)],
            account_mappings=[(msg_id, ckey)],
        )
        if uses_global_msg_ids(info.chat_key):
            db.add_synced_range(info.chat_key, msg_id, msg_id)
        if msg_dict.get("links"):
            rebuild_links_and_audit_md_from_parts(
                chat_dir,
                chat_key=info.chat_key,
                chat_title=sel.title,
            )
        console.print(f"[green]+ [{sel.title}][/green] msg #{msg_id} salva.")

    @client.on(events.MessageEdited())
    async def _on_edited(event: Any) -> None:
        chat = await event.get_chat()
        if chat is None:
            return
        info = classify_entity_to_chat_info(chat, self_id=account.account_id)
        sel = selections.get(info.chat_key)
        if sel is None or not sel.baixar:
            return
        msg = event.message
        locs = db.locate_by_account_msg(account.account_id, int(msg.id), chat_key=info.chat_key)
        if not locs:
            return
        loc = locs[0]
        chat_dir = resolve_chat_dir(config.data_dir, info.chat_key, sel.title)
        inc = serialize_message_to_dict(
            msg,
            account_id=account.account_id,
            canonical_key=loc.canonical_key,
        )
        pmap, edits = writer.upsert_day_messages(
            chat_dir=chat_dir,
            chat_key=info.chat_key,
            chat_title=sel.title,
            day_str=loc.day_str,
            incoming_messages=[inc],
        )
        db.update_day_part_numbers(info.chat_key, loc.day_str, pmap)
        if edits:
            rebuild_links_and_audit_md_from_parts(
                chat_dir,
                chat_key=info.chat_key,
                chat_title=sel.title,
            )
            console.print(f"[yellow]✏️ [{sel.title}][/yellow] edição em msg #{msg.id} versionada.")

    @client.on(events.MessageDeleted())
    async def _on_deleted(event: Any) -> None:
        chat_key_hint: str | None = None
        if getattr(event, "chat_id", None) is not None:
            chat = await event.get_chat()
            if chat is not None:
                info = classify_entity_to_chat_info(chat, self_id=account.account_id)
                chat_key_hint = info.chat_key

        for del_id in getattr(event, "deleted_ids", None) or []:
            locs = db.locate_by_account_msg(
                account.account_id,
                int(del_id),
                chat_key=chat_key_hint,
            )
            for loc in locs:
                sel = selections.get(loc.chat_key)
                if sel is not None and not sel.baixar:
                    continue
                title = sel.title if sel is not None else loc.chat_key
                chat_dir = resolve_chat_dir(config.data_dir, loc.chat_key, title)
                updated = writer.mark_message_deleted(
                    chat_dir=chat_dir,
                    day_str=loc.day_str,
                    part_num=loc.part_num,
                    canonical_key=loc.canonical_key,
                    source="live_watch",
                )
                if updated is not None:
                    db.mark_locator_deleted(loc.chat_key, loc.canonical_key)
                    rebuild_links_and_audit_md_from_parts(
                        chat_dir,
                        chat_key=loc.chat_key,
                        chat_title=title,
                    )
                    console.print(
                        f"[red]🗑️ [{title}][/red] msg #{del_id} apagada remotamente "
                        "(preservada localmente!)."
                    )

    @client.on(events.UserUpdate())
    async def _on_user_update(event: Any) -> None:
        user_id = getattr(event, "user_id", None)
        if user_id is None:
            return
        lo, hi = sorted((account.account_id, int(user_id)))
        dm_key = f"dm_{lo}_{hi}"
        sel = selections.get(dm_key)
        if sel is None or not sel.baixar:
            return
        chat_dir = resolve_chat_dir(config.data_dir, dm_key, sel.title)
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

        if desc_parts:
            entry = f"- `{now}` (live via `{account.alias}`): {', '.join(desc_parts)}"
            append_markdown_log_entries(
                chat_dir / "metadata" / "presence.md",
                header_title=f"Presença e Atividade — {sel.title} (`{dm_key}`)",
                entries=[entry],
            )


async def run_live_watcher(
    *,
    account_clients: list[tuple[AccountRecord, Any]],
    selections: dict[str, ChatSelection],
    config: AppConfig,
    db: CheckpointDB,
    console: Console | None = None,
) -> None:
    out = console or Console()
    writer = ChatDayWriter(max_json_bytes=config.max_json_bytes)
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
        f"[bold green]📡 Escutando eventos em tempo real em {len(account_clients)} conta(s)... "
        "(Pressione Ctrl+C para encerrar)[/bold green]"
    )
    await asyncio.gather(*(client.run_until_disconnected() for _, client in account_clients))
