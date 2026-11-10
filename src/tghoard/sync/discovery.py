"""Dialog discovery across accounts (`tghoard start` -> `<data_dir>/chats.md`)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.markup import escape

from tghoard.storage.chats_manifest import ChatSelection, merge_and_save_chats_manifest
from tghoard.storage.checkpoint_db import AccountRecord
from tghoard.sync.folders_sync import sync_folders_for_account
from tghoard.telegram.entities import classify_entity_to_chat_info


@dataclass
class DiscoverySummary:
    manifest_path: Path
    total_chats: int
    added_chats: int
    updated_chats: int
    accounts_scanned: int


async def discover_dialogs_from_client(
    client: Any,
    account: AccountRecord,
    data_dir: Path | None = None,
    console: Console | None = None,
) -> list[ChatSelection]:
    """Enumerate all dialogs for one connected Telegram client."""
    by_key: dict[str, ChatSelection] = {}
    acc_label = account.alias or str(account.account_id)

    # Always include Saved Messages explicitly
    saved_key = f"saved_{account.account_id}"
    by_key[saved_key] = ChatSelection(
        chat_key=saved_key,
        title=f"Saved Messages ({acc_label})",
        section="saved",
        username=account.username,
        accounts=[acc_label],
        baixar=True,
        photos=False,
        videos=False,
        audio=False,
        files=False,
    )

    dialogs: list[Any] = []
    async for dialog in client.iter_dialogs():
        dialogs.append(dialog)
        entity = getattr(dialog, "entity", None)
        if entity is None:
            continue
        info = classify_entity_to_chat_info(
            entity,
            self_id=account.account_id,
            account_alias=acc_label,
        )
        by_key[info.chat_key] = ChatSelection(
            chat_key=info.chat_key,
            title=info.title,
            section=info.section,
            username=info.username,
            accounts=[acc_label],
            baixar=True,
            photos=False,
            videos=False,
            audio=False,
            files=False,
        )

    if data_dir is not None:
        try:
            folders = await sync_folders_for_account(client, account, dialogs, data_dir)
            if console is not None:
                console.print(
                    f"[cyan]📁 {len(folders)} Telegram folder(s) saved to folders.md[/cyan]"
                )
        except Exception as exc:
            if console is not None:
                console.print(
                    f"[yellow]⚠ Telegram folders not saved: {type(exc).__name__}[/yellow]"
                )

    return list(by_key.values())


def consolidate_discovered_selections(
    per_account_lists: list[list[ChatSelection]],
) -> list[ChatSelection]:
    """Consolidate shared groups/channels across multiple accounts before writing `chats.md`."""
    consolidated: dict[str, ChatSelection] = {}
    for items in per_account_lists:
        for item in items:
            if item.chat_key in consolidated:
                prev = consolidated[item.chat_key]
                prev.accounts = sorted(set(prev.accounts) | set(item.accounts))
                if not prev.username and item.username:
                    prev.username = item.username
            else:
                consolidated[item.chat_key] = ChatSelection(
                    chat_key=item.chat_key,
                    title=item.title,
                    section=item.section,
                    username=item.username,
                    accounts=list(item.accounts),
                    baixar=item.baixar,
                    photos=item.photos,
                    videos=item.videos,
                    audio=item.audio,
                    files=item.files,
                )
    return list(consolidated.values())


async def run_discovery_and_write_manifest(
    *,
    account_clients: list[tuple[AccountRecord, Any]],
    manifest_path: Path,
    console: Console | None = None,
    data_dir: Path | None = None,
) -> DiscoverySummary:
    """Scan all provided `(account, client)` pairs and update `<data_dir>/chats.md`.

    `data_dir` receives `folders.md`/`.tghoard/folders.json` (defaults to the manifest's
    folder; pass it when `--chats-file` points elsewhere).
    """
    out = console or Console()
    data_dir = data_dir if data_dir is not None else manifest_path.parent
    per_account: list[list[ChatSelection]] = []

    for account, client in account_clients:
        out.print(
            f"[cyan]🔍 Fetching chats of account `{escape(account.alias)}` "
            f"({account.account_id})...[/cyan]"
        )
        items = await discover_dialogs_from_client(client, account, data_dir=data_dir, console=out)
        per_account.append(items)

    consolidated = consolidate_discovered_selections(per_account)
    merged, added, updated = merge_and_save_chats_manifest(manifest_path, consolidated)
    return DiscoverySummary(
        manifest_path=manifest_path,
        total_chats=len(merged),
        added_chats=added,
        updated_chats=updated,
        accounts_scanned=len(account_clients),
    )
