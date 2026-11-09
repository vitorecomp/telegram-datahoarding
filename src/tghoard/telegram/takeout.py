"""Telegram Takeout API session wrapper with automatic fallback on cooldown."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from rich.console import Console
from telethon import errors as tg_errors

from tghoard.config import SyncMode


@asynccontextmanager
async def open_sync_session(
    client: Any,
    *,
    mode: SyncMode,
    max_media_bytes: int | None,
    console: Console | None = None,
) -> AsyncIterator[tuple[Any, bool]]:
    """Yield `(active_client, is_takeout)` according to `mode` (`auto` | `takeout` | `standard`)."""
    out = console or Console()
    if mode == "standard" or not hasattr(client, "takeout"):
        yield client, False
        return

    max_file_size = max_media_bytes if max_media_bytes is not None else 4 * 1024 * 1024 * 1024
    try:
        async with client.takeout(
            finalize=True,
            contacts=True,
            users=True,
            chats=True,
            megagroups=True,
            channels=True,
            files=True,
            max_file_size=max_file_size,
        ) as takeout_client:
            yield takeout_client, True
            return
    except tg_errors.TakeoutInitDelayError as exc:
        hours = getattr(exc, "seconds", 0) / 3600.0
        if mode == "takeout":
            raise RuntimeError(
                f"Sessão Takeout em espera de segurança do Telegram ({hours:.1f}h restantes). "
                "Confirme no app móvel oficial ou use `--mode auto` / `--mode standard`."
            ) from exc
        out.print(
            f"[yellow]⚠️ Takeout temporariamente retido pelo Telegram ({hours:.1f}h restantes "
            "para sessão nova). Seguindo automaticamente em modo standard cadenciado.[/yellow]"
        )
    except tg_errors.RPCError as exc:
        if mode == "takeout":
            raise
        out.print(
            f"[yellow]⚠️ Não foi possível iniciar Takeout ({type(exc).__name__}). "
            "Seguindo em modo standard cadenciado.[/yellow]"
        )

    yield client, False
