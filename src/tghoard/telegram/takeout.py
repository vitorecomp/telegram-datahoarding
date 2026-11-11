"""Telegram Takeout API session wrapper with automatic fallback on cooldown.

A Takeout session is recorded in the account's `.session` file (`session.takeout_id`) and
closed with `account.finishTakeoutSession` when the sync ends. If a previous run died inside
it (killed, crash, power loss, unplugged drive), the id stays in the session file and Telethon
refuses to start a new one ("Can't send a takeout request while another takeout for the
current session still not been finished yet"): that leftover is finished here first.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from rich.console import Console
from telethon import errors as tg_errors

from tghoard.config import SyncMode

FINISH_TIMEOUT_SECS = 15.0  # never block shutdown on a dead connection


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
    entered: tuple[Any, Any] | None = None
    # Only *starting* the session is guarded here: errors raised by the sync itself (inside
    # the `yield`) must propagate, never be mistaken for "Takeout unavailable".
    try:
        entered = await _enter_takeout(client, max_file_size, out)
    except tg_errors.TakeoutInitDelayError as exc:
        hours = getattr(exc, "seconds", 0) / 3600.0
        if mode == "takeout":
            raise RuntimeError(
                f"Takeout session on a Telegram security hold ({hours:.1f}h left). "
                "Confirm it in the official mobile app or use `--mode auto` / `--mode standard`."
            ) from exc
        out.print(
            f"[yellow]⚠️ Takeout temporarily held by Telegram ({hours:.1f}h left "
            "for a new session). Continuing automatically in paced standard mode.[/yellow]"
        )
    except tg_errors.RPCError as exc:
        if mode == "takeout":
            raise
        out.print(
            f"[yellow]⚠️ Could not start Takeout ({type(exc).__name__}). "
            "Continuing in paced standard mode.[/yellow]"
        )

    if entered is None:
        yield client, False
        return

    takeout_cm, takeout_client = entered
    try:
        yield takeout_client, True
    except BaseException as exc:
        await _finish(takeout_cm, exc, out)
        raise
    await _finish(takeout_cm, None, out)


def _new_takeout(client: Any, max_file_size: int) -> Any:
    return client.takeout(
        finalize=True,
        contacts=True,
        users=True,
        chats=True,
        megagroups=True,
        channels=True,
        files=True,
        max_file_size=max_file_size,
    )


def _leftover_takeout_id(client: Any) -> int | None:
    return getattr(getattr(client, "session", None), "takeout_id", None)


async def _enter_takeout(client: Any, max_file_size: int, out: Console) -> tuple[Any, Any]:
    if _leftover_takeout_id(client) is not None:
        await _finish_leftover_takeout(client, out)
    cm = _new_takeout(client, max_file_size)
    try:
        return cm, await cm.__aenter__()
    except ValueError as exc:
        if "another takeout" not in str(exc):
            raise
        # The session still had an unfinished takeout we didn't see above: close it, retry once
        await _finish_leftover_takeout(client, out)
        cm = _new_takeout(client, max_file_size)
        return cm, await cm.__aenter__()


async def _finish_leftover_takeout(client: Any, out: Console) -> None:
    """Close a Takeout session a previous run left open (its id is still in the session file)."""
    out.print("[dim]Closing the Takeout session left open by a previous run…[/dim]")
    try:
        await asyncio.wait_for(client.end_takeout(success=False), FINISH_TIMEOUT_SECS)
    except (Exception, TimeoutError):
        pass  # already expired/invalid on Telegram's side, or no network: forget it locally
    session = getattr(client, "session", None)
    if session is not None and getattr(session, "takeout_id", None) is not None:
        session.takeout_id = None
        save = getattr(session, "save", None)
        if callable(save):
            save()


async def _finish(takeout_cm: Any, exc: BaseException | None, out: Console) -> None:
    """Finish the Takeout session (success only if the sync ended without an exception)."""
    try:
        args = (None, None, None) if exc is None else (type(exc), exc, exc.__traceback__)
        await asyncio.wait_for(takeout_cm.__aexit__(*args), FINISH_TIMEOUT_SECS)
    except (Exception, TimeoutError) as fin_exc:
        out.print(
            f"[yellow]⚠ Could not close the Takeout session ({type(fin_exc).__name__}); "
            "it is closed automatically on the next sync.[/yellow]"
        )
