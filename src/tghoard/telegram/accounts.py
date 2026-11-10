"""Multi-account authentication and Telethon SQLiteSession management."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import typer
from rich.console import Console
from rich.markup import escape
from telethon import TelegramClient
from telethon import errors as tg_errors

from tghoard.config import AppConfig
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB
from tghoard.telegram.entities import display_name_of_entity
from tghoard.telegram.flood_control import client_logger_name

_WARNED_SESSIONS: set[str] = set()  # warn once per session file per process


class AccountManager:
    """Manages multiple Telegram accounts backed by isolated `.session` files."""

    def __init__(
        self,
        config: AppConfig,
        checkpoint_db: CheckpointDB,
        console: Console | None = None,
    ) -> None:
        self.config = config
        self.db = checkpoint_db
        self.console = console or Console()

    def require_api_credentials(self) -> tuple[int, str]:
        api_id = self.config.tg_api_id
        api_hash = self.config.tg_api_hash
        if not api_id:
            raw_id = typer.prompt("Enter your TG_API_ID (https://my.telegram.org)")
            api_id = int(str(raw_id).strip())
            self.config.tg_api_id = api_id
        if not api_hash:
            raw_hash = typer.prompt("Enter your TG_API_HASH", hide_input=True)
            api_hash = str(raw_hash).strip()
            self.config.tg_api_hash = api_hash
        return api_id, api_hash

    def session_file_for(self, account_id: int) -> Path:
        self.config.ensure_dirs()
        return self.config.sessions_dir / f"{account_id}.session"

    async def interactive_add_account(self, alias: str | None = None) -> AccountRecord:
        """Run interactive OTP (+ 2FA if enabled) login and register the account."""
        self.config.ensure_dirs()
        api_id, api_hash = self.require_api_credentials()
        tmp_base = self.config.sessions_dir / f".tmp_login_{uuid.uuid4().hex[:10]}"
        tmp_session_file = Path(f"{tmp_base}.session")
        # Created 0600 up front (Telethon would create it with the umask, typically 0644,
        # and the auth key would be world-readable until the chmod below)
        os.close(os.open(tmp_session_file, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600))

        client = TelegramClient(
            str(tmp_base),
            api_id,
            api_hash,
            device_model="tghoard CLI",
            system_version="Linux",
            app_version="0.1.0",
        )
        await client.connect()
        try:
            phone = typer.prompt("Phone number with country code (e.g. +5511999999999)").strip()
            sent = await client.send_code_request(phone)
            code = typer.prompt("Code received via Telegram / SMS").strip()
            try:
                await client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=sent.phone_code_hash,
                )
            except tg_errors.SessionPasswordNeededError:
                password = typer.prompt("Two-step verification password (2FA)", hide_input=True)
                await client.sign_in(password=password)

            me = await client.get_me()
            if me is None:
                raise RuntimeError("Failed to get the profile of the authenticated account.")
            account_id = int(me.id)
            username = getattr(me, "username", None)
            disp_name = display_name_of_entity(me)
        except BaseException:
            await client.disconnect()
            # A half-done login leaves no stray (key-bearing) session file behind
            for leftover in (tmp_session_file, Path(f"{tmp_session_file}-journal")):
                leftover.unlink(missing_ok=True)
            raise
        await client.disconnect()

        final_session = self.session_file_for(account_id)
        if tmp_session_file.exists():
            os.replace(tmp_session_file, final_session)
            self._ensure_private(final_session)

        chosen_alias = alias or (str(username) if username else f"acc_{account_id}")
        rec = self.db.upsert_account(
            account_id=account_id,
            alias=chosen_alias,
            phone=phone,
            username=str(username) if username else None,
            display_name=disp_name,
        )
        self.console.print(
            f"[green]✓ Account authenticated:[/green] [bold]{escape(rec.display_name or '')}"
            f"[/bold] (alias=`{escape(rec.alias)}`, id=`{rec.account_id}`)"
        )
        return rec

    async def connect_account(self, account: AccountRecord) -> TelegramClient:
        """Open an authorized TelegramClient for an existing registered account."""
        api_id, api_hash = self.require_api_credentials()
        session_path = self.session_file_for(account.account_id)
        if not session_path.exists():
            raise RuntimeError(
                f"Session file missing for account `{account.alias}` ({session_path}). "
                "Run `tghoard account add` again."
            )
        session_prefix = str(session_path.with_suffix(""))
        client = TelegramClient(
            session_prefix,
            api_id,
            api_hash,
            device_model="tghoard CLI",
            system_version="Linux",
            app_version="0.1.0",
            # Per-account logger: lets the auto-throttle see Telethon's silent flood sleeps
            base_logger=client_logger_name(account.account_id),
        )
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError(
                f"The session of account `{account.alias}` ({account.account_id}) expired or was "
                "revoked. Re-authenticate with `tghoard account add`."
            )
        self._ensure_private(session_path)
        return client

    def _ensure_private(self, session_path: Path) -> None:
        """Sessions are full logins (they bypass 2FA): warn loudly if others can read them.

        `chmod` silently does nothing on exFAT/NTFS/vfat drives (permissions come from mount
        options) and fails on files owned by someone else, so check the result.
        """
        try:
            os.chmod(session_path, 0o600)
        except OSError:
            pass
        problems: list[str] = []
        for path, wanted in ((self.config.sessions_dir, 0o700), (session_path, 0o600)):
            try:
                st = path.stat()
            except OSError:
                continue
            if st.st_mode & 0o077:
                problems.append(
                    f"{path} has mode {oct(st.st_mode & 0o777)} (expected {oct(wanted)})"
                )
            if hasattr(os, "getuid") and st.st_uid != os.getuid():
                problems.append(f"{path} belongs to another user (uid {st.st_uid})")
        key = str(session_path)
        if problems and key not in _WARNED_SESSIONS:
            _WARNED_SESSIONS.add(key)
            self.console.print(
                "[bold red]⚠ Telegram session not private:[/bold red] "
                + escape("; ".join(problems))
                + ". Anyone who can read it can use your account. The filesystem may ignore "
                "permissions (exFAT/NTFS drive): keep TGHOARD_DATA_DIR on a Linux filesystem "
                "or restrict access to the drive."
            )

    def remove_account(self, identifier: int | str) -> AccountRecord | None:
        rec = self.db.remove_account(identifier)
        if rec is not None:
            session_path = self.session_file_for(rec.account_id)
            session_path.unlink(missing_ok=True)
            Path(f"{session_path}-journal").unlink(missing_ok=True)
        return rec
