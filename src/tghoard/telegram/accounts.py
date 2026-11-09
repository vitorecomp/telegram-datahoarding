"""Multi-account authentication and Telethon SQLiteSession management."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import typer
from rich.console import Console
from telethon import TelegramClient
from telethon import errors as tg_errors

from tghoard.config import AppConfig
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB
from tghoard.telegram.entities import display_name_of_entity


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
            raw_id = typer.prompt("Informe seu TG_API_ID (https://my.telegram.org)")
            api_id = int(str(raw_id).strip())
            self.config.tg_api_id = api_id
        if not api_hash:
            raw_hash = typer.prompt("Informe seu TG_API_HASH", hide_input=True)
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
            phone = typer.prompt("Número de telefone com DDI (ex: +5511999999999)").strip()
            sent = await client.send_code_request(phone)
            code = typer.prompt("Código recebido no Telegram / SMS").strip()
            try:
                await client.sign_in(
                    phone=phone,
                    code=code,
                    phone_code_hash=sent.phone_code_hash,
                )
            except tg_errors.SessionPasswordNeededError:
                password = typer.prompt(
                    "Senha de verificação em duas etapas (2FA)", hide_input=True
                )
                await client.sign_in(password=password)

            me = await client.get_me()
            if me is None:
                raise RuntimeError("Falha ao obter perfil da conta autenticada.")
            account_id = int(me.id)
            username = getattr(me, "username", None)
            disp_name = display_name_of_entity(me)
        finally:
            await client.disconnect()

        final_session = self.session_file_for(account_id)
        if tmp_session_file.exists():
            os.replace(tmp_session_file, final_session)
            try:
                os.chmod(final_session, 0o600)
            except OSError:
                pass

        chosen_alias = alias or (str(username) if username else f"acc_{account_id}")
        rec = self.db.upsert_account(
            account_id=account_id,
            alias=chosen_alias,
            phone=phone,
            username=str(username) if username else None,
            display_name=disp_name,
        )
        self.console.print(
            f"[green]✓ Conta autenticada:[/green] [bold]{rec.display_name}[/bold] "
            f"(alias=`{rec.alias}`, id=`{rec.account_id}`)"
        )
        return rec

    async def connect_account(self, account: AccountRecord) -> TelegramClient:
        """Open an authorized TelegramClient for an existing registered account."""
        api_id, api_hash = self.require_api_credentials()
        session_path = self.session_file_for(account.account_id)
        if not session_path.exists():
            raise RuntimeError(
                f"Arquivo de sessão ausente para a conta `{account.alias}` ({session_path}). "
                "Execute `tghoard account add` novamente."
            )
        session_prefix = str(session_path.with_suffix(""))
        client = TelegramClient(
            session_prefix,
            api_id,
            api_hash,
            device_model="tghoard CLI",
            system_version="Linux",
            app_version="0.1.0",
        )
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError(
                f"A sessão da conta `{account.alias}` ({account.account_id}) expirou ou foi "
                "revogada. Reautentique com `tghoard account add`."
            )
        try:
            os.chmod(session_path, 0o600)
        except OSError:
            pass
        return client

    def remove_account(self, identifier: int | str) -> AccountRecord | None:
        rec = self.db.remove_account(identifier)
        if rec is not None:
            session_path = self.session_file_for(rec.account_id)
            session_path.unlink(missing_ok=True)
            Path(f"{session_path}-journal").unlink(missing_ok=True)
        return rec
