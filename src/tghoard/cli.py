"""CLI entrypoint for `tghoard` (`start`, `sync`, `account`, `watch`, `search`, `stats`)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from tghoard.config import ALL_MEDIA_CATEGORIES, SyncMode, load_config
from tghoard.storage.chats_manifest import load_chats_manifest
from tghoard.storage.checkpoint_db import AccountRecord, CheckpointDB
from tghoard.storage.paths import iter_all_part_files
from tghoard.sync.discovery import run_discovery_and_write_manifest
from tghoard.sync.orchestrator import run_multi_account_sync
from tghoard.sync.rebuilder import (
    iter_archive_chat_dirs,
    rebuild_checkpoint_from_plaintext,
    rebuild_markdown_from_plaintext,
)
from tghoard.sync.watcher import run_live_watcher
from tghoard.telegram.accounts import AccountManager

app = typer.Typer(
    name="tghoard",
    help="Arquivador plain-text multi-conta do Telegram com checkpoints SQLite.",
    no_args_is_help=True,
)
account_app = typer.Typer(
    name="account",
    help="Gerenciar contas autenticadas do Telegram.",
    no_args_is_help=True,
)
app.add_typer(account_app, name="account")

console = Console()

DataDirOption = Annotated[
    Path | None,
    typer.Option(
        "--data-dir",
        "-d",
        help="Diretório de saída dos itens (padrão: TGHOARD_DATA_DIR ou ./data).",
    ),
]


def _resolve_runtime_config(
    *,
    data_dir: Path | None = None,
    max_json_kb: int | None = None,
    max_media_mb: int | None = None,
    concurrency: int | None = None,
    sync_mode: SyncMode | None = None,
):
    cfg = load_config(".env").with_overrides(
        data_dir=data_dir,
        max_json_kb=max_json_kb,
        max_media_mb=max_media_mb,
        concurrency=concurrency,
        sync_mode=sync_mode,
    )
    cfg.ensure_dirs()
    return cfg


def _select_accounts(
    db: CheckpointDB,
    account_filter: str | None,
) -> list[AccountRecord]:
    if account_filter:
        rec = db.get_account(account_filter)
        if rec is None:
            raise typer.BadParameter(f"Conta `{account_filter}` não encontrada.")
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
            help="Autenticar uma nova conta Telegram antes de atualizar o chats.md.",
        ),
    ] = False,
    alias: Annotated[
        str | None,
        typer.Option("--alias", help="Apelido para a nova conta autenticada."),
    ] = None,
    chats_file: Annotated[
        Path | None,
        typer.Option("--chats-file", help="Caminho alternativo para o arquivo chats.md."),
    ] = None,
) -> None:
    """Autentica conta(s) e cria/atualiza `<data_dir>/chats.md` com os checkboxes padrão."""
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
                )
            finally:
                for _, client in connected:
                    await client.disconnect()  # type: ignore[attr-defined]

            console.print(
                f"\n[bold green]✓ Manifesto atualizado em:[/bold green] `{summary.manifest_path}`\n"
                f"  • Total de chats listados: [bold]{summary.total_chats}[/bold] "
                f"(novos: {summary.added_chats}, preservados: {summary.updated_chats})\n"
                f"  • Próximo passo: edite os checkboxes em `{summary.manifest_path}` e execute "
                "`tghoard sync`."
            )

    asyncio.run(_run())


# ----------------------------------------------------------------------
# 2. `tghoard sync`
# ----------------------------------------------------------------------
@app.command("sync")
def cmd_sync(
    data_dir: DataDirOption = None,
    account: Annotated[
        str | None,
        typer.Option(
            "--account", "-a", help="Sincronizar apenas uma conta específica (alias ou ID)."
        ),
    ] = None,
    chat: Annotated[
        str | None,
        typer.Option(
            "--chat", "-c", help="Sincronizar apenas um chat específico (`chat_key` ou título)."
        ),
    ] = None,
    mode: Annotated[
        SyncMode | None,
        typer.Option("--mode", help="Modo de sessão: auto | takeout | standard."),
    ] = None,
    max_json_kb: Annotated[
        int | None,
        typer.Option("--max-json-kb", help="Tamanho máximo (KB) por parte YYYY-MM-DD-ptNNN.json."),
    ] = None,
    max_media_mb: Annotated[
        int | None,
        typer.Option("--max-media-mb", help="Tamanho máximo por anexo em MB (0 = sem limite)."),
    ] = None,
    concurrency: Annotated[
        int | None,
        typer.Option("--concurrency", help="Concorrência máxima de chamadas/downloads."),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option("--since", help="Data mínima YYYY-MM-DD para mensagens históricas."),
    ] = None,
    skip_members: Annotated[
        bool,
        typer.Option("--skip-members", help="Pular sincronização da lista de membros."),
    ] = False,
    skip_stories: Annotated[
        bool,
        typer.Option("--skip-stories", help="Pular captura de Stories ativos."),
    ] = False,
    skip_profile_photos: Annotated[
        bool,
        typer.Option("--skip-profile-photos", help="Pular histórico de fotos de perfil."),
    ] = False,
    chats_file: Annotated[
        Path | None,
        typer.Option("--chats-file", help="Caminho alternativo para o arquivo chats.md."),
    ] = None,
) -> None:
    """Sincroniza mensagens, links, metadados e mídias conforme `<data_dir>/chats.md`."""
    cfg = _resolve_runtime_config(
        data_dir=data_dir,
        max_json_kb=max_json_kb,
        max_media_mb=max_media_mb,
        concurrency=concurrency,
        sync_mode=mode,
    )
    manifest_path = chats_file or cfg.chats_manifest_path
    if not manifest_path.exists():
        console.print(
            f"[red]Arquivo `{manifest_path}` não encontrado. "
            "Execute `tghoard start` primeiro.[/red]"
        )
        raise typer.Exit(code=1)

    selections = load_chats_manifest(manifest_path)
    since_dt: datetime | None = None
    if since:
        since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=UTC)

    async def _run() -> None:
        with CheckpointDB(cfg.checkpoint_db_path) as db:
            accounts = _select_accounts(db, account)
            if not accounts:
                console.print(
                    "[red]Nenhuma conta autenticada encontrada. "
                    "Execute `tghoard start` primeiro.[/red]"
                )
                raise typer.Exit(code=1)

            mgr = AccountManager(cfg, db, console=console)
            connected: list[tuple[AccountRecord, object]] = []
            try:
                for acc in accounts:
                    client = await mgr.connect_account(acc)
                    connected.append((acc, client))

                await run_multi_account_sync(
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
                )
            finally:
                for _, client in connected:
                    await client.disconnect()  # type: ignore[attr-defined]

    asyncio.run(_run())


# ----------------------------------------------------------------------
# 3. `tghoard account ...`
# ----------------------------------------------------------------------
@account_app.command("list")
def cmd_account_list(data_dir: DataDirOption = None) -> None:
    """Lista todas as contas Telegram registradas em `<data_dir>`."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        accounts = db.list_accounts()

    if not accounts:
        console.print("[yellow]Nenhuma conta cadastrada neste diretório.[/yellow]")
        return

    table = Table(title=f"Contas Telegram ({cfg.data_dir})")
    table.add_column("Account ID", style="cyan")
    table.add_column("Alias", style="bold")
    table.add_column("Nome")
    table.add_column("Username")
    table.add_column("Último Sync")
    for acc in accounts:
        table.add_row(
            str(acc.account_id),
            acc.alias,
            acc.display_name or "-",
            f"@{acc.username}" if acc.username else "-",
            acc.last_sync_at or "Nunca",
        )
    console.print(table)


@account_app.command("add")
def cmd_account_add(
    data_dir: DataDirOption = None,
    alias: Annotated[str | None, typer.Option("--alias", help="Apelido da conta.")] = None,
) -> None:
    """Autentica e adiciona uma nova conta Telegram ao diretório de dados."""
    cfg = _resolve_runtime_config(data_dir=data_dir)

    async def _run() -> None:
        with CheckpointDB(cfg.checkpoint_db_path) as db:
            mgr = AccountManager(cfg, db, console=console)
            await mgr.interactive_add_account(alias=alias)

    asyncio.run(_run())


@account_app.command("remove")
def cmd_account_remove(
    identifier: Annotated[str, typer.Argument(help="Alias, username ou ID numérico da conta.")],
    data_dir: DataDirOption = None,
) -> None:
    """Remove a sessão de uma conta mantendo todos os dados plain-text já baixados."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        mgr = AccountManager(cfg, db, console=console)
        removed = mgr.remove_account(identifier)

    if removed is None:
        console.print(f"[red]Conta `{identifier}` não encontrada.[/red]")
        raise typer.Exit(code=1)
    console.print(
        f"[green]✓ Sessão da conta `{removed.alias}` ({removed.account_id}) removida. "
        "Dados arquivados permanecem intactos.[/green]"
    )


# ----------------------------------------------------------------------
# 4. `tghoard watch`
# ----------------------------------------------------------------------
@app.command("watch")
def cmd_watch(
    data_dir: DataDirOption = None,
    account: Annotated[
        str | None,
        typer.Option("--account", "-a", help="Escutar apenas uma conta específica."),
    ] = None,
    chats_file: Annotated[
        Path | None,
        typer.Option("--chats-file", help="Caminho alternativo para o arquivo chats.md."),
    ] = None,
) -> None:
    """Escuta eventos ao vivo (edições, deleções instantâneas, status online e digitação)."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    manifest_path = chats_file or cfg.chats_manifest_path
    if not manifest_path.exists():
        console.print(
            f"[red]Arquivo `{manifest_path}` não encontrado. Execute `tghoard start`.[/red]"
        )
        raise typer.Exit(code=1)
    selections = load_chats_manifest(manifest_path)

    async def _run() -> None:
        with CheckpointDB(cfg.checkpoint_db_path) as db:
            accounts = _select_accounts(db, account)
            if not accounts:
                console.print("[red]Nenhuma conta cadastrada.[/red]")
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
        asyncio.run(_run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Escuta encerrada pelo usuário.[/yellow]")


# ----------------------------------------------------------------------
# 5. `tghoard search` & `tghoard stats`
# ----------------------------------------------------------------------
@app.command("search")
def cmd_search(
    query: Annotated[str, typer.Argument(help="Termo de busca (case-insensitive).")],
    data_dir: DataDirOption = None,
    chat: Annotated[
        str | None, typer.Option("--chat", "-c", help="Filtrar por pasta/chat_key.")
    ] = None,
    deleted: Annotated[bool, typer.Option("--deleted", help="Apenas mensagens apagadas.")] = False,
    edited: Annotated[bool, typer.Option("--edited", help="Apenas mensagens editadas.")] = False,
    links: Annotated[
        bool, typer.Option("--links", help="Buscar também nas URLs extraídas.")
    ] = False,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Máximo de resultados.")] = 50,
) -> None:
    """Pesquisa rápida sobre os arquivos plain-text `chats/*-pt*.json`."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    q_lower = query.lower()
    matches = 0

    for chat_key, chat_dir in iter_archive_chat_dirs(cfg.data_dir):
        if chat and chat.lower() not in chat_dir.name.lower():
            continue
        for part_file in iter_all_part_files(chat_dir):
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
                        badges.append("[red]APAGADA[/red]")
                    if msg.get("revisions"):
                        badges.append(f"[yellow]EDITADA({len(msg['revisions'])})[/yellow]")
                    badge_str = (" " + " ".join(badges)) if badges else ""
                    sender = (msg.get("sender") or {}).get("name") or "?"
                    snippet = str(msg.get("text") or "").replace("\n", " ")[:180]
                    console.print(
                        f"[cyan]{title}[/cyan] (`{part_file.name}`) "
                        f"#{msg.get('id')} [{msg.get('date')}] "
                        f"[bold]{sender}[/bold]{badge_str}: {snippet}"
                    )
                    if matches >= limit:
                        console.print(f"[dim]Limite de {limit} resultados atingido.[/dim]")
                        return

    if matches == 0:
        console.print("[yellow]Nenhum resultado encontrado.[/yellow]")


@app.command("stats")
def cmd_stats(data_dir: DataDirOption = None) -> None:
    """Exibe estatísticas completas do acervo plain-text e do banco de checkpoints."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        db_counts = db.summary_counts()

    folder_counts = dict.fromkeys(ALL_MEDIA_CATEGORIES, 0)
    part_files_count = 0
    chat_dirs = iter_archive_chat_dirs(cfg.data_dir)
    for _, cdir in chat_dirs:
        part_files_count += len(iter_all_part_files(cdir))
        for cat in ALL_MEDIA_CATEGORIES:
            sub = cdir / cat
            if sub.exists():
                folder_counts[cat] += sum(
                    1 for f in sub.iterdir() if f.is_file() and not f.name.startswith(".")
                )

    table = Table(title=f"Estatísticas do Acervo ({cfg.data_dir})")
    table.add_column("Indicador", style="cyan")
    table.add_column("Valor", justify="right", style="bold")
    table.add_row("Contas cadastradas", str(db_counts["accounts"]))
    table.add_row("Pastas de chat no disco", str(len(chat_dirs)))
    table.add_row("Chats compartilhados entre 2+ contas", str(db_counts["shared_chats"]))
    table.add_row("Arquivos JSON particionados (*-ptNNN.json)", str(part_files_count))
    table.add_row("Mensagens indexadas", str(db_counts["messages"]))
    table.add_row("Mensagens apagadas preservadas", str(db_counts["deleted_messages"]))
    table.add_row("Fotos em photos/", str(folder_counts["photos"]))
    table.add_row("Vídeos em videos/", str(folder_counts["videos"]))
    table.add_row("Áudios em audio/", str(folder_counts["audio"]))
    table.add_row("Outros arquivos em files/", str(folder_counts["files"]))
    table.add_row("Mídias únicas no índice global", str(db_counts["unique_media_files"]))
    table.add_row("Mídias pendentes (checkbox desmarcado)", str(db_counts["pending_media"]))
    console.print(table)


# ----------------------------------------------------------------------
# 6. Maintenance: `rebuild-checkpoint` & `rebuild-md`
# ----------------------------------------------------------------------
@app.command("rebuild-checkpoint")
def cmd_rebuild_checkpoint(data_dir: DataDirOption = None) -> None:
    """Reconstrói `<data_dir>/.tghoard/checkpoint.db` varrendo apenas as pastas plain-text."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    with CheckpointDB(cfg.checkpoint_db_path) as db:
        rep = rebuild_checkpoint_from_plaintext(cfg.data_dir, db)
    console.print(
        f"[green]✓ Checkpoint reconstruído com sucesso![/green] "
        f"Chats: {rep.chats_scanned} | Partes JSON: {rep.part_files_scanned} | "
        f"Mensagens: {rep.messages_indexed} | Mídias: {rep.media_files_indexed}"
    )


@app.command("rebuild-md")
def cmd_rebuild_md(
    data_dir: DataDirOption = None,
    chat: Annotated[
        str | None, typer.Option("--chat", "-c", help="Filtrar chat específico.")
    ] = None,
) -> None:
    """Regenera `links.md` e `metadata/deleted_and_edits.md` a partir dos JSONs `*-ptNNN.json`."""
    cfg = _resolve_runtime_config(data_dir=data_dir)
    rep = rebuild_markdown_from_plaintext(cfg.data_dir, chat_filter=chat)
    console.print(
        f"[green]✓ Relatórios Markdown atualizados![/green] "
        f"Chats: {rep.chats_scanned} | Partes lidas: {rep.part_files_scanned} | "
        f"Links extraídos: {rep.links_rebuilt}"
    )


if __name__ == "__main__":
    app()
