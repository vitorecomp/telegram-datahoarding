"""Deterministic plain-text Markdown writers for `links/<folder>.md` and `metadata/<folder>/*.md`.

The generated text is archive content (users' existing files), so it stays in Portuguese.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tghoard.storage.atomic import atomic_write_text, utc_now_iso
from tghoard.storage.paths import ChatPaths, iter_all_part_files, single_line

URL_REGEX = re.compile(r"https?://[^\s<>()\[\]\"']+")


@dataclass(frozen=True)
class LinkEntry:
    day_str: str
    time_str: str
    msg_id: int | str
    canonical_key: str
    sender_name: str
    sender_username: str | None
    url: str
    snippet: str


def extract_links_from_text_and_entities(
    text: str,
    entities: Sequence[dict[str, Any]] | None = None,
    extra_urls: Sequence[str] | None = None,
) -> list[str]:
    """Extract unique HTTP(S) and t.me URLs preserving appearance order."""
    found: list[str] = []
    seen: set[str] = set()

    def _add(raw_url: str) -> None:
        cleaned = raw_url.strip().rstrip(".,;:!?)")
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            found.append(cleaned)

    for ent in entities or []:
        ent_url = ent.get("url")
        if isinstance(ent_url, str) and ent_url.startswith(("http://", "https://", "tg://")):
            _add(ent_url)

    for match in URL_REGEX.findall(text or ""):
        _add(match)

    for u in extra_urls or []:
        if isinstance(u, str):
            _add(u)

    return found


def ensure_initial_metadata_files(
    chat_paths: ChatPaths,
    *,
    chat_key: str,
    chat_title: str,
) -> None:
    """Create baseline `metadata/*.md` files if not yet present."""
    chat_title = single_line(chat_title)
    meta_dir = chat_paths.metadata_dir
    meta_dir.mkdir(parents=True, exist_ok=True)
    defaults: dict[str, str] = {
        "info.md": f"# Informações — {chat_title} (`{chat_key}`)\n\n_Sincronização pendente._\n",
        "members.md": f"# Membros — {chat_title} (`{chat_key}`)\n\n_Sem lista de membros._\n",
        "topics.md": f"# Tópicos — {chat_title} (`{chat_key}`)\n\n_Chat sem tópicos de fórum._\n",
        "stories.md": f"# Stories — {chat_title} (`{chat_key}`)\n\n_Nenhum story capturado._\n",
        "deleted_and_edits.md": (
            f"# Mensagens Apagadas e Edições — {chat_title} (`{chat_key}`)\n\n"
            "_Nenhuma mensagem apagada ou edição registrada até o momento._\n"
        ),
        "profile_history.md": (
            f"# Histórico de Perfil — {chat_title} (`{chat_key}`)\n\n"
            "_Nenhuma alteração de perfil detectada ainda._\n"
        ),
        "presence.md": (
            f"# Presença e Atividade — {chat_title} (`{chat_key}`)\n\n"
            "_Nenhum evento de presença registrado._\n"
        ),
        "events_and_admin_log.md": (
            f"# Eventos e Admin Log — {chat_title} (`{chat_key}`)\n\n"
            "_Nenhum evento de serviço ou Admin Log registrado._\n"
        ),
    }
    for filename, content in defaults.items():
        fpath = meta_dir / filename
        if not fpath.exists():
            atomic_write_text(fpath, content)


def rebuild_links_and_audit_md_from_parts(
    chat_paths: ChatPaths,
    *,
    chat_key: str,
    chat_title: str,
) -> tuple[int, int, int]:
    """Scan `messages/<folder>/*-pt*.json` to write `links/<folder>.md` and `deleted_and_edits.md`.

    Returns `(total_links, deleted_count, edited_count)`.
    """
    chat_title = single_line(chat_title)
    links_by_day: dict[str, list[LinkEntry]] = {}
    seen_link_keys: set[tuple[str, str]] = set()
    # An interrupted repack can briefly leave a message in two parts (until the month's next
    # write merges them): list each message once, keeping its most complete copy.
    audited: dict[str, dict[str, Any]] = {}

    for part_path in iter_all_part_files(chat_paths):
        payload = json.loads(part_path.read_text(encoding="utf-8"))
        month = str(payload.get("month") or part_path.name[:7])
        for msg in payload.get("messages") or []:
            ckey = str(msg.get("canonical_key") or msg.get("id"))
            msg_id = msg.get("id", "?")
            date_iso = str(msg.get("date") or "")
            # links stay grouped by day (month files hold many days)
            day_str = date_iso[:10] if len(date_iso) >= 10 else month
            time_str = date_iso[11:19] if len(date_iso) >= 19 else date_iso
            sender = msg.get("sender") or {}
            sender_name = single_line(sender.get("name") or sender.get("id") or "Desconhecido")
            sender_user = sender.get("username")
            text = str(msg.get("text") or "")
            snippet = single_line(text)
            if len(snippet) > 160:
                snippet = snippet[:157] + "..."

            for url in msg.get("links") or []:
                dedup_token = (ckey, url)
                if dedup_token in seen_link_keys:
                    continue
                seen_link_keys.add(dedup_token)
                links_by_day.setdefault(day_str, []).append(
                    LinkEntry(
                        day_str=day_str,
                        time_str=time_str,
                        msg_id=msg_id,
                        canonical_key=ckey,
                        sender_name=sender_name,
                        sender_username=sender_user,
                        url=url,
                        snippet=snippet,
                    )
                )

            prev = audited.get(ckey)
            if prev is None or _completeness(msg) > _completeness(prev):
                audited[ckey] = msg

    deleted_items = [m for m in audited.values() if m.get("is_deleted")]
    edited_items = [m for m in audited.values() if m.get("revisions")]

    # Write links.md
    total_links = sum(len(v) for v in links_by_day.values())
    lines = [
        f"# Links — {chat_title} (`{chat_key}`)",
        "",
        f"- **Total de links**: {total_links}",
        f"- **Última atualização**: {utc_now_iso()}",
        "",
    ]
    if total_links == 0:
        lines.append("_Nenhum link capturado ainda._")
    else:
        for day_str in sorted(links_by_day.keys()):
            lines.append(f"## {day_str}")
            lines.append("")
            for item in links_by_day[day_str]:
                handle = f" (`@{item.sender_username}`)" if item.sender_username else ""
                lines.append(
                    f"- `{item.time_str}` | **{item.sender_name}**{handle} | "
                    f"`msg #{item.msg_id}` — <{item.url}>"
                )
                if item.snippet and item.snippet != item.url:
                    lines.append(f"  > {item.snippet}")
            lines.append("")
    chat_paths.links_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(chat_paths.links_path, "\n".join(lines).rstrip() + "\n")

    # Write metadata/deleted_and_edits.md
    audit_lines = [
        f"# Mensagens Apagadas e Edições — {chat_title} (`{chat_key}`)",
        "",
        f"- **Mensagens apagadas preservadas**: {len(deleted_items)}",
        f"- **Mensagens com histórico de edições**: {len(edited_items)}",
        f"- **Última atualização**: {utc_now_iso()}",
        "",
        "## Mensagens Apagadas Preservadas",
        "",
    ]
    if not deleted_items:
        audit_lines.append("_Nenhuma mensagem apagada detectada._")
    else:
        for msg in deleted_items:
            sender = msg.get("sender") or {}
            sname = single_line(sender.get("name") or sender.get("id") or "Desconhecido")
            txt = single_line(msg.get("text") or "") or "_[sem texto / apenas mídia]_"
            media_note = ""
            if msg.get("media"):
                mpath = msg["media"].get("path") or msg["media"].get("category")
                media_note = f" | Mídia: `{mpath}`"
            audit_lines.append(
                f"- **`msg #{msg.get('id')}`** ({msg.get('date')}) por **{sname}** "
                f"— _detectada em `{msg.get('deleted_detected_at')}` "
                f"via `{msg.get('delete_source')}`_{media_note}"
            )
            audit_lines.append(f"  > {txt}")
    audit_lines.extend(["", "## Histórico de Edições", ""])
    if not edited_items:
        audit_lines.append("_Nenhuma edição detectada._")
    else:
        for msg in edited_items:
            sender = msg.get("sender") or {}
            sname = single_line(sender.get("name") or sender.get("id") or "Desconhecido")
            audit_lines.append(f"### `msg #{msg.get('id')}` ({msg.get('date')}) — **{sname}**")
            for rev in msg.get("revisions") or []:
                rev_txt = single_line(rev.get("text") or "")
                audit_lines.append(
                    f"- **Versão #{rev.get('revision')}** "
                    f"(capturada em `{rev.get('captured_at')}`): {rev_txt}"
                )
            cur_txt = single_line(msg.get("text") or "")
            audit_lines.append(
                f"- **Versão atual** (editada em `{msg.get('edit_date')}`): {cur_txt}"
            )
            audit_lines.append("")

    meta_dir = chat_paths.metadata_dir
    meta_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        meta_dir / "deleted_and_edits.md",
        "\n".join(audit_lines).rstrip() + "\n",
    )
    return total_links, len(deleted_items), len(edited_items)


def _completeness(msg: dict[str, Any]) -> tuple[bool, int]:
    return bool(msg.get("is_deleted")), len(msg.get("revisions") or [])


def write_info_md(
    chat_paths: ChatPaths,
    *,
    chat_key: str,
    chat_title: str,
    chat_type: str,
    peer_id: int,
    username: str | None,
    usernames: Sequence[str] | None,
    bio_or_about: str | None,
    phone: str | None,
    participants_count: int | None,
    accounts_info: Sequence[dict[str, Any]],
    synced_ranges: Sequence[tuple[int, int]],
    stats: dict[str, int] | None = None,
) -> None:
    # Line 1 is the folder's identity (`(`chat_key`)` at the end): keep it on one line
    chat_title = single_line(chat_title)
    lines = [
        f"# Informações — {chat_title} (`{chat_key}`)",
        "",
        f"- **Chat Key**: `{chat_key}`",
        f"- **Tipo**: `{chat_type}`",
        f"- **Telegram ID**: `{peer_id}`",
    ]
    if username:
        lines.append(f"- **Username Principal**: `@{username.lstrip('@')}`")
    if usernames:
        formatted = ", ".join(f"`@{u.lstrip('@')}`" for u in usernames)
        lines.append(f"- **Usernames Adicionais**: {formatted}")
    if phone:
        lines.append(f"- **Telefone Visível**: `{phone}`")
    if participants_count is not None:
        lines.append(f"- **Participantes Reportados**: {participants_count}")
    if synced_ranges:
        ranges_str = ", ".join(f"`{s}..{e}`" for s, e in synced_ranges)
        lines.append(f"- **Intervalos de Mensagens Sincronizados**: {ranges_str}")
    lines.append(f"- **Atualizado em**: {utc_now_iso()}")

    if bio_or_about:
        lines.extend(["", "## Descrição / Bio", "", bio_or_about.strip()])

    if stats:
        lines.extend(
            [
                "",
                "## Estatísticas Locais",
                "",
                f"- **Partes JSON (`messages/<pasta>/*-pt*.json`)**: {stats.get('parts', 0)}",
                f"- **Fotos em `photos/`**: {stats.get('photos', 0)}",
                f"- **Vídeos em `videos/`**: {stats.get('videos', 0)}",
                f"- **Áudios em `audio/`**: {stats.get('audio', 0)}",
                f"- **Arquivos em `files/`**: {stats.get('files', 0)}",
                f"- **Links em `links/<pasta>.md`**: {stats.get('links', 0)}",
            ]
        )

    lines.extend(["", "## Contas Sincronizadas Neste Chat", ""])
    if not accounts_info:
        lines.append("_Nenhuma conta registrada._")
    else:
        lines.append("| Account ID | Alias | Username | Entrou em | Última Atualização |")
        lines.append("| :--- | :--- | :--- | :--- | :--- |")
        for acc in accounts_info:
            uname = f"@{acc['username']}" if acc.get("username") else "-"
            joined = f"`{acc['joined_at']}`" if acc.get("joined_at") else "-"
            if acc.get("inviter_id"):
                joined += f" (convidado por `{acc['inviter_id']}`)"
            lines.append(
                f"| `{acc['account_id']}` | `{acc.get('alias', '-')}` | "
                f"`{uname}` | {joined} | `{acc.get('updated_at', '-')}` |"
            )

    atomic_write_text(chat_paths.metadata_dir / "info.md", "\n".join(lines).rstrip() + "\n")


def write_members_md(
    chat_paths: ChatPaths,
    *,
    chat_key: str,
    chat_title: str,
    members: Sequence[dict[str, Any]],
    note: str | None = None,
) -> None:
    chat_title = single_line(chat_title)
    lines = [
        f"# Membros — {chat_title} (`{chat_key}`)",
        "",
        f"- **Total listado**: {len(members)}",
        f"- **Atualizado em**: {utc_now_iso()}",
        "",
    ]
    if note:
        lines.extend([f"> {note}", ""])
    if not members:
        lines.append("_Nenhum membro visível ou chat 1:1._")
    else:
        lines.append("| User ID | Nome | Username | Cargo / Papel | Bot |")
        lines.append("| :--- | :--- | :--- | :--- | :---: |")
        for m in members:
            uname = f"`@{m['username']}`" if m.get("username") else "-"
            name = single_line(m.get("name") or "-").replace("|", "\\|")
            role = str(m.get("role") or "member")
            is_bot = "Sim" if m.get("is_bot") else "Não"
            lines.append(f"| `{m['id']}` | {name} | {uname} | `{role}` | {is_bot} |")

    atomic_write_text(chat_paths.metadata_dir / "members.md", "\n".join(lines).rstrip() + "\n")


def write_topics_md(
    chat_paths: ChatPaths,
    *,
    chat_key: str,
    chat_title: str,
    topics: Sequence[dict[str, Any]],
) -> None:
    chat_title = single_line(chat_title)
    lines = [
        f"# Tópicos — {chat_title} (`{chat_key}`)",
        "",
        f"- **Total de tópicos**: {len(topics)}",
        f"- **Atualizado em**: {utc_now_iso()}",
        "",
    ]
    if not topics:
        lines.append("_Chat sem tópicos de fórum ativos._")
    else:
        lines.append("| Topic ID | Título | Status | Fixado |")
        lines.append("| :--- | :--- | :--- | :---: |")
        for t in topics:
            title = single_line(t.get("title") or "-").replace("|", "\\|")
            status = "Fechado" if t.get("closed") else "Aberto"
            pinned = "Sim" if t.get("pinned") else "Não"
            lines.append(f"| `{t['id']}` | {title} | {status} | {pinned} |")

    atomic_write_text(chat_paths.metadata_dir / "topics.md", "\n".join(lines).rstrip() + "\n")


def append_markdown_log_entries(
    md_path: Path,
    *,
    header_title: str,
    entries: Sequence[str],
) -> int:
    """Append unique Markdown bullet lines to a log `.md` file.

    These logs are the only copy of the history (profile changes, stories, presence, admin
    log): existing lines are never dropped. Each entry is forced onto a single line, since
    entries embed Telegram-sourced text (bios, captions) and dedup is line-based.
    """
    entries = [_one_line_entry(e) for e in entries if e and e.strip()]
    if not entries:
        return 0

    existing_text = ""
    if md_path.exists():
        existing_text = md_path.read_text(encoding="utf-8")
    existing_lines = existing_text.rstrip().split("\n") if existing_text.strip() else []

    # Replace the file only while it is still exactly the initial placeholder
    body = [ln.strip() for ln in existing_lines[1:] if ln.strip()]
    if not existing_lines or (len(body) == 1 and body[0] in _LOG_PLACEHOLDERS):
        existing_lines = [f"# {single_line(header_title)}", ""]

    existing_set = set(existing_lines)
    added = 0
    for entry in entries:
        if entry not in existing_set:
            existing_lines.append(entry)
            existing_set.add(entry)
            added += 1

    if added > 0 or not md_path.exists():
        atomic_write_text(md_path, "\n".join(existing_lines).rstrip() + "\n")
    return added


_LOG_PLACEHOLDERS = frozenset(
    {
        "_Nenhum story capturado._",
        "_Nenhuma alteração de perfil detectada ainda._",
        "_Nenhum evento de presença registrado._",
        "_Nenhum evento de serviço ou Admin Log registrado._",
    }
)


def _one_line_entry(entry: str) -> str:
    """Keep the entry's leading indentation/bullet, collapse any embedded line break."""
    stripped = entry.lstrip(" ")
    return entry[: len(entry) - len(stripped)] + single_line(stripped)
