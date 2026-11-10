"""Telegram chat folders ("Work", "Personal", ...) -> `<data_dir>/folders.md` (+ JSON for the UI).

Telegram returns folders via `messages.getDialogFilters` as:
- explicit chats (`include_peers`), pinned chats and excluded chats, plus
- rules: contacts / non_contacts / groups / broadcasts / bots, and
  exclude_muted / exclude_read / exclude_archived.

Peers are resolved to our canonical `chat_key`s through the account's dialog list, and the
*members* of rule-based folders are computed from those dialogs (as the app would show them
right now — "exclude read/muted" depend on the moment of the sync).

Files (rewritten atomically; one section per account, other accounts preserved):
- `<data_dir>/folders.md`: human-readable (part of the plain-text archive)
- `<data_dir>/.tghoard/folders.json`: same data, machine-readable (web UI filter)
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tghoard.storage.atomic import atomic_write_text
from tghoard.storage.checkpoint_db import AccountRecord
from tghoard.storage.paths import single_line
from tghoard.telegram.entities import classify_entity_to_chat_info

# Labels are stored in folders.json/folders.md (archive content): kept in Portuguese.
RULE_LABELS = {
    "contacts": "contatos",
    "non_contacts": "não-contatos",
    "groups": "grupos",
    "broadcasts": "canais",
    "bots": "bots",
    "exclude_muted": "exceto silenciados",
    "exclude_read": "exceto lidos",
    "exclude_archived": "exceto arquivados",
}


@dataclass
class FolderInfo:
    account: str
    id: int
    title: str
    emoticon: str | None = None
    shared: bool = False  # shared folder ("chatlist" invite link)
    rules: list[str] = field(default_factory=list)
    pinned: list[str] = field(default_factory=list)
    included: list[str] = field(default_factory=list)
    excluded: list[str] = field(default_factory=list)
    members: list[str] = field(default_factory=list)  # effective chats in the folder

    @property
    def key(self) -> str:
        return f"{self.account}:{self.id}"


def _title_text(title: Any) -> str:
    """`title` is a plain str in old layers, `TextWithEntities` in new ones."""
    return single_line(getattr(title, "text", title) or "")


def _peer_key(peer: Any, by_raw: dict[tuple[str, int], str], self_key: str) -> str | None:
    name = type(peer).__name__
    if name == "InputPeerSelf":
        return self_key
    for attr, kind in (("channel_id", "channel"), ("chat_id", "chat"), ("user_id", "user")):
        raw = getattr(peer, attr, None)
        if raw is not None:
            return by_raw.get((kind, abs(int(raw))))
    return None


def _is_muted(dialog: Any, now: float) -> bool:
    settings = getattr(getattr(dialog, "dialog", None), "notify_settings", None)
    mute_until = getattr(settings, "mute_until", None)
    if mute_until is None:
        return False
    ts = mute_until.timestamp() if hasattr(mute_until, "timestamp") else float(mute_until)
    return ts > now


def compute_folders(
    filters: list[Any], dialogs: list[Any], account: AccountRecord
) -> list[FolderInfo]:
    """Resolve Telegram `DialogFilter`s against this account's dialogs."""
    self_key = f"saved_{account.account_id}"
    by_raw: dict[tuple[str, int], str] = {}
    rows: list[tuple[str, str, Any, Any]] = []  # (chat_key, kind, entity, dialog)
    for dialog in dialogs:
        ent = getattr(dialog, "entity", None)
        if ent is None:
            continue
        info = classify_entity_to_chat_info(
            ent, self_id=account.account_id, account_alias=account.alias
        )
        raw_kind = {"supergroup": "channel", "channel": "channel", "group": "chat"}.get(
            info.kind, "user"
        )
        by_raw[(raw_kind, abs(int(getattr(ent, "id", 0))))] = info.chat_key
        rows.append((info.chat_key, info.kind, ent, dialog))

    now = time.time()
    folders: list[FolderInfo] = []
    for f in filters:
        name = type(f).__name__
        if name == "DialogFilterDefault":  # the implicit "All chats"
            continue

        def resolve(peers: list[Any]) -> list[str]:
            keys = [_peer_key(p, by_raw, self_key) for p in peers or []]
            return [k for k in dict.fromkeys(keys) if k]

        folder = FolderInfo(
            account=account.alias,
            id=int(getattr(f, "id", 0)),
            title=_title_text(getattr(f, "title", "")) or f"Pasta {getattr(f, 'id', '?')}",
            emoticon=getattr(f, "emoticon", None) or None,
            shared=name == "DialogFilterChatlist",
            pinned=resolve(getattr(f, "pinned_peers", [])),
            included=resolve(getattr(f, "include_peers", [])),
            excluded=resolve(getattr(f, "exclude_peers", [])),
        )
        folder.rules = [label for flag, label in RULE_LABELS.items() if getattr(f, flag, False)]

        members = dict.fromkeys(folder.pinned + folder.included)
        wants = {flag: bool(getattr(f, flag, False)) for flag in RULE_LABELS}
        if any(wants[k] for k in ("contacts", "non_contacts", "groups", "broadcasts", "bots")):
            for chat_key, kind, ent, dialog in rows:
                matches = (
                    (wants["groups"] and kind in ("group", "supergroup"))
                    or (wants["broadcasts"] and kind == "channel")
                    or (wants["bots"] and kind == "bot")
                    or (wants["contacts"] and kind == "dm" and bool(getattr(ent, "contact", False)))
                    or (
                        wants["non_contacts"]
                        and kind == "dm"
                        and not getattr(ent, "contact", False)
                    )
                )
                if not matches:
                    continue
                if wants["exclude_archived"] and getattr(dialog, "folder_id", None) == 1:
                    continue
                if wants["exclude_muted"] and _is_muted(dialog, now):
                    continue
                if wants["exclude_read"] and not getattr(dialog, "unread_count", 0):
                    continue
                members.setdefault(chat_key)
        excluded = set(folder.excluded)
        folder.members = [k for k in members if k not in excluded]
        folders.append(folder)
    return folders


async def fetch_account_folders(
    client: Any, account: AccountRecord, dialogs: list[Any]
) -> list[FolderInfo]:
    from telethon.tl import functions

    res = await client(functions.messages.GetDialogFiltersRequest())
    filters = getattr(res, "filters", res)  # new layers wrap the list in messages.DialogFilters
    return compute_folders(list(filters or []), dialogs, account)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def folders_json_path(data_dir: Path) -> Path:
    return data_dir / ".tghoard" / "folders.json"


def load_folders(data_dir: Path) -> dict[str, Any]:
    """`{"accounts": {alias: {"updated_at": ..., "folders": [FolderInfo dict, ...]}}}`."""
    try:
        return dict(json.loads(folders_json_path(data_dir).read_text(encoding="utf-8")))
    except (OSError, ValueError):
        return {"accounts": {}}


def save_account_folders(
    data_dir: Path,
    account: AccountRecord,
    folders: list[FolderInfo],
    titles: dict[str, str],
) -> None:
    """Replace this account's folders in `folders.json` and re-render `folders.md`."""
    state = load_folders(data_dir)
    state.setdefault("accounts", {})[account.alias] = {
        "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "folders": [asdict(f) for f in folders],
    }
    known_titles = dict(state.get("titles", {}))
    known_titles.update(titles)
    state["titles"] = known_titles
    atomic_write_text(
        folders_json_path(data_dir), json.dumps(state, indent=1, ensure_ascii=False) + "\n"
    )
    atomic_write_text(data_dir / "folders.md", render_folders_md(state))


def render_folders_md(state: dict[str, Any]) -> str:
    """Render `folders.md` (archive content: its text is kept in Portuguese on purpose)."""
    titles: dict[str, str] = state.get("titles", {})
    lines = [
        "# Pastas do Telegram — tghoard",
        "",
        "Gerado automaticamente a cada `tghoard start`/`sync` (não edite: será sobrescrito).",
        "Pastas baseadas em regras listam os chats que as regras incluíam no momento do sync.",
        "",
    ]
    for alias, acc in sorted(state.get("accounts", {}).items()):
        folders = acc.get("folders", [])
        lines += [f"## Conta `{alias}`", "", f"_Atualizado em {acc.get('updated_at')}_", ""]
        if not folders:
            lines += ["_Nenhuma pasta criada nesta conta._", ""]
        for f in folders:
            icon = f"{f['emoticon']} " if f.get("emoticon") else "📁 "
            shared = " · compartilhada" if f.get("shared") else ""
            lines.append(f"### {icon}{single_line(f['title'])} (id {f['id']}{shared})")
            lines.append("")
            if f.get("rules"):
                lines.append(f"- **Regras**: {', '.join(f['rules'])}")
            lines.append(f"- **Chats**: {len(f.get('members', []))}")
            pinned = set(f.get("pinned", []))
            for key in f.get("members", []):
                pin = " 📌" if key in pinned else ""
                name = single_line(titles.get(key, key)).replace("**", "")
                lines.append(f"  - **{name}** [`{key}`]{pin}")
            if f.get("excluded"):
                excl = ", ".join(f"`{k}`" for k in f["excluded"])
                lines.append(f"- **Excluídos explicitamente**: {excl}")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


async def sync_folders_for_account(
    client: Any, account: AccountRecord, dialogs: list[Any], data_dir: Path
) -> list[FolderInfo]:
    """Fetch + save; returns the folders (empty list if the account has none)."""
    folders = await fetch_account_folders(client, account, dialogs)
    titles: dict[str, str] = {f"saved_{account.account_id}": f"Saved Messages ({account.alias})"}
    for dialog in dialogs:
        ent = getattr(dialog, "entity", None)
        if ent is not None:
            info = classify_entity_to_chat_info(
                ent, self_id=account.account_id, account_alias=account.alias
            )
            titles[info.chat_key] = info.title
    save_account_folders(data_dir, account, folders, titles)
    return folders
