"""Links to every chat in each Telegram folder (`tghoard folder-links`), fully offline.

Inputs (all produced by `tghoard start`/`sync`):
- `.tghoard/folders.json`: folders per account and their member chat_keys
- `chats.md`: titles and public @usernames
- `.tghoard/checkpoint.db` (optional): accounts (to tell DM peers apart) and the newest
  archived message id of private channels (`t.me/c/<id>/<post>` needs a post id)

Link rules (Telegram only allows these without admin rights):
- public channel/group/user (@username)  -> https://t.me/<username>       (anyone)
- private channel/supergroup             -> https://t.me/c/<id>/<post>    (members only)
- DM without username                    -> tg://user?id=<id>             (Telegram app)
- basic group / Saved Messages           -> no link
"""

from __future__ import annotations

import csv
import io
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from tghoard.storage.chats_manifest import VALID_USERNAME_RE, ChatSelection, load_chats_manifest
from tghoard.storage.paths import single_line
from tghoard.sync.folders_sync import load_folders

Visibility = Literal["public", "members", "app", "none"]
VISIBILITY_LABEL: dict[str, str] = {
    "public": "public",
    "members": "private (members only)",
    "app": "opens in the app",
    "none": "no link",
}


@dataclass
class ChatLink:
    chat_key: str
    title: str
    url: str | None
    visibility: Visibility
    note: str = ""


@dataclass
class FolderLinks:
    account: str
    folder_id: int
    title: str
    emoticon: str | None
    links: list[ChatLink]


def _db_facts(db_path: Path) -> tuple[set[int], dict[str, int]]:
    """(account ids, {chat_key: newest archived message id}) — empty if no database."""
    if not db_path.exists():
        return set(), {}
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        accounts = {int(r[0]) for r in conn.execute("SELECT account_id FROM accounts")}
        newest = {
            str(k): int(v)
            for k, v in conn.execute(
                "SELECT chat_key, MAX(end_id) FROM chat_synced_ranges GROUP BY chat_key"
            )
        }
        return accounts, newest
    except sqlite3.Error:
        return set(), {}
    finally:
        conn.close()


def link_for_chat(
    chat_key: str,
    entry: ChatSelection | None,
    *,
    account_ids: set[int],
    newest_post: dict[str, int],
) -> ChatLink:
    title = single_line(entry.title) if entry is not None else chat_key
    username = (entry.username or "").lstrip("@") if entry is not None else ""
    kind, _, rest = chat_key.partition("_")

    if username and VALID_USERNAME_RE.fullmatch(username):
        return ChatLink(chat_key, title, f"https://t.me/{username}", "public")
    if kind in ("channel", "supergroup"):
        post = newest_post.get(chat_key, 1)
        return ChatLink(
            chat_key,
            title,
            f"https://t.me/c/{rest}/{post}",
            "members",
            "opens at the newest archived post" if chat_key in newest_post else "",
        )
    if kind == "dm":
        ids = [int(x) for x in rest.split("_") if x.isdigit()]
        peers = [i for i in ids if i not in account_ids] or ids[-1:]
        return ChatLink(chat_key, title, f"tg://user?id={peers[0]}", "app")
    if kind == "group":
        return ChatLink(chat_key, title, None, "none", "private basic group")
    return ChatLink(chat_key, title, None, "none", "Saved Messages")


def build_folder_links(
    data_dir: Path,
    *,
    manifest_path: Path | None = None,
    only_channels: bool = False,
    only_public: bool = False,
) -> list[FolderLinks]:
    """Every folder of every account with one link per chat (in folder order)."""
    state = load_folders(data_dir)
    manifest = load_chats_manifest(manifest_path or data_dir / "chats.md")
    titles: dict[str, str] = state.get("titles", {})
    account_ids, newest = _db_facts(data_dir / ".tghoard" / "checkpoint.db")

    result: list[FolderLinks] = []
    for alias, acc in sorted(state.get("accounts", {}).items()):
        for f in acc.get("folders", []):
            links: list[ChatLink] = []
            for key in f.get("members", []):
                if only_channels and not key.startswith(("channel_", "supergroup_")):
                    continue
                link = link_for_chat(
                    key, manifest.get(key), account_ids=account_ids, newest_post=newest
                )
                if link.title == key and key in titles:
                    link.title = single_line(titles[key])
                if only_public and link.visibility != "public":
                    continue
                links.append(link)
            result.append(
                FolderLinks(alias, int(f["id"]), single_line(f["title"]), f.get("emoticon"), links)
            )
    return result


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def render_markdown(folders: list[FolderLinks]) -> str:
    stamp = datetime.now(UTC).isoformat(timespec="seconds")
    lines = [
        "# Links per folder — tghoard",
        "",
        f"_Generated at {stamp} by `tghoard folder-links`. "
        "`t.me/c/...` only opens for members; public links can be shared._",
        "",
    ]
    current_account = None
    for f in folders:
        if f.account != current_account:
            current_account = f.account
            lines += [f"## Account `{f.account}`", ""]
        icon = f"{f.emoticon} " if f.emoticon else "📁 "
        lines += [f"### {icon}{f.title} ({len(f.links)})", ""]
        if not f.links:
            lines.append("_No chats match the chosen filters._")
        for link in f.links:
            label = VISIBILITY_LABEL[link.visibility]
            note = f"; {link.note}" if link.note else ""
            title = link.title.replace("[", "(").replace("]", ")")
            if link.url:
                lines.append(f"- [{title}]({link.url}) — {label}{note}")
            else:
                lines.append(f"- {title} — {label}{note}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_txt(folders: list[FolderLinks]) -> str:
    """One URL per line, grouped by `# account / folder` headers (handy to paste/share)."""
    lines: list[str] = []
    for f in folders:
        urls = [link.url for link in f.links if link.url]
        if not urls:
            continue
        lines += [f"# {f.account} / {f.title}", *urls, ""]
    return "\n".join(lines).rstrip() + "\n"


def render_csv(folders: list[FolderLinks]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["account", "folder", "title", "chat_key", "url", "visibility"])
    for f in folders:
        for link in f.links:
            writer.writerow(
                [f.account, f.title, link.title, link.chat_key, link.url or "", link.visibility]
            )
    return buf.getvalue()


RENDERERS = {"md": render_markdown, "txt": render_txt, "csv": render_csv}
