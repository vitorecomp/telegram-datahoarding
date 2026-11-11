# AGENTS.md — Guidelines for the `telegram-datahoarding` Project (`tghoard`)

This document defines the architecture, mandatory invariants and workflows for AI agents that maintain or evolve this repository.

## 1. Overview

`tghoard` is a **100% CLI-driven** application (Python 3.13 + Telethon + Typer + Rich) for personal archiving of one or multiple Telegram accounts.

- **CLI first**: every feature exists in the CLI (`tghoard`) + editing `<data_dir>/chats.md`. The **local web UI** (`tghoard sync` opens it alongside in an interactive terminal; `tghoard web` on its own) is just a shortcut for editing `chats.md` and following progress — nothing may depend on it (`--no-web`, cron and pipes work the same).
- **Two-Layer Architecture**:
  1. **User Archive in Plain Text, organized by category**: `messages/<folder>/YYYY-MM-ptNNN.json`, `{photos,videos,audio,files}/<folder>/`, `links/<folder>.md` and `metadata/<folder>/*.md`, where `<folder>` = `kebab-title-<id>`.
  2. **Internal State in SQLite WAL (`<data_dir>/.tghoard/checkpoint.db` + `sessions/*.session`)**: exclusively for ACID pagination checkpoints, ranges synced across accounts, each account's join date in each chat (`chat_membership`), `O(log N)` message lookup (`msg_id -> (day_str, part_num)`), the `pending_media` queue and global media deduplication (`media_index`).

## 2. Mandatory Invariants (Never Break)

1. **Output Parameter (`data_dir`)**:
   - All generated artifacts (`chats.md`, `.tghoard/` and chat folders) must be saved inside `data_dir` (configured via `TGHOARD_DATA_DIR` in `.env` or `--data-dir` / `-d` on the CLI; default value: `./data`).
2. **`start` → `chats.md` → `sync` Flow**:
   - `tghoard start` creates or **non-destructively** merges `<data_dir>/chats.md`.
   - Each chat must have the 5 checkboxes + priority: `[x] baixar | [ ] photos | [ ] videos | [ ] audio | [ ] files | prioridade: 0` (`baixar` = download this chat, checked by default; the other 4 unchecked; `prioridade` = priority, default `0`, higher syncs first). These Portuguese tokens (and the section headings such as `## Grupos e Supergrupos`) are part of the file format and must not be renamed.
   - Re-running `tghoard start` must **never** overwrite `[x]`/`[ ]` selections or the `prioridade` already edited by the user (by hand or through the web UI) for existing chats.
   - `sync` re-reads `chats.md` each time a chat starts: priority/checkbox changes made during a sync apply to the chats that have not started yet.
3. **Uniform Monthly `-ptNNN.json` Naming**:
   - Every file in `messages/<folder>/` **must** follow `YYYY-MM-ptNNN.json` (e.g. `2026-11-pt000.json`), **including small months that did not need to be split** (`-pt000.json`).
   - When the serialized size of a month exceeds `max_json_kb` (default `1024 KB`), always split between messages (`-pt000.json`, `-pt001.json`, ...), keeping each part an independent, valid JSON document. Media file names keep the full date (`2026-11-09_<msg_id>_...`).
4. **Mandatory Per-Category Layout** (`<folder>` = `kebab-title-<id>`, e.g. `python-brasil-1234567890`; DMs `joao-<lo>-<hi>`; collisions get a `-2` suffix):
   - `messages/<folder>/` (`YYYY-MM-ptNNN.json`)
   - `photos/<folder>/` (chat photos, static non-sticker images, story/profile photos)
   - `videos/<folder>/` (videos, MP4 GIFs, *video notes* ("round videos"), story/profile videos)
   - `audio/<folder>/` (*voice notes* and audio/music files)
   - `files/<folder>/` (any attachment that is not a `photo`, `video` or `audio`: PDFs, ZIPs, docs, stickers, code, etc.)
   - `links/<folder>.md`
   - `folders.md` at the root (Telegram folders per account; generated on every `start`/`sync`)
   - `metadata/<folder>/` (`info.md`, `members.md`, `topics.md`, `stories.md`, `deleted_and_edits.md`, `profile_history.md`, `presence.md`, `events_and_admin_log.md`)
   - **Plain-text identity**: the first line of `metadata/<folder>/info.md` contains `` (`<chat_key>`) ``; folders are resolved by it (never by name), so renamed chats keep their folder and `checkpoint.db` can be rebuilt without internal state.
   - `media.path` in the JSON files is relative to `data_dir` (`photos/<folder>/<file>`).
   - Folders are created **only when a file is written to them** (never empty media folders).
   - Layout changes require a versioned migration in `src/tghoard/migrations/` (`tghoard migrate` shows the plan; `--apply` runs it). Commands that touch the archive refuse to run while migrations are pending.
5. **Historical Immutability**:
   - Remote deletion events (`UpdateDeleteMessages`) **never** delete local messages or files; they only set `"is_deleted": true` in the JSON of the corresponding part and record it in `metadata/deleted_and_edits.md`.
   - Edits (`UpdateEditMessage`) **never** discard the previous text; they push the prior version into `"revisions": [...]`.
6. **Multi-Account Deduplication**:
   - Two accounts in the same channel (`channel_<id>`), supergroup (`supergroup_<id>`) or basic group (`group_<id>`) share the **same folder** and the **same entry** in `chats.md`.
   - Messages already covered in `chat_synced_ranges` and media already registered in `media_index` (`(tg_type, tg_id)`) must not be downloaded from the network again (use an `os.link` hardlink with fallback to a local copy when the same media appears in different folders).
7. **Atomic Writes and Security**:
   - Always write `.json` and `.md` files via a temporary file in the same directory + `os.replace`.
   - Never commit `.env`, `/data/`, `*.session*` or `*.db*` to Git. The `.tghoard/sessions/` directory must have `0700` permissions and `.session` files `0600` permissions.

## 3. Code Map

- `src/tghoard/config.py`: Loading of `.env` (`pydantic-settings`) and global parameters.
- `src/tghoard/storage/`:
  - `atomic.py`: Atomic plain-text writes and range helpers.
  - `paths.py`: Canonical identity (`chat_key`), `ChatPaths` (per-category layout), `kebab-title-<id>` folders and `YYYY-MM-ptNNN.json` paths.
  - `chats_manifest.py`: Tolerant parser, generator/merger of `<data_dir>/chats.md`, `update_chat_selections` (web UI edits) and `ManifestSource` (re-reading during the sync).
  - `chat_stats.py`: Per-chat statistics (messages, media per category, links, sizes), space still needed per category (`estimate_to_download`) and media over the size limit (`list_oversized_media`) — used by the web UI and `tghoard stats`.
  - `checkpoint_db.py`: SQLite WAL (`checkpoint.db`), cursors, ranges, `message_locator`, `pending_media` and `media_index`.
  - `chat_writer.py`: Partitioned reading/writing of `messages/<folder>/YYYY-MM-ptNNN.json`, revisions and soft-delete.
  - `markdown_writer.py`: Generation of `links/<folder>.md` and `metadata/<folder>/*.md`.
  - `media_manager.py`: Deterministic classifier (`photos`/`videos`/`audio`/`files`), hardlinks and download (`needs_network` decides inline vs. pool).
  - `process_lock.py`: Single-writer `flock` on `.tghoard/archive.lock` (every command that writes to the archive takes it; a second writer exits with code 3).
- `src/tghoard/telegram/`:
  - `accounts.py`: Authentication and multi-account management (`SQLiteSession`).
  - `entities.py`: Normalization of Telethon entities (canonical `chat_key`, title, section, join date) and messages (`serialize_message_to_dict`) into plain dicts.
  - `takeout.py`: Takeout session (`InitTakeoutSessionRequest`) with automatic fallback.
  - `throttler.py`: `RateLimiter` (fixed API slots, *jitter*, automatic waiting on `FloodWaitError`) and `retry_transient` (backoff on 500s/timeouts); Telethon's spurious `CancelledError` on a dropped connection is treated as a transient `ConnectionError`.
  - `flood_control.py`: Per-account auto-throttle (`FloodController`): history FloodWaits raise the page pause, download FloodWaits (`GetFile*`) cut pool parallelism geometrically, quiet periods recover linearly; `DynamicSlots` (FIFO semaphore with a runtime limit).
- `src/tghoard/sync/`:
  - `discovery.py`: Implementation of `tghoard start`.
  - `pts_diff.py`: Offline replay of `getDifference` / `getChannelDifference`.
  - `media_pool.py`: Per-account download pool (fixed workers + bounded channel, per-chat cancellation).
  - `messages_sync.py`: Incremental sync by missing ranges and draining of `pending_media`.
  - `metadata_sync.py`: Sync of profiles, members, topics, stories, Admin Log and presence.
  - `watcher.py`: Optional real-time listener (`tghoard watch`).
  - `rebuilder.py`: Rebuilding of `checkpoint.db` and Markdown from the plain text.
  - `folders_sync.py`: Telegram chat folders (`messages.getDialogFilters`) → `<data_dir>/folders.md` + `.tghoard/folders.json` (web UI filter).
  - `orchestrator.py`: Multi-account orchestration of `tghoard sync` (priority queue, re-read from `chats.md`).
  - `live_state.py`: Live sync state (thread-safe) consumed by the web UI + `wait_for_user` (Enter or the web UI button).
  - `reporter.py`: Progress output with timestamp/chat prefix (`info` always; `debug` with `--verbose`).
- `src/tghoard/web/`: Optional local web UI (`server.py` stdlib HTTP on `127.0.0.1` only, `telegram_bridge.py` chat photos and Telegram totals with caching, `static/index.html`).
- `src/tghoard/utils/`: Offline utilities over the archive (`folder_links.py` → `tghoard folder-links`, `verify_media.py` → `tghoard verify-media`).
- `src/tghoard/migrations/`: Versioned archive migrations (`m0001_category_first_layout.py`, ...), applied by `tghoard migrate --apply`.
- `src/tghoard/cli.py`: Complete definition of the Typer subcommands.

## 4. Detailed Documentation

Always consult and keep in sync the documents in [`documentation/`](./documentation/):
- `documentation/architecture.md`
- `documentation/data-formats.md`
- `documentation/multi-account-and-dedup.md`
- `documentation/cli-reference.md`

## 5. Development and Verification Commands

```bash
# Run the full test suite
.venv/bin/pytest -v

# Run the linter and formatting check
.venv/bin/ruff check src tests
```
