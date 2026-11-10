# CLI and `.env` Variables Reference

## Environment Variables (`.env`)

`tghoard` automatically loads the `.env` file located in the current working directory:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `TG_API_ID` | *(required for network access)* | Numeric `api_id` obtained at <https://my.telegram.org> |
| `TG_API_HASH` | *(required for network access)* | `api_hash` obtained at <https://my.telegram.org> |
| `TGHOARD_DATA_DIR` | `./data` | Folder where everything (`chats.md`, `.tghoard/`, chat folders) is saved |
| `TGHOARD_MAX_JSON_KB` | `1024` | Target maximum size (in KB) per `messages/<folder>/YYYY-MM-ptNNN.json` file |
| `TGHOARD_HISTORY_WAIT` | `0` | **Minimum** pause (s) between pages of 100 messages (Telethon would use a fixed 1 s on large chats). With auto-throttling, the pause rises by itself when there are FloodWaits |
| `TGHOARD_DOWNLOAD_QUEUE` | `1024` | Downloads that can wait in the pool before holding back message reading (*backpressure*) |
| `TGHOARD_METADATA_EVERY_HOURS` | `24` | Each chat's metadata (info, members, stories, admin log, profile photos) is refreshed at most every N hours (`0` = every sync); messages and media always sync |
| `TGHOARD_AUTO_THROTTLE` | `true` | Per-account auto-throttling: FloodWaits increase the pause between pages / reduce parallelism; periods without FloodWaits return to full speed |
| `TGHOARD_WEB_PORT` | `8765` | Local web UI port (tries the next ones if it is busy) |
| `TGHOARD_MAX_MEDIA_MB` | `50` | Size limit per attachment in MB (`0` = no limit) |
| `TGHOARD_CONCURRENCY` | `5` | Download pool workers per account: maximum simultaneous media downloads (max `32`); the auto-throttle works below this ceiling |
| `TGHOARD_THROTTLE_CUT_RATIO` | `0.5` | Auto-throttle: on each download FloodWait, simultaneous downloads are multiplied by this ratio (geometric cut; `0.5` = halve) |
| `TGHOARD_THROTTLE_RECOVER_SECS` | `20` | Auto-throttle: every N seconds without a FloodWait, the page pause is halved / simultaneous downloads go up by 1 |
| `TGHOARD_PARALLEL_CHATS` | `4` | How many chats to sync simultaneously per account (`--parallel-chats` / `-j`) |
| `TGHOARD_SYNC_MODE` | `auto` | Session mode: `auto`, `takeout` or `standard` |
| `TGHOARD_AUDIT_WINDOW` | `200` | Window of recent messages audited if the `PTS` expires (`DifferenceTooLong`) |

---

## Main Commands

All commands accept `--data-dir <path>` (or `-d <path>`) to override `TGHOARD_DATA_DIR`.

**One writer at a time.** Commands that write to the archive (`start`, `sync`, `watch`, `account add`/`remove`, `migrate --apply`, `verify-media --fix`, `rebuild-checkpoint`, `rebuild-md`) take an exclusive lock on `<data_dir>/.tghoard/archive.lock`. If another one is already running on the same `data_dir`, the second exits immediately with code `3` and says which command (and pid) holds it. Read-only commands (`web`, `search`, `stats`, `folder-links`, `verify-media`, `migrate` without `--apply`) can run alongside a sync. The lock is released automatically when the process exits, even after a crash.

### 1. `tghoard start`
Authenticates an account (if no account is saved yet or if `--add-account` is given), discovers all dialogs and creates/updates `<data_dir>/chats.md`:

```bash
# First run (logs in if needed and generates ./data/chats.md)
tghoard start

# Add a second Telegram account sharing the same ./data
tghoard start --add-account --alias trabalho

# Use another output directory
tghoard start --data-dir /mnt/nas/telegram-backup
```

`chats.md` keeps its Portuguese tokens (`baixar` = download this chat, `prioridade` = priority, section headings such as `## Grupos e Supergrupos`) for compatibility with existing files.

### 2. `tghoard sync`
Reads `<data_dir>/chats.md` and syncs every chat checked with `[x] baixar`, in order of `prioridade` (highest first), downloading media according to the `[x] photos`, `[x] videos`, `[x] audio` and `[x] files` checkboxes.

**Interactive mode (terminal)**: also opens the local web UI (`http://127.0.0.1:8765/`) and the browser. From the web UI you can check/uncheck chats, change priorities (even during the sync: they apply to the chats that have not started yet), view per-chat statistics and follow progress bars. When there is nothing to do — no chat selected, or the queue is finished — the sync **waits for you to press Enter** (or click *Start sync* in the web UI) to run again; `q` + Enter, Ctrl+D or Ctrl+C exit.

**Non-interactive mode (cron, pipe, redirection)**: runs once and exits, without waiting for Enter; the web UI is started unless `--no-web` is passed (and the browser is not opened).

**Exit codes**: `0` everything synced · `1` some chats failed (listed in the summary; they resume on the next sync) · `2` invalid option (e.g. a malformed `--since`) or a pending required migration · `3` another tghoard process is writing to the archive · `130` interrupted with Ctrl+C (progress already checkpointed is kept).

| Option | Description |
|-------|-----------|
| `-a X` / `--account X` | Sync only this account (alias or id) |
| `-c X` / `--chat X` | Sync only this chat (`chat_key` or title) |
| `--since YYYY-MM-DD` | Do not fetch history older than this date (see below) |
| `--mode M` | Session mode: `auto`, `takeout` or `standard` (default `TGHOARD_SYNC_MODE`) |
| `--max-json-kb N` | Maximum size of each `YYYY-MM-ptNNN.json` part for this run |
| `--max-media-mb N` | Maximum size per attachment for this run (`0` = no limit) |
| `--concurrency N` | Download pool workers per account — including several media items from the **same** chat in parallel (default `TGHOARD_CONCURRENCY` / `5`) |
| `-j N` / `--parallel-chats N` | Simultaneous chats per account (default `TGHOARD_PARALLEL_CHATS` / `4`) |
| `--history-wait S` | Minimum pause between history pages (default `TGHOARD_HISTORY_WAIT` / `0`) |
| `--no-auto-throttle` | Turn off FloodWait-based auto-throttling (uses the fixed values) |
| `--refresh-metadata` | Refresh the metadata of all chats in this run (ignores `TGHOARD_METADATA_EVERY_HOURS`) |
| `--skip-members`, `--skip-stories`, `--skip-profile-photos` | Skip those metadata steps |
| `--chats-file PATH` | Use another `chats.md` |
| `--no-web` | Do not start the web UI (pure CLI) |
| `--web-port N` | Web UI port (default `TGHOARD_WEB_PORT` / `8765`) |
| `--no-browser` | Do not open the browser automatically |
| `-v` / `--verbose` | Detailed log in the terminal |

**`--since`.** Messages older than the date are not fetched, and the skipped history is **not** marked as synced: a later `tghoard sync` without `--since` goes back and fetches it. New messages are always fetched.

```bash
# Sync all accounts according to ./data/chats.md
tghoard sync

# Sync only one specific account or filter one specific chat
tghoard sync --account trabalho
tghoard sync --chat supergroup_9876543210

# Adjust the JSON split size or the media limit for this run
tghoard sync --max-json-kb 512 --max-media-mb 100

# Sync 8 chats simultaneously per account (or -j 1 for sequential)
tghoard sync -j 8

# Detailed log (each RPC with its duration, reused/deferred media, metadata steps)
tghoard sync -v
```

**Auto-throttling.** Each account has an AIMD controller fed by FloodWaits — including the ones Telethon silently "sleeps" through (< 60 s), captured from each client's log:

- FloodWait on a **file download** request (`GetFileRequest`, `GetCdnFileRequest`, …) → the account's simultaneous downloads are multiplied by `TGHOARD_THROTTLE_CUT_RATIO` (geometric progression; default `0.5`: 32 → 16 → 8 → 4 → 2 → 1; min 1). Parallel workers usually hit the same limit together, so a burst within 3 s counts as a single cut;
- FloodWait on **any other** request (history pages, `GetMessages`, metadata) → the pause between pages doubles (min 0.25 s, max 5 s); downloads are not touched;
- every `TGHOARD_THROTTLE_RECOVER_SECS` (default 20 s) without a FloodWait of the same kind → the pause is halved down to `TGHOARD_HISTORY_WAIT` / simultaneous downloads **go up by 1** (linear) up to `TGHOARD_CONCURRENCY`. The two recover independently.

Each adjustment shows up in the log (`🐢 FloodWait of 12s on GetFileRequest → pause 0.00s/page · simultaneous downloads 8/16 (--concurrency) · 3 FloodWait(s)`, `🐇 no FloodWait for 20s (downloads) → …`), in the web UI (the ⚙ line above the account's bar) and in the total at the end of the account.

**Web UI: two screens** (switched without reloading the page, via `#/dashboard` and `#/chats`):

- **Dashboard**: the run's chats and messages, downloaded media, download pool, auto-throttle, progress ring, **download speed line chart** (1/5/15/30 min windows, with average, peak and total; sampled once per second), largest chats in the archive, chats in progress, priority queue and log.
- **Chats**: the table for editing `chats.md` (All/Selected/Not selected/Queued tabs, search, section, folder, per-column sorting, bulk actions). Edits and periodic refreshes **do not re-sort the table** (the edited row neither jumps nor loses focus); when the order gets out of date, **↻ Re-sort** appears.

**Progress bars (web UI).** The terminal only shows log lines; the bars live on the Dashboard:

- one bar per account: completed chats (`37/503`), elapsed time and ETA;
- one bar per chat in progress: in channels/supergroups, ids covered over the missing total (with ETA); in DMs/Saved Messages, a message counter (total unknown in advance); while draining the queue, pending media;
- one bar per large download (≥ 1 MB): bytes, speed and ETA.

**Join date.** The *Joined* column (sortable) shows when you joined each channel/group (the oldest account; the tooltip lists each account and, in basic groups, who invited you).

**Telegram folders in the web UI.** The *All folders* filter shows only the chats of one of your Telegram folders (from `folders.md`); combined with *apply to visible → Check*, it checks download (`baixar`)/`photos`/… for an entire folder at once.

**Statistics in the web UI.** For each chat: the chat photo (downloaded on demand, cached in `.tghoard/avatars/`) and **downloaded / total on Telegram** for messages, photos, videos, audio and files. The totals come from `messages.getHistory` (count) and `messages.getSearchCounters`, are requested only for the visible rows and are cached for 6 h in `.tghoard/remote_counts.json` (`tghoard web` without a connection shows the last known value). Media folders only exist once something has been downloaded into them.

**Progress output.** Each line has a timestamp and the originating chat (`HH:MM:SS <chat> · ...`), since several chats run in parallel:

- Default: account steps (dialogs, PTS diff, Takeout/Standard session), start/end of each chat with a `[N/total]` counter and duration, the plan of missing ranges, a `💾 batch saved` every 100 messages (ids, days and running totals), each downloaded media item (`⬇ <path> (<size>) — msg #<id>`), media skipped because of the size limit, and retry/FloodWait warnings.
- `--verbose` / `-v`: adds each RPC call with its duration (`↳ ...`), each downloaded range, reused (`♻`) and deferred (`⏸`) media, metadata steps (members, stories, links) and errors previously silenced in those steps.

### 3. `tghoard account`
Direct management of the accounts authenticated in `<data_dir>/.tghoard/`:

```bash
tghoard account list
tghoard account add --alias pessoal
tghoard account remove trabalho
```

### 4. `tghoard watch`
Optional real-time mode (runs until `Ctrl+C`) to capture volatile events that the Telegram server does not keep offline (e.g. `typing` events, continuous `Online/Offline` transitions and messages deleted seconds after being sent):

```bash
tghoard watch
```

### 5. `tghoard search` and `tghoard stats`
Quick inspection of the plain-text archive straight from the terminal:

```bash
tghoard stats
tghoard search "benchmark"
tghoard search "confidencial" --deleted
tghoard search "https://" --links
```

### 6. `tghoard rebuild-checkpoint` and `tghoard rebuild-md`
Archive maintenance and portability tools:

```bash
# Rebuild data/.tghoard/checkpoint.db by scanning only the plain-text files in data/
tghoard rebuild-checkpoint

# Regenerate links/<folder>.md and metadata/<folder>/deleted_and_edits.md from the saved JSON files
tghoard rebuild-md
```

What `rebuild-checkpoint` recovers from the plain text alone: the message index (`message_locator`, per-account ids), the media index (`media_index`, with SHA-256), the **synced ranges of channels/supergroups built from the ids actually present** (gaps stay gaps: the next `sync` fetches them, and small holes left by deleted messages are re-checked cheaply, 100 ids per request) and the **media queue** (every media item whose JSON says `downloaded: false`). Not recovered: DM/Saved Messages/basic-group cursors (their history is walked again on the next `sync`, without duplicating anything) and the `pts` state (the next sync starts from the current state).

### 7. `tghoard web`
Opens only the web UI (edit `chats.md`, view statistics), without connecting to Telegram. Avatars only appear if they are already cached (downloaded during some `sync` with the web UI open).

```bash
tghoard web            # http://127.0.0.1:8765/
tghoard web --port 9000 --no-browser
```

### 8. `tghoard folder-links`
Generates the links of every chat in each Telegram folder, **offline** (from `.tghoard/folders.json`, `chats.md` and `checkpoint.db` — run `tghoard sync` first to fetch the folders):

| Chat | Link | Who can open it |
|------|------|-----------|
| Public (with @username) | `https://t.me/<username>` | anyone |
| Private channel/supergroup | `https://t.me/c/<id>/<most recent archived post>` | members only |
| DM without username | `tg://user?id=<id>` | Telegram app |
| Basic group / Saved Messages | — (listed without a link) | — |

```bash
tghoard folder-links                              # → <data_dir>/folder_links.md
tghoard folder-links -f txt --only-public -o -    # one URL per line, public only, to the screen
tghoard folder-links -f csv --folder trabalho     # → <data_dir>/folder_links.csv
tghoard folder-links --only-channels --account alice
```

Invite links (`t.me/+…`) for private chats can only be generated by administrators and are not part of this command.

### 9. `tghoard verify-media`
Checks, **offline**, that every media item the JSON files say was downloaded exists on disk with the recorded size (and SHA-256, with `--hash`), and that the ones not downloaded are in the download queue. Without `--fix`, it only reports (exit code 1 if there is a problem).

```bash
tghoard verify-media                    # report
tghoard verify-media --hash             # also check SHA-256 (slow)
tghoard verify-media --fix              # queue missing/corrupt files (run with the sync stopped)
tghoard verify-media --chat channel_123 --show 100
```

With `--fix`: missing/corrupt files get their JSON flagged (`downloaded: false`, `skip_reason: missing_on_disk`/`corrupt_on_disk`), are removed from the deduplication index and enter the queue — the next `sync` downloads them again (for the checked categories). Media missing from the queue is re-queued as well.

### 10. `tghoard migrate`
Converts the archive to the current layout using the versioned migrations in `src/tghoard/migrations/`. **Without `--apply` nothing is changed**: it shows the pending migrations and the detailed plan (old folder → new one, number of JSON files/media, size).

```bash
tghoard migrate          # preview
tghoard migrate --apply  # run (resumable if interrupted)
```

`0001_category_first_layout` moves each `<chat_key>__<slug>/` to the per-category layout: media is **moved** (rename on the same disk — no copy, hardlinks preserved; partial `.dl_*` downloads are discarded), the daily JSON files are grouped into `YYYY-MM-ptNNN.json` (with the media paths rewritten), and `checkpoint.db` is updated in place after a backup in `.tghoard/backups/` (cursors and synced ranges preserved). Old folders are only removed if they end up empty.

It is safe to interrupt at any point (Ctrl+C, crash, unplugged drive) and run `migrate --apply` again: every step only moves files or derives state from what is already on disk, the database is updated in one transaction **before** the old daily files are deleted, and name-clash renames are journaled in `.tghoard/migration-0001.json`. Leftover temporary files from older versions (`.dl_*`, hidden `*.tmp`) are discarded. Files the migration doesn't recognize are **never deleted**: they are listed by name (with the reason) and left in the old folder for you to move or remove, and the migration is still recorded as applied. While a **mandatory** migration is pending, `sync`, `watch`, `search`, `stats`, `web`, `folder-links`, `verify-media` and `rebuild-*` refuse to run (exit code 2).

`0003_merge_stray_daily_parts` (required, but only needed if such files exist): older versions of `tghoard watch` and of the Admin Log recovery wrote live/recovered messages to `messages/<folder>/YYYY-MM-DD-ptNNN.json`, a name no reader looks at. It merges them into the monthly parts by `canonical_key` (nothing duplicated, deletion flags and revisions kept), points `checkpoint.db` at the month and only then deletes each daily file. A file that can't be read is renamed to `*.unmerged`, never deleted.

`0002_remove_empty_folders` (optional — does not block any command) removes empty chat folders created by earlier versions (e.g. `audio/<folder>/` without any audio). Only truly empty directories are removed (`rmdir`); no file is deleted.

---

## Best Practices against `FloodWait` and Bans

- Leave `TGHOARD_SYNC_MODE=auto` (the default): `tghoard` tries to start an official **Telegram Takeout** session (`account.initTakeoutSession`), created by Telegram specifically for exporting personal data with relaxed rate limits.
- On freshly authenticated sessions, Telegram may require confirmation in the mobile app or apply a 24h security wait for Takeout (`TakeoutInitDelayError`). In that case, `auto` mode automatically continues in paced standard mode, honoring `FloodWaitError` with a countdown and *jitter*.
- `TGHOARD_CONCURRENCY` (default `5`) and `TGHOARD_PARALLEL_CHATS` (`2`–`6`): higher values speed up downloads but increase the chance of `FloodWait`; auto-throttling reduces parallelism by itself when that happens, so you can safely experiment with higher values. Use `-j 1` for the old sequential behavior.
- Transient Telegram errors (`RpcMcgetFailError`, `RpcCallFailError` and other `ServerError` 500s, timeouts) are retried per chat with progressive backoff (~15s → 30s → 60s → 120s → 240s), resuming from the last checkpointed batch. If the chat keeps failing, it is skipped, listed in the final summary as "Failed chats" and resumed automatically on the next `tghoard sync` — the other chats carry on normally.
