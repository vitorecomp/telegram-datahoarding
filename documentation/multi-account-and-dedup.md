# Multiple Accounts and Cross-Account Deduplication

`tghoard` was designed from the ground up to let **two or more Telegram accounts** save their data into the same `--data-dir` directory (e.g. `./data`) without duplicating network work or disk space.

## 1. Canonical Folder Identity (`chat_key`)

| Dialog Type | `chat_key` Format | Behavior between Account A (`111`) and Account B (`222`) |
| :--- | :--- | :--- |
| **Channel (`Channel` broadcast)** | `channel_<channel_id>` | **Same folder and same line in `chats.md`**. `msg_id` is global within the channel. |
| **Supergroup / Forum (`Megagroup`)** | `supergroup_<channel_id>` | **Same folder and same line in `chats.md`**. `msg_id` is global within the supergroup. |
| **Legacy Basic Group (`Chat`)** | `group_<chat_id>` | **Same folder and same line in `chats.md`**. `msg_id` is per account; deduplicates by deterministic fingerprint. |
| **1:1 Private Chat (`User` / `Bot`)** | `dm_<min_id>_<max_id>` | Conversations with third parties stay isolated (`dm_111_333` vs. `dm_222_333`), but a direct conversation between Account A and Account B (`dm_111_222`) shares the same folder without duplication. |
| **Saved Messages (`Self`)** | `saved_<self_id>` | Isolated per account (`saved_111` and `saved_222`). |

## 2. How We Avoid Re-Downloading Messages

### Channels and Supergroups (`chat_synced_ranges`)
In the MTProto protocol, channels and supergroups have a single sequential message numbering (`1, 2, 3, ...`) shared among all participants:
1. When **Account A** syncs the range `[1, 5400]`, `checkpoint.db` records `(chat_key='supergroup_9876543210', start_id=1, end_id=5400)` in `chat_synced_ranges`.
2. When **Account B** goes through the same group afterwards, it queries the server's current highest `msg_id` (e.g. `5405`) and computes `missing_ranges([1, 5405], synced=[[1, 5400]]) -> [[5401, 5405]]`.
3. Account B downloads **only** the 5 new messages (`5401..5405`) — or any older ranges if Account B is an older member and can see history earlier than Account A's.

### Legacy Basic Groups and Direct Conversations between Local Accounts
In legacy basic groups (`PeerChat`) and DMs between two local accounts, each account has its own `msg_id` counter in its personal inbox. In these cases:
- Each message gets a `canonical_key` based on a deterministic fingerprint:
  `fp:<sha256(sender_id | timestamp_iso | media:<tg_id>)>` for messages with media, otherwise `fp:<sha256(sender_id | timestamp_iso | grp:<grouped_id> | txt:<first 128 chars>)>`.
  Messages with neither text nor media (service events such as "X added Y", polls, dice, contacts, locations) also mix in a `|tag:<…>` built from stable fields every account sees the same way (action type, user ids, poll/call id, dice value…), so two such events from the same sender in the same second are no longer merged into one. Keys of ordinary messages never changed. Identical texts from the same sender in the same second still share a key — there is no account-independent way to tell them apart.
- Re-processing a message an account already archived resolves its key through `account_message_map` (account id + message id) first, so keys never drift for messages already in the archive.
- If Account B finds a message whose `canonical_key` already exists in `message_locator`, it just adds its ID to `"account_msg_ids"` and `"seen_by_accounts"` without duplicating the message or re-downloading the attachments.

## 3. How We Avoid Re-Downloading Photos, Videos, Audio and Files

In MTProto, the `photo.id` and `document.id` identifiers are **globally identical** across different Telegram accounts (only the `access_hash` changes per session):

1. **Same chat**: before calling `download_media`, `media_manager` looks up `media_index` by `(tg_type, tg_id)`. If the file is already in that chat's `photos/`, `videos/`, `audio/` or `files/` folder, it returns the existing path immediately (**0 bytes of network traffic**).
2. **Forwarded between different chats**: if the `(tg_type, tg_id)` was already downloaded into **another** chat's folder inside `data/`, `media_manager` creates a **local hardlink (`os.link`)** pointing to the same inode (with fallback to a local copy — written to a temporary file and renamed into place — if it is on another filesystem). An indexed file is only reused if it still has exactly the indexed size; otherwise it is downloaded again. That way, every chat folder stays complete and self-contained, without spending Telegram bandwidth or duplicating disk space.
