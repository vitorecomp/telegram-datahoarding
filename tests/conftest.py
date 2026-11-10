"""Shared test fixtures and realistic fake Telethon primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def _isolated_env(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch):
    """Never read the developer's real `.env` / TGHOARD_* settings during tests."""
    import os

    for key in list(os.environ):
        if key.startswith(("TGHOARD_", "TG_API_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path_factory.mktemp("cwd"))


@dataclass
class FakeUser:
    id: int
    first_name: str = "User"
    last_name: str = ""
    username: str | None = None
    phone: str | None = None
    bot: bool = False
    about: str | None = None
    status: Any = None
    usernames: list[Any] = field(default_factory=list)
    participant: Any = None


@dataclass
class FakeChat:
    """Legacy basic group (`PeerChat`)."""

    id: int
    title: str
    participants_count: int = 5


@dataclass
class FakeChannel:
    """Supergroup (`megagroup=True`) or Broadcast Channel (`broadcast=True`)."""

    id: int
    title: str
    username: str | None = None
    megagroup: bool = False
    broadcast: bool = False
    forum: bool = False
    pts: int = 10
    date: datetime | None = None  # Telegram: when *you* joined (if a member)
    left: bool = False
    participants_count: int = 42
    admin_rights: Any = None
    creator: bool = False


@dataclass
class FakeDialog:
    entity: Any


@dataclass
class FakePhotoSize:
    size: int = 1024


@dataclass
class FakePhoto:
    id: int
    sizes: list[FakePhotoSize] = field(default_factory=lambda: [FakePhotoSize(2048)])
    date: datetime = field(default_factory=lambda: datetime(2026, 11, 9, 12, 0, tzinfo=UTC))


@dataclass
class FakeMediaPhoto:
    photo: FakePhoto


class DocumentAttributeFilename:
    def __init__(self, file_name: str) -> None:
        self.file_name = file_name


class DocumentAttributeVideo:
    def __init__(self, round_message: bool = False) -> None:
        self.round_message = round_message


class DocumentAttributeAudio:
    def __init__(self, voice: bool = False) -> None:
        self.voice = voice


class DocumentAttributeSticker:
    pass


@dataclass
class FakeDocument:
    id: int
    mime_type: str
    size: int = 4096
    attributes: list[Any] = field(default_factory=list)


@dataclass
class FakeMediaDocument:
    document: FakeDocument


@dataclass
class FakeEntityTextUrl:
    offset: int
    length: int
    url: str


@dataclass
class FakeButton:
    url: str


@dataclass
class FakeRow:
    buttons: list[FakeButton]


@dataclass
class FakeMarkup:
    rows: list[FakeRow]


@dataclass
class FakeMessage:
    id: int
    date: datetime
    message: str = ""
    sender_id: int | None = None
    sender: Any = None
    media: Any = None
    entities: list[Any] = field(default_factory=list)
    reply_markup: Any = None
    reply_to_msg_id: int | None = None
    edit_date: datetime | None = None
    grouped_id: int | None = None
    views: int | None = None
    forwards: int | None = None
    noforwards: bool = False


class FakeTelegramClient:
    """In-memory mock of `TelegramClient` tracking RPC & media download counts."""

    def __init__(
        self,
        *,
        me: FakeUser,
        dialogs: list[Any],
        messages_by_entity_id: dict[int, list[FakeMessage]],
        media_payloads: dict[int, bytes] | None = None,
        rpc_handler: Any = None,
        fail_after_iter_count: int | None = None,
        fail_exc_factory: Any = None,
        fail_times: int | None = None,
        fail_entity_ids: set[int] | None = None,
        download_fail_ids: set[int] | None = None,
        download_empty_ids: set[int] | None = None,
    ) -> None:
        self.me = me
        self._dialogs = [FakeDialog(e) for e in dialogs]
        self._messages = messages_by_entity_id
        self._media_payloads = media_payloads or {}
        self._rpc_handler = rpc_handler
        self.fail_after_iter_count = fail_after_iter_count
        # Optional error injection: exception factory, max number of raises (None = always),
        # and restriction to specific entity ids (None = all).
        self.fail_exc_factory = fail_exc_factory or (
            lambda: RuntimeError("Simulated mid-sync interruption")
        )
        self.fail_times = fail_times
        self.fail_entity_ids = fail_entity_ids
        self.failures_raised = 0
        # Media ids whose download raises / produces an empty file (simulated failures)
        self.download_fail_ids = set(download_fail_ids or ())
        self.download_empty_ids = set(download_empty_ids or ())

        self.download_calls: list[int] = []
        self.progress_callbacks: list[int] = []
        self.wait_times: list[float | None] = []  # `wait_time` passed to iter_messages
        self.iter_messages_calls: int = 0

    async def get_me(self) -> FakeUser:
        return self.me

    async def disconnect(self) -> None:
        return None

    async def iter_dialogs(self):
        for dlg in self._dialogs:
            yield dlg

    async def get_messages(
        self,
        entity: Any,
        limit: int | None = None,
        ids: list[int] | None = None,
        max_id: int = 0,
        min_id: int = 0,
    ):
        ent_id = int(entity.id)
        msgs = list(self._messages.get(ent_id, []))
        msgs.sort(key=lambda m: m.id, reverse=True)
        if ids is not None:
            by_id = {m.id: m for m in msgs}
            return [by_id.get(i) for i in ids]
        # Telethon: newest first; max_id/min_id are exclusive bounds
        msgs = [m for m in msgs if (max_id <= 0 or m.id < max_id) and m.id > min_id]
        if limit is not None:
            return msgs[:limit]
        return msgs

    async def iter_messages(
        self,
        entity: Any,
        min_id: int = 0,
        max_id: int = 0,
        offset_id: int = 0,
        reverse: bool = False,
        wait_time: float | None = None,
        offset_date: Any = None,
    ):
        self.iter_messages_calls += 1
        self.wait_times.append(wait_time)
        ent_id = int(entity.id)
        msgs = [
            m
            for m in self._messages.get(ent_id, [])
            if m.id > min_id
            and (max_id <= 0 or m.id < max_id)
            and (offset_id <= 0 or m.id < offset_id)
            # Telethon: with reverse=True, offset_date returns messages AFTER the date
            and (offset_date is None or (m.date > offset_date if reverse else m.date < offset_date))
        ]
        msgs.sort(key=lambda m: m.id, reverse=not reverse)
        emitted = 0
        for m in msgs:
            emitted += 1
            if (
                self.fail_after_iter_count is not None
                and emitted > self.fail_after_iter_count
                and (self.fail_entity_ids is None or ent_id in self.fail_entity_ids)
                and (self.fail_times is None or self.failures_raised < self.fail_times)
            ):
                self.failures_raised += 1
                raise self.fail_exc_factory()
            yield m

    async def download_media(
        self, msg_or_media: Any, file: str, progress_callback: Any = None
    ) -> str:
        media = getattr(msg_or_media, "media", msg_or_media)
        tg_id = 0
        if getattr(media, "photo", None) is not None:
            tg_id = int(media.photo.id)
        elif getattr(media, "document", None) is not None:
            tg_id = int(media.document.id)
        elif getattr(media, "id", None) is not None:
            tg_id = int(media.id)

        self.download_calls.append(tg_id)
        if tg_id in self.download_fail_ids:
            raise RuntimeError(f"simulated download failure for media {tg_id}")
        if tg_id in self.download_empty_ids:
            return None
        payload = self._media_payloads.get(tg_id, f"binary-content-{tg_id}".encode())
        declared = getattr(getattr(media, "document", None), "size", None)
        if tg_id not in self._media_payloads and isinstance(declared, int) and declared > 0:
            # Like Telegram: a document downloads exactly its declared `size` (MediaManager
            # rejects a document whose downloaded size differs, as a truncated download)
            payload = (payload * (declared // len(payload) + 1))[:declared]
        if progress_callback is not None:
            size = getattr(getattr(media, "document", None), "size", None) or len(payload)
            self.progress_callbacks.append(tg_id)
            for done in (size // 2, size):
                progress_callback(done, size)
        target = Path(file)
        target.write_bytes(payload)
        return str(target)

    async def iter_participants(self, entity: Any, limit: int = 500):
        yield FakeUser(id=self.me.id, first_name=self.me.first_name, username=self.me.username)

    async def iter_profile_photos(self, entity: Any, limit: int = 25):
        if False:
            yield None

    async def __call__(self, request: Any) -> Any:
        if self._rpc_handler is not None:
            return await self._rpc_handler(request)
        raise NotImplementedError(type(request).__name__)


def chat_paths_for(data_dir: Path, chat_key: str):
    """Resolve a chat's `ChatPaths` by identity (cold index, like a fresh process)."""
    from tghoard.storage.paths import ChatPaths, index_chat_folders

    folder = index_chat_folders(data_dir, refresh=True)[chat_key]
    return ChatPaths(data_dir=data_dir, folder=folder)
