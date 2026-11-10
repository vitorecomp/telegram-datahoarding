"""Configuration loading via .env and CLI overrides."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SyncMode = Literal["auto", "takeout", "standard"]
MediaCategory = Literal["photos", "videos", "audio", "files"]
ALL_MEDIA_CATEGORIES: tuple[MediaCategory, ...] = ("photos", "videos", "audio", "files")


class AppConfig(BaseSettings):
    """Runtime settings loaded from .env and environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    tg_api_id: int | None = Field(default=None, validation_alias="TG_API_ID")
    tg_api_hash: str | None = Field(default=None, validation_alias="TG_API_HASH")
    data_dir: Path = Field(default=Path("./data"), validation_alias="TGHOARD_DATA_DIR")
    max_json_kb: int = Field(default=1024, ge=4, validation_alias="TGHOARD_MAX_JSON_KB")
    max_media_mb: int = Field(default=50, ge=0, validation_alias="TGHOARD_MAX_MEDIA_MB")
    concurrency: int = Field(default=5, ge=1, le=32, validation_alias="TGHOARD_CONCURRENCY")
    parallel_chats: int = Field(default=4, ge=1, le=16, validation_alias="TGHOARD_PARALLEL_CHATS")
    # Pause (s) between 100-message history pages. Telethon's default is 1s for big chats,
    # which caps a chat at ~100 msgs/s; FloodWaits are handled anyway (auto-sleep + retry).
    history_wait: float = Field(default=0.0, ge=0, le=10, validation_alias="TGHOARD_HISTORY_WAIT")
    # Media download pool: max downloads waiting in the channel before the message reader
    # is held back (backpressure)
    download_queue: int = Field(
        default=1024, ge=16, le=1_000_000, validation_alias="TGHOARD_DOWNLOAD_QUEUE"
    )
    # Chat metadata (full info, members, stories, admin log, profile photos) is refreshed at
    # most every N hours per chat (0 = every sync); messages/media are always synced.
    metadata_every_hours: float = Field(
        default=24.0, ge=0, validation_alias="TGHOARD_METADATA_EVERY_HOURS"
    )
    # Auto-throttle: adapt history pause / parallelism to FloodWaits (floors/ceilings above)
    auto_throttle: bool = Field(default=True, validation_alias="TGHOARD_AUTO_THROTTLE")
    # Geometric cut on each download FloodWait: parallelism x ratio (0.5 = halve)
    throttle_cut_ratio: float = Field(
        default=0.5, gt=0, lt=1, validation_alias="TGHOARD_THROTTLE_CUT_RATIO"
    )
    # Quiet period (s) after which the auto-throttle adds +1 download / halves the pause
    throttle_recover_secs: float = Field(
        default=20.0, ge=1, le=3600, validation_alias="TGHOARD_THROTTLE_RECOVER_SECS"
    )
    web_port: int = Field(default=8765, ge=0, le=65535, validation_alias="TGHOARD_WEB_PORT")
    sync_mode: SyncMode = Field(default="auto", validation_alias="TGHOARD_SYNC_MODE")
    audit_window: int = Field(default=200, ge=10, validation_alias="TGHOARD_AUDIT_WINDOW")

    @field_validator("tg_api_id", mode="before")
    @classmethod
    def _empty_str_to_none_int(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("tg_api_hash", mode="before")
    @classmethod
    def _empty_str_to_none_str(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @property
    def max_json_bytes(self) -> int:
        return self.max_json_kb * 1024

    @property
    def max_media_bytes(self) -> int | None:
        if self.max_media_mb <= 0:
            return None
        return self.max_media_mb * 1024 * 1024

    @property
    def chats_manifest_path(self) -> Path:
        return self.data_dir / "chats.md"

    @property
    def internal_dir(self) -> Path:
        return self.data_dir / ".tghoard"

    @property
    def checkpoint_db_path(self) -> Path:
        return self.internal_dir / "checkpoint.db"

    @property
    def sessions_dir(self) -> Path:
        return self.internal_dir / "sessions"

    def ensure_dirs(self) -> None:
        """Create data_dir and restricted .tghoard internal directories."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.internal_dir.mkdir(parents=True, exist_ok=True)
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.internal_dir, 0o700)
            os.chmod(self.sessions_dir, 0o700)
        except OSError:
            pass

    def with_overrides(
        self,
        *,
        data_dir: Path | None = None,
        max_json_kb: int | None = None,
        max_media_mb: int | None = None,
        concurrency: int | None = None,
        parallel_chats: int | None = None,
        sync_mode: SyncMode | None = None,
        history_wait: float | None = None,
        auto_throttle: bool | None = None,
        metadata_every_hours: float | None = None,
    ) -> AppConfig:
        updates: dict[str, Any] = {}
        if data_dir is not None:
            updates["data_dir"] = data_dir
        if max_json_kb is not None:
            updates["max_json_kb"] = max_json_kb
        if max_media_mb is not None:
            updates["max_media_mb"] = max_media_mb
        if concurrency is not None:
            updates["concurrency"] = concurrency
        if parallel_chats is not None:
            updates["parallel_chats"] = parallel_chats
        if history_wait is not None:
            updates["history_wait"] = history_wait
        if auto_throttle is not None:
            updates["auto_throttle"] = auto_throttle
        if metadata_every_hours is not None:
            updates["metadata_every_hours"] = metadata_every_hours
        if sync_mode is not None:
            updates["sync_mode"] = sync_mode
        return self.model_copy(update=updates)


def load_config(env_file: Path | str | None = ".env") -> AppConfig:
    """Load AppConfig using an explicit .env path if provided."""
    if env_file is not None:
        return AppConfig(_env_file=env_file)
    return AppConfig(_env_file=None)
