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
    concurrency: int = Field(default=3, ge=1, le=16, validation_alias="TGHOARD_CONCURRENCY")
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
        sync_mode: SyncMode | None = None,
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
        if sync_mode is not None:
            updates["sync_mode"] = sync_mode
        return self.model_copy(update=updates)


def load_config(env_file: Path | str | None = ".env") -> AppConfig:
    """Load AppConfig using an explicit .env path if provided."""
    if env_file is not None:
        return AppConfig(_env_file=env_file)
    return AppConfig(_env_file=None)
