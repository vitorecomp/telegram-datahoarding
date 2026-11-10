"""Versioned, data-directory migrations for the plain-text archive (`tghoard migrate`).

Each migration lives in its own module (`m0001_*.py`, `m0002_*.py`, ...) and exposes a
`MIGRATION` object. Applied migrations are recorded in `<data_dir>/.tghoard/migrations.json`;
each migration also knows how to detect whether it is still needed, so a lost record (or a
fresh `.tghoard/`) never re-runs work that is already done.

Rules for writing a migration:
- `plan()` must be read-only (it powers the default dry-run of `tghoard migrate`).
- `apply()` must be resumable: if interrupted, running it again finishes the job.
- Prefer moving (rename) over copying; never delete user content that wasn't migrated.
"""

from __future__ import annotations

import importlib
import json
import pkgutil
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from rich.console import Console

from tghoard.config import AppConfig
from tghoard.storage.atomic import atomic_write_text


@dataclass
class MigrationPlan:
    """Human-readable description of what `apply()` would do."""

    summary: str
    steps: list[str] = field(default_factory=list)
    table_rows: list[tuple[str, ...]] = field(default_factory=list)
    table_headers: tuple[str, ...] = ()


@dataclass
class Migration:
    id: str  # "0001"
    name: str  # "category_first_layout"
    description: str
    is_needed: Callable[[AppConfig], bool]
    plan: Callable[[AppConfig], MigrationPlan]
    apply: Callable[[AppConfig, Console], None]
    # Required migrations block commands that touch the archive until applied (mixed layouts
    # would corrupt data); optional ones (cleanups) are only listed by `tghoard migrate`.
    required: bool = True

    @property
    def key(self) -> str:
        return f"{self.id}_{self.name}"


def discover_migrations() -> list[Migration]:
    """All migrations in this package, ordered by id."""
    found: list[Migration] = []
    for mod_info in pkgutil.iter_modules(__path__):
        if not mod_info.name.startswith("m") or not mod_info.name[1:5].isdigit():
            continue
        module = importlib.import_module(f"{__name__}.{mod_info.name}")
        found.append(module.MIGRATION)
    return sorted(found, key=lambda m: m.id)


def _state_path(cfg: AppConfig) -> Path:
    return cfg.internal_dir / "migrations.json"


def load_applied(cfg: AppConfig) -> dict[str, str]:
    path = _state_path(cfg)
    if not path.exists():
        return {}
    return dict(json.loads(path.read_text(encoding="utf-8")).get("applied", {}))


def record_applied(cfg: AppConfig, migration: Migration) -> None:
    applied = load_applied(cfg)
    applied[migration.key] = datetime.now(UTC).isoformat(timespec="seconds")
    atomic_write_text(
        _state_path(cfg), json.dumps({"applied": applied}, indent=2, ensure_ascii=False) + "\n"
    )


def pending_migrations(cfg: AppConfig, *, required_only: bool = False) -> list[Migration]:
    applied = load_applied(cfg)
    return [
        m
        for m in discover_migrations()
        if m.key not in applied and (m.required or not required_only) and m.is_needed(cfg)
    ]


def backup_checkpoint_db(cfg: AppConfig, label: str) -> Path | None:
    """Consistent copy of `checkpoint.db` (SQLite online backup) before touching it."""
    src = cfg.checkpoint_db_path
    if not src.exists():
        return None
    backups = cfg.internal_dir / "backups"
    backups.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    dest = backups / f"checkpoint-{stamp}-before-{label}.db"
    with sqlite3.connect(src) as s_conn, sqlite3.connect(dest) as d_conn:
        s_conn.backup(d_conn)
    return dest
