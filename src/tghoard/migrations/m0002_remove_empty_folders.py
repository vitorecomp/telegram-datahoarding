"""0002 — remove empty chat folders left by older versions.

Earlier builds created `photos/`, `videos/`, `audio/`, `files/` (and `messages/`) for every
chat up front, even when nothing was downloaded there. Folders are now created only when a
file is written; this migration deletes the empty ones that already exist.

Only *empty* directories are removed (`rmdir`), so no file can ever be deleted. Folders
holding hidden files only (e.g. a stale `.dl_*` partial download) are left alone.
Non-blocking: commands keep working while it is pending.
"""

from __future__ import annotations

from pathlib import Path

from rich.console import Console

from tghoard.config import AppConfig
from tghoard.migrations import Migration, MigrationPlan
from tghoard.storage.paths import LAYOUT_ROOTS


def _empty_dirs(data_dir: Path) -> list[Path]:
    """Empty chat folders under each layout root, deepest first."""
    found: list[Path] = []
    for root_name in LAYOUT_ROOTS:
        root = data_dir / root_name
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
            if path.is_dir() and not any(path.iterdir()):
                found.append(path)
    # A root left with nothing inside is empty too (e.g. `audio/` when no audio exists)
    for root_name in LAYOUT_ROOTS:
        root = data_dir / root_name
        if root.is_dir() and all(p in found for p in root.iterdir()):
            found.append(root)
    return found


def _is_needed(cfg: AppConfig) -> bool:
    return bool(_empty_dirs(cfg.data_dir))


def _plan(cfg: AppConfig) -> MigrationPlan:
    dirs = _empty_dirs(cfg.data_dir)
    by_root: dict[str, int] = {}
    for d in dirs:
        rel = d.relative_to(cfg.data_dir)
        by_root[rel.parts[0]] = by_root.get(rel.parts[0], 0) + 1
    return MigrationPlan(
        summary=f"{len(dirs)} empty folder(s) will be removed (no file is deleted).",
        steps=["Removes only empty directories (rmdir); folders holding any file are kept"],
        table_headers=("Root", "Empty folders"),
        table_rows=[(root, str(n)) for root, n in sorted(by_root.items())],
    )


def _apply(cfg: AppConfig, console: Console) -> None:
    removed = 0
    for d in _empty_dirs(cfg.data_dir):
        try:
            d.rmdir()  # fails (and is skipped) if anything appeared inside meanwhile
            removed += 1
        except OSError:
            continue
    console.print(f"  🧹 {removed} empty folder(s) removed")


MIGRATION = Migration(
    id="0002",
    name="remove_empty_folders",
    description="Remove empty chat folders (photos/, videos/, audio/, files/, messages/ …)",
    is_needed=_is_needed,
    plan=_plan,
    apply=_apply,
    required=False,
)
