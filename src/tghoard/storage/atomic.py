"""Atomic plain-text file writers and integer interval arithmetic."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(UTC).isoformat(timespec="seconds")


def atomic_write_text(path: Path, content: str) -> None:
    """Atomically write UTF-8 text via a temporary file in the same directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def atomic_write_json(path: Path, payload: Any) -> None:
    """Atomically write pretty-printed UTF-8 JSON."""
    serialized = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    atomic_write_text(path, serialized)


def merge_ranges(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping or contiguous integer intervals [start, end]."""
    normalized = sorted((min(a, b), max(a, b)) for a, b in ranges if a > 0 and b > 0)
    if not normalized:
        return []
    merged: list[tuple[int, int]] = [normalized[0]]
    for start, end in normalized[1:]:
        prev_start, prev_end = merged[-1]
        if start <= prev_end + 1:
            merged[-1] = (prev_start, max(prev_end, end))
        else:
            merged.append((start, end))
    return merged


def compute_missing_ranges(
    min_id: int,
    max_id: int,
    synced: Sequence[tuple[int, int]],
) -> list[tuple[int, int]]:
    """Return uncovered sub-intervals within [min_id, max_id]."""
    if max_id < min_id or max_id <= 0:
        return []
    min_id = max(1, min_id)
    merged = merge_ranges(synced)
    missing: list[tuple[int, int]] = []
    cursor = min_id
    for start, end in merged:
        if end < cursor:
            continue
        if start > max_id:
            break
        if start > cursor:
            missing.append((cursor, min(max_id, start - 1)))
        cursor = max(cursor, end + 1)
        if cursor > max_id:
            break
    if cursor <= max_id:
        missing.append((cursor, max_id))
    return missing


def is_id_in_ranges(msg_id: int, synced: Sequence[tuple[int, int]]) -> bool:
    """Return True if msg_id falls inside any synced [start, end] interval."""
    lo = 0
    hi = len(synced) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        start, end = synced[mid]
        if msg_id < start:
            hi = mid - 1
        elif msg_id > end:
            lo = mid + 1
        else:
            return True
    return False
