"""Tiny local web UI to manage `chats.md` and watch the sync (stdlib HTTP, no extra deps).

`chats.md` stays the single source of truth: the UI only reads/rewrites it (atomically),
so everything it does can also be done by editing the file and running the CLI.

Security (it can write files, so it is locked to this machine):
- binds to 127.0.0.1 only;
- rejects requests whose `Host` isn't localhost (blocks DNS rebinding);
- state-changing requests need the custom `X-Tghoard` header, which browsers won't send
  cross-origin without a CORS preflight we never answer (blocks CSRF from other sites);
- API/avatar requests a browser marks as coming from another site (`Sec-Fetch-Site:
  cross-site`/`same-site`) are refused, so other pages can't probe which chats you have
  or trigger Telegram calls through `<img>`/`fetch` (only the page itself is served to
  links from elsewhere).

It has no login: anyone who can open connections on this machine's loopback can use it.
"""

from __future__ import annotations

import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from tghoard.storage.chat_stats import (
    ChatStats,
    FolderScanCache,
    compute_all_chat_stats,
    list_oversized_media,
    media_size_averages,
    open_readonly_db,
)
from tghoard.storage.chats_manifest import (
    SECTION_HEADERS,
    load_chats_manifest,
    update_chat_selections,
)
from tghoard.sync.folders_sync import load_folders
from tghoard.sync.live_state import LiveSyncState
from tghoard.web.telegram_bridge import TelegramBridge

ALLOWED_HOSTS = {"127.0.0.1", "localhost", "[::1]", "::1"}
STATS_TTL_SECS = 15.0  # stats are refreshed in the background at most this often
MAX_BODY_BYTES = 1024 * 1024
# English display titles for the UI; SECTION_HEADERS keeps the chats.md (file format) titles.
SECTION_TITLES: dict[str, str] = {
    "saved": "Saved Messages",
    "groups": "Groups & supergroups",
    "channels": "Channels",
    "private": "Private chats",
    "bots": "Bots",
}


class _QuietHTTPServer(ThreadingHTTPServer):
    """Don't print tracebacks when the browser drops a connection (reload, tab closed)."""

    def handle_error(self, request: Any, client_address: Any) -> None:
        import sys

        exc = sys.exc_info()[1]
        if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
            return
        super().handle_error(request, client_address)


def _kind_of(chat_key: str) -> str:
    return chat_key.split("_", 1)[0]


class WebUI:
    def __init__(
        self,
        *,
        data_dir: Path,
        manifest_path: Path,
        db_path: Path,
        live: LiveSyncState | None = None,
        bridge: TelegramBridge | None = None,
        host: str = "127.0.0.1",
        port: int = 8765,
        max_media_bytes: int | None = None,
    ) -> None:
        self.data_dir = data_dir
        # The sync's size limit (TGHOARD_MAX_MEDIA_MB / --max-media-mb); None = unlimited
        self.max_media_bytes = max_media_bytes
        self.manifest_path = manifest_path
        self.db_path = db_path
        self.live = live
        self.bridge = bridge
        self.host = host
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._stats_lock = threading.Lock()
        self._stats_cache: tuple[float, dict[str, ChatStats]] | None = None
        self._stats_refreshing = False
        # Files on disk are counted in the background (minutes on a big archive on a USB
        # disk); until the first count finishes, the table shows the database-only stats.
        self._folder_cache = FolderScanCache()
        self._media_ready = False
        self._last_scan_secs = 0.0
        # None = read static/index.html from disk on every page load, so UI updates show up
        # with a simple reload (no need to restart a running `sync`). Tests may set bytes.
        self._index_html: bytes | None = None

    # -- lifecycle ------------------------------------------------------------
    def start(self) -> str:
        """Start in a daemon thread on the first free port from `self.port`. Returns URL."""
        handler = self._make_handler()
        last_err: OSError | None = None
        for port in [self.port, *range(self.port + 1, self.port + 20)] if self.port else [0]:
            try:
                self._server = _QuietHTTPServer((self.host, port), handler)
                break
            except OSError as exc:
                last_err = exc
        if self._server is None:
            raise RuntimeError(f"no free port starting at {self.port}: {last_err}")
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="tghoard-web", daemon=True
        )
        self._thread.start()
        return self.url

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/"

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    def index_html(self) -> bytes:
        if self._index_html is not None:
            return self._index_html
        return resources.files("tghoard.web").joinpath("static/index.html").read_bytes()

    # -- data -----------------------------------------------------------------
    def _compute_stats(self, *, scan_folders: bool = True) -> dict[str, ChatStats]:
        conn = open_readonly_db(self.db_path)
        try:
            return compute_all_chat_stats(
                self.data_dir,
                conn,
                max_media_bytes=self.max_media_bytes,
                scan_folders=scan_folders,
                folder_cache=self._folder_cache,
            )
        finally:
            if conn is not None:
                conn.close()

    def _refresh_stats_in_background(self) -> None:
        started = time.monotonic()
        try:
            fresh = self._compute_stats()
            with self._stats_lock:
                self._stats_cache = (time.monotonic(), fresh)
                self._media_ready = True
        finally:
            with self._stats_lock:
                self._stats_refreshing = False
                self._last_scan_secs = time.monotonic() - started

    def _start_refresh_locked(self) -> None:
        if not self._stats_refreshing:
            self._stats_refreshing = True
            threading.Thread(
                target=self._refresh_stats_in_background, name="tghoard-stats", daemon=True
            ).start()

    def _stats(self) -> dict[str, ChatStats]:
        """Stale-while-revalidate: requests never wait for the files on disk to be counted.

        The very first request gets database-only stats (about a second even with millions
        of queued media); a single background thread counts the files (re-scanning only the
        directories that changed) and refreshes the cache — at most every STATS_TTL_SECS, and
        no more often than 3x the last scan took, so a slow disk isn't kept busy scanning.
        """
        with self._stats_lock:
            cached = self._stats_cache
            if cached is not None:
                interval = max(STATS_TTL_SECS, 3 * self._last_scan_secs)
                if not self._media_ready or time.monotonic() - cached[0] >= interval:
                    self._start_refresh_locked()
                return cached[1]
        quick = self._compute_stats(scan_folders=False)
        with self._stats_lock:
            if self._stats_cache is None:
                self._stats_cache = (time.monotonic(), quick)
            self._start_refresh_locked()
            return self._stats_cache[1]

    @property
    def media_pending(self) -> bool:
        with self._stats_lock:
            return not self._media_ready

    def invalidate_stats(self) -> None:
        with self._stats_lock:
            if self._stats_cache is not None:
                self._stats_cache = (0.0, self._stats_cache[1])

    def chats_payload(self) -> dict[str, Any]:
        entries = load_chats_manifest(self.manifest_path)
        stats = self._stats()
        remote = self.bridge.all_cached_counts() if self.bridge is not None else {}
        rows = []
        for key, e in entries.items():
            st = stats.get(key) or ChatStats()
            rows.append(
                {
                    "chat_key": key,
                    "title": e.title,
                    "section": e.section,
                    "kind": _kind_of(key),
                    "username": e.username,
                    "accounts": e.accounts,
                    "baixar": e.baixar,
                    "photos": e.photos,
                    "videos": e.videos,
                    "audio": e.audio,
                    "files": e.files,
                    "priority": e.priority,
                    "stats": st.to_dict(),
                    "remote": remote.get(key),
                    "avatar": f"/avatar/{key}",
                }
            )
        return {
            "chats": rows,
            "sections": [{"key": k, "title": SECTION_TITLES.get(k, t)} for k, t in SECTION_HEADERS],
            "manifest_path": str(self.manifest_path),
            "data_dir": str(self.data_dir),
            "live": self.live is not None,
            "can_fetch": self.bridge is not None and self.bridge.can_fetch,
            "folders": self._folders(),
            # Archive-wide average media size per category: estimates the space needed by
            # history not archived yet (see chat_stats.estimate_to_download)
            "media_avg": media_size_averages(stats.values()),
            "max_media_bytes": self.max_media_bytes,
            # True until the files on disk have been counted once (media columns are 0 until
            # then): the page shows a notice and refreshes
            "media_pending": self.media_pending,
            "oversized": self._oversized_summary(entries, stats),
        }

    @staticmethod
    def _oversized_summary(entries: dict[str, Any], stats: dict[str, ChatStats]) -> dict[str, int]:
        """Queued media skipped for size in checked chats/categories (navbar badge)."""
        items = size = 0
        for key, e in entries.items():
            st = stats.get(key)
            if st is None or not e.baixar:
                continue
            for cat in e.enabled_categories():
                items += st.oversized_counts.get(cat, 0)
                size += st.oversized_bytes.get(cat, 0)
        return {"items": items, "bytes": size}

    def oversized_payload(self, *, include_unselected: bool) -> dict[str, Any]:
        conn = open_readonly_db(self.db_path)
        try:
            report = list_oversized_media(
                conn,
                load_chats_manifest(self.manifest_path),
                self.max_media_bytes,
                include_unselected=include_unselected,
            )
        finally:
            if conn is not None:
                conn.close()
        return report.to_dict()

    def _folders(self) -> list[dict[str, Any]]:
        """Telegram chat folders of every account (from `.tghoard/folders.json`)."""
        state = load_folders(self.data_dir)
        multi = len(state.get("accounts", {})) > 1
        out = []
        for alias, acc in sorted(state.get("accounts", {}).items()):
            for f in acc.get("folders", []):
                icon = f.get("emoticon") or "📁"
                label = f"{icon} {f['title']}" + (f" ({alias})" if multi else "")
                out.append({"key": f"{alias}:{f['id']}", "label": label, "members": f["members"]})
        return out

    # -- HTTP -----------------------------------------------------------------
    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        app = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "tghoard-web"

            def log_message(self, *_: Any) -> None:  # keep the terminal for sync logs
                return

            # helpers
            def _host_ok(self) -> bool:
                host = (self.headers.get("Host") or "").strip().lower()
                hostname = host.rsplit(":", 1)[0] if not host.endswith("]") else host
                return hostname in ALLOWED_HOSTS

            def _from_other_site(self) -> bool:
                # Absent on non-browser clients (curl) and old browsers: the Host/X-Tghoard
                # checks still apply there.
                site = (self.headers.get("Sec-Fetch-Site") or "").strip().lower()
                return site not in ("", "same-origin", "none")

            def _send(self, code: int, body: bytes, ctype: str, extra: dict | None = None):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Referrer-Policy", "no-referrer")
                for k, v in (extra or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def _json(self, code: int, payload: Any) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self._send(
                    code, body, "application/json; charset=utf-8", {"Cache-Control": "no-store"}
                )

            def _error(self, code: int, msg: str) -> None:
                self._json(code, {"ok": False, "error": msg})

            # routes
            def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
                if not self._host_ok():
                    return self._error(HTTPStatus.FORBIDDEN, "host not allowed")
                url = urlparse(self.path)
                is_page = url.path in ("/", "/index.html")
                if not is_page and self._from_other_site():
                    return self._error(HTTPStatus.FORBIDDEN, "cross-site request refused")
                if is_page:
                    return self._send(
                        HTTPStatus.OK,
                        app.index_html(),
                        "text/html; charset=utf-8",
                        {
                            "Cache-Control": "no-store",
                            "Content-Security-Policy": (
                                "default-src 'self'; img-src 'self' data:; "
                                "style-src 'self' 'unsafe-inline'; "
                                "script-src 'self' 'unsafe-inline'"
                            ),
                        },
                    )
                if url.path == "/api/chats":
                    return self._json(HTTPStatus.OK, app.chats_payload())
                if url.path == "/api/status":
                    if app.live is None:
                        return self._json(HTTPStatus.OK, {"phase": "offline"})
                    since = parse_qs(url.query).get("log_since", [None])[0]
                    try:
                        log_since = float(since) if since else None
                    except ValueError:
                        return self._error(HTTPStatus.BAD_REQUEST, "log_since must be a number")
                    return self._json(HTTPStatus.OK, app.live.snapshot(log_since=log_since))
                if url.path.startswith("/avatar/"):
                    chat_key = url.path.removeprefix("/avatar/")
                    path = app.bridge.avatar(chat_key) if app.bridge else None
                    if path is None:
                        return self._send(HTTPStatus.NOT_FOUND, b"", "image/jpeg")
                    return self._send(
                        HTTPStatus.OK,
                        path.read_bytes(),
                        "image/jpeg",
                        {"Cache-Control": "max-age=86400"},
                    )
                if url.path == "/api/oversized":
                    include = parse_qs(url.query).get("all", ["0"])[0] == "1"
                    return self._json(
                        HTTPStatus.OK, app.oversized_payload(include_unselected=include)
                    )
                if url.path.startswith("/api/counts/"):
                    chat_key = url.path.removeprefix("/api/counts/")
                    counts = app.bridge.counts(chat_key) if app.bridge else None
                    return self._json(HTTPStatus.OK, {"chat_key": chat_key, "remote": counts})
                return self._error(HTTPStatus.NOT_FOUND, "not found")

            def do_POST(self) -> None:  # noqa: N802
                if not self._host_ok():
                    return self._error(HTTPStatus.FORBIDDEN, "host not allowed")
                if self.headers.get("X-Tghoard") != "1":
                    return self._error(HTTPStatus.FORBIDDEN, "missing X-Tghoard header")
                if self._from_other_site():
                    return self._error(HTTPStatus.FORBIDDEN, "cross-site request refused")
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    return self._error(HTTPStatus.BAD_REQUEST, "invalid Content-Length")
                if length < 0:
                    return self._error(HTTPStatus.BAD_REQUEST, "invalid Content-Length")
                if length > MAX_BODY_BYTES:
                    return self._error(
                        HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "request body too large"
                    )
                try:
                    body = json.loads(self.rfile.read(length) or b"{}")
                except ValueError:
                    return self._error(HTTPStatus.BAD_REQUEST, "invalid JSON")
                if not isinstance(body, dict):
                    return self._error(HTTPStatus.BAD_REQUEST, "expected a JSON object")

                url = urlparse(self.path)
                if url.path == "/api/chats":
                    updates = body.get("updates")
                    if not isinstance(updates, dict) or not updates:
                        return self._error(HTTPStatus.BAD_REQUEST, "expected {updates: {...}}")
                    try:
                        entries = update_chat_selections(app.manifest_path, updates)
                    except ValueError as exc:
                        return self._error(HTTPStatus.BAD_REQUEST, str(exc))
                    if app.live is not None:
                        app.live.add_log(f"📝 web UI: {len(updates)} chat(s) updated in chats.md")
                    changed = {
                        k: {
                            f: getattr(entries[k], f)
                            for f in ("baixar", "photos", "videos", "audio", "files", "priority")
                        }
                        for k in updates
                        if k in entries
                    }
                    return self._json(HTTPStatus.OK, {"ok": True, "chats": changed})
                if url.path == "/api/sync/start":
                    started = app.live.request_start() if app.live is not None else False
                    return self._json(HTTPStatus.OK, {"ok": started})
                return self._error(HTTPStatus.NOT_FOUND, "not found")

        return Handler
