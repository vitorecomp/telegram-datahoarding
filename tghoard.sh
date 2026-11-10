#!/usr/bin/env bash
# Bootstrap the virtualenv (if needed) and run the tghoard CLI.
# Usage: ./tghoard.sh start | sync | watch | ... (any tghoard subcommand/flags)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"
STAMP="$VENV/.tghoard-install-stamp"
PY_VERSION="${TGHOARD_PYTHON:-3.13}"

cd "$ROOT"

log() { printf '\033[1;34m[tghoard.sh]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[tghoard.sh]\033[0m %s\n' "$*" >&2; exit 1; }

# Hash of the dependency spec; reinstall only when it changes.
current_hash() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$ROOT/pyproject.toml" | cut -d' ' -f1
    else
        shasum -a 256 "$ROOT/pyproject.toml" | cut -d' ' -f1  # macOS
    fi
}

create_venv() {
    if command -v uv >/dev/null 2>&1; then
        log "Creating venv with uv (Python $PY_VERSION)..."
        uv venv --python "$PY_VERSION" "$VENV"
    else
        local py=""
        for cand in "python$PY_VERSION" python3 python; do
            if command -v "$cand" >/dev/null 2>&1 && \
               "$cand" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
                py="$cand"; break
            fi
        done
        [[ -n "$py" ]] || die "Python >= 3.11 not found. Install Python or uv (https://docs.astral.sh/uv/)."
        log "Creating venv with $py..."
        "$py" -m venv "$VENV"
    fi
}

install_deps() {
    log "Installing tghoard and dependencies..."
    if command -v uv >/dev/null 2>&1; then
        uv pip install --python "$VENV/bin/python" -e ".[dev]"
    else
        "$VENV/bin/python" -m pip install --upgrade pip
        "$VENV/bin/python" -m pip install -e ".[dev]"
    fi
    current_hash > "$STAMP"
}

if [[ ! -x "$VENV/bin/python" ]]; then
    create_venv
fi

if [[ ! -f "$STAMP" || "$(cat "$STAMP")" != "$(current_hash)" || ! -x "$VENV/bin/tghoard" ]]; then
    install_deps
fi

if [[ ! -f "$ROOT/.env" && -f "$ROOT/.env.example" ]]; then
    cp "$ROOT/.env.example" "$ROOT/.env"
    chmod 600 "$ROOT/.env"  # it will hold the API hash
    log "Created .env from .env.example — fill in TG_API_ID and TG_API_HASH (https://my.telegram.org)."
fi

exec "$VENV/bin/tghoard" "$@"
