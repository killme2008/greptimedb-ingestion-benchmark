#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Load .env if present (connection settings, etc.)
if [[ -f "$ROOT_DIR/.env" ]]; then
    # shellcheck disable=SC1091
    set -a
    source "$ROOT_DIR/.env"
    set +a
fi
DATA_DIR="$ROOT_DIR/greptimedb"
BINARY="$DATA_DIR/greptime"
PID_FILE="$DATA_DIR/greptime.pid"

# Pass all arguments through to the benchmark binary.
BENCH_ARGS=("$@")

cleanup() {
    echo "Stopping GreptimeDB..."
    if [[ -f "$PID_FILE" ]]; then
        kill "$(cat "$PID_FILE")" 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
}

get_installed_version() {
    "$BINARY" --version 2>/dev/null | awk '/^version:/ {print $2; exit}'
}

get_latest_version() {
    curl -fsSL https://api.github.com/repos/GreptimeTeam/greptimedb/releases/latest 2>/dev/null \
        | grep '"tag_name":' | head -1 | sed -E 's/.*"tag_name": *"v?([^"]+)".*/\1/'
}

install_greptimedb() {
    local latest installed
    latest="$(get_latest_version || true)"

    if [[ -x "$BINARY" ]]; then
        installed="$(get_installed_version || true)"
        if [[ -z "$latest" ]]; then
            echo "GreptimeDB ${installed:-unknown} installed at $BINARY (latest version lookup failed, skipping upgrade check)"
            return
        fi
        # GitHub's releases/latest excludes pre-releases, so `latest` is always
        # the newest GA. Any mismatch (including older GA or any pre-release
        # cached locally) should trigger a reinstall.
        if [[ "$installed" == "$latest" ]]; then
            echo "GreptimeDB $installed is up to date"
            return
        fi
        echo "Upgrading GreptimeDB ${installed:-unknown} -> $latest"
        rm -f "$BINARY"
    else
        echo "Installing GreptimeDB ${latest:-latest}..."
    fi

    mkdir -p "$DATA_DIR"
    # The install script extracts binary to CWD.
    (cd "$DATA_DIR" && curl -fsSL \
        https://raw.githubusercontent.com/greptimeteam/greptimedb/main/scripts/install.sh | sh)

    if [[ ! -x "$BINARY" ]]; then
        echo "ERROR: GreptimeDB binary not found after install" >&2
        exit 1
    fi
    echo "GreptimeDB installed at $BINARY"
}

start_greptimedb() {
    echo "Starting GreptimeDB standalone..."
    "$BINARY" standalone start \
        --data-home "$DATA_DIR/data" \
        --influxdb-enable \
        >"$DATA_DIR/greptime.log" 2>&1 &
    echo $! > "$PID_FILE"

    # Wait for all ports (gRPC=4001, HTTP=4000, MySQL=4002, Postgres=4003).
    echo -n "Waiting for GreptimeDB to be ready"
    for port in 4001 4000 4002 4003; do
        for i in $(seq 1 30); do
            if nc -z 127.0.0.1 "$port" 2>/dev/null; then
                break
            fi
            if [[ $i -eq 30 ]]; then
                echo ""
                echo "ERROR: GreptimeDB port $port not ready after 30s" >&2
                echo "Logs:" >&2
                tail -20 "$DATA_DIR/greptime.log" >&2
                cleanup
                exit 1
            fi
            echo -n "."
            sleep 1
        done
    done
    echo " ready"
}

build_benchmark() {
    echo "Building benchmark..."
    (cd "$ROOT_DIR" && go build -o "$ROOT_DIR/bin/ingestion-benchmark" .)
}

# For --help / -h, just build and print usage without starting GreptimeDB.
for arg in "${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"}"; do
    if [[ "$arg" == "-h" || "$arg" == "--help" || "$arg" == "-help" ]]; then
        build_benchmark
        "$ROOT_DIR/bin/ingestion-benchmark" -h
        exit 0
    fi
done

# If user specifies -host (flag or env), they manage GreptimeDB themselves.
MANAGED_DB=true
for arg in "${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"}"; do
    if [[ "$arg" == "-host" || "$arg" == "--host" ]]; then
        MANAGED_DB=false
        break
    fi
done
if [[ -n "${GREPTIME_HOST:-}" ]]; then
    MANAGED_DB=false
fi

build_benchmark

if [[ "$MANAGED_DB" == "true" ]]; then
    trap cleanup EXIT
    install_greptimedb
    start_greptimedb
fi

echo ""
"$ROOT_DIR/bin/ingestion-benchmark" "${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"}"
