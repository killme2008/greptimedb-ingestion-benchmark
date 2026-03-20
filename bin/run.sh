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

install_greptimedb() {
    if [[ -x "$BINARY" ]]; then
        echo "GreptimeDB already installed at $BINARY"
        return
    fi

    echo "Installing GreptimeDB..."
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

trap cleanup EXIT

install_greptimedb
build_benchmark
start_greptimedb

echo ""
"$ROOT_DIR/bin/ingestion-benchmark" "${BENCH_ARGS[@]+"${BENCH_ARGS[@]}"}"
