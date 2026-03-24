# GreptimeDB Ingestion Protocol Benchmark

A CLI benchmark tool that compares GreptimeDB's ingestion performance across different protocols under identical conditions.

## Protocols

| Protocol | Go Client | Port | Transport |
|----------|-----------|------|-----------|
| gRPC SDK | `greptimedb-ingester-go` v0.7.1 | 4001 | gRPC |
| gRPC Stream | `greptimedb-ingester-go` v0.7.1 | 4001 | gRPC (bidirectional streaming) |
| gRPC Bulk (Arrow) | `greptimedb-ingester-go` v0.7.1 | 4001 | Arrow Flight (DoPut) |
| InfluxDB Line Protocol | `influxdb-client-go` v2.14.0 | 4000 | HTTP |
| OpenTelemetry (OTLP) | `go.opentelemetry.io/otel` v1.42.0 | 4000 | HTTP+Protobuf (Logs) |
| MySQL INSERT | `go-sql-driver/mysql` v1.9.3 | 4002 | MySQL wire |
| PostgreSQL INSERT | `jackc/pgx` v5.8.0 | 4003 | PostgreSQL wire |

All clients use default SDK settings — no connection pool tuning, no buffer size adjustments, no custom timeouts.

## Quick Start

The one-liner script installs GreptimeDB (if needed), starts it, runs the benchmark, and cleans up on exit:

```bash
bin/run.sh
```

Pass any flags through to the benchmark:

```bash
bin/run.sh -protocols grpc,influxdb -total-rows 50000 -concurrency 8
bin/run.sh -protocols grpc,otel -batch-size 50,100,500,1000
bin/run.sh --help
```

When `-host` is specified (via flag or `GREPTIME_HOST` env var), the script skips installing and starting a local GreptimeDB — you manage the instance yourself:

```bash
bin/run.sh -host 10.0.0.1 -protocols grpc_bulk
```

## Manual Usage

If you already have a GreptimeDB instance running:

```bash
go build -o bin/ingestion-benchmark .
bin/ingestion-benchmark -host 127.0.0.1 -total-rows 100000
```

## Configuration

Connection settings can be configured via environment variables or CLI flags. CLI flags take precedence.

| Environment Variable | CLI Flag | Default | Description |
|---------------------|----------|---------|-------------|
| `GREPTIME_HOST` | `-host` | `127.0.0.1` | GreptimeDB host |
| `GREPTIME_DATABASE` | `-database` | `public` | Database name |
| `GREPTIME_USER` | `-user` | (empty) | Username |
| `GREPTIME_PASSWORD` | `-password` | (empty) | Password |

You can also put these in a `.env` file at the project root — `bin/run.sh` will pick them up.

```bash
# .env
GREPTIME_HOST=10.0.0.1
GREPTIME_DATABASE=benchmark
GREPTIME_USER=admin
GREPTIME_PASSWORD=secret
```

### Benchmark Flags

```
-protocols         Comma-separated: grpc,grpc_stream,grpc_bulk,influxdb,otel,mysql,postgres (default: all)
-total-rows        Total rows to write per protocol (default: 10000000)
-batch-size        Rows per batch, comma-separated for multiple (default: 1000)
-concurrency       Number of concurrent workers (default: 5)
-warmup-batches    Warm-up batches before measurement (default: 10)
-num-hosts         Number of unique hosts; series = num-hosts × 1000 (default: 100 → 100K series)
-seed              Random seed for data generation (default: 42)
-output            Output format: table, json (default: table)
```

## Data Model

Each protocol writes to its own isolated table to ensure fair comparison:

| Protocol | Table |
|----------|-------|
| gRPC SDK | `benchmark_grpc` |
| gRPC Stream | `benchmark_grpc_stream` |
| gRPC Bulk (Arrow) | `benchmark_grpc_bulk` |
| InfluxDB LP | `benchmark_influxdb` |
| OpenTelemetry | `benchmark_otel` |
| MySQL INSERT | `benchmark_mysql` |
| PostgreSQL INSERT | `benchmark_postgres` |

Schema:

- **Tags**: `host` (configurable via `-num-hosts`), `region` (5), `datacenter` (10), `service` (20) — series = num-hosts × 1,000 (default 100,000)
- **Fields**: `cpu`, `memory`, `disk_util`, `net_in`, `net_out` (all float64)
- **Timestamp**: millisecond precision

Data is pre-generated with a fixed random seed before measurement begins, ensuring deterministic and reproducible results across runs.

## How It Works

1. **Generate** — All data points are pre-generated in memory (not timed). Warmup data and measurement data use separate, non-overlapping batches.
2. **Setup** — Initialize the writer (connect, create per-protocol tables)
3. **Warm-up** — Write warmup-only batches, results discarded
4. **Measure** — N concurrent workers pull measurement batches from a shared channel; each `WriteBatch` call is timed. Streaming protocols (gRPC Stream, gRPC Bulk) use per-worker streams for maximum throughput.
5. **Verify** — Query actual row count via HTTP SQL API and compare against expected; log warning on mismatch.
6. **Report** — Compute throughput (rows/sec = total rows / wall clock time) and latency percentiles (p50/p95/p99)

Each protocol is benchmarked sequentially. Within a protocol run, batches are distributed across workers via a channel-based work queue.

## Verification

After a run, the tool automatically verifies row counts. You can also check manually:

```bash
mysql -h 127.0.0.1 -P 4002 -e "SHOW TABLES" public
mysql -h 127.0.0.1 -P 4002 -e "SELECT COUNT(*) FROM benchmark_grpc" public
```

## Requirements

- Go 1.25+
- `curl` and `nc` (for `bin/run.sh`)
