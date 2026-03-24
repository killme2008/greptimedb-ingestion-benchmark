package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/GreptimeTeam/ingestion-benchmark/benchmark"
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseBatchSizes(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	sizes := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid batch-size value %q: %w", p, err)
		}
		sizes = append(sizes, n)
	}
	if len(sizes) == 0 {
		return nil, fmt.Errorf("batch-size must have at least one value")
	}
	return sizes, nil
}

func main() {
	cfg := benchmark.Config{}

	flag.StringVar(&cfg.Host, "host", envOrDefault("GREPTIME_HOST", "127.0.0.1"), "GreptimeDB host")
	flag.StringVar(&cfg.Database, "database", envOrDefault("GREPTIME_DATABASE", "public"), "Database name")
	flag.StringVar(&cfg.User, "user", envOrDefault("GREPTIME_USER", ""), "Username")
	flag.StringVar(&cfg.Password, "password", envOrDefault("GREPTIME_PASSWORD", ""), "Password")

	var protocols string
	var batchSizeStr string
	flag.StringVar(&protocols, "protocols", "all", "Comma-separated list: grpc,grpc_stream,grpc_bulk,influxdb,otel,mysql,postgres (or 'all')")
	flag.IntVar(&cfg.TotalRows, "total-rows", 10000000, "Total rows to write per protocol")
	flag.StringVar(&batchSizeStr, "batch-size", "1000", "Rows per batch (comma-separated for multiple: 50,100,500,1000)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 5, "Number of concurrent workers")
	flag.IntVar(&cfg.WarmupBatches, "warmup-batches", 10, "Number of warm-up batches")
	flag.IntVar(&cfg.NumHosts, "num-hosts", 100, "Number of unique hosts (series = num-hosts × 5 regions × 10 datacenters × 20 services)")
	flag.Int64Var(&cfg.Seed, "seed", 42, "Random seed for data generation")
	flag.StringVar(&cfg.Output, "output", "table", "Output format: table, json")

	flag.Parse()

	if protocols == "all" {
		cfg.Protocols = benchmark.AllProtocols
	} else {
		cfg.Protocols = strings.Split(protocols, ",")
		for i, p := range cfg.Protocols {
			cfg.Protocols[i] = strings.TrimSpace(p)
		}
	}

	var err error
	cfg.BatchSizes, err = parseBatchSizes(batchSizeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := benchmark.Run(&cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
