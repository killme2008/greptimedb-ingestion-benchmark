package benchmark

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ProtocolStats holds the aggregated statistics for one protocol.
type ProtocolStats struct {
	Protocol     string  `json:"protocol"`
	BatchSize    int     `json:"batch_size"`
	RowsPerSec   float64 `json:"rows_per_sec"`
	DurationSec  float64 `json:"duration_sec"`
	P50Ms        float64 `json:"p50_ms"`
	P95Ms        float64 `json:"p95_ms"`
	P99Ms        float64 `json:"p99_ms"`
	Errors       int     `json:"errors"`
	TotalBatches int     `json:"total_batches"`
}

// ComputeStats computes aggregated stats from a benchmark result.
// Only successful batches contribute to throughput and latency percentiles.
func ComputeStats(r *BenchmarkResult, batchSize int) ProtocolStats {
	var errors int
	var successRows int
	durations := make([]time.Duration, 0, len(r.Results))
	for _, br := range r.Results {
		if br.Err != nil {
			errors++
			continue
		}
		successRows += br.Rows
		durations = append(durations, br.Duration)
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	wallSec := r.WallTime.Seconds()
	rowsPerSec := float64(successRows) / wallSec

	return ProtocolStats{
		Protocol:     r.Protocol,
		BatchSize:    batchSize,
		RowsPerSec:   rowsPerSec,
		DurationSec:  wallSec,
		P50Ms:        percentileMs(durations, 0.50),
		P95Ms:        percentileMs(durations, 0.95),
		P99Ms:        percentileMs(durations, 0.99),
		Errors:       errors,
		TotalBatches: len(r.Results),
	}
}

func percentileMs(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	// Use ceiling of rank to avoid underreporting tail latencies.
	// For N=100 and p=0.99: rank = ceil(100*0.99)-1 = 99, i.e. the last element.
	idx := int(math.Ceil(float64(len(sorted))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx].Microseconds()) / 1000.0
}

// PrintTable prints the results as a formatted ASCII table, grouped by batch size.
func PrintTable(stats []ProtocolStats, cfg *Config) {
	fmt.Println()
	fmt.Println("GreptimeDB Ingestion Benchmark Results")
	fmt.Println("=======================================")
	batchStrs := make([]string, len(cfg.BatchSizes))
	for i, bs := range cfg.BatchSizes {
		batchStrs[i] = strconv.Itoa(bs)
	}
	fmt.Printf("Config: %d rows | batch=%s | concurrency=%d | warmup=%d batches\n",
		cfg.TotalRows, strings.Join(batchStrs, ","), cfg.Concurrency, cfg.WarmupBatches)

	// Group stats by batch size, preserving order.
	type group struct {
		batchSize int
		stats     []ProtocolStats
	}
	var groups []group
	seen := map[int]int{} // batchSize -> index in groups
	for _, s := range stats {
		if idx, ok := seen[s.BatchSize]; ok {
			groups[idx].stats = append(groups[idx].stats, s)
		} else {
			seen[s.BatchSize] = len(groups)
			groups = append(groups, group{batchSize: s.BatchSize, stats: []ProtocolStats{s}})
		}
	}

	for _, g := range groups {
		fmt.Printf("\n--- Batch Size: %d ---\n", g.batchSize)
		fmt.Printf("%-20s %12s %12s %10s %10s %10s %8s\n",
			"Protocol", "Rows/sec", "Duration(s)", "P50(ms)", "P95(ms)", "P99(ms)", "Errors")
		fmt.Println("──────────────────────────────────────────────────────────────────────────────────────")

		for _, s := range g.stats {
			fmt.Printf("%-20s %12.0f %12.2f %10.1f %10.1f %10.1f %8d\n",
				s.Protocol, s.RowsPerSec, s.DurationSec,
				s.P50Ms, s.P95Ms, s.P99Ms, s.Errors)
		}
	}
	fmt.Println()
}

// PrintJSON prints the results as JSON to stdout.
func PrintJSON(stats []ProtocolStats, cfg *Config) {
	output := struct {
		Config struct {
			TotalRows     int    `json:"total_rows"`
			BatchSizes    []int  `json:"batch_sizes"`
			Concurrency   int    `json:"concurrency"`
			WarmupBatches int    `json:"warmup_batches"`
			Seed          int64  `json:"seed"`
			Host          string `json:"host"`
			Database      string `json:"database"`
		} `json:"config"`
		Results []ProtocolStats `json:"results"`
	}{
		Results: stats,
	}
	output.Config.TotalRows = cfg.TotalRows
	output.Config.BatchSizes = cfg.BatchSizes
	output.Config.Concurrency = cfg.Concurrency
	output.Config.WarmupBatches = cfg.WarmupBatches
	output.Config.Seed = cfg.Seed
	output.Config.Host = cfg.Host
	output.Config.Database = cfg.Database

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(output)
}
