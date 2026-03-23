package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BatchResult records the outcome of a single WriteBatch call.
type BatchResult struct {
	Duration time.Duration
	Rows     int
	Err      error
}

// BenchmarkResult holds all results for one protocol run.
type BenchmarkResult struct {
	Protocol   string
	Results    []BatchResult
	WarmupRows int
	WallTime   time.Duration
}

const defaultBatchTimeout = 30 * time.Second

// RunBenchmark executes the full benchmark lifecycle for a single writer.
// warmupBatches and measureBatches must be disjoint slices to avoid double-writing.
func RunBenchmark(w Writer, cfg *Config, warmupBatches, measureBatches [][]DataPoint) (*BenchmarkResult, error) {
	if err := w.Setup(cfg); err != nil {
		return nil, fmt.Errorf("setup %s: %w", w.Name(), err)
	}
	defer func() { _ = w.Close() }()

	batchTimeout := cfg.BatchTimeout
	if batchTimeout == 0 {
		batchTimeout = defaultBatchTimeout
	}

	// Warm-up phase: write warmup batches concurrently to also warm connection pools.
	ww, isWorkerWriter := w.(WorkerWriter)

	var warmupRows int64
	if len(warmupBatches) > 0 {
		ch := make(chan []DataPoint, len(warmupBatches))
		for _, b := range warmupBatches {
			ch <- b
		}
		close(ch)

		var wg sync.WaitGroup
		for i := 0; i < cfg.Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				bw := Writer(w)
				if isWorkerWriter {
					worker, err := ww.NewWorker()
					if err != nil {
						return
					}
					defer func() { _ = worker.Close() }()
					bw = worker
				}
				for batch := range ch {
					ctx, cancel := context.WithTimeout(context.Background(), batchTimeout)
					if err := bw.WriteBatch(ctx, batch); err == nil {
						atomic.AddInt64(&warmupRows, int64(len(batch)))
					}
					cancel()
				}
			}()
		}
		wg.Wait()
	}

	// Measurement phase.
	ch := make(chan []DataPoint, len(measureBatches))
	for _, b := range measureBatches {
		ch <- b
	}
	close(ch)

	var mu sync.Mutex
	var allResults []BatchResult
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bw := Writer(w)
			if isWorkerWriter {
				worker, err := ww.NewWorker()
				if err != nil {
					mu.Lock()
					allResults = append(allResults, BatchResult{Err: err})
					mu.Unlock()
					return
				}
				defer func() { _ = worker.Close() }()
				bw = worker
			}
			var local []BatchResult
			for batch := range ch {
				ctx, cancel := context.WithTimeout(context.Background(), batchTimeout)
				t0 := time.Now()
				err := bw.WriteBatch(ctx, batch)
				elapsed := time.Since(t0)
				cancel()
				local = append(local, BatchResult{
					Duration: elapsed,
					Rows:     len(batch),
					Err:      err,
				})
			}
			mu.Lock()
			allResults = append(allResults, local...)
			mu.Unlock()
		}()
	}

	wg.Wait()
	wallTime := time.Since(start)

	return &BenchmarkResult{
		Protocol:   w.Name(),
		Results:    allResults,
		WarmupRows: int(warmupRows),
		WallTime:   wallTime,
	}, nil
}

// execSQL executes a SQL statement via GreptimeDB HTTP API (/v1/sql).
func execSQL(cfg *Config, query string) ([]byte, error) {
	u := fmt.Sprintf("http://%s:4000/v1/sql?db=%s", cfg.Host, url.QueryEscape(cfg.Database))
	req, err := http.NewRequest(http.MethodPost, u, strings.NewReader(url.Values{"sql": {query}}.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if cfg.User != "" || cfg.Password != "" {
		req.SetBasicAuth(cfg.User, cfg.Password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	return body, nil
}

// truncateTable truncates a single table via GreptimeDB HTTP SQL API.
// Errors are logged but do not fail the benchmark (table may not exist on first run).
func truncateTable(cfg *Config, tableName string) {
	_, err := execSQL(cfg, fmt.Sprintf("TRUNCATE TABLE %s", tableName))
	if err != nil {
		// Table not found on first run is expected; only log other errors.
		if !strings.Contains(err.Error(), "Table not found") {
			log.Printf("  [truncate] %s: %v", tableName, err)
		}
	} else {
		log.Printf("  [truncate] %s: OK", tableName)
	}
}

// verifyRowCount queries GreptimeDB via HTTP SQL API to check actual row count
// against expected. Logs a warning on mismatch; does not fail the benchmark.
func verifyRowCount(cfg *Config, tableName string, expected int) {
	body, err := execSQL(cfg, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	if err != nil {
		log.Printf("  [verify] failed to query %s: %v", tableName, err)
		return
	}

	count, err := parseCountResult(body)
	if err != nil {
		log.Printf("  [verify] failed to parse result for %s: %v", tableName, err)
		return
	}

	if count != expected {
		log.Printf("  [verify] WARNING: %s has %d rows, expected %d", tableName, count, expected)
	} else {
		log.Printf("  [verify] %s: %d rows OK", tableName, count)
	}
}

// parseCountResult extracts the integer count from a GreptimeDB /v1/sql JSON response.
// Expected format: {"output":[{"records":{"rows":[[N]]}}], ...}
func parseCountResult(body []byte) (int, error) {
	var resp struct {
		Output []struct {
			Records struct {
				Rows [][]json.Number `json:"rows"`
			} `json:"records"`
		} `json:"output"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("unmarshal: %w (body: %s)", err, body)
	}
	if len(resp.Output) == 0 || len(resp.Output[0].Records.Rows) == 0 || len(resp.Output[0].Records.Rows[0]) == 0 {
		return 0, fmt.Errorf("unexpected response structure: %s", body)
	}
	count, err := resp.Output[0].Records.Rows[0][0].Int64()
	if err != nil {
		return 0, fmt.Errorf("parse count: %w", err)
	}
	return int(count), nil
}

// Run is the main entry point for the benchmark package.
// It generates data, runs benchmarks for all configured protocols across all
// configured batch sizes, and prints results.
func Run(cfg *Config) error {
	var allStats []ProtocolStats

	for _, batchSize := range cfg.BatchSizes {
		// Create a per-round copy so the shared cfg is never mutated.
		roundCfg := *cfg
		roundCfg.BatchSize = batchSize

		// Generate enough data for both warmup and measurement.
		warmupRows := roundCfg.WarmupBatches * batchSize
		totalRows := roundCfg.TotalRows + warmupRows
		log.Printf("Generating %d data points (%d warmup + %d measurement, batch=%d, seed=%d)...",
			totalRows, warmupRows, roundCfg.TotalRows, batchSize, roundCfg.Seed)
		allPoints := GenerateData(totalRows, roundCfg.Seed)
		allBatches := SplitBatches(allPoints, batchSize)

		// Split into warmup and measurement batches.
		warmupBatches := allBatches[:roundCfg.WarmupBatches]
		measureBatches := allBatches[roundCfg.WarmupBatches:]
		log.Printf("Split into %d warmup + %d measurement batches of up to %d rows each",
			len(warmupBatches), len(measureBatches), batchSize)

		// Each batch-size round creates fresh writers (new connections, prepared stmts).
		writers := buildWriters(roundCfg.Protocols)

		for _, pw := range writers {
			roundCfg.TableName = "benchmark_" + pw.Key
			log.Printf("Running benchmark: %s (table: %s, batch=%d)", pw.Writer.Name(), roundCfg.TableName, batchSize)

			// Truncate table from previous runs to ensure fresh insert measurement.
			truncateTable(&roundCfg, roundCfg.TableName)

			result, err := RunBenchmark(pw.Writer, &roundCfg, warmupBatches, measureBatches)
			if err != nil {
				log.Printf("ERROR [%s]: %v", pw.Writer.Name(), err)
				continue
			}
			stats := ComputeStats(result, batchSize)
			allStats = append(allStats, stats)
			log.Printf("  %s: %.0f rows/sec (%.2fs, %d errors)",
				stats.Protocol, stats.RowsPerSec, stats.DurationSec, stats.Errors)
			if stats.Errors > 0 {
				for _, br := range result.Results {
					if br.Err != nil {
						log.Printf("  first error: %v", br.Err)
						break
					}
				}
			}

			// Verify row count: expected = warmup rows + measurement rows (both successful only).
			measureRows := 0
			for _, br := range result.Results {
				if br.Err == nil {
					measureRows += br.Rows
				}
			}
			expectedRows := result.WarmupRows + measureRows
			verifyRowCount(&roundCfg, roundCfg.TableName, expectedRows)
		}
	}

	if len(allStats) == 0 {
		return fmt.Errorf("no protocols completed successfully")
	}

	switch cfg.Output {
	case "json":
		PrintJSON(allStats, cfg)
	default:
		PrintTable(allStats, cfg)
	}

	return nil
}
