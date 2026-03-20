package benchmark

import (
	"fmt"
	"strings"
)

type Config struct {
	Host     string
	Database string
	User     string
	Password string

	Protocols     []string
	TotalRows     int
	BatchSizes    []int
	Concurrency   int
	WarmupBatches int
	Seed          int64
	Output        string

	// BatchSize is set internally per batch-size round, not a CLI flag.
	BatchSize int
	// TableName is set internally per-protocol run, not a CLI flag.
	TableName string
}

var AllProtocols = []string{"grpc", "influxdb", "otel", "mysql", "postgres"}

func (c *Config) Validate() error {
	if c.TotalRows <= 0 {
		return fmt.Errorf("total-rows must be positive")
	}
	if len(c.BatchSizes) == 0 {
		return fmt.Errorf("batch-size must have at least one value")
	}
	for _, bs := range c.BatchSizes {
		if bs <= 0 {
			return fmt.Errorf("batch-size values must be positive, got %d", bs)
		}
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive")
	}
	if c.WarmupBatches < 0 {
		return fmt.Errorf("warmup-batches must be non-negative")
	}
	if c.Output != "table" && c.Output != "json" {
		return fmt.Errorf("output must be 'table' or 'json'")
	}

	valid := make(map[string]bool)
	for _, p := range AllProtocols {
		valid[p] = true
	}
	for _, p := range c.Protocols {
		if !valid[p] {
			return fmt.Errorf("unknown protocol: %s (valid: %s)", p, strings.Join(AllProtocols, ", "))
		}
	}

	return nil
}
