package benchmark

import "context"

// Writer is the common interface all protocol adapters must implement.
type Writer interface {
	// Name returns the display name of this protocol.
	Name() string
	// Setup initializes the connection and creates tables if needed.
	Setup(cfg *Config) error
	// WriteBatch writes a batch of data points. Must be safe for concurrent use.
	// Implementations should respect ctx cancellation/deadline.
	WriteBatch(ctx context.Context, points []DataPoint) error
	// Close releases resources and closes connections.
	Close() error
}

// WorkerWriter is an optional interface for writers that need per-goroutine
// instances (e.g., streaming protocols where the underlying connection is
// not thread-safe).
type WorkerWriter interface {
	// NewWorker creates an independent Writer for a single worker goroutine.
	// The caller must call Close on the returned Writer when done.
	NewWorker() (Writer, error)
}

// protocolWriter pairs a Writer with its protocol key for table name derivation.
type protocolWriter struct {
	Key    string
	Writer Writer
}

func buildWriters(protocols []string) []protocolWriter {
	registry := map[string]func() Writer{
		"grpc":        func() Writer { return &GRPCWriter{} },
		"grpc_stream": func() Writer { return &GRPCStreamWriter{} },
		"grpc_bulk":   func() Writer { return &GRPCBulkWriter{} },
		"influxdb":    func() Writer { return &InfluxDBWriter{} },
		"otel":        func() Writer { return &OTelWriter{} },
		"mysql":       func() Writer { return &MySQLWriter{} },
		"postgres":    func() Writer { return &PostgresWriter{} },
	}

	var writers []protocolWriter
	for _, p := range protocols {
		if factory, ok := registry[p]; ok {
			writers = append(writers, protocolWriter{Key: p, Writer: factory()})
		}
	}
	return writers
}
