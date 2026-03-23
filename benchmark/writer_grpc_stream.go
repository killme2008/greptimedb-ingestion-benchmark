package benchmark

import (
	"context"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
)

// GRPCStreamWriter uses gRPC bidirectional streaming (StreamWrite / CloseStream).
// The underlying stream is NOT thread-safe, so each worker goroutine must get
// its own instance via NewWorker.
type GRPCStreamWriter struct {
	cfg       *Config
	tableName string
	schema    []*gpb.ColumnSchema

	// Per-worker fields, only set on instances returned by NewWorker.
	cli      *greptime.Client
	isWorker bool
}

func (w *GRPCStreamWriter) Name() string { return "gRPC Stream" }

func (w *GRPCStreamWriter) Setup(cfg *Config) error {
	w.cfg = cfg
	w.tableName = cfg.TableName
	w.schema = buildGRPCSchema()
	return nil
}

// NewWorker creates an independent writer with its own gRPC Client and stream.
func (w *GRPCStreamWriter) NewWorker() (Writer, error) {
	cli, err := newGRPCClient(w.cfg)
	if err != nil {
		return nil, err
	}

	return &GRPCStreamWriter{
		cfg:       w.cfg,
		tableName: w.tableName,
		schema:    w.schema,
		cli:       cli,
		isWorker:  true,
	}, nil
}

func (w *GRPCStreamWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	tbl, err := buildGRPCTable(w.tableName, w.schema, points)
	if err != nil {
		return err
	}
	// Use context.Background() because the SDK binds the stream lifecycle to
	// the context of the first StreamWrite call. A per-batch timeout context
	// would tear down the stream when cancelled. The stream is flushed and
	// closed via CloseStream in Close().
	return w.cli.StreamWrite(context.Background(), tbl)
}

// Close flushes the stream via CloseStream, then closes the gRPC connection.
func (w *GRPCStreamWriter) Close() error {
	if w.cli == nil {
		return nil
	}
	// CloseStream sends CloseAndRecv, flushing buffered data.
	if _, err := w.cli.CloseStream(context.Background()); err != nil {
		_ = w.cli.Close()
		return err
	}
	return w.cli.Close()
}
