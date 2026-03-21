package benchmark

import (
	"context"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
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
	grpcCfg := greptime.NewConfig(w.cfg.Host).
		WithPort(4001).
		WithDatabase(w.cfg.Database).
		WithAuth(w.cfg.User, w.cfg.Password)

	cli, err := greptime.NewClient(grpcCfg)
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
	tbl, err := table.New(w.tableName)
	if err != nil {
		return err
	}
	tbl.WithColumnsSchema(w.schema)

	for _, p := range points {
		if err := tbl.AddRow(p.Host, p.Region, p.CPU, p.Memory, p.DiskUtil, p.NetIn, p.NetOut, p.Timestamp); err != nil {
			return err
		}
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

// buildGRPCSchema returns the shared column schema used by all gRPC-based writers.
func buildGRPCSchema() []*gpb.ColumnSchema {
	return []*gpb.ColumnSchema{
		{ColumnName: "host", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "cloud_region", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "cpu", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "memory", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "disk_util", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "net_in", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "net_out", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "ts", SemanticType: gpb.SemanticType_TIMESTAMP, Datatype: gpb.ColumnDataType_TIMESTAMP_MILLISECOND},
	}
}
