package benchmark

import (
	"context"
	"fmt"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
)

// GRPCBulkWriter uses the Arrow Flight based BulkWrite API.
// Each BulkWrite call creates an independent Flight stream, so this writer
// is safe for concurrent use — no need for WorkerWriter.
type GRPCBulkWriter struct {
	cli       *greptime.Client
	tableName string
	schema    []*gpb.ColumnSchema
}

func (w *GRPCBulkWriter) Name() string { return "gRPC Bulk (Arrow)" }

func (w *GRPCBulkWriter) Setup(cfg *Config) error {
	w.tableName = cfg.TableName
	w.schema = buildGRPCSchema()

	// BulkWrite (Arrow Flight DoPut) does not support auto-create tables.
	if _, err := execSQL(cfg, createTableDDL(cfg.TableName)); err != nil {
		return fmt.Errorf("create table for bulk write: %w", err)
	}

	cli, err := newGRPCClient(cfg)
	if err != nil {
		return err
	}
	w.cli = cli
	return nil
}

func (w *GRPCBulkWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	tbl, err := buildGRPCTable(w.tableName, w.schema, points)
	if err != nil {
		return err
	}
	_, err = w.cli.BulkWrite(context.Background(), tbl)
	return err
}

func (w *GRPCBulkWriter) Close() error {
	if w.cli != nil {
		return w.cli.Close()
	}
	return nil
}
