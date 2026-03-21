package benchmark

import (
	"context"
	"fmt"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
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
	createTable := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		host STRING,
		cloud_region STRING,
		cpu DOUBLE,
		memory DOUBLE,
		disk_util DOUBLE,
		net_in DOUBLE,
		net_out DOUBLE,
		ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP TIME INDEX,
		PRIMARY KEY (host, cloud_region)
	) ENGINE=mito`, cfg.TableName)
	if _, err := execSQL(cfg, createTable); err != nil {
		return fmt.Errorf("create table for bulk write: %w", err)
	}

	grpcCfg := greptime.NewConfig(cfg.Host).
		WithPort(4001).
		WithDatabase(cfg.Database).
		WithAuth(cfg.User, cfg.Password)

	cli, err := greptime.NewClient(grpcCfg)
	if err != nil {
		return err
	}
	w.cli = cli
	return nil
}

func (w *GRPCBulkWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
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

	_, err = w.cli.BulkWrite(ctx, tbl)
	return err
}

func (w *GRPCBulkWriter) Close() error {
	if w.cli != nil {
		return w.cli.Close()
	}
	return nil
}
