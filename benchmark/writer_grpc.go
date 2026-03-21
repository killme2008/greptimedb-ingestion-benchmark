package benchmark

import (
	"context"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
)

type GRPCWriter struct {
	cli       *greptime.Client
	tableName string
	// schema is built once in Setup and reused in every WriteBatch call,
	// avoiding per-batch AddTagColumn/AddFieldColumn/AddTimestampColumn overhead
	// (type conversion + name sanitization on every call).
	schema []*gpb.ColumnSchema
}

func (w *GRPCWriter) Name() string { return "gRPC SDK" }

func (w *GRPCWriter) Setup(cfg *Config) error {
	w.tableName = cfg.TableName

	grpcCfg := greptime.NewConfig(cfg.Host).
		WithPort(4001).
		WithDatabase(cfg.Database).
		WithAuth(cfg.User, cfg.Password)

	cli, err := greptime.NewClient(grpcCfg)
	if err != nil {
		return err
	}
	w.cli = cli

	// Build column schema once; WriteBatch reuses it via WithColumnsSchema.
	w.schema = buildGRPCSchema()
	return nil
}

func (w *GRPCWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
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

	_, err = w.cli.Write(ctx, tbl)
	return err
}

func (w *GRPCWriter) Close() error {
	if w.cli != nil {
		return w.cli.Close()
	}
	return nil
}
