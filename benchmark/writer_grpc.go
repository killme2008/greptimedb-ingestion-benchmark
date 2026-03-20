package benchmark

import (
	"context"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
)

type GRPCWriter struct {
	cli       *greptime.Client
	tableName string
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
	return nil
}

func (w *GRPCWriter) WriteBatch(points []DataPoint) error {
	tbl, err := table.New(w.tableName)
	if err != nil {
		return err
	}

	if err := tbl.AddTagColumn("host", types.STRING); err != nil {
		return err
	}
	if err := tbl.AddTagColumn("cloud_region", types.STRING); err != nil {
		return err
	}
	if err := tbl.AddFieldColumn("cpu", types.FLOAT64); err != nil {
		return err
	}
	if err := tbl.AddFieldColumn("memory", types.FLOAT64); err != nil {
		return err
	}
	if err := tbl.AddFieldColumn("disk_util", types.FLOAT64); err != nil {
		return err
	}
	if err := tbl.AddFieldColumn("net_in", types.FLOAT64); err != nil {
		return err
	}
	if err := tbl.AddFieldColumn("net_out", types.FLOAT64); err != nil {
		return err
	}
	if err := tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND); err != nil {
		return err
	}

	for _, p := range points {
		if err := tbl.AddRow(p.Host, p.Region, p.CPU, p.Memory, p.DiskUtil, p.NetIn, p.NetOut, p.Timestamp); err != nil {
			return err
		}
	}

	_, err = w.cli.Write(context.Background(), tbl)
	return err
}

func (w *GRPCWriter) Close() error {
	if w.cli != nil {
		return w.cli.Close()
	}
	return nil
}
