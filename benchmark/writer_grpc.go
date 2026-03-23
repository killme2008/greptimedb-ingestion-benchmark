package benchmark

import (
	"context"
	"fmt"

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
	w.schema = buildGRPCSchema()

	cli, err := newGRPCClient(cfg)
	if err != nil {
		return err
	}
	w.cli = cli
	return nil
}

func (w *GRPCWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	tbl, err := buildGRPCTable(w.tableName, w.schema, points)
	if err != nil {
		return err
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

// --- Shared helpers for all gRPC-based writers ---

// createTableDDL returns the CREATE TABLE IF NOT EXISTS statement for the
// benchmark table. Used by writers whose protocol does not support auto-create.
// quote is the identifier quoting character: backtick for MySQL/HTTP SQL, double-quote for PostgreSQL.
func createTableDDL(tableName string, quote string) string {
	q := func(name string) string { return quote + name + quote }
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"host STRING, "+
		"%s STRING, "+
		"datacenter STRING, "+
		"%s STRING, "+
		"cpu DOUBLE, "+
		"memory DOUBLE, "+
		"disk_util DOUBLE, "+
		"net_in DOUBLE, "+
		"net_out DOUBLE, "+
		"ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP TIME INDEX, "+
		"PRIMARY KEY (host, %s, datacenter, %s)"+
		") ENGINE=mito", tableName, q("region"), q("service"), q("region"), q("service"))
}

// newGRPCClient creates a greptime Client with standard connection settings.
func newGRPCClient(cfg *Config) (*greptime.Client, error) {
	grpcCfg := greptime.NewConfig(cfg.Host).
		WithPort(4001).
		WithDatabase(cfg.Database).
		WithAuth(cfg.User, cfg.Password)
	return greptime.NewClient(grpcCfg)
}

// buildGRPCSchema returns the shared column schema used by all gRPC-based writers.
func buildGRPCSchema() []*gpb.ColumnSchema {
	return []*gpb.ColumnSchema{
		{ColumnName: "host", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "region", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "datacenter", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "service", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "cpu", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "memory", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "disk_util", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "net_in", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "net_out", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_FLOAT64},
		{ColumnName: "ts", SemanticType: gpb.SemanticType_TIMESTAMP, Datatype: gpb.ColumnDataType_TIMESTAMP_MILLISECOND},
	}
}

// buildGRPCTable creates a Table populated with the given data points.
func buildGRPCTable(tableName string, schema []*gpb.ColumnSchema, points []DataPoint) (*table.Table, error) {
	tbl, err := table.New(tableName)
	if err != nil {
		return nil, err
	}
	tbl.WithColumnsSchema(schema)

	for _, p := range points {
		if err := tbl.AddRow(p.Host, p.Region, p.Datacenter, p.Service, p.CPU, p.Memory, p.DiskUtil, p.NetIn, p.NetOut, p.Timestamp); err != nil {
			return nil, err
		}
	}
	return tbl, nil
}
