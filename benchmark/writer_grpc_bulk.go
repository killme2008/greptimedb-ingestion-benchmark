package benchmark

import (
	"context"
	"fmt"
	"log"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/GreptimeTeam/greptimedb-ingester-go/bulk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCBulkWriter uses the Arrow Flight based BulkWrite API.
// Each worker goroutine gets its own BulkWriter (DoPut stream) via NewWorker,
// writes all batches through that single stream, and flushes on Close.
type GRPCBulkWriter struct {
	cfg       *Config
	tableName string
	schema    []*gpb.ColumnSchema

	// Per-worker fields, only set on instances returned by NewWorker.
	conn         *grpc.ClientConn
	bw           bulk.BulkWriter
	isWorker     bool
	expectedRows uint32
}

func (w *GRPCBulkWriter) Name() string { return "gRPC Bulk (Arrow)" }

func (w *GRPCBulkWriter) Setup(cfg *Config) error {
	w.cfg = cfg
	w.tableName = cfg.TableName
	w.schema = buildGRPCSchema()

	// BulkWrite (Arrow Flight DoPut) does not support auto-create tables.
	if _, err := execSQL(cfg, createTableDDL(cfg.TableName, "`")); err != nil {
		return fmt.Errorf("create table for bulk write: %w", err)
	}
	return nil
}

// NewWorker creates an independent writer with its own gRPC connection and BulkWriter stream.
func (w *GRPCBulkWriter) NewWorker() (Writer, error) {
	target := fmt.Sprintf("%s:4001", w.cfg.Host)
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	bc := bulk.NewBulkClient(conn)
	bw, err := bc.NewBulkWriter(context.Background())
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &GRPCBulkWriter{
		cfg:       w.cfg,
		tableName: w.tableName,
		schema:    w.schema,
		conn:      conn,
		bw:        bw,
		isWorker:  true,
	}, nil
}

func (w *GRPCBulkWriter) WriteBatch(_ context.Context, points []DataPoint) error {
	// ctx is unused: the underlying BulkWriter.Write (Arrow Flight DoPut) does
	// not accept a context — the stream lifecycle is managed by BulkWriter.Close.
	tbl, err := buildGRPCTable(w.tableName, w.schema, points)
	if err != nil {
		return err
	}
	if err := w.bw.Write(tbl); err != nil {
		return err
	}
	w.expectedRows += uint32(len(points))
	return nil
}

// Close flushes the BulkWriter stream, then closes the gRPC connection.
func (w *GRPCBulkWriter) Close() error {
	if w.bw != nil {
		rows, err := w.bw.Close()
		if err != nil {
			if w.conn != nil {
				_ = w.conn.Close()
			}
			return err
		}
		if rows != w.expectedRows {
			log.Printf("  [bulk] WARNING: server reported %d affected rows, expected %d", rows, w.expectedRows)
		}
	}
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}
