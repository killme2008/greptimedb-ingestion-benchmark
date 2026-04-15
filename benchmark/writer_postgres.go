package benchmark

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresWriter struct {
	pool      *pgxpool.Pool
	tableName string
	batchSize int
	// batchQuery caches the INSERT query text for the standard batch size,
	// avoiding repeated string construction on every WriteBatch call.
	batchQuery string
}

func (w *PostgresWriter) Name() string { return "PostgreSQL INSERT" }

func (w *PostgresWriter) Setup(cfg *Config) error {
	w.tableName = cfg.TableName
	w.batchSize = cfg.BatchSize
	w.batchQuery = buildPgInsert(cfg.TableName, cfg.BatchSize)

	dsn := fmt.Sprintf("postgres://%s:%s@%s:4003/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Database)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	// Use CacheStatement mode to cache prepared statements on the client side,
	// reducing Parse round-trips for repeated queries.
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	// Match pool size to concurrency to avoid connection wait under load.
	poolCfg.MaxConns = int32(cfg.Concurrency)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return err
	}
	w.pool = pool

	_, err = w.pool.Exec(context.Background(), createTableDDL(w.tableName, `"`), pgx.QueryExecModeExec)
	return err
}

func (w *PostgresWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	if len(points) == 0 {
		return nil
	}

	args := make([]any, 0, len(points)*numCols)
	for _, p := range points {
		args = append(args, p.Host, p.Region, p.Datacenter, p.Service, p.CPU, p.Memory, p.DiskUtil, p.NetIn, p.NetOut, p.Timestamp)
	}

	// Use cached query text for standard-size batches.
	query := w.batchQuery
	if len(points) != w.batchSize {
		query = buildPgInsert(w.tableName, len(points))
	}

	_, err := w.pool.Exec(ctx, query, args...)
	return err
}

func (w *PostgresWriter) Close() error {
	if w.pool != nil {
		w.pool.Close()
	}
	return nil
}

func buildPgInsert(tableName string, numRows int) string {
	rows := make([]string, numRows)
	for i := range rows {
		base := i*numCols + 1
		rows[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9)
	}
	return fmt.Sprintf(
		`INSERT INTO %s (host, "region", datacenter, "service", cpu, memory, disk_util, net_in, net_out, ts) VALUES %s`,
		tableName, strings.Join(rows, ", "))
}
