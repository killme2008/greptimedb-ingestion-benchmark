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
	// so CacheStatement mode can match and skip re-parsing.
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
	// QueryExecModeExec: sends Parse(unnamed)+Bind+Execute each time.
	// GreptimeDB does not support the Describe message required by
	// CacheStatement/CacheDescribe modes, so prepared statement caching
	// is not possible via pgx — every batch incurs a Parse round-trip.
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return err
	}
	w.pool = pool

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
	) ENGINE=mito`, w.tableName)
	// DDL uses Exec mode — not worth caching, and avoids Describe on DDL.
	_, err = w.pool.Exec(context.Background(), createTable, pgx.QueryExecModeExec)
	return err
}

func (w *PostgresWriter) WriteBatch(points []DataPoint) error {
	if len(points) == 0 {
		return nil
	}

	const numCols = 8
	args := make([]any, 0, len(points)*numCols)
	for _, p := range points {
		args = append(args, p.Host, p.Region, p.CPU, p.Memory, p.DiskUtil, p.NetIn, p.NetOut, p.Timestamp)
	}

	// Use cached query for standard-size batches (benefits from CacheStatement).
	query := w.batchQuery
	if len(points) != w.batchSize {
		query = buildPgInsert(w.tableName, len(points))
	}

	_, err := w.pool.Exec(context.Background(), query, args...)
	return err
}

func (w *PostgresWriter) Close() error {
	if w.pool != nil {
		w.pool.Close()
	}
	return nil
}

func buildPgInsert(tableName string, numRows int) string {
	const numCols = 8
	rows := make([]string, numRows)
	for i := range rows {
		base := i*numCols + 1
		rows[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7)
	}
	return fmt.Sprintf(
		"INSERT INTO %s (host, cloud_region, cpu, memory, disk_util, net_in, net_out, ts) VALUES %s",
		tableName, strings.Join(rows, ", "))
}
