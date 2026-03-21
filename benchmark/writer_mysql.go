package benchmark

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLWriter struct {
	db        *sql.DB
	tableName string
	batchStmt *sql.Stmt
	batchSize int
}

func (w *MySQLWriter) Name() string { return "MySQL INSERT" }

func (w *MySQLWriter) Setup(cfg *Config) error {
	w.tableName = cfg.TableName
	w.batchSize = cfg.BatchSize

	dsn := fmt.Sprintf("%s:%s@tcp(%s:4002)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	// Match pool size to concurrency to avoid connection churn.
	// Default MaxIdleConns=2 would cause frequent close/reopen under load.
	db.SetMaxOpenConns(cfg.Concurrency)
	db.SetMaxIdleConns(cfg.Concurrency)
	w.db = db

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
	if _, err = w.db.ExecContext(context.Background(), createTable); err != nil {
		return err
	}

	// Prepare the multi-row INSERT for the standard batch size.
	// database/sql lazily prepares on each pooled connection as needed.
	query := buildMySQLInsert(w.tableName, cfg.BatchSize)
	stmt, err := w.db.PrepareContext(context.Background(), query)
	if err != nil {
		return fmt.Errorf("prepare batch insert: %w", err)
	}
	w.batchStmt = stmt
	return nil
}

func (w *MySQLWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	if len(points) == 0 {
		return nil
	}

	args := make([]interface{}, 0, len(points)*8)
	for _, p := range points {
		args = append(args, p.Host, p.Region, p.CPU, p.Memory, p.DiskUtil, p.NetIn, p.NetOut, p.Timestamp)
	}

	// Use the cached prepared statement for standard-size batches.
	if len(points) == w.batchSize {
		_, err := w.batchStmt.ExecContext(ctx, args...)
		return err
	}

	// Fall back to unprepared exec for the last (smaller) batch.
	query := buildMySQLInsert(w.tableName, len(points))
	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

func (w *MySQLWriter) Close() error {
	if w.batchStmt != nil {
		_ = w.batchStmt.Close()
	}
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}

func buildMySQLInsert(tableName string, numRows int) string {
	const cols = "host, cloud_region, cpu, memory, disk_util, net_in, net_out, ts"
	row := "(?, ?, ?, ?, ?, ?, ?, ?)"
	rows := make([]string, numRows)
	for i := range rows {
		rows[i] = row
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName, cols, strings.Join(rows, ", "))
}
