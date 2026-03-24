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
	// stmtName is the name of the prepared statement for standard batch size.
	stmtName string
}

func (w *PostgresWriter) Name() string { return "PostgreSQL INSERT" }

func (w *PostgresWriter) Setup(cfg *Config) error {
	w.tableName = cfg.TableName
	w.batchSize = cfg.BatchSize
	w.batchQuery = buildPgInsert(cfg.TableName, cfg.BatchSize)
	w.stmtName = "batch_insert"

	dsn := fmt.Sprintf("postgres://%s:%s@%s:4003/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Database)

	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return err
	}
	_, err = conn.Exec(context.Background(), createTableDDL(w.tableName, `"`))
	conn.Close(context.Background())
	if err != nil {
		return err
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}

	paramTypes := make([]uint32, cfg.BatchSize*numCols)
	for i := 0; i < cfg.BatchSize; i++ {
		base := i * numCols
		paramTypes[base+0] = 25
		paramTypes[base+1] = 25
		paramTypes[base+2] = 25
		paramTypes[base+3] = 25
		paramTypes[base+4] = 701
		paramTypes[base+5] = 701
		paramTypes[base+6] = 701
		paramTypes[base+7] = 701
		paramTypes[base+8] = 701
		paramTypes[base+9] = 1184
	}

	batchQuery := w.batchQuery
	stmtName := w.stmtName
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.PgConn().Prepare(ctx, stmtName, batchQuery, paramTypes)
		return err
	}

	poolCfg.MaxConns = int32(cfg.Concurrency)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return err
	}
	w.pool = pool

	return nil
}

func (w *PostgresWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	if len(points) == 0 {
		return nil
	}

	args := make([]any, 0, len(points)*numCols)
	for _, p := range points {
		args = append(args, p.Host, p.Region, p.Datacenter, p.Service, p.CPU, p.Memory, p.DiskUtil, p.NetIn, p.NetOut, p.Timestamp)
	}

	if len(points) == w.batchSize {
		conn, err := w.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		defer conn.Release()

		pgConn := conn.Conn().PgConn()
		paramValues := make([][]byte, len(args))
		typeMap := conn.Conn().TypeMap()
		paramOIDs := []uint32{25, 25, 25, 25, 701, 701, 701, 701, 701, 1184}
		for i, arg := range args {
			oid := paramOIDs[i%numCols]
			paramValues[i], err = typeMap.Encode(oid, 0, arg, nil)
			if err != nil {
				return err
			}
		}

		result := pgConn.ExecPrepared(ctx, w.stmtName, paramValues, nil, nil)
		_, err = result.Close()
		return err
	}

	query := buildPgInsert(w.tableName, len(points))
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
