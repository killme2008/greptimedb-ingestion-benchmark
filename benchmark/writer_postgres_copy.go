package benchmark

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// pgCopyTimestampLayout renders timestamps the way GreptimeDB's CSV decoder
// (arrow-csv) accepts them: space-separated date and time with a fractional
// part matching the TIMESTAMP(3) column precision.
const pgCopyTimestampLayout = "2006-01-02 15:04:05.000"

// PostgresCopyWriter streams batches as CSV over the PostgreSQL COPY FROM
// STDIN subprotocol (`COPY <table> FROM STDIN WITH (FORMAT csv)`), using the
// raw pgconn streaming API instead of pgx's high-level CopyFrom (which uses
// the binary COPY format, not supported by GreptimeDB).
//
// A COPY stream owns its connection for the duration of the copy, so each
// worker goroutine gets a dedicated connection via NewWorker — one connection
// per worker, the same sizing as the pgxpool-based PostgreSQL INSERT writer.
type PostgresCopyWriter struct {
	cfg     *Config
	copySQL string

	// Per-worker fields, only set on instances returned by NewWorker.
	conn *pgx.Conn
	buf  bytes.Buffer
}

func (w *PostgresCopyWriter) Name() string { return "PostgreSQL COPY" }

func (w *PostgresCopyWriter) Setup(cfg *Config) error {
	w.cfg = cfg
	w.copySQL = fmt.Sprintf("COPY %s FROM STDIN WITH (FORMAT csv)", cfg.TableName)

	// COPY has no auto-create: the table must exist before the stream starts.
	// Create it over a short-lived connection; workers open their own.
	conn, err := connectPostgres(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	_, err = conn.Exec(context.Background(), createTableDDL(cfg.TableName, `"`))
	return err
}

// NewWorker creates an independent writer with its own connection and
// encoding buffer.
func (w *PostgresCopyWriter) NewWorker() (Writer, error) {
	conn, err := connectPostgres(w.cfg)
	if err != nil {
		return nil, err
	}
	return &PostgresCopyWriter{
		cfg:     w.cfg,
		copySQL: w.copySQL,
		conn:    conn,
	}, nil
}

func (w *PostgresCopyWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	if len(points) == 0 {
		return nil
	}

	// Encode the batch as CSV in table column order: RFC 4180 quoting
	// (matching arrow-csv's expectations on the server side).
	w.buf.Reset()
	cw := csv.NewWriter(&w.buf)
	record := make([]string, 0, numCols)
	for _, p := range points {
		record = record[:0]
		record = append(record,
			p.Host, p.Region, p.Datacenter, p.Service,
			strconv.FormatFloat(p.CPU, 'g', -1, 64),
			strconv.FormatFloat(p.Memory, 'g', -1, 64),
			strconv.FormatFloat(p.DiskUtil, 'g', -1, 64),
			strconv.FormatFloat(p.NetIn, 'g', -1, 64),
			strconv.FormatFloat(p.NetOut, 'g', -1, 64),
			p.Timestamp.Format(pgCopyTimestampLayout),
		)
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}

	// Stream the buffer to the server; the connection is exclusively owned
	// by this copy for its duration and free again once it returns.
	tag, err := w.conn.PgConn().CopyFrom(ctx, &w.buf, w.copySQL)
	if err != nil {
		return err
	}
	// GreptimeDB acknowledges the exact written row count; treat a mismatch
	// as a failure so the benchmark reports it instead of silently losing rows.
	if tag.RowsAffected() != int64(len(points)) {
		return fmt.Errorf("%s: server acknowledged %d rows, sent %d",
			w.Name(), tag.RowsAffected(), len(points))
	}
	return nil
}

func (w *PostgresCopyWriter) Close() error {
	if w.conn != nil {
		return w.conn.Close(context.Background())
	}
	return nil
}

// connectPostgres opens a single PostgreSQL protocol connection on port 4003.
func connectPostgres(cfg *Config) (*pgx.Conn, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:4003/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Database)
	return pgx.Connect(context.Background(), dsn)
}
