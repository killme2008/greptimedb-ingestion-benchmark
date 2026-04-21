package benchmark

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"time"

	logapi "go.opentelemetry.io/otel/log"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// OTelWriter ingests data as OTLP log records by design.
// This deliberately uses the log data model (not metrics) because GreptimeDB's
// OTLP log ingestion is a distinct pathway worth benchmarking separately.
// The comparison with other protocols reflects real-world ingestion diversity,
// not a 1:1 schema-equivalent test.
type OTelWriter struct {
	exporter *otlploghttp.Exporter
}

func (w *OTelWriter) Name() string { return "OTLP Logs (HTTP)" }

func (w *OTelWriter) Setup(cfg *Config) error {
	ctx := context.Background()

	headers := map[string]string{
		"X-Greptime-DB-Name":           cfg.Database,
		"X-Greptime-Log-Table-Name":    cfg.TableName,
		"X-Greptime-Log-Pipeline-Name": "greptime_identity",
	}
	if cfg.User != "" || cfg.Password != "" {
		creds := base64.StdEncoding.EncodeToString([]byte(cfg.User + ":" + cfg.Password))
		headers["Authorization"] = "Basic " + creds
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			MaxIdleConns:        cfg.Concurrency + 2,
			MaxIdleConnsPerHost: cfg.Concurrency + 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	exporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(fmt.Sprintf("http://%s:4000/v1/otlp/v1/logs", cfg.Host)),
		otlploghttp.WithHeaders(headers),
		otlploghttp.WithHTTPClient(httpClient),
	)
	if err != nil {
		return fmt.Errorf("create otlp log exporter: %w", err)
	}

	w.exporter = exporter
	return nil
}

func (w *OTelWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	records := make([]sdklog.Record, len(points))
	for i, p := range points {
		records[i].SetTimestamp(p.Timestamp)
		records[i].SetBody(logapi.StringValue("benchmark"))
		records[i].SetSeverity(logapi.SeverityInfo)
		records[i].AddAttributes(
			logapi.String("host", p.Host),
			logapi.String("region", p.Region),
			logapi.String("datacenter", p.Datacenter),
			logapi.String("service", p.Service),
			logapi.Float64("cpu", p.CPU),
			logapi.Float64("memory", p.Memory),
			logapi.Float64("disk_util", p.DiskUtil),
			logapi.Float64("net_in", p.NetIn),
			logapi.Float64("net_out", p.NetOut),
		)
	}
	return w.exporter.Export(ctx, records)
}

func (w *OTelWriter) Close() error {
	if w.exporter != nil {
		return w.exporter.Shutdown(context.Background())
	}
	return nil
}
