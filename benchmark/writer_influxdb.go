package benchmark

import (
	"context"
	"fmt"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

type InfluxDBWriter struct {
	client   influxdb2.Client
	writeAPI interface {
		WritePoint(ctx context.Context, point ...*write.Point) error
	}
	tableName string
}

func (w *InfluxDBWriter) Name() string { return "InfluxDB LP" }

func (w *InfluxDBWriter) Setup(cfg *Config) error {
	w.tableName = cfg.TableName

	// GreptimeDB serves InfluxDB v2 API under /v1/influxdb prefix.
	// The influxdb2 Go client appends /api/v2/write internally.
	serverURL := fmt.Sprintf("http://%s:4000/v1/influxdb", cfg.Host)
	// The token format is "username:password" for GreptimeDB.
	token := ""
	if cfg.User != "" || cfg.Password != "" {
		token = cfg.User + ":" + cfg.Password
	}

	w.client = influxdb2.NewClient(serverURL, token)
	w.writeAPI = w.client.WriteAPIBlocking("", cfg.Database)
	return nil
}

func (w *InfluxDBWriter) WriteBatch(ctx context.Context, points []DataPoint) error {
	influxPoints := make([]*write.Point, len(points))
	for i, p := range points {
		influxPoints[i] = influxdb2.NewPoint(
			w.tableName,
			map[string]string{
				"host":       p.Host,
				"region":     p.Region,
				"datacenter": p.Datacenter,
				"service":    p.Service,
			},
			map[string]interface{}{
				"cpu":       p.CPU,
				"memory":    p.Memory,
				"disk_util": p.DiskUtil,
				"net_in":    p.NetIn,
				"net_out":   p.NetOut,
			},
			p.Timestamp,
		)
	}

	return w.writeAPI.WritePoint(ctx, influxPoints...)
}

func (w *InfluxDBWriter) Close() error {
	if w.client != nil {
		w.client.Close()
	}
	return nil
}
