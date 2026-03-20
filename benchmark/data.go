package benchmark

import (
	"math/rand"
	"time"
)

type DataPoint struct {
	Host      string
	Region    string
	CPU       float64
	Memory    float64
	DiskUtil  float64
	NetIn     float64
	NetOut    float64
	Timestamp time.Time
}

var hosts = []string{
	"host-0", "host-1", "host-2", "host-3", "host-4",
	"host-5", "host-6", "host-7", "host-8", "host-9",
}

var regions = []string{
	"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1",
}

// GenerateData produces totalRows data points with deterministic values.
// Data is distributed round-robin across all host×region combinations.
func GenerateData(totalRows int, seed int64) []DataPoint {
	rng := rand.New(rand.NewSource(seed))
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	seriesCount := len(hosts) * len(regions) // 50

	points := make([]DataPoint, totalRows)
	for i := range points {
		seriesIdx := i % seriesCount
		hostIdx := seriesIdx / len(regions)
		regionIdx := seriesIdx % len(regions)

		points[i] = DataPoint{
			Host:      hosts[hostIdx],
			Region:    regions[regionIdx],
			CPU:       rng.Float64() * 100,
			Memory:    rng.Float64() * 100,
			DiskUtil:  rng.Float64() * 100,
			NetIn:     rng.Float64() * 1e9, // up to 1 Gbps
			NetOut:    rng.Float64() * 1e9,
			Timestamp: baseTime.Add(time.Duration(i) * time.Millisecond),
		}
	}

	return points
}

// SplitBatches divides points into fixed-size batches.
func SplitBatches(points []DataPoint, batchSize int) [][]DataPoint {
	var batches [][]DataPoint
	for i := 0; i < len(points); i += batchSize {
		end := i + batchSize
		if end > len(points) {
			end = len(points)
		}
		batches = append(batches, points[i:end])
	}
	return batches
}
