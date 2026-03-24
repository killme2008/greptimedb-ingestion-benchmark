package benchmark

import (
	"fmt"
	"math/rand"
	"time"
)

type DataPoint struct {
	Host       string
	Region     string
	Datacenter string
	Service    string
	CPU        float64
	Memory     float64
	DiskUtil   float64
	NetIn      float64
	NetOut     float64
	Timestamp  time.Time
}

// generateHosts creates a slice of host names with the given count.
func generateHosts(n int) []string {
	hosts := make([]string, n)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("host-%d", i)
	}
	return hosts
}

var regions = []string{
	"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ap-northeast-1",
}

var datacenters = []string{
	"dc-0", "dc-1", "dc-2", "dc-3", "dc-4",
	"dc-5", "dc-6", "dc-7", "dc-8", "dc-9",
}

var services = []string{
	"svc-00", "svc-01", "svc-02", "svc-03", "svc-04",
	"svc-05", "svc-06", "svc-07", "svc-08", "svc-09",
	"svc-10", "svc-11", "svc-12", "svc-13", "svc-14",
	"svc-15", "svc-16", "svc-17", "svc-18", "svc-19",
}

// SeriesCount returns the total number of unique time series for the given host count.
// series = numHosts × 5 regions × 10 datacenters × 20 services
func SeriesCount(numHosts int) int {
	return numHosts * len(regions) * len(datacenters) * len(services)
}

// GenerateData produces totalRows data points with deterministic values.
// Data is distributed round-robin across all series combinations.
func GenerateData(totalRows int, numHosts int, seed int64) []DataPoint {
	rng := rand.New(rand.NewSource(seed))
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	hosts := generateHosts(numHosts)
	seriesCount := SeriesCount(numHosts)

	points := make([]DataPoint, totalRows)
	for i := range points {
		seriesIdx := i % seriesCount
		hostIdx := seriesIdx / (len(regions) * len(datacenters) * len(services))
		rem := seriesIdx % (len(regions) * len(datacenters) * len(services))
		regionIdx := rem / (len(datacenters) * len(services))
		rem = rem % (len(datacenters) * len(services))
		dcIdx := rem / len(services)
		svcIdx := rem % len(services)

		points[i] = DataPoint{
			Host:       hosts[hostIdx],
			Region:     regions[regionIdx],
			Datacenter: datacenters[dcIdx],
			Service:    services[svcIdx],
			CPU:        rng.Float64() * 100,
			Memory:     rng.Float64() * 100,
			DiskUtil:   rng.Float64() * 100,
			NetIn:      rng.Float64() * 1e9, // up to 1 Gbps
			NetOut:     rng.Float64() * 1e9,
			Timestamp:  baseTime.Add(time.Duration(i) * time.Millisecond),
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
