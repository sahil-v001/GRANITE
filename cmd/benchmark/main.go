package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"granite/internal/storage"
)

func main() {
	numOps := flag.Int("ops", 5000, "number of operations to benchmark")
	dataDir := flag.String("data", "./data/bench", "directory for benchmark storage")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	_ = os.RemoveAll(*dataDir)
	engine, err := storage.Open(*dataDir)
	if err != nil {
		slog.Error("Failed to open storage engine for benchmark", "error", err)
		os.Exit(1)
	}
	defer engine.Close()

	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  🪨 GRANITE Storage Engine — Performance Benchmark")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  Operations Target : %d ops\n", *numOps)
	fmt.Printf("  Data Directory    : %s\n", *dataDir)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()

	writeLatencies := make([]time.Duration, *numOps)
	startWrite := time.Now()

	for i := 0; i < *numOps; i++ {
		k := []byte(fmt.Sprintf("bench_key_%06d", i))
		v := []byte(fmt.Sprintf("bench_val_%06d", i))

		opStart := time.Now()
		if err := engine.Put(k, v); err != nil {
			slog.Error("Put failed", "error", err)
		}
		writeLatencies[i] = time.Since(opStart)
	}

	totalWriteTime := time.Since(startWrite)
	writeOPS := float64(*numOps) / totalWriteTime.Seconds()

	sortLatencies(writeLatencies)
	p50Write := writeLatencies[int(float64(*numOps)*0.50)]
	p95Write := writeLatencies[int(float64(*numOps)*0.95)]
	p99Write := writeLatencies[int(float64(*numOps)*0.99)]

	fmt.Println("  📝 WRITE PERFORMANCE (WAL fsync + MemTable):")
	fmt.Printf("     Total Time : %v\n", totalWriteTime)
	fmt.Printf("     Throughput : %.0f writes/sec (sequential disk fsync)\n", writeOPS)
	fmt.Printf("     Latency P50: %v\n", p50Write)
	fmt.Printf("     Latency P95: %v\n", p95Write)
	fmt.Printf("     Latency P99: %v\n", p99Write)
	fmt.Println()

	readLatencies := make([]time.Duration, *numOps)
	startRead := time.Now()

	for i := 0; i < *numOps; i++ {
		k := []byte(fmt.Sprintf("bench_key_%06d", i))

		opStart := time.Now()
		_, _ = engine.Get(k)
		readLatencies[i] = time.Since(opStart)
	}

	totalReadTime := time.Since(startRead)
	readOPS := float64(*numOps) / totalReadTime.Seconds()

	sortLatencies(readLatencies)
	p50Read := readLatencies[int(float64(*numOps)*0.50)]
	p95Read := readLatencies[int(float64(*numOps)*0.95)]
	p99Read := readLatencies[int(float64(*numOps)*0.99)]

	fmt.Println("  🔍 READ PERFORMANCE (MemTable RAM Cache):")
	fmt.Printf("     Total Time : %v\n", totalReadTime)
	fmt.Printf("     Throughput : %.0f reads/sec\n", readOPS)
	fmt.Printf("     Latency P50: %v\n", p50Read)
	fmt.Printf("     Latency P95: %v\n", p95Read)
	fmt.Printf("     Latency P99: %v\n", p99Read)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println()
}

func sortLatencies(lats []time.Duration) {
	sort.Slice(lats, func(i, j int) bool {
		return lats[i] < lats[j]
	})
}
