// Command bench is a small concurrent load generator for mini-redis-go. It
// exists so this project's throughput/latency numbers are reproducible by
// anyone who clones the repo, without requiring a separate install of real
// Redis's redis-benchmark. It reuses the exact same resp package the
// server, AOF, and replication code all use — the RESP wire encoding is
// written once in this project, not reimplemented per consumer.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Breyes05/mini-redis-go/internal/resp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	concurrency := flag.Int("c", 50, "number of concurrent connections")
	total := flag.Int("n", 100000, "total requests to issue across all connections")
	keyspace := flag.Int("keyspace", 10000, "number of distinct keys to cycle through")
	getRatio := flag.Float64("get-ratio", 0.5, "fraction of requests that are GET (rest are SET)")
	valueSize := flag.Int("valuesize", 64, "size in bytes of the value used for SET")
	flag.Parse()

	perWorker := *total / *concurrency
	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = 'x'
	}
	valueStr := string(value)

	var wg sync.WaitGroup
	latencies := make([][]time.Duration, *concurrency)
	errCounts := make([]int, *concurrency)

	start := time.Now()
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			latencies[w], errCounts[w] = runWorker(*addr, perWorker, *keyspace, *getRatio, valueStr, w)
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []time.Duration
	totalErrs := 0
	for i, lat := range latencies {
		all = append(all, lat...)
		totalErrs += errCounts[i]
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	n := len(all)
	if n == 0 {
		log.Fatal("no successful requests completed — is the server running at ", *addr, "?")
	}
	opsPerSec := float64(n) / elapsed.Seconds()

	fmt.Printf("requests:    %d (%d errors)\n", n, totalErrs)
	fmt.Printf("concurrency: %d\n", *concurrency)
	fmt.Printf("duration:    %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:  %.0f ops/sec\n", opsPerSec)
	fmt.Printf("latency p50: %s\n", percentile(all, 0.50))
	fmt.Printf("latency p95: %s\n", percentile(all, 0.95))
	fmt.Printf("latency p99: %s\n", percentile(all, 0.99))
	fmt.Printf("latency max: %s\n", all[n-1])
}

// percentile returns the value at rank p (0..1) of a slice already sorted
// ascending. Simple nearest-rank method — fine for a benchmark tool
// reporting to two sig figs, not a statistics library.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// runWorker opens one connection and issues n requests sequentially over
// it (matching how a single client actually talks to the server: one
// request, wait for the reply, then the next — no pipelining), recording
// the round-trip latency of each.
func runWorker(addr string, n, keyspace int, getRatio float64, value string, seed int) ([]time.Duration, int) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("worker %d: dial: %v", seed, err)
		return nil, n
	}
	defer conn.Close()

	r := resp.NewReader(bufio.NewReader(conn))
	lat := make([]time.Duration, 0, n)
	errCount := 0
	rng := rand.New(rand.NewSource(int64(seed)*7919 + time.Now().UnixNano()))

	for i := 0; i < n; i++ {
		key := "bench:" + strconv.Itoa(rng.Intn(keyspace))
		var args []string
		if rng.Float64() < getRatio {
			args = []string{"GET", key}
		} else {
			args = []string{"SET", key, value}
		}

		start := time.Now()
		if _, err := conn.Write(resp.EncodeCommand(args)); err != nil {
			errCount++
			continue
		}
		v, err := r.ReadValue()
		if err != nil {
			errCount++
			continue
		}
		if v.Type == resp.Error {
			errCount++
			continue
		}
		lat = append(lat, time.Since(start))
	}
	return lat, errCount
}
