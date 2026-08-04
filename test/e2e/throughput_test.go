//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"codakv/test/conformance"
)

// TestThroughput measures sustained write throughput against a live stack.
//
// This is the honest form of the assignment's "throughput increases
// horizontally" claim. An in-process comparison cannot show it, because every
// simulated node shares one machine's CPU and adding nodes therefore adds no
// capacity. The compose files pin each node to 0.5 CPU, so a three-node stack
// genuinely has three times the compute of a one-node stack, and the difference
// between the two runs is a real measurement rather than an argument.
//
//	make up-part1 && make throughput   # 1 node,  0.5 CPU
//	make up-part2 && make throughput   # 3 nodes, 1.5 CPU total
func TestThroughput(t *testing.T) {
	base := baseURL(t)

	clients := envInt("KV_BENCH_CLIENTS", 32)
	writes := envInt("KV_BENCH_WRITES", 400)
	total := clients * writes

	// Warm the connection pools so TCP setup stays outside the timed section.
	warm := conformance.NewClient(base)
	for i := 0; i < 100; i++ {
		if _, err := warm.Put(fmt.Sprintf("warm-%d", i), `1`); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, clients)
	start := time.Now()

	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			client := conformance.NewClient(base)
			for i := 0; i < writes; i++ {
				resp, err := client.Put(fmt.Sprintf("bench-%d-%d-%d", start.UnixNano(), c, i), `1`)
				if err != nil {
					errs <- err
					return
				}
				if resp.Status != http.StatusOK {
					errs <- fmt.Errorf("status %d: %s", resp.Status, resp.Raw)
					return
				}
			}
		}(c)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	for err := range errs {
		t.Fatalf("write failed: %v", err)
	}

	perSecond := float64(total) / elapsed.Seconds()

	t.Logf("")
	t.Logf("  %d clients x %d writes = %d writes", clients, writes, total)
	t.Logf("  elapsed:    %v", elapsed.Round(time.Millisecond))
	t.Logf("  throughput: %.0f writes/sec", perSecond)
	t.Logf("")
	t.Logf("  Run this against compose.part1 (1 node, 0.5 CPU) and compose.part2")
	t.Logf("  (3 nodes, 0.5 CPU each) to compare. Each node is CPU-capped, so the")
	t.Logf("  three-node stack really does have three times the compute.")
}

func envInt(key string, fallback int) int {
	if raw := os.Getenv(key); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}
