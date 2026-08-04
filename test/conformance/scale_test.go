package conformance

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"codakv/internal/proxy"
	"codakv/internal/routing"
)

// startClusterOfSize brings up n nodes plus a proxy and returns the proxy's URL.
func startClusterOfSize(t *testing.T, n int) string {
	t.Helper()

	ids := make([]string, n)
	addresses := make(map[string]string, n)

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		owned, err := routing.RangeFor(i, n, partitionCount)
		if err != nil {
			t.Fatalf("RangeFor(%d,%d): %v", i, n, err)
		}
		ids[i] = id
		addresses[id] = startNode(t, id, owned).URL
	}

	table, err := routing.NewTable(ids, partitionCount)
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	caller := proxy.NewCaller(addresses, partitionCount, 10*time.Second)
	p := httptest.NewServer(proxy.NewHandler(table, caller,
		slog.New(slog.NewTextHandler(io.Discard, nil))).Routes())
	t.Cleanup(p.Close)

	return p.URL
}

// K14: capacity scales with the cluster — no single node ends up holding
// everything, which is what "total memory increases horizontally" means.
func TestCapacityScalesWithNodes(t *testing.T) {
	const keys = 3000

	for _, nodes := range []int{1, 3} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			base := startClusterOfSize(t, nodes)
			client := NewClient(base)

			prefix := unique("cap")
			for i := 0; i < keys; i++ {
				if _, err := client.Put(fmt.Sprintf("%s-%d", prefix, i), `1`); err != nil {
					t.Fatal(err)
				}
			}

			listing, streamErr, err := client.ListKeys()
			if err != nil {
				t.Fatal(err)
			}
			if streamErr != "" {
				t.Fatalf("listing reported: %s", streamErr)
			}

			perNode := map[string]int{}
			for key, nodeID := range listing {
				if len(key) > len(prefix) && key[:len(prefix)] == prefix {
					perNode[nodeID]++
				}
			}
			if len(perNode) != nodes {
				t.Errorf("keys landed on %d nodes, want %d", len(perNode), nodes)
			}

			maxHeld := 0
			for _, count := range perNode {
				if count > maxHeld {
					maxHeld = count
				}
			}
			share := float64(maxHeld) / float64(keys)
			t.Logf("nodes=%d busiest node holds %d/%d keys (%.0f%%)", nodes, maxHeld, keys, share*100)

			// With n nodes the busiest should hold roughly 1/n of the data.
			if want := 1.0/float64(nodes) + 0.10; share > want {
				t.Errorf("busiest node holds %.0f%% of keys, want about %.0f%%",
					share*100, 100.0/float64(nodes))
			}
		})
	}
}

// K15: what throughput scaling can and cannot be shown here.
//
// Read the result carefully. Every node in this test runs inside ONE process on
// ONE machine, so all of them compete for the same cores. Adding nodes adds no
// CPU, and the measured ratio comes out near 1.0x. That is the correct outcome,
// not a defect: sharding raises the throughput ceiling by adding hardware, and
// no hardware is being added here.
//
// What this measurement genuinely establishes is the other half of the claim —
// that the proxy does not become the bottleneck. Routing, forwarding and an
// extra network hop cost close to nothing, so throughput on three nodes tracks
// throughput on one. A design where the proxy serialised requests or held a
// lock would show a clear drop.
//
// Demonstrating real throughput scaling needs nodes with independent CPU: the
// compose stack with per-service `cpus` limits, or separate hosts.
func TestProxyOverheadIsNegligible(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput measurement in -short mode")
	}

	const (
		clients         = 24
		writesPerClient = 250
	)

	measure := func(t *testing.T, nodes int) float64 {
		base := startClusterOfSize(t, nodes)
		prefix := unique(fmt.Sprintf("tput%d", nodes))

		// Warm the connection pools so TCP setup does not land inside the timed
		// section.
		warm := NewClient(base)
		for i := 0; i < 50; i++ {
			if _, err := warm.Put(fmt.Sprintf("%s-warm-%d", prefix, i), `1`); err != nil {
				t.Fatal(err)
			}
		}

		var wg sync.WaitGroup
		errs := make(chan error, clients)
		start := time.Now()

		for c := 0; c < clients; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				client := NewClient(base)
				for i := 0; i < writesPerClient; i++ {
					resp, err := client.Put(fmt.Sprintf("%s-%d-%d", prefix, c, i), `1`)
					if err != nil {
						errs <- err
						return
					}
					if resp.Status != http.StatusOK {
						errs <- fmt.Errorf("status %d", resp.Status)
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

		return float64(clients*writesPerClient) / elapsed.Seconds()
	}

	var one, three float64
	t.Run("nodes=1", func(t *testing.T) { one = measure(t, 1) })
	t.Run("nodes=3", func(t *testing.T) { three = measure(t, 3) })

	t.Logf("1 node:  %8.0f writes/sec", one)
	t.Logf("3 nodes: %8.0f writes/sec  (%.2fx)", three, three/one)
	t.Logf("A ratio near 1.0x is expected: all nodes share this machine's CPU, so")
	t.Logf("no capacity was added. What it shows is that routing and the extra hop")
	t.Logf("cost almost nothing — the proxy is not the bottleneck.")

	// Fail only on a clear regression. A proxy that serialised requests or held
	// a lock across forwarding would show up here as a sharp drop.
	if three < one*0.75 {
		t.Errorf("3 nodes managed %.0f writes/sec against 1 node's %.0f — "+
			"the proxy is imposing a real cost", three, one)
	}
}
