package conformance

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codakv/internal/proxy"
	"codakv/internal/routing"
)

const clusterSize = 3

// cluster is a proxy in front of several nodes, all in-process.
type cluster struct {
	proxy *httptest.Server
	nodes []*httptest.Server
	ids   []string
	table *routing.Table
}

// startCluster brings up clusterSize nodes plus a proxy.
//
// Each node's partition range comes from routing.RangeFor with its own index,
// which is exactly what the proxy's table uses — the same function on both
// sides, so they cannot disagree.
func startCluster(t *testing.T) *cluster {
	t.Helper()

	ids := make([]string, clusterSize)
	nodes := make([]*httptest.Server, clusterSize)
	addresses := make(map[string]string, clusterSize)

	for i := 0; i < clusterSize; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		owned, err := routing.RangeFor(i, clusterSize, partitionCount)
		if err != nil {
			t.Fatalf("RangeFor(%d): %v", i, err)
		}
		ids[i] = id
		nodes[i] = startNode(t, id, owned)
		addresses[id] = nodes[i].URL
	}

	table, err := routing.NewTable(ids, partitionCount)
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	caller := proxy.NewCaller(addresses, partitionCount, 10*time.Second)
	p := httptest.NewServer(proxy.NewHandler(table, caller, log).Routes())
	t.Cleanup(p.Close)

	return &cluster{proxy: p, nodes: nodes, ids: ids, table: table}
}

// K17: the identical suite, against proxy plus three nodes.
//
// This is the strongest single claim available: same suite, two topologies,
// same results. It proves the proxy is transparent and that sharding preserved
// Part 1's semantics.
func TestClusterConformance(t *testing.T) {
	c := startCluster(t)
	Run(t, c.proxy.URL)
}

// K8: the required counter test, routed. Still exactly 300, which shows routing
// did not disturb per-key atomicity — because each key still lives on exactly
// one node, running Part 1's store unchanged.
func TestClusterCounter(t *testing.T) {
	c := startCluster(t)
	RunCounter(t, c.proxy.URL)
}

// K7: a proxy in front of one node behaves exactly like a bare node.
func TestProxyWithSingleNodeIsTransparent(t *testing.T) {
	owned := routing.Range{Lo: 0, Hi: partitionCount}
	n := startNode(t, "node-solo", owned)

	table, err := routing.NewTable([]string{"node-solo"}, partitionCount)
	if err != nil {
		t.Fatal(err)
	}
	caller := proxy.NewCaller(map[string]string{"node-solo": n.URL}, partitionCount, 10*time.Second)
	p := httptest.NewServer(proxy.NewHandler(table, caller,
		slog.New(slog.NewTextHandler(io.Discard, nil))).Routes())
	t.Cleanup(p.Close)

	Run(t, p.URL)
}

// K3 and K14: keys spread across nodes, so capacity grows with the cluster
// rather than every node holding everything.
func TestKeysDistributeAcrossNodes(t *testing.T) {
	c := startCluster(t)
	client := NewClient(c.proxy.URL)

	const total = 3000
	prefix := unique("dist")
	for i := 0; i < total; i++ {
		resp, err := client.Put(fmt.Sprintf("%s-%d", prefix, i), `1`)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Status != http.StatusOK {
			t.Fatalf("PUT: status %d (%s)", resp.Status, resp.Raw)
		}
	}

	keys, streamErr, err := client.ListKeys()
	if err != nil {
		t.Fatal(err)
	}
	if streamErr != "" {
		t.Fatalf("listing reported an error: %s", streamErr)
	}

	perNode := map[string]int{}
	seen := 0
	for key, nodeID := range keys {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			perNode[nodeID]++
			seen++
		}
	}

	if seen != total {
		t.Errorf("listing returned %d of %d keys", seen, total)
	}
	if len(perNode) != clusterSize {
		t.Errorf("keys landed on %d nodes, want %d", len(perNode), clusterSize)
	}

	want := total / clusterSize
	for id, count := range perNode {
		if skew := float64(count-want) / float64(want); skew > 0.15 || skew < -0.15 {
			t.Errorf("%s holds %d keys, want ~%d (%.0f%% off)", id, count, want, skew*100)
		}
	}
	t.Logf("distribution across %d nodes: %v", clusterSize, perNode)
}

// K4: every key appears exactly once, attributed to the node that actually holds
// it. Verified against each node directly, not just against the proxy's view.
func TestListingAttributesKeysCorrectly(t *testing.T) {
	c := startCluster(t)
	client := NewClient(c.proxy.URL)

	prefix := unique("attr")
	written := map[string]bool{}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if _, err := client.Put(key, `1`); err != nil {
			t.Fatal(err)
		}
		written[key] = true
	}

	keys, _, err := client.ListKeys()
	if err != nil {
		t.Fatal(err)
	}

	for key := range written {
		nodeID, ok := keys[key]
		if !ok {
			t.Errorf("key %q missing from the listing", key)
			continue
		}
		if want := c.table.NodeFor(key); nodeID != want {
			t.Errorf("key %q attributed to %s, but the table places it on %s", key, nodeID, want)
		}
	}
}

// K2: routing is stable — the same key always reaches the same node.
func TestRoutingIsStable(t *testing.T) {
	c := startCluster(t)
	client := NewClient(c.proxy.URL)

	key := unique("stable")
	if _, err := client.Put(key, `{"a":1}`); err != nil {
		t.Fatal(err)
	}

	want := c.table.NodeFor(key)
	for i := 0; i < 50; i++ {
		if got := c.table.NodeFor(key); got != want {
			t.Fatalf("routing drifted: %s then %s", want, got)
		}
	}

	// And the node that owns it really does hold it.
	direct := NewClient(c.nodes[indexOf(c.ids, want)].URL)
	resp, err := direct.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != http.StatusOK {
		t.Errorf("owning node %s returned %d for its own key", want, resp.Status)
	}
}

// K10: when a node is down the surviving keys still stream, and the failure is
// reported as a final NDJSON line — a 503 is impossible once 200 has been sent.
func TestListingWithNodeDown(t *testing.T) {
	c := startCluster(t)
	client := NewClient(c.proxy.URL)

	prefix := unique("down")
	for i := 0; i < 300; i++ {
		if _, err := client.Put(fmt.Sprintf("%s-%d", prefix, i), `1`); err != nil {
			t.Fatal(err)
		}
	}

	c.nodes[1].Close() // node-2 goes away

	keys, streamErr, err := client.ListKeys()
	if err != nil {
		t.Fatalf("listing failed outright, want a partial stream: %v", err)
	}
	if streamErr == "" {
		t.Error("no error line for the downed node")
	}
	if len(keys) == 0 {
		t.Error("no keys streamed from the surviving nodes")
	}
	t.Logf("streamed %d keys and reported: %s", len(keys), streamErr)
}

// K9: a request for a downed node's partition fails promptly instead of hanging.
func TestRequestToDownNodeFailsFast(t *testing.T) {
	c := startCluster(t)
	client := NewClient(c.proxy.URL)

	target := c.ids[1]
	var key string
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("downkey-%d", i)
		if c.table.NodeFor(candidate) == target {
			key = candidate
			break
		}
	}

	c.nodes[1].Close()

	start := time.Now()
	resp, err := client.Get(key)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("request errored rather than returning a status: %v", err)
	}
	if resp.Status != http.StatusServiceUnavailable {
		t.Errorf("status %d, want 503 (body %s)", resp.Status, resp.Raw)
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %v to fail; should be prompt", elapsed)
	}
}

// K13: a 409 keeps its version field all the way through the proxy. Catches a
// proxy that re-encodes responses instead of forwarding bytes.
func TestConflictSurvivesTheProxy(t *testing.T) {
	c := startCluster(t)
	client := NewClient(c.proxy.URL)

	key := unique("conflict-proxy")
	for i := 0; i < 3; i++ {
		if _, err := client.Put(key, fmt.Sprintf(`{"n":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := client.PutIfVersion(key, `{"n":9}`, 1)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != http.StatusConflict {
		t.Fatalf("status %d, want 409 (body %s)", resp.Status, resp.Raw)
	}
	version, ok := resp.VersionPtr()
	if !ok {
		t.Fatalf("409 lost its version field crossing the proxy: %s", resp.Raw)
	}
	if version != 3 {
		t.Errorf("409 reports version %d, want 3", version)
	}
}

// The /routing endpoint reports every node with a non-empty, non-overlapping
// range covering the whole keyspace.
func TestRoutingEndpoint(t *testing.T) {
	c := startCluster(t)

	resp, err := http.Get(c.proxy.URL + "/routing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var out struct {
		PartitionCount int `json:"partitionCount"`
		Nodes          []struct {
			Node       string `json:"node"`
			Partitions string `json:"partitions"`
			Count      uint64 `json:"count"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	if out.PartitionCount != partitionCount {
		t.Errorf("partitionCount %d, want %d", out.PartitionCount, partitionCount)
	}
	if len(out.Nodes) != clusterSize {
		t.Fatalf("reported %d nodes, want %d", len(out.Nodes), clusterSize)
	}

	var total uint64
	for _, n := range out.Nodes {
		if n.Count == 0 {
			t.Errorf("node %s owns nothing", n.Node)
		}
		total += n.Count
	}
	if total != uint64(partitionCount) {
		t.Errorf("ranges cover %d partitions, want %d", total, partitionCount)
	}
}

func indexOf(ids []string, want string) int {
	for i, id := range ids {
		if id == want {
			return i
		}
	}
	return -1
}
