package conformance

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

	"codakv/internal/node"
	"codakv/internal/routing"
	"codakv/internal/store"
)

const partitionCount = 1024

// startNode brings up one node in-process, owning the whole keyspace.
//
// httptest gives real HTTP over a real socket while staying fast, deterministic
// and race-detector friendly — no port allocation and no process lifecycle to
// manage. The actual binaries are exercised separately by the demo script.
func startNode(t *testing.T, nodeID string, owned routing.Range) *httptest.Server {
	t.Helper()

	kv, err := store.New(partitionCount)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	svc := node.NewService(kv, node.Config{
		NodeID:         nodeID,
		PartitionCount: partitionCount,
		Owned:          owned,
		MaxValueBytes:  1 << 20,
		MaxKeyBytes:    1 << 10,
	})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := httptest.NewServer(node.NewHandler(svc, log).Routes())
	t.Cleanup(srv.Close)
	return srv
}

// TestSingleNodeConformance runs the shared suite against one bare node.
// TestClusterConformance runs the identical suite against proxy + 3 nodes; the
// two must agree.
func TestSingleNodeConformance(t *testing.T) {
	srv := startNode(t, "node-1", routing.Range{Lo: 0, Hi: partitionCount})
	Run(t, srv.URL)
}

// TestSingleNodeCounter is the assignment's required test, plus its negative
// control, against a single node.
func TestSingleNodeCounter(t *testing.T) {
	srv := startNode(t, "node-1", routing.Range{Lo: 0, Hi: partitionCount})
	RunCounter(t, srv.URL)
}
