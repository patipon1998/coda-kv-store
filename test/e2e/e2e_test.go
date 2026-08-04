//go:build e2e

// Package e2e runs the shared conformance suite against a cluster that is
// already running — real binaries, real containers, real network.
//
// It is build-tagged so `go test ./...` stays hermetic. The in-process suites
// prove semantics; this proves the things only a real deployment can: flag and
// environment parsing, container wiring, and DNS between services.
//
//	make up-part2
//	KV_BASE_URL=http://localhost:8081 make e2e
package e2e

import (
	"net/http"
	"os"
	"testing"
	"time"

	"codakv/test/conformance"
)

func baseURL(t *testing.T) string {
	t.Helper()

	url := os.Getenv("KV_BASE_URL")
	if url == "" {
		t.Skip("KV_BASE_URL is not set; start a stack with `make up-part2` first")
	}

	// Fail fast and clearly if nothing is listening, rather than letting every
	// subtest time out separately.
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url + "/health")
	if err != nil {
		t.Fatalf("no service at %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s/health returned %d", url, resp.StatusCode)
	}
	return url
}

// TestE2EConformance runs the identical suite used against the in-process node
// and the in-process cluster — now against a live deployment.
func TestE2EConformance(t *testing.T) {
	conformance.Run(t, baseURL(t))
}

// TestE2ECounter is the assignment's required test plus its negative control,
// against a live deployment.
func TestE2ECounter(t *testing.T) {
	conformance.RunCounter(t, baseURL(t))
}
