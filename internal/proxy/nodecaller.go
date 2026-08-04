// Package proxy is the stateless router in front of the store nodes.
//
// It holds a routing table and nothing else: no keys, no locks, no versions.
// That is what makes it safe to run several of them, and it is why sharding
// added no concurrency complexity — every key lives on exactly one node, so
// per-key atomicity stayed where Part 1 put it.
package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"codakv/internal/node"
)

// Caller talks to store nodes over HTTP.
type Caller struct {
	addresses      map[string]string
	client         *http.Client
	partitionCount int
}

func NewCaller(addresses map[string]string, partitionCount int, timeout time.Duration) *Caller {
	transport := &http.Transport{
		// Keep connections warm: the proxy talks to a small, fixed set of nodes,
		// so a fresh TCP handshake per request would be pure overhead.
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	return &Caller{
		addresses:      addresses,
		partitionCount: partitionCount,
		client:         &http.Client{Transport: transport, Timeout: timeout},
	}
}

// Address returns a node's base URL.
func (c *Caller) Address(nodeID string) (string, bool) {
	addr, ok := c.addresses[nodeID]
	return addr, ok
}

// Forward sends a single-key request to a node and returns its raw response.
//
// The request is reproduced as-is: same method, same escaped path, same query,
// same body. The caller then copies the status and body through untouched.
// Re-encoding would drop the version from a 409 body and silently degrade every
// client's retry loop into an extra round trip.
//
// Note there is no retry here, deliberately. A timeout is ambiguous — the write
// may have been applied with only the response lost — and retrying a
// read-modify-write that already succeeded would double-apply it. Retries belong
// to the client, and only on a definitively received 409.
func (c *Caller) Forward(ctx context.Context, nodeID string, r *http.Request) (*http.Response, error) {
	base, ok := c.addresses[nodeID]
	if !ok {
		return nil, fmt.Errorf("proxy: unknown node %q", nodeID)
	}

	// RequestURI preserves the original escaping, so a key like "a/b" arriving as
	// %2F stays one key instead of turning into two path segments.
	target := base + r.URL.RequestURI()

	outbound, err := http.NewRequestWithContext(ctx, r.Method, target, r.Body)
	if err != nil {
		return nil, err
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		outbound.Header.Set("Content-Type", ct)
	}
	outbound.ContentLength = r.ContentLength

	// Declare which partition count we hashed against. A mismatch is otherwise a
	// silent-corruption bug: the proxy would route by one divisor while the node
	// validates ownership by another.
	outbound.Header.Set(node.HeaderPartitionCount, strconv.Itoa(c.partitionCount))

	return c.client.Do(outbound)
}

// LocalKeys opens a node's GET /kv stream. The caller must close the reader.
func (c *Caller) LocalKeys(ctx context.Context, nodeID string) (io.ReadCloser, error) {
	base, ok := c.addresses[nodeID]
	if !ok {
		return nil, fmt.Errorf("proxy: unknown node %q", nodeID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/kv", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(node.HeaderPartitionCount, strconv.Itoa(c.partitionCount))

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("node %s returned %d for GET /kv", nodeID, resp.StatusCode)
	}
	return resp.Body, nil
}
