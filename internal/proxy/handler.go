package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"codakv/internal/node"
	"codakv/internal/routing"
)

// Handler routes single-key requests and aggregates the key listing.
type Handler struct {
	table  *routing.Table
	caller *Caller
	log    *slog.Logger
}

func NewHandler(table *routing.Table, caller *Caller, log *slog.Logger) *Handler {
	return &Handler{table: table, caller: caller, log: log}
}

// Routes builds the proxy's mux.
//
// The surface is identical to a node's, so the proxy is a transparent
// pass-through and a single node is simply a one-node cluster. A client can be
// pointed at either.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /kv/{key}", h.route)
	mux.HandleFunc("PUT /kv/{key}", h.route)
	mux.HandleFunc("PATCH /kv/{key}", h.route)

	mux.HandleFunc("GET /kv", h.listKeys)

	// Less specific than the patterns above, so this catches every other method
	// and keeps the error shape consistent with a node's.
	mux.HandleFunc("/kv/{key}", h.methodNotAllowed)
	mux.HandleFunc("/kv", h.methodNotAllowed)

	// Dump the live partition-to-node table. Small, and it makes the routing
	// story legible during a demo instead of something to take on trust.
	mux.HandleFunc("GET /routing", h.routingTable)
	mux.HandleFunc("GET /health", h.health)

	return mux
}

// route forwards one key's request to the node that owns it.
func (h *Handler) route(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	nodeID := h.table.NodeFor(key)

	resp, err := h.caller.Forward(r.Context(), nodeID, r)
	if err != nil {
		h.log.Error("forward failed", "node", nodeID, "key", key, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, errorBody{
			Error: "node " + nodeID + " unreachable",
			Key:   key,
		})
		return
	}
	defer resp.Body.Close()

	// Copy the response through untouched. The proxy forwards bytes; it does not
	// parse or re-encode, because doing so would drop the version from a 409 body
	// and quietly break every client's retry loop.
	for _, header := range []string{"Content-Type"} {
		if v := resp.Header.Get(header); v != "" {
			w.Header().Set(header, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		h.log.Warn("copying response failed", "node", nodeID, "key", key, "err", err)
	}
}

// listKeys fans out to every node and merges their NDJSON streams.
//
// Merging is plain concatenation, which is the whole reason for NDJSON: no
// parsing, no bracket surgery, and memory stays constant however large the
// keyspace is. Ordering carries no meaning, so the streams interleave freely.
func (h *Handler) listKeys(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", node.ContentTypeNDJSON)
	w.WriteHeader(http.StatusOK)

	// The status is committed the moment it is written, so a node failing later
	// cannot be reported as a 503. The failure is emitted as one more NDJSON
	// line instead, and clients must check for it.
	flusher, _ := w.(http.Flusher)

	var (
		mu sync.Mutex // serialises writes to the shared ResponseWriter
		wg sync.WaitGroup
	)

	writeLine := func(b []byte) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = w.Write(b)
		_, _ = w.Write([]byte("\n"))
		if flusher != nil {
			flusher.Flush()
		}
	}

	for _, nodeID := range h.table.NodeIDs() {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			h.streamNode(r.Context(), nodeID, writeLine)
		}(nodeID)
	}
	wg.Wait()
}

func (h *Handler) streamNode(ctx context.Context, nodeID string, writeLine func([]byte)) {
	body, err := h.caller.LocalKeys(ctx, nodeID)
	if err != nil {
		h.log.Error("listing failed", "node", nodeID, "err", err)
		line, _ := json.Marshal(errorLine{Error: "node " + nodeID + " unreachable"})
		writeLine(line)
		return
	}
	defer body.Close()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 4096), 1<<20) // a key may be up to 1 KiB
	for scanner.Scan() {
		if line := scanner.Bytes(); len(line) > 0 {
			writeLine(line)
		}
	}
	if err := scanner.Err(); err != nil {
		h.log.Error("listing stream broke", "node", nodeID, "err", err)
		line, _ := json.Marshal(errorLine{Error: "node " + nodeID + " stream interrupted"})
		writeLine(line)
	}
}

func (h *Handler) routingTable(w http.ResponseWriter, _ *http.Request) {
	type nodeInfo struct {
		Node       string `json:"node"`
		Address    string `json:"address"`
		Partitions string `json:"partitions"`
		Count      uint64 `json:"count"`
	}

	ranges := h.table.Ranges()
	out := make([]nodeInfo, 0, len(ranges))
	for _, id := range h.table.NodeIDs() {
		addr, _ := h.caller.Address(id)
		r := ranges[id]
		out = append(out, nodeInfo{
			Node:       id,
			Address:    addr,
			Partitions: r.String(),
			Count:      r.Count(),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"partitionCount": h.table.PartitionCount(),
		"nodes":          out,
	})
}

func (h *Handler) methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	allow := "GET, PUT, PATCH"
	if r.URL.Path == "/kv" {
		allow = "GET"
	}
	w.Header().Set("Allow", allow)
	writeJSON(w, http.StatusMethodNotAllowed, errorBody{
		Error: r.Method + " is not supported on this resource",
		Key:   r.PathValue("key"),
	})
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"role":           "proxy",
		"partitionCount": h.table.PartitionCount(),
		"nodes":          h.table.NodeIDs(),
	})
}

type errorBody struct {
	Error string `json:"error"`
	Key   string `json:"key,omitempty"`
}

type errorLine struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
