package node

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"codakv/internal/store"
)

// Header the proxy uses to declare how many partitions it hashed against, so a
// configuration mismatch surfaces as an error rather than as silent misrouting.
// Part 2.
const HeaderPartitionCount = "X-Partition-Count"

// ContentTypeNDJSON is the media type of GET /kv.
//
// NDJSON rather than a JSON array because merging streams from several nodes is
// then plain concatenation — no parsing, no bracket surgery — memory stays
// constant regardless of keyspace size, and an error can still be signalled
// mid-stream by emitting one more line.
const ContentTypeNDJSON = "application/x-ndjson"

// flushEvery bounds how often the listing forces a flush. Per line would make
// streaming most visible but costs a syscall each time; this keeps the stream
// genuinely incremental without that.
const flushEvery = 100

// Handler exposes the node's HTTP API.
type Handler struct {
	svc *Service
	log *slog.Logger
}

func NewHandler(svc *Service, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

// Routes builds the node's mux.
//
// Go 1.22+ patterns handle the awkward cases correctly without manual
// unescaping: "/kv/a%2Fb" arrives as the single key "a/b", and "/kv/a/b" is two
// segments, matches nothing, and 404s.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /kv/{key}", h.get)
	mux.HandleFunc("PUT /kv/{key}", h.put)
	mux.HandleFunc("PATCH /kv/{key}", h.patch)

	// Part 2: list this node's keys as NDJSON. The proxy fans out to this and
	// concatenates the streams.
	mux.HandleFunc("GET /kv", h.listKeys)

	// Method-less patterns are less specific than the ones above, so Go routes
	// anything they do not cover here. Without this, the mux would emit its own
	// plain-text 405 and break the promise that every error is a JSON body.
	mux.HandleFunc("/kv/{key}", h.methodNotAllowed)
	mux.HandleFunc("/kv", h.methodNotAllowed)

	mux.HandleFunc("GET /health", h.health)

	return mux
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	if err := h.checkPartitionCount(w, r); err != nil {
		return
	}

	result, err := h.svc.Get(key)
	if err != nil {
		h.writeError(w, key, err)
		return
	}
	h.writeResult(w, http.StatusOK, result)
}

func (h *Handler) put(w http.ResponseWriter, r *http.Request) {
	h.write(w, r, store.ModeReplace)
}

func (h *Handler) patch(w http.ResponseWriter, r *http.Request) {
	h.write(w, r, store.ModeMerge)
}

func (h *Handler) write(w http.ResponseWriter, r *http.Request, mode store.Mode) {
	key := r.PathValue("key")

	if err := h.checkPartitionCount(w, r); err != nil {
		return
	}

	ifVersion, ifAbsent, err := parseGuards(r)
	if err != nil {
		h.writeStatus(w, http.StatusBadRequest, err.Error(), key, nil)
		return
	}

	// MaxBytesReader caps the body before it is buffered, so an oversized
	// request cannot be used to exhaust memory on an in-memory store.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.svc.cfg.MaxValueBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.writeStatus(w, http.StatusRequestEntityTooLarge, "value too large", key, nil)
			return
		}
		h.writeStatus(w, http.StatusBadRequest, "could not read request body", key, nil)
		return
	}

	result, err := h.svc.Write(WriteRequest{
		Key:       key,
		Mode:      mode,
		Body:      body,
		IfVersion: ifVersion,
		IfAbsent:  ifAbsent,
	})
	if err != nil {
		h.writeError(w, key, err)
		return
	}
	h.writeResult(w, http.StatusOK, result)
}

// listKeys streams this node's keys as NDJSON. Part 2.
//
// The node stamps its own id: it knows what it is, whereas the proxy would have
// to correlate lines back to connections for no benefit.
func (h *Handler) listKeys(w http.ResponseWriter, r *http.Request) {
	if err := h.checkPartitionCount(w, r); err != nil {
		return
	}

	w.Header().Set("Content-Type", ContentTypeNDJSON)
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w) // Encode appends a newline: exactly NDJSON framing
	nodeID := h.svc.NodeID()

	written := 0
	h.svc.Keys(func(key string) bool {
		if err := enc.Encode(keyLine{Key: key, Node: nodeID}); err != nil {
			// The client hung up. Nothing useful to report — the status is long
			// since committed.
			return false
		}
		written++
		if flusher != nil && written%flushEvery == 0 {
			flusher.Flush()
		}
		return true
	})

	if flusher != nil {
		flusher.Flush()
	}
}

// methodNotAllowed answers with the same JSON error shape as every other
// failure, plus the Allow header the status requires.
func (h *Handler) methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	allow := "GET, PUT, PATCH"
	if r.URL.Path == "/kv" {
		allow = "GET"
	}
	w.Header().Set("Allow", allow)
	h.writeStatus(w, http.StatusMethodNotAllowed,
		r.Method+" is not supported on this resource", r.PathValue("key"), nil)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"node":            h.svc.NodeID(),
		"partitions":      h.svc.PartitionCount(),
		"ownedPartitions": h.svc.Owned().String(),
	})
}

// checkPartitionCount rejects callers that hashed against a different partition
// count. Part 2. It writes the response and returns a non-nil error when the
// request must not proceed.
func (h *Handler) checkPartitionCount(w http.ResponseWriter, r *http.Request) error {
	raw := r.Header.Get(HeaderPartitionCount)
	if raw == "" {
		return nil // a direct client, not the proxy
	}
	theirs, err := strconv.Atoi(raw)
	if err != nil {
		h.writeStatus(w, http.StatusBadRequest, "invalid "+HeaderPartitionCount+" header", "", nil)
		return err
	}
	if err := h.svc.CheckPartitionCount(theirs); err != nil {
		h.writeStatus(w, http.StatusMisdirectedRequest, err.Error(), "", nil)
		return err
	}
	return nil
}

// parseGuards reads the ifVersion and ifAbsent query parameters.
//
// ifAbsent is not in the assignment's API. It was added because a
// compare-and-set loop cannot protect its first write: with the key absent there
// is no version to assert, so two clients would both write unconditionally and
// one increment would be lost. DynamoDB exposes the same primitive as
// attribute_not_exists.
func parseGuards(r *http.Request) (ifVersion *uint64, ifAbsent bool, err error) {
	q := r.URL.Query()

	if vals := q["ifVersion"]; len(vals) > 0 {
		// Ruling R7: a repeated parameter is ambiguous, and silently taking the
		// first is how real bugs hide.
		if len(vals) > 1 {
			return nil, false, errors.New("ifVersion given more than once")
		}
		// ParseUint rejects "abc", "-1" and anything overflowing uint64.
		v, parseErr := strconv.ParseUint(vals[0], 10, 64)
		if parseErr != nil {
			return nil, false, errors.New("ifVersion must be a non-negative integer")
		}
		ifVersion = &v
	}

	if vals := q["ifAbsent"]; len(vals) > 0 {
		if len(vals) > 1 {
			return nil, false, errors.New("ifAbsent given more than once")
		}
		switch vals[0] {
		case "true", "1":
			ifAbsent = true
		case "false", "0":
			ifAbsent = false
		default:
			return nil, false, errors.New("ifAbsent must be true or false")
		}
	}

	if ifAbsent && ifVersion != nil {
		return nil, false, errors.New("ifAbsent and ifVersion cannot both be set")
	}
	return ifVersion, ifAbsent, nil
}

// --- responses --------------------------------------------------------------

type successBody struct {
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
	Version uint64          `json:"version"`
}

type errorBody struct {
	Error string `json:"error"`
	Key   string `json:"key,omitempty"`
	// Version carries the key's current version on a 409 so a client's retry
	// loop does not need a second round trip to re-read it. Mirrors DynamoDB's
	// ReturnValuesOnConditionCheckFailure.
	Version *uint64 `json:"version,omitempty"`
}

type keyLine struct {
	Key  string `json:"key"`
	Node string `json:"node"`
}

func (h *Handler) writeResult(w http.ResponseWriter, status int, r Result) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(successBody{
		Key:     r.Key,
		Value:   r.Value,
		Version: r.Version,
	})
}

func (h *Handler) writeStatus(w http.ResponseWriter, status int, msg, key string, version *uint64) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: msg, Key: key, Version: version})
}

// writeError maps a service or store error onto its status code.
func (h *Handler) writeError(w http.ResponseWriter, key string, err error) {
	var mismatch *store.VersionMismatchError
	if errors.As(err, &mismatch) {
		// Current is 0 when the key does not exist, and no stored key ever has
		// version 0 — so omitting it there keeps the body unambiguous.
		var version *uint64
		if mismatch.Current != 0 {
			v := mismatch.Current
			version = &v
		}
		h.writeStatus(w, http.StatusConflict, "version mismatch", key, version)
		return
	}

	var misdirected *MisdirectedError
	if errors.As(err, &misdirected) {
		h.writeStatus(w, http.StatusMisdirectedRequest, misdirected.Error(), key, nil)
		return
	}

	switch {
	case errors.Is(err, ErrNotFound):
		h.writeStatus(w, http.StatusNotFound, "key not found", key, nil)
	case errors.Is(err, ErrKeyEmpty):
		h.writeStatus(w, http.StatusNotFound, "key must not be empty", key, nil)
	case errors.Is(err, ErrKeyTooLong):
		// Deliberately omit the key: echoing a kilobyte of it helps nobody.
		h.writeStatus(w, http.StatusRequestURITooLong, "key too long", "", nil)
	case errors.Is(err, ErrValueTooLarge):
		h.writeStatus(w, http.StatusRequestEntityTooLarge, "value too large", key, nil)
	case errors.Is(err, ErrInvalidJSON):
		h.writeStatus(w, http.StatusBadRequest, "body is not valid JSON", key, nil)
	case errors.Is(err, store.ErrConflictingGuards):
		h.writeStatus(w, http.StatusBadRequest, err.Error(), key, nil)
	default:
		h.log.Error("unhandled error", "key", key, "err", err)
		h.writeStatus(w, http.StatusInternalServerError, "internal error", key, nil)
	}
}
