// Package node is the store node: the Part 1 API, plus the two small additions
// Part 2 needs. Everything Part 2 contributed is marked with a "Part 2:" comment,
// so the whole delta is greppable.
package node

import (
	"encoding/json"
	"errors"
	"fmt"

	"codakv/internal/routing"
	"codakv/internal/store"
)

// Config is what the service needs to enforce its limits and ownership.
type Config struct {
	NodeID         string
	PartitionCount int
	Owned          routing.Range // Part 2: the range this node accepts
	MaxValueBytes  int64
	MaxKeyBytes    int
}

// Service holds the node's application logic: validation, limits, and building
// the store command.
//
// It deliberately never reads a value, decides, and writes it back. The version
// guard, the merge and the version bump have to happen inside one critical
// section, which only the store can provide — so the service describes the whole
// operation and hands it over. A read-then-write here would reintroduce exactly
// the lost update ifVersion exists to prevent, while still looking like clean
// layering.
type Service struct {
	store *store.Store
	cfg   Config
}

func NewService(s *store.Store, cfg Config) *Service {
	return &Service{store: s, cfg: cfg}
}

// NodeID returns this node's identifier, stamped into GET /kv output.
func (s *Service) NodeID() string { return s.cfg.NodeID }

// Owned reports the partition range this node serves. Part 2.
func (s *Service) Owned() routing.Range { return s.cfg.Owned }

// PartitionCount returns the partition count this node was configured with.
func (s *Service) PartitionCount() int { return s.cfg.PartitionCount }

// Result is a key's value and version, ready to serialise.
type Result struct {
	Key     string
	Value   json.RawMessage
	Version uint64
}

// WriteRequest describes a PUT or PATCH.
type WriteRequest struct {
	Key       string
	Mode      store.Mode
	Body      []byte
	IfVersion *uint64
	IfAbsent  bool
}

// Validation failures. Each maps to one status code in the handler.
var (
	ErrKeyEmpty      = errors.New("key must not be empty")
	ErrKeyTooLong    = errors.New("key too long")
	ErrValueTooLarge = errors.New("value too large")
	ErrInvalidJSON   = errors.New("body is not valid JSON")
	ErrNotFound      = errors.New("key not found")
)

// MisdirectedError reports a request for a partition this node does not own.
//
// Part 2. Without it, a proxy misconfiguration would write a key here, return
// 200, and every later read would be routed elsewhere and 404 — data lost with
// no error raised anywhere in the system.
type MisdirectedError struct {
	Key       string
	Partition uint64
	Owned     routing.Range
	NodeID    string
}

func (e *MisdirectedError) Error() string {
	return fmt.Sprintf("node %s owns partitions %s, but key %q is in partition %d",
		e.NodeID, e.Owned, e.Key, e.Partition)
}

// PartitionCountMismatchError reports that the caller and this node disagree
// about how many partitions exist.
//
// Part 2. A mismatch is a silent-corruption bug rather than a startup failure:
// the proxy would hash keys with one divisor while the node validates with
// another, so both would be internally consistent and route differently.
type PartitionCountMismatchError struct {
	NodeID string
	Ours   int
	Theirs int
}

func (e *PartitionCountMismatchError) Error() string {
	return fmt.Sprintf("node %s is configured for %d partitions, caller assumed %d",
		e.NodeID, e.Ours, e.Theirs)
}

// Get returns a key's current value and version.
func (s *Service) Get(key string) (Result, error) {
	if err := s.checkKey(key); err != nil {
		return Result{}, err
	}
	entry, ok := s.store.Get(key)
	if !ok {
		return Result{}, ErrNotFound
	}
	return Result{Key: key, Value: entry.Value, Version: entry.Version}, nil
}

// Write validates the request and applies it atomically.
func (s *Service) Write(req WriteRequest) (Result, error) {
	if err := s.checkKey(req.Key); err != nil {
		return Result{}, err
	}
	if int64(len(req.Body)) > s.cfg.MaxValueBytes {
		return Result{}, ErrValueTooLarge
	}
	// Validate here so a malformed body can never reach storage, and so GET can
	// never hand a client something it cannot parse.
	if !json.Valid(req.Body) {
		return Result{}, ErrInvalidJSON
	}

	entry, err := s.store.Write(store.WriteOp{
		Key:       req.Key,
		Body:      req.Body,
		Mode:      req.Mode,
		IfVersion: req.IfVersion,
		IfAbsent:  req.IfAbsent,
	})
	if err != nil {
		return Result{}, err
	}
	return Result{Key: req.Key, Value: entry.Value, Version: entry.Version}, nil
}

// CheckPartitionCount verifies a caller's assumed partition count against ours.
// Part 2.
func (s *Service) CheckPartitionCount(theirs int) error {
	if theirs != 0 && theirs != s.cfg.PartitionCount {
		return &PartitionCountMismatchError{
			NodeID: s.cfg.NodeID,
			Ours:   s.cfg.PartitionCount,
			Theirs: theirs,
		}
	}
	return nil
}

// Keys iterates every key held locally.
//
// Part 2: only the aggregating GET /kv endpoint needs this. Note that it takes
// no entry locks — key names live in the partition map, not in the entry, so a
// key being written concurrently still lists correctly because its name is not
// what is changing.
func (s *Service) Keys(yield func(key string) bool) {
	for key := range s.store.Keys() {
		if !yield(key) {
			return
		}
	}
}

func (s *Service) checkKey(key string) error {
	if key == "" {
		return ErrKeyEmpty
	}
	if len(key) > s.cfg.MaxKeyBytes {
		return ErrKeyTooLong
	}
	// Part 2: refuse keys belonging to another node.
	partition := routing.Partition(key, s.cfg.PartitionCount)
	if !s.cfg.Owned.Contains(partition) {
		return &MisdirectedError{
			Key:       key,
			Partition: partition,
			Owned:     s.cfg.Owned,
			NodeID:    s.cfg.NodeID,
		}
	}
	return nil
}
