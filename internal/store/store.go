// Package store is the in-memory key/value store: a partitioned map with
// per-key locking and per-key versions.
//
// # Locking
//
// Three layers, each protecting something different:
//
//	partition RWMutex  the Go map's structure — lookup and insert
//	entry RWMutex      one key's value and version
//	ifVersion          semantics across requests, which no lock can provide
//
// The partition lock is not optional and is not about semantics. Go maps are not
// safe for concurrent read and write, and the runtime detects violations with an
// unrecoverable "concurrent map read and map write" fatal error that takes the
// whole process — and every key in it — down. Note the hazard involves unrelated
// keys: inserting one key can grow the map and relocate the buckets another
// reader is walking.
//
// The partition lock is released before the entry lock is taken, which is sound
// only because entries are never removed. There is no DELETE, so an *entry
// pointer stays valid forever regardless of what happens to the map afterwards.
// Adding DELETE would break this, and would need a tombstone flag checked under
// the entry lock rather than just a map deletion.
//
// # Striping
//
// The map is split into partitions so that the exclusive lock taken on the
// create path does not serialise every first-write in the process. Less
// obviously, it also helps reads: RWMutex.RLock atomically bumps a shared reader
// counter, and that cache line bounces between every reading core even when no
// writer exists.
package store

import (
	"encoding/json"
	"iter"
	"sync"

	"codakv/internal/jsonmerge"
	"codakv/internal/routing"
)

// entry holds one key's data. Its mutex guards the fields below it.
type entry struct {
	mu      sync.RWMutex
	value   json.RawMessage
	version uint64
}

// partition is one lock stripe: a map plus the mutex guarding its structure.
type partition struct {
	mu sync.RWMutex
	m  map[string]*entry
}

// Store is a partitioned in-memory key/value store, safe for concurrent use.
type Store struct {
	parts          []*partition
	partitionCount int
}

// New creates a store with the given number of partitions, which must be a
// power of two so the partition index is a mask rather than a division.
func New(partitionCount int) (*Store, error) {
	if !routing.ValidPartitionCount(partitionCount) {
		return nil, &ConfigError{PartitionCount: partitionCount}
	}
	parts := make([]*partition, partitionCount)
	for i := range parts {
		parts[i] = &partition{m: make(map[string]*entry)}
	}
	return &Store{parts: parts, partitionCount: partitionCount}, nil
}

// ConfigError reports an unusable store configuration.
type ConfigError struct{ PartitionCount int }

func (e *ConfigError) Error() string {
	return "store: partition count must be a positive power of two, got " +
		itoa(e.PartitionCount)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// PartitionCount returns how many lock stripes the store was built with.
func (s *Store) PartitionCount() int { return s.partitionCount }

func (s *Store) partitionFor(key string) *partition {
	return s.parts[routing.Partition(key, s.partitionCount)]
}

// Get returns the current snapshot of a key.
//
// The entry lock is held only long enough to copy the value and version out
// together. That pairing matters as much as the lock itself: a reader that saw a
// new value beside an old version would send a stale ifVersion, pass the guard,
// and lose an update silently.
func (s *Store) Get(key string) (Entry, bool) {
	p := s.partitionFor(key)

	p.mu.RLock()
	e, ok := p.m[key]
	p.mu.RUnlock()

	if !ok {
		return Entry{}, false
	}

	e.mu.RLock()
	snapshot := Entry{Value: e.value, Version: e.version}
	e.mu.RUnlock()

	return snapshot, true
}

// Write applies op atomically and returns the resulting snapshot.
//
// Guard, computation and version bump all happen inside one critical section.
// Splitting them — checking the version, releasing, then writing — reintroduces
// the lost update the guard exists to prevent, even though every individual
// access would still be correctly locked.
func (s *Store) Write(op WriteOp) (Entry, error) {
	if op.IfAbsent && op.IfVersion != nil {
		return Entry{}, ErrConflictingGuards
	}

	p := s.partitionFor(op.Key)

	for {
		// Existing key: the whole operation runs under this key's lock.
		p.mu.RLock()
		e, ok := p.m[op.Key]
		p.mu.RUnlock()

		if ok {
			e.mu.Lock()
			if op.IfAbsent {
				current := e.version
				e.mu.Unlock()
				return Entry{}, &VersionMismatchError{
					Key:            op.Key,
					Current:        current,
					RequiredAbsent: true,
				}
			}
			if op.IfVersion != nil && *op.IfVersion != e.version {
				current := e.version
				e.mu.Unlock()
				return Entry{}, &VersionMismatchError{
					Key:      op.Key,
					Expected: *op.IfVersion,
					Current:  current,
				}
			}
			e.value = compute(e.value, op)
			e.version++
			snapshot := Entry{Value: e.value, Version: e.version}
			e.mu.Unlock()
			return snapshot, nil
		}

		// New key: build the finished entry and insert it under the partition's
		// write lock. Inserting a blank entry first and filling it in afterwards
		// would strand a half-initialised entry whenever the write then failed —
		// and that happens on ordinary error paths, not just crashes.
		p.mu.Lock()
		if _, exists := p.m[op.Key]; !exists {
			if op.IfVersion != nil {
				p.mu.Unlock()
				return Entry{}, &VersionMismatchError{
					Key:      op.Key,
					Expected: *op.IfVersion,
					Current:  0, // 0 means absent: no stored key ever has version 0
				}
			}
			created := &entry{value: clone(op.Body), version: FirstVersion}
			p.m[op.Key] = created

			// Snapshot before releasing the partition lock. Once it drops, the
			// entry is reachable and another goroutine may already be writing
			// through it, so reading created.value afterwards would be a race.
			snapshot := Entry{Value: created.value, Version: created.version}
			p.mu.Unlock()
			return snapshot, nil
		}
		p.mu.Unlock()

		// Another goroutine created the key between our lookup and our insert.
		// Re-check under the write lock is what makes that safe: without it both
		// goroutines would hold different *entry values for the same key, both
		// would lock successfully, and one write would vanish.
	}
}

// Keys iterates every key in the store.
//
// Partitions are visited one at a time rather than locked all at once, and each
// partition's keys are copied out before yielding so no caller runs — writing to
// a network, say — while a lock is held.
//
// The resulting view is not a point-in-time snapshot: a key present for the whole
// iteration is guaranteed to appear, while one created midway may or may not.
// Keys never move between partitions, so nothing can be seen twice or skipped
// because of the traversal itself. Locking every partition would give a true
// snapshot at the cost of blocking all writes, which is the wrong trade for a
// listing endpoint.
//
// Part 2: only the aggregating GET /kv endpoint needs this.
func (s *Store) Keys() iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, p := range s.parts {
			p.mu.RLock()
			keys := make([]string, 0, len(p.m))
			for k := range p.m {
				keys = append(keys, k)
			}
			p.mu.RUnlock()

			for _, k := range keys {
				if !yield(k) {
					return
				}
			}
		}
	}
}

// Len returns the number of keys held. It is O(partitions) and is intended for
// tests and diagnostics rather than the request path.
func (s *Store) Len() int {
	total := 0
	for _, p := range s.parts {
		p.mu.RLock()
		total += len(p.m)
		p.mu.RUnlock()
	}
	return total
}

// compute produces the new value for a write. It always allocates and never
// writes through the current value.
//
// That is what lets Get hand out the stored slice without copying: a writer
// installs a brand-new slice, so a reader still holding the previous one keeps
// reading intact bytes from an array nobody will touch again.
func compute(current json.RawMessage, op WriteOp) json.RawMessage {
	if op.Mode == ModeMerge {
		return jsonmerge.Apply(current, op.Body)
	}
	return clone(op.Body)
}

// clone copies the caller's bytes so the store never aliases a buffer the caller
// might reuse — an HTTP body buffer, for instance.
func clone(b json.RawMessage) json.RawMessage {
	out := make(json.RawMessage, len(b))
	copy(out, b)
	return out
}
