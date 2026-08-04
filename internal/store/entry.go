package store

import (
	"encoding/json"
	"errors"
	"fmt"
)

// FirstVersion is the version assigned by a key's first successful write.
//
// Version 0 never exists in the store. That is deliberate: it lets 0 mean
// "absent" unambiguously in a VersionMismatchError, and it makes ifVersion=0
// always a mismatch.
const FirstVersion uint64 = 1

// Mode selects how a write combines with whatever is already stored.
type Mode uint8

const (
	// ModeReplace overwrites the value entirely. This is PUT.
	ModeReplace Mode = iota
	// ModeMerge applies the assignment's PATCH rules. This is PATCH.
	ModeMerge
)

func (m Mode) String() string {
	switch m {
	case ModeReplace:
		return "replace"
	case ModeMerge:
		return "merge"
	default:
		return fmt.Sprintf("Mode(%d)", uint8(m))
	}
}

// Entry is a snapshot of a key: its value and the version that produced it.
//
// Value must be treated as read-only. It shares backing storage with the store's
// copy, which is safe only because published values are never mutated in place —
// a writer installs a new slice rather than editing the old one.
type Entry struct {
	Value   json.RawMessage
	Version uint64
}

// WriteOp is a complete description of a write, handed to the store as one unit.
//
// This shape is load-bearing. The version guard, the value computation and the
// version bump have to happen inside a single critical section, so a caller must
// never read the current value, decide, and write back — that is a
// time-of-check-to-time-of-use race and it loses updates. Describing the whole
// operation up front is what makes it possible for the store to apply it
// atomically.
type WriteOp struct {
	Key  string
	Body json.RawMessage
	Mode Mode

	// IfVersion is the optimistic-concurrency guard. nil means unconditional.
	// A non-nil value that does not match the key's current version fails with
	// VersionMismatchError, including when the key does not exist at all.
	IfVersion *uint64

	// IfAbsent makes the write create-only: it succeeds only if the key does not
	// yet exist, and fails with VersionMismatchError otherwise.
	//
	// Without this, a compare-and-set loop has an unprotected bootstrap window.
	// The first client to touch a key has no version to assert, so it must write
	// unconditionally — and two clients doing that concurrently both "succeed"
	// while one update is silently lost. Discovered by the 300-counter test,
	// which landed on 298. DynamoDB's attribute_not_exists exists for the same
	// reason.
	//
	// Setting both IfAbsent and IfVersion is contradictory and returns
	// ErrConflictingGuards.
	IfAbsent bool
}

// ErrConflictingGuards reports a WriteOp asserting both "must not exist" and
// "must be at version N".
var ErrConflictingGuards = errors.New("store: IfAbsent and IfVersion cannot both be set")

// VersionMismatchError reports a failed ifVersion guard.
//
// Current carries the version the key actually holds, so a client can retry
// without a second round trip to re-read it. Current is 0 when the key does not
// exist, which is unambiguous because no stored key ever has version 0.
type VersionMismatchError struct {
	Key      string
	Expected uint64
	Current  uint64

	// RequiredAbsent is set when the write asked for IfAbsent but the key was
	// already present.
	RequiredAbsent bool
}

// Absent reports whether the guard failed because the key does not exist.
func (e *VersionMismatchError) Absent() bool { return e.Current == 0 }

func (e *VersionMismatchError) Error() string {
	switch {
	case e.RequiredAbsent:
		return fmt.Sprintf("store: key %q already exists at version %d", e.Key, e.Current)
	case e.Current == 0:
		return fmt.Sprintf("store: key %q does not exist, cannot match version %d", e.Key, e.Expected)
	default:
		return fmt.Sprintf("store: key %q is at version %d, not %d", e.Key, e.Current, e.Expected)
	}
}
