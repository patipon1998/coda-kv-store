// Package jsonmerge implements the PATCH merge rules. It is pure: no locks, no
// HTTP, no state. The store calls Apply from inside a key's critical section, so
// this code must never block and must never mutate its inputs.
package jsonmerge

import (
	"bytes"
	"encoding/json"
)

// Apply merges delta into current according to the assignment's PATCH rules:
//
//  1. current is absent            → the caller handles creation; Apply is not called.
//  2. both are JSON objects        → shallow merge of top-level fields, delta wins.
//  3. anything else                → replace wholesale with delta.
//
// The merge is shallow by specification. A nested object in delta REPLACES the
// corresponding nested object in current; it is not merged recursively. This is
// the rule most often got wrong, and TestShallowNotDeep pins it.
//
// A null-valued field in delta SETS that field to null; it does not delete it.
// RFC 7386 JSON Merge Patch treats null as a delete, but the assignment
// specifies "shallow-merge top-level fields", which is not Merge Patch —
// deleting would be inventing semantics. See ruling R1 in docs/test-plan.md.
//
// Apply always returns freshly allocated bytes and never writes through either
// argument. The store relies on this: a published value must stay immutable so
// that a reader holding the old slice keeps reading valid data while a writer
// installs a new one.
func Apply(current, delta json.RawMessage) json.RawMessage {
	curObj, curIsObj := asObject(current)
	delObj, delIsObj := asObject(delta)

	if !curIsObj || !delIsObj {
		return clone(delta) // rule 3: replace
	}

	// Rule 2: shallow merge, delta wins on conflict.
	for k, v := range delObj {
		curObj[k] = v
	}

	merged, err := json.Marshal(curObj)
	if err != nil {
		// Unreachable: every value came out of a successful Unmarshal, so it is
		// already valid JSON. Falling back to replace keeps the store consistent
		// rather than storing something malformed.
		return clone(delta)
	}
	return merged
}

// asObject decodes raw into a map if — and only if — it is a JSON object.
//
// The leading-byte check is load-bearing. Unmarshalling the literal `null` into
// a map succeeds and yields a nil map, so err == nil alone would misclassify
// null as an empty object and merge into it instead of replacing.
func asObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, false
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, false
	}
	return m, true
}

func clone(b json.RawMessage) json.RawMessage {
	out := make(json.RawMessage, len(b))
	copy(out, b)
	return out
}
