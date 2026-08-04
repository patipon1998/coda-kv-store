package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const testPartitions = 64

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(testPartitions)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func put(t *testing.T, s *Store, key, body string) Entry {
	t.Helper()
	e, err := s.Write(WriteOp{Key: key, Body: json.RawMessage(body), Mode: ModeReplace})
	if err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
	return e
}

func patch(t *testing.T, s *Store, key, body string) Entry {
	t.Helper()
	e, err := s.Write(WriteOp{Key: key, Body: json.RawMessage(body), Mode: ModeMerge})
	if err != nil {
		t.Fatalf("patch %q: %v", key, err)
	}
	return e
}

func ptr(v uint64) *uint64 { return &v }

func equalJSON(t *testing.T, got, want json.RawMessage) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("stored value is not valid JSON: %v (%s)", err, got)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("expectation is not valid JSON: %v (%s)", err, want)
	}
	return reflect.DeepEqual(g, w)
}

// --- Versioning: U1-U5 ------------------------------------------------------

func TestVersioning(t *testing.T) {
	s := newStore(t)

	// U4: absent key.
	if _, ok := s.Get("missing"); ok {
		t.Fatal("Get on an absent key reported found")
	}

	// U1: first write.
	if e := put(t, s, "k", `{"a":1}`); e.Version != FirstVersion {
		t.Errorf("first write gave version %d, want %d", e.Version, FirstVersion)
	}

	// U2: second write.
	if e := put(t, s, "k", `{"a":2}`); e.Version != 2 {
		t.Errorf("second write gave version %d, want 2", e.Version)
	}

	// U3, U5: monotonic, no gaps.
	for want := uint64(3); want <= 10; want++ {
		if e := put(t, s, "k", `{"a":3}`); e.Version != want {
			t.Fatalf("write gave version %d, want %d", e.Version, want)
		}
	}

	got, ok := s.Get("k")
	if !ok {
		t.Fatal("key vanished")
	}
	if got.Version != 10 {
		t.Errorf("Get reports version %d, want 10", got.Version)
	}
}

// Version 0 must never be observable — the absent sentinel depends on it.
func TestVersionZeroNeverExists(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", `1`)
	patch(t, s, "j", `{"a":1}`)

	for _, key := range []string{"k", "j"} {
		e, ok := s.Get(key)
		if !ok {
			t.Fatalf("%q missing", key)
		}
		if e.Version == 0 {
			t.Errorf("%q has version 0, which must never exist", key)
		}
	}
}

// --- PUT: U6-U9 -------------------------------------------------------------

func TestPutReplacesWholeValue(t *testing.T) {
	cases := []struct {
		id      string
		current string
		body    string
		want    string
	}{
		{"U6", `{"a":1,"b":2}`, `{"c":3}`, `{"c":3}`}, // old fields gone
		{"U8", `{"a":1}`, `null`, `null`},
		{"U9", `{"a":1}`, `[]`, `[]`},
		{"", `{"a":1}`, `42`, `42`},
		{"", `{"a":1}`, `"str"`, `"str"`},
		{"", `{"a":1}`, `true`, `true`},
	}

	for _, c := range cases {
		t.Run(c.id+" "+c.current+" -> "+c.body, func(t *testing.T) {
			s := newStore(t)
			put(t, s, "k", c.current)
			put(t, s, "k", c.body)

			got, _ := s.Get("k")
			if !equalJSON(t, got.Value, json.RawMessage(c.want)) {
				t.Errorf("value is %s, want %s", got.Value, c.want)
			}
			if got.Version != 2 {
				t.Errorf("version is %d, want 2", got.Version)
			}
		})
	}
}

// U7: PUT on an absent key creates it at version 1.
func TestPutCreates(t *testing.T) {
	s := newStore(t)
	e := put(t, s, "k", `{"a":1}`)

	if e.Version != FirstVersion {
		t.Errorf("version %d, want %d", e.Version, FirstVersion)
	}
	if !equalJSON(t, e.Value, json.RawMessage(`{"a":1}`)) {
		t.Errorf("value %s, want {\"a\":1}", e.Value)
	}
}

// --- PATCH: U10-U20 ---------------------------------------------------------

func TestPatchRules(t *testing.T) {
	cases := []struct {
		id      string
		name    string
		current string // "" means the key is absent
		delta   string
		want    string
		wantVer uint64
	}{
		{"U10", "create from delta", "", `{"a":1}`, `{"a":1}`, 1},
		{"U11", "create stores a non-object delta verbatim", "", `[1,2]`, `[1,2]`, 1},
		{"U12", "merge adds a field", `{"a":1}`, `{"b":2}`, `{"a":1,"b":2}`, 2},
		{"U13", "delta wins", `{"a":1}`, `{"a":9}`, `{"a":9}`, 2},
		{"U14", "nested is replaced not deep-merged",
			`{"a":{"x":1}}`, `{"a":{"y":2}}`, `{"a":{"y":2}}`, 2},
		{"U15", "array current replaces", `[1,2]`, `{"a":1}`, `{"a":1}`, 2},
		{"U16", "array delta replaces", `{"a":1}`, `[1,2]`, `[1,2]`, 2},
		{"U17", "null delta replaces", `{"a":1}`, `null`, `null`, 2},
		{"U18", "string current replaces", `"str"`, `{"a":1}`, `{"a":1}`, 2},
		{"U19", "empty delta still bumps the version (ruling R2)",
			`{"a":1}`, `{}`, `{"a":1}`, 2},
		{"U20", "null field is set, not deleted (ruling R1)",
			`{"a":1,"b":2}`, `{"a":null}`, `{"a":null,"b":2}`, 2},
	}

	for _, c := range cases {
		t.Run(c.id+" "+c.name, func(t *testing.T) {
			s := newStore(t)
			if c.current != "" {
				put(t, s, "k", c.current)
			}
			patch(t, s, "k", c.delta)

			got, ok := s.Get("k")
			if !ok {
				t.Fatal("key missing after PATCH")
			}
			if !equalJSON(t, got.Value, json.RawMessage(c.want)) {
				t.Errorf("value %s, want %s", got.Value, c.want)
			}
			if got.Version != c.wantVer {
				t.Errorf("version %d, want %d", got.Version, c.wantVer)
			}
		})
	}
}

// --- ifVersion: U21-U26b, run against BOTH methods --------------------------
//
// The spec applies the guard to PUT and PATCH alike. Testing only PUT is the
// most likely way to ship a broken PATCH guard, since the two paths diverge
// exactly where the merge happens.

func TestIfVersionGuard(t *testing.T) {
	modes := []struct {
		name string
		mode Mode
	}{
		{"PUT", ModeReplace},
		{"PATCH", ModeMerge},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			t.Run("U21 match proceeds", func(t *testing.T) {
				s := newStore(t)
				put(t, s, "k", `{"a":1}`)
				e, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"a":2}`),
					Mode: m.mode, IfVersion: ptr(1)})
				if err != nil {
					t.Fatalf("matching guard rejected: %v", err)
				}
				if e.Version != 2 {
					t.Errorf("version %d, want 2", e.Version)
				}
			})

			t.Run("U22 stale is rejected and reports current", func(t *testing.T) {
				s := newStore(t)
				put(t, s, "k", `{"a":1}`)
				put(t, s, "k", `{"a":2}`)
				put(t, s, "k", `{"a":3}`) // now at version 3

				_, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{}`),
					Mode: m.mode, IfVersion: ptr(2)})

				var mismatch *VersionMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("got %v, want VersionMismatchError", err)
				}
				if mismatch.Current != 3 {
					t.Errorf("error reports current version %d, want 3", mismatch.Current)
				}
				if mismatch.Expected != 2 {
					t.Errorf("error reports expected version %d, want 2", mismatch.Expected)
				}
			})

			t.Run("U23 future version is rejected", func(t *testing.T) {
				s := newStore(t)
				put(t, s, "k", `{"a":1}`)
				_, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{}`),
					Mode: m.mode, IfVersion: ptr(99)})
				var mismatch *VersionMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("got %v, want VersionMismatchError", err)
				}
			})

			t.Run("U24 absent key is rejected with current 0", func(t *testing.T) {
				s := newStore(t)
				_, err := s.Write(WriteOp{Key: "nope", Body: json.RawMessage(`{"a":1}`),
					Mode: m.mode, IfVersion: ptr(1)})

				var mismatch *VersionMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("got %v, want VersionMismatchError", err)
				}
				if mismatch.Current != 0 {
					t.Errorf("current is %d, want 0 to signal absence", mismatch.Current)
				}
				if _, ok := s.Get("nope"); ok {
					t.Error("rejected write left a phantom entry behind")
				}
			})

			t.Run("U25 ifVersion=0 is always a mismatch", func(t *testing.T) {
				s := newStore(t)
				put(t, s, "k", `{"a":1}`)
				_, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{}`),
					Mode: m.mode, IfVersion: ptr(0)})
				var mismatch *VersionMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("got %v, want VersionMismatchError", err)
				}
			})

			t.Run("U26 omitted guard writes unconditionally", func(t *testing.T) {
				s := newStore(t)
				put(t, s, "k", `{"a":1}`)
				put(t, s, "k", `{"a":2}`)
				if _, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"a":3}`),
					Mode: m.mode}); err != nil {
					t.Fatalf("unconditional write failed: %v", err)
				}
			})

			t.Run("U26b create with no guard works", func(t *testing.T) {
				s := newStore(t)
				e, err := s.Write(WriteOp{Key: "fresh", Body: json.RawMessage(`{"a":1}`),
					Mode: m.mode})
				if err != nil {
					t.Fatalf("create failed: %v", err)
				}
				if e.Version != FirstVersion {
					t.Errorf("version %d, want %d", e.Version, FirstVersion)
				}
			})
		})
	}
}

// A rejected guard must not bump the version or alter the value.
func TestRejectedWriteChangesNothing(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", `{"a":1}`)

	before, _ := s.Get("k")
	_, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"a":99}`),
		Mode: ModeReplace, IfVersion: ptr(42)})
	if err == nil {
		t.Fatal("stale guard was accepted")
	}

	after, _ := s.Get("k")
	if after.Version != before.Version {
		t.Errorf("version moved from %d to %d on a rejected write", before.Version, after.Version)
	}
	if !equalJSON(t, after.Value, before.Value) {
		t.Errorf("value changed on a rejected write: %s -> %s", before.Value, after.Value)
	}
}

// --- Value fidelity: U27-U32 ------------------------------------------------

// U27, U28: raw bytes go in and the same bytes come out. A store that decoded
// into a map and re-encoded would reorder keys and normalise whitespace, so this
// is the test that proves the storage decision was real.
func TestPutIsByteExact(t *testing.T) {
	cases := []string{
		`{"b":1,"a":2}`,
		"{\"a\":  1}",
		"{\n  \"a\": 1\n}",
		`{"z":1,"m":2,"a":3}`,
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			s := newStore(t)
			put(t, s, "k", body)
			got, _ := s.Get("k")
			if string(got.Value) != body {
				t.Errorf("stored %s, got back %s", body, got.Value)
			}
		})
	}
}

// U29-U31: arbitrary JSON round-trips.
func TestArbitraryJSONValues(t *testing.T) {
	values := []string{
		`null`, `true`, `false`, `42`, `-1.5e10`, `"str"`, `""`,
		`[]`, `{}`, `[1,2,3]`, `{"a":1}`,
		`{"emoji":"🔑","th":"ไทย"}`,
		`{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":1}}}}}}}}}}`,
		`{"escaped":"line\nbreak\ttab\"quote"}`,
	}
	for _, v := range values {
		t.Run(v, func(t *testing.T) {
			s := newStore(t)
			put(t, s, "k", v)
			got, _ := s.Get("k")
			if string(got.Value) != v {
				t.Errorf("stored %s, got back %s", v, got.Value)
			}
		})
	}
}

// U32: Go's encoding/json takes the last of duplicate keys. Documented, not
// enforced — rejecting would need a custom decoder for no benefit.
func TestDuplicateJSONKeysLastWins(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", `{"a":1,"a":2}`)
	patch(t, s, "k", `{"b":3}`)

	got, _ := s.Get("k")
	if !equalJSON(t, got.Value, json.RawMessage(`{"a":2,"b":3}`)) {
		t.Errorf("value %s, want {\"a\":2,\"b\":3}", got.Value)
	}
}

// The store must not alias a caller's buffer — HTTP body buffers get reused.
func TestStoreDoesNotAliasCallerBuffer(t *testing.T) {
	s := newStore(t)
	body := []byte(`{"a":1}`)
	if _, err := s.Write(WriteOp{Key: "k", Body: body, Mode: ModeReplace}); err != nil {
		t.Fatal(err)
	}

	body[2] = 'X' // caller reuses its buffer

	got, _ := s.Get("k")
	if string(got.Value) != `{"a":1}` {
		t.Errorf("stored value followed the caller's buffer: %s", got.Value)
	}
}

// A value handed out by Get must stay readable even as writers replace it. This
// is the invariant that lets Get skip copying.
func TestReadValueSurvivesConcurrentWrites(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", `{"a":1}`)

	snapshot, _ := s.Get("k")
	held := string(snapshot.Value)

	for i := 0; i < 100; i++ {
		put(t, s, "k", fmt.Sprintf(`{"a":%d}`, i))
	}

	if string(snapshot.Value) != held {
		t.Errorf("a previously returned value changed underneath the reader: %s, was %s",
			snapshot.Value, held)
	}
}

// --- Keys (Part 2) ----------------------------------------------------------

func TestKeys(t *testing.T) {
	s := newStore(t)
	want := map[string]bool{}
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%d", i)
		put(t, s, key, `1`)
		want[key] = true
	}

	seen := map[string]int{}
	for k := range s.Keys() {
		seen[k]++
	}

	if len(seen) != len(want) {
		t.Errorf("iterated %d keys, want %d", len(seen), len(want))
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("key %q yielded %d times, want exactly 1", k, n)
		}
		if !want[k] {
			t.Errorf("unexpected key %q", k)
		}
	}
}

func TestKeysOnEmptyStore(t *testing.T) {
	s := newStore(t)
	for k := range s.Keys() {
		t.Fatalf("empty store yielded %q", k)
	}
}

func TestKeysStopsEarly(t *testing.T) {
	s := newStore(t)
	for i := 0; i < 100; i++ {
		put(t, s, fmt.Sprintf("key-%d", i), `1`)
	}

	count := 0
	for range s.Keys() {
		count++
		if count == 5 {
			break
		}
	}
	if count != 5 {
		t.Errorf("stopped after %d keys, want 5", count)
	}
}

// --- Concurrency: C1, C3, C4, C9, C10 ---------------------------------------

// C1: the assignment's required test, at the store level. Three clients, a
// hundred CAS increments each, exactly 300 — and version 300 too, which proves
// no write was wasted or double-applied.
func TestConcurrentCounterWithIfVersion(t *testing.T) {
	s := newStore(t)
	const (
		clients   = 3
		increment = 100
	)

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < increment; i++ {
				for { // ifVersion retry loop — this is what makes 300 reachable
					op := WriteOp{Key: "counter", Mode: ModeReplace}

					if cur, ok := s.Get("counter"); ok {
						var count int
						if err := json.Unmarshal(cur.Value, &count); err != nil {
							t.Errorf("counter is not a number: %s", cur.Value)
							return
						}
						v := cur.Version
						op.IfVersion = &v
						op.Body = json.RawMessage(fmt.Sprintf("%d", count+1))
					} else {
						// Bootstrap. Writing unconditionally here would let two
						// clients both create the key and lose one increment, so
						// the first write has to assert "must not exist".
						op.IfAbsent = true
						op.Body = json.RawMessage("1")
					}

					if _, err := s.Write(op); err == nil {
						break
					} else {
						var mismatch *VersionMismatchError
						if !errors.As(err, &mismatch) {
							t.Errorf("unexpected write error: %v", err)
							return
						}
						// Someone else won; re-read and try again.
					}
				}
			}
		}()
	}
	wg.Wait()

	got, ok := s.Get("counter")
	if !ok {
		t.Fatal("counter missing")
	}

	var final int
	if err := json.Unmarshal(got.Value, &final); err != nil {
		t.Fatal(err)
	}
	if want := clients * increment; final != want {
		t.Errorf("counter is %d, want %d — an update was lost", final, want)
	}
	if want := uint64(clients * increment); got.Version != want {
		t.Errorf("version is %d, want %d — writes were wasted or double-applied", got.Version, want)
	}
}

// C2: the negative control. Without the ifVersion guard, concurrent
// read-modify-write loses updates. If this ever reaches 300 the test above proves
// nothing, because it would mean the race never actually occurred.
func TestConcurrentCounterWithoutIfVersionLosesUpdates(t *testing.T) {
	s := newStore(t)
	const (
		clients   = 3
		increment = 100
	)

	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < increment; i++ {
				count := 0
				if cur, ok := s.Get("counter"); ok {
					_ = json.Unmarshal(cur.Value, &count)
				}
				// No IfVersion: last writer wins.
				_, _ = s.Write(WriteOp{
					Key:  "counter",
					Body: json.RawMessage(fmt.Sprintf("%d", count+1)),
					Mode: ModeReplace,
				})
			}
		}()
	}
	wg.Wait()

	got, _ := s.Get("counter")
	var final int
	if err := json.Unmarshal(got.Value, &final); err != nil {
		t.Fatal(err)
	}
	if final >= clients*increment {
		t.Errorf("got %d without ifVersion; expected fewer than %d. "+
			"No updates were lost, so the positive test proves nothing.",
			final, clients*increment)
	}
	t.Logf("without ifVersion: %d of %d increments survived", final, clients*increment)
}

// C3: many goroutines creating the same new key. Exactly one may create it, all
// must succeed, and the final version must equal the number of writes — the
// double-checked create is what makes this hold.
func TestConcurrentCreateRace(t *testing.T) {
	const writers = 50

	for attempt := 0; attempt < 20; attempt++ { // repeat: the window is narrow
		s := newStore(t)
		var wg sync.WaitGroup
		start := make(chan struct{})

		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start // maximise contention
				if _, err := s.Write(WriteOp{
					Key:  "contended",
					Body: json.RawMessage(fmt.Sprintf(`{"writer":%d}`, i)),
					Mode: ModeReplace,
				}); err != nil {
					t.Errorf("write failed: %v", err)
				}
			}(i)
		}

		close(start)
		wg.Wait()

		got, ok := s.Get("contended")
		if !ok {
			t.Fatal("key missing after concurrent creates")
		}
		if got.Version != writers {
			t.Fatalf("attempt %d: version is %d, want %d — a write was lost",
				attempt, got.Version, writers)
		}
		if s.Len() != 1 {
			t.Fatalf("store holds %d keys, want 1", s.Len())
		}
	}
}

// C4: distinct keys never interfere.
func TestConcurrentIndependentKeys(t *testing.T) {
	s := newStore(t)
	const (
		keys         = 100
		writesPerKey = 100
	)

	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", k)
			for i := 0; i < writesPerKey; i++ {
				if _, err := s.Write(WriteOp{
					Key:  key,
					Body: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
					Mode: ModeReplace,
				}); err != nil {
					t.Errorf("write to %q failed: %v", key, err)
					return
				}
			}
		}(k)
	}
	wg.Wait()

	if s.Len() != keys {
		t.Errorf("store holds %d keys, want %d", s.Len(), keys)
	}
	for k := 0; k < keys; k++ {
		got, ok := s.Get(fmt.Sprintf("key-%d", k))
		if !ok {
			t.Fatalf("key-%d missing", k)
		}
		if got.Version != writesPerKey {
			t.Errorf("key-%d is at version %d, want %d", k, got.Version, writesPerKey)
		}
	}
}

// C5: concurrent PATCHes of different fields must all survive, which proves the
// merge itself runs inside the critical section rather than around it.
func TestConcurrentMergeOfDifferentFields(t *testing.T) {
	s := newStore(t)
	put(t, s, "doc", `{}`)

	const (
		writers = 4
		bumps   = 50
	)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			field := fmt.Sprintf("f%d", w)
			for i := 1; i <= bumps; i++ {
				for {
					cur, _ := s.Get("doc")
					v := cur.Version
					_, err := s.Write(WriteOp{
						Key:       "doc",
						Body:      json.RawMessage(fmt.Sprintf(`{%q:%d}`, field, i)),
						Mode:      ModeMerge,
						IfVersion: &v,
					})
					if err == nil {
						break
					}
					var mismatch *VersionMismatchError
					if !errors.As(err, &mismatch) {
						t.Errorf("unexpected error: %v", err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	got, _ := s.Get("doc")
	var fields map[string]int
	if err := json.Unmarshal(got.Value, &fields); err != nil {
		t.Fatalf("document is not an object: %v (%s)", err, got.Value)
	}
	for w := 0; w < writers; w++ {
		field := fmt.Sprintf("f%d", w)
		if fields[field] != bumps {
			t.Errorf("field %s is %d, want %d — a merge was lost", field, fields[field], bumps)
		}
	}
}

// C7: a reader polling one key must never see its version go backwards, and must
// never see a value that fails to parse.
func TestReaderMonotonicity(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", `{"n":0}`)

	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			put(t, s, "k", fmt.Sprintf(`{"n":%d}`, i))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var last uint64
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			got, ok := s.Get("k")
			if !ok {
				t.Error("key vanished mid-read")
				return
			}
			if got.Version < last {
				t.Errorf("version went backwards: %d after %d", got.Version, last)
				return
			}
			last = got.Version
			if !json.Valid(got.Value) {
				t.Errorf("read a torn value: %q", got.Value)
				return
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(done)
	wg.Wait()
}

// C9: the assignment requires that different keys proceed concurrently. Note
// that TestConcurrentIndependentKeys would pass under a single global mutex —
// it only proves correctness. This is the test that proves concurrency, by
// holding one key busy and checking another is unaffected.
func TestDifferentKeysProceedConcurrently(t *testing.T) {
	s := newStore(t)
	put(t, s, "slow", `{}`)
	put(t, s, "fast", `{}`)

	const hold = 200 * time.Millisecond

	// Occupy "slow" with a long merge. A large value makes the critical section
	// genuinely slow rather than relying on a sleep inside the store.
	big := make(map[string]int, 20000)
	for i := 0; i < 20000; i++ {
		big[fmt.Sprintf("field-%d", i)] = i
	}
	bigJSON, err := json.Marshal(big)
	if err != nil {
		t.Fatal(err)
	}

	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		deadline := time.Now().Add(hold)
		for time.Now().Before(deadline) {
			if _, err := s.Write(WriteOp{Key: "slow", Body: bigJSON, Mode: ModeMerge}); err != nil {
				t.Errorf("slow write failed: %v", err)
				return
			}
		}
	}()

	// Meanwhile "fast" must stay responsive.
	start := time.Now()
	const fastWrites = 1000
	for i := 0; i < fastWrites; i++ {
		if _, err := s.Write(WriteOp{
			Key:  "fast",
			Body: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
			Mode: ModeReplace,
		}); err != nil {
			t.Fatalf("fast write failed: %v", err)
		}
	}
	elapsed := time.Since(start)
	<-slowDone

	if elapsed > hold {
		t.Errorf("%d writes to an idle key took %v while another key was busy for %v; "+
			"keys are serialising against each other", fastWrites, elapsed, hold)
	}
	t.Logf("%d writes to an idle key took %v while another key was under sustained load",
		fastWrites, elapsed)
}

// C10: the complement — the same key must serialise. Two writers' critical
// sections may never overlap, which is what per-key atomicity means.
func TestSameKeySerialises(t *testing.T) {
	s := newStore(t)
	put(t, s, "k", `{"a":0}`)

	const writers = 8
	const writesEach = 200

	var (
		mu     sync.Mutex
		inside int
		maxIn  int
	)

	// A merge callback cannot be injected, so approximate by checking that the
	// observed version sequence has no duplicates: every write must see a
	// distinct version, which can only happen if writes are serialised.
	versions := make(map[uint64]int)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesEach; i++ {
				e, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"a":1}`),
					Mode: ModeReplace})
				if err != nil {
					t.Errorf("write failed: %v", err)
					return
				}
				mu.Lock()
				versions[e.Version]++
				inside++
				if inside > maxIn {
					maxIn = inside
				}
				inside--
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(versions) != writers*writesEach {
		t.Errorf("saw %d distinct versions across %d writes — two writes shared a version, "+
			"so the critical sections overlapped", len(versions), writers*writesEach)
	}
	for v, n := range versions {
		if n != 1 {
			t.Errorf("version %d was returned %d times", v, n)
		}
	}
}

// C8: sustained mixed load. The point is the race detector, not the assertions.
func TestMixedOperationsUnderLoad(t *testing.T) {
	s := newStore(t)
	const goroutines = 16

	deadline := time.Now().Add(300 * time.Millisecond)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				key := fmt.Sprintf("key-%d", (g*7+i)%50)
				switch i % 4 {
				case 0:
					_, _ = s.Write(WriteOp{Key: key,
						Body: json.RawMessage(`{"a":1}`), Mode: ModeReplace})
				case 1:
					_, _ = s.Write(WriteOp{Key: key,
						Body: json.RawMessage(`{"b":2}`), Mode: ModeMerge})
				case 2:
					if e, ok := s.Get(key); ok && !json.Valid(e.Value) {
						t.Errorf("torn value at %q: %q", key, e.Value)
						return
					}
				case 3:
					n := 0
					for range s.Keys() {
						if n++; n > 10 {
							break
						}
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// --- Configuration ----------------------------------------------------------

func TestNewRejectsBadPartitionCount(t *testing.T) {
	for _, n := range []int{0, -1, 3, 100, 1000} {
		if _, err := New(n); err == nil {
			t.Errorf("New(%d) succeeded, want error", n)
		}
	}
	for _, n := range []int{1, 2, 64, 1024} {
		if _, err := New(n); err != nil {
			t.Errorf("New(%d): %v", n, err)
		}
	}
}

// Keys must land across partitions rather than piling into one.
func TestKeysSpreadAcrossPartitions(t *testing.T) {
	s := newStore(t)
	for i := 0; i < 5000; i++ {
		put(t, s, fmt.Sprintf("key-%d", i), `1`)
	}

	used := 0
	for _, p := range s.parts {
		if len(p.m) > 0 {
			used++
		}
	}
	if used < testPartitions/2 {
		t.Errorf("only %d of %d partitions used; the hash is not spreading keys",
			used, testPartitions)
	}
}

func TestModeString(t *testing.T) {
	if !strings.Contains(ModeReplace.String(), "replace") {
		t.Errorf("ModeReplace prints as %q", ModeReplace)
	}
	if !strings.Contains(ModeMerge.String(), "merge") {
		t.Errorf("ModeMerge prints as %q", ModeMerge)
	}
}

// --- Benchmarks -------------------------------------------------------------

func BenchmarkGet(b *testing.B) {
	s, _ := New(1024)
	_, _ = s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"a":1}`), Mode: ModeReplace})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = s.Get("k")
		}
	})
}

func BenchmarkPutDistinctKeys(b *testing.B) {
	s, _ := New(1024)
	body := json.RawMessage(`{"a":1}`)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			_, _ = s.Write(WriteOp{Key: fmt.Sprintf("key-%d", i), Body: body, Mode: ModeReplace})
		}
	})
}

// Partition count drives contention on the create path — the striping argument,
// measured. Run with: go test -bench BenchmarkCreateByPartitionCount ./internal/store/
func BenchmarkCreateByPartitionCount(b *testing.B) {
	for _, count := range []int{1, 16, 256, 1024} {
		b.Run(fmt.Sprintf("partitions=%d", count), func(b *testing.B) {
			s, _ := New(count)
			body := json.RawMessage(`{"a":1}`)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					i++
					_, _ = s.Write(WriteOp{
						Key:  fmt.Sprintf("k-%d-%d", i, count),
						Body: body,
						Mode: ModeReplace,
					})
				}
			})
		})
	}
}

// --- IfAbsent (create-only) -------------------------------------------------
//
// Added after the 300-counter test landed on 298: a CAS loop has no way to
// protect its very first write, because there is no version to assert yet.
// DynamoDB's attribute_not_exists solves the same problem.

func TestIfAbsent(t *testing.T) {
	t.Run("creates when the key is absent", func(t *testing.T) {
		s := newStore(t)
		e, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"a":1}`),
			Mode: ModeReplace, IfAbsent: true})
		if err != nil {
			t.Fatalf("create-only write on an absent key failed: %v", err)
		}
		if e.Version != FirstVersion {
			t.Errorf("version %d, want %d", e.Version, FirstVersion)
		}
	})

	t.Run("rejects when the key exists", func(t *testing.T) {
		s := newStore(t)
		put(t, s, "k", `{"a":1}`)

		_, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"a":2}`),
			Mode: ModeReplace, IfAbsent: true})

		var mismatch *VersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("got %v, want VersionMismatchError", err)
		}
		if !mismatch.RequiredAbsent {
			t.Error("error does not record that absence was required")
		}
		if mismatch.Current != 1 {
			t.Errorf("current version %d, want 1", mismatch.Current)
		}

		got, _ := s.Get("k")
		if got.Version != 1 {
			t.Errorf("rejected write bumped the version to %d", got.Version)
		}
	})

	t.Run("rejects conflicting guards", func(t *testing.T) {
		s := newStore(t)
		_, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`1`),
			Mode: ModeReplace, IfAbsent: true, IfVersion: ptr(1)})
		if !errors.Is(err, ErrConflictingGuards) {
			t.Errorf("got %v, want ErrConflictingGuards", err)
		}
	})

	t.Run("works for PATCH too", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"a":1}`),
			Mode: ModeMerge, IfAbsent: true}); err != nil {
			t.Fatalf("create-only PATCH failed: %v", err)
		}
		if _, err := s.Write(WriteOp{Key: "k", Body: json.RawMessage(`{"b":2}`),
			Mode: ModeMerge, IfAbsent: true}); err == nil {
			t.Error("create-only PATCH succeeded on an existing key")
		}
	})

	// Exactly one of many concurrent create-only writers may win. This is the
	// property the counter test's bootstrap depends on.
	t.Run("exactly one concurrent creator wins", func(t *testing.T) {
		for attempt := 0; attempt < 20; attempt++ {
			s := newStore(t)
			const writers = 50

			var (
				wg       sync.WaitGroup
				mu       sync.Mutex
				accepted int
			)
			start := make(chan struct{})

			for i := 0; i < writers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					_, err := s.Write(WriteOp{
						Key:      "race",
						Body:     json.RawMessage(fmt.Sprintf(`{"w":%d}`, i)),
						Mode:     ModeReplace,
						IfAbsent: true,
					})
					if err == nil {
						mu.Lock()
						accepted++
						mu.Unlock()
					}
				}(i)
			}
			close(start)
			wg.Wait()

			if accepted != 1 {
				t.Fatalf("attempt %d: %d writers created the key, want exactly 1",
					attempt, accepted)
			}
			got, _ := s.Get("race")
			if got.Version != FirstVersion {
				t.Fatalf("version %d, want %d", got.Version, FirstVersion)
			}
		}
	})
}
