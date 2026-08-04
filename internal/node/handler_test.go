package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"codakv/internal/routing"
	"codakv/internal/store"
)

const testPartitions = 1024

// newServer starts a node owning every partition, so ownership never interferes
// with the semantics tests. Misrouting gets its own test below.
func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	return newServerWithRange(t, routing.Range{Lo: 0, Hi: testPartitions})
}

func newServerWithRange(t *testing.T, owned routing.Range) *httptest.Server {
	t.Helper()

	kv, err := store.New(testPartitions)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	svc := NewService(kv, Config{
		NodeID:         "node-test",
		PartitionCount: testPartitions,
		Owned:          owned,
		MaxValueBytes:  1 << 20,
		MaxKeyBytes:    1 << 10,
	})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := httptest.NewServer(NewHandler(svc, log).Routes())
	t.Cleanup(srv.Close)
	return srv
}

// do issues a request against the server. target is appended to the base URL
// verbatim, so tests control the exact escaping.
func do(t *testing.T, srv *httptest.Server, method, target, body string) (int, []byte) {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+target, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, out
}

func decodeSuccess(t *testing.T, raw []byte) successBody {
	t.Helper()
	var out successBody
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("response is not a success body: %v (%s)", err, raw)
	}
	return out
}

func decodeError(t *testing.T, raw []byte) errorBody {
	t.Helper()
	var out errorBody
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("response is not an error body: %v (%s)", err, raw)
	}
	return out
}

// --- H1-H5: the assignment's own examples, in order -------------------------

func TestAssignmentExamples(t *testing.T) {
	srv := newServer(t)

	// H1: put a full object.
	status, body := do(t, srv, "PUT", "/kv/user:42", `{"name":"Ari","points":10}`)
	if status != http.StatusOK {
		t.Fatalf("PUT: status %d, body %s", status, body)
	}
	got := decodeSuccess(t, body)
	if got.Key != "user:42" || got.Version != 1 {
		t.Errorf("PUT gave key=%q version=%d, want user:42 / 1", got.Key, got.Version)
	}

	// H2: get it back.
	status, body = do(t, srv, "GET", "/kv/user:42", "")
	if status != http.StatusOK {
		t.Fatalf("GET: status %d, body %s", status, body)
	}
	got = decodeSuccess(t, body)
	if string(got.Value) != `{"name":"Ari","points":10}` {
		t.Errorf("GET returned %s, want the stored bytes unchanged", got.Value)
	}

	// H3: conditional replace.
	status, body = do(t, srv, "PUT", "/kv/user:42?ifVersion=1", `{"name":"Ari","points":20}`)
	if status != http.StatusOK {
		t.Fatalf("conditional PUT: status %d, body %s", status, body)
	}
	if got = decodeSuccess(t, body); got.Version != 2 {
		t.Errorf("version %d, want 2", got.Version)
	}

	// H4: upsert/merge.
	status, body = do(t, srv, "PATCH", "/kv/user:42", `{"rank":"gold"}`)
	if status != http.StatusOK {
		t.Fatalf("PATCH: status %d, body %s", status, body)
	}
	got = decodeSuccess(t, body)
	if got.Version != 3 {
		t.Errorf("version %d, want 3", got.Version)
	}

	var merged map[string]any
	if err := json.Unmarshal(got.Value, &merged); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{"name": "Ari", "points": float64(20), "rank": "gold"} {
		if merged[k] != want {
			t.Errorf("merged[%q] = %v, want %v", k, merged[k], want)
		}
	}
}

// --- H6-H18: errors ---------------------------------------------------------

func TestErrorResponses(t *testing.T) {
	cases := []struct {
		id     string
		name   string
		setup  string // optional PUT body to create /kv/k first
		method string
		target string
		body   string
		want   int
	}{
		{"H6", "GET missing", "", "GET", "/kv/nonexistent", "", http.StatusNotFound},
		{"H7", "malformed JSON", "", "PUT", "/kv/k", `{bad`, http.StatusBadRequest},
		{"H8", "empty body", "", "PUT", "/kv/k", "", http.StatusBadRequest},
		{"H10", "ifVersion on absent key", "", "PUT", "/kv/absent?ifVersion=1", `{"a":1}`, http.StatusConflict},
		{"H11", "ifVersion=0", `{"a":1}`, "PUT", "/kv/k?ifVersion=0", `{"a":2}`, http.StatusConflict},
		{"H12", "ifVersion not a number", `{"a":1}`, "PUT", "/kv/k?ifVersion=abc", `{"a":2}`, http.StatusBadRequest},
		{"H13", "negative ifVersion", `{"a":1}`, "PUT", "/kv/k?ifVersion=-1", `{"a":2}`, http.StatusBadRequest},
		{"H14", "ifVersion overflows uint64", `{"a":1}`, "PUT", "/kv/k?ifVersion=99999999999999999999999", `{"a":2}`, http.StatusBadRequest},
		{"H15", "duplicate ifVersion", `{"a":1}`, "PUT", "/kv/k?ifVersion=1&ifVersion=2", `{"a":2}`, http.StatusBadRequest},
		{"H16", "DELETE not allowed", `{"a":1}`, "DELETE", "/kv/k", "", http.StatusMethodNotAllowed},
		{"H17", "empty key", "", "PUT", "/kv/", `{"a":1}`, http.StatusNotFound},

		{"H15a", "PATCH ifVersion on absent key", "", "PATCH", "/kv/absent?ifVersion=1", `{"a":1}`, http.StatusConflict},
		{"H15c", "PATCH malformed delta", `{"a":1}`, "PATCH", "/kv/k", `{bad`, http.StatusBadRequest},

		{"", "conflicting guards", "", "PUT", "/kv/k?ifVersion=1&ifAbsent=true", `{"a":1}`, http.StatusBadRequest},
		{"", "ifAbsent not a boolean", "", "PUT", "/kv/k?ifAbsent=maybe", `{"a":1}`, http.StatusBadRequest},
		{"", "multi-segment path", "", "GET", "/kv/a/b", "", http.StatusNotFound},
	}

	for _, c := range cases {
		t.Run(c.id+" "+c.name, func(t *testing.T) {
			srv := newServer(t)
			if c.setup != "" {
				if status, body := do(t, srv, "PUT", "/kv/k", c.setup); status != http.StatusOK {
					t.Fatalf("setup failed: %d %s", status, body)
				}
			}
			status, body := do(t, srv, c.method, c.target, c.body)
			if status != c.want {
				t.Errorf("status %d, want %d (body %s)", status, c.want, body)
			}
		})
	}
}

// H9: the 409 body must carry the current version. This is the assertion that
// catches a proxy re-encoding responses instead of forwarding bytes — without
// it, a client's retry loop silently degrades into an extra round trip.
func TestConflictCarriesCurrentVersion(t *testing.T) {
	for _, method := range []string{"PUT", "PATCH"} {
		t.Run(method, func(t *testing.T) {
			srv := newServer(t)
			do(t, srv, "PUT", "/kv/k", `{"a":1}`)
			do(t, srv, "PUT", "/kv/k", `{"a":2}`)
			do(t, srv, "PUT", "/kv/k", `{"a":3}`) // version 3

			status, body := do(t, srv, method, "/kv/k?ifVersion=1", `{"a":9}`)
			if status != http.StatusConflict {
				t.Fatalf("status %d, want 409 (body %s)", status, body)
			}

			got := decodeError(t, body)
			if got.Version == nil {
				t.Fatalf("409 body has no version field: %s", body)
			}
			if *got.Version != 3 {
				t.Errorf("409 reports version %d, want 3", *got.Version)
			}
		})
	}
}

// H18: an oversized body is refused before it can be buffered.
func TestValueTooLarge(t *testing.T) {
	kv, _ := store.New(testPartitions)
	svc := NewService(kv, Config{
		NodeID:         "node-test",
		PartitionCount: testPartitions,
		Owned:          routing.Range{Lo: 0, Hi: testPartitions},
		MaxValueBytes:  1024, // small, so the test stays fast
		MaxKeyBytes:    1 << 10,
	})
	srv := httptest.NewServer(NewHandler(svc, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes())
	t.Cleanup(srv.Close)

	body := `{"a":"` + strings.Repeat("x", 4096) + `"}`
	status, out := do(t, srv, "PUT", "/kv/k", body)
	if status != http.StatusRequestEntityTooLarge {
		t.Errorf("status %d, want 413 (body %s)", status, out)
	}
}

// H24: an over-long key is refused.
func TestKeyTooLong(t *testing.T) {
	srv := newServer(t)
	key := strings.Repeat("k", 2048)
	status, body := do(t, srv, "PUT", "/kv/"+key, `{"a":1}`)
	if status != http.StatusRequestURITooLong {
		t.Errorf("status %d, want 414 (body %s)", status, body)
	}
}

// --- H19-H23: key encoding --------------------------------------------------
//
// Where hand-rolled routers break. Go's ServeMux gets these right: it matches on
// the escaped path and hands back a decoded segment, so no manual unescaping is
// needed.

func TestKeyEncoding(t *testing.T) {
	cases := []struct {
		id  string
		key string
	}{
		{"H19", "user:42"},
		{"H20", "a/b"},
		{"H21", "key with space"},
		{"H22", "k?x=1"},
		{"H23", "ไทย🔑"},
		{"", "a#b"},
		{"", "a&b=c"},
		{"", "100%"},
		{"", "emoji-only-🔑"},
	}

	for _, c := range cases {
		t.Run(c.id+" "+c.key, func(t *testing.T) {
			srv := newServer(t)
			target := "/kv/" + url.PathEscape(c.key)

			status, body := do(t, srv, "PUT", target, `{"a":1}`)
			if status != http.StatusOK {
				t.Fatalf("PUT %s: status %d, body %s", target, status, body)
			}
			if got := decodeSuccess(t, body); got.Key != c.key {
				t.Errorf("stored key %q, want %q", got.Key, c.key)
			}

			status, body = do(t, srv, "GET", target, "")
			if status != http.StatusOK {
				t.Fatalf("GET %s: status %d, body %s", target, status, body)
			}
			if got := decodeSuccess(t, body); got.Key != c.key {
				t.Errorf("read back key %q, want %q", got.Key, c.key)
			}
		})
	}
}

// H20 again, explicitly: an encoded slash must stay one key rather than becoming
// two path segments.
func TestEncodedSlashIsOneKey(t *testing.T) {
	srv := newServer(t)

	if status, body := do(t, srv, "PUT", "/kv/a%2Fb", `{"which":"encoded"}`); status != http.StatusOK {
		t.Fatalf("PUT /kv/a%%2Fb: status %d, body %s", status, body)
	}
	// The unencoded form is a different path with two segments, and matches no route.
	if status, _ := do(t, srv, "GET", "/kv/a/b", ""); status != http.StatusNotFound {
		t.Errorf("GET /kv/a/b: status %d, want 404", status)
	}
	status, body := do(t, srv, "GET", "/kv/a%2Fb", "")
	if status != http.StatusOK {
		t.Fatalf("GET /kv/a%%2Fb: status %d, body %s", status, body)
	}
	if got := decodeSuccess(t, body); got.Key != "a/b" {
		t.Errorf("key %q, want a/b", got.Key)
	}
}

// --- Value fidelity ---------------------------------------------------------

// U27 over HTTP: a value comes back with its key order intact.
//
// Key order is the property worth testing, because it is what distinguishes raw
// byte storage from decoding into a map and re-encoding — Go sorts map keys when
// marshalling, so a map-backed store would return {"a":2,"b":1} here.
//
// Insignificant whitespace does NOT survive: encoding/json compacts a
// RawMessage when it marshals one into the response. That is a wire-format
// detail rather than a storage one — the store itself keeps the original bytes,
// as internal/store's TestPutIsByteExact shows — and JSON treats the two forms
// as identical, so preserving it by hand-assembling responses would buy nothing.
func TestValueRoundTripsPreservingKeyOrder(t *testing.T) {
	values := []string{
		`{"b":1,"a":2}`,
		`{"z":1,"m":2,"a":3}`,
		`{"a":  1}`,
		`null`, `true`, `42`, `-1.5e10`, `"str"`, `[]`, `{}`,
		`{"emoji":"🔑","th":"ไทย"}`,
		`[1,[2,[3,[4]]]]`,
	}
	for _, v := range values {
		t.Run(v, func(t *testing.T) {
			srv := newServer(t)
			if status, body := do(t, srv, "PUT", "/kv/k", v); status != http.StatusOK {
				t.Fatalf("PUT: status %d, body %s", status, body)
			}

			var want bytes.Buffer
			if err := json.Compact(&want, []byte(v)); err != nil {
				t.Fatalf("test input is not valid JSON: %v", err)
			}

			_, body := do(t, srv, "GET", "/kv/k", "")
			if got := decodeSuccess(t, body); string(got.Value) != want.String() {
				t.Errorf("sent %s, got back %s, want %s", v, got.Value, want.String())
			}
		})
	}
}

// The order test on its own, since it is the one that would fail if the store
// ever started decoding values.
func TestKeyOrderIsNotSorted(t *testing.T) {
	srv := newServer(t)
	const value = `{"zebra":1,"apple":2,"mango":3}`

	if status, body := do(t, srv, "PUT", "/kv/k", value); status != http.StatusOK {
		t.Fatalf("PUT: status %d, body %s", status, body)
	}
	_, body := do(t, srv, "GET", "/kv/k", "")
	got := string(decodeSuccess(t, body).Value)

	if got != value {
		t.Errorf("got %s, want %s", got, value)
	}
	if got == `{"apple":2,"mango":3,"zebra":1}` {
		t.Error("keys came back sorted: the store is decoding and re-encoding values")
	}
}

// --- ifAbsent ---------------------------------------------------------------

func TestIfAbsentOverHTTP(t *testing.T) {
	srv := newServer(t)

	status, body := do(t, srv, "PUT", "/kv/k?ifAbsent=true", `{"a":1}`)
	if status != http.StatusOK {
		t.Fatalf("create-only on an absent key: status %d, body %s", status, body)
	}
	if got := decodeSuccess(t, body); got.Version != 1 {
		t.Errorf("version %d, want 1", got.Version)
	}

	status, body = do(t, srv, "PUT", "/kv/k?ifAbsent=true", `{"a":2}`)
	if status != http.StatusConflict {
		t.Fatalf("create-only on an existing key: status %d, want 409 (body %s)", status, body)
	}
	if got := decodeError(t, body); got.Version == nil || *got.Version != 1 {
		t.Errorf("409 body should report the current version 1, got %s", body)
	}
}

// --- Part 2: GET /kv --------------------------------------------------------

func TestListKeysNDJSON(t *testing.T) {
	srv := newServer(t)

	want := map[string]bool{}
	for i := 0; i < 250; i++ {
		key := fmt.Sprintf("key-%d", i)
		if status, body := do(t, srv, "PUT", "/kv/"+key, `1`); status != http.StatusOK {
			t.Fatalf("seed %s: status %d, body %s", key, status, body)
		}
		want[key] = true
	}

	resp, err := srv.Client().Get(srv.URL + "/kv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != ContentTypeNDJSON {
		t.Errorf("Content-Type %q, want %q", ct, ContentTypeNDJSON)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var entry keyLine
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("line %q is not valid JSON: %v", line, err)
		}
		if entry.Node != "node-test" {
			t.Errorf("line %q has node %q, want node-test", line, entry.Node)
		}
		seen[entry.Key]++
	}

	if len(seen) != len(want) {
		t.Errorf("listed %d keys, want %d", len(seen), len(want))
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("key %q listed %d times, want once", k, n)
		}
		if !want[k] {
			t.Errorf("unexpected key %q", k)
		}
	}
}

// K5: an empty store lists nothing and still returns 200.
func TestListKeysEmpty(t *testing.T) {
	srv := newServer(t)

	resp, err := srv.Client().Get(srv.URL + "/kv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d, want 200", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(raw)) != "" {
		t.Errorf("empty store listed %q", raw)
	}
}

// --- Part 2: misrouting -----------------------------------------------------

// K11: a key outside this node's range is refused with 421 rather than being
// stored where no read will ever look for it.
func TestMisdirectedRequest(t *testing.T) {
	// Own only the bottom half of the keyspace.
	srv := newServerWithRange(t, routing.Range{Lo: 0, Hi: testPartitions / 2})

	var owned, foreign string
	for i := 0; owned == "" || foreign == ""; i++ {
		key := fmt.Sprintf("key-%d", i)
		if routing.Partition(key, testPartitions) < testPartitions/2 {
			if owned == "" {
				owned = key
			}
		} else if foreign == "" {
			foreign = key
		}
	}

	if status, body := do(t, srv, "PUT", "/kv/"+owned, `{"a":1}`); status != http.StatusOK {
		t.Errorf("owned key %q: status %d, body %s", owned, status, body)
	}

	for _, method := range []string{"GET", "PUT", "PATCH"} {
		body := ""
		if method != "GET" {
			body = `{"a":1}`
		}
		status, out := do(t, srv, method, "/kv/"+foreign, body)
		if status != http.StatusMisdirectedRequest {
			t.Errorf("%s foreign key %q: status %d, want 421 (body %s)", method, foreign, status, out)
		}
	}
}

// A caller that hashed against a different partition count is rejected, because
// the disagreement would otherwise cause silent misrouting rather than an error.
func TestPartitionCountMismatch(t *testing.T) {
	srv := newServer(t)

	req, err := http.NewRequest("GET", srv.URL+"/kv/k", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(HeaderPartitionCount, "256") // node is configured for 1024

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMisdirectedRequest {
		t.Errorf("status %d, want 421", resp.StatusCode)
	}

	// The matching count is accepted.
	req, _ = http.NewRequest("GET", srv.URL+"/kv/k", nil)
	req.Header.Set(HeaderPartitionCount, fmt.Sprint(testPartitions))
	resp2, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("status %d, want 404 (the key simply does not exist)", resp2.StatusCode)
	}
}

func TestHealth(t *testing.T) {
	srv := newServer(t)
	resp, err := srv.Client().Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d, want 200", resp.StatusCode)
	}
}
