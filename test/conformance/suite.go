// Package conformance holds one HTTP semantics suite, run against several
// topologies. Requiring identical results from each is what proves the proxy is
// transparent and that Part 2 preserved Part 1's semantics — for almost no extra
// code.
//
// Everything here talks real HTTP over a real socket. What separates this
// package from test/e2e is who starts the server:
//
//	test/conformance   starts servers in-process with httptest.
//	                   Self-contained: `go test ./...` needs no setup, runs under
//	                   -race, and has no ports to allocate or processes to reap.
//
//	test/e2e           runs the SAME suite against an already-running deployment
//	                   via KV_BASE_URL. Build-tagged so it stays out of the
//	                   default run. It covers what in-process testing cannot:
//	                   environment parsing, container wiring, and DNS between
//	                   services.
//
// The suite itself is deployment-agnostic — it only ever sees a base URL — which
// is what lets one set of assertions serve a bare node, an in-process cluster,
// and a live Docker stack.
package conformance

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Client is a thin wrapper over the KV HTTP API. Tests and the demo script use
// the same compare-and-set loop, so neither can quietly diverge from the other.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Response is a decoded reply, successful or not.
type Response struct {
	Status  int
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
	Version uint64          `json:"version"`
	Error   string          `json:"error"`
	Raw     []byte          `json:"-"`
}

// VersionPtr reports the version and whether the response carried one.
func (r Response) VersionPtr() (uint64, bool) {
	var probe struct {
		Version *uint64 `json:"version"`
	}
	if err := json.Unmarshal(r.Raw, &probe); err != nil || probe.Version == nil {
		return 0, false
	}
	return *probe.Version, true
}

func (c *Client) do(method, target, body string) (Response, error) {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, c.BaseURL+target, reader)
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return Response{}, err
	}

	out := Response{Status: resp.StatusCode, Raw: raw}
	_ = json.Unmarshal(raw, &out) // error bodies decode partially, which is fine
	return out, nil
}

func (c *Client) Get(key string) (Response, error) {
	return c.do(http.MethodGet, "/kv/"+escape(key), "")
}

func (c *Client) Put(key, body string) (Response, error) {
	return c.do(http.MethodPut, "/kv/"+escape(key), body)
}

func (c *Client) Patch(key, body string) (Response, error) {
	return c.do(http.MethodPatch, "/kv/"+escape(key), body)
}

func (c *Client) PutIfVersion(key, body string, version uint64) (Response, error) {
	return c.do(http.MethodPut, fmt.Sprintf("/kv/%s?ifVersion=%d", escape(key), version), body)
}

func (c *Client) PutIfAbsent(key, body string) (Response, error) {
	return c.do(http.MethodPut, "/kv/"+escape(key)+"?ifAbsent=true", body)
}

// ListKeys reads GET /kv and returns the parsed NDJSON lines plus any error line
// the server appended mid-stream.
func (c *Client) ListKeys() (keys map[string]string, streamErr string, err error) {
	resp, err := c.HTTP.Get(c.BaseURL + "/kv")
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("GET /kv: status %d", resp.StatusCode)
	}

	keys = map[string]string{}
	dec := json.NewDecoder(resp.Body)
	for {
		var line struct {
			Key   string `json:"key"`
			Node  string `json:"node"`
			Error string `json:"error"`
		}
		if err := dec.Decode(&line); err == io.EOF {
			break
		} else if err != nil {
			return nil, "", fmt.Errorf("bad NDJSON line: %w", err)
		}
		if line.Error != "" {
			streamErr = line.Error
			continue
		}
		keys[line.Key] = line.Node
	}
	return keys, streamErr, nil
}

// escape percent-encodes a key so slashes, spaces and question marks stay part
// of the key rather than becoming path or query syntax.
func escape(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		ch := key[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9',
			ch == '-', ch == '_', ch == '.', ch == '~', ch == ':':
			b.WriteByte(ch)
		default:
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

// IncrementWithIfVersion performs one increment guarded by ifVersion, retrying
// while another client keeps winning.
//
// Two details make this correct. The retry only ever fires on a definitively
// received 409 — never on a timeout, which is ambiguous and could double-apply a
// write that actually succeeded. And the very first write uses ifAbsent rather
// than writing unconditionally, because with no version to assert two clients
// would otherwise both create the key and lose one increment.
func (c *Client) IncrementWithIfVersion(key string) error {
	const maxAttempts = 500

	for attempt := 0; attempt < maxAttempts; attempt++ {
		current, err := c.Get(key)
		if err != nil {
			return err
		}

		var resp Response
		switch current.Status {
		case http.StatusNotFound:
			resp, err = c.PutIfAbsent(key, "1")
		case http.StatusOK:
			var n int
			if err := json.Unmarshal(current.Value, &n); err != nil {
				return fmt.Errorf("counter %q is not a number: %s", key, current.Value)
			}
			resp, err = c.PutIfVersion(key, fmt.Sprint(n+1), current.Version)
		default:
			return fmt.Errorf("GET %q: unexpected status %d", key, current.Status)
		}
		if err != nil {
			return err
		}

		switch resp.Status {
		case http.StatusOK:
			return nil
		case http.StatusConflict:
			// Someone else won. Back off a little so heavy contention does not
			// turn into a tight retry storm.
			time.Sleep(time.Duration(rand.Intn(200)) * time.Microsecond)
		default:
			return fmt.Errorf("write %q: unexpected status %d (%s)", key, resp.Status, resp.Raw)
		}
	}
	return fmt.Errorf("increment %q: gave up after %d attempts", key, maxAttempts)
}

// IncrementWithoutIfVersion is the negative control: read, add one, write with
// no guard at all. Concurrent callers lose updates, which is exactly the point —
// the server still applies every write atomically, but it has no way to know two
// requests were meant to be one operation.
func (c *Client) IncrementWithoutIfVersion(key string) error {
	current, err := c.Get(key)
	if err != nil {
		return err
	}
	n := 0
	if current.Status == http.StatusOK {
		if err := json.Unmarshal(current.Value, &n); err != nil {
			return err
		}
	}
	_, err = c.Put(key, fmt.Sprint(n+1))
	return err
}

// Run executes the shared semantics suite against baseURL.
func Run(t *testing.T, baseURL string) {
	t.Helper()
	c := NewClient(baseURL)

	t.Run("assignment examples", func(t *testing.T) { testAssignmentExamples(t, c) })
	t.Run("errors", func(t *testing.T) { testErrors(t, c) })
	t.Run("conflict carries version", func(t *testing.T) { testConflictCarriesVersion(t, c) })
	t.Run("patch rules", func(t *testing.T) { testPatchRules(t, c) })
	t.Run("key encoding", func(t *testing.T) { testKeyEncoding(t, c) })
	t.Run("value fidelity", func(t *testing.T) { testValueFidelity(t, c) })
	t.Run("ifAbsent", func(t *testing.T) { testIfAbsent(t, c) })
	t.Run("list keys", func(t *testing.T) { testListKeys(t, c) })
}

func testAssignmentExamples(t *testing.T, c *Client) {
	m := must(t)
	key := unique("user")

	resp := m(c.Put(key, `{"name":"Ari","points":10}`))
	requireStatus(t, resp, http.StatusOK)
	if resp.Version != 1 {
		t.Errorf("first PUT gave version %d, want 1", resp.Version)
	}

	resp = m(c.Get(key))
	requireStatus(t, resp, http.StatusOK)
	if string(resp.Value) != `{"name":"Ari","points":10}` {
		t.Errorf("GET returned %s", resp.Value)
	}

	resp = m(c.PutIfVersion(key, `{"name":"Ari","points":20}`, 1))
	requireStatus(t, resp, http.StatusOK)
	if resp.Version != 2 {
		t.Errorf("conditional PUT gave version %d, want 2", resp.Version)
	}

	resp = m(c.Patch(key, `{"rank":"gold"}`))
	requireStatus(t, resp, http.StatusOK)
	if resp.Version != 3 {
		t.Errorf("PATCH gave version %d, want 3", resp.Version)
	}

	var merged map[string]any
	if err := json.Unmarshal(resp.Value, &merged); err != nil {
		t.Fatalf("merged value is not an object: %v (%s)", err, resp.Value)
	}
	for k, want := range map[string]any{"name": "Ari", "points": float64(20), "rank": "gold"} {
		if merged[k] != want {
			t.Errorf("merged[%q] = %v, want %v", k, merged[k], want)
		}
	}
}

func testErrors(t *testing.T, c *Client) {
	t.Run("H6 missing key", func(t *testing.T) {
		m := must(t)
		requireStatus(t, m(c.Get(unique("absent"))), http.StatusNotFound)
	})

	t.Run("H7 malformed JSON", func(t *testing.T) {
		m := must(t)
		requireStatus(t, m(c.Put(unique("k"), `{bad`)), http.StatusBadRequest)
	})

	t.Run("H8 empty body", func(t *testing.T) {
		m := must(t)
		requireStatus(t, m(c.Put(unique("k"), "")), http.StatusBadRequest)
	})

	t.Run("H10 ifVersion on absent key", func(t *testing.T) {
		m := must(t)
		requireStatus(t, m(c.PutIfVersion(unique("absent"), `{"a":1}`, 1)), http.StatusConflict)
	})

	t.Run("H11 ifVersion=0", func(t *testing.T) {
		m := must(t)
		key := unique("k")
		m(c.Put(key, `{"a":1}`))
		requireStatus(t, m(c.PutIfVersion(key, `{"a":2}`, 0)), http.StatusConflict)
	})

	for _, bad := range []string{"abc", "-1", "99999999999999999999999", ""} {
		t.Run("H12-H14 ifVersion="+bad, func(t *testing.T) {
			m := must(t)
			key := unique("k")
			m(c.Put(key, `{"a":1}`))
			resp, err := c.do(http.MethodPut, "/kv/"+escape(key)+"?ifVersion="+bad, `{"a":2}`)
			if err != nil {
				t.Fatal(err)
			}
			requireStatus(t, resp, http.StatusBadRequest)
		})
	}

	t.Run("H15 duplicate ifVersion", func(t *testing.T) {
		m := must(t)
		key := unique("k")
		m(c.Put(key, `{"a":1}`))
		resp, err := c.do(http.MethodPut, "/kv/"+escape(key)+"?ifVersion=1&ifVersion=2", `{"a":2}`)
		if err != nil {
			t.Fatal(err)
		}
		requireStatus(t, resp, http.StatusBadRequest)
	})

	t.Run("H16 DELETE not allowed", func(t *testing.T) {
		m := must(t)
		key := unique("k")
		m(c.Put(key, `{"a":1}`))
		resp, err := c.do(http.MethodDelete, "/kv/"+escape(key), "")
		if err != nil {
			t.Fatal(err)
		}
		requireStatus(t, resp, http.StatusMethodNotAllowed)
	})

	t.Run("H15a PATCH ifVersion on absent key", func(t *testing.T) {
		resp, err := c.do(http.MethodPatch, "/kv/"+escape(unique("absent"))+"?ifVersion=1", `{"a":1}`)
		if err != nil {
			t.Fatal(err)
		}
		requireStatus(t, resp, http.StatusConflict)
	})
}

// H9: the 409 body must carry the current version. Placed in the shared suite
// deliberately — it is what catches a proxy that re-encodes responses instead of
// forwarding bytes, which would silently break every client's retry loop.
func testConflictCarriesVersion(t *testing.T, c *Client) {
	for _, method := range []string{"PUT", "PATCH"} {
		t.Run(method, func(t *testing.T) {
			m := must(t)
			key := unique("conflict")
			m(c.Put(key, `{"a":1}`))
			m(c.Put(key, `{"a":2}`))
			m(c.Put(key, `{"a":3}`)) // version 3

			target := fmt.Sprintf("/kv/%s?ifVersion=1", escape(key))
			resp, err := c.do(method, target, `{"a":9}`)
			if err != nil {
				t.Fatal(err)
			}
			requireStatus(t, resp, http.StatusConflict)

			version, ok := resp.VersionPtr()
			if !ok {
				t.Fatalf("409 body carries no version: %s", resp.Raw)
			}
			if version != 3 {
				t.Errorf("409 reports version %d, want 3", version)
			}
		})
	}
}

func testPatchRules(t *testing.T, c *Client) {
	cases := []struct {
		id      string
		initial string // "" means the key starts absent
		delta   string
		want    string
	}{
		{"U10 create", "", `{"a":1}`, `{"a":1}`},
		{"U11 create non-object", "", `[1,2]`, `[1,2]`},
		{"U12 merge", `{"a":1}`, `{"b":2}`, `{"a":1,"b":2}`},
		{"U13 delta wins", `{"a":1}`, `{"a":9}`, `{"a":9}`},
		{"U14 nested replaced", `{"a":{"x":1}}`, `{"a":{"y":2}}`, `{"a":{"y":2}}`},
		{"U15 array current", `[1,2]`, `{"a":1}`, `{"a":1}`},
		{"U16 array delta", `{"a":1}`, `[1,2]`, `[1,2]`},
		{"U17 null delta", `{"a":1}`, `null`, `null`},
		{"U19 empty delta", `{"a":1}`, `{}`, `{"a":1}`},
		{"U20 null field set", `{"a":1,"b":2}`, `{"a":null}`, `{"a":null,"b":2}`},
	}

	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			m := must(t)
			key := unique("patch")
			if tc.initial != "" {
				m(c.Put(key, tc.initial))
			}
			resp := m(c.Patch(key, tc.delta))
			requireStatus(t, resp, http.StatusOK)

			if !sameJSON(t, resp.Value, []byte(tc.want)) {
				t.Errorf("value %s, want %s", resp.Value, tc.want)
			}
		})
	}
}

func testKeyEncoding(t *testing.T, c *Client) {
	for _, key := range []string{"user:42", "a/b", "key with space", "k?x=1", "ไทย🔑", "a&b=c"} {
		t.Run(key, func(t *testing.T) {
			m := must(t)
			full := unique("enc") + key
			resp := m(c.Put(full, `{"a":1}`))
			requireStatus(t, resp, http.StatusOK)
			if resp.Key != full {
				t.Errorf("stored key %q, want %q", resp.Key, full)
			}

			resp = m(c.Get(full))
			requireStatus(t, resp, http.StatusOK)
			if resp.Key != full {
				t.Errorf("read back key %q, want %q", resp.Key, full)
			}
		})
	}
}

// Key order survives the round trip, which is what distinguishes raw byte
// storage from decoding into a map and re-encoding. Whitespace does not:
// encoding/json compacts a RawMessage on the way out.
func testValueFidelity(t *testing.T, c *Client) {
	for _, value := range []string{
		`{"zebra":1,"apple":2,"mango":3}`,
		`null`, `true`, `42`, `-1.5e10`, `"str"`, `[]`, `{}`,
		`{"emoji":"🔑","th":"ไทย"}`,
	} {
		t.Run(value, func(t *testing.T) {
			m := must(t)
			key := unique("fidelity")
			m(c.Put(key, value))
			resp := m(c.Get(key))
			if string(resp.Value) != value {
				t.Errorf("sent %s, got back %s", value, resp.Value)
			}
		})
	}
}

func testIfAbsent(t *testing.T, c *Client) {
	m := must(t)
	key := unique("absent")

	resp := m(c.PutIfAbsent(key, `{"a":1}`))
	requireStatus(t, resp, http.StatusOK)
	if resp.Version != 1 {
		t.Errorf("version %d, want 1", resp.Version)
	}

	resp = m(c.PutIfAbsent(key, `{"a":2}`))
	requireStatus(t, resp, http.StatusConflict)
	if version, ok := resp.VersionPtr(); !ok || version != 1 {
		t.Errorf("409 should report current version 1, got %s", resp.Raw)
	}
}

func testListKeys(t *testing.T, c *Client) {
	m := must(t)
	prefix := unique("list")
	want := map[string]bool{}
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		m(c.Put(key, `1`))
		want[key] = true
	}

	keys, streamErr, err := c.ListKeys()
	if err != nil {
		t.Fatal(err)
	}
	if streamErr != "" {
		t.Fatalf("listing reported an error line: %s", streamErr)
	}

	for key := range want {
		node, ok := keys[key]
		if !ok {
			t.Errorf("key %q missing from the listing", key)
			continue
		}
		if node == "" {
			t.Errorf("key %q has no node attribution", key)
		}
	}
}

// CounterKeys reports the keys RunCounter will use.
//
// They are normally unique per run so repeated invocations cannot collide. The
// demo script sets KV_COUNTER_KEY to pin them, so it can read the final state
// back afterwards and show the value alongside the version.
func CounterKeys() (withIfVersion, withoutIfVersion string) {
	if fixed := os.Getenv("KV_COUNTER_KEY"); fixed != "" {
		return fixed + "-with-ifversion", fixed + "-without-ifversion"
	}
	return unique("counter-with-ifversion"), unique("counter-without-ifversion")
}

// RunCounter is the assignment's required test: three concurrent clients, a
// hundred increments each, exactly 300.
//
// It also runs the negative control. Without that, a passing result cannot
// distinguish "CAS works" from "the race never happened".
func RunCounter(t *testing.T, baseURL string) {
	t.Helper()

	const (
		clients   = 3
		increment = 100
		total     = clients * increment
	)

	withIfVersion, withoutIfVersion := CounterKeys()

	t.Run("C1 with ifVersion reaches exactly 300", func(t *testing.T) {
		m := must(t)
		c := NewClient(baseURL)
		key := withIfVersion

		var wg sync.WaitGroup
		errs := make(chan error, clients)

		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := NewClient(baseURL) // its own connection pool
				for n := 0; n < increment; n++ {
					if err := client.IncrementWithIfVersion(key); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("increment failed: %v", err)
		}

		resp := m(c.Get(key))
		requireStatus(t, resp, http.StatusOK)

		var final int
		if err := json.Unmarshal(resp.Value, &final); err != nil {
			t.Fatal(err)
		}
		if final != total {
			t.Errorf("counter is %d, want %d — an update was lost", final, total)
		}
		// Failed attempts return 409 without bumping the version, so exactly
		// `total` successful writes must have happened. A higher version would
		// mean writes were double-applied.
		if resp.Version != total {
			t.Errorf("version is %d, want %d — writes were wasted or double-applied",
				resp.Version, total)
		}
	})

	t.Run("C2 without ifVersion loses updates", func(t *testing.T) {
		m := must(t)
		c := NewClient(baseURL)
		key := withoutIfVersion

		var wg sync.WaitGroup
		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := NewClient(baseURL)
				for n := 0; n < increment; n++ {
					_ = client.IncrementWithoutIfVersion(key)
				}
			}()
		}
		wg.Wait()

		resp := m(c.Get(key))
		var final int
		if err := json.Unmarshal(resp.Value, &final); err != nil {
			t.Fatal(err)
		}
		if final >= total {
			t.Errorf("got %d without ifVersion, expected fewer than %d — no updates were "+
				"lost, so the positive test proves nothing", final, total)
		}
		// The version is the interesting half: all `total` writes succeeded, so
		// the version reaches `total` either way. Only the value differs, which
		// is precisely what a lost update looks like — writes that landed on top
		// of each other rather than after each other.
		t.Logf("without ifVersion: value %d of %d, at version %d — every write succeeded, "+
			"but %d increments were clobbered", final, total, resp.Version, total-final)
	})
}

// --- helpers ----------------------------------------------------------------

var uniqueCounter struct {
	sync.Mutex
	n int
}

// unique returns a fresh key prefix so subtests never collide, which matters
// because the suite runs twice against different topologies.
func unique(prefix string) string {
	uniqueCounter.Lock()
	defer uniqueCounter.Unlock()
	uniqueCounter.n++
	return fmt.Sprintf("%s-%d-", prefix, uniqueCounter.n)
}

// must returns a checker bound to t. Written as a closure factory because Go
// cannot mix an extra argument with a multi-value call: m(c.Put(...)) is
// not valid, whereas m(c.Put(...)) is.
func must(t *testing.T) func(Response, error) Response {
	return func(resp Response, err error) Response {
		t.Helper()
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		return resp
	}
}

func requireStatus(t *testing.T, resp Response, want int) {
	t.Helper()
	if resp.Status != want {
		t.Fatalf("status %d, want %d (body %s)", resp.Status, want, resp.Raw)
	}
}

func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("not valid JSON: %v (%s)", err, a)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("not valid JSON: %v (%s)", err, b)
	}
	return fmt.Sprint(x) == fmt.Sprint(y)
}
