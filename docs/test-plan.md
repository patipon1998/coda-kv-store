# Test Plan

> Design in [design-refined.md](design-refined.md). This document is the executable spec: every case with its concrete input and exact expected output.

## Two structural decisions

**One conformance suite, run against three topologies.** All HTTP-semantics tests are written once against a `baseURL` and executed against a bare node, a proxy in front of three nodes, and a live Docker deployment. Identical results required. This proves the proxy is transparent *and* that Part 2 preserves Part 1 semantics, at almost no extra cost. It is also the strongest single claim available in the walkthrough: *same suite, three topologies, identical results.*

**Negative control on the concurrency test.** The 300-counter is also run with CAS **disabled**, asserting the result is `< 300`. Without it, a passing test cannot distinguish "CAS works" from "the race never occurred." With it, the assignment's central concept is demonstrated rather than asserted.

**In-process `httptest` by default; a live stack on demand.** Every test here talks real HTTP over a real socket. What separates the two packages is *who starts the server*:

| | Server started by | Runs under |
|---|---|---|
| `test/conformance` | the test itself, via `httptest` | `go test ./...` — no setup, `-race`, no ports to allocate or processes to reap |
| `test/e2e` | you, via `make up-part2` | `make e2e` — build-tagged, reads `KV_BASE_URL` |

The suite is deployment-agnostic (it only ever sees a base URL), which is what lets one set of assertions serve all three topologies. The e2e run covers what in-process testing cannot: environment parsing, container wiring, and DNS between services.

## Coverage — every stated requirement mapped to a case

Taken directly from the assignment text, so coverage is checkable rather than assumed.

### Part 1

| Requirement (assignment wording) | Cases |
|---|---|
| `GET /kv/{key}` → `200 {key,value,version}` or `404` | H1, H2, H6 |
| `PUT` — replace entire value, bump version | H1, H3, U6–U9 |
| `PATCH` — body is a delta | H4, U10–U20 |
| `ifVersion` guard → `409`, **on both PUT and PATCH** | U21–U26b, H9–H15c |
| PATCH rule 1 — key absent → create with delta | U10, U11, H15b |
| PATCH rule 2 — both objects → **shallow** merge | U12, U13, **U14**, U19, U20 |
| PATCH rule 3 — otherwise replace | U15–U18 |
| "A version per key" | U1–U5 |
| "No version history stored" | implicit — no history endpoint exists |
| **"Atomicity by key: concurrent ops on the same key serialized"** | C1, C3, C6, **C10** |
| **"…different keys may proceed concurrently"** | **C9** ← the only test that verifies this |
| "No torn reads/writes" | C6, C7, `-race` on every concurrency test |
| "No lost updates under parallel clients" | C1 + **C2** (negative control) |
| **Required: 3 clients × 100 increments = 300** | **C1** |

### Part 2

| Requirement (assignment wording) | Cases |
|---|---|
| "Total memory for keys/values increases horizontally" | **K14** |
| "…**and** throughput increase horizontally" | **K15** (measured on CPU-capped containers), K15b |
| "Preserve Part 1 semantics" | **K17** — the entire conformance suite, re-run against the cluster |
| Per-key atomicity preserved across nodes | K8 (300-counter through the proxy) |
| `GET /kv` → NDJSON, one line per key, `{key, node}` | H5, K4, K5, K6 |
| "How keys/requests are distributed" | K1, K2, K3 |
| "How the client/router decides where to send a request" | K1, K2, K11 |
| "How list-keys aggregates from all nodes" | K4, K10 |
| Proxy adds no state or SPOF-of-data | **K16** |

### Part 3

Design only — nothing to execute. The diagrams and option analysis in [design-refined.md](design-refined.md) and [part3-options.md](part3-options.md) are the deliverable.

## Layout

```
internal/store/store_test.go          unit — pure store logic, no HTTP
internal/jsonmerge/merge_test.go      PATCH rules, pure
internal/routing/routing_test.go      hash determinism, partition→node table
internal/config/config_test.go        environment parsing; node/proxy agreement
internal/node/handler_test.go         node HTTP layer via httptest
test/conformance/suite.go             HTTP semantics, parameterised by baseURL
test/conformance/single_test.go         └─ run against 1 bare node
test/conformance/cluster_test.go        └─ run against proxy + 3 nodes
test/conformance/scale_test.go        capacity and proxy-overhead measurements
test/e2e/                             the same suite against a LIVE stack (tag: e2e)
scripts/demo-part1.sh                 single node — the assignment's examples
scripts/demo-part2.sh                 cluster — routing, failure, misrouting
```

`conformance.Client.Increment` implements the client CAS retry loop, and both the tests and the demo scripts drive it, so neither can quietly diverge from the other. Retry is bounded (500 attempts) so a bug fails rather than hangs, with jitter to avoid a retry storm. It retries **only** on a definitively received 409 — never on a timeout, which is ambiguous and could double-apply a write that actually succeeded.

The demo scripts pin the counter key with `KV_COUNTER_KEY` and drive the **live** stack via `KV_BASE_URL`, so the numbers printed on screen come from the same service the following `curl`s read back.

---

## Layer 1 — Store unit tests

No HTTP. Fast, deterministic, `-race`.

### Versioning

| # | Scenario | Input | Expected |
|---|---|---|---|
| U1 | First write | `PUT k = {"a":1}` | `version = 1` |
| U2 | Second write | then `PUT k = {"a":2}` | `version = 2` |
| U3 | Tenth write | 10 sequential writes | `version = 10` |
| U4 | Read absent | `GET missing` | not found |
| U5 | Version never reused | write, write, read | `version = 2`, no gaps |

### PUT — full replace

| # | Current | Input | Expected value | Version |
|---|---|---|---|---|
| U6 | `{"a":1,"b":2}` | `PUT {"c":3}` | `{"c":3}` — old fields **gone** | +1 |
| U7 | *(absent)* | `PUT {"a":1}` | `{"a":1}` | 1 |
| U8 | `{"a":1}` | `PUT null` | `null` | +1 |
| U9 | `{"a":1}` | `PUT []` | `[]` | +1 |

### PATCH — the three rules

| # | Current | Delta | Expected | Rule |
|---|---|---|---|---|
| U10 | *(absent)* | `{"a":1}` | `{"a":1}`, version 1 | 1 — create |
| U11 | *(absent)* | `[1,2]` | `[1,2]`, version 1 | 1 — create, delta verbatim even if not an object |
| U12 | `{"a":1}` | `{"b":2}` | `{"a":1,"b":2}` | 2 — merge |
| U13 | `{"a":1}` | `{"a":9}` | `{"a":9}` — delta wins | 2 — merge |
| U14 | `{"a":{"x":1}}` | `{"a":{"y":2}}` | `{"a":{"y":2}}` — **NOT** `{"a":{"x":1,"y":2}}` | 2 — **shallow only** |
| U15 | `[1,2]` | `{"a":1}` | `{"a":1}` | 3 — current not an object → replace |
| U16 | `{"a":1}` | `[1,2]` | `[1,2]` | 3 — delta not an object → replace |
| U17 | `{"a":1}` | `null` | `null` | 3 — `null` is not an object → replace |
| U18 | `"str"` | `{"a":1}` | `{"a":1}` | 3 — replace |
| U19 | `{"a":1}` | `{}` | `{"a":1}` unchanged, **version still +1** | 2 — see ruling R2 |
| U20 | `{"a":1,"b":2}` | `{"a":null}` | `{"a":null,"b":2}` — **set to null, not deleted** | 2 — see ruling R1 |

**U14 and U20 are the two most-failed cases.** Both get explicit tests.

### `ifVersion` guard — **every case runs twice, once as PUT and once as PATCH**

The spec states the guard *"applies to both PUT and PATCH"*, so this whole table is table-driven over `{PUT, PATCH}`. Testing it only against PUT is the most likely way to ship a broken PATCH guard.

| # | Current version | `ifVersion` | Expected (both methods) |
|---|---|---|---|
| U21 | 5 | 5 | proceeds, version → 6 |
| U22 | 5 | 4 | rejected, current version reported as 5 |
| U23 | 5 | 6 | rejected |
| U24 | *(absent)* | 1 | rejected — cannot assert a version on an absent key |
| U25 | 5 | 0 | rejected — 0 is never a valid version |
| U26 | 5 | *(omitted)* | proceeds unconditionally |
| U26b | *(absent)* | *(omitted)* | creates at version 1 — PATCH-create must still work with no guard |

### Value fidelity

| # | Input value | Expected on read |
|---|---|---|
| U27 | `{"b":1,"a":2}` | **byte-identical**, key order preserved — proves raw-bytes storage; a parsed-map implementation would reorder |
| U28 | `{"a":  1}` (extra whitespace) | byte-identical |
| U29 | `{"emoji":"🔑","th":"ไทย"}` | round-trips |
| U30 | `true` / `42` / `-1.5e10` / `"str"` / `[]` / `{}` | all stored and returned unchanged |
| U31 | deeply nested (10 levels) | round-trips |
| U32 | `{"a":1,"a":2}` (duplicate key) | accepted, last wins — Go `encoding/json` behaviour, documented not enforced |

---

## Layer 2 — HTTP conformance suite

Written once, run against a bare node **and** proxy + 3 nodes. Results must be identical.

### Success paths

| # | Request | Body | Status | Response body |
|---|---|---|---|---|
| H1 | `PUT /kv/user:42` | `{"name":"Ari","points":10}` | `200` | `{"key":"user:42","value":{"name":"Ari","points":10},"version":1}` |
| H2 | `GET /kv/user:42` | — | `200` | same as above |
| H3 | `PUT /kv/user:42?ifVersion=1` | `{"name":"Ari","points":20}` | `200` | `version: 2` |
| H4 | `PATCH /kv/user:42` | `{"rank":"gold"}` | `200` | `{"name":"Ari","points":20,"rank":"gold"}`, `version: 3` |
| H5 | `GET /kv` | — | `200` | NDJSON, `Content-Type: application/x-ndjson` |

H1–H4 are the assignment's own `curl` examples, in order. They run as a test *and* as the demo script.

### Error paths

| # | Request | Status | Notes |
|---|---|---|---|
| H6 | `GET /kv/nonexistent` | `404` | |
| H7 | `PUT /kv/k` body `{bad` | `400` | malformed JSON |
| H8 | `PUT /kv/k` body *(empty)* | `400` | empty is not valid JSON |
| H9 | `PUT /kv/k?ifVersion=99` on version 1 | `409` | **body must contain `"version":1`** |
| H10 | `PUT /kv/absent?ifVersion=1` | `409` | ruling R3 |
| H11 | `PUT /kv/k?ifVersion=0` | `409` | ruling R4 |
| H12 | `PUT /kv/k?ifVersion=abc` | `400` | not a number |
| H13 | `PUT /kv/k?ifVersion=-1` | `400` | negative |
| H14 | `PUT /kv/k?ifVersion=99999999999999999999999` | `400` | overflows `uint64` |
| H15 | `PUT /kv/k?ifVersion=1&ifVersion=2` | `400` | duplicate parameter — ruling R7 |
| H16 | `DELETE /kv/k` | `405` | method not allowed; not in scope |
| H17 | `PUT /kv/` (empty key) | `404` | ruling R5 |
| H18 | `PUT /kv/k` with 20 MB body | `413` | ruling R6 |

**H7–H15 run against `PATCH` as well as `PUT`.** The suite is table-driven over both methods, because the spec applies the `ifVersion` guard and body validation to both. Additional PATCH-only rows:

| # | Request | Status | Notes |
|---|---|---|---|
| H15a | `PATCH /kv/absent?ifVersion=1` | `409` | PATCH-create must respect the guard, not silently create |
| H15b | `PATCH /kv/absent` (no guard) | `200` | creates at version 1, delta stored verbatim |
| H15c | `PATCH /kv/k` body `{bad` | `400` | malformed delta |

**H9 lives in the shared suite deliberately** — it is the assertion that catches a proxy re-encoding responses instead of forwarding bytes. A proxy that drops `version` from the 409 body silently degrades every client's retry loop.

### Key handling

| # | Key | Encoded as | Expected |
|---|---|---|---|
| H19 | `user:42` | `/kv/user:42` | works — colons are legal in a path segment |
| H20 | `a/b` | `/kv/a%2Fb` | works, stored as `a/b` — **not** as two path segments |
| H21 | `key with space` | `/kv/key%20with%20space` | works |
| H22 | `k?x=1` | `/kv/k%3Fx%3D1` | works; the `?` is part of the key, not a query |
| H23 | `ไทย🔑` | percent-encoded UTF-8 | works |
| H24 | 10 KB key | — | `414` or `400` — ruling R8 |

H20 and H22 are the encoding cases most likely to be wrong in a hand-rolled router.

---

## Layer 3 — Concurrency

All under `go test -race`. **The race detector is the proof; a passing assertion alone is not** — it only means the timing did not line up that run.

| # | Test | Setup | Assertion |
|---|---|---|---|
| C1 | **300-counter** *(required)* | 3 clients × 100 CAS increments on one key | value `== 300` **and** final version `== 300` |
| C2 | **Negative control** | same, `ifVersion` omitted | value `< 300` — proves the harness detects lost updates |
| C3 | Create race | 50 goroutines `PUT` the same new key simultaneously | all `200`, exactly one create, final version `== 50` |
| C4 | Independent keys | 100 goroutines, 100 distinct keys, 100 writes each | every key at version 100, no cross-interference |
| C5 | Concurrent merge | 2 clients CAS-`PATCH` *different* fields, 100× each | both fields present with correct counts — proves the merge itself is atomic, not just the write |
| C6 | Concurrent same field | 2 clients `PATCH` the *same* field | last writer wins, value always valid JSON, never torn |
| C7 | Reader monotonicity | continuous readers during sustained writes | every response is valid JSON; version **never decreases** for a given reader |
| C8 | Mixed operations | random `GET`/`PUT`/`PATCH` across random keys, 10s | no panic, no race, final version per key `==` count of successful writes |
| C9 | **Different keys proceed *concurrently*** | hold a slow write on key A (injected delay inside its critical section); issue a write to key B | B completes well within A's delay — proves keys do not serialise against each other |
| C10 | Same key **does** serialise | two concurrent writes to key A, each recording entry/exit timestamps | critical sections never overlap |

**C9 is the only test that actually verifies a stated Part 1 requirement** — *"different keys may proceed concurrently."* C4 proves independent keys are *correct*, which a single global mutex would also satisfy. C9 fails under a global lock and passes under per-entry locking, so it is the test that justifies the locking design. C10 is its complement: it proves the guarantee we *do* make.

**C1's version assertion matters as much as the value.** Failed CAS attempts return `409` and do not bump the version, so exactly 300 successful writes must have occurred. A value of 300 with a version of 340 would mean writes were double-applied.

**C7 is cheap and catches a wide class of publication bugs** — a torn read or a misordered store surfaces as a version regression.

---

## Layer 4 — Cluster behaviour

Beyond the conformance suite, which already covers semantics.

| # | Test | Assertion |
|---|---|---|
| K1 | Hash determinism | same key → same partition across processes and restarts; guards the `maphash` trap |
| K2 | Partition→node stability | with a fixed table, a key always routes to the same node |
| K3 | Distribution | 10 000 keys → each of 3 nodes within ±10% of even |
| K4 | `GET /kv` completeness | every written key appears **exactly once**, attributed to the node that holds it |
| K5 | `GET /kv` on empty cluster | `200`, empty body, no error line |
| K6 | `GET /kv` line format | each line parses as JSON with exactly `key` and `node` |
| K7 | Transparency | proxy + 1 node is indistinguishable from a bare node |
| K8 | 300-counter through proxy | still exactly 300 — routing preserves per-key atomicity |
| K9 | Node down, single key | request for that node's partition fails **promptly**, does not hang |
| K10 | Node down, `GET /kv` | surviving nodes' keys stream, then `{"error":"node-3 unreachable"}` as the final line |
| K11 | Misroute → `421` | point the proxy at a deliberately wrong node; the node rejects rather than silently accepting |
| K12 | Large value through proxy | 1 MB round-trips unmodified |
| K13 | 409 through proxy | body still contains `version` — the re-encode trap, asserted end-to-end |
| K14 | **Capacity scales with nodes** | write 30 000 keys to a 3-node cluster; assert each node holds ≈10 000 — i.e. no node holds the whole set. This *is* the "total memory scales horizontally" claim |
| K15 | **Throughput scales with nodes** | measured against a live stack with each node pinned to 0.5 CPU, so three nodes genuinely have three times the compute: **~8,800 → ~14,200 writes/sec (1.6×)**. Not 3× — the gap is the proxy hop and Docker networking, which the single-node case does not pay. Run with `make up-part1 && make throughput`, then `make up-part2 && make throughput` |
| K15b | Proxy adds no measurable cost | the in-process comparison (`TestProxyOverheadIsNegligible`) cannot show scaling — simulated nodes share one machine's CPU, so the ratio is ~1.0× by construction. What it does prove is that routing and the extra hop are near-free; a proxy that serialised requests would show a sharp drop |
| K16 | **Proxy is stateless** | restart the proxy mid-run; in-flight requests fail, subsequent ones succeed with no data loss and no warm-up | 
| K17 | Part 1 semantics survive sharding | full conformance suite green against proxy + 3 nodes (same suite, same expectations) |

**K15 is the test that evidences the assignment's actual Part 2 requirement** — *"throughput increases horizontally."* Everything else in this table tests routing correctness; K15 tests the claim. The measured result was **1.6× on three times the CPU** — worth stating with the shortfall explained (proxy hop plus Docker networking) rather than rounded up. A number with a known gap is more credible than a claim.

**K16 is the answer to "isn't the proxy a SPOF?"** demonstrated rather than argued: it holds no data, so restarting it costs only in-flight requests.

---

## Rulings — ambiguities the tests forced into the open

Writing the cases surfaced decisions the spec does not make. Each is decided, tested, and worth mentioning in the walkthrough as *"here is where I had to choose."*

| # | Ambiguity | Ruling | Rationale |
|---|---|---|---|
| **R1** | `PATCH {"a":null}` — set field to null, or delete it? | **Set to null** (U20) | RFC 7386 JSON Merge Patch treats `null` as delete, but the assignment says "shallow-merge top-level fields", not Merge Patch. Deleting would be inventing semantics. Flag it as a real fork — a reviewer may expect RFC 7386. |
| **R2** | `PATCH {}` — does an empty delta bump the version? | **Yes** (U19) | A write was requested and succeeded. Version counts writes, not value changes. Also keeps `ifVersion` chains predictable. |
| **R3** | `ifVersion` on an absent key | **409** (U24, H10) | Consistent with "0 is not a valid version". DynamoDB's equivalent is `attribute_not_exists`; a distinct `?ifAbsent=true` would be cleaner than overloading this. |
| **R4** | `ifVersion=0` | **409** (U25, H11) | Versions start at 1, so 0 can never match. |
| **R5** | Empty key `PUT /kv/` | **404** | No key means no resource. |
| **R6** | Value size limit | **1 MB, else `413`** (H18) | Unbounded values in an in-memory store are a trivial OOM vector. |
| **R7** | Duplicate `ifVersion` parameters | **400** (H15) | Ambiguous intent; silently taking the first is how real bugs hide. |
| **R8** | Key length limit | **1 KB, else `414`** (H24) | Bounded memory per entry; also a URL-length reality. |
| **R9** | Duplicate JSON keys in a body | **Accept, last wins** (U32) | Go's `encoding/json` behaviour. Documented rather than enforced — rejecting would need a custom decoder for no benefit. |
| **R10** | Unsupported methods | **405** (H16) | Explicit, not a 404 — a 404 would suggest the key is missing. |

---

## Deliberately not tested

| Item | Why |
|---|---|
| DELETE semantics | Not in the spec. The prepared answer (tombstone + re-validation under the partition lock) is in [notes.md](notes.md). |
| Persistence / restart recovery | In-memory by assignment. |
| Part 3 replication | Design only — nothing to run. |
| Online resharding | Explicitly deferred; the table is config-driven and requires a restart. |
| Auth, TLS, rate limiting | Out of scope; the proxy is the natural place for all three. |

---

## Optional, if the build lands early

**Partition-count benchmark.** `go test -bench` measuring throughput at partition counts 1 vs 256. Roughly 20 lines, since the count is already a config knob — and it converts a design argument into a measured number ("striping gave N× on the create path"). High value per line: empirical justification of a design decision is rare in a take-home.

*(Note: K15 — throughput across 1 vs 3 **nodes** — is **not** optional. It is the only evidence for the assignment's core Part 2 requirement. This benchmark is the finer-grained sibling that justifies the Part 1 locking design.)*

---

## Demo script

`scripts/demo-part1.sh` (single node) and `scripts/demo-part2.sh` (proxy plus three nodes) launch the real containers and walk the story in order:

1. The assignment's own four `curl` examples (H1–H4).
2. `GET /kv` showing keys spread across all three nodes.
3. **The counter without `ifVersion`** → value well under 300, at version 300.
4. **The counter with `ifVersion`** → value exactly 300, at version 300.
5. Kill a node → `GET /kv` shows the error line; a request for its partition fails cleanly.
6. Misconfigure the proxy → node returns `421`.

Steps 3 and 4 back to back are the demo's centrepiece. Both runs reach **version 300**, so every write succeeded in both — the difference is entirely in the value. Without the guard the writes landed on top of each other; with it they landed after each other. The scripts drive the **live** stack via `KV_BASE_URL` and pin the key with `KV_COUNTER_KEY`, so the numbers printed come from the same service the following `curl`s read back.
