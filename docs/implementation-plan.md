# Implementation Plan

> Design: [design-refined.md](design-refined.md) · Tests: [test-plan.md](test-plan.md)
>
> **Status: executed.** This was written before any code and is kept as-is, so the
> plan can be compared against what was built. Where the two differ, the code is
> right and the difference is listed below.

### What changed once it met reality

| Planned | Built | Why |
|---|---|---|
| `test/concurrency/` package | never created | Goroutine-level concurrency tests belong beside the store (`internal/store`), and HTTP-level ones in the conformance suite. A third location would have split them for no reason. |
| `ifVersion` only | added **`?ifAbsent=true`** | The required counter test returned 298: a compare-and-set loop cannot guard its first write, and `ifVersion` cannot express "must not exist". |
| Port 7000, per the assignment | **8081** | macOS Control Centre already listens on 7000, so the assignment's own examples fail on any Mac. |
| Throughput measured in-process | measured on **CPU-capped containers** | An in-process comparison cannot show scaling — simulated nodes share one machine's CPU, so the ratio is ~1.0× by construction. |
| *(not planned)* | `docs/openapi.yaml` | A machine-readable spec makes the API reviewable without reading Go. |
| *(not planned)* | JSON `405` responses | Go's `ServeMux` emits plain text, which would have broken the promise that every error is a JSON body. |

## Scope reality check

Part 3 is design-only and **already complete** — nothing to build. What follows covers Parts 1 and 2 only.

Two processes: **node** (Part 1 store + Part 2 local endpoints) and **proxy** (Part 2 routing and aggregation).

---

## Repo layout

```
.
├── cmd/
│   ├── node/main.go              flags, wiring, server
│   └── proxy/main.go             flags, config load, wiring
│
├── internal/
│   ├── store/                    THE core — partitioned map, locking, versions
│   │   ├── store.go              partitions, Get, Write, Keys
│   │   ├── entry.go              Entry, WriteOp, Mode, errors
│   │   └── store_test.go
│   │
│   ├── jsonmerge/                pure PATCH rules — no locks, no HTTP
│   │   ├── merge.go
│   │   └── merge_test.go
│   │
│   ├── routing/                  SHARED by node and proxy
│   │   ├── hash.go               FNV-1a → partition
│   │   ├── table.go              partition → node, loaded from config
│   │   └── routing_test.go
│   │
│   ├── node/
│   │   ├── router.go             routes → handlers
│   │   ├── handler.go            HTTP ⇄ store ops
│   │   ├── usecase.go            validation, op building, error mapping
│   │   └── *_test.go
│   │
│   ├── proxy/
│   │   ├── router.go
│   │   ├── handler.go
│   │   ├── usecase.go            route decision, fan-out orchestration
│   │   ├── nodecaller.go         HTTP client, pooling, verbatim forward
│   │   └── *_test.go
│   │
│   └── config/env.go             env parsing for both binaries
│
├── test/
│   ├── conformance/suite.go      parameterised by baseURL
│   ├── conformance/single_test.go
│   ├── conformance/cluster_test.go
│   ├── concurrency/              300-counter, create race, C9/C10
│   └── e2e/                      drives real binaries via compose
│
├── deploy/
│   ├── Dockerfile                multi-stage, ARG selects binary
│   ├── compose.part1.yml         1 node
│   └── compose.part2.yml         proxy + 3 nodes
│
├── Makefile
└── docs/
```

**Part 2 additions inside the node are marked with `// Part 2:` comments** so the walkthrough can show the delta at a glance — the entire Part 2 node diff is greppable.

---

## Layering — and why the store is not a repository

```
handler   (driving adapter)    HTTP ⇄ command
service   (application layer)  validation, limits, policy, error mapping, orchestration
store     (domain aggregate)   owns the invariant, executes commands atomically
```

Named **`service`** — the hexagonal "application service", Clean Architecture's "use case". Purely a naming choice; consistency is what matters.

**The store is a domain aggregate, not a repository.** In a conventional hexagonal layout the application layer owns the transaction boundary: open transaction → call repository → commit. That is impossible here, because the atomic boundary is a mutex *inside* the store and it must span read → guard → compute → write. The store therefore owns the invariant (per-key atomicity plus version monotonicity) and enforces it internally, and the service passes a **command** down rather than reaching inside.

The two services are deliberately asymmetric:

- **Node service is thin** — parse and range-check `ifVersion` (→400), enforce size limits (→413/414), validate the body is JSON, build the command, map store errors to application errors.
- **Proxy service is where orchestration lives** — select the target node, fan out `GET /kv`, merge NDJSON streams, handle partial failure.

Padding the node's service to match the proxy's would be architecture for its own sake.

## The one architectural rule

**The service layer must never read-then-write.**

The `ifVersion` guard, the merge, and the version bump are **one atomic operation inside the store**. The usecase builds a `WriteOp` and hands it over; the store executes it under lock.

```go
// WRONG — reintroduces TOCTOU, the 300-counter will fail
cur, _ := store.Get(key)
if cur.Version != ifVersion { return conflict }
merged := jsonmerge.Merge(cur.Value, body)
store.Set(key, merged)

// RIGHT — one call, store does guard + compute + bump under one lock
store.Write(WriteOp{Key: key, Body: body, Mode: ModeMerge, IfVersion: &v})
```

A clean-layers instinct pulls hard toward the first version. It is the single most likely way to lose the whole design.

---

## Core types

```go
// internal/store
type Mode uint8
const (
    ModeReplace Mode = iota  // PUT
    ModeMerge                // PATCH
)

type Entry struct {
    Value   json.RawMessage
    Version uint64
}

type WriteOp struct {
    Key       string
    Body      json.RawMessage
    Mode      Mode
    IfVersion *uint64          // nil = unconditional
}

type Store struct {
    parts []*partition
    mask  uint64
}

func New(partitionCount int) *Store
func (s *Store) Get(key string) (Entry, bool)
func (s *Store) Write(op WriteOp) (Entry, error)   // atomic
func (s *Store) Keys(fn func(key string) bool)     // Part 2: partition-at-a-time

// errors
var ErrNotFound = errors.New("not found")
type ErrVersionMismatch struct{ Current uint64 }   // carries version for the 409 body
```

```go
// internal/routing — shared by node and proxy
func Partition(key string, count int) uint64        // FNV-1a, never maphash

// Range ownership — ONE function, used by both sides, so they cannot drift
type Range struct{ Lo, Hi uint64 }                  // [Lo, Hi)
func RangeFor(index, nodeCount, partitionCount int) Range
func ParseRange(s string) (Range, error)            // "0-85" override
func (r Range) Contains(p uint64) bool              // Part 2: the 421 check

type Table map[uint64]string                        // partition → nodeID (proxy side)
func (t Table) NodeFor(key string, count int) string
```

```go
// internal/proxy
type NodeCaller interface {
    Forward(ctx context.Context, nodeID string, r *http.Request) (*http.Response, error)
    LocalKeys(ctx context.Context, nodeID string) (io.ReadCloser, error)
}
```

`Forward` copies status and body **verbatim** — no decode, no re-encode. Re-serialising drops `version` from the 409 body and silently breaks every client's retry loop.

---

## Build phases

Each phase ends at a **demoable state**. If time runs out, stopping after Phase 3 still yields a complete Part 1 + Part 2 demo.

| Phase | Deliverable | Gate | Est. |
|---|---|---|---|
| **0** | `go mod`, Makefile skeleton, `make test` runs | green on an empty suite | 15 min |
| **1** | `jsonmerge` + `store` | U1–U32 pass under `-race` | 1.5–2 h |
| **2** | node HTTP layer, `cmd/node` | H1–H24 pass; assignment's four `curl` examples work | 1.5 h |
| **3** | **300-counter + negative control** | C1, C2, C3, C9, C10 pass under `-race` | 1 h |
| **4** | `routing` + `cmd/proxy` + `GET /kv` | conformance suite green against proxy + 3 nodes | 1.5–2 h |
| **5** | cluster tests | K1–K17 | 1 h |
| **6** | Docker, compose, demo script | `make demo-part2` works from clean | 1 h |
| **7** | *optional* — benchmarks K15, partition-count bench | numbers for the slides | 45 min |

**Total: 8–9 h**, against a 3–5 h suggested timebox. That is expected — the timebox assumes no design phase, and the tests are a deliverable here.

**Order matters:** Phase 3 before Phase 4. The 300-counter is the assignment's one required test; get it green on a single node before adding a network.

---

## Docker

Single multi-stage `Dockerfile`, binary chosen by build arg:

```dockerfile
ARG CMD=node

FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG CMD
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/app /app
USER app
ENTRYPOINT ["/app"]
```

`CGO_ENABLED=0` gives a static binary. **Alpine over distroless** deliberately — a shell makes live debugging possible if something misbehaves during the demo, which is worth more than 5 MB. Mention distroless as the production choice.

### compose.part1.yml — single node

```yaml
services:
  node:
    build: {context: .., dockerfile: deploy/Dockerfile, args: {CMD: node}}
    ports: ["7000:7000"]
    environment:
      NODE_ID: node-1
      LISTEN_ADDR: ":7000"
      PARTITION_COUNT: "256"
      MAX_VALUE_BYTES: "1048576"
      MAX_KEY_BYTES: "1024"
```

### compose.part2.yml — proxy + 3 nodes

Three nodes on an internal network, proxy the only published port. Routing table passed as env (`PARTITION_MAP` or `NODES=node-1:7001,node-2:7002,node-3:7003` with an even split computed at startup — simpler, and enough for a static table).

```yaml
services:
  node-1: {..., environment: {NODE_ID: node-1, OWNED_PARTITIONS: "0-85"}}
  node-2: {..., environment: {NODE_ID: node-2, OWNED_PARTITIONS: "86-170"}}
  node-3: {..., environment: {NODE_ID: node-3, OWNED_PARTITIONS: "171-255"}}
  proxy:
    ports: ["7000:7000"]
    environment:
      NODES: "node-1=http://node-1:7001,node-2=http://node-2:7001,node-3=http://node-3:7001"
      PARTITION_COUNT: "256"
```

`OWNED_PARTITIONS` is what enables the node's **421** check. It is also what makes the misroute demo possible — hand a node the wrong range on purpose.

---

## Makefile

```make
build          # go build both binaries
test           # go test ./...
race           # go test -race ./...          ← the real gate
cover          # coverage report
lint           # go vet + gofmt -l
counter        # run the 300-counter, both with and without CAS

docker-build   # build both images
up-part1       # compose up part 1
up-part2       # compose up part 2
down           # compose down -v

demo-part1     # up-part1 + the assignment's four curl examples
demo-part2     # up-part2 + curls + GET /kv + counter + kill-node + 421
e2e            # run test/e2e against a live compose stack

clean          # binaries, coverage, containers
```

`make race` is the gate that matters — a concurrency test passing without `-race` proves very little.

---

## Config, both binaries

| Var | Default | Applies to |
|---|---|---|
| `NODE_ID` | `node-1` | node — stamped into `GET /kv` output |
| `LISTEN_ADDR` | `:7000` | both |
| `PARTITION_COUNT` | `1024` | both — **must match**; mismatch means silent misrouting |
| `NODE_INDEX` | `0` | node — its slot in the cluster |
| `NODE_COUNT` | `1` | node — cluster size, for `RangeFor` |
| `OWNED_PARTITIONS` | *(derived)* | node — **override**, e.g. `"0-85"`; wins over index/count |
| `NODES` | — | proxy — `id=url` pairs |
| `MAX_VALUE_BYTES` | `1048576` | node — ruling R6 → 413 |
| `MAX_KEY_BYTES` | `1024` | node — ruling R8 → 414 |
| `REQUEST_TIMEOUT` | `5s` | proxy |

### Partition ownership — `NODE_INDEX` + `NODE_COUNT`, with an override

```
NODE_INDEX=0  NODE_COUNT=3  PARTITION_COUNT=1024  →  routing.RangeFor(0,3,1024) = [0,342)
OWNED_PARTITIONS=0-341                            →  if set, used verbatim
```

### Why 1024, and why one number still serves both roles

An empty partition costs about **80 bytes** (map header 48 + `RWMutex` 24 + pointer 8), and `GET /kv` walks every partition, so the count is also a per-listing lock cost at roughly 25 ns uncontended.

| Partitions | 3 nodes | 10 nodes | 50 nodes | Memory | `GET /kv` walk |
|---|---|---|---|---|---|
| 256 | ~1% skew | ~4% | **~20%** | 20 KB | 6 µs |
| **1024** | ~1% | ~1% | ~5% | 80 KB | 25 µs |
| 4096 | ~1% | ~1% | ~1% | 330 KB | 100 µs |
| 16384 | ~1% | ~1% | ~1% | 1.3 MB | 400 µs |

1024 keeps skew under 5% out to roughly 100 nodes for 80 KB and a negligible walk. 256 only begins to hurt past ~25 nodes, but there is no reason to accept that when the fix is free.

**The two roles stay unified.** Splitting lock-striping from placement would cost a second config value *and* a second vocabulary word — "partition" was deliberately fixed to mean one thing, and introducing "stripe" versus "slot" is real explanatory overhead for a benefit that only materialises at a few thousand nodes. The decoupling argument already appears in Part 3 (1024 lock stripes versus ~16 replication groups), which is where it belongs.

Walkthrough line: *one number serves both roles because 1024 is simultaneously a good stripe count and a good placement granularity; they only need separating at Redis scale, and Part 3 shows exactly where.*

The node learns *"I am slot 0 of 3"* and nothing about where its peers live, so the design's **"the node is ignorant of the cluster"** claim survives in its meaningful form — while the shared `routing.RangeFor` guarantees the proxy derives an identical range. Drift would require misconfiguring `NODE_COUNT` itself.

| Rejected alternative | Why |
|---|---|
| Auto-split from a full `NODES` list on every node | Forces the node to know peer addresses — breaks the ignorance claim |
| Explicit range only, no derivation | Mapping defined in two places, free to drift |
| No ownership check at all | Loses the `421` misroute defence, and with it the silent-data-loss protection |

The override earns its place twice over: it is the **misroute demo lever** — set a deliberately wrong range and watch the node return `421`, no code change needed — and it leaves room for uneven placement later without reworking the config.

**`PARTITION_COUNT` mismatch between proxy and node is a silent-corruption bug**, not a startup error — the proxy would route by one function while the node validates by another. The proxy sends it as a header on every request and nodes reject a mismatch; both also log it loudly at startup.

---

## If time runs short — cut in this order

1. **Phase 7** benchmarks — nice, not necessary.
2. **K9–K11** (node-down, 421) — demo them by hand instead of in tests.
3. **Docker** — run binaries directly with a shell script; compose is convenience, not substance.
4. **C7, C8** (monotonicity, mixed ops) — the valuable concurrency tests are C1, C2, C3, C9.

**Never cut:** the store, the `-race` gate, C1, C2, or the conformance suite running against both topologies. Those four are the substance of the demo.

---

## Decisions taken

| | Decision | Choice |
|---|---|---|
| 1 | Partition ownership config | **`NODE_INDEX` + `NODE_COUNT`** through the shared `routing.RangeFor`, with an `OWNED_PARTITIONS` override for the misroute demo |
| 2 | HTTP router | **stdlib `net/http.ServeMux`** — Go 1.22+ supports method and wildcard patterns, so no dependency is needed and "zero external deps" stays true |
| 3 | `/routing` endpoint on the proxy | **Yes** — ~15 lines, dumps the live partition→node table, makes the demo far easier to narrate |
| 4 | Layer naming | **`service`** (hexagonal "application service"). The store is a **domain aggregate**, not a repository |

Nothing outstanding. Ready to build on approval.
