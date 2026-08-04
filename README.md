# codakv — a versioned key/value service

A small key/value store in three parts: a single in-memory node, a horizontally
sharded cluster, and a design-only roadmap for replication.

Go 1.24, standard library only. **No external dependencies.**

```bash
make race          # the gate that matters
make demo-part1    # single node: the assignment's examples, plus the counter test
make demo-part2    # cluster: routing, listing, node failure, misrouting
```

> The service listens on **8081**. Override with `HOST_PORT=...` if that is taken
> on your machine. (Port 7000, which the assignment's examples use, is occupied by
> Control Center's AirPlay Receiver on macOS — hence 8081.)

---

## The idea in one page

A **version per key** is a logical clock, and everything else follows from taking
that seriously.

| | |
|---|---|
| **Part 1** | `version` is a per-key logical clock; `ifVersion` is compare-and-set against it. One mutex is the serialization point. |
| **Part 2** | Shard by key, so each key lives on exactly **one** node. Per-key atomicity stays local, and sharding needs **no new concurrency code**. |
| **Part 3** | Replicate, and that single serialization point disappears. Compare-and-set now needs consensus rather than a mutex — and the API already says which operations need it. |

**Part 2 was easy precisely because it partitioned instead of replicating.**
Everything that makes Part 3 hard is the thing Part 2 deliberately avoided.

---

## API

| Method | Path | Behaviour |
|---|---|---|
| `GET` | `/kv/{key}` | `200 {key, value, version}` · `404` if absent |
| `PUT` | `/kv/{key}` | Replace the whole value, bump the version |
| `PATCH` | `/kv/{key}` | Shallow-merge top-level fields |
| `GET` | `/kv` | List every key as NDJSON — `{"key":…, "node":…}` |
| `GET` | `/routing` | *(proxy)* the live partition→node table |
| `GET` | `/health` | liveness plus configuration |

Full machine-readable spec: **[docs/openapi.yaml](docs/openapi.yaml)** (OpenAPI 3.1).

**Guards** (query parameters, valid on `PUT` and `PATCH` alike):

- `?ifVersion=N` — apply only if the key is at version `N`, else `409`. The 409
  body carries the **current** version so a client can retry without re-reading.
- `?ifAbsent=true` — apply only if the key does not exist, else `409`.

`ifAbsent` is **not in the assignment**. It was added because a compare-and-set
loop cannot protect its *first* write: with no version to assert, two clients
both write unconditionally and one update is lost. The required counter test
found this by landing on 298.

It is purely additive — omit the parameter and behaviour is identical to the
specification. Seeding the counter before the concurrent phase would also have
made the test pass, but that hides the gap rather than closing it. Every
optimistic-concurrency store has this primitive for the same reason (DynamoDB
`attribute_not_exists`, Cassandra `IF NOT EXISTS`, Redis `SETNX`).

A full list of everything here that is not in the assignment — added endpoints,
extra status codes, and the deliberate interpretations — is in
[docs/notes.md](docs/notes.md#everything-that-is-not-in-the-assignment--the-no-surprises-list).

**PATCH is shallow.** A nested object in the delta *replaces* its counterpart
rather than merging into it, and a `null` field is *set to null* rather than
deleted. The second point differs from RFC 7386 JSON Merge Patch, deliberately —
the assignment says "shallow-merge top-level fields", which is not Merge Patch,
and deleting would be inventing semantics.

---

## Design

```
handler   (driving adapter)    HTTP ⇄ command
service   (application)        validation, limits, policy, error mapping
store     (domain aggregate)   owns the invariant, applies commands atomically
```

**The store is an aggregate, not a repository.** In a conventional layering the
application layer owns the transaction boundary, but that is impossible here:
the atomic boundary is a mutex spanning read → guard → compute → write. So the
service passes a *command* down and never reaches inside. A `Get()` → check →
`Set()` shape would look like clean layering and would silently reintroduce the
lost update `ifVersion` exists to prevent.

### Three locks, three different problems

| Layer | Protects | Why it cannot be skipped |
|---|---|---|
| Partition `RWMutex` | the Go map's *structure* | Concurrent map read/write is not merely undefined in Go — the runtime detects it and throws an unrecoverable fatal error, killing the process. The hazard involves *unrelated* keys: inserting one key can grow the map and relocate the buckets another reader is walking. |
| Entry `RWMutex` | one key's *contents* | Without it a reader can observe a new value beside an old version, send a stale `ifVersion`, pass the guard, and lose an update silently. |
| `ifVersion` | *semantics across requests* | No lock can help: `GET` and `PUT` are separate round trips, so there is no critical section to hold. |

The partition lock is released *before* the entry lock is taken, which is sound
only because entries are never removed. There is no `DELETE`, so an `*entry`
pointer stays valid forever. Adding `DELETE` would break this and need a
tombstone checked under the entry lock.

### Partitioning

`key → FNV-1a → partition (1024) → node`

- **FNV-1a, never `hash/maphash`** — maphash seeds itself randomly *per process*,
  which is fine for in-process striping and fatal across nodes.
- **1024 partitions**, fixed for the cluster's life, serving as both the lock
  stripe and the unit of placement. It costs ~80 KB in empty partitions and keeps
  placement skew under 5% out to roughly a hundred nodes.
- **Partition count is independent of node count.** `hash % nodeCount` would move
  ~75% of keys when growing 3→4 nodes; a partition table moves exactly 25%, the
  theoretical floor. Whole partitions move; no key is ever rehashed.

Each node derives its own range from `NODE_INDEX` and `NODE_COUNT` through the
same `routing.RangeFor` the proxy uses to build its table — so the two cannot
drift, and **a node never learns where its peers are.**

### The proxy

Stateless. It holds a routing table and nothing else: no keys, no locks, no
versions. It **forwards bytes** and never re-encodes — doing so would drop the
version from a 409 body and quietly degrade every client's retry loop.

It also **never retries a write.** A timeout is ambiguous: the write may have
been applied with only the response lost, and retrying a read-modify-write would
double-apply it. Retries belong to the client, and only on a definitively
received 409.

---

## Testing

```bash
make race     # everything, under the race detector
make counter  # the required test, with and without CAS
make e2e      # the same suite against a live stack (needs make up-part2)
```

**One conformance suite, three topologies.** The HTTP semantics tests are written
once against a `baseURL` and run against a bare node, an in-process cluster, and
a live Docker deployment. Identical results are required, which is what proves
the proxy is transparent and that Part 2 preserved Part 1's semantics.

**The counter test has a negative control.** Alongside "3 clients × 100
increments = exactly 300", the same test runs with CAS disabled and asserts the
result is *below* 300. Without it, a pass could not distinguish "CAS works" from
"the race never happened".

```
without CAS: 113 of 300 increments survived
with CAS:    300, at version 300
```

The version assertion matters as much as the value: failed attempts return 409
without bumping the version, so exactly 300 successful writes must have occurred.
A value of 300 at version 340 would mean writes were double-applied.

**`-race` is the gate, not the assertions.** It caught a real bug on the first
run: the create path inserted an entry, released the partition lock, and *then*
read the entry to build its return value — by which point another goroutine could
already be writing through it. Every functional test passed regardless.

### Measured, not asserted

Both nodes are pinned to 0.5 CPU in compose, so a three-node stack genuinely has
three times the compute:

| Stack | Throughput |
|---|---|
| 1 node, 0.5 CPU | ~8,800 writes/sec |
| 3 nodes, 1.5 CPU | ~14,200 writes/sec (**1.6×**) |

Not 3×. The gap is the proxy hop — every request is client→proxy→node — plus
Docker's network stack, neither of which the single-node case pays. A real gain,
with an explainable shortfall.

Capacity scaling is cleaner: with one node the busiest node holds 100% of keys;
with three it holds 34%.

---

## Layout

```
cmd/node          store process — the whole Part 1 API
cmd/proxy         stateless router — Part 2

internal/store        partitioned map, per-key locking, versions
internal/jsonmerge    PATCH rules, pure
internal/routing      hashing and partition ranges — SHARED by both binaries
internal/node         service + HTTP handler
internal/proxy        routing, fan-out, verbatim forwarding
internal/config       environment parsing

test/conformance      the shared suite, plus scaling measurements
test/e2e              the same suite against a live stack (build tag: e2e)
deploy/               Dockerfile and compose stacks
docs/                 design, test plan, and background reading
```

Everything Part 2 added to the node is marked `// Part 2:` — the whole delta is
greppable, and it is three things: a node id, an owned partition range, and
`GET /kv`.

---

## Configuration

| Variable | Default | Applies to |
|---|---|---|
| `NODE_ID` | `node-1` | node — stamped into `GET /kv` |
| `LISTEN_ADDR` | `:8081` | both |
| `PARTITION_COUNT` | `1024` | both — **must match**; a mismatch is silent misrouting, so the proxy sends it as a header and nodes reject a disagreement with `421` |
| `NODE_INDEX` / `NODE_COUNT` | `0` / `1` | node — its slot, and the cluster size |
| `OWNED_PARTITIONS` | *derived* | node — override, e.g. `0-341`. Also the misrouting demo lever |
| `NODES` | — | proxy — ordered `id=url` pairs |
| `MAX_VALUE_BYTES` | `1048576` | node — exceeded gives `413` |
| `MAX_KEY_BYTES` | `1024` | node — exceeded gives `414` |
| `REQUEST_TIMEOUT` | `5s` | proxy |

---

## Documentation

| Document | Contents |
|---|---|
| [docs/openapi.yaml](docs/openapi.yaml) | OpenAPI 3.1 spec — every path, status code and rule |
| [docs/design-refined.md](docs/design-refined.md) | The settled design, all three parts, with diagrams |
| [docs/test-plan.md](docs/test-plan.md) | ~100 numbered cases, and the ten rulings the spec leaves open |

---

## Known limitations

- **No `DELETE`.** Not in the spec, and it would need a tombstone: the design
  releases the partition lock before taking the entry lock, which is only safe
  because entries are immortal.
- **No idempotency keys.** A response lost after a successful write is
  indistinguishable from a failure, so a client's retry can double-apply a
  read-modify-write. `ifVersion` guards against *concurrent writers*, not against
  duplicate delivery of your own request.
- **Adding a node needs a restart.** The routing table is config-driven.
  Freeze-and-flip online migration is designed in `docs/part3-options.md` and
  deliberately not built.
- **No persistence, auth, TLS or rate limiting.** All out of scope; the proxy is
  the natural home for the last three.
