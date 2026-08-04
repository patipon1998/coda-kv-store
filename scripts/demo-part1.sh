#!/usr/bin/env bash
#
# Part 1 demo: a single node, the assignment's own curl examples, then the
# counter test with and without compare-and-set.
set -euo pipefail

HOST_PORT="${HOST_PORT:-8081}"
BASE="${BASE:-http://localhost:$HOST_PORT}"
export HOST_PORT
COMPOSE="docker compose -f deploy/compose.part1.yml"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() { printf '\033[36m$ %s\033[0m\n' "$*"; }

cleanup() {
  if [[ "${KEEP_UP:-0}" != "1" ]]; then
    bold "Tearing down"
    $COMPOSE down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

bold "Starting a single node"
$COMPOSE up -d --build --wait

bold "1. PUT a full object"
step "curl -X PUT $BASE/kv/user:42 -d '{\"name\":\"Ari\",\"points\":10}'"
curl -fsS -X PUT "$BASE/kv/user:42" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Ari","points":10}'
echo

bold "2. GET it back — note the bytes come back with key order intact"
step "curl $BASE/kv/user:42"
curl -fsS "$BASE/kv/user:42"
echo

bold "3. Conditional replace with a matching version"
step "curl -X PUT '$BASE/kv/user:42?ifVersion=1' -d '{\"name\":\"Ari\",\"points\":20}'"
curl -fsS -X PUT "$BASE/kv/user:42?ifVersion=1" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Ari","points":20}'
echo

bold "4. The same conditional write again — now stale, so 409"
step "curl -i -X PUT '$BASE/kv/user:42?ifVersion=1' -d '...'"
curl -sS -o /tmp/kv-409.json -w 'HTTP %{http_code}\n' \
  -X PUT "$BASE/kv/user:42?ifVersion=1" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Ari","points":99}'
echo "body: $(cat /tmp/kv-409.json)"
echo "      ^ the 409 carries the CURRENT version, so a client can retry without re-reading"

bold "5. PATCH merges top-level fields"
step "curl -X PATCH $BASE/kv/user:42 -d '{\"rank\":\"gold\"}'"
curl -fsS -X PATCH "$BASE/kv/user:42" \
  -H 'Content-Type: application/json' \
  -d '{"rank":"gold"}'
echo

bold "6. PATCH is SHALLOW — a nested object is replaced, not deep-merged"
curl -fsS -X PUT "$BASE/kv/doc" -H 'Content-Type: application/json' \
  -d '{"a":{"x":1},"keep":true}' >/dev/null
step "PATCH {\"a\":{\"y\":2}} onto {\"a\":{\"x\":1},\"keep\":true}"
curl -fsS -X PATCH "$BASE/kv/doc" -H 'Content-Type: application/json' -d '{"a":{"y":2}}'
echo "      ^ a.x is gone; only top-level fields merge"

bold "7. The required test: 3 clients x 100 increments"
echo "The same increment loop, run twice: once WITHOUT the ifVersion guard and"
echo "once WITH it. The server is identical in both runs — only the client differs."
echo "Without the negative control, a pass could not distinguish 'the guard works'"
echo "from 'the race never happened'."
echo
# Drive the LIVE stack, not an in-process one: the numbers on screen have to
# come from the same service the curls below read back.
KV_BASE_URL="$BASE" KV_COUNTER_KEY=demo:counter go test -count=1 -v -tags e2e \
  ./test/e2e/ -run TestE2ECounter 2>&1 \
  | grep -E 'without ifVersion|--- (PASS|FAIL)' || true

echo
step "curl $BASE/kv/demo:counter-without-ifversion"
curl -fsS "$BASE/kv/demo:counter-without-ifversion"; echo
step "curl $BASE/kv/demo:counter-with-ifversion"
curl -fsS "$BASE/kv/demo:counter-with-ifversion"; echo
echo
echo "      Compare the two. Both reached version 300, so all 300 writes"
echo "      SUCCEEDED in both runs — the difference is entirely in the value."
echo "      Without ifVersion the writes landed ON TOP of each other; with it"
echo "      they landed AFTER each other. That gap is the lost update."
echo
echo "      Note what this means: every individual write was already atomic."
echo "      The per-key lock guarantees that on both paths. The update is lost"
echo "      BETWEEN the GET and the PUT, and no server-side lock can close that"
echo "      gap — the server cannot know two requests were meant to be one"
echo "      operation, and it cannot hold a lock across client think-time."
echo
echo
echo "      Version 300 with value 300 also proves nothing was double-applied:"
echo "      a rejected write returns 409 WITHOUT bumping the version, so exactly"
echo "      300 successful writes occurred."

bold "Done"
echo "Re-run with KEEP_UP=1 to leave the node running."
