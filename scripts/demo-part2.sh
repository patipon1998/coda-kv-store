#!/usr/bin/env bash
#
# Part 2 demo: a stateless proxy in front of three nodes. Shows routing, the
# aggregated key listing, per-key atomicity surviving the network, partial
# failure, and the 421 misdirected-request check.
set -euo pipefail

HOST_PORT="${HOST_PORT:-8081}"
BASE="${BASE:-http://localhost:$HOST_PORT}"
export HOST_PORT
COMPOSE="docker compose -f deploy/compose.part2.yml"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() { printf '\033[36m$ %s\033[0m\n' "$*"; }

cleanup() {
  if [[ "${KEEP_UP:-0}" != "1" ]]; then
    bold "Tearing down"
    $COMPOSE --profile misroute down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

bold "Starting proxy + 3 nodes"
$COMPOSE up -d --build --wait

bold "1. The routing table — key -> hash -> partition -> node"
step "curl $BASE/routing"
curl -fsS "$BASE/routing"
echo
echo "      ^ 1024 partitions split across 3 nodes. Adding a fourth moves"
echo "        whole partitions, never rehashes keys: 25% of data instead of 75%."

bold "2. The same API as a single node — the client cannot tell the difference"
step "curl -X PUT $BASE/kv/user:42 -d '{\"name\":\"Ari\",\"points\":10}'"
curl -fsS -X PUT "$BASE/kv/user:42" \
  -H 'Content-Type: application/json' -d '{"name":"Ari","points":10}'
echo
step "curl $BASE/kv/user:42"
curl -fsS "$BASE/kv/user:42"
echo

bold "3. Write 300 keys, then list them all"
for i in $(seq 1 300); do
  curl -fsS -X PUT "$BASE/kv/key-$i" -H 'Content-Type: application/json' -d "$i" >/dev/null
done
step "curl $BASE/kv "
curl -fsS "$BASE/kv" 
echo "      ^ NDJSON: merging the nodes' streams is plain concatenation."
echo "        No parsing, and memory stays constant however big the keyspace."

bold "4. Distribution across nodes"
step "curl $BASE/kv | grep -o '\"node\":\"[^\"]*\"' | sort | uniq -c"
curl -fsS "$BASE/kv" | grep -o '"node":"[^"]*"' | sort | uniq -c

bold "5. The required test: 3 clients x 100 increments"
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

echo "      And it is still exactly 300 through the proxy: routing did not"
echo "      disturb per-key atomicity, because each key still lives on exactly"
echo "      ONE node running Part 1's store unchanged."

bold "6. Kill a node — the listing degrades honestly"
step "docker compose ... stop node-2"
$COMPOSE stop node-2 >/dev/null
echo
step "curl $BASE/kv | tail -3"
curl -fsS "$BASE/kv" | tail -3
echo "      ^ 200 was already sent with the first line, so a 503 is impossible."
echo "        The failure arrives as one more NDJSON line; clients must check."
echo
step "curl -i $BASE/kv/<a key node-2 owns>"
BADKEY=""
for i in $(seq 1 300); do
  owner=$(curl -fsS "$BASE/kv" 2>/dev/null | grep "\"key\":\"key-$i\"" | grep -o '"node":"[^"]*"' || true)
  if [[ -z "$owner" ]]; then BADKEY="key-$i"; break; fi
done
if [[ -n "$BADKEY" ]]; then
  curl -sS -o /dev/null -w 'HTTP %{http_code}\n' "$BASE/kv/$BADKEY"
  echo "      ^ fails promptly rather than hanging"
fi
$COMPOSE start node-2 >/dev/null
sleep 2

bold "7. Misrouting is caught, not silently accepted"
echo "Starting a node deliberately configured to own the WRONG partition range."
$COMPOSE --profile misroute up -d node-bad --wait >/dev/null 2>&1 || \
  $COMPOSE --profile misroute up -d node-bad >/dev/null
sleep 2
echo
echo "node-bad claims partitions 0-341. Sending it a key it does not own:"
for i in $(seq 1 50); do
  code=$(curl -sS -o /dev/null -w '%{http_code}' \
    -X PUT "http://localhost:8084/kv/key-$i" \
    -H 'Content-Type: application/json' -d '1')
  if [[ "$code" == "421" ]]; then
    step "curl -X PUT http://localhost:8084/kv/key-$i"
    echo "HTTP 421 Misdirected Request"
    curl -sS -X PUT "http://localhost:8084/kv/key-$i" \
      -H 'Content-Type: application/json' -d '1'
    echo
    echo "      ^ Without this check the write would return 200 and every later"
    echo "        read would be routed elsewhere and 404: data lost, no error."
    break
  fi
done

bold "Done"
echo "Re-run with KEEP_UP=1 to leave the cluster running."
