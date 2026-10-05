#!/usr/bin/env bash
# Checks the HQ infrastructure after `docker compose up -d`:
#   1. the HQ DB accepts connections
#   2. a client on the `wan` network (a branch, or anyone on the internet) cannot resolve the HQ DB
#   3. no HQ container except the consumer is on `wan`: HQ has no inbound port
# The branch side has its own smoke test (producer repo, smoke-test.sh).
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep container paths as they are
cd "$(dirname "$0")"

echo "1. HQ DB"
docker exec hq-db pg_isready -U hq_app -d hq_sales -q || { echo "   FAIL: hq-db not ready"; exit 1; }
echo "   ok"

echo "2. wan cannot resolve hq-db"
if docker run --rm --network branch-sales-wan alpine:3.22 getent hosts hq-db > /dev/null; then
  echo "   FAIL: hq-db resolvable from wan"; exit 1
fi
echo "   ok"

echo "3. HQ containers on wan"
on_wan=$(docker network inspect branch-sales-wan -f '{{range .Containers}}{{.Name}} {{end}}' | tr ' ' '\n' | grep '^hq-' || true)
if [[ -n "$on_wan" && "$on_wan" != "hq-consumer" ]]; then
  echo "   FAIL: on wan: $on_wan"; exit 1
fi
echo "   ok (${on_wan:-none})"

echo "smoke test passed"
