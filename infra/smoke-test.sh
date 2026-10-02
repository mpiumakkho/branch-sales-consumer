#!/usr/bin/env bash
# Checks the HQ infrastructure after `docker compose up -d`:
#   1. both contract topics exist
#   2. a client on the `wan` network (a simulated branch) can produce through the external listener
#   3. an HQ client on the `hq` network can read that record through the internal listener
#   4. the simulated branch cannot resolve the HQ DB
# Uses a temporary topic, so no test data is left in the contract topics.
set -euo pipefail
# Git Bash on Windows rewrites arguments that start with "/" into Windows paths; container paths must stay as-is.
export MSYS_NO_PATHCONV=1

IMAGE=apache/kafka:4.3.1
BIN=/opt/kafka/bin
TEMP_TOPIC="infra.smoke-test"
MARKER="smoke-$(date +%s)"

on() { # on <network> <command>
  docker run --rm --network "$1" --entrypoint /bin/bash "$IMAGE" -c "$2"
}

cleanup() {
  on branch-sales-hq "$BIN/kafka-topics.sh --bootstrap-server kafka:19092 --delete --if-exists --topic $TEMP_TOPIC" > /dev/null 2>&1 || true
}
trap cleanup EXIT

echo "1. contract topics"
topics=$(on branch-sales-hq "$BIN/kafka-topics.sh --bootstrap-server kafka:19092 --list")
for t in branch-sales.daily-summary branch-sales.daily-summary.dlt; do
  grep -qx "$t" <<<"$topics" || { echo "   FAIL: topic $t missing"; exit 1; }
done
echo "   ok"

on branch-sales-hq "$BIN/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists \
  --topic $TEMP_TOPIC --partitions 1 --replication-factor 1" > /dev/null

echo "2. branch -> kafka.hq.example:9094 (wan)"
on branch-sales-wan "echo '$MARKER' | $BIN/kafka-console-producer.sh \
  --bootstrap-server kafka.hq.example:9094 --topic $TEMP_TOPIC > /dev/null"
echo "   ok"

echo "3. hq <- kafka:19092 (hq)"
on branch-sales-hq "$BIN/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic $TEMP_TOPIC \
  --from-beginning --max-messages 1 --timeout-ms 15000 2>/dev/null" | grep -q "$MARKER" \
  || { echo "   FAIL: record not received"; exit 1; }
echo "   ok"

echo "4. branch cannot resolve hq-db"
if on branch-sales-wan "getent hosts hq-db > /dev/null"; then
  echo "   FAIL: hq-db resolvable from wan"; exit 1
fi
echo "   ok"

echo "smoke test passed"
