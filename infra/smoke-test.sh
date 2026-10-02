#!/usr/bin/env bash
# Checks the HQ infrastructure after `docker compose up -d`:
#   1. the dead-letter topic exists
#   2. a client on the `wan` network (a simulated branch) with valid SCRAM credentials can write, over TLS, to the topic
#      its ACL allows, through the external listener
#   3. an HQ client on the `hq` network can read that record through the internal listener
#   4. the same client is refused on a topic its ACL does not allow (one branch cannot write another branch's topic)
#   5. a wrong password is refused
#   6. a client without TLS/SASL cannot use the external listener
#   7. the simulated branch cannot resolve the HQ DB
#   8. the simulated branch cannot reach Kafka's PLAINTEXT listeners (only hq-edge:9094 is on wan)
# Uses a temporary user and temporary topics that do not match the branch topic pattern, so the consumer never reads
# them; all are removed at the end.
set -euo pipefail
# Git Bash on Windows rewrites arguments that start with "/" into Windows paths; container paths must stay as-is.
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")"

IMAGE=apache/kafka:4.3.1
BIN=/opt/kafka/bin
USER_NAME="smoke-test"
PASSWORD="smoke-$RANDOM-$RANDOM"
ALLOWED="infra.smoke-test"
DENIED="infra.smoke-test-denied"
MARKER="smoke-$(date +%s)"
CA_CERT="$(pwd -W 2>/dev/null || pwd)/tls/out/ca.crt"

[[ -f tls/out/ca.crt ]] || { echo "tls/out/ca.crt missing: run tls/generate-certs.sh"; exit 1; }

admin() { # admin <tool> <args...>: Kafka CLI inside the broker, PLAINTEXT host listener
  local tool=$1; shift
  docker exec hq-kafka "$BIN/$tool" --bootstrap-server localhost:9092 "$@"
}

hq() { # hq <command>: run on the hq network
  docker run --rm --network branch-sales-hq --entrypoint /bin/bash "$IMAGE" -c "$1"
}

branch() { # branch <password> <protocol> <command>: run on the wan network as a branch client
  docker run --rm -i --network branch-sales-wan -v "$CA_CERT:/certs/ca.crt:ro" --entrypoint /bin/bash "$IMAGE" -c "
cat > /tmp/client.properties <<P
security.protocol=$2
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$USER_NAME\" password=\"$1\";
ssl.truststore.type=PEM
ssl.truststore.location=/certs/ca.crt
max.block.ms=10000
request.timeout.ms=10000
delivery.timeout.ms=15000
P
$3"
}

produce() { # produce <topic>: one record from stdin, prints the client output
  echo "$BIN/kafka-console-producer.sh --bootstrap-server kafka.hq.example:9094 --topic $1 --command-config /tmp/client.properties 2>&1"
}

cleanup() {
  admin kafka-acls.sh --remove --force --allow-principal "User:$USER_NAME" --operation Write --operation Describe \
    --topic "$ALLOWED" > /dev/null 2>&1 || true
  admin kafka-configs.sh --alter --entity-type users --entity-name "$USER_NAME" --delete-config SCRAM-SHA-512 > /dev/null 2>&1 || true
  admin kafka-topics.sh --delete --if-exists --topic "$ALLOWED" > /dev/null 2>&1 || true
  admin kafka-topics.sh --delete --if-exists --topic "$DENIED" > /dev/null 2>&1 || true
}
trap cleanup EXIT

echo "1. dead-letter topic"
admin kafka-topics.sh --list | grep -qx branch-sales.daily-summary.dlt || { echo "   FAIL: topic branch-sales.daily-summary.dlt missing"; exit 1; }
echo "   ok"

for t in "$ALLOWED" "$DENIED"; do
  admin kafka-topics.sh --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 > /dev/null
done
admin kafka-configs.sh --alter --entity-type users --entity-name "$USER_NAME" \
  --add-config "SCRAM-SHA-512=[iterations=8192,password=$PASSWORD]" > /dev/null
admin kafka-acls.sh --add --allow-principal "User:$USER_NAME" --operation Write --operation Describe --topic "$ALLOWED" > /dev/null

echo "2. branch -> kafka.hq.example:9094 (wan, TLS + SCRAM), allowed topic"
out=$(echo "$MARKER" | branch "$PASSWORD" SASL_SSL "$(produce "$ALLOWED")")
grep -qiE 'exception|error' <<<"$out" && { echo "   FAIL: $out"; exit 1; }
echo "   ok"

echo "3. hq <- kafka:19092 (hq)"
hq "$BIN/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic $ALLOWED \
  --from-beginning --max-messages 1 --timeout-ms 15000 2>/dev/null" | grep -q "$MARKER" \
  || { echo "   FAIL: record not received"; exit 1; }
echo "   ok"

echo "4. branch -> topic without ACL is refused"
out=$(echo "$MARKER" | branch "$PASSWORD" SASL_SSL "$(produce "$DENIED")")
grep -q 'TopicAuthorizationException' <<<"$out" || { echo "   FAIL: write was not refused: $out"; exit 1; }
[[ "$(admin kafka-get-offsets.sh --topic "$DENIED" 2>/dev/null)" == "$DENIED:0:0" ]] \
  || { echo "   FAIL: a record reached $DENIED"; exit 1; }
echo "   ok"

echo "5. wrong password is refused"
out=$(branch "wrong-password" SASL_SSL "$BIN/kafka-topics.sh --bootstrap-server kafka.hq.example:9094 \
  --command-config /tmp/client.properties --list 2>&1" || true)
grep -q 'SaslAuthenticationException' <<<"$out" || { echo "   FAIL: login was not refused: $out"; exit 1; }
echo "   ok"

echo "6. client without TLS/SASL cannot use the external listener"
if branch "$PASSWORD" PLAINTEXT "timeout 20 $BIN/kafka-topics.sh --bootstrap-server kafka.hq.example:9094 \
  --command-config /tmp/client.properties --list > /dev/null 2>&1"; then
  echo "   FAIL: plaintext client got a topic list"; exit 1
fi
echo "   ok"

echo "7. branch cannot resolve hq-db"
if docker run --rm --network branch-sales-wan --entrypoint /bin/bash "$IMAGE" -c "getent hosts hq-db > /dev/null"; then
  echo "   FAIL: hq-db resolvable from wan"; exit 1
fi
echo "   ok"

echo "8. branch cannot reach Kafka's PLAINTEXT listeners"
docker run --rm --network branch-sales-wan --entrypoint /bin/bash "$IMAGE" -c '
  if getent hosts kafka > /dev/null; then echo "   FAIL: kafka resolvable from wan"; exit 1; fi
  for port in 19092 9092 9093; do
    if timeout 5 bash -c "echo > /dev/tcp/kafka.hq.example/$port" 2> /dev/null; then
      echo "   FAIL: kafka.hq.example:$port reachable from wan"; exit 1
    fi
  done'
echo "   ok"

echo "smoke test passed"
