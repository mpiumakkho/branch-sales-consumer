#!/usr/bin/env bash
# Demo: sends a JSON file the way a faulty branch would: logged in as that branch (TLS + SCRAM), on that branch's
# topic, with the branch code as key, through the WAN listener.
#   BRANCH_KAFKA_PASSWORD=... demo/send-raw.sh BR0001 contract/examples/invalid-business/total-mismatch.json
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep container paths as they are
branch=${1:?usage: BRANCH_KAFKA_PASSWORD=... $0 <branchCode> <file>}
file=${2:?usage: BRANCH_KAFKA_PASSWORD=... $0 <branchCode> <file>}
password=${BRANCH_KAFKA_PASSWORD:?set BRANCH_KAFKA_PASSWORD to the branch SCRAM password}
ca_cert="$(cd "$(dirname "$0")/../infra/tls/out" && (pwd -W 2>/dev/null || pwd))/ca.crt"

# One record per line for the console producer: key|value, value with line breaks removed
printf '%s|%s\n' "$branch" "$(tr -d '\r\n' < "$file")" | docker run --rm -i --network branch-sales-wan \
  -v "$ca_cert:/certs/ca.crt:ro" --entrypoint /bin/bash apache/kafka:4.3.1 -c "
cat > /tmp/client.properties <<P
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$branch\" password=\"$password\";
ssl.truststore.type=PEM
ssl.truststore.location=/certs/ca.crt
P
/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka.hq.example:9094 \
  --topic branch-sales.daily-summary.$branch --command-config /tmp/client.properties \
  --reader-property parse.key=true --reader-property key.separator='|'"
