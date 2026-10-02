#!/usr/bin/env bash
# Demo: sends a JSON file to the contract topic the way a faulty branch would, through the WAN listener.
#   demo/send-raw.sh BR0001 contract/examples/invalid-business/total-mismatch.json
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep container paths as they are
key=$1
file=$2
# One record per line for the console producer: key|value, value with line breaks removed
printf '%s|%s\n' "$key" "$(tr -d '\r\n' < "$file")" | docker run --rm -i --network branch-sales-wan apache/kafka:4.3.1 \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka.hq.example:9094 \
  --topic branch-sales.daily-summary --reader-property parse.key=true --reader-property key.separator='|'
