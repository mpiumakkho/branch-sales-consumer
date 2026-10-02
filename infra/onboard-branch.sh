#!/usr/bin/env bash
# Onboards a branch at HQ. Run once per branch, by the HQ admin:
#   BRANCH_KAFKA_PASSWORD=... infra/onboard-branch.sh BR0001 "Branch 1"
#
#   1. SCRAM-SHA-512 user <branchCode> on Kafka (the branch producer logs in with it)
#   2. topic branch-sales.daily-summary.<branchCode> (1 partition, 14 days retention)
#   3. ACL: that user may write (and describe) only this topic. A branch cannot write another branch's topic,
#      so the consumer can trust that the topic suffix is the sender (requirements Q5)
#   4. quota producer_byte_rate for that user: a branch that keeps sending is slowed down, not the others
#   5. the branch in the HQ branch registry (otherwise its messages are rejected as UNKNOWN_BRANCH)
#
# Safe to run again: it updates the password and quota and skips what exists.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep container paths as they are

code=${1:?usage: BRANCH_KAFKA_PASSWORD=... $0 <branchCode> <name>}
name=${2:?usage: BRANCH_KAFKA_PASSWORD=... $0 <branchCode> <name>}
password=${BRANCH_KAFKA_PASSWORD:?set BRANCH_KAFKA_PASSWORD (the branch puts the same value in its producer config)}
# A branch sends a few KB per hour; 10 KB/s is far above normal use and limits a faulty producer
byte_rate=${BRANCH_PRODUCER_BYTE_RATE:-10240}

[[ $code =~ ^[A-Z0-9]{3,10}$ ]] || { echo "branch code must match ^[A-Z0-9]{3,10}\$ (contract)"; exit 1; }
# Characters that need no quoting in the Kafka config syntax, the JAAS line of the producer, or a .env file
[[ $password =~ ^[A-Za-z0-9._~-]{8,128}$ ]] || { echo "password: 8-128 characters from A-Z a-z 0-9 . _ ~ -"; exit 1; }
topic="branch-sales.daily-summary.$code"

kafka() { # kafka <tool> <args...>: Kafka CLI inside the broker container, on its PLAINTEXT host listener
  local tool=$1; shift
  docker exec hq-kafka "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"
}

echo "1. SCRAM user $code"
# The password is on the docker exec command line for the duration of this call (visible to local admins in the
# process list). kafka-configs --add-config-file is not an option: it turns "SCRAM-SHA-512" into "SCRAM_SHA_512".
kafka kafka-configs.sh --alter --entity-type users --entity-name "$code" \
  --add-config "SCRAM-SHA-512=[iterations=8192,password=$password]" > /dev/null

echo "2. topic $topic"
kafka kafka-topics.sh --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 \
  --config retention.ms=1209600000 > /dev/null

echo "3. ACL: User:$code may write $topic only"
kafka kafka-acls.sh --add --allow-principal "User:$code" --operation Write --operation Describe \
  --topic "$topic" > /dev/null

echo "4. quota producer_byte_rate=$byte_rate"
kafka kafka-configs.sh --alter --entity-type users --entity-name "$code" \
  --add-config "producer_byte_rate=$byte_rate" > /dev/null

echo "5. HQ branch registry"
docker exec -i hq-db psql -U hq_app -d hq_sales -q -v ON_ERROR_STOP=1 -v code="$code" -v name="$name" <<'SQL'
insert into branch (branch_code, name) values (:'code', :'name')
on conflict (branch_code) do update set name = excluded.name;
SQL

echo "branch $code onboarded"
