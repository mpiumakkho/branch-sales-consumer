#!/usr/bin/env bash
# Revokes a branch's access to Kafka, e.g. when credentials leak or the branch closes:
#   infra/offboard-branch.sh BR0001
#
# Removes the branch's ACLs (its next write is refused, even on an open connection) and its SCRAM credentials
# (it cannot log in again; an open session ends at its next re-login, at most 10 minutes later).
# Keeps the topic, so records already sent are still consumed, and keeps the HQ branch row and its sales data.
# To give access back, run onboard-branch.sh again with a new password.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep container paths as they are

code=${1:?usage: $0 <branchCode>}
[[ $code =~ ^[A-Z0-9]{3,10}$ ]] || { echo "branch code must match ^[A-Z0-9]{3,10}\$ (contract)"; exit 1; }

kafka() {
  local tool=$1; shift
  docker exec hq-kafka "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"
}

echo "1. remove ACLs of User:$code"
kafka kafka-acls.sh --remove --force --allow-principal "User:$code" --operation Write --operation Describe \
  --topic "branch-sales.daily-summary.$code" > /dev/null

echo "2. delete SCRAM credentials of $code"
# Deleting credentials that do not exist is an error, so check first: running the script twice is fine
if kafka kafka-configs.sh --describe --entity-type users --entity-name "$code" | grep -q 'SCRAM-SHA-512'; then
  kafka kafka-configs.sh --alter --entity-type users --entity-name "$code" --delete-config SCRAM-SHA-512 > /dev/null
else
  echo "   no SCRAM credentials for $code"
fi

echo "branch $code offboarded"
