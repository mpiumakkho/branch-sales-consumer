#!/usr/bin/env bash
# Stops HQ reading from a branch, e.g. when the branch closes:
#   infra/offboard-branch.sh BR0001
#
# Clears the branch's Kafka address in the registry (the consumer disconnects within a minute) and deletes HQ's
# password for it. Keeps the branch row and its sales data at HQ (Q6). Records the branch has not sent yet stay in
# the branch's own Kafka; wait until the branch has nothing pending before offboarding a closed branch.
# To read from the branch again, run onboard-branch.sh again.
#
# If HQ's password for the branch leaked, the branch also removes user "hq" at its broker or sets a new password
# (producer repo, README).
set -euo pipefail
cd "$(dirname "$0")"

code=${1:?usage: $0 <branchCode>}
[[ $code =~ ^[A-Z0-9]{3,10}$ ]] || { echo "branch code must match ^[A-Z0-9]{3,10}\$ (contract)"; exit 1; }

echo "1. HQ branch registry: stop reading $code"
docker exec -i hq-db psql -U hq_app -d hq_sales -q -v ON_ERROR_STOP=1 -v code="$code" <<'SQL'
update branch set kafka_bootstrap = null where branch_code = :'code';
SQL

echo "2. delete HQ password for $code"
rm -f "secrets/branch-kafka/$code"

echo "branch $code offboarded"
