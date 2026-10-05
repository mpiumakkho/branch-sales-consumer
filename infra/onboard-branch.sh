#!/usr/bin/env bash
# Onboards a branch at HQ. Run once per branch, by the HQ admin:
#   HQ_KAFKA_PASSWORD=... infra/onboard-branch.sh BR0001 "Branch 1"
#
#   1. broker certificate for kafka.<code in lower case>.example, signed by the HQ CA (tls/generate-certs.sh)
#   2. HQ's password at that branch's broker, in secrets/branch-kafka/<code> (read by the consumer)
#   3. the branch in the HQ branch registry with its Kafka address; the consumer connects within a minute
#
# Then hand to the branch: tls/out/branches/<code>/kafka.pem and the same HQ_KAFKA_PASSWORD. The branch's kafka-init
# creates user "hq" with that password and its ACLs (producer repo, docker-compose.yml).
#
# Safe to run again: it renews the certificate, replaces the password and updates the registry row.
set -euo pipefail
cd "$(dirname "$0")"

code=${1:?usage: HQ_KAFKA_PASSWORD=... $0 <branchCode> <name>}
name=${2:?usage: HQ_KAFKA_PASSWORD=... $0 <branchCode> <name>}
password=${HQ_KAFKA_PASSWORD:?set HQ_KAFKA_PASSWORD (the branch puts the same value in its .env)}
# Branch broker port as seen from HQ (the branch's edge)
port=${BRANCH_KAFKA_PORT:-9094}

[[ $code =~ ^[A-Z0-9]{3,10}$ ]] || { echo "branch code must match ^[A-Z0-9]{3,10}\$ (contract)"; exit 1; }
# Characters that need no quoting in the Kafka config syntax, the JAAS line of the consumer, or a .env file
[[ $password =~ ^[A-Za-z0-9._~-]{8,128}$ ]] || { echo "password: 8-128 characters from A-Z a-z 0-9 . _ ~ -"; exit 1; }
bootstrap="kafka.$(tr '[:upper:]' '[:lower:]' <<<"$code").example:$port"

echo "1. broker certificate"
tls/generate-certs.sh "$code" > /dev/null
echo "   tls/out/branches/$code/kafka.pem"

echo "2. HQ password for $code"
mkdir -p secrets/branch-kafka
# Readable by the consumer container (non-root uid). Demo only: production would use a secret store.
(umask 022 && printf '%s' "$password" > "secrets/branch-kafka/$code")

echo "3. HQ branch registry: $code at $bootstrap"
docker exec -i hq-db psql -U hq_app -d hq_sales -q -v ON_ERROR_STOP=1 -v code="$code" -v name="$name" \
  -v bootstrap="$bootstrap" <<'SQL'
insert into branch (branch_code, name, kafka_bootstrap) values (:'code', :'name', :'bootstrap')
on conflict (branch_code) do update set name = excluded.name, kafka_bootstrap = excluded.kafka_bootstrap;
SQL

echo "branch $code onboarded; hand over tls/out/branches/$code/kafka.pem and the password"
