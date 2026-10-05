#!/usr/bin/env bash
# HQ is the certificate authority for every branch broker (requirements §16.5). Output goes to infra/tls/out/, which
# is git-ignored: never commit keys.
#
#   infra/tls/generate-certs.sh            # the HQ CA, once (skipped if it exists)
#   infra/tls/generate-certs.sh BR0001     # the broker certificate of branch BR0001 (called by onboard-branch.sh)
#
# out/ca.crt                    CA certificate: the HQ consumer trusts branch brokers signed by it
# out/ca.key                    CA private key: stays at HQ
# out/branches/<code>/kafka.pem branch broker private key (PKCS#8, unencrypted) + certificate for
#                               kafka.<code in lower case>.example; handed to the branch with its HQ password
#
# Demo simplification: the branch key is created at HQ and handed over. A real deployment would have the branch
# create its key and send a certificate request (CSR), so the key never leaves the branch, and would use the
# organisation's CA.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep -subj values as they are
cd "$(dirname "$0")"
mkdir -p out
cd out

if [[ $# -eq 0 ]]; then
  if [[ -f ca.key ]]; then
    echo "tls/out/ca.key exists, nothing to do (delete tls/out/ to start over; every branch then needs a new certificate)"
    exit 0
  fi
  openssl req -x509 -newkey rsa:3072 -nodes -days 825 -subj "/CN=branch-sales demo HQ CA" \
    -keyout ca.key -out ca.crt
  # The consumer runs as a non-root user in its container and must read ca.crt; the CA key is never mounted
  chmod 644 ca.crt
  chmod 600 ca.key
  # Created here, before docker compose mounts it into the consumer: a directory Docker creates for a bind mount is
  # owned by root on Linux, and onboard-branch.sh could then not write password files into it
  mkdir -p ../../secrets/branch-kafka
  echo "created tls/out/ca.crt, tls/out/ca.key, secrets/branch-kafka/"
  exit 0
fi

code=$1
[[ $code =~ ^[A-Z0-9]{3,10}$ ]] || { echo "branch code must match ^[A-Z0-9]{3,10}\$ (contract)"; exit 1; }
[[ -f ca.key ]] || { echo "tls/out/ca.key missing: run tls/generate-certs.sh first"; exit 1; }
host="kafka.$(tr '[:upper:]' '[:lower:]' <<<"$code").example"
dir="branches/$code"
mkdir -p "$dir"

openssl req -newkey rsa:3072 -nodes -subj "/CN=$host" -keyout "$dir/kafka.key" -out "$dir/kafka.csr" 2>/dev/null
# HQ checks that the host name it connects to is in the certificate (subjectAltName)
printf 'subjectAltName=DNS:%s\nextendedKeyUsage=serverAuth\n' "$host" > "$dir/kafka.ext"
openssl x509 -req -in "$dir/kafka.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 \
  -extfile "$dir/kafka.ext" -out "$dir/kafka.crt" 2>/dev/null

# Kafka PEM keystore: private key in PKCS#8 followed by the certificate chain
openssl pkcs8 -topk8 -nocrypt -in "$dir/kafka.key" -out "$dir/kafka.p8"
cat "$dir/kafka.p8" "$dir/kafka.crt" ca.crt > "$dir/kafka.pem"
rm -f "$dir/kafka.key" "$dir/kafka.p8" "$dir/kafka.csr" "$dir/kafka.ext" ca.srl
# The branch broker runs as a non-root user (uid 1000) in its container and must be able to read kafka.pem
chmod 644 "$dir/kafka.pem"

echo "created tls/out/$dir/kafka.pem for $host"
