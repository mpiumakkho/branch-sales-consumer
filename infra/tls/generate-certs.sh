#!/usr/bin/env bash
# Creates a demo certificate authority and the Kafka broker certificate for the EXTERNAL listener
# (kafka.hq.example:9094). Output goes to infra/tls/out/, which is git-ignored: never commit keys.
#
#   infra/tls/generate-certs.sh          # creates the files if they do not exist
#
# out/ca.crt      CA certificate: give it to every branch (producer truststore)
# out/ca.key      CA private key: stays at HQ, signs the broker certificate
# out/kafka.pem   broker private key (PKCS#8, unencrypted) + certificate, read by the broker
#
# A real deployment would use the organisation's CA and an encrypted key or a secret store.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: keep -subj values as they are
cd "$(dirname "$0")"
mkdir -p out
cd out

if [[ -f kafka.pem ]]; then
  echo "tls/out/kafka.pem exists, nothing to do (delete tls/out/ to create new certificates)"
  exit 0
fi

openssl req -x509 -newkey rsa:3072 -nodes -days 825 -subj "/CN=branch-sales demo CA" \
  -keyout ca.key -out ca.crt

openssl req -newkey rsa:3072 -nodes -subj "/CN=kafka.hq.example" \
  -keyout kafka.key -out kafka.csr
# Clients check that the host name they connect to is in the certificate (subjectAltName)
printf 'subjectAltName=DNS:kafka.hq.example\nextendedKeyUsage=serverAuth\n' > kafka.ext
openssl x509 -req -in kafka.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 \
  -extfile kafka.ext -out kafka.crt

# Kafka PEM keystore: private key in PKCS#8 followed by the certificate chain
openssl pkcs8 -topk8 -nocrypt -in kafka.key -out kafka.p8
cat kafka.p8 kafka.crt ca.crt > kafka.pem
rm kafka.key kafka.p8 kafka.csr kafka.ext ca.srl
# The broker runs as a non-root user (uid 1000) in the container and must be able to read kafka.pem. The CA key is
# never mounted into a container.
chmod 644 kafka.pem ca.crt
chmod 600 ca.key

echo "created tls/out/ca.crt, tls/out/ca.key, tls/out/kafka.crt, tls/out/kafka.pem"
openssl x509 -in kafka.crt -noout -subject -ext subjectAltName
