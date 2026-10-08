# HQ infrastructure

The HQ database and the consumer for local development and the demo, the HQ certificate authority, and the HQ admin scripts for branch onboarding. Kafka runs at every branch (producer repo); HQ has no Kafka and no inbound port.

| Service | Image | Purpose |
|---|---|---|
| `hq-db` | `postgres:18.6-alpine` | HQ database `hq_sales`. Tables are created by the consumer's migrations |
| `consumer` | built from this repo (`../Dockerfile`) | The HQ consumer, profile `consumer`. The only HQ container on the `wan` network: it connects out to every branch. Health and metrics on `127.0.0.1:8081` (`CONSUMER_HTTP_PORT`), bound to the consumer's `hq` address so `wan` cannot reach them |

## Run

```bash
cd infra
cp .env.example .env          # then set HQ_DB_PASSWORD
tls/generate-certs.sh         # HQ CA in tls/out/ (git-ignored)
docker compose --profile consumer up -d --build
docker compose ps             # hq-db healthy, hq-consumer up
./smoke-test.sh
curl -s localhost:8081/actuator/health   # {"status":"UP", ... "branches": {... "connected": 0 ...}}
```

Without `--profile consumer` only the database starts (for running the consumer from the IDE). Stop with `docker compose --profile consumer down`; add `-v` to delete the DB data.

## Onboarding a branch

```bash
HQ_KAFKA_PASSWORD=... ./onboard-branch.sh BR0001 "Branch 1"
```

| Step | What |
|---|---|
| 1 | broker certificate for `kafka.br0001.example` (the branch's host name on the WAN), signed by the HQ CA: `tls/out/branches/BR0001/kafka.pem` |
| 2 | HQ's password at that branch's broker, in `secrets/branch-kafka/BR0001` (git-ignored, read by the consumer) |
| 3 | `BR0001` in the HQ `branch` table with `kafka_bootstrap = kafka.br0001.example:9094` (`BRANCH_KAFKA_PORT` to change the port) and `shard = default` (`BRANCH_SHARD` to assign the branch to another consumer instance) |

The password must be 8–128 characters from `A-Z a-z 0-9 . _ ~ -` (no quoting needed in the Kafka configuration or a `.env` file). Hand the branch its `kafka.pem` and the same password: the branch's `kafka-init` creates user `hq` with it and the ACLs that let HQ read summaries and write receipts, and nothing else. The consumer connects within a minute of the registry change (`BRANCH_REGISTRY_REFRESH_MS`); until the branch is up, `docker logs hq-consumer` shows `Cannot connect to branch BR0001` once a minute. Running the script again renews the certificate, replaces the password and updates the registry row.

```bash
./offboard-branch.sh BR0001    # branch closed, or HQ's password at the branch leaked
```

Offboarding clears the branch's address in the registry (the consumer disconnects within a minute) and deletes HQ's password file. The branch row and the stored sales stay (Q6). Records the branch has not sent yet stay in its own Kafka, so offboard a closing branch only after its producer shows nothing pending (`demo/sync-state.sh` in the producer repo). If the password leaked, the branch also removes user `hq` at its broker or sets a new one (its `kafka-init`). To read from the branch again, onboard it again.

## Networks

```
 branch-br0001 (branch network)        branch-sales-wan                 branch-sales-hq
 ┌──────────────────────────────┐   ┌─────────────────────────┐   ┌──────────────────────┐
 │ producer ─► kafka :19092     │   │                         │   │                      │
 │ mongodb     kafka :9094 ◄────┼── edge (kafka.br0001.example:9094) ◄── consumer ─► hq-db │
 │ branch-db   (SASL_SSL)       │   │  TLS passes through     │   │                      │
 └──────────────────────────────┘   └─────────────────────────┘   └──────────────────────┘
```

- `branch-sales-hq`: the consumer and the HQ DB. Fixed subnet `10.231.0.0/24` (`HQ_SUBNET`), the consumer at `10.231.0.10` (`HQ_CONSUMER_IP`): the consumer's health/metrics port listens on that address only.
- `branch-sales-wan`: stands in for the internet. HQ's only container on it is the consumer, which connects out. There is no HQ port on it: `smoke-test.sh` checks that `hq-db` cannot be resolved from `wan` and that no other HQ container is on it.
- Each branch joins `wan` with its edge only (alias `kafka.<branch>.example`), which forwards port 9094 to the branch broker's `SASL_SSL` listener. The branch side is described in the producer repo.

## HQ's access at a branch broker

| | |
|---|---|
| Address | `kafka.<branch code in lower case>.example:9094`, through the branch's edge |
| Security | `SASL_SSL`: TLS with the branch broker certificate signed by the HQ CA, host name verified; SCRAM-SHA-512 user `hq`, re-login every 10 minutes |
| ACLs (set by the branch) | Read + Describe `branch-sales.daily-summary`, `branch-sales.daily-return` and `branch-sales.shift-close`, Write + Describe `branch-sales.receipt`, Read group `hq-branch-sales-consumer` |

## Smoke test

`./smoke-test.sh` checks:

1. the HQ DB accepts connections
2. a client on `wan` cannot resolve `hq-db`
3. no HQ container other than the consumer is on `wan`
4. `wan` cannot open the consumer's health/metrics port (when the consumer is running)

Each branch has its own smoke test (`smoke-test.sh` in the producer repo), run from `wan` as HQ would connect.

## Limits of this setup

- The branch broker key is created at HQ (`tls/generate-certs.sh BR0001`) and handed over with the certificate. Production would have the branch create its key and send a certificate request, so the key never leaves the branch, and would use the organisation's CA. The keys are not encrypted.
- Passwords are passed to the scripts as environment variables and stored as plain files in `secrets/branch-kafka/` (readable by the consumer container's non-root user, so by every user on this machine). Production would use a secret store.
- The consumer opens one consumer and one producer client per branch. For thousands of branches, run several consumer instances with different `CONSUMER_SHARD` values and spread the branches over them with `BRANCH_SHARD` when onboarding (or `update branch set shard = ...`). The compose file here runs one instance.
- One HQ database instance, no replica.
