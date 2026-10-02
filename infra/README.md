# HQ infrastructure

Kafka and the HQ database for local development and the demo, plus the HQ admin scripts for branch onboarding.

| Service | Image | Purpose |
|---|---|---|
| `kafka` | `apache/kafka:4.3.1` | Single KRaft node (broker + controller) |
| `edge` | `haproxy:3.2.25-alpine` | Stands in for the HQ firewall: the only HQ container on the `wan` network, forwards TCP 9094 to Kafka's `EXTERNAL` listener ([edge/haproxy.cfg](edge/haproxy.cfg)) |
| `kafka-init` | `apache/kafka:4.3.1` | Creates the dead-letter topic, then exits |
| `hq-db` | `postgres:18.6-alpine` | HQ database `hq_sales`. Tables are created by the consumer's migrations |
| `consumer` | built from this repo (`../Dockerfile`) | The HQ consumer. Profile `consumer` |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0` | Optional, profile `tools` |

## Run

```bash
cd infra
cp .env.example .env          # then set HQ_DB_PASSWORD
tls/generate-certs.sh         # demo CA + broker certificate in tls/out/ (git-ignored)
docker compose up -d
docker compose ps             # kafka and hq-db healthy, kafka-init exited 0
./smoke-test.sh

docker compose --profile consumer up -d --build # the consumer as well (otherwise run it from the IDE)
docker compose --profile tools up -d kafka-ui   # optional, http://localhost:8088
```

Stop with `docker compose down`. Add `-v` to delete Kafka and DB data.

## Onboarding a branch

```bash
BRANCH_KAFKA_PASSWORD=... ./onboard-branch.sh BR0001 "Branch 1"
```

| Step | What |
|---|---|
| 1 | Kafka user `BR0001` (SCRAM-SHA-512) with that password |
| 2 | topic `branch-sales.daily-summary.BR0001` (1 partition, 14 days retention) |
| 3 | ACL: user `BR0001` may write (and describe) this topic only |
| 4 | quota `producer_byte_rate` = 10240 bytes/s for user `BR0001` (`BRANCH_PRODUCER_BYTE_RATE` to change it). A branch sends a few KB per hour; a faulty producer that keeps sending is slowed down without affecting other branches |
| 5 | `BR0001` in the HQ `branch` table |

The password must be 8–128 characters from `A-Z a-z 0-9 . _ ~ -` (no quoting needed in the Kafka and producer configuration). The branch gets its code, the password and `tls/out/ca.crt`. The consumer picks up the new topic within a minute (topic pattern, `metadata.max.age.ms`). Running the script again sets a new password and quota and skips what exists.

```bash
./offboard-branch.sh BR0001    # credentials leaked or branch closed
```

Closing a branch: HQ still accepts its back-dated sales (duplicates are skipped by revision), so offboard only after the branch has nothing pending (no `CONFIRMED` day without a `SENT` row in its `sync_log`). Offboarding removes the ACLs, so the branch's next write is refused even on an open connection, and deletes the SCRAM credentials, so it cannot log in again. Branch sessions must log in again every 10 minutes (`connections.max.reauth.ms` on `EXTERNAL`), so a session opened with an old password ends within 10 minutes, also when the branch is onboarded again with a new password. The topic, the HQ branch row and the stored sales stay. To give access back, onboard again with a new password.

## Networks

```
         branch-sales-wan                               branch-sales-hq
 ┌───────────────────────────────┐      ┌───────────────────────────────────────────┐
 │ branch producer(s)            │      │                                           │
 │     │ TLS + SCRAM             │      │   kafka :9094 EXTERNAL   consumer   hq-db │
 │     └─► kafka.hq.example:9094 ┼─ hq-edge ─► ▲                    │              │
 │         (hq-edge, port 9094)  │      │   kafka :19092 INTERNAL ◄┘              │
 └───────────────────────────────┘      └───────────────────────────────────────────┘
```

- `branch-sales-hq`: Kafka (all listeners), the consumer, the HQ DB, and `hq-edge`.
- `branch-sales-wan`: stands in for the internet. The only HQ container on it is `hq-edge`, under the alias `kafka.hq.example`. It forwards port 9094 and nothing else, like a firewall port forward. TLS passes through it unchanged.
- Kafka is not on `branch-sales-wan`, so the `PLAINTEXT` listeners cannot be reached from there. `smoke-test.sh` checks this.
- Branch producers join `branch-sales-wan` (as an external network) plus their own branch network. They can reach `kafka.hq.example:9094` but cannot resolve `hq-db` or `kafka`.

## Kafka listeners

| Listener | Address clients use | Security | Used by |
|---|---|---|---|
| `EXTERNAL` | `kafka.hq.example:9094` (through `hq-edge`) | `SASL_SSL`: TLS (certificate for `kafka.hq.example`, signed by the demo CA) + SCRAM-SHA-512 user per branch, re-login every 10 minutes | branch producers (network `wan`) |
| `INTERNAL` | `kafka:19092` | `PLAINTEXT` | consumer, kafka-ui, kafka-init (network `hq`) |
| `HOST` | `localhost:9092` (published on `127.0.0.1` only) | `PLAINTEXT` | apps run from an IDE and the admin scripts (inside the broker container) |
| `CONTROLLER` | `kafka:9093` | `PLAINTEXT` | KRaft only |

ACLs are on (`StandardAuthorizer`, nothing allowed without an ACL). Clients on the `PLAINTEXT` listeners have no identity (`User:ANONYMOUS`), which is a super user, so HQ services need no ACLs.

## Smoke test

`./smoke-test.sh` uses a temporary Kafka user and temporary topics (removed at the end) and checks:

1. the dead-letter topic exists
2. a client on `wan` with valid credentials can write over TLS + SCRAM to the topic its ACL allows
3. an HQ client on `hq` reads that record through `INTERNAL`
4. the same client is refused (`TopicAuthorizationException`) on a topic its ACL does not allow
5. a wrong password is refused (`SaslAuthenticationException`)
6. a client without TLS/SASL cannot use `EXTERNAL`
7. a client on `wan` cannot resolve `hq-db`
8. a client on `wan` cannot resolve `kafka` or connect to ports 19092, 9092, 9093 (only 9094 is forwarded)

## Topics

| Topic | Created by | Partitions | Retention |
|---|---|---|---|
| `branch-sales.daily-summary.<branchCode>` | `onboard-branch.sh` | 1 | 14 days |
| `branch-sales.daily-summary.dlt` | `kafka-init` | 6 | 30 days |

Auto topic creation is off. See [`../contract/`](../contract/) for the message format.

Upgrading a Kafka volume from before one-topic-per-branch: the shared topic `branch-sales.daily-summary` is no longer created or read. Let the old consumer finish reading it, then delete it (`docker exec hq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic branch-sales.daily-summary`).

## Limits of this setup

- One Kafka node, replication factor 1: no broker fault tolerance. Fine for development; production needs at least 3 nodes with `min.insync.replicas=2`.
- `INTERNAL` and `HOST` are `PLAINTEXT` with `User:ANONYMOUS` as super user. They are reachable only from the HQ network and from this machine (`127.0.0.1`), because Kafka is not on `wan` and the published ports are bound to loopback. Anything that gets onto the HQ network has full Kafka access; production should give HQ services their own credentials (or mTLS) too.
- The CA and the broker key are created by a script, and the broker key is not encrypted (readable by all users on this machine, because the broker runs as uid 1000 in its container). Only `kafka.pem` is mounted into the broker; the CA key stays in `tls/out/`. Production uses the organisation's CA and a secret store.
- Branch passwords are passed to the scripts as environment variables, appear on the `docker exec` command line while `onboard-branch.sh` runs (visible to local admins in the process list), and end up in each branch's `.env`. Production would hand them over through a secret store.
