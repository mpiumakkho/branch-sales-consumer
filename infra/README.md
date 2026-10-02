# HQ infrastructure

Kafka and the HQ database for local development and the demo.

| Service | Image | Purpose |
|---|---|---|
| `kafka` | `apache/kafka:4.3.1` | Single KRaft node (broker + controller) |
| `kafka-init` | `apache/kafka:4.3.1` | Creates the contract topics, then exits |
| `hq-db` | `postgres:18.6-alpine` | HQ database `hq_sales`. Tables are created by the consumer's migrations |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0` | Optional, profile `tools` |

## Run

```bash
cd infra
cp .env.example .env          # then set HQ_DB_PASSWORD
docker compose up -d
docker compose ps             # kafka and hq-db healthy, kafka-init exited 0
./smoke-test.sh

docker compose --profile tools up -d kafka-ui   # optional, http://localhost:8088
```

Stop with `docker compose down`. Add `-v` to delete Kafka and DB data.

## Networks

```
            branch-sales-wan                       branch-sales-hq
 ┌──────────────────────────────┐     ┌─────────────────────────────────────┐
 │ branch producer(s)           │     │ consumer                            │
 │        │                     │     │    │                                │
 │        └─► kafka.hq.example:9094 ◄─┼─ kafka ─► :19092 ◄─┘    hq-db       │
 └──────────────────────────────┘     └─────────────────────────────────────┘
```

- `branch-sales-hq`: Kafka, the consumer and the HQ DB.
- `branch-sales-wan`: stands in for the internet. Only Kafka is on it, under the alias `kafka.hq.example`.
- Branch producers join `branch-sales-wan` (as an external network) plus their own branch network. They can reach Kafka but cannot resolve `hq-db`. `smoke-test.sh` checks this.

## Kafka listeners

| Listener | Address clients use | Used by |
|---|---|---|
| `INTERNAL` | `kafka:19092` | consumer, kafka-ui, kafka-init (network `hq`) |
| `EXTERNAL` | `kafka.hq.example:9094` | branch producers (network `wan`) |
| `HOST` | `localhost:9092` | apps run from an IDE on this machine |
| `CONTROLLER` | `kafka:9093` | KRaft only |

All listeners are `PLAINTEXT` for now. TLS and per-branch SASL/SCRAM credentials come in step 6.5, on `EXTERNAL` first.

## Topics

| Topic | Partitions | Retention |
|---|---|---|
| `branch-sales.daily-summary` | 6 | 14 days |
| `branch-sales.daily-summary.dlt` | 6 | 30 days |

Auto topic creation is off. See [`../contract/`](../contract/) for the message format.

## Limits of this setup

- One Kafka node, replication factor 1: no broker fault tolerance. Fine for development; production needs at least 3 nodes with `min.insync.replicas=2`.
- No TLS or authentication yet (step 6.5).
