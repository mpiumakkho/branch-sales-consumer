# branch-sales-consumer

[![ci](https://github.com/mpiumakkho/branch-sales-consumer/actions/workflows/ci.yml/badge.svg)](https://github.com/mpiumakkho/branch-sales-consumer/actions/workflows/ci.yml)

HQ side of Branch Daily Sales Sync. Every branch runs its own Kafka broker; the consumer connects to each branch in the HQ branch registry, reads the confirmed daily sales summaries the branch published, validates them against the [message contract](contract/), stores them in the HQ PostgreSQL database, and writes a receipt back to the branch for every record.

| Folder | Content |
|---|---|
| [`contract/`](contract/) | Message contract shared with the branch producer: summary and receipt JSON Schemas, examples, topics |
| [`infra/`](infra/) | Docker Compose for the HQ database and the consumer, HQ CA, branch onboarding scripts |
| [`demo/`](demo/) | End-to-end demo with two branches: [demo/README.md](demo/README.md) |
| [`bench/`](bench/) | Scale measurement: cost of a connected branch in the consumer JVM and records per second, with results: [bench/README.md](bench/README.md) |
| `src/` | The consumer (Java 25, Spring Boot 4) |

## How a record is handled

```
branch registry (table branch, kafka_bootstrap)  ──► one listener container per branch, refreshed every minute
        │
        └──► branch-sales.daily-summary in that branch's Kafka ──► per record:
                  parse → schema → identity (branch, key) → business → reference (branch, category)
                                     │ fail                                 │ pass
                                     ▼                                      ▼
                        table dead_letter (bytes unchanged)      upsert by revision, own DB transaction
                                     └──────────────────┬───────────────────┘
                                  receipt to branch-sales.receipt in that branch's Kafka, wait for ack
                                  offsets committed after the whole batch is handled
```

- The consumer reaches a branch through the address registered for it, so the cluster identifies the sender: a message whose `branchCode` is not that branch is rejected (`BRANCH_MISMATCH`), as is a key that is not the `branchCode` (`KEY_MISMATCH`). Validation layers and reject reasons: [`contract/README.md`](contract/README.md). The schema files are copied from `contract/` onto the classpath at build time, so there is one copy only.
- Each record gets its own database transaction. A rejected record is kept in `dead_letter` and the batch continues.
- Every record gets a receipt (`INSERTED`, `UPDATED`, `DUPLICATE`, `STALE` or `REJECTED` with the reason), written to the branch's receipt topic after the database commit. The consumer waits for the branch broker's ack before moving on, and commits offsets after the listener returns (ack mode `BATCH`), so a branch never misses a receipt for a record HQ has handled.
- Revisions (rules R4–R6): one `INSERT ... ON CONFLICT DO UPDATE ... WHERE stored.revision < incoming.revision`. A higher revision replaces the header and all lines; the same revision is `DUPLICATE`; a lower one is `STALE` (logged at WARN). Neither is a rejection.
- Any other failure (HQ database or the branch broker unreachable) is retried with exponential back-off (1 s up to 60 s) and no attempt limit, per branch. Records before the failed one are committed; that branch waits at the failed record and nothing is skipped. Other branches are not affected: each has its own container and thread.
- Branches are connected from the registry: a row with a `kafka_bootstrap` address is connected within a minute, a cleared address disconnects, a changed address reconnects. A branch that cannot be connected yet (host name not resolvable, password file missing) is tried again at every refresh. See [infra/README.md](infra/README.md#onboarding-a-branch).
- Several consumer instances can share the branches: each instance is started with a shard name (`CONSUMER_SHARD`) and reads only the branches whose registry row has that shard (`infra/onboard-branch.sh`, `BRANCH_SHARD`). Moving a branch to another shard in the registry moves it between instances within a minute. Replays of rejected records are done by the instance that reads the branch. Measured cost per connected branch and sizing: [bench/README.md](bench/README.md) (about 1 MB RSS, 3 threads and 0.15% of a core idle per branch; 300–500 branches per instance is a reasonable shard).

## Replaying rejected records

`dead_letter` keeps the branch, source offset, the record key and value bytes, reject reason, detail and time. When the cause is fixed at HQ (for example a category was added to `category`), ask for the record to be processed again:

```sql
update dead_letter set replay_requested_at = now() where id = 42;
```

Within `DEAD_LETTER_REPLAY_INTERVAL_MS` the consumer runs the record through all layers again and sends the branch a new receipt for the same source offset. The row then shows `replayed_at` and `replay_result` (`INSERTED`, ..., or `REJECTED` again with the new reason). A row whose branch is not connected waits for the next run.

## Database

Flyway migrations in [`src/main/resources/db/migration`](src/main/resources/db/migration):

| Table | Content |
|---|---|
| `branch` | Branch registry: code, name, `kafka_bootstrap` (the address HQ reads the branch at; null = not read) and `shard` (which consumer instance reads it). Set by `infra/onboard-branch.sh` / `offboard-branch.sh` |
| `category` | Standard categories from [`contract/categories.md`](contract/categories.md), seeded by `V2` |
| `branch_daily_sales` | One row per `(branch_code, sale_date)` with the highest revision received, its `event_id`, `confirmed_at` and `received_at` |
| `branch_daily_sales_line` | Category lines of that revision |
| `dead_letter` | Rejected records, one per `(branch_code, source_offset)`, with replay request and result |

The upsert uses `RETURNING old.*`, which needs PostgreSQL 18 or later.

## Run

In Docker, together with the HQ database ([infra/README.md](infra/README.md)):

```bash
cd infra && cp .env.example .env    # set HQ_DB_PASSWORD
tls/generate-certs.sh               # HQ CA
docker compose --profile consumer up -d --build && ./smoke-test.sh
HQ_KAFKA_PASSWORD=... ./onboard-branch.sh BR0001 "Branch 1"    # per branch
```

From the IDE or the command line instead (requires JDK 25), with only the DB in Docker. Branch brokers are then reached from this machine, so the registry must hold addresses this machine can resolve, and `BRANCH_KAFKA_PASSWORD_DIR` / `BRANCH_KAFKA_TRUSTSTORE` must point at `infra/secrets/branch-kafka` and `infra/tls/out/ca.crt`:

```bash
cd infra && cp .env.example .env && tls/generate-certs.sh && docker compose up -d && cd ..
set -a; . infra/.env; set +a
BRANCH_KAFKA_PASSWORD_DIR=infra/secrets/branch-kafka BRANCH_KAFKA_TRUSTSTORE=infra/tls/out/ca.crt ./mvnw spring-boot:run
```

Configuration (environment variables):

| Variable | Default | |
|---|---|---|
| `HQ_DB_URL` | `jdbc:postgresql://localhost:5433/hq_sales` | |
| `HQ_DB_USER` | `hq_app` | |
| `HQ_DB_PASSWORD` | none | required |
| `BRANCH_KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` | how every branch broker is reached: TLS + SCRAM-SHA-512 as user `hq`. `PLAINTEXT` for tests only |
| `BRANCH_KAFKA_TRUSTSTORE` | `/certs/ca.crt` | the HQ CA certificate (PEM) that signs every branch broker certificate; host names are verified |
| `BRANCH_KAFKA_PASSWORD_DIR` | `/run/secrets/branch-kafka` | one file per branch code with HQ's password at that branch (written by `onboard-branch.sh`) |
| `CONSUMER_SHARD` | `default` | this instance reads the branches whose `branch.shard` is this value (`a-z 0-9 -`, up to 30 characters) |
| `BRANCH_REGISTRY_REFRESH_MS` | `60000` | how often the registry is read to connect or disconnect branches |
| `DEAD_LETTER_REPLAY_INTERVAL_MS` | `30000` | how often replay requests are looked for |
| `CONSUMER_MAX_POLL_RECORDS` | `100` | records per poll and branch; limits load on the HQ DB |

The receipt topic, summary topic and consumer group are fixed by the contract (`branch-sales.receipt`, `branch-sales.daily-summary`, `hq-branch-sales-consumer`).

## Test

```bash
./mvnw test
```

Needs Docker. The tests start a PostgreSQL container and one Kafka container per test branch (BR0001, BR0002), register the branches in the HQ registry and read HQ's receipts from the branch topics; they do not use the running infra.

| Test | Checks |
|---|---|
| `SummaryValidatorTest` | every file in `contract/examples/` gets its expected result from the parse, schema, identity and business layers; layer order; edge cases (not JSON, repeated key, formats, out-of-range numbers) |
| `DailySummaryFlowTest` | through two branch brokers: valid examples stored, every invalid example in `dead_letter` with unchanged bytes and a `REJECTED` receipt with the right reason, one receipt per record matched by source offset (checked against the receipt schema), `BRANCH_MISMATCH` / `KEY_MISMATCH`, revision rules R4–R6 with their receipts, replay of a dead letter after the cause is fixed, following the registry (offboard, onboard, a branch in another shard, a branch that cannot be connected yet) |
| `DatabaseFailureTest` | a database error is retried until the record is stored, with exactly one receipt and no dead letter |
