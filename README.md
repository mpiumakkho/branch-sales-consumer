# branch-sales-consumer

HQ side of Branch Daily Sales Sync. Reads confirmed daily sales summaries that branches publish to Kafka, validates them against the [message contract](contract/), and stores them in the HQ PostgreSQL database.

| Folder | Content |
|---|---|
| [`contract/`](contract/) | Message contract shared with the branch producer: JSON Schema, examples, topics |
| [`infra/`](infra/) | Docker Compose for Kafka and the HQ database |
| `src/` | The consumer (Java 25, Spring Boot 4) |

## How a record is handled

```
branch-sales.daily-summary ──► batch listener ──► per record:
                                                   parse → schema → business → reference (branch, category)
                                                     │ fail                          │ pass
                                                     ▼                               ▼
                                     branch-sales.daily-summary.dlt      upsert by revision, own DB transaction
                                                     └───────────────┬───────────────┘
                                          offsets committed after the whole batch is handled
```

- Validation layers and reject reasons are defined in [`contract/README.md`](contract/README.md). The schema file is copied from `contract/` onto the classpath at build time, so there is one copy only.
- Each record gets its own database transaction. A rejected record goes to the dead-letter topic and the batch continues.
- Offsets are committed after the listener returns (ack mode `BATCH`), so after every record's transaction has committed.
- Revisions (rules R4–R6): one `INSERT ... ON CONFLICT DO UPDATE ... WHERE stored.revision < incoming.revision`. A higher revision replaces the header and all lines; the same revision is logged as `DUPLICATE`; a lower one is logged as `STALE` at WARN. Neither goes to the dead-letter topic.
- Any other failure (database or Kafka unreachable) is retried with exponential back-off (1 s up to 60 s) and no attempt limit. Records before the failed one are committed; the partition waits at the failed record and nothing is skipped.

## Database

Flyway migrations in [`src/main/resources/db/migration`](src/main/resources/db/migration):

| Table | Content |
|---|---|
| `branch` | Branch registry. Rows are added when a branch is onboarded; the consumer only reads it |
| `category` | Standard categories from [`contract/categories.md`](contract/categories.md), seeded by `V2` |
| `branch_daily_sales` | One row per `(branch_code, sale_date)` with the highest revision received, its `event_id`, `confirmed_at` and `received_at` |
| `branch_daily_sales_line` | Category lines of that revision |

The upsert uses `RETURNING old.*`, which needs PostgreSQL 18 or later.

## Run

Requires JDK 25 and Docker.

```bash
cd infra && cp .env.example .env    # set HQ_DB_PASSWORD
docker compose up -d && ./smoke-test.sh && cd ..

set -a; . infra/.env; set +a        # HQ_DB_PASSWORD for the consumer
./mvnw spring-boot:run
```

Before branch data is accepted, the branch must exist in the `branch` table:

```sql
insert into branch (branch_code, name) values ('BR0001', 'Branch 1');
```

Configuration (environment variables):

| Variable | Default | |
|---|---|---|
| `HQ_DB_URL` | `jdbc:postgresql://localhost:5433/hq_sales` | |
| `HQ_DB_USER` | `hq_app` | |
| `HQ_DB_PASSWORD` | none | required |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka `HOST` listener; inside the `branch-sales-hq` network use `kafka:19092` |
| `CONSUMER_MAX_POLL_RECORDS` | `100` | records per poll; limits load on the HQ DB |
| `CONSUMER_CONCURRENCY` | `3` | listener threads; the topic has 6 partitions |

## Test

```bash
./mvnw test
```

Needs Docker. The tests start their own Kafka and PostgreSQL containers (same images as `infra/`) and do not use the running infra.

| Test | Checks |
|---|---|
| `SummaryValidatorTest` | every file in `contract/examples/` gets its expected result from the parse, schema and business layers; edge cases (not JSON, repeated key, formats, out-of-range numbers) |
| `DailySummaryFlowTest` | through Kafka: valid examples stored, every invalid example in the dead-letter topic with the right `reject-reason` and unchanged bytes, revision rules R4–R6 |
| `DatabaseFailureTest` | a database error is retried until the record is stored, and is not sent to the dead-letter topic |
