# End-to-end demo

Runs HQ (Kafka, HQ database, consumer) and two branches (branch database + producer each) on one machine with Docker, then walks through the normal flow and the failure cases.

```
 branch-br0001 (network)          branch-sales-wan            branch-sales-hq
 ┌─────────────────────┐                                  ┌──────────────────────────┐
 │ branch-db  producer ├──┐                               │ consumer ──► hq-db       │
 └─────────────────────┘  ├──► kafka.hq.example:9094 ─────┤    ▲                     │
 ┌─────────────────────┐  │        (Kafka EXTERNAL)       │ kafka:19092 (INTERNAL)   │
 │ branch-db  producer ├──┘                               └──────────────────────────┘
 └─────────────────────┘
 branch-br0002 (network)
```

| Branch | Back-office category codes | Mapping file |
|---|---|---|
| BR0001 | `BEV`, `SNK`, `RTE`, `HH` | `branch-sales-producer/demo/BR0001/branch.yaml` |
| BR0002 | `C01`, `C02`, `C03`, `C05`, `C08`, `C99` | `branch-sales-producer/demo/BR0002/branch.yaml` (`C01` and `C02` both map to `BEVERAGE`; `C99` is not mapped) |

The demo branches start a send round every minute with up to 15 s random delay. The real default is every hour with up to 30 minutes (requirements Q4).

## Requirements

- Docker with Compose v2, Git Bash (on Windows)
- Both repos cloned next to each other. All commands below run from the folder that contains them:

```
branch-sales-consumer/
branch-sales-producer/
```

Status queries used throughout:

```bash
# HQ: what HQ has stored
docker exec -i hq-db psql -U hq_app -d hq_sales < branch-sales-consumer/demo/hq-status.sql
# Branch: days in the back-office and their send status (sync_log)
branch-sales-producer/demo/sql.sh BR0001 branch-sales-producer/demo/branch-status.sql
```

## 1. Start HQ

```bash
cp branch-sales-consumer/infra/.env.example branch-sales-consumer/infra/.env   # set HQ_DB_PASSWORD
(cd branch-sales-consumer/infra && docker compose --profile consumer up -d --build && ./smoke-test.sh)

# Onboard the two demo branches in the HQ branch registry
docker exec -i hq-db psql -U hq_app -d hq_sales -q < branch-sales-consumer/demo/register-branches.sql
```

`docker logs hq-consumer` shows `partitions assigned` for all 6 partitions.

## 2. Start the branches

```bash
cp branch-sales-producer/.env.example branch-sales-producer/.env   # set BRANCH_DB_PASSWORD
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0001.compose.yaml up -d --build)
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0002.compose.yaml up -d)
```

Each producer creates `sync_log` in its branch database (Flyway baseline 0, then `V1`). A branch can reach Kafka but not the HQ database:

```bash
docker exec branch-br0001-producer-1 getent hosts kafka.hq.example   # resolves
docker exec branch-br0001-producer-1 getent hosts hq-db              # no output
```

## 3. Scenarios

### 3.1 Confirm a day at both branches

```bash
branch-sales-producer/demo/sql.sh BR0001 branch-sales-producer/demo/BR0001/01-enter-and-confirm.sql
branch-sales-producer/demo/sql.sh BR0002 branch-sales-producer/demo/BR0002/01-enter-and-confirm.sql
```

Within about a minute, HQ has both days. BR0002's `C01` and `C02` arrive as one `BEVERAGE` line. The two branches send at different seconds because of the random delay:

```
 branch_code | sale_date  | revision | total_amount | ... | received_at_bkk     | lines
 BR0001      | 2026-10-01 |        1 |     41870.50 | ... | 2026-10-02 13:30:01 | BEVERAGE 18200.00 x410, HOUSEHOLD 2500.00 x37, READY_MEAL 9120.50 x152, SNACK 12050.00 x395
 BR0002      | 2026-10-01 |        1 |      7730.00 | ... | 2026-10-02 13:30:15 | BEVERAGE 4650.00 x132, FRESH_FOOD 980.00 x30, SNACK 2100.00 x95
```

At the branches, `sync_log` shows `SENT`, `attempts 1`.

### 3.2 Edit after sending (revision 2)

```bash
branch-sales-producer/demo/sql.sh BR0001 branch-sales-producer/demo/BR0001/02-edit-and-reconfirm.sql
```

The manager edits 2026-10-01 (back to `DRAFT`) and confirms again. The producer sends revision 2, and HQ replaces the header and lines (`docker logs hq-consumer`: `UPDATED BR0001/2026-10-01 revision 2`):

```
 BR0001      | 2026-10-01 |        2 |     42370.50 | ... | BEVERAGE 18700.00 x422, HOUSEHOLD 2500.00 x37, ...
```

### 3.3 A category code with no HQ mapping

```bash
branch-sales-producer/demo/sql.sh BR0002 branch-sales-producer/demo/BR0002/02-unmapped-category.sql
```

The day contains `C99`, which is not in BR0002's mapping. The producer does not send it, because HQ would only reject it. It stays pending:

```
 sale_date  |  status   | revision | sync_status | attempts | last_error
 2026-10-02 | CONFIRMED |        1 | FAILED      |        1 | no HQ category mapping for local category [C99]
```

Fix the mapping (add `C99: OTHER` to `branch-sales-producer/demo/BR0002/branch.yaml`) and restart the producer:

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0002.compose.yaml restart producer)
```

The next round sends it (`SENT`, `attempts 2`), and HQ shows `OTHER 500.00 x5`. Undo the mapping change afterwards if you want to run this scenario again.

### 3.4 Branch offline

Cut the branch off from the WAN, confirm a day, and wait for a round:

```bash
docker network disconnect branch-sales-wan branch-br0001-producer-1
branch-sales-producer/demo/sql.sh BR0001 branch-sales-producer/demo/BR0001/03-next-day.sql
```

After about 30 s (the producer's send timeout), the day is `FAILED`, with the reason in `last_error`:

```
not acknowledged by Kafka: org.apache.kafka.common.errors.TimeoutException: Expiring 1 record(s) for branch-sales.daily-summary-5:30001 ms has passed since batch creation
```

If the producer was restarted while disconnected, the reason is `No resolvable bootstrap urls given in bootstrap.servers` instead: it cannot even resolve `kafka.hq.example`.

The data is still in the branch database. Reconnect:

```bash
docker network connect branch-sales-wan branch-br0001-producer-1
```

The next round sends the day (`SENT`, `attempts 2`), and HQ stores it. `sync_log` no longer shows the error; the attempt history does:

```bash
branch-sales-producer/demo/sql.sh BR0001 branch-sales-producer/demo/sync-attempts.sql
```

```
 sale_date  | revision |  attempted_at_bkk   | result |               event_id               | error
 2026-10-04 |        1 | 2026-10-02 14:19:06 | FAILED | b3eb9ccd-31fa-4d7f-96bd-4b6706e50ec2 | not acknowledged by Kafka: ... No resolvable bootstrap urls given in bootstrap.servers
 2026-10-04 |        1 | 2026-10-02 14:20:02 | SENT   | d3559b83-f367-4e2e-af0b-6882262fe4e1 |
```

The `event_id` of the `SENT` attempt is the one HQ stores in `branch_daily_sales.event_id`.

A message the producer recorded as failed may still have reached Kafka. In both test runs of this demo, the request sent while disconnected was delivered once the connection came back, so HQ received the day twice:

```
INSERTED BR0001/2026-10-02 revision 1 (branch-sales.daily-summary-5@2)
DUPLICATE BR0001/2026-10-02 revision 1 (branch-sales.daily-summary-5@3)
```

This is the at-least-once delivery the design expects. HQ keeps one copy, because the revision is the same (rule R5).

### 3.5 A message HQ rejects (dead-letter topic)

Real producers check their data before sending, so this sends contract example files directly, as a faulty branch would:

```bash
(cd branch-sales-consumer && demo/send-raw.sh BR0001 contract/examples/invalid-business/total-mismatch.json)
(cd branch-sales-consumer && demo/send-raw.sh BR0001 contract/examples/invalid-schema/amount-as-number.json)
(cd branch-sales-consumer && demo/read-dlt.sh)
```

```
  detail: TOTAL_MISMATCH: totalAmount 48250.00 but lines sum to 30250.00
key=BR0001 reason=TOTAL_MISMATCH
  detail: SCHEMA_INVALID: /totalAmount: number found, string expected; /lines/0/amount: number found, string expected
key=BR0001 reason=SCHEMA_INVALID
```

The stored HQ data does not change, and the consumer continues with the next records. Kafka UI shows all dead-letter headers: `(cd branch-sales-consumer/infra && docker compose --profile tools up -d kafka-ui)`, then open <http://localhost:8088>.

## 4. Stop and reset

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0001.compose.yaml down -v)
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0002.compose.yaml down -v)
(cd branch-sales-consumer/infra && docker compose --profile consumer --profile tools down -v)
```

`-v` deletes the branch databases, the HQ database and the Kafka data. Leave it out to keep the data.

## Known limits

- A record in the dead-letter topic is not replayed automatically. The producer has already recorded it as `SENT`, because the broker accepted it. After the cause is fixed (for example, a branch that was not registered), the branch has to send that revision again, for example by deleting its `SENT` row in `sync_log`.
- The network is simulated with Docker networks. TLS and per-branch credentials come in step 6.5.
