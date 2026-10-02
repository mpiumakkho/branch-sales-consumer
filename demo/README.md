# End-to-end demo

Runs HQ (Kafka, HQ database, consumer) and two branches (branch database + producer each) on one machine with Docker, then walks through the normal flow and the failure cases.

```
 branch-br0001 (network)          branch-sales-wan              branch-sales-hq
 ┌─────────────────────┐                                    ┌──────────────────────────┐
 │ branch-db  producer ├──┐   TLS + SCRAM user BR0001       │ consumer ──► hq-db       │
 └─────────────────────┘  ├──► kafka.hq.example:9094 ── hq-edge ─► kafka:9094 EXTERNAL │
 ┌─────────────────────┐  │    (port forward, TLS passes)   │ kafka:19092 INTERNAL ◄─┘ │
 │ branch-db  producer ├──┘   TLS + SCRAM user BR0002       └──────────────────────────┘
 └─────────────────────┘
 branch-br0002 (network)
```

| Branch | Kafka topic | Back-office category codes | Branch configuration |
|---|---|---|---|
| BR0001 | `branch-sales.daily-summary.BR0001` | `BEV`, `SNK`, `RTE`, `HH` | `branch-sales-producer/demo/BR0001/branch.yaml` |
| BR0002 | `branch-sales.daily-summary.BR0002` | `C01`, `C02`, `C03`, `C05`, `C08`, `C99` | `branch-sales-producer/demo/BR0002/branch.yaml` (`C01` and `C02` both map to `BEVERAGE`; `C99` is not mapped) |

The demo branches start a send round every minute with up to 15 s random delay. The real default is every hour with up to 30 minutes (requirements Q4).

## Requirements

- Docker with Compose v2, Git Bash (on Windows), OpenSSL (included in Git Bash)
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
branch-sales-consumer/infra/tls/generate-certs.sh                              # demo CA + broker certificate
(cd branch-sales-consumer/infra && docker compose --profile consumer up -d --build && ./smoke-test.sh)
```

The smoke test checks TLS + SCRAM on the external listener, that a client cannot write a topic its ACL does not allow, that a wrong password and a plaintext client are refused, and that the WAN can reach neither the HQ DB nor Kafka's internal listeners.

Onboard the two demo branches: Kafka user, topic, ACL, quota and HQ branch registry ([infra/README.md](../infra/README.md#onboarding-a-branch)). Choose a password per branch (8–128 characters from `A-Z a-z 0-9 . _ ~ -`):

```bash
BRANCH_KAFKA_PASSWORD=pw-br0001 branch-sales-consumer/infra/onboard-branch.sh BR0001 "Demo branch 1"
BRANCH_KAFKA_PASSWORD=pw-br0002 branch-sales-consumer/infra/onboard-branch.sh BR0002 "Demo branch 2"
```

Within a minute, `docker logs hq-consumer` shows the consumer picking up the new topics (`partitions assigned: [branch-sales.daily-summary.BR0001-0, ...]`).

## 2. Start the branches

```bash
cp branch-sales-producer/.env.example branch-sales-producer/.env
# in branch-sales-producer/.env: BRANCH_DB_PASSWORD, and BR0001_KAFKA_PASSWORD=pw-br0001, BR0002_KAFKA_PASSWORD=pw-br0002
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0001.compose.yaml up -d --build)
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0002.compose.yaml up -d)
```

Each producer creates `sync_log` and `sync_attempt` in its branch database (Flyway baseline 0, then `V1`, `V2`) and logs in to Kafka as its branch. A branch can reach Kafka but not the HQ database:

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
 BR0001      | 2026-10-01 |        1 |     41870.50 | ... | 2026-10-02 16:21:12 | BEVERAGE 18200.00 x410, HOUSEHOLD 2500.00 x37, READY_MEAL 9120.50 x152, SNACK 12050.00 x395
 BR0002      | 2026-10-01 |        1 |      7730.00 | ... | 2026-10-02 16:21:07 | BEVERAGE 4650.00 x132, FRESH_FOOD 980.00 x30, SNACK 2100.00 x95
```

At the branches, `sync_log` shows `SENT`, `attempts 1`.

### 3.2 Edit after sending (revision 2)

```bash
branch-sales-producer/demo/sql.sh BR0001 branch-sales-producer/demo/BR0001/02-edit-and-reconfirm.sql
```

The manager edits 2026-10-01 (back to `DRAFT`) and confirms again. The producer sends revision 2, and HQ replaces the header and lines (`docker logs hq-consumer`: `UPDATED BR0001/2026-10-01 revision 2`):

```
 BR0001      | 2026-10-01 |        2 |     42370.50 | ... | BEVERAGE 18700.00 x422, HOUSEHOLD 2500.00 x37, READY_MEAL 9120.50 x152, SNACK 12050.00 x395
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
not acknowledged by Kafka: org.apache.kafka.common.errors.TimeoutException: Expiring 1 record(s) for branch-sales.daily-summary.BR0001-0:30001 ms has passed since batch creation
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
 2026-10-01 |        1 | 2026-10-02 16:21:12 | SENT   | a3136bb9-0e64-4721-a95b-2befce84f45a |
 2026-10-01 |        2 | 2026-10-02 16:22:07 | SENT   | ca4be5a6-3639-4b3a-b925-375c2405993b |
 2026-10-02 |        1 | 2026-10-02 16:25:43 | FAILED | fe761992-c74a-4289-987f-7e85d8b669d5 | not acknowledged by Kafka: ... TimeoutException: Expiring 1 record(s) ...
 2026-10-02 |        1 | 2026-10-02 16:26:03 | SENT   | 06010ae7-643e-4bb6-9e87-c7219cbffe17 |
```

The `event_id` of the `SENT` attempt is the one HQ stores in `branch_daily_sales.event_id` (here `06010ae7-...`).

A message the producer recorded as failed may still have reached Kafka: a request sent while disconnected can be delivered once the connection comes back, and then HQ receives the day twice. In earlier runs of this demo that looked like this:

```
INSERTED BR0001/2026-10-02 revision 1 (...@2)
DUPLICATE BR0001/2026-10-02 revision 1 (...@3)
```

This is the at-least-once delivery the design expects. HQ keeps one copy, because the revision is the same (rule R5).

### 3.5 Messages HQ rejects (dead-letter topic)

Real producers check their data before sending, so this sends contract example files directly, as a faulty branch would: logged in as BR0001, on BR0001's topic, with key `BR0001`.

```bash
export BRANCH_KAFKA_PASSWORD=pw-br0001
(cd branch-sales-consumer && demo/send-raw.sh BR0001 contract/examples/invalid-business/total-mismatch.json)
# BR0002's data sent by BR0001: the topic says the sender is BR0001
(cd branch-sales-consumer && demo/send-raw.sh BR0001 contract/examples/valid/unknown-field.json)
(cd branch-sales-consumer && demo/read-dlt.sh)
```

```
  detail: TOTAL_MISMATCH: totalAmount 48250.00 but lines sum to 30250.00
key=BR0001 reason=TOTAL_MISMATCH
  detail: BRANCH_MISMATCH: branchCode BR0002 sent on the topic of branch BR0001
key=BR0001 reason=BRANCH_MISMATCH
```

The stored HQ data does not change, and the consumer continues with the next records. BR0001 cannot write BR0002's topic at all: the broker refuses it (`TopicAuthorizationException`, smoke test step 4). Kafka UI shows all dead-letter headers: `(cd branch-sales-consumer/infra && docker compose --profile tools up -d kafka-ui)`, then open <http://localhost:8088>.

### 3.6 Revoke a branch

HQ revokes BR0002 (for example, its password leaked), and BR0002 confirms a day:

```bash
branch-sales-consumer/infra/offboard-branch.sh BR0002
branch-sales-producer/demo/sql.sh BR0002 branch-sales-producer/demo/BR0002/03-next-day.sql
```

The producer is still connected, but its next send is refused (the ACL is gone; the old session would also end at its next re-login, within 10 minutes). The day stays pending:

```
 sale_date  |  status   | revision | sync_status | attempts | last_error
 2026-10-03 | CONFIRMED |        1 | FAILED      |        1 | not acknowledged by Kafka: org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [branch-sales.daily-summary.BR0002]
```

HQ onboards the branch again with a new password, and the branch updates its configuration and restarts the producer:

```bash
BRANCH_KAFKA_PASSWORD=pw-br0002-new branch-sales-consumer/infra/onboard-branch.sh BR0002 "Demo branch 2"
# in branch-sales-producer/.env: BR0002_KAFKA_PASSWORD=pw-br0002-new
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0002.compose.yaml up -d producer)
```

The next round sends the day, and HQ stores it (`INSERTED BR0002/2026-10-03 revision 1`).

## 4. Stop and reset

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0001.compose.yaml down -v)
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo/BR0002.compose.yaml down -v)
(cd branch-sales-consumer/infra && docker compose --profile consumer --profile tools down -v)
```

`-v` deletes the branch databases, the HQ database and the Kafka data, including the Kafka users. Leave it out to keep the data. Delete `branch-sales-consumer/infra/tls/out/` to create new certificates next time.

## Known limits

- A record in the dead-letter topic is not replayed automatically. The producer has already recorded it as `SENT`, because the broker accepted it. After the cause is fixed (for example, a branch that was not registered), the branch has to send that revision again, for example by deleting its `SENT` row in `sync_log` (requirements Q8).
- The network is simulated with Docker networks. The HQ-internal listeners are plaintext; see [infra/README.md](../infra/README.md#limits-of-this-setup).
