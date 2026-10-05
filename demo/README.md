# End-to-end demo

Runs HQ (HQ database, consumer) and two branches (back-office database, MongoDB, Kafka broker, edge and producer each) on one machine with Docker, then walks through the normal flow and the failure cases.

```
 branch-br0001 (network)                        branch-sales-wan                 branch-sales-hq
 ┌───────────────────────────────────────┐                                  ┌────────────────────┐
 │ branch-db ◄─ producer ─► kafka:19092  │                                  │                    │
 │ mongodb  ◄──┘            kafka:9094 ◄─┼─ edge (kafka.br0001.example) ◄───┤ consumer ─► hq-db  │
 └───────────────────────────────────────┘    TLS + SCRAM user hq           │    │               │
 ┌───────────────────────────────────────┐                                  │    │               │
 │ branch-db ◄─ producer ─► kafka:19092  │                                  │    │               │
 │ mongodb  ◄──┘            kafka:9094 ◄─┼─ edge (kafka.br0002.example) ◄───┼────┘               │
 └───────────────────────────────────────┘                                  └────────────────────┘
 branch-br0002 (network)
```

HQ connects out to each branch; neither side has an inbound port other than the branch edge's 9094. Each branch's Kafka holds `branch-sales.daily-summary` (written by the producer, read by HQ) and `branch-sales.receipt` (written by HQ, read by the producer).

| Branch | Back-office category codes | Branch configuration |
|---|---|---|
| BR0001 | `BEV`, `SNK`, `RTE`, `HH`, `GC` | `branch-sales-producer/demo-branches/BR0001/branch.yaml` (`GC` maps to `GIFT_CARD`, which HQ does not have yet) |
| BR0002 | `C01`, `C02`, `C03`, `C05`, `C08`, `C99` | `branch-sales-producer/demo-branches/BR0002/branch.yaml` (`C01` and `C02` both map to `BEVERAGE`; `C99` is not mapped) |

The demo branches start a send round every minute with up to 15 s random delay, and read confirmed days of the last 10 years (the demo days are fixed dates). The real defaults are every hour with up to 30 minutes, and 60 days (requirements Q4, §6).

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
# HQ: rejected records
docker exec hq-db psql -U hq_app -d hq_sales -c "select id, branch_code, source_offset, reject_reason, replay_result from dead_letter"
# Branch: send state of every (day, revision), from its MongoDB; --history adds every attempt and receipt
branch-sales-producer/demo-branches/sync-state.sh BR0001
# Branch: days in the back-office
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/branch-status.sql
```

## 1. Start HQ

```bash
cp branch-sales-consumer/infra/.env.example branch-sales-consumer/infra/.env   # set HQ_DB_PASSWORD
branch-sales-consumer/infra/tls/generate-certs.sh                              # HQ CA
(cd branch-sales-consumer/infra && docker compose --profile consumer up -d --build && ./smoke-test.sh)
```

Onboard the two demo branches: broker certificate, HQ's password at the branch, and the HQ branch registry ([infra/README.md](../infra/README.md#onboarding-a-branch)). Choose a password per branch (8–128 characters from `A-Z a-z 0-9 . _ ~ -`):

```bash
HQ_KAFKA_PASSWORD=pw-hq-at-br0001 branch-sales-consumer/infra/onboard-branch.sh BR0001 "Demo branch 1"
HQ_KAFKA_PASSWORD=pw-hq-at-br0002 branch-sales-consumer/infra/onboard-branch.sh BR0002 "Demo branch 2"
```

Until the branches are up, `docker logs hq-consumer` shows `Cannot connect to branch BR0001 at kafka.br0001.example:9094` once a minute: the host name does not exist yet.

## 2. Start the branches

```bash
cp branch-sales-producer/.env.example branch-sales-producer/.env
# in branch-sales-producer/.env: the four passwords, BR0001_HQ_KAFKA_PASSWORD=pw-hq-at-br0001, BR0002_HQ_KAFKA_PASSWORD=pw-hq-at-br0002
# (the *_KAFKA_PEM paths in .env.example already point at the certificates onboard-branch.sh created)
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0001.compose.yaml up -d --build)
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0002.compose.yaml up -d)
(cd branch-sales-producer && HQ_KAFKA_PASSWORD=pw-hq-at-br0001 ./smoke-test.sh BR0001 ../branch-sales-consumer/infra/tls/out/ca.crt)
(cd branch-sales-producer && HQ_KAFKA_PASSWORD=pw-hq-at-br0002 ./smoke-test.sh BR0002 ../branch-sales-consumer/infra/tls/out/ca.crt)
```

Each branch's `kafka-init` creates the two topics, user `hq` and its ACLs, then exits. The smoke test connects from `wan` as HQ does (TLS with the HQ CA, host name checked, SCRAM) and checks that nothing but the edge's port 9094 is reachable. Within a minute, `docker logs hq-consumer` shows `Connected to branch BR0001 at kafka.br0001.example:9094` and the same for BR0002.

## 3. Scenarios

### 3.1 Confirm a day at both branches

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/01-enter-and-confirm.sql
branch-sales-producer/demo-branches/sql.sh BR0002 branch-sales-producer/demo-branches/BR0002/01-enter-and-confirm.sql
```

Within about a minute, HQ has both days. BR0002's `C01` and `C02` arrive as one `BEVERAGE` line:

```
 branch_code | sale_date  | revision | total_amount | ... | received_at_bkk     | lines
 BR0001      | 2026-10-01 |        1 |     41870.50 | ... | 2026-10-05 14:09:11 | BEVERAGE 18200.00 x410, HOUSEHOLD 2500.00 x37, READY_MEAL 9120.50 x152, SNACK 12050.00 x395
 BR0002      | 2026-10-01 |        1 |      7730.00 | ... | 2026-10-05 14:09:13 | BEVERAGE 4650.00 x132, FRESH_FOOD 980.00 x30, SNACK 2100.00 x95
```

Each branch got HQ's receipt within a few seconds of sending (`sync-state.sh BR0001`):

```
2026-10-01  r1  HQ_ACCEPTED  attempts=1  offsets=0  INSERTED stored revision 1
```

### 3.2 Edit after sending (revision 2)

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/02-edit-and-reconfirm.sql
```

The manager edits 2026-10-01 (back to `DRAFT`) and confirms again. The producer sends revision 2 and HQ replaces the header and lines; the branch sees the receipt:

```
2026-10-01  r1  HQ_ACCEPTED  attempts=1  offsets=0  INSERTED stored revision 1
2026-10-01  r2  HQ_ACCEPTED  attempts=1  offsets=1  UPDATED stored revision 2
```

```
 BR0001      | 2026-10-01 |        2 |     42370.50 | ... | BEVERAGE 18700.00 x422, HOUSEHOLD 2500.00 x37, READY_MEAL 9120.50 x152, SNACK 12050.00 x395
```

### 3.3 A category code with no HQ mapping

```bash
branch-sales-producer/demo-branches/sql.sh BR0002 branch-sales-producer/demo-branches/BR0002/02-unmapped-category.sql
```

The day contains `C99`, which is not in BR0002's mapping. The producer does not send it, because HQ would only reject it. It is tried again every round, with the reason:

```
2026-10-02  r1  FAILED       attempts=1  offsets=  no HQ category mapping for local category [C99]
```

Fix the mapping (add `C99: OTHER` to `branch-sales-producer/demo-branches/BR0002/branch.yaml`) and restart the producer:

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0002.compose.yaml restart producer)
```

The next round sends it and HQ stores it (`HQ_ACCEPTED`, `OTHER 500.00 x5` at HQ). Undo the mapping change afterwards if you want to run this scenario again.

### 3.4 A message HQ rejects, and its replay

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/04-gift-card.sql
```

The day has gift cards, mapped to `GIFT_CARD`, a category HQ does not have. The producer sends it (the mapping is fine as far as the branch knows) and HQ rejects it (`UNKNOWN_CATEGORY`). Unlike a message that was never acknowledged, the branch learns the reason from HQ's receipt:

```
2026-10-03  r1  HQ_REJECTED  attempts=1  offsets=2  UNKNOWN_CATEGORY: categoryCode [GIFT_CARD] is not in the category table
```

HQ keeps the record unchanged:

```
 id | branch_code | source_offset |  reject_reason   | replay_result
  1 | BR0001      |             2 | UNKNOWN_CATEGORY |
```

The branch does not send it again on its own. HQ fixes the cause (adds the category) and asks for the record to be processed again:

```bash
docker exec hq-db psql -U hq_app -d hq_sales -c "insert into category (category_code, description) values ('GIFT_CARD', 'Gift cards sold')"
docker exec hq-db psql -U hq_app -d hq_sales -c "update dead_letter set replay_requested_at = now() where id = 1"
```

Within 30 s the consumer replays it (`docker logs hq-consumer`: `Replayed dead letter 1 (BR0001 offset 2): INSERTED`), HQ has the day, the row shows `replay_result = INSERTED`, and the branch gets a second receipt for the same offset (`sync-state.sh BR0001 --history`):

```
2026-10-03  r1  HQ_ACCEPTED  attempts=1  offsets=2  INSERTED stored revision 1
    2026-10-05 14:11:05  SENT          offset=2
    2026-10-05 14:11:05  HQ_REJECTED   offset=2  UNKNOWN_CATEGORY: categoryCode [GIFT_CARD] is not in the category table
    2026-10-05 14:12:45  HQ_INSERTED   offset=2
```

Messages that fail other contract checks (not JSON, schema, totals, wrong branch code) take the same path; the consumer tests send every example in `contract/examples/` through it.

### 3.5 Branch offline

Cut BR0002 off from the WAN by stopping its edge, then confirm a day:

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0002.compose.yaml stop edge)
branch-sales-producer/demo-branches/sql.sh BR0002 branch-sales-producer/demo-branches/BR0002/03-next-day.sql
```

The producer's next round still succeeds, because it writes to the broker in the branch: the day is `SENT`, with no receipt. HQ cannot reach the branch (`docker logs hq-consumer` repeats `UnknownHostException: kafka.br0002.example` for BR0002's client) and keeps reading the other branch:

```
2026-10-03  r1  SENT         attempts=1  offsets=1
```

Reconnect:

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0002.compose.yaml start edge)
```

HQ's client for BR0002 finds the broker again, reads the waiting record and answers; the branch shows `HQ_ACCEPTED`. Nothing was sent twice: the record waited in the branch's Kafka. If the branch's broker had lost it (a `SENT` day without a receipt after `SEND_RESEND_AFTER`, 24 hours by default), the producer would send it again, and HQ would answer `DUPLICATE` if it had stored it after all.

### 3.6 Offboard and onboard a branch

HQ stops reading BR0001 (for example, HQ's password at that branch leaked), and BR0001 confirms a day meanwhile:

```bash
branch-sales-consumer/infra/offboard-branch.sh BR0001
# docker logs hq-consumer: "Disconnected from branch BR0001 ..." within a minute, then:
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/03-next-day.sql
```

The branch sends as usual, into its own broker, and waits for a receipt (`2026-10-02  r1  SENT`). HQ onboards the branch again with a new password; the branch puts the new password into its `.env` and runs `kafka-init` again, which replaces user `hq`'s password:

```bash
HQ_KAFKA_PASSWORD=pw-hq-at-br0001-new branch-sales-consumer/infra/onboard-branch.sh BR0001 "Demo branch 1"
# in branch-sales-producer/.env: BR0001_HQ_KAFKA_PASSWORD=pw-hq-at-br0001-new
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0001.compose.yaml up -d kafka-init)
```

Within a minute the consumer connects again, reads the waiting day and HQ stores it; the branch shows `HQ_ACCEPTED`. The branch's broker kept its certificate, which is still valid; onboarding also wrote a renewed one, which the branch would mount at its next broker restart.

## 4. Stop and reset

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0001.compose.yaml down -v)
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0002.compose.yaml down -v)
(cd branch-sales-consumer/infra && docker compose --profile consumer down -v)
```

`-v` deletes the branch databases, MongoDB, the branches' Kafka data (including user `hq`) and the HQ database. Leave it out to keep the data. Delete `branch-sales-consumer/infra/tls/out/` and `branch-sales-consumer/infra/secrets/` to start over with a new CA; every branch then needs onboarding again.

## Known limits

- The network is simulated with Docker networks; `kafka.<branch>.example` is a network alias of the branch's edge. A real deployment needs a resolvable host name per branch and a firewall rule at the branch that allows port 9094 from HQ's address only.
- The branch broker key is created at HQ and handed over; see [infra/README.md](../infra/README.md#limits-of-this-setup).
- A branch runs one Kafka node. If its data is lost, the producer sends what HQ has not acknowledged again after `SEND_RESEND_AFTER`; HQ keeps one copy per revision.
