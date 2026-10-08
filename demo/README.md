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

HQ connects out to each branch; neither side has an inbound port other than the branch edge's 9094. Each branch's Kafka holds the record topics `branch-sales.daily-summary`, `branch-sales.daily-return` and `branch-sales.shift-close` (written by the producer, read by HQ) and `branch-sales.receipt` (written by HQ, read by the producer).

| Branch | Back-office category codes | Branch configuration |
|---|---|---|
| BR0001 | `BEV`, `SNK`, `RTE`, `HH`, `GC` | `branch-sales-producer/demo-branches/BR0001/branch.yaml` (`GC` maps to `GIFT_CARD`, which HQ does not have yet) |
| BR0002 | `C01`, `C02`, `C03`, `C05`, `C08`, `C99` | `branch-sales-producer/demo-branches/BR0002/branch.yaml` (`C01` and `C02` both map to `BEVERAGE`; `C99` is not mapped) |

The POS tender codes are mapped the same way (`branch-sales.tender-mapping`): BR0001 `CSH`, `CRD`, `DBT`, `QR`; BR0002 `T1`, `T2`, `T3`, with `GV` (gift voucher) not mapped.

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
docker exec hq-db psql -U hq_app -d hq_sales -c "select id, branch_code, record_type, source_offset, record_date, reject_reason, replay_result from dead_letter"
# Branch: send state of every (type, day, revision), from its MongoDB; --history adds every attempt and receipt
branch-sales-producer/demo-branches/sync-state.sh BR0001
# Branch: days in the back-office
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/branch-status.sql
# Health: HQ consumer (connected / unreachable branches), producer of BR0001 (its Kafka, MongoDB, database)
curl -s localhost:8081/actuator/health
curl -s localhost:8091/actuator/health          # BR0002: 8092
# Metrics in Prometheus format: records by outcome at HQ, sync_state by status at the branch
curl -s localhost:8081/actuator/prometheus | grep branch_sales
curl -s localhost:8091/actuator/prometheus | grep branch_sales
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

Each branch's `kafka-init` creates the four topics (sales, returns, shift closes, receipts), user `hq` and its ACLs, then exits. The smoke test connects from `wan` as HQ does (TLS with the HQ CA, host name checked, SCRAM) and checks that nothing but the edge's port 9094 is reachable. Within a minute, `docker logs hq-consumer` shows `Connected to branch BR0001 at kafka.br0001.example:9094` and the same for BR0002.

## 3. Scenarios

### 3.1 Confirm a day at both branches

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/01-enter-and-confirm.sql
branch-sales-producer/demo-branches/sql.sh BR0002 branch-sales-producer/demo-branches/BR0002/01-enter-and-confirm.sql
```

Within about a minute, HQ has both days. BR0002's `C01` and `C02` arrive as one `BEVERAGE` line:

```
 record | branch_code | business_date | revision | total_amount | ... | received_at_bkk     | lines
 SALES  | BR0001      | 2026-10-01    |        1 |     41870.50 | ... | 2026-10-05 14:09:11 | BEVERAGE 18200.00 x410, HOUSEHOLD 2500.00 x37, READY_MEAL 9120.50 x152, SNACK 12050.00 x395
 SALES  | BR0002      | 2026-10-01    |        1 |      7730.00 | ... | 2026-10-05 14:09:13 | BEVERAGE 4650.00 x132, FRESH_FOOD 980.00 x30, SNACK 2100.00 x95
```

Each branch got HQ's receipt within a few seconds of sending (`sync-state.sh BR0001`):

```
SALES  2026-10-01  r1  HQ_ACCEPTED  attempts=1  offsets=0  INSERTED stored revision 1
```

### 3.2 Edit after sending (revision 2)

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/02-edit-and-reconfirm.sql
```

The manager edits 2026-10-01 (back to `DRAFT`) and confirms again. The producer sends revision 2 and HQ replaces the header and lines; the branch sees the receipt:

```
SALES  2026-10-01  r1  HQ_ACCEPTED  attempts=1  offsets=0  INSERTED stored revision 1
SALES  2026-10-01  r2  HQ_ACCEPTED  attempts=1  offsets=1  UPDATED stored revision 2
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
SALES  2026-10-02  r1  FAILED       attempts=1  offsets=  no HQ category mapping for local category [C99]
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
SALES  2026-10-03  r1  HQ_REJECTED  attempts=1  offsets=2  UNKNOWN_CATEGORY: categoryCode [GIFT_CARD] is not in the category table
```

HQ keeps the record unchanged:

```
 id | branch_code |  record_type  | source_offset | record_date |  reject_reason   | replay_result
  1 | BR0001      | DAILY_SUMMARY |             2 |             | UNKNOWN_CATEGORY |
```

The branch does not send it again on its own. HQ fixes the cause (adds the category) and asks for the record to be processed again:

```bash
docker exec hq-db psql -U hq_app -d hq_sales -c "insert into category (category_code, description) values ('GIFT_CARD', 'Gift cards sold')"
docker exec hq-db psql -U hq_app -d hq_sales -c "update dead_letter set replay_requested_at = now() where id = 1"
```

Within 30 s the consumer replays it (`docker logs hq-consumer`: `Replayed dead letter 1 (DAILY_SUMMARY BR0001 offset 2): INSERTED`), HQ has the day, the row shows `replay_result = INSERTED`, and the branch gets a second receipt for the same offset (`sync-state.sh BR0001 --history`):

```
SALES  2026-10-03  r1  HQ_ACCEPTED  attempts=1  offsets=2  INSERTED stored revision 1
    2026-10-05 14:11:05  SENT          offset=2
    2026-10-05 14:11:05  HQ_REJECTED   offset=2  UNKNOWN_CATEGORY: categoryCode [GIFT_CARD] is not in the category table
    2026-10-05 14:12:45  HQ_INSERTED   offset=2
```

Messages that fail other contract checks (not JSON, schema, totals, wrong branch code) take the same path; the consumer tests send every example in `contract/examples/` through it.

### 3.5 Returns of a day, before and after its sales

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/05-returns.sql
```

The manager confirms the returns of two days. They go to the branch's second topic, `branch-sales.daily-return`. HQ stores a day's returns only when it has that day's sales: 2026-10-01 is there (3.1), so its returns are stored right away; 2026-10-05 has no sales yet, so HQ keeps those returns in `dead_letter` and tells the branch why (`sync-state.sh BR0001`):

```
RETURN 2026-10-01  r1  HQ_ACCEPTED  attempts=1  offsets=0  INSERTED stored revision 1
RETURN 2026-10-05  r1  HQ_REJECTED  attempts=1  offsets=1  PARENT_MISSING: no daily sales of BR0001 for 2026-10-05 at HQ yet; replayed when they arrive
```

Offsets start at 0 again: they are per topic, and the receipt's `type` tells the branch which one. Nothing to do at either side: the sales of 2026-10-05 are confirmed later,

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/06-sales-after-returns.sql
```

and when HQ stores them it asks for the waiting returns itself (`docker logs hq-consumer`: `Replayed dead letter 2 (DAILY_RETURN BR0001 offset 1): INSERTED`). The branch gets a second receipt and shows `HQ_ACCEPTED` for the returns; `hq-status.sql` lists `SALES` and `RETURN` rows for 2026-10-05.

### 3.6 Shift closes of the POS terminals

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/07-shift-close.sql
```

**(a) Three shifts closed.** The POS opens a shift (`OPEN`) and closes it with the Z-report (`CLOSED`, revision + 1): POS01 shift 1 and shift 2, and POS02 shift 1. Only closed shifts are sent, to the branch's third topic `branch-sales.shift-close`, after the sales and returns of the same round. The branch's local tender codes (`CSH`, `CRD`, `DBT`, `QR`) are mapped to HQ tender types (`branch-sales.tender-mapping` in `branch.yaml`, [contract/tender-types.md](../contract/tender-types.md)). HQ stores one row per (branch, business date, terminal, shift) and answers each with `INSERTED`; the receipt carries `terminalId` and `shiftNo` besides `saleDate` (the business date). `hq-status.sql` lists them as `SHIFT` rows, with the tenders as lines and `cash_over_short` computed at HQ (cash counted - cash expected; POS01 shift 1 counted 9100.00 against 9120.00 expected, so `-20.00`):

```
 record | branch_code | business_date | revision | total_amount | terminal_shift | cash_over_short | ... | lines
 SHIFT  | BR0001      | 2026-10-01    |        1 |          ... | POS01#1        |          -20.00 | ... | CASH ... x..., CREDIT_CARD ... x..., QR_PAYMENT ... x...
```

**(b) Reopen and close again.**

```bash
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/08-reopen-shift.sql
```

The cashier reopens POS01 shift 1 and recounts the drawer. Closing it again raises its revision to 2; the shift number stays the same (R11). HQ replaces the header and the tender lines (`UPDATED stored revision 2`) and recomputes `cash_over_short`. The other shifts are not touched.

**(c) A shift before the day's sales.**

```bash
branch-sales-producer/demo-branches/sql.sh BR0002 branch-sales-producer/demo-branches/BR0002/04-shift-before-summary.sql
```

BR0002 closes a shift of a day whose sales it has not confirmed. Unlike a return (3.5), a shift close has no parent: HQ stores it (`INSERTED`), nothing goes to `dead_letter`, and there is no `PARENT_MISSING` (R12). Shifts close during the day and the day is confirmed in the evening, so either can arrive first. To compare the shifts of a day with its sales:

```bash
docker exec -i hq-db psql -U hq_app -d hq_sales < branch-sales-consumer/demo/hq-reconciliation.sql
```

One row per branch and business day: the number of shifts, the sum of their totals, the daily sales total, the difference, and the sum of over/short. This is a query, not a rule: HQ rejects neither side over a difference (R13). Here BR0002's day shows the shift total and no daily sales total until the day is confirmed.

**(d) A tender code with no HQ mapping.**

```bash
branch-sales-producer/demo-branches/sql.sh BR0002 branch-sales-producer/demo-branches/BR0002/05-shift-unmapped-tender.sql
```

The shift has payments with `GV` (gift voucher), which is not in BR0002's tender mapping. As with an unmapped category (3.3), the producer does not send it; `sync-state.sh BR0002` shows the shift `FAILED` with the unmapped code, tried again every round.

**(e) A tender type HQ does not have, and its replay.** Map `GV` to a tender type HQ does not have yet (add `GV: GIFT_VOUCHER` under `branch-sales.tender-mapping` in `branch-sales-producer/demo-branches/BR0002/branch.yaml`) and restart the producer:

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0002.compose.yaml restart producer)
```

The next round sends the shift and HQ rejects it with `UNKNOWN_TENDER`: `dead_letter` has a row with `record_type = SHIFT_CLOSE` and `record_date` = the business date, and the branch shows `HQ_REJECTED  ... UNKNOWN_TENDER: tenderType [GIFT_VOUCHER] is not in the tender_type table`. HQ adds the tender type and asks for the replay, as in 3.4:

```bash
docker exec hq-db psql -U hq_app -d hq_sales -c "insert into tender_type (tender_type, description) values ('GIFT_VOUCHER', 'Gift voucher')"
docker exec hq-db psql -U hq_app -d hq_sales -c "update dead_letter set replay_requested_at = now() where record_type = 'SHIFT_CLOSE' and reject_reason = 'UNKNOWN_TENDER' and replayed_at is null"
```

Within 30 s the consumer replays it (`Replayed dead letter ... (SHIFT_CLOSE BR0002 offset ...): INSERTED`) and the branch gets a second receipt (`HQ_ACCEPTED`). Undo the mapping change afterwards if you want to run this scenario again.

**(f) Optional: a branch that cannot read the shift topic yet.** A branch whose `kafka-init` has not created the shift topic, or has not given HQ access to it, is read for the topics it has. Remove HQ's Read on the shift topic at BR0002:

```bash
MSYS_NO_PATHCONV=1 docker exec branch-br0002-kafka-1 /opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:19092 \
  --remove --force --allow-principal User:hq --operation Read --operation Describe --topic branch-sales.shift-close
docker restart hq-consumer
```

After the restart, `docker logs hq-consumer` shows `Branch BR0002 has only [branch-sales.daily-summary, branch-sales.daily-return] of the record topics [...]: reading those until its kafka-init adds the rest`, and BR0002's sales and returns are still stored. Shift closes wait in the branch's Kafka. Run `kafka-init` again to restore the ACL:

```bash
(cd branch-sales-producer && docker compose -f docker-compose.yml -f demo-branches/BR0002.compose.yaml up -d kafka-init)
```

Within a minute the consumer logs `Branch BR0002 now has all record topics; reconnecting` and reads the waiting shift closes.

### 3.7 Branch offline

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

### 3.8 Offboard and onboard a branch

HQ stops reading BR0001 (for example, HQ's password at that branch leaked), and BR0001 confirms a day meanwhile:

```bash
branch-sales-consumer/infra/offboard-branch.sh BR0001
# docker logs hq-consumer: "Disconnected from branch BR0001 ..." within a minute, then:
branch-sales-producer/demo-branches/sql.sh BR0001 branch-sales-producer/demo-branches/BR0001/03-next-day.sql
```

The branch sends as usual, into its own broker, and waits for a receipt (`SALES  2026-10-02  r1  SENT`). HQ onboards the branch again with a new password; the branch puts the new password into its `.env` and runs `kafka-init` again, which replaces user `hq`'s password:

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

`-v` deletes the branch databases, MongoDB, the branches' Kafka data (including user `hq`) and the HQ database. Leave it out to keep the data. The back-office tables (`branch-db-data` volume) are created by init scripts that run only on an empty volume: a branch started before the shift close tables existed (`pos_shift`, `pos_shift_tender`) needs `down -v` once to get them; until then its producer keeps sending sales and returns and logs an error for shift closes every round. HQ adds its tables itself at start (Flyway V8). Delete `branch-sales-consumer/infra/tls/out/` and `branch-sales-consumer/infra/secrets/` to start over with a new CA; every branch then needs onboarding again.

## Known limits

- The network is simulated with Docker networks; `kafka.<branch>.example` is a network alias of the branch's edge. A real deployment needs a resolvable host name per branch and a firewall rule at the branch that allows port 9094 from HQ's address only.
- The branch broker key is created at HQ and handed over; see [infra/README.md](../infra/README.md#limits-of-this-setup).
- A branch runs one Kafka node. If its data is lost, the producer sends what HQ has not acknowledged again after `SEND_RESEND_AFTER`; HQ keeps one copy per revision.
