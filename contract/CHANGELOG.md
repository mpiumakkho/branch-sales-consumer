# Contract changelog

## v1 — 2026-10-08 (shift close per POS terminal)

- New record type `ShiftClose` v1 ([schema](shift-close.v1.schema.json)): the shift close (Z-report) of one POS terminal, one message per `(branchCode, businessDate, terminalId, shiftNo, revision)`. Lines are tenders (`tenderType`, `amount`, `quantity`), may be empty for a shift with no transactions, and the message adds `openedAt`, `closedAt`, `transactionCount`, `cashExpected`, `cashCounted` and the optional `cashierId`. Reopening a shift and closing it again is the next revision of the same key. Sent to the new topic `branch-sales.shift-close` in the branch's cluster (`kafka-init` creates it; HQ's user gets Read on it).
- HQ stores a shift close whether or not it holds the branch's `DailySalesSummary` of the same date; there is no parent rule and no cross-check between the two.
- New HQ-owned master of tender types ([tender-types.md](tender-types.md)): `CASH`, `CREDIT_CARD`, `DEBIT_CARD`, `QR_PAYMENT`, `E_WALLET`, `COUPON`, `OTHER`.
- New reject reasons: `DUPLICATE_TENDER` (a `tenderType` repeated within `lines`), `UNKNOWN_TENDER` (a `tenderType` not in the HQ `tender_type` table) and `SHIFT_TIMES_INVALID` (`closedAt` before `openedAt`). `TOTAL_MISMATCH` applies as for the other types.
- `DailySalesReceipt`: `type` gets the value `SHIFT_CLOSE`, and the optional fields `terminalId` and `shiftNo` are written for a shift close when the record could be read. `saleDate` carries the shift's `businessDate`. The summary and return schemas do not change.

## v1 — 2026-10-08 (daily returns)

- New record type `DailyReturn` v1 ([schema](daily-return.v1.schema.json)): the confirmed returns and voids of one day by category, same shape as the summary with `returnDate` instead of `saleDate`. Sent to the new topic `branch-sales.daily-return` in the branch's cluster (`kafka-init` creates it; HQ's user gets Read on it).
- HQ stores a return only when it holds the branch's `DailySalesSummary` of the same date; otherwise `PARENT_MISSING`, a new reject reason, and HQ replays the return by itself when that summary arrives.
- `DailySalesReceipt` gets the optional field `type` (`DAILY_SUMMARY` or `DAILY_RETURN`; absent = `DAILY_SUMMARY`). Receipts are matched by `(type, sourceOffset)`, since offsets are per topic. `saleDate` in a receipt carries the record's business date, so a return's `returnDate`. The summary schema does not change.

## v1 — 2026-10-05 (Kafka at the branch, receipts)

The summary schema does not change (`schemaVersion` stays 1). Where it travels and what comes back does:

- Every branch runs its own Kafka broker. Summaries go to `branch-sales.daily-summary` in the branch's cluster, and HQ connects to each branch to read them. The per-branch topics and the HQ cluster are gone.
- `BRANCH_MISMATCH` now compares `branchCode` with the branch whose cluster the record was read from.
- New: `DailySalesReceipt` v1 ([schema](daily-sales-receipt.v1.schema.json)) on `branch-sales.receipt` in the branch's cluster, one per summary record.
- Rejected records are stored in the HQ table `dead_letter` instead of the topic `branch-sales.daily-summary.dlt`, and can be replayed.

## v1 — 2026-10-02 (one topic per branch)

The message schema does not change (`schemaVersion` stays 1). Where a branch sends it does:

- Branches send to their own topic `branch-sales.daily-summary.<branchCode>` instead of the shared `branch-sales.daily-summary`, logged in with their own Kafka user. The shared topic is no longer read.
- New reject reasons: `BRANCH_MISMATCH` (message `branchCode` is not the branch of the topic) and `KEY_MISMATCH` (record key missing or not equal to `branchCode`).
- Dead-letter records are partitioned by key instead of keeping the source partition number.

## v1 — 2026-10-02 (dead-letter headers)

- Documented the dead-letter record format: header `reject-reason` plus the `kafka_dlt-*` headers. The message schema does not change.

## v1 — 2026-10-02

- First version of `DailySalesSummary` (`schemaVersion: 1`)
- Topics `branch-sales.daily-summary` and `branch-sales.daily-summary.dlt`
- Standard categories: `BEVERAGE`, `SNACK`, `READY_MEAL`, `FRESH_FOOD`, `HOUSEHOLD`, `PERSONAL_CARE`, `OTHER`
