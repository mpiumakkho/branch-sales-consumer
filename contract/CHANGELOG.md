# Contract changelog

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
