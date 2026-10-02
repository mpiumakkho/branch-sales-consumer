# Contract changelog

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
