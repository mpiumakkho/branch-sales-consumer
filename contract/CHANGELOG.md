# Contract changelog

## v1 — 2026-10-02 (dead-letter headers)

- Documented the dead-letter record format: header `reject-reason` plus the `kafka_dlt-*` headers. The message schema does not change.

## v1 — 2026-10-02

- First version of `DailySalesSummary` (`schemaVersion: 1`)
- Topics `branch-sales.daily-summary` and `branch-sales.daily-summary.dlt`
- Standard categories: `BEVERAGE`, `SNACK`, `READY_MEAL`, `FRESH_FOOD`, `HOUSEHOLD`, `PERSONAL_CARE`, `OTHER`
