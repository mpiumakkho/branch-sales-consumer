# Message contract

The only thing the branch producer and the HQ consumer share. There is no shared code library: each side has its own classes and they meet at this JSON format.

## Topics

Every branch runs its own Kafka broker. HQ connects to each branch's broker to read the branch's records and write receipts. All topics exist in every branch's cluster, with one partition each: one topic per record type, plus the receipts.

| Topic | Written by | Read by | Content |
|---|---|---|---|
| `branch-sales.daily-summary` | the branch producer | HQ consumer | `DailySalesSummary`: the confirmed sales of one day by category |
| `branch-sales.daily-return` | the branch producer | HQ consumer | `DailyReturn`: the confirmed returns and voids of one day by category. HQ stores it only after the `DailySalesSummary` of the same date |
| `branch-sales.shift-close` | the branch producer | HQ consumer | `ShiftClose`: the shift close (Z-report) of one POS terminal by tender type. HQ stores it with or without the `DailySalesSummary` of the same date |
| `branch-sales.receipt` | HQ consumer | the branch producer | `DailySalesReceipt`: what HQ did with each record of the record topics |

The cluster a record is read from tells HQ which branch sent it: HQ connects to a branch through the address registered for that branch, and checks that the `branchCode` in the message is the same (`BRANCH_MISMATCH`).

## Record format

| Part | Value |
|---|---|
| Key | `branchCode` (UTF-8 string), required, equal to `branchCode` in the value (`KEY_MISMATCH` otherwise). |
| Value | UTF-8 JSON matching the schema of the topic: [`daily-sales-summary.v1.schema.json`](daily-sales-summary.v1.schema.json), [`daily-return.v1.schema.json`](daily-return.v1.schema.json) or [`shift-close.v1.schema.json`](shift-close.v1.schema.json). The summary and the return have the same shape; a summary has `saleDate`, a return has `returnDate`. A shift close has `businessDate` + `terminalId` + `shiftNo`, its lines are tenders (`tenderType`, [tender-types.md](tender-types.md)) and it adds the shift's open and close times, transaction count and cash expected/counted |
| Headers | none required in v1 |

Examples: [`examples/valid/basic.json`](examples/valid/basic.json), [`return-examples/valid/basic.json`](return-examples/valid/basic.json), [`shift-close-examples/valid/basic.json`](shift-close-examples/valid/basic.json)

## Validation layers

The consumer checks a message in this order. The first failure rejects the record: it is stored in the HQ `dead_letter` table and a `REJECTED` receipt goes back to the branch. Other records in the same batch continue.

| Layer | Rule | Reject reason |
|---|---|---|
| Parse | value is valid JSON | `INVALID_JSON` |
| Schema | matches the JSON Schema, with `format` validation enabled (`uuid`, `date`, `date-time`); also rejects a value the schema accepts but HQ cannot store (`revision` beyond a 32-bit integer, `quantity` beyond a 64-bit integer) | `SCHEMA_INVALID` |
| Identity | `branchCode` equals the branch whose cluster the record was read from | `BRANCH_MISMATCH` |
| Identity | record key equals `branchCode` | `KEY_MISMATCH` |
| Business | `totalAmount` equals the sum of `lines[].amount` | `TOTAL_MISMATCH` |
| Business (`DailySalesSummary`, `DailyReturn`) | `categoryCode` is unique within `lines` | `DUPLICATE_CATEGORY` |
| Business (`ShiftClose`) | `tenderType` is unique within `lines` | `DUPLICATE_TENDER` |
| Business (`ShiftClose`) | `closedAt` is not before `openedAt` | `SHIFT_TIMES_INVALID` |
| Reference | `branchCode` exists in the HQ `branch` table | `UNKNOWN_BRANCH` |
| Reference (`DailySalesSummary`, `DailyReturn`) | every `categoryCode` exists in the HQ `category` table ([categories.md](categories.md)) | `UNKNOWN_CATEGORY` |
| Reference (`ShiftClose`) | every `tenderType` exists in the HQ `tender_type` table ([tender-types.md](tender-types.md)) | `UNKNOWN_TENDER` |
| Reference (`DailyReturn` only) | HQ holds the branch's `DailySalesSummary` of `returnDate` | `PARENT_MISSING`: kept in `dead_letter` like any rejection, but replayed by HQ itself as soon as that summary is stored. The branch does nothing; it gets a second receipt then |

`UNKNOWN_BRANCH` cannot occur while the consumer reads only branches from the registry and checks `BRANCH_MISMATCH` first. It stays as a safety check.

Failures that are not contract rejections, such as the HQ database being unreachable, are not rejections. The consumer retries the record until it succeeds, and sends no receipt until then.

Messages that pass all layers are applied by revision. The key is `saleDate` of a summary, `returnDate` of a return and `(businessDate, terminalId, shiftNo)` of a shift close. These are normal outcomes, not errors:

| Stored revision for `(type, branchCode, key)` | Incoming revision | Action | Receipt `outcome` |
|---|---|---|---|
| none | any | insert | `INSERTED` |
| `n` | `> n` | replace header and lines | `UPDATED` |
| `n` | `= n` | skip (duplicate delivery) | `DUPLICATE` |
| `n` | `< n` | skip and log (stale) | `STALE` |

## Receipts

HQ writes one receipt to the branch's `branch-sales.receipt` topic for every record (summary, return or shift close) it has finished with: after the HQ database commit, or after storing a rejected record in `dead_letter`. Offsets of the record topics are committed only after the receipt is acknowledged, so every record gets at least one receipt. A record that is read again (for example after an HQ restart) gets another receipt; for a stored record that one says `DUPLICATE`.

| Part | Value |
|---|---|
| Key | `branchCode` |
| Value | UTF-8 JSON matching [`daily-sales-receipt.v1.schema.json`](daily-sales-receipt.v1.schema.json) |

`type` says which topic the record came from (`DAILY_SUMMARY`, `DAILY_RETURN` or `SHIFT_CLOSE`; absent means `DAILY_SUMMARY`, as receipts were written before the return topic existed) and `sourceOffset` is its offset there, so the branch can match a receipt to what it sent by `(type, sourceOffset)` even when the value was not readable. Offsets are per topic: the first summary, the first return and the first shift close of a branch are all offset 0. `saleDate` carries the record's business date (`returnDate` of a return, `businessDate` of a shift close). A shift close receipt also carries `terminalId` and `shiftNo` when the record could be read. `INSERTED`, `UPDATED`, `DUPLICATE` and `STALE` mean HQ holds this revision or a higher one; `REJECTED` means it does not, with `rejectReason` and `detail`.

Examples: [`receipt-examples/`](receipt-examples/)

## Rejected records at HQ

Rejected records are kept in the HQ table `dead_letter`: branch, record type, source offset, the record key and value bytes unchanged, reject reason, detail, time and (when readable) the business date. To process one again after the cause is fixed at HQ (for example a missing category or tender type was added), set `replay_requested_at`; the consumer processes it through all layers again within a minute and sends a new receipt to the branch. A return rejected with `PARENT_MISSING` gets its replay request from HQ itself when the sales of that date are stored.

## Money

Amounts are decimal strings with exactly two decimal places, for example `"18200.00"`. Parse them as `BigDecimal`. JSON numbers are rejected so that no client reads money as floating point.

## Compatibility rules

| Change | Allowed in v1? | How |
|---|---|---|
| Add an optional field | yes | update the schema, add a CHANGELOG entry. Consumers ignore fields they do not know (see [`examples/valid/unknown-field.json`](examples/valid/unknown-field.json)) |
| Add a category code or tender type | yes | see [categories.md](categories.md) and [tender-types.md](tender-types.md). The schema does not change |
| Make an optional field required, remove a field, change a type or meaning | no | publish `daily-sales-summary.v2.schema.json` (or `daily-return.v2...`) with `schemaVersion: 2` |
| Add a record type | yes | a new schema and topic, read by the consumer after it is deployed; branches start sending it when they are upgraded. The receipt's `type` names it |

The same rules apply to `daily-sales-receipt.v1.schema.json`, with the branch producer as the reader.

Deploy order for a new summary schema version: upgrade the HQ consumer to accept both versions first, then upgrade branches one by one. Branches upgrade at different times, so the consumer keeps accepting the old version until no branch sends it.

Deploy order for a new record type (as with `DailyReturn` and `ShiftClose`): the consumer first. At a branch whose `kafka-init` has not created the new topic (or its ACL) yet, the consumer reads the topics it can and checks again at every registry refresh; the branch's sales are not affected. The producer first would be wrong: it would fail to send the new type, and only the new type, until the broker has the topic.

## Examples

| Folder | Expected result |
|---|---|
| [`examples/valid/`](examples/valid/) | accepted |
| [`examples/invalid-schema/`](examples/invalid-schema/) | rejected by JSON Schema |
| [`examples/invalid-business/`](examples/invalid-business/) | valid against the schema, rejected by business or reference checks |
| [`return-examples/valid/`](return-examples/valid/) | a `DailyReturn` accepted when the sales of its date are stored; `PARENT_MISSING` before that |
| [`return-examples/invalid-schema/`](return-examples/invalid-schema/) | a return with `saleDate` instead of `returnDate`: rejected by the return schema |
| [`shift-close-examples/valid/`](shift-close-examples/valid/) | a `ShiftClose` accepted, also without the sales of its date |
| [`shift-close-examples/invalid-schema/`](shift-close-examples/invalid-schema/) | rejected by the shift close schema |
| [`shift-close-examples/invalid-business/`](shift-close-examples/invalid-business/) | valid against the shift close schema, rejected by business or reference checks |

Reference checks in the examples assume each example is read from the cluster of its own `branchCode`, the HQ branch registry contains `BR0001` and `BR0002` but not `BR9999`, the category table matches [categories.md](categories.md) and the tender type table matches [tender-types.md](tender-types.md).
The consumer tests use these files directly.
