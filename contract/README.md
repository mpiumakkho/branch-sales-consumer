# Message contract

The only thing the branch producer and the HQ consumer share. There is no shared code library: each side has its own classes and they meet at this JSON format.

## Topics

Every branch runs its own Kafka broker. HQ connects to each branch's broker to read the branch's records and write receipts. All topics exist in every branch's cluster, with one partition each: one topic per record type, plus the receipts.

| Topic | Written by | Read by | Content |
|---|---|---|---|
| `branch-sales.daily-summary` | the branch producer | HQ consumer | `DailySalesSummary`: the confirmed sales of one day by category |
| `branch-sales.daily-return` | the branch producer | HQ consumer | `DailyReturn`: the confirmed returns and voids of one day by category. HQ stores it only after the `DailySalesSummary` of the same date |
| `branch-sales.receipt` | HQ consumer | the branch producer | `DailySalesReceipt`: what HQ did with each record of either topic |

The cluster a record is read from tells HQ which branch sent it: HQ connects to a branch through the address registered for that branch, and checks that the `branchCode` in the message is the same (`BRANCH_MISMATCH`).

## Record format

| Part | Value |
|---|---|
| Key | `branchCode` (UTF-8 string), required, equal to `branchCode` in the value (`KEY_MISMATCH` otherwise). |
| Value | UTF-8 JSON matching the schema of the topic: [`daily-sales-summary.v1.schema.json`](daily-sales-summary.v1.schema.json) or [`daily-return.v1.schema.json`](daily-return.v1.schema.json). The two have the same shape; a summary has `saleDate`, a return has `returnDate` |
| Headers | none required in v1 |

Examples: [`examples/valid/basic.json`](examples/valid/basic.json), [`return-examples/valid/basic.json`](return-examples/valid/basic.json)

## Validation layers

The consumer checks a message in this order. The first failure rejects the record: it is stored in the HQ `dead_letter` table and a `REJECTED` receipt goes back to the branch. Other records in the same batch continue.

| Layer | Rule | Reject reason |
|---|---|---|
| Parse | value is valid JSON | `INVALID_JSON` |
| Schema | matches the JSON Schema, with `format` validation enabled (`uuid`, `date`, `date-time`); also rejects a value the schema accepts but HQ cannot store (`revision` beyond a 32-bit integer, `quantity` beyond a 64-bit integer) | `SCHEMA_INVALID` |
| Identity | `branchCode` equals the branch whose cluster the record was read from | `BRANCH_MISMATCH` |
| Identity | record key equals `branchCode` | `KEY_MISMATCH` |
| Business | `totalAmount` equals the sum of `lines[].amount` | `TOTAL_MISMATCH` |
| Business | `categoryCode` is unique within `lines` | `DUPLICATE_CATEGORY` |
| Reference | `branchCode` exists in the HQ `branch` table | `UNKNOWN_BRANCH` |
| Reference | every `categoryCode` exists in the HQ `category` table ([categories.md](categories.md)) | `UNKNOWN_CATEGORY` |
| Reference (`DailyReturn` only) | HQ holds the branch's `DailySalesSummary` of `returnDate` | `PARENT_MISSING`: kept in `dead_letter` like any rejection, but replayed by HQ itself as soon as that summary is stored. The branch does nothing; it gets a second receipt then |

`UNKNOWN_BRANCH` cannot occur while the consumer reads only branches from the registry and checks `BRANCH_MISMATCH` first. It stays as a safety check.

Failures that are not contract rejections, such as the HQ database being unreachable, are not rejections. The consumer retries the record until it succeeds, and sends no receipt until then.

Messages that pass all layers are applied by revision. These are normal outcomes, not errors:

| Stored revision for `(type, branchCode, date)` | Incoming revision | Action | Receipt `outcome` |
|---|---|---|---|
| none | any | insert | `INSERTED` |
| `n` | `> n` | replace header and lines | `UPDATED` |
| `n` | `= n` | skip (duplicate delivery) | `DUPLICATE` |
| `n` | `< n` | skip and log (stale) | `STALE` |

## Receipts

HQ writes one receipt to the branch's `branch-sales.receipt` topic for every record (summary or return) it has finished with: after the HQ database commit, or after storing a rejected record in `dead_letter`. Offsets of the record topics are committed only after the receipt is acknowledged, so every record gets at least one receipt. A record that is read again (for example after an HQ restart) gets another receipt; for a stored record that one says `DUPLICATE`.

| Part | Value |
|---|---|
| Key | `branchCode` |
| Value | UTF-8 JSON matching [`daily-sales-receipt.v1.schema.json`](daily-sales-receipt.v1.schema.json) |

`type` says which topic the record came from (`DAILY_SUMMARY` or `DAILY_RETURN`; absent means `DAILY_SUMMARY`, as receipts were written before the return topic existed) and `sourceOffset` is its offset there, so the branch can match a receipt to what it sent by `(type, sourceOffset)` even when the value was not readable. Offsets are per topic: the first summary and the first return of a branch are both offset 0. `saleDate` carries the record's business date (`returnDate` of a return). `INSERTED`, `UPDATED`, `DUPLICATE` and `STALE` mean HQ holds this revision or a higher one; `REJECTED` means it does not, with `rejectReason` and `detail`.

Examples: [`receipt-examples/`](receipt-examples/)

## Rejected records at HQ

Rejected records are kept in the HQ table `dead_letter`: branch, record type, source offset, the record key and value bytes unchanged, reject reason, detail, time and (when readable) the business date. To process one again after the cause is fixed at HQ (for example a missing category was added), set `replay_requested_at`; the consumer processes it through all layers again within a minute and sends a new receipt to the branch. A return rejected with `PARENT_MISSING` gets its replay request from HQ itself when the sales of that date are stored.

## Money

Amounts are decimal strings with exactly two decimal places, for example `"18200.00"`. Parse them as `BigDecimal`. JSON numbers are rejected so that no client reads money as floating point.

## Compatibility rules

| Change | Allowed in v1? | How |
|---|---|---|
| Add an optional field | yes | update the schema, add a CHANGELOG entry. Consumers ignore fields they do not know (see [`examples/valid/unknown-field.json`](examples/valid/unknown-field.json)) |
| Add a category code | yes | see [categories.md](categories.md). The schema does not change |
| Make an optional field required, remove a field, change a type or meaning | no | publish `daily-sales-summary.v2.schema.json` (or `daily-return.v2...`) with `schemaVersion: 2` |
| Add a record type | yes | a new schema and topic, read by the consumer after it is deployed; branches start sending it when they are upgraded. The receipt's `type` names it |

The same rules apply to `daily-sales-receipt.v1.schema.json`, with the branch producer as the reader.

Deploy order for a new summary schema version: upgrade the HQ consumer to accept both versions first, then upgrade branches one by one. Branches upgrade at different times, so the consumer keeps accepting the old version until no branch sends it.

## Examples

| Folder | Expected result |
|---|---|
| [`examples/valid/`](examples/valid/) | accepted |
| [`examples/invalid-schema/`](examples/invalid-schema/) | rejected by JSON Schema |
| [`examples/invalid-business/`](examples/invalid-business/) | valid against the schema, rejected by business or reference checks |
| [`return-examples/valid/`](return-examples/valid/) | a `DailyReturn` accepted when the sales of its date are stored; `PARENT_MISSING` before that |
| [`return-examples/invalid-schema/`](return-examples/invalid-schema/) | a return with `saleDate` instead of `returnDate`: rejected by the return schema |

Reference checks in the examples assume each example is read from the cluster of its own `branchCode`, the HQ branch registry contains `BR0001` and `BR0002` but not `BR9999`, and the category table matches [categories.md](categories.md).
The consumer tests use these files directly.
