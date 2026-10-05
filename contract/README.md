# Message contract

The only thing the branch producer and the HQ consumer share. There is no shared code library: each side has its own classes and they meet at this JSON format.

## Topics

Every branch runs its own Kafka broker. HQ connects to each branch's broker to read summaries and write receipts. Both topics exist in every branch's cluster, with one partition each.

| Topic | Written by | Read by | Content |
|---|---|---|---|
| `branch-sales.daily-summary` | the branch producer | HQ consumer | `DailySalesSummary` messages of that branch |
| `branch-sales.receipt` | HQ consumer | the branch producer | `DailySalesReceipt`: what HQ did with each summary record |

The cluster a record is read from tells HQ which branch sent it: HQ connects to a branch through the address registered for that branch, and checks that the `branchCode` in the message is the same (`BRANCH_MISMATCH`).

## Record format

| Part | Value |
|---|---|
| Key | `branchCode` (UTF-8 string), required, equal to `branchCode` in the value (`KEY_MISMATCH` otherwise). |
| Value | UTF-8 JSON matching [`daily-sales-summary.v1.schema.json`](daily-sales-summary.v1.schema.json) |
| Headers | none required in v1 |

Example: [`examples/valid/basic.json`](examples/valid/basic.json)

## Validation layers

The consumer checks a message in this order. The first failure rejects the record: it is stored in the HQ `dead_letter` table and a `REJECTED` receipt goes back to the branch. Other records in the same batch continue.

| Layer | Rule | Reject reason |
|---|---|---|
| Parse | value is valid JSON | `INVALID_JSON` |
| Schema | matches the JSON Schema, with `format` validation enabled (`uuid`, `date`, `date-time`) | `SCHEMA_INVALID` |
| Identity | `branchCode` equals the branch whose cluster the record was read from | `BRANCH_MISMATCH` |
| Identity | record key equals `branchCode` | `KEY_MISMATCH` |
| Business | `totalAmount` equals the sum of `lines[].amount` | `TOTAL_MISMATCH` |
| Business | `categoryCode` is unique within `lines` | `DUPLICATE_CATEGORY` |
| Reference | `branchCode` exists in the HQ `branch` table | `UNKNOWN_BRANCH` |
| Reference | every `categoryCode` exists in the HQ `category` table ([categories.md](categories.md)) | `UNKNOWN_CATEGORY` |

`UNKNOWN_BRANCH` cannot occur while the consumer reads only branches from the registry and checks `BRANCH_MISMATCH` first. It stays as a safety check.

Failures that are not contract rejections, such as the HQ database being unreachable, are not rejections. The consumer retries the record until it succeeds, and sends no receipt until then.

Messages that pass all layers are applied by revision. These are normal outcomes, not errors:

| Stored revision for `(branchCode, saleDate)` | Incoming revision | Action | Receipt `outcome` |
|---|---|---|---|
| none | any | insert | `INSERTED` |
| `n` | `> n` | replace header and lines | `UPDATED` |
| `n` | `= n` | skip (duplicate delivery) | `DUPLICATE` |
| `n` | `< n` | skip and log (stale) | `STALE` |

## Receipts

HQ writes one receipt to the branch's `branch-sales.receipt` topic for every summary record it has finished with: after the HQ database commit, or after storing a rejected record in `dead_letter`. Offsets of the summary topic are committed only after the receipt is acknowledged, so every summary record gets at least one receipt. A record that is read again (for example after an HQ restart) gets another receipt; for a stored record that one says `DUPLICATE`.

| Part | Value |
|---|---|
| Key | `branchCode` |
| Value | UTF-8 JSON matching [`daily-sales-receipt.v1.schema.json`](daily-sales-receipt.v1.schema.json) |

`sourceOffset` is the offset of the summary record, so the branch can match a receipt to what it sent even when the value was not readable. `INSERTED`, `UPDATED`, `DUPLICATE` and `STALE` mean HQ holds this revision or a higher one; `REJECTED` means it does not, with `rejectReason` and `detail`.

Examples: [`receipt-examples/`](receipt-examples/)

## Rejected records at HQ

Rejected records are kept in the HQ table `dead_letter`: branch, source offset, the record key and value bytes unchanged, reject reason, detail and time. To process one again after the cause is fixed at HQ (for example a missing category was added), set `replay_requested_at`; the consumer processes it through all layers again within a minute and sends a new receipt to the branch.

## Money

Amounts are decimal strings with exactly two decimal places, for example `"18200.00"`. Parse them as `BigDecimal`. JSON numbers are rejected so that no client reads money as floating point.

## Compatibility rules

| Change | Allowed in v1? | How |
|---|---|---|
| Add an optional field | yes | update the schema, add a CHANGELOG entry. Consumers ignore fields they do not know (see [`examples/valid/unknown-field.json`](examples/valid/unknown-field.json)) |
| Add a category code | yes | see [categories.md](categories.md). The schema does not change |
| Make an optional field required, remove a field, change a type or meaning | no | publish `daily-sales-summary.v2.schema.json` with `schemaVersion: 2` |

The same rules apply to `daily-sales-receipt.v1.schema.json`, with the branch producer as the reader.

Deploy order for a new summary schema version: upgrade the HQ consumer to accept both versions first, then upgrade branches one by one. Branches upgrade at different times, so the consumer keeps accepting the old version until no branch sends it.

## Examples

| Folder | Expected result |
|---|---|
| [`examples/valid/`](examples/valid/) | accepted |
| [`examples/invalid-schema/`](examples/invalid-schema/) | rejected by JSON Schema |
| [`examples/invalid-business/`](examples/invalid-business/) | valid against the schema, rejected by business or reference checks |

Reference checks in the examples assume each example is read from the cluster of its own `branchCode`, the HQ branch registry contains `BR0001` and `BR0002` but not `BR9999`, and the category table matches [categories.md](categories.md).
The consumer tests use these files directly.
