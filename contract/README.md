# Message contract

The only thing the branch producer and the HQ consumer share. There is no shared code library: each side has its own classes and they meet at this JSON format.

## Topics

| Topic | Written by | Content |
|---|---|---|
| `branch-sales.daily-summary.<branchCode>` | that branch's producer only | `DailySalesSummary` messages of one branch. One topic per branch, created when the branch is onboarded |
| `branch-sales.daily-summary.dlt` | HQ consumer | messages the consumer could not accept, with the reason in record headers |

Each branch logs in to Kafka with its own user (= branch code), which may write only its own topic. The topic therefore tells HQ which branch sent a record, and HQ checks that the `branchCode` in the message is the same (`BRANCH_MISMATCH`).

## Record format

| Part | Value |
|---|---|
| Key | `branchCode` (UTF-8 string), required, equal to `branchCode` in the value (`KEY_MISMATCH` otherwise). |
| Value | UTF-8 JSON matching [`daily-sales-summary.v1.schema.json`](daily-sales-summary.v1.schema.json) |
| Headers | none required in v1 |

Example: [`examples/valid/basic.json`](examples/valid/basic.json)

## Validation layers

The consumer checks a message in this order. The first failure sends the record to the dead-letter topic; other records in the same batch continue.

| Layer | Rule | Reject reason |
|---|---|---|
| Parse | value is valid JSON | `INVALID_JSON` |
| Schema | matches the JSON Schema, with `format` validation enabled (`uuid`, `date`, `date-time`) | `SCHEMA_INVALID` |
| Identity | `branchCode` equals the branch of the topic (`branch-sales.daily-summary.<branchCode>`) | `BRANCH_MISMATCH` |
| Identity | record key equals `branchCode` | `KEY_MISMATCH` |
| Business | `totalAmount` equals the sum of `lines[].amount` | `TOTAL_MISMATCH` |
| Business | `categoryCode` is unique within `lines` | `DUPLICATE_CATEGORY` |
| Reference | `branchCode` exists in the HQ `branch` table | `UNKNOWN_BRANCH` |
| Reference | every `categoryCode` exists in the HQ `category` table ([categories.md](categories.md)) | `UNKNOWN_CATEGORY` |

### Dead-letter records

A dead-letter record has the same key and the same value bytes as the rejected record. It is partitioned by key. Headers:

| Header | Value |
|---|---|
| `reject-reason` | one of the reject reasons above, UTF-8 |
| `kafka_dlt-exception-message` | reason and detail, e.g. `TOTAL_MISMATCH: totalAmount 48250.00 but lines sum to 30250.00` |
| `kafka_dlt-original-topic` | source topic, UTF-8 |
| `kafka_dlt-original-partition` | source partition, 4-byte big-endian int |
| `kafka_dlt-original-offset` | source offset, 8-byte big-endian long |

The `kafka_dlt-*` headers are written by Spring Kafka's `DeadLetterPublishingRecoverer`; it also adds the original timestamp and the exception class.

Failures that are not contract rejections, such as the HQ database being unreachable, never produce dead letters. The consumer retries the record until it succeeds.

Messages that pass all layers are applied by revision. These are normal outcomes, not errors, and do not go to the dead-letter topic:

| Stored revision for `(branchCode, saleDate)` | Incoming revision | Action |
|---|---|---|
| none | any | insert |
| `n` | `> n` | replace header and lines |
| `n` | `= n` | skip (duplicate delivery) |
| `n` | `< n` | skip and log (stale) |

## Money

Amounts are decimal strings with exactly two decimal places, for example `"18200.00"`. Parse them as `BigDecimal`. JSON numbers are rejected so that no client reads money as floating point.

## Compatibility rules

| Change | Allowed in v1? | How |
|---|---|---|
| Add an optional field | yes | update the schema, add a CHANGELOG entry. Consumers ignore fields they do not know (see [`examples/valid/unknown-field.json`](examples/valid/unknown-field.json)) |
| Add a category code | yes | see [categories.md](categories.md). The schema does not change |
| Make an optional field required, remove a field, change a type or meaning | no | publish `daily-sales-summary.v2.schema.json` with `schemaVersion: 2` |

Deploy order for a new schema version: upgrade the HQ consumer to accept both versions first, then upgrade branches one by one. Branches upgrade at different times, so the consumer keeps accepting the old version until no branch sends it.

## Examples

| Folder | Expected result |
|---|---|
| [`examples/valid/`](examples/valid/) | accepted |
| [`examples/invalid-schema/`](examples/invalid-schema/) | rejected by JSON Schema |
| [`examples/invalid-business/`](examples/invalid-business/) | valid against the schema, rejected by business or reference checks |

Reference checks in the examples assume the HQ branch registry contains `BR0001` and `BR0002` but not `BR9999`, and the category table matches [categories.md](categories.md).
The consumer tests use these files directly.
