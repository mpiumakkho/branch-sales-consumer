# Standard tender types

HQ owns this list. It is the source for the HQ `tender_type` table.

POS and back-office systems may use their own tender codes. Each branch producer maps its local codes to the codes below through its own configuration (`branch-sales.tender-mapping`).

| Code | Description |
|---|---|
| `CASH` | Cash |
| `CREDIT_CARD` | Credit card |
| `DEBIT_CARD` | Debit card |
| `QR_PAYMENT` | QR payment |
| `E_WALLET` | E-wallet |
| `COUPON` | Coupon or voucher |
| `OTHER` | Other tender |

## Changing the list

- Adding a code: add it to the HQ `tender_type` table and this file first, then let branches map to it. The JSON Schema does not change, because `tenderType` is validated by pattern, not by enum.
- Removing or renaming a code is a breaking change for branches still sending it. Keep the old code accepted until no branch sends it.
- A message with a code that is not in the HQ table is rejected with reason `UNKNOWN_TENDER`: stored in the HQ `dead_letter` table, with a `REJECTED` receipt to the branch.
