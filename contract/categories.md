# Standard product categories

HQ owns this list. It is the source for the HQ `category` table.

Branch back-office systems may use their own category codes. Each branch producer maps its local codes to the codes below through its own configuration.

| Code | Description |
|---|---|
| `BEVERAGE` | Drinks: soft drinks, water, coffee, tea, juice |
| `SNACK` | Packaged snacks, confectionery |
| `READY_MEAL` | Ready-to-eat and heat-and-eat meals |
| `FRESH_FOOD` | Bakery, fruit, dairy and other short-shelf-life food |
| `HOUSEHOLD` | Cleaning supplies and household goods |
| `PERSONAL_CARE` | Toiletries and personal care products |
| `OTHER` | Anything not covered above |

## Changing the list

- Adding a code: add it to the HQ `category` table and this file first, then let branches map to it. The JSON Schema does not change, because `categoryCode` is validated by pattern, not by enum.
- Removing or renaming a code is a breaking change for branches still sending it. Keep the old code accepted until no branch sends it.
- A message with a code that is not in the HQ table is rejected with reason `UNKNOWN_CATEGORY`: stored in the HQ `dead_letter` table, with a `REJECTED` receipt to the branch.
