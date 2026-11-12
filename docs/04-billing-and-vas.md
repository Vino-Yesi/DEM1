# 3PL Billing and Value-Added Services Engine

## 1. Contract model

`tenant_contracts` is date-effective, tenant-owned, and currency-aware. Its `EXCLUDE USING gist` constraint prevents overlapping active contracts. `storage_basis` selects `PALLET_POSITION`, `CUBIC_FOOT`, `MAX_OF_BOTH`, or `TIERED`. `rounding_mode` is `HALF_UP`, `HALF_EVEN`, `CEILING`, or `FLOOR`. `tax_mode` is `EXCLUSIVE`, `INCLUSIVE`, or `EXEMPT`; the service emits tax-exclusive net amounts and leaves jurisdiction-specific tax to an external engine.

Rate tables are also date-effective and tenant-owned:

| Table | Key columns | Lookup |
|---|---|---|
| `contract_storage_rates` | contract, basis, optional location type, effective dates, pallet/cubic rates, multipliers, minimum, tiers | `GET_STORAGE_RATE` |
| `contract_pick_rates` | contract, `order_mode`, effective dates, piece/pallet/order/line rates, surcharges, minimum, tiers | `GET_PICK_RATE` |
| `vas_rate_cards` | contract, VAS code, effective dates, unit basis, rate, setup, materials, minimum, complexity, tiers | `GET_VAS_RATE` |

Representative active contract:

```json
{
  "tenant_id": "11111111-1111-4111-8111-111111111111",
  "currency": "AED",
  "storage_basis": "PALLET_POSITION",
  "storage_free_days": 2,
  "pallet_position_volume_cm3": "1800000",
  "cm3_per_cubic_foot": "28316.846592",
  "rounding_mode": "HALF_UP",
  "tax_mode": "EXCLUSIVE",
  "status": "active",
  "valid_from": "2026-01-01",
  "valid_to": null
}
```

Representative VAS rate card:

```json
{
  "vas_code": "GIFT_WRAP",
  "unit_basis": "EACH",
  "rate_per_unit": "2.000000",
  "setup_fee": "5.0000",
  "material_fee_per_unit": "0.250000",
  "minimum_charge": "10.0000",
  "complexity_multiplier": "1.1000",
  "tiers": [{"up_to": "100", "rate": "1.750000"}],
  "currency": "AED",
  "effective_from": "2026-01-01",
  "effective_to": null
}
```

## 2. Money and tiers

`currency_exponent` returns 0 for JPY, KRW, VND, CLP, ISK; 3 for BHD, KWD, OMR, TND, JOD; and 2 otherwise. `round_money` quantizes with the contract rounding mode. `tiered_unit_rate` sorts tiers by `up_to` with null last and selects the first tier whose bound is null or greater than/equal to the billable quantity.

`_finalize` contract:

- `gross` is the pre-discount amount and already includes surcharges;
- `discount` is subtracted exactly once;
- `tax` is added;
- `net = max(0, round(gross - discount + tax))`;
- `surcharge_amount` is informational and stored with components;
- `components` and `rate_snapshot` are persisted with the billing event.

This avoids the double-discount error that occurs when a tiered rate is both used to compute gross and subtracted again as a discount.

## 3. Storage charge

`BillingEngine.rate_storage` loads the active contract and storage rate, then calls `calculate_storage_charge`.

```text
billable_days = max(0, days - storage_free_days)
computed_positions = ceil(volume_cm3 / pallet_position_volume_cm3)
positions = max(computed_positions, supplied_pallet_positions)
cubic_feet = volume_cm3 / cm3_per_cubic_foot

standard_pallet_charge = positions * rate_per_pallet_day * billable_days
standard_cubic_charge = cubic_rate * cubic_feet * billable_days
tiered_cubic_charge = tier_rate * cubic_feet * billable_days
tier_discount = max(0, standard_cubic_charge - tiered_cubic_charge)
```

`PALLET_POSITION` uses standard pallet charge; `CUBIC_FOOT` and `TIERED` use standard cubic charge plus tier discount; `MAX_OF_BOTH` uses the greater of the two standard charges. Hazardous and non-ambient temperature multipliers are multiplied together and applied to the standard base; their cross-term is also recorded as a surcharge component. Minimum-charge adjustment is computed against `gross - discount`.

## 4. Pick charge

`BillingEngine.rate_pick` loads `GET_PICK_RATE` for mode `B2C` or `B2B` and calls `calculate_pick_charge`.

B2C:

```text
piece_charge = standard_rate_per_piece * pieces
discount = max(0, (standard_rate - tier_rate) * pieces)
base = piece_charge + line_fee + order_fee + first_piece_fee
```

B2B:

```text
pallet_charge = rate_per_pallet * pallets
base = pallet_charge + line_fee + order_fee
discount = 0
```

Both modes add hazardous piece surcharge, oversized piece surcharge, same-day percentage surcharge, and cross-dock pallet fee. `gross = base + surcharge`; minimum charge is applied to `gross - discount`; `net = gross - discount`.

## 5. VAS charge

`BillingEngine.rate_vas` persists a `vas_transactions` row idempotently on `(tenant_id, idempotency_key)`, then loads `GET_VAS_RATE` and calls `calculate_vas_charge`.

Supported VAS codes: `KITTING`, `LABELING`, `GIFT_WRAP`, `RELABEL`, `INSPECTION`, `ASSEMBLY`, `SERIAL_CAPTURE`, `CUSTOM`.

Supported unit bases: `EACH`, `ORDER`, `MINUTE`, `PALLET`, `CARTON`. The rate basis selects `quantity`, `order_count`, `minutes`, `pallets`, or `cartons`; zero billable units is rejected.

```text
standard_units_charge = standard_rate_per_unit * units
discount = max(0, (standard_rate - tiered_rate) * units)
base = standard_units_charge + setup_fee + material_fee_per_unit * units
discounted_subtotal = base - discount
complexity_surcharge = discounted_subtotal * (complexity_multiplier - 1)
gross = base + complexity_surcharge
net = gross - discount
```

Complexity is applied after the volume discount. Minimum charge is applied to `gross - discount`. Complexity multipliers below 1 are rejected.

## 6. Persistence and idempotency

`_persist_charge` inserts a partitioned `billing_events` row with quantity, gross, discount, surcharge, tax, net, currency, `rate_snapshot`, `components`, and idempotency key. It then inserts `billing_event_keys`. If the key conflict wins, the newly inserted billing event is deleted, the existing event is loaded from `GET_BILLING_EVENT_BY_KEY`, and that response is returned. On success it emits `billing.rated` and stores the idempotent API response.

`REPEATABLE READ` prevents rate-card or contract changes from producing inconsistent snapshots within one charge. Rate snapshots make historical invoices reproducible after rate changes.

## 7. Validation cases

Unit tests in `services/orchestrator/tests/test_pure_logic.py` assert:

- pallet-position storage of one position for two days at 10 per day equals `20.00`;
- cubic-foot storage with a 0.10-to-0.08 tier produces gross `0.20`, discount `0.04`, net `0.16`;
- B2C pick with standard rate 1.00, tier rate 0.80, and 10 pieces produces gross `10.00`, discount `2.00`, net `8.00`;
- B2B pick of three pallets at 25 plus cross-dock fee 2 per pallet produces gross/net `81.00` and surcharge `6.00`;
- VAS with standard 2.00, tier 1.50, setup 5.00, ten units, and complexity 1.25 produces gross `30.00`, discount `5.00`, surcharge `5.00`, net `25.00`;
- simple gift wrap of three units at 2.00 plus setup 5.00 produces net `11.00`.
