# Digital Twin and Spatial Exception Engine

## 1. Contract

`POST /v1/events/inventory-discrepancy` accepts an AMR, RF, WMS, API, or IoT observation. `DiscrepancyEventIn` requires exactly one location identifier, exactly one SKU identifier, at most one lot identifier, non-negative expected/found quantities, an aware UTC `observed_at`, and a unique `source_event_id`. The handler is idempotent on `(tenant_id, source, source_event_id)` and on `X-Idempotency-Key`.

Example "Expected 10 units of Tenant A's SKU at Location X, found 0" request:

```json
{
  "source": "AMR",
  "source_event_id": "amr-2026-11-10-000001",
  "warehouse_id": "11111111-1111-4111-8111-111111111111",
  "location_barcode": "DXB-A01-B01-L1-P1",
  "sku_code": "TENANT-A-SKU-1",
  "lot_code": "LOT-2026-11-A",
  "expected_qty": "10",
  "found_qty": "0",
  "wave_id": "22222222-2222-4222-8222-222222222222",
  "observed_at": "2026-11-10T08:15:00Z"
}
```

## 2. Deterministic state machine

| Step | SQL / function | Committed effect |
|---|---|---|
| 1 | `catalog.validate_api_client`, `resolve_location`, `resolve_sku_by_id`, `resolve_lot_by_id_or_code` | authenticated tenant-scoped coordinate/SKU/lot |
| 2 | `INSERT_DISCREPANCY_EVENT` | `inventory_discrepancy_events.status = processing` |
| 3 | `LOCK_LOCATION_FOR_QUARANTINE`, `UPDATE_LOCATION_QUARANTINE` | `locations.status = quarantine`, reason, timestamp, version increment |
| 4 | `RELEASE_ALLOCATIONS_AT_LOCATION`, `UPDATE_ALLOCATION_RELEASE`, `UPDATE_ORDER_LINE_RELEASE`, `CANCEL_TASKS_BY_ALLOCATION`, `CANCEL_TASKS_FOR_REF` | reserved allocations released, line allocation decremented, affected tasks cancelled |
| 5 | `LOCK_DISCREPANCY_BUCKETS`, `UPSERT_QUARANTINE_BALANCE`, `DELETE_BALANCE_ROW`, `INSERT_LEDGER` | positive physical balances moved to `quarantine`, double-entry history |
| 6 | `GET_WAVE_UNFULFILLED_LINES`, `GET_LINES_BY_IDS`, `get_wave_route` | unfulfilled wave lines and surviving preferred route locations |
| 7 | `AllocationRepository.allocate_line` with exact reported lot, `require_same_lot=true`, excluded quarantined coordinate | replacement allocations at alternative identical SKU/lot stock |
| 8 | `wave_repo.rebuild_wave_route` | quarantined stops removed, new stops merged, deterministic sequence |
| 9 | `INSERT_CYCLE_COUNT`, `TaskDispatcher.enqueue_task` | priority-1000 system-directed `CYCLE_COUNT` task |
| 10 | `UPDATE_DISCREPANCY_PROCESSED`, `emit_event` | `processed` result document, `location.quarantined`, `discrepancy.processed` |

Every step runs in one tenant transaction. If any version check fails, the transaction aborts and no partial quarantine/reallocation is committed.

## 3. Quarantine semantics

`LOCK_LOCATION_FOR_QUARANTINE` takes `FOR UPDATE` on the location row. `UPDATE_LOCATION_QUARANTINE` requires the exact `version` read under that lock and either a status change or `force=true`. The engine passes `force=true` so repeated observations with a new `source_event_id` still preserve a consistent quarantine state and increment the version.

`LOCK_DISCREPANCY_BUCKETS` selects positive `available`, `allocated`, `damaged`, and `qc_hold` balances for the tenant, coordinate, SKU, and reported lot. Reserved `allocated` balances are released before this step, so stock is not double-counted. Each balance row is deleted under its version and reinserted under `quarantine`; two ledger rows record the debit and credit.

## 4. Reroute semantics

`GET_WAVE_UNFULFILLED_LINES` returns only lines where `required_qty > picked_qty + allocated_qty`. Lines already fulfilled elsewhere remain untouched. Released lines outside the supplied wave are added from `GET_LINES_BY_IDS`. Preferred alternative locations come from the wave route after excluding the quarantined coordinate.

For each line, the engine sets `effective_lot = line.lot_id or reported_lot`, replaces the line requirement with that exact lot and `require_same_lot=true`, passes `exclude_location_ids=[quarantined_location_id]`, and calls `allocate_line` inside its savepoint. This guarantees replacement stock has the identical SKU and lot. If no such stock is sufficient, the line is marked `backordered`, a `REPLENISHMENT` task is created, and a shortage document is recorded.

## 5. Cycle count

`INSERT_CYCLE_COUNT` creates an `open` cycle count linked to the discrepancy event, warehouse, location, SKU, and lot. `TaskDispatcher.enqueue_task` creates a `CYCLE_COUNT` task with priority `exceptions.cycle_count_priority = 1000`, equipment `RF_SCANNER` and `AMR`, `required_zone` equal to the quarantined zone, and payload containing expected/found quantities and correlation ID. `GET_OPEN_TASK_BY_REF` resolves an existing task on idempotent task conflict.

## 6. Response

`DiscrepancyResponse` contains:

| Field | Meaning |
|---|---|
| `discrepancy_event_id` | persisted source-event aggregate ID |
| `location_id` | quarantined coordinate |
| `location_status` | `quarantine` |
| `quarantined_buckets` | bucket IDs whose physical balance moved to quarantine |
| `released_allocations` | cancelled reservation IDs |
| `reallocated_lines` | lines successfully rerouted to identical SKU/lot stock |
| `backordered_lines` | lines with no sufficient alternative stock |
| `cycle_count_task_id` | high-priority system-directed count task |
| `processing_result` | expected/found quantities, shortages, task IDs, affected wave IDs, correlation ID |

`processing_result` is stored in `inventory_discrepancy_events.processing_result` and returned on exact retries. A different payload with the same idempotency key is rejected by `IdempotencyGuard`.

## 7. Outbox events

| Event type | Payload |
|---|---|
| `location.quarantined` | discrepancy ID, source, expected/found quantities, SKU ID, lot ID |
| `discrepancy.processed` | full deterministic processing result |
| `task.created` | cycle-count, reallocation-pick, and replenishment tasks |
| `inventory.allocated` | replacement allocations produced by `allocate_line` |

All events are inserted in the same transaction as the state changes; `OutboxRelay` publishes them after commit.
