# Data Architecture and Concurrency Contract

## 1. Schema foundation

`sql/001_schema.sql` creates schema `wms`, enables `pgcrypto` and `btree_gist`, defines a `wms_migration` BYPASSRLS role for DDL and outbox relay, and a `wms_app` role subject to forced RLS. Tenant context is read by `app_current_tenant_id()` from `app.current_tenant_id`.

The DDL contains 36 base tables, 54 explicit `CREATE INDEX` statements, 14 enum types, status/time range partitions for `inventory_ledger`, `billing_events`, and `event_outbox`, and forced RLS policies on tenant-owned tables and partitions.

| Enum | Values |
|---|---|
| `order_mode` | `B2C`, `B2B`, `HYBRID` |
| `order_status` | `received`, `routing`, `allocated`, `picking`, `packed`, `shipped`, `cancelled`, `exception` |
| `line_status` | `open`, `allocated`, `picking`, `picked`, `packed`, `shipped`, `backordered`, `cancelled` |
| `stock_status` | `available`, `allocated`, `damaged`, `qc_hold`, `quarantine`, `in_transit`, `consumed` |
| `location_status` | `active`, `quarantine`, `blocked`, `maintenance`, `disabled` |
| `location_type` | `piece_pick`, `pallet`, `bulk`, `cross_dock`, `staging`, `qc`, `dock`, `returns`, `quarantine` |
| `handling_unit_type` | `EACH`, `CARTON`, `TOTE`, `PALLET`, `CONTAINER` |
| `hu_status` | `available`, `allocated`, `in_transit`, `damaged`, `quarantine`, `consumed` |
| `equipment_type` | `RF_SCANNER`, `CART`, `AMR`, `FORKLIFT`, `REACH_TRUCK`, `PALLET_JACK`, `AGV`, `MANUAL` |
| `task_type` | `PICK_PIECE`, `PICK_CLUSTER`, `PICK_ZONE`, `PICK_MULTI_BATCH`, `PALLET_PICK`, `PALLET_PUTAWAY`, `CROSS_DOCK_MOVE`, `CYCLE_COUNT`, `REPLENISHMENT`, `VAS` |
| `task_state` | `queued`, `assigned`, `accepted`, `in_progress`, `completed`, `cancelled`, `failed`, `blocked` |
| `wave_status` | `planned`, `released`, `picking`, `packed`, `closed`, `cancelled` |
| `picking_strategy` | `WAVE`, `CLUSTER`, `ZONE`, `MULTI_BATCH` |
| `storage_basis` | `PALLET_POSITION`, `CUBIC_FOOT`, `MAX_OF_BOTH`, `TIERED` |

## 2. Table groups

| Group | Tables | Purpose |
|---|---|---|
| Identity and access | `tenants`, `warehouses`, `tenant_warehouse_access`, `tenant_api_clients`, `workflow_definitions` | tenant registry, shared warehouse grants, client API segmentation, versioned custom workflows |
| Master data | `skus`, `lots`, `locations`, `location_tenant_policies` | dimensions/weight/velocity/hazmat metadata, FEFO lots, 3D locations, shared-slot policies |
| Inventory | `handling_units`, `inventory_bucket_headers`, `inventory_balances`, `inventory_ledger` | physical HUs, lockable natural buckets, status-keyed balances, append-only double-entry history |
| Orders and waves | `orders`, `order_lines`, `waves`, `wave_orders`, `wave_order_lines`, `wave_route_stops`, `inventory_allocations` | order intake, affinity waves, deterministic routes, reservations |
| Labour and tasks | `workers`, `worker_zone_permissions`, `task_queue`, `task_assignments` | operational state, zone permissions, priority queue, assignment audit |
| Exceptions | `inventory_discrepancy_events`, `cycle_counts` | source-event idempotency, processing result, high-priority counts |
| Billing | `tenant_contracts`, `contract_storage_rates`, `contract_pick_rates`, `vas_rate_cards`, `vas_transactions`, `billing_events`, `billing_event_keys`, `billing_invoice_lines` | date-effective contracts, tiered rates, VAS transactions, reproducible charge ledger |
| Platform | `event_outbox`, `idempotency_keys` | transactional publication and exactly-once API effects |

## 3. Required 3PL inventory fields

### `skus`

`tenant_id`, `sku_code`, `description`, `uom`, `length_cm`, `width_cm`, `height_cm`, generated `unit_volume_cm3`, `unit_weight_kg`, `units_per_carton`, `units_per_pallet`, `pallet_tie`, `pallet_high`, `hazardous`, `hazmat_class`, `temperature_band`, `velocity_class`, `velocity_score`, `avg_daily_picks`, `avg_daily_units`, `velocity_calculated_at`, `requires_lot`, `requires_serial`, `shelf_life_days`, `status`, `metadata`, and `version`.

Indexes: `ix_skus_slotting (tenant_id, status, velocity_class, velocity_score DESC, avg_daily_picks DESC)` and `ix_skus_hazmat (tenant_id, hazardous, temperature_band, status)`.

### `locations`

`warehouse_id`, `zone`, `aisle`, `bay`, `level`, `position`, `barcode`, `location_type`, `status`, `exclusive_tenant_id`, `x_m`, `y_m`, `z_m`, `travel_seq`, `max_volume_cm3`, `max_weight_kg`, `current_volume_cm3`, `current_weight_kg`, generated `available_volume_cm3`, generated `available_weight_kg`, `pick_face`, mixed SKU/lot/tenant flags, `allows_hazmat`, `temperature_band`, `slot_class`, `quarantine_reason`, `quarantined_at`, and `version`.

Constraints: unique warehouse coordinate, unique barcode, non-negative occupancy, `current_* <= max_*`, and `(status='quarantine') = (quarantined_at IS NOT NULL)`.

Indexes: `ix_locations_routing`, `ix_locations_capacity`, `ix_locations_tenant` partial, and `ix_locations_quarantine` partial.

### `inventory_bucket_headers` and `inventory_balances`

A bucket is the lockable natural stock identity:

```text
(tenant_id, sku_id, warehouse_id, location_id, lot_id, serial_number,
 expiration_date, handling_unit_id)
```

`UNIQUE NULLS NOT DISTINCT` makes null lot/serial/expiry/HU values deterministic. `inventory_balances` is status-keyed with `UNIQUE (tenant_id, bucket_id, stock_status)` and `CHECK (qty >= 0)`. `ix_balances_available` is a partial index on positive available balances; `ix_balances_nonconsumed` covers physical statuses.

`inventory_available` is a `security_invoker=true, security_barrier=true` view that joins balances, headers, active locations, and active SKUs. It is the only read model used by B2C candidate planning.

## 4. Functions, triggers, and generated state

- `location_travel_cost` is an immutable SQL function used by allocation, full-pallet, and putaway ordering.
- `location_slot_compatibility` is a `SECURITY DEFINER` plpgsql function with `search_path = wms, pg_temp`. It rejects inactive locations, exclusive slots owned by another tenant, mixed-SKU violations, mixed-tenant violations, and lot mismatches.
- `set_updated_at` maintains `updated_at` on every mutable table.
- `refresh_location_occupancy` and `touch_location_occupancy` maintain `current_volume_cm3` and `current_weight_kg` from HUs, bucket headers, and balances.
- `apply_tenant_rls` enables and forces RLS, then creates `p_tenant_isolation` with matching `USING` and `WITH CHECK` predicates.

## 5. Partitioning

`inventory_ledger`, `billing_events`, and `event_outbox` are `PARTITION BY RANGE (occurred_at)`. The migration creates a default partition and twelve monthly 2026 partitions for each parent. Every partition receives the same tenant isolation policy as its parent. This keeps append-heavy audit, billing, and event tables maintainable without changing the application SQL.

`idempotency_keys` is intentionally non-partitioned with a partial index on incomplete keys and `expires_at`, because it is a small, high-churn, correctness-critical table.

## 6. Row-level security

`app_current_tenant_id()` reads `current_setting('app.current_tenant_id', true)`. `Database.tenant_transaction` sets it with transaction-local `set_config`, so a pooled connection cannot leak tenant context into another request.

`p_tenant_isolation` on tenant-owned tables:

```sql
USING (tenant_id = wms.app_current_tenant_id())
WITH CHECK (tenant_id = wms.app_current_tenant_id())
```

`p_locations_access` permits shared locations only when `tenant_warehouse_access.can_read` is true, and permits writes only when `can_write` is true. `p_warehouses_access` restricts the shared warehouse registry. `p_tenants_self` restricts a tenant to its own row. The client-facing API segmentation is therefore enforced at gateway, application, and database layers.

## 7. Allocation concurrency contract

`services/orchestrator/sql.py::LOCK_ALLOCATION_CANDIDATE` locks one candidate at a time:

```sql
WITH eligible_lots AS (... HAVING SUM(b.qty) >= %(remaining_qty)s::numeric),
     chosen_lot AS (... LIMIT 1),
     locked AS (
         SELECT ...
         FROM inventory_balances b
         JOIN inventory_bucket_headers h ...
         JOIN locations l ...
         WHERE ...
         ORDER BY preferred location, FEFO, travel, pick face, bucket id
         FOR UPDATE OF h, b, l SKIP LOCKED
         LIMIT 1
     )
SELECT * FROM locked;
```

`lot_filter_mode` values are:

| Mode | Meaning |
|---|---|
| `IGNORE` | any compatible lot |
| `EXACT` | `lot_id IS NOT DISTINCT FROM lot_filter` |
| `NULL` | only null-lot stock |
| `SINGLE_LOT` | one deterministic lot with summed available quantity sufficient for the remaining line quantity |

Version-checked statements:

```sql
UPDATE inventory_balances
   SET qty = qty - %(qty)s, version = version + 1
 WHERE tenant_id = %(tenant_id)s AND id = %(balance_id)s
   AND version = %(balance_version)s AND qty >= %(qty)s
RETURNING qty, version;
```

```sql
UPDATE order_lines
   SET allocated_qty = allocated_qty + %(qty)s, version = version + 1
 WHERE tenant_id = %(tenant_id)s AND id = %(order_line_id)s
   AND version = %(line_version)s
   AND allocated_qty + %(qty)s <= required_qty - picked_qty
RETURNING allocated_qty, version, status;
```

`AllocationRepository.allocate_line` wraps the line in `async with conn.transaction()` (a SAVEPOINT). All partial state is rolled back if the line cannot be fully allocated. The same pattern is used by `B2BRepository.reserve_full_pallets`.

## 8. Other concurrency controls

| Operation | Control |
|---|---|
| Task queue claim | `ORDER BY priority DESC, not_before, enqueued_at, id ... FOR UPDATE SKIP LOCKED` |
| Worker selection | `CROSS JOIN LATERAL` active-task count, heartbeat/equipment/zone filters, `FOR UPDATE OF w SKIP LOCKED` |
| Task assignment | `UPDATE ... WHERE version = %(version)s` plus `task_assignments` insert |
| Quarantine | `SELECT ... FOR UPDATE` on `locations`, then version-checked status update with force flag |
| Discrepancy buckets | `FOR UPDATE OF h, b` on bucket headers and balances before status transition |
| Billing | `REPEATABLE READ`, `billing_event_keys` unique key, `INSERT ... ON CONFLICT DO NOTHING RETURNING`, conflict-path cleanup |
| Outbox relay | BYPASSRLS system pool, `FOR UPDATE SKIP LOCKED`, idempotent Kafka producer, dead-letter state |

## 9. State lifecycle

| State transition | Tables | Ledger |
|---|---|---|
| Receipt / directed putaway | `handling_units`, `inventory_bucket_headers`, `inventory_balances(in_transit)` | one receipt row |
| Putaway completion | `inventory_balances(in_transit) -> available`, HU and bucket location update | two `pallet_move` rows |
| Allocation | `available -> allocated`, `inventory_allocations`, `order_lines.allocated_qty` | two `allocation` rows |
| Discrepancy | reserved allocations released, physical balances `-> quarantine`, location `-> quarantine` | two rows per quarantined balance |
| Reallocation | alternative location `available -> allocated`, new allocation row | two `allocation` rows |
| Billing | `vas_transactions`, `billing_events`, `billing_event_keys` | `billing_events.components` and `rate_snapshot` |

Every state change is accompanied by an outbox event in the same transaction, so downstream consumers see committed state only.
