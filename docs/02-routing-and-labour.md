# Hybrid Picking, Affinity Waves, and Labour Orchestration

## 1. Module responsibilities

| Module | Responsibility |
|---|---|
| `routing.py` | `HybridRoutingService`: order intake, mode fork, wave planning, allocation orchestration, task creation, putaway endpoints |
| `affinity.py` | pure deterministic travel, compatibility, stop, route, affinity, wave-choice, cutoff, and workflow-validation functions |
| `allocation.py` | atomic piece allocation with row locks, version checks, ledger, outbox, and savepoint |
| `b2b.py` | full-pallet reservation, directed putaway, cross-dock, pallet-move completion |
| `task_dispatcher.py` | priority queue, worker eligibility, heartbeat, claim, assignment, Redis push |
| `catalog.py`, `order_repo.py`, `wave_repo.py` | tenant-aware master-data resolution, order/line persistence, wave persistence and route rebuild |
| `sql.py` | all audited SQL statements |

## 2. Core immutable data structures

| Structure | Fields |
|---|---|
| `Coordinate` | `x`, `y`, `z`, `zone` |
| `AffinityConfig` | `strategy`, `weights`, `threshold`, `max_cluster_travel_m`, `max_zone_count`, `zone_penalty_m`, `vertical_penalty`, `prefer_pick_face`, `release_window_minutes`, `cutoff_bucket_minutes`, `allowed_location_types` |
| `CandidateStock` | bucket/SKU/lot/serial/expiry/HU identity, warehouse, 3D location, available quantity, balance/header versions, barcode, zone/aisle/bay/level/position, coordinates, travel sequence, location type, pick-face, hazmat/temperature compatibility, SKU velocity and dimensions |
| `LineRequirement` | order/line/SKU identity, lot, required/picked/allocated quantities, same-lot flag, expiry bounds, hazmat/temperature, lot/serial requirements, mode, fulfillment unit, allowed location types, version, units per pallet |
| `PlannedStop` | bucket, location, SKU, lot, quantity, coordinate, zone, travel sequence, location type, expiry, stop sequence |
| `RoutePlan` | dock, ordered `PlannedStop` tuple, total route metres |
| `WaveCandidate` | wave identity, tenant, warehouse, strategy, status, priority, carrier/cutoff/zone, release time, capacities, current counts, current route |
| `OrderWaveMeta` | order identity, warehouse, strategy, timestamp, priority, line/unit counts, SLA, carrier, cutoff bucket |
| `AffinityBreakdown` | score, location/SKU/zone Jaccard values, travel and SLA scores, incremental and merged route metres, zone count |
| `WaveDecision` | selected wave ID, breakdown, create-new flag |

`AffinityConfig` is built by `affinity_config_from_workflow`; weights must sum to `1.0` within `0.0001`. All quantities are `Decimal`; all identifiers are `UUID`.

## 3. Order routing transaction

`HybridRoutingService.route_order` executes one idempotent tenant transaction:

1. `IdempotencyGuard.begin` returns the stored response for exact retries and rejects payload/key conflicts.
2. `catalog.validate_api_client` checks client status, scopes, allowed mode, CIDR allowlist, and rate-limit metadata.
3. `catalog.load_workflow` merges the tenant's active workflow over `DEFAULT_WORKFLOW`.
4. `order_repo.create_order` inserts `orders` and `order_lines`, resolves SKUs/lots, and enforces B2B pallet multiples.
5. `B2C`, `B2B`, or both branches run. HYBRID executes the B2C branch and the B2B branch in the same transaction.
6. Order status becomes `allocated` or `exception`; `order.routed` is emitted; the idempotent response is stored.

## 4. B2C affinity algorithm

### 4.1 Candidate read

`catalog.get_b2c_candidate_rows` reads `inventory_available` with `lot_filter_mode` `EXACT` when an order line specifies a lot and `IGNORE` otherwise. Expiry bounds, allowed location types, active location, active SKU, hazmat, temperature, and lot/serial requirements are enforced by `candidate_is_compatible`.

### 4.2 Lot-aware stop planning

`plan_line_stops`:

- uses `line.remaining_qty`, not `required_qty`, so partially allocated/released lines are planned correctly;
- sorts compatible candidates by pick-face preference, FEFO expiry, dock travel, travel sequence, location ID, and bucket ID;
- if an explicit `line.lot_id` exists, filters to that exact lot;
- if `require_same_lot=true` and no lot is specified, groups by lot, keeps only groups whose summed available quantity satisfies the remaining quantity, and selects the best group by the same sort key;
- emits `PlannedStop` rows and calls `build_route`.

This prevents both mixed-lot fulfilment and short-lot wave plans.

### 4.3 Route construction

`build_route` aggregates stops by `(location_id, sku_id, lot_id)`, sorts deterministically, then repeatedly selects the nearest unvisited stop from the current coordinate using `travel_m`. It appends the return leg to the dock. `merge_routes` rebuilds a route from the union of two stop sets, so wave routes remain deterministic after an exception.

### 4.4 Affinity score

```text
location_j = Jaccard(order_route.location_signature, wave_route.location_signature)
sku_j      = Jaccard(order_route.sku_signature,      wave_route.sku_signature)
zone_j     = Jaccard(order_route.zone_signature,     wave_route.zone_signature)
incremental = max(0, merge_routes(wave_route, order_route).route_m - wave_route.route_m)
travel_score = clamp(1 - incremental / max_cluster_travel_m, 0, 1)
sla_score = sla_compatibility(order_sla, wave_cutoff, order_bucket, wave_bucket, window)

score = 0.42*location_j + 0.18*sku_j + 0.15*zone_j + 0.20*travel_score + 0.05*sla_score
```

Default `threshold = 0.62`, `max_zone_count = 2`, `max_cluster_travel_m = 900`. `choose_wave` filters by warehouse, strategy, status, release horizon, capacity, carrier/cutoff bucket for `WAVE`, exact zone for `ZONE`, score threshold, zone count, and merged travel. The winner is `min((-score, incremental_travel_m, wave_id))`; otherwise a new wave is created.

### 4.5 Wave persistence

`wave_repo.create_wave` inserts `waves` with strategy, priority, cutoff bucket, release time, capacities, and workflow version. `attach_order_to_wave` inserts `wave_orders` and `wave_order_lines` with `RETURNING` conflict detection, increments wave metrics, and rebuilds `wave_route_stops` with deterministic `uuid5` stop IDs. `ZONE` strategy creates one wave per zone; other strategies create one wave per order.

## 5. B2C allocation and task creation

`AllocationRepository.allocate_line` is called with preferred locations from the planned route. It runs in a savepoint and returns `AllocationResult` objects containing allocation ID, bucket, location, lot, quantity, zone, coordinate, travel sequence, expiry, HU, and wave ID.

Tasks are grouped by `(wave_id, zone)` and chunked by `b2c.max_lines_per_task = 40`. Each task payload contains ordered stops, allocation IDs, dock coordinate, strategy, wave IDs, order ID, zone, and correlation ID. `pick_priority` is:

```text
urgency = 400 when SLA is overdue, 300 within 30 minutes,
          200 within 60 minutes, 100 within 120 minutes, otherwise 0
bonus = 50 for CLUSTER or MULTI_BATCH
priority = min(1000, max(0, order_priority + urgency + bonus))
```

Task types are `PICK_PIECE` for `WAVE`, `PICK_CLUSTER` for `CLUSTER`, `PICK_ZONE` for `ZONE`, and `PICK_MULTI_BATCH` for `MULTI_BATCH`.

## 6. B2B full-pallet routing

B2B lines are validated at order intake: `fulfillment_unit = PALLET` and `required_qty % units_per_pallet = 0`. `reserve_full_pallets` recomputes the multiple from `line.remaining_qty`, then uses `LOCK_FULL_PALLET`:

- `EXACT` when an order-line lot is specified;
- `SINGLE_LOT` when same-lot fulfilment is required, choosing one lot with at least `pallets_needed` full pallets;
- `IGNORE` when mixed lots are permitted;
- `NULL` is supported for explicit null-lot-only selection.

The SQL locks `handling_units`, `inventory_bucket_headers`, `inventory_balances`, and `locations` with `FOR UPDATE ... SKIP LOCKED`, prefers cross-dock/staging/pallet/bulk by policy, and orders by dock travel and barcode. Reservation updates the balance, allocated balance, header version, HU status/version, `inventory_allocations`, order-line version, and two ledger rows. Shortages roll back partial reservations, emit `pallet.shortage`, mark the line `backordered`, and create `REPLENISHMENT`.

Successful reservations create `PALLET_PICK` tasks with `wave_id = NULL`, `required_equipment` from `b2b.required_equipment` (`FORKLIFT`, `REACH_TRUCK`, `AGV`), and forklift-class worker assignment. B2B therefore never enters a piece-pick queue.

## 7. Directed putaway and cross-dock

`plan_directed_putaway` requires `qty = full_pallet_qty`, resolves SKU/lot/source/dock, calculates required volume and weight, and calls `FIND_PUTAWAY_LOCATION`. That SQL checks available volume/weight, hazmat, temperature, allowed location types, and `location_slot_compatibility`. It creates an `in_transit` HU/bucket/balance and a `PALLET_PUTAWAY` or `CROSS_DOCK_MOVE` task. `complete_pallet_move` verifies the physical count, target capacity, and slot compatibility before converting `in_transit` to `available`.

## 8. Labour priority queue

`TaskDispatcher.enqueue_task` inserts `task_queue` with `ON CONFLICT DO NOTHING`, emits `task.created`, and returns the task ID. `assign_queued_tasks` claims queued tasks by priority, `not_before`, enqueue time, and ID with `FOR UPDATE SKIP LOCKED`. For each task it calls `FIND_ELIGIBLE_WORKER`:

```text
score = task_priority * 10
      + 200 when current_zone IS NOT DISTINCT FROM required_zone
      + 150 when active_equipment is in required_equipment
      + 80 when status = available
      - min(active_tasks, 10) * 20
      - abs(current_x_m - dock_x) - abs(current_y_m - dock_y)
```

Eligibility requires tenant, warehouse, `available`/`working` status, heartbeat within TTL, matching equipment, zone permission, and capacity below `max_concurrent_tasks`. Assignment is version-checked, stored in `task_assignments`, emitted as `task.assigned`, and pushed to Redis channel `wms.tasks.{tenant_id}.{worker_id}`.

`claim_tasks` is the worker-driven equivalent. It locks the worker row, re-checks heartbeat/status, claims tasks with zone and equipment filters, enforces `max_concurrent_tasks`, and applies the same assignment/outbox/push sequence. Heartbeats upsert worker state and zone permission, then trigger assignment.

## 9. Determinism and failure semantics

- All ordering keys include stable UUID or barcode tie-breakers.
- `compute_cutoff_bucket` rounds UTC timestamps to the configured minute bucket.
- Task and stop IDs use deterministic `uuid5` namespaces for source references.
- Partial allocation and partial pallet reservation are rolled back by savepoints.
- Version conflicts raise `ConflictError` or `ConcurrencyRetryExhausted`; they never silently double-allocate.
- Redis publish failure is retried with exponential backoff and does not roll back committed assignment; workers can always reclaim from PostgreSQL.
