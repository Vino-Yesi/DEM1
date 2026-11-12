from __future__ import annotations

# All statements use psycopg named placeholders and wms schema search_path.
# Complex locking predicates are intentionally centralized for auditability.

LOCK_ALLOCATION_CANDIDATE = """
WITH eligible_lots AS (
    SELECT h.lot_id,
           SUM(b.qty) AS available_qty,
           MIN(h.expiration_date) AS first_expiry,
           MIN(location_travel_cost(
               %(dock_x)s::numeric, %(dock_y)s::numeric, %(dock_z)s::numeric,
               l.x_m, l.y_m, l.z_m, %(dock_zone)s::text, l.zone
           )) AS min_travel
    FROM inventory_balances b
    JOIN inventory_bucket_headers h
      ON h.tenant_id = b.tenant_id AND h.id = b.bucket_id
    JOIN locations l
      ON l.id = h.location_id AND l.warehouse_id = h.warehouse_id
    JOIN skus s
      ON s.tenant_id = h.tenant_id AND s.id = h.sku_id
    WHERE b.tenant_id = %(tenant_id)s
      AND b.stock_status = 'available'
      AND b.qty > 0
      AND h.sku_id = %(sku_id)s
      AND (
          %(lot_filter_mode)s::text = 'SINGLE_LOT'
          OR %(lot_filter_mode)s::text = 'IGNORE'
          OR (%(lot_filter_mode)s::text = 'EXACT'
              AND h.lot_id IS NOT DISTINCT FROM %(lot_filter)s::uuid)
          OR (%(lot_filter_mode)s::text = 'NULL' AND h.lot_id IS NULL)
      )
      AND (cardinality(%(exclude_locations)s::uuid[]) = 0
           OR NOT (h.location_id = ANY(%(exclude_locations)s::uuid[])))
      AND (%(expiry_min)s::date IS NULL OR h.expiration_date >= %(expiry_min)s::date)
      AND (%(expiry_max)s::date IS NULL OR h.expiration_date <= %(expiry_max)s::date)
      AND (cardinality(%(allowed_types)s::location_type[]) = 0
           OR l.location_type = ANY(%(allowed_types)s::location_type[]))
      AND l.status = 'active'
      AND s.status = 'active'
      AND (NOT s.hazardous OR l.allows_hazmat)
      AND (l.temperature_band = 'ANY' OR l.temperature_band = s.temperature_band)
      AND (NOT s.requires_lot OR h.lot_id IS NOT NULL)
      AND (NOT s.requires_serial OR h.serial_number IS NOT NULL)
    GROUP BY h.lot_id
    HAVING SUM(b.qty) >= %(remaining_qty)s::numeric
),
chosen_lot AS (
    SELECT lot_id
    FROM eligible_lots
    ORDER BY (first_expiry IS NULL), first_expiry, min_travel,
             COALESCE(lot_id::text, '')
    LIMIT 1
),
locked AS (
    SELECT
        h.id AS bucket_id,
        h.version AS header_version,
        h.lot_id,
        h.serial_number,
        h.expiration_date,
        h.handling_unit_id,
        b.id AS balance_id,
        b.version AS balance_version,
        b.qty AS available_qty,
        l.id AS location_id,
        l.barcode AS location_barcode,
        l.zone,
        l.aisle,
        l.bay,
        l.level,
        l.position,
        l.x_m,
        l.y_m,
        l.z_m,
        l.travel_seq,
        l.location_type,
        l.pick_face,
        s.sku_code,
        s.unit_volume_cm3,
        s.unit_weight_kg
    FROM inventory_balances b
    JOIN inventory_bucket_headers h
      ON h.tenant_id = b.tenant_id AND h.id = b.bucket_id
    JOIN locations l
      ON l.id = h.location_id AND l.warehouse_id = h.warehouse_id
    JOIN skus s
      ON s.tenant_id = h.tenant_id AND s.id = h.sku_id
    WHERE b.tenant_id = %(tenant_id)s
      AND b.stock_status = 'available'
      AND b.qty > 0
      AND h.sku_id = %(sku_id)s
      AND (
          %(lot_filter_mode)s::text = 'IGNORE'
          OR (%(lot_filter_mode)s::text = 'EXACT'
              AND h.lot_id IS NOT DISTINCT FROM %(lot_filter)s::uuid)
          OR (%(lot_filter_mode)s::text = 'NULL' AND h.lot_id IS NULL)
          OR (%(lot_filter_mode)s::text = 'SINGLE_LOT'
              AND EXISTS (
                  SELECT 1 FROM chosen_lot c
                  WHERE c.lot_id IS NOT DISTINCT FROM h.lot_id
              ))
      )
      AND (cardinality(%(exclude_locations)s::uuid[]) = 0
           OR NOT (h.location_id = ANY(%(exclude_locations)s::uuid[])))
      AND (%(expiry_min)s::date IS NULL OR h.expiration_date >= %(expiry_min)s::date)
      AND (%(expiry_max)s::date IS NULL OR h.expiration_date <= %(expiry_max)s::date)
      AND (cardinality(%(allowed_types)s::location_type[]) = 0
           OR l.location_type = ANY(%(allowed_types)s::location_type[]))
      AND l.status = 'active'
      AND s.status = 'active'
      AND (NOT s.hazardous OR l.allows_hazmat)
      AND (l.temperature_band = 'ANY' OR l.temperature_band = s.temperature_band)
      AND (NOT s.requires_lot OR h.lot_id IS NOT NULL)
      AND (NOT s.requires_serial OR h.serial_number IS NOT NULL)
    ORDER BY
        CASE WHEN cardinality(%(preferred_locations)s::uuid[]) > 0
                  AND h.location_id = ANY(%(preferred_locations)s::uuid[])
             THEN 0 ELSE 1 END,
        (h.expiration_date IS NULL),
        h.expiration_date,
        location_travel_cost(
            %(dock_x)s::numeric, %(dock_y)s::numeric, %(dock_z)s::numeric,
            l.x_m, l.y_m, l.z_m, %(dock_zone)s::text, l.zone
        ),
        l.travel_seq,
        l.pick_face DESC,
        h.id
    FOR UPDATE OF h, b, l SKIP LOCKED
    LIMIT 1
)
SELECT * FROM locked
"""

UPDATE_AVAILABLE_BALANCE = """
UPDATE inventory_balances
   SET qty = qty - %(qty)s,
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(balance_id)s
   AND version = %(balance_version)s
   AND qty >= %(qty)s
RETURNING qty, version
"""

UPSERT_ALLOCATED_BALANCE = """
INSERT INTO inventory_balances
    (tenant_id, bucket_id, stock_status, qty)
VALUES
    (%(tenant_id)s, %(bucket_id)s, 'allocated', %(qty)s)
ON CONFLICT (tenant_id, bucket_id, stock_status)
DO UPDATE SET qty = inventory_balances.qty + EXCLUDED.qty,
              version = inventory_balances.version + 1,
              updated_at = now()
RETURNING qty, version
"""

BUMP_HEADER_VERSION = """
UPDATE inventory_bucket_headers
   SET version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(bucket_id)s
   AND version = %(header_version)s
RETURNING version
"""

INSERT_ALLOCATION = """
INSERT INTO inventory_allocations
    (tenant_id, order_line_id, bucket_id, wave_id, sku_id, lot_id,
     warehouse_id, location_id, handling_unit_id, qty, status)
VALUES
    (%(tenant_id)s, %(order_line_id)s, %(bucket_id)s, %(wave_id)s,
     %(sku_id)s, %(lot_id)s, %(warehouse_id)s, %(location_id)s,
     %(handling_unit_id)s, %(qty)s, 'reserved')
RETURNING id
"""

UPDATE_ORDER_LINE_ALLOCATED = """
UPDATE order_lines
   SET allocated_qty = allocated_qty + %(qty)s,
       status = CASE
           WHEN allocated_qty + %(qty)s >= required_qty - picked_qty THEN 'allocated'
           ELSE status
       END,
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(order_line_id)s
   AND version = %(line_version)s
   AND allocated_qty + %(qty)s <= required_qty - picked_qty
RETURNING allocated_qty, version, status
"""

INSERT_LEDGER = """
INSERT INTO inventory_ledger
    (tenant_id, occurred_at, bucket_id, sku_id, warehouse_id, location_id,
     lot_id, serial_number, from_status, to_status, qty_delta,
     balance_after, reason, ref_type, ref_id, actor, correlation_id, metadata)
VALUES
    (%(tenant_id)s, now(), %(bucket_id)s, %(sku_id)s, %(warehouse_id)s,
     %(location_id)s, %(lot_id)s, %(serial_number)s, %(from_status)s,
     %(to_status)s, %(qty_delta)s, %(balance_after)s, %(reason)s,
     %(ref_type)s, %(ref_id)s, %(actor)s, %(correlation_id)s, %(metadata)s::jsonb)
RETURNING id, occurred_at
"""

GET_B2C_CANDIDATES = """
SELECT
    bucket_id, sku_id, lot_id, serial_number, expiration_date, handling_unit_id,
    warehouse_id, location_id, available_qty, balance_version, header_version,
    location_barcode, zone, aisle, bay, level, position,
    x_m, y_m, z_m, travel_seq, location_type, pick_face,
    allows_hazmat, location_temperature_band, sku_code, hazardous,
    sku_temperature_band, requires_lot, requires_serial, velocity_score,
    unit_volume_cm3, unit_weight_kg
FROM inventory_available
WHERE tenant_id = %(tenant_id)s
  AND warehouse_id = %(warehouse_id)s
  AND sku_id = %(sku_id)s
  AND (
      %(lot_filter_mode)s::text = 'IGNORE'
      OR (%(lot_filter_mode)s::text = 'EXACT'
          AND lot_id IS NOT DISTINCT FROM %(lot_filter)s::uuid)
      OR (%(lot_filter_mode)s::text = 'NULL' AND lot_id IS NULL)
  )
  AND (%(expiry_min)s::date IS NULL OR expiration_date >= %(expiry_min)s::date)
  AND (%(expiry_max)s::date IS NULL OR expiration_date <= %(expiry_max)s::date)
  AND (cardinality(%(allowed_types)s::location_type[]) = 0
       OR location_type = ANY(%(allowed_types)s::location_type[]))
ORDER BY (expiration_date IS NULL), expiration_date, travel_seq, pick_face DESC, bucket_id
"""

LOAD_OPEN_WAVES = """
SELECT w.*,
       COALESCE(
           jsonb_agg(
               jsonb_build_object(
                   'location_id', s.location_id,
                   'sku_id', s.sku_id,
                   'lot_id', s.lot_id,
                   'qty', s.qty,
                   'stop_seq', s.stop_seq,
                   'x_m', s.x_m,
                   'y_m', s.y_m,
                   'z_m', s.z_m,
                   'zone', s.zone
               ) ORDER BY s.stop_seq
           ) FILTER (WHERE s.id IS NOT NULL),
           '[]'::jsonb
       ) AS stops
FROM waves w
LEFT JOIN wave_route_stops s
  ON s.tenant_id = w.tenant_id AND s.wave_id = w.id
WHERE w.tenant_id = %(tenant_id)s
  AND w.warehouse_id = %(warehouse_id)s
  AND w.status IN ('planned', 'released')
  AND w.strategy = %(strategy)s::picking_strategy
  AND w.release_at <= %(now)s::timestamptz + make_interval(mins => %(window_minutes)s::int)
  AND w.order_count < w.max_orders
  AND w.line_count < w.max_lines
  AND w.unit_count < w.max_units
GROUP BY w.tenant_id, w.id
ORDER BY w.priority DESC, w.release_at, w.id
"""

INSERT_WAVE = """
INSERT INTO waves
    (tenant_id, warehouse_id, wave_code, strategy, status, carrier_code,
     cutoff_bucket, zone, affinity_score, estimated_travel_m, priority,
     release_at, cutoff_at, max_orders, max_lines, max_units, metadata)
VALUES
    (%(tenant_id)s, %(warehouse_id)s, %(wave_code)s, %(strategy)s::picking_strategy,
     'planned', %(carrier_code)s, %(cutoff_bucket)s, %(zone)s,
     %(affinity_score)s, %(estimated_travel_m)s, %(priority)s,
     %(release_at)s, %(cutoff_at)s, %(max_orders)s, %(max_lines)s,
     %(max_units)s, %(metadata)s::jsonb)
RETURNING id
"""

INSERT_WAVE_ORDER = """
INSERT INTO wave_orders (tenant_id, wave_id, order_id, sequence)
VALUES (%(tenant_id)s, %(wave_id)s, %(order_id)s, %(sequence)s)
ON CONFLICT (tenant_id, wave_id, order_id) DO NOTHING
RETURNING order_id
"""

INSERT_WAVE_ORDER_LINE = """
INSERT INTO wave_order_lines (tenant_id, wave_id, order_line_id, sequence)
VALUES (%(tenant_id)s, %(wave_id)s, %(order_line_id)s, %(sequence)s)
ON CONFLICT (tenant_id, wave_id, order_line_id) DO NOTHING
RETURNING order_line_id
"""

LOCK_WAVE = """
SELECT * FROM waves
WHERE tenant_id = %(tenant_id)s AND id = %(wave_id)s
FOR UPDATE
"""

UPSERT_WAVE_ROUTE_STOP = """
INSERT INTO wave_route_stops
    (tenant_id, wave_id, warehouse_id, location_id, sku_id, lot_id,
     qty, stop_seq, x_m, y_m, z_m, zone)
VALUES
    (%(tenant_id)s, %(wave_id)s, %(warehouse_id)s, %(location_id)s,
     %(sku_id)s, %(lot_id)s, %(qty)s, %(stop_seq)s,
     %(x_m)s, %(y_m)s, %(z_m)s, %(zone)s)
ON CONFLICT ON CONSTRAINT uq_wave_route_stop
DO UPDATE SET qty = wave_route_stops.qty + EXCLUDED.qty
RETURNING id
"""

DELETE_WAVE_ROUTE_STOPS_AT_LOCATION = """
DELETE FROM wave_route_stops
WHERE tenant_id = %(tenant_id)s
  AND wave_id = %(wave_id)s
  AND location_id = %(location_id)s
"""

UPDATE_WAVE_METRICS = """
UPDATE waves
   SET order_count = order_count + %(order_delta)s,
       line_count = line_count + %(line_delta)s,
       unit_count = unit_count + %(unit_delta)s,
       estimated_travel_m = %(estimated_travel_m)s,
       affinity_score = %(affinity_score)s,
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s AND id = %(wave_id)s
RETURNING order_count, line_count, unit_count, version
"""

GET_WAVE_LINE_IDS = """
SELECT order_line_id
FROM wave_order_lines
WHERE tenant_id = %(tenant_id)s AND wave_id = %(wave_id)s
ORDER BY sequence
"""

LOCK_FULL_PALLET = """
WITH eligible_lots AS (
    SELECT hu.lot_id,
           COUNT(DISTINCT hu.id) AS pallet_count,
           MIN(h.expiration_date) AS first_expiry,
           MIN(location_travel_cost(
               %(dock_x)s::numeric, %(dock_y)s::numeric, %(dock_z)s::numeric,
               l.x_m, l.y_m, l.z_m, %(dock_zone)s::text, l.zone
           )) AS min_travel
    FROM handling_units hu
    JOIN inventory_bucket_headers h
      ON h.tenant_id = hu.tenant_id AND h.handling_unit_id = hu.id
    JOIN inventory_balances b
      ON b.tenant_id = h.tenant_id AND b.bucket_id = h.id AND b.stock_status = 'available'
    JOIN locations l
      ON l.id = hu.location_id AND l.warehouse_id = hu.warehouse_id
    JOIN skus s
      ON s.tenant_id = hu.tenant_id AND s.id = hu.sku_id
    WHERE hu.tenant_id = %(tenant_id)s
      AND hu.sku_id = %(sku_id)s
      AND (
          %(lot_filter_mode)s::text = 'SINGLE_LOT'
          OR %(lot_filter_mode)s::text = 'IGNORE'
          OR (%(lot_filter_mode)s::text = 'EXACT'
              AND hu.lot_id IS NOT DISTINCT FROM %(lot_filter)s::uuid)
          OR (%(lot_filter_mode)s::text = 'NULL' AND hu.lot_id IS NULL)
      )
      AND hu.hu_type = 'PALLET'
      AND hu.is_full
      AND hu.status = 'available'
      AND hu.qty = s.units_per_pallet
      AND b.qty >= hu.qty
      AND l.status = 'active'
      AND l.location_type = ANY(%(allowed_types)s::location_type[])
      AND (NOT s.hazardous OR l.allows_hazmat)
      AND (l.temperature_band = 'ANY' OR l.temperature_band = s.temperature_band)
      AND (cardinality(%(exclude_locations)s::uuid[]) = 0
           OR NOT (l.id = ANY(%(exclude_locations)s::uuid[])))
    GROUP BY hu.lot_id
    HAVING COUNT(DISTINCT hu.id) >= %(pallets_needed)s::int
),
chosen_lot AS (
    SELECT lot_id
    FROM eligible_lots
    ORDER BY (first_expiry IS NULL), first_expiry, min_travel,
             COALESCE(lot_id::text, '')
    LIMIT 1
),
locked AS (
    SELECT
        hu.id AS handling_unit_id,
        hu.barcode AS hu_barcode,
        hu.qty AS hu_qty,
        hu.full_pallet_qty,
        hu.lot_id,
        hu.version AS hu_version,
        hu.volume_cm3 AS hu_volume_cm3,
        hu.tare_weight_kg AS hu_tare_weight_kg,
        h.id AS bucket_id,
        h.version AS header_version,
        h.sku_id,
        h.warehouse_id,
        b.id AS balance_id,
        b.version AS balance_version,
        b.qty AS available_qty,
        l.id AS location_id,
        l.barcode AS location_barcode,
        l.zone,
        l.location_type,
        l.x_m,
        l.y_m,
        l.z_m,
        l.travel_seq
    FROM handling_units hu
    JOIN inventory_bucket_headers h
      ON h.tenant_id = hu.tenant_id AND h.handling_unit_id = hu.id
    JOIN inventory_balances b
      ON b.tenant_id = h.tenant_id AND b.bucket_id = h.id AND b.stock_status = 'available'
    JOIN locations l
      ON l.id = hu.location_id AND l.warehouse_id = hu.warehouse_id
    JOIN skus s
      ON s.tenant_id = hu.tenant_id AND s.id = hu.sku_id
    WHERE hu.tenant_id = %(tenant_id)s
      AND hu.sku_id = %(sku_id)s
      AND (
          %(lot_filter_mode)s::text = 'IGNORE'
          OR (%(lot_filter_mode)s::text = 'EXACT'
              AND hu.lot_id IS NOT DISTINCT FROM %(lot_filter)s::uuid)
          OR (%(lot_filter_mode)s::text = 'NULL' AND hu.lot_id IS NULL)
          OR (%(lot_filter_mode)s::text = 'SINGLE_LOT'
              AND EXISTS (
                  SELECT 1 FROM chosen_lot c
                  WHERE c.lot_id IS NOT DISTINCT FROM hu.lot_id
              ))
      )
      AND hu.hu_type = 'PALLET'
      AND hu.is_full
      AND hu.status = 'available'
      AND hu.qty = s.units_per_pallet
      AND b.qty >= hu.qty
      AND l.status = 'active'
      AND l.location_type = ANY(%(allowed_types)s::location_type[])
      AND (NOT s.hazardous OR l.allows_hazmat)
      AND (l.temperature_band = 'ANY' OR l.temperature_band = s.temperature_band)
      AND (cardinality(%(exclude_locations)s::uuid[]) = 0
           OR NOT (l.id = ANY(%(exclude_locations)s::uuid[])))
    ORDER BY
        CASE
            WHEN %(prefer_cross_dock)s::boolean AND l.location_type = 'cross_dock' THEN 0
            WHEN l.location_type = 'staging' THEN 1
            WHEN l.location_type = 'pallet' THEN 2
            ELSE 3
        END,
        location_travel_cost(
            %(dock_x)s::numeric, %(dock_y)s::numeric, %(dock_z)s::numeric,
            l.x_m, l.y_m, l.z_m, %(dock_zone)s::text, l.zone
        ),
        l.travel_seq,
        hu.barcode
    FOR UPDATE OF hu, h, b, l SKIP LOCKED
    LIMIT %(limit)s
)
SELECT * FROM locked
"""

FIND_PUTAWAY_LOCATION = """
SELECT l.*,
       s.units_per_pallet,
       s.unit_volume_cm3,
       s.unit_weight_kg,
       s.hazardous,
       s.temperature_band AS sku_temperature_band
FROM locations l
JOIN skus s
  ON s.tenant_id = %(tenant_id)s AND s.id = %(sku_id)s
WHERE l.warehouse_id = %(warehouse_id)s
  AND l.status = 'active'
  AND l.location_type = ANY(%(allowed_types)s::location_type[])
  AND l.available_volume_cm3 >= %(required_volume)s
  AND l.available_weight_kg >= %(required_weight)s
  AND (NOT s.hazardous OR l.allows_hazmat)
  AND (l.temperature_band = 'ANY' OR l.temperature_band = s.temperature_band)
  AND location_slot_compatibility(l.id, %(tenant_id)s, %(sku_id)s, %(lot_id)s)
ORDER BY
    CASE
        WHEN %(prefer_cross_dock)s::boolean AND l.location_type = 'cross_dock' THEN 0
        WHEN l.location_type = 'pallet' THEN 1
        WHEN l.location_type = 'bulk' THEN 2
        ELSE 3
    END,
    location_travel_cost(
        %(dock_x)s::numeric, %(dock_y)s::numeric, %(dock_z)s::numeric,
        l.x_m, l.y_m, l.z_m, %(dock_zone)s::text, l.zone
    ),
    (l.available_volume_cm3 - %(required_volume)s),
    l.travel_seq,
    l.id
FOR UPDATE OF l SKIP LOCKED
LIMIT 1
"""

TASK_INSERT = """
INSERT INTO task_queue
    (tenant_id, task_type, state, priority, warehouse_id, required_equipment,
     required_zone, location_id, source_ref_type, source_ref_id, wave_id,
     order_id, payload, not_before)
VALUES
    (%(tenant_id)s, %(task_type)s::task_type, 'queued', %(priority)s,
     %(warehouse_id)s, %(required_equipment)s::equipment_type[],
     %(required_zone)s, %(location_id)s, %(source_ref_type)s,
     %(source_ref_id)s, %(wave_id)s, %(order_id)s, %(payload)s::jsonb,
     %(not_before)s)
ON CONFLICT DO NOTHING
RETURNING id
"""

CANCEL_TASKS_FOR_REF = """
UPDATE task_queue
   SET state = 'cancelled',
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND source_ref_id = %(source_ref_id)s
   AND task_type = ANY(%(task_types)s::task_type[])
   AND state IN ('queued', 'assigned', 'accepted', 'in_progress', 'blocked')
RETURNING id
"""

TASK_QUEUE_CLAIM = """
SELECT *
FROM task_queue
WHERE tenant_id = %(tenant_id)s
  AND state = 'queued'
  AND not_before <= %(now)s
  AND (%(warehouse_id)s::uuid IS NULL OR warehouse_id = %(warehouse_id)s::uuid)
ORDER BY priority DESC, not_before, enqueued_at, id
LIMIT %(limit)s
FOR UPDATE SKIP LOCKED
"""

FIND_ELIGIBLE_WORKER = """
SELECT w.*,
       load.active_tasks,
       (
           %(task_priority)s::int * 10
           + CASE WHEN w.current_zone IS NOT DISTINCT FROM %(required_zone)s::text THEN 200 ELSE 0 END
           + CASE WHEN w.active_equipment = ANY(%(required_equipment)s::equipment_type[]) THEN 150 ELSE 0 END
           + CASE WHEN w.status = 'available' THEN 80 ELSE 0 END
           - LEAST(load.active_tasks, 10) * 20
           - abs(w.current_x_m - %(worker_x)s::numeric)
           - abs(w.current_y_m - %(worker_y)s::numeric)
       ) AS score
FROM workers w
CROSS JOIN LATERAL (
    SELECT count(*) AS active_tasks
    FROM task_queue tq
    WHERE tq.tenant_id = w.tenant_id
      AND tq.assigned_worker_id = w.id
      AND tq.state IN ('assigned', 'accepted', 'in_progress')
) AS load
WHERE w.tenant_id = %(tenant_id)s
  AND w.warehouse_id = %(warehouse_id)s
  AND w.status IN ('available', 'working')
  AND w.last_heartbeat_at >= %(heartbeat_cutoff)s
  AND w.active_equipment = ANY(%(required_equipment)s::equipment_type[])
  AND (
      %(required_zone)s::text IS NULL
      OR w.current_zone = %(required_zone)s::text
      OR EXISTS (
          SELECT 1 FROM worker_zone_permissions p
          WHERE p.tenant_id = w.tenant_id
            AND p.worker_id = w.id
            AND p.warehouse_id = w.warehouse_id
            AND p.zone = %(required_zone)s::text
      )
  )
  AND load.active_tasks < w.max_concurrent_tasks
ORDER BY score DESC, w.last_heartbeat_at DESC, w.id
LIMIT 1
FOR UPDATE OF w SKIP LOCKED
"""

TASK_CLAIM_FOR_WORKER = """
SELECT tq.*
FROM task_queue tq
WHERE tq.tenant_id = %(tenant_id)s
  AND tq.state = 'queued'
  AND tq.not_before <= %(now)s
  AND tq.warehouse_id = %(warehouse_id)s
  AND (
      tq.required_zone IS NULL
      OR tq.required_zone = %(zone)s
      OR EXISTS (
          SELECT 1 FROM worker_zone_permissions p
          WHERE p.tenant_id = tq.tenant_id
            AND p.worker_id = %(worker_id)s
            AND p.warehouse_id = tq.warehouse_id
            AND p.zone = tq.required_zone
      )
  )
  AND tq.required_equipment && ARRAY[%(equipment)s]::equipment_type[]
ORDER BY tq.priority DESC, tq.not_before, tq.enqueued_at, tq.id
LIMIT %(limit)s
FOR UPDATE OF tq SKIP LOCKED
"""

UPDATE_TASK_ASSIGNED = """
UPDATE task_queue
   SET state = 'assigned',
       assigned_worker_id = %(worker_id)s,
       claimed_at = now(),
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(task_id)s
   AND state = 'queued'
   AND version = %(version)s
RETURNING id, version
"""

INSERT_TASK_ASSIGNMENT = """
INSERT INTO task_assignments
    (tenant_id, task_id, worker_id, state, score)
VALUES
    (%(tenant_id)s, %(task_id)s, %(worker_id)s, 'assigned', %(score)s)
ON CONFLICT (tenant_id, task_id, worker_id)
DO UPDATE SET assigned_at = now(),
              state = 'assigned',
              score = EXCLUDED.score
RETURNING task_id
"""

UPSERT_WORKER = """
INSERT INTO workers
    (tenant_id, id, employee_code, display_name, warehouse_id, status,
     active_equipment, current_zone, current_x_m, current_y_m,
     max_concurrent_tasks, skills, last_heartbeat_at)
VALUES
    (%(tenant_id)s, %(worker_id)s, %(employee_code)s, %(display_name)s,
     %(warehouse_id)s, %(status)s::worker_status,
     %(active_equipment)s::equipment_type, %(current_zone)s,
     %(x_m)s, %(y_m)s, %(max_concurrent_tasks)s, %(skills)s::text[], now())
ON CONFLICT (tenant_id, id) DO UPDATE
SET employee_code = EXCLUDED.employee_code,
    display_name = EXCLUDED.display_name,
    warehouse_id = EXCLUDED.warehouse_id,
    status = EXCLUDED.status,
    active_equipment = EXCLUDED.active_equipment,
    current_zone = EXCLUDED.current_zone,
    current_x_m = EXCLUDED.current_x_m,
    current_y_m = EXCLUDED.current_y_m,
    max_concurrent_tasks = EXCLUDED.max_concurrent_tasks,
    skills = EXCLUDED.skills,
    last_heartbeat_at = now(),
    version = workers.version + 1,
    updated_at = now()
RETURNING id, status
"""

UPSERT_WORKER_ZONE = """
INSERT INTO worker_zone_permissions
    (tenant_id, worker_id, warehouse_id, zone)
VALUES
    (%(tenant_id)s, %(worker_id)s, %(warehouse_id)s, %(zone)s)
ON CONFLICT (tenant_id, worker_id, warehouse_id, zone) DO NOTHING
"""

LOCK_LOCATION_FOR_QUARANTINE = """
SELECT * FROM locations
WHERE id = %(location_id)s
FOR UPDATE
"""

UPDATE_LOCATION_QUARANTINE = """
UPDATE locations
   SET status = 'quarantine',
       quarantine_reason = %(reason)s,
       quarantined_at = now(),
       version = version + 1,
       updated_at = now()
 WHERE id = %(location_id)s
   AND version = %(location_version)s
   AND (%(force)s::boolean OR status <> 'quarantine')
RETURNING id, status, version
"""

LOCK_DISCREPANCY_BUCKETS = """
SELECT h.id AS bucket_id,
       h.tenant_id,
       h.sku_id,
       h.warehouse_id,
       h.location_id,
       h.lot_id,
       h.serial_number,
       h.expiration_date,
       h.handling_unit_id,
       h.version AS header_version,
       b.id AS balance_id,
       b.stock_status,
       b.qty AS balance_qty,
       b.version AS balance_version
FROM inventory_bucket_headers h
JOIN inventory_balances b
  ON b.tenant_id = h.tenant_id AND b.bucket_id = h.id
WHERE h.tenant_id = %(tenant_id)s
  AND h.location_id = %(location_id)s
  AND h.sku_id = %(sku_id)s
  AND (%(lot_id)s::uuid IS NULL OR h.lot_id IS NOT DISTINCT FROM %(lot_id)s::uuid)
  AND b.stock_status IN ('available', 'allocated', 'damaged', 'qc_hold')
  AND b.qty > 0
ORDER BY h.id, b.stock_status
FOR UPDATE OF h, b
"""

RELEASE_ALLOCATIONS_AT_LOCATION = """
SELECT a.id AS allocation_id,
       a.qty AS allocation_qty,
       a.order_line_id,
       a.bucket_id,
       a.wave_id,
       ol.version AS line_version,
       ol.required_qty,
       ol.picked_qty,
       ol.allocated_qty,
       ol.status AS line_status
FROM inventory_allocations a
JOIN order_lines ol
  ON ol.tenant_id = a.tenant_id AND ol.id = a.order_line_id
WHERE a.tenant_id = %(tenant_id)s
  AND a.location_id = %(location_id)s
  AND a.status = 'reserved'
  AND (%(wave_id)s::uuid IS NULL
       OR a.wave_id = %(wave_id)s::uuid
       OR a.wave_id IS NULL)
  AND ol.status NOT IN ('packed', 'shipped', 'cancelled')
ORDER BY a.id
FOR UPDATE OF a, ol
"""

UPDATE_ALLOCATION_RELEASE = """
UPDATE inventory_allocations
   SET status = 'released_discrepancy',
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(allocation_id)s
   AND status = 'reserved'
RETURNING id
"""

UPDATE_ORDER_LINE_RELEASE = """
UPDATE order_lines
   SET allocated_qty = allocated_qty - %(qty)s,
       status = 'open',
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(order_line_id)s
   AND version = %(line_version)s
   AND allocated_qty >= %(qty)s
RETURNING allocated_qty, version
"""

GET_WAVE_UNFULFILLED_LINES = """
SELECT ol.*,
       s.sku_code,
       s.units_per_pallet,
       s.hazardous,
       s.temperature_band,
       s.requires_lot,
       s.requires_serial
FROM wave_order_lines wol
JOIN order_lines ol
  ON ol.tenant_id = wol.tenant_id AND ol.id = wol.order_line_id
JOIN skus s
  ON s.tenant_id = ol.tenant_id AND s.id = ol.sku_id
WHERE wol.tenant_id = %(tenant_id)s
  AND wol.wave_id = %(wave_id)s
  AND ol.status NOT IN ('packed', 'shipped', 'cancelled')
  AND ol.required_qty > ol.picked_qty + ol.allocated_qty
ORDER BY wol.sequence
FOR UPDATE OF ol
"""

GET_BILLING_CONTRACT = """
SELECT *
FROM tenant_contracts
WHERE tenant_id = %(tenant_id)s
  AND status = 'active'
  AND valid_from <= %(as_of)s::date
  AND (valid_to IS NULL OR %(as_of)s::date < valid_to)
ORDER BY valid_from DESC
LIMIT 1
"""

GET_STORAGE_RATE = """
SELECT *
FROM contract_storage_rates
WHERE tenant_id = %(tenant_id)s
  AND contract_id = %(contract_id)s
  AND basis = %(basis)s::storage_basis
  AND effective_from <= %(as_of)s::date
  AND (effective_to IS NULL OR %(as_of)s::date < effective_to)
  AND (location_type IS NULL OR location_type = %(location_type)s::location_type)
ORDER BY (location_type IS NULL) ASC, effective_from DESC
LIMIT 1
"""

GET_PICK_RATE = """
SELECT *
FROM contract_pick_rates
WHERE tenant_id = %(tenant_id)s
  AND contract_id = %(contract_id)s
  AND mode = %(mode)s::order_mode
  AND effective_from <= %(as_of)s::date
  AND (effective_to IS NULL OR %(as_of)s::date < effective_to)
ORDER BY effective_from DESC
LIMIT 1
"""

GET_VAS_RATE = """
SELECT *
FROM vas_rate_cards
WHERE tenant_id = %(tenant_id)s
  AND contract_id = %(contract_id)s
  AND vas_code = %(vas_code)s
  AND effective_from <= %(as_of)s::date
  AND (effective_to IS NULL OR %(as_of)s::date < effective_to)
ORDER BY effective_from DESC
LIMIT 1
"""

INSERT_BILLING_EVENT = """
INSERT INTO billing_events
    (tenant_id, contract_id, source_type, source_id, event_type, occurred_at,
     quantity, gross_amount, discount_amount, surcharge_amount, tax_amount,
     net_amount, currency, rate_snapshot, components, idempotency_key)
VALUES
    (%(tenant_id)s, %(contract_id)s, %(source_type)s, %(source_id)s,
     %(event_type)s, %(occurred_at)s, %(quantity)s, %(gross_amount)s,
     %(discount_amount)s, %(surcharge_amount)s, %(tax_amount)s,
     %(net_amount)s, %(currency)s, %(rate_snapshot)s::jsonb,
     %(components)s::jsonb, %(idempotency_key)s)
RETURNING id, occurred_at
"""

INSERT_BILLING_EVENT_KEY = """
INSERT INTO billing_event_keys
    (tenant_id, source_type, source_id, event_type, idempotency_key,
     billing_event_id, occurred_at)
VALUES
    (%(tenant_id)s, %(source_type)s, %(source_id)s, %(event_type)s,
     %(idempotency_key)s, %(billing_event_id)s, %(occurred_at)s)
ON CONFLICT (tenant_id, source_type, source_id, event_type, idempotency_key)
DO NOTHING
RETURNING billing_event_id
"""

GET_BILLING_EVENT_BY_KEY = """
SELECT be.id AS billing_event_id,
       be.event_type,
       be.currency,
       be.quantity,
       be.gross_amount,
       be.discount_amount,
       be.surcharge_amount,
       be.tax_amount,
       be.net_amount,
       be.components,
       be.rate_snapshot
FROM billing_event_keys k
JOIN billing_events be
  ON be.id = k.billing_event_id AND be.occurred_at = k.occurred_at
WHERE k.tenant_id = %(tenant_id)s
  AND k.source_type = %(source_type)s
  AND k.source_id = %(source_id)s
  AND k.event_type = %(event_type)s
  AND k.idempotency_key = %(idempotency_key)s
"""

DELETE_BILLING_EVENT_BY_ID = """
DELETE FROM billing_events
WHERE id = %(billing_event_id)s
  AND occurred_at = %(occurred_at)s
"""

INSERT_VAS_TRANSACTION = """
INSERT INTO vas_transactions
    (tenant_id, order_id, order_line_id, vas_code, quantity, minutes,
     pallets, cartons, order_count, occurred_at, attributes, idempotency_key)
VALUES
    (%(tenant_id)s, %(order_id)s, %(order_line_id)s, %(vas_code)s,
     %(quantity)s, %(minutes)s, %(pallets)s, %(cartons)s,
     %(order_count)s, %(occurred_at)s, %(attributes)s::jsonb,
     %(idempotency_key)s)
ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
RETURNING id
"""

UPDATE_HU_ALLOCATED = """
UPDATE handling_units
   SET status = 'allocated',
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(handling_unit_id)s
   AND version = %(hu_version)s
   AND status = 'available'
RETURNING id, version
"""

LOCK_TASK_FOR_COMPLETION = """
SELECT * FROM task_queue
WHERE tenant_id = %(tenant_id)s AND id = %(task_id)s
FOR UPDATE
"""

LOCK_HU_FOR_COMPLETION = """
SELECT * FROM handling_units
WHERE tenant_id = %(tenant_id)s AND barcode = %(hu_barcode)s
FOR UPDATE
"""

LOCK_TARGET_LOCATION = """
SELECT * FROM locations
WHERE id = %(location_id)s
FOR UPDATE
"""

UPDATE_HU_LOCATION = """
UPDATE handling_units
   SET location_id = %(target_location_id)s,
       warehouse_id = %(warehouse_id)s,
       status = 'available',
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(handling_unit_id)s
   AND version = %(hu_version)s
RETURNING id, version
"""

UPDATE_BUCKET_LOCATION = """
UPDATE inventory_bucket_headers
   SET location_id = %(target_location_id)s,
       warehouse_id = %(warehouse_id)s,
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND handling_unit_id = %(handling_unit_id)s
RETURNING id, version
"""

UPSERT_AVAILABLE_BALANCE = """
INSERT INTO inventory_balances
    (tenant_id, bucket_id, stock_status, qty)
VALUES
    (%(tenant_id)s, %(bucket_id)s, 'available', %(qty)s)
ON CONFLICT (tenant_id, bucket_id, stock_status)
DO UPDATE SET qty = inventory_balances.qty + EXCLUDED.qty,
              version = inventory_balances.version + 1,
              updated_at = now()
RETURNING qty, version
"""

DELETE_IN_TRANSIT_BALANCE = """
DELETE FROM inventory_balances
WHERE tenant_id = %(tenant_id)s
  AND bucket_id = %(bucket_id)s
  AND stock_status = 'in_transit'
RETURNING qty
"""

COMPLETE_TASK = """
UPDATE task_queue
   SET state = 'completed',
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND id = %(task_id)s
   AND state IN ('assigned', 'accepted', 'in_progress')
RETURNING id, state
"""

COMPLETE_ASSIGNMENT = """
UPDATE task_assignments
   SET state = 'completed',
       completed_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND task_id = %(task_id)s
   AND state IN ('assigned', 'accepted')
"""

INSERT_DISCREPANCY_EVENT = """
INSERT INTO inventory_discrepancy_events
    (tenant_id, source, source_event_id, warehouse_id, location_id, sku_id,
     lot_id, expected_qty, found_qty, wave_id, observed_at, status)
VALUES
    (%(tenant_id)s, %(source)s::discrepancy_source, %(source_event_id)s,
     %(warehouse_id)s, %(location_id)s, %(sku_id)s, %(lot_id)s,
     %(expected_qty)s, %(found_qty)s, %(wave_id)s, %(observed_at)s,
     'processing')
ON CONFLICT (tenant_id, source, source_event_id) DO NOTHING
RETURNING id
"""

UPDATE_DISCREPANCY_PROCESSED = """
UPDATE inventory_discrepancy_events
   SET status = 'processed',
       processing_result = %(result)s::jsonb
 WHERE tenant_id = %(tenant_id)s
   AND id = %(event_id)s
RETURNING id
"""

UPSERT_QUARANTINE_BALANCE = """
INSERT INTO inventory_balances
    (tenant_id, bucket_id, stock_status, qty)
VALUES
    (%(tenant_id)s, %(bucket_id)s, 'quarantine', %(qty)s)
ON CONFLICT (tenant_id, bucket_id, stock_status)
DO UPDATE SET qty = inventory_balances.qty + EXCLUDED.qty,
              version = inventory_balances.version + 1,
              updated_at = now()
RETURNING qty, version
"""

DELETE_BALANCE_ROW = """
DELETE FROM inventory_balances
WHERE tenant_id = %(tenant_id)s
  AND id = %(balance_id)s
  AND version = %(balance_version)s
RETURNING id
"""

CANCEL_TASKS_BY_ALLOCATION = """
UPDATE task_queue
   SET state = 'cancelled',
       version = version + 1,
       updated_at = now()
 WHERE tenant_id = %(tenant_id)s
   AND task_type = ANY(%(task_types)s::task_type[])
   AND state IN ('queued', 'assigned', 'accepted', 'in_progress', 'blocked')
   AND payload -> 'allocations' ? %(allocation_id)s::text
RETURNING id
"""

GET_LINES_BY_IDS = """
SELECT ol.*,
       s.sku_code,
       s.units_per_pallet,
       s.hazardous,
       s.temperature_band,
       s.requires_lot,
       s.requires_serial
FROM order_lines ol
JOIN skus s ON s.tenant_id = ol.tenant_id AND s.id = ol.sku_id
WHERE ol.tenant_id = %(tenant_id)s
  AND ol.id = ANY(%(line_ids)s::uuid[])
  AND ol.status NOT IN ('packed', 'shipped', 'cancelled')
ORDER BY ol.id
FOR UPDATE OF ol
"""

INSERT_CYCLE_COUNT = """
INSERT INTO cycle_counts
    (tenant_id, discrepancy_event_id, warehouse_id, location_id, sku_id,
     lot_id, reason, priority, status)
VALUES
    (%(tenant_id)s, %(discrepancy_event_id)s, %(warehouse_id)s,
     %(location_id)s, %(sku_id)s, %(lot_id)s, %(reason)s,
     %(priority)s, 'open')
RETURNING id
"""

GET_OPEN_TASK_BY_REF = """
SELECT id FROM task_queue
WHERE tenant_id = %(tenant_id)s
  AND source_ref_type = %(source_ref_type)s
  AND source_ref_id = %(source_ref_id)s
  AND task_type = %(task_type)s::task_type
  AND state IN ('queued','assigned','accepted','in_progress','blocked')
ORDER BY enqueued_at DESC
LIMIT 1
"""
