-- =============================================================================
-- Hybrid / 3PL WMS 2026 — PostgreSQL 16+ initial schema
-- Run as a migration superuser or as wms_migration (BYPASSRLS).
-- Application connections MUST use wms_app, which is subject to RLS.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA IF NOT EXISTS wms;
SET search_path = wms, public;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wms_app') THEN
        CREATE ROLE wms_app NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wms_migration') THEN
        CREATE ROLE wms_migration NOLOGIN BYPASSRLS;
    END IF;
END $$;

ALTER ROLE wms_app SET search_path = wms, public;
ALTER ROLE wms_migration SET search_path = wms, public;

CREATE OR REPLACE FUNCTION app_current_tenant_id() RETURNS UUID
LANGUAGE sql STABLE
SET search_path = wms, pg_temp
AS $$
    SELECT NULLIF(current_setting('app.current_tenant_id', TRUE), '')::UUID;
$$;

DO $$
DECLARE
    ddl TEXT;
BEGIN
    FOREACH ddl IN ARRAY ARRAY[
        $ddl$CREATE TYPE tenant_status AS ENUM ('onboarding','active','suspended','offboarded')$ddl$,
        $ddl$CREATE TYPE order_mode AS ENUM ('B2C','B2B','HYBRID')$ddl$,
        $ddl$CREATE TYPE order_status AS ENUM ('received','routing','allocated','picking','packed','shipped','cancelled','exception')$ddl$,
        $ddl$CREATE TYPE line_status AS ENUM ('open','allocated','picking','picked','packed','shipped','backordered','cancelled')$ddl$,
        $ddl$CREATE TYPE stock_status AS ENUM ('available','allocated','damaged','qc_hold','quarantine','in_transit','consumed')$ddl$,
        $ddl$CREATE TYPE location_status AS ENUM ('active','quarantine','blocked','maintenance','disabled')$ddl$,
        $ddl$CREATE TYPE location_type AS ENUM ('piece_pick','pallet','bulk','cross_dock','staging','qc','dock','returns','quarantine')$ddl$,
        $ddl$CREATE TYPE handling_unit_type AS ENUM ('EACH','CARTON','TOTE','PALLET','CONTAINER')$ddl$,
        $ddl$CREATE TYPE hu_status AS ENUM ('available','allocated','in_transit','damaged','quarantine','consumed')$ddl$,
        $ddl$CREATE TYPE equipment_type AS ENUM ('RF_SCANNER','CART','AMR','FORKLIFT','REACH_TRUCK','PALLET_JACK','AGV','MANUAL')$ddl$,
        $ddl$CREATE TYPE task_type AS ENUM ('PICK_PIECE','PICK_CLUSTER','PICK_ZONE','PICK_MULTI_BATCH','PALLET_PICK','PALLET_PUTAWAY','CROSS_DOCK_MOVE','CYCLE_COUNT','REPLENISHMENT','VAS')$ddl$,
        $ddl$CREATE TYPE task_state AS ENUM ('queued','assigned','accepted','in_progress','completed','cancelled','failed','blocked')$ddl$,
        $ddl$CREATE TYPE wave_status AS ENUM ('planned','released','picking','packed','closed','cancelled')$ddl$,
        $ddl$CREATE TYPE picking_strategy AS ENUM ('WAVE','CLUSTER','ZONE','MULTI_BATCH')$ddl$,
        $ddl$CREATE TYPE storage_basis AS ENUM ('PALLET_POSITION','CUBIC_FOOT','MAX_OF_BOTH','TIERED')$ddl$,
        $ddl$CREATE TYPE worker_status AS ENUM ('off_shift','available','working','break','blocked')$ddl$,
        $ddl$CREATE TYPE discrepancy_source AS ENUM ('AMR','RF','WMS','API','IOT')$ddl$
    ] LOOP
        BEGIN
            EXECUTE ddl;
        EXCEPTION WHEN duplicate_object THEN
            NULL;
        END;
    END LOOP;
END $$;

-- -----------------------------------------------------------------------------
-- Identity, access, and tenant-specific workflow segmentation
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tenants (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code                     TEXT NOT NULL UNIQUE CHECK (code ~ '^[A-Z0-9_-]{2,32}$'),
    legal_name               TEXT NOT NULL,
    status                   tenant_status NOT NULL DEFAULT 'active',
    default_currency         CHAR(3) NOT NULL DEFAULT 'AED' CHECK (default_currency ~ '^[A-Z]{3}$'),
    timezone                 TEXT NOT NULL DEFAULT 'Asia/Dubai',
    data_region              TEXT NOT NULL DEFAULT 'me-central-1',
    isolation_tier           TEXT NOT NULL DEFAULT 'RLS' CHECK (isolation_tier IN ('RLS','DEDICATED_SCHEMA','DEDICATED_DB')),
    billing_cutoff_local     TIME NOT NULL DEFAULT TIME '00:00',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0)
);

CREATE TABLE IF NOT EXISTS warehouses (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code                     TEXT NOT NULL UNIQUE CHECK (code ~ '^[A-Z0-9_-]{2,32}$'),
    name                     TEXT NOT NULL,
    timezone                 TEXT NOT NULL DEFAULT 'Asia/Dubai',
    address                  JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(address) = 'object'),
    is_active                BOOLEAN NOT NULL DEFAULT TRUE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0)
);

CREATE TABLE IF NOT EXISTS tenant_warehouse_access (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    warehouse_id             UUID NOT NULL REFERENCES warehouses(id) ON DELETE CASCADE,
    can_read                 BOOLEAN NOT NULL DEFAULT TRUE,
    can_write                BOOLEAN NOT NULL DEFAULT TRUE,
    can_bill                 BOOLEAN NOT NULL DEFAULT FALSE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS tenant_api_clients (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    client_id                TEXT NOT NULL UNIQUE CHECK (client_id ~ '^[A-Za-z0-9_.-]{3,96}$'),
    secret_hash              TEXT NOT NULL CHECK (length(secret_hash) >= 32),
    scopes                   TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    allowed_modes            order_mode[] NOT NULL DEFAULT ARRAY['B2C','B2B','HYBRID']::order_mode[],
    allowed_cidrs            CIDR[] NOT NULL DEFAULT ARRAY[]::CIDR[],
    rate_limit_rps           INTEGER NOT NULL DEFAULT 50 CHECK (rate_limit_rps > 0),
    status                   TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended','revoked')),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at               TIMESTAMPTZ,
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    CHECK (revoked_at IS NULL OR status = 'revoked')
);
CREATE INDEX IF NOT EXISTS ix_api_clients_tenant ON tenant_api_clients(tenant_id, status, client_id);

CREATE TABLE IF NOT EXISTS workflow_definitions (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    workflow_key             TEXT NOT NULL CHECK (workflow_key IN ('fulfillment','exceptions','billing','vas','slotting')),
    version                  INTEGER NOT NULL CHECK (version > 0),
    spec                     JSONB NOT NULL CHECK (jsonb_typeof(spec) = 'object'),
    checksum                 TEXT NOT NULL CHECK (checksum ~ '^[a-f0-9]{64}$'),
    is_active                BOOLEAN NOT NULL DEFAULT FALSE,
    effective_from           TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, workflow_key, version)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_active
    ON workflow_definitions(tenant_id, workflow_key)
    WHERE is_active;
CREATE INDEX IF NOT EXISTS ix_workflow_effective
    ON workflow_definitions(tenant_id, workflow_key, effective_from DESC, version DESC);

-- -----------------------------------------------------------------------------
-- Master data and 3D location model
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS skus (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    sku_code                 TEXT NOT NULL CHECK (length(sku_code) BETWEEN 1 AND 96),
    description              TEXT NOT NULL,
    uom                      TEXT NOT NULL DEFAULT 'EA' CHECK (uom IN ('EA','KG','L','M','BOX')),
    length_cm                NUMERIC(12,3) NOT NULL CHECK (length_cm > 0),
    width_cm                 NUMERIC(12,3) NOT NULL CHECK (width_cm > 0),
    height_cm                NUMERIC(12,3) NOT NULL CHECK (height_cm > 0),
    unit_volume_cm3          NUMERIC(20,3) GENERATED ALWAYS AS (length_cm * width_cm * height_cm) STORED,
    unit_weight_kg           NUMERIC(14,4) NOT NULL CHECK (unit_weight_kg > 0),
    units_per_carton         INTEGER NOT NULL DEFAULT 1 CHECK (units_per_carton > 0),
    units_per_pallet         INTEGER NOT NULL DEFAULT 1 CHECK (units_per_pallet > 0),
    pallet_tie               INTEGER NOT NULL DEFAULT 1 CHECK (pallet_tie > 0),
    pallet_high              INTEGER NOT NULL DEFAULT 1 CHECK (pallet_high > 0),
    hazardous                BOOLEAN NOT NULL DEFAULT FALSE,
    hazmat_class             TEXT,
    temperature_band         TEXT NOT NULL DEFAULT 'AMBIENT' CHECK (temperature_band IN ('AMBIENT','CHILLED','FROZEN','CONTROLLED')),
    velocity_class           TEXT NOT NULL DEFAULT 'C' CHECK (velocity_class IN ('A','B','C','D','X')),
    velocity_score           NUMERIC(12,4) NOT NULL DEFAULT 0 CHECK (velocity_score >= 0),
    avg_daily_picks          NUMERIC(16,4) NOT NULL DEFAULT 0 CHECK (avg_daily_picks >= 0),
    avg_daily_units          NUMERIC(16,4) NOT NULL DEFAULT 0 CHECK (avg_daily_units >= 0),
    velocity_calculated_at   TIMESTAMPTZ,
    requires_lot             BOOLEAN NOT NULL DEFAULT FALSE,
    requires_serial          BOOLEAN NOT NULL DEFAULT FALSE,
    shelf_life_days          INTEGER CHECK (shelf_life_days IS NULL OR shelf_life_days > 0),
    status                   TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','inactive','blocked')),
    metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, sku_code),
    CHECK (unit_volume_cm3 > 0),
    CHECK (units_per_pallet >= units_per_carton),
    CHECK ((NOT hazardous) OR hazmat_class IS NOT NULL),
    CHECK (hazmat_class IS NULL OR hazardous)
);
CREATE INDEX IF NOT EXISTS ix_skus_slotting
    ON skus(tenant_id, status, velocity_class, velocity_score DESC, avg_daily_picks DESC);
CREATE INDEX IF NOT EXISTS ix_skus_hazmat
    ON skus(tenant_id, hazardous, temperature_band, status);

CREATE TABLE IF NOT EXISTS lots (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    sku_id                   UUID NOT NULL,
    lot_code                 TEXT NOT NULL CHECK (length(lot_code) BETWEEN 1 AND 96),
    manufactured_on          DATE,
    expires_on               DATE,
    attributes               JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(attributes) = 'object'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, sku_id, lot_code),
    UNIQUE (tenant_id, sku_id, id),
    FOREIGN KEY (tenant_id, sku_id) REFERENCES skus(tenant_id, id) ON DELETE CASCADE,
    CHECK (expires_on IS NULL OR manufactured_on IS NULL OR expires_on >= manufactured_on)
);
CREATE INDEX IF NOT EXISTS ix_lots_fefo ON lots(tenant_id, sku_id, expires_on NULLS LAST, lot_code);

CREATE TABLE IF NOT EXISTS locations (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    warehouse_id             UUID NOT NULL REFERENCES warehouses(id) ON DELETE RESTRICT,
    zone                     TEXT NOT NULL CHECK (length(zone) BETWEEN 1 AND 32),
    aisle                    TEXT NOT NULL CHECK (length(aisle) BETWEEN 1 AND 32),
    bay                      TEXT NOT NULL CHECK (length(bay) BETWEEN 1 AND 32),
    level                    TEXT NOT NULL CHECK (length(level) BETWEEN 1 AND 32),
    position                 TEXT NOT NULL CHECK (length(position) BETWEEN 1 AND 32),
    barcode                  TEXT NOT NULL UNIQUE CHECK (length(barcode) BETWEEN 2 AND 128),
    location_type            location_type NOT NULL,
    status                   location_status NOT NULL DEFAULT 'active',
    exclusive_tenant_id      UUID REFERENCES tenants(id) ON DELETE RESTRICT,
    x_m                      NUMERIC(12,2) NOT NULL CHECK (x_m >= 0),
    y_m                      NUMERIC(12,2) NOT NULL CHECK (y_m >= 0),
    z_m                      NUMERIC(12,2) NOT NULL CHECK (z_m >= 0),
    travel_seq               BIGINT NOT NULL CHECK (travel_seq >= 0),
    max_volume_cm3           NUMERIC(20,3) NOT NULL CHECK (max_volume_cm3 > 0),
    max_weight_kg            NUMERIC(18,3) NOT NULL CHECK (max_weight_kg > 0),
    current_volume_cm3       NUMERIC(20,3) NOT NULL DEFAULT 0 CHECK (current_volume_cm3 >= 0),
    current_weight_kg        NUMERIC(18,3) NOT NULL DEFAULT 0 CHECK (current_weight_kg >= 0),
    available_volume_cm3     NUMERIC(20,3) GENERATED ALWAYS AS (max_volume_cm3 - current_volume_cm3) STORED,
    available_weight_kg      NUMERIC(18,3) GENERATED ALWAYS AS (max_weight_kg - current_weight_kg) STORED,
    pick_face                BOOLEAN NOT NULL DEFAULT FALSE,
    allows_mixed_lots        BOOLEAN NOT NULL DEFAULT FALSE,
    allows_mixed_skus        BOOLEAN NOT NULL DEFAULT FALSE,
    allows_mixed_tenants     BOOLEAN NOT NULL DEFAULT FALSE,
    allows_hazmat            BOOLEAN NOT NULL DEFAULT FALSE,
    temperature_band         TEXT NOT NULL DEFAULT 'AMBIENT' CHECK (temperature_band IN ('AMBIENT','CHILLED','FROZEN','CONTROLLED','ANY')),
    slot_class               TEXT NOT NULL DEFAULT 'C' CHECK (slot_class IN ('A','B','C','D','X')),
    quarantine_reason        TEXT,
    quarantined_at           TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    UNIQUE (warehouse_id, zone, aisle, bay, level, position),
    UNIQUE (warehouse_id, id),
    CHECK (current_volume_cm3 <= max_volume_cm3),
    CHECK (current_weight_kg <= max_weight_kg),
    CHECK ((status = 'quarantine') = (quarantined_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS ix_locations_routing
    ON locations(warehouse_id, status, location_type, zone, aisle, bay, level, position, travel_seq);
CREATE INDEX IF NOT EXISTS ix_locations_capacity
    ON locations(warehouse_id, status, location_type, available_volume_cm3 DESC, available_weight_kg DESC, travel_seq);
CREATE INDEX IF NOT EXISTS ix_locations_tenant
    ON locations(exclusive_tenant_id, warehouse_id, status) WHERE exclusive_tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_locations_quarantine
    ON locations(warehouse_id, status, quarantined_at DESC) WHERE status = 'quarantine';

CREATE TABLE IF NOT EXISTS location_tenant_policies (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    location_id              UUID NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    allowed_modes            order_mode[] NOT NULL DEFAULT ARRAY['B2C','B2B','HYBRID']::order_mode[],
    max_tenant_mix           INTEGER NOT NULL DEFAULT 1 CHECK (max_tenant_mix > 0),
    priority                 INTEGER NOT NULL DEFAULT 100 CHECK (priority >= 0),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, location_id)
);
CREATE INDEX IF NOT EXISTS ix_location_policies_location ON location_tenant_policies(location_id, tenant_id, priority DESC);

CREATE OR REPLACE FUNCTION location_travel_cost(
    p_from_x NUMERIC,
    p_from_y NUMERIC,
    p_from_z NUMERIC,
    p_to_x NUMERIC,
    p_to_y NUMERIC,
    p_to_z NUMERIC,
    p_from_zone TEXT DEFAULT NULL,
    p_to_zone TEXT DEFAULT NULL
) RETURNS NUMERIC
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
    SELECT abs(p_to_x - p_from_x)
         + abs(p_to_y - p_from_y)
         + 3.0 * abs(p_to_z - p_from_z)
         + CASE WHEN p_from_zone IS DISTINCT FROM p_to_zone THEN 25.0 ELSE 0.0 END;
$$;

CREATE OR REPLACE FUNCTION location_slot_compatibility(
    p_location_id UUID,
    p_tenant_id UUID,
    p_sku_id UUID,
    p_lot_id UUID
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = wms, pg_temp
AS $$
DECLARE
    v_location locations%ROWTYPE;
BEGIN
    SELECT * INTO v_location FROM locations WHERE id = p_location_id;
    IF NOT FOUND OR v_location.status <> 'active' THEN
        RETURN FALSE;
    END IF;
    IF NOT (v_location.exclusive_tenant_id IS NULL OR v_location.exclusive_tenant_id = p_tenant_id) THEN
        RETURN FALSE;
    END IF;
    IF NOT v_location.allows_mixed_skus AND EXISTS (
        SELECT 1 FROM inventory_bucket_headers h
        WHERE h.location_id = p_location_id AND h.sku_id <> p_sku_id
    ) THEN
        RETURN FALSE;
    END IF;
    IF NOT v_location.allows_mixed_tenants AND EXISTS (
        SELECT 1 FROM inventory_bucket_headers h
        WHERE h.location_id = p_location_id AND h.tenant_id <> p_tenant_id
    ) THEN
        RETURN FALSE;
    END IF;
    IF NOT v_location.allows_mixed_lots AND EXISTS (
        SELECT 1 FROM inventory_bucket_headers h
        WHERE h.location_id = p_location_id AND h.lot_id IS DISTINCT FROM p_lot_id
    ) THEN
        RETURN FALSE;
    END IF;
    RETURN TRUE;
END $$;

-- -----------------------------------------------------------------------------
-- Hybrid inventory engine
-- The header is the lockable natural bucket. Balances are status partitions.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS handling_units (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    barcode                  TEXT NOT NULL CHECK (length(barcode) BETWEEN 2 AND 128),
    hu_type                  handling_unit_type NOT NULL,
    status                   hu_status NOT NULL DEFAULT 'available',
    warehouse_id             UUID NOT NULL,
    location_id              UUID NOT NULL,
    sku_id                   UUID NOT NULL,
    lot_id                   UUID,
    qty                      NUMERIC(18,4) NOT NULL CHECK (qty >= 0),
    full_pallet_qty          NUMERIC(18,4) NOT NULL CHECK (full_pallet_qty > 0),
    is_full                  BOOLEAN NOT NULL DEFAULT FALSE,
    volume_cm3               NUMERIC(20,3) NOT NULL DEFAULT 0 CHECK (volume_cm3 >= 0),
    tare_weight_kg           NUMERIC(14,4) NOT NULL DEFAULT 0 CHECK (tare_weight_kg >= 0),
    attributes               JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(attributes) = 'object'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, barcode),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, sku_id) REFERENCES skus(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id, lot_id) REFERENCES lots(tenant_id, sku_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (warehouse_id, location_id) REFERENCES locations(warehouse_id, id) ON DELETE RESTRICT,
    CHECK (NOT is_full OR qty = full_pallet_qty),
    CHECK (hu_type <> 'PALLET' OR full_pallet_qty > 0)
);
CREATE INDEX IF NOT EXISTS ix_handling_units_full_pallet
    ON handling_units(tenant_id, sku_id, lot_id, status, is_full, hu_type, location_id)
    WHERE status = 'available' AND is_full AND hu_type = 'PALLET';
CREATE INDEX IF NOT EXISTS ix_handling_units_location
    ON handling_units(warehouse_id, location_id, tenant_id, status);

CREATE TABLE IF NOT EXISTS inventory_bucket_headers (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    sku_id                   UUID NOT NULL,
    warehouse_id             UUID NOT NULL,
    location_id              UUID NOT NULL,
    lot_id                   UUID,
    serial_number            TEXT,
    expiration_date          DATE,
    handling_unit_id         UUID,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, id),
    CONSTRAINT uq_bucket_natural UNIQUE NULLS NOT DISTINCT
        (tenant_id, sku_id, warehouse_id, location_id, lot_id, serial_number, expiration_date, handling_unit_id),
    FOREIGN KEY (tenant_id, sku_id) REFERENCES skus(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id, lot_id) REFERENCES lots(tenant_id, sku_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (warehouse_id, location_id) REFERENCES locations(warehouse_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, handling_unit_id) REFERENCES handling_units(tenant_id, id) ON DELETE RESTRICT,
    CHECK (serial_number IS NULL OR length(serial_number) > 0),
    CHECK (expiration_date IS NULL OR expiration_date >= DATE '2000-01-01')
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_serial_per_tenant
    ON inventory_bucket_headers(tenant_id, serial_number)
    WHERE serial_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_bucket_sku_location_lot
    ON inventory_bucket_headers(tenant_id, sku_id, location_id, lot_id, expiration_date NULLS LAST, serial_number);
CREATE INDEX IF NOT EXISTS ix_bucket_location
    ON inventory_bucket_headers(warehouse_id, location_id, tenant_id, sku_id, lot_id);
CREATE INDEX IF NOT EXISTS ix_bucket_hu
    ON inventory_bucket_headers(tenant_id, handling_unit_id) WHERE handling_unit_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS inventory_balances (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    bucket_id                UUID NOT NULL,
    stock_status             stock_status NOT NULL,
    qty                      NUMERIC(18,4) NOT NULL CHECK (qty >= 0),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, bucket_id, stock_status),
    FOREIGN KEY (tenant_id, bucket_id) REFERENCES inventory_bucket_headers(tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_balances_available
    ON inventory_balances(tenant_id, stock_status, bucket_id, qty DESC)
    WHERE stock_status = 'available' AND qty > 0;
CREATE INDEX IF NOT EXISTS ix_balances_nonconsumed
    ON inventory_balances(tenant_id, bucket_id, stock_status, qty)
    WHERE stock_status IN ('available','allocated','damaged','qc_hold','quarantine');

COMMENT ON TABLE inventory_bucket_headers IS
    'Lockable natural stock bucket: tenant + SKU + 3D location + lot + serial + expiry + handling unit.';
COMMENT ON TABLE inventory_balances IS
    'Status-keyed quantity for a bucket. Allocation locks the header and balance rows, then atomically moves qty from available to allocated.';

CREATE OR REPLACE VIEW inventory_available
WITH (security_invoker = true, security_barrier = true) AS
SELECT
    h.tenant_id,
    h.id                AS bucket_id,
    h.sku_id,
    h.lot_id,
    h.serial_number,
    h.expiration_date,
    h.handling_unit_id,
    h.warehouse_id,
    h.location_id,
    b.qty               AS available_qty,
    b.version           AS balance_version,
    h.version           AS header_version,
    l.barcode           AS location_barcode,
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
    l.allows_hazmat,
    l.temperature_band  AS location_temperature_band,
    s.sku_code,
    s.hazardous,
    s.temperature_band  AS sku_temperature_band,
    s.requires_lot,
    s.requires_serial,
    s.velocity_score,
    s.unit_volume_cm3,
    s.unit_weight_kg
FROM inventory_balances b
JOIN inventory_bucket_headers h
  ON h.tenant_id = b.tenant_id AND h.id = b.bucket_id
JOIN locations l
  ON l.id = h.location_id AND l.warehouse_id = h.warehouse_id
JOIN skus s
  ON s.tenant_id = h.tenant_id AND s.id = h.sku_id
WHERE b.stock_status = 'available'
  AND b.qty > 0
  AND l.status = 'active'
  AND s.status = 'active';

-- -----------------------------------------------------------------------------
-- Orders, waves, allocations, tasks, labour
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS orders (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    external_order_id        TEXT NOT NULL CHECK (length(external_order_id) BETWEEN 1 AND 128),
    warehouse_id             UUID NOT NULL REFERENCES warehouses(id) ON DELETE RESTRICT,
    mode                     order_mode NOT NULL,
    status                   order_status NOT NULL DEFAULT 'received',
    priority                 INTEGER NOT NULL DEFAULT 100 CHECK (priority BETWEEN 0 AND 1000),
    sla_ship_at              TIMESTAMPTZ,
    carrier_code             TEXT,
    customer_ref             TEXT,
    workflow_version         INTEGER NOT NULL DEFAULT 1 CHECK (workflow_version > 0),
    metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    idempotency_key          TEXT NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, external_order_id),
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS ix_orders_routing
    ON orders(tenant_id, warehouse_id, status, mode, priority DESC, sla_ship_at NULLS LAST, created_at);
CREATE INDEX IF NOT EXISTS ix_orders_cutoff
    ON orders(tenant_id, carrier_code, sla_ship_at, status) WHERE sla_ship_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS order_lines (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    order_id                 UUID NOT NULL,
    line_no                  INTEGER NOT NULL CHECK (line_no > 0),
    sku_id                   UUID NOT NULL,
    lot_id                   UUID,
    mode                     order_mode NOT NULL,
    fulfillment_unit         TEXT NOT NULL DEFAULT 'EACH' CHECK (fulfillment_unit IN ('EACH','CARTON','PALLET')),
    required_qty             NUMERIC(18,4) NOT NULL CHECK (required_qty > 0),
    allocated_qty            NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (allocated_qty >= 0),
    picked_qty               NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (picked_qty >= 0),
    shipped_qty              NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (shipped_qty >= 0),
    status                   line_status NOT NULL DEFAULT 'open',
    require_same_lot         BOOLEAN NOT NULL DEFAULT TRUE,
    expiry_min               DATE,
    expiry_max               DATE,
    metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, order_id, line_no),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, sku_id) REFERENCES skus(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id, lot_id) REFERENCES lots(tenant_id, sku_id, id) ON DELETE RESTRICT,
    CHECK (allocated_qty <= required_qty),
    CHECK (picked_qty <= required_qty),
    CHECK (shipped_qty <= picked_qty),
    CHECK (expiry_min IS NULL OR expiry_max IS NULL OR expiry_min <= expiry_max),
    CHECK (status <> 'allocated' OR allocated_qty > 0)
);
CREATE INDEX IF NOT EXISTS ix_order_lines_open
    ON order_lines(tenant_id, status, mode, sku_id, lot_id, required_qty);
CREATE INDEX IF NOT EXISTS ix_order_lines_order
    ON order_lines(tenant_id, order_id, line_no);

CREATE TABLE IF NOT EXISTS waves (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    warehouse_id             UUID NOT NULL REFERENCES warehouses(id) ON DELETE RESTRICT,
    wave_code                TEXT NOT NULL CHECK (length(wave_code) BETWEEN 4 AND 128),
    strategy                 picking_strategy NOT NULL,
    status                   wave_status NOT NULL DEFAULT 'planned',
    carrier_code             TEXT,
    cutoff_bucket            TIMESTAMPTZ,
    zone                     TEXT,
    affinity_score           NUMERIC(10,4) NOT NULL DEFAULT 0 CHECK (affinity_score >= 0),
    estimated_travel_m       NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (estimated_travel_m >= 0),
    priority                 INTEGER NOT NULL DEFAULT 100 CHECK (priority BETWEEN 0 AND 1000),
    release_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    cutoff_at                TIMESTAMPTZ,
    max_orders               INTEGER NOT NULL DEFAULT 200 CHECK (max_orders > 0),
    max_lines                INTEGER NOT NULL DEFAULT 1000 CHECK (max_lines > 0),
    max_units                NUMERIC(18,4) NOT NULL DEFAULT 10000 CHECK (max_units > 0),
    order_count              INTEGER NOT NULL DEFAULT 0 CHECK (order_count >= 0),
    line_count               INTEGER NOT NULL DEFAULT 0 CHECK (line_count >= 0),
    unit_count               NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (unit_count >= 0),
    metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, wave_code),
    UNIQUE (tenant_id, id),
    CHECK (cutoff_at IS NULL OR cutoff_at >= release_at)
);
CREATE INDEX IF NOT EXISTS ix_waves_open
    ON waves(tenant_id, warehouse_id, status, strategy, priority DESC, release_at, cutoff_at, created_at)
    WHERE status IN ('planned','released');

CREATE TABLE IF NOT EXISTS wave_orders (
    tenant_id                UUID NOT NULL,
    wave_id                  UUID NOT NULL,
    order_id                 UUID NOT NULL,
    sequence                 INTEGER NOT NULL CHECK (sequence > 0),
    PRIMARY KEY (tenant_id, wave_id, order_id),
    FOREIGN KEY (tenant_id, wave_id) REFERENCES waves(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders(tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_wave_orders_order ON wave_orders(tenant_id, order_id, wave_id);

CREATE TABLE IF NOT EXISTS wave_order_lines (
    tenant_id                UUID NOT NULL,
    wave_id                  UUID NOT NULL,
    order_line_id            UUID NOT NULL,
    sequence                 INTEGER NOT NULL CHECK (sequence > 0),
    PRIMARY KEY (tenant_id, wave_id, order_line_id),
    FOREIGN KEY (tenant_id, wave_id) REFERENCES waves(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, order_line_id) REFERENCES order_lines(tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_wave_order_lines_line ON wave_order_lines(tenant_id, order_line_id, wave_id);

CREATE TABLE IF NOT EXISTS wave_route_stops (
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id                UUID NOT NULL,
    wave_id                  UUID NOT NULL,
    warehouse_id             UUID NOT NULL,
    location_id              UUID NOT NULL,
    sku_id                   UUID NOT NULL,
    lot_id                   UUID,
    qty                      NUMERIC(18,4) NOT NULL CHECK (qty > 0),
    stop_seq                 INTEGER NOT NULL CHECK (stop_seq > 0),
    x_m                      NUMERIC(12,2) NOT NULL,
    y_m                      NUMERIC(12,2) NOT NULL,
    z_m                      NUMERIC(12,2) NOT NULL,
    zone                     TEXT NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT uq_wave_route_stop UNIQUE NULLS NOT DISTINCT
        (tenant_id, wave_id, location_id, sku_id, lot_id),
    CONSTRAINT uq_wave_route_sequence UNIQUE
        (tenant_id, wave_id, stop_seq),
    FOREIGN KEY (tenant_id, wave_id) REFERENCES waves(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (warehouse_id, location_id) REFERENCES locations(warehouse_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id) REFERENCES skus(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id, lot_id) REFERENCES lots(tenant_id, sku_id, id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS ix_wave_route_stops_wave ON wave_route_stops(tenant_id, wave_id, stop_seq);
CREATE INDEX IF NOT EXISTS ix_wave_route_stops_location ON wave_route_stops(tenant_id, location_id, wave_id);

CREATE TABLE IF NOT EXISTS inventory_allocations (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    order_line_id            UUID NOT NULL,
    bucket_id                UUID NOT NULL,
    wave_id                  UUID,
    sku_id                   UUID NOT NULL,
    lot_id                   UUID,
    warehouse_id             UUID NOT NULL,
    location_id              UUID NOT NULL,
    handling_unit_id         UUID,
    qty                      NUMERIC(18,4) NOT NULL CHECK (qty > 0),
    status                   TEXT NOT NULL DEFAULT 'reserved' CHECK (status IN ('reserved','picked','released_discrepancy','cancelled','consumed')),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, order_line_id) REFERENCES order_lines(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, bucket_id) REFERENCES inventory_bucket_headers(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, wave_id) REFERENCES waves(tenant_id, id) ON DELETE SET NULL,
    FOREIGN KEY (warehouse_id, location_id) REFERENCES locations(warehouse_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id) REFERENCES skus(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id, lot_id) REFERENCES lots(tenant_id, sku_id, id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_reserved
    ON inventory_allocations(tenant_id, order_line_id, bucket_id)
    WHERE status = 'reserved';
CREATE INDEX IF NOT EXISTS ix_allocations_location
    ON inventory_allocations(tenant_id, location_id, status, bucket_id, order_line_id);
CREATE INDEX IF NOT EXISTS ix_allocations_wave
    ON inventory_allocations(tenant_id, wave_id, status, order_line_id);
CREATE INDEX IF NOT EXISTS ix_allocations_line
    ON inventory_allocations(tenant_id, order_line_id, status, qty);

CREATE TABLE IF NOT EXISTS workers (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    employee_code            TEXT NOT NULL CHECK (length(employee_code) BETWEEN 1 AND 64),
    display_name             TEXT NOT NULL,
    warehouse_id             UUID NOT NULL REFERENCES warehouses(id) ON DELETE RESTRICT,
    status                   worker_status NOT NULL DEFAULT 'off_shift',
    active_equipment         equipment_type NOT NULL DEFAULT 'RF_SCANNER',
    current_zone             TEXT,
    current_x_m              NUMERIC(12,2) NOT NULL DEFAULT 0,
    current_y_m              NUMERIC(12,2) NOT NULL DEFAULT 0,
    max_concurrent_tasks     INTEGER NOT NULL DEFAULT 1 CHECK (max_concurrent_tasks > 0),
    skills                   TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    last_heartbeat_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, employee_code),
    UNIQUE (tenant_id, id)
);
CREATE INDEX IF NOT EXISTS ix_workers_dispatch
    ON workers(tenant_id, warehouse_id, status, active_equipment, current_zone, last_heartbeat_at DESC)
    WHERE status IN ('available','working');

CREATE TABLE IF NOT EXISTS worker_zone_permissions (
    tenant_id                UUID NOT NULL,
    worker_id                UUID NOT NULL,
    warehouse_id             UUID NOT NULL,
    zone                     TEXT NOT NULL,
    PRIMARY KEY (tenant_id, worker_id, warehouse_id, zone),
    FOREIGN KEY (tenant_id, worker_id) REFERENCES workers(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_queue (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    task_type                task_type NOT NULL,
    state                    task_state NOT NULL DEFAULT 'queued',
    priority                 INTEGER NOT NULL DEFAULT 100 CHECK (priority BETWEEN 0 AND 1000),
    warehouse_id             UUID NOT NULL REFERENCES warehouses(id) ON DELETE RESTRICT,
    required_equipment       equipment_type[] NOT NULL DEFAULT ARRAY['RF_SCANNER']::equipment_type[],
    required_zone            TEXT,
    location_id              UUID REFERENCES locations(id) ON DELETE RESTRICT,
    source_ref_type          TEXT NOT NULL CHECK (length(source_ref_type) BETWEEN 2 AND 64),
    source_ref_id            UUID NOT NULL,
    wave_id                  UUID,
    order_id                 UUID,
    payload                  JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
    not_before               TIMESTAMPTZ NOT NULL DEFAULT now(),
    enqueued_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at               TIMESTAMPTZ,
    assigned_worker_id       UUID,
    attempt_count            INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts             INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (tenant_id, wave_id) REFERENCES waves(tenant_id, id) ON DELETE SET NULL,
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders(tenant_id, id) ON DELETE SET NULL,
    FOREIGN KEY (tenant_id, assigned_worker_id) REFERENCES workers(tenant_id, id) ON DELETE SET NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_task_queue_open_ref
    ON task_queue(tenant_id, source_ref_type, source_ref_id, task_type)
    WHERE state IN ('queued','assigned','accepted','in_progress','blocked');
CREATE INDEX IF NOT EXISTS ix_task_queue_claim
    ON task_queue(tenant_id, state, warehouse_id, required_zone, priority DESC, not_before, enqueued_at, id)
    WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS ix_task_queue_equipment
    ON task_queue USING GIN (required_equipment)
    WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS ix_task_queue_worker
    ON task_queue(tenant_id, assigned_worker_id, state, priority DESC, enqueued_at)
    WHERE assigned_worker_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS task_assignments (
    tenant_id                UUID NOT NULL,
    task_id                  UUID NOT NULL,
    worker_id                UUID NOT NULL,
    assigned_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    accepted_at              TIMESTAMPTZ,
    completed_at             TIMESTAMPTZ,
    state                    TEXT NOT NULL DEFAULT 'assigned' CHECK (state IN ('assigned','accepted','rejected','completed','cancelled')),
    score                    NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (tenant_id, task_id, worker_id),
    FOREIGN KEY (tenant_id, task_id) REFERENCES task_queue(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, worker_id) REFERENCES workers(tenant_id, id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_task_assignments_worker ON task_assignments(tenant_id, worker_id, state, assigned_at DESC);

-- -----------------------------------------------------------------------------
-- Digital twin exceptions and cycle counts
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS inventory_discrepancy_events (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    source                   discrepancy_source NOT NULL,
    source_event_id          TEXT NOT NULL CHECK (length(source_event_id) BETWEEN 1 AND 160),
    warehouse_id             UUID NOT NULL,
    location_id              UUID NOT NULL,
    sku_id                   UUID NOT NULL,
    lot_id                   UUID,
    expected_qty             NUMERIC(18,4) NOT NULL CHECK (expected_qty >= 0),
    found_qty                NUMERIC(18,4) NOT NULL CHECK (found_qty >= 0),
    wave_id                  UUID,
    observed_at              TIMESTAMPTZ NOT NULL,
    received_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    status                   TEXT NOT NULL DEFAULT 'received' CHECK (status IN ('received','processing','processed','failed')),
    processing_result        JSONB,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, source, source_event_id),
    UNIQUE (tenant_id, id),
    FOREIGN KEY (warehouse_id, location_id) REFERENCES locations(warehouse_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id) REFERENCES skus(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id, lot_id) REFERENCES lots(tenant_id, sku_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, wave_id) REFERENCES waves(tenant_id, id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS ix_discrepancy_location
    ON inventory_discrepancy_events(tenant_id, warehouse_id, location_id, sku_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS ix_discrepancy_wave
    ON inventory_discrepancy_events(tenant_id, wave_id, status) WHERE wave_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS cycle_counts (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    discrepancy_event_id     UUID NOT NULL,
    warehouse_id             UUID NOT NULL,
    location_id              UUID NOT NULL,
    sku_id                   UUID,
    lot_id                   UUID,
    reason                   TEXT NOT NULL,
    priority                 INTEGER NOT NULL DEFAULT 1000 CHECK (priority BETWEEN 0 AND 1000),
    status                   TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','assigned','in_progress','completed','cancelled')),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, discrepancy_event_id),
    FOREIGN KEY (tenant_id, discrepancy_event_id) REFERENCES inventory_discrepancy_events(tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (warehouse_id, location_id) REFERENCES locations(warehouse_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id) REFERENCES skus(tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, sku_id, lot_id) REFERENCES lots(tenant_id, sku_id, id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS ix_cycle_counts_open
    ON cycle_counts(tenant_id, status, priority DESC, created_at)
    WHERE status IN ('open','assigned','in_progress');

-- -----------------------------------------------------------------------------
-- 3PL contracts, VAS, billing
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tenant_contracts (
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    contract_code            TEXT NOT NULL CHECK (length(contract_code) BETWEEN 2 AND 96),
    currency                 CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    valid_from               DATE NOT NULL,
    valid_to                 DATE,
    timezone                 TEXT NOT NULL DEFAULT 'Asia/Dubai',
    billing_cycle            TEXT NOT NULL DEFAULT 'MONTHLY' CHECK (billing_cycle IN ('DAILY','WEEKLY','MONTHLY')),
    storage_basis            storage_basis NOT NULL DEFAULT 'PALLET_POSITION',
    storage_free_days        INTEGER NOT NULL DEFAULT 0 CHECK (storage_free_days >= 0),
    pallet_position_volume_cm3 NUMERIC(20,3) NOT NULL DEFAULT 1800000 CHECK (pallet_position_volume_cm3 > 0),
    cm3_per_cubic_foot       NUMERIC(18,6) NOT NULL DEFAULT 28316.846592 CHECK (cm3_per_cubic_foot > 0),
    rounding_mode            TEXT NOT NULL DEFAULT 'HALF_UP' CHECK (rounding_mode IN ('HALF_UP','HALF_EVEN','CEILING','FLOOR')),
    tax_mode                 TEXT NOT NULL DEFAULT 'EXCLUSIVE' CHECK (tax_mode IN ('EXCLUSIVE','INCLUSIVE','EXEMPT')),
    status                   TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('draft','active','suspended','closed')),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    version                  BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, contract_code),
    UNIQUE (tenant_id, id),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    EXCLUDE USING gist (
        tenant_id WITH =,
        daterange(valid_from, valid_to, '[)') WITH &&
    ) WHERE (status = 'active')
);
CREATE INDEX IF NOT EXISTS ix_contract_valid
    ON tenant_contracts(tenant_id, status, valid_from DESC, valid_to)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS contract_storage_rates (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    contract_id              UUID NOT NULL,
    effective_from           DATE NOT NULL,
    effective_to             DATE,
    basis                    storage_basis NOT NULL,
    location_type            location_type,
    rate_per_pallet_day      NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (rate_per_pallet_day >= 0),
    rate_per_cubic_foot_day  NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (rate_per_cubic_foot_day >= 0),
    hazardous_multiplier     NUMERIC(10,4) NOT NULL DEFAULT 1 CHECK (hazardous_multiplier >= 1),
    chilled_multiplier       NUMERIC(10,4) NOT NULL DEFAULT 1 CHECK (chilled_multiplier >= 1),
    minimum_charge           NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (minimum_charge >= 0),
    tiers                    JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(tiers) = 'array'),
    currency                 CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE NULLS NOT DISTINCT (tenant_id, contract_id, basis, location_type, effective_from),
    FOREIGN KEY (tenant_id, contract_id) REFERENCES tenant_contracts(tenant_id, id) ON DELETE CASCADE,
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);
CREATE INDEX IF NOT EXISTS ix_storage_rate_lookup
    ON contract_storage_rates(tenant_id, contract_id, basis, effective_from DESC, effective_to)
    WHERE location_type IS NULL;
CREATE INDEX IF NOT EXISTS ix_storage_rate_lookup_type
    ON contract_storage_rates(tenant_id, contract_id, basis, location_type, effective_from DESC, effective_to)
    WHERE location_type IS NOT NULL;

CREATE TABLE IF NOT EXISTS contract_pick_rates (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    contract_id              UUID NOT NULL,
    effective_from           DATE NOT NULL,
    effective_to             DATE,
    mode                     order_mode NOT NULL,
    rate_per_piece           NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (rate_per_piece >= 0),
    rate_per_pallet          NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (rate_per_pallet >= 0),
    rate_per_order           NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (rate_per_order >= 0),
    rate_per_line            NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (rate_per_line >= 0),
    first_piece_surcharge    NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (first_piece_surcharge >= 0),
    hazardous_surcharge_per_piece NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (hazardous_surcharge_per_piece >= 0),
    oversized_surcharge_per_piece NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (oversized_surcharge_per_piece >= 0),
    same_day_surcharge_pct   NUMERIC(10,4) NOT NULL DEFAULT 0 CHECK (same_day_surcharge_pct >= 0),
    cross_dock_fee_per_pallet NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (cross_dock_fee_per_pallet >= 0),
    minimum_charge           NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (minimum_charge >= 0),
    tiers                    JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(tiers) = 'array'),
    currency                 CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, contract_id, mode, effective_from),
    FOREIGN KEY (tenant_id, contract_id) REFERENCES tenant_contracts(tenant_id, id) ON DELETE CASCADE,
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);
CREATE INDEX IF NOT EXISTS ix_pick_rate_lookup
    ON contract_pick_rates(tenant_id, contract_id, mode, effective_from DESC, effective_to);

CREATE TABLE IF NOT EXISTS vas_rate_cards (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    contract_id              UUID NOT NULL,
    vas_code                 TEXT NOT NULL CHECK (vas_code IN ('KITTING','LABELING','GIFT_WRAP','RELABEL','INSPECTION','ASSEMBLY','SERIAL_CAPTURE','CUSTOM')),
    effective_from           DATE NOT NULL,
    effective_to             DATE,
    unit_basis               TEXT NOT NULL CHECK (unit_basis IN ('EACH','ORDER','MINUTE','PALLET','CARTON')),
    rate_per_unit            NUMERIC(18,6) NOT NULL CHECK (rate_per_unit >= 0),
    setup_fee                NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (setup_fee >= 0),
    material_fee_per_unit    NUMERIC(18,6) NOT NULL DEFAULT 0 CHECK (material_fee_per_unit >= 0),
    minimum_charge           NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (minimum_charge >= 0),
    complexity_multiplier    NUMERIC(10,4) NOT NULL DEFAULT 1 CHECK (complexity_multiplier >= 1),
    tiers                    JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(tiers) = 'array'),
    currency                 CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, contract_id, vas_code, effective_from),
    FOREIGN KEY (tenant_id, contract_id) REFERENCES tenant_contracts(tenant_id, id) ON DELETE CASCADE,
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);
CREATE INDEX IF NOT EXISTS ix_vas_rate_lookup
    ON vas_rate_cards(tenant_id, contract_id, vas_code, effective_from DESC, effective_to);

CREATE TABLE IF NOT EXISTS vas_transactions (
    tenant_id                UUID NOT NULL,
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    order_id                 UUID,
    order_line_id            UUID,
    vas_code                 TEXT NOT NULL,
    quantity                 NUMERIC(18,4) NOT NULL DEFAULT 1 CHECK (quantity > 0),
    minutes                  NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (minutes >= 0),
    pallets                  NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (pallets >= 0),
    cartons                  NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (cartons >= 0),
    order_count              NUMERIC(18,4) NOT NULL DEFAULT 1 CHECK (order_count > 0),
    occurred_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    attributes               JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(attributes) = 'object'),
    idempotency_key          TEXT NOT NULL,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders(tenant_id, id) ON DELETE SET NULL,
    FOREIGN KEY (tenant_id, order_line_id) REFERENCES order_lines(tenant_id, id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS ix_vas_transactions_time
    ON vas_transactions(tenant_id, occurred_at DESC, vas_code);

CREATE TABLE IF NOT EXISTS billing_events (
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id                UUID NOT NULL,
    contract_id              UUID NOT NULL,
    source_type              TEXT NOT NULL CHECK (source_type IN ('STORAGE_SNAPSHOT','PICK_CONFIRMATION','VAS_TRANSACTION','CROSS_DOCK','ACCESSORIAL')),
    source_id                UUID NOT NULL,
    event_type               TEXT NOT NULL CHECK (event_type IN ('STORAGE','PICK','VAS','CROSS_DOCK','ACCESSORIAL')),
    occurred_at              TIMESTAMPTZ NOT NULL,
    quantity                 NUMERIC(18,4) NOT NULL,
    gross_amount             NUMERIC(18,4) NOT NULL CHECK (gross_amount >= 0),
    discount_amount          NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    surcharge_amount         NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (surcharge_amount >= 0),
    tax_amount               NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
    net_amount               NUMERIC(18,4) NOT NULL CHECK (net_amount >= 0),
    currency                 CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    rate_snapshot            JSONB NOT NULL CHECK (jsonb_typeof(rate_snapshot) = 'object'),
    components               JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(components) = 'array'),
    idempotency_key          TEXT NOT NULL,
    invoice_id               UUID,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, occurred_at),
    FOREIGN KEY (tenant_id, contract_id) REFERENCES tenant_contracts(tenant_id, id) ON DELETE RESTRICT
) PARTITION BY RANGE (occurred_at);
CREATE INDEX IF NOT EXISTS ix_billing_events_tenant_time
    ON billing_events(tenant_id, event_type, occurred_at DESC, source_id);
CREATE INDEX IF NOT EXISTS ix_billing_events_invoice
    ON billing_events(tenant_id, invoice_id, occurred_at) WHERE invoice_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_billing_events_brin_time
    ON billing_events USING BRIN (occurred_at);

CREATE TABLE IF NOT EXISTS billing_event_keys (
    tenant_id                UUID NOT NULL,
    source_type              TEXT NOT NULL,
    source_id                UUID NOT NULL,
    event_type               TEXT NOT NULL,
    idempotency_key          TEXT NOT NULL,
    billing_event_id         UUID NOT NULL,
    occurred_at              TIMESTAMPTZ NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_type, source_id, event_type, idempotency_key),
    FOREIGN KEY (billing_event_id, occurred_at) REFERENCES billing_events(id, occurred_at) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS billing_invoice_lines (
    tenant_id                UUID NOT NULL,
    invoice_id               UUID NOT NULL,
    line_no                  INTEGER NOT NULL CHECK (line_no > 0),
    billing_event_id         UUID NOT NULL,
    occurred_at              TIMESTAMPTZ NOT NULL,
    description              TEXT NOT NULL,
    quantity                 NUMERIC(18,4) NOT NULL,
    unit_rate                NUMERIC(18,6) NOT NULL,
    amount                   NUMERIC(18,4) NOT NULL,
    currency                 CHAR(3) NOT NULL,
    PRIMARY KEY (tenant_id, invoice_id, line_no),
    FOREIGN KEY (billing_event_id, occurred_at) REFERENCES billing_events(id, occurred_at) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_invoice_lines_invoice ON billing_invoice_lines(tenant_id, invoice_id, line_no);

-- -----------------------------------------------------------------------------
-- Immutable ledger, outbox, and idempotency
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS inventory_ledger (
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id                UUID NOT NULL,
    occurred_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    bucket_id                UUID NOT NULL,
    sku_id                   UUID NOT NULL,
    warehouse_id             UUID NOT NULL,
    location_id              UUID NOT NULL,
    lot_id                   UUID,
    serial_number            TEXT,
    from_status              stock_status,
    to_status                stock_status NOT NULL,
    qty_delta                NUMERIC(18,4) NOT NULL CHECK (qty_delta <> 0),
    balance_after            NUMERIC(18,4) NOT NULL CHECK (balance_after >= 0),
    reason                   TEXT NOT NULL CHECK (reason IN ('receipt','allocation','release','pick','discrepancy_quarantine','cycle_count_adjustment','vas_consumption','pallet_move','returns','manual_adjustment')),
    ref_type                 TEXT NOT NULL,
    ref_id                   UUID NOT NULL,
    actor                    TEXT NOT NULL DEFAULT 'system',
    correlation_id           UUID NOT NULL,
    metadata                 JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    PRIMARY KEY (id, occurred_at),
    FOREIGN KEY (tenant_id, bucket_id) REFERENCES inventory_bucket_headers(tenant_id, id) ON DELETE RESTRICT
) PARTITION BY RANGE (occurred_at);
CREATE INDEX IF NOT EXISTS ix_inventory_ledger_bucket
    ON inventory_ledger(tenant_id, bucket_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_inventory_ledger_location
    ON inventory_ledger(tenant_id, warehouse_id, location_id, occurred_at DESC, sku_id);
CREATE INDEX IF NOT EXISTS ix_inventory_ledger_brin
    ON inventory_ledger USING BRIN (occurred_at);

CREATE TABLE IF NOT EXISTS event_outbox (
    id                       UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id                UUID NOT NULL,
    aggregate_type           TEXT NOT NULL CHECK (length(aggregate_type) BETWEEN 2 AND 64),
    aggregate_id             UUID NOT NULL,
    event_type               TEXT NOT NULL CHECK (length(event_type) BETWEEN 2 AND 96),
    routing_key              TEXT NOT NULL,
    payload                  JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    headers                  JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(headers) = 'object'),
    occurred_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at             TIMESTAMPTZ,
    publish_attempts         INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at);
CREATE INDEX IF NOT EXISTS ix_event_outbox_unpublished
    ON event_outbox(occurred_at, id)
    WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_event_outbox_aggregate
    ON event_outbox(tenant_id, aggregate_type, aggregate_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_event_outbox_brin
    ON event_outbox USING BRIN (occurred_at);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id                UUID NOT NULL,
    scope                    TEXT NOT NULL CHECK (length(scope) BETWEEN 2 AND 96),
    key                      TEXT NOT NULL CHECK (length(key) BETWEEN 8 AND 256),
    request_hash             CHAR(64) NOT NULL CHECK (request_hash ~ '^[a-f0-9]{64}$'),
    status                   TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress','completed','failed')),
    response                 JSONB,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at             TIMESTAMPTZ,
    expires_at               TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '7 days',
    PRIMARY KEY (tenant_id, scope, key),
    CHECK (status <> 'completed' OR response IS NOT NULL),
    CHECK (expires_at > created_at)
);
CREATE INDEX IF NOT EXISTS ix_idempotency_expiry
    ON idempotency_keys(expires_at)
    WHERE status <> 'completed';

-- -----------------------------------------------------------------------------
-- Partition creation for append-heavy tables
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION apply_tenant_rls(p_table TEXT) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = wms, pg_temp
AS $$
BEGIN
    IF to_regclass('wms.' || p_table) IS NULL THEN
        RETURN;
    END IF;
    EXECUTE format('ALTER TABLE wms.%I ENABLE ROW LEVEL SECURITY', p_table);
    EXECUTE format('ALTER TABLE wms.%I FORCE ROW LEVEL SECURITY', p_table);
    EXECUTE format('DROP POLICY IF EXISTS p_tenant_isolation ON wms.%I', p_table);
    EXECUTE format(
        'CREATE POLICY p_tenant_isolation ON wms.%I USING (tenant_id = wms.app_current_tenant_id()) WITH CHECK (tenant_id = wms.app_current_tenant_id())',
        p_table
    );
END $$;

DO $$
DECLARE
    parents TEXT[] := ARRAY['inventory_ledger','billing_events','event_outbox'];
    parent TEXT;
    partition_name TEXT;
    m INTEGER;
    start_text TEXT;
    end_text TEXT;
BEGIN
    FOREACH parent IN ARRAY parents LOOP
        EXECUTE format('CREATE TABLE IF NOT EXISTS wms.%I PARTITION OF wms.%I DEFAULT', parent || '_default', parent);
        PERFORM apply_tenant_rls(parent || '_default');
        FOR m IN 1..12 LOOP
            partition_name := parent || '_2026_' || lpad(m::TEXT, 2, '0');
            start_text := to_char(make_date(2026, m, 1), 'YYYY-MM-DD') || ' 00:00:00+00';
            end_text := to_char((make_date(2026, m, 1) + INTERVAL '1 month')::DATE, 'YYYY-MM-DD') || ' 00:00:00+00';
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS wms.%I PARTITION OF wms.%I FOR VALUES FROM (%L) TO (%L)',
                partition_name, parent, start_text, end_text
            );
            PERFORM apply_tenant_rls(partition_name);
        END LOOP;
    END LOOP;
END $$;

-- -----------------------------------------------------------------------------
-- updated_at and occupancy maintenance
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = wms, pg_temp
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END $$;

DO $$
DECLARE
    t TEXT;
    tables TEXT[] := ARRAY[
        'tenants','warehouses','tenant_api_clients','skus','locations','handling_units',
        'inventory_bucket_headers','inventory_balances','orders','order_lines','waves',
        'inventory_allocations','workers','task_queue','cycle_counts','tenant_contracts'
    ];
BEGIN
    FOREACH t IN ARRAY tables LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_set_updated_at ON wms.%I', t);
        EXECUTE format(
            'CREATE TRIGGER trg_set_updated_at BEFORE UPDATE ON wms.%I FOR EACH ROW EXECUTE FUNCTION wms.set_updated_at()',
            t
        );
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION refresh_location_occupancy(p_location_id UUID) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = wms, pg_temp
AS $$
DECLARE
    v_volume NUMERIC(20,3);
    v_weight NUMERIC(18,3);
BEGIN
    IF p_location_id IS NULL THEN
        RETURN;
    END IF;

    WITH sku_occ AS (
        SELECT COALESCE(SUM(b.qty * s.unit_volume_cm3), 0)::NUMERIC(20,3) AS volume_cm3,
               COALESCE(SUM(b.qty * s.unit_weight_kg), 0)::NUMERIC(18,3) AS weight_kg
        FROM inventory_balances b
        JOIN inventory_bucket_headers h
          ON h.tenant_id = b.tenant_id AND h.id = b.bucket_id
        JOIN skus s
          ON s.tenant_id = h.tenant_id AND s.id = h.sku_id
        WHERE h.location_id = p_location_id
          AND b.stock_status IN ('available','allocated','damaged','qc_hold','quarantine')
    ),
    hu_occ AS (
        SELECT COALESCE(SUM(hu.volume_cm3), 0)::NUMERIC(20,3) AS volume_cm3,
               COALESCE(SUM(hu.tare_weight_kg), 0)::NUMERIC(18,3) AS weight_kg
        FROM handling_units hu
        WHERE hu.location_id = p_location_id
          AND hu.status IN ('available','allocated','in_transit','damaged','quarantine')
    )
    SELECT GREATEST(sku_occ.volume_cm3, hu_occ.volume_cm3),
           sku_occ.weight_kg + hu_occ.weight_kg
      INTO v_volume, v_weight
      FROM sku_occ CROSS JOIN hu_occ;

    UPDATE locations
       SET current_volume_cm3 = v_volume,
           current_weight_kg = v_weight,
           version = version + 1
     WHERE id = p_location_id;
END $$;

CREATE OR REPLACE FUNCTION touch_location_occupancy() RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = wms, pg_temp
AS $$
DECLARE
    v_old_location UUID;
    v_new_location UUID;
BEGIN
    IF TG_TABLE_NAME = 'inventory_balances' THEN
        IF TG_OP = 'DELETE' THEN
            SELECT h.location_id INTO v_old_location
              FROM inventory_bucket_headers h
             WHERE h.tenant_id = OLD.tenant_id AND h.id = OLD.bucket_id;
        ELSE
            SELECT h.location_id INTO v_new_location
              FROM inventory_bucket_headers h
             WHERE h.tenant_id = NEW.tenant_id AND h.id = NEW.bucket_id;
        END IF;
        PERFORM refresh_location_occupancy(COALESCE(v_new_location, v_old_location));
    ELSIF TG_TABLE_NAME IN ('inventory_bucket_headers','handling_units') THEN
        IF TG_OP IN ('UPDATE','DELETE') THEN
            v_old_location := OLD.location_id;
        END IF;
        IF TG_OP IN ('INSERT','UPDATE') THEN
            v_new_location := NEW.location_id;
        END IF;
        IF v_old_location IS NOT NULL THEN
            PERFORM refresh_location_occupancy(v_old_location);
        END IF;
        IF v_new_location IS NOT NULL AND v_new_location IS DISTINCT FROM v_old_location THEN
            PERFORM refresh_location_occupancy(v_new_location);
        END IF;
    END IF;
    RETURN NULL;
END $$;

DROP TRIGGER IF EXISTS trg_inventory_balances_occupancy ON inventory_balances;
CREATE TRIGGER trg_inventory_balances_occupancy
AFTER INSERT OR UPDATE OF qty, stock_status OR DELETE ON inventory_balances
FOR EACH ROW EXECUTE FUNCTION touch_location_occupancy();

DROP TRIGGER IF EXISTS trg_bucket_headers_occupancy ON inventory_bucket_headers;
CREATE TRIGGER trg_bucket_headers_occupancy
AFTER INSERT OR UPDATE OF location_id OR DELETE ON inventory_bucket_headers
FOR EACH ROW EXECUTE FUNCTION touch_location_occupancy();

DROP TRIGGER IF EXISTS trg_handling_units_occupancy ON handling_units;
CREATE TRIGGER trg_handling_units_occupancy
AFTER INSERT OR UPDATE OF location_id, status, qty, volume_cm3, tare_weight_kg OR DELETE ON handling_units
FOR EACH ROW EXECUTE FUNCTION touch_location_occupancy();

-- -----------------------------------------------------------------------------
-- Row-level security: tenant isolation, shared warehouse visibility
-- -----------------------------------------------------------------------------

SELECT apply_tenant_rls(u.t)
FROM unnest(ARRAY[
    'tenant_warehouse_access','tenant_api_clients','workflow_definitions','skus','lots',
    'location_tenant_policies','handling_units','inventory_bucket_headers','inventory_balances',
    'inventory_ledger','orders','order_lines','waves','wave_orders','wave_order_lines',
    'wave_route_stops','inventory_allocations','workers','worker_zone_permissions','task_queue',
    'task_assignments','inventory_discrepancy_events','cycle_counts','tenant_contracts',
    'contract_storage_rates','contract_pick_rates','vas_rate_cards','vas_transactions',
    'billing_events','billing_event_keys','billing_invoice_lines','event_outbox','idempotency_keys'
]) AS u(t);

DROP POLICY IF EXISTS p_tenants_self ON tenants;
CREATE POLICY p_tenants_self ON tenants
    USING (id = app_current_tenant_id())
    WITH CHECK (id = app_current_tenant_id());
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_warehouses_access ON warehouses;
CREATE POLICY p_warehouses_access ON warehouses
    USING (EXISTS (
        SELECT 1 FROM tenant_warehouse_access a
        WHERE a.tenant_id = app_current_tenant_id()
          AND a.warehouse_id = warehouses.id
          AND a.can_read
    ));
ALTER TABLE warehouses ENABLE ROW LEVEL SECURITY;
ALTER TABLE warehouses FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS p_locations_access ON locations;
CREATE POLICY p_locations_access ON locations
    USING (
        exclusive_tenant_id = app_current_tenant_id()
        OR (
            exclusive_tenant_id IS NULL
            AND EXISTS (
                SELECT 1 FROM tenant_warehouse_access a
                WHERE a.tenant_id = app_current_tenant_id()
                  AND a.warehouse_id = locations.warehouse_id
                  AND a.can_read
            )
        )
    )
    WITH CHECK (
        exclusive_tenant_id = app_current_tenant_id()
        OR (
            exclusive_tenant_id IS NULL
            AND EXISTS (
                SELECT 1 FROM tenant_warehouse_access a
                WHERE a.tenant_id = app_current_tenant_id()
                  AND a.warehouse_id = locations.warehouse_id
                  AND a.can_write
            )
        )
    );
ALTER TABLE locations ENABLE ROW LEVEL SECURITY;
ALTER TABLE locations FORCE ROW LEVEL SECURITY;

-- -----------------------------------------------------------------------------
-- Privileges
-- -----------------------------------------------------------------------------

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wms_app') THEN
        GRANT USAGE ON SCHEMA wms TO wms_app;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA wms TO wms_app;
        GRANT USAGE ON ALL SEQUENCES IN SCHEMA wms TO wms_app;
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA wms TO wms_app;
        ALTER DEFAULT PRIVILEGES IN SCHEMA wms GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO wms_app;
        ALTER DEFAULT PRIVILEGES IN SCHEMA wms GRANT USAGE ON SEQUENCES TO wms_app;
        ALTER DEFAULT PRIVILEGES IN SCHEMA wms GRANT EXECUTE ON FUNCTIONS TO wms_app;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wms_migration') THEN
        GRANT ALL ON SCHEMA wms TO wms_migration;
        GRANT ALL ON ALL TABLES IN SCHEMA wms TO wms_migration;
        GRANT ALL ON ALL SEQUENCES IN SCHEMA wms TO wms_migration;
        GRANT ALL ON ALL FUNCTIONS IN SCHEMA wms TO wms_migration;
    END IF;
END $$;

GRANT SELECT ON inventory_available TO PUBLIC;

-- =============================================================================
-- CONCURRENCY CONTRACT — enforced by services/orchestrator/allocation.py
-- -----------------------------------------------------------------------------
-- 1. Every transaction sets: SELECT set_config('app.current_tenant_id', $1, true).
-- 2. Allocation candidates are selected with:
--      FOR UPDATE OF h, b SKIP LOCKED
--    on inventory_bucket_headers h and inventory_balances b.
-- 3. The available balance is decremented only with:
--      WHERE version = locked_version AND qty >= requested_qty
--    returning zero rows causes the service to skip to the next locked-out
--    candidate. The header version is incremented in the same transaction.
-- 4. The allocated balance is upserted and an inventory_allocations row with a
--    partial unique index on reserved (tenant, order_line, bucket) is inserted.
-- 5. order_lines.allocated_qty is updated with its own version predicate and a
--    database CHECK preventing allocated_qty > required_qty.
-- 6. Task claiming uses FOR UPDATE SKIP LOCKED on task_queue. Billing uses a
--    REPEATABLE READ snapshot plus billing_event_keys for exactly-once rating.
-- 7. All externally observable mutations write event_outbox in the same DB
--    transaction; the outbox relay publishes with an idempotent Kafka producer.
-- =============================================================================
