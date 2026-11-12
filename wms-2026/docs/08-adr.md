# Architecture Decision Records

## ADR-001 — Shared-schema RLS isolation

**Context:** A 3PL operates one physical warehouse estate for many clients. Schema-per-tenant would multiply migrations and prevent efficient shared-location routing. A single table without database isolation would rely entirely on application correctness.

**Decision:** Use one `wms` schema, tenant-prefixed composite keys, `app.current_tenant_id`, forced RLS policies, `tenant_warehouse_access`, `locations.exclusive_tenant_id`, and tenant-owned workflow/contract/rate tables.

**Consequences:** One migration surface and efficient shared warehouse queries; every application connection must set tenant context; the migration role must never serve client traffic.

## ADR-002 — Lockable natural bucket plus status-keyed balances

**Context:** Inventory identity spans tenant, SKU, lot, serial, expiry, location, and handling unit. Allocation must lock a stable row and move quantity between statuses. Null lot/serial/expiry must still be unique.

**Decision:** `inventory_bucket_headers` owns the natural identity with `UNIQUE NULLS NOT DISTINCT`. `inventory_balances` stores one row per `(tenant_id, bucket_id, stock_status)` with `qty` and `version`. Allocation locks the header, balance, and location rows.

**Consequences:** A single balance row is the unit of concurrency; status changes are atomic upserts/deletes; partial indexes isolate positive available stock. The DDL comment and documentation describe this as status-keyed, not physically partitioned.

## ADR-003 — Pessimistic row locks, optimistic versions, and savepoints

**Context:** `SERIALIZABLE` across an entire order would serialize unrelated SKUs and reduce throughput. Bare `SELECT ... FOR UPDATE` without version checks can still apply stale writes after lock waits.

**Decision:** Use `FOR UPDATE ... SKIP LOCKED` for candidate and queue claims, version predicates on every balance/header/order-line update, and per-line/per-pallet nested transaction savepoints.

**Consequences:** Unrelated pickers proceed concurrently; losing writers fail fast; a shortage never commits a partial reservation. The retry budget is bounded and exposes `CONCURRENCY_RETRY_EXHAUSTED`.

## ADR-004 — Pure affinity and pricing functions

**Context:** Travel scoring and money calculation must be deterministic, auditable, and unit testable. Embedding them in SQL or FastAPI handlers would make regression testing brittle.

**Decision:** `affinity.py` and `pricing.py` are side-effect-free modules with frozen dataclasses and `Decimal` arithmetic. Repositories and services orchestrate I/O around them.

**Consequences:** Formulas can be tested without PostgreSQL; SQL and Python share the same 3D travel ordering; billing components are reproducible.

## ADR-005 — Transactional outbox

**Context:** Writing PostgreSQL state and Kafka in one request creates a dual-write failure window.

**Decision:** Use `event_outbox` inserted in the same transaction as state changes. `OutboxRelay` uses the BYPASSRLS system pool, `FOR UPDATE SKIP LOCKED`, idempotent Kafka producer, exponential retries, and dead-letter state.

**Consequences:** At-least-once delivery with idempotent consumers; no lost committed events; slight publication latency bounded by relay poll interval.

## ADR-006 — Redis for pushes only

**Context:** Worker devices need low-latency task notifications, but a Redis-only queue can lose authoritative assignments.

**Decision:** PostgreSQL `task_queue` and `task_assignments` are the system of record. Redis publishes `wms.tasks.{tenant_id}.{worker_id}` after commit; publish failures are retried and workers can claim from PostgreSQL.

**Consequences:** Notifications are ephemeral and recoverable; no Redis persistence dependency for correctness.

## ADR-007 — Hard B2B/B2C queue separation

**Context:** Pallet orders must not be fragmented into piece picks or placed in waves sized for each picking.

**Decision:** B2B lines are pallet-unit and full-pallet-multiple validated. They use `LOCK_FULL_PALLET`, `wave_id = NULL`, `PALLET_PICK`, and forklift-class equipment. B2C lines use waves, affinity routes, and RF/AMR/cart equipment.

**Consequences:** Equipment and labour pools are correctly segmented; HYBRID orders still share one order transaction and one idempotency key.

## ADR-008 — Quarantine before reroute

**Context:** A discrepancy such as "expected 10, found 0" can otherwise be consumed by another picker while reallocation is in progress.

**Decision:** The exception engine version-locks and quarantines the location first, then releases allocations, moves balances to `quarantine`, and only then reroutes remaining wave lines to identical SKU/lot stock.

**Consequences:** Phantom stock cannot be reallocated; cycle counts are always generated at the exact coordinate; short alternative supply produces backorders and replenishment tasks.

## ADR-009 — Reproducible billing snapshots

**Context:** Rate cards change over time; invoices must remain reproducible and auditable.

**Decision:** Every `billing_events` row stores `rate_snapshot`, `components`, currency, rounding mode, quantity, gross/discount/surcharge/tax/net amounts, and source/idempotency keys.

**Consequences:** Historical invoices do not depend on current rate cards; storage grows with billing volume and is partitioned by month.

## ADR-010 — External tax engine

**Context:** Tax jurisdiction, exemption certificates, and filing rules vary by tenant and region and change independently of warehouse operations.

**Decision:** The WMS calculates tax-exclusive operational charges and records `tax_amount = 0`; `tenant_contracts.tax_mode` documents the downstream expectation.

**Consequences:** Operational billing remains deterministic; tax is applied by the financial system using `billing.rated` events and stored components.

## ADR-011 — Date-effective tenant contracts

**Context:** 3PL contracts have effective periods, free days, minimum charges, tiers, and tenant-specific VAS catalogs.

**Decision:** Use a GiST exclusion constraint to prevent overlapping active contracts and effective-dated rate-card tables with tier JSONB.

**Consequences:** Rate lookup is deterministic for `occurred_at`; backdated corrections require an explicit new effective-dated rate card rather than mutating history.

## ADR-012 — PGlite validation with native PG16 deployment gate

**Context:** The sandbox has no native PostgreSQL 16 server. PGlite provides a PostgreSQL-compatible engine for DDL execution and SQL preparation.

**Decision:** Validate DDL execution, all SQL placeholders, and prepared statements under PGlite; parse Mermaid and unit test Python locally. Require deployment pipelines to execute the same DDL on the target PG16+ cluster.

**Consequences:** High confidence in schema and SQL syntax during development; platform-specific features such as exact PG16 extension packaging remain a deployment gate, not an unverified runtime claim.
