# Deployment, Security, Observability, and Operations

## 1. Runtime topology

| Unit | Process | State | Scaling |
|---|---|---|---|
| API | `uvicorn orchestrator.main:app` | stateless | horizontal behind gateway; connection pools per replica |
| Outbox relay | `python -m orchestrator.run_relay` | stateless; BYPASSRLS system pool | multiple replicas safe with `SKIP LOCKED` |
| PostgreSQL 16+ | primary plus replicas | authoritative | partitioned append-heavy tables, RLS, connection pooling |
| Redis 7+ | task push pub/sub | ephemeral | not a system of record |
| Kafka 3+ | domain event bus | durable downstream log | idempotent consumers on `event_id` |
| API Gateway | OIDC/mTLS, rate limits | tenant/client identity | segmented client credentials per tenant |

`main.py` wires `Database`, `TaskDispatcher`, `IdempotencyGuard`, `OutboxRelay`, `HybridRoutingService`, `DigitalTwinExceptionEngine`, and `BillingEngine` in the FastAPI lifespan. `WMS_START_OUTBOX_RELAY=true` runs a relay inside the API process for small deployments; production uses the separate process.

## 2. Configuration

`config.get_settings` reads `WMS_*` variables. Defaults are production-safe baselines, not development shortcuts.

| Variable | Default | Purpose |
|---|---|---|
| `WMS_DATABASE_URL` | `postgresql://wms_app@127.0.0.1:5432/wms?options=-csearch_path%3Dwms,public` | RLS-guarded application DSN |
| `WMS_SYSTEM_DATABASE_URL` | same as application DSN | migration/outbox DSN; set to `wms_migration` in production |
| `WMS_REDIS_URL` | `redis://127.0.0.1:6379/0` | worker push channel |
| `WMS_KAFKA_BOOTSTRAP_SERVERS` | `kafka-1:9092,kafka-2:9092,kafka-3:9092` | event bus |
| `WMS_KAFKA_TOPIC_PREFIX` | `wms` | topic namespace |
| `WMS_DB_POOL_MIN` / `WMS_DB_POOL_MAX` | `8` / `64` | async psycopg pool bounds |
| `WMS_STATEMENT_TIMEOUT_MS` | `5000` | transaction-local statement timeout |
| `WMS_WORKER_HEARTBEAT_TTL_SECONDS` | `90` | worker liveness |
| `WMS_MAX_TASKS_PER_DISPATCH` | `50` | assignment batch |
| `WMS_TASK_PUBLISH_RETRIES` | `4` | Redis publish retries |
| `WMS_TASK_PUBLISH_BACKOFF_SECONDS` | `0.15` | exponential backoff base |
| `WMS_OUTBOX_BATCH_SIZE` | `200` | relay batch |
| `WMS_OUTBOX_POLL_SECONDS` | `0.25` | relay idle poll |
| `WMS_OUTBOX_MAX_ATTEMPTS` | `12` | dead-letter threshold |
| `WMS_ENVIRONMENT` | `production` | log/environment tag |
| `WMS_LOG_LEVEL` | `INFO` | structured logging level |

`.env.example` lists the full surface.

## 3. Commissioning order

1. Create database and roles; install `pgcrypto` and `btree_gist`.
2. Apply `sql/001_schema.sql` with `wms_migration`.
3. Insert `tenants`, `warehouses`, and `tenant_warehouse_access`.
4. Insert `tenant_api_clients` with hashed secrets, scopes, allowed modes, CIDRs, and rate limits.
5. Insert `workflow_definitions` for each tenant where behaviour differs from `DEFAULT_WORKFLOW`.
6. Insert `skus`, `lots`, `locations`, `location_tenant_policies`, and `workers` / `worker_zone_permissions`.
7. Insert active `tenant_contracts`, `contract_storage_rates`, `contract_pick_rates`, and `vas_rate_cards`.
8. Commission inventory through `handling_units`, `inventory_bucket_headers`, and `inventory_balances`; location occupancy triggers recalculate capacity.
9. Start API and outbox relay; verify `/health/ready`.
10. Register worker devices and send heartbeats before releasing orders.

## 4. Security controls

- OIDC/mTLS at gateway; no client connects directly to PostgreSQL.
- `wms_app` has forced RLS; `wms_migration` is restricted to DDL/relay operations.
- `tenant_api_clients` scopes are checked both at gateway and in `catalog.validate_api_client`.
- `allowed_modes` prevents a B2C-only client from submitting B2B or HYBRID orders.
- `allowed_cidrs` restricts client source ranges.
- `locations.exclusive_tenant_id` and `location_tenant_policies` constrain shared physical slots.
- All SQL uses parameterized statements from `sql.py`; no string interpolation of tenant or user input.
- `SECURITY DEFINER` functions set `search_path = wms, pg_temp`.
- `inventory_available` uses `security_invoker` and `security_barrier`.
- Structured errors do not leak connection strings, secrets, or cross-tenant identifiers.

## 5. Observability

Structured JSON logs include timestamp, level, logger, and message. Correlation IDs are propagated from `HybridRoutingService` into allocations, tasks, ledger metadata, outbox headers, and discrepancy processing results.

Required metrics:

| Metric | Alert condition |
|---|---|
| `order_route_latency_p99` | above 800 ms for five minutes |
| `allocation_version_conflict_total` | sustained non-zero after retry budget |
| `allocation_double_alloc_total` | any non-zero |
| `task_claim_latency_p99` | above 200 ms |
| `worker_heartbeat_stale_total` | above expected shift baseline |
| `discrepancy_quarantine_latency_p99` | above one transaction SLO |
| `cycle_count_open_age_p99` | above site SLA |
| `outbox_pending_oldest_seconds` | above 60 seconds |
| `outbox_dead_letter_total` | any non-zero |
| `billing_key_conflict_total` | sustained non-zero |
| `redis_publish_failure_total` | any sustained non-zero |

Audit queries use `inventory_ledger` by `correlation_id`, `billing_events.rate_snapshot`, `task_assignments`, `idempotency_keys`, and `event_outbox`.

## 6. Runbooks

| Condition | Action |
|---|---|
| Duplicate order submission | exact idempotency key returns stored response; conflicting key/payload returns `CONFLICT` |
| Allocation version conflict | transaction/savepoint rolls back; client retries; if persistent, inspect concurrent pick tasks for the SKU/location |
| B2B pallet shortage | line is `backordered`; `REPLENISHMENT` task created; no partial pallet reservation is committed |
| Quarantined location | confirm cycle count, reconcile `quarantine` balances, then release location through an approved maintenance workflow |
| Outbox backlog | scale relay replicas; inspect dead-letter rows; replay with idempotent Kafka producer |
| Redis publish failure | workers call `/tasks/claim`; PostgreSQL assignment remains authoritative |
| Worker stale heartbeat | heartbeat endpoint updates state; assignment excludes stale workers |
| Rate-card correction | create a new effective-dated rate card; historical billing events retain their rate snapshot |
| Tenant offboarding | suspend `tenant_api_clients`, close contracts, retain ledger/outbox per retention policy |

## 7. SLOs and capacity assumptions

- 500 orders/minute per warehouse at p99 order-route latency under 800 ms.
- 10,000 concurrent SKU/location candidate reads per minute with `inventory_available` and compound indexes.
- 1,000 task claims per minute per warehouse with `FOR UPDATE SKIP LOCKED`.
- Zero committed double allocations; zero lost outbox events; zero cross-tenant reads.
- Discrepancy quarantine, release, reroute, and cycle-count creation complete within one tenant transaction.
- Monthly partitions are created ahead of time by migration automation; default partitions are safety nets, not steady-state storage.
