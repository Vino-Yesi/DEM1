# API, Events, Idempotency, and Error Contract

## 1. Request context

Every state-changing endpoint requires these headers:

| Header | Validation |
|---|---|
| `X-Tenant-ID` | UUID, matched to gateway identity and `app.current_tenant_id` |
| `X-Client-ID` | registered `tenant_api_clients.client_id` |
| `X-Idempotency-Key` | non-empty stable key; exact retry returns stored response |
| `X-Scopes` / gateway token | must include the endpoint's required scope |
| `X-Forwarded-For` / client IP | checked against `allowed_cidrs` when configured |

`auth.get_tenant_context` constructs `TenantContext` with `tenant_id`, `client_id`, `idempotency_key`, `scopes`, and `client_ip`. `Database.tenant_transaction` sets statement timeout and transaction-local tenant context. The `wms_app` role cannot bypass RLS.

## 2. Endpoints

| Method | Path | Scope | Request model | Response model |
|---|---|---|---|---|
| GET | `/health/live` | public | none | `{"status":"live"}` |
| GET | `/health/ready` | public | none | readiness JSON with database/Redis booleans |
| POST | `/v1/orders/route` | `orders:write` | `OrderIn` | `RouteOrderResponse` |
| POST | `/v1/inbound/pallets/directed-putaway` | `inbound:write` | `DirectedPutawayIn` | `DirectedPutawayResponse` |
| POST | `/v1/tasks/{task_id}/putaway-complete` | `tasks:write` | `PutawayCompleteIn` | `PutawayCompleteResponse` |
| POST | `/v1/events/inventory-discrepancy` | `events:write` | `DiscrepancyEventIn` | `DiscrepancyResponse` |
| POST | `/v1/workers/{worker_id}/heartbeat` | `workers:write` | `WorkerHeartbeatIn` | `WorkerHeartbeatResponse` |
| POST | `/v1/workers/{worker_id}/tasks/claim` | `tasks:claim` | `TaskClaimIn` | `TaskClaimResponse` |
| POST | `/v1/billing/storage` | `billing:write` | `BillingStorageIn` | `ChargeResponse` |
| POST | `/v1/billing/picks` | `billing:write` | `BillingPickIn` | `ChargeResponse` |
| POST | `/v1/billing/vas` | `billing:write` | `VASChargeIn` | `ChargeResponse` |

All Pydantic models derive from `StrictModel` with `extra="forbid"`, stripped strings, assignment validation, aware UTC timestamps, and bounded `Decimal` quantities. `OrderIn` enforces line-mode compatibility, B2B PALLET units, B2C non-PALLET units, and HYBRID line modes. `DirectedPutawayIn` enforces full-pallet equality and cross-dock order linkage. `DiscrepancyEventIn` enforces exactly one location and SKU identifier.

## 3. Representative order request

```json
{
  "external_order_id": "OMS-2026-000001",
  "warehouse_id": "11111111-1111-4111-8111-111111111111",
  "mode": "B2C",
  "priority": 200,
  "sla_ship_at": "2026-11-10T14:00:00Z",
  "carrier_code": "DXB-EXPRESS",
  "lines": [
    {
      "line_no": 1,
      "sku_code": "SKU-A-1",
      "lot_code": "LOT-A-2026",
      "fulfillment_unit": "EACH",
      "required_qty": "10",
      "require_same_lot": true,
      "expiry_min": "2026-11-10",
      "expiry_max": "2027-11-10"
    }
  ]
}
```

`RouteOrderResponse` contains `order_id`, `mode`, `strategy`, `wave_ids`, `allocations`, `tasks`, `backordered_lines`, and `status`.

## 4. Error model

`WMSError` is rendered as JSON by the FastAPI exception handler:

```json
{
  "error": {
    "code": "INSUFFICIENT_STOCK",
    "message": "no unlocked available stock satisfies the order line",
    "details": {"order_line_id": "...", "shortage_qty": "3"}
  }
}
```

| Code | HTTP | Cause |
|---|---|---|
| `NOT_FOUND` | 404 | SKU, lot, location, worker, contract, rate card, task, or dock missing |
| `CONFLICT` | 409 | external order ID conflict, version conflict, task insert conflict, invalid pallet move |
| `VALIDATION_ERROR` | 422 | strict schema or workflow invariant rejected |
| `PERMISSION_DENIED` | 403 | client inactive, missing scope, disallowed mode, CIDR mismatch |
| `INSUFFICIENT_STOCK` | 409 | no unlocked compatible stock or no sufficient single lot |
| `CONCURRENCY_RETRY_EXHAUSTED` | 409 | partial allocation could not finish within retry budget |

## 5. Idempotency

`IdempotencyGuard` hashes scope, key, and canonical JSON payload with `jsonable_encoder`. The `idempotency_keys` row stores `status`, `response_json`, and `expires_at`. Exact retries with a completed key return the stored response without re-running the use case. Key reuse with a different payload raises `CONFLICT`. In-progress rows produce a retryable conflict. Billing additionally uses `billing_event_keys`; VAS transactions use a tenant-scoped `idempotency_key`; discrepancy events use `(tenant_id, source, source_event_id)`.

## 6. Domain events

| Event type | Aggregate | Key payload |
|---|---|---|
| `order.routed` | order | external ID, mode, strategy, wave IDs, task IDs, backorders, status |
| `wave.released` | order | strategy, wave IDs, task IDs |
| `inventory.allocated` | order line | wave ID, SKU, allocations with bucket/location/lot/quantity |
| `pallet.reserved` / `pallet.shortage` | order line | pallets needed/reserved/missing, HU allocations |
| `pallet.pick.tasks_created` | order | task IDs, reserved pallet count |
| `pallet.backordered` | order line | order ID, SKU, missing pallets |
| `task.created` / `task.assigned` | task | type, priority, zone, equipment, worker ID, score |
| `location.quarantined` | location | discrepancy ID, source, expected/found, SKU, lot |
| `discrepancy.processed` | discrepancy event | full processing result |
| `inbound.pallet.received` / `inbound.pallet.putaway_completed` | handling unit | HU, source/target locations, quantity, worker |
| `billing.rated` | billing event | billing event ID, source, net amount, currency |

Events are inserted into `event_outbox` with aggregate type/ID, event type, tenant ID, JSON payload, headers containing correlation ID, and occurred timestamp. The relay uses `FOR UPDATE SKIP LOCKED`, an idempotent Kafka producer, exponential backoff, and dead-letter routing. Consumers deduplicate on `headers.event_id`.

## 7. Consistency boundaries

- One API request equals one tenant transaction plus optional post-commit Redis pushes.
- One order line allocation equals one nested transaction savepoint.
- One full-pallet reservation equals one nested transaction savepoint.
- One billing charge equals one `REPEATABLE READ` tenant transaction.
- One discrepancy observation equals one all-or-nothing tenant transaction.
- Kafka is never written directly from a use-case transaction; only `event_outbox` is written in the transaction.
- Redis is never the system of record; a missed push is recoverable by worker claim from PostgreSQL.
