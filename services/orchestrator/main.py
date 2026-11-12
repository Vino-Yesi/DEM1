from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from . import catalog
from .auth import TenantContext, get_tenant_context
from .billing import BillingEngine
from .config import get_settings
from .db import Database
from .errors import WMSError
from .exceptions import DigitalTwinExceptionEngine
from .idempotency import IdempotencyGuard
from .outbox import OutboxRelay
from .routing import HybridRoutingService
from .schemas import (
    BillingPickIn,
    BillingStorageIn,
    ChargeResponse,
    DirectedPutawayIn,
    DirectedPutawayResponse,
    DiscrepancyEventIn,
    DiscrepancyResponse,
    OrderIn,
    PutawayCompleteIn,
    PutawayCompleteResponse,
    RouteOrderResponse,
    TaskClaimIn,
    TaskClaimResponse,
    VASChargeIn,
    WorkerHeartbeatIn,
    WorkerHeartbeatResponse,
)
from .task_dispatcher import TaskDispatcher

logging.basicConfig(
    level=os.getenv("WMS_LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
)
logger = logging.getLogger("wms.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    db = Database(settings)
    await db.open()
    dispatcher = TaskDispatcher(db, settings)
    guard = IdempotencyGuard()
    relay = OutboxRelay(db, settings)
    relay_task: asyncio.Task[Any] | None = None
    if os.getenv("WMS_START_OUTBOX_RELAY", "false").lower() in ("1", "true", "yes"):
        relay_task = asyncio.create_task(relay.run_forever(), name="wms-outbox-relay")

    app.state.settings = settings
    app.state.db = db
    app.state.dispatcher = dispatcher
    app.state.guard = guard
    app.state.outbox_relay = relay
    app.state.routing = HybridRoutingService(db, settings, dispatcher, guard)
    app.state.exceptions = DigitalTwinExceptionEngine(db, settings, dispatcher, guard)
    app.state.billing = BillingEngine(db, settings, guard)
    logger.info("orchestrator started", extra={"environment": settings.environment})
    try:
        yield
    finally:
        if relay_task is not None:
            relay_task.cancel()
            try:
                await relay_task
            except asyncio.CancelledError:
                pass
            await relay.stop()
        await dispatcher.close()
        await db.close()
        logger.info("orchestrator stopped")


app = FastAPI(
    title="Hybrid 3PL WMS Orchestrator",
    version="2026.1.0",
    description="Tenant-isolated hybrid B2C, B2B, digital twin and 3PL billing orchestration.",
    lifespan=lifespan,
)


@app.exception_handler(WMSError)
async def handle_wms_error(_: Request, exc: WMSError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.to_dict())


def get_db(request: Request) -> Database:
    return request.app.state.db


def get_dispatcher(request: Request) -> TaskDispatcher:
    return request.app.state.dispatcher


def get_routing(request: Request) -> HybridRoutingService:
    return request.app.state.routing


def get_exceptions(request: Request) -> DigitalTwinExceptionEngine:
    return request.app.state.exceptions


def get_billing(request: Request) -> BillingEngine:
    return request.app.state.billing


@app.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "live"}


@app.get("/health/ready")
async def health_ready(
    db: Database = Depends(get_db),
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> JSONResponse:
    db_ok = await db.ready()
    redis_ok = await dispatcher.redis_ping()
    status = 200 if db_ok and redis_ok else 503
    return JSONResponse(
        status_code=status,
        content={
            "status": "ready" if status == 200 else "degraded",
            "database": db_ok,
            "redis": redis_ok,
        },
    )


@app.post("/v1/orders/route", response_model=RouteOrderResponse)
async def route_order(
    payload: OrderIn,
    ctx: TenantContext = Depends(get_tenant_context),
    service: HybridRoutingService = Depends(get_routing),
) -> RouteOrderResponse:
    return await service.route_order(ctx, payload, ctx.idempotency_key)


@app.post(
    "/v1/inbound/pallets/directed-putaway", response_model=DirectedPutawayResponse
)
async def directed_putaway(
    payload: DirectedPutawayIn,
    ctx: TenantContext = Depends(get_tenant_context),
    service: HybridRoutingService = Depends(get_routing),
) -> DirectedPutawayResponse:
    return await service.plan_directed_putaway(ctx, payload, ctx.idempotency_key)


@app.post(
    "/v1/tasks/{task_id}/putaway-complete", response_model=PutawayCompleteResponse
)
async def putaway_complete(
    task_id: UUID,
    payload: PutawayCompleteIn,
    ctx: TenantContext = Depends(get_tenant_context),
    service: HybridRoutingService = Depends(get_routing),
) -> PutawayCompleteResponse:
    return await service.complete_putaway(ctx, task_id, payload, ctx.idempotency_key)


@app.post("/v1/events/inventory-discrepancy", response_model=DiscrepancyResponse)
async def inventory_discrepancy(
    payload: DiscrepancyEventIn,
    ctx: TenantContext = Depends(get_tenant_context),
    engine: DigitalTwinExceptionEngine = Depends(get_exceptions),
) -> DiscrepancyResponse:
    return await engine.handle_discrepancy(ctx, payload, ctx.idempotency_key)


@app.post("/v1/workers/{worker_id}/heartbeat", response_model=WorkerHeartbeatResponse)
async def worker_heartbeat(
    worker_id: UUID,
    payload: WorkerHeartbeatIn,
    ctx: TenantContext = Depends(get_tenant_context),
    db: Database = Depends(get_db),
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> WorkerHeartbeatResponse:
    async with db.tenant_transaction(ctx.tenant_id) as conn:
        await catalog.validate_api_client(
            conn, ctx, required_scope="workers:write", mode=None
        )
        response, pushes = await dispatcher.heartbeat(
            conn, ctx.tenant_id, worker_id, payload
        )
    await dispatcher.publish(pushes)
    return response


@app.post("/v1/workers/{worker_id}/tasks/claim", response_model=TaskClaimResponse)
async def claim_tasks(
    worker_id: UUID,
    payload: TaskClaimIn,
    ctx: TenantContext = Depends(get_tenant_context),
    db: Database = Depends(get_db),
    dispatcher: TaskDispatcher = Depends(get_dispatcher),
) -> TaskClaimResponse:
    async with db.tenant_transaction(ctx.tenant_id) as conn:
        await catalog.validate_api_client(
            conn, ctx, required_scope="tasks:claim", mode=None
        )
        response, pushes = await dispatcher.claim_tasks(
            conn, ctx.tenant_id, worker_id, payload
        )
    await dispatcher.publish(pushes)
    return response


@app.post("/v1/billing/storage", response_model=ChargeResponse)
async def rate_storage(
    payload: BillingStorageIn,
    ctx: TenantContext = Depends(get_tenant_context),
    engine: BillingEngine = Depends(get_billing),
) -> ChargeResponse:
    return await engine.rate_storage(ctx, payload, ctx.idempotency_key)


@app.post("/v1/billing/picks", response_model=ChargeResponse)
async def rate_pick(
    payload: BillingPickIn,
    ctx: TenantContext = Depends(get_tenant_context),
    engine: BillingEngine = Depends(get_billing),
) -> ChargeResponse:
    return await engine.rate_pick(ctx, payload, ctx.idempotency_key)


@app.post("/v1/billing/vas", response_model=ChargeResponse)
async def rate_vas(
    payload: VASChargeIn,
    ctx: TenantContext = Depends(get_tenant_context),
    engine: BillingEngine = Depends(get_billing),
) -> ChargeResponse:
    return await engine.rate_vas(ctx, payload, ctx.idempotency_key)
