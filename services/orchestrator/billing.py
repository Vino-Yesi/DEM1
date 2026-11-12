from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb

from . import catalog, sql
from .auth import TenantContext
from .config import Settings
from .db import Database, fetch_one
from .errors import ConflictError, NotFoundError
from .idempotency import IdempotencyGuard
from .outbox import emit_event
from .pricing import (
    ChargeResult,
    PickChargeParams,
    StorageChargeParams,
    VASChargeParams,
    calculate_pick_charge,
    calculate_storage_charge,
    calculate_vas_charge,
)
from .schemas import BillingPickIn, BillingStorageIn, ChargeResponse, VASChargeIn


class BillingEngine:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        guard: IdempotencyGuard | None = None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.guard = guard or IdempotencyGuard()

    async def rate_storage(
        self,
        ctx: TenantContext,
        payload: BillingStorageIn,
        idempotency_key: str,
    ) -> ChargeResponse:
        async with self.db.tenant_transaction(
            ctx.tenant_id, repeatable_read=True
        ) as conn:
            cached = await self.guard.begin(
                conn,
                ctx.tenant_id,
                "billing.storage",
                idempotency_key,
                {"client_id": ctx.client_id, "charge": payload},
            )
            if cached is not None:
                return ChargeResponse(**cached)
            await catalog.validate_api_client(
                conn,
                ctx,
                required_scope="billing:write",
                mode=None,
            )
            as_of = payload.occurred_at.date()
            contract = await catalog.get_contract_row(conn, ctx.tenant_id, as_of)
            rate = await fetch_one(
                conn,
                sql.GET_STORAGE_RATE,
                {
                    "tenant_id": ctx.tenant_id,
                    "contract_id": contract["id"],
                    "basis": contract["storage_basis"],
                    "as_of": as_of,
                    "location_type": payload.location_type,
                },
            )
            if rate is None:
                raise NotFoundError(
                    "Active storage rate card",
                    f"{ctx.tenant_id}:{as_of}:{contract['storage_basis']}:{payload.location_type}",
                )
            params = StorageChargeParams(
                as_of_date=as_of,
                occurred_at=payload.occurred_at,
                days=payload.days,
                volume_cm3=payload.volume_cm3,
                weight_kg=payload.weight_kg,
                pallet_positions=payload.pallet_positions,
                hazardous=payload.hazardous,
                temperature_band=payload.temperature_band,
                location_type=payload.location_type,
            )
            result = calculate_storage_charge(params, contract, rate)
            return await self._persist_charge(
                conn,
                ctx=ctx,
                idempotency_key=idempotency_key,
                scope="billing.storage",
                source_type="STORAGE_SNAPSHOT",
                source_id=payload.source_id,
                event_type="STORAGE",
                occurred_at=payload.occurred_at,
                contract=contract,
                result=result,
            )

    async def rate_pick(
        self,
        ctx: TenantContext,
        payload: BillingPickIn,
        idempotency_key: str,
    ) -> ChargeResponse:
        async with self.db.tenant_transaction(
            ctx.tenant_id, repeatable_read=True
        ) as conn:
            cached = await self.guard.begin(
                conn,
                ctx.tenant_id,
                "billing.pick",
                idempotency_key,
                {"client_id": ctx.client_id, "charge": payload},
            )
            if cached is not None:
                return ChargeResponse(**cached)
            await catalog.validate_api_client(
                conn,
                ctx,
                required_scope="billing:write",
                mode=None,
            )
            as_of = payload.occurred_at.date()
            contract = await catalog.get_contract_row(conn, ctx.tenant_id, as_of)
            rate = await fetch_one(
                conn,
                sql.GET_PICK_RATE,
                {
                    "tenant_id": ctx.tenant_id,
                    "contract_id": contract["id"],
                    "mode": payload.mode,
                    "as_of": as_of,
                },
            )
            if rate is None:
                raise NotFoundError(
                    "Active pick rate card",
                    f"{ctx.tenant_id}:{as_of}:{payload.mode}",
                )
            params = PickChargeParams(
                mode=payload.mode,
                occurred_at=payload.occurred_at,
                pieces=payload.pieces,
                pallets=payload.pallets,
                lines=payload.lines,
                orders_count=payload.orders_count,
                hazardous_pieces=payload.hazardous_pieces,
                oversized_pieces=payload.oversized_pieces,
                same_day=payload.same_day,
                cross_dock=payload.cross_dock,
            )
            result = calculate_pick_charge(params, rate, contract)
            return await self._persist_charge(
                conn,
                ctx=ctx,
                idempotency_key=idempotency_key,
                scope="billing.pick",
                source_type="PICK_CONFIRMATION",
                source_id=payload.source_id,
                event_type="PICK",
                occurred_at=payload.occurred_at,
                contract=contract,
                result=result,
            )

    async def rate_vas(
        self,
        ctx: TenantContext,
        payload: VASChargeIn,
        idempotency_key: str,
    ) -> ChargeResponse:
        async with self.db.tenant_transaction(
            ctx.tenant_id, repeatable_read=True
        ) as conn:
            cached = await self.guard.begin(
                conn,
                ctx.tenant_id,
                "billing.vas",
                idempotency_key,
                {"client_id": ctx.client_id, "charge": payload},
            )
            if cached is not None:
                return ChargeResponse(**cached)
            await catalog.validate_api_client(
                conn,
                ctx,
                required_scope="billing:write",
                mode=None,
            )
            as_of = payload.occurred_at.date()
            contract = await catalog.get_contract_row(conn, ctx.tenant_id, as_of)
            vas_code = payload.vas_code.value
            rate = await fetch_one(
                conn,
                sql.GET_VAS_RATE,
                {
                    "tenant_id": ctx.tenant_id,
                    "contract_id": contract["id"],
                    "vas_code": vas_code,
                    "as_of": as_of,
                },
            )
            if rate is None:
                raise NotFoundError(
                    "Active VAS rate card", f"{ctx.tenant_id}:{as_of}:{vas_code}"
                )
            await conn.execute(
                sql.INSERT_VAS_TRANSACTION,
                {
                    "tenant_id": ctx.tenant_id,
                    "order_id": payload.order_id,
                    "order_line_id": payload.order_line_id,
                    "vas_code": vas_code,
                    "quantity": payload.quantity,
                    "minutes": payload.minutes,
                    "pallets": payload.pallets,
                    "cartons": payload.cartons,
                    "order_count": payload.order_count,
                    "occurred_at": payload.occurred_at,
                    "attributes": Jsonb(jsonable_encoder(payload.attributes)),
                    "idempotency_key": idempotency_key,
                },
            )
            params = VASChargeParams(
                vas_code=vas_code,
                occurred_at=payload.occurred_at,
                quantity=payload.quantity,
                minutes=payload.minutes,
                pallets=payload.pallets,
                cartons=payload.cartons,
                order_count=payload.order_count,
            )
            result = calculate_vas_charge(params, rate, contract)
            return await self._persist_charge(
                conn,
                ctx=ctx,
                idempotency_key=idempotency_key,
                scope="billing.vas",
                source_type="VAS_TRANSACTION",
                source_id=payload.source_id,
                event_type="VAS",
                occurred_at=payload.occurred_at,
                contract=contract,
                result=result,
            )

    async def _persist_charge(
        self,
        conn: Any,
        *,
        ctx: TenantContext,
        idempotency_key: str,
        scope: str,
        source_type: str,
        source_id: UUID,
        event_type: str,
        occurred_at: Any,
        contract: dict[str, Any],
        result: ChargeResult,
    ) -> ChargeResponse:
        event = await fetch_one(
            conn,
            sql.INSERT_BILLING_EVENT,
            {
                "tenant_id": ctx.tenant_id,
                "contract_id": contract["id"],
                "source_type": source_type,
                "source_id": source_id,
                "event_type": event_type,
                "occurred_at": occurred_at,
                "quantity": result.quantity,
                "gross_amount": result.gross_amount,
                "discount_amount": result.discount_amount,
                "surcharge_amount": result.surcharge_amount,
                "tax_amount": result.tax_amount,
                "net_amount": result.net_amount,
                "currency": result.currency,
                "rate_snapshot": Jsonb(jsonable_encoder(result.rate_snapshot)),
                "components": Jsonb(jsonable_encoder(result.components)),
                "idempotency_key": idempotency_key,
            },
        )
        if event is None:
            raise ConflictError("billing event insert returned no row")
        key = await fetch_one(
            conn,
            sql.INSERT_BILLING_EVENT_KEY,
            {
                "tenant_id": ctx.tenant_id,
                "source_type": source_type,
                "source_id": source_id,
                "event_type": event_type,
                "idempotency_key": idempotency_key,
                "billing_event_id": event["id"],
                "occurred_at": event["occurred_at"],
            },
        )
        if key is None:
            existing = await fetch_one(
                conn,
                sql.GET_BILLING_EVENT_BY_KEY,
                {
                    "tenant_id": ctx.tenant_id,
                    "source_type": source_type,
                    "source_id": source_id,
                    "event_type": event_type,
                    "idempotency_key": idempotency_key,
                },
            )
            await conn.execute(
                sql.DELETE_BILLING_EVENT_BY_ID,
                {"billing_event_id": event["id"], "occurred_at": event["occurred_at"]},
            )
            if existing is None:
                raise ConflictError("billing event key conflict could not be resolved")
            response = ChargeResponse(**dict(existing))
            await self.guard.complete(
                conn, ctx.tenant_id, scope, idempotency_key, response
            )
            return response

        response = ChargeResponse(
            billing_event_id=UUID(str(event["id"])),
            event_type=event_type,
            currency=result.currency,
            quantity=result.quantity,
            gross_amount=result.gross_amount,
            discount_amount=result.discount_amount,
            surcharge_amount=result.surcharge_amount,
            tax_amount=result.tax_amount,
            net_amount=result.net_amount,
            components=jsonable_encoder(result.components),
            rate_snapshot=jsonable_encoder(result.rate_snapshot),
        )
        await emit_event(
            conn,
            tenant_id=ctx.tenant_id,
            aggregate_type="billing_event",
            aggregate_id=UUID(str(event["id"])),
            event_type="billing.rated",
            payload={
                "billing_event_id": str(event["id"]),
                "source_type": source_type,
                "source_id": str(source_id),
                "event_type": event_type,
                "net_amount": result.net_amount,
                "currency": result.currency,
            },
            occurred_at=occurred_at,
            headers={"correlation_id": str(uuid4())},
        )
        await self.guard.complete(conn, ctx.tenant_id, scope, idempotency_key, response)
        return response
