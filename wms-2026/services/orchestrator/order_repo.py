from __future__ import annotations

from typing import Any, Sequence
from uuid import UUID, uuid4

from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb

from . import catalog
from .affinity import LineRequirement
from .auth import TenantContext
from .db import fetch_all, fetch_one
from .errors import ConflictError, NotFoundError, ValidationError
from .schemas import OrderIn, OrderMode
from .utils import to_decimal


async def create_order(
    conn: Any,
    ctx: TenantContext,
    payload: OrderIn,
    workflow_version: int,
    idempotency_key: str,
) -> tuple[UUID, dict[str, list[UUID]]]:
    row = await fetch_one(
        conn,
        """
        INSERT INTO orders
            (tenant_id, external_order_id, warehouse_id, mode, status,
             priority, sla_ship_at, carrier_code, customer_ref,
             workflow_version, metadata, idempotency_key)
        VALUES
            (%(tenant_id)s, %(external_order_id)s, %(warehouse_id)s,
             %(mode)s::order_mode, 'received', %(priority)s, %(sla_ship_at)s,
             %(carrier_code)s, %(customer_ref)s, %(workflow_version)s,
             %(metadata)s::jsonb, %(idempotency_key)s)
        ON CONFLICT (tenant_id, external_order_id) DO NOTHING
        RETURNING id
        """,
        {
            "tenant_id": ctx.tenant_id,
            "external_order_id": payload.external_order_id,
            "warehouse_id": payload.warehouse_id,
            "mode": payload.mode.value,
            "priority": payload.priority,
            "sla_ship_at": payload.sla_ship_at,
            "carrier_code": payload.carrier_code,
            "customer_ref": payload.customer_ref,
            "workflow_version": workflow_version,
            "metadata": Jsonb(jsonable_encoder(payload.metadata)),
            "idempotency_key": idempotency_key,
        },
    )
    if row is None:
        existing = await fetch_one(
            conn,
            """
            SELECT id, idempotency_key, status
            FROM orders
            WHERE tenant_id = %(tenant_id)s
              AND external_order_id = %(external_order_id)s
            """,
            {
                "tenant_id": ctx.tenant_id,
                "external_order_id": payload.external_order_id,
            },
        )
        if existing is None:
            raise ConflictError("order insert conflict could not be resolved")
        raise ConflictError(
            "external_order_id already exists; retry with the original idempotency key",
            {
                "external_order_id": payload.external_order_id,
                "stored_idempotency_key": str(existing["idempotency_key"]),
                "order_status": str(existing["status"]),
            },
        )

    order_id = UUID(str(row["id"]))
    line_ids: dict[str, list[UUID]] = {"B2C": [], "B2B": []}
    for line in payload.lines:
        sku = await catalog.resolve_sku(conn, ctx.tenant_id, line.sku_code)
        sku_id = UUID(str(sku["id"]))
        lot_id = await catalog.resolve_lot(
            conn,
            ctx.tenant_id,
            sku_id,
            line.lot_code,
            required=bool(sku["requires_lot"]),
        )
        effective_mode = line.mode or payload.mode
        units_per_pallet = to_decimal(sku["units_per_pallet"])
        if (
            effective_mode == OrderMode.B2B
            and line.required_qty % units_per_pallet != 0
        ):
            raise ValidationError(
                "B2B pallet order quantity must be an exact multiple of units_per_pallet",
                {
                    "line_no": line.line_no,
                    "sku_code": line.sku_code,
                    "required_qty": str(line.required_qty),
                    "units_per_pallet": str(units_per_pallet),
                },
            )
        line_id = uuid4()
        await conn.execute(
            """
            INSERT INTO order_lines
                (tenant_id, id, order_id, line_no, sku_id, lot_id, mode,
                 fulfillment_unit, required_qty, status, require_same_lot,
                 expiry_min, expiry_max, metadata)
            VALUES
                (%(tenant_id)s, %(line_id)s, %(order_id)s, %(line_no)s,
                 %(sku_id)s, %(lot_id)s, %(mode)s::order_mode,
                 %(fulfillment_unit)s, %(required_qty)s, 'open',
                 %(require_same_lot)s, %(expiry_min)s, %(expiry_max)s,
                 %(metadata)s::jsonb)
            """,
            {
                "tenant_id": ctx.tenant_id,
                "line_id": line_id,
                "order_id": order_id,
                "line_no": line.line_no,
                "sku_id": sku_id,
                "lot_id": lot_id,
                "mode": effective_mode.value,
                "fulfillment_unit": line.fulfillment_unit.value,
                "required_qty": line.required_qty,
                "require_same_lot": line.require_same_lot,
                "expiry_min": line.expiry_min,
                "expiry_max": line.expiry_max,
                "metadata": Jsonb(jsonable_encoder(line.metadata)),
            },
        )
        line_ids[effective_mode.value].append(line_id)
    return order_id, line_ids


async def get_order_header(
    conn: Any, tenant_id: UUID, order_id: UUID
) -> dict[str, Any]:
    row = await fetch_one(
        conn,
        """
        SELECT * FROM orders
        WHERE tenant_id = %(tenant_id)s AND id = %(order_id)s
        """,
        {"tenant_id": tenant_id, "order_id": order_id},
    )
    if row is None:
        raise NotFoundError("Order", order_id, {"tenant_id": str(tenant_id)})
    return row


async def get_routable_lines(
    conn: Any,
    tenant_id: UUID,
    order_id: UUID,
    modes: Sequence[str],
) -> list[dict[str, Any]]:
    return await fetch_all(
        conn,
        """
        SELECT ol.id,
               ol.order_id,
               ol.line_no,
               ol.sku_id,
               ol.lot_id,
               ol.mode,
               ol.fulfillment_unit,
               ol.required_qty,
               ol.allocated_qty,
               ol.picked_qty,
               ol.shipped_qty,
               ol.status,
               ol.require_same_lot,
               ol.expiry_min,
               ol.expiry_max,
               ol.version,
               s.sku_code,
               s.units_per_pallet,
               s.hazardous,
               s.temperature_band,
               s.requires_lot,
               s.requires_serial
        FROM order_lines ol
        JOIN skus s ON s.tenant_id = ol.tenant_id AND s.id = ol.sku_id
        WHERE ol.tenant_id = %(tenant_id)s
          AND ol.order_id = %(order_id)s
          AND ol.mode = ANY(%(modes)s::order_mode[])
          AND ol.status = 'open'
        ORDER BY ol.line_no
        """,
        {"tenant_id": tenant_id, "order_id": order_id, "modes": list(modes)},
    )


def to_line_requirement(
    row: dict[str, Any],
    allowed_location_types: Sequence[str],
) -> LineRequirement:
    return LineRequirement(
        order_line_id=UUID(str(row["id"])),
        order_id=UUID(str(row["order_id"])),
        sku_id=UUID(str(row["sku_id"])),
        lot_id=None if row.get("lot_id") is None else UUID(str(row["lot_id"])),
        required_qty=to_decimal(row["required_qty"]),
        picked_qty=to_decimal(row.get("picked_qty") or 0),
        allocated_qty=to_decimal(row.get("allocated_qty") or 0),
        require_same_lot=bool(row.get("require_same_lot", True)),
        expiry_min=row.get("expiry_min"),
        expiry_max=row.get("expiry_max"),
        hazardous=bool(row.get("hazardous", False)),
        temperature_band=str(row.get("temperature_band") or "AMBIENT"),
        requires_lot=bool(row.get("requires_lot", False)),
        requires_serial=bool(row.get("requires_serial", False)),
        mode=str(row["mode"]),
        fulfillment_unit=str(row.get("fulfillment_unit") or "EACH"),
        allowed_location_types=tuple(allowed_location_types),
        version=int(row.get("version") or 0),
        units_per_pallet=to_decimal(row.get("units_per_pallet") or 1),
    )


async def update_order_status(
    conn: Any,
    tenant_id: UUID,
    order_id: UUID,
    status: str,
) -> None:
    await conn.execute(
        """
        UPDATE orders
           SET status = %(status)s::order_status,
               version = version + 1,
               updated_at = now()
         WHERE tenant_id = %(tenant_id)s AND id = %(order_id)s
        """,
        {"tenant_id": tenant_id, "order_id": order_id, "status": status},
    )


async def mark_line_backordered(
    conn: Any, tenant_id: UUID, order_line_id: UUID
) -> None:
    await conn.execute(
        """
        UPDATE order_lines
           SET status = 'backordered',
               version = version + 1,
               updated_at = now()
         WHERE tenant_id = %(tenant_id)s
           AND id = %(order_line_id)s
           AND status NOT IN ('shipped', 'cancelled')
        """,
        {"tenant_id": tenant_id, "order_line_id": order_line_id},
    )
