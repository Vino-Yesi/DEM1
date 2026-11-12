from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb

from . import catalog, sql
from .affinity import Coordinate, LineRequirement
from .db import fetch_all, fetch_one
from .errors import ConflictError, NotFoundError, ValidationError
from .outbox import emit_event
from .schemas import (
    DirectedPutawayIn,
    DirectedPutawayResponse,
    PutawayCompleteIn,
    PutawayCompleteResponse,
)
from .task_dispatcher import TaskDispatcher, TaskPush, TaskSpec
from .utils import ceil_decimal, to_decimal, utcnow


def _date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _optional_uuid(value: Any) -> UUID | None:
    return None if value is None else UUID(str(value))


class _PalletShortage(Exception):
    """Internal signal that rolls back a partial full-pallet reservation."""


@dataclass(frozen=True)
class PalletReservation:
    allocation_id: UUID
    handling_unit_id: UUID
    hu_barcode: str
    bucket_id: UUID
    location_id: UUID
    location_barcode: str
    lot_id: UUID | None
    qty: Decimal
    zone: str
    coordinate: Coordinate
    location_type: str
    travel_seq: int
    order_line_id: UUID

    def payload_item(self) -> dict[str, Any]:
        return {
            "allocation_id": str(self.allocation_id),
            "handling_unit_id": str(self.handling_unit_id),
            "hu_barcode": self.hu_barcode,
            "location_id": str(self.location_id),
            "location_barcode": self.location_barcode,
            "lot_id": str(self.lot_id) if self.lot_id else None,
            "qty": self.qty,
            "zone": self.zone,
            "location_type": self.location_type,
        }


class B2BRepository:
    """Full-pallet availability, directed putaway, cross-dock and heavy-equipment routing."""

    async def reserve_full_pallets(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        warehouse_id: UUID,
        line: LineRequirement,
        dock: Coordinate,
        workflow: dict[str, Any],
        correlation_id: UUID,
    ) -> tuple[list[PalletReservation], int]:
        units_per_pallet = line.units_per_pallet
        remaining_units = line.remaining_qty
        if remaining_units <= 0:
            return [], 0
        if remaining_units % units_per_pallet != 0:
            raise ValidationError(
                "remaining B2B quantity is not an exact number of full pallets",
                {
                    "order_line_id": str(line.order_line_id),
                    "remaining_units": str(remaining_units),
                    "units_per_pallet": str(units_per_pallet),
                },
            )
        pallets_needed = int(ceil_decimal(remaining_units / units_per_pallet))
        b2b = workflow["b2b"]
        if line.lot_id is not None:
            lot_mode, lot_filter = "EXACT", line.lot_id
        elif line.require_same_lot:
            lot_mode, lot_filter = "SINGLE_LOT", None
        else:
            lot_mode, lot_filter = "IGNORE", None

        results: list[PalletReservation] = []
        try:
            async with conn.transaction():
                locked = await fetch_all(
                    conn,
                    sql.LOCK_FULL_PALLET,
                    {
                        "tenant_id": tenant_id,
                        "sku_id": line.sku_id,
                        "lot_filter_mode": lot_mode,
                        "lot_filter": lot_filter,
                        "pallets_needed": pallets_needed,
                        "allowed_types": list(b2b["allowed_pallet_location_types"]),
                        "exclude_locations": [],
                        "prefer_cross_dock": bool(b2b["prefer_cross_dock"]),
                        "dock_x": dock.x,
                        "dock_y": dock.y,
                        "dock_z": dock.z,
                        "dock_zone": dock.zone,
                        "limit": pallets_needed,
                    },
                )
                line_version = line.version
                allocated_units = Decimal(0)
                results = []
                for row in locked:
                    take = to_decimal(row["hu_qty"])
                    if allocated_units + take > remaining_units:
                        break
                    available_after = await fetch_one(
                        conn,
                        sql.UPDATE_AVAILABLE_BALANCE,
                        {
                            "tenant_id": tenant_id,
                            "balance_id": row["balance_id"],
                            "balance_version": int(row["balance_version"]),
                            "qty": take,
                        },
                    )
                    if available_after is None:
                        continue
                    allocated_after = await fetch_one(
                        conn,
                        sql.UPSERT_ALLOCATED_BALANCE,
                        {
                            "tenant_id": tenant_id,
                            "bucket_id": row["bucket_id"],
                            "qty": take,
                        },
                    )
                    header_after = await fetch_one(
                        conn,
                        sql.BUMP_HEADER_VERSION,
                        {
                            "tenant_id": tenant_id,
                            "bucket_id": row["bucket_id"],
                            "header_version": int(row["header_version"]),
                        },
                    )
                    hu_after = await fetch_one(
                        conn,
                        sql.UPDATE_HU_ALLOCATED,
                        {
                            "tenant_id": tenant_id,
                            "handling_unit_id": row["handling_unit_id"],
                            "hu_version": int(row["hu_version"]),
                        },
                    )
                    if (
                        allocated_after is None
                        or header_after is None
                        or hu_after is None
                    ):
                        raise ConflictError(
                            "full-pallet reservation lost a versioned row",
                            {"handling_unit_id": str(row["handling_unit_id"])},
                        )
                    allocation = await fetch_one(
                        conn,
                        sql.INSERT_ALLOCATION,
                        {
                            "tenant_id": tenant_id,
                            "order_line_id": line.order_line_id,
                            "bucket_id": row["bucket_id"],
                            "wave_id": None,
                            "sku_id": line.sku_id,
                            "lot_id": row["lot_id"],
                            "warehouse_id": warehouse_id,
                            "location_id": row["location_id"],
                            "handling_unit_id": row["handling_unit_id"],
                            "qty": take,
                        },
                    )
                    if allocation is None:
                        raise ConflictError("pallet allocation insert returned no row")
                    allocation_id = UUID(str(allocation["id"]))
                    line_after = await fetch_one(
                        conn,
                        sql.UPDATE_ORDER_LINE_ALLOCATED,
                        {
                            "tenant_id": tenant_id,
                            "order_line_id": line.order_line_id,
                            "qty": take,
                            "line_version": line_version,
                        },
                    )
                    if line_after is None:
                        raise ConflictError(
                            "order line version changed during pallet reservation",
                            {"order_line_id": str(line.order_line_id)},
                        )
                    line_version = int(line_after["version"])
                    metadata = {
                        "correlation_id": str(correlation_id),
                        "order_line_id": str(line.order_line_id),
                        "allocation_id": str(allocation_id),
                        "handling_unit_id": str(row["handling_unit_id"]),
                        "hu_barcode": str(row["hu_barcode"]),
                    }
                    ledger_common = {
                        "tenant_id": tenant_id,
                        "bucket_id": row["bucket_id"],
                        "sku_id": line.sku_id,
                        "warehouse_id": warehouse_id,
                        "location_id": row["location_id"],
                        "lot_id": row["lot_id"],
                        "serial_number": None,
                        "reason": "allocation",
                        "ref_type": "pallet_allocation",
                        "ref_id": allocation_id,
                        "actor": "system",
                        "correlation_id": correlation_id,
                        "metadata": Jsonb(metadata),
                    }
                    await conn.execute(
                        sql.INSERT_LEDGER,
                        {
                            **ledger_common,
                            "from_status": "available",
                            "to_status": "allocated",
                            "qty_delta": -take,
                            "balance_after": to_decimal(available_after["qty"]),
                        },
                    )
                    await conn.execute(
                        sql.INSERT_LEDGER,
                        {
                            **ledger_common,
                            "from_status": None,
                            "to_status": "allocated",
                            "qty_delta": take,
                            "balance_after": to_decimal(allocated_after["qty"]),
                        },
                    )
                    results.append(
                        PalletReservation(
                            allocation_id=allocation_id,
                            handling_unit_id=UUID(str(row["handling_unit_id"])),
                            hu_barcode=str(row["hu_barcode"]),
                            bucket_id=UUID(str(row["bucket_id"])),
                            location_id=UUID(str(row["location_id"])),
                            location_barcode=str(row["location_barcode"]),
                            lot_id=_optional_uuid(row["lot_id"]),
                            qty=take,
                            zone=str(row["zone"]),
                            coordinate=Coordinate(
                                x=to_decimal(row["x_m"]),
                                y=to_decimal(row["y_m"]),
                                z=to_decimal(row["z_m"]),
                                zone=str(row["zone"]),
                            ),
                            location_type=str(row["location_type"]),
                            travel_seq=int(row["travel_seq"]),
                            order_line_id=line.order_line_id,
                        )
                    )
                    allocated_units += take
                if allocated_units < remaining_units or len(results) < pallets_needed:
                    raise _PalletShortage
        except _PalletShortage:
            results = []

        missing_pallets = max(0, pallets_needed - len(results))
        await emit_event(
            conn,
            tenant_id=tenant_id,
            aggregate_type="order_line",
            aggregate_id=line.order_line_id,
            event_type="pallet.reserved" if missing_pallets == 0 else "pallet.shortage",
            payload={
                "order_line_id": str(line.order_line_id),
                "pallets_needed": pallets_needed,
                "pallets_reserved": len(results),
                "missing_pallets": missing_pallets,
                "allocations": [item.payload_item() for item in results],
            },
            headers={"correlation_id": str(correlation_id)},
        )
        return results, missing_pallets

    async def plan_directed_putaway(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        payload: DirectedPutawayIn,
        workflow: dict[str, Any],
        correlation_id: UUID,
        dispatcher: TaskDispatcher,
    ) -> tuple[DirectedPutawayResponse, list[TaskPush]]:
        sku = await catalog.resolve_sku(conn, tenant_id, payload.sku_code)
        sku_id = UUID(str(sku["id"]))
        lot_id = await catalog.resolve_lot(
            conn,
            tenant_id,
            sku_id,
            payload.lot_code,
            required=bool(sku["requires_lot"]),
        )
        source = await catalog.resolve_location(
            conn,
            tenant_id,
            location_id=payload.source_location_id,
            warehouse_id=payload.warehouse_id,
        )
        dock, dock_row = await catalog.get_dock(
            conn,
            payload.warehouse_id,
            payload.dock_location_id or payload.source_location_id,
        )
        lot_expiry: date | None = None
        if lot_id is not None:
            lot_row = await fetch_one(
                conn,
                "SELECT expires_on FROM lots WHERE tenant_id = %(tenant_id)s AND id = %(lot_id)s",
                {"tenant_id": tenant_id, "lot_id": lot_id},
            )
            lot_expiry = _date(lot_row["expires_on"]) if lot_row else None
        if payload.attributes.get("expiration_date") is not None:
            lot_expiry = _date(payload.attributes["expiration_date"])

        pallet_volume = to_decimal(payload.attributes.get("pallet_volume_cm3") or 0)
        tare_weight = to_decimal(payload.attributes.get("tare_weight_kg") or 0)
        sku_volume = payload.qty * to_decimal(sku["unit_volume_cm3"])
        sku_weight = payload.qty * to_decimal(sku["unit_weight_kg"])
        required_volume = max(sku_volume, pallet_volume)
        required_weight = sku_weight + tare_weight
        allowed_types = list(
            workflow["b2b"]["putaway_location_types"][payload.putaway_policy.value]
        )
        target = await fetch_one(
            conn,
            sql.FIND_PUTAWAY_LOCATION,
            {
                "tenant_id": tenant_id,
                "warehouse_id": payload.warehouse_id,
                "sku_id": sku_id,
                "lot_id": lot_id,
                "allowed_types": allowed_types,
                "required_volume": required_volume,
                "required_weight": required_weight,
                "prefer_cross_dock": payload.putaway_policy.value == "CROSS_DOCK",
                "dock_x": dock.x,
                "dock_y": dock.y,
                "dock_z": dock.z,
                "dock_zone": dock.zone,
            },
        )
        if target is None:
            raise NotFoundError(
                "Compatible directed putaway location",
                f"{payload.putaway_policy.value}:{payload.sku_code}",
                {
                    "required_volume_cm3": str(required_volume),
                    "required_weight_kg": str(required_weight),
                },
            )

        handling_unit_id = uuid4()
        hu_row = await fetch_one(
            conn,
            """
            INSERT INTO handling_units
                (tenant_id, id, barcode, hu_type, status, warehouse_id, location_id,
                 sku_id, lot_id, qty, full_pallet_qty, is_full, volume_cm3,
                 tare_weight_kg, attributes)
            VALUES
                (%(tenant_id)s, %(handling_unit_id)s, %(barcode)s, 'PALLET',
                 'in_transit', %(warehouse_id)s, %(source_location_id)s,
                 %(sku_id)s, %(lot_id)s, %(qty)s, %(full_pallet_qty)s, TRUE,
                 %(volume_cm3)s, %(tare_weight_kg)s, %(attributes)s::jsonb)
            ON CONFLICT (tenant_id, barcode) DO NOTHING
            RETURNING id
            """,
            {
                "tenant_id": tenant_id,
                "handling_unit_id": handling_unit_id,
                "barcode": payload.handling_unit_barcode,
                "warehouse_id": payload.warehouse_id,
                "source_location_id": source["id"],
                "sku_id": sku_id,
                "lot_id": lot_id,
                "qty": payload.qty,
                "full_pallet_qty": payload.full_pallet_qty,
                "volume_cm3": pallet_volume,
                "tare_weight_kg": tare_weight,
                "attributes": Jsonb(jsonable_encoder(payload.attributes)),
            },
        )
        if hu_row is None:
            raise ConflictError(
                "handling unit barcode already exists for this tenant",
                {"handling_unit_barcode": payload.handling_unit_barcode},
            )
        handling_unit_id = UUID(str(hu_row["id"]))

        bucket_id = uuid4()
        await conn.execute(
            """
            INSERT INTO inventory_bucket_headers
                (tenant_id, id, sku_id, warehouse_id, location_id, lot_id,
                 expiration_date, handling_unit_id)
            VALUES
                (%(tenant_id)s, %(bucket_id)s, %(sku_id)s, %(warehouse_id)s,
                 %(source_location_id)s, %(lot_id)s, %(expiration_date)s,
                 %(handling_unit_id)s)
            """,
            {
                "tenant_id": tenant_id,
                "bucket_id": bucket_id,
                "sku_id": sku_id,
                "warehouse_id": payload.warehouse_id,
                "source_location_id": source["id"],
                "lot_id": lot_id,
                "expiration_date": lot_expiry,
                "handling_unit_id": handling_unit_id,
            },
        )
        await conn.execute(
            """
            INSERT INTO inventory_balances
                (tenant_id, bucket_id, stock_status, qty)
            VALUES
                (%(tenant_id)s, %(bucket_id)s, 'in_transit', %(qty)s)
            """,
            {"tenant_id": tenant_id, "bucket_id": bucket_id, "qty": payload.qty},
        )
        await conn.execute(
            sql.INSERT_LEDGER,
            {
                "tenant_id": tenant_id,
                "bucket_id": bucket_id,
                "sku_id": sku_id,
                "warehouse_id": payload.warehouse_id,
                "location_id": source["id"],
                "lot_id": lot_id,
                "serial_number": None,
                "from_status": None,
                "to_status": "in_transit",
                "qty_delta": payload.qty,
                "balance_after": payload.qty,
                "reason": "receipt",
                "ref_type": "handling_unit",
                "ref_id": handling_unit_id,
                "actor": "system",
                "correlation_id": correlation_id,
                "metadata": Jsonb(
                    {"handling_unit_barcode": payload.handling_unit_barcode}
                ),
            },
        )

        task_type = (
            "CROSS_DOCK_MOVE"
            if payload.putaway_policy.value == "CROSS_DOCK"
            else "PALLET_PUTAWAY"
        )
        priority = 700 if task_type == "CROSS_DOCK_MOVE" else 600
        task_payload = {
            "handling_unit_id": str(handling_unit_id),
            "handling_unit_barcode": payload.handling_unit_barcode,
            "sku_id": str(sku_id),
            "lot_id": str(lot_id) if lot_id else None,
            "qty": payload.qty,
            "source_location_id": str(source["id"]),
            "target_location_id": str(target["id"]),
            "target_barcode": str(target["barcode"]),
            "putaway_policy": payload.putaway_policy.value,
            "cross_dock_order_id": str(payload.cross_dock_order_id)
            if payload.cross_dock_order_id
            else None,
            "dock": {
                "x": dock.x,
                "y": dock.y,
                "z": dock.z,
                "zone": dock.zone or str(dock_row["zone"]),
            },
            "correlation_id": str(correlation_id),
        }
        task_id = await dispatcher.enqueue_task(
            conn,
            TaskSpec(
                tenant_id=tenant_id,
                task_type=task_type,
                priority=priority,
                warehouse_id=payload.warehouse_id,
                required_equipment=list(workflow["b2b"]["required_equipment"]),
                required_zone=str(target["zone"]),
                location_id=UUID(str(source["id"])),
                source_ref_type="handling_unit",
                source_ref_id=handling_unit_id,
                wave_id=None,
                order_id=payload.cross_dock_order_id,
                payload=task_payload,
                not_before=utcnow(),
            ),
        )
        if task_id is None:
            existing = await fetch_one(
                conn,
                """
                SELECT id FROM task_queue
                WHERE tenant_id = %(tenant_id)s
                  AND source_ref_type = 'handling_unit'
                  AND source_ref_id = %(source_ref_id)s
                  AND state IN ('queued','assigned','accepted','in_progress','blocked')
                ORDER BY enqueued_at DESC
                LIMIT 1
                """,
                {"tenant_id": tenant_id, "source_ref_id": handling_unit_id},
            )
            if existing is None:
                raise ConflictError(
                    "putaway task insert conflict could not be resolved"
                )
            task_id = UUID(str(existing["id"]))

        await emit_event(
            conn,
            tenant_id=tenant_id,
            aggregate_type="handling_unit",
            aggregate_id=handling_unit_id,
            event_type="inbound.pallet.received",
            payload=task_payload,
            headers={"correlation_id": str(correlation_id)},
        )
        pushes = await dispatcher.assign_queued_tasks(
            conn,
            tenant_id,
            warehouse_id=payload.warehouse_id,
            limit=1,
        )
        response = DirectedPutawayResponse(
            handling_unit_id=handling_unit_id,
            bucket_id=bucket_id,
            task_id=task_id,
            task_type=task_type,
            source_location_id=UUID(str(source["id"])),
            target_location_id=UUID(str(target["id"])),
            target_barcode=str(target["barcode"]),
            required_equipment=list(workflow["b2b"]["required_equipment"]),
            status="queued_or_assigned",
        )
        return response, pushes

    async def complete_pallet_move(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        task_id: UUID,
        payload: PutawayCompleteIn,
        correlation_id: UUID,
    ) -> PutawayCompleteResponse:
        task = await fetch_one(
            conn,
            sql.LOCK_TASK_FOR_COMPLETION,
            {"tenant_id": tenant_id, "task_id": task_id},
        )
        if task is None:
            raise NotFoundError("Putaway task", task_id)
        if task["task_type"] not in ("PALLET_PUTAWAY", "CROSS_DOCK_MOVE"):
            raise ValidationError(
                "task is not a pallet movement",
                {"task_id": str(task_id), "task_type": str(task["task_type"])},
            )
        task_payload = task.get("payload") or {}
        if (
            str(task_payload.get("handling_unit_barcode"))
            != payload.handling_unit_barcode
        ):
            raise ValidationError(
                "handling unit barcode does not match the task payload"
            )
        target_location_id = payload.target_location_id or UUID(
            str(task_payload["target_location_id"])
        )
        warehouse_id = UUID(str(task["warehouse_id"]))

        hu = await fetch_one(
            conn,
            sql.LOCK_HU_FOR_COMPLETION,
            {"tenant_id": tenant_id, "hu_barcode": payload.handling_unit_barcode},
        )
        if hu is None:
            raise NotFoundError("Handling unit", payload.handling_unit_barcode)
        if hu["status"] != "in_transit":
            raise ConflictError(
                "handling unit is not in transit",
                {"handling_unit_id": str(hu["id"]), "status": str(hu["status"])},
            )
        hu_qty = to_decimal(hu["qty"])
        if payload.found_qty != hu_qty:
            raise ConflictError(
                "physical count differs from expected full pallet quantity",
                {
                    "handling_unit_id": str(hu["id"]),
                    "expected_qty": str(hu_qty),
                    "found_qty": str(payload.found_qty),
                },
            )
        target = await fetch_one(
            conn,
            sql.LOCK_TARGET_LOCATION,
            {"location_id": target_location_id},
        )
        if target is None or target["status"] != "active":
            raise ConflictError(
                "target location is not active",
                {"target_location_id": str(target_location_id)},
            )
        sku = await catalog.resolve_sku_by_id(conn, tenant_id, UUID(str(hu["sku_id"])))
        required_volume = max(
            hu_qty * to_decimal(sku["unit_volume_cm3"]),
            to_decimal(hu["volume_cm3"]),
        )
        required_weight = hu_qty * to_decimal(sku["unit_weight_kg"]) + to_decimal(
            hu["tare_weight_kg"]
        )
        if (
            to_decimal(target["available_volume_cm3"]) < required_volume
            or to_decimal(target["available_weight_kg"]) < required_weight
        ):
            raise ConflictError(
                "target location lacks remaining capacity",
                {
                    "target_location_id": str(target_location_id),
                    "required_volume_cm3": str(required_volume),
                    "required_weight_kg": str(required_weight),
                },
            )
        compatible = await fetch_one(
            conn,
            """
            SELECT location_slot_compatibility(
                %(target_location_id)s, %(tenant_id)s, %(sku_id)s, %(lot_id)s
            ) AS ok
            """,
            {
                "target_location_id": target_location_id,
                "tenant_id": tenant_id,
                "sku_id": hu["sku_id"],
                "lot_id": hu["lot_id"],
            },
        )
        if compatible is None or not bool(compatible["ok"]):
            raise ConflictError(
                "target location violates tenant, SKU or lot slotting compatibility",
                {"target_location_id": str(target_location_id)},
            )

        hu_after = await fetch_one(
            conn,
            sql.UPDATE_HU_LOCATION,
            {
                "tenant_id": tenant_id,
                "handling_unit_id": hu["id"],
                "hu_version": int(hu["version"]),
                "target_location_id": target_location_id,
                "warehouse_id": warehouse_id,
            },
        )
        if hu_after is None:
            raise ConflictError(
                "handling unit version changed during putaway completion"
            )
        bucket = await fetch_one(
            conn,
            sql.UPDATE_BUCKET_LOCATION,
            {
                "tenant_id": tenant_id,
                "handling_unit_id": hu["id"],
                "target_location_id": target_location_id,
                "warehouse_id": warehouse_id,
            },
        )
        if bucket is None:
            raise ConflictError("inventory bucket for handling unit was not found")
        bucket_id = UUID(str(bucket["id"]))
        in_transit = await fetch_one(
            conn,
            """
            SELECT id, qty, version FROM inventory_balances
            WHERE tenant_id = %(tenant_id)s
              AND bucket_id = %(bucket_id)s
              AND stock_status = 'in_transit'
            FOR UPDATE
            """,
            {"tenant_id": tenant_id, "bucket_id": bucket_id},
        )
        if in_transit is None:
            raise ConflictError(
                "in-transit balance was not found", {"bucket_id": str(bucket_id)}
            )
        qty = to_decimal(in_transit["qty"])
        available_after = await fetch_one(
            conn,
            sql.UPSERT_AVAILABLE_BALANCE,
            {"tenant_id": tenant_id, "bucket_id": bucket_id, "qty": qty},
        )
        if available_after is None:
            raise ConflictError("available balance upsert failed")
        deleted = await fetch_one(
            conn,
            sql.DELETE_IN_TRANSIT_BALANCE,
            {"tenant_id": tenant_id, "bucket_id": bucket_id},
        )
        if deleted is None:
            raise ConflictError("in-transit balance delete failed")

        metadata = {
            "correlation_id": str(correlation_id),
            "task_id": str(task_id),
            "handling_unit_id": str(hu["id"]),
            "target_location_id": str(target_location_id),
        }
        ledger_common = {
            "tenant_id": tenant_id,
            "bucket_id": bucket_id,
            "sku_id": hu["sku_id"],
            "warehouse_id": warehouse_id,
            "location_id": target_location_id,
            "lot_id": hu["lot_id"],
            "serial_number": None,
            "reason": "pallet_move",
            "ref_type": "task",
            "ref_id": task_id,
            "actor": str(payload.worker_id),
            "correlation_id": correlation_id,
            "metadata": Jsonb(metadata),
        }
        await conn.execute(
            sql.INSERT_LEDGER,
            {
                **ledger_common,
                "from_status": "in_transit",
                "to_status": "available",
                "qty_delta": -qty,
                "balance_after": Decimal("0"),
            },
        )
        await conn.execute(
            sql.INSERT_LEDGER,
            {
                **ledger_common,
                "from_status": None,
                "to_status": "available",
                "qty_delta": qty,
                "balance_after": to_decimal(available_after["qty"]),
            },
        )
        completed = await fetch_one(
            conn,
            sql.COMPLETE_TASK,
            {"tenant_id": tenant_id, "task_id": task_id},
        )
        if completed is None:
            raise ConflictError("task could not be completed from its current state")
        await conn.execute(
            sql.COMPLETE_ASSIGNMENT,
            {"tenant_id": tenant_id, "task_id": task_id},
        )
        await emit_event(
            conn,
            tenant_id=tenant_id,
            aggregate_type="handling_unit",
            aggregate_id=UUID(str(hu["id"])),
            event_type="inbound.pallet.putaway_completed",
            payload={
                "task_id": str(task_id),
                "handling_unit_id": str(hu["id"]),
                "target_location_id": str(target_location_id),
                "qty": qty,
                "worker_id": str(payload.worker_id),
            },
            headers={"correlation_id": str(correlation_id)},
        )
        return PutawayCompleteResponse(
            task_id=task_id,
            handling_unit_id=UUID(str(hu["id"])),
            target_location_id=target_location_id,
            state=str(completed["state"]),
            stock_status="available",
        )
