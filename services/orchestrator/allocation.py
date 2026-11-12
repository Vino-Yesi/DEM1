from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Sequence
from uuid import UUID

from psycopg.types.json import Jsonb

from . import sql
from .affinity import AffinityConfig, Coordinate, LineRequirement, PlannedStop
from .db import fetch_one
from .errors import ConcurrencyRetryExhausted, ConflictError, InsufficientStockError
from .outbox import emit_event
from .utils import to_decimal, utcnow


def _optional_uuid(value: Any) -> UUID | None:
    return None if value is None else UUID(str(value))


def _date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _lot_mode(
    line: LineRequirement,
    *,
    require_same_lot: bool,
    lot_locked: bool,
    locked_lot: UUID | None,
) -> tuple[str, UUID | None]:
    if line.lot_id is not None:
        return "EXACT", line.lot_id
    if not require_same_lot:
        return "IGNORE", None
    if lot_locked:
        return ("NULL", None) if locked_lot is None else ("EXACT", locked_lot)
    return "SINGLE_LOT", None


@dataclass(frozen=True)
class AllocationResult:
    allocation_id: UUID
    order_line_id: UUID
    bucket_id: UUID
    location_id: UUID
    lot_id: UUID | None
    qty: Decimal
    zone: str
    coordinate: Coordinate
    travel_seq: int
    expiration_date: date | None
    handling_unit_id: UUID | None
    wave_id: UUID | None

    def to_planned_stop(self, sku_id: UUID) -> PlannedStop:
        return PlannedStop(
            bucket_id=self.bucket_id,
            location_id=self.location_id,
            sku_id=sku_id,
            lot_id=self.lot_id,
            qty=self.qty,
            coordinate=self.coordinate,
            zone=self.zone,
            travel_seq=self.travel_seq,
            location_type="",
            expiration_date=self.expiration_date,
            stop_seq=0,
        )


class AllocationRepository:
    """Atomic stock reservation with row locks, version predicates and ledger."""

    async def allocate_line(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        warehouse_id: UUID,
        line: LineRequirement,
        dock: Coordinate,
        cfg: AffinityConfig,
        wave_id: UUID | None,
        prefer_location_ids: Sequence[UUID] = (),
        exclude_location_ids: Sequence[UUID] = (),
        require_same_lot: bool | None = None,
        correlation_id: UUID,
        actor: str = "system",
        max_attempts: int = 32,
    ) -> list[AllocationResult]:
        """Reserve one line inside a SAVEPOINT.

        A shortage or a version conflict rolls back every partial balance move,
        allocation row, order-line version and outbox event for this line.
        """
        async with conn.transaction():
            return await self._allocate_line(
                conn,
                tenant_id=tenant_id,
                warehouse_id=warehouse_id,
                line=line,
                dock=dock,
                cfg=cfg,
                wave_id=wave_id,
                prefer_location_ids=prefer_location_ids,
                exclude_location_ids=exclude_location_ids,
                require_same_lot=require_same_lot,
                correlation_id=correlation_id,
                actor=actor,
                max_attempts=max_attempts,
            )

    async def _allocate_line(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        warehouse_id: UUID,
        line: LineRequirement,
        dock: Coordinate,
        cfg: AffinityConfig,
        wave_id: UUID | None,
        prefer_location_ids: Sequence[UUID] = (),
        exclude_location_ids: Sequence[UUID] = (),
        require_same_lot: bool | None = None,
        correlation_id: UUID,
        actor: str = "system",
        max_attempts: int = 32,
    ) -> list[AllocationResult]:
        same_lot = (
            line.require_same_lot if require_same_lot is None else require_same_lot
        )
        remaining = line.remaining_qty
        line_version = line.version
        allowed_types = list(line.allowed_location_types or cfg.allowed_location_types)
        preferred = list(dict.fromkeys(prefer_location_ids))
        excluded = list(dict.fromkeys(exclude_location_ids))
        locked_lot: UUID | None = line.lot_id
        lot_locked = line.lot_id is not None or not same_lot
        results: list[AllocationResult] = []

        for _attempt in range(max_attempts):
            if remaining <= 0:
                break
            mode, lot_value = _lot_mode(
                line,
                require_same_lot=same_lot,
                lot_locked=lot_locked,
                locked_lot=locked_lot,
            )
            locked = await fetch_one(
                conn,
                sql.LOCK_ALLOCATION_CANDIDATE,
                {
                    "tenant_id": tenant_id,
                    "sku_id": line.sku_id,
                    "lot_filter_mode": mode,
                    "lot_filter": lot_value,
                    "remaining_qty": remaining,
                    "exclude_locations": excluded,
                    "expiry_min": line.expiry_min,
                    "expiry_max": line.expiry_max,
                    "allowed_types": allowed_types,
                    "preferred_locations": preferred,
                    "dock_x": dock.x,
                    "dock_y": dock.y,
                    "dock_z": dock.z,
                    "dock_zone": dock.zone,
                },
            )
            if locked is None:
                break
            take = min(to_decimal(locked["available_qty"]), remaining)
            if take <= 0:
                continue

            available_after = await fetch_one(
                conn,
                sql.UPDATE_AVAILABLE_BALANCE,
                {
                    "tenant_id": tenant_id,
                    "balance_id": locked["balance_id"],
                    "balance_version": int(locked["balance_version"]),
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
                    "bucket_id": locked["bucket_id"],
                    "qty": take,
                },
            )
            if allocated_after is None:
                raise ConflictError("allocated balance upsert returned no row")

            header_after = await fetch_one(
                conn,
                sql.BUMP_HEADER_VERSION,
                {
                    "tenant_id": tenant_id,
                    "bucket_id": locked["bucket_id"],
                    "header_version": int(locked["header_version"]),
                },
            )
            if header_after is None:
                raise ConcurrencyRetryExhausted(
                    "inventory bucket header version changed while locked",
                    {"bucket_id": str(locked["bucket_id"])},
                )

            allocation = await fetch_one(
                conn,
                sql.INSERT_ALLOCATION,
                {
                    "tenant_id": tenant_id,
                    "order_line_id": line.order_line_id,
                    "bucket_id": locked["bucket_id"],
                    "wave_id": wave_id,
                    "sku_id": line.sku_id,
                    "lot_id": locked["lot_id"],
                    "warehouse_id": warehouse_id,
                    "location_id": locked["location_id"],
                    "handling_unit_id": locked["handling_unit_id"],
                    "qty": take,
                },
            )
            if allocation is None:
                raise ConflictError("allocation insert returned no row")
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
                    "order line version changed or allocation exceeds remaining quantity",
                    {"order_line_id": str(line.order_line_id)},
                )
            line_version = int(line_after["version"])

            metadata = {
                "correlation_id": str(correlation_id),
                "wave_id": str(wave_id) if wave_id else None,
                "order_line_id": str(line.order_line_id),
                "allocation_id": str(allocation_id),
                "preferred_locations": [str(item) for item in preferred],
                "excluded_locations": [str(item) for item in excluded],
            }
            ledger_common = {
                "tenant_id": tenant_id,
                "bucket_id": locked["bucket_id"],
                "sku_id": line.sku_id,
                "warehouse_id": warehouse_id,
                "location_id": locked["location_id"],
                "lot_id": locked["lot_id"],
                "serial_number": locked["serial_number"],
                "reason": "allocation",
                "ref_type": "inventory_allocation",
                "ref_id": allocation_id,
                "actor": actor,
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

            result = AllocationResult(
                allocation_id=allocation_id,
                order_line_id=line.order_line_id,
                bucket_id=UUID(str(locked["bucket_id"])),
                location_id=UUID(str(locked["location_id"])),
                lot_id=_optional_uuid(locked["lot_id"]),
                qty=take,
                zone=str(locked["zone"]),
                coordinate=Coordinate(
                    x=to_decimal(locked["x_m"]),
                    y=to_decimal(locked["y_m"]),
                    z=to_decimal(locked["z_m"]),
                    zone=str(locked["zone"]),
                ),
                travel_seq=int(locked["travel_seq"]),
                expiration_date=_date(locked["expiration_date"]),
                handling_unit_id=_optional_uuid(locked["handling_unit_id"]),
                wave_id=wave_id,
            )
            results.append(result)
            remaining -= take
            if same_lot and not lot_locked:
                locked_lot = result.lot_id
                lot_locked = True

        if remaining > 0:
            if not results:
                raise InsufficientStockError(
                    "no unlocked available stock satisfies the order line",
                    {
                        "order_line_id": str(line.order_line_id),
                        "sku_id": str(line.sku_id),
                        "shortage_qty": str(remaining),
                        "excluded_locations": [str(item) for item in excluded],
                    },
                )
            raise ConcurrencyRetryExhausted(
                "partial allocation could not be completed within the retry budget",
                {
                    "order_line_id": str(line.order_line_id),
                    "shortage_qty": str(remaining),
                    "allocated_qty": str(
                        sum((item.qty for item in results), Decimal("0"))
                    ),
                },
            )

        await emit_event(
            conn,
            tenant_id=tenant_id,
            aggregate_type="order_line",
            aggregate_id=line.order_line_id,
            event_type="inventory.allocated",
            payload={
                "order_id": str(line.order_id),
                "wave_id": str(wave_id) if wave_id else None,
                "sku_id": str(line.sku_id),
                "allocations": [
                    {
                        "allocation_id": str(item.allocation_id),
                        "bucket_id": str(item.bucket_id),
                        "location_id": str(item.location_id),
                        "lot_id": str(item.lot_id) if item.lot_id else None,
                        "qty": item.qty,
                    }
                    for item in results
                ],
            },
            occurred_at=utcnow(),
            headers={"correlation_id": str(correlation_id)},
        )
        return results
