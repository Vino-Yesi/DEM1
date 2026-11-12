from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb

from . import catalog, order_repo, sql, wave_repo
from .affinity import (
    AffinityConfig,
    Coordinate,
    PlannedStop,
    affinity_config_from_workflow,
)
from .allocation import AllocationRepository
from .auth import TenantContext
from .config import Settings
from .db import Database, fetch_all, fetch_one
from .errors import (
    ConcurrencyRetryExhausted,
    ConflictError,
    InsufficientStockError,
    NotFoundError,
    ValidationError,
)
from .idempotency import IdempotencyGuard
from .outbox import emit_event
from .schemas import DiscrepancyEventIn, DiscrepancyResponse
from .task_dispatcher import TaskDispatcher, TaskPush, TaskSpec
from .utils import to_decimal, utcnow

B2C_TASK_TYPES = ["PICK_PIECE", "PICK_CLUSTER", "PICK_ZONE", "PICK_MULTI_BATCH"]
ALL_PICK_TASK_TYPES = [*B2C_TASK_TYPES, "PALLET_PICK"]


def _reallocation_task_type(strategy: str) -> str:
    return {
        "WAVE": "PICK_PIECE",
        "CLUSTER": "PICK_CLUSTER",
        "MULTI_BATCH": "PICK_MULTI_BATCH",
        "ZONE": "PICK_ZONE",
    }.get(strategy, "PICK_CLUSTER")


class DigitalTwinExceptionEngine:
    """Deterministic discrepancy handling: quarantine, reroute, cycle count."""

    def __init__(
        self,
        db: Database,
        settings: Settings,
        dispatcher: TaskDispatcher,
        guard: IdempotencyGuard | None = None,
    ) -> None:
        self.db = db
        self.settings = settings
        self.dispatcher = dispatcher
        self.guard = guard or IdempotencyGuard()
        self.allocations = AllocationRepository()

    async def handle_discrepancy(
        self,
        ctx: TenantContext,
        payload: DiscrepancyEventIn,
        idempotency_key: str,
    ) -> DiscrepancyResponse:
        pushes: list[TaskPush] = []
        async with self.db.tenant_transaction(ctx.tenant_id) as conn:
            cached = await self.guard.begin(
                conn,
                ctx.tenant_id,
                "events.discrepancy",
                idempotency_key,
                {"client_id": ctx.client_id, "event": payload},
            )
            if cached is not None:
                return DiscrepancyResponse(**cached)

            await catalog.validate_api_client(
                conn,
                ctx,
                required_scope="events:write",
                mode=None,
            )
            workflow, _ = await catalog.load_workflow(
                conn, ctx.tenant_id, self.settings
            )
            location = await catalog.resolve_location(
                conn,
                ctx.tenant_id,
                location_id=payload.location_id,
                location_barcode=payload.location_barcode,
                warehouse_id=payload.warehouse_id,
            )
            location_id = UUID(str(location["id"]))
            warehouse_id = UUID(str(location["warehouse_id"]))
            if warehouse_id != payload.warehouse_id:
                raise ValidationError(
                    "discrepancy location warehouse does not match payload warehouse",
                    {
                        "location_id": str(location_id),
                        "location_warehouse_id": str(warehouse_id),
                        "payload_warehouse_id": str(payload.warehouse_id),
                    },
                )
            if payload.sku_id is not None:
                sku = await catalog.resolve_sku_by_id(
                    conn, ctx.tenant_id, payload.sku_id
                )
            else:
                sku = await catalog.resolve_sku(
                    conn, ctx.tenant_id, str(payload.sku_code)
                )
            sku_id = UUID(str(sku["id"]))
            lot_id = await catalog.resolve_lot_by_id_or_code(
                conn,
                ctx.tenant_id,
                sku_id,
                payload.lot_id,
                payload.lot_code,
            )

            event_row = await fetch_one(
                conn,
                sql.INSERT_DISCREPANCY_EVENT,
                {
                    "tenant_id": ctx.tenant_id,
                    "source": payload.source.value,
                    "source_event_id": payload.source_event_id,
                    "warehouse_id": warehouse_id,
                    "location_id": location_id,
                    "sku_id": sku_id,
                    "lot_id": lot_id,
                    "expected_qty": payload.expected_qty,
                    "found_qty": payload.found_qty,
                    "wave_id": payload.wave_id,
                    "observed_at": payload.observed_at,
                },
            )
            if event_row is None:
                existing = await fetch_one(
                    conn,
                    """
                    SELECT id, processing_result
                    FROM inventory_discrepancy_events
                    WHERE tenant_id = %(tenant_id)s
                      AND source = %(source)s::discrepancy_source
                      AND source_event_id = %(source_event_id)s
                    """,
                    {
                        "tenant_id": ctx.tenant_id,
                        "source": payload.source.value,
                        "source_event_id": payload.source_event_id,
                    },
                )
                raise ConflictError(
                    "source_event_id already exists; retry with the original idempotency key",
                    {
                        "source": payload.source.value,
                        "source_event_id": payload.source_event_id,
                        "existing_event_id": str(existing["id"]) if existing else None,
                    },
                )
            discrepancy_event_id = UUID(str(event_row["id"]))
            correlation_id = uuid4()

            locked_location = await fetch_one(
                conn,
                sql.LOCK_LOCATION_FOR_QUARANTINE,
                {"location_id": location_id},
            )
            if locked_location is None:
                raise NotFoundError("Location", location_id)
            quarantine = await fetch_one(
                conn,
                sql.UPDATE_LOCATION_QUARANTINE,
                {
                    "location_id": location_id,
                    "location_version": int(locked_location["version"]),
                    "force": True,
                    "reason": (
                        f"{payload.source.value} discrepancy: expected {payload.expected_qty}, "
                        f"found {payload.found_qty} for {sku['sku_code']}"
                    ),
                },
            )
            if quarantine is None:
                raise ConflictError("location quarantine update failed")

            released_allocations, released_line_ids = await self._release_allocations(
                conn,
                tenant_id=ctx.tenant_id,
                location_id=location_id,
                wave_id=payload.wave_id,
            )
            quarantined_buckets = await self._quarantine_stock(
                conn,
                tenant_id=ctx.tenant_id,
                warehouse_id=warehouse_id,
                location_id=location_id,
                sku_id=sku_id,
                lot_id=lot_id,
                discrepancy_event_id=discrepancy_event_id,
                correlation_id=correlation_id,
            )

            strategy = str(workflow["picking"]["strategy"]).upper()
            cfg: AffinityConfig = affinity_config_from_workflow(workflow, strategy)
            dock, dock_row = await catalog.get_dock(conn, warehouse_id)
            line_rows = await self._unfulfilled_line_rows(
                conn,
                tenant_id=ctx.tenant_id,
                wave_id=payload.wave_id,
                released_line_ids=released_line_ids,
            )
            preferred_locations = await self._preferred_alternative_locations(
                conn,
                tenant_id=ctx.tenant_id,
                wave_id=payload.wave_id,
                exclude_location_id=location_id,
                dock=dock,
                cfg=cfg,
            )
            reallocated_lines: list[UUID] = []
            backordered_lines: list[UUID] = []
            new_stops: list[PlannedStop] = []
            reallocation_task_ids: list[UUID] = []
            reallocation_shortages: list[dict[str, Any]] = []

            for row in line_rows:
                line = order_repo.to_line_requirement(row, cfg.allowed_location_types)
                effective_lot = line.lot_id or lot_id
                line = replace(line, lot_id=effective_lot, require_same_lot=True)
                if line.remaining_qty <= 0:
                    continue
                try:
                    results = await self.allocations.allocate_line(
                        conn,
                        tenant_id=ctx.tenant_id,
                        warehouse_id=warehouse_id,
                        line=line,
                        dock=dock,
                        cfg=cfg,
                        wave_id=payload.wave_id,
                        prefer_location_ids=preferred_locations,
                        exclude_location_ids=[location_id],
                        require_same_lot=True,
                        correlation_id=correlation_id,
                        actor="exception_engine",
                    )
                except (InsufficientStockError, ConcurrencyRetryExhausted) as exc:
                    backordered_lines.append(line.order_line_id)
                    await order_repo.mark_line_backordered(
                        conn, ctx.tenant_id, line.order_line_id
                    )
                    shortage = {
                        "order_line_id": str(line.order_line_id),
                        "sku_id": str(line.sku_id),
                        "remaining_qty": str(line.remaining_qty),
                        "reason": exc.code,
                    }
                    reallocation_shortages.append(shortage)
                    await self._create_replenishment_task(
                        conn,
                        ctx=ctx,
                        warehouse_id=warehouse_id,
                        line=line,
                        workflow=workflow,
                        correlation_id=correlation_id,
                        discrepancy_event_id=discrepancy_event_id,
                    )
                    continue

                reallocated_lines.append(line.order_line_id)
                for result in results:
                    new_stops.append(result.to_planned_stop(line.sku_id))
                task_id = await self._create_reallocation_pick_task(
                    conn,
                    ctx=ctx,
                    warehouse_id=warehouse_id,
                    wave_id=payload.wave_id,
                    line_id=line.order_line_id,
                    sku_id=line.sku_id,
                    strategy=strategy,
                    results=results,
                    dock=dock,
                    dock_row=dock_row,
                    workflow=workflow,
                    correlation_id=correlation_id,
                    discrepancy_event_id=discrepancy_event_id,
                )
                if task_id is not None:
                    reallocation_task_ids.append(task_id)

            affected_wave_ids = await self._affected_wave_ids(
                conn,
                tenant_id=ctx.tenant_id,
                explicit_wave_id=payload.wave_id,
                line_ids=reallocated_lines,
            )
            for wave_id in affected_wave_ids:
                await wave_repo.rebuild_wave_route(
                    conn,
                    tenant_id=ctx.tenant_id,
                    wave_id=wave_id,
                    warehouse_id=warehouse_id,
                    dock=dock,
                    cfg=cfg,
                    new_stops=new_stops,
                    exclude_location_id=location_id,
                )

            cycle_count_task_id = await self._create_cycle_count(
                conn,
                ctx=ctx,
                warehouse_id=warehouse_id,
                location=location,
                sku_id=sku_id,
                lot_id=lot_id,
                discrepancy_event_id=discrepancy_event_id,
                payload=payload,
                workflow=workflow,
                correlation_id=correlation_id,
            )

            pushes.extend(
                await self.dispatcher.assign_queued_tasks(
                    conn,
                    ctx.tenant_id,
                    warehouse_id=warehouse_id,
                    limit=max(1, len(reallocation_task_ids) + 1),
                )
            )
            processing_result = {
                "discrepancy_event_id": str(discrepancy_event_id),
                "expected_qty": str(payload.expected_qty),
                "found_qty": str(payload.found_qty),
                "quarantined_buckets": [str(item) for item in quarantined_buckets],
                "released_allocations": [str(item) for item in released_allocations],
                "reallocated_lines": [str(item) for item in reallocated_lines],
                "backordered_lines": [str(item) for item in backordered_lines],
                "reallocation_shortages": reallocation_shortages,
                "reallocation_task_ids": [str(item) for item in reallocation_task_ids],
                "cycle_count_task_id": str(cycle_count_task_id),
                "affected_wave_ids": [str(item) for item in affected_wave_ids],
                "correlation_id": str(correlation_id),
            }
            response = DiscrepancyResponse(
                discrepancy_event_id=discrepancy_event_id,
                location_id=location_id,
                location_status=str(quarantine["status"]),
                quarantined_buckets=quarantined_buckets,
                released_allocations=released_allocations,
                reallocated_lines=reallocated_lines,
                backordered_lines=backordered_lines,
                cycle_count_task_id=cycle_count_task_id,
                processing_result=processing_result,
            )
            await conn.execute(
                sql.UPDATE_DISCREPANCY_PROCESSED,
                {
                    "tenant_id": ctx.tenant_id,
                    "event_id": discrepancy_event_id,
                    "result": Jsonb(jsonable_encoder(processing_result)),
                },
            )
            await emit_event(
                conn,
                tenant_id=ctx.tenant_id,
                aggregate_type="location",
                aggregate_id=location_id,
                event_type="location.quarantined",
                payload={
                    "discrepancy_event_id": str(discrepancy_event_id),
                    "source": payload.source.value,
                    "expected_qty": payload.expected_qty,
                    "found_qty": payload.found_qty,
                    "sku_id": str(sku_id),
                    "lot_id": str(lot_id) if lot_id else None,
                },
                headers={"correlation_id": str(correlation_id)},
            )
            await emit_event(
                conn,
                tenant_id=ctx.tenant_id,
                aggregate_type="discrepancy_event",
                aggregate_id=discrepancy_event_id,
                event_type="discrepancy.processed",
                payload=processing_result,
                headers={"correlation_id": str(correlation_id)},
            )
            await self.guard.complete(
                conn,
                ctx.tenant_id,
                "events.discrepancy",
                idempotency_key,
                response,
            )
        await self.dispatcher.publish(pushes)
        return response

    async def _release_allocations(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        location_id: UUID,
        wave_id: UUID | None,
    ) -> tuple[list[UUID], set[UUID]]:
        rows = await fetch_all(
            conn,
            sql.RELEASE_ALLOCATIONS_AT_LOCATION,
            {"tenant_id": tenant_id, "location_id": location_id, "wave_id": wave_id},
        )
        released: list[UUID] = []
        line_ids: set[UUID] = set()
        line_versions: dict[UUID, int] = {}
        for row in rows:
            allocation_id = UUID(str(row["allocation_id"]))
            order_line_id = UUID(str(row["order_line_id"]))
            qty = to_decimal(row["allocation_qty"])
            version = int(row["line_version"])
            current_version = line_versions.get(order_line_id, version)
            updated = await fetch_one(
                conn,
                sql.UPDATE_ALLOCATION_RELEASE,
                {"tenant_id": tenant_id, "allocation_id": allocation_id},
            )
            if updated is None:
                continue
            line_after = await fetch_one(
                conn,
                sql.UPDATE_ORDER_LINE_RELEASE,
                {
                    "tenant_id": tenant_id,
                    "order_line_id": order_line_id,
                    "qty": qty,
                    "line_version": current_version,
                },
            )
            if line_after is None:
                raise ConflictError(
                    "order line version changed during discrepancy allocation release",
                    {"order_line_id": str(order_line_id)},
                )
            line_versions[order_line_id] = int(line_after["version"])
            await conn.execute(
                sql.CANCEL_TASKS_BY_ALLOCATION,
                {
                    "tenant_id": tenant_id,
                    "task_types": ALL_PICK_TASK_TYPES,
                    "allocation_id": str(allocation_id),
                },
            )
            await conn.execute(
                sql.CANCEL_TASKS_FOR_REF,
                {
                    "tenant_id": tenant_id,
                    "source_ref_id": order_line_id,
                    "task_types": ALL_PICK_TASK_TYPES,
                },
            )
            released.append(allocation_id)
            line_ids.add(order_line_id)
        return released, line_ids

    async def _quarantine_stock(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        warehouse_id: UUID,
        location_id: UUID,
        sku_id: UUID,
        lot_id: UUID | None,
        discrepancy_event_id: UUID,
        correlation_id: UUID,
    ) -> list[UUID]:
        rows = await fetch_all(
            conn,
            sql.LOCK_DISCREPANCY_BUCKETS,
            {
                "tenant_id": tenant_id,
                "location_id": location_id,
                "sku_id": sku_id,
                "lot_id": lot_id,
            },
        )
        quarantined: list[UUID] = []
        header_versions: dict[UUID, int] = {}
        for row in rows:
            bucket_id = UUID(str(row["bucket_id"]))
            qty = to_decimal(row["balance_qty"])
            if qty <= 0:
                continue
            if bucket_id not in header_versions:
                header_versions[bucket_id] = int(row["header_version"])
                bumped = await fetch_one(
                    conn,
                    sql.BUMP_HEADER_VERSION,
                    {
                        "tenant_id": tenant_id,
                        "bucket_id": bucket_id,
                        "header_version": header_versions[bucket_id],
                    },
                )
                if bumped is None:
                    raise ConflictError(
                        "bucket header version changed during quarantine",
                        {"bucket_id": str(bucket_id)},
                    )
                header_versions[bucket_id] = int(bumped["version"])
            quarantine_after = await fetch_one(
                conn,
                sql.UPSERT_QUARANTINE_BALANCE,
                {"tenant_id": tenant_id, "bucket_id": bucket_id, "qty": qty},
            )
            deleted = await fetch_one(
                conn,
                sql.DELETE_BALANCE_ROW,
                {
                    "tenant_id": tenant_id,
                    "balance_id": row["balance_id"],
                    "balance_version": int(row["balance_version"]),
                },
            )
            if quarantine_after is None or deleted is None:
                raise ConflictError(
                    "stock status transition to quarantine failed",
                    {"bucket_id": str(bucket_id), "balance_id": str(row["balance_id"])},
                )
            metadata = {
                "correlation_id": str(correlation_id),
                "discrepancy_event_id": str(discrepancy_event_id),
                "from_status": str(row["stock_status"]),
            }
            ledger_common = {
                "tenant_id": tenant_id,
                "bucket_id": bucket_id,
                "sku_id": sku_id,
                "warehouse_id": warehouse_id,
                "location_id": location_id,
                "lot_id": row["lot_id"],
                "serial_number": row["serial_number"],
                "reason": "discrepancy_quarantine",
                "ref_type": "inventory_discrepancy",
                "ref_id": discrepancy_event_id,
                "actor": "exception_engine",
                "correlation_id": correlation_id,
                "metadata": Jsonb(metadata),
            }
            await conn.execute(
                sql.INSERT_LEDGER,
                {
                    **ledger_common,
                    "from_status": str(row["stock_status"]),
                    "to_status": "quarantine",
                    "qty_delta": -qty,
                    "balance_after": Decimal("0"),
                },
            )
            await conn.execute(
                sql.INSERT_LEDGER,
                {
                    **ledger_common,
                    "from_status": None,
                    "to_status": "quarantine",
                    "qty_delta": qty,
                    "balance_after": to_decimal(quarantine_after["qty"]),
                },
            )
            quarantined.append(bucket_id)
        return list(dict.fromkeys(quarantined))

    async def _unfulfilled_line_rows(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        wave_id: UUID | None,
        released_line_ids: set[UUID],
    ) -> list[dict[str, Any]]:
        rows_by_id: dict[UUID, dict[str, Any]] = {}
        if wave_id is not None:
            wave_rows = await fetch_all(
                conn,
                sql.GET_WAVE_UNFULFILLED_LINES,
                {"tenant_id": tenant_id, "wave_id": wave_id},
            )
            for row in wave_rows:
                rows_by_id[UUID(str(row["id"]))] = row
            extra_ids = released_line_ids - set(rows_by_id)
        else:
            extra_ids = released_line_ids
        if extra_ids:
            extra_rows = await fetch_all(
                conn,
                sql.GET_LINES_BY_IDS,
                {"tenant_id": tenant_id, "line_ids": list(extra_ids)},
            )
            for row in extra_rows:
                rows_by_id[UUID(str(row["id"]))] = row
        return [rows_by_id[key] for key in sorted(rows_by_id, key=str)]

    async def _preferred_alternative_locations(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        wave_id: UUID | None,
        exclude_location_id: UUID,
        dock: Coordinate,
        cfg: AffinityConfig,
    ) -> list[UUID]:
        if wave_id is None:
            return []
        route = await wave_repo.get_wave_route(
            conn,
            tenant_id=tenant_id,
            wave_id=wave_id,
            dock=dock,
            cfg=cfg,
        )
        return list(
            dict.fromkeys(
                stop.location_id
                for stop in route.stops
                if stop.location_id != exclude_location_id
            )
        )

    async def _create_reallocation_pick_task(
        self,
        conn: Any,
        *,
        ctx: TenantContext,
        warehouse_id: UUID,
        wave_id: UUID | None,
        line_id: UUID,
        sku_id: UUID,
        strategy: str,
        results: list[Any],
        dock: Coordinate,
        dock_row: dict[str, Any],
        workflow: dict[str, Any],
        correlation_id: UUID,
        discrepancy_event_id: UUID,
    ) -> UUID | None:
        task_type = _reallocation_task_type(strategy)
        source_ref_id = uuid5(
            NAMESPACE_URL,
            f"wms:reallocation:{ctx.tenant_id}:{discrepancy_event_id}:{line_id}",
        )
        stops = [
            {
                "location_id": str(item.location_id),
                "sku_id": str(sku_id),
                "lot_id": str(item.lot_id) if item.lot_id else None,
                "qty": item.qty,
                "zone": item.zone,
                "x_m": item.coordinate.x,
                "y_m": item.coordinate.y,
                "z_m": item.coordinate.z,
            }
            for item in results
        ]
        zone = results[0].zone if results else None
        payload = {
            "wave_id": str(wave_id) if wave_id else None,
            "order_line_id": str(line_id),
            "origin_discrepancy_event_id": str(discrepancy_event_id),
            "stops": stops,
            "allocations": [str(item.allocation_id) for item in results],
            "dock": {
                "x": dock.x,
                "y": dock.y,
                "z": dock.z,
                "zone": dock.zone or str(dock_row["zone"]),
            },
            "correlation_id": str(correlation_id),
        }
        priority = int(workflow["exceptions"]["replenishment_priority"]) + 50
        return await self.dispatcher.enqueue_task(
            conn,
            TaskSpec(
                tenant_id=ctx.tenant_id,
                task_type=task_type,
                priority=min(1000, priority),
                warehouse_id=warehouse_id,
                required_equipment=list(workflow["b2c"]["required_equipment"]),
                required_zone=zone,
                location_id=results[0].location_id if results else None,
                source_ref_type="discrepancy_reallocation",
                source_ref_id=source_ref_id,
                wave_id=wave_id,
                order_id=None,
                payload=payload,
                not_before=utcnow(),
            ),
        )

    async def _create_replenishment_task(
        self,
        conn: Any,
        *,
        ctx: TenantContext,
        warehouse_id: UUID,
        line: Any,
        workflow: dict[str, Any],
        correlation_id: UUID,
        discrepancy_event_id: UUID,
    ) -> UUID | None:
        source_ref_id = uuid5(
            NAMESPACE_URL,
            f"wms:discrepancy-replenishment:{ctx.tenant_id}:{discrepancy_event_id}:{line.order_line_id}",
        )
        return await self.dispatcher.enqueue_task(
            conn,
            TaskSpec(
                tenant_id=ctx.tenant_id,
                task_type="REPLENISHMENT",
                priority=int(workflow["exceptions"]["replenishment_priority"]),
                warehouse_id=warehouse_id,
                required_equipment=list(workflow["b2b"]["required_equipment"]),
                required_zone=None,
                location_id=None,
                source_ref_type="discrepancy_shortage",
                source_ref_id=source_ref_id,
                wave_id=None,
                order_id=line.order_id,
                payload={
                    "order_id": str(line.order_id),
                    "order_line_id": str(line.order_line_id),
                    "sku_id": str(line.sku_id),
                    "lot_id": str(line.lot_id) if line.lot_id else None,
                    "remaining_qty": line.remaining_qty,
                    "origin_discrepancy_event_id": str(discrepancy_event_id),
                    "correlation_id": str(correlation_id),
                },
                not_before=utcnow(),
            ),
        )

    async def _affected_wave_ids(
        self,
        conn: Any,
        *,
        tenant_id: UUID,
        explicit_wave_id: UUID | None,
        line_ids: list[UUID],
    ) -> list[UUID]:
        if explicit_wave_id is not None:
            return [explicit_wave_id]
        if not line_ids:
            return []
        rows = await fetch_all(
            conn,
            """
            SELECT DISTINCT wave_id
            FROM wave_order_lines
            WHERE tenant_id = %(tenant_id)s
              AND order_line_id = ANY(%(line_ids)s::uuid[])
            """,
            {"tenant_id": tenant_id, "line_ids": line_ids},
        )
        return [UUID(str(row["wave_id"])) for row in rows]

    async def _create_cycle_count(
        self,
        conn: Any,
        *,
        ctx: TenantContext,
        warehouse_id: UUID,
        location: dict[str, Any],
        sku_id: UUID,
        lot_id: UUID | None,
        discrepancy_event_id: UUID,
        payload: DiscrepancyEventIn,
        workflow: dict[str, Any],
        correlation_id: UUID,
    ) -> UUID:
        cycle_count = await fetch_one(
            conn,
            sql.INSERT_CYCLE_COUNT,
            {
                "tenant_id": ctx.tenant_id,
                "discrepancy_event_id": discrepancy_event_id,
                "warehouse_id": warehouse_id,
                "location_id": location["id"],
                "sku_id": sku_id,
                "lot_id": lot_id,
                "reason": (
                    f"AMR/RF discrepancy expected {payload.expected_qty}, found {payload.found_qty}"
                ),
                "priority": int(workflow["exceptions"]["cycle_count_priority"]),
            },
        )
        if cycle_count is None:
            raise ConflictError("cycle count insert failed")
        task_type = "CYCLE_COUNT"
        source_ref_type = "discrepancy_event"
        task_id = await self.dispatcher.enqueue_task(
            conn,
            TaskSpec(
                tenant_id=ctx.tenant_id,
                task_type=task_type,
                priority=int(workflow["exceptions"]["cycle_count_priority"]),
                warehouse_id=warehouse_id,
                required_equipment=list(
                    workflow["exceptions"]["cycle_count_equipment"]
                ),
                required_zone=str(location["zone"]),
                location_id=UUID(str(location["id"])),
                source_ref_type=source_ref_type,
                source_ref_id=discrepancy_event_id,
                wave_id=payload.wave_id,
                order_id=None,
                payload={
                    "cycle_count_id": str(cycle_count["id"]),
                    "discrepancy_event_id": str(discrepancy_event_id),
                    "location_id": str(location["id"]),
                    "location_barcode": str(location["barcode"]),
                    "zone": str(location["zone"]),
                    "sku_id": str(sku_id),
                    "lot_id": str(lot_id) if lot_id else None,
                    "expected_qty": payload.expected_qty,
                    "found_qty": payload.found_qty,
                    "correlation_id": str(correlation_id),
                },
                not_before=utcnow(),
            ),
        )
        if task_id is not None:
            return task_id
        existing = await fetch_one(
            conn,
            sql.GET_OPEN_TASK_BY_REF,
            {
                "tenant_id": ctx.tenant_id,
                "source_ref_type": source_ref_type,
                "source_ref_id": discrepancy_event_id,
                "task_type": task_type,
            },
        )
        if existing is None:
            raise NotFoundError("Open cycle count task", discrepancy_event_id)
        return UUID(str(existing["id"]))
