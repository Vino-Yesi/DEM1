from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Sequence
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from . import catalog, order_repo, wave_repo
from .affinity import (
    AffinityConfig,
    CandidateStock,
    LineRequirement,
    OrderWaveMeta,
    RoutePlan,
    WaveDecision,
    affinity_config_from_workflow,
    build_route,
    choose_wave,
    compute_cutoff_bucket,
    merge_routes,
    plan_line_stops,
)
from .allocation import AllocationRepository, AllocationResult
from .auth import TenantContext
from .b2b import B2BRepository, PalletReservation
from .config import Settings
from .db import Database
from .errors import ValidationError
from .idempotency import IdempotencyGuard
from .outbox import emit_event
from .schemas import (
    AllocationCreated,
    DirectedPutawayIn,
    DirectedPutawayResponse,
    OrderIn,
    OrderMode,
    PutawayCompleteIn,
    PutawayCompleteResponse,
    RouteOrderResponse,
    TaskCreated,
)
from .task_dispatcher import TaskDispatcher, TaskPush, TaskSpec
from .utils import utcnow


@dataclass
class PathResult:
    wave_ids: list[UUID] = field(default_factory=list)
    allocations: list[AllocationCreated] = field(default_factory=list)
    tasks: list[TaskCreated] = field(default_factory=list)
    backordered_lines: list[UUID] = field(default_factory=list)
    strategy: str | None = None


@dataclass(frozen=True)
class WavePlan:
    wave_id: UUID
    zone: str | None
    release_at: datetime
    line_ids: tuple[UUID, ...]
    merged_route: RoutePlan
    decision: WaveDecision
    meta: OrderWaveMeta


def _aware(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _chunks(items: Sequence[Any], size: int) -> list[list[Any]]:
    step = max(int(size), 1)
    return [list(items[index : index + step]) for index in range(0, len(items), step)]


def pick_priority(
    order_priority: int, sla_ship_at: Any, now: datetime, strategy: str
) -> int:
    urgency = 0
    sla = _aware(sla_ship_at)
    if sla is not None:
        minutes = (sla - now).total_seconds() / 60.0
        if minutes <= 30:
            urgency = 300
        elif minutes <= 60:
            urgency = 200
        elif minutes <= 120:
            urgency = 100
        elif minutes < 0:
            urgency = 400
    strategy_bonus = 50 if strategy in ("CLUSTER", "MULTI_BATCH") else 0
    return min(1000, max(0, int(order_priority) + urgency + strategy_bonus))


def _task_type_for_strategy(strategy: str) -> str:
    return {
        "WAVE": "PICK_PIECE",
        "CLUSTER": "PICK_CLUSTER",
        "MULTI_BATCH": "PICK_MULTI_BATCH",
        "ZONE": "PICK_ZONE",
    }[strategy]


class HybridRoutingService:
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
        self.b2b = B2BRepository()

    async def route_order(
        self,
        ctx: TenantContext,
        payload: OrderIn,
        idempotency_key: str,
    ) -> RouteOrderResponse:
        pushes: list[TaskPush] = []
        async with self.db.tenant_transaction(ctx.tenant_id) as conn:
            cached = await self.guard.begin(
                conn,
                ctx.tenant_id,
                "orders.route",
                idempotency_key,
                {"client_id": ctx.client_id, "order": payload},
            )
            if cached is not None:
                return RouteOrderResponse(**cached)

            await catalog.validate_api_client(
                conn,
                ctx,
                required_scope="orders:write",
                mode=payload.mode,
            )
            workflow, workflow_version = await catalog.load_workflow(
                conn, ctx.tenant_id, self.settings
            )
            order_id, _ = await order_repo.create_order(
                conn,
                ctx,
                payload,
                workflow_version,
                idempotency_key,
            )
            correlation_id = uuid4()
            path = PathResult()

            if payload.mode == OrderMode.B2C:
                path = await self._route_b2c(
                    conn,
                    ctx=ctx,
                    order_id=order_id,
                    workflow=workflow,
                    workflow_version=workflow_version,
                    correlation_id=correlation_id,
                    pushes=pushes,
                )
            elif payload.mode == OrderMode.B2B:
                path = await self._route_b2b(
                    conn,
                    ctx=ctx,
                    order_id=order_id,
                    workflow=workflow,
                    correlation_id=correlation_id,
                    pushes=pushes,
                )
            else:
                b2c_path = await self._route_b2c(
                    conn,
                    ctx=ctx,
                    order_id=order_id,
                    workflow=workflow,
                    workflow_version=workflow_version,
                    correlation_id=correlation_id,
                    pushes=pushes,
                )
                b2b_path = await self._route_b2b(
                    conn,
                    ctx=ctx,
                    order_id=order_id,
                    workflow=workflow,
                    correlation_id=correlation_id,
                    pushes=pushes,
                )
                path = PathResult(
                    wave_ids=b2c_path.wave_ids,
                    allocations=[*b2c_path.allocations, *b2b_path.allocations],
                    tasks=[*b2c_path.tasks, *b2b_path.tasks],
                    backordered_lines=[
                        *b2c_path.backordered_lines,
                        *b2b_path.backordered_lines,
                    ],
                    strategy="HYBRID_B2C_PLUS_B2B",
                )

            status = "exception" if path.backordered_lines else "allocated"
            await order_repo.update_order_status(conn, ctx.tenant_id, order_id, status)
            response = RouteOrderResponse(
                order_id=order_id,
                mode=payload.mode,
                strategy=path.strategy,
                wave_ids=path.wave_ids,
                allocations=path.allocations,
                tasks=path.tasks,
                backordered_lines=path.backordered_lines,
                status=status,
            )
            await emit_event(
                conn,
                tenant_id=ctx.tenant_id,
                aggregate_type="order",
                aggregate_id=order_id,
                event_type="order.routed",
                payload={
                    "external_order_id": payload.external_order_id,
                    "mode": payload.mode.value,
                    "strategy": path.strategy,
                    "wave_ids": [str(item) for item in path.wave_ids],
                    "task_ids": [str(item.task_id) for item in path.tasks],
                    "backordered_lines": [str(item) for item in path.backordered_lines],
                    "status": status,
                },
                headers={"correlation_id": str(correlation_id)},
            )
            await self.guard.complete(
                conn,
                ctx.tenant_id,
                "orders.route",
                idempotency_key,
                response,
            )
        await self.dispatcher.publish(pushes)
        return response

    async def _route_b2c(
        self,
        conn: Any,
        *,
        ctx: TenantContext,
        order_id: UUID,
        workflow: dict[str, Any],
        workflow_version: int,
        correlation_id: UUID,
        pushes: list[TaskPush],
    ) -> PathResult:
        header = await order_repo.get_order_header(conn, ctx.tenant_id, order_id)
        warehouse_id = UUID(str(header["warehouse_id"]))
        strategy = str(workflow["picking"]["strategy"]).upper()
        cfg: AffinityConfig = affinity_config_from_workflow(workflow, strategy)
        dock, dock_row = await catalog.get_dock(conn, warehouse_id)
        rows = await order_repo.get_routable_lines(
            conn, ctx.tenant_id, order_id, ["B2C"]
        )
        if not rows:
            return PathResult(strategy=strategy)

        lines = [
            order_repo.to_line_requirement(row, cfg.allowed_location_types)
            for row in rows
        ]
        line_plans: list[tuple[LineRequirement, RoutePlan]] = []
        for line in lines:
            candidate_rows = await catalog.get_b2c_candidate_rows(
                conn,
                ctx.tenant_id,
                warehouse_id,
                line,
                list(cfg.allowed_location_types),
            )
            candidates = [CandidateStock.from_row(row) for row in candidate_rows]
            line_plans.append((line, plan_line_stops(line, candidates, dock, cfg)))

        order_stops = [stop for _, route in line_plans for stop in route.stops]
        order_route = build_route(order_stops, dock, cfg)
        now = utcnow()
        sla = _aware(header["sla_ship_at"])
        meta = OrderWaveMeta(
            order_id=order_id,
            warehouse_id=warehouse_id,
            strategy=strategy,
            now=now,
            priority=int(header["priority"]),
            line_count=len(lines),
            unit_count=sum((line.required_qty for line in lines), Decimal("0")),
            sla_ship_at=sla,
            carrier_code=header["carrier_code"],
            cutoff_bucket=compute_cutoff_bucket(sla, cfg.cutoff_bucket_minutes),
        )
        all_candidates = await wave_repo.load_open_waves(
            conn,
            tenant_id=ctx.tenant_id,
            warehouse_id=warehouse_id,
            strategy=strategy,
            cfg=cfg,
            now=now,
            dock=dock,
        )
        wave_plans = await self._build_wave_plans(
            conn,
            ctx=ctx,
            order_id=order_id,
            warehouse_id=warehouse_id,
            strategy=strategy,
            cfg=cfg,
            dock=dock,
            order_route=order_route,
            line_plans=line_plans,
            meta=meta,
            all_candidates=all_candidates,
            workflow=workflow,
            workflow_version=workflow_version,
        )

        allocation_wave_id = None if strategy == "ZONE" else wave_plans[0].wave_id
        all_allocations: list[AllocationResult] = []
        result = PathResult(
            wave_ids=[plan.wave_id for plan in wave_plans],
            strategy=strategy,
        )
        line_by_id = {line.order_line_id: line for line in lines}
        for line, route in line_plans:
            preferred = list(dict.fromkeys(stop.location_id for stop in route.stops))
            allocated = await self.allocations.allocate_line(
                conn,
                tenant_id=ctx.tenant_id,
                warehouse_id=warehouse_id,
                line=line,
                dock=dock,
                cfg=cfg,
                wave_id=allocation_wave_id,
                prefer_location_ids=preferred,
                exclude_location_ids=[],
                correlation_id=correlation_id,
            )
            all_allocations.extend(allocated)
            for item in allocated:
                result.allocations.append(
                    AllocationCreated(
                        order_line_id=item.order_line_id,
                        allocation_id=item.allocation_id,
                        bucket_id=item.bucket_id,
                        location_id=item.location_id,
                        lot_id=item.lot_id,
                        qty=item.qty,
                    )
                )

        zone_to_wave = {
            plan.zone: plan.wave_id for plan in wave_plans if plan.zone is not None
        }
        default_wave_id = wave_plans[0].wave_id
        groups: dict[tuple[UUID, str], list[AllocationResult]] = {}
        for allocation in all_allocations:
            wave_id = (
                allocation.wave_id
                or zone_to_wave.get(allocation.zone)
                or default_wave_id
            )
            groups.setdefault((wave_id, allocation.zone), []).append(allocation)

        task_type = _task_type_for_strategy(strategy)
        required_equipment = list(workflow["b2c"]["required_equipment"])
        max_lines_per_task = int(workflow["b2c"]["max_lines_per_task"])
        priority = pick_priority(int(header["priority"]), sla, now, strategy)
        release_at = wave_plans[0].release_at
        for (wave_id, zone), items in sorted(
            groups.items(), key=lambda entry: (str(entry[0][0]), entry[0][1])
        ):
            for chunk_index, chunk in enumerate(
                _chunks(items, max_lines_per_task), start=1
            ):
                source_ref_id = uuid5(
                    NAMESPACE_URL,
                    f"wms:pick-task:{ctx.tenant_id}:{wave_id}:{zone}:{order_id}:{chunk_index}",
                )
                stops = [
                    {
                        "location_id": str(item.location_id),
                        "sku_id": str(line_by_id[item.order_line_id].sku_id),
                        "lot_id": str(item.lot_id) if item.lot_id else None,
                        "qty": item.qty,
                        "zone": item.zone,
                        "x_m": item.coordinate.x,
                        "y_m": item.coordinate.y,
                        "z_m": item.coordinate.z,
                    }
                    for item in chunk
                ]
                payload = {
                    "strategy": strategy,
                    "wave_ids": [str(plan.wave_id) for plan in wave_plans],
                    "order_id": str(order_id),
                    "zone": zone,
                    "stops": stops,
                    "allocations": [str(item.allocation_id) for item in chunk],
                    "dock": {
                        "x": dock.x,
                        "y": dock.y,
                        "z": dock.z,
                        "zone": dock.zone or str(dock_row["zone"]),
                    },
                    "correlation_id": str(correlation_id),
                }
                task_id = await self.dispatcher.enqueue_task(
                    conn,
                    TaskSpec(
                        tenant_id=ctx.tenant_id,
                        task_type=task_type,
                        priority=priority,
                        warehouse_id=warehouse_id,
                        required_equipment=required_equipment,
                        required_zone=zone,
                        location_id=chunk[0].location_id,
                        source_ref_type="wave_zone",
                        source_ref_id=source_ref_id,
                        wave_id=wave_id,
                        order_id=order_id,
                        payload=payload,
                        not_before=now
                        if workflow["b2c"]["release_immediately"]
                        else release_at,
                    ),
                )
                if task_id is not None:
                    result.tasks.append(
                        TaskCreated(
                            task_id=task_id,
                            task_type=task_type,
                            priority=priority,
                            required_zone=zone,
                            required_equipment=required_equipment,
                        )
                    )
        await emit_event(
            conn,
            tenant_id=ctx.tenant_id,
            aggregate_type="order",
            aggregate_id=order_id,
            event_type="wave.released",
            payload={
                "strategy": strategy,
                "wave_ids": [str(plan.wave_id) for plan in wave_plans],
                "task_ids": [str(item.task_id) for item in result.tasks],
            },
            headers={"correlation_id": str(correlation_id)},
        )
        pushes.extend(
            await self.dispatcher.assign_queued_tasks(
                conn,
                ctx.tenant_id,
                warehouse_id=warehouse_id,
                limit=max(1, len(result.tasks)),
            )
        )
        return result

    async def _build_wave_plans(
        self,
        conn: Any,
        *,
        ctx: TenantContext,
        order_id: UUID,
        warehouse_id: UUID,
        strategy: str,
        cfg: AffinityConfig,
        dock: Any,
        order_route: RoutePlan,
        line_plans: Sequence[tuple[LineRequirement, RoutePlan]],
        meta: OrderWaveMeta,
        all_candidates: Sequence[Any],
        workflow: dict[str, Any],
        workflow_version: int,
    ) -> list[WavePlan]:
        plans: list[WavePlan] = []
        if strategy != "ZONE":
            decision = choose_wave(order_route, list(all_candidates), meta, cfg)
            if decision.create_new or decision.wave_id is None:
                wave_id = await wave_repo.create_wave(
                    conn,
                    tenant_id=ctx.tenant_id,
                    warehouse_id=warehouse_id,
                    strategy=strategy,
                    meta=meta,
                    cfg=cfg,
                    route=order_route,
                    decision=decision,
                    workflow=workflow,
                    workflow_version=workflow_version,
                )
                existing_route = build_route([], dock, cfg)
                release_at = meta.now
            else:
                wave_id = decision.wave_id
                existing_route = await wave_repo.get_wave_route(
                    conn,
                    tenant_id=ctx.tenant_id,
                    wave_id=wave_id,
                    dock=dock,
                    cfg=cfg,
                )
                candidate = next(
                    (item for item in all_candidates if item.wave_id == wave_id),
                    None,
                )
                release_at = candidate.release_at if candidate else meta.now
            merged = merge_routes(existing_route, order_route, cfg)
            line_ids = tuple(line.order_line_id for line, _ in line_plans)
            await wave_repo.attach_order_to_wave(
                conn,
                tenant_id=ctx.tenant_id,
                wave_id=wave_id,
                order_id=order_id,
                line_ids=line_ids,
                merged_route=merged,
                meta=meta,
                decision=decision,
            )
            plans.append(
                WavePlan(
                    wave_id=wave_id,
                    zone=None,
                    release_at=release_at,
                    line_ids=line_ids,
                    merged_route=merged,
                    decision=decision,
                    meta=meta,
                )
            )
            return plans

        for zone in sorted(order_route.zone_signature()):
            zone_stops = [stop for stop in order_route.stops if stop.zone == zone]
            zone_route = build_route(zone_stops, dock, cfg)
            zone_line_ids = tuple(
                line.order_line_id
                for line, route in line_plans
                if any(stop.zone == zone for stop in route.stops)
            )
            zone_meta = replace(
                meta,
                line_count=len(zone_line_ids),
                unit_count=zone_route.total_qty(),
            )
            decision = choose_wave(zone_route, list(all_candidates), zone_meta, cfg)
            if decision.create_new or decision.wave_id is None:
                wave_id = await wave_repo.create_wave(
                    conn,
                    tenant_id=ctx.tenant_id,
                    warehouse_id=warehouse_id,
                    strategy=strategy,
                    meta=zone_meta,
                    cfg=cfg,
                    route=zone_route,
                    decision=decision,
                    workflow=workflow,
                    workflow_version=workflow_version,
                )
                existing_route = build_route([], dock, cfg)
                release_at = meta.now
            else:
                wave_id = decision.wave_id
                existing_route = await wave_repo.get_wave_route(
                    conn,
                    tenant_id=ctx.tenant_id,
                    wave_id=wave_id,
                    dock=dock,
                    cfg=cfg,
                )
                candidate = next(
                    (item for item in all_candidates if item.wave_id == wave_id),
                    None,
                )
                release_at = candidate.release_at if candidate else meta.now
            merged = merge_routes(existing_route, zone_route, cfg)
            await wave_repo.attach_order_to_wave(
                conn,
                tenant_id=ctx.tenant_id,
                wave_id=wave_id,
                order_id=order_id,
                line_ids=zone_line_ids,
                merged_route=merged,
                meta=zone_meta,
                decision=decision,
            )
            plans.append(
                WavePlan(
                    wave_id=wave_id,
                    zone=zone,
                    release_at=release_at,
                    line_ids=zone_line_ids,
                    merged_route=merged,
                    decision=decision,
                    meta=zone_meta,
                )
            )
        if not plans:
            raise ValidationError("ZONE strategy produced no zone waves")
        return plans

    async def _route_b2b(
        self,
        conn: Any,
        *,
        ctx: TenantContext,
        order_id: UUID,
        workflow: dict[str, Any],
        correlation_id: UUID,
        pushes: list[TaskPush],
    ) -> PathResult:
        header = await order_repo.get_order_header(conn, ctx.tenant_id, order_id)
        warehouse_id = UUID(str(header["warehouse_id"]))
        dock, dock_row = await catalog.get_dock(conn, warehouse_id)
        rows = await order_repo.get_routable_lines(
            conn, ctx.tenant_id, order_id, ["B2B"]
        )
        result = PathResult(strategy="B2B_DIRECTED_PALLET")
        if not rows:
            return result
        b2b = workflow["b2b"]
        allowed_types = list(b2b["allowed_pallet_location_types"])
        lines = [order_repo.to_line_requirement(row, allowed_types) for row in rows]
        now = utcnow()
        priority = pick_priority(
            int(header["priority"]), header["sla_ship_at"], now, "B2B"
        )
        reservations: list[PalletReservation] = []
        for line in lines:
            line_reservations, missing_pallets = await self.b2b.reserve_full_pallets(
                conn,
                tenant_id=ctx.tenant_id,
                warehouse_id=warehouse_id,
                line=line,
                dock=dock,
                workflow=workflow,
                correlation_id=correlation_id,
            )
            reservations.extend(line_reservations)
            for item in line_reservations:
                result.allocations.append(
                    AllocationCreated(
                        order_line_id=item.order_line_id,
                        allocation_id=item.allocation_id,
                        bucket_id=item.bucket_id,
                        location_id=item.location_id,
                        lot_id=item.lot_id,
                        qty=item.qty,
                    )
                )
            if missing_pallets > 0:
                result.backordered_lines.append(line.order_line_id)
                await order_repo.mark_line_backordered(
                    conn, ctx.tenant_id, line.order_line_id
                )
                source_ref_id = uuid5(
                    NAMESPACE_URL,
                    f"wms:replenishment:{ctx.tenant_id}:{order_id}:{line.order_line_id}",
                )
                replenishment_priority = int(
                    workflow["exceptions"]["replenishment_priority"]
                )
                task_id = await self.dispatcher.enqueue_task(
                    conn,
                    TaskSpec(
                        tenant_id=ctx.tenant_id,
                        task_type="REPLENISHMENT",
                        priority=max(priority, replenishment_priority),
                        warehouse_id=warehouse_id,
                        required_equipment=list(b2b["required_equipment"]),
                        required_zone=None,
                        location_id=None,
                        source_ref_type="order_line_shortage",
                        source_ref_id=source_ref_id,
                        wave_id=None,
                        order_id=order_id,
                        payload={
                            "order_id": str(order_id),
                            "order_line_id": str(line.order_line_id),
                            "sku_id": str(line.sku_id),
                            "lot_id": str(line.lot_id) if line.lot_id else None,
                            "missing_pallets": missing_pallets,
                            "units_per_pallet": line.units_per_pallet,
                            "correlation_id": str(correlation_id),
                        },
                        not_before=now,
                    ),
                )
                if task_id is not None:
                    result.tasks.append(
                        TaskCreated(
                            task_id=task_id,
                            task_type="REPLENISHMENT",
                            priority=max(priority, replenishment_priority),
                            required_zone=None,
                            required_equipment=list(b2b["required_equipment"]),
                        )
                    )
                await emit_event(
                    conn,
                    tenant_id=ctx.tenant_id,
                    aggregate_type="order_line",
                    aggregate_id=line.order_line_id,
                    event_type="pallet.backordered",
                    payload={
                        "order_id": str(order_id),
                        "sku_id": str(line.sku_id),
                        "missing_pallets": missing_pallets,
                    },
                    headers={"correlation_id": str(correlation_id)},
                )

        groups: dict[str, list[PalletReservation]] = {}
        for item in reservations:
            groups.setdefault(item.zone, []).append(item)
        max_pallets = int(b2b["max_pallets_per_task"])
        for zone, items in sorted(groups.items()):
            for chunk_index, chunk in enumerate(_chunks(items, max_pallets), start=1):
                source_ref_id = uuid5(
                    NAMESPACE_URL,
                    f"wms:pallet-pick:{ctx.tenant_id}:{order_id}:{zone}:{chunk_index}",
                )
                payload = {
                    "order_id": str(order_id),
                    "zone": zone,
                    "handling_units": [item.payload_item() for item in chunk],
                    "dock": {
                        "x": dock.x,
                        "y": dock.y,
                        "z": dock.z,
                        "zone": dock.zone or str(dock_row["zone"]),
                    },
                    "correlation_id": str(correlation_id),
                }
                task_id = await self.dispatcher.enqueue_task(
                    conn,
                    TaskSpec(
                        tenant_id=ctx.tenant_id,
                        task_type="PALLET_PICK",
                        priority=priority,
                        warehouse_id=warehouse_id,
                        required_equipment=list(b2b["required_equipment"]),
                        required_zone=zone,
                        location_id=chunk[0].location_id,
                        source_ref_type="pallet_pick_group",
                        source_ref_id=source_ref_id,
                        wave_id=None,
                        order_id=order_id,
                        payload=payload,
                        not_before=now,
                    ),
                )
                if task_id is not None:
                    result.tasks.append(
                        TaskCreated(
                            task_id=task_id,
                            task_type="PALLET_PICK",
                            priority=priority,
                            required_zone=zone,
                            required_equipment=list(b2b["required_equipment"]),
                        )
                    )
        await emit_event(
            conn,
            tenant_id=ctx.tenant_id,
            aggregate_type="order",
            aggregate_id=order_id,
            event_type="pallet.pick.tasks_created",
            payload={
                "task_ids": [str(item.task_id) for item in result.tasks],
                "reserved_pallets": len(reservations),
            },
            headers={"correlation_id": str(correlation_id)},
        )
        pushes.extend(
            await self.dispatcher.assign_queued_tasks(
                conn,
                ctx.tenant_id,
                warehouse_id=warehouse_id,
                limit=max(1, len(result.tasks)),
            )
        )
        return result

    async def plan_directed_putaway(
        self,
        ctx: TenantContext,
        payload: DirectedPutawayIn,
        idempotency_key: str,
    ) -> DirectedPutawayResponse:
        pushes: list[TaskPush] = []
        async with self.db.tenant_transaction(ctx.tenant_id) as conn:
            cached = await self.guard.begin(
                conn,
                ctx.tenant_id,
                "inbound.putaway",
                idempotency_key,
                {"client_id": ctx.client_id, "putaway": payload},
            )
            if cached is not None:
                return DirectedPutawayResponse(**cached)
            await catalog.validate_api_client(
                conn,
                ctx,
                required_scope="inbound:write",
                mode=OrderMode.B2B,
            )
            workflow, _ = await catalog.load_workflow(
                conn, ctx.tenant_id, self.settings
            )
            response, pushes = await self.b2b.plan_directed_putaway(
                conn,
                tenant_id=ctx.tenant_id,
                payload=payload,
                workflow=workflow,
                correlation_id=uuid4(),
                dispatcher=self.dispatcher,
            )
            await self.guard.complete(
                conn,
                ctx.tenant_id,
                "inbound.putaway",
                idempotency_key,
                response,
            )
        await self.dispatcher.publish(pushes)
        return response

    async def complete_putaway(
        self,
        ctx: TenantContext,
        task_id: UUID,
        payload: PutawayCompleteIn,
        idempotency_key: str,
    ) -> PutawayCompleteResponse:
        async with self.db.tenant_transaction(ctx.tenant_id) as conn:
            cached = await self.guard.begin(
                conn,
                ctx.tenant_id,
                "tasks.putaway_complete",
                idempotency_key,
                {"client_id": ctx.client_id, "task_id": task_id, "completion": payload},
            )
            if cached is not None:
                return PutawayCompleteResponse(**cached)
            await catalog.validate_api_client(
                conn,
                ctx,
                required_scope="tasks:write",
                mode=OrderMode.B2B,
            )
            response = await self.b2b.complete_pallet_move(
                conn,
                tenant_id=ctx.tenant_id,
                task_id=task_id,
                payload=payload,
                correlation_id=uuid4(),
            )
            await self.guard.complete(
                conn,
                ctx.tenant_id,
                "tasks.putaway_complete",
                idempotency_key,
                response,
            )
        return response
