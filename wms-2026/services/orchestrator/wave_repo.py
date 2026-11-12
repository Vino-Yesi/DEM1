from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Sequence
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb

from . import sql
from .affinity import (
    AffinityConfig,
    Coordinate,
    OrderWaveMeta,
    PlannedStop,
    RoutePlan,
    WaveCandidate,
    WaveDecision,
    build_route,
)
from .db import fetch_all, fetch_one
from .errors import NotFoundError
from .utils import to_decimal


def _uuid(value: Any) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _optional_uuid(value: Any) -> UUID | None:
    return None if value is None else _uuid(value)


def _dec(value: Any, default: Any = 0) -> Any:
    return to_decimal(default) if value is None else to_decimal(value)


def _dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _stop_bucket_id(
    wave_id: UUID, location_id: UUID, sku_id: UUID, lot_id: UUID | None
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"wms-wave-stop:{wave_id}:{location_id}:{sku_id}:{lot_id or 'NO_LOT'}",
    )


def _stops_payload(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, str):
        return list(json.loads(value))
    return list(value)


def _planned_stop_from_wave_row(row: dict[str, Any]) -> PlannedStop:
    location_id = _uuid(row["location_id"])
    sku_id = _uuid(row["sku_id"])
    lot_id = _optional_uuid(row.get("lot_id"))
    wave_id = _uuid(row["wave_id"])
    coordinate = Coordinate(
        x=_dec(row["x_m"]),
        y=_dec(row["y_m"]),
        z=_dec(row["z_m"]),
        zone=str(row["zone"]),
    )
    return PlannedStop(
        bucket_id=_stop_bucket_id(wave_id, location_id, sku_id, lot_id),
        location_id=location_id,
        sku_id=sku_id,
        lot_id=lot_id,
        qty=_dec(row["qty"]),
        coordinate=coordinate,
        zone=str(row["zone"]),
        travel_seq=int(row["stop_seq"]),
        location_type="",
        expiration_date=None,
        stop_seq=int(row["stop_seq"]),
    )


def _planned_stop_from_json(wave_id: UUID, item: dict[str, Any]) -> PlannedStop:
    location_id = _uuid(item["location_id"])
    sku_id = _uuid(item["sku_id"])
    lot_id = _optional_uuid(item.get("lot_id"))
    coordinate = Coordinate(
        x=_dec(item["x_m"]),
        y=_dec(item["y_m"]),
        z=_dec(item["z_m"]),
        zone=str(item["zone"]),
    )
    return PlannedStop(
        bucket_id=_stop_bucket_id(wave_id, location_id, sku_id, lot_id),
        location_id=location_id,
        sku_id=sku_id,
        lot_id=lot_id,
        qty=_dec(item["qty"]),
        coordinate=coordinate,
        zone=str(item["zone"]),
        travel_seq=int(item["stop_seq"]),
        location_type="",
        expiration_date=None,
        stop_seq=int(item["stop_seq"]),
    )


async def load_open_waves(
    conn: Any,
    *,
    tenant_id: UUID,
    warehouse_id: UUID,
    strategy: str,
    cfg: AffinityConfig,
    now: datetime,
    dock: Coordinate,
) -> list[WaveCandidate]:
    rows = await fetch_all(
        conn,
        sql.LOAD_OPEN_WAVES,
        {
            "tenant_id": tenant_id,
            "warehouse_id": warehouse_id,
            "strategy": strategy,
            "now": now,
            "window_minutes": cfg.release_window_minutes,
        },
    )
    candidates: list[WaveCandidate] = []
    for row in rows:
        wave_id = _uuid(row["id"])
        stops = [
            _planned_stop_from_json(wave_id, item)
            for item in _stops_payload(row.get("stops"))
        ]
        route = build_route(stops, dock, cfg)
        release_at = _dt(row["release_at"]) or now
        candidates.append(
            WaveCandidate(
                wave_id=wave_id,
                tenant_id=_uuid(row["tenant_id"]),
                warehouse_id=_uuid(row["warehouse_id"]),
                strategy=str(row["strategy"]),
                status=str(row["status"]),
                priority=int(row["priority"]),
                carrier_code=None
                if row.get("carrier_code") is None
                else str(row["carrier_code"]),
                cutoff_bucket=_dt(row.get("cutoff_bucket")),
                cutoff_at=_dt(row.get("cutoff_at")),
                zone=None if row.get("zone") is None else str(row["zone"]),
                release_at=release_at,
                max_orders=int(row["max_orders"]),
                max_lines=int(row["max_lines"]),
                max_units=_dec(row["max_units"]),
                order_count=int(row["order_count"]),
                line_count=int(row["line_count"]),
                unit_count=_dec(row["unit_count"]),
                route=route,
            )
        )
    return candidates


async def create_wave(
    conn: Any,
    *,
    tenant_id: UUID,
    warehouse_id: UUID,
    strategy: str,
    meta: OrderWaveMeta,
    cfg: AffinityConfig,
    route: RoutePlan,
    decision: WaveDecision,
    workflow: dict[str, Any],
    workflow_version: int,
) -> UUID:
    picking = workflow["picking"]
    wave_code = f"W-{warehouse_id.hex[:8]}-{meta.now.strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:10]}"
    zones = route.zone_signature()
    zone = next(iter(zones)) if strategy == "ZONE" and len(zones) == 1 else None
    metadata = {
        "workflow_version": workflow_version,
        "affinity": decision.breakdown.as_dict() if decision.breakdown else None,
        "created_from": "hybrid_order_router",
    }
    row = await fetch_one(
        conn,
        sql.INSERT_WAVE,
        {
            "tenant_id": tenant_id,
            "warehouse_id": warehouse_id,
            "wave_code": wave_code,
            "strategy": strategy,
            "carrier_code": meta.carrier_code,
            "cutoff_bucket": meta.cutoff_bucket,
            "zone": zone,
            "affinity_score": decision.breakdown.score if decision.breakdown else 0,
            "estimated_travel_m": route.route_m,
            "priority": meta.priority,
            "release_at": meta.now,
            "cutoff_at": meta.sla_ship_at,
            "max_orders": int(picking["max_orders_per_wave"]),
            "max_lines": int(picking["max_lines_per_wave"]),
            "max_units": to_decimal(picking["max_units_per_wave"]),
            "metadata": Jsonb(jsonable_encoder(metadata)),
        },
    )
    if row is None:
        raise NotFoundError("Wave insert", wave_code)
    return _uuid(row["id"])


async def attach_order_to_wave(
    conn: Any,
    *,
    tenant_id: UUID,
    wave_id: UUID,
    order_id: UUID,
    line_ids: Sequence[UUID],
    merged_route: RoutePlan,
    meta: OrderWaveMeta,
    decision: WaveDecision,
) -> None:
    wave = await fetch_one(
        conn,
        sql.LOCK_WAVE,
        {"tenant_id": tenant_id, "wave_id": wave_id},
    )
    if wave is None:
        raise NotFoundError("Wave", wave_id, {"tenant_id": str(tenant_id)})

    order_inserted = await fetch_one(
        conn,
        sql.INSERT_WAVE_ORDER,
        {
            "tenant_id": tenant_id,
            "wave_id": wave_id,
            "order_id": order_id,
            "sequence": int(wave["order_count"]) + 1,
        },
    )
    if order_inserted is None:
        return

    inserted_lines = 0
    for index, line_id in enumerate(line_ids, start=1):
        result = await fetch_one(
            conn,
            sql.INSERT_WAVE_ORDER_LINE,
            {
                "tenant_id": tenant_id,
                "wave_id": wave_id,
                "order_line_id": line_id,
                "sequence": index,
            },
        )
        if result is not None:
            inserted_lines += 1

    await conn.execute(
        "DELETE FROM wave_route_stops WHERE tenant_id = %(tenant_id)s AND wave_id = %(wave_id)s",
        {"tenant_id": tenant_id, "wave_id": wave_id},
    )
    for stop in merged_route.stops:
        await conn.execute(
            sql.UPSERT_WAVE_ROUTE_STOP,
            {
                "tenant_id": tenant_id,
                "wave_id": wave_id,
                "warehouse_id": meta.warehouse_id,
                "location_id": stop.location_id,
                "sku_id": stop.sku_id,
                "lot_id": stop.lot_id,
                "qty": stop.qty,
                "stop_seq": stop.stop_seq,
                "x_m": stop.coordinate.x,
                "y_m": stop.coordinate.y,
                "z_m": stop.coordinate.z,
                "zone": stop.zone,
            },
        )
    await conn.execute(
        sql.UPDATE_WAVE_METRICS,
        {
            "tenant_id": tenant_id,
            "wave_id": wave_id,
            "order_delta": 1,
            "line_delta": inserted_lines,
            "unit_delta": meta.unit_count,
            "estimated_travel_m": merged_route.route_m,
            "affinity_score": decision.breakdown.score if decision.breakdown else 0,
        },
    )


async def get_wave_route(
    conn: Any,
    *,
    tenant_id: UUID,
    wave_id: UUID,
    dock: Coordinate,
    cfg: AffinityConfig,
) -> RoutePlan:
    rows = await fetch_all(
        conn,
        """
        SELECT * FROM wave_route_stops
        WHERE tenant_id = %(tenant_id)s AND wave_id = %(wave_id)s
        ORDER BY stop_seq
        """,
        {"tenant_id": tenant_id, "wave_id": wave_id},
    )
    return build_route([_planned_stop_from_wave_row(row) for row in rows], dock, cfg)


async def rebuild_wave_route(
    conn: Any,
    *,
    tenant_id: UUID,
    wave_id: UUID,
    warehouse_id: UUID,
    dock: Coordinate,
    cfg: AffinityConfig,
    new_stops: Sequence[PlannedStop],
    exclude_location_id: UUID | None,
) -> RoutePlan:
    wave = await fetch_one(
        conn,
        sql.LOCK_WAVE,
        {"tenant_id": tenant_id, "wave_id": wave_id},
    )
    if wave is None:
        raise NotFoundError("Wave", wave_id, {"tenant_id": str(tenant_id)})
    rows = await fetch_all(
        conn,
        """
        SELECT * FROM wave_route_stops
        WHERE tenant_id = %(tenant_id)s
          AND wave_id = %(wave_id)s
          AND (%(exclude_location_id)s::uuid IS NULL
               OR location_id <> %(exclude_location_id)s::uuid)
        ORDER BY stop_seq
        """,
        {
            "tenant_id": tenant_id,
            "wave_id": wave_id,
            "exclude_location_id": exclude_location_id,
        },
    )
    existing = [_planned_stop_from_wave_row(row) for row in rows]
    merged = build_route([*existing, *new_stops], dock, cfg)
    await conn.execute(
        "DELETE FROM wave_route_stops WHERE tenant_id = %(tenant_id)s AND wave_id = %(wave_id)s",
        {"tenant_id": tenant_id, "wave_id": wave_id},
    )
    for stop in merged.stops:
        await conn.execute(
            sql.UPSERT_WAVE_ROUTE_STOP,
            {
                "tenant_id": tenant_id,
                "wave_id": wave_id,
                "warehouse_id": warehouse_id,
                "location_id": stop.location_id,
                "sku_id": stop.sku_id,
                "lot_id": stop.lot_id,
                "qty": stop.qty,
                "stop_seq": stop.stop_seq,
                "x_m": stop.coordinate.x,
                "y_m": stop.coordinate.y,
                "z_m": stop.coordinate.z,
                "zone": stop.zone,
            },
        )
    await conn.execute(
        """
        UPDATE waves
           SET estimated_travel_m = %(estimated_travel_m)s,
               version = version + 1,
               updated_at = now()
         WHERE tenant_id = %(tenant_id)s AND id = %(wave_id)s
        """,
        {
            "tenant_id": tenant_id,
            "wave_id": wave_id,
            "estimated_travel_m": merged.route_m,
        },
    )
    return merged


async def get_wave_line_ids(conn: Any, tenant_id: UUID, wave_id: UUID) -> list[UUID]:
    rows = await fetch_all(
        conn,
        sql.GET_WAVE_LINE_IDS,
        {"tenant_id": tenant_id, "wave_id": wave_id},
    )
    return [_uuid(row["order_line_id"]) for row in rows]
