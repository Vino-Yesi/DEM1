from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID

from .errors import InsufficientStockError, ValidationError
from .utils import clamp, to_decimal

ZERO = Decimal("0")
ONE = Decimal("1")


def _uuid(value: Any) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


def _optional_uuid(value: Any) -> UUID | None:
    return None if value is None else _uuid(value)


def _dec(value: Any, default: Decimal = ZERO) -> Decimal:
    return default if value is None else to_decimal(value)


def _int(value: Any, default: int = 0) -> int:
    return default if value is None else int(value)


def _bool(value: Any, default: bool = False) -> bool:
    return default if value is None else bool(value)


def _date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _str(value: Any, default: str = "") -> str:
    return default if value is None else str(value)


@dataclass(frozen=True)
class Coordinate:
    x: Decimal
    y: Decimal
    z: Decimal
    zone: str | None = None


@dataclass(frozen=True)
class AffinityConfig:
    strategy: str
    weights: Mapping[str, Decimal]
    threshold: Decimal
    max_cluster_travel_m: Decimal
    max_zone_count: int
    zone_penalty_m: Decimal
    vertical_penalty: Decimal
    prefer_pick_face: bool
    release_window_minutes: int
    cutoff_bucket_minutes: int
    allowed_location_types: tuple[str, ...]


@dataclass(frozen=True)
class CandidateStock:
    bucket_id: UUID
    sku_id: UUID
    lot_id: UUID | None
    serial_number: str | None
    expiration_date: date | None
    handling_unit_id: UUID | None
    warehouse_id: UUID
    location_id: UUID
    available_qty: Decimal
    balance_version: int
    header_version: int
    location_barcode: str
    zone: str
    aisle: str
    bay: str
    level: str
    position: str
    x_m: Decimal
    y_m: Decimal
    z_m: Decimal
    travel_seq: int
    location_type: str
    pick_face: bool
    allows_hazmat: bool
    location_temperature_band: str
    sku_code: str
    hazardous: bool
    sku_temperature_band: str
    requires_lot: bool
    requires_serial: bool
    velocity_score: Decimal
    unit_volume_cm3: Decimal
    unit_weight_kg: Decimal

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> CandidateStock:
        return cls(
            bucket_id=_uuid(row["bucket_id"]),
            sku_id=_uuid(row["sku_id"]),
            lot_id=_optional_uuid(row.get("lot_id")),
            serial_number=None
            if row.get("serial_number") is None
            else str(row["serial_number"]),
            expiration_date=_date(row.get("expiration_date")),
            handling_unit_id=_optional_uuid(row.get("handling_unit_id")),
            warehouse_id=_uuid(row["warehouse_id"]),
            location_id=_uuid(row["location_id"]),
            available_qty=_dec(row["available_qty"]),
            balance_version=_int(row.get("balance_version")),
            header_version=_int(row.get("header_version")),
            location_barcode=_str(row.get("location_barcode")),
            zone=_str(row.get("zone")),
            aisle=_str(row.get("aisle")),
            bay=_str(row.get("bay")),
            level=_str(row.get("level")),
            position=_str(row.get("position")),
            x_m=_dec(row.get("x_m")),
            y_m=_dec(row.get("y_m")),
            z_m=_dec(row.get("z_m")),
            travel_seq=_int(row.get("travel_seq")),
            location_type=_str(row.get("location_type"), "piece_pick"),
            pick_face=_bool(row.get("pick_face")),
            allows_hazmat=_bool(row.get("allows_hazmat")),
            location_temperature_band=_str(row.get("location_temperature_band"), "ANY"),
            sku_code=_str(row.get("sku_code")),
            hazardous=_bool(row.get("hazardous")),
            sku_temperature_band=_str(row.get("sku_temperature_band"), "AMBIENT"),
            requires_lot=_bool(row.get("requires_lot")),
            requires_serial=_bool(row.get("requires_serial")),
            velocity_score=_dec(row.get("velocity_score")),
            unit_volume_cm3=_dec(row.get("unit_volume_cm3")),
            unit_weight_kg=_dec(row.get("unit_weight_kg")),
        )

    @property
    def coordinate(self) -> Coordinate:
        return Coordinate(self.x_m, self.y_m, self.z_m, self.zone)


@dataclass(frozen=True)
class LineRequirement:
    order_line_id: UUID
    order_id: UUID
    sku_id: UUID
    lot_id: UUID | None
    required_qty: Decimal
    picked_qty: Decimal
    allocated_qty: Decimal
    require_same_lot: bool
    expiry_min: date | None
    expiry_max: date | None
    hazardous: bool
    temperature_band: str
    requires_lot: bool
    requires_serial: bool
    mode: str
    fulfillment_unit: str
    allowed_location_types: tuple[str, ...]
    version: int
    units_per_pallet: Decimal

    @property
    def remaining_qty(self) -> Decimal:
        return max(ZERO, self.required_qty - self.picked_qty - self.allocated_qty)


@dataclass(frozen=True)
class PlannedStop:
    bucket_id: UUID
    location_id: UUID
    sku_id: UUID
    lot_id: UUID | None
    qty: Decimal
    coordinate: Coordinate
    zone: str
    travel_seq: int
    location_type: str
    expiration_date: date | None
    stop_seq: int = 0

    @property
    def stop_key(self) -> tuple[UUID, UUID, UUID | None]:
        return (self.location_id, self.sku_id, self.lot_id)


@dataclass(frozen=True)
class RoutePlan:
    dock: Coordinate
    stops: tuple[PlannedStop, ...]
    route_m: Decimal

    def total_qty(self) -> Decimal:
        return sum((stop.qty for stop in self.stops), ZERO)

    def location_signature(self) -> frozenset[tuple[UUID, UUID, UUID | None]]:
        return frozenset(
            (stop.location_id, stop.sku_id, stop.lot_id) for stop in self.stops
        )

    def sku_signature(self) -> frozenset[UUID]:
        return frozenset(stop.sku_id for stop in self.stops)

    def zone_signature(self) -> frozenset[str]:
        return frozenset(stop.zone for stop in self.stops)


@dataclass(frozen=True)
class WaveCandidate:
    wave_id: UUID
    tenant_id: UUID
    warehouse_id: UUID
    strategy: str
    status: str
    priority: int
    carrier_code: str | None
    cutoff_bucket: datetime | None
    cutoff_at: datetime | None
    zone: str | None
    release_at: datetime
    max_orders: int
    max_lines: int
    max_units: Decimal
    order_count: int
    line_count: int
    unit_count: Decimal
    route: RoutePlan


@dataclass(frozen=True)
class OrderWaveMeta:
    order_id: UUID
    warehouse_id: UUID
    strategy: str
    now: datetime
    priority: int
    line_count: int
    unit_count: Decimal
    sla_ship_at: datetime | None
    carrier_code: str | None
    cutoff_bucket: datetime | None


@dataclass(frozen=True)
class AffinityBreakdown:
    score: Decimal
    location_jaccard: Decimal
    sku_jaccard: Decimal
    zone_jaccard: Decimal
    travel_score: Decimal
    sla_score: Decimal
    incremental_travel_m: Decimal
    merged_route_m: Decimal
    zone_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "location_jaccard": self.location_jaccard,
            "sku_jaccard": self.sku_jaccard,
            "zone_jaccard": self.zone_jaccard,
            "travel_score": self.travel_score,
            "sla_score": self.sla_score,
            "incremental_travel_m": self.incremental_travel_m,
            "merged_route_m": self.merged_route_m,
            "zone_count": self.zone_count,
        }


@dataclass(frozen=True)
class WaveDecision:
    wave_id: UUID | None
    breakdown: AffinityBreakdown | None
    create_new: bool


def travel_m(source: Coordinate, target: Coordinate, cfg: AffinityConfig) -> Decimal:
    zone_penalty = (
        cfg.zone_penalty_m
        if source.zone is not None
        and target.zone is not None
        and source.zone != target.zone
        else ZERO
    )
    return (
        abs(target.x - source.x)
        + abs(target.y - source.y)
        + cfg.vertical_penalty * abs(target.z - source.z)
        + zone_penalty
    )


def _candidate_sort_key(
    candidate: CandidateStock,
    dock: Coordinate,
    cfg: AffinityConfig,
) -> tuple[Any, ...]:
    pick_face_rank = 0 if not cfg.prefer_pick_face or candidate.pick_face else 1
    expiry = candidate.expiration_date or date.max
    return (
        pick_face_rank,
        expiry,
        travel_m(dock, candidate.coordinate, cfg),
        candidate.travel_seq,
        str(candidate.location_id),
        str(candidate.bucket_id),
    )


def candidate_is_compatible(
    line: LineRequirement,
    candidate: CandidateStock,
    cfg: AffinityConfig,
) -> bool:
    if candidate.sku_id != line.sku_id or candidate.available_qty <= ZERO:
        return False
    if line.lot_id is not None and candidate.lot_id != line.lot_id:
        return False
    if line.requires_lot and candidate.lot_id is None:
        return False
    if line.requires_serial and candidate.serial_number is None:
        return False
    if line.expiry_min is not None and (
        candidate.expiration_date is None or candidate.expiration_date < line.expiry_min
    ):
        return False
    if line.expiry_max is not None and (
        candidate.expiration_date is None or candidate.expiration_date > line.expiry_max
    ):
        return False
    if line.hazardous and not candidate.allows_hazmat:
        return False
    if candidate.location_temperature_band not in ("ANY", line.temperature_band):
        return False
    if (
        line.allowed_location_types
        and candidate.location_type not in line.allowed_location_types
    ):
        return False
    if (
        cfg.allowed_location_types
        and candidate.location_type not in cfg.allowed_location_types
    ):
        return False
    return True


def build_route(
    stops: Iterable[PlannedStop], dock: Coordinate, cfg: AffinityConfig
) -> RoutePlan:
    aggregated: dict[tuple[UUID, UUID, UUID | None], PlannedStop] = {}
    for stop in stops:
        key = stop.stop_key
        if key in aggregated:
            current = aggregated[key]
            aggregated[key] = replace(current, qty=current.qty + stop.qty)
        else:
            aggregated[key] = replace(stop, stop_seq=0)

    unvisited = sorted(
        aggregated.values(),
        key=lambda stop: (
            stop.travel_seq,
            str(stop.location_id),
            str(stop.sku_id),
            str(stop.lot_id or ""),
        ),
    )
    ordered: list[PlannedStop] = []
    current = dock
    total = ZERO
    sequence = 1
    while unvisited:
        next_stop = min(
            unvisited,
            key=lambda stop: (
                travel_m(current, stop.coordinate, cfg),
                stop.travel_seq,
                str(stop.location_id),
                str(stop.sku_id),
                str(stop.lot_id or ""),
            ),
        )
        total += travel_m(current, next_stop.coordinate, cfg)
        ordered.append(replace(next_stop, stop_seq=sequence))
        current = next_stop.coordinate
        unvisited.remove(next_stop)
        sequence += 1
    total += travel_m(current, dock, cfg)
    return RoutePlan(dock=dock, stops=tuple(ordered), route_m=total)


def plan_line_stops(
    line: LineRequirement,
    candidates: Sequence[CandidateStock],
    dock: Coordinate,
    cfg: AffinityConfig,
) -> RoutePlan:
    compatible = [
        candidate
        for candidate in candidates
        if candidate_is_compatible(line, candidate, cfg)
    ]
    compatible.sort(key=lambda candidate: _candidate_sort_key(candidate, dock, cfg))

    remaining = line.remaining_qty
    if remaining <= ZERO:
        return build_route([], dock, cfg)
    planned: list[PlannedStop] = []
    if line.require_same_lot and line.lot_id is None:
        grouped: dict[UUID | None, list[CandidateStock]] = defaultdict(list)
        for candidate in compatible:
            grouped[candidate.lot_id].append(candidate)
        eligible = [
            (lot_id, rows)
            for lot_id, rows in grouped.items()
            if sum((row.available_qty for row in rows), ZERO) >= remaining
        ]
        if not eligible:
            raise InsufficientStockError(
                "no single lot has enough available stock for the order line",
                {
                    "order_line_id": str(line.order_line_id),
                    "sku_id": str(line.sku_id),
                    "required_qty": str(remaining),
                    "available_qty": str(
                        sum((row.available_qty for row in compatible), ZERO)
                    ),
                },
            )
        _, compatible = min(
            eligible,
            key=lambda item: (
                _candidate_sort_key(item[1][0], dock, cfg),
                str(item[0] or ""),
            ),
        )
    elif line.lot_id is not None:
        compatible = [
            candidate for candidate in compatible if candidate.lot_id == line.lot_id
        ]

    available_total = sum((item.available_qty for item in compatible), ZERO)
    for candidate in compatible:
        if remaining <= ZERO:
            break
        take = min(candidate.available_qty, remaining)
        planned.append(
            PlannedStop(
                bucket_id=candidate.bucket_id,
                location_id=candidate.location_id,
                sku_id=candidate.sku_id,
                lot_id=candidate.lot_id,
                qty=take,
                coordinate=candidate.coordinate,
                zone=candidate.zone,
                travel_seq=candidate.travel_seq,
                location_type=candidate.location_type,
                expiration_date=candidate.expiration_date,
            )
        )
        remaining -= take

    if remaining > ZERO:
        raise InsufficientStockError(
            "available stock cannot satisfy order line",
            {
                "order_line_id": str(line.order_line_id),
                "sku_id": str(line.sku_id),
                "required_qty": str(line.required_qty),
                "available_qty": str(available_total),
                "shortage_qty": str(remaining),
            },
        )
    return build_route(planned, dock, cfg)


def merge_routes(left: RoutePlan, right: RoutePlan, cfg: AffinityConfig) -> RoutePlan:
    return build_route([*left.stops, *right.stops], left.dock, cfg)


def jaccard(left: frozenset[Any], right: frozenset[Any]) -> Decimal:
    union = left | right
    if not union:
        return ONE
    return Decimal(len(left & right)) / Decimal(len(union))


def sla_compatibility(
    order_sla: datetime | None,
    wave_cutoff: datetime | None,
    order_bucket: datetime | None,
    wave_bucket: datetime | None,
    window_minutes: int,
) -> Decimal:
    if (
        order_bucket is not None
        and wave_bucket is not None
        and order_bucket != wave_bucket
    ):
        return ZERO
    if order_sla is None or wave_cutoff is None:
        return Decimal("0.5")
    delta_minutes = abs((order_sla - wave_cutoff).total_seconds()) / 60.0
    window = Decimal(max(window_minutes, 1))
    return clamp(ONE - to_decimal(delta_minutes) / window, ZERO, ONE)


def compute_affinity(
    order_route: RoutePlan,
    wave_route: RoutePlan,
    meta: OrderWaveMeta,
    candidate: WaveCandidate,
    cfg: AffinityConfig,
) -> AffinityBreakdown:
    location_j = jaccard(
        order_route.location_signature(), wave_route.location_signature()
    )
    sku_j = jaccard(order_route.sku_signature(), wave_route.sku_signature())
    zone_j = jaccard(order_route.zone_signature(), wave_route.zone_signature())
    merged = merge_routes(wave_route, order_route, cfg)
    incremental = max(ZERO, merged.route_m - wave_route.route_m)
    travel_score = (
        clamp(ONE - incremental / cfg.max_cluster_travel_m, ZERO, ONE)
        if cfg.max_cluster_travel_m > ZERO
        else ZERO
    )
    sla_score = sla_compatibility(
        meta.sla_ship_at,
        candidate.cutoff_at,
        meta.cutoff_bucket,
        candidate.cutoff_bucket,
        cfg.cutoff_bucket_minutes,
    )
    weights = cfg.weights
    score = (
        weights["location"] * location_j
        + weights["sku"] * sku_j
        + weights["zone"] * zone_j
        + weights["travel"] * travel_score
        + weights["sla"] * sla_score
    )
    zone_count = len(order_route.zone_signature() | wave_route.zone_signature())
    return AffinityBreakdown(
        score=score,
        location_jaccard=location_j,
        sku_jaccard=sku_j,
        zone_jaccard=zone_j,
        travel_score=travel_score,
        sla_score=sla_score,
        incremental_travel_m=incremental,
        merged_route_m=merged.route_m,
        zone_count=zone_count,
    )


def choose_wave(
    order_route: RoutePlan,
    candidates: Sequence[WaveCandidate],
    meta: OrderWaveMeta,
    cfg: AffinityConfig,
) -> WaveDecision:
    best_key: tuple[Any, ...] | None = None
    best_candidate: WaveCandidate | None = None
    best_breakdown: AffinityBreakdown | None = None
    release_horizon = meta.now + timedelta(minutes=cfg.release_window_minutes)

    for candidate in candidates:
        if candidate.status not in ("planned", "released"):
            continue
        if (
            candidate.warehouse_id != meta.warehouse_id
            or candidate.strategy != cfg.strategy
        ):
            continue
        if candidate.release_at > release_horizon:
            continue
        if candidate.order_count + 1 > candidate.max_orders:
            continue
        if candidate.line_count + meta.line_count > candidate.max_lines:
            continue
        if candidate.unit_count + meta.unit_count > candidate.max_units:
            continue
        if cfg.strategy == "WAVE":
            if candidate.carrier_code != meta.carrier_code:
                continue
            if candidate.cutoff_bucket != meta.cutoff_bucket:
                continue
        if cfg.strategy == "ZONE":
            order_zones = order_route.zone_signature()
            if len(order_zones) != 1 or candidate.zone != next(iter(order_zones)):
                continue

        breakdown = compute_affinity(order_route, candidate.route, meta, candidate, cfg)
        if breakdown.score < cfg.threshold:
            continue
        if breakdown.zone_count > cfg.max_zone_count:
            continue
        if breakdown.merged_route_m > cfg.max_cluster_travel_m:
            continue
        key = (-breakdown.score, breakdown.incremental_travel_m, str(candidate.wave_id))
        if best_key is None or key < best_key:
            best_key = key
            best_candidate = candidate
            best_breakdown = breakdown

    if best_candidate is None or best_breakdown is None:
        return WaveDecision(wave_id=None, breakdown=None, create_new=True)
    return WaveDecision(
        wave_id=best_candidate.wave_id, breakdown=best_breakdown, create_new=False
    )


def compute_cutoff_bucket(value: datetime | None, minutes: int) -> datetime | None:
    if value is None:
        return None
    bucket_seconds = max(int(minutes), 1) * 60
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    elapsed = int((value.astimezone(UTC) - epoch).total_seconds())
    return epoch + timedelta(seconds=(elapsed // bucket_seconds) * bucket_seconds)


def affinity_config_from_workflow(
    workflow: Mapping[str, Any], strategy: str
) -> AffinityConfig:
    picking = workflow["picking"]
    raw_weights = picking["weights"]
    weights = {
        "location": to_decimal(raw_weights["location"]),
        "sku": to_decimal(raw_weights["sku"]),
        "zone": to_decimal(raw_weights["zone"]),
        "travel": to_decimal(raw_weights["travel"]),
        "sla": to_decimal(raw_weights["sla"]),
    }
    total = sum(weights.values(), ZERO)
    if abs(total - ONE) > Decimal("0.0001"):
        raise ValidationError(
            "picking affinity weights must sum to 1.0",
            {
                "weights": {key: str(value) for key, value in weights.items()},
                "sum": str(total),
            },
        )
    return AffinityConfig(
        strategy=str(strategy),
        weights=weights,
        threshold=to_decimal(picking["affinity_threshold"]),
        max_cluster_travel_m=to_decimal(picking["max_cluster_travel_m"]),
        max_zone_count=int(picking["max_zone_count"]),
        zone_penalty_m=to_decimal(picking["zone_penalty_m"]),
        vertical_penalty=to_decimal(picking["vertical_penalty"]),
        prefer_pick_face=bool(picking["prefer_pick_face"]),
        release_window_minutes=int(picking["wave_release_window_minutes"]),
        cutoff_bucket_minutes=int(picking["cutoff_bucket_minutes"]),
        allowed_location_types=tuple(picking["allowed_b2c_location_types"]),
    )
