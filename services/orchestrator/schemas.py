from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

Quantity = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=4)]
NonNegativeQuantity = Annotated[Decimal, Field(ge=0, max_digits=18, decimal_places=4)]
NonNegativeInt = Annotated[int, Field(ge=0)]


class StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True,
        use_enum_values=False,
    )


class OrderMode(str, Enum):
    B2C = "B2C"
    B2B = "B2B"
    HYBRID = "HYBRID"


class FulfillmentUnit(str, Enum):
    EACH = "EACH"
    CARTON = "CARTON"
    PALLET = "PALLET"


class PickingStrategy(str, Enum):
    WAVE = "WAVE"
    CLUSTER = "CLUSTER"
    ZONE = "ZONE"
    MULTI_BATCH = "MULTI_BATCH"


class DiscrepancySource(str, Enum):
    AMR = "AMR"
    RF = "RF"
    WMS = "WMS"
    API = "API"
    IOT = "IOT"


class WorkerStatus(str, Enum):
    OFF_SHIFT = "off_shift"
    AVAILABLE = "available"
    WORKING = "working"
    BREAK = "break"
    BLOCKED = "blocked"


class EquipmentType(str, Enum):
    RF_SCANNER = "RF_SCANNER"
    CART = "CART"
    AMR = "AMR"
    FORKLIFT = "FORKLIFT"
    REACH_TRUCK = "REACH_TRUCK"
    PALLET_JACK = "PALLET_JACK"
    AGV = "AGV"
    MANUAL = "MANUAL"


class PutawayPolicy(str, Enum):
    RESERVE = "RESERVE"
    CROSS_DOCK = "CROSS_DOCK"
    BULK_STAGING = "BULK_STAGING"


class VasCode(str, Enum):
    KITTING = "KITTING"
    LABELING = "LABELING"
    GIFT_WRAP = "GIFT_WRAP"
    RELABEL = "RELABEL"
    INSPECTION = "INSPECTION"
    ASSEMBLY = "ASSEMBLY"
    SERIAL_CAPTURE = "SERIAL_CAPTURE"
    CUSTOM = "CUSTOM"


def _aware_utc(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return value


class OrderLineIn(StrictModel):
    line_no: int = Field(gt=0)
    sku_code: str = Field(min_length=1, max_length=96)
    lot_code: str | None = Field(default=None, max_length=96)
    mode: OrderMode | None = None
    fulfillment_unit: FulfillmentUnit = FulfillmentUnit.EACH
    required_qty: Quantity
    require_same_lot: bool = True
    expiry_min: date | None = None
    expiry_max: date | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_dates(self) -> OrderLineIn:
        if self.expiry_min and self.expiry_max and self.expiry_min > self.expiry_max:
            raise ValueError("expiry_min must be less than or equal to expiry_max")
        return self


class OrderIn(StrictModel):
    external_order_id: str = Field(min_length=1, max_length=128)
    warehouse_id: UUID
    mode: OrderMode
    priority: int = Field(default=100, ge=0, le=1000)
    sla_ship_at: AwareDatetime | None = None
    carrier_code: str | None = Field(default=None, max_length=64)
    customer_ref: str | None = Field(default=None, max_length=128)
    metadata: dict[str, Any] = Field(default_factory=dict)
    lines: list[OrderLineIn] = Field(min_length=1, max_length=500)

    @field_validator("sla_ship_at", mode="before")
    @classmethod
    def parse_sla(cls, value: Any) -> Any:
        return _aware_utc(value)

    @model_validator(mode="after")
    def validate_lines(self) -> OrderIn:
        for line in self.lines:
            effective_mode = line.mode or self.mode
            if (
                self.mode in (OrderMode.B2C, OrderMode.B2B)
                and effective_mode != self.mode
            ):
                raise ValueError(
                    f"line {line.line_no} mode {effective_mode.value} conflicts with order mode {self.mode.value}"
                )
            if self.mode == OrderMode.HYBRID and effective_mode not in (
                OrderMode.B2C,
                OrderMode.B2B,
            ):
                raise ValueError(
                    f"line {line.line_no} in HYBRID order must be B2C or B2B"
                )
            if (
                effective_mode == OrderMode.B2B
                and line.fulfillment_unit != FulfillmentUnit.PALLET
            ):
                raise ValueError(
                    f"line {line.line_no} B2B must use PALLET fulfillment unit"
                )
            if (
                effective_mode == OrderMode.B2C
                and line.fulfillment_unit == FulfillmentUnit.PALLET
            ):
                raise ValueError(
                    f"line {line.line_no} B2C cannot use PALLET fulfillment unit"
                )
        return self


class AllocationCreated(StrictModel):
    order_line_id: UUID
    allocation_id: UUID
    bucket_id: UUID
    location_id: UUID
    lot_id: UUID | None
    qty: Decimal
    stock_status: str = "allocated"


class TaskCreated(StrictModel):
    task_id: UUID
    task_type: str
    priority: int
    required_zone: str | None
    required_equipment: list[str]
    state: str = "queued"


class RouteOrderResponse(StrictModel):
    order_id: UUID
    mode: OrderMode
    strategy: str | None
    wave_ids: list[UUID]
    allocations: list[AllocationCreated]
    tasks: list[TaskCreated]
    backordered_lines: list[UUID] = Field(default_factory=list)
    status: str


class DirectedPutawayIn(StrictModel):
    handling_unit_barcode: str = Field(min_length=2, max_length=128)
    warehouse_id: UUID
    sku_code: str = Field(min_length=1, max_length=96)
    lot_code: str | None = Field(default=None, max_length=96)
    qty: Quantity
    full_pallet_qty: Quantity
    source_location_id: UUID
    dock_location_id: UUID | None = None
    putaway_policy: PutawayPolicy = PutawayPolicy.RESERVE
    cross_dock_order_id: UUID | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_pallet(self) -> DirectedPutawayIn:
        if self.qty != self.full_pallet_qty:
            raise ValueError("directed putaway requires qty to equal full_pallet_qty")
        if (
            self.putaway_policy == PutawayPolicy.CROSS_DOCK
            and self.cross_dock_order_id is None
        ):
            raise ValueError("CROSS_DOCK putaway requires cross_dock_order_id")
        return self


class DirectedPutawayResponse(StrictModel):
    handling_unit_id: UUID
    bucket_id: UUID
    task_id: UUID
    task_type: str
    source_location_id: UUID
    target_location_id: UUID
    target_barcode: str
    required_equipment: list[str]
    status: str


class PutawayCompleteIn(StrictModel):
    worker_id: UUID
    handling_unit_barcode: str = Field(min_length=2, max_length=128)
    found_qty: Quantity
    target_location_id: UUID | None = None


class PutawayCompleteResponse(StrictModel):
    task_id: UUID
    handling_unit_id: UUID
    target_location_id: UUID
    state: str
    stock_status: str


class DiscrepancyEventIn(StrictModel):
    source: DiscrepancySource
    source_event_id: str = Field(min_length=1, max_length=160)
    warehouse_id: UUID
    location_id: UUID | None = None
    location_barcode: str | None = None
    sku_id: UUID | None = None
    sku_code: str | None = None
    lot_id: UUID | None = None
    lot_code: str | None = None
    expected_qty: NonNegativeQuantity
    found_qty: NonNegativeQuantity
    wave_id: UUID | None = None
    observed_at: AwareDatetime

    @field_validator("observed_at", mode="before")
    @classmethod
    def parse_observed(cls, value: Any) -> Any:
        return _aware_utc(value)

    @model_validator(mode="after")
    def validate_identifiers(self) -> DiscrepancyEventIn:
        if (self.location_id is None) == (self.location_barcode is None):
            raise ValueError("provide exactly one of location_id or location_barcode")
        if (self.sku_id is None) == (self.sku_code is None):
            raise ValueError("provide exactly one of sku_id or sku_code")
        if self.lot_id is not None and self.lot_code is not None:
            raise ValueError("provide either lot_id or lot_code, not both")
        return self


class DiscrepancyResponse(StrictModel):
    discrepancy_event_id: UUID
    location_id: UUID
    location_status: str
    quarantined_buckets: list[UUID]
    released_allocations: list[UUID]
    reallocated_lines: list[UUID]
    backordered_lines: list[UUID]
    cycle_count_task_id: UUID
    processing_result: dict[str, Any]


class WorkerHeartbeatIn(StrictModel):
    employee_code: str = Field(min_length=1, max_length=64)
    display_name: str = Field(min_length=1, max_length=128)
    warehouse_id: UUID
    status: WorkerStatus
    active_equipment: EquipmentType
    current_zone: str | None = None
    x_m: Decimal = Field(default=Decimal("0"), ge=0)
    y_m: Decimal = Field(default=Decimal("0"), ge=0)
    max_concurrent_tasks: int = Field(default=1, gt=0)
    skills: list[str] = Field(default_factory=list)


class WorkerHeartbeatResponse(StrictModel):
    worker_id: UUID
    status: WorkerStatus
    assigned_tasks: list[UUID]


class TaskClaimIn(StrictModel):
    max_tasks: int = Field(default=1, gt=0, le=20)


class TaskClaimed(StrictModel):
    task_id: UUID
    task_type: str
    priority: int
    payload: dict[str, Any]
    score: Decimal


class TaskClaimResponse(StrictModel):
    worker_id: UUID
    tasks: list[TaskClaimed]


class BillingStorageIn(StrictModel):
    source_id: UUID
    occurred_at: AwareDatetime
    as_of_date: date
    location_id: UUID
    sku_id: UUID
    lot_id: UUID | None = None
    days: Quantity
    volume_cm3: Quantity
    weight_kg: NonNegativeQuantity
    pallet_positions: NonNegativeQuantity | None = None
    hazardous: bool = False
    temperature_band: Literal["AMBIENT", "CHILLED", "FROZEN", "CONTROLLED"] = "AMBIENT"
    location_type: Literal[
        "piece_pick",
        "pallet",
        "bulk",
        "cross_dock",
        "staging",
        "qc",
        "dock",
        "returns",
        "quarantine",
    ] = "pallet"

    @field_validator("occurred_at", mode="before")
    @classmethod
    def parse_occurred(cls, value: Any) -> Any:
        return _aware_utc(value)


class BillingPickIn(StrictModel):
    source_id: UUID
    occurred_at: AwareDatetime
    mode: Literal["B2C", "B2B"]
    order_id: UUID
    order_line_id: UUID
    pieces: NonNegativeQuantity = Decimal("0")
    pallets: NonNegativeQuantity = Decimal("0")
    lines: NonNegativeInt = 1
    orders_count: NonNegativeInt = 1
    hazardous_pieces: NonNegativeQuantity = Decimal("0")
    oversized_pieces: NonNegativeQuantity = Decimal("0")
    same_day: bool = False
    cross_dock: bool = False

    @field_validator("occurred_at", mode="before")
    @classmethod
    def parse_occurred(cls, value: Any) -> Any:
        return _aware_utc(value)

    @model_validator(mode="after")
    def validate_units(self) -> BillingPickIn:
        if self.mode == "B2C" and self.pieces <= 0:
            raise ValueError("B2C pick requires pieces > 0")
        if self.mode == "B2B" and self.pallets <= 0:
            raise ValueError("B2B pick requires pallets > 0")
        if self.mode == "B2C" and self.pallets > 0:
            raise ValueError("B2C pick cannot bill pallets")
        if self.mode == "B2B" and self.pieces > 0:
            raise ValueError("B2B pick cannot bill pieces")
        if self.cross_dock and self.mode != "B2B":
            raise ValueError("cross_dock fee is only valid for B2B")
        return self


class VASChargeIn(StrictModel):
    source_id: UUID
    occurred_at: AwareDatetime
    vas_code: VasCode
    order_id: UUID | None = None
    order_line_id: UUID | None = None
    quantity: Quantity = Decimal("1")
    minutes: NonNegativeQuantity = Decimal("0")
    pallets: NonNegativeQuantity = Decimal("0")
    cartons: NonNegativeQuantity = Decimal("0")
    order_count: NonNegativeQuantity = Decimal("1")
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at", mode="before")
    @classmethod
    def parse_occurred(cls, value: Any) -> Any:
        return _aware_utc(value)


class ChargeResponse(StrictModel):
    billing_event_id: UUID
    event_type: str
    currency: str
    quantity: Decimal
    gross_amount: Decimal
    discount_amount: Decimal
    surcharge_amount: Decimal
    tax_amount: Decimal
    net_amount: Decimal
    components: list[dict[str, Any]]
    rate_snapshot: dict[str, Any]
