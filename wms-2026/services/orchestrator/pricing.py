from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import (
    ROUND_CEILING,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    Decimal,
)
from typing import Any, Mapping

from .errors import ValidationError
from .utils import ceil_decimal, to_decimal

ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")

ROUNDING_MODES = {
    "HALF_UP": ROUND_HALF_UP,
    "HALF_EVEN": ROUND_HALF_EVEN,
    "CEILING": ROUND_CEILING,
    "FLOOR": ROUND_FLOOR,
}

ZERO_DECIMAL_CURRENCIES = {"JPY", "KRW", "VND", "CLP", "ISK"}
THREE_DECIMAL_CURRENCIES = {"BHD", "KWD", "OMR", "TND", "JOD"}


def _d(value: Any, default: Decimal = ZERO) -> Decimal:
    return default if value is None else to_decimal(value)


def currency_exponent(currency: str) -> int:
    code = currency.upper()
    if code in ZERO_DECIMAL_CURRENCIES:
        return 0
    if code in THREE_DECIMAL_CURRENCIES:
        return 3
    return 2


def round_money(value: Decimal, currency: str, rounding_mode: str) -> Decimal:
    mode = ROUNDING_MODES.get(rounding_mode.upper())
    if mode is None:
        raise ValidationError(
            "unsupported rounding mode", {"rounding_mode": rounding_mode}
        )
    exponent = currency_exponent(currency)
    quantum = Decimal(1).scaleb(-exponent)
    return value.quantize(quantum, rounding=mode)


def tiered_unit_rate(quantity: Decimal, tiers: Any, default_rate: Decimal) -> Decimal:
    if not tiers:
        return default_rate
    parsed: list[tuple[Decimal | None, Decimal]] = []
    for tier in tiers:
        if not isinstance(tier, Mapping) or "rate" not in tier:
            raise ValidationError("invalid rate tier", {"tier": str(tier)})
        up_to_raw = tier.get("up_to")
        up_to = None if up_to_raw is None else _d(up_to_raw)
        parsed.append((up_to, _d(tier["rate"])))
    parsed.sort(key=lambda item: (item[0] is None, item[0] or ZERO))
    for up_to, rate in parsed:
        if up_to is None or quantity <= up_to:
            return rate
    return parsed[-1][1]


def _component(
    code: str,
    description: str,
    quantity: Decimal,
    rate: Decimal,
    amount: Decimal,
) -> dict[str, Any]:
    return {
        "code": code,
        "description": description,
        "quantity": quantity,
        "rate": rate,
        "amount": amount,
    }


@dataclass(frozen=True)
class ChargeResult:
    quantity: Decimal
    gross_amount: Decimal
    discount_amount: Decimal
    surcharge_amount: Decimal
    tax_amount: Decimal
    net_amount: Decimal
    currency: str
    components: list[dict[str, Any]]
    rate_snapshot: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _finalize(
    *,
    # `gross` is the pre-discount amount and already includes surcharges.
    # `discount` is subtracted exactly once to derive the net amount.
    quantity: Decimal,
    gross: Decimal,
    discount: Decimal,
    surcharge: Decimal,
    tax: Decimal,
    currency: str,
    rounding_mode: str,
    components: list[dict[str, Any]],
    rate_snapshot: dict[str, Any],
) -> ChargeResult:
    gross_rounded = round_money(max(gross, ZERO), currency, rounding_mode)
    discount_rounded = round_money(
        min(max(discount, ZERO), gross_rounded), currency, rounding_mode
    )
    surcharge_rounded = round_money(max(surcharge, ZERO), currency, rounding_mode)
    tax_rounded = round_money(max(tax, ZERO), currency, rounding_mode)
    net = round_money(
        gross_rounded - discount_rounded + tax_rounded, currency, rounding_mode
    )
    return ChargeResult(
        quantity=quantity,
        gross_amount=gross_rounded,
        discount_amount=discount_rounded,
        surcharge_amount=surcharge_rounded,
        tax_amount=tax_rounded,
        net_amount=max(net, ZERO),
        currency=currency,
        components=components,
        rate_snapshot=rate_snapshot,
    )


@dataclass(frozen=True)
class StorageChargeParams:
    as_of_date: date
    occurred_at: datetime
    days: Decimal
    volume_cm3: Decimal
    weight_kg: Decimal
    pallet_positions: Decimal | None
    hazardous: bool
    temperature_band: str
    location_type: str


def calculate_storage_charge(
    params: StorageChargeParams,
    contract: Mapping[str, Any],
    rate: Mapping[str, Any],
) -> ChargeResult:
    currency = str(contract["currency"])
    rounding_mode = str(contract["rounding_mode"])
    basis = str(contract["storage_basis"])
    free_days = _d(contract["storage_free_days"])
    billable_days = max(ZERO, params.days - free_days)
    pallet_volume = _d(contract["pallet_position_volume_cm3"], ONE)
    cm3_per_cubic_foot = _d(contract["cm3_per_cubic_foot"], ONE)
    computed_positions = ceil_decimal(params.volume_cm3 / pallet_volume)
    positions = (
        computed_positions
        if params.pallet_positions is None
        else max(computed_positions, params.pallet_positions)
    )
    cubic_feet = params.volume_cm3 / cm3_per_cubic_foot

    pallet_rate = _d(rate["rate_per_pallet_day"])
    cubic_rate = _d(rate["rate_per_cubic_foot_day"])
    tier_rate = tiered_unit_rate(cubic_feet, rate.get("tiers"), cubic_rate)
    pallet_charge = positions * pallet_rate * billable_days
    standard_cubic_charge = cubic_rate * cubic_feet * billable_days
    tiered_cubic_charge = tier_rate * cubic_feet * billable_days
    tier_discount = max(ZERO, standard_cubic_charge - tiered_cubic_charge)

    components: list[dict[str, Any]] = []
    if basis == "PALLET_POSITION":
        base = pallet_charge
        discount = ZERO
        billable_quantity = positions * billable_days
        components.append(
            _component(
                "STORAGE_PALLET_DAY",
                "Daily pallet-position storage",
                billable_quantity,
                pallet_rate,
                base,
            )
        )
    elif basis in ("CUBIC_FOOT", "TIERED"):
        base = standard_cubic_charge
        discount = tier_discount
        billable_quantity = cubic_feet * billable_days
        components.append(
            _component(
                "STORAGE_CUBIC_FOOT_DAY",
                "Daily cubic-foot storage at standard rate",
                billable_quantity,
                cubic_rate,
                base,
            )
        )
        if tier_discount > ZERO:
            components.append(
                _component(
                    "STORAGE_TIER_DISCOUNT",
                    "Volume tier discount",
                    billable_quantity,
                    ZERO,
                    -tier_discount,
                )
            )
    elif basis == "MAX_OF_BOTH":
        base = max(pallet_charge, standard_cubic_charge)
        discount = ZERO
        billable_quantity = max(positions * billable_days, cubic_feet * billable_days)
        components.append(
            _component(
                "STORAGE_MAX_OF_BOTH",
                "Greater of pallet-position and cubic-foot storage",
                billable_quantity,
                ZERO,
                base,
            )
        )
    else:
        raise ValidationError("unsupported storage basis", {"basis": basis})

    hazard_multiplier = (
        _d(rate["hazardous_multiplier"], ONE) if params.hazardous else ONE
    )
    temperature_multiplier = (
        _d(rate["chilled_multiplier"], ONE)
        if params.temperature_band != "AMBIENT"
        else ONE
    )
    multiplier = hazard_multiplier * temperature_multiplier
    surcharge = base * (multiplier - ONE)
    hazard_component = base * (hazard_multiplier - ONE)
    temperature_component = base * (temperature_multiplier - ONE)
    if hazard_component > ZERO:
        components.append(
            _component(
                "HAZARDOUS_SURCHARGE",
                "Hazardous goods multiplier",
                base,
                hazard_multiplier - ONE,
                hazard_component,
            )
        )
    if temperature_component > ZERO:
        components.append(
            _component(
                "TEMPERATURE_SURCHARGE",
                "Temperature-controlled multiplier",
                base,
                temperature_multiplier - ONE,
                temperature_component,
            )
        )
    combined_component = surcharge - hazard_component - temperature_component
    if combined_component > ZERO:
        components.append(
            _component(
                "COMBINED_MULTIPLIER_SURCHARGE",
                "Cross-term of hazardous and temperature multipliers",
                base,
                multiplier - hazard_multiplier - temperature_multiplier + ONE,
                combined_component,
            )
        )

    gross = base + surcharge
    minimum_charge = _d(rate["minimum_charge"])
    net_before_minimum = gross - discount
    if net_before_minimum < minimum_charge:
        adjustment = minimum_charge - net_before_minimum
        gross += adjustment
        surcharge += adjustment
        components.append(
            _component(
                "MINIMUM_CHARGE",
                "Contract minimum charge adjustment",
                ONE,
                ZERO,
                adjustment,
            )
        )

    snapshot = {
        "contract_id": contract.get("id"),
        "rate_card_id": rate.get("id"),
        "basis": basis,
        "rate_basis": str(rate.get("basis")),
        "currency": currency,
        "rounding_mode": rounding_mode,
        "free_days": free_days,
        "billable_days": billable_days,
        "computed_pallet_positions": computed_positions,
        "billed_pallet_positions": positions,
        "cubic_feet": cubic_feet,
        "pallet_rate_per_day": pallet_rate,
        "cubic_foot_rate_per_day": cubic_rate,
        "tiered_cubic_foot_rate_per_day": tier_rate,
        "tier_discount": tier_discount,
        "hazardous_multiplier": hazard_multiplier,
        "chilled_multiplier": temperature_multiplier,
        "combined_multiplier": multiplier,
        "location_type": params.location_type,
    }
    return _finalize(
        quantity=billable_quantity,
        gross=gross,
        discount=discount,
        surcharge=surcharge,
        tax=ZERO,
        currency=currency,
        rounding_mode=rounding_mode,
        components=components,
        rate_snapshot=snapshot,
    )


@dataclass(frozen=True)
class PickChargeParams:
    mode: str
    occurred_at: datetime
    pieces: Decimal
    pallets: Decimal
    lines: int
    orders_count: int
    hazardous_pieces: Decimal
    oversized_pieces: Decimal
    same_day: bool
    cross_dock: bool


def calculate_pick_charge(
    params: PickChargeParams,
    rate: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> ChargeResult:
    currency = str(contract["currency"])
    rounding_mode = str(contract["rounding_mode"])
    components: list[dict[str, Any]] = []
    standard_piece_rate = _d(rate["rate_per_piece"])
    tier_piece_rate = tiered_unit_rate(
        params.pieces, rate.get("tiers"), standard_piece_rate
    )

    if params.mode == "B2C":
        quantity = params.pieces
        piece_charge = standard_piece_rate * params.pieces
        line_charge = _d(rate["rate_per_line"]) * Decimal(params.lines)
        order_charge = _d(rate["rate_per_order"]) * Decimal(params.orders_count)
        first_piece_charge = _d(rate["first_piece_surcharge"]) * Decimal(params.lines)
        base = piece_charge + line_charge + order_charge + first_piece_charge
        discount = max(ZERO, (standard_piece_rate - tier_piece_rate) * params.pieces)
        components.extend(
            [
                _component(
                    "PICK_PER_PIECE",
                    "B2C piece pick at standard rate",
                    params.pieces,
                    standard_piece_rate,
                    piece_charge,
                ),
                _component(
                    "PICK_PER_LINE",
                    "Pick line fee",
                    Decimal(params.lines),
                    _d(rate["rate_per_line"]),
                    line_charge,
                ),
                _component(
                    "PICK_PER_ORDER",
                    "Order fee",
                    Decimal(params.orders_count),
                    _d(rate["rate_per_order"]),
                    order_charge,
                ),
                _component(
                    "FIRST_PIECE_SURCHARGE",
                    "First-piece handling per line",
                    Decimal(params.lines),
                    _d(rate["first_piece_surcharge"]),
                    first_piece_charge,
                ),
            ]
        )
        if discount > ZERO:
            components.append(
                _component(
                    "PICK_VOLUME_DISCOUNT",
                    "Piece-rate tier discount",
                    params.pieces,
                    ZERO,
                    -discount,
                )
            )
    elif params.mode == "B2B":
        quantity = params.pallets
        pallet_charge = _d(rate["rate_per_pallet"]) * params.pallets
        line_charge = _d(rate["rate_per_line"]) * Decimal(params.lines)
        order_charge = _d(rate["rate_per_order"]) * Decimal(params.orders_count)
        base = pallet_charge + line_charge + order_charge
        discount = ZERO
        components.extend(
            [
                _component(
                    "PICK_PER_PALLET",
                    "B2B directed pallet pick",
                    params.pallets,
                    _d(rate["rate_per_pallet"]),
                    pallet_charge,
                ),
                _component(
                    "PICK_PER_LINE",
                    "Pallet line fee",
                    Decimal(params.lines),
                    _d(rate["rate_per_line"]),
                    line_charge,
                ),
                _component(
                    "PICK_PER_ORDER",
                    "Order fee",
                    Decimal(params.orders_count),
                    _d(rate["rate_per_order"]),
                    order_charge,
                ),
            ]
        )
    else:
        raise ValidationError("unsupported pick mode", {"mode": params.mode})

    hazardous_surcharge = params.hazardous_pieces * _d(
        rate["hazardous_surcharge_per_piece"]
    )
    oversized_surcharge = params.oversized_pieces * _d(
        rate["oversized_surcharge_per_piece"]
    )
    same_day_surcharge = (
        base * _d(rate["same_day_surcharge_pct"]) / HUNDRED if params.same_day else ZERO
    )
    cross_dock_fee = (
        _d(rate["cross_dock_fee_per_pallet"]) * params.pallets
        if params.cross_dock
        else ZERO
    )
    surcharge = (
        hazardous_surcharge + oversized_surcharge + same_day_surcharge + cross_dock_fee
    )
    for code, description, qty, unit_rate, amount in (
        (
            "HAZARDOUS_PIECE_SURCHARGE",
            "Hazardous piece surcharge",
            params.hazardous_pieces,
            _d(rate["hazardous_surcharge_per_piece"]),
            hazardous_surcharge,
        ),
        (
            "OVERSIZE_PIECE_SURCHARGE",
            "Oversized piece surcharge",
            params.oversized_pieces,
            _d(rate["oversized_surcharge_per_piece"]),
            oversized_surcharge,
        ),
        (
            "SAME_DAY_SURCHARGE",
            "Same-day SLA surcharge",
            base,
            _d(rate["same_day_surcharge_pct"]) / HUNDRED,
            same_day_surcharge,
        ),
        (
            "CROSS_DOCK_FEE",
            "Cross-dock pallet handling",
            params.pallets,
            _d(rate["cross_dock_fee_per_pallet"]),
            cross_dock_fee,
        ),
    ):
        if amount > ZERO:
            components.append(_component(code, description, qty, unit_rate, amount))

    gross = base + surcharge
    minimum_charge = _d(rate["minimum_charge"])
    net_before_minimum = gross - discount
    if net_before_minimum < minimum_charge:
        adjustment = minimum_charge - net_before_minimum
        gross += adjustment
        surcharge += adjustment
        components.append(
            _component(
                "MINIMUM_CHARGE",
                "Contract minimum pick charge",
                ONE,
                ZERO,
                adjustment,
            )
        )

    snapshot = {
        "contract_id": contract.get("id"),
        "rate_card_id": rate.get("id"),
        "mode": params.mode,
        "currency": currency,
        "rounding_mode": rounding_mode,
        "standard_rate_per_piece": standard_piece_rate,
        "tiered_rate_per_piece": tier_piece_rate,
        "rate_per_pallet": _d(rate["rate_per_pallet"]),
        "rate_per_line": _d(rate["rate_per_line"]),
        "rate_per_order": _d(rate["rate_per_order"]),
        "same_day_surcharge_pct": _d(rate["same_day_surcharge_pct"]),
        "cross_dock": params.cross_dock,
        "tiers": rate.get("tiers") or [],
    }
    return _finalize(
        quantity=quantity,
        gross=gross,
        discount=discount,
        surcharge=surcharge,
        tax=ZERO,
        currency=currency,
        rounding_mode=rounding_mode,
        components=components,
        rate_snapshot=snapshot,
    )


@dataclass(frozen=True)
class VASChargeParams:
    vas_code: str
    occurred_at: datetime
    quantity: Decimal
    minutes: Decimal
    pallets: Decimal
    cartons: Decimal
    order_count: Decimal


def calculate_vas_charge(
    params: VASChargeParams,
    rate: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> ChargeResult:
    currency = str(contract["currency"])
    rounding_mode = str(contract["rounding_mode"])
    unit_basis = str(rate["unit_basis"])
    units_map = {
        "EACH": params.quantity,
        "ORDER": params.order_count,
        "MINUTE": params.minutes,
        "PALLET": params.pallets,
        "CARTON": params.cartons,
    }
    if unit_basis not in units_map:
        raise ValidationError("unsupported VAS unit basis", {"unit_basis": unit_basis})
    units = units_map[unit_basis]
    if units <= ZERO:
        raise ValidationError(
            "VAS transaction has zero billable units for its rate basis",
            {
                "vas_code": params.vas_code,
                "unit_basis": unit_basis,
                "units": str(units),
            },
        )

    standard_rate = _d(rate["rate_per_unit"])
    applied_rate = tiered_unit_rate(units, rate.get("tiers"), standard_rate)
    discount = max(ZERO, (standard_rate - applied_rate) * units)
    setup_fee = _d(rate["setup_fee"])
    material_fee = _d(rate["material_fee_per_unit"]) * units
    standard_units_charge = standard_rate * units
    base = standard_units_charge + setup_fee + material_fee
    discounted_subtotal = base - discount

    complexity = _d(rate["complexity_multiplier"], ONE)
    if complexity < ONE:
        raise ValidationError(
            "VAS complexity multiplier cannot be below one",
            {"complexity_multiplier": str(complexity)},
        )
    complexity_surcharge = discounted_subtotal * (complexity - ONE)
    surcharge = complexity_surcharge

    components = [
        _component(
            "VAS_UNITS",
            f"{params.vas_code} billable units at standard rate",
            units,
            standard_rate,
            standard_units_charge,
        ),
        _component("VAS_SETUP", "VAS setup fee", ONE, ZERO, setup_fee),
        _component(
            "VAS_MATERIALS",
            "VAS materials",
            units,
            _d(rate["material_fee_per_unit"]),
            material_fee,
        ),
    ]
    if discount > ZERO:
        components.append(
            _component(
                "VAS_VOLUME_DISCOUNT", "VAS tier discount", units, ZERO, -discount
            )
        )
    if complexity_surcharge > ZERO:
        components.append(
            _component(
                "VAS_COMPLEXITY",
                "VAS complexity multiplier applied after discount",
                discounted_subtotal,
                complexity - ONE,
                complexity_surcharge,
            )
        )

    gross = base + surcharge
    minimum_charge = _d(rate["minimum_charge"])
    net_before_minimum = gross - discount
    if net_before_minimum < minimum_charge:
        adjustment = minimum_charge - net_before_minimum
        gross += adjustment
        surcharge += adjustment
        components.append(
            _component(
                "MINIMUM_CHARGE", "Contract minimum VAS charge", ONE, ZERO, adjustment
            )
        )

    snapshot = {
        "contract_id": contract.get("id"),
        "rate_card_id": rate.get("id"),
        "vas_code": params.vas_code,
        "unit_basis": unit_basis,
        "billable_units": units,
        "standard_rate_per_unit": standard_rate,
        "applied_rate_per_unit": applied_rate,
        "tier_discount": discount,
        "setup_fee": setup_fee,
        "material_fee_per_unit": _d(rate["material_fee_per_unit"]),
        "complexity_multiplier": complexity,
        "currency": currency,
        "rounding_mode": rounding_mode,
        "tiers": rate.get("tiers") or [],
    }
    return _finalize(
        quantity=units,
        gross=gross,
        discount=discount,
        surcharge=surcharge,
        tax=ZERO,
        currency=currency,
        rounding_mode=rounding_mode,
        components=components,
        rate_snapshot=snapshot,
    )
