from __future__ import annotations

import sys
import unittest
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from orchestrator.affinity import (  # noqa: E402
    AffinityConfig,
    CandidateStock,
    Coordinate,
    LineRequirement,
    OrderWaveMeta,
    PlannedStop,
    RoutePlan,
    WaveCandidate,
    affinity_config_from_workflow,
    choose_wave,
    compute_cutoff_bucket,
    jaccard,
    plan_line_stops,
    travel_m,
)
from orchestrator.pricing import (  # noqa: E402
    PickChargeParams,
    StorageChargeParams,
    VASChargeParams,
    calculate_pick_charge,
    calculate_storage_charge,
    calculate_vas_charge,
    round_money,
    tiered_unit_rate,
)


def cfg(strategy: str = "CLUSTER") -> AffinityConfig:
    workflow = {
        "picking": {
            "strategy": strategy,
            "wave_release_window_minutes": 15,
            "max_orders_per_wave": 120,
            "max_lines_per_wave": 600,
            "max_units_per_wave": 4000,
            "max_zone_count": 2,
            "max_cluster_travel_m": 900,
            "affinity_threshold": Decimal("0.40"),
            "weights": {
                "location": Decimal("0.42"),
                "sku": Decimal("0.18"),
                "zone": Decimal("0.15"),
                "travel": Decimal("0.20"),
                "sla": Decimal("0.05"),
            },
            "zone_penalty_m": Decimal("25"),
            "vertical_penalty": Decimal("3"),
            "allowed_b2c_location_types": ["piece_pick", "pallet", "bulk"],
            "prefer_pick_face": True,
            "cutoff_bucket_minutes": 15,
        }
    }
    return affinity_config_from_workflow(workflow, strategy)


def candidate(
    sku_id: UUID,
    location_id: UUID,
    lot_id: UUID | None,
    qty: Decimal,
    x: Decimal,
    *,
    zone: str = "Z1",
    pick_face: bool = True,
) -> CandidateStock:
    return CandidateStock(
        bucket_id=uuid4(),
        sku_id=sku_id,
        lot_id=lot_id,
        serial_number=None,
        expiration_date=date(2027, 1, 1),
        handling_unit_id=None,
        warehouse_id=uuid4(),
        location_id=location_id,
        available_qty=qty,
        balance_version=1,
        header_version=1,
        location_barcode=f"LOC-{location_id.hex[:8]}",
        zone=zone,
        aisle="A01",
        bay="B01",
        level="L1",
        position="P1",
        x_m=x,
        y_m=Decimal("0"),
        z_m=Decimal("0"),
        travel_seq=int(x),
        location_type="piece_pick",
        pick_face=pick_face,
        allows_hazmat=True,
        location_temperature_band="ANY",
        sku_code="SKU-1",
        hazardous=False,
        sku_temperature_band="AMBIENT",
        requires_lot=False,
        requires_serial=False,
        velocity_score=Decimal("1"),
        unit_volume_cm3=Decimal("1000"),
        unit_weight_kg=Decimal("1"),
    )


class AffinityTests(unittest.TestCase):
    def test_travel_cost_includes_zone_and_vertical_penalty(self) -> None:
        config = cfg()
        source = Coordinate(Decimal("0"), Decimal("0"), Decimal("0"), "Z1")
        target = Coordinate(Decimal("10"), Decimal("5"), Decimal("2"), "Z2")
        cost = travel_m(source, target, config)
        self.assertEqual(
            cost, Decimal("10") + Decimal("5") + Decimal("6") + Decimal("25")
        )

    def test_jaccard(self) -> None:
        self.assertEqual(
            jaccard(frozenset({1, 2}), frozenset({2, 3})), Decimal("1") / Decimal("3")
        )
        self.assertEqual(jaccard(frozenset(), frozenset()), Decimal("1"))

    def test_plan_line_stops_locks_lot_and_fefo(self) -> None:
        config = cfg()
        sku_id = uuid4()
        dock = Coordinate(Decimal("0"), Decimal("0"), Decimal("0"), "OUT")
        lot_a = uuid4()
        lot_b = uuid4()
        candidates = [
            candidate(sku_id, uuid4(), lot_a, Decimal("4"), Decimal("1")),
            candidate(sku_id, uuid4(), lot_b, Decimal("9"), Decimal("2")),
        ]
        line = LineRequirement(
            order_line_id=uuid4(),
            order_id=uuid4(),
            sku_id=sku_id,
            lot_id=None,
            required_qty=Decimal("5"),
            picked_qty=Decimal("0"),
            allocated_qty=Decimal("0"),
            require_same_lot=True,
            expiry_min=None,
            expiry_max=None,
            hazardous=False,
            temperature_band="AMBIENT",
            requires_lot=False,
            requires_serial=False,
            mode="B2C",
            fulfillment_unit="EACH",
            allowed_location_types=("piece_pick",),
            version=1,
            units_per_pallet=Decimal("100"),
        )
        with self.assertRaises(Exception):
            plan_line_stops(line, candidates[:1], dock, config)
        candidates[0] = candidate(
            sku_id, candidates[0].location_id, lot_a, Decimal("6"), Decimal("1")
        )
        route = plan_line_stops(line, candidates, dock, config)
        self.assertEqual(route.total_qty(), Decimal("5"))
        self.assertEqual(len({stop.lot_id for stop in route.stops}), 1)
        self.assertGreater(route.route_m, Decimal("0"))

    def test_plan_line_stops_chooses_lot_with_sufficient_qty(self) -> None:
        config = cfg()
        sku_id = uuid4()
        dock = Coordinate(Decimal("0"), Decimal("0"), Decimal("0"), "OUT")
        lot_a = uuid4()
        lot_b = uuid4()
        candidates = [
            candidate(sku_id, uuid4(), lot_a, Decimal("4"), Decimal("1")),
            candidate(sku_id, uuid4(), lot_b, Decimal("6"), Decimal("10")),
        ]
        line = LineRequirement(
            order_line_id=uuid4(),
            order_id=uuid4(),
            sku_id=sku_id,
            lot_id=None,
            required_qty=Decimal("5"),
            picked_qty=Decimal("0"),
            allocated_qty=Decimal("0"),
            require_same_lot=True,
            expiry_min=None,
            expiry_max=None,
            hazardous=False,
            temperature_band="AMBIENT",
            requires_lot=False,
            requires_serial=False,
            mode="B2C",
            fulfillment_unit="EACH",
            allowed_location_types=("piece_pick",),
            version=1,
            units_per_pallet=Decimal("100"),
        )
        route = plan_line_stops(line, candidates, dock, config)
        self.assertEqual(route.total_qty(), Decimal("5"))
        self.assertEqual({stop.lot_id for stop in route.stops}, {lot_b})

    def test_choose_wave_is_deterministic(self) -> None:
        config = cfg()
        sku_id = uuid4()
        location_id = uuid4()
        dock = Coordinate(Decimal("0"), Decimal("0"), Decimal("0"), "OUT")
        stop = PlannedStop(
            bucket_id=uuid4(),
            location_id=location_id,
            sku_id=sku_id,
            lot_id=None,
            qty=Decimal("2"),
            coordinate=Coordinate(Decimal("5"), Decimal("0"), Decimal("0"), "Z1"),
            zone="Z1",
            travel_seq=1,
            location_type="piece_pick",
            expiration_date=None,
            stop_seq=1,
        )
        order_route = RoutePlan(dock, (stop,), Decimal("10"))
        wave_id = uuid4()
        now = datetime(2026, 11, 10, 10, 0, tzinfo=UTC)
        wave = WaveCandidate(
            wave_id=wave_id,
            tenant_id=uuid4(),
            warehouse_id=uuid4(),
            strategy="CLUSTER",
            status="planned",
            priority=100,
            carrier_code=None,
            cutoff_bucket=None,
            cutoff_at=None,
            zone=None,
            release_at=now,
            max_orders=100,
            max_lines=100,
            max_units=Decimal("1000"),
            order_count=1,
            line_count=1,
            unit_count=Decimal("10"),
            route=RoutePlan(dock, (stop,), Decimal("10")),
        )
        meta = OrderWaveMeta(
            order_id=uuid4(),
            warehouse_id=wave.warehouse_id,
            strategy="CLUSTER",
            now=now,
            priority=100,
            line_count=1,
            unit_count=Decimal("2"),
            sla_ship_at=None,
            carrier_code=None,
            cutoff_bucket=None,
        )
        decision = choose_wave(order_route, [wave], meta, config)
        self.assertFalse(decision.create_new)
        self.assertEqual(decision.wave_id, wave_id)
        self.assertIsNotNone(decision.breakdown)
        self.assertEqual(compute_cutoff_bucket(None, 15), None)


class PricingTests(unittest.TestCase):
    def test_tiered_rate_and_rounding(self) -> None:
        tiers = [{"up_to": "100", "rate": "0.80"}, {"up_to": None, "rate": "0.60"}]
        self.assertEqual(
            tiered_unit_rate(Decimal("10"), tiers, Decimal("1")), Decimal("0.80")
        )
        self.assertEqual(
            tiered_unit_rate(Decimal("101"), tiers, Decimal("1")), Decimal("0.60")
        )
        self.assertEqual(
            round_money(Decimal("1.005"), "AED", "HALF_UP"), Decimal("1.01")
        )

    def test_storage_pallet_charge(self) -> None:
        contract = {
            "id": uuid4(),
            "currency": "AED",
            "rounding_mode": "HALF_UP",
            "storage_basis": "PALLET_POSITION",
            "storage_free_days": 0,
            "pallet_position_volume_cm3": Decimal("1800000"),
            "cm3_per_cubic_foot": Decimal("28316.846592"),
        }
        rate = {
            "id": uuid4(),
            "basis": "PALLET_POSITION",
            "rate_per_pallet_day": Decimal("10"),
            "rate_per_cubic_foot_day": Decimal("0.10"),
            "hazardous_multiplier": Decimal("1.25"),
            "chilled_multiplier": Decimal("1.10"),
            "minimum_charge": Decimal("0"),
            "tiers": [],
        }
        params = StorageChargeParams(
            as_of_date=date(2026, 11, 10),
            occurred_at=datetime(2026, 11, 10, tzinfo=UTC),
            days=Decimal("2"),
            volume_cm3=Decimal("1800000"),
            weight_kg=Decimal("500"),
            pallet_positions=None,
            hazardous=False,
            temperature_band="AMBIENT",
            location_type="pallet",
        )
        result = calculate_storage_charge(params, contract, rate)
        self.assertEqual(result.net_amount, Decimal("20.00"))
        self.assertEqual(result.currency, "AED")

    def test_b2c_pick_tier_discount(self) -> None:
        contract = {"id": uuid4(), "currency": "AED", "rounding_mode": "HALF_UP"}
        rate = {
            "id": uuid4(),
            "rate_per_piece": Decimal("1"),
            "rate_per_pallet": Decimal("0"),
            "rate_per_order": Decimal("0"),
            "rate_per_line": Decimal("0"),
            "first_piece_surcharge": Decimal("0"),
            "hazardous_surcharge_per_piece": Decimal("0"),
            "oversized_surcharge_per_piece": Decimal("0"),
            "same_day_surcharge_pct": Decimal("0"),
            "cross_dock_fee_per_pallet": Decimal("0"),
            "minimum_charge": Decimal("0"),
            "tiers": [{"up_to": "100", "rate": "0.80"}],
        }
        params = PickChargeParams(
            mode="B2C",
            occurred_at=datetime(2026, 11, 10, tzinfo=UTC),
            pieces=Decimal("10"),
            pallets=Decimal("0"),
            lines=1,
            orders_count=1,
            hazardous_pieces=Decimal("0"),
            oversized_pieces=Decimal("0"),
            same_day=False,
            cross_dock=False,
        )
        result = calculate_pick_charge(params, rate, contract)
        self.assertEqual(result.gross_amount, Decimal("10.00"))
        self.assertEqual(result.discount_amount, Decimal("2.00"))
        self.assertEqual(result.net_amount, Decimal("8.00"))

    def test_storage_cubic_foot_tier_discount(self) -> None:
        contract = {
            "id": uuid4(),
            "currency": "AED",
            "rounding_mode": "HALF_UP",
            "storage_basis": "CUBIC_FOOT",
            "storage_free_days": 0,
            "pallet_position_volume_cm3": Decimal("1800000"),
            "cm3_per_cubic_foot": Decimal("28316.846592"),
        }
        rate = {
            "id": uuid4(),
            "basis": "CUBIC_FOOT",
            "rate_per_pallet_day": Decimal("10"),
            "rate_per_cubic_foot_day": Decimal("0.10"),
            "hazardous_multiplier": Decimal("1.25"),
            "chilled_multiplier": Decimal("1.10"),
            "minimum_charge": Decimal("0"),
            "tiers": [{"up_to": None, "rate": "0.08"}],
        }
        params = StorageChargeParams(
            as_of_date=date(2026, 11, 10),
            occurred_at=datetime(2026, 11, 10, tzinfo=UTC),
            days=Decimal("2"),
            volume_cm3=Decimal("28316.846592"),
            weight_kg=Decimal("10"),
            pallet_positions=None,
            hazardous=False,
            temperature_band="AMBIENT",
            location_type="bulk",
        )
        result = calculate_storage_charge(params, contract, rate)
        self.assertEqual(result.gross_amount, Decimal("0.20"))
        self.assertEqual(result.discount_amount, Decimal("0.04"))
        self.assertEqual(result.net_amount, Decimal("0.16"))

    def test_b2b_pallet_pick_and_cross_dock_fee(self) -> None:
        contract = {"id": uuid4(), "currency": "AED", "rounding_mode": "HALF_UP"}
        rate = {
            "id": uuid4(),
            "rate_per_piece": Decimal("1"),
            "rate_per_pallet": Decimal("25"),
            "rate_per_order": Decimal("0"),
            "rate_per_line": Decimal("0"),
            "first_piece_surcharge": Decimal("0"),
            "hazardous_surcharge_per_piece": Decimal("0"),
            "oversized_surcharge_per_piece": Decimal("0"),
            "same_day_surcharge_pct": Decimal("0"),
            "cross_dock_fee_per_pallet": Decimal("2"),
            "minimum_charge": Decimal("0"),
            "tiers": [],
        }
        params = PickChargeParams(
            mode="B2B",
            occurred_at=datetime(2026, 11, 10, tzinfo=UTC),
            pieces=Decimal("0"),
            pallets=Decimal("3"),
            lines=1,
            orders_count=1,
            hazardous_pieces=Decimal("0"),
            oversized_pieces=Decimal("0"),
            same_day=False,
            cross_dock=True,
        )
        result = calculate_pick_charge(params, rate, contract)
        self.assertEqual(result.gross_amount, Decimal("81.00"))
        self.assertEqual(result.discount_amount, Decimal("0.00"))
        self.assertEqual(result.surcharge_amount, Decimal("6.00"))
        self.assertEqual(result.net_amount, Decimal("81.00"))

    def test_vas_tier_discount_and_complexity(self) -> None:
        contract = {"id": uuid4(), "currency": "AED", "rounding_mode": "HALF_UP"}
        rate = {
            "id": uuid4(),
            "unit_basis": "EACH",
            "rate_per_unit": Decimal("2"),
            "setup_fee": Decimal("5"),
            "material_fee_per_unit": Decimal("0"),
            "minimum_charge": Decimal("0"),
            "complexity_multiplier": Decimal("1.25"),
            "tiers": [{"up_to": None, "rate": "1.50"}],
        }
        params = VASChargeParams(
            vas_code="KITTING",
            occurred_at=datetime(2026, 11, 10, tzinfo=UTC),
            quantity=Decimal("10"),
            minutes=Decimal("0"),
            pallets=Decimal("0"),
            cartons=Decimal("0"),
            order_count=Decimal("1"),
        )
        result = calculate_vas_charge(params, rate, contract)
        self.assertEqual(result.gross_amount, Decimal("30.00"))
        self.assertEqual(result.discount_amount, Decimal("5.00"))
        self.assertEqual(result.surcharge_amount, Decimal("5.00"))
        self.assertEqual(result.net_amount, Decimal("25.00"))

    def test_vas_charge(self) -> None:
        contract = {"id": uuid4(), "currency": "AED", "rounding_mode": "HALF_UP"}
        rate = {
            "id": uuid4(),
            "unit_basis": "EACH",
            "rate_per_unit": Decimal("2"),
            "setup_fee": Decimal("5"),
            "material_fee_per_unit": Decimal("0"),
            "minimum_charge": Decimal("0"),
            "complexity_multiplier": Decimal("1"),
            "tiers": [],
        }
        params = VASChargeParams(
            vas_code="GIFT_WRAP",
            occurred_at=datetime(2026, 11, 10, tzinfo=UTC),
            quantity=Decimal("3"),
            minutes=Decimal("0"),
            pallets=Decimal("0"),
            cartons=Decimal("0"),
            order_count=Decimal("1"),
        )
        result = calculate_vas_charge(params, rate, contract)
        self.assertEqual(result.net_amount, Decimal("11.00"))
        self.assertEqual(result.quantity, Decimal("3"))


if __name__ == "__main__":
    unittest.main()
