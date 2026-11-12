from __future__ import annotations

import ipaddress
from typing import Any
from uuid import UUID

from .affinity import Coordinate
from .auth import TenantContext
from .config import Settings
from .db import fetch_all, fetch_one
from .errors import NotFoundError, PermissionDeniedError, ValidationError
from .schemas import OrderMode
from .sql import GET_B2C_CANDIDATES
from .utils import deep_merge, to_decimal, utcnow


async def validate_api_client(
    conn: Any,
    ctx: TenantContext,
    *,
    required_scope: str,
    mode: OrderMode | None = None,
) -> dict[str, Any]:
    row = await fetch_one(
        conn,
        """
        SELECT scopes, allowed_modes, allowed_cidrs, status, rate_limit_rps, revoked_at
        FROM tenant_api_clients
        WHERE tenant_id = %(tenant_id)s AND client_id = %(client_id)s
        """,
        {"tenant_id": ctx.tenant_id, "client_id": ctx.client_id},
    )
    if row is None:
        raise PermissionDeniedError("API client is not registered for this tenant")
    if row["status"] != "active" or row["revoked_at"] is not None:
        raise PermissionDeniedError("API client is suspended or revoked")

    db_scopes = set(row["scopes"] or [])
    if required_scope not in ctx.scopes or required_scope not in db_scopes:
        raise PermissionDeniedError(
            "API client lacks the required scope",
            {
                "required_scope": required_scope,
                "gateway_scopes": sorted(ctx.scopes),
                "registered_scopes": sorted(db_scopes),
            },
        )
    allowed_modes = {str(item) for item in (row["allowed_modes"] or [])}
    if mode is not None and mode.value not in allowed_modes:
        raise PermissionDeniedError(
            "API client is not permitted to submit this order mode",
            {"mode": mode.value, "allowed_modes": sorted(allowed_modes)},
        )

    cidrs = [str(item) for item in (row["allowed_cidrs"] or [])]
    if cidrs:
        allowed = False
        try:
            client_ip = ipaddress.ip_address(ctx.client_ip)
            for cidr in cidrs:
                try:
                    if client_ip in ipaddress.ip_network(cidr, strict=False):
                        allowed = True
                        break
                except ValueError:
                    continue
        except ValueError:
            allowed = False
        if not allowed:
            raise PermissionDeniedError(
                "API client source IP is outside the registered CIDR allowlist",
                {"client_ip": ctx.client_ip, "allowed_cidrs": cidrs},
            )
    return {
        "rate_limit_rps": int(row["rate_limit_rps"]),
        "allowed_modes": sorted(allowed_modes),
    }


async def load_workflow(
    conn: Any,
    tenant_id: UUID,
    settings: Settings,
    workflow_key: str = "fulfillment",
) -> tuple[dict[str, Any], int]:
    row = await fetch_one(
        conn,
        """
        SELECT spec, version
        FROM workflow_definitions
        WHERE tenant_id = %(tenant_id)s
          AND workflow_key = %(workflow_key)s
          AND is_active
          AND effective_from <= now()
        ORDER BY version DESC, effective_from DESC
        LIMIT 1
        """,
        {"tenant_id": tenant_id, "workflow_key": workflow_key},
    )
    if row is None:
        return settings.default_workflow, 1
    return deep_merge(settings.default_workflow, row["spec"]), int(row["version"])


async def resolve_sku(conn: Any, tenant_id: UUID, sku_code: str) -> dict[str, Any]:
    row = await fetch_one(
        conn,
        """
        SELECT * FROM skus
        WHERE tenant_id = %(tenant_id)s
          AND sku_code = %(sku_code)s
          AND status = 'active'
        """,
        {"tenant_id": tenant_id, "sku_code": sku_code},
    )
    if row is None:
        raise NotFoundError("SKU", sku_code, {"tenant_id": str(tenant_id)})
    return row


async def resolve_sku_by_id(conn: Any, tenant_id: UUID, sku_id: UUID) -> dict[str, Any]:
    row = await fetch_one(
        conn,
        """
        SELECT * FROM skus
        WHERE tenant_id = %(tenant_id)s AND id = %(sku_id)s
        """,
        {"tenant_id": tenant_id, "sku_id": sku_id},
    )
    if row is None:
        raise NotFoundError("SKU", sku_id, {"tenant_id": str(tenant_id)})
    return row


async def resolve_lot(
    conn: Any,
    tenant_id: UUID,
    sku_id: UUID,
    lot_code: str | None,
    *,
    required: bool = False,
) -> UUID | None:
    if lot_code is None:
        if required:
            raise ValidationError(
                "SKU requires a lot code",
                {"tenant_id": str(tenant_id), "sku_id": str(sku_id)},
            )
        return None
    row = await fetch_one(
        conn,
        """
        SELECT id, expires_on
        FROM lots
        WHERE tenant_id = %(tenant_id)s
          AND sku_id = %(sku_id)s
          AND lot_code = %(lot_code)s
        """,
        {"tenant_id": tenant_id, "sku_id": sku_id, "lot_code": lot_code},
    )
    if row is None:
        raise NotFoundError(
            "Lot", f"{sku_id}:{lot_code}", {"tenant_id": str(tenant_id)}
        )
    return UUID(str(row["id"]))


async def resolve_lot_by_id_or_code(
    conn: Any,
    tenant_id: UUID,
    sku_id: UUID,
    lot_id: UUID | None,
    lot_code: str | None,
) -> UUID | None:
    if lot_id is not None:
        row = await fetch_one(
            conn,
            """
            SELECT id FROM lots
            WHERE tenant_id = %(tenant_id)s AND sku_id = %(sku_id)s AND id = %(lot_id)s
            """,
            {"tenant_id": tenant_id, "sku_id": sku_id, "lot_id": lot_id},
        )
        if row is None:
            raise NotFoundError(
                "Lot", lot_id, {"tenant_id": str(tenant_id), "sku_id": str(sku_id)}
            )
        return UUID(str(row["id"]))
    if lot_code is not None:
        return await resolve_lot(conn, tenant_id, sku_id, lot_code)
    return None


async def resolve_location(
    conn: Any,
    tenant_id: UUID,
    *,
    location_id: UUID | None = None,
    location_barcode: str | None = None,
    warehouse_id: UUID | None = None,
) -> dict[str, Any]:
    if (location_id is None) == (location_barcode is None):
        raise ValidationError("provide exactly one of location_id or location_barcode")
    params: dict[str, Any] = {
        "tenant_id": tenant_id,
        "location_id": location_id,
        "location_barcode": location_barcode,
        "warehouse_id": warehouse_id,
    }
    row = await fetch_one(
        conn,
        """
        SELECT l.*
        FROM locations l
        WHERE (
            (%(location_id)s::uuid IS NOT NULL AND l.id = %(location_id)s::uuid)
            OR (%(location_barcode)s::text IS NOT NULL
                AND l.barcode = %(location_barcode)s::text)
        )
          AND (l.exclusive_tenant_id IS NULL
               OR l.exclusive_tenant_id = %(tenant_id)s::uuid)
          AND EXISTS (
              SELECT 1
              FROM tenant_warehouse_access a
              WHERE a.tenant_id = %(tenant_id)s
                AND a.warehouse_id = l.warehouse_id
                AND a.can_read
          )
          AND (%(warehouse_id)s::uuid IS NULL
               OR l.warehouse_id = %(warehouse_id)s::uuid)
        ORDER BY CASE WHEN %(location_id)s::uuid IS NOT NULL THEN 0 ELSE 1 END,
                 l.travel_seq,
                 l.id
        LIMIT 1
        """,
        params,
    )
    if row is None:
        raise NotFoundError("Location", location_id or location_barcode)
    if warehouse_id is not None and UUID(str(row["warehouse_id"])) != warehouse_id:
        raise ValidationError(
            "location does not belong to the requested warehouse",
            {
                "location_id": str(row["id"]),
                "location_warehouse_id": str(row["warehouse_id"]),
                "requested_warehouse_id": str(warehouse_id),
            },
        )
    return row


async def get_dock(
    conn: Any,
    warehouse_id: UUID,
    preferred_location_id: UUID | None = None,
) -> tuple[Coordinate, dict[str, Any]]:
    if preferred_location_id is not None:
        row = await fetch_one(
            conn,
            """
            SELECT * FROM locations
            WHERE id = %(location_id)s
              AND warehouse_id = %(warehouse_id)s
              AND status = 'active'
            """,
            {"location_id": preferred_location_id, "warehouse_id": warehouse_id},
        )
        if row is not None:
            return _coordinate(row), row
    row = await fetch_one(
        conn,
        """
        SELECT * FROM locations
        WHERE warehouse_id = %(warehouse_id)s
          AND status = 'active'
          AND location_type IN ('dock', 'staging')
        ORDER BY CASE WHEN upper(zone) LIKE 'OUT%%' THEN 0 ELSE 1 END,
                 travel_seq, id
        LIMIT 1
        """,
        {"warehouse_id": warehouse_id},
    )
    if row is None:
        row = await fetch_one(
            conn,
            """
            SELECT * FROM locations
            WHERE warehouse_id = %(warehouse_id)s AND status = 'active'
            ORDER BY x_m, y_m, z_m, travel_seq, id
            LIMIT 1
            """,
            {"warehouse_id": warehouse_id},
        )
    if row is None:
        raise NotFoundError("Outbound dock or staging location", warehouse_id)
    return _coordinate(row), row


def _coordinate(row: dict[str, Any]) -> Coordinate:
    return Coordinate(
        x=to_decimal(row["x_m"]),
        y=to_decimal(row["y_m"]),
        z=to_decimal(row["z_m"]),
        zone=str(row["zone"]),
    )


async def get_b2c_candidate_rows(
    conn: Any,
    tenant_id: UUID,
    warehouse_id: UUID,
    line: Any,
    allowed_types: list[str],
) -> list[dict[str, Any]]:
    return await fetch_all(
        conn,
        GET_B2C_CANDIDATES,
        {
            "tenant_id": tenant_id,
            "warehouse_id": warehouse_id,
            "sku_id": line.sku_id,
            "lot_filter_mode": "EXACT" if line.lot_id is not None else "IGNORE",
            "lot_filter": line.lot_id,
            "expiry_min": line.expiry_min,
            "expiry_max": line.expiry_max,
            "allowed_types": allowed_types,
        },
    )


async def get_contract_row(
    conn: Any,
    tenant_id: UUID,
    as_of: Any,
) -> dict[str, Any]:
    from .sql import GET_BILLING_CONTRACT

    row = await fetch_one(
        conn,
        GET_BILLING_CONTRACT,
        {"tenant_id": tenant_id, "as_of": as_of},
    )
    if row is None:
        raise NotFoundError("Active 3PL contract", f"{tenant_id}:{as_of}")
    return row


async def current_utc(_: Any = None) -> Any:
    return utcnow()
