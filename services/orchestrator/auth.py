from __future__ import annotations

import re
from dataclasses import dataclass
from uuid import UUID

from fastapi import Request

from .errors import PermissionDeniedError, WMSError

_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9_.:-]{8,256}$")


@dataclass(frozen=True)
class TenantContext:
    tenant_id: UUID
    client_id: str
    scopes: frozenset[str]
    idempotency_key: str
    client_ip: str


def _required_header(request: Request, name: str) -> str:
    value = request.headers.get(name)
    if not value or not value.strip():
        raise WMSError(
            "MISSING_HEADER",
            f"{name} is required",
            status_code=401,
            details={"header": name},
        )
    return value.strip()


def _resolve_client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        first = forwarded.split(",", 1)[0].strip()
        if first:
            return first

    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        cleaned = real_ip.strip()
        if cleaned:
            return cleaned

    client = request.client
    if client is not None:
        return client.host or ""
    return ""


def get_tenant_context(request: Request) -> TenantContext:
    tenant_raw = _required_header(request, "X-Tenant-Id")
    client_id = _required_header(request, "X-Client-Id")
    idempotency_key = _required_header(request, "X-Idempotency-Key")
    if not _IDEMPOTENCY_KEY.fullmatch(idempotency_key):
        raise WMSError(
            "INVALID_IDEMPOTENCY_KEY",
            "X-Idempotency-Key must be 8-256 characters using letters, digits, underscore, dot, colon or hyphen",
            status_code=400,
        )
    try:
        tenant_id = UUID(tenant_raw)
    except ValueError as exc:
        raise WMSError(
            "INVALID_TENANT_ID",
            "X-Tenant-Id must be a UUID",
            status_code=400,
        ) from exc

    raw_scopes = request.headers.get("X-Scopes", "")
    scopes = frozenset(item.strip() for item in raw_scopes.split(",") if item.strip())
    return TenantContext(
        tenant_id=tenant_id,
        client_id=client_id,
        scopes=scopes,
        idempotency_key=idempotency_key,
        client_ip=_resolve_client_ip(request),
    )


def require_scope(ctx: TenantContext, scope: str) -> None:
    if scope not in ctx.scopes:
        raise PermissionDeniedError(
            "API client lacks required gateway scope",
            {"required_scope": scope, "present_scopes": sorted(ctx.scopes)},
        )
