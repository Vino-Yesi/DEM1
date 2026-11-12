from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb

from .db import fetch_one
from .errors import ConflictError
from .utils import sha256_json


class IdempotencyGuard:
    """Exactly-once API effects using a non-partitioned key table."""

    async def begin(
        self,
        conn: Any,
        tenant_id: UUID,
        scope: str,
        key: str,
        payload: Any,
    ) -> dict[str, Any] | None:
        payload_json = jsonable_encoder(payload)
        request_hash = sha256_json(
            {"scope": scope, "key": key, "payload": payload_json}
        )
        params = {
            "tenant_id": tenant_id,
            "scope": scope,
            "key": key,
            "request_hash": request_hash,
        }
        inserted = await fetch_one(
            conn,
            """
            INSERT INTO idempotency_keys
                (tenant_id, scope, key, request_hash, status)
            VALUES
                (%(tenant_id)s, %(scope)s, %(key)s, %(request_hash)s, 'in_progress')
            ON CONFLICT (tenant_id, scope, key) DO NOTHING
            RETURNING 1 AS inserted
            """,
            params,
        )
        row = await fetch_one(
            conn,
            """
            SELECT status, request_hash, response, created_at
            FROM idempotency_keys
            WHERE tenant_id = %(tenant_id)s
              AND scope = %(scope)s
              AND key = %(key)s
            """,
            params,
        )
        if row is None:
            raise ConflictError("idempotency key row disappeared during request")

        if inserted is not None:
            return None

        if str(row["request_hash"]) != request_hash:
            raise ConflictError(
                "idempotency key was already used with a different request",
                {"scope": scope, "key": key},
            )
        if row["status"] == "completed":
            return dict(row["response"] or {})
        if row["status"] == "failed":
            raise ConflictError(
                "idempotency key belongs to a failed request; submit a new key",
                {"scope": scope, "key": key},
            )
        raise ConflictError(
            "an identical request is already in progress; retry with the same key after completion",
            {"scope": scope, "key": key},
        )

    async def complete(
        self,
        conn: Any,
        tenant_id: UUID,
        scope: str,
        key: str,
        response: Any,
    ) -> None:
        response_json = jsonable_encoder(response)
        await conn.execute(
            """
            UPDATE idempotency_keys
               SET status = 'completed',
                   response = %(response)s::jsonb,
                   completed_at = now()
             WHERE tenant_id = %(tenant_id)s
               AND scope = %(scope)s
               AND key = %(key)s
            """,
            {
                "tenant_id": tenant_id,
                "scope": scope,
                "key": key,
                "response": Jsonb(response_json),
            },
        )

    async def fail(self, conn: Any, tenant_id: UUID, scope: str, key: str) -> None:
        await conn.execute(
            """
            UPDATE idempotency_keys
               SET status = 'failed'
             WHERE tenant_id = %(tenant_id)s
               AND scope = %(scope)s
               AND key = %(key)s
               AND status = 'in_progress'
            """,
            {"tenant_id": tenant_id, "scope": scope, "key": key},
        )
