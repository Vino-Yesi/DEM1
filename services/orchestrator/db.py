from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import Settings


class Database:
    """Dual-pool PostgreSQL access.

    app_pool uses wms_app and is always guarded by RLS. system_pool uses
    wms_migration/BYPASSRLS and is reserved for the transactional outbox relay.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        pool_kwargs: dict[str, Any] = {
            "row_factory": dict_row,
            "autocommit": False,
        }
        self.app_pool = AsyncConnectionPool(
            conninfo=settings.database_dsn,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
            open=False,
            kwargs=pool_kwargs,
        )
        self.system_pool = AsyncConnectionPool(
            conninfo=settings.system_database_dsn,
            min_size=max(1, settings.db_pool_min // 4),
            max_size=max(2, settings.db_pool_max // 4),
            open=False,
            kwargs=pool_kwargs,
        )

    async def open(self) -> None:
        await asyncio.gather(
            self.app_pool.open(
                wait=True, timeout=self.settings.db_connect_timeout_seconds
            ),
            self.system_pool.open(
                wait=True, timeout=self.settings.db_connect_timeout_seconds
            ),
        )

    async def close(self) -> None:
        await asyncio.gather(self.app_pool.close(), self.system_pool.close())

    async def ready(self) -> bool:
        async with self.app_pool.connection() as conn:
            row = await (await conn.execute("SELECT 1 AS ok")).fetchone()
            return bool(row and row["ok"] == 1)

    @asynccontextmanager
    async def tenant_transaction(
        self,
        tenant_id: UUID,
        *,
        repeatable_read: bool = False,
    ) -> AsyncIterator[Any]:
        async with self.app_pool.connection() as conn:
            async with conn.transaction():
                if repeatable_read:
                    await conn.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                    )
                await conn.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (str(self.settings.statement_timeout_ms),),
                )
                await conn.execute(
                    "SELECT set_config('app.current_tenant_id', %s, true)",
                    (str(tenant_id),),
                )
                yield conn

    @asynccontextmanager
    async def system_transaction(self) -> AsyncIterator[Any]:
        async with self.system_pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (str(self.settings.statement_timeout_ms),),
                )
                await conn.execute(
                    "SELECT set_config('app.current_tenant_id', '', true)"
                )
                yield conn


async def fetch_one(conn: Any, sql: str, params: Any = None) -> dict[str, Any] | None:
    cursor = await conn.execute(sql, params or ())
    return await cursor.fetchone()


async def fetch_all(conn: Any, sql: str, params: Any = None) -> list[dict[str, Any]]:
    cursor = await conn.execute(sql, params or ())
    return list(await cursor.fetchall())


async def execute(conn: Any, sql: str, params: Any = None) -> Any:
    return await conn.execute(sql, params or ())


async def psycopg_version(conn: Any) -> str:
    row = await fetch_one(conn, "SELECT version() AS version")
    return str(row["version"]) if row else "unknown"
