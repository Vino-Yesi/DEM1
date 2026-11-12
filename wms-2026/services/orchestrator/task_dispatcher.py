from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, Sequence
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb
from redis.asyncio import Redis

from . import sql
from .config import Settings
from .db import Database, fetch_all, fetch_one
from .errors import NotFoundError, PermissionDeniedError
from .outbox import emit_event
from .schemas import (
    TaskClaimed,
    TaskClaimIn,
    TaskClaimResponse,
    WorkerHeartbeatIn,
    WorkerHeartbeatResponse,
)
from .utils import to_decimal, utcnow

logger = logging.getLogger("wms.tasks")


@dataclass(frozen=True)
class TaskSpec:
    tenant_id: UUID
    task_type: str
    priority: int
    warehouse_id: UUID
    required_equipment: Sequence[str]
    required_zone: str | None
    location_id: UUID | None
    source_ref_type: str
    source_ref_id: UUID
    wave_id: UUID | None
    order_id: UUID | None
    payload: dict[str, Any]
    not_before: Any


@dataclass(frozen=True)
class TaskPush:
    channel: str
    payload: dict[str, Any]


class TaskDispatcher:
    """Postgres-backed priority queue plus Redis push to eligible workers."""

    def __init__(self, db: Database, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self.redis = Redis.from_url(settings.redis_url, decode_responses=False)

    async def close(self) -> None:
        await self.redis.aclose()

    async def redis_ping(self) -> bool:
        try:
            return bool(await self.redis.ping())
        except Exception:
            logger.exception("redis health check failed")
            return False

    async def publish(self, pushes: Sequence[TaskPush]) -> None:
        if not pushes:
            return
        for push in pushes:
            message = json.dumps(
                jsonable_encoder(push.payload),
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
            delay = self.settings.task_publish_backoff_seconds
            for attempt in range(1, self.settings.task_publish_retries + 1):
                try:
                    await self.redis.publish(push.channel, message)
                    break
                except Exception:
                    if attempt == self.settings.task_publish_retries:
                        logger.exception(
                            "task push exhausted retries",
                            extra={
                                "channel": push.channel,
                                "task_id": str(push.payload.get("task_id")),
                            },
                        )
                    else:
                        await asyncio.sleep(delay)
                        delay *= 2

    async def enqueue_task(self, conn: Any, spec: TaskSpec) -> UUID | None:
        row = await fetch_one(
            conn,
            sql.TASK_INSERT,
            {
                "tenant_id": spec.tenant_id,
                "task_type": spec.task_type,
                "priority": spec.priority,
                "warehouse_id": spec.warehouse_id,
                "required_equipment": list(spec.required_equipment),
                "required_zone": spec.required_zone,
                "location_id": spec.location_id,
                "source_ref_type": spec.source_ref_type,
                "source_ref_id": spec.source_ref_id,
                "wave_id": spec.wave_id,
                "order_id": spec.order_id,
                "payload": Jsonb(jsonable_encoder(spec.payload)),
                "not_before": spec.not_before,
            },
        )
        if row is None:
            return None
        task_id = UUID(str(row["id"]))
        await emit_event(
            conn,
            tenant_id=spec.tenant_id,
            aggregate_type="task",
            aggregate_id=task_id,
            event_type="task.created",
            payload={
                "task_id": str(task_id),
                "task_type": spec.task_type,
                "priority": spec.priority,
                "required_zone": spec.required_zone,
                "required_equipment": list(spec.required_equipment),
                "source_ref_type": spec.source_ref_type,
                "source_ref_id": str(spec.source_ref_id),
            },
            headers={"correlation_id": str(spec.source_ref_id)},
        )
        return task_id

    async def assign_queued_tasks(
        self,
        conn: Any,
        tenant_id: UUID,
        *,
        warehouse_id: UUID | None = None,
        limit: int | None = None,
    ) -> list[TaskPush]:
        now = utcnow()
        tasks = await fetch_all(
            conn,
            sql.TASK_QUEUE_CLAIM,
            {
                "tenant_id": tenant_id,
                "now": now,
                "warehouse_id": warehouse_id,
                "limit": limit or self.settings.max_tasks_per_dispatch,
            },
        )
        pushes: list[TaskPush] = []
        heartbeat_cutoff = now - timedelta(
            seconds=self.settings.worker_heartbeat_ttl_seconds
        )
        for task in tasks:
            payload = task.get("payload") or {}
            dock = payload.get("dock") or {}
            worker = await fetch_one(
                conn,
                sql.FIND_ELIGIBLE_WORKER,
                {
                    "tenant_id": tenant_id,
                    "warehouse_id": task["warehouse_id"],
                    "required_equipment": list(task["required_equipment"]),
                    "required_zone": task["required_zone"],
                    "heartbeat_cutoff": heartbeat_cutoff,
                    "task_priority": int(task["priority"]),
                    "worker_x": to_decimal(dock.get("x", 0)),
                    "worker_y": to_decimal(dock.get("y", 0)),
                },
            )
            if worker is None:
                continue
            updated = await fetch_one(
                conn,
                sql.UPDATE_TASK_ASSIGNED,
                {
                    "tenant_id": tenant_id,
                    "task_id": task["id"],
                    "worker_id": worker["id"],
                    "version": int(task["version"]),
                },
            )
            if updated is None:
                continue
            score = to_decimal(worker["score"])
            await conn.execute(
                sql.INSERT_TASK_ASSIGNMENT,
                {
                    "tenant_id": tenant_id,
                    "task_id": task["id"],
                    "worker_id": worker["id"],
                    "score": score,
                },
            )
            await emit_event(
                conn,
                tenant_id=tenant_id,
                aggregate_type="task",
                aggregate_id=UUID(str(task["id"])),
                event_type="task.assigned",
                payload={
                    "task_id": str(task["id"]),
                    "worker_id": str(worker["id"]),
                    "score": score,
                    "task_type": str(task["task_type"]),
                    "required_zone": task["required_zone"],
                },
                headers={"correlation_id": str(task["id"])},
            )
            pushes.append(
                TaskPush(
                    channel=f"wms.tasks.{tenant_id}.{worker['id']}",
                    payload={
                        "task_id": UUID(str(task["id"])),
                        "task_type": str(task["task_type"]),
                        "priority": int(task["priority"]),
                        "required_zone": task["required_zone"],
                        "required_equipment": list(task["required_equipment"]),
                        "payload": payload,
                        "score": score,
                        "assigned_at": now,
                        "state": "assigned",
                    },
                )
            )
        return pushes

    async def heartbeat(
        self,
        conn: Any,
        tenant_id: UUID,
        worker_id: UUID,
        payload: WorkerHeartbeatIn,
    ) -> tuple[WorkerHeartbeatResponse, list[TaskPush]]:
        row = await fetch_one(
            conn,
            sql.UPSERT_WORKER,
            {
                "tenant_id": tenant_id,
                "worker_id": worker_id,
                "employee_code": payload.employee_code,
                "display_name": payload.display_name,
                "warehouse_id": payload.warehouse_id,
                "status": payload.status.value,
                "active_equipment": payload.active_equipment.value,
                "current_zone": payload.current_zone,
                "x_m": payload.x_m,
                "y_m": payload.y_m,
                "max_concurrent_tasks": payload.max_concurrent_tasks,
                "skills": list(payload.skills),
            },
        )
        if row is None:
            raise NotFoundError("Worker", worker_id)
        if payload.current_zone is not None:
            await conn.execute(
                sql.UPSERT_WORKER_ZONE,
                {
                    "tenant_id": tenant_id,
                    "worker_id": worker_id,
                    "warehouse_id": payload.warehouse_id,
                    "zone": payload.current_zone,
                },
            )
        pushes = await self.assign_queued_tasks(
            conn,
            tenant_id,
            warehouse_id=payload.warehouse_id,
            limit=payload.max_concurrent_tasks * 2,
        )
        assigned_ids = [UUID(str(item.payload["task_id"])) for item in pushes]
        return (
            WorkerHeartbeatResponse(
                worker_id=worker_id,
                status=payload.status,
                assigned_tasks=assigned_ids,
            ),
            pushes,
        )

    async def claim_tasks(
        self,
        conn: Any,
        tenant_id: UUID,
        worker_id: UUID,
        claim: TaskClaimIn,
    ) -> tuple[TaskClaimResponse, list[TaskPush]]:
        now = utcnow()
        worker = await fetch_one(
            conn,
            """
            SELECT * FROM workers
            WHERE tenant_id = %(tenant_id)s AND id = %(worker_id)s
            FOR UPDATE
            """,
            {"tenant_id": tenant_id, "worker_id": worker_id},
        )
        if worker is None:
            raise NotFoundError("Worker", worker_id)
        heartbeat_cutoff = now - timedelta(
            seconds=self.settings.worker_heartbeat_ttl_seconds
        )
        if (
            worker["status"] not in ("available", "working")
            or worker["last_heartbeat_at"] < heartbeat_cutoff
        ):
            raise PermissionDeniedError(
                "worker is not operationally available for task claiming",
                {"worker_id": str(worker_id), "status": str(worker["status"])},
            )
        tasks = await fetch_all(
            conn,
            sql.TASK_CLAIM_FOR_WORKER,
            {
                "tenant_id": tenant_id,
                "worker_id": worker_id,
                "warehouse_id": worker["warehouse_id"],
                "zone": worker["current_zone"],
                "equipment": str(worker["active_equipment"]),
                "now": now,
                "limit": claim.max_tasks,
            },
        )
        claimed: list[TaskClaimed] = []
        pushes: list[TaskPush] = []
        active_count_row = await fetch_one(
            conn,
            """
            SELECT count(*) AS n
            FROM task_queue
            WHERE tenant_id = %(tenant_id)s
              AND assigned_worker_id = %(worker_id)s
              AND state IN ('assigned', 'accepted', 'in_progress')
            """,
            {"tenant_id": tenant_id, "worker_id": worker_id},
        )
        active_count = int(active_count_row["n"]) if active_count_row else 0
        for task in tasks:
            if active_count >= int(worker["max_concurrent_tasks"]):
                break
            updated = await fetch_one(
                conn,
                sql.UPDATE_TASK_ASSIGNED,
                {
                    "tenant_id": tenant_id,
                    "task_id": task["id"],
                    "worker_id": worker_id,
                    "version": int(task["version"]),
                },
            )
            if updated is None:
                continue
            zone_match = (
                task["required_zone"] is None
                or task["required_zone"] == worker["current_zone"]
            )
            score = Decimal(
                int(task["priority"]) * 10 + (200 if zone_match else 0) + 150
            )
            await conn.execute(
                sql.INSERT_TASK_ASSIGNMENT,
                {
                    "tenant_id": tenant_id,
                    "task_id": task["id"],
                    "worker_id": worker_id,
                    "score": score,
                },
            )
            await emit_event(
                conn,
                tenant_id=tenant_id,
                aggregate_type="task",
                aggregate_id=UUID(str(task["id"])),
                event_type="task.assigned",
                payload={
                    "task_id": str(task["id"]),
                    "worker_id": str(worker_id),
                    "score": score,
                    "task_type": str(task["task_type"]),
                    "required_zone": task["required_zone"],
                },
                headers={"correlation_id": str(task["id"])},
            )
            task_payload = task.get("payload") or {}
            claimed.append(
                TaskClaimed(
                    task_id=UUID(str(task["id"])),
                    task_type=str(task["task_type"]),
                    priority=int(task["priority"]),
                    payload=dict(task_payload),
                    score=score,
                )
            )
            pushes.append(
                TaskPush(
                    channel=f"wms.tasks.{tenant_id}.{worker_id}",
                    payload={
                        "task_id": UUID(str(task["id"])),
                        "task_type": str(task["task_type"]),
                        "priority": int(task["priority"]),
                        "required_zone": task["required_zone"],
                        "required_equipment": list(task["required_equipment"]),
                        "payload": task_payload,
                        "score": score,
                        "assigned_at": now,
                        "state": "assigned",
                    },
                )
            )
            active_count += 1
        return TaskClaimResponse(worker_id=worker_id, tasks=claimed), pushes
