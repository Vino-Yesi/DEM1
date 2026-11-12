from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer
from fastapi.encoders import jsonable_encoder
from psycopg.types.json import Jsonb

from .config import Settings
from .db import Database, fetch_all
from .utils import utcnow

logger = logging.getLogger("wms.outbox")


async def emit_event(
    conn: Any,
    *,
    tenant_id: UUID,
    aggregate_type: str,
    aggregate_id: UUID,
    event_type: str,
    payload: dict[str, Any],
    occurred_at: datetime | None = None,
    routing_key: str | None = None,
    headers: dict[str, Any] | None = None,
) -> UUID:
    event_id = uuid4()
    event_time = occurred_at or utcnow()
    event_headers = dict(headers or {})
    event_headers.setdefault("event_id", str(event_id))
    event_headers.setdefault("correlation_id", str(aggregate_id))
    await conn.execute(
        """
        INSERT INTO event_outbox
            (id, tenant_id, aggregate_type, aggregate_id, event_type,
             routing_key, payload, headers, occurred_at)
        VALUES
            (%(id)s, %(tenant_id)s, %(aggregate_type)s, %(aggregate_id)s,
             %(event_type)s, %(routing_key)s, %(payload)s::jsonb,
             %(headers)s::jsonb, %(occurred_at)s)
        """,
        {
            "id": event_id,
            "tenant_id": tenant_id,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "routing_key": routing_key or f"{aggregate_type}.{event_type}",
            "payload": Jsonb(jsonable_encoder(payload)),
            "headers": Jsonb(jsonable_encoder(event_headers)),
            "occurred_at": event_time,
        },
    )
    return event_id


class OutboxRelay:
    """Publishes committed outbox rows to Kafka with an idempotent producer.

    Consumers deduplicate on headers.event_id. The relay is safe to run with
    multiple replicas because candidate rows are locked with SKIP LOCKED.
    """

    def __init__(self, db: Database, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self._producer: AIOKafkaProducer | None = None
        self._stop_event = asyncio.Event()
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        brokers = [
            item.strip()
            for item in self.settings.kafka_bootstrap_servers.split(",")
            if item.strip()
        ]
        self._producer = AIOKafkaProducer(
            bootstrap_servers=brokers,
            client_id=f"{self.settings.service_name}-outbox",
            enable_idempotence=True,
            acks="all",
            linger_ms=5,
            request_timeout_ms=30000,
            retry_backoff_ms=100,
        )
        await self._producer.start()
        self._started = True
        self._stop_event.clear()

    async def stop(self) -> None:
        self._stop_event.set()
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        self._started = False

    def _topic(self, routing_key: str) -> str:
        normalized = (
            routing_key.strip().replace(":", ".").replace(" ", "_").replace("/", ".")
        )
        return f"{self.settings.kafka_topic_prefix}.{normalized}"

    @staticmethod
    def _kafka_headers(
        row: dict[str, Any], dead_letter: bool
    ) -> list[tuple[str, bytes]]:
        merged = dict(row.get("headers") or {})
        merged.update(
            {
                "event_id": str(row["id"]),
                "tenant_id": str(row["tenant_id"]),
                "event_type": str(row["event_type"]),
                "aggregate_type": str(row["aggregate_type"]),
                "aggregate_id": str(row["aggregate_id"]),
                "occurred_at": str(row["occurred_at"]),
                "dead_letter": "true" if dead_letter else "false",
            }
        )
        return [(str(k), str(v).encode("utf-8")) for k, v in merged.items()]

    async def run_once(self) -> int:
        if not self._started or self._producer is None:
            await self.start()

        assert self._producer is not None
        producer = self._producer
        async with self.db.system_transaction() as conn:
            rows = await fetch_all(
                conn,
                """
                SELECT id, tenant_id, aggregate_type, aggregate_id, event_type,
                       routing_key, payload, headers, occurred_at, publish_attempts
                FROM event_outbox
                WHERE published_at IS NULL
                  AND publish_attempts < %(max_attempts)s
                ORDER BY occurred_at, id
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
                """,
                {
                    "max_attempts": self.settings.outbox_max_attempts,
                    "limit": self.settings.outbox_batch_size,
                },
            )
            processed = 0
            for row in rows:
                dead_letter = (
                    int(row["publish_attempts"]) + 1
                    >= self.settings.outbox_max_attempts
                )
                topic = (
                    f"{self.settings.kafka_topic_prefix}.deadletter.{row['event_type']}"
                    if dead_letter
                    else self._topic(str(row["routing_key"]))
                )
                value = json.dumps(
                    {
                        "event_id": str(row["id"]),
                        "tenant_id": str(row["tenant_id"]),
                        "aggregate_type": row["aggregate_type"],
                        "aggregate_id": str(row["aggregate_id"]),
                        "event_type": row["event_type"],
                        "occurred_at": str(row["occurred_at"]),
                        "payload": row["payload"],
                        "headers": row.get("headers") or {},
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode("utf-8")
                try:
                    await producer.send_and_wait(
                        topic,
                        key=str(row["aggregate_id"]).encode("utf-8"),
                        value=value,
                        headers=self._kafka_headers(row, dead_letter),
                    )
                    await conn.execute(
                        """
                        UPDATE event_outbox
                           SET published_at = now(),
                               publish_attempts = publish_attempts + 1
                         WHERE id = %(id)s
                           AND occurred_at = %(occurred_at)s
                        """,
                        {"id": row["id"], "occurred_at": row["occurred_at"]},
                    )
                    processed += 1
                except Exception:
                    logger.exception(
                        "outbox publish failed",
                        extra={
                            "event_id": str(row["id"]),
                            "event_type": row["event_type"],
                            "topic": topic,
                            "attempt": int(row["publish_attempts"]) + 1,
                        },
                    )
                    await conn.execute(
                        """
                        UPDATE event_outbox
                           SET publish_attempts = publish_attempts + 1
                         WHERE id = %(id)s
                           AND occurred_at = %(occurred_at)s
                        """,
                        {"id": row["id"], "occurred_at": row["occurred_at"]},
                    )
            return processed

    async def run_forever(self) -> None:
        await self.start()
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox relay iteration failed")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.settings.outbox_poll_seconds
                )
            except asyncio.TimeoutError:
                continue
