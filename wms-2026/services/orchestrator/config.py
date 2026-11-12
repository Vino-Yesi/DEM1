from __future__ import annotations

import copy
import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

DEFAULT_WORKFLOW: dict[str, Any] = {
    "version": 1,
    "picking": {
        "strategy": "CLUSTER",
        "wave_release_window_minutes": 15,
        "max_orders_per_wave": 120,
        "max_lines_per_wave": 600,
        "max_units_per_wave": 4000,
        "max_zone_count": 2,
        "max_cluster_travel_m": 900,
        "affinity_threshold": 0.62,
        "weights": {
            "location": 0.42,
            "sku": 0.18,
            "zone": 0.15,
            "travel": 0.20,
            "sla": 0.05,
        },
        "zone_penalty_m": 25,
        "vertical_penalty": 3,
        "allowed_b2c_location_types": ["piece_pick", "pallet", "bulk"],
        "prefer_pick_face": True,
        "cutoff_bucket_minutes": 15,
    },
    "b2c": {
        "required_equipment": ["RF_SCANNER", "CART", "AMR"],
        "max_lines_per_task": 40,
        "release_immediately": True,
    },
    "b2b": {
        "prefer_cross_dock": True,
        "allowed_pallet_location_types": ["cross_dock", "staging", "pallet", "bulk"],
        "putaway_location_types": {
            "RESERVE": ["pallet", "bulk"],
            "BULK_STAGING": ["bulk", "staging"],
            "CROSS_DOCK": ["cross_dock", "staging"],
        },
        "required_equipment": ["FORKLIFT", "REACH_TRUCK", "AGV"],
        "max_pallets_per_task": 8,
    },
    "exceptions": {
        "quarantine_on_discrepancy": True,
        "cycle_count_priority": 1000,
        "reallocate_same_lot": True,
        "cycle_count_equipment": ["RF_SCANNER", "AMR"],
        "replenishment_priority": 900,
    },
    "billing": {
        "rounding": "HALF_UP",
        "tax_engine": "EXTERNAL",
    },
}


@dataclass(frozen=True)
class Settings:
    service_name: str
    environment: str
    database_dsn: str
    system_database_dsn: str
    redis_url: str
    kafka_bootstrap_servers: str
    kafka_topic_prefix: str
    db_pool_min: int
    db_pool_max: int
    db_connect_timeout_seconds: int
    statement_timeout_ms: int
    worker_heartbeat_ttl_seconds: int
    task_publish_retries: int
    task_publish_backoff_seconds: float
    max_tasks_per_dispatch: int
    outbox_batch_size: int
    outbox_poll_seconds: float
    outbox_max_attempts: int
    default_workflow: dict[str, Any] = field(default_factory=dict)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    database_dsn = os.getenv(
        "WMS_DATABASE_URL",
        "postgresql://wms_app@127.0.0.1:5432/wms?options=-csearch_path%3Dwms,public",
    )
    return Settings(
        service_name=os.getenv("WMS_SERVICE_NAME", "wms-orchestrator"),
        environment=os.getenv("WMS_ENVIRONMENT", "production"),
        database_dsn=database_dsn,
        system_database_dsn=os.getenv("WMS_SYSTEM_DATABASE_URL", database_dsn),
        redis_url=os.getenv("WMS_REDIS_URL", "redis://127.0.0.1:6379/0"),
        kafka_bootstrap_servers=os.getenv(
            "WMS_KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        ),
        kafka_topic_prefix=os.getenv("WMS_KAFKA_TOPIC_PREFIX", "wms"),
        db_pool_min=int(os.getenv("WMS_DB_POOL_MIN", "8")),
        db_pool_max=int(os.getenv("WMS_DB_POOL_MAX", "64")),
        db_connect_timeout_seconds=int(
            os.getenv("WMS_DB_CONNECT_TIMEOUT_SECONDS", "10")
        ),
        statement_timeout_ms=int(os.getenv("WMS_STATEMENT_TIMEOUT_MS", "5000")),
        worker_heartbeat_ttl_seconds=int(
            os.getenv("WMS_WORKER_HEARTBEAT_TTL_SECONDS", "90")
        ),
        task_publish_retries=int(os.getenv("WMS_TASK_PUBLISH_RETRIES", "4")),
        task_publish_backoff_seconds=float(
            os.getenv("WMS_TASK_PUBLISH_BACKOFF_SECONDS", "0.15")
        ),
        max_tasks_per_dispatch=int(os.getenv("WMS_MAX_TASKS_PER_DISPATCH", "50")),
        outbox_batch_size=int(os.getenv("WMS_OUTBOX_BATCH_SIZE", "200")),
        outbox_poll_seconds=float(os.getenv("WMS_OUTBOX_POLL_SECONDS", "0.25")),
        outbox_max_attempts=int(os.getenv("WMS_OUTBOX_MAX_ATTEMPTS", "12")),
        default_workflow=copy.deepcopy(DEFAULT_WORKFLOW),
    )
